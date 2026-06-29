//! Heartbeat bridge: local node → control plane.
//!
//! The sidecar is the node's voice to the brain. On a timer it scrapes the local
//! node's `/v1/status` (which shards it hosts/leads) and POSTs a compact heartbeat
//! — plus the node's [`NodeMeta`] (address + failure domain) — to the brain's
//! `/v1/nodes/{id}/heartbeat`. This keeps the **node decoupled** from the control
//! plane: the node doesn't need to know the brain exists; the sidecar bridges.
//!
//! Caveat (flagged, not resolved): if liveness is reported *only* by the sidecar,
//! a dead sidecar looks like a dead node. Either tie sidecar liveness to the
//! node's, or keep a minimal direct node→brain ping and let the sidecar own only
//! the richer metadata.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::Serialize;
use serde_json::Value;

use crate::meta::NodeMeta;

/// Run the heartbeat loop forever.
pub async fn run(node_url: String, brain_url: String, meta: NodeMeta, interval: Duration) {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap_or_default();
    // Monotonic heartbeat sequence. Seeded from the wall clock at startup so a
    // sidecar restart resumes ABOVE its previous numbers (a restart can't be
    // mistaken for a stale heartbeat), then incremented once per send. The brain
    // ignores any heartbeat whose seq is not strictly newer than the last seen,
    // so reordered/duplicated deliveries can't revert newer reported state.
    let seq = AtomicU64::new(now_ms());
    let mut tick = tokio::time::interval(interval);
    loop {
        tick.tick().await;
        match scrape_node_status(&client, &node_url).await {
            Some(status) => {
                let n = seq.fetch_add(1, Ordering::Relaxed);
                if let Err(err) = send_heartbeat(&client, &brain_url, &meta, status, n).await {
                    tracing::warn!("heartbeat to brain {brain_url} failed: {err}");
                }
            }
            None => {
                // Node unreachable: skip and let the brain's failure detector
                // notice the missed heartbeats (Healthy → Suspect → Dead).
                tracing::warn!("local node {node_url} unreachable; skipping heartbeat");
            }
        }
    }
}

/// A compact view of what the local node reports about itself.
#[derive(Debug, Default)]
pub struct NodeStatusSummary {
    pub leading_shards: Vec<u32>,
    #[cfg(test)]
    pub following_shards: Vec<u32>,
    pub hosted_shards: Vec<u32>,
}

/// The body the brain expects at `/v1/nodes/{id}/heartbeat` (mirrors the brain's
/// `HeartbeatReport`; kept local so the two services share no compile dependency).
#[derive(Debug, Serialize)]
struct HeartbeatBody {
    address: String,
    failure_domain: String,
    hosted_shards: Vec<u32>,
    leading_shards: Vec<u32>,
    /// Monotonic per-send sequence so the brain can drop reordered/duplicated
    /// heartbeats (it ignores any whose `seq` is not newer than the last seen).
    seq: u64,
}

/// `GET {node_url}/v1/status` → distill the `consensus` block to what the brain
/// needs (which shards this node hosts, and which it leads).
async fn scrape_node_status(client: &reqwest::Client, node_url: &str) -> Option<NodeStatusSummary> {
    let url = format!("{}/v1/status", node_url.trim_end_matches('/'));
    let body: Value = client.get(url).send().await.ok()?.json().await.ok()?;
    Some(status_from_value(&body))
}

fn status_from_value(body: &Value) -> NodeStatusSummary {
    let consensus = &body["consensus"];

    let mut leading_shards = u32_array(&consensus["leading_shards"]);
    if leading_shards.is_empty() {
        leading_shards = shard_ids_by_role(consensus, "leader");
    }
    let mut following_shards = u32_array(&consensus["following_shards"]);
    if following_shards.is_empty() {
        following_shards = shard_ids_by_role(consensus, "follower");
    }
    let mut hosted_shards = u32_array(&consensus["hosted_shards"]);
    if hosted_shards.is_empty() {
        hosted_shards = shard_ids_from_rows(consensus);
    }
    if hosted_shards.is_empty() {
        hosted_shards.extend(leading_shards.iter().copied());
        hosted_shards.extend(following_shards.iter().copied());
    }

    sort_dedup(&mut leading_shards);
    sort_dedup(&mut following_shards);
    sort_dedup(&mut hosted_shards);

    NodeStatusSummary {
        leading_shards,
        #[cfg(test)]
        following_shards,
        hosted_shards,
    }
}

/// `POST {brain_url}/v1/nodes/{node_id}/heartbeat` with metadata + shard summary.
async fn send_heartbeat(
    client: &reqwest::Client,
    brain_url: &str,
    meta: &NodeMeta,
    status: NodeStatusSummary,
    seq: u64,
) -> Result<(), reqwest::Error> {
    let url = format!(
        "{}/v1/nodes/{}/heartbeat",
        brain_url.trim_end_matches('/'),
        meta.node_id
    );
    let body = HeartbeatBody {
        address: meta.address.clone(),
        failure_domain: meta.failure_domain(),
        hosted_shards: status.hosted_shards,
        leading_shards: status.leading_shards,
        seq,
    };
    client
        .post(url)
        .json(&body)
        .send()
        .await?
        .error_for_status()?;
    Ok(())
}

/// Wall-clock milliseconds since the Unix epoch (seeds the heartbeat sequence).
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn u32_array(value: &Value) -> Vec<u32> {
    value
        .as_array()
        .map(|xs| {
            xs.iter()
                .filter_map(|x| x.as_u64().and_then(|n| u32::try_from(n).ok()))
                .collect()
        })
        .unwrap_or_default()
}

fn shard_ids_from_rows(consensus: &Value) -> Vec<u32> {
    consensus["shards"]
        .as_array()
        .map(|shards| {
            shards
                .iter()
                .filter_map(|s| s["shard_id"].as_u64().and_then(|n| u32::try_from(n).ok()))
                .collect()
        })
        .unwrap_or_default()
}

fn shard_ids_by_role(consensus: &Value, role: &str) -> Vec<u32> {
    consensus["shards"]
        .as_array()
        .map(|shards| {
            shards
                .iter()
                .filter(|s| s["role"].as_str() == Some(role))
                .filter_map(|s| s["shard_id"].as_u64().and_then(|n| u32::try_from(n).ok()))
                .collect()
        })
        .unwrap_or_default()
}

fn sort_dedup(shards: &mut Vec<u32>) {
    shards.sort_unstable();
    shards.dedup();
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn status_parser_extracts_leading_and_hosted_shards() {
        let status = status_from_value(&json!({
            "consensus": {
                "hosted_shards": [3, 2, 1, 0, 0],
                "leading_shards": [2, 0, 2],
                "following_shards": [3, 1],
                "shards": [
                    { "shard_id": 0, "role": "leader" },
                    { "shard_id": 1, "role": "follower" },
                    { "shard_id": 2, "role": "leader" },
                    { "shard_id": 3, "role": "follower" }
                ]
            }
        }));

        assert_eq!(status.leading_shards, vec![0, 2]);
        assert_eq!(status.following_shards, vec![1, 3]);
        assert_eq!(status.hosted_shards, vec![0, 1, 2, 3]);
    }

    #[test]
    fn status_parser_derives_roles_from_shard_rows_when_compact_fields_are_missing() {
        let status = status_from_value(&json!({
            "consensus": {
                "shards": [
                    { "shard_id": 7, "role": "follower" },
                    { "shard_id": 5, "role": "leader" },
                    { "shard_id": 6, "role": "follower" },
                    { "shard_id": 4, "role": "leader" }
                ]
            }
        }));

        assert_eq!(status.leading_shards, vec![4, 5]);
        assert_eq!(status.following_shards, vec![6, 7]);
        assert_eq!(status.hosted_shards, vec![4, 5, 6, 7]);
    }

    #[test]
    fn status_parser_tolerates_missing_or_malformed_consensus_fields() {
        let status = status_from_value(&json!({
            "consensus": {
                "leading_shards": ["bad", 3, 4294967296u64],
                "shards": [
                    { "shard_id": "bad" },
                    { "shard_id": 4, "role": "follower" },
                    { "shard_id": 4294967296u64, "role": "follower" },
                    {}
                ]
            }
        }));

        assert_eq!(status.leading_shards, vec![3]);
        assert_eq!(status.following_shards, vec![4]);
        assert_eq!(status.hosted_shards, vec![4]);
    }

    #[test]
    fn status_parser_uses_role_arrays_as_hosted_fallback() {
        let status = status_from_value(&json!({
            "consensus": {
                "leading_shards": [9, 2, 9],
                "following_shards": [3, 2, 8]
            }
        }));

        assert_eq!(status.leading_shards, vec![2, 9]);
        assert_eq!(status.following_shards, vec![2, 3, 8]);
        assert_eq!(status.hosted_shards, vec![2, 3, 8, 9]);
    }

    #[test]
    fn status_parser_ignores_unknown_shard_roles() {
        let status = status_from_value(&json!({
            "consensus": {
                "shards": [
                    { "shard_id": 11, "role": "leader" },
                    { "shard_id": 12, "role": "candidate" },
                    { "shard_id": 13, "role": "follower" },
                    { "shard_id": 14, "role": null }
                ]
            }
        }));

        assert_eq!(status.leading_shards, vec![11]);
        assert_eq!(status.following_shards, vec![13]);
        assert_eq!(status.hosted_shards, vec![11, 12, 13, 14]);
    }

    #[test]
    fn u32_array_keeps_only_unsigned_u32_values() {
        let shards = u32_array(&json!([4, -1, "5", 4294967295u64, 4294967296u64, null]));

        assert_eq!(shards, vec![4, u32::MAX]);
    }
}

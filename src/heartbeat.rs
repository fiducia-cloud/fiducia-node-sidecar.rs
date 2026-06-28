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

    let leading_shards = u32_array(&consensus["leading_shards"]);
    let hosted_shards = consensus["shards"]
        .as_array()
        .map(|shards| {
            shards
                .iter()
                .filter_map(|s| s["shard_id"].as_u64().map(|n| n as u32))
                .collect()
        })
        .unwrap_or_default();

    NodeStatusSummary {
        leading_shards,
        hosted_shards,
    }
}

/// `POST {brain_url}/v1/nodes/{node_id}/heartbeat` with metadata + shard summary.
async fn send_heartbeat(
    client: &reqwest::Client,
    brain_url: &str,
    meta: &NodeMeta,
    status: NodeStatusSummary,
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
    };
    client
        .post(url)
        .json(&body)
        .send()
        .await?
        .error_for_status()?;
    Ok(())
}

fn u32_array(value: &Value) -> Vec<u32> {
    value
        .as_array()
        .map(|xs| {
            xs.iter()
                .filter_map(|x| x.as_u64().map(|n| n as u32))
                .collect()
        })
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn status_parser_extracts_leading_and_hosted_shards() {
        let status = status_from_value(&json!({
            "consensus": {
                "leading_shards": [0, 2],
                "shards": [
                    { "shard_id": 0, "role": "leader" },
                    { "shard_id": 1, "role": "follower" },
                    { "shard_id": 2, "role": "leader" }
                ]
            }
        }));

        assert_eq!(status.leading_shards, vec![0, 2]);
        assert_eq!(status.hosted_shards, vec![0, 1, 2]);
    }

    #[test]
    fn status_parser_tolerates_missing_or_malformed_consensus_fields() {
        let status = status_from_value(&json!({
            "consensus": {
                "leading_shards": ["bad", 3],
                "shards": [
                    { "shard_id": "bad" },
                    { "shard_id": 4 },
                    {}
                ]
            }
        }));

        assert_eq!(status.leading_shards, vec![3]);
        assert_eq!(status.hosted_shards, vec![4]);
    }
}

//! Heartbeat bridge: local node → control plane.
//!
//! The sidecar is the node's voice to the brain. On a timer it scrapes the local
//! node's `/v1/status` (which shards it hosts/leads) and POSTs a compact heartbeat
//! — plus the node's [`NodeMeta`] (address + failure domain) — to the brain's
//! `/v1/nodes/{id}/heartbeat`. This keeps the **node decoupled** from the control
//! plane: the node doesn't need to know the brain exists; the sidecar bridges.
//!
//! Liveness is deliberately reported only by the sidecar. If the sidecar cannot
//! observe the local node or reach the brain, the brain must stop treating that
//! node as schedulable; claiming health from the node process alone would hide a
//! broken control-plane path. Kubernetes restarts the failed sidecar container,
//! while the brain's failure detector keeps placement fail-closed in the interim.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::Serialize;
use serde_json::Value;

use crate::auth::attach;
use crate::meta::NodeMeta;
use crate::metrics::SidecarMetrics;

/// Run the heartbeat loop forever.
pub async fn run(
    node_url: String,
    brain_url: String,
    meta: NodeMeta,
    interval: Duration,
    metrics: std::sync::Arc<SidecarMetrics>,
) {
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
    let node_endpoint = crate::endpoint_label(&node_url);
    let brain_endpoint = crate::endpoint_label(&brain_url);
    let mut tick = tokio::time::interval(interval);
    loop {
        tick.tick().await;
        metrics.node_scrape_attempt();
        match scrape_node_status(&client, &node_url).await {
            Ok(status) => {
                let n = seq.fetch_add(1, Ordering::Relaxed);
                metrics.heartbeat_attempt();
                if let Err(err) = send_heartbeat(&client, &brain_url, &meta, status, n).await {
                    metrics.heartbeat_failure();
                    tracing::warn!(
                        endpoint = %brain_endpoint,
                        timeout = err.is_timeout(),
                        connect = err.is_connect(),
                        status = err.status().map(|status| status.as_u16()),
                        "heartbeat to brain failed"
                    );
                } else {
                    metrics.heartbeat_success();
                }
            }
            Err(error) => {
                metrics.node_scrape_failure();
                // Node unreachable: skip and let the brain's failure detector
                // notice the missed heartbeats (Healthy → Suspect → Dead).
                tracing::warn!(
                    endpoint = %node_endpoint,
                    failure = error.kind(),
                    status = error.status(),
                    "local node status unavailable; skipping heartbeat"
                );
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
#[derive(Debug, PartialEq, Eq)]
enum ScrapeFailure {
    Timeout,
    Connect,
    Status(u16),
    Decode,
    Request,
}

impl ScrapeFailure {
    fn from_reqwest(error: &reqwest::Error) -> Self {
        if error.is_timeout() {
            Self::Timeout
        } else if error.is_connect() {
            Self::Connect
        } else if let Some(status) = error.status() {
            Self::Status(status.as_u16())
        } else if error.is_decode() {
            Self::Decode
        } else {
            Self::Request
        }
    }

    fn kind(&self) -> &'static str {
        match self {
            Self::Timeout => "timeout",
            Self::Connect => "connect",
            Self::Status(_) => "status",
            Self::Decode => "decode",
            Self::Request => "request",
        }
    }

    fn status(&self) -> Option<u16> {
        match self {
            Self::Status(status) => Some(*status),
            _ => None,
        }
    }
}

async fn scrape_node_status(
    client: &reqwest::Client,
    node_url: &str,
) -> Result<NodeStatusSummary, ScrapeFailure> {
    let url = format!("{}/v1/status", node_url.trim_end_matches('/'));
    let response = attach(client.get(url))
        .send()
        .await
        .map_err(|error| ScrapeFailure::from_reqwest(&error))?
        .error_for_status()
        .map_err(|error| ScrapeFailure::from_reqwest(&error))?;
    let body: Value = response
        .json()
        .await
        .map_err(|error| ScrapeFailure::from_reqwest(&error))?;
    Ok(status_from_value(&body))
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
    attach(client.post(url).json(&body))
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
    use axum::extract::{Path, State};
    use axum::http::StatusCode;
    use axum::routing::{get, post};
    use axum::{Json, Router};
    use serde_json::json;
    use std::sync::{Arc, Mutex};

    type HeartbeatCapture = Arc<Mutex<Option<(String, Value)>>>;

    async fn mock_node(status: StatusCode, body: &'static str) -> String {
        let app = Router::new().route("/v1/status", get(move || async move { (status, body) }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        format!("http://{address}")
    }

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

    #[tokio::test]
    async fn scrape_rejects_an_upstream_error_instead_of_reporting_empty_topology() {
        let base = mock_node(StatusCode::SERVICE_UNAVAILABLE, "try later").await;
        let error = scrape_node_status(&reqwest::Client::new(), &base)
            .await
            .unwrap_err();
        assert_eq!(error, ScrapeFailure::Status(503));
    }

    #[tokio::test]
    async fn scrape_rejects_a_malformed_status_document() {
        let base = mock_node(StatusCode::OK, "not-json").await;
        let error = scrape_node_status(&reqwest::Client::new(), &base)
            .await
            .unwrap_err();
        assert_eq!(error, ScrapeFailure::Decode);
    }

    #[tokio::test]
    async fn heartbeat_posts_the_monotonic_sequence_and_failure_domain_contract() {
        let captured: HeartbeatCapture = Arc::new(Mutex::new(None));
        let app = Router::new()
            .route(
                "/v1/nodes/:node_id/heartbeat",
                post(
                    |State(captured): State<HeartbeatCapture>,
                     Path(node_id): Path<String>,
                     Json(body): Json<Value>| async move {
                        *captured.lock().unwrap() = Some((node_id, body));
                        StatusCode::NO_CONTENT
                    },
                ),
            )
            .with_state(captured.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let meta = NodeMeta {
            node_id: "fiducia-node-2.gcp".to_string(),
            address: "http://10.0.0.12:8090".to_string(),
            region: Some("gcp".to_string()),
            availability_zone: Some("us-central1-a".to_string()),
            rack: None,
            version: Some("v1".to_string()),
        };
        send_heartbeat(
            &reqwest::Client::new(),
            &format!("http://{address}"),
            &meta,
            NodeStatusSummary {
                leading_shards: vec![3, 7],
                following_shards: vec![1],
                hosted_shards: vec![1, 3, 7],
            },
            42,
        )
        .await
        .unwrap();

        let (node_id, body) = captured.lock().unwrap().clone().unwrap();
        assert_eq!(node_id, "fiducia-node-2.gcp");
        assert_eq!(body["address"], "http://10.0.0.12:8090");
        assert_eq!(body["failure_domain"], "gcp");
        assert_eq!(body["hosted_shards"], json!([1, 3, 7]));
        assert_eq!(body["leading_shards"], json!([3, 7]));
        assert_eq!(body["seq"], 42);
    }
}

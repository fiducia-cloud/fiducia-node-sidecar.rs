//! Heartbeat bridge: local node → control plane (skeleton).
//!
//! The sidecar is the node's voice to the brain. On a timer it scrapes the local
//! node's `/v1/status` (which shards it hosts/leads, commit progress) and POSTs a
//! compact heartbeat — plus the node's [`NodeMeta`] — to the brain's
//! `/v1/nodes/{id}/heartbeat`. This keeps the **node decoupled** from the control
//! plane: the node doesn't need to know the brain exists; the sidecar bridges.
//!
//! Caveat to decide later: if liveness is reported *only* by the sidecar, a dead
//! sidecar looks like a dead node. Options: tie sidecar liveness to the node's,
//! or keep a minimal direct node→brain liveness ping and let the sidecar own only
//! the richer metadata. Flagged, not resolved.
//!
//! Skeleton: the loop is real; the two HTTP calls are stubbed (need a client).

use std::time::Duration;

use crate::meta::NodeMeta;

/// Run the heartbeat loop forever.
pub async fn run(node_url: String, brain_url: String, meta: NodeMeta, interval: Duration) {
    let mut tick = tokio::time::interval(interval);
    loop {
        tick.tick().await;
        match scrape_node_status(&node_url).await {
            Some(status) => send_heartbeat(&brain_url, &meta, status).await,
            None => {
                // Node unreachable: report it (or simply skip and let the brain's
                // failure detector notice the missed heartbeats).
                tracing::warn!("local node {} unreachable; skipping heartbeat", node_url);
            }
        }
    }
}

/// A compact view of what the local node reports about itself.
pub struct NodeStatusSummary {
    pub leading_shards: Vec<u32>,
    pub hosted_shards: Vec<u32>,
}

/// GET `{node_url}/v1/status` and distill it to what the brain needs.
///
/// TODO: real HTTP GET + parse the node's `consensus` block into the summary.
async fn scrape_node_status(_node_url: &str) -> Option<NodeStatusSummary> {
    // TODO
    None
}

/// POST the heartbeat (+ metadata) to the brain.
///
/// TODO: real HTTP POST to `{brain_url}/v1/nodes/{meta.node_id}/heartbeat` with a
/// body carrying `meta` and the reported shard summary.
async fn send_heartbeat(_brain_url: &str, _meta: &NodeMeta, _status: NodeStatusSummary) {
    // TODO
}

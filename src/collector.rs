//! Observability collection: logs + metrics (skeleton).
//!
//! The other half of the sidecar's job: get the node's telemetry off the box and
//! into the observability stack — **not** the brain (the brain only wants
//! placement metadata, never data-plane logs).
//!
//!   * **logs** — tail the node's stdout / log file and ship to the log backend
//!     (Loki / Elasticsearch / a Vector pipeline).
//!   * **metrics** — scrape the node's `/metrics` and re-expose (or remote-write)
//!     for Prometheus, annotated with this node's identity/failure-domain.
//!
//! Skeleton: signatures + intent only.

/// Tail the local node's logs and forward them to the log backend.
///
/// TODO: follow the node's stdout/log file, batch, and ship (or just run a
/// Vector/Fluent Bit sidecar instead and delete this).
pub async fn ship_logs(_node_log_source: String, _sink: String) {
    // TODO
}

/// Scrape the local node's Prometheus metrics, to be re-exposed on the sidecar's
/// own `/metrics` (annotated with node id / region / AZ).
///
/// TODO: real HTTP GET `{node_url}/metrics`; merge with sidecar-local metrics.
#[allow(dead_code)] // the observability half (logs/metrics) is still a stub
pub async fn scrape_node_metrics(_node_url: &str) -> String {
    // TODO
    String::new()
}

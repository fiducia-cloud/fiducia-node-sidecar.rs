//! fiducia-node-sidecar — the per-node operational sidecar.
//!
//! Runs alongside each `fiducia-node` (same pod, localhost to the node) and owns
//! everything *operational* so the node binary can stay a pure coordination
//! engine:
//!
//!   * **control-plane bridge** — scrape the local node's `/v1/status` and
//!     heartbeat it (plus node metadata / failure domain) to `fiducia-brain`
//!     (see `heartbeat.rs` / `meta.rs`);
//!   * **observability** — ship the node's logs and re-expose its metrics to the
//!     telemetry stack (see `collector.rs`).
//!
//! This is a **skeleton**: the loops and HTTP surface are wired; the calls to the
//! node and brain (and the log/metric shipping) are stubbed pending an HTTP
//! client.

mod collector;
mod heartbeat;
mod meta;

use std::net::SocketAddr;
use std::time::Duration;

use axum::{routing::get, Json, Router};
use serde_json::{json, Value};
use tower_http::trace::TraceLayer;

use meta::NodeMeta;

const SERVICE: &str = "fiducia-node-sidecar";

#[tokio::main]
async fn main() {
    fiducia_telemetry::init(SERVICE);

    let node_url =
        std::env::var("FIDUCIA_NODE_URL").unwrap_or_else(|_| "http://localhost:8090".to_string());
    let brain_url =
        std::env::var("FIDUCIA_BRAIN_URL").unwrap_or_else(|_| "http://localhost:8095".to_string());
    let interval = Duration::from_millis(
        std::env::var("FIDUCIA_HEARTBEAT_MS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(2000),
    );
    let node_meta = NodeMeta::from_env();

    tracing::info!(
        "{SERVICE} for node_id={} (node={node_url}, brain={brain_url}, every {:?})",
        node_meta.node_id,
        interval
    );

    // Bridge: heartbeat the local node's status + metadata to the brain.
    tokio::spawn(heartbeat::run(
        node_url.clone(),
        brain_url,
        node_meta.clone(),
        interval,
    ));

    // Observability: ship logs + scrape metrics (stubs).
    tokio::spawn(collector::ship_logs(
        std::env::var("FIDUCIA_NODE_LOG_SOURCE").unwrap_or_default(),
        std::env::var("FIDUCIA_LOG_SINK").unwrap_or_default(),
    ));

    let app = Router::new()
        .route("/healthz", get(health))
        .route("/readyz", get(health))
        .route("/meta", get(move || meta_handler(node_meta.clone())))
        .route("/metrics", get(metrics))
        .layer(TraceLayer::new_for_http());

    let port: u16 = std::env::var("PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(8091);
    let addr = SocketAddr::from(([0, 0, 0, 0], port));

    tracing::info!("{SERVICE} listening on http://{addr}");
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn health() -> Json<Value> {
    Json(json!({ "status": "ok", "service": SERVICE }))
}

/// `GET /meta` — the node metadata this sidecar reports upstream.
async fn meta_handler(node_meta: NodeMeta) -> Json<Value> {
    Json(json!(node_meta))
}

/// `GET /metrics` — re-exposed node metrics + sidecar-local metrics.
///
/// TODO: merge `collector::scrape_node_metrics(node_url)` with sidecar metrics in
/// Prometheus exposition format.
async fn metrics() -> String {
    String::new()
}

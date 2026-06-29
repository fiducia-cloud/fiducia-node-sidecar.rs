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
//! The heartbeat loop talks to the local node and brain; the observability path
//! ships configured log files and re-exposes the local node metrics endpoint.

mod collector;
mod heartbeat;
mod meta;

use std::net::SocketAddr;
use std::time::Duration;

use axum::{routing::get, Json, Router};
use serde_json::{json, Value};
use tower_http::{
    catch_panic::CatchPanicLayer, limit::RequestBodyLimitLayer, timeout::TimeoutLayer,
    trace::TraceLayer,
};

use meta::NodeMeta;

const SERVICE: &str = "fiducia-node-sidecar";

/// Bound request handling time (the sidecar's endpoints are all fast/local).
const REQUEST_TIMEOUT_SECS: u64 = 15;
/// Cap request bodies; the sidecar serves tiny meta/metrics responses.
const MAX_BODY_BYTES: usize = 64 * 1024;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
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

    // Observability: ship logs and re-expose scraped node metrics.
    tokio::spawn(collector::ship_logs(
        std::env::var("FIDUCIA_NODE_LOG_SOURCE").unwrap_or_default(),
        std::env::var("FIDUCIA_LOG_SINK").unwrap_or_default(),
    ));
    let metrics_node_url = node_url.clone();

    let app = Router::new()
        .route("/healthz", get(health))
        .route("/readyz", get(health))
        .route("/meta", get(move || meta_handler(node_meta.clone())))
        .route("/metrics", get(move || metrics(metrics_node_url.clone())))
        // Hardening stack (outermost last): catch handler panics → 500, bound
        // request time, and cap body size.
        .layer(TraceLayer::new_for_http())
        .layer(TimeoutLayer::new(Duration::from_secs(REQUEST_TIMEOUT_SECS)))
        .layer(RequestBodyLimitLayer::new(MAX_BODY_BYTES))
        .layer(CatchPanicLayer::new());

    let port: u16 = std::env::var("PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(8091);
    let addr = SocketAddr::from(([0, 0, 0, 0], port));

    tracing::info!("{SERVICE} listening on http://{addr}");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn health() -> Json<Value> {
    Json(json!({ "status": "ok", "service": SERVICE }))
}

/// `GET /meta` — the node metadata this sidecar reports upstream.
async fn meta_handler(node_meta: NodeMeta) -> Json<Value> {
    Json(json!(node_meta))
}

/// `GET /metrics` — re-exposed node metrics + sidecar-local metrics. Prefixes a
/// `fiducia_sidecar_up` gauge (the sidecar is serving); the scraped node metrics
/// carry their own `fiducia_sidecar_node_scrape_up` gauge so node-down is visible
/// even when this endpoint is up.
async fn metrics(node_url: String) -> String {
    let node_metrics = collector::scrape_node_metrics(&node_url).await;
    format!(
        "# HELP fiducia_sidecar_up Whether the fiducia node sidecar is serving.\n\
         # TYPE fiducia_sidecar_up gauge\n\
         fiducia_sidecar_up 1\n\
         {node_metrics}"
    )
}

#[cfg(test)]
mod interface_contract_tests {
    use fiducia_interfaces::{LockAcquireManyRequest, ProposeErrorReason};

    #[test]
    fn generated_interfaces_are_importable() {
        let request = LockAcquireManyRequest {
            keys: vec!["orders/42".to_string(), "inventory/sku-7".to_string()],
            holder: Some("worker-a".to_string()),
            ttl_ms: Some(30_000),
            wait: Some(false),
        };

        assert_eq!(request.keys.len(), 2);
        assert!(matches!(
            ProposeErrorReason::NotLeader,
            ProposeErrorReason::NotLeader
        ));
    }
}

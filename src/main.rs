//! fiducia-node-sidecar — the per-node operational sidecar.
//!
//! Runs alongside each `fiducia-node` (same pod, localhost to the node) and owns
//! everything *operational* so the node binary can stay a pure coordination
//! engine:
//!
//!   * **control-plane bridge** — scrape the local node's `/v1/status` and
//!     heartbeat it (plus node metadata / failure domain) to `fiducia-brain`
//!     (see `heartbeat.rs` / `meta.rs`);
//!   * **observability** — ship the node's logs (see `collector.rs`) and expose a
//!     Prometheus `/metrics` endpoint that translates the node's (or the brain's)
//!     JSON introspection into metric families (see `exporter.rs`).
//!
//! The heartbeat loop talks to the local node and brain; the observability path
//! ships configured log files and renders `/metrics` from the node/brain observe
//! APIs. Both outbound planes are authenticated by the shared `auth` module.

mod auth;
mod collector;
mod exporter;
mod heartbeat;
mod meta;
mod metrics;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::{routing::get, Json, Router};
use serde_json::{json, Value};
use tower_http::{
    catch_panic::CatchPanicLayer, limit::RequestBodyLimitLayer, timeout::TimeoutLayer,
    trace::TraceLayer,
};

use exporter::Exporter;
use meta::NodeMeta;
use metrics::SidecarMetrics;

const SERVICE: &str = "fiducia-node-sidecar";

/// Bound request handling time (the sidecar's endpoints are all fast/local).
const REQUEST_TIMEOUT_SECS: u64 = 15;
/// Cap request bodies; the sidecar serves tiny meta/metrics responses.
const MAX_BODY_BYTES: usize = 64 * 1024;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    fiducia_telemetry::init(SERVICE);

    // Both the local node and brain control planes fail closed on this trusted-
    // hop secret. Refuse to run a heartbeat sidecar that can authenticate to
    // neither endpoint.
    required_env("FIDUCIA_INTERNAL_SECRET")?;

    let node_url =
        std::env::var("FIDUCIA_NODE_URL").unwrap_or_else(|_| "http://localhost:8090".to_string());
    let brain_url =
        std::env::var("FIDUCIA_BRAIN_URL").unwrap_or_else(|_| "http://localhost:8095".to_string());
    let interval = positive_ms_env("FIDUCIA_HEARTBEAT_MS", 2000);
    let node_meta = NodeMeta::from_env();
    let role = SidecarRole::from_env();
    let sidecar_metrics = Arc::new(SidecarMetrics::default());

    tracing::info!(
        "{SERVICE} for node_id={} role={role:?} (node={}, brain={}, every {:?})",
        node_meta.node_id,
        endpoint_label(&node_url),
        endpoint_label(&brain_url),
        interval
    );

    // Observability exporter: translate the local node's (or the brain's) JSON
    // introspection into Prometheus metrics for `/metrics`. Built before the
    // heartbeat spawn since that consumes `brain_url`.
    let exporter = Arc::new(Exporter::from_env(
        node_url.clone(),
        brain_url.clone(),
        &node_meta,
    ));

    if role.runs_node_bridge() {
        // Bridge: heartbeat the local node's status + metadata to the brain.
        tokio::spawn(heartbeat::run(
            node_url.clone(),
            brain_url,
            node_meta.clone(),
            interval,
            sidecar_metrics.clone(),
        ));
    } else {
        // Exporter-only (e.g. a brain-mode sidecar) never registers a node.
        tracing::info!("{SERVICE} exporter-only role: node heartbeat disabled");
    }

    // Log forwarding is independent of the node-heartbeat role. The generic
    // source lets the same image observe a brain or another Fiducia workload;
    // the node-specific name remains a compatibility fallback.
    let log_source = std::env::var("FIDUCIA_LOG_SOURCE")
        .or_else(|_| std::env::var("FIDUCIA_NODE_LOG_SOURCE"))
        .unwrap_or_default();
    tokio::spawn(collector::ship_logs(
        log_source,
        std::env::var("FIDUCIA_LOG_SINK").unwrap_or_default(),
        sidecar_metrics.clone(),
    ));

    let app = build_router(node_meta, exporter, sidecar_metrics);

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

/// Assemble the sidecar's HTTP surface. Shared by `main` and the tests so both
/// exercise the exact same routes, handlers, and hardening layers.
fn build_router(
    node_meta: NodeMeta,
    exporter: Arc<Exporter>,
    sidecar_metrics: Arc<SidecarMetrics>,
) -> Router {
    Router::new()
        .route("/healthz", get(health))
        .route("/readyz", get(health))
        .route("/meta", get(move || meta_handler(node_meta.clone())))
        .route(
            "/metrics",
            get(move || metrics(exporter.clone(), sidecar_metrics.clone())),
        )
        // Hardening stack (outermost last): catch handler panics → 500, bound
        // request time, and cap body size.
        .layer(TraceLayer::new_for_http())
        .layer(TimeoutLayer::new(Duration::from_secs(REQUEST_TIMEOUT_SECS)))
        .layer(RequestBodyLimitLayer::new(MAX_BODY_BYTES))
        .layer(CatchPanicLayer::new())
}

fn required_env(name: &str) -> Result<String, std::io::Error> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| std::io::Error::other(format!("{name} must be configured")))
}

/// Credential-safe endpoint label for logs. Paths, query strings, fragments,
/// and userinfo are never operational dimensions and may contain secrets.
pub(crate) fn endpoint_label(raw: &str) -> String {
    let Ok(url) = reqwest::Url::parse(raw) else {
        return "invalid-endpoint".to_string();
    };
    let Some(host) = url.host_str() else {
        return "invalid-endpoint".to_string();
    };
    match url.port() {
        Some(port) => format!("{}://{host}:{port}", url.scheme()),
        None => format!("{}://{host}", url.scheme()),
    }
}

/// What the sidecar runs alongside the `/metrics` exporter.
///
/// A node sidecar (`Full`) also heartbeats the local node to the brain and ships
/// its logs. A brain-mode sidecar has no local node to bridge, so it runs as an
/// `Exporter` only. Selected by `FIDUCIA_SIDECAR_ROLE` (`full` | `exporter`), and
/// forced to `Exporter` whenever `FIDUCIA_EXPORT_TARGET=brain` (a brain sidecar
/// must never register itself as a node).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SidecarRole {
    Full,
    Exporter,
}

impl SidecarRole {
    fn from_env() -> Self {
        Self::classify(
            std::env::var("FIDUCIA_SIDECAR_ROLE").ok().as_deref(),
            std::env::var("FIDUCIA_EXPORT_TARGET").ok().as_deref(),
        )
    }

    /// Pure classifier (testable without touching the process environment).
    fn classify(role: Option<&str>, export_target: Option<&str>) -> Self {
        if matches!(
            export_target
                .map(|t| t.trim().to_ascii_lowercase())
                .as_deref(),
            Some("brain")
        ) {
            return Self::Exporter;
        }
        match role.map(|r| r.trim().to_ascii_lowercase()).as_deref() {
            Some("exporter") => Self::Exporter,
            _ => Self::Full,
        }
    }

    fn runs_node_bridge(self) -> bool {
        matches!(self, Self::Full)
    }
}

/// Parse a positive millisecond interval from the environment. Missing,
/// unparsable, and zero values all fall back to `default_ms`: a zero period
/// makes `tokio::time::interval` panic, which would silently kill the spawned
/// heartbeat task while `/healthz` keeps answering ok (the brain would then see
/// a dead node), and a zero log-ship interval would busy-loop.
pub(crate) fn positive_ms_env(name: &str, default_ms: u64) -> Duration {
    positive_ms(std::env::var(name).ok(), default_ms)
}

fn positive_ms(raw: Option<String>, default_ms: u64) -> Duration {
    Duration::from_millis(
        raw.and_then(|s| s.parse().ok())
            .filter(|&ms| ms > 0)
            .unwrap_or(default_ms),
    )
}

async fn health() -> Json<Value> {
    Json(json!({ "status": "ok", "service": SERVICE }))
}

/// `GET /meta` — the node metadata this sidecar reports upstream.
async fn meta_handler(node_meta: NodeMeta) -> Json<Value> {
    Json(json!(node_meta))
}

/// `GET /metrics` — sidecar-local metrics + the translated node/brain scrape.
/// Prefixes a `fiducia_sidecar_up` gauge (this endpoint is serving); the exporter
/// then appends `fiducia_sidecar_scrape_up{target=...}` so a failed upstream fetch
/// is visible as `up=0` even while this endpoint returns 200.
async fn metrics(exporter: Arc<Exporter>, sidecar_metrics: Arc<SidecarMetrics>) -> String {
    let body = exporter.render().await;
    let local = sidecar_metrics.render();
    format!(
        "# HELP fiducia_sidecar_up Whether the fiducia node sidecar is serving.\n\
         # TYPE fiducia_sidecar_up gauge\n\
         fiducia_sidecar_up 1\n\
         {body}{local}"
    )
}

#[cfg(test)]
mod interval_tests {
    use super::*;

    #[test]
    fn zero_unparsable_negative_and_missing_intervals_fall_back_to_default() {
        for raw in [Some("0"), Some("abc"), Some("-5"), Some(""), None] {
            assert_eq!(
                positive_ms(raw.map(str::to_string), 2000),
                Duration::from_millis(2000),
                "raw={raw:?}"
            );
        }
    }

    #[test]
    fn positive_intervals_are_honored() {
        assert_eq!(
            positive_ms(Some("250".into()), 2000),
            Duration::from_millis(250)
        );
    }

    #[test]
    fn endpoint_labels_drop_credentials_paths_queries_and_fragments() {
        assert_eq!(
            endpoint_label("https://user:secret@brain.example:9443/private?token=x#fragment"),
            "https://brain.example:9443",
        );
        assert_eq!(endpoint_label("not a url"), "invalid-endpoint");
    }
}

#[cfg(test)]
mod role_tests {
    use super::SidecarRole;

    #[test]
    fn default_and_full_role_run_the_node_bridge() {
        assert_eq!(SidecarRole::classify(None, None), SidecarRole::Full);
        assert_eq!(SidecarRole::classify(Some("full"), None), SidecarRole::Full);
        assert_eq!(
            SidecarRole::classify(Some(" FULL "), Some("node")),
            SidecarRole::Full
        );
        assert!(SidecarRole::Full.runs_node_bridge());
    }

    #[test]
    fn explicit_exporter_role_skips_the_node_bridge() {
        assert_eq!(
            SidecarRole::classify(Some("exporter"), None),
            SidecarRole::Exporter
        );
        assert!(!SidecarRole::Exporter.runs_node_bridge());
    }

    #[test]
    fn brain_export_target_forces_exporter_even_if_role_says_full() {
        // A brain sidecar must never heartbeat itself in as a node.
        assert_eq!(
            SidecarRole::classify(Some("full"), Some("brain")),
            SidecarRole::Exporter
        );
        assert_eq!(
            SidecarRole::classify(None, Some(" Brain ")),
            SidecarRole::Exporter
        );
    }
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

#[cfg(test)]
mod metrics_endpoint_tests {
    use super::*;
    use axum::extract::State;
    use axum::http::HeaderMap;
    use serde_json::json;
    use std::sync::Mutex;

    /// Shared capture of the headers every mock-node route saw.
    #[derive(Clone, Default)]
    struct Captured(Arc<Mutex<Vec<HeaderMap>>>);

    async fn mock_shards(State(seen): State<Captured>, headers: HeaderMap) -> Json<Value> {
        seen.0.lock().unwrap().push(headers);
        Json(json!({
            "node_id": "node-a",
            "shard_count": 1,
            "leader_count": 1,
            "follower_count": 0,
            "quorum": {
                "leaderless_shards": [],
                "at_risk_led_shards": [],
                "storage_faulted_shards": [],
                "unresponsive_shards": [],
                "status_complete": true
            },
            "shards": [{
                "shard_id": 0, "role": "leader", "term": 4, "leader_id": "node-a",
                "commit_index": 12, "last_applied": 12, "last_log_index": 12,
                "snapshot_index": 0, "retained_log_entries": 12,
                "storage_healthy": true, "healthy_replicas": 3, "has_quorum": true,
                "replication": [
                    { "peer": "node-b", "match_index": 12, "lag": 0, "in_flight": false }
                ],
                "metrics": {
                    "append_rtt_ms_last": 2, "quorum_rtt_ms_last": 5,
                    "follower_lag_max": 0, "leader_transfer_count": 0
                }
            }]
        }))
    }

    async fn mock_metrics(State(seen): State<Captured>, headers: HeaderMap) -> Json<Value> {
        seen.0.lock().unwrap().push(headers);
        Json(json!({
            "operations": [{
                "op": "kv.put", "count": 2, "errors": 0, "avg_ms": 1.0, "max_ms": 2.0,
                "buckets": [
                    { "le_ms": 1.0, "count": 1 }, { "le_ms": 5.0, "count": 2 },
                    { "le_ms": 25.0, "count": 2 }, { "le_ms": 100.0, "count": 2 },
                    { "le_ms": 500.0, "count": 2 }, { "le_ms": 2000.0, "count": 2 },
                    { "le_ms": null, "count": 2 }
                ]
            }]
        }))
    }

    async fn mock_readyz(State(seen): State<Captured>, headers: HeaderMap) -> Json<Value> {
        seen.0.lock().unwrap().push(headers);
        Json(json!({
            "status": "ok", "all_shards_running": true,
            "unresponsive_shards": [], "storage_faulted_shards": []
        }))
    }

    async fn spawn(app: Router) -> SocketAddr {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        addr
    }

    fn node_meta() -> NodeMeta {
        NodeMeta {
            node_id: "node-a".to_string(),
            address: "http://localhost:8090".to_string(),
            region: Some("us-east-1".to_string()),
            availability_zone: None,
            rack: None,
            version: None,
        }
    }

    fn test_client() -> reqwest::Client {
        reqwest::Client::builder()
            .timeout(Duration::from_secs(3))
            .build()
            .expect("test client")
    }

    #[tokio::test]
    async fn metrics_endpoint_presents_internal_auth_and_never_sends_org_header() {
        let seen = Captured::default();
        let node_app = Router::new()
            .route("/v1/observe/shards", get(mock_shards))
            .route("/v1/observe/metrics", get(mock_metrics))
            .route("/readyz", get(mock_readyz))
            .with_state(seen.clone());
        let node_addr = spawn(node_app).await;

        // The exporter carries an explicit secret so the auth-header assertion is
        // deterministic regardless of process env / OnceLock ordering.
        let exporter = Arc::new(exporter::Exporter {
            target: exporter::Target::Node,
            node_url: format!("http://{node_addr}"),
            brain_url: String::new(),
            client: test_client(),
            secret: Some("test-secret".to_string()),
            labels: exporter::ConstLabels {
                node_id: "node-a".to_string(),
                region: Some("us-east-1".to_string()),
            },
        });

        let sidecar_addr = spawn(build_router(
            node_meta(),
            exporter,
            Arc::new(SidecarMetrics::default()),
        ))
        .await;

        let response = reqwest::get(format!("http://{sidecar_addr}/metrics"))
            .await
            .expect("scrape the sidecar");
        assert_eq!(
            response.status(),
            200,
            "a failed upstream fetch must still 200"
        );
        let body = response.text().await.expect("read body");

        // Sidecar-local gauge first, then the translated node scrape.
        assert!(body.starts_with("# HELP fiducia_sidecar_up"));
        assert!(body.contains("fiducia_sidecar_up 1\n"));
        assert!(body.contains(
            "fiducia_sidecar_scrape_up{node_id=\"node-a\",region=\"us-east-1\",target=\"node\"} 1\n"
        ));
        assert!(body.contains("fiducia_node_up{node_id=\"node-a\",region=\"us-east-1\"} 1\n"));
        assert!(body.contains(
            "fiducia_raft_is_leader{node_id=\"node-a\",region=\"us-east-1\",shard=\"0\"} 1\n"
        ));
        assert!(body.contains(
            "fiducia_op_requests_total{node_id=\"node-a\",region=\"us-east-1\",op=\"kv.put\"} 2\n"
        ));

        // Every upstream fetch presented the trusted-hop secret and no org header.
        let captured = seen.0.lock().unwrap();
        assert_eq!(
            captured.len(),
            3,
            "shards + metrics + readyz each fetched once"
        );
        for headers in captured.iter() {
            assert_eq!(
                headers
                    .get("x-fiducia-internal-auth")
                    .and_then(|v| v.to_str().ok()),
                Some("test-secret"),
                "exporter must present the internal-auth header"
            );
            assert!(
                headers.get("x-fiducia-org-id").is_none(),
                "exporter must not send an org header to org-exempt observe paths"
            );
        }
    }

    async fn mock_brain_status(State(seen): State<Captured>, headers: HeaderMap) -> Json<Value> {
        seen.0.lock().unwrap().push(headers);
        Json(json!({
            "brain_cluster": {
                "is_leader": true,
                "available": true,
                "ha_configured": true,
                "placement_generation": 42
            },
            "topology": {
                "nodes_by_health": { "healthy": 3, "suspect": 1, "dead": 0 }
            },
            "placement": {
                "unplaced_shards": 0,
                "under_replicated_shards": 2,
                "leaderless_shards": 0,
                "shards_with_unhealthy_replicas": 1
            }
        }))
    }

    /// The same sidecar image serves brain pods: `FIDUCIA_EXPORT_TARGET=brain`
    /// translates the brain's `/v1/status` rollup instead of the node observe
    /// API, over the same `/metrics` route and the same trusted-hop auth.
    #[tokio::test]
    async fn brain_mode_metrics_translate_the_brain_status_rollup() {
        let seen = Captured::default();
        let brain_app = Router::new()
            .route("/v1/status", get(mock_brain_status))
            .with_state(seen.clone());
        let brain_addr = spawn(brain_app).await;

        let exporter = Arc::new(exporter::Exporter {
            target: exporter::Target::Brain,
            node_url: String::new(),
            brain_url: format!("http://{brain_addr}"),
            client: test_client(),
            secret: Some("test-secret".to_string()),
            labels: exporter::ConstLabels {
                node_id: "brain-a".to_string(),
                region: Some("us-east-1".to_string()),
            },
        });

        let meta = NodeMeta {
            node_id: "brain-a".to_string(),
            address: format!("http://{brain_addr}"),
            ..node_meta()
        };
        let sidecar_addr = spawn(build_router(meta, exporter, Arc::new(SidecarMetrics::default()))).await;

        let body = reqwest::get(format!("http://{sidecar_addr}/metrics"))
            .await
            .expect("scrape the sidecar")
            .text()
            .await
            .expect("read body");

        assert!(body.contains(
            "fiducia_sidecar_scrape_up{node_id=\"brain-a\",region=\"us-east-1\",target=\"brain\"} 1\n"
        ));
        assert!(body.contains("fiducia_brain_up{node_id=\"brain-a\",region=\"us-east-1\"} 1\n"));
        assert!(
            body.contains("fiducia_brain_is_leader{node_id=\"brain-a\",region=\"us-east-1\"} 1\n")
        );
        assert!(body.contains(
            "fiducia_brain_nodes_by_health{node_id=\"brain-a\",region=\"us-east-1\",health=\"suspect\"} 1\n"
        ));
        assert!(body.contains(
            "fiducia_placement_under_replicated_shards{node_id=\"brain-a\",region=\"us-east-1\"} 2\n"
        ));

        let captured = seen.0.lock().unwrap();
        assert_eq!(captured.len(), 1, "/v1/status fetched once");
        assert_eq!(
            captured[0]
                .get("x-fiducia-internal-auth")
                .and_then(|v| v.to_str().ok()),
            Some("test-secret"),
            "brain-mode exporter must present the internal-auth header on /v1/status"
        );
    }

    /// A down export target must keep `/metrics` at 200 with `scrape_up=0` so
    /// Prometheus records the outage instead of marking the sidecar itself down.
    #[tokio::test]
    async fn scrape_failure_reports_scrape_up_zero_but_still_serves() {
        // Bind-then-drop a listener to get a port with nothing behind it.
        let dead = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let dead_addr = dead.local_addr().unwrap();
        drop(dead);

        let exporter = Arc::new(exporter::Exporter {
            target: exporter::Target::Brain,
            node_url: String::new(),
            brain_url: format!("http://{dead_addr}"),
            client: test_client(),
            secret: Some("test-secret".to_string()),
            labels: exporter::ConstLabels {
                node_id: "brain-a".to_string(),
                region: None,
            },
        });
        let sidecar_addr = spawn(build_router(node_meta(), exporter, Arc::new(SidecarMetrics::default()))).await;

        let response = reqwest::get(format!("http://{sidecar_addr}/metrics"))
            .await
            .expect("scrape the sidecar");
        assert_eq!(response.status(), 200, "a failed scrape must still 200");
        let body = response.text().await.expect("read body");

        assert!(body.contains("fiducia_sidecar_up 1\n"));
        assert!(
            body.contains("fiducia_sidecar_scrape_up{node_id=\"brain-a\",target=\"brain\"} 0\n"),
            "scrape_up must be 0 when the target is unreachable: {body}"
        );
        assert!(body.contains("# brain scrape failed (unreachable)"));
        assert!(
            !body.contains("fiducia_brain_up"),
            "no translated families may be emitted for a failed scrape"
        );
    }
}

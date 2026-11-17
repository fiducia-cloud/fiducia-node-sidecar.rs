//! Real HTTP node -> sidecar -> brain bridge lifecycle. The test imports the
//! production heartbeat loop; both upstreams are separate Axum TCP listeners.
//! It verifies leader-to-follower transitions, monotonic delivery identifiers,
//! outage observability, recovery and silence when local-node status is lost.
use std::{
    sync::{atomic::{AtomicBool, AtomicU8, Ordering}, Arc, Mutex},
    time::{Duration, Instant},
};
use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use serde_json::{json, Value};

#[path = "../src/auth.rs"]
mod auth;
#[path = "../src/meta.rs"]
mod meta;
#[path = "../src/metrics.rs"]
mod metrics;
#[path = "../src/heartbeat.rs"]
mod heartbeat;

// Only heartbeat's diagnostic endpoint formatter comes from main.rs; this
// equivalently redacts the URL to a host label. The HTTP transport, parsing,
// retry loop, and payload serialization are the production implementations.
fn endpoint_label(url: &str) -> String {
    reqwest::Url::parse(url).ok().and_then(|u| {
        u.host_str().map(|host| format!("{}://{}", u.scheme(), host))
    }).unwrap_or_else(|| "invalid-endpoint".to_string())
}

async fn spawn(app: Router) -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await.expect("bind fixture");
    let address = listener.local_addr().expect("fixture addr");
    let task = tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve fixture");
    });
    (format!("http://{address}"), task)
}
fn count(metrics: &metrics::SidecarMetrics, name: &str) -> u64 {
    let rendered = metrics.render();
    rendered.lines()
        .find(|line| line.starts_with(&format!("{name} ")))
        .and_then(|line| line.split_whitespace().last())
        .and_then(|word| word.parse().ok())
        .unwrap_or_else(|| panic!("missing metric {name}:\n{rendered}"))
}
async fn until(mut condition: impl FnMut() -> bool, label: &str) {
    let deadline = Instant::now() + Duration::from_secs(7);
    while !condition() {
        assert!(Instant::now() < deadline, "timed out: {label}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[derive(Clone)]
struct Node {
    // 0=unavailable, 1=leader, 2=follower
    mode: Arc<AtomicU8>,
}
async fn node_handler(State(node): State<Node>) -> (StatusCode, Json<Value>) {
    match node.mode.load(Ordering::SeqCst) {
        0 => (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"error":"node-down"}))),
        role => (StatusCode::OK, Json(json!({"consensus":{"shards":[
            {"shard_id":5,"role":"follower"},
            {"shard_id":7,"role": if role == 1 {"leader"} else {"follower"}}
        ]}}))),
    }
}

#[derive(Clone, Default)]
struct Brain {
    refuse: Arc<AtomicBool>,
    deliveries: Arc<Mutex<Vec<(String, u16, Value)>>>,
}
async fn heartbeat_handler(
    State(brain): State<Brain>,
    Path(node_id): Path<String>,
    Json(body): Json<Value>,
) -> StatusCode {
    let status = if brain.refuse.load(Ordering::SeqCst) {
        StatusCode::SERVICE_UNAVAILABLE
    } else {
        StatusCode::NO_CONTENT
    };
    brain.deliveries.lock().unwrap().push((node_id, status.as_u16(), body));
    status
}
fn accepted(brain: &Brain) -> Vec<Value> {
    brain.deliveries.lock().unwrap().iter()
        .filter(|(_, status, _)| *status == 204)
        .map(|(_, _, body)| body.clone())
        .collect()
}

#[tokio::test]
async fn node_to_brain_bridge_reconverges_after_both_failures_without_stale_leaders() {
    let node = Node { mode: Arc::new(AtomicU8::new(0)) };
    let brain = Brain::default();
    let (node_url, node_server) = spawn(Router::new()
        .route("/v1/status", get(node_handler))
        .with_state(node.clone())).await;
    let (brain_url, brain_server) = spawn(Router::new()
        .route("/v1/nodes/:node_id/heartbeat", post(heartbeat_handler))
        .with_state(brain.clone())).await;
    let metrics = Arc::new(metrics::SidecarMetrics::default());
    let bridge = tokio::spawn(heartbeat::run(
        node_url,
        brain_url,
        meta::NodeMeta {
            node_id: "node-regional".to_string(),
            address: "http://node-regional:8090".to_string(),
            region: Some("region-west".to_string()),
            availability_zone: Some("az-2".to_string()),
            rack: None,
            version: None,
        },
        Duration::from_millis(40),
        metrics.clone(),
    ));

    until(|| count(&metrics, "fiducia_sidecar_node_scrape_failures_total") >= 2,
        "node outage failed scrapes").await;
    assert!(brain.deliveries.lock().unwrap().is_empty(),
        "no heartbeat may be sent for an unobservable node");

    node.mode.store(1, Ordering::SeqCst);
    until(|| !accepted(&brain).is_empty(), "first successful leader heartbeat").await;
    let leader = accepted(&brain).last().cloned().unwrap();
    assert_eq!(leader["address"], "http://node-regional:8090");
    assert_eq!(leader["failure_domain"], "region-west");
    assert_eq!(leader["hosted_shards"], json!([5, 7]));
    assert_eq!(leader["leading_shards"], json!([7]));
    let leader_seq = leader["seq"].as_u64().expect("heartbeat sequence");

    // Same node flips follower; the bridge must report fresh topology and
    // strictly increasing sequences, rather than replaying a stale leader.
    node.mode.store(2, Ordering::SeqCst);
    until(|| accepted(&brain).iter().any(|body| {
        body["seq"].as_u64().unwrap_or(0) > leader_seq
            && body["leading_shards"] == json!([])
    }), "leader-to-follower transition").await;
    let follower = accepted(&brain).into_iter()
        .find(|body| body["seq"].as_u64().unwrap_or(0) > leader_seq
            && body["leading_shards"] == json!([]))
        .unwrap();
    assert_eq!(follower["hosted_shards"], json!([5, 7]));

    // Brain becomes unavailable: failed delivery must register as failure,
    // and the production bridge must remain alive to retry next tick.
    brain.refuse.store(true, Ordering::SeqCst);
    until(|| count(&metrics, "fiducia_sidecar_heartbeat_failures_total") >= 2,
        "brain HTTP rejection accounted").await;
    let last_seq_before_recovery = brain.deliveries.lock().unwrap().iter()
        .map(|(_, _, body)| body["seq"].as_u64().unwrap())
        .max().expect("at least one attempt");

    brain.refuse.store(false, Ordering::SeqCst);
    until(|| accepted(&brain).iter().any(|body|
        body["seq"].as_u64().unwrap_or(0) > last_seq_before_recovery
    ), "brain recovery heartbeat").await;
    let accepted_deliveries = accepted(&brain);
    assert!(accepted_deliveries.iter().all(|body|
        body["hosted_shards"] == json!([5, 7])
    ));
    let all_sequences: Vec<u64> = brain.deliveries.lock().unwrap().iter()
        .map(|(id, _, body)| {
            assert_eq!(id, "node-regional");
            body["seq"].as_u64().expect("non-numeric heartbeat seq")
        }).collect();
    assert!(all_sequences.windows(2).all(|pair| pair[0] < pair[1]),
        "every attempted heartbeat must carry a strictly increasing seq");

    node.mode.store(0, Ordering::SeqCst);
    let failed_before = count(&metrics, "fiducia_sidecar_node_scrape_failures_total");
    until(|| count(&metrics, "fiducia_sidecar_node_scrape_failures_total") >= failed_before + 3,
        "node outage resumed").await;
    let before_silence = brain.deliveries.lock().unwrap().len();
    tokio::time::sleep(Duration::from_millis(160)).await;
    assert_eq!(brain.deliveries.lock().unwrap().len(), before_silence,
        "must never manufacture heartbeats during local-node outage");

    bridge.abort();
    node_server.abort();
    brain_server.abort();
}

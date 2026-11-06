//! Drive the *real* exporter process through malformed, not-ready, and healthy
//! node responses over a separate loopback HTTP listener. No external service.
use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    routing::get,
    Router,
};
use std::{
    process::{Child, Command, Stdio},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

const SECRET: &str = "synthetic-observe-hop-auth-20261105";

#[derive(Clone, Default)]
struct NodeState {
    phase: Arc<AtomicUsize>,
    seen: Arc<Mutex<Vec<Option<String>>>>,
}

fn record(state: &NodeState, headers: &HeaderMap) {
    let credential = headers
        .get("x-fiducia-internal-auth")
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
    state
        .seen
        .lock()
        .expect("HTTP observations")
        .push(credential);
}

async fn shards(State(state): State<NodeState>, headers: HeaderMap) -> (StatusCode, String) {
    record(&state, &headers);
    if state.phase.load(Ordering::SeqCst) == 0 {
        return (StatusCode::OK, "{broken-json".to_string());
    }
    (StatusCode::OK, r#"{"shard_count":1,"leader_count":1,"follower_count":0,"shards":[],"quorum":{"leaderless_shards":[]}}"#.to_string())
}

async fn metrics(State(state): State<NodeState>, headers: HeaderMap) -> (StatusCode, &'static str) {
    record(&state, &headers);
    (StatusCode::OK, "{}")
}

async fn readiness(
    State(state): State<NodeState>,
    headers: HeaderMap,
) -> (StatusCode, &'static str) {
    record(&state, &headers);
    if state.phase.load(Ordering::SeqCst) == 1 {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            r#"{"status":"not_ready","all_shards_running":false}"#,
        )
    } else {
        (
            StatusCode::OK,
            r#"{"status":"ok","all_shards_running":true}"#,
        )
    }
}

struct ChildGuard(Child);
impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

async fn start() -> (String, NodeState, tokio::task::JoinHandle<()>, ChildGuard) {
    let node = NodeState::default();
    let router = Router::new()
        .route("/v1/observe/shards", get(shards))
        .route("/v1/observe/metrics", get(metrics))
        .route("/readyz", get(readiness))
        .with_state(node.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("local node listener");
    let node_url = format!("http://{}", listener.local_addr().expect("node bound"));
    let node_server = tokio::spawn(async move {
        axum::serve(listener, router)
            .await
            .expect("synthetic node HTTP");
    });
    let reserved = std::net::TcpListener::bind("127.0.0.1:0").expect("sidecar port");
    let port = reserved.local_addr().expect("sidecar address").port();
    drop(reserved);
    let process = Command::new(env!("CARGO_BIN_EXE_fiducia-node-sidecar"))
        .env("FIDUCIA_INTERNAL_SECRET", SECRET)
        .env("FIDUCIA_SIDECAR_ROLE", "exporter")
        .env("FIDUCIA_EXPORT_TARGET", "node")
        .env("FIDUCIA_NODE_ID", "observe-recovery-test-node")
        .env("FIDUCIA_NODE_URL", node_url)
        .env("FIDUCIA_BRAIN_URL", "http://127.0.0.1:1")
        .env("FIDUCIA_SIDECAR_BIND", format!("127.0.0.1:{port}"))
        .env("FIDUCIA_OBSERVE_TIMEOUT_MS", "1000")
        .env_remove("OTEL_EXPORTER_OTLP_ENDPOINT")
        .env_remove("FIDUCIA_LOG_SOURCE")
        .env_remove("FIDUCIA_LOG_SINK")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("actual sidecar process");
    let url = format!("http://127.0.0.1:{port}");
    (url, node, node_server, ChildGuard(process))
}

async fn scrape(client: &reqwest::Client, base: &str) -> String {
    let response = client
        .get(format!("{base}/metrics"))
        .send()
        .await
        .expect("live exporter scrape");
    assert_eq!(response.status(), StatusCode::OK);
    response.text().await.expect("Prometheus response body")
}

fn gauge(body: &str, name: &str, target: &str, value: &str) {
    let present = body
        .lines()
        .any(|line| line.starts_with(name) && line.contains(target) && line.ends_with(value));
    assert!(
        present,
        "missing {name}, target={target}, value={value}: {body}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn malformed_node_json_then_readiness_503_then_recovery_are_separate_states() {
    let (base, node, server, _process) = start().await;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(4))
        .build()
        .expect("HTTP client");

    // Exporter readiness must not require any upstream scrape to succeed.
    tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            if let Ok(response) = client.get(format!("{base}/readyz")).send().await {
                if response.status() == StatusCode::OK {
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("exporter process readiness");

    let broken = scrape(&client, &base).await;
    gauge(
        &broken,
        "fiducia_sidecar_scrape_up",
        "target=\"node\"",
        " 0",
    );
    assert!(!broken.contains("fiducia_node_up"));
    assert!(!broken.contains(SECRET));

    node.phase.store(1, Ordering::SeqCst);
    let unready = scrape(&client, &base).await;
    gauge(
        &unready,
        "fiducia_sidecar_scrape_up",
        "target=\"node\"",
        " 1",
    );
    gauge(
        &unready,
        "fiducia_node_ready",
        "node_id=\"observe-recovery-test-node\"",
        " 0",
    );
    assert!(!unready.contains(SECRET));

    node.phase.store(2, Ordering::SeqCst);
    let healthy = scrape(&client, &base).await;
    gauge(
        &healthy,
        "fiducia_sidecar_scrape_up",
        "target=\"node\"",
        " 1",
    );
    gauge(
        &healthy,
        "fiducia_node_ready",
        "node_id=\"observe-recovery-test-node\"",
        " 1",
    );
    let observed = node.seen.lock().expect("node HTTP observations").clone();
    assert!(
        observed.len() >= 7,
        "full recovery must fetch node metrics and readiness"
    );
    assert!(observed.iter().all(|auth| auth.as_deref() == Some(SECRET)));
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recovered_node_handles_concurrent_scrapes_without_leaking_credentials() {
    let (base, node, server, _process) = start().await;
    node.phase.store(2, Ordering::SeqCst);
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(4))
        .build()
        .expect("HTTP client");
    let mut tasks = Vec::new();
    for _ in 0..12 {
        let c = client.clone();
        let url = base.clone();
        tasks.push(tokio::spawn(async move {
            let body = scrape(&c, &url).await;
            gauge(&body, "fiducia_sidecar_scrape_up", "target=\"node\"", " 1");
            assert!(!body.contains(SECRET));
            assert!(!body.contains("untrusted.invalid"));
        }));
    }
    for task in tasks {
        task.await.expect("concurrent exporter response");
    }
    let seen = node.seen.lock().expect("node HTTP observations").clone();
    assert!(
        seen.len() >= 36,
        "12 node scrapes must fetch three upstream paths"
    );
    assert!(seen.iter().all(|auth| auth.as_deref() == Some(SECRET)));
    server.abort();
}

//! Real-process sidecar -> local node -> brain integration.
//! A loopback topology proves HTTP contract, auth, heartbeat sequence and
//! readiness without a live production control plane.

use std::{
    process::{Child, Command, Stdio},
    sync::{Arc, Mutex},
    time::Duration,
};

use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    routing::{get, post},
    Json, Router,
};
use serde_json::{json, Value};
use tokio::task::JoinHandle;

const SECRET: &str = "local-sidecar-to-brain-test-internal-proof";

#[derive(Clone, Default)]
struct Observations {
    node_auth: Arc<Mutex<Vec<Option<String>>>>,
    brain_post: Arc<Mutex<Vec<(Option<String>, Value)>>>,
}

fn secret(headers: &HeaderMap) -> Option<String> {
    headers
        .get("x-fiducia-internal-auth")
        .and_then(|value| value.to_str().ok())
        .map(str::to_string)
}

async fn node_status(
    State(observations): State<Observations>,
    headers: HeaderMap,
) -> Json<Value> {
    observations.node_auth.lock().unwrap().push(secret(&headers));
    Json(json!({
        "consensus": {
            "hosted_shards": [3, 1, 1, 2],
            "leading_shards": [3, 1, 3],
            "following_shards": [2],
        }
    }))
}

async fn brain_heartbeat(
    State(observations): State<Observations>,
    headers: HeaderMap,
    Json(payload): Json<Value>,
) -> StatusCode {
    let mut posted = observations.brain_post.lock().unwrap();
    posted.push((secret(&headers), payload));
    // Fail the first delivery; a successful scrape is not equivalent to an
    // accepted heartbeat. The sidecar must keep retrying and only then become
    // ready for control-plane placement.
    if posted.len() == 1 {
        StatusCode::SERVICE_UNAVAILABLE
    } else {
        StatusCode::NO_CONTENT
    }
}

async fn serve(app: Router) -> (String, JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind server");
    let url = format!("http://{}", listener.local_addr().expect("server addr"));
    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve fixture");
    });
    (url, handle)
}

struct SidecarChild(Child);

impl Drop for SidecarChild {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn start_sidecar(node_url: &str, brain_url: &str) -> (String, SidecarChild) {
    let reserved = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve bind");
    let port = reserved.local_addr().expect("port").port();
    drop(reserved);
    let child = Command::new(env!("CARGO_BIN_EXE_fiducia-node-sidecar"))
        .env("FIDUCIA_INTERNAL_SECRET", SECRET)
        .env("FIDUCIA_NODE_URL", node_url)
        .env("FIDUCIA_BRAIN_URL", brain_url)
        .env("FIDUCIA_SIDECAR_BIND", format!("127.0.0.1:{port}"))
        .env("FIDUCIA_NODE_ID", "node-sidecar-e2e")
        .env("FIDUCIA_NODE_ADDRESS", "http://node.private.internal:8090")
        .env("FIDUCIA_REGION", "eu-west-1")
        .env("FIDUCIA_AZ", "eu-west-1b")
        .env("FIDUCIA_HEARTBEAT_MS", "40")
        .env("FIDUCIA_SIDECAR_ROLE", "full")
        .env_remove("OTEL_EXPORTER_OTLP_ENDPOINT")
        .env_remove("FIDUCIA_LOG_SOURCE")
        .env_remove("FIDUCIA_LOG_SINK")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("launch sidecar process");
    (format!("http://127.0.0.1:{port}"), SidecarChild(child))
}

#[tokio::test]
async fn real_sidecar_authenticates_both_hops_and_retries_brain_without_regressing_sequence() {
    let observations = Observations::default();
    let (node_url, node_server) = serve(
        Router::new()
            .route("/v1/status", get(node_status))
            .with_state(observations.clone()),
    )
    .await;
    let (brain_url, brain_server) = serve(
        Router::new()
            .route("/v1/nodes/node-sidecar-e2e/heartbeat", post(brain_heartbeat))
            .with_state(observations.clone()),
    )
    .await;
    let (sidecar_url, _sidecar) = start_sidecar(&node_url, &brain_url);
    let client = reqwest::Client::new();

    tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            let count = observations.brain_post.lock().unwrap().len();
            if count >= 2 {
                if let Ok(ready) = client.get(format!("{sidecar_url}/readyz")).send().await {
                    if ready.status() == StatusCode::OK {
                        break;
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("sidecar must retry failed heartbeat and become ready");

    let node_auth = observations.node_auth.lock().unwrap().clone();
    let brain_post = observations.brain_post.lock().unwrap().clone();
    assert!(node_auth.len() >= 2);
    assert!(brain_post.len() >= 2);
    assert!(node_auth.iter().all(|value| value.as_deref() == Some(SECRET)));

    let mut last_seq = 0;
    for (proof, body) in brain_post {
        assert_eq!(proof.as_deref(), Some(SECRET));
        assert_eq!(body["address"], "http://node.private.internal:8090");
        assert_eq!(body["failure_domain"], "eu-west-1");
        assert_eq!(body["hosted_shards"], json!([1, 2, 3]));
        assert_eq!(body["leading_shards"], json!([1, 3]));
        let sequence = body["seq"].as_u64().expect("monotonic sequence");
        assert!(sequence > last_seq, "retries must advance heartbeat sequence");
        last_seq = sequence;
    }

    let metrics = client
        .get(format!("{sidecar_url}/metrics"))
        .send()
        .await
        .expect("live metrics request")
        .text()
        .await
        .expect("metrics body");
    assert!(metrics.contains("fiducia_sidecar_heartbeat_successes_total"));
    assert!(metrics.contains("fiducia_sidecar_heartbeat_failures_total"));

    node_server.abort();
    brain_server.abort();
}

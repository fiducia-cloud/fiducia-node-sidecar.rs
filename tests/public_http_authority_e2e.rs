//! Real-process regression for untrusted client -> sidecar HTTP requests.
//! The exporter role must not grant a browser or a forwarded host the power
//! to mutate the local sidecar, redirect it, or expose startup credentials.

use serde_json::Value;
use std::{
    process::{Child, Command, Stdio},
    time::Duration,
};

struct SidecarChild(Child);

impl Drop for SidecarChild {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn start_exporter() -> (String, SidecarChild) {
    let reserved = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve port");
    let port = reserved.local_addr().expect("bound addr").port();
    drop(reserved);
    let child = Command::new(env!("CARGO_BIN_EXE_fiducia-node-sidecar"))
        .env("FIDUCIA_INTERNAL_SECRET", "synthetic-exporter-local-only-proof")
        .env("FIDUCIA_NODE_URL", "http://127.0.0.1:1")
        .env("FIDUCIA_BRAIN_URL", "http://127.0.0.1:1")
        .env("FIDUCIA_SIDECAR_ROLE", "exporter")
        .env("FIDUCIA_SIDECAR_BIND", format!("127.0.0.1:{port}"))
        .env("FIDUCIA_NODE_ID", "http-authority-e2e-node")
        .env_remove("OTEL_EXPORTER_OTLP_ENDPOINT")
        .env_remove("FIDUCIA_LOG_SOURCE")
        .env_remove("FIDUCIA_LOG_SINK")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("start real sidecar binary");
    (format!("http://127.0.0.1:{port}"), SidecarChild(child))
}

async fn wait_until_ready(client: &reqwest::Client, base: &str) {
    tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            if let Ok(response) = client.get(format!("{base}/readyz")).send().await {
                if response.status().is_success() {
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
    })
    .await
    .expect("exporter sidecar must bind and become ready");
}

#[tokio::test]
async fn hostile_forwarded_headers_and_cookies_cannot_taint_24_public_client_responses() {
    let (base, _child) = start_exporter();
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(3))
        .build()
        .expect("client");
    wait_until_ready(&client, &base).await;

    let mut tasks = Vec::new();
    for number in 0..24 {
        let client = client.clone();
        let base = base.clone();
        tasks.push(tokio::spawn(async move {
            let path = match number % 3 {
                0 => "/healthz",
                1 => "/readyz",
                _ => "/meta",
            };
            let response = client
                .get(format!("{base}{path}?override=admin"))
                .header("Origin", "https://untrusted.invalid")
                .header("Forwarded", "for=192.0.2.1;host=untrusted.invalid")
                .header("X-Forwarded-Host", "untrusted.invalid")
                .header("Cookie", "session=synthetic-client-token-proof")
                .send()
                .await
                .expect("real client request");
            assert_eq!(response.status(), reqwest::StatusCode::OK);
            assert!(response.headers().get("access-control-allow-origin").is_none());
            assert!(response.headers().get("location").is_none());
            let body: Value = response.json().await.expect("JSON response");
            let text = body.to_string();
            assert!(!text.contains("untrusted.invalid"));
            assert!(!text.contains("synthetic-client-token-proof"));
            assert!(!text.contains("synthetic-exporter-local-only-proof"));
            if path == "/readyz" {
                assert_eq!(body["mode"], "exporter");
            }
            if path == "/meta" {
                assert_eq!(body["node_id"], "http-authority-e2e-node");
            }
        }));
    }
    for task in tasks {
        task.await.expect("one client cannot crash another");
    }
}

#[tokio::test]
async fn public_read_endpoints_refuse_mutating_methods_and_preserve_health() {
    let (base, _child) = start_exporter();
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(3))
        .build()
        .expect("client");
    wait_until_ready(&client, &base).await;

    for (method, path) in [
        (reqwest::Method::POST, "/healthz"),
        (reqwest::Method::PUT, "/readyz"),
        (reqwest::Method::PATCH, "/meta"),
        (reqwest::Method::DELETE, "/meta"),
    ] {
        let response = client
            .request(method, format!("{base}{path}"))
            .header("Content-Type", "application/json")
            .body(r#"{"node_id":"attacker-controlled"}"#)
            .send()
            .await
            .expect("mutating HTTP attempt");
        assert_eq!(
            response.status(),
            reqwest::StatusCode::METHOD_NOT_ALLOWED,
            "{path}"
        );
    }
    let after = client
        .get(format!("{base}/meta"))
        .send()
        .await
        .expect("metadata remains available");
    assert_eq!(after.status(), reqwest::StatusCode::OK);
    let meta: Value = after.json().await.expect("metadata JSON");
    assert_eq!(meta["node_id"], "http-authority-e2e-node");
    let health = client
        .get(format!("{base}/healthz"))
        .send()
        .await
        .expect("liveness after attempted mutation");
    assert_eq!(health.status(), reqwest::StatusCode::OK);
}

//! Real-process public HTTP read-plane isolation across simultaneous GET/HEAD
//! clients. Synthetic local-only inputs: no production node, brain or token.
use std::{
    process::{Child, Command, Stdio},
    time::Duration,
};

use serde_json::Value;

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn exporter() -> (String, ChildGuard) {
    let reserved = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve loopback port");
    let port = reserved.local_addr().expect("bound port").port();
    drop(reserved);

    let child = Command::new(env!("CARGO_BIN_EXE_fiducia-node-sidecar"))
        .env("FIDUCIA_SIDECAR_ROLE", "exporter")
        .env("FIDUCIA_SIDECAR_BIND", format!("127.0.0.1:{port}"))
        .env("FIDUCIA_NODE_URL", "http://127.0.0.1:1")
        .env("FIDUCIA_BRAIN_URL", "http://127.0.0.1:1")
        .env("FIDUCIA_INTERNAL_SECRET", "synthetic-local-exporter-proof")
        .env("FIDUCIA_NODE_ID", "readonly-e2e-node")
        .env_remove("OTEL_EXPORTER_OTLP_ENDPOINT")
        .env_remove("FIDUCIA_LOG_SOURCE")
        .env_remove("FIDUCIA_LOG_SINK")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn production sidecar binary");
    (format!("http://127.0.0.1:{port}"), ChildGuard(child))
}

#[tokio::test]
async fn exporter_parallel_get_head_and_malformed_public_methods_remain_isolated() {
    let (base, _guard) = exporter();
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(4))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .expect("build local client");

    tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            if let Ok(response) = client.get(format!("{base}/readyz")).send().await {
                if response.status() == reqwest::StatusCode::OK {
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("exporter becomes ready");

    let routes = ["/healthz", "/readyz", "/meta", "/metrics"];
    let mut tasks = Vec::new();
    for number in 0..32 {
        let client = client.clone();
        let base = base.clone();
        tasks.push(tokio::spawn(async move {
            let path = routes[number % routes.len()];
            let method = if number % 2 == 0 {
                reqwest::Method::HEAD
            } else {
                reqwest::Method::GET
            };
            let response = client
                .request(method.clone(), format!("{base}{path}?untrusted=synthetic-client"))
                .header("Origin", "https://untrusted.invalid")
                .header("Forwarded", "for=192.0.2.1;host=untrusted.invalid")
                .header("Authorization", "Bearer synthetic-client-no-authority")
                .send()
                .await
                .expect("live sidecar HTTP request");
            assert_eq!(response.status(), reqwest::StatusCode::OK, "{method} {path}");
            assert!(response.headers().get("location").is_none());
            assert!(response.headers().get("access-control-allow-origin").is_none());

            let text = response.text().await.expect("bounded response");
            if method == reqwest::Method::HEAD {
                assert!(text.is_empty(), "HEAD {path} must not return representation bytes");
            } else {
                assert!(!text.contains("untrusted.invalid"));
                assert!(!text.contains("synthetic-client-no-authority"));
                assert!(!text.contains("synthetic-local-exporter-proof"));
                if path == "/readyz" {
                    let value: Value = serde_json::from_str(&text).expect("readiness JSON");
                    assert_eq!(value["mode"], "exporter");
                }
                if path == "/meta" {
                    let value: Value = serde_json::from_str(&text).expect("metadata JSON");
                    assert_eq!(value["node_id"], "readonly-e2e-node");
                }
            }
        }));
    }
    for task in tasks {
        task.await.expect("no parallel peer may panic");
    }

    for (method, path, expected) in [
        (reqwest::Method::POST, "/healthz", 405),
        (reqwest::Method::PUT, "/metrics", 405),
        (reqwest::Method::DELETE, "/meta", 405),
        (reqwest::Method::GET, "/unregistered-public-path", 404),
    ] {
        let response = client
            .request(method, format!("{base}{path}"))
            .send()
            .await
            .expect("negative route request");
        assert_eq!(response.status().as_u16(), expected);
    }

    let final_response = client.get(format!("{base}/meta")).send().await.expect("final metadata");
    assert_eq!(final_response.status(), reqwest::StatusCode::OK);
    let meta: Value = final_response.json().await.expect("final metadata JSON");
    assert_eq!(meta["node_id"], "readonly-e2e-node");
}

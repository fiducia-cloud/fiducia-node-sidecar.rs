//! Real sidecar processes isolate Prometheus scrape failures from health and
//! readiness. No collector, node, brain or external network is contacted.
use serde_json::Value;
use std::{
    process::{Child, Command, Stdio},
    time::Duration,
};

struct Sidecar(Child);
impl Drop for Sidecar {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn spawn_exporter(target: &str, node_id: &str) -> (String, Sidecar) {
    let reserve = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve TCP port");
    let port = reserve.local_addr().expect("bind").port();
    drop(reserve);
    let child = Command::new(env!("CARGO_BIN_EXE_fiducia-node-sidecar"))
        .env("FIDUCIA_INTERNAL_SECRET", "synthetic-metrics-boundary-proof")
        .env("FIDUCIA_EXPORT_TARGET", target)
        .env("FIDUCIA_SIDECAR_ROLE", "exporter")
        .env("FIDUCIA_NODE_ID", node_id)
        .env("FIDUCIA_NODE_URL", "http://127.0.0.1:1")
        .env("FIDUCIA_BRAIN_URL", "http://127.0.0.1:1")
        .env("FIDUCIA_SIDECAR_BIND", format!("127.0.0.1:{port}"))
        .env("FIDUCIA_OBSERVE_TIMEOUT_MS", "100")
        .env_remove("OTEL_EXPORTER_OTLP_ENDPOINT")
        .env_remove("FIDUCIA_LOG_SOURCE")
        .env_remove("FIDUCIA_LOG_SINK")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn actual sidecar binary");
    (format!("http://127.0.0.1:{port}"), Sidecar(child))
}

async fn ready(client: &reqwest::Client, base: &str) {
    tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            if let Ok(response) = client.get(format!("{base}/readyz")).send().await {
                if response.status() == reqwest::StatusCode::OK {
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("sidecar exporter-only role must start without a reachable upstream");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn node_and_brain_exporters_are_isolated_when_both_upstreams_are_offline() {
    let (node_url, _node_child) = spawn_exporter("node", "node-offline-e2e");
    let (brain_url, _brain_child) = spawn_exporter("brain", "brain-offline-e2e");
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(3))
        .build()
        .expect("bounded HTTP client");
    ready(&client, &node_url).await;
    ready(&client, &brain_url).await;

    let mut calls = Vec::new();
    for index in 0..20 {
        let client = client.clone();
        let (base, target, node_id) = if index % 2 == 0 {
            (node_url.clone(), "node", "node-offline-e2e")
        } else {
            (brain_url.clone(), "brain", "brain-offline-e2e")
        };
        calls.push(tokio::spawn(async move {
            let scrape = client
                .get(format!("{base}/metrics"))
                .header("Forwarded", "host=untrusted.invalid")
                .header("Cookie", "session=synthetic-browser-caller")
                .send()
                .await
                .expect("real metrics scrape");
            assert_eq!(scrape.status(), reqwest::StatusCode::OK);
            let body = scrape.text().await.expect("Prometheus exposition");
            assert!(body.contains("fiducia_sidecar_up 1"));
            assert!(body.contains("fiducia_sidecar_scrape_up"));
            assert!(
                body.lines()
                    .any(|line| line.starts_with("fiducia_sidecar_scrape_up{")
                        && line.contains(&format!("target=\"{target}\""))
                        && line.ends_with(" 0")),
                "unreachable upstream must be visible in its own {target} scrape: {body}"
            );
            assert!(body.contains(&format!("node_id=\"{node_id}\"")));
            assert!(!body.contains("synthetic-metrics-boundary-proof"));
            assert!(!body.contains("synthetic-browser-caller"));
            assert!(!body.contains("untrusted.invalid"));
            assert!(!body.contains("node-offline-e2e") || target == "node");
            assert!(!body.contains("brain-offline-e2e") || target == "brain");

            let readiness = client
                .get(format!("{base}/readyz"))
                .send()
                .await
                .expect("readiness after failed scrape");
            assert_eq!(readiness.status(), reqwest::StatusCode::OK);
            let body: Value = readiness.json().await.expect("readiness JSON");
            assert_eq!(body["mode"], "exporter");
            assert_eq!(body["status"], "ok");
        }));
    }
    for call in calls {
        call.await.expect("independent scraper response");
    }
}

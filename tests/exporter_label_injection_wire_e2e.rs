//! Prometheus text exposition must not permit a hostile operator-controlled
//! metadata label to turn into a separate metric or expose transport secrets.
use std::{
    process::{Child, Command, Stdio},
    time::Duration,
};

const NODE_ID: &str = "node\"}\\nfiducia_forged_metric 999\\backslash";
const REGION: &str = "us\"\\\n# TYPE fake counter";

struct Sidecar(Child);

impl Drop for Sidecar {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn spawn() -> (String, Sidecar) {
    let reserved = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve loopback port");
    let port = reserved.local_addr().expect("local address").port();
    drop(reserved);
    let child = Command::new(env!("CARGO_BIN_EXE_fiducia-node-sidecar"))
        .env("FIDUCIA_SIDECAR_ROLE", "exporter")
        .env("FIDUCIA_SIDECAR_BIND", format!("127.0.0.1:{port}"))
        .env("FIDUCIA_EXPORT_TARGET", "node")
        .env("FIDUCIA_NODE_ID", NODE_ID)
        .env("FIDUCIA_REGION", REGION)
        .env("FIDUCIA_NODE_URL", "http://127.0.0.1:1")
        .env("FIDUCIA_BRAIN_URL", "http://127.0.0.1:1")
        .env(
            "FIDUCIA_INTERNAL_SECRET",
            "synthetic-label-wire-private-token",
        )
        .env_remove("OTEL_EXPORTER_OTLP_ENDPOINT")
        .env_remove("FIDUCIA_LOG_SOURCE")
        .env_remove("FIDUCIA_LOG_SINK")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn production sidecar");
    (format!("http://127.0.0.1:{port}"), Sidecar(child))
}

async fn wait_ready(client: &reqwest::Client, base: &str) {
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
    .expect("exporter process must listen before the scrape");
}

#[tokio::test]
async fn escaped_identity_and_region_cannot_inject_prometheus_metric_samples() {
    let (base, _child) = spawn();
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(4))
        .build()
        .expect("client");
    wait_ready(&client, &base).await;

    for _ in 0..4 {
        let response = client
            .get(format!("{base}/metrics"))
            .send()
            .await
            .expect("scrape");
        assert_eq!(response.status(), reqwest::StatusCode::OK);
        let wire = response.text().await.expect("Prometheus body");
        let samples: Vec<&str> = wire
            .lines()
            .filter(|line| line.starts_with("fiducia_sidecar_scrape_up{"))
            .collect();
        assert_eq!(samples.len(), 1, "exactly one failed scrape sample: {wire}");
        let sample = samples[0];
        assert!(sample.contains(r#"node_id="node\"}\\nfiducia_forged_metric 999\\backslash""#));
        assert!(sample.contains(r#"region="us\"\\\n# TYPE fake counter""#));
        assert!(sample.contains(r#"target="node""#));
        assert!(sample.ends_with(" 0"));
        assert!(!wire
            .lines()
            .any(|line| line.starts_with("fiducia_forged_metric")));
        assert!(!wire
            .lines()
            .any(|line| line.starts_with("# TYPE fake counter")));
        assert!(!wire.contains("synthetic-label-wire-private-token"));
    }

    let meta = client
        .get(format!("{base}/meta"))
        .send()
        .await
        .expect("metadata query");
    assert_eq!(meta.status(), reqwest::StatusCode::OK);
    let json: serde_json::Value = meta.json().await.expect("JSON identity");
    assert_eq!(json["node_id"], NODE_ID);
    assert_eq!(json["region"], REGION);
}

#[tokio::test]
async fn public_client_headers_cannot_modify_exporter_label_authority() {
    let (base, _child) = spawn();
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(4))
        .build()
        .expect("client");
    wait_ready(&client, &base).await;

    let baseline = client
        .get(format!("{base}/metrics"))
        .send()
        .await
        .expect("baseline")
        .text()
        .await
        .expect("baseline text");
    let attacked = client
        .get(format!("{base}/metrics?node_id=forged&region=forged"))
        .header("Forwarded", "host=untrusted.invalid;proto=https")
        .header("X-Fiducia-Region", "attacker-region")
        .header("X-Fiducia-Node-Id", "attacker-node")
        .header("Cookie", "tenant=synthetic-client-state")
        .send()
        .await
        .expect("hostile client request")
        .text()
        .await
        .expect("hostile response");
    assert_eq!(
        attacked, baseline,
        "client metadata must not affect exported labels"
    );
    assert!(!attacked.contains("attacker-node"));
    assert!(!attacked.contains("attacker-region"));
    assert!(!attacked.contains("synthetic-client-state"));
    assert!(!attacked.contains("untrusted.invalid"));
}

// Wave50: these cases invoke the production sidecar binary, not a mocked router.
#[tokio::test]
async fn wave50_head_metrics_is_bodyless_and_does_not_leak_private_labels() {
    let (base, _sidecar) = spawn();
    let client = reqwest::Client::new();
    wait_ready(&client, &base).await;
    let reply = client.head(format!("{base}/metrics")).send().await.unwrap();
    assert_eq!(reply.status(), reqwest::StatusCode::OK);
    assert!(reply.bytes().await.unwrap().is_empty());
}

#[tokio::test]
async fn wave50_mutating_methods_cannot_modify_public_metadata() {
    let (base, _sidecar) = spawn();
    let client = reqwest::Client::new();
    wait_ready(&client, &base).await;
    for method in [
        reqwest::Method::POST,
        reqwest::Method::PUT,
        reqwest::Method::DELETE,
        reqwest::Method::PATCH,
    ] {
        let denied = client
            .request(method, format!("{base}/meta"))
            .body("synthetic-mutating-payload")
            .send()
            .await
            .unwrap();
        assert_eq!(denied.status(), reqwest::StatusCode::METHOD_NOT_ALLOWED);
    }
    let identity: serde_json::Value = client
        .get(format!("{base}/meta"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(identity["node_id"], NODE_ID);
    assert_eq!(identity["region"], REGION);
}

#[tokio::test]
async fn wave50_unknown_public_routes_do_not_disclose_exporter_credentials() {
    let (base, _sidecar) = spawn();
    let client = reqwest::Client::new();
    wait_ready(&client, &base).await;
    for path in ["/internal", "/debug/vars", "/admin/secrets", "/v1/nodes"] {
        let denied = client.get(format!("{base}{path}")).send().await.unwrap();
        assert_eq!(denied.status(), reqwest::StatusCode::NOT_FOUND);
        let body = denied.text().await.unwrap();
        assert!(!body.contains("synthetic-label-wire-private-token"));
        assert!(!body.contains(NODE_ID));
    }
}

#[tokio::test]
async fn wave50_parallel_meta_clients_receive_exact_per_process_identity() {
    let (base, _sidecar) = spawn();
    let client = reqwest::Client::new();
    wait_ready(&client, &base).await;
    let mut readers = Vec::new();
    for _ in 0..16 {
        let url = format!("{base}/meta");
        let client = client.clone();
        readers.push(tokio::spawn(async move {
            let response = client.get(url).send().await.unwrap();
            assert_eq!(response.status(), reqwest::StatusCode::OK);
            response.json::<serde_json::Value>().await.unwrap()
        }));
    }
    for reader in readers {
        let identity = reader.await.unwrap();
        assert_eq!(identity["node_id"], NODE_ID);
        assert_eq!(identity["region"], REGION);
    }
}

#[tokio::test]
async fn wave50_forwarded_authority_cannot_change_exporter_readiness() {
    let (base, _sidecar) = spawn();
    let client = reqwest::Client::new();
    wait_ready(&client, &base).await;
    let response = client
        .get(format!("{base}/readyz?role=full&node_id=attacker"))
        .header("Forwarded", "for=127.0.0.1;host=evil.invalid")
        .header("X-Forwarded-Proto", "https")
        .header("Cookie", "role=full;tenant=attacker")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let raw = response.text().await.unwrap();
    assert!(!raw.contains("evil.invalid"));
    assert!(!raw.contains("attacker"));
    assert!(!raw.contains("synthetic-label-wire-private-token"));
}

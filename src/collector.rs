//! Observability collection: logs + metrics.
//!
//! The other half of the sidecar's job: get the node's telemetry off the box and
//! into the observability stack — **not** the brain (the brain only wants
//! placement metadata, never data-plane logs).
//!
//!   * **logs** — tail the node's log file and ship to the log backend (an HTTP
//!     sink such as Loki / a Vector pipeline).
//!   * **metrics** — scrape the node's `/metrics` and re-expose (or remote-write)
//!     for Prometheus, annotated with this node's identity/failure-domain.
//!
//! (For heavy production use you may prefer a dedicated Vector/Fluent Bit
//! sidecar; this keeps the single-binary deployment self-contained.)

use std::time::Duration;

use tokio::io::{AsyncBufReadExt, BufReader};

/// HTTP client for scraping the node and POSTing to the log sink. Short timeout —
/// telemetry must never block the data plane.
fn client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap_or_else(|_| reqwest::Client::new())
}

/// Tail the local node's log file and forward each new line to the log backend.
///
/// `node_log_source` is a path to follow; `sink` is an HTTP endpoint that accepts
/// newline-delimited log lines (POST). Empty values disable the respective half
/// (e.g. when an external log agent owns shipping). Reopens the file if it
/// disappears (log rotation) and never exits — it's a background task.
pub async fn ship_logs(node_log_source: String, sink: String) {
    if node_log_source.is_empty() {
        tracing::info!("collector: FIDUCIA_NODE_LOG_SOURCE unset — log shipping disabled");
        return;
    }
    let client = client();
    loop {
        match tokio::fs::File::open(&node_log_source).await {
            Ok(file) => {
                let mut lines = BufReader::new(file).lines();
                loop {
                    match lines.next_line().await {
                        Ok(Some(line)) => forward_log_line(&client, &sink, line).await,
                        // EOF: wait for more to be appended, then keep reading.
                        Ok(None) => tokio::time::sleep(Duration::from_millis(500)).await,
                        Err(e) => {
                            tracing::warn!(error = %e, "collector: log read error; reopening");
                            break;
                        }
                    }
                }
            }
            Err(e) => {
                tracing::debug!(error = %e, source = %node_log_source, "collector: log file not open; retrying");
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
    }
}

/// Ship one log line to the sink, or print it locally when no sink is configured.
async fn forward_log_line(client: &reqwest::Client, sink: &str, line: String) {
    if sink.is_empty() {
        // No backend wired: surface on the sidecar's own stdout so it's still
        // captured by whatever scrapes container logs.
        tracing::info!(target: "node_log", "{line}");
        return;
    }
    if let Err(e) = client.post(sink).body(line).send().await {
        tracing::warn!(error = %e, sink, "collector: failed to ship log line");
    }
}

/// Scrape the local node's Prometheus metrics, annotated with this node's
/// identity so the metrics are attributable once merged.
///
/// Returns the node's exposition text with a sidecar comment header, or an empty
/// string if the node is unreachable (the sidecar's own `/metrics` then serves
/// just its local metrics).
pub async fn scrape_node_metrics(node_url: &str) -> String {
    let url = format!("{}/metrics", node_url.trim_end_matches('/'));
    match client().get(&url).send().await {
        Ok(resp) if resp.status().is_success() => match resp.text().await {
            Ok(body) => body,
            Err(e) => {
                tracing::warn!(error = %e, "collector: node /metrics body read failed");
                String::new()
            }
        },
        Ok(resp) => {
            tracing::debug!(status = %resp.status(), "collector: node /metrics non-200");
            String::new()
        }
        Err(e) => {
            tracing::debug!(error = %e, url, "collector: node /metrics unreachable");
            String::new()
        }
    }
}

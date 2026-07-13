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

use std::io::SeekFrom;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncSeekExt};

/// HTTP client for scraping the node and POSTing to the log sink. Short timeout —
/// telemetry must never block the data plane.
fn client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap_or_else(|_| reqwest::Client::new())
}

/// Tail the local node's logs and forward them to the log backend.
///
/// `node_log_source` is a path to follow; `sink` is `stdout`/`stderr`/`tracing`
/// or an HTTP(S) endpoint that accepts the chunk (POST). Empty values disable
/// shipping (e.g. when an external log agent owns it). Tracks a byte offset across
/// reads, resets it on truncation (log rotation), and never exits — it's a
/// background task. Interval is `FIDUCIA_LOG_SHIP_INTERVAL_MS` (default 5s).
pub async fn ship_logs(node_log_source: String, sink: String) {
    if node_log_source.trim().is_empty() || sink.trim().is_empty() {
        tracing::info!("log shipping disabled: FIDUCIA_NODE_LOG_SOURCE or FIDUCIA_LOG_SINK empty");
        return;
    }

    let interval = crate::positive_ms_env("FIDUCIA_LOG_SHIP_INTERVAL_MS", 5_000);
    let mut offset = 0u64;

    loop {
        match read_new_log_bytes(&node_log_source, offset).await {
            Ok((next_offset, chunk)) => {
                offset = next_offset;
                if !chunk.is_empty() {
                    ship_log_chunk(&node_log_source, &sink, chunk).await;
                }
            }
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    source = %node_log_source,
                    "failed to read node log source"
                );
            }
        }
        tokio::time::sleep(interval).await;
    }
}

/// Scrape the local node's Prometheus metrics, annotated with this node's
/// identity so the metrics are attributable once merged.
///
/// Prefixes a `fiducia_sidecar_node_scrape_up` gauge (1 when the node was
/// scraped, 0 otherwise, with the failure reason as a comment) so scrape failures
/// are alertable, then the node's own exposition text on success.
pub async fn scrape_node_metrics(node_url: &str) -> String {
    let url = format!("{}/metrics", node_url.trim_end_matches('/'));
    match client().get(url).send().await {
        Ok(response) if response.status().is_success() => {
            let body = response.text().await.unwrap_or_default();
            format!(
                "# HELP fiducia_sidecar_node_scrape_up Whether the sidecar scraped the local node metrics endpoint.\n\
                 # TYPE fiducia_sidecar_node_scrape_up gauge\n\
                 fiducia_sidecar_node_scrape_up 1\n\
                 {body}"
            )
        }
        Ok(response) => format!(
            "# HELP fiducia_sidecar_node_scrape_up Whether the sidecar scraped the local node metrics endpoint.\n\
             # TYPE fiducia_sidecar_node_scrape_up gauge\n\
             fiducia_sidecar_node_scrape_up 0\n\
             # node metrics scrape returned HTTP {}\n",
            response.status().as_u16()
        ),
        Err(err) => format!(
            "# HELP fiducia_sidecar_node_scrape_up Whether the sidecar scraped the local node metrics endpoint.\n\
             # TYPE fiducia_sidecar_node_scrape_up gauge\n\
             fiducia_sidecar_node_scrape_up 0\n\
             # node metrics scrape error: {}\n",
            sanitize_metric_comment(&err.to_string())
        ),
    }
}

async fn read_new_log_bytes(
    path: &str,
    offset: u64,
) -> Result<(u64, String), Box<dyn std::error::Error + Send + Sync>> {
    let metadata = tokio::fs::metadata(path).await?;
    let offset = if metadata.len() < offset { 0 } else { offset };
    let mut file = tokio::fs::File::open(path).await?;
    file.seek(SeekFrom::Start(offset)).await?;
    let mut chunk = String::new();
    file.read_to_string(&mut chunk).await?;
    Ok((offset.saturating_add(chunk.len() as u64), chunk))
}

async fn ship_log_chunk(source: &str, sink: &str, chunk: String) {
    match sink {
        "stdout" | "stderr" | "tracing" => {
            tracing::info!(source, bytes = chunk.len(), log_chunk = %chunk, "node log chunk");
        }
        sink if sink.starts_with("http://") || sink.starts_with("https://") => {
            if let Err(err) = client()
                .post(sink)
                .json(&serde_json::json!({ "source": source, "message": chunk }))
                .send()
                .await
                .and_then(|response| response.error_for_status())
            {
                tracing::warn!(error = %err, sink, "failed to ship node log chunk");
            }
        }
        _ => {
            tracing::warn!(
                sink,
                "unsupported log sink; use stdout, stderr, tracing, or HTTP(S)"
            );
        }
    }
}

fn sanitize_metric_comment(value: &str) -> String {
    value.replace('\n', " ")
}

//! Observability collection: logs + metrics.
//!
//! The other half of the sidecar's job: get the node's telemetry off the box and
//! into the observability stack — **not** the brain (the brain only wants
//! placement metadata, never data-plane logs).
//!
//!   * **logs** — tail the node's stdout / log file and ship to the log backend
//!     (Loki / Elasticsearch / a Vector pipeline).
//!   * **metrics** — scrape the node's `/metrics` and re-expose (or remote-write)
//!     for Prometheus, annotated with this node's identity/failure-domain.
//!
use std::io::SeekFrom;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncSeekExt};

/// Tail the local node's logs and forward them to the log backend.
pub async fn ship_logs(node_log_source: String, sink: String) {
    if node_log_source.trim().is_empty() || sink.trim().is_empty() {
        tracing::info!("log shipping disabled: FIDUCIA_NODE_LOG_SOURCE or FIDUCIA_LOG_SINK empty");
        return;
    }

    let interval = Duration::from_millis(
        std::env::var("FIDUCIA_LOG_SHIP_INTERVAL_MS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(5_000),
    );
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

/// Scrape the local node's Prometheus metrics, to be re-exposed on the sidecar's
/// own `/metrics` (annotated with node id / region / AZ).
///
pub async fn scrape_node_metrics(node_url: &str) -> String {
    let url = format!("{}/metrics", node_url.trim_end_matches('/'));
    match reqwest::Client::new().get(url).send().await {
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
            if let Err(err) = reqwest::Client::new()
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

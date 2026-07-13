//! Observability collection: log shipping.
//!
//! Half of the sidecar's observability job: get the node's logs off the box and
//! into the observability stack — **not** the brain (the brain only wants
//! placement metadata, never data-plane logs).
//!
//!   * **logs** — tail the node's log file and ship to the log backend (an HTTP
//!     sink such as Loki / a Vector pipeline).
//!
//! Metrics are handled by [`crate::exporter`], which translates the node's
//! structured observe API into Prometheus text for `/metrics` (the node has no
//! `/metrics` route of its own to re-expose).
//!
//! (For heavy production use you may prefer a dedicated Vector/Fluent Bit
//! sidecar; this keeps the single-binary deployment self-contained.)

use std::io::SeekFrom;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncSeekExt};

/// HTTP client for POSTing to the log sink. Short timeout — telemetry must never
/// block the data plane.
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

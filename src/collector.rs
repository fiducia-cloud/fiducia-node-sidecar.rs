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

use crate::metrics::SidecarMetrics;

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
pub async fn ship_logs(log_source: String, sink: String, metrics: std::sync::Arc<SidecarMetrics>) {
    if log_source.trim().is_empty() || sink.trim().is_empty() {
        tracing::info!("log shipping disabled: FIDUCIA_LOG_SOURCE or FIDUCIA_LOG_SINK empty");
        return;
    }

    let interval = crate::positive_ms_env("FIDUCIA_LOG_SHIP_INTERVAL_MS", 5_000);
    let mut offset = 0u64;

    loop {
        match read_new_log_bytes(&log_source, offset).await {
            Ok((next_offset, chunk)) => {
                offset = next_offset;
                if !chunk.is_empty() {
                    ship_log_chunk(&sink, chunk, &metrics).await;
                }
            }
            Err(_) => {
                metrics.log_read_failure();
                tracing::warn!("failed to read configured workload log source");
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
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes).await?;
    let next_offset = offset.saturating_add(bytes.len() as u64);
    Ok((next_offset, String::from_utf8_lossy(&bytes).into_owned()))
}

async fn ship_log_chunk(sink: &str, chunk: String, metrics: &SidecarMetrics) {
    let bytes = chunk.len();
    match sink {
        "stdout" | "stderr" | "tracing" => {
            tracing::info!(bytes, log_chunk = %chunk, "workload log chunk");
            metrics.log_ship_success(bytes);
        }
        sink if sink.starts_with("http://") || sink.starts_with("https://") => {
            if let Err(error) = client()
                .post(sink)
                .json(&serde_json::json!({ "message": chunk }))
                .send()
                .await
                .and_then(|response| response.error_for_status())
            {
                metrics.log_ship_failure();
                tracing::warn!(
                    endpoint = %crate::endpoint_label(sink),
                    timeout = error.is_timeout(),
                    connect = error.is_connect(),
                    status = error.status().map(|status| status.as_u16()),
                    "failed to ship workload log chunk"
                );
            } else {
                metrics.log_ship_success(bytes);
            }
        }
        _ => {
            metrics.log_ship_failure();
            tracing::warn!("unsupported log sink; use stdout, stderr, tracing, or HTTP(S)");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn unsupported_sink_is_observable_without_echoing_its_value() {
        let metrics = SidecarMetrics::default();
        ship_log_chunk("not-a-supported-sink", "payload".into(), &metrics).await;
        assert!(metrics
            .render()
            .contains("fiducia_sidecar_log_ship_failures_total 1\n"));
    }
}

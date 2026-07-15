//! Sidecar-local counters, rendered alongside the target's translated metrics.

use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Default)]
pub(crate) struct SidecarMetrics {
    node_scrape_attempts: AtomicU64,
    node_scrape_failures: AtomicU64,
    heartbeat_attempts: AtomicU64,
    heartbeat_successes: AtomicU64,
    heartbeat_failures: AtomicU64,
    log_read_failures: AtomicU64,
    log_ship_successes: AtomicU64,
    log_ship_failures: AtomicU64,
    log_bytes_shipped: AtomicU64,
}

macro_rules! incrementer {
    ($name:ident, $field:ident) => {
        pub(crate) fn $name(&self) {
            self.$field.fetch_add(1, Ordering::Relaxed);
        }
    };
}

impl SidecarMetrics {
    incrementer!(node_scrape_attempt, node_scrape_attempts);
    incrementer!(node_scrape_failure, node_scrape_failures);
    incrementer!(heartbeat_attempt, heartbeat_attempts);
    incrementer!(heartbeat_success, heartbeat_successes);
    incrementer!(heartbeat_failure, heartbeat_failures);
    incrementer!(log_read_failure, log_read_failures);
    incrementer!(log_ship_failure, log_ship_failures);

    pub(crate) fn log_ship_success(&self, bytes: usize) {
        self.log_ship_successes.fetch_add(1, Ordering::Relaxed);
        self.log_bytes_shipped
            .fetch_add(bytes as u64, Ordering::Relaxed);
    }

    pub(crate) fn render(&self) -> String {
        let value = |counter: &AtomicU64| counter.load(Ordering::Relaxed);
        let rows = [
            (
                "fiducia_sidecar_node_scrape_attempts_total",
                value(&self.node_scrape_attempts),
            ),
            (
                "fiducia_sidecar_node_scrape_failures_total",
                value(&self.node_scrape_failures),
            ),
            (
                "fiducia_sidecar_heartbeat_attempts_total",
                value(&self.heartbeat_attempts),
            ),
            (
                "fiducia_sidecar_heartbeat_successes_total",
                value(&self.heartbeat_successes),
            ),
            (
                "fiducia_sidecar_heartbeat_failures_total",
                value(&self.heartbeat_failures),
            ),
            (
                "fiducia_sidecar_log_read_failures_total",
                value(&self.log_read_failures),
            ),
            (
                "fiducia_sidecar_log_ship_successes_total",
                value(&self.log_ship_successes),
            ),
            (
                "fiducia_sidecar_log_ship_failures_total",
                value(&self.log_ship_failures),
            ),
            (
                "fiducia_sidecar_log_bytes_shipped_total",
                value(&self.log_bytes_shipped),
            ),
        ];
        let mut output = String::new();
        for (name, current) in rows {
            output.push_str(&format!("# TYPE {name} counter\n{name} {current}\n"));
        }
        output
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_independent_delivery_and_heartbeat_counters() {
        let metrics = SidecarMetrics::default();
        metrics.node_scrape_attempt();
        metrics.heartbeat_attempt();
        metrics.heartbeat_failure();
        metrics.log_ship_success(17);
        let output = metrics.render();
        assert!(output.contains("fiducia_sidecar_node_scrape_attempts_total 1\n"));
        assert!(output.contains("fiducia_sidecar_heartbeat_failures_total 1\n"));
        assert!(output.contains("fiducia_sidecar_log_bytes_shipped_total 17\n"));
        assert!(output.contains("fiducia_sidecar_log_ship_failures_total 0\n"));
    }
}

# src — fiducia-node-sidecar

The Rust source for the shared operational sidecar. The same image runs beside
each node and brain pod with target-specific behavior, keeping both Raft
binaries independent from the telemetry backend.

Key files:

- `main.rs` — process entry point and wiring.
- `heartbeat.rs` — the implemented control-plane bridge: scrapes the local node's
  `/v1/status` and POSTs heartbeats (address, failure domain, hosted/led shards)
  to the brain.
- `meta.rs` — static node metadata the sidecar reports on the node's behalf
  (notably its failure domain / region).
- `collector.rs` — tails and forwards an optional colocated workload log.
- `exporter.rs` — fetches the node's observe API (or the brain's `/v1/status`)
  and translates it into Prometheus text exposition for `/metrics`.
- `metrics.rs` — sidecar-local scrape, heartbeat, and log-delivery counters.
- `auth.rs` — the shared trusted-hop `x-fiducia-internal-auth` header used by both
  the heartbeat bridge and the exporter on their outbound `/v1` calls.

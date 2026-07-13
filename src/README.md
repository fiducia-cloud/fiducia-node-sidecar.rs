# src — fiducia-node-sidecar

The Rust source for the per-node operational sidecar. One runs alongside each
`fiducia-node` (same pod, localhost to the node) and owns everything operational
so the node binary can stay a pure coordination engine.

Key files:

- `main.rs` — process entry point and wiring.
- `heartbeat.rs` — the implemented control-plane bridge: scrapes the local node's
  `/v1/status` and POSTs heartbeats (address, failure domain, hosted/led shards)
  to the brain.
- `meta.rs` — static node metadata the sidecar reports on the node's behalf
  (notably its failure domain / region).
- `collector.rs` — implemented log tailing/shipping plus node metrics scraping
  and re-exposure with scrape-health telemetry.

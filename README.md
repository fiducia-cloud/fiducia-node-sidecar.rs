# fiducia-node-sidecar

The per-node **operational sidecar** for [fiducia.cloud](https://fiducia.cloud).
One runs alongside each [`fiducia-node`](https://github.com/fiducia-cloud/fiducia-node.rs)
(same pod, localhost to the node) and owns everything *operational* so the node
binary stays a pure coordination engine. This repository is a **skeleton**.

## Why split it out

The node should do exactly one thing well: sharded Raft coordination. Everything
else — talking to the control plane, shipping logs, exposing metrics — lives
here, so the node has no dependency on the brain or the telemetry stack.

| Concern | What the sidecar does | Goes to |
|---------|-----------------------|---------|
| **Control-plane bridge** | scrape local node `/v1/status`; heartbeat liveness + reported shards + node **metadata** (region/AZ/rack) | `fiducia-brain` |
| **Logs** | tail the node's stdout/log file and ship | log backend (Loki / Vector pipeline) |
| **Metrics** | scrape the node's `/metrics`, re-expose annotated with node identity | Prometheus |

Note: data-plane **Raft logs are never shipped** anywhere — their durability is
the replication itself. The sidecar moves *telemetry* and *placement metadata*,
never coordination data. The brain gets metadata; the observability stack gets
logs/metrics.

## Failure-domain metadata

The most important thing the sidecar reports is the node's **failure domain**
(`FIDUCIA_REGION` / `FIDUCIA_AZ` / `FIDUCIA_RACK`). The brain uses it to spread a
shard's replicas so one rack/zone loss can't take a quorum.

> Caveat (left open): if liveness is reported *only* via the sidecar, a dead
> sidecar looks like a dead node. Either tie sidecar liveness to the node's, or
> keep a minimal direct node→brain liveness ping and let the sidecar own only the
> richer metadata.

## Endpoints

| Route | Purpose |
|-------|---------|
| `/healthz`, `/readyz` | sidecar liveness |
| `/meta` | node metadata the sidecar reports upstream |
| `/metrics` | re-exposed node metrics + sidecar-local metrics |

## Layout

| File              | Responsibility                                          |
|-------------------|---------------------------------------------------------|
| `src/main.rs`     | wiring, spawns heartbeat + collectors, HTTP surface     |
| `src/heartbeat.rs`| scrape node status → heartbeat to brain                 |
| `src/meta.rs`     | node identity + failure-domain metadata                 |
| `src/collector.rs`| log shipping + metric scraping                          |

## Run locally

```bash
FIDUCIA_NODE_ID=node-a FIDUCIA_NODE_URL=http://localhost:8090 \
FIDUCIA_BRAIN_URL=http://localhost:8095 FIDUCIA_AZ=us-east-1a cargo run   # :8091
```

Env: `PORT`, `FIDUCIA_NODE_ID`, `FIDUCIA_NODE_URL`, `FIDUCIA_BRAIN_URL`,
`FIDUCIA_HEARTBEAT_MS`, `FIDUCIA_NODE_ADDRESS`, `FIDUCIA_REGION`, `FIDUCIA_AZ`,
`FIDUCIA_RACK`, `FIDUCIA_NODE_VERSION`.

## Related

- [`fiducia-node.rs`](https://github.com/fiducia-cloud/fiducia-node.rs) — the coordination engine this wraps.
- [`fiducia-brain.rs`](https://github.com/fiducia-cloud/fiducia-brain.rs) — control plane it heartbeats to.
- [`fiducia-load-balance.rs`](https://github.com/fiducia-cloud/fiducia-load-balance.rs) — edge router.

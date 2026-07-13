# fiducia-node-sidecar

The per-node **operational sidecar** for [fiducia.cloud](https://fiducia.cloud).
One runs alongside each [`fiducia-node`](https://github.com/fiducia-cloud/fiducia-node.rs)
(same pod, localhost to the node) and owns everything *operational* so the node
binary stays a pure coordination engine.

The **control-plane bridge is implemented**: on a timer it scrapes the local
node's `/v1/status` and POSTs a heartbeat (address, failure domain = region, and
the shards it hosts/leads) to the brain's `/v1/nodes/{id}/heartbeat`.

The observability path is implemented too: it tails a configured node log and
forwards new chunks to tracing or an HTTP sink, and it exposes a Prometheus
`/metrics` endpoint that **translates** the node's structured observability API
(`/v1/observe/shards`, `/v1/observe/metrics`, `/readyz`) — or, in `brain` mode,
the brain's `/v1/status` rollup — into `fiducia_`-prefixed metric families. (The
node has no `/metrics` route of its own to re-expose; the sidecar renders one from
the JSON introspection instead.) A dedicated Vector or Fluent Bit sidecar can
replace log shipping in larger installs by leaving the log source and sink unset.

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

## Configuration

The sidecar is configured entirely from the environment. `FIDUCIA_INTERNAL_SECRET`
is **required**: the process refuses to start without it (see *Trust boundary*).

| Variable | Type | Default | Secret? | Meaning |
|----------|------|---------|:-------:|---------|
| `FIDUCIA_INTERNAL_SECRET` | string | *(none — required)* | **yes** | Trusted-hop auth secret sent as `x-fiducia-internal-auth` on every node/brain `/v1` call. Startup fails closed if unset/empty. |
| `PORT` | integer | `8091` | no | TCP port the sidecar HTTP surface listens on. |
| `FIDUCIA_NODE_ID` | string | `node-a` | no | Stable identifier of the local node. |
| `FIDUCIA_NODE_URL` | string | `http://localhost:8090` | no | Base URL of the local node to scrape (`/v1/status`, `/metrics`). |
| `FIDUCIA_BRAIN_URL` | string | `http://localhost:8095` | no | Base URL of the control-plane brain to heartbeat to. |
| `FIDUCIA_HEARTBEAT_MS` | positive integer | `2000` | no | Heartbeat interval, milliseconds. Zero, negative, or unparsable values fall back to the default instead of panicking the background task. |
| `FIDUCIA_NODE_ADDRESS` | string | `http://localhost:8090` | no | Address peers/clients reach the node at (advertised to the brain). |
| `FIDUCIA_REGION` | string | *(unset)* | no | Region — the primary failure domain the brain spreads replicas across. |
| `FIDUCIA_AZ` | string | *(unset)* | no | Availability zone (failure-domain metadata). |
| `FIDUCIA_RACK` | string | *(unset)* | no | Rack (failure-domain metadata). |
| `FIDUCIA_NODE_VERSION` | string | *(unset)* | no | Reported node version (metadata). |
| `FIDUCIA_NODE_LOG_SOURCE` | string | *(unset)* | no | Path to the node log file to tail and ship. Empty disables log shipping. |
| `FIDUCIA_LOG_SINK` | string | *(unset)* | no | Log sink: `stdout`, `stderr`, `tracing`, or an HTTP(S) endpoint. Empty disables log shipping. |
| `FIDUCIA_LOG_SHIP_INTERVAL_MS` | positive integer | `5000` | no | Log-shipping poll interval, milliseconds. Zero, negative, or unparsable values fall back to the default instead of busy-looping. |

## Trust boundary

`FIDUCIA_INTERNAL_SECRET` authenticates the sidecar's **outbound** trusted-hop
calls to the two guarded `/v1` planes — the local node (`GET /v1/status`) and the
brain (`POST /v1/nodes/{id}/heartbeat`) — attached as the `x-fiducia-internal-auth`
header. It is **secure by default**: `main.rs` requires it at startup and the
process refuses to boot (exits with an error) if it is unset or blank, so the
sidecar can never silently emit unauthenticated heartbeats. The secret is never
logged and never compared in-process (the sidecar only *presents* it), so there
is no timing-comparison surface here.

The sidecar's own HTTP surface (`/healthz`, `/readyz`, `/meta`, `/metrics`) is
unauthenticated by design: it exposes only local liveness and telemetry and is
meant to be reached over pod-local networking / a scrape ACL, never the public
internet.

## Run locally (single node)

Provide a dev secret so the sidecar starts; everything else can default:

```bash
FIDUCIA_INTERNAL_SECRET=dev-secret \
FIDUCIA_NODE_ID=node-a FIDUCIA_NODE_URL=http://localhost:8090 \
FIDUCIA_BRAIN_URL=http://localhost:8095 FIDUCIA_AZ=us-east-1a cargo run --locked   # :8091
```

Use a throwaway value for `FIDUCIA_INTERNAL_SECRET` in dev; in production it must
match the node's and brain's configured trusted-hop secret and be delivered as a
real secret (never committed, never a shell-history literal).

### Reproducible container and CI dependency

The sidecar consumes generated contracts from the sibling
`fiducia-interfaces` repository. CI and the Dockerfile pin it to commit
`487e470c45ab5851e8f6f3b1dc048fe067fbf408`; neither follows a moving branch.
The Docker build checks the commit out detached and verifies that its full
`HEAD` equals `INTERFACES_SHA`, so branches, tags, and abbreviated hashes fail
closed. Update the Dockerfile argument and CI checkout `ref` together when
adopting a reviewed contracts commit.

```bash
docker build \
  --build-arg INTERFACES_SHA=<40-character-commit-sha> \
  -t fiducia-node-sidecar:local .
```

## flags-2-env

Non-secret settings can be mapped to the `FIDUCIA_*`/`PORT` env vars above through the
pinned [`ORESoftware/flags-2-env`](https://github.com/ORESoftware/flags-2-env)
parser (vendored as a submodule). The schema lives in `.cli-flags.toml` and is
audited in CI (`.github/workflows/cli-flags.yml`).

```bash
git submodule update --init --recursive
make -B -C vendor/flags-2-env all
FIDUCIA_INTERNAL_SECRET="$FIDUCIA_INTERNAL_SECRET" \
  scripts/with-flags2env.sh --node-id=node-a --brain-url=http://localhost:8095 -- cargo run --locked
```

`scripts/with-flags2env.sh` runs `flags2env` against `.cli-flags.toml`, exports
the resulting env map, then execs the given command.
`FIDUCIA_INTERNAL_SECRET` is deliberately excluded from the CLI schema; inject it
through the environment or a secret store.

## Security

Hardening applied / verified:

- **Fail-closed auth secret** — `FIDUCIA_INTERNAL_SECRET` is required at startup;
  the sidecar refuses to run (and therefore never sends unauthenticated
  outbound calls) when it is missing or blank.
- **Request hardening stack** — every endpoint runs behind a body-size cap
  (`RequestBodyLimitLayer`, 64 KiB), a request timeout (`TimeoutLayer`, 15 s),
  and `CatchPanicLayer` so a panicking handler returns `500` instead of dropping
  the connection.
- **Safe background intervals** — heartbeat and log-shipping periods must parse
  as positive milliseconds; missing, zero, negative, and malformed values use
  their documented defaults so heartbeat cannot panic and log shipping cannot
  spin in a zero-delay loop.
- **No unsafe / no reachable panics** — no `unsafe` blocks; network-facing paths
  use fallible parsing and `unwrap_or_*` fallbacks rather than `unwrap()/expect()`.
- **No timing-unsafe secret comparison** — the secret is only presented outbound,
  never compared in-process.

Accepted advisories: none. `cargo audit` is clean (0 vulnerabilities across the
dependency tree at the last scan).

## Related

- [`fiducia-node.rs`](https://github.com/fiducia-cloud/fiducia-node.rs) — the coordination engine this wraps.
- [`fiducia-brain.rs`](https://github.com/fiducia-cloud/fiducia-brain.rs) — control plane it heartbeats to.
- [`fiducia-load-balance.rs`](https://github.com/fiducia-cloud/fiducia-load-balance.rs) — edge router.

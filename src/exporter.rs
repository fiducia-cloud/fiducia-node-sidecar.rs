//! Prometheus exporter: translate a fiducia control plane's JSON introspection
//! into text exposition format (v0.0.4).
//!
//! The sidecar's `/metrics` endpoint delegates here. Rather than proxy a
//! `/metrics` route the node does not have, this fetches the node's structured
//! observability JSON (`/v1/observe/shards`, `/v1/observe/metrics`, `/readyz`) —
//! or, in `brain` mode, the brain's `/v1/status` rollup — and renders a
//! Prometheus scrape from it, all families `fiducia_`-prefixed and carrying this
//! node's identity (`node_id`, and `region` when set) as constant labels.
//!
//! Every outbound fetch presents the trusted-hop `x-fiducia-internal-auth` header
//! (see [`crate::auth`]); the node's observe/readyz paths are org-exempt, so no
//! `x-fiducia-org-id` is ever sent. A fetch failure never fails the endpoint: it
//! sets `fiducia_sidecar_scrape_up{target=...} 0` plus a comment naming the
//! failure class and still returns `200`, so Prometheus records the `up=0` signal.

use serde_json::Value;

use crate::meta::NodeMeta;

/// Which control plane this sidecar exports. Selected by `FIDUCIA_EXPORT_TARGET`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Target {
    Node,
    Brain,
}

impl Target {
    fn label(self) -> &'static str {
        match self {
            Target::Node => "node",
            Target::Brain => "brain",
        }
    }
}

/// Constant labels stamped on every family the exporter emits.
#[derive(Debug, Clone)]
pub(crate) struct ConstLabels {
    pub(crate) node_id: String,
    pub(crate) region: Option<String>,
}

impl ConstLabels {
    /// The ordered constant label set: `node_id` always, `region` only when set.
    fn pairs(&self) -> Vec<(&'static str, String)> {
        let mut pairs = vec![("node_id", self.node_id.clone())];
        if let Some(region) = self.region.as_ref().filter(|r| !r.is_empty()) {
            pairs.push(("region", region.clone()));
        }
        pairs
    }
}

/// The exporter's fetch + translate configuration.
pub(crate) struct Exporter {
    pub(crate) target: Target,
    pub(crate) node_url: String,
    pub(crate) brain_url: String,
    pub(crate) client: reqwest::Client,
    pub(crate) secret: Option<String>,
    pub(crate) labels: ConstLabels,
}

impl Exporter {
    /// Build from the process environment plus the resolved node/brain URLs and
    /// this node's metadata. `FIDUCIA_EXPORT_TARGET` selects the plane (default
    /// `node`); `FIDUCIA_OBSERVE_TIMEOUT_MS` bounds each fetch (default 3000).
    pub(crate) fn from_env(node_url: String, brain_url: String, meta: &NodeMeta) -> Self {
        let target = match std::env::var("FIDUCIA_EXPORT_TARGET") {
            Ok(value) if value.trim().eq_ignore_ascii_case("brain") => Target::Brain,
            _ => Target::Node,
        };
        Exporter {
            target,
            node_url,
            brain_url,
            // One client for the sidecar's lifetime (connection reuse on the
            // localhost scrape path). Fail fast at startup: the old per-scrape
            // fallback to `Client::new()` silently dropped the timeout, so a hung
            // upstream could stall every scrape.
            client: reqwest::Client::builder()
                .timeout(crate::positive_ms_env("FIDUCIA_OBSERVE_TIMEOUT_MS", 3000))
                .build()
                .expect("failed to build the exporter HTTP client"),
            secret: crate::auth::internal_secret().map(str::to_string),
            labels: ConstLabels {
                node_id: meta.node_id.clone(),
                region: meta.region.clone(),
            },
        }
    }

    /// Fetch the configured target and render a Prometheus scrape. Never fails
    /// the endpoint: on a fetch error it logs, emits the scrape-down signal, and
    /// still returns a valid exposition body.
    pub(crate) async fn render(&self) -> String {
        let consts = self.labels.pairs();

        let scraped = match self.target {
            Target::Node => self.scrape_node().await,
            Target::Brain => self.scrape_brain().await,
        };

        match scraped {
            Ok(families) => render_ok(&consts, self.target, &families),
            Err(fail) => {
                // The scrape body only carries `scrape_up=0` + a comment; without
                // a log line a persistently-down target is invisible to anyone
                // reading logs instead of metrics.
                tracing::warn!(
                    target = self.target.label(),
                    class = fail.class(),
                    detail = %fail.detail(),
                    "sidecar: export-target scrape failed; /metrics reports scrape_up=0"
                );
                render_fail(&consts, self.target, &fail)
            }
        }
    }

    async fn scrape_node(&self) -> Result<Vec<Family>, FetchFail> {
        let shards = self
            .fetch_json(&self.node_url, "/v1/observe/shards", false)
            .await?;
        let metrics = self
            .fetch_json(&self.node_url, "/v1/observe/metrics", false)
            .await?;
        // /readyz answers 503 (not 5xx-error) when the node is up but not ready;
        // that is a valid readiness reading, not a scrape failure.
        let readyz = self.fetch_json(&self.node_url, "/readyz", true).await?;
        Ok(node_families(&shards, &metrics, &readyz))
    }

    async fn scrape_brain(&self) -> Result<Vec<Family>, FetchFail> {
        let status = self
            .fetch_json(&self.brain_url, "/v1/status", false)
            .await?;
        Ok(brain_families(&status))
    }

    async fn fetch_json(
        &self,
        base: &str,
        path: &str,
        accept_503: bool,
    ) -> Result<Value, FetchFail> {
        let url = format!("{}{}", base.trim_end_matches('/'), path);
        let response = crate::auth::attach_with(self.client.get(&url), self.secret.as_deref())
            .send()
            .await
            .map_err(FetchFail::from_reqwest)?;
        let status = response.status();
        let acceptable = status.is_success() || (accept_503 && status.as_u16() == 503);
        if !acceptable {
            return Err(FetchFail::BadStatus(status.as_u16()));
        }
        let body = response.text().await.map_err(FetchFail::from_reqwest)?;
        serde_json::from_str(&body).map_err(|_| FetchFail::BadBody)
    }
}

// ---------------------------------------------------------------------------
// Fetch failure classification.
// ---------------------------------------------------------------------------

/// Why a scrape could not be completed, mapped to a stable class name for the
/// comment line so operators/alerts can distinguish causes.
#[derive(Debug)]
enum FetchFail {
    Timeout,
    Unreachable,
    BadStatus(u16),
    BadBody,
}

impl FetchFail {
    fn from_reqwest(err: reqwest::Error) -> Self {
        if err.is_timeout() {
            FetchFail::Timeout
        } else {
            // Connection refused, DNS, connect errors, and body-read failures all
            // present as the node/brain being effectively unreachable.
            FetchFail::Unreachable
        }
    }

    fn class(&self) -> &'static str {
        match self {
            FetchFail::Timeout => "timeout",
            FetchFail::Unreachable => "unreachable",
            FetchFail::BadStatus(_) => "bad status",
            FetchFail::BadBody => "bad body",
        }
    }

    fn detail(&self) -> String {
        match self {
            FetchFail::BadStatus(code) => format!("HTTP {code}"),
            _ => String::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// Metric family model + rendering.
// ---------------------------------------------------------------------------

/// One sample line: an optional name suffix (for histogram `_bucket`/`_sum`/
/// `_count`), the labels beyond the constant set, and a pre-formatted value.
struct Sample {
    suffix: &'static str,
    labels: Vec<(&'static str, String)>,
    value: String,
}

impl Sample {
    fn scalar(value: String) -> Self {
        Sample {
            suffix: "",
            labels: Vec::new(),
            value,
        }
    }

    fn labeled(labels: Vec<(&'static str, String)>, value: String) -> Self {
        Sample {
            suffix: "",
            labels,
            value,
        }
    }
}

/// A metric family: HELP/TYPE emitted once, then every sample. A family with no
/// samples is skipped entirely (no bare HELP/TYPE).
struct Family {
    base: &'static str,
    help: &'static str,
    kind: &'static str,
    samples: Vec<Sample>,
}

impl Family {
    fn scalar(base: &'static str, help: &'static str, kind: &'static str, value: String) -> Self {
        Family {
            base,
            help,
            kind,
            samples: vec![Sample::scalar(value)],
        }
    }
}

fn render_ok(consts: &[(&'static str, String)], target: Target, families: &[Family]) -> String {
    let mut out = String::new();
    render_family(&mut out, consts, &scrape_up_family(target, true));
    for family in families {
        render_family(&mut out, consts, family);
    }
    out
}

fn render_fail(consts: &[(&'static str, String)], target: Target, fail: &FetchFail) -> String {
    let mut out = String::new();
    render_family(&mut out, consts, &scrape_up_family(target, false));
    let detail = fail.detail();
    if detail.is_empty() {
        out.push_str(&format!(
            "# {} scrape failed ({})\n",
            target.label(),
            fail.class()
        ));
    } else {
        out.push_str(&format!(
            "# {} scrape failed ({}): {}\n",
            target.label(),
            fail.class(),
            sanitize_comment(&detail)
        ));
    }
    out
}

fn scrape_up_family(target: Target, up: bool) -> Family {
    Family {
        base: "fiducia_sidecar_scrape_up",
        help: "1 if the sidecar fetched and translated its export target this scrape.",
        kind: "gauge",
        samples: vec![Sample::labeled(
            vec![("target", target.label().to_string())],
            bool_value(up),
        )],
    }
}

fn render_family(out: &mut String, consts: &[(&'static str, String)], family: &Family) {
    if family.samples.is_empty() {
        return;
    }
    out.push_str("# HELP ");
    out.push_str(family.base);
    out.push(' ');
    out.push_str(family.help);
    out.push('\n');
    out.push_str("# TYPE ");
    out.push_str(family.base);
    out.push(' ');
    out.push_str(family.kind);
    out.push('\n');
    for sample in &family.samples {
        out.push_str(family.base);
        out.push_str(sample.suffix);
        out.push_str(&render_labels(consts, &sample.labels));
        out.push(' ');
        out.push_str(&sample.value);
        out.push('\n');
    }
}

fn render_labels(consts: &[(&'static str, String)], extra: &[(&'static str, String)]) -> String {
    let mut parts: Vec<String> = Vec::with_capacity(consts.len() + extra.len());
    for (key, value) in consts.iter().chain(extra.iter()) {
        parts.push(format!("{key}=\"{}\"", escape_label(value)));
    }
    if parts.is_empty() {
        String::new()
    } else {
        format!("{{{}}}", parts.join(","))
    }
}

// ---------------------------------------------------------------------------
// Node-mode translation.
// ---------------------------------------------------------------------------

/// Translate `/v1/observe/shards` + `/v1/observe/metrics` + `/readyz` into the
/// node-mode metric families, in a fixed (test-stable) order.
fn node_families(shards: &Value, metrics: &Value, readyz: &Value) -> Vec<Family> {
    let mut families = Vec::new();

    // Node-level gauges.
    families.push(Family::scalar(
        "fiducia_node_up",
        "1 if the sidecar reached the local node's observe API this scrape.",
        "gauge",
        "1".to_string(),
    ));
    let ready = readyz.get("status").and_then(Value::as_str) == Some("ok");
    families.push(Family::scalar(
        "fiducia_node_ready",
        "1 if the local node reports ready (GET /readyz).",
        "gauge",
        bool_value(ready),
    ));
    families.push(Family::scalar(
        "fiducia_node_all_shards_running",
        "1 if every hosted shard is running and responsive.",
        "gauge",
        bool_value(
            readyz
                .get("all_shards_running")
                .and_then(Value::as_bool)
                .unwrap_or(false),
        ),
    ));
    families.push(Family::scalar(
        "fiducia_node_shards",
        "Number of shards hosted by this node.",
        "gauge",
        u64_value(shards.get("shard_count").and_then(Value::as_u64)),
    ));
    families.push(Family::scalar(
        "fiducia_node_leader_count",
        "Number of hosted shards this node currently leads.",
        "gauge",
        u64_value(shards.get("leader_count").and_then(Value::as_u64)),
    ));
    families.push(Family::scalar(
        "fiducia_node_follower_count",
        "Number of hosted shards this node currently follows.",
        "gauge",
        u64_value(shards.get("follower_count").and_then(Value::as_u64)),
    ));

    // Quorum rollup gauges.
    let quorum = &shards["quorum"];
    families.push(Family::scalar(
        "fiducia_quorum_leaderless_shards",
        "Hosted shards that currently have no known leader.",
        "gauge",
        array_len_value(&quorum["leaderless_shards"]),
    ));
    families.push(Family::scalar(
        "fiducia_quorum_at_risk_led_shards",
        "Led shards where a majority is not caught up (one more failure stalls them).",
        "gauge",
        array_len_value(&quorum["at_risk_led_shards"]),
    ));
    families.push(Family::scalar(
        "fiducia_quorum_storage_faulted_shards",
        "Hosted shards whose durable store reported a fault.",
        "gauge",
        array_len_value(&quorum["storage_faulted_shards"]),
    ));
    families.push(Family::scalar(
        "fiducia_quorum_unresponsive_shards",
        "Hosted shard actors that did not answer the bounded status probe.",
        "gauge",
        array_len_value(&quorum["unresponsive_shards"]),
    ));
    families.push(Family::scalar(
        "fiducia_quorum_status_complete",
        "1 if every hosted shard reported a complete, responsive status.",
        "gauge",
        bool_value(
            quorum
                .get("status_complete")
                .and_then(Value::as_bool)
                .unwrap_or(false),
        ),
    ));

    // Per-shard (and per-shard-peer) families.
    let mut rows: Vec<&Value> = shards["shards"]
        .as_array()
        .map(|a| a.iter().collect())
        .unwrap_or_default();
    rows.sort_by_key(|s| s["shard_id"].as_u64().unwrap_or(0));
    families.extend(per_shard_families(&rows));

    // Per-op families.
    families.extend(op_families(metrics));

    families
}

/// Build the per-shard and per-shard-peer families from the (sorted) shard rows.
fn per_shard_families(rows: &[&Value]) -> Vec<Family> {
    let mut term = Vec::new();
    let mut commit_index = Vec::new();
    let mut last_applied = Vec::new();
    let mut last_log_index = Vec::new();
    let mut snapshot_index = Vec::new();
    let mut retained = Vec::new();
    let mut storage_healthy = Vec::new();
    let mut has_quorum = Vec::new();
    let mut healthy_replicas = Vec::new();
    let mut is_leader = Vec::new();
    let mut append_rtt = Vec::new();
    let mut quorum_rtt = Vec::new();
    let mut follower_lag = Vec::new();
    let mut leader_transfers = Vec::new();
    let mut replication_lag = Vec::new();
    let mut replication_in_flight = Vec::new();
    let mut replication_match = Vec::new();

    for row in rows {
        let shard = shard_label(row);
        let per = |value: String| Sample::labeled(vec![shard.clone()], value);
        let m = &row["metrics"];

        term.push(per(u64_value(row["term"].as_u64())));
        commit_index.push(per(u64_value(row["commit_index"].as_u64())));
        last_applied.push(per(u64_value(row["last_applied"].as_u64())));
        last_log_index.push(per(u64_value(row["last_log_index"].as_u64())));
        snapshot_index.push(per(u64_value(row["snapshot_index"].as_u64())));
        retained.push(per(u64_value(row["retained_log_entries"].as_u64())));
        storage_healthy.push(per(bool_value(
            row["storage_healthy"].as_bool().unwrap_or(false),
        )));
        has_quorum.push(per(bool_value(
            row["has_quorum"].as_bool().unwrap_or(false),
        )));
        healthy_replicas.push(per(u64_value(row["healthy_replicas"].as_u64())));
        is_leader.push(per(bool_value(row["role"].as_str() == Some("leader"))));
        follower_lag.push(per(u64_value(m["follower_lag_max"].as_u64())));
        leader_transfers.push(per(u64_value(m["leader_transfer_count"].as_u64())));
        // RTT gauges are leader-observed; absent (null) on followers, so we omit
        // the sample rather than emit a misleading zero.
        if let Some(v) = m["append_rtt_ms_last"].as_u64() {
            append_rtt.push(per(v.to_string()));
        }
        if let Some(v) = m["quorum_rtt_ms_last"].as_u64() {
            quorum_rtt.push(per(v.to_string()));
        }

        // Per-peer replication (leader only; the node omits the array otherwise).
        if let Some(peers) = row["replication"].as_array() {
            let mut peers: Vec<&Value> = peers.iter().collect();
            peers.sort_by(|a, b| {
                a["peer"]
                    .as_str()
                    .unwrap_or("")
                    .cmp(b["peer"].as_str().unwrap_or(""))
            });
            for peer in peers {
                let labels = vec![
                    shard.clone(),
                    ("peer", peer["peer"].as_str().unwrap_or("").to_string()),
                ];
                replication_lag.push(Sample::labeled(
                    labels.clone(),
                    u64_value(peer["lag"].as_u64()),
                ));
                replication_in_flight.push(Sample::labeled(
                    labels.clone(),
                    bool_value(peer["in_flight"].as_bool().unwrap_or(false)),
                ));
                replication_match.push(Sample::labeled(
                    labels,
                    u64_value(peer["match_index"].as_u64()),
                ));
            }
        }
    }

    vec![
        Family {
            base: "fiducia_raft_term",
            help: "Current Raft term of a hosted shard.",
            kind: "gauge",
            samples: term,
        },
        Family {
            base: "fiducia_raft_commit_index",
            help: "Highest committed log index of a hosted shard.",
            kind: "gauge",
            samples: commit_index,
        },
        Family {
            base: "fiducia_raft_last_applied",
            help: "Highest log index applied to the state machine.",
            kind: "gauge",
            samples: last_applied,
        },
        Family {
            base: "fiducia_raft_last_log_index",
            help: "Highest log index present in a hosted shard's log.",
            kind: "gauge",
            samples: last_log_index,
        },
        Family {
            base: "fiducia_raft_snapshot_index",
            help: "Highest index included in the durable snapshot.",
            kind: "gauge",
            samples: snapshot_index,
        },
        Family {
            base: "fiducia_raft_retained_log_entries",
            help: "Log entries retained after compaction.",
            kind: "gauge",
            samples: retained,
        },
        Family {
            base: "fiducia_raft_storage_healthy",
            help: "1 if the shard's durable store is healthy.",
            kind: "gauge",
            samples: storage_healthy,
        },
        Family {
            base: "fiducia_raft_has_quorum",
            help: "1 if a majority of the shard group is caught up (leader-judged).",
            kind: "gauge",
            samples: has_quorum,
        },
        Family {
            base: "fiducia_raft_healthy_replicas",
            help: "Replicas (incl. self) caught up to commit_index (leader-only).",
            kind: "gauge",
            samples: healthy_replicas,
        },
        Family {
            base: "fiducia_raft_is_leader",
            help: "1 if this node currently leads the shard.",
            kind: "gauge",
            samples: is_leader,
        },
        Family {
            base: "fiducia_raft_append_rtt_ms",
            help: "Last AppendEntries round-trip in ms (leader-observed).",
            kind: "gauge",
            samples: append_rtt,
        },
        Family {
            base: "fiducia_raft_quorum_rtt_ms",
            help: "Last append-to-quorum-commit latency in ms (leader-observed).",
            kind: "gauge",
            samples: quorum_rtt,
        },
        Family {
            base: "fiducia_raft_follower_lag_max",
            help: "Max leader-to-follower match-index lag across peers.",
            kind: "gauge",
            samples: follower_lag,
        },
        Family {
            base: "fiducia_raft_leader_transfers_total",
            help: "Observed leadership changes into or out of leader on this shard.",
            kind: "counter",
            samples: leader_transfers,
        },
        Family {
            base: "fiducia_raft_replication_lag",
            help: "Per-peer replication lag behind the leader's log tail.",
            kind: "gauge",
            samples: replication_lag,
        },
        Family {
            base: "fiducia_raft_replication_in_flight",
            help: "1 if an AppendEntries to the peer is currently outstanding.",
            kind: "gauge",
            samples: replication_in_flight,
        },
        Family {
            base: "fiducia_raft_replication_match_index",
            help: "Highest log index the leader knows the peer has stored.",
            kind: "gauge",
            samples: replication_match,
        },
    ]
}

/// Build per-operation families from `/v1/observe/metrics`.
fn op_families(metrics: &Value) -> Vec<Family> {
    let mut requests = Vec::new();
    let mut errors = Vec::new();
    let mut histogram = Vec::new();

    let mut ops: Vec<&Value> = metrics["operations"]
        .as_array()
        .map(|a| a.iter().collect())
        .unwrap_or_default();
    ops.sort_by(|a, b| {
        a["op"]
            .as_str()
            .unwrap_or("")
            .cmp(b["op"].as_str().unwrap_or(""))
    });

    for op in ops {
        let name = op["op"].as_str().unwrap_or("").to_string();
        let count = op["count"].as_u64().unwrap_or(0);
        let op_errors = op["errors"].as_u64().unwrap_or(0);
        let avg_ms = op["avg_ms"].as_f64().unwrap_or(0.0);
        let op_label = || vec![("op", name.clone())];

        requests.push(Sample::labeled(op_label(), count.to_string()));
        errors.push(Sample::labeled(op_label(), op_errors.to_string()));

        // Buckets are already cumulative in the node's snapshot; the null-`le_ms`
        // bucket is `+Inf` and its count equals the op's total count.
        if let Some(buckets) = op["buckets"].as_array() {
            for bucket in buckets {
                let le = match bucket["le_ms"].as_f64() {
                    Some(bound) => format_number(bound),
                    None => "+Inf".to_string(),
                };
                histogram.push(Sample {
                    suffix: "_bucket",
                    labels: vec![("op", name.clone()), ("le", le)],
                    value: u64_value(bucket["count"].as_u64()),
                });
            }
        }
        histogram.push(Sample {
            suffix: "_sum",
            labels: op_label(),
            value: format_number(avg_ms * count as f64),
        });
        histogram.push(Sample {
            suffix: "_count",
            labels: op_label(),
            value: count.to_string(),
        });
    }

    vec![
        Family {
            base: "fiducia_op_requests_total",
            help: "Operations recorded by the node, by op.",
            kind: "counter",
            samples: requests,
        },
        Family {
            base: "fiducia_op_errors_total",
            help: "Operations the node could not satisfy locally (errors), by op.",
            kind: "counter",
            samples: errors,
        },
        Family {
            base: "fiducia_op_latency_ms",
            help: "Operation latency histogram; buckets are exact cumulative counts, _sum is approximate (avg_ms * count).",
            kind: "histogram",
            samples: histogram,
        },
    ]
}

// ---------------------------------------------------------------------------
// Brain-mode translation.
// ---------------------------------------------------------------------------

/// Translate the brain's `/v1/status` rollup into brain-mode families.
fn brain_families(status: &Value) -> Vec<Family> {
    let mut families = Vec::new();
    let cluster = &status["brain_cluster"];

    families.push(Family::scalar(
        "fiducia_brain_up",
        "1 if the sidecar reached the brain's status API this scrape.",
        "gauge",
        "1".to_string(),
    ));
    families.push(Family::scalar(
        "fiducia_brain_is_leader",
        "1 if the reporting brain member currently drives reconciliation.",
        "gauge",
        bool_value(cluster["is_leader"].as_bool().unwrap_or(false)),
    ));
    families.push(Family::scalar(
        "fiducia_brain_available",
        "1 if the brain control plane is available.",
        "gauge",
        bool_value(cluster["available"].as_bool().unwrap_or(false)),
    ));
    families.push(Family::scalar(
        "fiducia_brain_ha_configured",
        "1 if the brain is configured with a quorum of members (HA).",
        "gauge",
        bool_value(cluster["ha_configured"].as_bool().unwrap_or(false)),
    ));
    families.push(Family::scalar(
        "fiducia_placement_generation",
        "Monotonic placement-map generation the brain has produced.",
        "gauge",
        u64_value(cluster["placement_generation"].as_u64()),
    ));

    // Node-health rollup: one series per health state the brain reports, keyed by
    // the field names it uses (`topology.nodes_by_health`).
    let mut health = Vec::new();
    if let Some(map) = status["topology"]["nodes_by_health"].as_object() {
        let mut keys: Vec<&String> = map.keys().collect();
        keys.sort();
        for key in keys {
            health.push(Sample::labeled(
                vec![("health", key.clone())],
                u64_value(map[key].as_u64()),
            ));
        }
    }
    families.push(Family {
        base: "fiducia_brain_nodes_by_health",
        help: "Known data-plane nodes by brain-assessed health.",
        kind: "gauge",
        samples: health,
    });

    let placement = &status["placement"];
    families.push(Family::scalar(
        "fiducia_placement_unplaced_shards",
        "Shards with no placement assignment.",
        "gauge",
        u64_value(placement["unplaced_shards"].as_u64()),
    ));
    families.push(Family::scalar(
        "fiducia_placement_under_replicated_shards",
        "Shards placed below the replication factor.",
        "gauge",
        u64_value(placement["under_replicated_shards"].as_u64()),
    ));
    families.push(Family::scalar(
        "fiducia_placement_leaderless_shards",
        "Shards with no preferred leader.",
        "gauge",
        u64_value(placement["leaderless_shards"].as_u64()),
    ));
    families.push(Family::scalar(
        "fiducia_placement_shards_with_unhealthy_replicas",
        "Shards with at least one replica on a non-healthy node.",
        "gauge",
        u64_value(placement["shards_with_unhealthy_replicas"].as_u64()),
    ));

    families
}

// ---------------------------------------------------------------------------
// Value / label formatting helpers.
// ---------------------------------------------------------------------------

fn bool_value(b: bool) -> String {
    if b {
        "1".to_string()
    } else {
        "0".to_string()
    }
}

fn u64_value(v: Option<u64>) -> String {
    v.unwrap_or(0).to_string()
}

fn array_len_value(v: &Value) -> String {
    v.as_array().map(|a| a.len()).unwrap_or(0).to_string()
}

fn shard_label(row: &Value) -> (&'static str, String) {
    ("shard", row["shard_id"].as_u64().unwrap_or(0).to_string())
}

/// Render a float minimally: integral values lose the `.0` (so histogram bounds
/// read `le="1"`, not `le="1.0"`); non-integral values keep their decimals.
fn format_number(v: f64) -> String {
    if v.is_finite() && v.fract() == 0.0 && v.abs() < 1e15 {
        (v as i64).to_string()
    } else {
        v.to_string()
    }
}

/// Escape a Prometheus label value: backslash, double-quote, and newline.
fn escape_label(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for c in value.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            _ => out.push(c),
        }
    }
    out
}

/// Comments cannot span lines; collapse any newlines in a failure detail.
fn sanitize_comment(value: &str) -> String {
    value.replace(['\n', '\r'], " ")
}

// ---------------------------------------------------------------------------
// Tests.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::time::Duration;

    fn consts(region: Option<&str>) -> Vec<(&'static str, String)> {
        ConstLabels {
            node_id: "node-a".to_string(),
            region: region.map(str::to_string),
        }
        .pairs()
    }

    fn render(families: &[Family], consts: &[(&'static str, String)]) -> String {
        let mut out = String::new();
        for family in families {
            render_family(&mut out, consts, family);
        }
        out
    }

    fn leader_shard() -> Value {
        json!({
            "shard_id": 0,
            "role": "leader",
            "term": 7,
            "leader_id": "node-a",
            "commit_index": 100,
            "last_applied": 99,
            "last_log_index": 101,
            "snapshot_index": 50,
            "retained_log_entries": 51,
            "storage_healthy": true,
            "healthy_replicas": 3,
            "has_quorum": true,
            "replication": [
                { "peer": "node-c", "match_index": 90, "lag": 11, "in_flight": true },
                { "peer": "node-b", "match_index": 101, "lag": 0, "in_flight": false }
            ],
            "metrics": {
                "append_rtt_ms_last": 4,
                "quorum_rtt_ms_last": 9,
                "follower_lag_max": 11,
                "leader_transfer_count": 2
            }
        })
    }

    fn follower_shard() -> Value {
        json!({
            "shard_id": 1,
            "role": "follower",
            "term": 7,
            "leader_id": "node-a",
            "commit_index": 80,
            "last_applied": 80,
            "last_log_index": 80,
            "snapshot_index": 40,
            "retained_log_entries": 40,
            "storage_healthy": true,
            "healthy_replicas": 0,
            "has_quorum": false,
            "metrics": {
                "append_rtt_ms_last": null,
                "quorum_rtt_ms_last": null,
                "follower_lag_max": 0,
                "leader_transfer_count": 0
            }
        })
    }

    fn faulted_shard() -> Value {
        json!({
            "shard_id": 2,
            "role": "leader",
            "term": 5,
            "leader_id": "node-a",
            "commit_index": 10,
            "last_applied": 10,
            "last_log_index": 10,
            "snapshot_index": 0,
            "retained_log_entries": 10,
            "storage_healthy": false,
            "storage_error": "disk write failed",
            "healthy_replicas": 1,
            "has_quorum": false,
            "replication": [],
            "metrics": {
                "append_rtt_ms_last": 3,
                "quorum_rtt_ms_last": null,
                "follower_lag_max": 7,
                "leader_transfer_count": 1
            }
        })
    }

    fn shards_doc(rows: Vec<Value>) -> Value {
        json!({
            "node_id": "node-a",
            "shard_count": rows.len(),
            "leader_count": 2,
            "follower_count": 1,
            "quorum": {
                "leaderless_shards": [],
                "at_risk_led_shards": [2],
                "all_led_shards_have_quorum": false,
                "storage_faulted_shards": [2],
                "unresponsive_shards": [],
                "status_complete": true
            },
            "shards": rows
        })
    }

    fn metrics_doc() -> Value {
        json!({
            "operations": [
                {
                    "op": "kv.put",
                    "count": 3,
                    "errors": 1,
                    "avg_ms": 2.0,
                    "max_ms": 9000.0,
                    "buckets": [
                        { "le_ms": 1.0, "count": 1 },
                        { "le_ms": 5.0, "count": 2 },
                        { "le_ms": 25.0, "count": 2 },
                        { "le_ms": 100.0, "count": 2 },
                        { "le_ms": 500.0, "count": 2 },
                        { "le_ms": 2000.0, "count": 2 },
                        { "le_ms": null, "count": 3 }
                    ]
                },
                {
                    "op": "lock.acquire",
                    "count": 1,
                    "errors": 0,
                    "avg_ms": 0.5,
                    "max_ms": 0.5,
                    "buckets": [
                        { "le_ms": 1.0, "count": 1 },
                        { "le_ms": 5.0, "count": 1 },
                        { "le_ms": 25.0, "count": 1 },
                        { "le_ms": 100.0, "count": 1 },
                        { "le_ms": 500.0, "count": 1 },
                        { "le_ms": 2000.0, "count": 1 },
                        { "le_ms": null, "count": 1 }
                    ]
                }
            ]
        })
    }

    fn ready_doc() -> Value {
        json!({
            "status": "ok",
            "service": "fiducia-node",
            "all_shards_running": true,
            "unresponsive_shards": [],
            "storage_faulted_shards": []
        })
    }

    #[test]
    fn leader_shard_emits_replication_and_rtt_with_exact_lines() {
        let doc = shards_doc(vec![leader_shard()]);
        let families = node_families(&doc, &json!({ "operations": [] }), &ready_doc());
        let out = render(&families, &consts(Some("us-east-1")));

        assert!(out.contains(
            "fiducia_raft_term{node_id=\"node-a\",region=\"us-east-1\",shard=\"0\"} 7\n"
        ));
        assert!(out.contains(
            "fiducia_raft_is_leader{node_id=\"node-a\",region=\"us-east-1\",shard=\"0\"} 1\n"
        ));
        assert!(out.contains(
            "fiducia_raft_has_quorum{node_id=\"node-a\",region=\"us-east-1\",shard=\"0\"} 1\n"
        ));
        assert!(out.contains(
            "fiducia_raft_append_rtt_ms{node_id=\"node-a\",region=\"us-east-1\",shard=\"0\"} 4\n"
        ));
        // Peers sorted: node-b (lag 0) before node-c (lag 11).
        assert!(out.contains(
            "fiducia_raft_replication_lag{node_id=\"node-a\",region=\"us-east-1\",shard=\"0\",peer=\"node-b\"} 0\n"
        ));
        assert!(out.contains(
            "fiducia_raft_replication_lag{node_id=\"node-a\",region=\"us-east-1\",shard=\"0\",peer=\"node-c\"} 11\n"
        ));
        assert!(out.contains(
            "fiducia_raft_replication_in_flight{node_id=\"node-a\",region=\"us-east-1\",shard=\"0\",peer=\"node-c\"} 1\n"
        ));
        assert!(out.contains(
            "fiducia_raft_leader_transfers_total{node_id=\"node-a\",region=\"us-east-1\",shard=\"0\"} 2\n"
        ));
        // HELP/TYPE appear exactly once per family.
        assert_eq!(out.matches("# TYPE fiducia_raft_term gauge\n").count(), 1);
        assert_eq!(
            out.matches("# TYPE fiducia_raft_leader_transfers_total counter\n")
                .count(),
            1
        );
    }

    #[test]
    fn follower_shard_omits_leader_only_series() {
        let doc = shards_doc(vec![follower_shard()]);
        let families = node_families(&doc, &json!({ "operations": [] }), &ready_doc());
        let out = render(&families, &consts(None));

        assert!(out.contains("fiducia_raft_is_leader{node_id=\"node-a\",shard=\"1\"} 0\n"));
        assert!(out.contains("fiducia_raft_has_quorum{node_id=\"node-a\",shard=\"1\"} 0\n"));
        // No region label when region is unset.
        assert!(!out.contains("region="));
        // Follower: no replication series and no leader-observed RTT.
        assert!(!out.contains("fiducia_raft_replication_lag"));
        assert!(!out.contains("fiducia_raft_append_rtt_ms"));
        assert!(!out.contains("fiducia_raft_quorum_rtt_ms"));
        // follower_lag / leader_transfers still present (always reported).
        assert!(out.contains("fiducia_raft_follower_lag_max{node_id=\"node-a\",shard=\"1\"} 0\n"));
    }

    #[test]
    fn storage_faulted_shard_reports_unhealthy_and_rolls_up() {
        let doc = shards_doc(vec![faulted_shard()]);
        let families = node_families(&doc, &json!({ "operations": [] }), &ready_doc());
        let out = render(&families, &consts(None));

        assert!(out.contains("fiducia_raft_storage_healthy{node_id=\"node-a\",shard=\"2\"} 0\n"));
        assert!(out.contains("fiducia_quorum_storage_faulted_shards{node_id=\"node-a\"} 1\n"));
        assert!(out.contains("fiducia_quorum_at_risk_led_shards{node_id=\"node-a\"} 1\n"));
        // A faulted leader with an empty replication array emits no peer series.
        assert!(!out.contains("fiducia_raft_replication_lag"));
        // quorum_rtt is null here and must be omitted even though append_rtt is set.
        assert!(out.contains("fiducia_raft_append_rtt_ms{node_id=\"node-a\",shard=\"2\"} 3\n"));
        assert!(!out.contains("fiducia_raft_quorum_rtt_ms"));
    }

    #[test]
    fn node_level_and_ready_gauges_translate() {
        let doc = shards_doc(vec![leader_shard(), follower_shard()]);
        let families = node_families(&doc, &json!({ "operations": [] }), &ready_doc());
        let out = render(&families, &consts(None));

        assert!(out.contains("fiducia_node_up{node_id=\"node-a\"} 1\n"));
        assert!(out.contains("fiducia_node_ready{node_id=\"node-a\"} 1\n"));
        assert!(out.contains("fiducia_node_all_shards_running{node_id=\"node-a\"} 1\n"));
        assert!(out.contains("fiducia_node_shards{node_id=\"node-a\"} 2\n"));
        assert!(out.contains("fiducia_node_leader_count{node_id=\"node-a\"} 2\n"));
        assert!(out.contains("fiducia_node_follower_count{node_id=\"node-a\"} 1\n"));
        assert!(out.contains("fiducia_quorum_status_complete{node_id=\"node-a\"} 1\n"));
    }

    #[test]
    fn ready_gauge_is_zero_when_node_reports_unavailable() {
        let doc = shards_doc(vec![faulted_shard()]);
        let readyz = json!({
            "status": "unavailable",
            "all_shards_running": false,
            "unresponsive_shards": [],
            "storage_faulted_shards": [2]
        });
        let families = node_families(&doc, &json!({ "operations": [] }), &readyz);
        let out = render(&families, &consts(None));
        assert!(out.contains("fiducia_node_ready{node_id=\"node-a\"} 0\n"));
        assert!(out.contains("fiducia_node_all_shards_running{node_id=\"node-a\"} 0\n"));
    }

    #[test]
    fn op_metrics_translate_all_buckets_including_inf() {
        let families = op_families(&metrics_doc());
        let out = render(&families, &consts(None));

        // Counters.
        assert!(out.contains("fiducia_op_requests_total{node_id=\"node-a\",op=\"kv.put\"} 3\n"));
        assert!(out.contains("fiducia_op_errors_total{node_id=\"node-a\",op=\"kv.put\"} 1\n"));
        assert!(out.contains("fiducia_op_errors_total{node_id=\"node-a\",op=\"lock.acquire\"} 0\n"));

        // Histogram buckets with integer `le` and the +Inf overflow.
        assert!(out.contains(
            "fiducia_op_latency_ms_bucket{node_id=\"node-a\",op=\"kv.put\",le=\"1\"} 1\n"
        ));
        assert!(out.contains(
            "fiducia_op_latency_ms_bucket{node_id=\"node-a\",op=\"kv.put\",le=\"5\"} 2\n"
        ));
        assert!(out.contains(
            "fiducia_op_latency_ms_bucket{node_id=\"node-a\",op=\"kv.put\",le=\"2000\"} 2\n"
        ));
        assert!(out.contains(
            "fiducia_op_latency_ms_bucket{node_id=\"node-a\",op=\"kv.put\",le=\"+Inf\"} 3\n"
        ));
        // _sum = avg_ms * count = 2.0 * 3 = 6; _count = 3.
        assert!(out.contains("fiducia_op_latency_ms_sum{node_id=\"node-a\",op=\"kv.put\"} 6\n"));
        assert!(out.contains("fiducia_op_latency_ms_count{node_id=\"node-a\",op=\"kv.put\"} 3\n"));

        // One HELP/TYPE for the whole histogram family across both ops.
        assert_eq!(
            out.matches("# TYPE fiducia_op_latency_ms histogram\n")
                .count(),
            1
        );
        // Ops are emitted in sorted order.
        let put = out.find("op=\"kv.put\"").unwrap();
        let lock = out.find("op=\"lock.acquire\"").unwrap();
        assert!(put < lock);
    }

    #[test]
    fn label_values_are_escaped() {
        // A region with a backslash, a quote, and a newline must be escaped.
        let doc = shards_doc(vec![leader_shard()]);
        let families = node_families(&doc, &json!({ "operations": [] }), &ready_doc());
        let out = render(&families, &consts(Some("a\\b\"c\nd")));
        assert!(out.contains("fiducia_node_up{node_id=\"node-a\",region=\"a\\\\b\\\"c\\nd\"} 1\n"));
    }

    fn brain_doc() -> Value {
        json!({
            "service": "fiducia-brain",
            "ready": true,
            "topology": {
                "nodes_by_health": { "healthy": 3, "suspect": 1 }
            },
            "placement": {
                "placed_shards": 6,
                "unplaced_shards": 0,
                "under_replicated_shards": 2,
                "leaderless_shards": 1,
                "shards_with_unhealthy_replicas": 1
            },
            "brain_cluster": {
                "ha_configured": true,
                "available": true,
                "placement_generation": 42,
                "is_leader": true,
                "leader": "http://brain-0:8095"
            }
        })
    }

    #[test]
    fn brain_status_translates_exact_lines() {
        let families = brain_families(&brain_doc());
        let out = render(&families, &consts(None));

        assert!(out.contains("fiducia_brain_up{node_id=\"node-a\"} 1\n"));
        assert!(out.contains("fiducia_brain_is_leader{node_id=\"node-a\"} 1\n"));
        assert!(out.contains("fiducia_brain_available{node_id=\"node-a\"} 1\n"));
        assert!(out.contains("fiducia_brain_ha_configured{node_id=\"node-a\"} 1\n"));
        assert!(out.contains("fiducia_placement_generation{node_id=\"node-a\"} 42\n"));
        assert!(out
            .contains("fiducia_brain_nodes_by_health{node_id=\"node-a\",health=\"healthy\"} 3\n"));
        assert!(out
            .contains("fiducia_brain_nodes_by_health{node_id=\"node-a\",health=\"suspect\"} 1\n"));
        assert!(out.contains("fiducia_placement_unplaced_shards{node_id=\"node-a\"} 0\n"));
        assert!(out.contains("fiducia_placement_under_replicated_shards{node_id=\"node-a\"} 2\n"));
        assert!(out.contains("fiducia_placement_leaderless_shards{node_id=\"node-a\"} 1\n"));
        assert!(out
            .contains("fiducia_placement_shards_with_unhealthy_replicas{node_id=\"node-a\"} 1\n"));
    }

    // -- Error paths: never panic, endpoint content shows scrape down. --------

    fn exporter_at(
        target: Target,
        node_url: String,
        brain_url: String,
        timeout_ms: u64,
    ) -> Exporter {
        Exporter {
            target,
            node_url,
            brain_url,
            client: reqwest::Client::builder()
                .timeout(Duration::from_millis(timeout_ms))
                .build()
                .expect("test client"),
            secret: Some("test-secret".to_string()),
            labels: ConstLabels {
                node_id: "node-a".to_string(),
                region: None,
            },
        }
    }

    async fn closed_addr() -> std::net::SocketAddr {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener); // free the port so connections are refused
        addr
    }

    async fn serve(app: axum::Router) -> std::net::SocketAddr {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        addr
    }

    #[tokio::test]
    async fn node_scrape_connection_refused_reports_scrape_down() {
        let addr = closed_addr().await;
        let exporter = exporter_at(Target::Node, format!("http://{addr}"), String::new(), 500);
        let out = exporter.render().await;
        assert!(out.contains("fiducia_sidecar_scrape_up{node_id=\"node-a\",target=\"node\"} 0\n"));
        assert!(out.contains("# node scrape failed (unreachable)\n"));
        assert!(!out.contains("fiducia_node_up"));
    }

    #[tokio::test]
    async fn node_scrape_bad_status_reports_scrape_down() {
        let app = axum::Router::new().route(
            "/v1/observe/shards",
            axum::routing::get(|| async { (axum::http::StatusCode::NOT_FOUND, "nope") }),
        );
        let addr = serve(app).await;
        let exporter = exporter_at(Target::Node, format!("http://{addr}"), String::new(), 1000);
        let out = exporter.render().await;
        assert!(out.contains("fiducia_sidecar_scrape_up{node_id=\"node-a\",target=\"node\"} 0\n"));
        assert!(out.contains("# node scrape failed (bad status): HTTP 404\n"));
    }

    #[tokio::test]
    async fn node_scrape_timeout_reports_scrape_down() {
        let app = axum::Router::new().route(
            "/v1/observe/shards",
            axum::routing::get(|| async {
                tokio::time::sleep(Duration::from_millis(500)).await;
                axum::Json(json!({}))
            }),
        );
        let addr = serve(app).await;
        let exporter = exporter_at(Target::Node, format!("http://{addr}"), String::new(), 50);
        let out = exporter.render().await;
        assert!(out.contains("fiducia_sidecar_scrape_up{node_id=\"node-a\",target=\"node\"} 0\n"));
        assert!(out.contains("# node scrape failed (timeout)\n"));
    }

    #[tokio::test]
    async fn brain_scrape_connection_refused_reports_scrape_down() {
        let addr = closed_addr().await;
        let exporter = exporter_at(Target::Brain, String::new(), format!("http://{addr}"), 500);
        let out = exporter.render().await;
        assert!(out.contains("fiducia_sidecar_scrape_up{node_id=\"node-a\",target=\"brain\"} 0\n"));
        assert!(out.contains("# brain scrape failed (unreachable)\n"));
        assert!(!out.contains("fiducia_brain_up"));
    }

    #[tokio::test]
    async fn brain_mode_render_succeeds_against_a_mock_brain() {
        let app = axum::Router::new().route(
            "/v1/status",
            axum::routing::get(|| async { axum::Json(brain_doc()) }),
        );
        let addr = serve(app).await;
        let exporter = exporter_at(Target::Brain, String::new(), format!("http://{addr}"), 1000);
        let out = exporter.render().await;
        assert!(out.contains("fiducia_sidecar_scrape_up{node_id=\"node-a\",target=\"brain\"} 1\n"));
        assert!(out.contains("fiducia_brain_up{node_id=\"node-a\"} 1\n"));
        assert!(out.contains("fiducia_placement_generation{node_id=\"node-a\"} 42\n"));
    }
}

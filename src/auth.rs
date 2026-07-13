//! Shared trusted-hop authentication for the sidecar's outbound `/v1` calls.
//!
//! The sidecar talks to guarded control planes — the local node (`GET /v1/status`,
//! `GET /v1/observe/*`, `GET /readyz`) and the brain (`POST /v1/nodes/{id}/heartbeat`,
//! `GET /v1/status`) — all of which require the cluster trusted-hop secret in the
//! `x-fiducia-internal-auth` header when it is configured. The heartbeat bridge and
//! the Prometheus exporter share this one definition so neither can drift into
//! sending unauthenticated calls (the gap the old collector metric-scrape had: it
//! hit the node with no auth header at all).
//!
//! The secret is only ever *presented* outbound, never compared in-process, so
//! there is no timing-comparison surface here.

/// Header carrying the cluster trusted-hop secret on outbound calls.
pub const INTERNAL_AUTH_HEADER: &str = "x-fiducia-internal-auth";

/// The cluster trusted-hop secret, read from `FIDUCIA_INTERNAL_SECRET` exactly
/// once. Blank/whitespace-only values are treated as unset.
pub fn internal_secret() -> Option<&'static str> {
    static SECRET: std::sync::OnceLock<Option<String>> = std::sync::OnceLock::new();
    SECRET
        .get_or_init(|| {
            std::env::var("FIDUCIA_INTERNAL_SECRET")
                .ok()
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
        })
        .as_deref()
}

/// Attach the trusted-hop header using the process-wide secret when set.
pub fn attach(builder: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
    attach_with(builder, internal_secret())
}

/// Attach the trusted-hop header with an explicitly supplied secret. Used by the
/// exporter so the secret can be injected directly in tests without mutating
/// process environment (which a `OnceLock` would then cache across the suite).
pub fn attach_with(
    builder: reqwest::RequestBuilder,
    secret: Option<&str>,
) -> reqwest::RequestBuilder {
    match secret {
        Some(secret) => builder.header(INTERNAL_AUTH_HEADER, secret),
        None => builder,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn attach_with_sets_the_header_when_a_secret_is_present() {
        let client = reqwest::Client::new();
        let request = attach_with(client.get("http://node.local/v1/status"), Some("hunter2"))
            .build()
            .expect("request builds");
        assert_eq!(
            request
                .headers()
                .get(INTERNAL_AUTH_HEADER)
                .and_then(|v| v.to_str().ok()),
            Some("hunter2")
        );
    }

    #[test]
    fn attach_with_omits_the_header_when_no_secret() {
        let client = reqwest::Client::new();
        let request = attach_with(client.get("http://node.local/v1/status"), None)
            .build()
            .expect("request builds");
        assert!(request.headers().get(INTERNAL_AUTH_HEADER).is_none());
    }
}

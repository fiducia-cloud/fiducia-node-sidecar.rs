//! Pure listener-address validation, separate from service startup effects.

use std::net::SocketAddr;

/// Loopback by default. `0.0.0.0`/`::` stay rejected; a unicast non-loopback
/// bind requires `ORES_OTEL_SIDECAR_ALLOW_NON_LOOPBACK=1`.
pub(crate) fn listen_addr(
    bind: Option<&str>,
    port: Option<&str>,
    allow_non_loopback: bool,
) -> Result<SocketAddr, Box<dyn std::error::Error>> {
    let raw = match bind.map(str::trim).filter(|value| !value.is_empty()) {
        Some(value) => value.to_string(),
        None => {
            let port: u16 = port
                .and_then(|value| value.parse().ok())
                .filter(|&value| value > 0)
                .unwrap_or(8091);
            format!("127.0.0.1:{port}")
        }
    };
    let addr: SocketAddr = raw
        .parse()
        .map_err(|_| std::io::Error::other(format!("invalid sidecar bind {raw:?}")))?;
    if addr.ip().is_unspecified() || (!addr.ip().is_loopback() && !allow_non_loopback) {
        return Err(std::io::Error::other(format!(
            "refusing non-loopback sidecar bind {raw:?}; set ORES_OTEL_SIDECAR_ALLOW_NON_LOOPBACK=1 to override"
        ))
        .into());
    }
    Ok(addr)
}

#[cfg(test)]
mod tests {
    use super::listen_addr;
    use std::net::{IpAddr, Ipv4Addr};

    #[test]
    fn default_and_port_bind_loopback() {
        let addr = listen_addr(None, None, false).unwrap();
        assert_eq!(addr.ip(), IpAddr::V4(Ipv4Addr::LOCALHOST));
        assert_eq!(addr.port(), 8091);
        let addr = listen_addr(None, Some("9091"), false).unwrap();
        assert_eq!(addr.port(), 9091);
        assert!(addr.ip().is_loopback());
    }

    #[test]
    fn unspecified_is_rejected_even_with_override() {
        for bind in ["0.0.0.0:8091", "[::]:8091"] {
            for allow in [false, true] {
                assert!(listen_addr(Some(bind), None, allow).is_err());
            }
        }
    }

    #[test]
    fn public_bind_requires_override() {
        assert!(listen_addr(Some("1.1.1.1:8091"), None, false).is_err());
        assert!(listen_addr(Some("1.1.1.1:8091"), None, true).is_ok());
    }

    #[test]
    fn explicit_loopback_and_bind_precedence_are_preserved() {
        for bind in ["127.0.0.1:9091", "[::1]:9091"] {
            let addr = listen_addr(Some(bind), Some("8091"), false).unwrap();
            assert!(addr.ip().is_loopback());
            assert_eq!(addr.port(), 9091);
        }
    }

    #[test]
    fn invalid_explicit_bind_never_falls_back_to_the_port() {
        for bind in ["localhost:8091", "not-an-address", "127.0.0.1:99999"] {
            for allow in [false, true] {
                assert!(listen_addr(Some(bind), Some("8091"), allow).is_err());
            }
        }
    }

    #[test]
    fn empty_bind_and_invalid_port_retain_loopback_defaults() {
        for port in [None, Some("0"), Some("-1"), Some("bad"), Some("65536")] {
            let addr = listen_addr(Some("  "), port, false).unwrap();
            assert_eq!(
                addr,
                "127.0.0.1:8091".parse::<std::net::SocketAddr>().unwrap()
            );
        }
    }
}

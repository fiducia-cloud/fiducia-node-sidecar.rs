use std::process::{Command, Output};

fn run_with_internal_secret(value: Option<&str>) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_fiducia-node-sidecar"));
    command
        .env_remove("OTEL_EXPORTER_OTLP_ENDPOINT")
        .env("FIDUCIA_LOG_FORMAT", "text")
        .env("NO_COLOR", "1")
        .env_remove("RUST_LOG");

    match value {
        Some(value) => {
            command.env("FIDUCIA_INTERNAL_SECRET", value);
        }
        None => {
            command.env_remove("FIDUCIA_INTERNAL_SECRET");
        }
    }

    command.output().expect("run fiducia-node-sidecar")
}

#[test]
fn missing_or_blank_trusted_hop_secret_aborts_startup() {
    for value in [None, Some(""), Some(" \t\n ")] {
        let output = run_with_internal_secret(value);
        assert!(
            !output.status.success(),
            "sidecar unexpectedly started with secret={value:?}"
        );

        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        let combined = format!("{stdout}\n{stderr}");
        assert!(
            combined.contains("FIDUCIA_INTERNAL_SECRET must be configured"),
            "startup failure must identify the missing configuration key without ambiguity:\n{combined}"
        );
        assert!(
            !combined.contains("listening on"),
            "the HTTP listener must not start before trusted-hop authentication is configured:\n{combined}"
        );
    }
}

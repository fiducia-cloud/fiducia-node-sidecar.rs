# syntax=docker/dockerfile:1
# Multi-stage build for fiducia-node-sidecar (no shared-crate dependency).
FROM rust:1-slim-bookworm AS build
WORKDIR /build/fiducia-node-sidecar.rs
COPY . .
RUN cargo build --release && strip target/release/fiducia-node-sidecar

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && useradd --uid 10001 --user-group --home-dir /nonexistent --shell /usr/sbin/nologin fiducia
COPY --from=build --chown=10001:10001 /build/fiducia-node-sidecar.rs/target/release/fiducia-node-sidecar /usr/local/bin/fiducia-node-sidecar
EXPOSE 8091
USER 10001:10001
ENTRYPOINT ["/usr/local/bin/fiducia-node-sidecar"]

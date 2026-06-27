# syntax=docker/dockerfile:1
# Multi-stage build for fiducia-node-sidecar.
FROM rust:1-slim-bookworm AS build
RUN apt-get update \
    && apt-get install -y --no-install-recommends git ca-certificates
WORKDIR /build
ARG INTERFACES_REF=main
RUN git clone --depth 1 --branch "$INTERFACES_REF" \
    https://github.com/fiducia-cloud/fiducia-interfaces.git fiducia-interfaces
COPY . fiducia-node-sidecar.rs
WORKDIR /build/fiducia-node-sidecar.rs
RUN cargo build --release && strip target/release/fiducia-node-sidecar

FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=build --chown=65532:65532 /build/fiducia-node-sidecar.rs/target/release/fiducia-node-sidecar /usr/local/bin/fiducia-node-sidecar
EXPOSE 8091
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/fiducia-node-sidecar"]

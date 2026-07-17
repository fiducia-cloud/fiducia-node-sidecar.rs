# syntax=docker/dockerfile:1
# Multi-stage build for fiducia-node-sidecar.
FROM rust:1.97.0-slim-bookworm@sha256:6d220bf85c74e842a79da63997af8d2e74455c0b8847d8bb3a5888572334991d AS build
RUN apt-get update \
    && apt-get install -y --no-install-recommends git ca-certificates
WORKDIR /build
# Immutable cross-repository input. Bump this SHA together with the CI checkout.
ARG INTERFACES_SHA=487e470c45ab5851e8f6f3b1dc048fe067fbf408
RUN git init fiducia-interfaces \
    && git -C fiducia-interfaces remote add origin \
       https://github.com/fiducia-cloud/fiducia-interfaces.git \
    && git -C fiducia-interfaces fetch --depth 1 origin "$INTERFACES_SHA" \
    && git -C fiducia-interfaces checkout --detach FETCH_HEAD \
    && test "$(git -C fiducia-interfaces rev-parse HEAD)" = "$INTERFACES_SHA"
COPY . fiducia-node-sidecar.rs
WORKDIR /build/fiducia-node-sidecar.rs
RUN cargo build --locked --release && strip target/release/fiducia-node-sidecar

FROM gcr.io/distroless/cc-debian12:nonroot@sha256:66aa873a4a14fb164aa01296058efd8253744606d72715e45acface073359faa
COPY --from=build --chown=65532:65532 /build/fiducia-node-sidecar.rs/target/release/fiducia-node-sidecar /usr/local/bin/fiducia-node-sidecar
EXPOSE 8091
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/fiducia-node-sidecar"]

# workflows

GitHub Actions pipelines for fiducia-node-sidecar:

- `ci.yml` — enforce formatting, locked all-target Clippy/tests, and pinned
  cargo-audit on push and pull request.
  The sibling `fiducia-interfaces` checkout is pinned to the same immutable
  commit as the Dockerfile, and dependency-resolving Cargo commands use
  `--locked`.
- `docker.yml` — build and push the service container image on push to `main`,
  using only its immutable commit-SHA tag plus provenance and an SBOM.
  The Dockerfile fetches interfaces by full SHA, checks it out detached, and
  verifies the resulting `HEAD` before compiling with `--locked`; the workflow
  passes that same SHA explicitly.
- `cli-flags.yml` — audits `.cli-flags.toml` with the pinned `flags2env`
  submodule whenever the CLI flag schema, scripts, or submodule change.

This repository contains no kubeconfig or rollout workflow; deployment is
owned by `fiducia-monorepo`.

## Security baseline

Every executable workflow uses explicit least-privilege permissions, immutable
third-party action or container references, non-persisted checkout credentials,
concurrency control, and a job timeout. The main CI workflow validates this
directory with the digest-pinned actionlint container. Environment mutation is
forbidden unless this README documents a repository-specific platform exception.

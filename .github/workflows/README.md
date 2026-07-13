# workflows

GitHub Actions pipelines for fiducia-node-sidecar:

- `ci.yml` — enforce formatting, locked all-target Clippy/tests, and pinned
  cargo-audit on push and pull request.
  The sibling `fiducia-interfaces` checkout is pinned to the same immutable
  commit as the Dockerfile, and dependency-resolving Cargo commands use
  `--locked`.
- `docker.yml` — build and push the service container image on push to `main`.
  The Dockerfile fetches interfaces by full SHA, checks it out detached, and
  verifies the resulting `HEAD` before compiling with `--locked`; the workflow
  passes that same SHA explicitly.
- `deploy-test.yml` — fail-closed TEST rollout: it requires `KUBE_CONFIG_TEST`,
  an existing deployment, and a successful rollout.
- `cli-flags.yml` — audits `.cli-flags.toml` with the pinned `flags2env`
  submodule whenever the CLI flag schema, scripts, or submodule change.

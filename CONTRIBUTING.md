# Contributing to Oxidant

## Build

```sh
cargo build --workspace
cargo test  --workspace
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
```

### Local CI (matches GitHub Actions)

```sh
chmod +x scripts/ci-local.sh .githooks/pre-push
./scripts/ci-local.sh          # full gate suite before a PR
git config core.hooksPath .githooks   # optional: run fmt/clippy/test on git push
```

The `oxidant-cli` binary must be built before workspace tests because
`crates/oxidant-cli/tests/cli_driver_worker.rs` spawns it as a subprocess.
`scripts/ci-local.sh` and CI do this automatically; `cargo test --workspace` alone does not.

The stub workspace builds on Rust 1.72+. The runtime crates (DataFusion/Arrow/tonic) will
require **Rust ≥ 1.80** and **protoc**; their dependencies are stubbed out today and noted
as `TODO(deps)` in each crate's `Cargo.toml`.

## Layout

See [`docs/architecture.md`](docs/architecture.md). Crates live in `crates/oxidant-*`;
benchmarks in `bench/`; the Python helper package in `python/pyoxidant`.

## Ground rules (non-negotiable, from the architecture)

1. **Arrow is the currency between operators.** Don't invent a second in-memory format.
2. **The columnar hot loop stays in `oxidant-loom`.**
3. **One backend, no second runtime.** Execution work lands as Loom operators.
4. **Every claim is measured.** Performance changes ride with a ClickBench/TPC-H number.

## Releasing

**Never cut a release from the GitHub Releases UI or with `gh release create`.**

Releases go through [`.github/workflows/release.yml`](.github/workflows/release.yml) only:

1. Label the PR `patch`, `minor`, or `major` before merging to main (precedence:
   major > minor > patch). Or: Actions -> release -> Run workflow.
2. That opens a `release/vX.Y.Z` PR bumping the workspace version in `Cargo.toml` and
   `Cargo.lock`. Merge it — it has to go green like any other PR.
3. That merge tags the version (annotated, by `github-actions[bot]`) and dispatches
   `binaries.yml` (tarballs, curl installer, Homebrew formula, .deb/.rpm) and
   `oxidant-image.yml` (GHCR).

The bump in step 2 is the whole point. Tagging by hand creates a *lightweight* tag on a
commit whose `Cargo.toml` still holds the previous version, and cargo-dist then refuses to
build anything ("This workspace doesn't have anything for dist to Release!"). That is
exactly how v0.2.3, v0.2.4 and v0.2.5 shipped as public releases with zero assets while
`latest` — which the README's install line follows — pointed at the newest empty one.

Two guards now catch it: `preflight` in `binaries.yml` rejects a lightweight or
version-mismatched tag (and puts the release back into draft), and `verify-release`
refuses to let the pipeline announce a release that is missing assets. To check a live
release the way a user would:

```sh
./scripts/release-healthcheck.sh          # whatever /releases/latest resolves to
./scripts/release-healthcheck.sh v0.2.6   # a specific release
```

`release-healthcheck.yml` runs the same script daily and after every release, and files a
single tracking issue when a documented install path breaks.

## Commit / MR conventions

- Conventional-commit style subjects (`feat(loom): …`, `fix(connect): …`).
- An MR that changes execution must include a benchmark delta.

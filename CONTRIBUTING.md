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
3. **One backend, no second runtime.** Execution work lands as Loom operators — see
   [`docs/HVM_VERDICT.md`](docs/HVM_VERDICT.md) for the removed HVM2 bet and why.
4. **Every claim is measured.** Performance changes ride with a ClickBench/TPC-H number.

## Commit / MR conventions

- Conventional-commit style subjects (`feat(loom): …`, `fix(connect): …`).
- An MR that changes execution must include a benchmark delta.

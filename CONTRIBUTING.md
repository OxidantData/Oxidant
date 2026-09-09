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

## Persistent CI runner prerequisites and resource ownership

The `build-test` and `query-gates` jobs in `.github/workflows/ci.yml` use the
self-hosted `fremont` runner. Its operator must provision sufficient disk space,
Rust, Python 3.10+, Docker, curl, and (for official TPC kits) `gcc`, `g++`, `make`,
`flex`, and `bison`. Missing build tools fail the kit step with the tool name;
These jobs do not install host packages or delete shared SDK/tool-cache directories.
The capacity step reports `df -h /`; it does not reclaim another job's storage.

Workspace tests run through `scripts/ci_minio.py`. This requires a **local Linux
Docker daemon**, a Unix-socket context, and host networking for the MinIO client.
Select a context with `DOCKER_CONTEXT` if necessary; unset ambiguous `DOCKER_HOST`.
The helper records the context name, socket endpoint and daemon ID without
changing global context. It creates a fresh server and `mc` client with a random
owner label and run/attempt/job identity, recording each immutable CID in a
private `$RUNNER_TEMP/oxidant-minio-*` directory. No pre-existing container is
adopted, including a name collision. MinIO publishes an ephemeral **loopback-only**
port; health, bucket initialization and the actual `cargo test --workspace`
consumer use that port, overriding an ambient `OXIDANT_MINIO_ENDPOINT`.

Normal setup/test failure triggers cleanup; an `always()` workflow step retries
cleanup after interruption. Cleanup verifies both CIDs and ownership labels on
the recorded daemon before removal, including container-owned anonymous volumes
(no shared mounts or named volumes are supplied). The primary failure status is
preserved; cleanup-only failure fails the test step, and failed fallback cleanup
also fails its step. Successful repeated cleanup is a no-op. Image caches are
not pruned. The existing image references are unchanged; this is an ownership
repair, not image provenance or supply-chain verification.

Receipts are retained until the Actions runner clears its job temp directory;
failed cleanup prints the receipt path. While that directory exists, an operator
can retry `python3 -B scripts/ci_minio.py cleanup /absolute/receipt/directory`
with the original `GITHUB_REPOSITORY`, `GITHUB_RUN_ID`, `GITHUB_RUN_ATTEMPT` and
`GITHUB_JOB` environment. Missing/malformed CIDs, mismatched labels or changed
daemon identity **refuse removal**, not fall back to names. A daemon failure,
SIGKILL or runner shutdown may leave resources behind. Preserve the receipt
before the runner clears temp and investigate the exact IDs/context manually;
never fix this by deleting matching names or broadly pruning the daemon. A
create failure without a CID cannot establish ownership and requires operator
reconciliation, even if a matching name is visible.

The optional DuckDB oracle downloads/extracts only into a fresh
`$RUNNER_TEMP/oxidant-duckdb.*` directory. Its exact directory is prepended through
`GITHUB_PATH` only after that binary passes `--version`, so the unchanged Rust
oracle resolves it before any global binary. `unzip` must be provisioned on the
host; setup failure retains the existing optional outcome and execute-only
fallback. Private downloads/binaries remain for the job and are left to the
runner's temp lifecycle; `/tmp/duckdb.zip` and `/usr/local/bin` are not modified.

Run the cheap regression suite with:

```sh
python3 -B scripts/tests/test_ci_ownership.py
```

It executes the relevant workflow shell steps and shipped helper with strict
Docker/curl/cargo/unzip/sudo recorders and private filesystem sentinels. It runs
in the existing `rustfmt` job without replacing other gates. These are **offline
command-boundary tests**, not real Docker/MinIO readiness, an actual DuckDB
oracle, Cargo test results, or a completed GitHub Actions run. They require no
Docker daemon, network, downloads, Rust compilation, or third-party Python modules.

## Layout

See [`docs/architecture.md`](docs/architecture.md). Crates live in `crates/oxidant-*`;
benchmarks in `bench/`; the Python helper package in `python/pyoxidant`.

## Ground rules (non-negotiable, from the architecture)

1. **Arrow is the currency between operators.** Don't invent a second in-memory format.
2. **The columnar hot loop stays in `oxidant-loom`.**
3. **One backend, no second runtime.** Execution work lands as Loom operators.
4. **Every claim is measured.** Performance changes ride with a ClickBench/TPC-H number.

## Commit / MR conventions

- Conventional-commit style subjects (`feat(loom): …`, `fix(connect): …`).
- An MR that changes execution must include a benchmark delta.

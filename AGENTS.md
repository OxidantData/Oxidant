# AGENTS.md

## Cursor Cloud specific instructions

Weft is a Rust workspace implementing a drop-in Apache Spark replacement that speaks the
Spark Connect gRPC protocol. There are two runnable products in this repo:

1. **Weft engine** (Rust) — the `weft` binary (`crates/weft-cli`) starts a Spark Connect
   server that real PySpark / Spark SQL clients connect to. This is the core product.
2. **Showcase site** (`site/`) — a React 18 + Vite 5 + TypeScript marketing/benchmark site.

The README's "stubbed deps / does not yet execute queries" note is outdated: the workspace
pulls in real DataFusion/Arrow/tonic and the engine executes SQL end-to-end.

### Toolchains / build notes
- Rust toolchain is pinned by `rust-toolchain.toml` (1.90) and auto-installs via rustup.
- `protoc` is **not** required — `crates/weft-proto/build.rs` compiles the vendored Spark
  Connect protos with the pure-Rust `protox` crate.
- A clean `cargo build --workspace` takes a few minutes the first time.

### Standard commands (see `CONTRIBUTING.md` / `site/README.md`)
- Rust: `cargo build --workspace`, `cargo test --workspace`, `cargo fmt --all -- --check`,
  `cargo clippy --workspace --all-targets -- -D warnings`.
- Bench/coverage gates (CI, runnable locally): `cargo run -p weft-bench -- clickbench --rows 20000`,
  `cargo run -p weft-bench -- clickbench-grpc --rows 20000`, `cargo run -p weft-bench -- tpch`.
- Spark-SQL parity gate: `cargo build -p weft-spark-compat --bin weft-parity` then
  `./target/debug/weft-parity ratchet --baseline parity/baseline.json --out-dir parity`.
- Site: `npm install` then `npm run dev` (serves http://localhost:5174/weft/),
  `npm run typecheck`, `npm run build` — all run from inside `site/`.

### Running the engine + a hello-world query
- Start the server: `./target/debug/weft spark server --port 50051`
  (build first with `cargo build -p weft-cli`). It listens on `sc://0.0.0.0:50051`.
- To drive it with a real client, install the stock PySpark Connect client:
  `pip install "pyspark-client>=4.0"` (pure-Python, no JVM needed), then:
  ```python
  from pyspark.sql import SparkSession
  spark = SparkSession.builder.remote("sc://localhost:50051").getOrCreate()
  spark.sql("SELECT 1 AS hello").show()
  ```
- Engine gotcha (pre-alpha): SQL like `range(5)` returns a column named `range().value`
  rather than Spark's conventional `id`, so `SELECT id FROM range(5)` errors. Use explicit
  `VALUES (...) AS t(...)` tables or aliased projections in smoke tests.

### Distributed mode (optional)
The `weft` binary also has `worker` and `driver` subcommands for a Flight-based
driver/worker cluster (`weft worker --port ...`, `weft driver --workers h:p,... --partial-sql ... --final-sql ...`).
Not needed for the basic single-server flow.

For **Kubernetes / EKS** (Helm: one connect-server + N workers, AWS CLI baked into the
image), see [`docs/distributed-k8s.md`](docs/distributed-k8s.md). Local TPC-H distributed
gate: `cargo run -p weft-bench -- tpch-distributed --sf 0.01 --workers 2`.

### CI gotchas (commit / push / PR)

GitHub Actions gates live in `.github/workflows/ci.yml`. Before pushing, run
`./scripts/ci-local.sh` (or install the optional pre-push hook:
`git config core.hooksPath .githooks`). The following issues have bitten real PRs:

#### `weft-cli` must be built before `cargo test --workspace`

`weft-cli` is a **binary-only** crate (`[[bin]] weft`). `cargo test --workspace` does **not**
build orphan binaries, so `CARGO_BIN_EXE_weft` is unset unless you built it explicitly.

- **Symptom:** `cli_driver_worker_matches_single_node` panics with
  `weft binary not found at …/target/debug/weft`.
- **Fix:** `cargo build -p weft-cli` before tests. CI and `scripts/ci-local.sh` do this.
- **Test location:** the driver/worker subprocess smoke test lives in
  `crates/weft-cli/tests/cli_driver_worker.rs` (not `weft-execution`) so Cargo sets
  `CARGO_BIN_EXE_weft` when the test is built via `cargo test -p weft-cli`.
- **`weft_bin()` fallback:** when the env var is missing, probe (in order)
  `$CARGO_TARGET_DIR/$PROFILE/weft`, `target/$PROFILE/weft`, and
  `target/llvm-cov-target/$PROFILE/weft` (see llvm-cov below).

#### `cargo llvm-cov` uses a separate target directory

The informational `line-coverage` job runs `cargo llvm-cov --workspace --html`, which
re-runs the full test suite under `target/llvm-cov-target/` (not `target/debug/`).

- **Symptom:** same `weft binary not found` failure, but only in the `line-coverage` job
  even when `clippy + test + tpch` passes.
- **Fix (CI):** `cargo build -p weft-cli --target-dir target/llvm-cov-target` before
  `cargo llvm-cov`. Upload artifact from `target/llvm-cov/html` (not `coverage/`).
- **Flag gotcha:** do **not** pass `--output-path coverage/` together with `--html` —
  `cargo-llvm-cov` rejects incompatible flags. Use `--html` alone.
- **Job is non-blocking** (`continue-on-error: true`) but should still be kept green for
  trending artifacts.

#### `tpch-distributed` auto-splitter SQL must re-parse on workers

`cargo run -p weft-bench -- tpch-distributed --sf 0.01 --workers 2` is a **blocking** CI
gate. The auto-splitter (`weft_execution::plan::plan_distributed`) unparses logical plans
to stage SQL via DataFusion's `Unparser`, then workers re-parse that SQL under the
Databricks dialect. Some Unparser output is **invalid on round-trip**:

| Unparser output | Problem | Sanitized to |
|----------------|---------|--------------|
| `shipping."volume"` | dot + double-quoted column | `shipping.volume` |
| `"part".p_partkey` | dot access on quoted table (reserved name) | `` `part`.p_partkey `` |

- **Symptom:** Q7/Q8 `ParserError: Expected identifier after '.'`; Q9
  `Dot access not supported for non-string expr`.
- **Fix:** `sanitize_generated_sql()` in `crates/weft-execution/src/plan/stage_planner.rs`
  rewrites these patterns before stage SQL is sent to workers.
- **Debug locally:** `WEFT_TPCH_ONLY=Q7 WEFT_TPCH_DEBUG=1 cargo run -p weft-bench -- tpch-distributed --sf 0.01 --workers 2`

#### CI job map (quick reference)

| Job | Blocking? | Key command |
|-----|-----------|-------------|
| rustfmt | yes | `cargo fmt --all -- --check` |
| clippy + test + tpch | yes | `cargo build -p weft-cli` then clippy/test/tpch/tpch-distributed |
| coverage gates | yes | clickbench, clickbench-grpc, correctness |
| Spark SQL parity ratchet | yes | `weft-parity ratchet --baseline parity/baseline.json` |
| line coverage | no (informational) | `cargo llvm-cov --workspace --html` |

## Daily maintenance routine

A Cursor **Scheduled Agent** runs a daily bug / security-vuln / dependency-CVE /
maintenance pass over this repo. It is grounded on a deterministic scan and bounded by
strict guardrails — it opens **draft PRs + one triage issue** and merges nothing.

- **Playbook (the contract):** `.cursor/rules/daily-maintenance.mdc` — procedure, the
  finding→delivery routing table, and the hard guardrails (never push to `main`, one
  concern per PR, ≤5 PRs/day, no exploit detail in public since this repo is public →
  security findings go to a private GitHub Security Advisory).
- **Scan (run it yourself too):** `bash scripts/daily-maintenance.sh` runs the cheap-core
  gates (fmt, clippy, test, `cargo audit`, `cargo deny`, dep-update check) and writes
  machine-readable reports to `target/daily-maintenance/`. It deliberately skips the heavy
  bench/parity gates in `scripts/ci-local.sh`.
- **Env:** `.cursor/environment.json` boots Rust 1.90 and installs `cargo-audit` +
  `cargo-deny`. Policy for the latter is `deny.toml` (starts permissive, tightened over
  time by the daily `chore(deps)` PRs).
- **Scheduling** is configured in the Cursor dashboard (not version-controlled); the
  repo owns *what* runs, the dashboard only fixes *when*.

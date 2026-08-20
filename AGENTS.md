# AGENTS.md

## Start here (maps)

| Doc | Use when |
|-----|----------|
| [docs/README.md](docs/README.md) | Docs index (user guides + internals) |
| [docs/getting-started.md](docs/getting-started.md) | Install/run + first query (UI, CLI, PySpark) |
| [docs/web-ui.md](docs/web-ui.md) | Monitoring UI, SQL editor, notebooks |
| [docs/api.md](docs/api.md) | REST statement API reference |
| [docs/config.md](docs/config.md) | `oxidant.yaml` reference (catalogs, engine, local catalog) |
| [docs/pipelines.md](docs/pipelines.md) | Declarative Kafka → lakehouse table DAG (`oxidant pipeline`) |
| [docs/cli.md](docs/cli.md) | `oxidant sql` CLI reference |
| [docs/mcp.md](docs/mcp.md) | `oxidant mcp` MCP server setup/tools |
| [docs/workers.md](docs/workers.md) | Adding workers (local-cluster / multi-host) |
| [docs/catalogs-glue.md](docs/catalogs-glue.md) | Glue catalog end-to-end |
| [docs/catalogs-lakeformation.md](docs/catalogs-lakeformation.md) | Lake Formation column/row security on Glue tables |
| [docs/streaming.md](docs/streaming.md) | Structured Streaming: Kafka → live Delta tables in Glue, readable as Iceberg |
| [docs/CODEMAP.md](docs/CODEMAP.md) | Crate / bench / site ownership |
| [docs/architecture.md](docs/architecture.md) | Engine design (Loom / Connect) |
| [docs/distributed-ec2.md](docs/distributed-ec2.md) | EC2 ASG data plane (Packer + CFN) |
| [docs/deployment.md](docs/deployment.md) | Self-hosted platform deploy outline |
| [docs/catalogs.md](docs/catalogs.md) | External catalog SPI (Hive / Glue / REST) |
| [docs/runtime-contract.md](docs/runtime-contract.md) | Engine image env contract |
| [docs/databricks-coverage.md](docs/databricks-coverage.md) | Databricks SQL coverage matrix (what works today + owning ticket) |
| [docs/databricks-parity-plan.md](docs/databricks-parity-plan.md) | Databricks parity plan (Glue + Lake Formation epic) |

Deployment options: the free Community AMI on AWS Marketplace (listing in progress) or
`docker pull ghcr.io/oxidantdata/oxidant`; EC2 autoscaling via CloudFormation is
documented in [`docs/distributed-ec2.md`](docs/distributed-ec2.md).

## Cursor Cloud specific instructions

Oxidant is a Rust workspace implementing a drop-in Apache Spark replacement that speaks the
Spark Connect gRPC protocol. The runnable product in this repo is the **Oxidant engine**
(Rust) — the `oxidant` binary (`crates/oxidant-cli`) starts a Spark Connect server that real
PySpark / Spark SQL clients connect to. The marketing/benchmark site lives in a separate
private repo.

The README's "stubbed deps / does not yet execute queries" note is outdated: the workspace
pulls in real DataFusion/Arrow/tonic and the engine executes SQL end-to-end.

### Toolchains / build notes
- Rust toolchain is pinned by `rust-toolchain.toml` (1.90) and auto-installs via rustup.
- `protoc` is **not** required — `crates/oxidant-proto/build.rs` compiles the vendored Spark
  Connect protos with the pure-Rust `protox` crate.
- A clean `cargo build --workspace` takes a few minutes the first time.

### Standard commands (see `CONTRIBUTING.md`)
- Rust: `cargo build --workspace`, `cargo test --workspace`, `cargo fmt --all -- --check`,
  `cargo clippy --workspace --all-targets -- -D warnings`.
- Bench/coverage gates (CI, runnable locally): `cargo run -p oxidant-bench -- clickbench --rows 20000`,
  `cargo run -p oxidant-bench -- clickbench-grpc --rows 20000`,
  `cargo run -p oxidant-bench -- tpch --sf 1` (official `dbgen`; kits via `./bench/tpc/fetch-kits.sh`).
- Spark-SQL parity gate: `cargo build -p oxidant-spark-compat --bin oxidant-parity` then
  `./target/debug/oxidant-parity ratchet --baseline parity/baseline.json --out-dir parity`.

### Running the engine + a hello-world query
- Start the server: `./target/debug/oxidant spark server --port 50051`
  (build first with `cargo build -p oxidant-cli`). It listens on `sc://0.0.0.0:50051`.
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
The `oxidant` binary also has `worker` and `driver` subcommands for a Flight-based
driver/worker cluster (`oxidant worker --port ...`, `oxidant driver --workers h:p,... --partial-sql ... --final-sql ...`).
Not needed for the basic single-server flow.

For **EC2 / CloudFormation + ASG** (Packer AMI, fixed worker count, ASG private-IP
membership — not Route53), see [`docs/distributed-ec2.md`](docs/distributed-ec2.md).
Local TPC-H distributed gate:
`cargo run -p oxidant-bench -- tpch-distributed --sf 1 --workers 2`.

### SF100 / TPC-DS memory gotchas (do not re-learn the hard way)

PR #51 added **auto-size** when `OXIDANT_MEMORY_LIMIT_BYTES` is unset (cgroup/host RAM ×
`OXIDANT_MEMORY_POOL_FRACTION`, shuffle cache = ¼ of that pool, `OXIDANT_COLOCATED_ENGINES`
for in-process multi-worker). That fixed the *unbounded-pool* class of failures
(opaque `do_get: transport error` after a worker OOM — classic TPC-DS **Q2** / Q5 on SF100
before the fix).

**Auto-size is not enough for SF100 honesty runs** if HashJoin stays on the critical path
(build sides are **not** spillable — memory lands outside the FairSpillPool). Oxidant now
matches Spark EMR's strategy by default: prefer spillable SortMergeJoin for large equi-joins
(`OXIDANT_HASH_JOIN_PER_PARTITION_THRESHOLD_BYTES` × shuffle partitions), and shuffle
defaults to `max(200, worker_vcpus)` — **never** bare `WorkerCount` (empty CFN no longer
stamps 2 buckets on a 2-worker cluster).

| Symptom | Likely cause | Required fix |
|---------|--------------|--------------|
| Opaque `do_get: transport error`, worker dead, dmesg OOM | Unbounded / missing pool (`MEMORY_LIMIT` unset **and** auto-size unavailable, or `=0`) | Leave unset for auto-size **or** set explicit limits; never `=0` on SF100 |
| One worker ~50+ GiB RSS, other light; Q2 / multi-fact joins die | Shuffle partitions forced to worker count | Leave `ShufflePartitions` empty (engine default ≥200) or set ≥200 |
| Hash joins blow cgroup despite a bounded pool | Forced `OXIDANT_PREFER_HASH_JOIN=true` or threshold raised | Keep `auto`; do not raise per-partition threshold for honesty runs |

**Canonical SF100 topology** (Spark EMR-class; copy from [`docs/distributed-ec2.md`](docs/distributed-ec2.md)
§ SF100 — keep in sync):

| Knob | Value |
|------|-------|
| Driver | **`c6g.xlarge`** (8 GiB), 100 GiB root — no `MEMORY_LIMIT` / no S3 cache; `OXIDANT_DISTRIBUTED_STRICT=1` |
| Workers | 2 × `m8g.4xlarge` (64 GiB), spill EBS **500 GiB** gp3 |
| `OXIDANT_MEMORY_LIMIT_BYTES` | ~40%–70% of worker RAM on **workers only** (e.g. `28895544320` ≈ 27 Gi); never on the driver |
| `OXIDANT_SHUFFLE_SPILL_BYTES` | e.g. `8589934592` (8 Gi) on **workers only** |
| `OXIDANT_SHUFFLE_PARTITIONS` | unset (default ≥200) or explicit `200` |
| S3 cache | Off on driver; workers ok with `OXIDANT_S3_CACHE_MAX_OBJECT_BYTES=2Gi` |
| Deploy | `./deploy/cloudformation/deploy-stack.sh … --driver-type c6g.xlarge --worker-type m8g.4xlarge --worker-spill-size 500 --distributed-strict true …` |

Before blaming the planner on SF100 TPC-DS Q2 (or TPC-H multi-fact joins), verify on **every**
worker: `OXIDANT_MEMORY_LIMIT_BYTES`, `OXIDANT_SHUFFLE_SPILL_BYTES`, and on the driver
**no** `OXIDANT_MEMORY_LIMIT_BYTES` plus shuffle ≥200. Check `journalctl -u oxidant-worker` /
dmesg for OOM and per-worker RSS skew before changing code. Full operational postmortem:
[`bench/tpch/SF100-EC2-NOTES.md`](bench/tpch/SF100-EC2-NOTES.md).

**Distribution smoke check (mandatory before publishing):** if worker CPU stays ~0% while the
driver NetworkIn climbs by multi‑GiB, the query is running **driver-local** (Spark EMR
anti-pattern). Require `OXIDANT_DISTRIBUTED_STRICT=1`, non-empty `OXIDANT_WORKERS` on the
driver, and `Oxidant stage summary:` on workers. Soft local fallback with workers configured
is a product bug — do not “fix” it by upsizing the driver.

### CI gotchas (commit / push / PR)

GitHub Actions gates live in `.github/workflows/ci.yml`. Before pushing, run
`./scripts/ci-local.sh` (or install the optional pre-push hook:
`git config core.hooksPath .githooks`). The following issues have bitten real PRs:

#### `oxidant-cli` must be built before `cargo test --workspace`

`oxidant-cli` is a **binary-only** crate (`[[bin]] oxidant`). `cargo test --workspace` does **not**
build orphan binaries, so `CARGO_BIN_EXE_oxidant` is unset unless you built it explicitly.

- **Symptom:** `cli_driver_worker_matches_single_node` panics with
  `oxidant binary not found at …/target/debug/oxidant`.
- **Fix:** `cargo build -p oxidant-cli` before tests. CI and `scripts/ci-local.sh` do this.
- **Test location:** the driver/worker subprocess smoke test lives in
  `crates/oxidant-cli/tests/cli_driver_worker.rs` (not `oxidant-execution`) so Cargo sets
  `CARGO_BIN_EXE_oxidant` when the test is built via `cargo test -p oxidant-cli`.
- **`oxidant_bin()` fallback:** when the env var is missing, probe (in order)
  `$CARGO_TARGET_DIR/$PROFILE/oxidant`, `target/$PROFILE/oxidant`, and
  `target/llvm-cov-target/$PROFILE/oxidant` (see llvm-cov below).

#### `cargo llvm-cov` uses a separate target directory

The informational `line-coverage` job runs `cargo llvm-cov --workspace --html --jobs 2`, which
re-runs the full test suite under `target/llvm-cov-target/` (not `target/debug/`).

- **Symptom:** same `oxidant binary not found` failure, but only in the `line-coverage` job
  even when `clippy + test + tpch` passes.
- **Fix (CI):** `cargo build -p oxidant-cli --target-dir target/llvm-cov-target` before
  `cargo llvm-cov`. Upload artifact from `target/llvm-cov/html` (not `coverage/`).
- **Linker OOM gotcha:** the job must set `CARGO_PROFILE_TEST_DEBUG=1` and
  `CARGO_PROFILE_DEV_DEBUG=1` (same as `build-test`). Without them, full debuginfo +
  `-C instrument-coverage` makes each `oxidant-execution` integration binary ~1–2 GB at link
  time; parallel `ld` on the 7 GiB `ubuntu-latest` runner dies with `collect2: ld terminated
  with signal 7 [Bus error]` (~54–60 min wall clock, not a missing oxidant-cli build). Cap
  link parallelism with `--jobs 2`; job timeout is 180 min for cold-cache compiles.
- **Flag gotcha:** do **not** pass `--output-path coverage/` together with `--html` —
  `cargo-llvm-cov` rejects incompatible flags. Use `--html` alone.
- **Job is non-blocking** (`continue-on-error: true`) but should still be kept green for
  trending artifacts. Nightly uses the same recipe (`coverage-nightly.yml`).

#### `tpch-distributed` auto-splitter SQL must re-parse on workers

`cargo run -p oxidant-bench -- tpch-distributed --sf 1 --workers 2` is a **blocking** CI
gate. The auto-splitter (`oxidant_execution::plan::plan_distributed`) unparses logical plans
to stage SQL via DataFusion's `Unparser`, then workers re-parse that SQL under the
Databricks dialect. Some Unparser output is **invalid on round-trip**:

| Unparser output | Problem | Sanitized to |
|----------------|---------|--------------|
| `shipping."volume"` | dot + double-quoted column | `shipping.volume` |
| `"part".p_partkey` | dot access on quoted table (reserved name) | `` `part`.p_partkey `` |

- **Symptom:** Q7/Q8 `ParserError: Expected identifier after '.'`; Q9
  `Dot access not supported for non-string expr`.
- **Fix:** `sanitize_generated_sql()` in `crates/oxidant-execution/src/plan/stage_planner.rs`
  rewrites these patterns before stage SQL is sent to workers.
- **Debug locally:** `OXIDANT_TPCH_ONLY=Q7 OXIDANT_TPCH_DEBUG=1 cargo run -p oxidant-bench -- tpch-distributed --sf 1 --workers 2`

#### CI job map (quick reference)

| Job | Blocking? | Key command |
|-----|-----------|-------------|
| rustfmt | yes | `cargo fmt --all -- --check` |
| clippy + test | yes | `cargo build -p oxidant-cli` then clippy/test |
| query-gates (main / `full-gates`) | yes | official TPC kits + `tpch`/`tpcds`/`*-distributed` at **SF1** |
| coverage gates | yes | clickbench, clickbench-grpc, correctness |
| Spark SQL parity ratchet | yes | `oxidant-parity ratchet --baseline parity/baseline.json` |
| line coverage | no (informational) | `CARGO_PROFILE_*_DEBUG=1` + `cargo llvm-cov --workspace --html --jobs 2` |


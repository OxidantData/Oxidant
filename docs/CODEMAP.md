# Weft (OSS) — code map

Quick ownership map for the open query engine.
Architecture: [architecture.md](architecture.md). Open work: [TODOS.md](TODOS.md).
Agent indexes: [AGENT_INDEXING.md](AGENT_INDEXING.md).

## Crates

| Path | Purpose | Entrypoint |
|------|---------|------------|
| `crates/weft-cli/` | `weft` binary: spark server, worker, driver | `src/main.rs` |
| `crates/weft-connect/` | Spark Connect gRPC + DataFrame translate | `src/lib.rs` |
| `crates/weft-loom/` | Vectorized CPU engine (DataFusion 54) | `src/lib.rs` |
| `crates/weft-execution/` | Local + distributed Flight driver/worker/shuffle | `src/lib.rs` |
| `crates/weft-sql/` | Spark SQL dialect → warp IR | `src/lib.rs` |
| `crates/weft-plan/` | Warp unresolved logical IR | `src/lib.rs` |
| `crates/weft-analyzer/` | Name/type resolve vs catalog | `src/lib.rs` |
| `crates/weft-optimizer/` | Hedle opts + Loom vs HVM routing | `src/lib.rs` |
| `crates/weft-physical/` | Physical plan / `ExecutionPlan` | `src/lib.rs` |
| `crates/weft-datasource/` | Parquet/CSV/JSON + Delta/Iceberg resolvers | `src/lib.rs` |
| `crates/weft-catalog/` | Catalog SPI + registry | `src/lib.rs` |
| `crates/weft-catalog-hive/` | Hive Metastore provider | `src/lib.rs` |
| `crates/weft-catalog-glue/` | AWS Glue provider | `src/lib.rs` |
| `crates/weft-catalog-rest/` | Iceberg REST / Unity-compatible | `src/lib.rs` |
| `crates/weft-proto/` | Vendored Spark Connect protos (protox) | `src/lib.rs` |
| `crates/weft-common/` | Shared errors, config, session identity | `src/lib.rs` |
| `crates/weft-hvm/` | Opt-in Bend→HVM2 backend (feature-gated) | `src/lib.rs` |
| `crates/weft-streaming/` | Structured Streaming micro-batch | `src/lib.rs` |
| `crates/weft-observability/` | Events, Spark REST DTOs, app state | `src/lib.rs` |
| `crates/weft-ui-server/` | Spark-compat `/api/v1` + embedded UI | `src/lib.rs` |
| `crates/weft-spark-compat/` | Golden SQL parity harness / scoreboard | `src/lib.rs` |
| `crates/weft-bench/` | ClickBench / TPC-H / TPC-DS / correctness | `src/main.rs` |
| `crates/weft-gateway/` | Legacy/control-plane remnants in public tree | `src/main.rs` |
| `crates/weft-orchestrator/` | Static / K8s worker-pool backends | `src/lib.rs` |

> Platform-owned control plane lives in **weft-platform** (private). Prefer that repo for
> SSO/SCIM/gateway product work; keep engine protocol + execution changes here.

## Other trees

| Path | Purpose |
|------|---------|
| `python/pyweft/` | Pip helper that launches Connect for stock PySpark |
| `bench/` | ClickBench / TPC harnesses + install scripts |
| `site/` | Vite/React marketing + charts (GitHub Pages) |
| `parity/` | Spark golden baseline ratchet (`baseline.json`) |
| `docs/` | Architecture, issues, next steps, catalogs, runtime contract |
| `scripts/` | CI local, daily maintenance, `index-agents.sh` |

## Docs map

| Path | Purpose |
|------|---------|
| `architecture.md` | Canonical engine design |
| `CODEMAP.md` | This file |
| `TODOS.md` | Indexed open work |
| `AGENT_INDEXING.md` | CodeGraph + GitNexus |
| `NEXT_STEPS.md` | Resume / phase narrative |
| `ISSUES.md` | Issue-level history |
| `runtime-contract.md` | Contract for weft-platform consumers |
| `catalogs.md` | External catalog SPI |

# Oxidant (OSS) — code map

Quick ownership map for the open query engine.
Architecture: [architecture.md](architecture.md).

## Crates

| Path | Purpose | Entrypoint |
|------|---------|------------|
| `crates/oxidant-cli/` | `oxidant` binary: spark server, worker, driver | `src/main.rs` |
| `crates/oxidant-connect/` | Spark Connect gRPC + DataFrame translate | `src/lib.rs` |
| `crates/oxidant-loom/` | Vectorized CPU engine (DataFusion 54) | `src/lib.rs` |
| `crates/oxidant-execution/` | Local + distributed Flight driver/worker/shuffle | `src/lib.rs` |
| `crates/oxidant-sql/` | Spark SQL dialect → warp IR | `src/lib.rs` |
| `crates/oxidant-plan/` | Warp unresolved logical IR | `src/lib.rs` |
| `crates/oxidant-analyzer/` | Name/type resolve vs catalog | `src/lib.rs` |
| `crates/oxidant-optimizer/` | Hedle opts + Loom vs HVM routing | `src/lib.rs` |
| `crates/oxidant-physical/` | Physical plan / `ExecutionPlan` | `src/lib.rs` |
| `crates/oxidant-datasource/` | Parquet/CSV/JSON + Delta/Iceberg resolvers | `src/lib.rs` |
| `crates/oxidant-catalog/` | Catalog SPI + registry | `src/lib.rs` |
| `crates/oxidant-catalog-hive/` | Hive Metastore provider | `src/lib.rs` |
| `crates/oxidant-catalog-glue/` | AWS Glue provider | `src/lib.rs` |
| `crates/oxidant-catalog-rest/` | Iceberg REST / Unity-compatible | `src/lib.rs` |
| `crates/oxidant-proto/` | Vendored Spark Connect protos (protox) | `src/lib.rs` |
| `crates/oxidant-common/` | Shared errors, config, session identity | `src/lib.rs` |
| `crates/oxidant-hvm/` | Opt-in Bend→HVM2 backend (feature-gated) | `src/lib.rs` |
| `crates/oxidant-streaming/` | Structured Streaming micro-batch | `src/lib.rs` |
| `crates/oxidant-observability/` | Events, Spark REST DTOs, app state | `src/lib.rs` |
| `crates/oxidant-ui-server/` | Spark-compat `/api/v1` + embedded UI | `src/lib.rs` |
| `crates/oxidant-spark-compat/` | Golden SQL parity harness / scoreboard | `src/lib.rs` |
| `crates/oxidant-bench/` | ClickBench / TPC-H / TPC-DS / correctness | `src/main.rs` |
| `crates/oxidant-gateway/` | Legacy/control-plane remnants in public tree | `src/main.rs` |
| `crates/oxidant-orchestrator/` | Static / K8s worker-pool backends | `src/lib.rs` |

> Platform-owned control plane lives in **oxidant-platform** (private). Prefer that repo for
> SSO/SCIM/gateway product work; keep engine protocol + execution changes here.

## Other trees

| Path | Purpose |
|------|---------|
| `python/pyoxidant/` | Pip helper that launches Connect for stock PySpark |
| `bench/` | ClickBench / TPC harnesses + install scripts |
| `site/` | Vite/React marketing + charts (GitHub Pages) |
| `parity/` | Spark golden baseline ratchet (`baseline.json`) |
| `docs/` | Architecture, catalogs, deployment, runtime contract |
| `deploy/docker/` | connect-server / worker container images |
| `deploy/packer/` | Hardened AL2023 AMI for EC2 driver/workers |
| `deploy/cloudformation/` | CFN + ASG data plane |
| `scripts/` | CI local, repo rename helper |

## Docs map

| Path | Purpose |
|------|---------|
| `architecture.md` | Canonical engine design |
| `CODEMAP.md` | This file |
| `runtime-contract.md` | Contract for oxidant-platform consumers |
| `distributed-ec2.md` | Packer AMI + CFN/ASG data plane |
| `deployment.md` | Self-hosted platform deploy outline |
| `catalogs.md` | External catalog SPI |

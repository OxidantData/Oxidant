# Weft (OSS) — open work

Indexed queue for agents. Phase narrative and history stay in [NEXT_STEPS.md](NEXT_STEPS.md)
and [ISSUES.md](ISSUES.md) — this table is the short “what to pick next” view.
Control-plane TODOs: [weft-platform docs/TODOS.md](https://gitlab.com/weftlabs/weft-platform)
(private; see sibling checkout).

| ID | Area | Status | Why it matters | Primary paths |
|----|------|--------|----------------|---------------|
| E-4.5 | Connect / govern | Open (blocks platform) | Per-session `GovernedCatalog` + gateway auth so platform can open non-admin cluster SQL | `crates/weft-connect/`; `docs/runtime-contract.md`; platform `docs/k8s-migration.md` Step 4.5 |
| E-HVM | Research | Gated | Phase-2 go/no-go: ≥2× Loom on bounded parallel workload or shelve | `crates/weft-hvm/`; `docs/architecture.md` |
| E-STREAM | Streaming | Open | Structured Streaming + Kafka source (Phase 2) | `crates/weft-streaming/` |
| E-UC | Catalog | Open | Unity / Iceberg REST + temp credentials depth | `crates/weft-catalog-rest/`; `docs/catalogs.md` |
| E-DIST | Distributed | Open | Close the distribution gap (plan coverage, DataFrame routing, fault proof, honest SF100 re-measure) | `docs/DISTRIBUTED_DONE.md`; `crates/weft-execution/src/plan/`; `crates/weft-connect/src/distributed.rs` |
| E-DS-THROUGHPUT | Perf | Partial | TPC-DS SF10 v13: **65.6s vs Spark 286.8s (0.229×), 98/99 wins, 99/99 golden**. Root-caused + fixed: (1) legacy forced `WEFT_PREFER_HASH_JOIN` (sort-merge pathology; `auto` everywhere is the deployed/IaC default), (2) driver split the *unoptimized* plan — now `extract_equijoin_predicate`+`push_down_filter` run pre-split behind a shape gate (aliases/expr-subqueries/Union/Window skip; Q78 7.6→1.4s, Q39 1.8→0.5s). Residual: Q4 1.29× behind Spark (Union-gated), Q17 0.51→1.15s and Q25 0.27→0.41s minor pushdown-era regressions to re-tune | `crates/weft-loom/src/s3_cache.rs`; `crates/weft-loom/src/lib.rs` (`optimize_logical_plan`, `splittable_after_rewrite`); `crates/weft-connect/src/distributed.rs` |
| E-LOOM-FLAKE | Tests | Open | Two pre-existing lib tests flake ~1/10 under load: `auto_join_selection_q62_arm_chain_builds_row_bounded_sides` (1 GiB pool external-sort reservation race), `catalog_bridge::tests::catalog_parquet_scan_caches_footer_across_queries`. Green in CI and in isolation runs; make pool sizing/timing deterministic | `crates/weft-loom/src/lib.rs:9180`; `crates/weft-loom/src/catalog_bridge.rs` |
| E-DIST-DISPATCH-LEAF | Distributed | Open | Root-cause why a linear star-scan leaf task ran ~2x slower under `WEFT_CONCURRENT_STAGES=1` (Q63 hot 10.0s vs 2.97s at SF10; same worker/shard/SQL, nothing to overlap). Default stays on — matrix-net 319s vs 369s — but unexplained | `crates/weft-execution/src/driver.rs` (`run_stages_concurrent`, `producer_task_futures`) |
| E-DIST-BCAST | Distributed | Done | Auto-broadcast dims from table sizes; `WEFT_REPLICATED_TABLES` optional override only (KAN-1) | `crates/weft-loom/src/shard.rs`; `crates/weft-connect/src/lib.rs`; `docs/runtime-contract.md` |
| E-K8S | Deploy | Open | Engine-side K8s deploy story (platform owns product cutover) | `docs/distributed-k8s.md`; `crates/weft-orchestrator/` |
| E-EC2 | Deploy | Open | CFN + ASG data plane (Packer AMI, fixed worker ASG, Route53 discovery). **AMI rebuild needed:** the baked `bootstrap.sh` predates the KAN-58 fixes (crashes mounting the root disk as spill on workers; `enable --now` circular-start deadlock on driver) and defaults `weft:prefer-hash-join` to forced values instead of `auto` — every fresh boot needs manual repair (see `scripts/sf10-start.sh` header + `deploy/packer/files/bootstrap.sh`) | `docs/distributed-ec2.md`; `deploy/packer/`; `deploy/cloudformation/` |
| E-DF-UDF | DataFrame | Open | Python UDFs, pivot w/o values, Stat/ML/Catalog relations, streaming, reattach | `crates/weft-connect/` |
| E-CLICK | Bench | Loose end | Upstream ClickBench PR + median-per-query vs Spark exit | `bench/clickbench/`; `crates/weft-bench/` |
| E-TPCH-ORACLE | Bench | Open | TPC-H oracle-diff vs DuckDB | `crates/weft-bench/` |
| E-CLAP | CLI | Open | Replace hand-rolled args with clap | `crates/weft-cli/src/main.rs`; `crates/weft-cli/Cargo.toml` |
| E-ROUTE | Optimizer | Partial | `route` always Loom until HVM gate | `crates/weft-optimizer/` |
| E-ANALYZER | Planner | Partial | Resolve still largely no-op vs full catalog typing | `crates/weft-analyzer/` |
| E-SQLPARSER | SQL | Scaffold notes | sqlparser dep TODO in Cargo.toml while dialect work continues | `crates/weft-sql/Cargo.toml` |

## How to add an item

1. Short `E-*` id, area, status.
2. File pointers required.
3. If platform-blocked (E-4.5), keep the matching `P-4.5` row in weft-platform in sync.

## Related

- [architecture.md](architecture.md) — design decisions
- [NEXT_STEPS.md](NEXT_STEPS.md) — resume guide
- [AGENT_INDEXING.md](AGENT_INDEXING.md) — CodeGraph / GitNexus

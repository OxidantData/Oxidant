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
| E-DS-THROUGHPUT | Perf | Partial | TPC-DS SF10 v14: **56.4s vs Spark 282.8s (0.199×), 98/99 wins, 99/99 golden** (Q4 11.4→3.25s, Q11 4.5→1.6s). Fixes: (1) legacy forced `WEFT_PREFER_HASH_JOIN` (`auto` everywhere is deployed/IaC default), (2) pre-split `extract_equijoin_predicate`+`push_down_filter` behind a shape gate, (3) union-extended rule set — plans with a `Union` additionally run filter-scoped constant folding (`FoldConstantFilters`; full `SimplifyExpressions` folds decimal casts to bare literals that unparse as `0.00` → `DECIMAL(3,2)` scale drift, Q5) + `eliminate_filter`/`propagate_empty_relation`/`optimize_unions`, so pushed predicates prune contradictory union arms (Q4's six `year_total` occurrences → single-fact slices; pushdown alone exploded 15→66 stages, v12 do_get failure). Defense: driver stage-explosion guard (>40 stages → prefer the unoptimized split). Residual: Q72 4.5s vs Spark 4.4s (alias-gated; flips win/loss across cluster generations — the real lever is un-gating SQL-table-alias plans, the Q72 base-vs-alias qualifier problem); Q17 0.51→1.3s and Q25 0.27→0.4s pushdown-era regressions — falsified join-strategy (verified A/B), `WEFT_CONCURRENT_STAGES`, cache asymmetry, and HEAD-revalidation; worker leaf tasks intrinsically ~2× v11 (w0 0.82s/w1 1.23s) with identical stage SQL, needs a worker-side profile pass | `crates/weft-loom/src/s3_cache.rs`; `crates/weft-loom/src/lib.rs` (`optimize_logical_plan`, `PreSplitRewrite`, `FoldConstantFilters`); `crates/weft-connect/src/distributed.rs` |
| E-LOOM-FLAKE | Tests | Done | Three load-sensitive test races root-caused + fixed: (1) `auto_join_selection_q62_arm_chain_builds_row_bounded_sides` — its SMJ parity re-plan registered partitions × 8 `ExternalSorter` consumers on the 1 GiB `FairSpillPool` (~100 at 12 cores → ~9 MiB fair share < one ~10 MiB first batch, unspillable when empty); the test now pins `WEFT_TARGET_PARTITIONS=2`. (2) `catalog_parquet_scan_caches_footer_across_queries` family — the unlocked `WEFT_WORKER_COUNT`/`WEFT_SHARD_INDEX` window in `explicit_assignment_task_local_wins_over_env` sharded concurrent multi-file catalog listings (`without_declared_schema_merge_fails` observed reading one of two files); a test-only `SHARD_ENV_GATE` now serializes `ShardAssignment::from_env` against env-mutating tests. (3) Same-process temp-dir collisions (pid + coarse nanos tick) in the shared `weft-cat`/`weft-mixed` helpers — sibling `remove_dir_all` deleted files mid-scan (ENOENT); helpers now carry an atomic sequence like `shard::tests::write_parts_with_rows` | `crates/weft-loom/src/lib.rs`; `crates/weft-loom/src/shard.rs`; `crates/weft-loom/src/catalog_bridge.rs` |
| E-DIST-DISPATCH-LEAF | Distributed | Open | Root-cause why a linear star-scan leaf task ran ~2x slower under `WEFT_CONCURRENT_STAGES=1` (Q63 hot 10.0s vs 2.97s at SF10; same worker/shard/SQL, nothing to overlap). Default stays on — matrix-net 319s vs 369s — but unexplained | `crates/weft-execution/src/driver.rs` (`run_stages_concurrent`, `producer_task_futures`) |
| E-DIST-BCAST | Distributed | Done | Auto-broadcast dims from table sizes; `WEFT_REPLICATED_TABLES` optional override only (KAN-1) | `crates/weft-loom/src/shard.rs`; `crates/weft-connect/src/lib.rs`; `docs/runtime-contract.md` |
| E-K8S | Deploy | Open | Engine-side K8s deploy story (platform owns product cutover) | `docs/distributed-k8s.md`; `crates/weft-orchestrator/` |
| E-EC2 | Deploy | Done | CFN + ASG data plane (Packer AMI, fixed worker ASG, Route53 discovery). AMI rebuilt (current: `ami-06f5c873b9d2e8d71`) with the KAN-58 bootstrap fixes + `WEFT_S3_CACHE_DIR` on by default + this PR's engine; the weft-sf10 stack is updated (AmiId + `PreferHashJoin=auto`); fresh boots come up with zero manual repair (validated end-to-end, incl. two full CFN instance refreshes). Refresh-time shard race filed + fixed as E-EC2-SHARD-REFRESH | `docs/distributed-ec2.md`; `deploy/packer/`; `deploy/cloudformation/` |
| E-EC2-SHARD-REFRESH | Deploy | Done | CFN instance refresh replaces workers one at a time; an early replacement computed `WEFT_SHARD_INDEX` against a peer list containing a doomed old worker — sort order handed BOTH workers index 0 (the other shard read by NOBODY: silent partial results; observed live 2026-08-05). bootstrap.sh now loud-fails on an incomplete peer list, and `weft-shard-resolve.timer` (2min, stability + hysteresis guards) re-resolves against settled membership and restarts `weft-worker` on divergence. Baked in AMI v3 `ami-06f5c873b9d2e8d71` (stack updated); live-validated: poisoned index corrected 0→1 in <5 min, refresh boots correct 0/1 | `deploy/packer/files/bootstrap.sh`; `deploy/packer/files/shard-resolve.sh`; `deploy/packer/files/systemd/weft-shard-resolve.{service,timer}` |
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

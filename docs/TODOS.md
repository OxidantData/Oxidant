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
| E-DIST-BCAST | Distributed | Done | Auto-broadcast dims from table sizes; `WEFT_REPLICATED_TABLES` optional override only (KAN-1) | `crates/weft-loom/src/shard.rs`; `crates/weft-connect/src/lib.rs`; `docs/runtime-contract.md` |
| E-K8S | Deploy | Open | Engine-side K8s deploy story (platform owns product cutover) | `docs/distributed-k8s.md`; `crates/weft-orchestrator/` |
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

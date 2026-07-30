# Distribution — definition of done

Companion to [DISTRIBUTED_PARITY.md](DISTRIBUTED_PARITY.md). That doc is the *gap analysis*
("here is what EMR / Photon / Lakesail do that we don't"). **This doc is the closing
checklist**: every item below has an owner path and an acceptance test, and when all of
them are checked we can say — without asterisks — that Weft distributes the load.

Status date: **2026-07-25**. Items are `D-<phase>.<n>`; phases are ordered by dependency,
not by size. Nothing in Phase 2+ is measurable until Phase 0 lands.

## The claim we want to earn

1. Any Spark Connect client distributes — **SQL and DataFrame API alike**.
2. With `N` workers, scans are disjoint and shuffles cross node boundaries.
3. Falling back to the driver is an **exception**, is **visible**, and can be made **fatal**.
4. A worker dying mid-query yields a correct answer, not a wrong one or a hang.
5. The published SF100 numbers came from a real multi-worker run that could not
   silently have been a driver run.

## Current state (verified against code, not docs)

| Area | Reality today |
|------|---------------|
| Entry point | `RelType::Sql` only — [crates/weft-connect/src/lib.rs](../crates/weft-connect/src/lib.rs) `base_relation_batches`. DataFrame relations lower via `translate` to the local engine. |
| Plan shapes | Grouped/global aggs, broadcast star joins, left-deep single-equijoin shuffle chains, `HAVING`, `UNION ALL`, replicated-only subqueries, narrow aggregate windows. |
| Non-aggregate queries | **Partial (D-2.2).** `try_non_aggregate` in [shape_extensions.rs](../crates/weft-execution/src/plan/shape_extensions.rs) lowers scan/filter/sort/limit over one sharded table (scatter + global finalize) and all-replicated scans (Forward). `peel()` also strips `SubqueryAlias` / WHERE filters. CTE-heavy cross joins (Q1/Q4/…) still reject — aggregate lives inside the CTE. |
| Fallback | Silent. [distributed.rs](../crates/weft-connect/src/distributed.rs) maps `Error::Unsupported` to `Ok(None)` and discards the reason. |
| Fault tolerance | **Implemented, unproven.** [scheduler.rs](../crates/weft-execution/src/scheduler.rs) has retries, alternate-worker failover, health checks, lineage recompute, speculation (`WEFT_SPECULATIVE`, default off). No failure-injection test. |
| Exchange | `do_exchange` in [flight.rs](../crates/weft-execution/src/flight.rs) buffers the whole stream into a `Vec` before caching — push transport, not streaming. |
| Sharding | Round-robin `i % N` over a sorted file list ([shard.rs](../crates/weft-loom/src/shard.rs)) — disjoint but size-blind, so skewed files skew workers. |
| Measurement | No multi-worker SF100 run exists. Site numbers are single-node. |

## Phase 0 — Observability and honesty gates

Blocks everything: coverage cannot be measured while fallback is silent, and no benchmark
is trustworthy while a "distributed" run can quietly be a driver run.

- [x] **D-0.1 Surface the fallback reason.** Replace the `Ok(None)` discard in
  `try_run_distributed` with a typed outcome carrying the `Unsupported` message; log at
  `warn` and emit into `QueryTracker` so it appears in query observability next to the plan.
  *Accept:* an unsupported query's reject reason is visible in the observability payload.
- [x] **D-0.2 Strict mode.** `WEFT_DISTRIBUTED_STRICT=1` turns fallback into an error
  instead of local execution. Benchmarks and `bench/sf100/run-time-gate.sh` set it.
  *Accept:* a known-unsupported query fails under strict mode with the reject reason.
- [x] **D-0.3 Doc corrections.** `DISTRIBUTED_PARITY.md` item 13 (fault retry / speculative)
  is stale `[ ]` — the code exists; mark `[~]` (unproven) until D-3.1 lands.

## Phase 1 — Measure coverage locally, for free

All 99 TPC-DS queries and a generator already exist
([crates/weft-bench/src/tpcds.rs](../crates/weft-bench/src/tpcds.rs), `bench/tpcds/queries/`).
Turn "many shapes fall back" into an exact number before any cloud spend.

- [x] **D-1.1 Planner-coverage harness.** `weft-bench tpcds-distributed` runs
  `plan_distributed` over all 99 queries and reports supported vs fallback, grouped by
  reject reason. Same for the TPC-H set.
  *Accept:* one command prints `N/99 distributable` plus a reason histogram.
  *(2026-07-26: **46/99**; baseline in `bench/distributed/tpcds-planner-baseline.json`. This
  counts queries a plan can be **built** for and says nothing about the answer — D-1.2 is the
  number to quote.)*
- [x] **D-1.2 Correctness of the supported subset.** Execute every distributable query on
  in-process workers and assert row-for-row equality with single-node, extending the
  pattern of `two_worker_groupby_matches_single_node`.
  *Accept:* zero mismatches across the supported subset at small SF.
  *(2026-07-26: **46/46 execute-verified**, zero mismatches, zero errors, at sf0.01 on two
  workers — `tpcds-distributed --execute`, ratcheted against
  `bench/distributed/tpcds-execute-baseline.json`. Getting here meant dropping the planner count
  from 67 to 46: a full execute sweep showed 24 of those 67 returning a wrong answer or failing
  on the worker, and the planner now declines those shapes so they fall back to single-node.)*
- [x] **D-1.3 CI ratchet.** Commit the coverage JSON as a baseline and fail CI when the
  supported count drops, next to the existing Spark-parity ratchet.
  *Accept:* a deliberate regression turns CI red.

The D-1.1 histogram, not this document, decides the order inside Phase 2.

## Phase 2 — Plan coverage

- [x] **D-2.1 DataFrame API routing.** Route translated Connect relations through the
  distributed planner. Prefer refactoring `plan_distributed` to accept a `LogicalPlan`
  directly (it already begins with `engine.logical_plan(sql)`, so SQL is only an entry
  format) over unparsing the translated plan back to SQL.
  *Accept:* a PySpark `df.groupBy().agg()` produces worker stages, verified by task counts.
  *(`plan_distributed_logical` + `try_run_distributed_plan`;
  `crates/weft-connect/tests/distributed_pyspark.rs`.)*
- [~] **D-2.2 Non-aggregate queries.** Projection / filter / sort / limit and un-aggregated
  joins should distribute as a parallel scan plus gather, with per-worker top-N when a
  `LIMIT` is present. Requires relaxing `peel()`'s `Aggregate` requirement.
  *Accept:* `SELECT … FROM <sharded> WHERE … LIMIT n` runs on workers and matches single-node.
  *Progress (2026-07-25):* `try_non_aggregate` + `peel()` SubqueryAlias/WHERE peel landed;
  TPC-DS planner **52/99** (+6). Residual: outer queries whose logical plan still tops out at
  `CrossJoin` with grouped CTE inputs (cannot scatter the outer without a distributed CTE stage).
- [x] **D-2.3 Ranking and `ORDER BY` windows.** `ROW_NUMBER`/`RANK`/`DENSE_RANK`/`LAG`/`LEAD`
  with a non-empty `PARTITION BY` reuse the existing narrow-window shuffle in
  [shape_extensions.rs](../crates/weft-execution/src/plan/shape_extensions.rs) — ranking is
  exact once the partition is co-located; only the re-combinable-aggregate check blocks it.
- [x] **D-2.4 Global windows** (no `PARTITION BY`) — single-partition gather stage, or an
  explicit permanent reject with a documented rationale.
  *(Explicit permanent reject; test `global_window_is_rejected`.)*
- [x] **D-2.5 `UNION` distinct / `INTERSECT` / `EXCEPT`** — branch stages plus a
  hash-shuffled dedup stage. *(Note: Jira KAN-1 reused "D-2.5" for auto-broadcast —
  that lives under `E-DIST-BCAST` / `WEFT_AUTO_BROADCAST_THRESHOLD_BYTES`.)*
- [x] **D-2.6 Subqueries over sharded tables.** Decorrelate / gather via
  `try_materialize_subquery_fact` + `try_materialize_complex_fact` (KAN-12); non-gatherable
  shapes remain explicit rejects.
- [x] **D-2.7 Multi-key equijoins.** Composite `ON` keys hash all columns (KAN-10).
- [x] **D-2.8 Outer / semi / anti shuffle joins.** Left-deep chain supports
  LEFT/RIGHT/FULL/LEFT SEMI/LEFT ANTI.
- [x] **D-2.9 Non-equi residual filters** alongside the equijoin key.
- [ ] **D-2.10 Global `COUNT(DISTINCT)`** — currently an explicit reject in
  `global_aggregation_stages`; shuffle by the distinct argument, then combine.
- [~] **D-2.11 Coverage exit.** TPC-DS execute-verified **95/99**. Remaining deliberate
  declines: **Q5, Q14, Q77, Q80** (ROLLUP + UNION/INTERSECT Unparser round-trip breakage) —
  documented in `DISTRIBUTED_PARITY.md` (KAN-13).

*Accept for the phase:* the D-1.3 ratchet reaches the agreed threshold and every remaining
fallback is a listed, intentional one.

## Phase 3 — Cluster semantics worth trusting

- [x] **D-3.1 Failure injection.** Kill a worker mid-stage and assert a correct result via
  retry / alternate worker / lineage recompute. Cover both producer and consumer stages.
  *Accept:* a test that reliably kills a worker and still matches single-node output.
  *(`cargo test -p weft-cli --test cli_fault_tolerance` + `WEFT_FAULT_EXIT_*`.)*
- [x] **D-3.2 Speculation default.** With D-3.1 in place, measure straggler benefit and
  decide whether `WEFT_SPECULATIVE` defaults on; record the decision.
  *(Decision: default **off** — see DISTRIBUTED_PARITY item 13.)*
- [x] **D-3.3 Streaming `do_exchange`.** Consume incrementally with backpressure instead of
  collecting into a `Vec`, so a stage larger than worker memory streams.
  *Accept:* a stage exceeding the worker memory budget completes without OOM.
- [x] **D-3.4 Spill parity on the streaming path.** The ticket/cache path spills
  (`WEFT_SHUFFLE_SPILL_BYTES`); the streaming path must too.
- [x] **D-3.5 Skew-aware sharding.** Weight shard assignment by file size in
  `apply_file_shard` so unequal files don't produce unequal workers.
  *Accept:* on a deliberately skewed file set, per-worker bytes stay within a set tolerance.
- [x] **D-3.6 Autoscaling on query parallelism** in
  [crates/weft-orchestrator/src/backend.rs](../crates/weft-orchestrator/src/backend.rs),
  replacing idle-pod scaling.
- [x] **D-3.7 Worker memory budget.** Workers honour a per-pod limit and degrade by
  spilling rather than being OOM-killed, mirroring the driver's `WEFT_MEMORY_LIMIT_BYTES`.

## Phase 4 — The honest re-measure

Only after Phases 0–2; Phase 3 items that are not yet done must be stated as caveats.

- [ ] **D-4.1** Ship engine + platform images per the deploy checklist in
  `DISTRIBUTED_PARITY.md`. *(Cloud cutover — not blocked on engine code.)*
- [ ] **D-4.2** Multi-worker smoke: cluster with `worker_min=worker_max=N>1`, confirm driver
  env and `applied file-list shard` in worker logs.
- [~] **D-4.3** `DISTRIBUTED_SF100=1 WORKER_MIN=2 WORKER_MAX=2 ./bench/sf100/run-time-gate.sh`
  with `WEFT_DISTRIBUTED_STRICT=1`, so any fallback fails the run rather than producing a
  driver-only number labelled distributed.
  *(Gate script exports `WEFT_DISTRIBUTED_STRICT=1`. Preferred honesty path is EC2 CF
  with canonical topology — 1× c6g.xlarge + 2× m8g.4xlarge / 500 GiB spill — see
  `docs/distributed-ec2.md` § SF100 topology and `bench/sf100/remeasure-distributed.sh`.)*
- [ ] **D-4.4** Update `site/src/data/{tpch,tpcds}.json` and the Performance page, publishing
  the distributed coverage fraction alongside the timings. Until then site numbers stay
  labelled single-node.

## Acceptance gates (all green = done)

| Gate | Command |
|------|---------|
| Planner coverage ratchet | `cargo run -p weft-bench -- tpcds-distributed` (D-1.1/1.3) |
| Execute correctness ratchet | `cargo run -p weft-bench -- tpcds-distributed --execute --sf 0.01 --workers 2` (D-1.2) |
| Distributed correctness | `cargo run -p weft-bench -- correctness-distributed` |
| TPC-H distributed | `cargo run -p weft-bench -- tpch-distributed --sf 0.01 --workers 2` |
| Fault tolerance | worker-kill test from D-3.1 in `cargo test --workspace` |
| Strict mode | SF100 gate run with `WEFT_DISTRIBUTED_STRICT=1` (D-4.3) |

## Explicit non-goals for "done"

Out of scope for this claim, so it cannot creep: adaptive re-optimization beyond the
[aqe.rs](../crates/weft-execution/src/aqe.rs) stub, cost-based shuffle partition sizing,
dynamic partition pruning, multi-tenant / fair-share scheduling across concurrent queries,
and cross-cluster (multi-region) execution.

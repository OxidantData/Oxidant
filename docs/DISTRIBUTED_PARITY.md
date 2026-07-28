# Distributed execution gaps vs EMR / Photon / Lakesail

Gap analysis. For the closing checklist — every remaining item with an acceptance test,
such that finishing all of them earns the "Weft distributes the load" claim — see
[DISTRIBUTED_DONE.md](DISTRIBUTED_DONE.md).

**Status (2026-07-25):** Connect SQL can fan out to workers when `WEFT_WORKERS` or
`WEFT_WORKER_SERVICE` is set. File-list sharding + replicated dims make multi-worker
scans disjoint. Full TPC-DS plan coverage is still incomplete — unsupported shapes
fall back to local driver execution.

## What we verified live (pre-fix)

| Check | Result |
|-------|--------|
| Cluster pods during SF100 | **1 driver**, `worker_min=worker_max=0` |
| Harness comment | `SQL path is driver Connect only` — scales workers to 0 |
| Gateway routing | `cluster_client` → Spark Connect `Sql` on driver Service |
| Connect execution (then) | `weft-connect` → `Engine::sql` (local DataFusion/`weft-loom`) |
| `weft-execution` | Used by CLI `weft driver` / tests only — **not** by Connect |
| CPU/memory | One pod requesting 28 CPU / 200Gi on one node; no cross-node shuffle |

Intra-pod DataFusion threading can use many cores on that one box. That is **not**
distributed execution and is not comparable to EMR / Databricks Photon / Lakesail.

## Architecture (after this workstream)

```
Gateway POST /api/sql
    → sc://driver:50051  (Spark Connect)
        → plan_distributed + run_stages  (when workers configured)
            → Arrow Flight shuffle to worker pods :50561
            → each worker scans its file-list shard (Glue/Parquet)

Fallback: unsupported plan shapes → Engine::sql on driver
```

## Gap checklist (priority order)

### P0 — Make Connect use workers at all

1. [x] **Wire Connect SQL → `plan_distributed` + `run_stages`** when workers are configured;
   fall back to local `Engine::sql` on `Unsupported`.
2. [x] **Inject worker endpoints into the driver** (`WEFT_WORKERS` + `WEFT_WORKER_SERVICE` DNS).
3. [x] **K8s wiring**: headless Service for worker StatefulSet; NetworkPolicy allowing
   driver↔worker Arrow Flight (`50561`) inside the cluster namespace.
4. [x] **Same catalog on workers**: workers load `WEFT_CATALOG_CONF` / Glue like the driver.

### P0 — Parallel scans (otherwise N workers each read the full table)

5. [x] **File-list sharding** for Glue/Parquet: partition the active file list by
   `worker_index / worker_count` so each worker scans a disjoint shard.
6. [x] **Replicated vs sharded table policy**: small dims auto-replicated from Glue/Parquet
   (or lakehouse) file sizes — tables smaller than the query's largest scan and under
   `WEFT_AUTO_BROADCAST_THRESHOLD_BYTES` (default 32 GiB). Optional
   `WEFT_REPLICATED_TABLES` force-include override only.

### P1 — TPC-DS / Photon-class plan coverage

7. [x] Shuffle joins between **two sharded** tables (auto-derive single equijoin + agg).
8. [x] Multi-stage DAGs (join chains, TPC-H Q5 / TPC-DS shape).
   Left-deep pairwise shuffle-join chains for 2+ sharded tables; multi-dim broadcast
   star (1 sharded fact + N replicated dims) folds into the partial. CrossJoin+filter
   Q5 with dims replicated uses the broadcast path.
9. [x] Windows, subqueries, `HAVING`, ungrouped aggregates, set ops.
    Supported: `HAVING`, ungrouped/global aggs, subqueries over sharded facts via gather
    (`try_materialize_*`), `UNION`/`INTERSECT`/`EXCEPT` arms, multi-key equijoins + residuals,
    outer/semi/anti shuffle chains, narrow aggregate windows, non-aggregate scan/gather.
    **Deliberate permanent rejects (KAN-13 / D-2.11 residual):** TPC-DS **Q5, Q14, Q77, Q80** —
    ROLLUP/CUBE/GROUPING SETS composed with UNION/INTERSECT where DataFusion Unparser stage SQL
    does not round-trip under the workers' Databricks dialect (correctness-first decline).
    Also still unsupported: global windows (no `PARTITION BY`), global `COUNT(DISTINCT)` without
    gather.
10. [x] Shuffle spill + `do_exchange` streaming.
    Ticket/cache path spills stage buckets to disk when over budget (`WEFT_SHUFFLE_SPILL_BYTES`,
    default 256 MiB; files under `WEFT_SPILL_DIR` or temp). Streaming `do_exchange` appends
    incrementally with the same spill budget (`do_exchange_streams_large_partition_under_memory_budget`).

### P1 — Cluster semantics

11. [x] `DnsMembership` / `WEFT_WORKER_SERVICE` (headless Service A records) instead of
    static `WEFT_WORKERS` only. (`WEFT_WORKERS` remains as fallback.)
12. [x] Autoscaling that tracks query parallelism (not idle Flight pods).
    See `weft-orchestrator` / `weft-execution` `autoscale` modules and gateway wiring.
13. [x] Fault retry / speculative tasks — implemented in
    [`scheduler.rs`](../crates/weft-execution/src/scheduler.rs); proven by
    `cargo test -p weft-cli --test cli_fault_tolerance` (worker kill via
    `WEFT_FAULT_EXIT_*`, restart, retry / lineage recompute). **Speculation default stays off**
    (`WEFT_SPECULATIVE` unset → false): fault recovery is covered by retries + alternate worker +
    upstream recompute without duplicating stage work; the 5 s straggler timeout would add latency
    on fast queries and we have no measured SF/TPC straggler win yet on this path.

### P2 — Benchmark honesty

14. [x] Site / harness must label runs **single-node** until a multi-worker SF run is
    re-measured on the distributed path.
15. [ ] Time-gate methodology only after distributed path is the default for `/api/sql`
    and SF100 is re-run with workers > 0.

## Comparable engines (what “done” looks like)

| Capability | EMR Spark | Photon | Lakesail | Weft `/api/sql` (this branch) |
|------------|-----------|--------|----------|-------------------------------|
| Multi-executor scan | yes | yes | yes | **yes** (file-list shard + workers) |
| Shuffle across nodes | yes | yes | yes | **yes** (Flight; spill on ticket/cache path) |
| Complex TPC-DS plans distributed | yes | yes | yes | **partial** (fallback local) |
| Catalog + object storage on all executors | yes | yes | yes | **yes** |

## Deploy checklist before SF100 re-run

1. Ship `weft` images with Connect distributed path + loom file sharding.
2. Ship `weft-platform` orchestrator with worker Service, Flight NetworkPolicy,
   `WEFT_WORKERS` / `WEFT_WORKER_SERVICE` / `WEFT_WORKER_COUNT` /
   `WEFT_AUTO_BROADCAST_THRESHOLD_BYTES` (optional) / `WEFT_REPLICATED_TABLES`
   (optional override) / `WEFT_POD_NAME`. See platform `docs/DISTRIBUTED_DEPLOY.md`
   for build/push/verify steps.
3. Create cluster with `worker_min=worker_max=N` (N>1), confirm driver env and worker
   shard logs (`applied file-list shard`).
4. Re-measure TPC-H/DS SF100 with **`DISTRIBUTED_SF100=1`** (harness default is refuse):
   ```sh
   DISTRIBUTED_SF100=1 WORKER_MIN=2 WORKER_MAX=2 ./bench/sf100/run-time-gate.sh
   # or ad-hoc:
   python3 bench/sf100/run-via-gateway.py --create-cluster --distributed --worker-count 2 ...
   ```
5. Only then update site numbers as distributed.

**Do not** run SF100 without an explicit mode — `run-time-gate.sh` exits unless
`DISTRIBUTED_SF100=1` or `ALLOW_SINGLE_NODE_GATE=1` (driver-only, not comparable).

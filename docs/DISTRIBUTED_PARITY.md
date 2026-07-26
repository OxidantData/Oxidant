# Distributed execution gaps vs EMR / Photon / Lakesail

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
6. [x] **Replicated vs sharded table policy**: small dims replicated via
   `WEFT_REPLICATED_TABLES` (platform default covers TPC-H/DS dims).

### P1 — TPC-DS / Photon-class plan coverage

7. [x] Shuffle joins between **two sharded** tables (auto-derive single equijoin + agg).
8. [ ] Multi-stage DAGs (join chains, TPC-H Q5 / TPC-DS shape).
9. [~] Windows, subqueries, `HAVING`, ungrouped aggregates, set ops.
   (`HAVING` + ungrouped/global aggregates supported; windows/subqueries/set ops not.)
10. [~] Shuffle spill + `do_exchange` streaming.
    Ticket/cache path spills stage buckets to disk when over budget (`WEFT_SHUFFLE_SPILL_BYTES`,
    default 256 MiB; files under `WEFT_SPILL_DIR` or temp). Streaming `do_exchange` still stubbed.

### P1 — Cluster semantics

11. [x] `DnsMembership` / `WEFT_WORKER_SERVICE` (headless Service A records) instead of
    static `WEFT_WORKERS` only. (`WEFT_WORKERS` remains as fallback.)
12. [ ] Autoscaling that tracks query parallelism (not idle Flight pods).
13. [ ] Fault retry / speculative tasks.

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
   `WEFT_WORKERS` / `WEFT_WORKER_SERVICE` / `WEFT_WORKER_COUNT` / `WEFT_REPLICATED_TABLES`.
3. Create cluster with `worker_min=worker_max=N` (N>1), confirm driver env and worker
   shard logs (`applied file-list shard`).
4. Re-measure TPC-H/DS SF100; only then update site numbers as distributed.

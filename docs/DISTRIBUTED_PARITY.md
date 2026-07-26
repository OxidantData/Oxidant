# Distributed execution gaps vs EMR / Photon / Lakesail

**Status (2026-07-25):** TPC-H/TPC-DS SF100 runs via `POST /api/sql` are **driver-only**.
Workers are not on the query path. Fat single-node Spot runs were stopped; infra torn down.

## What we verified live

| Check | Result |
|-------|--------|
| Cluster pods during SF100 | **1 driver**, `worker_min=worker_max=0` |
| Harness comment | `SQL path is driver Connect only` — scales workers to 0 |
| Gateway routing | `cluster_client` → Spark Connect `Sql` on driver Service |
| Connect execution | `weft-connect` → `Engine::sql` (local DataFusion/`weft-loom`) |
| `weft-execution` | Used by CLI `weft driver` / tests only — **not** by Connect |
| CPU/memory | One pod requesting 28 CPU / 200Gi on one node; no cross-node shuffle |

Intra-pod DataFusion threading can use many cores on that one box. That is **not**
distributed execution and is not comparable to EMR / Databricks Photon / Lakesail.

## Architecture today

```
Gateway POST /api/sql
    → sc://driver:50051  (Spark Connect)
        → Engine::sql on driver only
            → Glue catalog + S3 Parquet scanned on driver

Workers (if scaled): weft worker :50561 (Arrow Flight)
    → never dialed by Connect
```

## What exists but is unused on the product path

- `weft-execution`: 2-stage `partial-agg → hash shuffle → final-agg`, shuffle join tests
- `plan_distributed`: auto-split **simple grouped aggregations** (+ broadcast star joins)
- CLI: `weft driver --workers …` / `weft worker --port …`
- `ClusterMembership` trait; **`K8sMembership` not implemented**

## Gap checklist (priority order)

### P0 — Make Connect use workers at all

1. **Wire Connect SQL → `plan_distributed` + `run_stages`** when workers are configured;
   fall back to local `Engine::sql` on `Unsupported`.
2. **Inject worker endpoints into the driver** (`WEFT_WORKERS` or DNS membership).
3. **K8s wiring**: headless Service for worker StatefulSet; NetworkPolicy allowing
   driver↔worker Arrow Flight (`50561`) inside the cluster namespace.
4. **Same catalog on workers**: workers must load `WEFT_CATALOG_CONF` / Glue like the driver
   (today `weft worker` starts an empty engine unless `--data` is passed).

### P0 — Parallel scans (otherwise N workers each read the full table)

5. **File-list sharding** for Glue/Parquet: partition the active file list by
   `worker_index / worker_count` so each worker scans a disjoint shard.
6. **Replicated vs sharded table policy**: small dims replicated; facts sharded
   (matches `plan_distributed`'s broadcast-join model).

### P1 — TPC-DS / Photon-class plan coverage

7. Shuffle joins between **two sharded** tables (auto-derive, not hand-built tests only).
8. Multi-stage DAGs (join chains, TPC-H Q5 / TPC-DS shape).
9. Windows, subqueries, `HAVING`, ungrouped aggregates, set ops.
10. Shuffle spill + `do_exchange` streaming (MVP has no spill).

### P1 — Cluster semantics

11. `K8sMembership` (EndpointSlice / DNS-SRV) instead of static `WEFT_WORKERS`.
12. Autoscaling that tracks query parallelism (not idle Flight pods).
13. Fault retry / speculative tasks.

### P2 — Benchmark honesty

14. Site / harness must label runs **single-node** until P0 items land and a
    multi-worker SF run is re-measured.
15. Time-gate methodology only after distributed path is the default for `/api/sql`.

## Comparable engines (what “done” looks like)

| Capability | EMR Spark | Photon | Lakesail | Weft today (`/api/sql`) |
|------------|-----------|--------|----------|-------------------------|
| Multi-executor scan | yes | yes | yes | **no** (driver local) |
| Shuffle across nodes | yes | yes | yes | CLI MVP only |
| Complex TPC-DS plans distributed | yes | yes | yes | **no** |
| Catalog + object storage on all executors | yes | yes | yes | driver only |

## Immediate next commits

1. Platform: worker Service + Flight NetworkPolicy + `WEFT_WORKERS` on driver.
2. Engine: Connect tries distributed when `WEFT_WORKERS` is set; else local.
3. Workers: bootstrap same catalogs as driver.
4. Scan sharding for Glue/Parquet file lists (blocks real speedups).

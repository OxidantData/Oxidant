# Oxidant OSS engine — runtime contract for `oxidant-platform`

This document defines the environment contract between the **OSS engine images**
(`connect-server`, `worker`) and the **`oxidant-platform`** orchestrator (Terraform/Helm/HPA).

## Images

| Image | Entrypoint | Role |
|-------|------------|------|
| `connect-server` | `oxidant spark server --port 50051` | Spark Connect driver |
| `worker` | `oxidant worker --port 50561` | Arrow Flight worker |

## Driver (connect-server pod)

| Variable | Required | Description |
|----------|----------|-------------|
| `OXIDANT_WORKERS` | Recommended (bare metal / EC2) | Comma-separated static `host:port` Flight endpoints. **Authoritative when non-empty** (bare metal, VMs, EC2 ASG bootstrap pin). |
| `OXIDANT_WORKER_SERVICE` | Optional (Kubernetes) | Headless Service DNS name (e.g. `oxidant-worker.…svc.cluster.local`). Used only when `OXIDANT_WORKERS` / config list is empty; resolves A/AAAA → Flight endpoints. Not used for EC2 ASG honesty deploys. |
| `OXIDANT_WORKER_PORT` | Optional | Flight port workers listen on (default `50561`). Used with `OXIDANT_WORKER_SERVICE`. |
| `OXIDANT_STATUS_TOKEN` | Optional | Shared bearer token enabling `GET /api/status` on the driver's HTTP port (see [Driver status polling](#driver-status-polling)). Unset ⇒ the endpoint returns `404` and the platform gets no auto-termination / autoscaling signal from it. One token per cluster. |
| `OXIDANT_SHUFFLE_PARTITIONS` | Optional | Hash shuffle partition count. Default (when unset): Spark-like `max(200, OXIDANT_WORKER_VCPUS, worker_count)` — **never** bare worker count (a 2-worker cluster must not collapse to a 2-bucket shuffle). May exceed replica count. |
| `OXIDANT_WORKER_VCPUS` / `OXIDANT_WORKER_CORES` | Optional | Hints for the shuffle default: total cluster worker vCPUs, or per-worker cores × count. |
| `OXIDANT_AQE` | Optional | Default **on** (Spark 3+ / EMR parity). `0`/`false`/`off` disables adaptive shuffle-partition coalescing. After each producer stage the driver samples per-partition bucket row counts and, when the skew guard allows, coalesces toward Spark's advisory partition size (`OXIDANT_AQE_ADVISORY_PARTITION_BYTES`, default 64 MiB) floored at `num_workers`. Consumer `p` pulls every producer bucket `b ≡ p (mod new_p)`, so every bucket is still read exactly once. The planned partition count never shrinks mid-query. |
| `OXIDANT_AQE_ADVISORY_PARTITION_BYTES` | Optional | Target bytes per coalesced reader partition (default `67108864` = 64 MiB, Spark EMR advisory). |
| `OXIDANT_SHUFFLE_PULL_CONCURRENCY` | Optional | Max concurrent remote shuffle bucket pulls per upstream on a consumer task (default `8`). Caps consume-side RSS when many buckets fan in. |
| `OXIDANT_STAGE_INPUT_STATS` | Optional | Default **on** (KAN-2 A3). After each producer stage the driver counts its output rows per bucket (the same cheap local `bucket_row_counts` worker action AQE uses — one round trip per worker per producer stage, no data movement — shared with the AQE sample when `OXIDANT_AQE` is also on) and ships the exact per-bucket totals on downstream stage tickets. Workers attach the measured row count of the buckets each task pulls to that task's `shuffle_input*` scans, so the `auto` join-strategy guard (worker table below) sizes hash-join build sides from measured data: measured-small builds keep hash joins instead of defaulting to sort-merge, while genuinely oversized builds still reroute (Spark AQE's runtime SMJ→hash conversion; the safety valve stays). `0`/`false`/`off`/`no` restores the pre-A3 path (no sampling, no ticket counts, plain MemTable registration). |
| `OXIDANT_CONCURRENT_STAGES` | Optional | Default **on**. Dependency-aware stage dispatch: a stage dispatches as soon as ALL of its upstream stages complete instead of waiting out the whole previous stage, so independent branch arms (TPC-DS Q4/Q61/Q78 shapes) overlap — a consumer still waits for every upstream, so per-consumer stage-barrier semantics, AQE coalesce decisions, and stage-input-stats sampling are unchanged (each stage's barrier runs before its dependents are released). A stage failure skips its transitive dependents and surfaces the original error immediately. In-flight tasks stay bounded by the per-stage task counts and the workers' server-side task slots — no new concurrency bound. `0`/`false`/`off`/`no` restores the legacy strictly-sequential dispatch (also the automatic fallback while `OXIDANT_REOPT_JOIN_ORDER` is active, which splices the stage list mid-dispatch). Known anomaly (KAN-2 follow-up): at SF10, simple linear star-scan queries (Q63/Q13/Q96 class) run their leaf scan ~2x slower with it on (Q63 hot 10.0s vs 2.97s off) — mechanism not yet root-caused; the matrix-net win stays on (319s vs 369s), so the default is on. |
| `OXIDANT_S3_CACHE_DIR` | Optional | Local disk cache for S3 object reads (Databricks/Snowflake-style remote-table cache). When set (e.g. `/var/lib/oxidant/s3cache`), object reads through the bucket's object store materialize the file locally once (subject to `OXIDANT_S3_CACHE_MAX_OBJECT_BYTES`); repeat `get`/`get_ranges` calls (the parquet hot path) are served from NVMe. Entries revalidate by S3 `HEAD` (size + etag) after `OXIDANT_S3_CACHE_TTL_MS` (default 300000) so overwritten objects go stale at most that long; `OXIDANT_S3_CACHE_MAX_BYTES` (default 20 GiB) bounds the cache with LRU eviction. Writes invalidate the affected path; any cache error falls back to direct S3 reads. **Set on workers** (they run the fact scans). Leave unset/empty on the Connect driver — whole-object materialization of SF100 facts OOMs small drivers. |
| `OXIDANT_S3_CACHE_MAX_OBJECT_BYTES` | Optional | Per-object materialization cap (default `2147483648` = 2 GiB). Larger objects skip the cache and stay on ranged S3 GETs (`0` disables the cap). Required for single-file SF100 facts (~21 GiB `lineitem.parquet`). **KAN-153 note:** ranged GETs for oversized objects bypass this whole-object cache entirely (warm ≈ cold on every fact stage). A ranged/block-level cache for >2 GiB objects is a documented follow-up; until then prefer higher `OXIDANT_S3_RANGE_CONCURRENCY` + `OXIDANT_PARQUET_PREFETCH_BATCHES`, or split facts into <2 GiB files. |
| `OXIDANT_S3_RANGE_CONCURRENCY` | Optional | Max concurrent coalesced ranged GETs per `get_ranges` call (KAN-153). Default `32` (object_store's stock `coalesce_ranges` hard-caps at 10). Raise on high-bandwidth worker NICs when cold fact scans show high iowait / low decode CPU; set `10` to restore stock parallelism. Values `<1` fall back to the default. |
| `OXIDANT_S3_RANGE_COALESCE_BYTES` | Optional | Gap (bytes) under which adjacent ranges are merged into one GET before the concurrency fan-out (KAN-153). Default `1048576` = 1 MiB (same as object_store's `OBJECT_STORE_COALESCE_DEFAULT`). |
| `OXIDANT_PARQUET_PREFETCH_BATCHES` | Optional | DataFusion `maximum_buffered_record_batches_per_stream` (KAN-153 bounded readahead). Default `4` (stock DataFusion is `2`). Higher values keep more row-group ranged reads in flight behind the decoder; `1` disables readahead. Respect `OXIDANT_MEMORY_LIMIT_BYTES` — each buffered batch holds decoded Arrow. |
| `OXIDANT_DEFAULT_PARALLELISM` | Optional | Default local parallelism. In `spark server --mode local-cluster`, this is the default worker count when `--workers` is omitted (fallback `2`). |
| `OXIDANT_TASK_MAX_RETRIES` | Optional | Per-task retry attempts before alternate worker fallback (default `3`). |
| `OXIDANT_MEMORY_LIMIT_BYTES` | Optional | DataFusion spill pool size. Unset → auto-size from the **process** cgroup (`/proc/self/cgroup` → `memory.max` / v1 limit), then host RAM, × `OXIDANT_MEMORY_POOL_FRACTION` (default `0.7`). Explicit positive integer overrides; `0` keeps the unbounded pool (legacy local/test mode). When unset, also seeds the shuffle spill threshold at **¼ of the auto-sized pool** (so pool + shuffle cache do not both claim ~70% of RAM); an explicit positive value still seeds shuffle 1:1 unless `OXIDANT_SHUFFLE_SPILL_BYTES` is set. |
| `OXIDANT_MEMORY_POOL_FRACTION` | Optional | Fraction of detected RAM used when `OXIDANT_MEMORY_LIMIT_BYTES` is unset (f64 in (0, 1], default `0.7`). |
| `OXIDANT_COLOCATED_ENGINES` | Optional | When >1, divide the auto-sized pool by this count so in-process multi-worker modes do not each claim ~70% of RAM. Set automatically by `oxidant spark server --mode local-cluster`. |
| `OXIDANT_AUTO_BROADCAST_THRESHOLD_BYTES` | Optional | Cap for size-based dim replication (default `34359738368` = 32 GiB). Per query, every scanned table smaller than the largest **and** ≤ this cap is treated as fully replicated on every worker. `0` disables auto (override only). (v0.1.11 briefly defaulted to 4 GiB to shard SF100 mid facts — reverted: the planner's multi-sharded shape support lands with KAN-162 before the re-flip.) |
| `OXIDANT_REPLICATE_MAX_ROW_MULTIPLE` | Optional | Default **`4.0`** (on). When the catalog carries row counts (`numRows` / `spark.sql.statistics.numRows` table properties, read on the same `load_table` the byte sizing walk performs — no extra I/O), a byte-eligible replicate candidate whose row count exceeds multiple × the largest (by bytes) table's rows stays **sharded** (TPC-DS SF10 `inventory`: 117M rows in ~0.5 GB parquet vs the 14.4M-row byte-anchor `catalog_sales`). Unknown row counts keep the byte-only decision per table; `OXIDANT_REPLICATED_TABLES` still wins; `0`/negative/unparseable disables. On by default: the shuffle-join-chain planner re-roots a dim-leftmost inner chain at a sharded leaf (substituting folded-dim join keys through the query's equality web), so TPC-DS Q37/Q82's `item`-first comma chains plan distributed under the 2-sharded classification instead of falling back. (Q72's trailing replicated *outer* joins still fall back — a replicated LEFT JOIN after the last sharded step is not folded yet.) |
| `OXIDANT_REPLICATED_TABLES` | Optional | Comma-separated force-include override for replicate/broadcast dims. Auto-broadcast from file sizes is the primary path; operators should not need a bench-specific dim list. |
| `OXIDANT_SAMPLE_DATA_DIR` | Optional | Sample-data directory to register as the `samples` schema at startup (same as `--sample-data <DIR>`; the flag wins). Set to `/opt/oxidant/sample-data` in the OSS image, where the bundled TPC-H SF 0.01 tree (parquet/csv/delta/iceberg) is baked in. Best-effort: missing dirs or unreadable tables are logged and skipped, never a boot failure. Unset/empty ⇒ no `samples` schema. |
| `OXIDANT_CATALOG_CACHE_TTL_MS` | Optional | External-catalog table cache TTL (default `60000`; `0` revalidates every resolution). Past the TTL, a cached non-lakehouse table's metadata is re-read from the metastore and compared by fingerprint (location + format + schema + partition columns): unchanged keeps the provider, changed rebuilds it and bumps the catalog version (invalidating cached stage plans), and a revalidation error serves the cached provider rather than failing the query. `spark.catalog.refreshTable` evicts immediately but only reaches the driver — this TTL is what converges workers after an out-of-band metastore change (e.g. re-typed Glue tables). |

## Worker pod

| Variable | Required | Description |
|----------|----------|-------------|
| `OXIDANT_WORKER_TASK_SLOTS` | Optional | Advisory task concurrency per worker. The platform should set this to the CPU slots allocated to each worker pod; the current OSS worker treats one Flight request as one task and future schedulers will use this as the per-worker slot count. |
| `OXIDANT_SHUFFLE_SPILL_DIR` | Optional | Directory for spilled shuffle buckets when in-memory cache is full. |
| `OXIDANT_STAGE_OUTPUT_TTL_SECS` | Optional | Retention for cached stage outputs (default `3600`; `0` disables). Backstop for driver-side eviction — swept lazily on insert. |
| `OXIDANT_STAGE_TIMEOUT_MS` | Optional | Per-stage wall-clock limit (default `600000`). A stage that exceeds it errors out non-retryably so its task slot frees (KAN-17). ASG bootstrap sets `3600000` for SF100-sized cold scans. |
| `OXIDANT_STAGE_NO_PROGRESS_SECS` | Optional | No-progress watchdog budget per stage task (default `600`). The worker samples the task's batch heartbeat, the engine memory-pool activity, and the DataFusion + shuffle spill bytes roughly every `min(budget/4, 30s)`; if none change for the budget, the stage is aborted with an actionable KAN-47 error (possible DataFusion spill-pool deadlock) instead of burning the full wall-clock timeout silently — then retried **once** on the worker with the flipped join strategy (KAN-53: the wedge class is strategy-dependent) before the query fails. Every stage-task exit also logs a `Oxidant stage summary:` line (stage id, partitions, batches, spill bytes, duration, KAN-153 `s3_wait_ms`/`decode_ms`/`s3_bytes`/`cache_*` when the task installed IO counters, and the last-progress age on abort). |
| `OXIDANT_MEMORY_LIMIT_BYTES` | Optional | Same spill pool tuning as the driver (auto-sizes when unset; see driver table). |
| `OXIDANT_MEMORY_POOL_FRACTION` | Optional | Same auto-size fraction as the driver. |
| `OXIDANT_AUTO_BROADCAST_THRESHOLD_BYTES` | Optional | Same auto-broadcast cap as the driver (default 32 GiB). |
| `OXIDANT_REPLICATED_TABLES` | Optional | Same force-include override as the driver. Stage tickets also carry the driver's classified list as a task-local overlay so workers match planning without relying on this env. |
| `OXIDANT_DIM_CACHE_BYTES` | Optional | Process-global replicated-scan cache cap in bytes (default `2147483648` = 2 GiB; `0` disables). Replicated dims are decoded once per worker per data version and reused across stages and queries as MemTables, instead of re-reading + re-decoding every file from S3 per stage occurrence. Cache key includes a data fingerprint (pinned lakehouse snapshot, or sorted file path/size/mtime/etag), so a restated table never serves stale rows; LRU eviction by decoded Arrow bytes; counters (hits/misses/inserts/evictions/bytes) via `oxidant_loom::dim_cache::global().stats()`. |
| `OXIDANT_CATALOG_CACHE_TTL_MS` | Optional | Same external-catalog table cache TTL as the driver (default `60000`; `0` revalidates every resolution). This is the knob that converges workers after an out-of-band metastore change — `spark.catalog.refreshTable` only evicts on the driver. |
| `OXIDANT_PREFER_HASH_JOIN` | Optional | `auto` (default) \| `true` \| `false` (KAN-53). `auto` chooses the join strategy **per join** (KAN-142), matching Spark `preferSortMergeJoin=true` + AQE: with a bounded pool a query needing any strategy decision is re-planned once with the per-join rule — (1) build-side estimate over `min(pool × OXIDANT_HASH_JOIN_MAX_BUILD_FRACTION, spark_shj_cap)` ⇒ that join converts to sort-merge; (2) **no usable estimate ⇒ sort-merge**; (3) INNER-join build provably ≤ `OXIDANT_BROADCAST_JOIN_THRESHOLD_BYTES` (`Exact` bound, never `Inexact` — KAN-146) ⇒ broadcast (`CollectLeft`) hash join; (4) under budget ⇒ hash join; (5) runtime pool exhaustion ⇒ one sort-merge retry. A hash join the per-join rule cannot convert still falls back to the whole-stage sort-merge re-plan. `spark_shj_cap` is Spark's `canBuildLocalHashMap` rule (`OXIDANT_HASH_JOIN_PER_PARTITION_THRESHOLD_BYTES × shuffle_partitions / 2`). Without a bounded pool there is no budget, so plans keep their hash joins. `true`/`false` force one strategy session-wide; legacy `1`/`0`/`on`/`off` spellings are accepted. |
| `OXIDANT_HASH_JOIN_MAX_BUILD_FRACTION` | Optional | Per-join build-side budget as a fraction of `OXIDANT_MEMORY_LIMIT_BYTES` (f64 in (0, 1], default `0.25`). Combined with the Spark SHJ cap below; ineffective without a bounded pool. |
| `OXIDANT_HASH_JOIN_PER_PARTITION_THRESHOLD_BYTES` | Optional | Spark `autoBroadcastJoinThreshold` analogue for shuffled HashJoin admission (default `10485760` = 10 MiB). Effective HashJoin build budget is also capped at `threshold × shuffle_partitions / 2` so SF100 fact⋈fact joins prefer SMJ on EMR-class (`m8g.4xlarge`) workers. |
| `OXIDANT_BROADCAST_JOIN_THRESHOLD_BYTES` | Optional | Spark `autoBroadcastJoinThreshold` analogue for KAN-142 runtime broadcast conversion (default `10485760` = 10 MiB; `0` disables). Under `auto` with a bounded pool, a partitioned INNER hash join whose build side is PROVABLY at or below this threshold — an `Exact` row upper bound (stage-barrier measured or footer-exact statistics) seen through row-preserving wrappers, never an `Inexact` estimate (KAN-146: an `Inexact(min(l, r))` chain-intermediate estimate is phantom-small for FK star shapes, and broadcasting it coalesces the real fact-wide build to one partition) — clamped to the build budget, converts to a broadcast (`CollectLeft`) hash join, eliding both sides' shuffle repartitions within the stage. |
| `OXIDANT_SORT_MERGE_FALLBACK` | Optional | Default `false` (KAN-45). When `true`, queries whose hash-join build side exceeds the budget (or exhausts the pool at runtime) are re-planned with sort-merge joins even when `OXIDANT_PREFER_HASH_JOIN` forces a strategy. Unneeded under the default `auto` selection: the DataFusion 54.1.0 upgrade fixed the bounded-pool sort-merge deadlock (delta-io/delta-rs#4614) the KAN-45 default guarded against, so `auto` re-plans with sort-merge on its own. |
| `OXIDANT_PARQUET_SCAN_STATS` | Optional | Default `true` (KAN-8). Catalog Parquet/Delta/Iceberg scans attach exact parquet-footer row counts (metadata-cache-cached, so repeat scans cost no extra I/O) to the physical scan, so the `auto` join selection and DataFusion's own join ordering see real table sizes instead of unknown statistics — the unknown-estimate ⇒ sort-merge reroute now engages only for genuinely un-estimable build sides (join/aggregate outputs, CSV/JSON) or `0`/`false`/`off`/`no` here. |
| `OXIDANT_PARQUET_COLUMN_STATS` | Optional | Default `true` (KAN-143). On top of row counts, catalog scans attach per-column footer statistics (min/max, null counts, distinct counts where written) so DataFusion's join cardinality estimation sizes FK star joins at the fact cardinality instead of `Inexact(min(left, right))`. A per-file trust gate attaches column stats only when the declared schema resolves to the file's physical columns by exact name (post type-coercion): a case-mismatched column would otherwise come back `null_count == num_rows`, which the parquet opener reads as an all-null constant-column proof and literal-replaces real data away — such files keep row counts only. Genuinely absent columns (schema evolution) stay attached; their all-null stats are correct. `0`/`false`/`off`/`no` restores the row-counts-only shape; `OXIDANT_PARQUET_SCAN_STATS=0` still disables footer reads entirely. |
| `OXIDANT_DYN_FILTER_INLIST_MAX_DISTINCT` | Optional | Default `150` (= DataFusion stock), per build partition (KAN-2 R2). Hash-join dynamic filters are pinned **on** session-wide (`optimizer.enable_join_dynamic_filter_pushdown`): a hash join's build side (the replicated dim) publishes a runtime bounds+membership filter over the probe-side join keys, and the sharded fact's parquet scan absorbs it for row-group/page-index/bloom pruning. Up to this many distinct build-side join keys are pushed as a transparent `IN (SET)` membership the scan can also prune with; above the cap (or `OXIDANT_DYN_FILTER_INLIST_MAX_BYTES`) membership degrades to an opaque hash-table lookup that only filters decoded batches (min/max bounds still prune either way — and bounds carry the row-group pruning for clustered fact keys). **Do not raise the default lightly**: a 100k/32 MiB configuration was measured at SF10 to make TPC-DS Q4/Q11/Q18/Q21 3–6× slower (hash-set construction cost ≈ distinct × build partitions × joins, plus its memory footprint). |
| `OXIDANT_DYN_FILTER_INLIST_MAX_BYTES` | Optional | Default `131072` = 128 KiB (= DataFusion stock), per build partition (KAN-2 R2). Byte companion of `OXIDANT_DYN_FILTER_INLIST_MAX_DISTINCT` for the `IN (SET)` dynamic-filter strategy; worst-case extra build-side memory ≈ this × target partitions per join. See the regression note on the distinct knob before raising either. |
| `OXIDANT_SEMI_JOIN_FILTERS` | Optional | Default `true` (KAN-150); `0`/`false` disables. Cross-stage semi-join runtime filters for distributed shuffle-join chains: an INNER equijoin to a **filtered replicated** dimension injects an `<fact key> IN (SELECT <dim key> FROM <dim> WHERE <dim filters>)` conjunct into the sharded leaf's scan-stage SQL, so the dim's key set filters fact rows BEFORE the shuffle write. Exactness is structural — a leaf row matching no (filtered) dim row can never contribute to an INNER join's output, and the join still runs unaltered downstream; injection is skipped for outer/right-preserving boundaries (null-extended rows must survive), unfiltered dims (no provable selectivity — KAN-146), and volatile dim filters (the subquery re-evaluates them per leaf). Worker-side the subquery re-plans to a hash semi join building on the replicated dim, and DataFusion's dynamic filter (KAN-2 R2) reaches the fact's parquet scan for row-group/page-index pruning — the runtime filter crosses the stage boundary as SQL. |
| `OXIDANT_SEMI_JOIN_FILTER_MAX_DIM_ROWS` | Optional | Default `1000000` (KAN-160). Size gate on the `OXIDANT_SEMI_JOIN_FILTERS` admission: a filtered replicated dim injects its `IN (SELECT …)` conjunct only when the dim provider's `TableProvider::statistics()` reports an **Exact** row count at or below this cap (the unfiltered cardinality conservatively bounds the filtered key set every leaf-stage task hash-builds). `Inexact` counts are rejected (KAN-146 provable-admission discipline); `Absent` fails open so providers without statistics keep the KAN-150 behavior. Lakehouse tables report exact parquet-footer row counts at logical-plan time via `LakehouseTableProvider::statistics()` (same KAN-143 footer aggregate the physical scan attaches, computed once at provider construction through the shared metadata cache; a partially footer-readable table merges the readable object-store groups with the total degraded to `Inexact`). Dim-cache-served dims (`OXIDANT_DIM_CACHE_BYTES`, on by default) report the EXACT decoded row count at decode time, so the gate engages on the default path — `Absent` fail-open now covers only providers with no statistics at all. |

## Local-cluster mode

For single-host development and parity testing, the connect-server binary can embed a small Flight
cluster:

```bash
oxidant spark server --mode local-cluster --workers 4 --port 50051
```

`local-cluster` starts `N` in-process Arrow Flight workers on ephemeral `127.0.0.1` ports, then
starts the Spark Connect server in the same process. The CLI builds the generated worker endpoint
list, mirrors it into `OXIDANT_WORKERS` for helper paths, and passes the same list to
`oxidant-connect` `ServerConfig.workers`, so auto-splittable SQL routes through the distributed driver
without requiring a separate worker Deployment.

If `--workers` is omitted, the CLI uses `OXIDANT_DEFAULT_PARALLELISM`; if that is unset, it starts
two local workers. `local-cluster` is intended for local development and CI smoke tests. Production
clusters should continue to run one connect-server pod plus an autoscaled worker Deployment.

## Platform responsibilities (`oxidant-platform`)

- Deploy **one driver pod** + **N worker pods** from the OSS images above.
- Expose a headless Service for workers (`clusterIP: None`) so `OXIDANT_WORKER_SERVICE` DNS resolves pod IPs.
- HPA on worker Deployment using external metric `oxidant_pending_stage_tasks` (requires a metrics adapter), **or** proactive scale via the gateway `POST /clusters/{id}/scale` API when the driver sets `OXIDANT_AUTOSCALE=1`.
- IRSA / S3 credentials for data paths (engine talks to the Glue catalog in-process via `aws-sdk-glue`, using the standard AWS credential chain).

## Autoscaling (parallelism-driven)

| Variable | Required | Description |
|----------|----------|-------------|
| `OXIDANT_AUTOSCALE` | Optional | When `1`/`true`, compute a scale recommendation from shuffle partition count and peak stage task demand before each distributed query. |
| `OXIDANT_GATEWAY_URL` | With autoscale | Control-plane gateway base URL (e.g. `http://oxidant-gateway:8080`). |
| `OXIDANT_CLUSTER_ID` | With autoscale | Cluster id passed to `POST /clusters/{id}/scale`. |
| `OXIDANT_WORKER_MIN` | Optional | Lower bound for recommended worker count (default = current worker count). |
| `OXIDANT_WORKER_MAX` | Optional | Upper bound for recommended worker count (default = `min × 4`). |
| `OXIDANT_WORKER_MEMORY_LIMIT_BYTES` | Optional | Per-worker spill pool wired into provisioned worker manifests (gateway). |

Recommendation formula: `ceil(max(shuffle_partitions, peak_stage_tasks) / OXIDANT_WORKER_TASK_SLOTS)`, clamped to `[min, max]`, scale-up only.

## Driver status polling

`GET /api/status` on the driver's HTTP port (`4040`) is the engine-side signal for
auto-termination and autoscaling. It is served by the driver process itself — the connect
server — so a single-node cluster and a distributed cluster's driver expose it identically.

```sh
curl -s http://<driver>:4040/api/status -H "Authorization: Bearer $OXIDANT_STATUS_TOKEN"
```

- **Disabled by default.** Set `OXIDANT_STATUS_TOKEN` on the driver to enable it; without the
  token the route answers `404`. A wrong or missing bearer credential answers `401`.
- **Auto-termination:** `active_queries == 0` and `last_query_at` older than the idle budget
  (or `null` since boot, with `uptime_secs` past the grace period) means the cluster is idle.
- **Autoscaling:** `active_queries` is the concurrency signal; the per-query `rows`/`bytes` and
  `duration_ms` describe the shape of what is running. The parallelism-driven recommendation
  above stays the primary scale-up path.
- **Trust model:** plain HTTP — the token authenticates the caller, not the wire. Keep `4040`
  inside the cluster's private subnet / security group; the unauthenticated `/api/v1` routes on
  the same port already require this. Full reference: [api.md § Driver status](api.md#driver-status).

## Health checks

Workers respond to Arrow Flight `do_action` type `health`. The driver probes workers before scheduling and retries failed tasks on alternate healthy endpoints.

# Weft OSS engine — runtime contract for `weft-platform`

This document defines the environment contract between the **OSS engine images**
(`connect-server`, `worker`) and the **`weft-platform`** orchestrator (Terraform/Helm/HPA).

## Images

| Image | Entrypoint | Role |
|-------|------------|------|
| `connect-server` | `weft spark server --port 50051` | Spark Connect driver |
| `worker` | `weft worker --port 50561` | Arrow Flight worker |

## Driver (connect-server pod)

| Variable | Required | Description |
|----------|----------|-------------|
| `WEFT_WORKER_SERVICE` | Recommended | Headless Service DNS name for worker discovery (e.g. `weft-worker.weft-cl-abc.svc.cluster.local`). When set, the driver resolves live worker endpoints via DNS on each distributed query. |
| `WEFT_WORKERS` | Alternative | Comma-separated static `host:port` list (local dev / tests). Ignored when `WEFT_WORKER_SERVICE` resolves. |
| `WEFT_WORKER_PORT` | Optional | Flight port workers listen on (default `50561`). Used with `WEFT_WORKER_SERVICE`. |
| `WEFT_SHUFFLE_PARTITIONS` | Optional | Hash shuffle partition count (default: worker count). May exceed replica count. |
| `WEFT_DEFAULT_PARALLELISM` | Optional | Default local parallelism. In `spark server --mode local-cluster`, this is the default worker count when `--workers` is omitted (fallback `2`). |
| `WEFT_TASK_MAX_RETRIES` | Optional | Per-task retry attempts before alternate worker fallback (default `3`). |
| `WEFT_MEMORY_LIMIT_BYTES` | Recommended | DataFusion spill pool size (e.g. `26000000000` on a 32 GB node). |
| `WEFT_AUTO_BROADCAST_THRESHOLD_BYTES` | Optional | Cap for size-based dim replication (default `34359738368` = 32 GiB). Per query, every scanned table smaller than the largest **and** ≤ this cap is treated as fully replicated on every worker. `0` disables auto (override only). |
| `WEFT_REPLICATED_TABLES` | Optional | Comma-separated force-include override for replicate/broadcast dims. Auto-broadcast from file sizes is the primary path; operators should not need a bench-specific dim list. |

## Worker pod

| Variable | Required | Description |
|----------|----------|-------------|
| `WEFT_WORKER_TASK_SLOTS` | Optional | Advisory task concurrency per worker. The platform should set this to the CPU slots allocated to each worker pod; the current OSS worker treats one Flight request as one task and future schedulers will use this as the per-worker slot count. |
| `WEFT_SHUFFLE_SPILL_DIR` | Optional | Directory for spilled shuffle buckets when in-memory cache is full. |
| `WEFT_STAGE_OUTPUT_TTL_SECS` | Optional | Retention for cached stage outputs (default `3600`; `0` disables). Backstop for driver-side eviction — swept lazily on insert. |
| `WEFT_STAGE_TIMEOUT_MS` | Optional | Per-stage wall-clock limit (default `600000`). A stage that exceeds it errors out non-retryably so its task slot frees (KAN-17). |
| `WEFT_STAGE_NO_PROGRESS_SECS` | Optional | No-progress watchdog budget per stage task (default `600`). The worker samples the task's batch heartbeat, the engine memory-pool activity, and the DataFusion + shuffle spill bytes roughly every `min(budget/4, 30s)`; if none change for the budget, the stage is aborted with an actionable KAN-47 error (possible DataFusion spill-pool deadlock) instead of burning the full wall-clock timeout silently — then retried **once** on the worker with the flipped join strategy (KAN-53: the wedge class is strategy-dependent) before the query fails. Every stage-task exit also logs a `Weft stage summary:` line (stage id, partitions, batches, spill bytes, duration, and the last-progress age on abort). |
| `WEFT_MEMORY_LIMIT_BYTES` | Recommended | Same spill pool tuning as the driver. |
| `WEFT_AUTO_BROADCAST_THRESHOLD_BYTES` | Optional | Same auto-broadcast cap as the driver (default 32 GiB). |
| `WEFT_REPLICATED_TABLES` | Optional | Same force-include override as the driver. Stage tickets also carry the driver's classified list as a task-local overlay so workers match planning without relying on this env. |
| `WEFT_PREFER_HASH_JOIN` | Optional | `auto` (default) \| `true` \| `false` (KAN-53). `auto` chooses the join strategy **per query**, not per deployment. With a bounded pool the fallback order is: (1) build-side estimate (row count × row width, else scan byte size) over `WEFT_HASH_JOIN_MAX_BUILD_FRACTION` of the pool ⇒ sort-merge re-plan; (2) **no usable estimate ⇒ sort-merge re-plan** (safe default — an unaccounted hash build can OOM-kill the worker before the runtime retry fires; SF10 TPC-H Q16/Q21, TPC-DS Q11, KAN-57); (3) positive estimate under budget ⇒ hash join; (4) runtime pool exhaustion under a hash join ⇒ one sort-merge retry (backstop for under-estimates). Without a bounded pool there is no budget, so plans keep their hash joins. `true`/`false` force one strategy session-wide (the pre-KAN-53 behavior); legacy `1`/`0`/`on`/`off` spellings are accepted. |
| `WEFT_HASH_JOIN_MAX_BUILD_FRACTION` | Optional | Per-join build-side budget as a fraction of `WEFT_MEMORY_LIMIT_BYTES` (f64 in (0, 1], default `0.25`). Drives the `auto` plan-time selection (and the KAN-45 opt-in fallback); ineffective without a bounded pool. |
| `WEFT_SORT_MERGE_FALLBACK` | Optional | Default `false` (KAN-45). When `true`, queries whose hash-join build side exceeds the budget (or exhausts the pool at runtime) are re-planned with sort-merge joins even when `WEFT_PREFER_HASH_JOIN` forces a strategy. Unneeded under the default `auto` selection: the DataFusion 54.1.0 upgrade fixed the bounded-pool sort-merge deadlock (delta-io/delta-rs#4614) the KAN-45 default guarded against, so `auto` re-plans with sort-merge on its own. |

## Local-cluster mode

For single-host development and parity testing, the connect-server binary can embed a small Flight
cluster:

```bash
weft spark server --mode local-cluster --workers 4 --port 50051
```

`local-cluster` starts `N` in-process Arrow Flight workers on ephemeral `127.0.0.1` ports, then
starts the Spark Connect server in the same process. The CLI builds the generated worker endpoint
list, mirrors it into `WEFT_WORKERS` for helper paths, and passes the same list to
`weft-connect` `ServerConfig.workers`, so auto-splittable SQL routes through the distributed driver
without requiring a separate worker Deployment.

If `--workers` is omitted, the CLI uses `WEFT_DEFAULT_PARALLELISM`; if that is unset, it starts
two local workers. `local-cluster` is intended for local development and CI smoke tests. Production
clusters should continue to run one connect-server pod plus an autoscaled worker Deployment.

## Platform responsibilities (`weft-platform`)

- Deploy **one driver pod** + **N worker pods** from the OSS images above.
- Expose a headless Service for workers (`clusterIP: None`) so `WEFT_WORKER_SERVICE` DNS resolves pod IPs.
- HPA on worker Deployment using external metric `weft_pending_stage_tasks` (requires a metrics adapter), **or** proactive scale via the gateway `POST /clusters/{id}/scale` API when the driver sets `WEFT_AUTOSCALE=1`.
- IRSA / S3 credentials for data paths (engine uses AWS CLI in the connect-server image for Glue catalog).

## Autoscaling (parallelism-driven)

| Variable | Required | Description |
|----------|----------|-------------|
| `WEFT_AUTOSCALE` | Optional | When `1`/`true`, compute a scale recommendation from shuffle partition count and peak stage task demand before each distributed query. |
| `WEFT_GATEWAY_URL` | With autoscale | Control-plane gateway base URL (e.g. `http://weft-gateway:8080`). |
| `WEFT_CLUSTER_ID` | With autoscale | Cluster id passed to `POST /clusters/{id}/scale`. |
| `WEFT_WORKER_MIN` | Optional | Lower bound for recommended worker count (default = current worker count). |
| `WEFT_WORKER_MAX` | Optional | Upper bound for recommended worker count (default = `min × 4`). |
| `WEFT_WORKER_MEMORY_LIMIT_BYTES` | Optional | Per-worker spill pool wired into provisioned worker manifests (gateway). |

Recommendation formula: `ceil(max(shuffle_partitions, peak_stage_tasks) / WEFT_WORKER_TASK_SLOTS)`, clamped to `[min, max]`, scale-up only.

## Health checks

Workers respond to Arrow Flight `do_action` type `health`. The driver probes workers before scheduling and retries failed tasks on alternate healthy endpoints.

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

## Worker pod

| Variable | Required | Description |
|----------|----------|-------------|
| `WEFT_WORKER_TASK_SLOTS` | Optional | Advisory task concurrency per worker. The platform should set this to the CPU slots allocated to each worker pod; the current OSS worker treats one Flight request as one task and future schedulers will use this as the per-worker slot count. |
| `WEFT_SHUFFLE_SPILL_DIR` | Optional | Directory for spilled shuffle buckets when in-memory cache is full. |
| `WEFT_MEMORY_LIMIT_BYTES` | Recommended | Same spill pool tuning as the driver. |

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
- HPA on worker Deployment (CPU-based or custom metrics); the engine picks up scaled workers on the **next** distributed query via DNS refresh.
- IRSA / S3 credentials for data paths (engine uses AWS CLI in the connect-server image for Glue catalog).

## Health checks

Workers respond to Arrow Flight `do_action` type `health`. The driver probes workers before scheduling and retries failed tasks on alternate healthy endpoints.

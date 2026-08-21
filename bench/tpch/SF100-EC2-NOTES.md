# SF100 EC2 run notes

Operational notes for SF100 honesty runs against Glue `tpch_sf100` / `tpcds_sf100`.
Keep in sync with [`docs/distributed-ec2.md`](../../docs/distributed-ec2.md) § SF100 and
the Spark EMR memory-parity work (prefer SMJ + ≥200 shuffle partitions).

## Canonical topology (Spark EMR-class)

| Role | Instance | Disk |
|------|----------|------|
| Driver | `c6g.xlarge` (4 vCPU / 8 GiB) | 100 GiB gp3 root |
| Workers | 2× `m8g.4xlarge` (16 vCPU / 64 GiB) | 500 GiB gp3 spill each |

Engine defaults that make this topology viable (do not revert without re-validation):

- `OXIDANT_PREFER_HASH_JOIN=auto` + Spark `canBuildLocalHashMap` budget
  (`OXIDANT_HASH_JOIN_PER_PARTITION_THRESHOLD_BYTES` × shuffle partitions / 2)
- Shuffle default `max(200, worker_vcpus)` — empty CFN must **not** stamp `WorkerCount`
- `OXIDANT_AQE` default on (64 MiB advisory coalesce)
- Driver: no `OXIDANT_MEMORY_LIMIT_BYTES`, no S3 cache; `OXIDANT_DISTRIBUTED_STRICT=1`
- Workers: bounded pool + shuffle spill; `OXIDANT_S3_CACHE_MAX_OBJECT_BYTES=2Gi`

## Historical failure modes (pre-parity)

| Failure | Root cause | Fix |
|---------|------------|-----|
| Driver OOM on Q3 (`c6g.xlarge`) | Worker 40 Gi `MEMORY_LIMIT` stamped on driver | Bootstrap applies pool only on workers |
| Worker cgroup OOM on multi-fact joins (`m8g.4xlarge`) | Non-spillable HashJoin + 2-bucket shuffle | SMJ preference + ≥200 partitions |
| Stage timeout / hang | Whole-object S3 cache of 21 GiB `lineitem` | Max-object 2 GiB → ranged GETs |
| Cascaded `NO_ACTIVE_SESSION` | No reconnect after driver death | `bench/tpch/run-ec2-connect.py` / `bench/tpcds/run-ec2-connect.py` |
| **Driver-local SF100** (workers ~0% CPU, driver NetworkIn multi‑GiB) | Empty `OXIDANT_WORKERS` (Route53 race) + soft local fallback | **ASG private-IP membership** (no Route53); pin `OXIDANT_WORKERS`; `OXIDANT_DISTRIBUTED_STRICT=1` |

### What Spark EMR / Databricks do (and we must match)

- **Driver ≠ executor.** EMR master / Databricks driver coordinates; cores / worker nodes own scans, shuffles, and joins. Publishing “distributed” numbers from a driver that scanned S3 alone is invalid.
- **Cluster manager hands out executor IPs.** YARN/EMR and Databricks do **not** rely on a shared DNS name for executors. Oxidant EC2 uses the same idea: IAM `DescribeAutoScalingGroups` + `DescribeInstances` → private `ip:50561` list in `OXIDANT_WORKERS`. No Route53; no UDP broadcast.
- **Fail closed.** Missing executors is an error (or a blocked cluster), not a silent single-node run. Use `--distributed-strict true` on every honesty deploy.

**Do not restart TPC-H/TPC-DS on a stack that fails the distribution smoke check** in [`docs/distributed-ec2.md`](../../docs/distributed-ec2.md) § Driver vs workers.

## Dataset

Prefer **many** Snappy Parquet files (or Iceberg) under `warehouse/`. A single
~15–21 GiB `store_sales.parquet` / `lineitem.parquet` makes file-level sharding
assign the whole fact to **one** worker (the other sits idle). That looks like
“workers not distributed” in CloudWatch (especially with S3 gateway endpoints,
where S3 bytes often do **not** appear in instance `NetworkIn`).

### Distribution debug checklist (2026-08-10)

1. `curl localhost:4040/api/v1/cluster/status` → `"mode":"distributed"` and two `http://…:50561` workers.
2. `journalctl -u oxidant-worker` → `Oxidant stage summary:` with `num_partitions≥200`.
3. For single-file facts: **one** worker shows multi-second `duration_ms` on stage 0; the other stays `batches=0`. That is expected until the dataset is split.
4. Do not use driver-vs-worker `NetworkIn` alone when S3 is via a VPC gateway endpoint.

## Harness

```sh
python3 bench/tpch/run-ec2-connect.py \
  --endpoint sc://$DRIVER_IP:50051 \
  --glue-database tpch_sf100 \
  --tries 1 \
  --out bench/tpch/results/tpch-sf100-ec2-emr-parity.json

python3 bench/tpcds/run-ec2-connect.py \
  --endpoint sc://$DRIVER_IP:50051 \
  --glue-database tpcds_sf100 \
  --tries 1 \
  --out bench/tpcds/results/tpcds-sf100-ec2-emr-parity.json
```

While multi-fact queries run, workers must show `Oxidant stage summary:` lines and
non-trivial CPU — idle workers with climbing driver RSS means local fallback; stop and
debug distribution under `OXIDANT_DISTRIBUTED_STRICT=1`.

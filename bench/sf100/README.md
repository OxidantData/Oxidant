# SF100 on S3 + Glue + EKS

Publish TPC-H / TPC-DS **SF100** against Weft:

1. Dump DuckDB’s pre-built SF100 databases to
   `s3://weft-artifacts-<account>/{tpch,tpcds}-sf100/<table>/` as Parquet.
2. Register Glue databases `tpch_sf100` / `tpcds_sf100` (empty Columns — Weft
   infers Parquet schema).
3. Run queries either:
   - **Direct Spark Connect** (preferred for this Helm data-plane chart):
     `bench/sf100/run-spark-connect.py` → `sc://host:50051`
   - **Gateway HTTP** (private control-plane, not in this chart):
     `bench/sf100/run-via-gateway.py` → `POST /api/sql`
4. Compare Parquet / Iceberg / Delta result checksums once lakehouse formats land.

**Today the dump/register path is Parquet-only.** Iceberg and Delta Glue tables are
landing on a separate branch — do not claim those formats work from this harness yet.

## Paths

| Artifact | Location |
|----------|----------|
| Parquet | `s3://weft-artifacts-810738286322/tpch-sf100/`, `…/tpcds-sf100/` |
| Glue | `tpch_sf100.*`, `tpcds_sf100.*` |
| Query SQL | `SELECT … FROM glue.tpch_sf100.lineitem` |
| IRSA | annotate the chart `serviceAccount` (see `values-sf100.yaml` placeholders) |

Existing `glue.tpch.*` (~SF10, ~60 M lineitem rows) is left untouched.

## Dump (AMD EC2)

```sh
# uploads scripts to S3, launches c6a.4xlarge (400 GB), self-terminates when done
./bench/sf100/launch-dump-ec2.sh

# watch
aws s3 cp s3://weft-artifacts-810738286322/bench/sf100/dump.log - 
aws s3 ls s3://weft-artifacts-810738286322/bench/sf100/DUMP_COMPLETE
```

Then register Glue from a principal that can mutate the catalog (the dump
instance role is S3-only):

```sh
SUITE=tpch  SF=100 ./bench/sf100/register-glue.sh
SUITE=tpcds SF=100 ./bench/sf100/register-glue.sh
```

## Helm data-plane (sharding contract)

Workers are a **StatefulSet** (`weft-worker-0`, `weft-worker-1`, …). Each worker pod
must have:

| Env | Source | Why |
|-----|--------|-----|
| `WEFT_WORKER_COUNT` | `worker.replicas` | Shard modulus; must be fixed |
| `WEFT_POD_NAME` | `fieldRef: metadata.name` | Trailing ordinal → shard index |

`ShardAssignment::from_env` (`crates/weft-loom/src/shard.rs`) returns `None` when these
are missing, and **every worker then reads the whole table** (silent duplication).
Autoscaling is **chart-rejected** (`helm template … --set worker.autoscaling.enabled=true`
fails) because an HPA that changes replica count while `WEFT_WORKER_COUNT` stays at
render-time is incoherent.

SF100 topology overlay:

```sh
helm upgrade --install weft deploy/helm/weft \
  --namespace weft \
  -f deploy/helm/weft/values-sf100.yaml \
  --set connect.image=$CONNECT_REF \
  --set worker.image=$WORKER_REF
```

See [`docs/distributed-k8s.md`](../../docs/distributed-k8s.md).

## Run via Spark Connect (direct)

Requires `pyspark-client>=4.0` (pure Python, no JVM). The connect pod should have
`WEFT_DISTRIBUTED_STRICT=1` (SF100 overlay sets `connect.distributedStrict: true`).

```sh
pip install "pyspark-client>=4.0"
kubectl -n weft port-forward svc/weft-connect 50051:50051

WEFT_DISTRIBUTED_STRICT=1 python3 bench/sf100/run-spark-connect.py \
  --endpoint sc://localhost:50051 \
  --suite tpcds --sf 100 --glue-db tpcds_sf100 \
  --only 1,3,6 \
  --json /tmp/tpcds-sc.jsonl

# Full sweep (resumable JSONL)
WEFT_DISTRIBUTED_STRICT=1 python3 bench/sf100/run-spark-connect.py \
  --endpoint sc://localhost:50051 \
  --suite tpcds --sf 100 --glue-db tpcds_sf100 \
  --json results/tpcds-sf100-parquet.jsonl --resume
```

Each JSONL record includes `wall_s` / `hot_s`, `row_count`, and a SHA-256 `checksum`
of the collected rows for cross-format comparison.

SF≥100 refuses to start without `WEFT_DISTRIBUTED_STRICT=1` or `--strict`.

## Run via gateway / EKS

**Default:** harnesses refuse SF100 unless you set an explicit mode (cost + honesty guard).

| Mode | Flag | Workers | Use |
|------|------|---------|-----|
| Distributed re-measure | `DISTRIBUTED_SF100=1` | `worker_min=worker_max=N` (default N=2) | Comparable multi-worker SF100 |
| Driver-only profiling | `ALLOW_SINGLE_NODE_GATE=1` | scaled to 0 | Legacy single-node; not publishable |

```sh
DISTRIBUTED_SF100=1 WORKER_MIN=2 WORKER_MAX=2 ./bench/sf100/run-time-gate.sh
```

```sh
python3 bench/sf100/run-via-gateway.py \
  --suite tpch --sf 100 --glue-db tpch_sf100 \
  --create-cluster --distributed --worker-count 2 --worker-size xlarge \
  --json site/src/data/tpch.json
```

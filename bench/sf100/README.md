# SF100 on S3 + Glue (+ EC2 CF or EKS)

Publish TPC-H / TPC-DS **SF100** against Weft:

### Canonical compute topology

Keep published SF100 numbers on this shape (EKS Helm overlay **or** EC2 ASG — same
iron). Full EC2 deploy recipe:
[`docs/distributed-ec2.md` § SF100 topology](../../docs/distributed-ec2.md#sf100-topology-canonical).

| Role | Count | Instance | Disk |
|------|------:|----------|------|
| Driver / Connect | 1 | `c6g.xlarge` (4 vCPU / 8 GiB, arm64) | 100 GiB gp3 root |
| Workers | 2 (min=max=2) | `m8g.8xlarge` (32 vCPU / 128 GiB, arm64) | 500 GiB gp3 spill each |

EKS: [`deploy/helm/weft/values-sf100.yaml`](../../deploy/helm/weft/values-sf100.yaml).
EC2 remeasure (driver IP only — **no NLB**):
`STACK=weft-sf100 SUITE=all ./bench/sf100/remeasure-distributed.sh`.

1. Dump DuckDB’s pre-built SF100 databases to
   `s3://weft-artifacts-<account>/{tpch,tpcds}-sf100/<table>/` as Parquet.
2. Register Glue databases `tpch_sf100` / `tpcds_sf100` (empty Columns — Weft
   infers Parquet schema).
3. *(Optional lakehouse)* Lay Iceberg + Delta **metadata** over the same Parquet
   objects and register sibling Glue databases (see [Lakehouse formats](#lakehouse-formats-iceberg--delta)).
4. Run queries either:
   - **Direct Spark Connect** (preferred for this Helm data-plane chart):
     `bench/sf100/run-spark-connect.py` → `sc://host:50051`
   - **Gateway HTTP** (private control-plane, not in this chart):
     `bench/sf100/run-via-gateway.py` → `POST /api/sql`
5. Compare Parquet / Iceberg / Delta result checksums.
6. Write `site/src/data/{tpch,tpcds}.json` for the Performance page.

**Status:** dataset generation for all three formats works (step 3). *Reading* Iceberg
and Delta from S3 is landing on `vamzi/lakehouse-s3-formats`; until that merges, only
the Parquet Glue databases are queryable end to end - do not publish Iceberg/Delta
numbers from this harness yet.

## Paths

| Artifact | Location |
|----------|----------|
| Parquet | `s3://weft-artifacts-810738286322/tpch-sf100/`, `…/tpcds-sf100/` |
| Glue (Parquet) | `tpch_sf100.*`, `tpcds_sf100.*` |
| Glue (Iceberg) | `tpch_sf100_iceberg.*`, `tpcds_sf100_iceberg.*` |
| Glue (Delta) | `tpch_sf100_delta.*`, `tpcds_sf100_delta.*` |
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
| `TMPDIR` | `/var/lib/weft/spill` | Threshold spill root on the PVC |
| `WEFT_SHUFFLE_SPILL_BYTES` | `worker.shuffleSpillBytes` | Spill threshold (not force-spill) |
| `WEFT_MEMORY_LIMIT_BYTES` | `worker.memoryLimitBytes` | Fallback threshold / DF pool |

Do **not** set `WEFT_SHUFFLE_SPILL_DIR` for benchmarks (`forceShuffleSpill` is debug-only
and forces every shuffle bucket to disk).

`ShardAssignment::from_env` (`crates/weft-loom/src/shard.rs`) returns `None` when the
sharding env is missing, and **every worker then reads the whole table** (silent
duplication). Autoscaling is **chart-rejected**. A **not-Ready** worker is a different
failure: remaining workers still only read their own shard → silent row loss. The chart
adds Flight readiness probes; the Spark Connect runner preflights Ready pod count.

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
  --namespace weft --worker-count 2 \
  --only 1,3,6 \
  --json /tmp/tpcds-sc.jsonl

# Full sweep (resumable JSONL)
WEFT_DISTRIBUTED_STRICT=1 python3 bench/sf100/run-spark-connect.py \
  --endpoint sc://localhost:50051 \
  --suite tpcds --sf 100 --glue-db tpcds_sf100 \
  --namespace weft --worker-count 2 \
  --json results/tpcds-sf100-parquet.jsonl --resume
```

Preflight (default for SF≥100): refuses to start unless Ready `app=weft-worker` pods
equal `--worker-count` (or `$WEFT_WORKER_COUNT`). Override only with
`--skip-worker-preflight` (unsafe).

The same check also brackets **every query**, before and after. The one-shot preflight
only proves the cluster was healthy at t=0; a worker that restarts or fails a probe
mid-sweep drops out of headless DNS, and the survivors keep sharding over the
render-time `WEFT_WORKER_COUNT`, so every later query silently returns a subset —
faster, and marked `ok`. A resumable sweep is one long process, so one blip would
otherwise poison the rest of the run. On mismatch the runner aborts; fix the
StatefulSet and re-run with `--resume` (completed queries are kept). Successful
records carry `workers_ready` so results stay auditable after the fact.

Each JSONL record includes `wall_s` / `hot_s`, `row_count`, and a SHA-256 `checksum`
of the collected rows for cross-format comparison.

SF≥100 refuses to start without `WEFT_DISTRIBUTED_STRICT=1` or `--strict`.

## Lakehouse formats (Iceberg + Delta)

**One Parquet copy, three catalog entries.** Iceberg and Delta are metadata over the
same objects `dump-to-s3.sh` already wrote — so format timing comparisons are not
confounded by different row-group layouts. Weft cannot write Iceberg/Delta (Glue
`build_table_input` rejects those write targets); generation is intentionally
out-of-band via PyIceberg / delta-rs.

| Step | Command |
|------|---------|
| 0. Parquet dump | `./bench/sf100/dump-to-s3.sh` (or EC2 launcher) |
| 1. Iceberg + Delta metadata | see below |
| 2. Verify | `COUNT(*)` via gateway against each Glue DB |
| 3. Tear down | `./bench/sf100/teardown-lakehouse.sh` |

```sh
python3 -m venv .venv && . .venv/bin/activate
pip install -r bench/sf100/requirements.txt

# Always dry-run first (no AWS writes)
python3 bench/sf100/build-lakehouse.py \
  --suite tpcds --sf 100 \
  --source-prefix s3://weft-artifacts-$ACCOUNT/tpcds-sf100 \
  --formats iceberg,delta \
  --dry-run

# Operator run (creates metadata + Glue DBs)
python3 bench/sf100/build-lakehouse.py \
  --suite tpcds --sf 100 \
  --source-prefix s3://weft-artifacts-$ACCOUNT/tpcds-sf100 \
  --iceberg-warehouse s3://weft-artifacts-$ACCOUNT/tpcds-sf100-iceberg \
  --formats iceberg,delta
```

**Glue parameters (paired with Weft `detect_format` on `vamzi/lakehouse-s3-formats`):**

| Format | Glue DB | Parameters set |
|--------|---------|----------------|
| Parquet | `{suite}_sf{SF}` | `classification=parquet` |
| Iceberg | `{suite}_sf{SF}_iceberg` | `table_type=ICEBERG`, `metadata_location=…` (wins detector) |
| Delta | `{suite}_sf{SF}_delta` | `classification=delta`, `provider=delta`, `spark.sql.sources.provider=delta` |

Harness tip: point `--glue-db` at one of the three DB names above.

**Cost (order of magnitude, us-west-2):** SF100 Parquet for TPC-DS is on the order of
**~100–300 GiB** (exact size after dump — check `aws s3 ls --summarize`). Storing one
copy at ~$0.023/GB-month is roughly **$3–$7/month**; Iceberg metadata + Delta `_delta_log`
add little. Idle datasets still bill — tear down when done:

```sh
SUITE=tpcds SF=100 ./bench/sf100/teardown-lakehouse.sh          # Glue only
SUITE=tpcds SF=100 DELETE_DATA=1 ./bench/sf100/teardown-lakehouse.sh  # + purge metadata
```

**Local rehearsal (no AWS):**

```sh
./bench/sf100/rehearse-local.sh   # skips cleanly if Python deps missing
```

**Shared prefix hazard (Parquet ↔ Delta):** `convert_to_deltalake` writes `_delta_log/`
*inside* the Parquet table directory by design (one data copy). Commit-0 only has JSON
actions, which a `.parquet` extension filter ignores. Delta **checkpoints** are
`_delta_log/*.checkpoint.parquet` — those *do* match a naive Parquet lister
(`ListingOptions.with_file_extension(".parquet")`) and will fail or corrupt a Parquet
benchmark scan once anything optimizes/writes the Delta table. The local rehearsal plants
a dummy checkpoint and checks both: naive listing is contaminated; listing that excludes
`_delta_log/` is not. Engine fix (separate worker): skip `_delta_log` in Parquet listings.
Alternative if we ever need physical isolation: Delta allows absolute paths in `add.path`,
so `_delta_log` can live in a sibling prefix referencing the same data files — not
implemented here.

Verified library APIs (pinned in `requirements.txt`): `pyiceberg==0.11.1`
`Table.add_files`, `deltalake==1.6.2` `convert_to_deltalake(..., mode='ignore')`.

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

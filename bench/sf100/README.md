# SF100 on S3 + Glue + EKS

Publish TPC-H / TPC-DS **SF100** against the live Weft platform:

1. Dump DuckDB’s pre-built SF100 databases to
   `s3://weft-artifacts-<account>/{tpch,tpcds}-sf100/<table>/` as Parquet.
2. Register Glue databases `tpch_sf100` / `tpcds_sf100` (empty Columns — Weft
   infers Parquet schema).
3. *(Optional lakehouse)* Lay Iceberg + Delta **metadata** over the same Parquet
   objects and register sibling Glue databases (see [Lakehouse formats](#lakehouse-formats-iceberg--delta)).
4. Run queries through the control-plane gateway (`POST /api/sql`) with the
   existing `glue` connection, optionally pinned to an EKS compute cluster.
5. Write `site/src/data/{tpch,tpcds}.json` for the Performance page.

## Paths

| Artifact | Location |
|----------|----------|
| Parquet | `s3://weft-artifacts-810738286322/tpch-sf100/`, `…/tpcds-sf100/` |
| Glue (Parquet) | `tpch_sf100.*`, `tpcds_sf100.*` |
| Glue (Iceberg) | `tpch_sf100_iceberg.*`, `tpcds_sf100_iceberg.*` |
| Glue (Delta) | `tpch_sf100_delta.*`, `tpcds_sf100_delta.*` |
| Query SQL | `SELECT … FROM glue.tpch_sf100.lineitem` |
| IRSA | `weft-cluster-irsa` already allows read on the artifacts bucket + Glue |

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

Verified library APIs (pinned in `requirements.txt`): `pyiceberg==0.11.1`
`Table.add_files`, `deltalake==1.6.2` `convert_to_deltalake(..., mode='ignore')`.

## Run via gateway / EKS

**Default:** harnesses refuse SF100 unless you set an explicit mode (cost + honesty guard).

| Mode | Flag | Workers | Use |
|------|------|---------|-----|
| Distributed re-measure | `DISTRIBUTED_SF100=1` | `worker_min=worker_max=N` (default N=2) | Comparable multi-worker SF100 |
| Driver-only profiling | `ALLOW_SINGLE_NODE_GATE=1` | scaled to 0 | Legacy single-node; not publishable |

Smoke (multi-worker, cheap — see platform `docs/DISTRIBUTED_DEPLOY.md`):

```sh
# After engine + platform images are deployed
curl -sS -X POST "$GW/api/clusters" ... \
  -d '{"name":"dist-smoke","worker_size":"small","worker_min":2,"worker_max":2}'
```

Distributed SF100 (only after smoke passes):

```sh
DISTRIBUTED_SF100=1 WORKER_MIN=2 WORKER_MAX=2 ./bench/sf100/run-time-gate.sh
```

Ad-hoc query run with workers:

```sh
python3 bench/sf100/run-via-gateway.py \
  --suite tpch --sf 100 --glue-db tpch_sf100 \
  --create-cluster --distributed --worker-count 2 --worker-size xlarge \
  --json site/src/data/tpch.json
```

Driver-only (not comparable — requires explicit opt-in):

```sh
ALLOW_SINGLE_NODE_GATE=1 ./bench/sf100/run-time-gate.sh
```

```sh
# needs kubectl context on weft-platform (reads admin password from weft-gateway-jwt)
python3 bench/sf100/run-via-gateway.py \
  --suite tpch --sf 100 --glue-db tpch_sf100 \
  --create-cluster --worker-size xlarge \
  --json site/src/data/tpch.json

python3 bench/sf100/run-via-gateway.py \
  --suite tpcds --sf 100 --glue-db tpcds_sf100 \
  --cluster-id <id> \
  --json site/src/data/tpcds.json
```

Smoke (after Glue is up):

```sh
curl -sS -X POST "$GW/api/sql" -H "authorization: Bearer $TOKEN" \
  -H 'content-type: application/json' \
  -d '{"sql":"SELECT count(*) FROM glue.tpch_sf100.nation","no_limit":true}'
```

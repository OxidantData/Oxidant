# SF100 on S3 + Glue + EKS

Publish TPC-H / TPC-DS **SF100** against the live Weft platform:

1. Dump DuckDB’s pre-built SF100 databases to
   `s3://weft-artifacts-<account>/{tpch,tpcds}-sf100/<table>/` as Parquet.
2. Register Glue databases `tpch_sf100` / `tpcds_sf100` (empty Columns — Weft
   infers Parquet schema).
3. Run queries through the control-plane gateway (`POST /api/sql`) with the
   existing `glue` connection, optionally pinned to an EKS compute cluster.
4. Write `site/src/data/{tpch,tpcds}.json` for the Performance page.

## Paths

| Artifact | Location |
|----------|----------|
| Parquet | `s3://weft-artifacts-810738286322/tpch-sf100/`, `…/tpcds-sf100/` |
| Glue | `tpch_sf100.*`, `tpcds_sf100.*` |
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

## Run via gateway / EKS

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

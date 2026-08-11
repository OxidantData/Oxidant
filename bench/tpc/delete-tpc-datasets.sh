#!/usr/bin/env bash
# Delete Oxidant TPC test datasets from S3 + Glue (sf1/sf10/sf100/sf300/sf1000).
#
# Usage:
#   BUCKET=weft-artifacts-… ./bench/tpc/delete-tpc-datasets.sh
#   BUCKET=… DRY_RUN=1 ./bench/tpc/delete-tpc-datasets.sh
set -euo pipefail

BUCKET="${BUCKET:?set BUCKET=…}"
REGION="${AWS_REGION:-${AWS_DEFAULT_REGION:-us-west-2}}"
DRY_RUN="${DRY_RUN:-0}"

PREFIXES=(
  tpcds-sf1 tpcds-sf10 tpcds-sf100 tpcds-sf300 tpcds-sf1000
  tpcds-sf1-iceberg tpcds-sf10-iceberg tpcds-sf100-iceberg
  tpcds-sf1-delta tpcds-sf10-delta tpcds-sf100-delta
  tpch-sf1 tpch-sf10 tpch-sf100 tpch-sf300 tpch-sf1000
  tpch-sf1-iceberg tpch-sf10-iceberg tpch-sf100-iceberg
  tpch-sf1-delta tpch-sf10-delta tpch-sf100-delta
  tpch
)

# Also wipe warehouse dirs that hold Iceberg/Delta catalog data for TPC DBs.
WAREHOUSE_DBS=(
  tpcds_sf1 tpcds_sf10 tpcds_sf100 tpcds_sf300 tpcds_sf1000
  tpcds_sf1_iceberg tpcds_sf10_iceberg tpcds_sf100_iceberg
  tpcds_sf1_delta tpcds_sf10_delta tpcds_sf100_delta
  tpch_sf1 tpch_sf10 tpch_sf100 tpch_sf300 tpch_sf1000
  tpch_sf1_iceberg tpch_sf10_iceberg tpch_sf100_iceberg
  tpch_sf1_delta tpch_sf10_delta tpch_sf100_delta
  tpch
)

GLUE_DBS=(
  tpcds_sf1 tpcds_sf10 tpcds_sf100 tpcds_sf300 tpcds_sf1000
  tpcds_sf1_iceberg tpcds_sf10_iceberg tpcds_sf100_iceberg
  tpcds_sf1_delta tpcds_sf10_delta tpcds_sf100_delta
  tpch_sf1 tpch_sf10 tpch_sf100 tpch_sf300 tpch_sf1000
  tpch_sf1_iceberg tpch_sf10_iceberg tpch_sf100_iceberg
  tpch_sf1_delta tpch_sf10_delta tpch_sf100_delta
  tpch
)

_rm_prefix() {
  local p="$1"
  if aws s3 ls "s3://${BUCKET}/${p}/" --region "$REGION" >/dev/null 2>&1; then
    echo "[delete] s3://${BUCKET}/${p}/"
    if [[ "$DRY_RUN" == "1" ]]; then
      return
    fi
    aws s3 rm "s3://${BUCKET}/${p}/" --recursive --region "$REGION"
  else
    echo "[delete] skip missing s3://${BUCKET}/${p}/"
  fi
}

_drop_glue_db() {
  local db="$1"
  if ! aws glue get-database --name "$db" --region "$REGION" >/dev/null 2>&1; then
    echo "[delete] skip missing glue db $db"
    return
  fi
  echo "[delete] glue database $db"
  if [[ "$DRY_RUN" == "1" ]]; then
    return
  fi
  # Drop all tables first (Glue requires empty DB).
  local tables
  tables="$(
    aws glue get-tables --database-name "$db" --region "$REGION" \
      --query 'TableList[].Name' --output text 2>/dev/null || true
  )"
  for t in $tables; do
    [[ -n "$t" && "$t" != "None" ]] || continue
    aws glue delete-table --database-name "$db" --name "$t" --region "$REGION" || true
  done
  aws glue delete-database --name "$db" --region "$REGION"
}

for p in "${PREFIXES[@]}"; do
  _rm_prefix "$p"
done
for db in "${WAREHOUSE_DBS[@]}"; do
  _rm_prefix "warehouse/${db}"
done
# Legacy Iceberg layout used by prior SF10 runs.
_rm_prefix "warehouse/weft_external"
for db in "${GLUE_DBS[@]}"; do
  _drop_glue_db "$db"
done

echo "[delete] done (DRY_RUN=${DRY_RUN})"

#!/usr/bin/env bash
# Tear down SF100 lakehouse Glue databases and optional S3 prefixes.
#
# Default: delete Glue DBs only (Parquet data left intact — cheapest safe cleanup).
# With DELETE_DATA=1 also removes Delta `_delta_log` and the Iceberg warehouse prefix.
#
#   SUITE=tpcds SF=100 ./bench/sf100/teardown-lakehouse.sh
#   SUITE=tpcds SF=100 DELETE_DATA=1 ./bench/sf100/teardown-lakehouse.sh
#
# Does nothing without AWS creds; refuse to run if DRY_RUN=1 prints only.
set -euo pipefail

REGION="${AWS_REGION:-${REGION:-us-west-2}}"
ACCOUNT="${AWS_ACCOUNT_ID:-$(aws sts get-caller-identity --query Account --output text)}"
BUCKET="${BUCKET:-weft-artifacts-${ACCOUNT}}"
SF="${SF:-100}"
SUITE="${SUITE:?Set SUITE=tpch or SUITE=tpcds}"
DELETE_DATA="${DELETE_DATA:-0}"
DRY_RUN="${DRY_RUN:-0}"

SF_TOKEN="${SF//./_}"
prefix="${SUITE}-sf${SF}"
dbs=(
  "${SUITE}_sf${SF_TOKEN}"
  "${SUITE}_sf${SF_TOKEN}_iceberg"
  "${SUITE}_sf${SF_TOKEN}_delta"
)

run() {
  if [[ "$DRY_RUN" == "1" ]]; then
    echo "DRY: $*"
  else
    eval "$@"
  fi
}

for db in "${dbs[@]}"; do
  echo "[teardown] glue database ${db}"
  # Drop tables then database (Glue requires empty DB on some accounts).
  if [[ "$DRY_RUN" == "1" ]]; then
    echo "DRY: aws glue get-tables/delete-table/delete-database ${db}"
    continue
  fi
  tables=$(aws glue get-tables --region "$REGION" --database-name "$db" \
    --query 'TableList[].Name' --output text 2>/dev/null || true)
  for t in $tables; do
    aws glue delete-table --region "$REGION" --database-name "$db" --name "$t" || true
  done
  aws glue delete-database --region "$REGION" --name "$db" 2>/dev/null || true
done

if [[ "$DELETE_DATA" == "1" ]]; then
  echo "[teardown] DELETE_DATA=1 — removing Delta logs + Iceberg warehouse (Parquet data files kept)"
  # Delta logs live beside Parquet; delete only _delta_log folders.
  case "$SUITE" in
    tpch) TABLES=(nation region supplier customer part partsupp orders lineitem) ;;
    tpcds)
      TABLES=(
        call_center catalog_page catalog_returns catalog_sales customer customer_address
        customer_demographics date_dim household_demographics income_band inventory item
        promotion reason ship_mode store store_returns store_sales time_dim warehouse
        web_page web_returns web_sales web_site
      )
      ;;
  esac
  for t in "${TABLES[@]}"; do
    run "aws s3 rm \"s3://${BUCKET}/${prefix}/${t}/_delta_log\" --region \"$REGION\" --recursive --quiet || true"
  done
  run "aws s3 rm \"s3://${BUCKET}/${prefix}-iceberg\" --region \"$REGION\" --recursive --quiet || true"
else
  echo "[teardown] Parquet data + Delta/Iceberg metadata left on S3 (set DELETE_DATA=1 to purge metadata)"
fi

echo "[teardown] done"

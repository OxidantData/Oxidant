#!/usr/bin/env bash
# Sync local Parquet to S3 and register EXTERNAL Hive Parquet tables in Glue.
#
# Usage:
#   SF=10 SUITE=tpcds BUCKET=weft-artifacts-… ./bench/tpc/register-parquet-glue.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"

SF="${SF:-10}"
SUITE="${SUITE:-tpch}"
DATA_ROOT="${DATA_ROOT:-/data}"
OUT="${OUT:-${DATA_ROOT}/${SUITE}-sf${SF}}"
PARQUET="${PARQUET:-${OUT}/parquet}"
BUCKET="${BUCKET:?set BUCKET=…}"
REGION="${AWS_REGION:-${AWS_DEFAULT_REGION:-us-west-2}}"
DB="${DB:-${SUITE}_sf${SF}}"
PREFIX="${PREFIX:-${SUITE}-sf${SF}}"

[[ -d "$PARQUET" ]] || {
  echo "[glue] missing $PARQUET — run ./bench/tpc/prepare.sh first" >&2
  exit 1
}

echo "[glue] sync $PARQUET → s3://${BUCKET}/${PREFIX}/"
aws s3 sync "$PARQUET" "s3://${BUCKET}/${PREFIX}/" --region "$REGION" --delete

export SF SUITE DB REGION BUCKET PREFIX
python3 "$ROOT/bench/tpc/register_parquet_glue.py"

echo "[glue] registered Parquet tables in glue.${DB}"
echo "[glue] query example: SELECT count(*) FROM glue.${DB}.$([[ $SUITE == tpch ]] && echo lineitem || echo store_sales)"

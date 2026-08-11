#!/usr/bin/env bash
# Upload Parquet to S3 and register Iceberg tables in AWS Glue.
#
# Requires: aws CLI, python3 packages pyiceberg[glue,s3fs] pyarrow
#
# Usage:
#   SF=100 SUITE=tpch BUCKET=oxidant-artifacts-123 ./bench/tpc/register-iceberg-glue.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"

SF="${SF:-100}"
SUITE="${SUITE:-tpch}"
DATA_ROOT="${DATA_ROOT:-/data}"
OUT="${OUT:-${DATA_ROOT}/${SUITE}-sf${SF}}"
PARQUET="${PARQUET:-${OUT}/parquet}"
BUCKET="${BUCKET:?set BUCKET=oxidant-artifacts-<account>}"
REGION="${AWS_REGION:-${AWS_DEFAULT_REGION:-us-west-2}}"
DB="${DB:-${SUITE}_sf${SF}_iceberg}"
WAREHOUSE="${WAREHOUSE:-s3://${BUCKET}/${SUITE}-sf${SF}-iceberg}"
# Source Parquet prefix (usually the EXTERNAL parquet layout after register-parquet-glue).
PREFIX="${PREFIX:-${SUITE}-sf${SF}}"
SKIP_SYNC="${SKIP_SYNC:-0}"

[[ -d "$PARQUET" ]] || {
  echo "[glue] missing $PARQUET — run ./bench/tpc/prepare.sh first" >&2
  exit 1
}

if [[ "$SKIP_SYNC" != "1" ]]; then
  echo "[glue] sync $PARQUET → s3://${BUCKET}/${PREFIX}/"
  aws s3 sync "$PARQUET" "s3://${BUCKET}/${PREFIX}/" --region "$REGION"
fi

if ! python3 -c 'import pyiceberg' 2>/dev/null; then
  if [[ -x /tmp/oxidant-tpc-kits/.tpc-venv311/bin/python ]]; then
    PYTHON=/tmp/oxidant-tpc-kits/.tpc-venv311/bin/python
  else
    echo "[glue] installing pyiceberg[glue,s3fs] …"
    python3 -m pip install --user 'pyiceberg[glue,s3fs]' 'pyarrow>=14' >/dev/null
    PYTHON=python3
  fi
else
  PYTHON=python3
fi

export SF SUITE DB WAREHOUSE REGION BUCKET PREFIX LOCAL_PARQUET="$PARQUET"
"$PYTHON" "$ROOT/bench/tpc/register_iceberg_glue.py"

echo "[glue] registered Iceberg tables in glue.${DB} (warehouse $WAREHOUSE)"
echo "[glue] query example: SELECT count(*) FROM glue.${DB}.lineitem"

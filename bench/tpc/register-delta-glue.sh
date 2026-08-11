#!/usr/bin/env bash
# Convert local Parquet dirs to Delta Lake on S3 and register in Glue.
#
# Usage:
#   SF=10 SUITE=tpcds BUCKET=weft-artifacts-… ./bench/tpc/register-delta-glue.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"

SF="${SF:-10}"
SUITE="${SUITE:-tpch}"
DATA_ROOT="${DATA_ROOT:-/data}"
OUT="${OUT:-${DATA_ROOT}/${SUITE}-sf${SF}}"
PARQUET="${PARQUET:-${OUT}/parquet}"
BUCKET="${BUCKET:?set BUCKET=…}"
REGION="${AWS_REGION:-${AWS_DEFAULT_REGION:-us-west-2}}"
DB="${DB:-${SUITE}_sf${SF}_delta}"
PREFIX="${PREFIX:-${SUITE}-sf${SF}-delta}"

[[ -d "$PARQUET" ]] || {
  echo "[delta] missing $PARQUET — run ./bench/tpc/prepare.sh first" >&2
  exit 1
}

if ! python3 -c 'import deltalake' 2>/dev/null; then
  if [[ -x /tmp/oxidant-tpc-kits/.tpc-venv311/bin/python ]]; then
    PYTHON=/tmp/oxidant-tpc-kits/.tpc-venv311/bin/python
  elif [[ -x /tmp/oxidant-tpc-kits/.tpc-venv/bin/python ]]; then
    PYTHON=/tmp/oxidant-tpc-kits/.tpc-venv/bin/python
  else
    echo "[delta] installing deltalake …"
    python3 -m pip install --user 'deltalake>=0.17' 'pyarrow>=14' >/dev/null
    PYTHON=python3
  fi
else
  PYTHON=python3
fi

export SF SUITE DB REGION BUCKET PREFIX LOCAL_PARQUET="$PARQUET"
"$PYTHON" "$ROOT/bench/tpc/register_delta_glue.py"

echo "[delta] registered Delta tables in glue.${DB}"
echo "[delta] query example: SELECT count(*) FROM glue.${DB}.$([[ $SUITE == tpch ]] && echo lineitem || echo store_sales)"

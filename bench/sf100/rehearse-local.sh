#!/usr/bin/env bash
# Local SF0.01-style rehearsal: tiny Parquet → Iceberg add_files + Delta convert,
# assert matching row counts. No AWS. Skips cleanly if Python deps are missing.
#
#   ./bench/sf100/rehearse-local.sh
#   # or from CI:
#   pip install -r bench/sf100/requirements.txt && ./bench/sf100/rehearse-local.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORK="${WORK:-$(mktemp -d -t weft-sf001-XXXXXX)}"
cleanup() {
  if [[ "${KEEP_WORK:-0}" != "1" ]]; then
    rm -rf "$WORK"
  else
    echo "[rehearse] KEEP_WORK=1 — left at $WORK"
  fi
}
trap cleanup EXIT

if ! python3 -c 'import pyiceberg, deltalake, pyarrow' 2>/dev/null; then
  echo "[rehearse] SKIP: install deps first:"
  echo "  python3 -m venv .venv && . .venv/bin/activate && pip install -r bench/sf100/requirements.txt"
  exit 0
fi

echo "[rehearse] work=$WORK"
SRC="$WORK/tpch-sf0.01"
WH="$WORK/tpch-sf0.01-iceberg"
mkdir -p "$SRC/nation" "$SRC/region"

python3 - <<PY
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
src = Path("$SRC")
pq.write_table(
    pa.table({"n_nationkey": pa.array([0, 1, 2], type=pa.int64()), "n_name": ["ALGERIA", "ARGENTINA", "BRAZIL"]}),
    src / "nation" / "nation.parquet",
)
pq.write_table(
    pa.table({"r_regionkey": pa.array([0, 1], type=pa.int64()), "r_name": ["AFRICA", "AMERICA"]}),
    src / "region" / "region.parquet",
)
print("[rehearse] wrote sample parquet")
PY

python3 "$SCRIPT_DIR/build-lakehouse.py" \
  --suite tpch \
  --sf 0.01 \
  --source-prefix "$SRC" \
  --iceberg-warehouse "$WH" \
  --formats iceberg,delta \
  --tables nation,region \
  --skip-glue \
  --verify

echo "[rehearse] PASS"

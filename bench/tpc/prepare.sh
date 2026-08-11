#!/usr/bin/env bash
# End-to-end: official TPC kits → raw → Snappy Parquet, with scale-size checks.
#
# Usage:
#   SF=1    SUITE=tpch  DATA_ROOT=/data ./bench/tpc/prepare.sh   # ~500 MiB
#   SF=100  SUITE=tpch  DATA_ROOT=/data ./bench/tpc/prepare.sh   # ~10 GiB
#   SF=300  SUITE=tpch  DATA_ROOT=/data ./bench/tpc/prepare.sh   # ~31 GiB
#   SF=1000 SUITE=tpch  DATA_ROOT=/data ./bench/tpc/prepare.sh   # ~130 GiB
#   SF=100  SUITE=tpcds DATA_ROOT=/data ./bench/tpc/prepare.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
# shellcheck source=/dev/null
source "$ROOT/bench/tpc/scales.env"

SF="${SF:-100}"
SUITE="${SUITE:-tpch}"
DATA_ROOT="${DATA_ROOT:-/data}"
KITS_DIR="${KITS_DIR:-${DATA_ROOT}/kits}"
OUT="${OUT:-${DATA_ROOT}/${SUITE}-sf${SF}}"
PARQUET="${OUT}/parquet"
SKIP_FETCH="${SKIP_FETCH:-0}"
SKIP_SIZE_CHECK="${SKIP_SIZE_CHECK:-0}"

export SF SUITE DATA_ROOT KITS_DIR OUT

case "$SF" in
  1) TARGET="$TARGET_BYTES_1"; LABEL="~500 MiB" ;;
  100) TARGET="$TARGET_BYTES_100"; LABEL="~10 GiB" ;;
  300) TARGET="$TARGET_BYTES_300"; LABEL="~31 GiB" ;;
  1000) TARGET="$TARGET_BYTES_1000"; LABEL="~130 GiB" ;;
  *)
    TARGET=""
    LABEL="(no published target for SF${SF})"
    echo "[prepare] note: SF${SF} has no size band in scales.env — size check skipped"
    ;;
esac

echo "[prepare] suite=$SUITE sf=$SF target=$LABEL out=$OUT"

if [[ ! -x "$KITS_DIR/tpch-kit/dbgen/dbgen" && ! -x "$KITS_DIR/tpcds-kit/tools/dsdgen" ]]; then
  if [[ "$SKIP_FETCH" == "1" ]]; then
    echo "[prepare] kits missing and SKIP_FETCH=1" >&2
    exit 1
  fi
  "$ROOT/bench/tpc/fetch-kits.sh"
  "$ROOT/bench/tpc/build-kits.sh"
elif [[ ! -x "$KITS_DIR/tpch-kit/dbgen/dbgen" ]] || [[ ! -x "$KITS_DIR/tpcds-kit/tools/dsdgen" ]]; then
  "$ROOT/bench/tpc/build-kits.sh"
fi

"$ROOT/bench/tpc/generate.sh"

if { [[ "$SUITE" == "tpch" ]] && compgen -G "$PARQUET/lineitem/part-*.parquet" >/dev/null; } \
  || { [[ "$SUITE" == "tpcds" ]] && compgen -G "$PARQUET/store_sales/part-*.parquet" >/dev/null; }; then
  echo "[prepare] parquet already at $PARQUET — skipping convert"
else
  if ! python3 -c 'import pyarrow' 2>/dev/null; then
    VENV="${DATA_ROOT}/.tpc-venv"
    if [[ ! -x "$VENV/bin/python" ]]; then
      echo "[prepare] creating $VENV with pyarrow ..."
      python3 -m venv "$VENV"
      "$VENV/bin/pip" install -q 'pyarrow>=14'
    fi
    PYTHON="$VENV/bin/python"
  else
    PYTHON=python3
  fi
  "$PYTHON" "$ROOT/bench/tpc/tbl_to_parquet.py" --suite "$SUITE" --raw "$OUT/raw" --out "$PARQUET"
fi

# Flatten convenience copies for oxidant-bench tpch-bench (expects <table>.parquet OR <table>/).
if [[ "$SUITE" == "tpch" ]]; then
  for t in nation region supplier customer part partsupp orders lineitem; do
    if [[ -d "$PARQUET/$t" && ! -f "$PARQUET/$t.parquet" ]]; then
      # leave directory form — run_bench already accepts <table>/ dirs
      true
    fi
  done
fi

echo "[prepare] parquet tree:"
du -sh "$PARQUET"/* 2>/dev/null | sort -h | tail -n 30 || true
# Prefer Python: macOS `du` has no -sb and returns 64 for that flag (breaks set -e).
TOTAL="$(python3 - <<PY
from pathlib import Path
root = Path(r"""$PARQUET""")
print(sum(p.stat().st_size for p in root.rglob('*') if p.is_file()))
PY
)"
echo "[prepare] total parquet bytes: $TOTAL ($LABEL expected)"

if [[ -n "$TARGET" && "$SKIP_SIZE_CHECK" != "1" ]]; then
  python3 - <<PY
target = int("$TARGET")
total = int("$TOTAL")
sf = "$SF"
label = "$LABEL"
lo = int(target * 0.60)
hi = int(target * 1.40)
pct = (100.0 * total / target) if target else 0
print(f"[prepare] size check: {total} bytes = {pct:.0f}% of target {target} (band {lo}..{hi})")
if total < lo or total > hi:
    print(
        f"[prepare] WARN: SF{sf} parquet size outside ±40% of {label} — "
        "inspect compression/partitioning before publishing",
        flush=True,
    )
else:
    print("[prepare] size check OK")
PY
fi

echo "[prepare] done: $PARQUET"
echo "[prepare] next (optional): SF=$SF SUITE=$SUITE BUCKET=… ./bench/tpc/register-iceberg-glue.sh"

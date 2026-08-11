#!/usr/bin/env bash
# Generate TPC-H / TPC-DS query text from the official TPC toolkits (qgen / dsqgen)
# into bench/tpch/queries and bench/tpcds/queries.
#
# Usage:
#   DATA_ROOT=/data ./bench/tpc/generate-queries.sh
#   SUITE=tpch SF=100 ./bench/tpc/generate-queries.sh
#
# TPC-H Q11 keeps a `__OXIDANT_SF__` placeholder (spec fraction is 0.0001/SF) so the
# same committed SQL works across scale factors. All other binds use qgen/dsqgen
# qualification defaults (-d / -QUALIFY Y).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
DATA_ROOT="${DATA_ROOT:-/data}"
KITS_DIR="${KITS_DIR:-${DATA_ROOT}/kits}"
SUITE="${SUITE:-all}" # all | tpch | tpcds
SF="${SF:-100}"       # qgen -s / dsqgen -SCALE (Q11 rewritten afterward)

TPCH_OUT="${TPCH_OUT:-$ROOT/bench/tpch/queries}"
TPCDS_OUT="${TPCDS_OUT:-$ROOT/bench/tpcds/queries}"
DIALECT_SRC="$ROOT/bench/tpc/dialects/oxidant.tpl"
POST="$ROOT/bench/tpc/postprocess_query.py"

find_dbgen_dir() {
  if [[ -x "$KITS_DIR/tpch-kit/dbgen/qgen" ]]; then
    echo "$KITS_DIR/tpch-kit/dbgen"
    return
  fi
  local match
  match="$(find "$KITS_DIR/tpch-kit" -type f -name qgen 2>/dev/null | head -1 || true)"
  [[ -n "$match" ]] || return 1
  dirname "$match"
}

generate_tpch() {
  local dbgen_dir
  dbgen_dir="$(find_dbgen_dir)" || {
    echo "[generate-queries] qgen not found — run fetch-kits.sh && build-kits.sh" >&2
    exit 1
  }
  mkdir -p "$TPCH_OUT"
  echo "[generate-queries] TPC-H via $dbgen_dir/qgen (-d -s $SF) -> $TPCH_OUT"
  local n rawf
  for n in $(seq 1 22); do
    rawf="$(mktemp)"
    (
      cd "$dbgen_dir"
      DSS_QUERY=./queries ./qgen -d -s "$SF" "$n" >"$rawf" 2>/dev/null
    )
    python3 "$POST" --suite tpch --num "$n" --raw "$rawf" --out "$TPCH_OUT/q${n}.sql"
    rm -f "$rawf"
  done
}

generate_tpcds() {
  local tools="$KITS_DIR/tpcds-kit/tools"
  local templates="$KITS_DIR/tpcds-kit/query_templates"
  [[ -x "$tools/dsqgen" ]] || {
    echo "[generate-queries] dsqgen not found — run fetch-kits.sh && build-kits.sh" >&2
    exit 1
  }
  mkdir -p "$TPCDS_OUT"
  cp "$DIALECT_SRC" "$templates/oxidant.tpl"
  echo "[generate-queries] TPC-DS via $tools/dsqgen (-QUALIFY Y -SCALE $SF -DIALECT oxidant) -> $TPCDS_OUT"
  local n rawf errf tpl
  for n in $(seq 1 99); do
    tpl="query${n}.tpl"
    [[ -f "$templates/$tpl" ]] || {
      echo "[generate-queries] missing $templates/$tpl" >&2
      exit 1
    }
    rawf="$(mktemp)"
    errf="$(mktemp)"
    (
      cd "$tools"
      ./dsqgen \
        -DIRECTORY "$templates" \
        -TEMPLATE "$tpl" \
        -DIALECT oxidant \
        -SCALE "$SF" \
        -QUALIFY Y \
        -FILTER Y \
        -DISTRIBUTIONS "$tools/tpcds.idx" \
        >"$rawf" 2>"$errf"
    )
    if grep -q 'ERROR:' "$errf"; then
      echo "[generate-queries] dsqgen failed for Q$n:" >&2
      cat "$errf" >&2
      rm -f "$rawf" "$errf"
      exit 1
    fi
    python3 "$POST" --suite tpcds --num "$n" --raw "$rawf" --out "$TPCDS_OUT/q${n}.sql"
    rm -f "$rawf" "$errf"
  done
}

case "$SUITE" in
  all)
    generate_tpch
    generate_tpcds
    ;;
  tpch) generate_tpch ;;
  tpcds) generate_tpcds ;;
  *)
    echo "SUITE must be all|tpch|tpcds" >&2
    exit 1
    ;;
esac

echo "[generate-queries] done (suite=$SUITE sf=$SF)"

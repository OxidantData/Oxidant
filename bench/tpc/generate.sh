#!/usr/bin/env bash
# Run official dbgen / dsdgen into $OUT/raw for the requested scale factor.
#
# By default, parallel children keep separate part files (do **not** concatenate).
# Oxidant shards scans by file list; one giant .tbl/.dat → one Parquet file → one
# worker. KEEP_PARTS=0 restores the old concat behaviour.
#
# Usage:
#   SF=100 SUITE=tpch DATA_ROOT=/data ./bench/tpc/generate.sh
#   SF=100 SUITE=tpcds DATA_ROOT=/data ./bench/tpc/generate.sh
set -euo pipefail

SF="${SF:?set SF (1, 100, 300, 1000, …)}"
SUITE="${SUITE:?set SUITE=tpch|tpcds}"
DATA_ROOT="${DATA_ROOT:-/data}"
KITS_DIR="${KITS_DIR:-${DATA_ROOT}/kits}"
# Parallel children for dbgen (-C) / dsdgen (-PARALLEL). Default = CPU count.
CHILDREN="${CHILDREN:-$(sysctl -n hw.ncpu 2>/dev/null || nproc 2>/dev/null || echo 4)}"
# Keep parallel part files so Parquet conversion emits multiple part-*.parquet.
KEEP_PARTS="${KEEP_PARTS:-1}"

case "$SUITE" in
  tpch|tpcds) ;;
  *) echo "SUITE must be tpch or tpcds" >&2; exit 1 ;;
esac

OUT="${OUT:-${DATA_ROOT}/${SUITE}-sf${SF}}"
RAW="${OUT}/raw"
mkdir -p "$RAW"

_tpch_raw_ready() {
  [[ -f "$RAW/lineitem.tbl" ]] || compgen -G "$RAW/lineitem.tbl.[0-9]*" >/dev/null
}

_tpcds_raw_ready() {
  [[ -f "$RAW/store_sales.dat" ]] || compgen -G "$RAW/store_sales_[0-9]*_[0-9]*.dat" >/dev/null
}

if [[ "$SUITE" == "tpch" ]]; then
  if _tpch_raw_ready; then
    echo "[generate] TPC-H SF${SF} raw already at $RAW — skipping"
    exit 0
  fi
  dbgen=""
  for cand in \
    "$KITS_DIR/tpch-kit/dbgen/dbgen" \
    "$KITS_DIR"/tpch-kit/TPC-H_Tools*/dbgen/dbgen; do
    if [[ -x "$cand" ]]; then dbgen="$cand"; break; fi
  done
  [[ -n "$dbgen" ]] || {
    echo "[generate] dbgen not found — run ./bench/tpc/fetch-kits.sh && ./bench/tpc/build-kits.sh" >&2
    exit 1
  }
  dbgen_dir="$(cd "$(dirname "$dbgen")" && pwd)"
  echo "[generate] TPC-H SF${SF} via $dbgen (-s ${SF} -C ${CHILDREN}, KEEP_PARTS=${KEEP_PARTS}) -> $RAW"
  (
    cd "$dbgen_dir"
    # dists.dss must live next to the working directory dbgen uses.
    if [[ "$CHILDREN" -gt 1 ]]; then
      # dbgen requires -S (child step) with -C; spawn one process per chunk.
      for ((c = 1; c <= CHILDREN; c++)); do
        ./dbgen -s "$SF" -C "$CHILDREN" -S "$c" -f &
      done
      # Some children exit non-zero when a table is not owned by that step; still collect parts.
      wait || true
      if [[ "$KEEP_PARTS" != "1" ]]; then
        for base in nation region supplier customer part partsupp orders lineitem; do
          if [[ -f "${base}.tbl" ]]; then
            continue
          fi
          shopt -s nullglob
          parts=( "${base}.tbl".* )
          if ((${#parts[@]})); then
            cat "${parts[@]}" > "${base}.tbl"
            rm -f "${parts[@]}"
          fi
          shopt -u nullglob
        done
      fi
    else
      ./dbgen -s "$SF" -f
    fi
    shopt -s nullglob
    for f in *.tbl *.tbl.[0-9]*; do
      [[ -e "$f" ]] || continue
      mv -f "$f" "$RAW/"
    done
    shopt -u nullglob
  )
  _tpch_raw_ready || { echo "[generate] failed to produce lineitem under $RAW" >&2; exit 1; }
else
  if _tpcds_raw_ready; then
    echo "[generate] TPC-DS SF${SF} raw already at $RAW — skipping"
    exit 0
  fi
  dsdgen="$KITS_DIR/tpcds-kit/tools/dsdgen"
  [[ -x "$dsdgen" ]] || {
    echo "[generate] dsdgen not found — run ./bench/tpc/fetch-kits.sh && ./bench/tpc/build-kits.sh" >&2
    exit 1
  }
  tools="$(cd "$(dirname "$dsdgen")" && pwd)"
  echo "[generate] TPC-DS SF${SF} via $dsdgen (−SCALE ${SF} −PARALLEL ${CHILDREN}, KEEP_PARTS=${KEEP_PARTS}) → $RAW"
  (
    cd "$RAW"
    "$dsdgen" \
      -DIR "$RAW" \
      -SCALE "$SF" \
      -PARALLEL "$CHILDREN" \
      -CHILD 1 \
      -FORCE \
      -VERBOSE Y \
      -DISTRIBUTIONS "$tools/tpcds.idx"
    # When PARALLEL>1, spawn remaining children.
    if [[ "$CHILDREN" -gt 1 ]]; then
      for ((c = 2; c <= CHILDREN; c++)); do
        "$dsdgen" \
          -DIR "$RAW" \
          -SCALE "$SF" \
          -PARALLEL "$CHILDREN" \
          -CHILD "$c" \
          -FORCE \
          -DISTRIBUTIONS "$tools/tpcds.idx" &
      done
      wait
    fi
  )
  if [[ "$KEEP_PARTS" != "1" ]] && ! [[ -f "$RAW/store_sales.dat" ]]; then
    # Legacy: concatenate parallel CHILD parts into one .dat per table.
    shopt -s nullglob
    parts=( "$RAW"/store_sales_*.dat )
    if ((${#parts[@]})); then
      echo "[generate] concatenating parallel TPC-DS parts …"
      for table_glob in "$RAW"/*_1_"${CHILDREN}".dat; do
        [[ -e "$table_glob" ]] || continue
        base="$(basename "$table_glob" | sed -E "s/_[0-9]+_${CHILDREN}\\.dat$//")"
        cat "$RAW"/"${base}"_*_"${CHILDREN}".dat > "$RAW/${base}.dat"
        rm -f "$RAW"/"${base}"_*_"${CHILDREN}".dat
      done
    fi
    shopt -u nullglob
  fi
  _tpcds_raw_ready || { echo "[generate] failed to produce store_sales under $RAW" >&2; exit 1; }
fi

echo "[generate] done: $RAW"
du -sh "$RAW" || true

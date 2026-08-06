#!/usr/bin/env bash
# Prepare TPC-H Parquet for oxidant-bench tpch-bench.
#
# Default SF100 uses DuckDB's pre-built database (~27 GB):
#   https://blobs.duckdb.org/data/tpch-sf100.db
#
# Usage:
#   SF=100 DATA_ROOT=/data ./bench/tpch/prepare.sh
#   SF=1   DATA_ROOT=/tmp/oxidant-bench ./bench/tpch/prepare.sh   # generates via dbgen
set -euo pipefail

SF="${SF:-100}"
DATA_ROOT="${DATA_ROOT:-/data}"
DB="${DATA_ROOT}/tpch-sf${SF}.db"
OUT="${DATA_ROOT}/tpch-sf${SF}"
DUCKDB_BIN="${DUCKDB_BIN:-duckdb}"
BLOB_URL="https://blobs.duckdb.org/data/tpch-sf${SF}.db"

mkdir -p "$DATA_ROOT"

if [[ ! -x "$(command -v "$DUCKDB_BIN")" && ! -x "$DUCKDB_BIN" ]]; then
  echo "[prepare] installing DuckDB CLI into /usr/local/bin …"
  ARCH="$(uname -m)"
  case "$ARCH" in
    x86_64|amd64) ZIP=duckdb_cli-linux-amd64.zip ;;
    aarch64|arm64) ZIP=duckdb_cli-linux-aarch64.zip ;;
    *) echo "unsupported arch $ARCH"; exit 1 ;;
  esac
  TMP="$(mktemp -d)"
  curl -fsSL -o "$TMP/duckdb.zip" "https://github.com/duckdb/duckdb/releases/download/v1.3.2/${ZIP}"
  unzip -o "$TMP/duckdb.zip" -d "$TMP"
  sudo install -m 755 "$TMP/duckdb" /usr/local/bin/duckdb
  DUCKDB_BIN=/usr/local/bin/duckdb
fi

if [[ -d "$OUT" && -f "$OUT/lineitem.parquet" ]]; then
  echo "[prepare] parquet already at $OUT — skipping"
  exit 0
fi

if [[ ! -f "$DB" ]]; then
  if curl -fsI "$BLOB_URL" >/dev/null 2>&1; then
    echo "[prepare] downloading $BLOB_URL …"
    curl -fL --retry 5 --retry-delay 2 -o "${DB}.partial" "$BLOB_URL"
    mv "${DB}.partial" "$DB"
  else
    echo "[prepare] no blob for SF${SF}; generating with DuckDB dbgen (this can take a while) …"
    "$DUCKDB_BIN" "$DB" -c "INSTALL tpch; LOAD tpch; CALL dbgen(sf=${SF});"
  fi
fi

echo "[prepare] exporting Parquet → $OUT …"
rm -rf "$OUT"
"$DUCKDB_BIN" "$DB" -c "EXPORT DATABASE '${OUT}' (FORMAT PARQUET);"
# Flatten nested EXPORT layout if present: prefer top-level <table>.parquet
if [[ ! -f "$OUT/lineitem.parquet" ]]; then
  for t in nation region supplier customer part partsupp orders lineitem; do
    if [[ -f "$OUT/${t}.parquet" ]]; then
      continue
    elif [[ -d "$OUT/$t" ]]; then
      # single-file or glob — leave directory for register_parquet
      true
    fi
  done
fi
echo "[prepare] done: $OUT"
ls -lh "$OUT" | head -30

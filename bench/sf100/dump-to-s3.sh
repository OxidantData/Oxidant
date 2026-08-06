#!/usr/bin/env bash
# Download DuckDB's pre-built TPC-H / TPC-DS SF100 databases, export each table as
# Parquet into s3://oxidant-artifacts-<account>/<prefix>/<table>/, and register Glue
# EXTERNAL_TABLE entries (empty Columns — Oxidant infers Parquet schema at read time).
#
# Usage (on a box with ~150 GB free disk + AWS creds that can s3:PutObject + glue:*):
#   ./bench/sf100/dump-to-s3.sh              # both suites
#   SUITES=tpch ./bench/sf100/dump-to-s3.sh  # TPC-H only
#   SUITES=tpcds SF=100 ./bench/sf100/dump-to-s3.sh
set -euo pipefail

REGION="${AWS_REGION:-us-west-2}"
ACCOUNT="${AWS_ACCOUNT_ID:-$(aws sts get-caller-identity --query Account --output text)}"
BUCKET="${BUCKET:-oxidant-artifacts-${ACCOUNT}}"
SF="${SF:-100}"
SUITES="${SUITES:-tpch tpcds}"
WORK="${WORK:-/data/oxidant-sf${SF}}"
DUCKDB_BIN="${DUCKDB_BIN:-duckdb}"
DUCKDB_VERSION="${DUCKDB_VERSION:-v1.3.2}"
# Set SKIP_GLUE=1 when the instance role can write S3 but not mutate Glue;
# then run register-glue.sh from a principal that can.
SKIP_GLUE="${SKIP_GLUE:-0}"

TPCH_TABLES=(nation region supplier customer part partsupp orders lineitem)
TPCDS_TABLES=(
  call_center catalog_page catalog_returns catalog_sales customer customer_address
  customer_demographics date_dim household_demographics income_band inventory item
  promotion reason ship_mode store store_returns store_sales time_dim warehouse
  web_page web_returns web_sales web_site
)

mkdir -p "$WORK"
cd "$WORK"

ensure_duckdb() {
  if command -v "$DUCKDB_BIN" >/dev/null 2>&1; then
    return
  fi
  echo "[dump] installing DuckDB CLI ${DUCKDB_VERSION} …"
  ARCH="$(uname -m)"
  OS="$(uname -s)"
  case "$OS-$ARCH" in
    Linux-x86_64|Linux-amd64) ZIP=duckdb_cli-linux-amd64.zip ;;
    Linux-aarch64|Linux-arm64) ZIP=duckdb_cli-linux-aarch64.zip ;;
    Darwin-arm64|Darwin-x86_64) ZIP=duckdb_cli-osx-universal.zip ;;
    *) echo "unsupported platform $OS-$ARCH"; exit 1 ;;
  esac
  curl -fsSL -o duckdb.zip \
    "https://github.com/duckdb/duckdb/releases/download/${DUCKDB_VERSION}/${ZIP}"
  unzip -o duckdb.zip
  chmod +x duckdb
  DUCKDB_BIN="$WORK/duckdb"
}

download_db() {
  local suite="$1"
  local db="${WORK}/${suite}-sf${SF}.db"
  local url="https://blobs.duckdb.org/data/${suite}-sf${SF}.db"
  if [[ -f "$db" ]]; then
    echo "[dump] $db already present"
    return
  fi
  echo "[dump] downloading $url ($(curl -fsSI "$url" | awk 'tolower($1)=="content-length:"{printf "%.1f GiB", $2/1024/1024/1024}')) …"
  curl -fL --retry 5 --retry-delay 2 -o "${db}.partial" "$url"
  mv "${db}.partial" "$db"
}

export_and_upload() {
  local suite="$1"
  shift
  local -a tables=("$@")
  local db="${WORK}/${suite}-sf${SF}.db"
  local prefix="${suite}-sf${SF}"
  local glue_db="${suite}_sf${SF}"
  local local_out="${WORK}/${prefix}-parquet"

  echo "[dump] exporting $suite SF${SF} → Parquet (local ${local_out}) …"
  rm -rf "$local_out"
  mkdir -p "$local_out"

  # EXPORT DATABASE writes <table>.parquet (+ schema.sql). Faster than per-table COPY for SF100.
  "$DUCKDB_BIN" "$db" -c "EXPORT DATABASE '${local_out}' (FORMAT PARQUET);"

  for t in "${tables[@]}"; do
    local src=""
    if [[ -f "${local_out}/${t}.parquet" ]]; then
      src="${local_out}/${t}.parquet"
    elif [[ -d "${local_out}/${t}" ]]; then
      src="${local_out}/${t}"
    else
      echo "[dump] WARN: missing export for ${t}; skipping"
      continue
    fi

    local s3_uri="s3://${BUCKET}/${prefix}/${t}/"
    echo "[dump] sync ${t} → ${s3_uri}"
    if [[ -d "$src" ]]; then
      aws s3 sync "$src" "$s3_uri" --region "$REGION" --only-show-errors
    else
      aws s3 cp "$src" "${s3_uri}${t}.parquet" --region "$REGION" --only-show-errors
    fi
  done

  aws s3 ls "s3://${BUCKET}/${prefix}/" --region "$REGION" --human-readable --summarize | tail -8

  if [[ "$SKIP_GLUE" == "1" ]]; then
    echo "[dump] SKIP_GLUE=1 — S3 only; register later with register-glue.sh"
  else
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    SUITE="$suite" SF="$SF" BUCKET="$BUCKET" REGION="$REGION" \
      bash "$SCRIPT_DIR/register-glue.sh"
  fi
}

ensure_duckdb
echo "[dump] account=$ACCOUNT bucket=$BUCKET region=$REGION work=$WORK duckdb=$DUCKDB_BIN"

for suite in $SUITES; do
  case "$suite" in
    tpch)
      download_db tpch
      export_and_upload tpch "${TPCH_TABLES[@]}"
      ;;
    tpcds)
      download_db tpcds
      export_and_upload tpcds "${TPCDS_TABLES[@]}"
      ;;
    *)
      echo "unknown suite: $suite (want tpch|tpcds)"; exit 2
      ;;
  esac
done

echo "[dump] ALL DONE"
date -u +%Y-%m-%dT%H:%M:%SZ | aws s3 cp - "s3://${BUCKET}/bench/sf${SF}/DUMP_COMPLETE" --region "$REGION"

#!/usr/bin/env bash
# Clone TPC-H / TPC-DS toolkits into $KITS_DIR (default: $DATA_ROOT/kits).
#
# Prefer official TPC.org zips after accepting the license. Defaults pull community
# mirrors of those same generators so the pipeline is automatable.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
# shellcheck source=/dev/null
source "$ROOT/bench/tpc/scales.env" 2>/dev/null || true

DATA_ROOT="${DATA_ROOT:-/data}"
KITS_DIR="${KITS_DIR:-${DATA_ROOT}/kits}"
TPCH_KIT_URL="${TPCH_KIT_URL:-https://github.com/gregrahn/tpch-kit.git}"
TPCDS_KIT_URL="${TPCDS_KIT_URL:-https://github.com/databricks/tpcds-kit.git}"
TPCH_REF="${TPCH_REF:-master}"
TPCDS_REF="${TPCDS_REF:-master}"

mkdir -p "$KITS_DIR"

clone_or_update() {
  local url="$1" ref="$2" dest="$3"
  if [[ -d "$dest/.git" ]]; then
    echo "[fetch-kits] updating $dest …"
    git -C "$dest" fetch --depth 1 origin "$ref"
    git -C "$dest" checkout -q FETCH_HEAD
  elif [[ -d "$dest" ]]; then
    echo "[fetch-kits] $dest exists (non-git) — leaving as-is"
  else
    echo "[fetch-kits] cloning $url → $dest …"
    git clone --depth 1 --branch "$ref" "$url" "$dest" \
      || git clone --depth 1 "$url" "$dest"
  fi
}

clone_or_update "$TPCH_KIT_URL" "$TPCH_REF" "$KITS_DIR/tpch-kit"
clone_or_update "$TPCDS_KIT_URL" "$TPCDS_REF" "$KITS_DIR/tpcds-kit"

echo "[fetch-kits] done:"
echo "  TPC-H:  $KITS_DIR/tpch-kit"
echo "  TPC-DS: $KITS_DIR/tpcds-kit"
echo
echo "Official alternative: download kits from"
echo "  https://www.tpc.org/tpc_documents_current_versions/current_specifications.asp"
echo "and set KITS_DIR to the parent that contains tpch-kit/ (dbgen/) and tpcds-kit/ (tools/)."

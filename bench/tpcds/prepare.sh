#!/usr/bin/env bash
# Prepare TPC-DS Parquet via official TPC.org dsdgen (not DuckDB).
#
# Target footprints (Snappy Parquet):
#   SF=1    ~500 MiB
#   SF=100  ~10 GiB   (default)
#   SF=300  ~31 GiB
#   SF=1000 ~130 GiB
#
# Usage:
#   SF=100 DATA_ROOT=/data ./bench/tpcds/prepare.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
export SF="${SF:-100}"
export SUITE=tpcds
export DATA_ROOT="${DATA_ROOT:-/data}"
exec "$ROOT/bench/tpc/prepare.sh"

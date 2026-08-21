#!/usr/bin/env bash
# Prepare TPC-H Parquet via official TPC.org dbgen (not DuckDB).
#
# Target footprints (Snappy Parquet):
#   SF=1    ~500 MiB
#   SF=100  ~10 GiB   (default)
#   SF=300  ~31 GiB
#   SF=1000 ~130 GiB
#
# Usage:
#   SF=100 DATA_ROOT=/data ./bench/tpch/prepare.sh
#   SF=1   DATA_ROOT=/tmp/oxidant-bench ./bench/tpch/prepare.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
export SF="${SF:-100}"
export SUITE=tpch
export DATA_ROOT="${DATA_ROOT:-/data}"
exec "$ROOT/bench/tpc/prepare.sh"

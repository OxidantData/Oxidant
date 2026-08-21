#!/usr/bin/env bash
# Reproducible Oxidant ClickBench entry — run on a prepared Linux box (run ./install first).
# Builds Oxidant, ensures the 14.78 GB hits.parquet is present, then runs all 43 queries through
# the live oxidant-connect Spark Connect server over gRPC (3 tries each, hot = min of try 2/3) and
# writes a ClickBench-format results/<machine>.json.
#
# Env: BENCH_DATA (parquet path), OXIDANT_MEMORY_LIMIT_BYTES (spill-pool size; default 26 GB),
#      OXIDANT_TARGET_PARTITIONS (default = vCPUs).
set -euo pipefail
cd "$(dirname "$0")/../.."   # repo root

DATA="${BENCH_DATA:-$PWD/bench/clickbench/hits.parquet}"
export OXIDANT_MEMORY_LIMIT_BYTES="${OXIDANT_MEMORY_LIMIT_BYTES:-26000000000}"

echo "[bench] building oxidant-bench (release) …"
cargo build --release -p oxidant-bench

if [ ! -f "$DATA" ]; then
  echo "[bench] downloading hits.parquet (~14.78 GB) → $DATA"
  curl -sL -o "$DATA" https://datasets.clickhouse.com/hits_compatible/athena/hits.parquet
fi
echo "[bench] data: $(ls -la "$DATA" | awk '{print $5}') bytes"

./target/release/oxidant-bench clickbench-grpc --data "$DATA"
echo "[bench] results written to bench/clickbench/results/c6a.4xlarge.json"

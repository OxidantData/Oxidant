#!/usr/bin/env bash
# Download the dataset once, then run all four engines sequentially on this box and collect
# results/<engine>.json. Each engine runs alone (the others' servers are stopped) so every run
# gets the full machine — the only fair way to compare on a single host.
#
#   bash bench/clickbench/multi/run-all.sh                 # all four
#   ENGINES="oxidant sail" bash .../run-all.sh                # subset
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
REPO="$(cd "$HERE/../../.." && pwd)"
DATA="${BENCH_DATA:-$REPO/bench/clickbench/hits.parquet}"
ENGINES="${ENGINES:-oxidant sail spark gluten}"

if [ ! -f "$DATA" ]; then
  echo "[all] downloading hits.parquet (~14.78 GB) → $DATA"
  mkdir -p "$(dirname "$DATA")"
  curl -fSL -o "$DATA" https://datasets.clickhouse.com/hits_compatible/athena/hits.parquet
fi
echo "[all] dataset: $(stat -c%s "$DATA" 2>/dev/null) bytes"

for e in $ENGINES; do
  echo; echo "==================== $e ===================="
  if bash "$HERE/run-engine.sh" "$e"; then
    echo "[all] $e OK"
  else
    echo "[all] $e FAILED — leaving results/$e.json absent (site shows it pending)"
  fi
  sleep 3   # let ports/daemons fully release between engines
done

echo; echo "[all] done. results:"; ls -la "$HERE/results" 2>/dev/null || true
echo "[all] next: python3 $HERE/to-site.py   (writes site/src/data/benchmarks.json)"

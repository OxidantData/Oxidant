#!/usr/bin/env bash
# KAN-14 — full SF100 TPC-H + TPC-DS re-measure on a live distributed cluster.
#
# Prerequisites:
#   - Wave0 (KAN-4..7) + KAN-1 + Wave1/2 planner fixes deployed to the cluster
#   - WEFT_DISTRIBUTED_STRICT=1 on the connect pod (values-sf100.yaml)
#   - Workers Ready == WEFT_WORKER_COUNT (no silent shard loss)
#   - Glue SF100 Parquet registered (see docs/distributed-k8s.md / distributed-ec2.md)
#   - Optional: leave WEFT_REPLICATED_TABLES unset to validate KAN-1 auto-broadcast
#
# Usage:
#   CONNECT=sc://$HOST:50051 NS=weft-sf100 ./bench/sf100/remeasure-distributed.sh
#   CONNECT=sc://$HOST:50051 NS=weft-sf100 SUITE=tpch ./bench/sf100/remeasure-distributed.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
CONNECT="${CONNECT:?set CONNECT=sc://host:50051}"
NS="${NS:-weft}"
SUITE="${SUITE:-all}"   # all | tpch | tpcds
GLUE_DB_TPCH="${GLUE_DB_TPCH:-tpch_sf100}"
GLUE_DB_TPCDS="${GLUE_DB_TPCDS:-tpcds_sf100}"
OUT_DIR="${OUT_DIR:-$ROOT/bench/sf100/results/remeasure-$(date -u +%Y%m%dT%H%M%SZ)}"
WORKERS="${WORKERS:-2}"

mkdir -p "$OUT_DIR"
export WEFT_DISTRIBUTED_STRICT=1

run_suite() {
  local suite="$1" db="$2" out="$3"
  echo "[remeasure] suite=$suite glue_db=$db -> $out"
  python3 "$ROOT/bench/sf100/run-spark-connect.py" \
    --endpoint "$CONNECT" \
    --suite "$suite" \
    --sf 100 \
    --glue-db "$db" \
    --namespace "$NS" \
    --expected-workers "$WORKERS" \
    --strict \
    --json "$out" \
    --resume
}

case "$SUITE" in
  tpch)  run_suite tpch  "$GLUE_DB_TPCH"  "$OUT_DIR/tpch-sf100.jsonl" ;;
  tpcds) run_suite tpcds "$GLUE_DB_TPCDS" "$OUT_DIR/tpcds-sf100.jsonl" ;;
  all)
    run_suite tpch  "$GLUE_DB_TPCH"  "$OUT_DIR/tpch-sf100.jsonl"
    run_suite tpcds "$GLUE_DB_TPCDS" "$OUT_DIR/tpcds-sf100.jsonl"
    ;;
  *) echo "SUITE must be all|tpch|tpcds" >&2; exit 2 ;;
esac

echo "[remeasure] done. Summarize with:"
echo "  python3 -c \"import json; from pathlib import Path
for p in Path('$OUT_DIR').glob('*.jsonl'):
  rows=[json.loads(l) for l in p.read_text().splitlines() if l.strip()]
  ok=sum(1 for r in rows if r.get('status')=='ok')
  print(f'{p.name}: {ok}/{len(rows)} ok')\""

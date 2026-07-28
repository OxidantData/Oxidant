#!/usr/bin/env bash
# KAN-14 — full SF100 TPC-H + TPC-DS re-measure on the EC2 CloudFormation data plane
# (Packer AMI + ASG). Prefer this over EKS for SF100 honesty runs.
#
# Prerequisites:
#   - Stack from deploy/cloudformation/weft-cluster.yaml (see docs/distributed-ec2.md)
#   - Connect reachable at CONNECT (NLB or driver private IP sc://host:50051)
#   - WEFT_DISTRIBUTED_STRICT=1 on the driver (CF bootstrap sets this for SF100 overlays)
#   - Glue SF100 Parquet registered; WorkerCount>=2 with stable shard indices
#   - Optional: leave WEFT_REPLICATED_TABLES unset to validate KAN-1 auto-broadcast
#
# Usage:
#   CONNECT=sc://$NLB_DNS:50051 ./bench/sf100/remeasure-distributed.sh
#   CONNECT=sc://$DRIVER_IP:50051 SUITE=tpch ./bench/sf100/remeasure-distributed.sh
#
# Deploy / tear-down helpers:
#   ./deploy/cloudformation/deploy-stack.sh   # create/update ASG cluster
#   aws cloudformation delete-stack --stack-name <name> --region us-west-2
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
CONNECT="${CONNECT:?set CONNECT=sc://host:50051 (CF NLB or driver IP)}"
SUITE="${SUITE:-all}"   # all | tpch | tpcds
GLUE_DB_TPCH="${GLUE_DB_TPCH:-tpch_sf100}"
GLUE_DB_TPCDS="${GLUE_DB_TPCDS:-tpcds_sf100}"
OUT_DIR="${OUT_DIR:-$ROOT/bench/sf100/results/remeasure-ec2-$(date -u +%Y%m%dT%H%M%SZ)}"
WORKERS="${WORKERS:-2}"
# EC2 path: skip kubectl Ready gate unless NS is explicitly set for a hybrid check.
NS="${NS:-}"

mkdir -p "$OUT_DIR"
export WEFT_DISTRIBUTED_STRICT=1

run_suite() {
  local suite="$1" db="$2" out="$3"
  echo "[remeasure-ec2] suite=$suite glue_db=$db -> $out"
  local args=(
    --endpoint "$CONNECT"
    --suite "$suite"
    --sf 100
    --glue-db "$db"
    --expected-workers "$WORKERS"
    --strict
    --json "$out"
    --resume
  )
  if [[ -n "$NS" ]]; then
    args+=(--namespace "$NS")
  else
    # No in-cluster Ready probe on ASG; operator confirms WorkerCount via CF/ASG.
    args+=(--skip-worker-preflight)
  fi
  python3 "$ROOT/bench/sf100/run-spark-connect.py" "${args[@]}"
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

echo "[remeasure-ec2] done. Summarize with:"
echo "  python3 -c \"import json; from pathlib import Path
for p in Path('$OUT_DIR').glob('*.jsonl'):
  rows=[json.loads(l) for l in p.read_text().splitlines() if l.strip()]
  ok=sum(1 for r in rows if r.get('status')=='ok')
  print(f'{p.name}: {ok}/{len(rows)} ok')\""

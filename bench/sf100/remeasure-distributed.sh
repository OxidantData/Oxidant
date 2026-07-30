#!/usr/bin/env bash
# KAN-14 — full SF100 TPC-H + TPC-DS re-measure on the EC2 CloudFormation data plane
# (Packer AMI + ASG). Prefer this over EKS for SF100 honesty runs.
#
# Canonical SF100 EC2 topology (track here + docs/distributed-ec2.md § SF100):
#   Driver:  1× c6g.xlarge  (4 vCPU / 8 GiB)  + 100 GiB gp3 root
#   Workers: 2× m8g.8xlarge (32 vCPU / 128 GiB) + 500 GiB gp3 spill each
#            ASG Min=Max=Desired=2 (pinned; arm64 AMI required)
#   Env:     WEFT_DISTRIBUTED_STRICT=1, WEFT_PREFER_HASH_JOIN=false,
#            WEFT_MEMORY_LIMIT_BYTES=42949672960, WEFT_SHUFFLE_SPILL_BYTES=8589934592,
#            WEFT_SHUFFLE_PARTITIONS=32 (driver; ≈ worker vCPU, reduces shuffle skew)
#   Data:    Glue Parquet tpch_sf100 / tpcds_sf100
#   Connect: driver instance IP only — do NOT use an NLB (ExposeConnect=false)
#
# Prerequisites:
#   - Stack from deploy/cloudformation/weft-cluster.yaml (see docs/distributed-ec2.md)
#   - Connect at CONNECT=sc://<driver-ip>:50051 (or omit CONNECT + set STACK to auto-resolve)
#   - WEFT_DISTRIBUTED_STRICT=1 on the driver (--distributed-strict true)
#   - Glue SF100 Parquet registered; WorkerCount=2 with stable shard indices
#   - Optional: leave WEFT_REPLICATED_TABLES unset to validate KAN-1 auto-broadcast
#
# Usage:
#   STACK=weft-sf100 ./bench/sf100/remeasure-distributed.sh
#   CONNECT=sc://$DRIVER_IP:50051 SUITE=tpch ./bench/sf100/remeasure-distributed.sh
#
# Deploy / tear-down helpers:
#   ./deploy/cloudformation/deploy-stack.sh   # create/update ASG cluster
#   aws cloudformation delete-stack --stack-name <name> --region us-west-2
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
REGION="${AWS_REGION:-${AWS_DEFAULT_REGION:-us-west-2}}"
STACK="${STACK:-weft-sf100}"
SUITE="${SUITE:-all}"   # all | tpch | tpcds
SF="${SF:-100}"         # scale factor; SF=10 for the cheap iteration loop
SF_INT="${SF%.*}"       # 100 -> tpch_sf100, 10 -> tpch_sf10
GLUE_DB_TPCH="${GLUE_DB_TPCH:-tpch_sf${SF_INT}}"
GLUE_DB_TPCDS="${GLUE_DB_TPCDS:-tpcds_sf${SF_INT}}"
OUT_DIR="${OUT_DIR:-$ROOT/bench/sf100/results/remeasure-ec2-$(date -u +%Y%m%dT%H%M%SZ)}"
WORKERS="${WORKERS:-2}"
# EC2 path: skip kubectl Ready gate unless NS is explicitly set for a hybrid check.
NS="${NS:-}"

resolve_driver_connect() {
  local ip
  ip="$(aws ec2 describe-instances --region "${REGION}" \
    --filters "Name=tag:Name,Values=${STACK}-driver" "Name=instance-state-name,Values=running" \
    --query 'Reservations[0].Instances[0].PublicIpAddress' --output text 2>/dev/null || true)"
  if [[ -z "${ip}" || "${ip}" == "None" ]]; then
    ip="$(aws ec2 describe-instances --region "${REGION}" \
      --filters "Name=tag:Name,Values=${STACK}-driver" "Name=instance-state-name,Values=running" \
      --query 'Reservations[0].Instances[0].PrivateIpAddress' --output text 2>/dev/null || true)"
  fi
  if [[ -z "${ip}" || "${ip}" == "None" ]]; then
    echo "error: no running driver for stack tag Name=${STACK}-driver in ${REGION}" >&2
    exit 1
  fi
  printf 'sc://%s:50051' "${ip}"
}

if [[ -z "${CONNECT:-}" ]]; then
  CONNECT="$(resolve_driver_connect)"
  echo "[remeasure-ec2] CONNECT unset — using driver IP ${CONNECT} (stack=${STACK}; not NLB)"
elif [[ "${CONNECT}" == *elb.amazonaws.com* || "${CONNECT}" == *DEPRECATED-NLB* ]]; then
  echo "error: CONNECT points at an NLB (${CONNECT})." >&2
  echo "  Spark Connect for this data plane must use the driver instance IP." >&2
  echo "  Unset CONNECT (auto-resolve via STACK=${STACK}) or set CONNECT=sc://<driver-ip>:50051" >&2
  echo "  See docs/distributed-ec2.md § Do not put Spark Connect behind an NLB." >&2
  exit 2
fi

mkdir -p "$OUT_DIR"
export WEFT_DISTRIBUTED_STRICT=1

run_suite() {
  local suite="$1" db="$2" out="$3"
  echo "[remeasure-ec2] suite=$suite glue_db=$db -> $out"
  local args=(
    --endpoint "$CONNECT"
    --suite "$suite"
    --sf "$SF"
    --glue-db "$db"
    --worker-count "$WORKERS"
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

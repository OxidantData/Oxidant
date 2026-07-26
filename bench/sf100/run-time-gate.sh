#!/usr/bin/env bash
# Run TPC-H + TPC-DS SF100 against a fat EKS driver with a 20-minute wall clock
# gate (starts after the cluster is READY).
#
# Prereq: Glue DBs tpch_sf100 / tpcds_sf100 populated; kubectl context on
# weft-platform; gateway reachable (WEFT_GATEWAY or port-forward on :18080).
#
# Usage:
#   ./bench/sf100/run-time-gate.sh
#   GATE_SECS=1200 CLUSTER_ID=abc ./bench/sf100/run-time-gate.sh
set -euo pipefail

# STOP: SF100 via POST /api/sql was verified DRIVER-ONLY (see docs/DISTRIBUTED_PARITY.md).
# Do not publish multi-executor comparisons until Connect+workers+scan sharding land.
# Set ALLOW_SINGLE_NODE_GATE=1 to force a single-node measurement anyway.
if [[ "${ALLOW_SINGLE_NODE_GATE:-0}" != "1" ]]; then
  echo "[gate] refused: TPC path is not distributed yet (docs/DISTRIBUTED_PARITY.md)." >&2
  echo "[gate] set ALLOW_SINGLE_NODE_GATE=1 to override for single-node profiling only." >&2
  exit 3
fi


ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

GW="${WEFT_GATEWAY:-http://127.0.0.1:18080}"
GATE_SECS="${GATE_SECS:-1200}"
MACHINE="${MACHINE:-eks/r6g.8xlarge-spot-bench}"
SIZE="${SIZE:-xlarge}"   # use xlarge until gateway image knows "bench"; then SIZE=bench
WORKER_MIN="${WORKER_MIN:-0}"
WORKER_MAX="${WORKER_MAX:-0}"
PATCH_FAT="${PATCH_FAT:-1}"   # patch driver to fat CPU/mem after create (live bypass)
# Defaults fit On-Demand r6g.4xlarge (~16 vCPU / 128 GiB) under a 32 vCPU Standard
# quota. For r6g.8xlarge use FAT_CPU=28 FAT_MEM=200Gi (needs Spot or quota headroom).
FAT_CPU="${FAT_CPU:-28}"
FAT_MEM="${FAT_MEM:-200Gi}"

ADMIN_PW="${WEFT_ADMIN_PASSWORD:-$(kubectl -n weft-system get secret weft-gateway-jwt -o jsonpath='{.data.admin-password}' | base64 -d)}"
TOKEN="$(curl -sS -X POST "$GW/api/auth/login" -H 'content-type: application/json' \
  -d "{\"username\":\"${WEFT_ADMIN_USER:-admin}\",\"password\":\"${ADMIN_PW}\"}" \
  | python3 -c 'import sys,json; print(json.load(sys.stdin)["token"])')"

auth=(-H "authorization: Bearer $TOKEN" -H 'content-type: application/json')

if [[ -z "${CLUSTER_ID:-}" ]]; then
  echo "[gate] creating cluster size=${SIZE} workers=${WORKER_MIN}-${WORKER_MAX}"
  CLUSTER_ID="$(curl -sS -X POST "$GW/api/clusters" "${auth[@]}" \
    -d "{\"name\":\"sf100-time-gate\",\"worker_size\":\"${SIZE}\",\"worker_min\":${WORKER_MIN},\"worker_max\":${WORKER_MAX}}" \
    | python3 -c 'import sys,json; print(json.load(sys.stdin)["id"])')"
  echo "[gate] cluster_id=${CLUSTER_ID}"
fi

echo "[gate] waiting for RUNNING …"
for i in $(seq 1 90); do
  STATE="$(curl -sS "$GW/api/clusters" -H "authorization: Bearer $TOKEN" \
    | python3 -c "import sys,json; cs=json.load(sys.stdin); print(next((c['state'] for c in cs if c['id']=='${CLUSTER_ID}'),'gone'))")"
  echo "[gate] state=${STATE}"
  [[ "$STATE" == "RUNNING" ]] && break
  [[ "$STATE" == "gone" || "$STATE" == "FAILED" || "$STATE" == "ERROR" || "$STATE" == "TERMINATED" ]] && exit 1
  sleep 10
done
[[ "${STATE:-}" == "RUNNING" ]] || { echo "[gate] cluster never became RUNNING"; exit 1; }

NS="weft-cl-${CLUSTER_ID}"
DRV="$(kubectl -n "$NS" get deploy -o jsonpath='{.items[0].metadata.name}')"

if [[ "$PATCH_FAT" == "1" ]]; then
  echo "[gate] raising ResourceQuota + patching driver ${NS}/${DRV} → ${FAT_CPU} CPU / ${FAT_MEM}"
  # Namespace quota is sized to worker_size at create time; raise it before the fat pod.
  kubectl -n "$NS" patch resourcequota weft-cluster-quota --type=merge -p="{\"spec\":{\"hard\":{\"requests.cpu\":\"${FAT_CPU}\",\"requests.memory\":\"${FAT_MEM}\",\"pods\":\"2\"}}}"
  # Ensure xlarge-pool toleration (live gateway may omit it when pool label is absent).
  kubectl -n "$NS" patch deploy "$DRV" --type=strategic -p='{"spec":{"template":{"spec":{"tolerations":[{"key":"weft.io/pool","operator":"Equal","value":"xlarge","effect":"NoSchedule"}]}}}}'
  # Recreate (not RollingUpdate): single-node pools cannot surge an 8→14 CPU pod.
  kubectl -n "$NS" patch deploy "$DRV" --type=json -p="[
    {\"op\":\"replace\",\"path\":\"/spec/strategy\",\"value\":{\"type\":\"Recreate\"}},
    {\"op\":\"replace\",\"path\":\"/spec/template/spec/containers/0/resources/requests/cpu\",\"value\":\"${FAT_CPU}\"},
    {\"op\":\"replace\",\"path\":\"/spec/template/spec/containers/0/resources/limits/cpu\",\"value\":\"${FAT_CPU}\"},
    {\"op\":\"replace\",\"path\":\"/spec/template/spec/containers/0/resources/requests/memory\",\"value\":\"${FAT_MEM}\"},
    {\"op\":\"replace\",\"path\":\"/spec/template/spec/containers/0/resources/limits/memory\",\"value\":\"${FAT_MEM}\"}
  ]"
  # Drop workers if any — SQL path is driver Connect only.
  kubectl -n "$NS" scale sts -l weft.io/role=worker --replicas=0 2>/dev/null || true
  # Nudge the ReplicaSet out of FailedCreate backoff after the quota raise.
  kubectl -n "$NS" annotate deploy "$DRV" "kubectl.kubernetes.io/restartedAt=$(date -u +%Y-%m-%dT%H:%M:%SZ)" --overwrite
  kubectl -n "$NS" rollout status deploy/"$DRV" --timeout=600s
  # Confirm scheduled on a fat node
  NODE="$(kubectl -n "$NS" get pod -l weft.io/role=driver -o jsonpath='{.items[0].spec.nodeName}')"
  TYPE="$(kubectl get node "$NODE" -o jsonpath='{.metadata.labels.node\.kubernetes\.io/instance-type}')"
  echo "[gate] driver on ${NODE} (${TYPE})"
fi

# Smoke
curl -sS -m 120 -X POST "$GW/api/sql" "${auth[@]}" \
  -d "{\"sql\":\"SELECT count(*) AS n FROM glue.tpch_sf100.nation\",\"cluster_id\":\"${CLUSTER_ID}\",\"no_limit\":true}" \
  | python3 -c 'import sys,json; d=json.load(sys.stdin); assert d.get("status")=="FINISHED", d; print("[gate] smoke ok", d["rows"])'

echo "[gate] START clock (${GATE_SECS}s) — TPC-H then TPC-DS"
START="$(date +%s)"

python3 bench/sf100/run-via-gateway.py \
  --gateway "$GW" \
  --suite tpch --sf 100 --glue-db tpch_sf100 \
  --cluster-id "$CLUSTER_ID" \
  --machine "$MACHINE" \
  --json site/src/data/tpch.json

MID="$(date +%s)"
ELAPSED=$((MID - START))
echo "[gate] TPC-H done in ${ELAPSED}s; remaining $((GATE_SECS - ELAPSED))s"

python3 bench/sf100/run-via-gateway.py \
  --gateway "$GW" \
  --suite tpcds --sf 100 --glue-db tpcds_sf100 \
  --cluster-id "$CLUSTER_ID" \
  --machine "$MACHINE" \
  --json site/src/data/tpcds.json

END="$(date +%s)"
TOTAL=$((END - START))
echo "[gate] TOTAL wall ${TOTAL}s (budget ${GATE_SECS}s)"
if (( TOTAL > GATE_SECS )); then
  echo "[gate] FAIL time gate"
  exit 2
fi
echo "[gate] PASS"
echo "$CLUSTER_ID" > /tmp/sf100-gate-cluster-id

#!/usr/bin/env bash
# Isolated per-query matrix: restart workers before each query so a wedged
# query cannot poison the next one. Records worker/driver RSS before+after.
set -u
KEY=~/.ssh/id_ed25519_kaicoder03
DRIVER=18.236.223.115
W0=172.31.50.17
W1=172.31.41.60
REPO="$(cd "$(dirname "$0")/../../../.." && pwd)"
OUT="$REPO/bench/sf100/results/matrix-sf10"
PY=/tmp/oxidant-smoke-venv/bin/python3
QUERIES="${QUERIES:-4,5,8,9,10,11,12,13,14,15,16,17,18,19,21,22}"
TIMEOUT="${TIMEOUT:-240}"

sshw() { # sshw <worker-ip> <cmd>
  ssh -i "$KEY" -o IdentitiesOnly=yes -o ConnectTimeout=10 \
    -o "ProxyCommand ssh -i $KEY -o IdentitiesOnly=yes -W %h:%p ec2-user@$DRIVER" \
    ec2-user@"$1" "$2" 2>/dev/null
}
rss() { # rss <worker-ip> -> KB
  sshw "$1" "ps -eo rss,comm | awk '\$2==\"oxidant\"{print \$1}'"
}
driver_rss() {
  ssh -i "$KEY" -o IdentitiesOnly=yes -o ConnectTimeout=10 ec2-user@$DRIVER \
    "ps -eo rss,comm | awk '\$2==\"oxidant\"{print \$1}'" 2>/dev/null
}
restart_workers() {
  sshw "$W0" 'sudo systemctl restart oxidant-worker' &
  sshw "$W1" 'sudo systemctl restart oxidant-worker' &
  wait
  sleep 8
}

echo "q,result,secs,w0_before_kb,w0_after_kb,w1_before_kb,w1_after_kb,driver_after_kb" > "$OUT/matrix.csv"
IFS=',' read -ra QS <<< "$QUERIES"
for q in "${QS[@]}"; do
  echo "[matrix] === Q$q: restarting workers $(date -u +%H:%M:%SZ) ==="
  restart_workers
  b0=$(rss "$W0"); b1=$(rss "$W1")
  start=$(date +%s)
  res=$("$PY" "$REPO/bench/sf100/run-spark-connect.py" \
    --endpoint "sc://$DRIVER:50051" --suite tpch --sf 10 --glue-db tpch_sf10 \
    --only "$q" --strict --worker-count 2 --skip-worker-preflight \
    --query-timeout "$TIMEOUT" --json "$OUT/q$q.jsonl" 2>&1 | grep -E "^Q$q" | tail -2 | tr '\n' ' ')
  end=$(date +%s)
  a0=$(rss "$W0"); a1=$(rss "$W1"); ad=$(driver_rss)
  if grep -q HOT "$OUT/q$q.jsonl" 2>/dev/null || [[ "$res" == *" HOT "* ]]; then
    verdict=PASS
  else
    verdict=FAIL
  fi
  echo "[matrix] Q$q $verdict $((end-start))s  w0:${b0}->${a0} w1:${b1}->${a1} drv:$ad"
  echo "[matrix]   $res"
  echo "$q,$verdict,$((end-start)),$b0,$a0,$b1,$a1,$ad" >> "$OUT/matrix.csv"
done
echo "[matrix] DONE $(date -u +%H:%M:%SZ)"

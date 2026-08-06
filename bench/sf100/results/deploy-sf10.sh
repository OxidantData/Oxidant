#!/usr/bin/env bash
# Deploy the locally cross-built linux/arm64 oxidant binary to the oxidant-sf10 cluster.
# Replaces /usr/local/bin/oxidant on driver + both workers and restarts services.
set -euo pipefail
KEY=~/.ssh/id_ed25519_kaicoder03
# Override for fresh clusters (ASG start assigns new IPs); discover via:
#   aws ec2 describe-instances --filters "Name=tag:aws:autoscaling:groupName,Values=oxidant-sf10-driver|oxidant-sf10-workers"
DRIVER="${OXIDANT_SF10_DRIVER:-18.236.223.115}"
W0="${OXIDANT_SF10_W0:-172.31.50.17}"
W1="${OXIDANT_SF10_W1:-172.31.41.60}"
BIN="${1:-target/linux-cross/aarch64-unknown-linux-gnu/release/oxidant}"
[ -f "$BIN" ] || { echo "binary not found: $BIN"; exit 1; }
SSH_OPTS=(-i "$KEY" -o IdentitiesOnly=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=accept-new)
PROXY=(-o "ProxyCommand ssh -i $KEY -o IdentitiesOnly=yes -W %h:%p ec2-user@$DRIVER")

echo "== uploading to driver $DRIVER =="
scp "${SSH_OPTS[@]}" "$BIN" ec2-user@$DRIVER:/tmp/oxidant-new
for W in "$W0" "$W1"; do
  echo "== uploading to worker $W =="
  scp "${SSH_OPTS[@]}" "${PROXY[@]}" "$BIN" ec2-user@$W:/tmp/oxidant-new
done

echo "== installing + restarting driver =="
ssh "${SSH_OPTS[@]}" ec2-user@$DRIVER 'sudo cp -a /usr/local/bin/oxidant /usr/local/bin/oxidant.bak 2>/dev/null || true; sudo install -m 0755 /tmp/oxidant-new /usr/local/bin/oxidant && sudo systemctl restart oxidant-driver && systemctl is-active oxidant-driver'
for W in "$W0" "$W1"; do
  echo "== installing + restarting worker $W =="
  ssh "${SSH_OPTS[@]}" "${PROXY[@]}" ec2-user@$W 'sudo cp -a /usr/local/bin/oxidant /usr/local/bin/oxidant.bak 2>/dev/null || true; sudo install -m 0755 /tmp/oxidant-new /usr/local/bin/oxidant && sudo systemctl restart oxidant-worker && systemctl is-active oxidant-worker'
done
echo "== deploy complete =="

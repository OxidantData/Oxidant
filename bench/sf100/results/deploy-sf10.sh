#!/usr/bin/env bash
# Deploy the locally cross-built linux/arm64 weft binary to the weft-sf10 cluster.
# Replaces /usr/local/bin/weft on driver + both workers and restarts services.
set -euo pipefail
KEY=~/.ssh/id_ed25519_kaicoder03
# Override for fresh clusters (ASG start assigns new IPs); discover via:
#   aws ec2 describe-instances --filters "Name=tag:aws:autoscaling:groupName,Values=weft-sf10-driver|weft-sf10-workers"
DRIVER="${WEFT_SF10_DRIVER:-18.236.223.115}"
W0="${WEFT_SF10_W0:-172.31.50.17}"
W1="${WEFT_SF10_W1:-172.31.41.60}"
BIN="${1:-target/linux-cross/aarch64-unknown-linux-gnu/release/weft}"
[ -f "$BIN" ] || { echo "binary not found: $BIN"; exit 1; }
SSH_OPTS=(-i "$KEY" -o IdentitiesOnly=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=accept-new)
PROXY=(-o "ProxyCommand ssh -i $KEY -o IdentitiesOnly=yes -W %h:%p ec2-user@$DRIVER")

echo "== uploading to driver $DRIVER =="
scp "${SSH_OPTS[@]}" "$BIN" ec2-user@$DRIVER:/tmp/weft-new
for W in "$W0" "$W1"; do
  echo "== uploading to worker $W =="
  scp "${SSH_OPTS[@]}" "${PROXY[@]}" "$BIN" ec2-user@$W:/tmp/weft-new
done

echo "== installing + restarting driver =="
ssh "${SSH_OPTS[@]}" ec2-user@$DRIVER 'sudo cp -a /usr/local/bin/weft /usr/local/bin/weft.bak 2>/dev/null || true; sudo install -m 0755 /tmp/weft-new /usr/local/bin/weft && sudo systemctl restart weft-driver && systemctl is-active weft-driver'
for W in "$W0" "$W1"; do
  echo "== installing + restarting worker $W =="
  ssh "${SSH_OPTS[@]}" "${PROXY[@]}" ec2-user@$W 'sudo cp -a /usr/local/bin/weft /usr/local/bin/weft.bak 2>/dev/null || true; sudo install -m 0755 /tmp/weft-new /usr/local/bin/weft && sudo systemctl restart weft-worker && systemctl is-active weft-worker'
done
echo "== deploy complete =="

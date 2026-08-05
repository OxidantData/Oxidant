#!/usr/bin/env bash
# weft-shard-resolve — periodic shard-index re-resolution for the fixed-size worker ASG.
#
# Why this exists: bootstrap assigns WEFT_SHARD_INDEX once, from the sorted InService
# peer list visible at that moment. A CloudFormation instance refresh replaces workers
# one at a time, so an early replacement computes its slot against a peer list that
# still contains a doomed old worker — and never recomputes. Instance-id sort order
# can then hand two workers the SAME index: the other shard's files are read by nobody
# and queries silently return partial results. This timer-driven pass re-resolves the
# index against settled membership and restarts weft-worker only when it actually
# diverged, with two guards:
#   - stability: the divergent index must reproduce on two polls 20s apart (a member-
#     ship churn mid-refresh must not flap the worker), and
#   - hysteresis: at most one self-restart per 10 minutes.
# Driver nodes exit immediately (WEFT_ROLE != worker). Enabled by weft-shard-resolve.timer.
set -euo pipefail

ENV_FILE=/etc/weft/weft.env
STAMP=/var/lib/weft/.shard-resolve-last-restart
PENDING=/var/lib/weft/.shard-resolve-pending
AWS_BIN="${WEFT_AWS_BIN:-/usr/local/bin/aws}"
export PATH="/usr/local/bin:/usr/bin:/bin:${PATH:-}"

log() { echo "[weft-shard-resolve] $*"; }

[[ -f "${ENV_FILE}" ]] || exit 0
role="$(sed -n 's/^WEFT_ROLE=//p' "${ENV_FILE}" | head -1)"
[[ "${role}" == "worker" ]] || exit 0

# Not while bootstrap is mid-flight (it owns the env file and the unit graph).
if systemctl is-active --quiet weft-bootstrap.service && \
   ! systemctl show weft-bootstrap.service -p ActiveEnterTimestamp --value | grep -q .; then
  exit 0
fi

TOKEN="$(curl -fsS -X PUT "http://169.254.169.254/latest/api/token" \
  -H "X-aws-ec2-metadata-token-ttl-seconds: 21600")"
imds() { curl -fsS -H "X-aws-ec2-metadata-token: ${TOKEN}" "http://169.254.169.254/latest/$1"; }
INSTANCE_ID="$(imds meta-data/instance-id)"
REGION="$(imds meta-data/placement/region)"

WORKER_COUNT="$(sed -n 's/^WEFT_WORKER_COUNT=//p' "${ENV_FILE}" | head -1)"
CURRENT="$(sed -n 's/^WEFT_SHARD_INDEX=//p' "${ENV_FILE}" | head -1)"
WORKER_ASG="$("${AWS_BIN}" ec2 describe-tags --region "${REGION}" \
  --filters "Name=resource-id,Values=${INSTANCE_ID}" "Name=key,Values=weft:worker-asg" \
  --query 'Tags[0].Value' --output text 2>/dev/null | sed 's/^None$//' || true)"
[[ -n "${WORKER_COUNT}" && -n "${CURRENT}" && -n "${WORKER_ASG}" ]] || exit 0

mapfile -t PEER_IDS < <("${AWS_BIN}" autoscaling describe-auto-scaling-groups \
  --region "${REGION}" --auto-scaling-group-names "${WORKER_ASG}" \
  --query 'AutoScalingGroups[0].Instances[?LifecycleState==`InService`].InstanceId' \
  --output text 2>/dev/null | tr '\t' '\n' | sort -u)
# Self must be in the set (we may not be InService yet / describe eventual consistency).
if ! printf '%s\n' "${PEER_IDS[@]}" | grep -qx "${INSTANCE_ID}"; then
  PEER_IDS+=("${INSTANCE_ID}")
fi
IFS=$'\n' PEER_IDS=($(printf '%s\n' "${PEER_IDS[@]}" | sort -u))
# Incomplete membership: churn in progress — do nothing this round (bootstrap's
# loud-fail covers the boot-time case; here the running worker keeps its index).
(( ${#PEER_IDS[@]} >= WORKER_COUNT )) || exit 0

RESOLVED=-1
for i in "${!PEER_IDS[@]}"; do
  [[ "${PEER_IDS[$i]}" == "${INSTANCE_ID}" ]] && RESOLVED=$i && break
done
(( RESOLVED >= 0 )) || exit 0
[[ "${RESOLVED}" != "${CURRENT}" ]] || exit 0

# Stability: the divergent index must reproduce on the next poll, 20s later.
if [[ -f "${PENDING}" ]] && [[ "$(cat "${PENDING}")" == "${RESOLVED}" ]]; then
  rm -f "${PENDING}"
else
  echo "${RESOLVED}" > "${PENDING}"
  log "resolved WEFT_SHARD_INDEX=${RESOLVED} (current ${CURRENT}); pending confirmation next poll"
  exit 0
fi

# Hysteresis: at most one self-restart per 10 minutes.
now=$(date +%s)
if [[ -f "${STAMP}" ]] && (( now - $(cat "${STAMP}") < 600 )); then
  log "would move to shard ${RESOLVED} but a correction ran <10m ago; skipping"
  exit 0
fi

log "correcting WEFT_SHARD_INDEX ${CURRENT} -> ${RESOLVED} (ASG membership settled); restarting weft-worker"
tmp="$(mktemp "${ENV_FILE}.tmp.XXXXXX")"
sed "s/^WEFT_SHARD_INDEX=.*/WEFT_SHARD_INDEX=${RESOLVED}/" "${ENV_FILE}" > "${tmp}"
chown --reference="${ENV_FILE}" "${tmp}"
chmod --reference="${ENV_FILE}" "${tmp}"
mv -f "${tmp}" "${ENV_FILE}"
echo "${now}" > "${STAMP}"
systemctl restart weft-worker.service

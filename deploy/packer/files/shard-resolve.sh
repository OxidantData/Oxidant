#!/usr/bin/env bash
# oxidant-shard-resolve — periodic shard-index re-resolution for the fixed-size worker ASG.
#
# Why this exists: bootstrap assigns OXIDANT_SHARD_INDEX once, from the sorted InService
# peer list visible at that moment. A CloudFormation instance refresh replaces workers
# one at a time, so an early replacement computes its slot against a peer list that
# still contains a doomed old worker — and never recomputes. Instance-id sort order
# can then hand two workers the SAME index: the other shard's files are read by nobody
# and queries silently return partial results. This timer-driven pass re-resolves the
# index against settled membership and restarts oxidant-worker only when it actually
# diverged, with two guards:
#   - stability: the divergent index must reproduce on two consecutive timer polls
#     (OnUnitActiveSec=2min, AccuracySec=30s; a membership churn mid-refresh must not
#     flap the worker), and
#   - hysteresis: at most one self-restart per 10 minutes.
# Membership must be unique, exact-size, and include this instance. Do not insert an
# unobserved self; an oversized InService set used to publish ordinal >= WORKER_COUNT
# and disable file sharding (OxidantData/Oxidant#184).
# Driver nodes exit immediately (OXIDANT_ROLE != worker). Enabled by oxidant-shard-resolve.timer.
set -euo pipefail

ENV_FILE=/etc/oxidant/oxidant.env
STAMP=/var/lib/oxidant/.shard-resolve-last-restart
PENDING=/var/lib/oxidant/.shard-resolve-pending
AWS_BIN="${OXIDANT_AWS_BIN:-/usr/local/bin/aws}"
export PATH="/usr/local/bin:/usr/bin:/bin:${PATH:-}"

log() { echo "[oxidant-shard-resolve] $*"; }

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=shard-membership.sh
source "${SCRIPT_DIR}/shard-membership.sh"

[[ -f "${ENV_FILE}" ]] || exit 0
role="$(sed -n 's/^OXIDANT_ROLE=//p' "${ENV_FILE}" | head -1)"
[[ "${role}" == "worker" ]] || exit 0

# Not while bootstrap is mid-flight (it owns the env file and the unit graph).
if systemctl is-active --quiet oxidant-bootstrap.service && \
   ! systemctl show oxidant-bootstrap.service -p ActiveEnterTimestamp --value | grep -q .; then
  exit 0
fi

TOKEN="$(curl -fsS -X PUT "http://169.254.169.254/latest/api/token" \
  -H "X-aws-ec2-metadata-token-ttl-seconds: 21600")"
imds() { curl -fsS -H "X-aws-ec2-metadata-token: ${TOKEN}" "http://169.254.169.254/latest/$1"; }
INSTANCE_ID="$(imds meta-data/instance-id)"
REGION="$(imds meta-data/placement/region)"

WORKER_COUNT="$(sed -n 's/^OXIDANT_WORKER_COUNT=//p' "${ENV_FILE}" | head -1)"
CURRENT="$(sed -n 's/^OXIDANT_SHARD_INDEX=//p' "${ENV_FILE}" | head -1)"
WORKER_ASG="$("${AWS_BIN}" ec2 describe-tags --region "${REGION}" \
  --filters "Name=resource-id,Values=${INSTANCE_ID}" "Name=key,Values=oxidant:worker-asg" \
  --query 'Tags[0].Value' --output text 2>/dev/null | sed 's/^None$//' || true)"
[[ -n "${WORKER_COUNT}" && -n "${CURRENT}" && -n "${WORKER_ASG}" ]] || exit 0

mapfile -t PEER_IDS < <("${AWS_BIN}" autoscaling describe-auto-scaling-groups \
  --region "${REGION}" --auto-scaling-group-names "${WORKER_ASG}" \
  --query 'AutoScalingGroups[0].Instances[?LifecycleState==`InService`].InstanceId' \
  --output text 2>/dev/null | tr '\t' '\n' | sort -u)
# Incomplete, oversized, duplicate, or self-absent membership: churn in progress.
# Keep the last validated index; do not insert this instance into an unobserved set
# (that published ordinal >= WORKER_COUNT and disabled file sharding).
RESOLVED="$(oxidant_resolve_shard_index "${WORKER_COUNT}" "${INSTANCE_ID}" "${PEER_IDS[@]}")" || exit 0
[[ "${RESOLVED}" != "${CURRENT}" ]] || exit 0

# Stability: the divergent index must reproduce on the next timer poll (~2 minutes).
if [[ -f "${PENDING}" ]] && [[ "$(cat "${PENDING}")" == "${RESOLVED}" ]]; then
  rm -f "${PENDING}"
else
  echo "${RESOLVED}" > "${PENDING}"
  log "resolved OXIDANT_SHARD_INDEX=${RESOLVED} (current ${CURRENT}); pending confirmation next poll"
  exit 0
fi

# Hysteresis: at most one self-restart per 10 minutes.
now=$(date +%s)
if [[ -f "${STAMP}" ]] && (( now - $(cat "${STAMP}") < 600 )); then
  log "would move to shard ${RESOLVED} but a correction ran <10m ago; skipping"
  exit 0
fi

log "correcting OXIDANT_SHARD_INDEX ${CURRENT} -> ${RESOLVED} (ASG membership settled); restarting oxidant-worker"
tmp="$(mktemp "${ENV_FILE}.tmp.XXXXXX")"
sed "s/^OXIDANT_SHARD_INDEX=.*/OXIDANT_SHARD_INDEX=${RESOLVED}/" "${ENV_FILE}" > "${tmp}"
chown --reference="${ENV_FILE}" "${tmp}"
chmod --reference="${ENV_FILE}" "${tmp}"
mv -f "${tmp}" "${ENV_FILE}"
echo "${now}" > "${STAMP}"
systemctl restart oxidant-worker.service

#!/usr/bin/env bash
# Weft EC2 bootstrap — runs once at boot (and --deregister on shutdown).
#
# Responsibilities:
#   1. Mount spill EBS (if present) at /var/lib/weft/spill
#   2. Read instance tags / IMDS for role + cluster config
#   3. Workers: assign WEFT_SHARD_INDEX from sorted InService ASG peers
#   4. Workers: upsert (or delete) this instance's A record in the private zone
#   5. Write /etc/weft/weft.env and enable the matching systemd unit
#
# No credentials are stored — uses the instance profile only.
set -euo pipefail

ENV_FILE=/etc/weft/weft.env
SPILL_MOUNT=/var/lib/weft/spill
SPILL_DEVICE_CANDIDATES=(/dev/nvme1n1 /dev/xvdf /dev/sdf)
IMDS_TOKEN_TTL=21600

log() { echo "[weft-bootstrap] $*"; }

imds_token() {
  curl -fsS -X PUT "http://169.254.169.254/latest/api/token" \
    -H "X-aws-ec2-metadata-token-ttl-seconds: ${IMDS_TOKEN_TTL}"
}

imds_get() {
  local path="$1"
  curl -fsS -H "X-aws-ec2-metadata-token: ${TOKEN}" \
    "http://169.254.169.254/latest/${path}"
}

tag_value() {
  local key="$1"
  local attempt out
  for attempt in 1 2 3 4 5 6 7 8 9 10; do
    out="$(aws ec2 describe-tags \
      --region "${REGION}" \
      --filters "Name=resource-id,Values=${INSTANCE_ID}" "Name=key,Values=${key}" \
      --query 'Tags[0].Value' --output text 2>/dev/null | sed 's/^None$//' || true)"
    if [[ -n "${out}" ]]; then
      printf '%s' "${out}"
      return 0
    fi
    sleep 2
  done
  printf ''
}

mount_spill_volume() {
  mkdir -p "${SPILL_MOUNT}"
  local dev=""
  for candidate in "${SPILL_DEVICE_CANDIDATES[@]}"; do
    if [[ -b "${candidate}" ]]; then
      dev="${candidate}"
      break
    fi
  done
  if [[ -z "${dev}" ]]; then
    log "no spill block device found; using root filesystem at ${SPILL_MOUNT}"
    chown weft:weft "${SPILL_MOUNT}"
    return 0
  fi
  if ! blkid "${dev}" >/dev/null 2>&1; then
    log "formatting ${dev} as xfs"
    mkfs.xfs -f "${dev}"
  fi
  if ! findmnt -n "${SPILL_MOUNT}" >/dev/null 2>&1; then
    mount -o defaults,nofail "${dev}" "${SPILL_MOUNT}"
  fi
  # Persist across reboot.
  local uuid
  uuid="$(blkid -s UUID -o value "${dev}")"
  if ! grep -q "${uuid}" /etc/fstab 2>/dev/null; then
    echo "UUID=${uuid} ${SPILL_MOUNT} xfs defaults,nofail 0 2" >> /etc/fstab
  fi
  chown weft:weft "${SPILL_MOUNT}"
  log "spill volume mounted at ${SPILL_MOUNT} (${dev})"
}

wait_for_workers() {
  local asg="$1"
  local expected="$2"
  local deadline=$((SECONDS + 240))
  local ids=""
  while (( SECONDS < deadline )); do
    ids="$(aws autoscaling describe-auto-scaling-groups \
      --region "${REGION}" \
      --auto-scaling-group-names "${asg}" \
      --query 'AutoScalingGroups[0].Instances[?LifecycleState==`InService`].InstanceId' \
      --output text 2>/dev/null | tr '\t' '\n' | sort)"
    local count
    count="$(printf '%s\n' "${ids}" | grep -c . || true)"
    if [[ "${count}" -ge "${expected}" ]]; then
      printf '%s\n' "${ids}"
      return 0
    fi
    log "waiting for ${expected} InService workers in ${asg} (have ${count})"
    sleep 5
  done
  # Best-effort: return whatever we have so the instance still starts.
  printf '%s\n' "${ids}"
}

# Multi-value answer set: maintain one A RRSet with all worker IPs.
# Each worker rewrites the full set from the current InService peer list so
# stale IPs are pruned without needing SET_IDENTIFIER / multivalue routing.
sync_worker_dns() {
  local zone_id="$1"
  local fqdn="$2"
  local asg="$3"
  local ids ip records json
  ids="$(aws autoscaling describe-auto-scaling-groups \
    --region "${REGION}" \
    --auto-scaling-group-names "${asg}" \
    --query 'AutoScalingGroups[0].Instances[?LifecycleState==`InService`].InstanceId' \
    --output text 2>/dev/null | tr '\t' '\n' | sort)"
  records=""
  while IFS= read -r id; do
    [[ -z "${id}" ]] && continue
    ip="$(aws ec2 describe-instances \
      --region "${REGION}" \
      --instance-ids "${id}" \
      --query 'Reservations[0].Instances[0].PrivateIpAddress' \
      --output text)"
    if [[ -n "${ip}" && "${ip}" != "None" ]]; then
      if [[ -n "${records}" ]]; then
        records+=","
      fi
      records+="{\"Value\":\"${ip}\"}"
    fi
  done <<< "${ids}"

  if [[ -z "${records}" ]]; then
    # Nothing InService — delete the RRSet if present.
    local existing
    existing="$(aws route53 list-resource-record-sets \
      --hosted-zone-id "${zone_id}" \
      --query "ResourceRecordSets[?Name=='${fqdn}.' || Name=='${fqdn}'] | [0]" \
      --output json 2>/dev/null || echo "null")"
    if [[ "${existing}" != "null" && -n "${existing}" ]]; then
      aws route53 change-resource-record-sets \
        --hosted-zone-id "${zone_id}" \
        --change-batch "{\"Changes\":[{\"Action\":\"DELETE\",\"ResourceRecordSet\":${existing}}]}" \
        || log "Route53 DELETE skipped (already gone)"
    fi
    log "no InService workers; DNS cleared for ${fqdn}"
    return 0
  fi

  json="$(cat <<EOF
{
  "Comment": "weft workers sync ${INSTANCE_ID}",
  "Changes": [{
    "Action": "UPSERT",
    "ResourceRecordSet": {
      "Name": "${fqdn}",
      "Type": "A",
      "TTL": 10,
      "ResourceRecords": [${records}]
    }
  }]
}
EOF
)"
  aws route53 change-resource-record-sets \
    --hosted-zone-id "${zone_id}" \
    --change-batch "${json}"
  log "Route53 UPSERT ${fqdn} with peers: ${records}"
}

write_env() {
  local role="$1"
  umask 022
  mkdir -p /etc/weft
  # systemd EnvironmentFile: quote values that contain ';' or spaces.
  cat > "${ENV_FILE}" <<EOF
# Generated by weft-bootstrap — do not edit by hand.
WEFT_ROLE=${role}
WEFT_AWS_BIN=/usr/local/bin/aws
AWS_REGION=${REGION}
AWS_DEFAULT_REGION=${REGION}
TMPDIR=${SPILL_MOUNT}
HOME=/var/lib/weft
WEFT_WORKER_COUNT=${WORKER_COUNT}
WEFT_WORKER_PORT=50561
EOF
  if [[ -n "${MEMORY_LIMIT_BYTES}" && "${MEMORY_LIMIT_BYTES}" != "None" ]]; then
    echo "WEFT_MEMORY_LIMIT_BYTES=${MEMORY_LIMIT_BYTES}" >> "${ENV_FILE}"
  fi
  if [[ -n "${SHUFFLE_SPILL_BYTES}" && "${SHUFFLE_SPILL_BYTES}" != "None" ]]; then
    echo "WEFT_SHUFFLE_SPILL_BYTES=${SHUFFLE_SPILL_BYTES}" >> "${ENV_FILE}"
  fi
  if [[ -n "${CATALOG_CONF}" && "${CATALOG_CONF}" != "None" && "${CATALOG_CONF}" != "none" ]]; then
    # Escape embedded double-quotes for systemd EnvironmentFile quoting.
    local escaped
    escaped="${CATALOG_CONF//\"/\\\"}"
    echo "WEFT_CATALOG_CONF=\"${escaped}\"" >> "${ENV_FILE}"
  fi
  if [[ "${role}" == "driver" ]]; then
    cat >> "${ENV_FILE}" <<EOF
WEFT_WORKER_SERVICE=${WORKER_DNS_NAME}
WEFT_SHUFFLE_PARTITIONS=${SHUFFLE_PARTITIONS}
EOF
  else
    cat >> "${ENV_FILE}" <<EOF
WEFT_SHARD_INDEX=${SHARD_INDEX}
EOF
  fi
  chown root:weft "${ENV_FILE}"
  chmod 640 "${ENV_FILE}"
  log "wrote ${ENV_FILE}"
}

enable_role_unit() {
  local role="$1"
  systemctl daemon-reload
  if [[ "${role}" == "driver" ]]; then
    systemctl disable --now weft-worker.service 2>/dev/null || true
    systemctl enable weft-driver.service
  else
    systemctl disable --now weft-driver.service 2>/dev/null || true
    systemctl enable weft-worker.service
  fi
}

# ---- main --------------------------------------------------------------------

TOKEN="$(imds_token)"
INSTANCE_ID="$(imds_get meta-data/instance-id)"
REGION="$(imds_get meta-data/placement/region)"
PRIVATE_IP="$(imds_get meta-data/local-ipv4)"

ROLE="$(tag_value weft:role)"
WORKER_COUNT="$(tag_value weft:worker-count)"
WORKER_ASG="$(tag_value weft:worker-asg)"
HOSTED_ZONE_ID="$(tag_value weft:hosted-zone-id)"
WORKER_DNS_NAME="$(tag_value weft:worker-dns-name)"
MEMORY_LIMIT_BYTES="$(tag_value weft:memory-limit-bytes)"
SHUFFLE_SPILL_BYTES="$(tag_value weft:shuffle-spill-bytes)"
SHUFFLE_PARTITIONS="$(tag_value weft:shuffle-partitions)"
CATALOG_CONF="$(tag_value weft:catalog-conf)"

ROLE="${ROLE:-worker}"
WORKER_COUNT="${WORKER_COUNT:-1}"
SHUFFLE_PARTITIONS="${SHUFFLE_PARTITIONS:-${WORKER_COUNT}}"

if [[ "${1:-}" == "--deregister" ]]; then
  if [[ "${ROLE}" == "worker" && -n "${HOSTED_ZONE_ID}" && -n "${WORKER_DNS_NAME}" && -n "${WORKER_ASG}" ]]; then
    # Re-sync DNS without this instance (lifecycle may already mark it Terminating).
    sync_worker_dns "${HOSTED_ZONE_ID}" "${WORKER_DNS_NAME}" "${WORKER_ASG}" || true
  fi
  exit 0
fi

mount_spill_volume

SHARD_INDEX=0
if [[ "${ROLE}" == "worker" ]]; then
  if [[ -z "${WORKER_ASG}" || "${WORKER_ASG}" == "None" ]]; then
    log "ERROR: weft:worker-asg tag missing; cannot assign shard index"
    exit 1
  fi
  mapfile -t PEER_IDS < <(wait_for_workers "${WORKER_ASG}" "${WORKER_COUNT}")
  # Ensure self is in the list even if not yet InService.
  if ! printf '%s\n' "${PEER_IDS[@]}" | grep -qx "${INSTANCE_ID}"; then
    PEER_IDS+=("${INSTANCE_ID}")
  fi
  IFS=$'\n' PEER_IDS=($(printf '%s\n' "${PEER_IDS[@]}" | sort -u))
  SHARD_INDEX=-1
  for i in "${!PEER_IDS[@]}"; do
    if [[ "${PEER_IDS[$i]}" == "${INSTANCE_ID}" ]]; then
      SHARD_INDEX=$i
      break
    fi
  done
  if (( SHARD_INDEX < 0 )); then
    log "ERROR: could not determine shard index for ${INSTANCE_ID}"
    exit 1
  fi
  log "assigned WEFT_SHARD_INDEX=${SHARD_INDEX} (of ${WORKER_COUNT})"

  if [[ -n "${HOSTED_ZONE_ID}" && -n "${WORKER_DNS_NAME}" ]]; then
    sync_worker_dns "${HOSTED_ZONE_ID}" "${WORKER_DNS_NAME}" "${WORKER_ASG}"
  else
    log "WARNING: Route53 tags missing; driver discovery will fail"
  fi
fi

write_env "${ROLE}"
enable_role_unit "${ROLE}"
log "bootstrap complete role=${ROLE} instance=${INSTANCE_ID}"

#!/usr/bin/env bash
# Weft EC2 bootstrap — runs once at boot (and --deregister on shutdown).
#
# Responsibilities:
#   1. Mount spill EBS (if present) at /var/lib/weft/spill
#   2. Read instance tags / IMDS for role + cluster config
#   3. Workers: assign WEFT_SHARD_INDEX from sorted InService ASG peers
#   4. Workers: upsert the shared A RRSet (boot: full set incl. own IP) and
#      remove ONLY this instance's IP on shutdown (symmetric deregistration)
#   5. Write /etc/weft/weft.env atomically and enable (never start) the
#      matching systemd unit — UserData / the unit graph starts it after us
#
# No credentials are stored — uses the instance profile only.
set -euo pipefail

ENV_FILE=/etc/weft/weft.env
SPILL_MOUNT=/var/lib/weft/spill
IMDS_TOKEN_TTL=21600
# Systemd oneshot PATH can omit /usr/local/bin on some images; pin the AMI aws CLI.
AWS_BIN="${WEFT_AWS_BIN:-/usr/local/bin/aws}"
export PATH="/usr/local/bin:/usr/bin:/bin:${PATH:-}"

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
    out="$("${AWS_BIN}" ec2 describe-tags \
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

# Single-shot variant for the shutdown path (--deregister): no 10×2s retry
# budget when the network may be going away — TimeoutStopSec bounds the whole
# ExecStop, and a missed deregistration is pruned by the next boot's full sync.
tag_value_fast() {
  local key="$1"
  "${AWS_BIN}" ec2 describe-tags \
    --region "${REGION}" \
    --filters "Name=resource-id,Values=${INSTANCE_ID}" "Name=key,Values=${key}" \
    --query 'Tags[0].Value' --output text \
    --cli-connect-timeout 5 --cli-read-timeout 10 2>/dev/null | sed 's/^None$//' || true
}

find_spill_device() {
  # Pick the largest whole-disk block device that is NOT the root disk and has
  # nothing mounted on it (neither the disk itself nor any child). No device
  # name guessing: on Nitro the spill volume may enumerate as nvme0n1, nvme1n1,
  # nvme2n1… depending on attach/enumeration order, so name hints silently miss
  # it (KAN-58: a ~100G spill NVMe went unused and spill landed on the root fs).
  local root_src root_disk="" best="" best_size=0
  root_src="$(findmnt -n -o SOURCE / 2>/dev/null || true)"
  if [[ -n "${root_src}" ]]; then
    root_disk="$(lsblk -no PKNAME "${root_src}" 2>/dev/null || true)"
    if [[ -z "${root_disk}" ]]; then
      root_disk="$(basename "${root_src}" | sed -E 's/p?[0-9]+$//')"
    fi
  fi
  local name size dev
  while read -r name size; do
    [[ -n "${name}" && -n "${size}" ]] || continue
    [[ -n "${root_disk}" && "${name}" == "${root_disk}" ]] && continue
    dev="/dev/${name}"
    # Skip disks with child partitions (root-style layout): mkfs.xfs refuses a
    # partition table anyway, and we never want to clobber one.
    if lsblk -n -o NAME,TYPE "${dev}" 2>/dev/null | awk 'NR>1 && $2=="part"{found=1} END{exit !found}'; then
      continue
    fi
    # Skip anything with a mountpoint on the disk or a descendant (paranoia on
    # top of the root-disk exclusion; also catches [SWAP]).
    if [[ -n "$(lsblk -n -o MOUNTPOINT "${dev}" 2>/dev/null | tr -d '[:space:]')" ]]; then
      continue
    fi
    if (( size > best_size )); then
      best="${dev}"
      best_size="${size}"
    fi
  done < <(lsblk -dbn -o NAME,SIZE,TYPE 2>/dev/null | awk '$3=="disk" && $2>0 {print $1, $2}')
  printf '%s' "${best}"
}

mount_spill_volume() {
  mkdir -p "${SPILL_MOUNT}"
  local dev
  dev="$(find_spill_device)"
  if [[ -z "${dev}" ]]; then
    log "no spill block device found; using root filesystem at ${SPILL_MOUNT}"
    chown weft:weft "${SPILL_MOUNT}"
    return 0
  fi
  if findmnt -n "${SPILL_MOUNT}" >/dev/null 2>&1 || mountpoint -q "${SPILL_MOUNT}" 2>/dev/null; then
    log "spill already mounted at ${SPILL_MOUNT}"
    chown weft:weft "${SPILL_MOUNT}"
    return 0
  fi
  if ! blkid "${dev}" >/dev/null 2>&1; then
    log "formatting ${dev} as xfs"
    mkfs.xfs -f "${dev}"
  fi
  # Idempotent: first boot / fstab / prior bootstrap may already own the device.
  if ! mount -o defaults,nofail "${dev}" "${SPILL_MOUNT}" 2>/tmp/weft-mount.err; then
    if findmnt -n "${SPILL_MOUNT}" >/dev/null 2>&1 || mountpoint -q "${SPILL_MOUNT}" 2>/dev/null; then
      log "spill became mounted at ${SPILL_MOUNT} during race"
    elif grep -qiE 'already mounted|busy' /tmp/weft-mount.err 2>/dev/null; then
      log "spill device ${dev} busy; continuing with ${SPILL_MOUNT}"
    else
      log "ERROR: mount ${dev} -> ${SPILL_MOUNT} failed: $(cat /tmp/weft-mount.err 2>/dev/null || true)"
      return 1
    fi
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
    ids="$("${AWS_BIN}" autoscaling describe-auto-scaling-groups \
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
# Boot path: rewrite the full set from the current InService peer list PLUS
# this instance's own IP. Self must always be included — on a cold start the
# ASG may not have flipped us to InService yet, and without self in the set
# the last worker to boot would upsert a set missing itself until some other
# worker reboots. Dead instances are pruned here because they are no longer
# InService. Shutdown path is remove_self_dns() (symmetric, self-only).
sync_worker_dns() {
  local zone_id="$1"
  local fqdn="$2"
  local asg="$3"
  local ids ip records json
  ids="$("${AWS_BIN}" autoscaling describe-auto-scaling-groups \
    --region "${REGION}" \
    --auto-scaling-group-names "${asg}" \
    --query 'AutoScalingGroups[0].Instances[?LifecycleState==`InService`].InstanceId' \
    --output text 2>/dev/null | tr '\t' '\n' | sort)"
  # Own IP first, then InService peers (dedup via sort -u when rendering).
  local ips="${PRIVATE_IP}"
  while IFS= read -r id; do
    [[ -z "${id}" || "${id}" == "${INSTANCE_ID}" ]] && continue
    ip="$("${AWS_BIN}" ec2 describe-instances \
      --region "${REGION}" \
      --instance-ids "${id}" \
      --query 'Reservations[0].Instances[0].PrivateIpAddress' \
      --output text)"
    if [[ -n "${ip}" && "${ip}" != "None" ]]; then
      ips+=$'\n'"${ip}"
    fi
  done <<< "${ids}"

  records=""
  while IFS= read -r ip; do
    [[ -z "${ip}" ]] && continue
    records+="${records:+,}{\"Value\":\"${ip}\"}"
  done < <(printf '%s\n' "${ips}" | sort -u)

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
  "${AWS_BIN}" route53 change-resource-record-sets \
    --hosted-zone-id "${zone_id}" \
    --change-batch "${json}"
  log "Route53 UPSERT ${fqdn} (self + InService peers): ${records}"
}

# Shutdown / scale-in deregistration: remove ONLY this instance's IP from the
# shared RRSet (symmetric with boot, which adds it). Never rewrite the full
# peer set here — at stop time the ASG may still list us InService, so a full
# re-sync would re-add our own dying IP and leave it stale forever if no other
# worker happens to bootstrap afterwards (KAN-58: zombie worker IPs in
# workers.<zone> → driver queries failed with "no free task slots" on dead
# instances). Instances that die without running this are pruned by the next
# worker boot's sync_worker_dns().
remove_self_dns() {
  local zone_id="$1"
  local fqdn="$2"
  local values ip records="" json existing
  values="$("${AWS_BIN}" route53 list-resource-record-sets \
    --hosted-zone-id "${zone_id}" \
    --query "ResourceRecordSets[?Name=='${fqdn}.' || Name=='${fqdn}'].ResourceRecords[].Value" \
    --output text --cli-connect-timeout 5 --cli-read-timeout 10 2>/dev/null || true)"
  if [[ -z "${values//[[:space:]]/}" ]]; then
    log "no A records for ${fqdn}; nothing to deregister"
    return 0
  fi
  for ip in ${values}; do
    [[ "${ip}" == "${PRIVATE_IP}" || "${ip}" == "None" ]] && continue
    records+="${records:+,}{\"Value\":\"${ip}\"}"
  done
  if [[ -z "${records}" ]]; then
    # Ours was the last record — delete the whole RRSet (needs the exact set).
    existing="$("${AWS_BIN}" route53 list-resource-record-sets \
      --hosted-zone-id "${zone_id}" \
      --query "ResourceRecordSets[?Name=='${fqdn}.' || Name=='${fqdn}'] | [0]" \
      --output json --cli-connect-timeout 5 --cli-read-timeout 10 2>/dev/null || true)"
    if [[ -n "${existing}" && "${existing}" != "null" && "${existing}" != "None" ]]; then
      if "${AWS_BIN}" route53 change-resource-record-sets \
        --hosted-zone-id "${zone_id}" \
        --change-batch "{\"Changes\":[{\"Action\":\"DELETE\",\"ResourceRecordSet\":${existing}}]}" \
        --cli-connect-timeout 5 --cli-read-timeout 10; then
        log "Route53 DELETE ${fqdn} (removed last record ${PRIVATE_IP})"
      else
        log "Route53 DELETE ${fqdn} failed (already gone?)"
      fi
    fi
    return 0
  fi
  json="$(cat <<EOF
{
  "Comment": "weft worker deregister ${INSTANCE_ID}",
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
  if "${AWS_BIN}" route53 change-resource-record-sets \
    --hosted-zone-id "${zone_id}" \
    --change-batch "${json}" \
    --cli-connect-timeout 5 --cli-read-timeout 10; then
    log "Route53 removed ${PRIVATE_IP} from ${fqdn}; remaining: ${records}"
  else
    log "Route53 deregister of ${PRIVATE_IP} from ${fqdn} FAILED — next worker boot's full sync will prune it"
  fi
}

write_env() {
  local role="$1"
  umask 022
  mkdir -p /etc/weft
  # Atomic: render into a temp file in the same directory, then rename. A
  # bootstrap kill (TimeoutStartSec, power loss) mid-write must never leave a
  # truncated env file behind — systemd's EnvironmentFile= would silently load
  # partial config (missing WEFT_SHARD_INDEX ⇒ duplicate shards / wrong counts).
  local tmp
  tmp="$(mktemp "${ENV_FILE}.tmp.XXXXXX")"
  {
    # systemd EnvironmentFile: quote values that contain ';' or spaces.
    cat <<EOF
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
      echo "WEFT_MEMORY_LIMIT_BYTES=${MEMORY_LIMIT_BYTES}"
    fi
    if [[ -n "${SHUFFLE_SPILL_BYTES}" && "${SHUFFLE_SPILL_BYTES}" != "None" ]]; then
      echo "WEFT_SHUFFLE_SPILL_BYTES=${SHUFFLE_SPILL_BYTES}"
    fi
    if [[ -n "${CATALOG_CONF}" && "${CATALOG_CONF}" != "None" && "${CATALOG_CONF}" != "none" ]]; then
      # Escape embedded double-quotes for systemd EnvironmentFile quoting.
      local escaped
      escaped="${CATALOG_CONF//\"/\\\"}"
      echo "WEFT_CATALOG_CONF=\"${escaped}\""
    fi
    if [[ -n "${PREFER_HASH_JOIN}" && "${PREFER_HASH_JOIN}" != "None" ]]; then
      echo "WEFT_PREFER_HASH_JOIN=${PREFER_HASH_JOIN}"
    fi
    if [[ "${role}" == "driver" ]]; then
      cat <<EOF
WEFT_WORKER_SERVICE=${WORKER_DNS_NAME}
WEFT_SHUFFLE_PARTITIONS=${SHUFFLE_PARTITIONS}
EOF
      if [[ "${DISTRIBUTED_STRICT}" == "true" || "${DISTRIBUTED_STRICT}" == "1" ]]; then
        echo "WEFT_DISTRIBUTED_STRICT=1"
      fi
    else
      cat <<EOF
WEFT_SHARD_INDEX=${SHARD_INDEX}
EOF
    fi
  } > "${tmp}"
  chown root:weft "${tmp}"
  chmod 640 "${tmp}"
  mv -f "${tmp}" "${ENV_FILE}"
  log "wrote ${ENV_FILE} (atomic)"
}

enable_role_unit() {
  local role="$1"
  # Enable only — NEVER `systemctl start` / `--now` the role unit from inside
  # this oneshot: weft-driver/weft-worker declare Requires=+After=
  # weft-bootstrap, so a synchronous start here circular-waits until
  # TimeoutStartSec kills bootstrap and (via Requires=) the role unit never
  # starts again (KAN-58 boot deadlock). UserData starts the role unit on
  # first boot; the WantedBy/Requires/After graph does it on reboots — always
  # AFTER this unit has completed (env file + DNS + spill are the
  # preconditions). The opposite-role stop below is --no-block for the same
  # reason: bootstrap must not wait on any other unit's job.
  systemctl daemon-reload
  if [[ "${role}" == "driver" ]]; then
    systemctl disable weft-worker.service 2>/dev/null || true
    systemctl stop --no-block weft-worker.service 2>/dev/null || true
    systemctl enable weft-driver.service
  else
    systemctl disable weft-driver.service 2>/dev/null || true
    systemctl stop --no-block weft-driver.service 2>/dev/null || true
    systemctl enable weft-worker.service
  fi
}

# ---- main --------------------------------------------------------------------

TOKEN="$(imds_token)"
INSTANCE_ID="$(imds_get meta-data/instance-id)"
REGION="$(imds_get meta-data/placement/region)"
PRIVATE_IP="$(imds_get meta-data/local-ipv4)"

# Shutdown / scale-in fast path: fetch ONLY the tags deregistration needs,
# single-shot (network may be going away; TimeoutStopSec bounds us).
if [[ "${1:-}" == "--deregister" ]]; then
  ROLE="$(tag_value_fast weft:role)"
  HOSTED_ZONE_ID="$(tag_value_fast weft:hosted-zone-id)"
  WORKER_DNS_NAME="$(tag_value_fast weft:worker-dns-name)"
  ROLE="${ROLE:-worker}"
  if [[ "${ROLE}" == "worker" && -n "${HOSTED_ZONE_ID}" && -n "${WORKER_DNS_NAME}" ]]; then
    remove_self_dns "${HOSTED_ZONE_ID}" "${WORKER_DNS_NAME}" || true
  fi
  exit 0
fi

ROLE="$(tag_value weft:role)"
WORKER_COUNT="$(tag_value weft:worker-count)"
WORKER_ASG="$(tag_value weft:worker-asg)"
HOSTED_ZONE_ID="$(tag_value weft:hosted-zone-id)"
WORKER_DNS_NAME="$(tag_value weft:worker-dns-name)"
MEMORY_LIMIT_BYTES="$(tag_value weft:memory-limit-bytes)"
SHUFFLE_SPILL_BYTES="$(tag_value weft:shuffle-spill-bytes)"
SHUFFLE_PARTITIONS="$(tag_value weft:shuffle-partitions)"
CATALOG_CONF="$(tag_value weft:catalog-conf)"
DISTRIBUTED_STRICT="$(tag_value weft:distributed-strict)"
PREFER_HASH_JOIN="$(tag_value weft:prefer-hash-join)"

ROLE="${ROLE:-worker}"
WORKER_COUNT="${WORKER_COUNT:-1}"
SHUFFLE_PARTITIONS="${SHUFFLE_PARTITIONS:-${WORKER_COUNT}}"
DISTRIBUTED_STRICT="${DISTRIBUTED_STRICT:-false}"
PREFER_HASH_JOIN="${PREFER_HASH_JOIN:-auto}"

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

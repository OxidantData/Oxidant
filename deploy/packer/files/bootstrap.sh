#!/usr/bin/env bash
# Oxidant EC2 bootstrap — runs once at boot (and --deregister on shutdown).
#
# Responsibilities:
#   1. Mount spill EBS (if present) at /var/lib/oxidant/spill
#   2. Read instance tags / IMDS for role + cluster config
#   3. Workers: assign OXIDANT_SHARD_INDEX from sorted InService ASG peers
#   4. Driver: wait for WorkerCount InService peers, pin private IPs into
#      OXIDANT_WORKERS (Spark EMR / YARN-style membership — not Route53/DNS)
#   5. Write /etc/oxidant/oxidant.env atomically and enable (never start) the
#      matching systemd unit — UserData / the unit graph starts it after us
#
# No credentials are stored — uses the instance profile only (IAM Describe*).
set -euo pipefail

ENV_FILE=/etc/oxidant/oxidant.env
SPILL_MOUNT=/var/lib/oxidant/spill
IMDS_TOKEN_TTL=21600
# Systemd oneshot PATH can omit /usr/local/bin on some images; pin the AMI aws CLI.
AWS_BIN="${OXIDANT_AWS_BIN:-/usr/local/bin/aws}"
export PATH="/usr/local/bin:/usr/bin:/bin:${PATH:-}"

# Always stderr — stdout from helpers (e.g. wait_for_worker_private_ips) is machine-parsed.
log() { echo "[oxidant-bootstrap] $*" >&2; }

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
    chown oxidant:oxidant "${SPILL_MOUNT}"
    return 0
  fi
  if findmnt -n "${SPILL_MOUNT}" >/dev/null 2>&1 || mountpoint -q "${SPILL_MOUNT}" 2>/dev/null; then
    log "spill already mounted at ${SPILL_MOUNT}"
    chown oxidant:oxidant "${SPILL_MOUNT}"
    return 0
  fi
  if ! blkid "${dev}" >/dev/null 2>&1; then
    log "formatting ${dev} as xfs"
    mkfs.xfs -f "${dev}"
  fi
  # Idempotent: first boot / fstab / prior bootstrap may already own the device.
  if ! mount -o defaults,nofail "${dev}" "${SPILL_MOUNT}" 2>/tmp/oxidant-mount.err; then
    if findmnt -n "${SPILL_MOUNT}" >/dev/null 2>&1 || mountpoint -q "${SPILL_MOUNT}" 2>/dev/null; then
      log "spill became mounted at ${SPILL_MOUNT} during race"
    elif grep -qiE 'already mounted|busy' /tmp/oxidant-mount.err 2>/dev/null; then
      log "spill device ${dev} busy; continuing with ${SPILL_MOUNT}"
    else
      log "ERROR: mount ${dev} -> ${SPILL_MOUNT} failed: $(cat /tmp/oxidant-mount.err 2>/dev/null || true)"
      return 1
    fi
  fi
  # Persist across reboot.
  local uuid
  uuid="$(blkid -s UUID -o value "${dev}")"
  if ! grep -q "${uuid}" /etc/fstab 2>/dev/null; then
    echo "UUID=${uuid} ${SPILL_MOUNT} xfs defaults,nofail 0 2" >> /etc/fstab
  fi
  chown oxidant:oxidant "${SPILL_MOUNT}"
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

# Spark EMR / YARN-style membership: the control plane learns executor private IPs
# from the cluster manager (here: ASG + EC2 DescribeInstances via instance role).
# No Route53, no UDP broadcast (broadcast is insecure and does not cross AWS subnets).
# Output: one private IPv4 per line, sorted unique. Fails closed if count < expected.
# stdin: private IPv4 lines → stdout: `ip:50561,ip:50561` (sorted unique, empty → empty).
private_ips_to_workers_csv() {
  local port="${1:-50561}"
  # Only accept dotted IPv4 — never let a log prefix like "[oxidant-bootstrap]" become a host.
  sort -u | awk -v port="${port}" '$1 ~ /^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$/ {printf sep $1":"port; sep=","}'
}

wait_for_worker_private_ips() {
  local asg="$1"
  local expected="$2"
  # Tests may shrink the wait; production keeps the cold-start ASG budget.
  local wait_secs="${OXIDANT_BOOTSTRAP_WAIT_SECS:-300}"
  local deadline=$((SECONDS + wait_secs))
  local ids="" id ip ips="" count=0
  while (( SECONDS < deadline )); do
    ids="$("${AWS_BIN}" autoscaling describe-auto-scaling-groups \
      --region "${REGION}" \
      --auto-scaling-group-names "${asg}" \
      --query 'AutoScalingGroups[0].Instances[?LifecycleState==`InService`].InstanceId' \
      --output text 2>/dev/null | tr '\t' '\n' | sort -u)"
    ips=""
    while IFS= read -r id; do
      [[ -z "${id}" ]] && continue
      ip="$("${AWS_BIN}" ec2 describe-instances \
        --region "${REGION}" \
        --instance-ids "${id}" \
        --query 'Reservations[0].Instances[0].PrivateIpAddress' \
        --output text 2>/dev/null || true)"
      if [[ -n "${ip}" && "${ip}" != "None" ]]; then
        ips+="${ip}"$'\n'
      fi
    done <<< "${ids}"
    count="$(printf '%s' "${ips}" | grep -c . || true)"
    # Exact match only — during ASG instance refresh InService can briefly exceed
    # WorkerCount; pinning that set makes driver fan-out ≠ OXIDANT_WORKER_COUNT and
    # fail closed ("silently drop data"). Wait for a settled membership.
    if [[ "${count}" -eq "${expected}" ]]; then
      log "ASG ${asg}: ${count}/${expected} worker private IPs ready"
      printf '%s' "${ips}" | sort -u
      return 0
    fi
    log "waiting for exactly ${expected} worker private IPs from ASG ${asg} (have ${count})"
    sleep "${OXIDANT_BOOTSTRAP_POLL_SECS:-5}"
  done
  log "ERROR: timed out waiting for ${expected} worker private IPs from ASG ${asg} (have ${count})"
  return 1
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
  "Comment": "oxidant workers sync ${INSTANCE_ID}",
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
  "Comment": "oxidant worker deregister ${INSTANCE_ID}",
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
  mkdir -p /etc/oxidant
  # S3 disk cache (PR #76): workers only. The cache materializes *whole* S3 objects on
  # first touch — fine for SF10 multi-file facts, lethal on the driver (and on SF100
  # single-file 20+ GiB parquet) because Connect planning / schema reads pull the same
  # keys and the driver then downloads tens of GiB onto an 8–16 GiB box. Empty/unset
  # disables the cache (ranged parquet reads stay on S3).
  mkdir -p /var/lib/oxidant/s3cache
  chown oxidant:oxidant /var/lib/oxidant/s3cache
  # Atomic: render into a temp file in the same directory, then rename. A
  # bootstrap kill (TimeoutStartSec, power loss) mid-write must never leave a
  # truncated env file behind — systemd's EnvironmentFile= would silently load
  # partial config (missing OXIDANT_SHARD_INDEX ⇒ duplicate shards / wrong counts).
  local tmp
  tmp="$(mktemp "${ENV_FILE}.tmp.XXXXXX")"
  {
    # systemd EnvironmentFile: quote values that contain ';' or spaces.
    cat <<EOF
# Generated by oxidant-bootstrap — do not edit by hand.
OXIDANT_ROLE=${role}
OXIDANT_AWS_BIN=/usr/local/bin/aws
AWS_REGION=${REGION}
AWS_DEFAULT_REGION=${REGION}
TMPDIR=${SPILL_MOUNT}
HOME=/var/lib/oxidant
EOF
    # Cluster membership vars are meaningless on a standalone single node.
    if [[ "${role}" != "standalone" ]]; then
      cat <<EOF
OXIDANT_WORKER_COUNT=${WORKER_COUNT}
OXIDANT_WORKER_PORT=50561
EOF
    fi
    # Worker spill pool only. The CFN MemoryLimitBytes tag is the *worker* SF100
    # invariant (40 Gi on m8g.8xlarge). Applying it on the driver stamps a 40 Gi
    # FairSpillPool onto an 8–16 GiB c6g and the OOM killer reaps Connect mid-query
    # (TPC-H Q3, 2026-08-10). Drivers auto-size from cgroup instead.
    if [[ "${role}" == "worker" || "${role}" == "standalone" ]]; then
      if [[ -n "${MEMORY_LIMIT_BYTES}" && "${MEMORY_LIMIT_BYTES}" != "None" ]]; then
        echo "OXIDANT_MEMORY_LIMIT_BYTES=${MEMORY_LIMIT_BYTES}"
      fi
      if [[ -n "${SHUFFLE_SPILL_BYTES}" && "${SHUFFLE_SPILL_BYTES}" != "None" ]]; then
        echo "OXIDANT_SHUFFLE_SPILL_BYTES=${SHUFFLE_SPILL_BYTES}"
      fi
      echo "OXIDANT_S3_CACHE_DIR=/var/lib/oxidant/s3cache"
      # Cap single-object materialization so a 20+ GiB lineitem.parquet cannot pin the
      # worker for 10+ minutes downloading the whole file (ranged reads still work).
      echo "OXIDANT_S3_CACHE_MAX_OBJECT_BYTES=2147483648"
    fi
    if [[ -n "${CATALOG_CONF}" && "${CATALOG_CONF}" != "None" && "${CATALOG_CONF}" != "none" ]]; then
      # Escape embedded double-quotes for systemd EnvironmentFile quoting.
      local escaped
      escaped="${CATALOG_CONF//\"/\\\"}"
      echo "OXIDANT_CATALOG_CONF=\"${escaped}\""
    fi
    if [[ -n "${PREFER_HASH_JOIN}" && "${PREFER_HASH_JOIN}" != "None" ]]; then
      echo "OXIDANT_PREFER_HASH_JOIN=${PREFER_HASH_JOIN}"
    fi
    if [[ "${role}" == "driver" ]]; then
      cat <<EOF
OXIDANT_S3_CACHE_DIR=
OXIDANT_STAGE_TIMEOUT_MS=3600000
OXIDANT_AQE=1
RUST_LOG=info,oxidant=info,oxidant_connect=info,oxidant_execution=info
EOF
      # Membership = private Flight endpoints from ASG (see wait_for_worker_private_ips).
      # Do not set OXIDANT_WORKER_SERVICE for EC2 — that DNS path was Route53-coupled and
      # raced empty at boot. Optional k8s headless DNS remains a separate deploy model.
      if [[ -n "${DRIVER_WORKERS_CSV:-}" ]]; then
        echo "OXIDANT_WORKERS=${DRIVER_WORKERS_CSV}"
        log "pinned OXIDANT_WORKERS=${DRIVER_WORKERS_CSV}" >&2
      else
        log "ERROR: driver has empty worker IP list — refusing silent local fallback" >&2
        return 1
      fi
      if [[ -n "${SHUFFLE_PARTITIONS}" && "${SHUFFLE_PARTITIONS}" != "None" ]]; then
        echo "OXIDANT_SHUFFLE_PARTITIONS=${SHUFFLE_PARTITIONS}"
      fi
      if [[ "${DISTRIBUTED_STRICT}" == "true" || "${DISTRIBUTED_STRICT}" == "1" ]]; then
        echo "OXIDANT_DISTRIBUTED_STRICT=1"
      fi
    elif [[ "${role}" == "worker" ]]; then
      cat <<EOF
OXIDANT_SHARD_INDEX=${SHARD_INDEX}
OXIDANT_STAGE_TIMEOUT_MS=3600000
OXIDANT_AQE=1
RUST_LOG=info,oxidant=info,oxidant_connect=info,oxidant_execution=info
EOF
    fi
    # standalone: no cluster vars at all — the server runs single-node.
  } > "${tmp}"
  chown root:oxidant "${tmp}"
  chmod 640 "${tmp}"
  mv -f "${tmp}" "${ENV_FILE}"
  log "wrote ${ENV_FILE} (atomic)"
}

enable_role_unit() {
  local role="$1"
  # Enable only — NEVER `systemctl start` / `--now` the role unit from inside
  # this oneshot: oxidant-driver/oxidant-worker declare Requires=+After=
  # oxidant-bootstrap, so a synchronous start here circular-waits until
  # TimeoutStartSec kills bootstrap and (via Requires=) the role unit never
  # starts again (KAN-58 boot deadlock). UserData starts the role unit on
  # first boot; the WantedBy/Requires/After graph does it on reboots — always
  # AFTER this unit has completed (env file + DNS + spill are the
  # preconditions). The opposite-role stop below is --no-block for the same
  # reason: bootstrap must not wait on any other unit's job.
  systemctl daemon-reload
  if [[ "${role}" == "driver" ]]; then
    systemctl disable oxidant-worker.service oxidant-standalone.service 2>/dev/null || true
    systemctl stop --no-block oxidant-worker.service oxidant-standalone.service 2>/dev/null || true
    systemctl enable oxidant-driver.service
  elif [[ "${role}" == "worker" ]]; then
    systemctl disable oxidant-driver.service oxidant-standalone.service 2>/dev/null || true
    systemctl stop --no-block oxidant-driver.service oxidant-standalone.service 2>/dev/null || true
    systemctl enable oxidant-worker.service
  else
    # standalone: single-node Connect server, no driver/worker split.
    systemctl disable oxidant-driver.service oxidant-worker.service 2>/dev/null || true
    systemctl stop --no-block oxidant-driver.service oxidant-worker.service 2>/dev/null || true
    systemctl enable oxidant-standalone.service
  fi
}

# ---- main --------------------------------------------------------------------

oxidant_bootstrap_main() {
  TOKEN="$(imds_token)"
  INSTANCE_ID="$(imds_get meta-data/instance-id)"
  REGION="$(imds_get meta-data/placement/region)"
  PRIVATE_IP="$(imds_get meta-data/local-ipv4)"

  # Shutdown / scale-in fast path: fetch ONLY the tags deregistration needs,
  # single-shot (network may be going away; TimeoutStopSec bounds us).
  if [[ "${1:-}" == "--deregister" ]]; then
    ROLE="$(tag_value_fast oxidant:role)"
    HOSTED_ZONE_ID="$(tag_value_fast oxidant:hosted-zone-id)"
    WORKER_DNS_NAME="$(tag_value_fast oxidant:worker-dns-name)"
    ROLE="${ROLE:-worker}"
    if [[ "${ROLE}" == "worker" && -n "${HOSTED_ZONE_ID}" && -n "${WORKER_DNS_NAME}" ]]; then
      remove_self_dns "${HOSTED_ZONE_ID}" "${WORKER_DNS_NAME}" || true
    fi
    return 0
  fi

  ROLE="$(tag_value oxidant:role)"
  WORKER_COUNT="$(tag_value oxidant:worker-count)"
  WORKER_ASG="$(tag_value oxidant:worker-asg)"
  HOSTED_ZONE_ID="$(tag_value oxidant:hosted-zone-id)"
  WORKER_DNS_NAME="$(tag_value oxidant:worker-dns-name)"
  MEMORY_LIMIT_BYTES="$(tag_value oxidant:memory-limit-bytes)"
  SHUFFLE_SPILL_BYTES="$(tag_value oxidant:shuffle-spill-bytes)"
  SHUFFLE_PARTITIONS="$(tag_value oxidant:shuffle-partitions)"
  CATALOG_CONF="$(tag_value oxidant:catalog-conf)"
  DISTRIBUTED_STRICT="$(tag_value oxidant:distributed-strict)"
  PREFER_HASH_JOIN="$(tag_value oxidant:prefer-hash-join)"

  # No role tag = Marketplace single-node AMI path: boot straight into a
  # standalone Spark Connect server (no shard index, no ASG membership).
  ROLE="${ROLE:-standalone}"
  WORKER_COUNT="${WORKER_COUNT:-1}"
  # Leave shuffle partitions empty when the CFN tag is unset so the engine applies its
  # Spark-like default (max(200, worker_vcpus)) — never pin to WorkerCount (2-bucket SF100 skew).
  SHUFFLE_PARTITIONS="${SHUFFLE_PARTITIONS:-}"
  DISTRIBUTED_STRICT="${DISTRIBUTED_STRICT:-false}"
  PREFER_HASH_JOIN="${PREFER_HASH_JOIN:-auto}"

  mount_spill_volume

  SHARD_INDEX=0
  if [[ "${ROLE}" == "worker" ]]; then
    if [[ -z "${WORKER_ASG}" || "${WORKER_ASG}" == "None" ]]; then
      log "ERROR: oxidant:worker-asg tag missing; cannot assign shard index"
      return 1
    fi
    mapfile -t PEER_IDS < <(wait_for_workers "${WORKER_ASG}" "${WORKER_COUNT}")
    # Ensure self is in the list even if not yet InService.
    if ! printf '%s\n' "${PEER_IDS[@]}" | grep -qx "${INSTANCE_ID}"; then
      PEER_IDS+=("${INSTANCE_ID}")
    fi
    IFS=$'\n' PEER_IDS=($(printf '%s\n' "${PEER_IDS[@]}" | sort -u))
    # Loud-fail on an incomplete peer list: assigning a shard index from fewer than
    # WORKER_COUNT peers silently duplicates an index on another worker (the doomed
    # shard is then read by NOBODY — wrong query results, no error). Note this cannot
    # cover the instance-refresh transitional case (an early replacement legitimately
    # sees a full list that still contains a doomed old worker and takes its slot);
    # that one self-heals via oxidant-shard-resolve.timer re-resolving against settled
    # membership.
    if (( ${#PEER_IDS[@]} < WORKER_COUNT )); then
      log "ERROR: only ${#PEER_IDS[@]} of ${WORKER_COUNT} worker peers visible; refusing to assign a shard index from an incomplete list"
      return 1
    fi
    SHARD_INDEX=-1
    for i in "${!PEER_IDS[@]}"; do
      if [[ "${PEER_IDS[$i]}" == "${INSTANCE_ID}" ]]; then
        SHARD_INDEX=$i
        break
      fi
    done
    if (( SHARD_INDEX < 0 )); then
      log "ERROR: could not determine shard index for ${INSTANCE_ID}"
      return 1
    fi
    log "assigned OXIDANT_SHARD_INDEX=${SHARD_INDEX} (of ${WORKER_COUNT})"
  fi

  # Driver: wait for full InService set, pin private IPs into OXIDANT_WORKERS (Spark
  # "executors registered" gate — not DNS / Route53).
  DRIVER_WORKERS_CSV=""
  if [[ "${ROLE}" == "driver" ]]; then
    if [[ -z "${WORKER_ASG}" || "${WORKER_ASG}" == "None" ]]; then
      log "ERROR: oxidant:worker-asg tag missing on driver; cannot discover workers"
      return 1
    fi
    local_ips=""
    local_ips="$(wait_for_worker_private_ips "${WORKER_ASG}" "${WORKER_COUNT}")" || return 1
    DRIVER_WORKERS_CSV="$(printf '%s\n' "${local_ips}" | private_ips_to_workers_csv 50561)"
    if [[ -z "${DRIVER_WORKERS_CSV}" ]]; then
      log "ERROR: ASG ${WORKER_ASG} yielded no private IPs"
      return 1
    fi
    log "driver membership OXIDANT_WORKERS=${DRIVER_WORKERS_CSV}"
  fi

  write_env "${ROLE}"
  enable_role_unit "${ROLE}"
  log "bootstrap complete role=${ROLE} instance=${INSTANCE_ID}"
}

# Sourced by deploy/packer/tests — keep functions without running IMDS/main.
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  oxidant_bootstrap_main "$@"
fi

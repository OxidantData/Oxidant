#!/usr/bin/env bash
# Attach a dedicated spill volume to every running weft-sf10 instance.
# The AMI bootstrap looks for a spill block device but the LaunchTemplate
# provides none, so spill lands on the 40GB root and SF10 queries ENOSPC
# (KAN-57). This gives each node a 200GB gp3 at /var/lib/weft/spill,
# marked DeleteOnTermination so it dies with the instance (ASG scale-to-0
# = full cleanup, no orphan volumes).
# Usage: bash scripts/sf10-attach-spill.sh   (after sf10-start.sh)
set -euo pipefail
REGION="${AWS_REGION:-us-west-2}"
SIZE_GIB="${SPILL_GIB:-200}"
KEY=~/.ssh/id_ed25519_kaicoder03
SSH_OPTS=(-i "$KEY" -o IdentitiesOnly=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=accept-new)

DRIVER_IP=$(aws ec2 describe-instances --region "$REGION" \
  --filters "Name=tag:aws:autoscaling:groupName,Values=weft-sf10-driver" "Name=instance-state-name,Values=running" \
  --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)

aws ec2 describe-instances --region "$REGION" \
  --filters "Name=tag:aws:autoscaling:groupName,Values=weft-sf10-driver,weft-sf10-workers" "Name=instance-state-name,Values=running" \
  --query 'Reservations[].Instances[].[InstanceId,Placement.AvailabilityZone,PrivateIpAddress,Tags[?Key==`aws:autoscaling:groupName`]|[0].Value]' \
  --output text | while read -r IID AZ PRIV ASG; do
  echo "== $ASG $IID ($AZ, $PRIV) =="
  EXISTING=$(aws ec2 describe-volumes --region "$REGION" \
    --filters "Name=attachment.instance-id,Values=$IID" "Name=attachment.device,Values=/dev/sdf" \
    --query 'Volumes[0].VolumeId' --output text)
  if [ "$EXISTING" != "None" ] && [ -n "$EXISTING" ]; then
    echo "  spill volume already attached: $EXISTING"
  else
    VOL=$(aws ec2 create-volume --region "$REGION" --availability-zone "$AZ" \
          --volume-type gp3 --size "$SIZE_GIB" \
          --tag-specifications "ResourceType=volume,Tags=[{Key=Name,Value=weft-sf10-spill},{Key=cluster,Value=weft-sf10}]" \
          --query 'VolumeId' --output text)
    aws ec2 wait volume-available --region "$REGION" --volume-ids "$VOL"
    aws ec2 attach-volume --region "$REGION" --instance-id "$IID" --volume-id "$VOL" --device /dev/sdf
    aws ec2 modify-instance-attribute --region "$REGION" --instance-id "$IID" \
      --block-device-mappings "[{\"DeviceName\":\"/dev/sdf\",\"Ebs\":{\"DeleteOnTermination\":true}}]"
    echo "  attached $VOL (DeleteOnTermination=true)"
  fi

  if [ "$ASG" = "weft-sf10-driver" ]; then
    TARGET="ec2-user@$DRIVER_IP"; EXTRA=()
  else
    TARGET="ec2-user@$PRIV"
    EXTRA=(-o "ProxyCommand ssh -i $KEY -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new -W %h:%p ec2-user@$DRIVER_IP")
  fi
  # wait for the device node, then format (if fresh) + mount
  ssh -n "${SSH_OPTS[@]}" "${EXTRA[@]}" "$TARGET" '
    set -e
    for i in $(seq 1 30); do
      DEV=$(sudo lsblk -nrbo NAME,SIZE,TYPE,MOUNTPOINT | awk "\$3==\"disk\" && \$4==\"\" {print \$2, \"/dev/\" \$1}" | sort -rn | awk "NR==1 {print \$2}")
      [ -n "$DEV" ] && break; sleep 2
    done
    [ -n "$DEV" ] || { echo "no unmounted spill device found"; exit 1; }
    if ! sudo blkid $DEV >/dev/null 2>&1; then sudo mkfs.xfs -q $DEV; fi
    sudo mkdir -p /var/lib/weft/spill
    mountpoint -q /var/lib/weft/spill || sudo mount $DEV /var/lib/weft/spill
    sudo chown weft:weft /var/lib/weft/spill
    echo "weft-sf10-spill $DEV on /var/lib/weft/spill" | sudo tee /etc/weft/spill-device >/dev/null
    df -h /var/lib/weft/spill | tail -1' && echo "  mounted at /var/lib/weft/spill" || echo "  MOUNT FAILED on $PRIV ($ASG)"
done
echo "== spill volumes attached on all nodes; restart weft services to use them =="
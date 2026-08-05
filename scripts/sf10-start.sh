#!/usr/bin/env bash
# Start the weft-sf10 cluster after scripts/sf10-stop.sh: scales the ASGs back
# (driver 1, workers 2), waits for instances, prints the new driver public IP.
#
# The baked AMI (ami-06062f1fc09e4e2fa and later) brings nodes up fully
# configured — WEFT_PREFER_HASH_JOIN=auto, S3 disk cache on, spill on the
# right disk, no manual repair. To run the LATEST engine build instead of the
# baked binary, redeploy it:
#   WEFT_SF10_DRIVER=<ip> WEFT_SF10_W0=<w0> WEFT_SF10_W1=<w1> \
#     bash bench/sf100/results/deploy-sf10.sh target/linux-al2023/release/weft
# Caveat: a CloudFormation instance *refresh* (stack AmiId change) can hand two
# workers the same shard index (E-EC2-SHARD-REFRESH) — weft-shard-resolve.timer
# self-heals within minutes on AMI v3+; on older AMIs check
# `grep SHARD_INDEX /etc/weft/weft.env` on both workers and
# `sudo systemctl restart weft-bootstrap && sudo systemctl restart weft-worker`
# on the duplicate.
# Note: the driver gets a NEW public IP — update SSH commands and the
# sc:// endpoint accordingly.
set -euo pipefail
REGION="${AWS_REGION:-us-west-2}"
WORKERS="${1:-2}"

echo "== scaling weft-sf10-driver to 1, weft-sf10-workers to $WORKERS =="
aws autoscaling update-auto-scaling-group --auto-scaling-group-name weft-sf10-driver  --min-size 1 --desired-capacity 1 --region "$REGION"
aws autoscaling update-auto-scaling-group --auto-scaling-group-name weft-sf10-workers --min-size "$WORKERS" --desired-capacity "$WORKERS" --region "$REGION"

echo "== waiting for instances =="
for want in "weft-sf10-driver:1" "weft-sf10-workers:$WORKERS"; do
  asg="${want%%:*}"; exp="${want##*:}"
  while :; do
    n=$(aws autoscaling describe-auto-scaling-groups --auto-scaling-group-names "$asg" --region "$REGION" \
        --query "length(AutoScalingGroups[0].Instances[?LifecycleState=='InService'])" --output text)
    [ "$n" = "$exp" ] && break
    echo "  $asg: $n/$exp InService..."; sleep 20
  done
  echo "  $asg: ready"
done

DRIVER=$(aws ec2 describe-instances --region "$REGION" \
  --filters "Name=tag:aws:autoscaling:groupName,Values=weft-sf10-driver" "Name=instance-state-name,Values=running" \
  --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
echo "== cluster up =="
echo "driver public IP: $DRIVER"
echo "NEXT: redeploy binary + env (see header), endpoint sc://$DRIVER:50051"

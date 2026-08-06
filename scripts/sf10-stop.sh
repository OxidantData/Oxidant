#!/usr/bin/env bash
# Stop the oxidant-sf10 cluster WITHOUT tearing down the CloudFormation stack:
# scales both ASGs to 0. EC2 compute cost drops to $0; remaining cost is the
# Route53 private hosted zone (~$0.50/mo) and S3 data (unchanged).
# Instances are TERMINATED (ASG semantics), so /usr/local/bin/oxidant and
# /etc/oxidant/oxidant.env customizations are lost — sf10-start.sh restores them.
set -euo pipefail
REGION="${AWS_REGION:-us-west-2}"

echo "== scaling oxidant-sf10-driver and oxidant-sf10-workers to 0 =="
aws autoscaling update-auto-scaling-group --auto-scaling-group-name oxidant-sf10-driver  --min-size 0 --desired-capacity 0 --region "$REGION"
aws autoscaling update-auto-scaling-group --auto-scaling-group-name oxidant-sf10-workers --min-size 0 --desired-capacity 0 --region "$REGION"

echo "== waiting for all instances to terminate =="
for asg in oxidant-sf10-driver oxidant-sf10-workers; do
  while :; do
    n=$(aws autoscaling describe-auto-scaling-groups --auto-scaling-group-names "$asg" --region "$REGION" \
        --query 'length(AutoScalingGroups[0].Instances)' --output text)
    [ "$n" = "0" ] && break
    echo "  $asg: $n instance(s) still draining..."; sleep 15
  done
  echo "  $asg: empty"
done
echo "== oxidant-sf10 stopped (stack intact; S3/Glue data untouched) =="

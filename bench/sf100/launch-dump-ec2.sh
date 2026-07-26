#!/usr/bin/env bash
# Launch an AMD (c6a) EC2 box that dumps TPC-H + TPC-DS SF100 Parquet into
# s3://weft-artifacts-*/{tpch,tpcds}-sf100/ then self-terminates.
#
# Prereq: scripts already uploaded to s3://$BUCKET/bench/sf100/dump-to-s3.sh
#   aws s3 cp bench/sf100/dump-to-s3.sh s3://weft-artifacts-$ACCOUNT/bench/sf100/
#
# Usage:
#   ./bench/sf100/launch-dump-ec2.sh
set -euo pipefail

REGION="${AWS_REGION:-us-west-2}"
ACCOUNT="$(aws sts get-caller-identity --query Account --output text)"
BUCKET="${BUCKET:-weft-artifacts-${ACCOUNT}}"
INSTANCE_TYPE="${INSTANCE_TYPE:-c6a.4xlarge}"
VOLUME_GB="${VOLUME_GB:-400}"
KEY_NAME="${KEY_NAME:-weft-sf100-bench}"
IAM_PROFILE="${IAM_PROFILE:-weft-glue-profile}"

AMI_ID="$(aws ssm get-parameter --region "$REGION" \
  --name /aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp3/ami-id \
  --query Parameter.Value --output text)"

# Upload latest dump script so the box doesn't need git clone of a private branch.
ROOT="$(cd "$(dirname "$0")" && pwd)"
aws s3 cp "$ROOT/dump-to-s3.sh" "s3://${BUCKET}/bench/sf100/dump-to-s3.sh" --region "$REGION"
aws s3 cp "$ROOT/register-glue.sh" "s3://${BUCKET}/bench/sf100/register-glue.sh" --region "$REGION"

USER_DATA="$(mktemp)"
trap 'rm -f "$USER_DATA"' EXIT
cat > "$USER_DATA" <<CLOUDINIT
#!/usr/bin/env bash
set -euxo pipefail
export DEBIAN_FRONTEND=noninteractive
apt-get update -y
apt-get install -y curl unzip ca-certificates

# AWS CLI v2
curl -fsSL -o /tmp/awscliv2.zip https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip
unzip -q /tmp/awscliv2.zip -d /tmp
/tmp/aws/install

# Large scratch volume is the root disk; work under /data.
mkdir -p /data
aws s3 cp "s3://${BUCKET}/bench/sf100/dump-to-s3.sh" /data/dump-to-s3.sh --region ${REGION}
chmod +x /data/dump-to-s3.sh

export AWS_REGION=${REGION}
export AWS_DEFAULT_REGION=${REGION}
export BUCKET=${BUCKET}
export SF=100
export SUITES="tpch tpcds"
export WORK=/data/weft-sf100
export SKIP_GLUE=1

# Log everything; keep the box up on failure so we can inspect.
set +e
bash /data/dump-to-s3.sh 2>&1 | tee /data/dump.log
RC=\${PIPESTATUS[0]}
aws s3 cp /data/dump.log "s3://${BUCKET}/bench/sf100/dump.log" --region ${REGION} || true
if [[ \$RC -eq 0 ]]; then
  date -u +%Y-%m-%dT%H:%M:%SZ | aws s3 cp - "s3://${BUCKET}/bench/sf100/DUMP_COMPLETE" --region ${REGION}
  shutdown -h now
else
  echo DUMP_FAILED > /data/FAILED
  aws s3 cp /data/FAILED "s3://${BUCKET}/bench/sf100/DUMP_FAILED" --region ${REGION} || true
  # Leave running 2h for debug, then terminate anyway.
  sleep 7200
  shutdown -h now
fi
CLOUDINIT

IID="$(aws ec2 run-instances --region "$REGION" \
  --image-id "$AMI_ID" \
  --instance-type "$INSTANCE_TYPE" \
  --key-name "$KEY_NAME" \
  --iam-instance-profile "Name=${IAM_PROFILE}" \
  --instance-initiated-shutdown-behavior terminate \
  --block-device-mappings "DeviceName=/dev/sda1,Ebs={VolumeSize=${VOLUME_GB},VolumeType=gp3,Iops=6000,Throughput=500,DeleteOnTermination=true}" \
  --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=weft-sf100-dump},{Key=Purpose,Value=tpch-tpcds-sf100-s3-dump}]" \
  --user-data "file://${USER_DATA}" \
  --query 'Instances[0].InstanceId' --output text)"

echo "[launch] instance=$IID type=$INSTANCE_TYPE volume=${VOLUME_GB}G"
echo "[launch] watch log:  aws s3 cp s3://${BUCKET}/bench/sf100/dump.log - --region ${REGION}"
echo "[launch] complete:   aws s3 ls s3://${BUCKET}/bench/sf100/DUMP_COMPLETE --region ${REGION}"
echo "[launch] terminate:  aws ec2 terminate-instances --region ${REGION} --instance-ids ${IID}"
echo "$IID"

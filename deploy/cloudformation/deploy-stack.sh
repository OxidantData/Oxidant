#!/usr/bin/env bash
# Deploy / update the Weft EC2 ASG CloudFormation stack.
#
# Required:
#   --ami ami-...
#   --vpc vpc-...
#   --subnets subnet-abc,subnet-def
#
# Optional (see template Parameters):
#   --stack NAME                 (default weft-cluster)
#   --region REGION              (default $AWS_REGION or us-west-2)
#   --driver-type TYPE
#   --worker-type TYPE
#   --workers N
#   --expose-connect true|false
#   --client-cidr CIDR
#   --data-buckets arn1,arn2
#   --glue true|false
#   --memory-limit-bytes N
#   --shuffle-spill-bytes N
#   --driver-root-size GiB --worker-root-size GiB
#   --driver-spill-size GiB --worker-spill-size GiB
#   --key-name NAME
#   --hosted-zone-name weft.internal
#   --catalog-conf 'spark.sql.catalog.glue.type=glue;...'
#   --distributed-strict true|false
#   --prefer-hash-join true|false
#   --extra KEY=VALUE            (repeatable raw template params)
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TEMPLATE="${ROOT}/deploy/cloudformation/weft-cluster.yaml"
STACK="weft-cluster"
REGION="${AWS_REGION:-${REGION:-us-west-2}}"
AMI=""
VPC=""
SUBNETS=""
DRIVER_TYPE="m6i.xlarge"
WORKER_TYPE="m6i.2xlarge"
WORKERS="2"
EXPOSE="false"
CLIENT_CIDR="10.0.0.0/8"
DATA_BUCKETS=""
GLUE="false"
MEMORY_LIMIT=""
SHUFFLE_SPILL=""
SHUFFLE_PARTITIONS=""
CATALOG_CONF=""
DISTRIBUTED_STRICT="false"
PREFER_HASH_JOIN="true"
DRIVER_ROOT=40
WORKER_ROOT=40
DRIVER_SPILL=100
WORKER_SPILL=200
KEY_NAME=""
HOSTED_ZONE="weft.internal"
EXTRA_PARAMS=()

usage() {
  sed -n '2,29p' "$0" | sed 's/^# \?//'
  exit 1
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --ami) AMI="${2:?}"; shift 2 ;;
    --vpc) VPC="${2:?}"; shift 2 ;;
    --subnets) SUBNETS="${2:?}"; shift 2 ;;
    --stack) STACK="${2:?}"; shift 2 ;;
    --region) REGION="${2:?}"; shift 2 ;;
    --driver-type) DRIVER_TYPE="${2:?}"; shift 2 ;;
    --worker-type) WORKER_TYPE="${2:?}"; shift 2 ;;
    --workers) WORKERS="${2:?}"; shift 2 ;;
    --expose-connect) EXPOSE="${2:?}"; shift 2 ;;
    --client-cidr) CLIENT_CIDR="${2:?}"; shift 2 ;;
    --data-buckets) DATA_BUCKETS="${2:?}"; shift 2 ;;
    --glue) GLUE="${2:?}"; shift 2 ;;
    --memory-limit-bytes) MEMORY_LIMIT="${2:?}"; shift 2 ;;
    --shuffle-spill-bytes) SHUFFLE_SPILL="${2:?}"; shift 2 ;;
    --shuffle-partitions) SHUFFLE_PARTITIONS="${2:?}"; shift 2 ;;
    --catalog-conf) CATALOG_CONF="${2:?}"; shift 2 ;;
    --distributed-strict) DISTRIBUTED_STRICT="${2:?}"; shift 2 ;;
    --prefer-hash-join) PREFER_HASH_JOIN="${2:?}"; shift 2 ;;
    --driver-root-size) DRIVER_ROOT="${2:?}"; shift 2 ;;
    --worker-root-size) WORKER_ROOT="${2:?}"; shift 2 ;;
    --driver-spill-size) DRIVER_SPILL="${2:?}"; shift 2 ;;
    --worker-spill-size) WORKER_SPILL="${2:?}"; shift 2 ;;
    --key-name) KEY_NAME="${2:?}"; shift 2 ;;
    --hosted-zone-name) HOSTED_ZONE="${2:?}"; shift 2 ;;
    --extra) EXTRA_PARAMS+=("${2:?}"); shift 2 ;;
    -h|--help) usage ;;
    *) echo "unknown arg: $1" >&2; usage ;;
  esac
done

if [[ -z "${AMI}" || -z "${VPC}" || -z "${SUBNETS}" ]]; then
  echo "error: --ami, --vpc, and --subnets are required" >&2
  exit 1
fi

if [[ ${#CATALOG_CONF} -gt 256 ]]; then
  echo "error: --catalog-conf exceeds EC2 tag limit (256 chars); shorten warehouse URI or name" >&2
  exit 1
fi

if ! command -v aws >/dev/null 2>&1; then
  echo "error: aws CLI not found" >&2
  exit 1
fi

if [[ "${EXPOSE}" == "true" ]]; then
  echo "error: --expose-connect true is deprecated and blocked." >&2
  echo "  Do not put Spark Connect behind an NLB for this data plane / SF100." >&2
  echo "  Use --expose-connect false (default) and connect to the driver instance IP" >&2
  echo "  with --client-cidr <your-/32-or-VPC>. See docs/distributed-ec2.md." >&2
  exit 2
fi

echo "[deploy] validating template…"
aws cloudformation validate-template \
  --region "${REGION}" \
  --template-body "file://${TEMPLATE}" >/dev/null

echo "[deploy] deploying stack ${STACK} in ${REGION}…"
# shellcheck disable=SC2086
aws cloudformation deploy \
  --region "${REGION}" \
  --stack-name "${STACK}" \
  --template-file "${TEMPLATE}" \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides \
    AmiId="${AMI}" \
    VpcId="${VPC}" \
    SubnetIds="${SUBNETS}" \
    DriverInstanceType="${DRIVER_TYPE}" \
    WorkerInstanceType="${WORKER_TYPE}" \
    WorkerCount="${WORKERS}" \
    ExposeConnect="${EXPOSE}" \
    ClientCidr="${CLIENT_CIDR}" \
    EnableGlueAccess="${GLUE}" \
    DriverRootVolumeSize="${DRIVER_ROOT}" \
    WorkerRootVolumeSize="${WORKER_ROOT}" \
    DriverSpillVolumeSize="${DRIVER_SPILL}" \
    WorkerSpillVolumeSize="${WORKER_SPILL}" \
    HostedZoneName="${HOSTED_ZONE}" \
    MemoryLimitBytes="${MEMORY_LIMIT}" \
    ShuffleSpillBytes="${SHUFFLE_SPILL}" \
    ShufflePartitions="${SHUFFLE_PARTITIONS}" \
    CatalogConf="${CATALOG_CONF}" \
    DistributedStrict="${DISTRIBUTED_STRICT}" \
    PreferHashJoin="${PREFER_HASH_JOIN}" \
    DataBucketArns="${DATA_BUCKETS}" \
    KeyName="${KEY_NAME}" \
    ${EXTRA_PARAMS[@]+"${EXTRA_PARAMS[@]}"}

echo "[deploy] outputs:"
aws cloudformation describe-stacks \
  --region "${REGION}" \
  --stack-name "${STACK}" \
  --query 'Stacks[0].Outputs' \
  --output table

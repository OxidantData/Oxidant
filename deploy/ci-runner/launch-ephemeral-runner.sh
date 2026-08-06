#!/usr/bin/env bash
# Launch ONE ephemeral, self-terminating GitHub Actions runner for the heavy ClickBench
# (.github/workflows/bench.yml). The box registers itself, runs exactly one queued job, then
# powers off — and because it is launched with shutdown-behavior=terminate, powering off
# TERMINATES it. No idle cost, no stale runner left behind.
#
# This script is NEVER called by CI. You run it by hand when you want a benchmark, AFTER
# triggering bench.yml (Actions tab -> Run workflow) so a job is queued for it to pick up.
#
# Prereq: a runner REGISTRATION TOKEN (needs repo-admin, which the kaicoder03 gh login lacks):
#   gh api -X POST repos/OxidantData/Oxidant/actions/runners/registration-token --jq .token
#   ...or repo Settings -> Actions -> Runners -> New self-hosted runner.
#
# Usage:
#   export OXIDANT_RUNNER_TOKEN="<registration-token>"
#   ./deploy/ci-runner/launch-ephemeral-runner.sh
#
# Tunables (env, with defaults):
#   REGION=us-west-2  INSTANCE_TYPE=c6a.4xlarge  VOLUME_GB=120  KEY_NAME=oxidant-platform
#   REPO_URL=https://github.com/OxidantData/Oxidant  LABELS=self-hosted,linux,x64,clickbench
#   SUBNET_ID=...  SECURITY_GROUP_IDS=...   (optional; default VPC/SG if unset)
set -euo pipefail

: "${OXIDANT_RUNNER_TOKEN:?Set OXIDANT_RUNNER_TOKEN to a runner registration token (see header)}"
REGION="${REGION:-us-west-2}"
INSTANCE_TYPE="${INSTANCE_TYPE:-c6a.4xlarge}"
VOLUME_GB="${VOLUME_GB:-120}"
KEY_NAME="${KEY_NAME:-oxidant-platform}"
REPO_URL="${REPO_URL:-https://github.com/OxidantData/Oxidant}"
LABELS="${LABELS:-self-hosted,linux,x64,clickbench}"

echo "[runner] resolving latest Ubuntu 24.04 (amd64) AMI in ${REGION} …"
AMI_ID="$(aws ssm get-parameter --region "$REGION" \
  --name /aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp3/ami-id \
  --query Parameter.Value --output text)"
echo "[runner] AMI = ${AMI_ID}"

# cloud-init: install toolchain, register an EPHEMERAL runner, run one job, then terminate.
USER_DATA="$(mktemp)"
trap 'rm -f "$USER_DATA"' EXIT
cat > "$USER_DATA" <<CLOUDINIT
#!/usr/bin/env bash
set -euxo pipefail
export DEBIAN_FRONTEND=noninteractive
apt-get update -y
apt-get install -y curl tar jq git python3 build-essential pkg-config libssl-dev

# Rust for the 'ubuntu' user; symlink proxies onto the default PATH so CI 'run:' steps see cargo.
sudo -u ubuntu bash -lc 'curl --proto "=https" --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y'
ln -sf /home/ubuntu/.cargo/bin/cargo  /usr/local/bin/cargo
ln -sf /home/ubuntu/.cargo/bin/rustc  /usr/local/bin/rustc
ln -sf /home/ubuntu/.cargo/bin/rustup /usr/local/bin/rustup

# GitHub Actions runner (latest), configured ephemeral so it deregisters after one job.
RUNNER_VERSION=\$(curl -fsSL https://api.github.com/repos/actions/runner/releases/latest \
  | python3 -c 'import sys,json;print(json.load(sys.stdin)["tag_name"].lstrip("v"))')
sudo -u ubuntu bash -lc "
  set -euxo pipefail
  cd /home/ubuntu
  mkdir -p actions-runner && cd actions-runner
  curl -fsSL -o runner.tar.gz https://github.com/actions/runner/releases/download/v\${RUNNER_VERSION}/actions-runner-linux-x64-\${RUNNER_VERSION}.tar.gz
  tar xzf runner.tar.gz
  sudo ./bin/installdependencies.sh
  ./config.sh --unattended --ephemeral --replace \
    --url ${REPO_URL} --token ${OXIDANT_RUNNER_TOKEN} \
    --name oxidant-ci-\$(hostname) --labels ${LABELS}
  ./run.sh || true
"

# One job done (or registration failed) -> terminate via shutdown-behavior=terminate.
shutdown -h now
CLOUDINIT

EXTRA=()
[ -n "${SUBNET_ID:-}" ] && EXTRA+=(--subnet-id "$SUBNET_ID")
[ -n "${SECURITY_GROUP_IDS:-}" ] && EXTRA+=(--security-group-ids "$SECURITY_GROUP_IDS")

echo "[runner] launching ${INSTANCE_TYPE} (terminate-on-shutdown, ${VOLUME_GB} GB gp3) …"
INSTANCE_ID="$(aws ec2 run-instances --region "$REGION" \
  --image-id "$AMI_ID" --instance-type "$INSTANCE_TYPE" --key-name "$KEY_NAME" \
  --instance-initiated-shutdown-behavior terminate \
  --block-device-mappings "DeviceName=/dev/sda1,Ebs={VolumeSize=${VOLUME_GB},VolumeType=gp3,DeleteOnTermination=true}" \
  --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=oxidant-ci-runner},{Key=Purpose,Value=clickbench-ephemeral}]' \
  --user-data "file://${USER_DATA}" \
  "${EXTRA[@]}" \
  --query 'Instances[0].InstanceId' --output text)"

echo "[runner] launched ${INSTANCE_ID}"
echo "[runner] it will register with labels '${LABELS}', run one bench.yml job, then self-terminate."
echo "[runner] watch:     aws ec2 describe-instances --region ${REGION} --instance-ids ${INSTANCE_ID} --query 'Reservations[].Instances[].State.Name' --output text"
echo "[runner] kill early: aws ec2 terminate-instances --region ${REGION} --instance-ids ${INSTANCE_ID}"

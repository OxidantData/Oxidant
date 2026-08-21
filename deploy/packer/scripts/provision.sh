#!/usr/bin/env bash
# Packer provisioner: harden AL2023 and install Oxidant runtime bits.
set -euo pipefail

OXIDANT_UID=65532
OXIDANT_GID=65532
OXIDANT_BINARY_URL="${OXIDANT_BINARY_URL:-}"
# Staged by build-ami.sh into files/oxidant → uploaded under /tmp/oxidant-files/oxidant.
STAGED_BINARY="/tmp/oxidant-files/oxidant"

echo "[provision] updating packages"
dnf -y update
dnf -y install \
  ca-certificates \
  curl \
  unzip \
  xfsprogs \
  chrony \
  --allowerasing

# AWS CLI v2 (official installer). AL2023 may ship awscli v1; replace with v2.
if ! /usr/local/bin/aws --version 2>/dev/null | grep -q 'aws-cli/2'; then
  echo "[provision] installing AWS CLI v2"
  ARCH="$(uname -m)"
  case "${ARCH}" in
    x86_64) A=x86_64 ;;
    aarch64) A=aarch64 ;;
    *) echo "unsupported arch ${ARCH}"; exit 1 ;;
  esac
  curl -fsSLo /tmp/awscli.zip "https://awscli.amazonaws.com/awscli-exe-linux-${A}.zip"
  unzip -q /tmp/awscli.zip -d /tmp
  /tmp/aws/install -i /usr/local/aws-cli -b /usr/local/bin
  rm -rf /tmp/aws /tmp/awscli.zip
fi
/usr/local/bin/aws --version

echo "[provision] creating oxidant user ${OXIDANT_UID}:${OXIDANT_GID}"
groupadd -g "${OXIDANT_GID}" oxidant 2>/dev/null || true
useradd -u "${OXIDANT_UID}" -g "${OXIDANT_GID}" -m -d /var/lib/oxidant -s /sbin/nologin oxidant 2>/dev/null || true
mkdir -p /var/lib/oxidant/spill /etc/oxidant /usr/local/lib/oxidant
chown -R oxidant:oxidant /var/lib/oxidant
chmod 755 /var/lib/oxidant /var/lib/oxidant/spill

echo "[provision] installing oxidant binary"
if [[ -f "${STAGED_BINARY}" ]]; then
  install -m 0755 "${STAGED_BINARY}" /usr/local/bin/oxidant
elif [[ -n "${OXIDANT_BINARY_URL}" ]]; then
  curl -fsSLo /usr/local/bin/oxidant "${OXIDANT_BINARY_URL}"
  chmod 0755 /usr/local/bin/oxidant
else
  echo "[provision] ERROR: stage files/oxidant via build-ami.sh --binary, or set -var oxidant_binary_url=..." >&2
  exit 1
fi
# Binary is linux/amd64; a quick existence check is enough in the bake host.
test -x /usr/local/bin/oxidant
# Do not leave the staged upload in /tmp for the AMI.
rm -f "${STAGED_BINARY}"

echo "[provision] installing bootstrap + systemd units"
install -m 0755 /tmp/oxidant-files/bootstrap.sh /usr/local/lib/oxidant/bootstrap.sh
install -m 0755 /tmp/oxidant-files/shard-resolve.sh /usr/local/lib/oxidant/shard-resolve.sh
install -m 0644 /tmp/oxidant-files/systemd/oxidant-bootstrap.service /etc/systemd/system/oxidant-bootstrap.service
install -m 0644 /tmp/oxidant-files/systemd/oxidant-driver.service /etc/systemd/system/oxidant-driver.service
install -m 0644 /tmp/oxidant-files/systemd/oxidant-worker.service /etc/systemd/system/oxidant-worker.service
install -m 0644 /tmp/oxidant-files/systemd/oxidant-standalone.service /etc/systemd/system/oxidant-standalone.service
install -m 0644 /tmp/oxidant-files/systemd/oxidant-shard-resolve.service /etc/systemd/system/oxidant-shard-resolve.service
install -m 0644 /tmp/oxidant-files/systemd/oxidant-shard-resolve.timer /etc/systemd/system/oxidant-shard-resolve.timer
systemctl daemon-reload
systemctl enable oxidant-bootstrap.service
systemctl enable oxidant-shard-resolve.timer
# Role units are enabled at boot by bootstrap based on the oxidant:role tag.
# oxidant-standalone is ENABLED here (Marketplace single-node: no UserData
# exists to start it, and Requires= would deadlock — it Wants bootstrap only).
# Cluster images: bootstrap disables + cancels it before it starts.
systemctl disable oxidant-driver.service oxidant-worker.service 2>/dev/null || true
systemctl enable oxidant-standalone.service

echo "[provision] writing version stamp, MOTD, and quickstart"
mkdir -p /usr/local/share/oxidant
cat > /etc/oxidant/VERSION <<EOF
engine=${OXIDANT_ENGINE_VERSION:-unknown}
built=$(date -u +%Y-%m-%dT%H:%M:%SZ)
EOF
cat > /etc/motd <<'EOF'
===============================================================================
 Oxidant — the fast Apache Spark-compatible analytics runtime
 AGPLv3 open source | commercial licensing: hello@oxidantdata.com
 Docs: https://oxidantdata.com  |  Repo: https://github.com/OxidantData/Oxidant

 This instance is running the Oxidant Spark Connect server on port 50051.
 Point any PySpark client at it:
     SparkSession.builder.remote("sc://<this-host>:50051").getOrCreate()

 Monitoring UI (loopback only): ssh -L 4040:localhost:4040 ec2-user@<this-host>
 Quickstart: /usr/local/share/oxidant/QUICKSTART.md
===============================================================================
EOF
install -m 0644 /tmp/oxidant-files/QUICKSTART.md /usr/local/share/oxidant/QUICKSTART.md

echo "[provision] hardening ssh / imds posture helpers"
# Password auth off; AMI is intended for SSM Session Manager (no required KeyName).
if [[ -f /etc/ssh/sshd_config ]]; then
  sed -i 's/^#\?PasswordAuthentication.*/PasswordAuthentication no/' /etc/ssh/sshd_config
  sed -i 's/^#\?PermitRootLogin.*/PermitRootLogin no/' /etc/ssh/sshd_config
fi

# Automatic security updates.
dnf -y install dnf-automatic
systemctl enable --now dnf-automatic-install.timer 2>/dev/null \
  || systemctl enable dnf-automatic.timer 2>/dev/null \
  || true

# Host firewall: leave SSH available for break-glass; production ingress is
# owned by the CloudFormation security groups (Connect 50051 / Flight 50561).
# Do not reload a deny-all policy here — that would kill the Packer SSH session.
systemctl disable firewalld 2>/dev/null || true
systemctl stop firewalld 2>/dev/null || true


# Clean packer leftovers.
dnf -y clean all
rm -rf /var/cache/dnf/* /tmp/* /var/tmp/*

echo "[provision] done"

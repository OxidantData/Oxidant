#!/usr/bin/env bash
# Packer provisioner: harden AL2023 and install Weft runtime bits.
set -euo pipefail

WEFT_UID=65532
WEFT_GID=65532
WEFT_BINARY_URL="${WEFT_BINARY_URL:-}"
# Staged by build-ami.sh into files/weft → uploaded under /tmp/weft-files/weft.
STAGED_BINARY="/tmp/weft-files/weft"

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

echo "[provision] creating weft user ${WEFT_UID}:${WEFT_GID}"
groupadd -g "${WEFT_GID}" weft 2>/dev/null || true
useradd -u "${WEFT_UID}" -g "${WEFT_GID}" -m -d /var/lib/weft -s /sbin/nologin weft 2>/dev/null || true
mkdir -p /var/lib/weft/spill /etc/weft /usr/local/lib/weft
chown -R weft:weft /var/lib/weft
chmod 755 /var/lib/weft /var/lib/weft/spill

echo "[provision] installing weft binary"
if [[ -f "${STAGED_BINARY}" ]]; then
  install -m 0755 "${STAGED_BINARY}" /usr/local/bin/weft
elif [[ -n "${WEFT_BINARY_URL}" ]]; then
  curl -fsSLo /usr/local/bin/weft "${WEFT_BINARY_URL}"
  chmod 0755 /usr/local/bin/weft
else
  echo "[provision] ERROR: stage files/weft via build-ami.sh --binary, or set -var weft_binary_url=..." >&2
  exit 1
fi
# Binary is linux/amd64; a quick existence check is enough in the bake host.
test -x /usr/local/bin/weft
# Do not leave the staged upload in /tmp for the AMI.
rm -f "${STAGED_BINARY}"

echo "[provision] installing bootstrap + systemd units"
install -m 0755 /tmp/weft-files/bootstrap.sh /usr/local/lib/weft/bootstrap.sh
install -m 0644 /tmp/weft-files/systemd/weft-bootstrap.service /etc/systemd/system/weft-bootstrap.service
install -m 0644 /tmp/weft-files/systemd/weft-driver.service /etc/systemd/system/weft-driver.service
install -m 0644 /tmp/weft-files/systemd/weft-worker.service /etc/systemd/system/weft-worker.service
systemctl daemon-reload
systemctl enable weft-bootstrap.service
# Role units are enabled at boot by bootstrap based on the weft:role tag.
systemctl disable weft-driver.service weft-worker.service 2>/dev/null || true

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

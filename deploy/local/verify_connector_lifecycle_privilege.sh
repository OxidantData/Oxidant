#!/usr/bin/env bash
# Runtime proof for docs/api.md ("Pipeline lifecycle" -> "Privilege") and
# deploy/packer/files/polkit/49-oxidant-connector-lifecycle.rules: that the unprivileged
# `oxidant` user can actually `systemctl start|stop|restart` a `oxidant-connector-*.service`
# unit, end to end through real systemd + real polkit, from a caller running under the same
# sandbox settings as the driver unit
# (deploy/packer/files/systemd/oxidant-driver.service: NoNewPrivileges=true,
# ProtectSystem=strict).
#
# deploy/packer/tests/test_connector_lifecycle_privilege.sh proves the *shape* of the
# mechanism — the rule file's guards, the driver unit's hardening flags, provision.sh wiring —
# by static inspection, with no polkit installed and no root required. This script proves the
# *mechanism itself* works, which nothing else in this repository does (PR #154 review, finding
# 6): a plausible-looking polkit rule and a plausible-looking systemd sandbox are exactly the
# kind of thing that reads correctly and fails at 3am.
#
# Requires: Linux, systemd (systemctl + systemd-run), polkit (polkitd + pkaction), runuser, and
# root — to create a throwaway `oxidant` user and unit, install the rule under
# /etc/polkit-1/rules.d, and clean up after. None of that is available in the sandboxes this
# repo's other tests run in (macOS dev machines, containerized CI with no systemd/polkit) — see
# the RESIDUAL RISK note at the bottom for what that means in practice.
#
# Run: sudo bash deploy/local/verify_connector_lifecycle_privilege.sh
# Best run on: the AMI itself, post-provision and pre-bake (deploy/packer/scripts/provision.sh
# already installs polkit and the rule by that point), or any disposable Linux box with
# systemd + polkit installed.
set -uo pipefail

PASS=0
FAIL=0
SKIPPED=0

ok() { echo "ok - $1"; PASS=$((PASS + 1)); }
not_ok() { echo "not ok - $1"; shift; [[ $# -gt 0 ]] && printf '  %s\n' "$@"; FAIL=$((FAIL + 1)); }
skip() { echo "skip - $1"; shift; [[ $# -gt 0 ]] && printf '  %s\n' "$@"; SKIPPED=$((SKIPPED + 1)); }

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
RULES_FILE="${ROOT}/deploy/packer/files/polkit/49-oxidant-connector-lifecycle.rules"
RULES_DEST="/etc/polkit-1/rules.d/49-oxidant-connector-lifecycle.rules"
TEST_UNIT="oxidant-connector-selftest.service"
TEST_UNIT_PATH="/etc/systemd/system/${TEST_UNIT}"
TEST_USER="oxidant"
CREATED_USER=0
INSTALLED_RULE=0

cleanup() {
  systemctl stop "${TEST_UNIT}" >/dev/null 2>&1 || true
  rm -f "${TEST_UNIT_PATH}"
  if [[ "${INSTALLED_RULE}" -eq 1 ]]; then
    rm -f "${RULES_DEST}"
  fi
  systemctl daemon-reload >/dev/null 2>&1 || true
  if [[ "${CREATED_USER}" -eq 1 ]]; then
    userdel "${TEST_USER}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

# --- prerequisites: skip, don't fail, when this environment cannot run the real thing --------
if [[ "$(uname -s)" != "Linux" ]]; then
  skip "runs only on Linux (systemd + polkit are Linux-only)" "uname -s = $(uname -s)"
  echo; echo "${PASS} passed, ${FAIL} failed, ${SKIPPED} skipped (prerequisites not met)"
  exit 0
fi
if [[ "$(id -u)" -ne 0 ]]; then
  skip "needs root to install the polkit rule, a throwaway unit, and (if absent) the oxidant user" \
    "re-run as: sudo bash $0"
  echo; echo "${PASS} passed, ${FAIL} failed, ${SKIPPED} skipped (prerequisites not met)"
  exit 0
fi
for bin in systemctl systemd-run runuser pkaction polkitd; do
  if ! command -v "${bin}" >/dev/null 2>&1; then
    skip "needs ${bin} on PATH" "not found — this host has no systemd/polkit installed"
    echo; echo "${PASS} passed, ${FAIL} failed, ${SKIPPED} skipped (prerequisites not met)"
    exit 0
  fi
done
if ! systemctl is-active polkit >/dev/null 2>&1; then
  skip "needs the polkit service active" "systemctl is-active polkit failed"
  echo; echo "${PASS} passed, ${FAIL} failed, ${SKIPPED} skipped (prerequisites not met)"
  exit 0
fi

# --- fixture: a real (harmless) unit named like a connector, a real oxidant user, the real rule
if ! id "${TEST_USER}" >/dev/null 2>&1; then
  useradd --system --no-create-home --shell /usr/sbin/nologin "${TEST_USER}"
  CREATED_USER=1
fi

cat >"${TEST_UNIT_PATH}" <<'UNIT'
[Unit]
Description=oxidant lifecycle privilege self-test (harmless, deleted on exit)

[Service]
Type=oneshot
RemainAfterExit=yes
ExecStart=/bin/true
ExecStop=/bin/true
UNIT

install -m 0644 "${RULES_FILE}" "${RULES_DEST}"
INSTALLED_RULE=1
systemctl daemon-reload

# --- the proof: the same call this route makes, wrapped in the driver's own sandbox properties
# `systemd-run --uid=oxidant ... -- systemctl <verb> <unit>` is the accurate reproduction of
# "an unprivileged process with the driver's sandbox settings calls systemctl" — a bare
# `runuser -u oxidant -- systemctl ...` proves the polkit rule but not the sandbox interaction,
# so this wraps it in the same NoNewPrivileges/ProtectSystem the driver unit sets.
run_sandboxed() {
  local verb="$1"
  systemd-run --quiet --pipe --wait \
    --uid="${TEST_USER}" \
    --property=NoNewPrivileges=yes \
    --property=ProtectSystem=strict \
    --property=CapabilityBoundingSet= \
    -- systemctl "${verb}" "${TEST_UNIT}"
}

for verb in start stop restart; do
  if run_sandboxed "${verb}"; then
    ok "runuser-equivalent (oxidant, NoNewPrivileges=true, ProtectSystem=strict) systemctl ${verb} ${TEST_UNIT} exits 0"
  else
    not_ok "runuser-equivalent (oxidant, NoNewPrivileges=true, ProtectSystem=strict) systemctl ${verb} ${TEST_UNIT} exits 0" \
      "exit $? — the polkit rule, the sandbox properties, or this systemd version disagree with docs/api.md"
  fi
done

# --- negative control: a unit name the rule does NOT authorize must be refused ----------------
OTHER_UNIT="oxidant-driver.service"
if systemd-run --quiet --pipe --wait --uid="${TEST_USER}" \
  --property=NoNewPrivileges=yes --property=ProtectSystem=strict --property=CapabilityBoundingSet= \
  -- systemctl stop "${OTHER_UNIT}" >/dev/null 2>&1; then
  not_ok "the rule does NOT authorize a non-connector unit (${OTHER_UNIT})" \
    "systemctl stop succeeded — the polkit rule is broader than docs/api.md claims"
else
  ok "the rule does NOT authorize a non-connector unit (${OTHER_UNIT}) — refused as expected"
fi

echo
echo "${PASS} passed, ${FAIL} failed, ${SKIPPED} skipped"
[[ ${FAIL} -eq 0 ]]

#!/usr/bin/env bash
# Guarding tests for the privilege mechanism behind `POST /api/v1/pipelines/lifecycle`
# (crates/oxidant-ui-server/src/lifecycle.rs).
#
# The route needs to `systemctl start/stop/restart` a connector's unit as the unprivileged
# `oxidant` user. The driver unit sets `NoNewPrivileges=true`
# (deploy/packer/files/systemd/oxidant-driver.service), which rules out a `sudo`-based
# mechanism outright — `sudo` elevates by executing a setuid binary, exactly what
# `NoNewPrivileges` blocks, for any binary, unconditionally. The chosen mechanism is a polkit
# rule authorizing `oxidant` to manage only `oxidant-connector-*.service` units over D-Bus,
# which does not need new privileges from the calling process at all. These checks exist so a
# future edit cannot quietly reintroduce the sudoers shape (which would silently fail at
# runtime — `sudo` inside a `NoNewPrivileges=true` unit exits `1`, "Permission denied", every
# time) or loosen the driver's own hardening to make one work.
#
# Static checks only: no AWS, no packer binary, no network, no polkit installed on this
# machine.
#
# Run: bash deploy/packer/tests/test_connector_lifecycle_privilege.sh
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
PROVISION="${ROOT}/deploy/packer/scripts/provision.sh"
DRIVER_UNIT="${ROOT}/deploy/packer/files/systemd/oxidant-driver.service"
RULES_FILE="${ROOT}/deploy/packer/files/polkit/49-oxidant-connector-lifecycle.rules"
PASS=0
FAIL=0

ok() {
  echo "ok - $1"
  PASS=$((PASS + 1))
}

not_ok() {
  echo "not ok - $1"
  shift
  [[ $# -gt 0 ]] && printf '  %s\n' "$@"
  FAIL=$((FAIL + 1))
}

# --- the driver keeps its hardening -----------------------------------------
# The whole reason this route does not use sudo. If this regresses to `false` (or is removed),
# the polkit mechanism below is no longer the only option and this file's premise is stale.
if [[ -f "${DRIVER_UNIT}" ]] && grep -qE '^[[:space:]]*NoNewPrivileges=true[[:space:]]*$' "${DRIVER_UNIT}"; then
  ok "oxidant-driver.service keeps NoNewPrivileges=true"
else
  not_ok "oxidant-driver.service keeps NoNewPrivileges=true" \
    "if this was intentionally loosened, this test (and the polkit-only design it guards) is stale"
fi

# --- provision.sh never grants oxidant a sudoers rule -----------------------
# A sudoers drop-in for systemctl would look correct in isolation and fail silently at
# runtime under NoNewPrivileges=true — this is the regression to catch before a bake, not
# after a demo. Only *executable* lines count; a comment explaining why sudoers was rejected
# (as this very provisioner has) must not trip the check.
if grep -vE '^[[:space:]]*#' "${PROVISION}" | grep -qiE 'sudoers\.d|visudo'; then
  not_ok "provision.sh does not install a sudoers rule for oxidant" \
    "sudo cannot elevate inside a NoNewPrivileges=true unit; see oxidant-driver.service"
else
  ok "provision.sh does not install a sudoers rule for oxidant"
fi

# --- provision.sh installs and enables polkit -------------------------------
if grep -qE '^[[:space:]]*dnf -y install polkit[[:space:]]*$' "${PROVISION}"; then
  ok "provision.sh installs polkit explicitly"
else
  not_ok "provision.sh installs polkit explicitly"
fi

if grep -qE '^[[:space:]]*rpm -q polkit[[:space:]]*$' "${PROVISION}"; then
  ok "the bake asserts polkit is installed"
else
  not_ok "the bake asserts polkit is installed"
fi

if grep -qE '^[[:space:]]*systemctl enable --now polkit[[:space:]]*$' "${PROVISION}" \
  && grep -qE '^[[:space:]]*systemctl is-active polkit[[:space:]]*$' "${PROVISION}"; then
  ok "the bake enables polkit and asserts it is active"
else
  not_ok "the bake enables polkit and asserts it is active"
fi

# --- provision.sh installs the rules file to the right place ---------------
if grep -qE '/etc/polkit-1/rules\.d/49-oxidant-connector-lifecycle\.rules[[:space:]]*$' "${PROVISION}"; then
  ok "provision.sh installs the rule under /etc/polkit-1/rules.d"
else
  not_ok "provision.sh installs the rule under /etc/polkit-1/rules.d"
fi

# --- the rule itself is scoped correctly -------------------------------------
if [[ ! -f "${RULES_FILE}" ]]; then
  not_ok "deploy/packer/files/polkit/49-oxidant-connector-lifecycle.rules exists"
else
  ok "deploy/packer/files/polkit/49-oxidant-connector-lifecycle.rules exists"

  if grep -qE 'action\.id[[:space:]]*!=[[:space:]]*"org\.freedesktop\.systemd1\.manage-units"' "${RULES_FILE}"; then
    ok "the rule scopes to org.freedesktop.systemd1.manage-units"
  else
    not_ok "the rule scopes to org.freedesktop.systemd1.manage-units"
  fi

  if grep -qE 'subject\.user[[:space:]]*!=[[:space:]]*"oxidant"' "${RULES_FILE}"; then
    ok "the rule scopes to the oxidant user"
  else
    not_ok "the rule scopes to the oxidant user"
  fi

  if grep -qE 'oxidant-connector-' "${RULES_FILE}" && grep -qE '\\\.service' "${RULES_FILE}"; then
    ok "the rule scopes to oxidant-connector-*.service unit names"
  else
    not_ok "the rule scopes to oxidant-connector-*.service unit names"
  fi

  # No wildcard `YES` for anything else — every path through the rule that is not the narrow
  # match above must fall through to NOT_HANDLED, never grant blindly.
  YES_COUNT="$(grep -cE 'polkit\.Result\.YES' "${RULES_FILE}")"
  if [[ "${YES_COUNT}" -eq 1 ]]; then
    ok "the rule grants YES from exactly one, narrowly-guarded, branch"
  else
    not_ok "the rule grants YES from exactly one, narrowly-guarded, branch" "found ${YES_COUNT}"
  fi
fi

# --- /etc/oxidant/connectors is created, so config_path can resolve --------
if grep -qE 'mkdir -p .*\/etc\/oxidant\/connectors' "${PROVISION}"; then
  ok "provision.sh creates /etc/oxidant/connectors"
else
  not_ok "provision.sh creates /etc/oxidant/connectors" \
    "crates/oxidant-ui-server/src/lifecycle.rs refuses every config_path if this does not exist"
fi

echo
echo "${PASS} passed, ${FAIL} failed"
[[ ${FAIL} -eq 0 ]]

#!/usr/bin/env bash
# Guarding tests for operator access to a cluster node.
#
# The bug these exist to prevent shipped once: ami-0f49adb73c7774fb9 carried no
# amazon-ssm-agent at all, while provision.sh's comments promised Session
# Manager and the driver instance role carried AmazonSSMManagedInstanceCore.
# The platform launches drivers and workers with no key pair and opens
# 50051/50561 alone, so when the demo cluster's driver failed to start its
# engine service there was no route to /var/log/oxidant-userdata.log or
# `journalctl -u oxidant-driver` by any documented means. Nothing in the build
# failed to say so.
#
# Three independent things have to hold, so all three are tested:
#   1. the base-AMI filter cannot select an "al2023-ami-minimal-*" image
#      (most_recent picks it whenever it is published last, and the minimal
#      variant ships no agent);
#   2. provision.sh installs and enables the agent regardless, so a base image
#      that stops carrying it is still caught at bake time;
#   3. the shell provisioner actually passes its environment_vars through, so
#      the version stamp that identifies a broken AMI is not "unknown".
#
# Static checks only: no AWS, no packer binary, no network.
#
# Run: bash deploy/packer/tests/test_ssm_agent.sh
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
TEMPLATE="${ROOT}/deploy/packer/oxidant-runtime.pkr.hcl"
PROVISION="${ROOT}/deploy/packer/scripts/provision.sh"
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

assert_eq() {
  local name="$1" got="$2" want="$3"
  if [[ "${got}" == "${want}" ]]; then
    ok "${name}"
  else
    not_ok "${name}" "got:  ${got}" "want: ${want}"
  fi
}

# --- 1. the base-AMI glob ----------------------------------------------------
# Pull the literal out of the template and resolve ${var.architecture} the way
# packer would, then match real AL2023 image names against it with bash's own
# globbing — the same semantics EC2's name filter uses for `*`.
GLOB_LINE="$(grep -E '^[[:space:]]*ami_name_glob' "${TEMPLATE}" || true)"
if [[ -z "${GLOB_LINE}" ]]; then
  not_ok "oxidant-runtime.pkr.hcl defines ami_name_glob"
else
  ok "oxidant-runtime.pkr.hcl defines ami_name_glob"
  GLOB="$(sed -E 's/.*=[[:space:]]*"([^"]*)".*/\1/' <<<"${GLOB_LINE}")"

  matches() {  # matches <glob> <name> -> yes|no
    local g="$1" n="$2"
    # shellcheck disable=SC2053
    [[ "${n}" == ${g} ]] && echo yes || echo no
  }

  for arch in arm64 x86_64; do
    G="${GLOB//\$\{var.architecture\}/${arch}}"
    # The names below are real, from us-west-2 owner 137112412989. The minimal
    # image was the most_recent of the plain "al2023-ami-*-arm64" glob on
    # 2026-08-25, which is exactly how the agent-less AMI got baked.
    assert_eq "${arch}: the full AL2023 image still matches" \
      "$(matches "${G}" "al2023-ami-2023.12.20260817.0-kernel-6.1-${arch}")" "yes"
    assert_eq "${arch}: a newer kernel of the full image still matches" \
      "$(matches "${G}" "al2023-ami-2023.12.20260817.0-kernel-6.18-${arch}")" "yes"
    assert_eq "${arch}: the MINIMAL image is never selectable" \
      "$(matches "${G}" "al2023-ami-minimal-2023.12.20260817.0-kernel-6.18-${arch}")" "no"
    assert_eq "${arch}: the minimal image on any kernel is never selectable" \
      "$(matches "${G}" "al2023-ami-minimal-2023.12.20260817.0-kernel-6.1-${arch}")" "no"
    assert_eq "${arch}: the other architecture is never selected" \
      "$(matches "${G}" "al2023-ami-2023.12.20260817.0-kernel-6.1-notthisarch")" "no"
  done
fi

# --- 2. provision.sh installs the agent anyway --------------------------------
# Belt and braces: the glob is a filter over names AWS controls, so the image
# is not allowed to be the only thing standing between an operator and a shell.
if grep -qE '^[[:space:]]*dnf -y install amazon-ssm-agent[[:space:]]*$' "${PROVISION}"; then
  ok "provision.sh installs amazon-ssm-agent explicitly"
else
  not_ok "provision.sh installs amazon-ssm-agent explicitly"
fi

if grep -qE '^[[:space:]]*systemctl enable amazon-ssm-agent[[:space:]]*$' "${PROVISION}"; then
  ok "provision.sh enables amazon-ssm-agent"
else
  not_ok "provision.sh enables amazon-ssm-agent"
fi

# The install and the enable must be able to fail the bake. provision.sh runs
# under `set -e`, so the danger is a trailing `|| true` on either line — the
# shape used elsewhere in this script for genuinely optional steps.
if grep -E 'amazon-ssm-agent' "${PROVISION}" | grep -qE '\|\|[[:space:]]*true'; then
  not_ok "the SSM steps are not silenced with '|| true'" \
    "a swallowed failure is how the agent went missing in the first place"
else
  ok "the SSM steps are not silenced with '|| true'"
fi

# And the bake must assert the end state, not just the commands.
if grep -qE '^[[:space:]]*rpm -q amazon-ssm-agent[[:space:]]*$' "${PROVISION}" \
  && grep -qE '^[[:space:]]*systemctl is-enabled amazon-ssm-agent[[:space:]]*$' "${PROVISION}"; then
  ok "the bake asserts the agent is installed and enabled"
else
  not_ok "the bake asserts the agent is installed and enabled"
fi

# --- 3. the provisioner really passes its environment_vars --------------------
# `sudo -E` preserves the environment it is given; it does not create one.
# Packer only injects environment_vars where the execute_command interpolates
# {{ .Vars }}, so omitting it leaves OXIDANT_ENGINE_VERSION unset and the AMI
# stamps "engine=unknown" — the stamp an operator reads to tell a fixed image
# from the broken one.
EXEC_LINE="$(grep -E '^[[:space:]]*execute_command' "${TEMPLATE}" || true)"
if [[ -z "${EXEC_LINE}" ]]; then
  not_ok "oxidant-runtime.pkr.hcl defines execute_command for provision.sh"
else
  ok "oxidant-runtime.pkr.hcl defines execute_command for provision.sh"
  if grep -qE '\{\{[[:space:]]*\.Vars[[:space:]]*\}\}' <<<"${EXEC_LINE}"; then
    ok "execute_command interpolates {{ .Vars }}"
  else
    not_ok "execute_command interpolates {{ .Vars }}" \
      "got: ${EXEC_LINE}" \
      "without it the declared environment_vars never reach provision.sh"
  fi
fi

# The stamp must also still fall back rather than expand to an empty value, so
# a bake driven without GIT_SHA is loud rather than silently blank.
if grep -qE 'engine=\$\{OXIDANT_ENGINE_VERSION:-unknown\}' "${PROVISION}"; then
  ok "the VERSION stamp names its fallback"
else
  not_ok "the VERSION stamp names its fallback"
fi

echo
echo "${PASS} passed, ${FAIL} failed"
[[ ${FAIL} -eq 0 ]]

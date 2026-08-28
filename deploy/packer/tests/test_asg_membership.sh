#!/usr/bin/env bash
# Regression tests for EC2 driver membership: ASG → private IPs → OXIDANT_WORKERS.
# No real AWS calls — mocks the aws CLI. Run: bash deploy/packer/tests/test_asg_membership.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
BOOTSTRAP="${ROOT}/deploy/packer/files/bootstrap.sh"
PASS=0
FAIL=0

assert_eq() {
  local name="$1" got="$2" want="$3"
  if [[ "${got}" == "${want}" ]]; then
    echo "ok - ${name}"
    PASS=$((PASS + 1))
  else
    echo "not ok - ${name}"
    echo "  got:  ${got}"
    echo "  want: ${want}"
    FAIL=$((FAIL + 1))
  fi
}

assert_ok() {
  local name="$1"
  shift
  if "$@" >/tmp/oxidant-asg-test.out 2>/tmp/oxidant-asg-test.err; then
    echo "ok - ${name}"
    PASS=$((PASS + 1))
  else
    echo "not ok - ${name} (exit $?)"
    cat /tmp/oxidant-asg-test.err >&2 || true
    FAIL=$((FAIL + 1))
  fi
}

assert_fail() {
  local name="$1"
  shift
  if "$@" >/tmp/oxidant-asg-test.out 2>/tmp/oxidant-asg-test.err; then
    echo "not ok - ${name} (expected failure)"
    FAIL=$((FAIL + 1))
  else
    echo "ok - ${name}"
    PASS=$((PASS + 1))
  fi
}

# Source helpers without executing IMDS main.
# shellcheck source=../files/bootstrap.sh
source "${BOOTSTRAP}"

# --- pure CSV helper (bare-metal / ASG share the same pin format) -------------
got="$(printf '10.0.0.2\n10.0.0.1\n10.0.0.1\n' | private_ips_to_workers_csv 50561)"
assert_eq "private_ips_to_workers_csv sorts unique" \
  "${got}" "10.0.0.1:50561,10.0.0.2:50561"

got="$(printf '\n' | private_ips_to_workers_csv 50561)"
assert_eq "private_ips_to_workers_csv empty stays empty" "${got}" ""

# Regression: AMI once shipped `log()` to stdout; wait_for_worker_private_ips then
# captured `[oxidant-bootstrap] ASG … ready` into OXIDANT_WORKERS and fan-out became 3≠2.
got="$(printf '%s\n' \
  '[oxidant-bootstrap] ASG oxidant-sf100-workers: 2/2 worker private IPs ready' \
  '10.0.0.1' \
  '10.0.0.2' | private_ips_to_workers_csv 50561)"
assert_eq "private_ips_to_workers_csv ignores log-line pollution" \
  "${got}" "10.0.0.1:50561,10.0.0.2:50561"

# log() must not touch stdout — helpers' stdout is machine-parsed into OXIDANT_WORKERS.
log_out="$(log 'must not appear on stdout' 2>/dev/null || true)"
assert_eq "log writes only to stderr" "${log_out}" ""

# --- mocked ASG Describe* path ------------------------------------------------
REGION=us-west-2
AWS_BIN=aws
export REGION AWS_BIN

mock_aws_full() {
  # shellcheck disable=SC2317
  aws() {
    local args=("$@")
    local joined="$*"
    if [[ "${joined}" == *describe-auto-scaling-groups* ]]; then
      printf 'i-aaa\ti-bbb\n'
      return 0
    fi
    if [[ "${joined}" == *describe-instances* && "${joined}" == *i-aaa* ]]; then
      printf '172.31.1.10\n'
      return 0
    fi
    if [[ "${joined}" == *describe-instances* && "${joined}" == *i-bbb* ]]; then
      printf '172.31.2.20\n'
      return 0
    fi
    echo "unexpected aws invocation: ${joined}" >&2
    return 1
  }
  export -f aws
}

mock_aws_partial() {
  # shellcheck disable=SC2317
  aws() {
    local joined="$*"
    if [[ "${joined}" == *describe-auto-scaling-groups* ]]; then
      printf 'i-only\n'
      return 0
    fi
    if [[ "${joined}" == *describe-instances* ]]; then
      printf '172.31.9.9\n'
      return 0
    fi
    return 1
  }
  export -f aws
}

mock_aws_full
export OXIDANT_BOOTSTRAP_WAIT_SECS=30 OXIDANT_BOOTSTRAP_POLL_SECS=1
ips="$(wait_for_worker_private_ips my-workers 2)"
csv="$(printf '%s\n' "${ips}" | private_ips_to_workers_csv 50561)"
assert_eq "ASG membership pins both private Flight endpoints" \
  "${csv}" "172.31.1.10:50561,172.31.2.20:50561"

# Incomplete ASG must fail closed (never pin a partial worker set for honesty runs).
mock_aws_partial
export OXIDANT_BOOTSTRAP_WAIT_SECS=2 OXIDANT_BOOTSTRAP_POLL_SECS=1
assert_fail "incomplete ASG fails closed" wait_for_worker_private_ips my-workers 2

# Over-subscribed ASG (instance refresh) must also wait — pinning 3 when WorkerCount=2
# trips driver fan-out ≠ OXIDANT_WORKER_COUNT.
mock_aws_over() {
  # shellcheck disable=SC2317
  aws() {
    local joined="$*"
    if [[ "${joined}" == *describe-auto-scaling-groups* ]]; then
      printf 'i-a\ti-b\ti-c\n'
      return 0
    fi
    if [[ "${joined}" == *describe-instances* && "${joined}" == *i-a* ]]; then
      printf '172.31.1.1\n'; return 0
    fi
    if [[ "${joined}" == *describe-instances* && "${joined}" == *i-b* ]]; then
      printf '172.31.1.2\n'; return 0
    fi
    if [[ "${joined}" == *describe-instances* && "${joined}" == *i-c* ]]; then
      printf '172.31.1.3\n'; return 0
    fi
    return 1
  }
  export -f aws
}
mock_aws_over
export OXIDANT_BOOTSTRAP_WAIT_SECS=2 OXIDANT_BOOTSTRAP_POLL_SECS=1
assert_fail "over-subscribed ASG fails closed (exact WorkerCount)" wait_for_worker_private_ips my-workers 2
assert_fail "wait_for_workers also exact (shard index)" wait_for_workers my-workers 2

mock_aws_full
export OXIDANT_BOOTSTRAP_WAIT_SECS=30 OXIDANT_BOOTSTRAP_POLL_SECS=1
got="$(wait_for_workers my-workers 2 | tr '\n' ' ' | sed 's/ *$//')"
assert_eq "wait_for_workers returns exact peer ids" "${got}" "i-aaa i-bbb"

# Guard: driver membership must not wait on / pin Route53 DNS.
if grep -q 'wait_for_worker_dns' "${BOOTSTRAP}"; then
  echo "not ok - wait_for_worker_dns must stay removed (ASG private IPs only)"
  FAIL=$((FAIL + 1))
else
  echo "ok - no wait_for_worker_dns"
  PASS=$((PASS + 1))
fi
if grep -n 'OXIDANT_WORKER_SERVICE=' "${BOOTSTRAP}" | grep -vE '^\s*#|k8s|not set|Do not set'; then
  echo "not ok - must not write OXIDANT_WORKER_SERVICE into oxidant.env on EC2"
  FAIL=$((FAIL + 1))
else
  echo "ok - OXIDANT_WORKER_SERVICE not stamped into env"
  PASS=$((PASS + 1))
fi
assert_ok "bootstrap references ASG private-IP helpers" \
  grep -q 'wait_for_worker_private_ips' "${BOOTSTRAP}"
assert_ok "bootstrap references CSV pin helper" \
  grep -q 'private_ips_to_workers_csv' "${BOOTSTRAP}"

# W24 single-node: the driver ASG peer wait/pin block in oxidant_bootstrap_main must be
# skipped entirely when worker-count=0 (DRIVER_WORKERS_CSV stays "" — the single-node
# signal render_env reads downstream). The gating predicate is extracted as
# driver_needs_peer_wait() specifically so it can be asserted behaviorally instead of by
# grepping bootstrap.sh for the literal condition text.
assert_fail "driver ASG wait is skipped when worker-count=0" \
  driver_needs_peer_wait driver 0
assert_ok "driver ASG wait runs when worker-count>0" \
  driver_needs_peer_wait driver 2
assert_fail "driver ASG wait predicate is driver-only (worker role)" \
  driver_needs_peer_wait worker 2
assert_fail "driver ASG wait skipped on leading-zero worker-count '00'" \
  driver_needs_peer_wait driver 00

echo
echo "${PASS} passed, ${FAIL} failed"
[[ "${FAIL}" -eq 0 ]]

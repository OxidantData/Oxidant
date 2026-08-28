#!/usr/bin/env bash
# KAN-139 regression tests for tag→env stamping in render_env / worker_count_or_fail:
#   - an empty/missing tag NEVER stamps its OXIDANT_* var (engine default applies);
#   - a non-numeric shuffle-partitions tag is ignored with a loud warning;
#   - an explicit value below the engine floor (max(200, worker_vcpus)) is stamped
#     (operator intent wins) but warns loudly at boot;
#   - an empty/garbage worker-count tag fails closed on clustered roles.
# No real AWS calls or fs side effects — render_env is pure stdout.
# Run: bash deploy/packer/tests/test_env_stamping.sh
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
  if "$@" >/tmp/oxidant-env-test.out 2>/tmp/oxidant-env-test.err; then
    echo "ok - ${name}"
    PASS=$((PASS + 1))
  else
    echo "not ok - ${name} (exit $?)"
    cat /tmp/oxidant-env-test.err >&2 || true
    FAIL=$((FAIL + 1))
  fi
}

assert_fail() {
  local name="$1"
  shift
  if "$@" >/tmp/oxidant-env-test.out 2>/tmp/oxidant-env-test.err; then
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

# Globals render_env reads (normally populated from instance tags in main).
REGION=us-west-2
WORKER_COUNT=2
SHARD_INDEX=0
MEMORY_LIMIT_BYTES=""
SHUFFLE_SPILL_BYTES=""
SHUFFLE_PARTITIONS=""
CATALOG_CONF="none"
DISTRIBUTED_STRICT="false"
PREFER_HASH_JOIN="auto"
DRIVER_WORKERS_CSV="10.0.0.1:50561,10.0.0.2:50561"

driver_env() { render_env driver 2>/tmp/oxidant-env-test.err; }
driver_err() { render_env driver >/dev/null 2>/tmp/oxidant-env-test.err || true; cat /tmp/oxidant-env-test.err; }

# --- OXIDANT_SHUFFLE_PARTITIONS stamping contract -------------------------------

# Empty tag: never stamps (engine default max(200, worker_vcpus) applies), no warning.
SHUFFLE_PARTITIONS=""
out="$(driver_env)"
if grep -q '^OXIDANT_SHUFFLE_PARTITIONS=' <<<"${out}"; then
  assert_eq "empty shuffle-partitions tag never stamps" "stamped" ""
else
  assert_eq "empty shuffle-partitions tag never stamps" "" ""
fi
err="$(driver_err)"
assert_eq "empty shuffle-partitions tag stays quiet" \
  "$(grep -c 'shuffle-partitions' <<<"${err}" || true)" "0"

# The literal None (aws CLI empty-tag rendering) must behave like empty.
SHUFFLE_PARTITIONS="None"
out="$(driver_env)"
if grep -q '^OXIDANT_SHUFFLE_PARTITIONS=' <<<"${out}"; then
  assert_eq "None shuffle-partitions tag never stamps" "stamped" ""
else
  assert_eq "None shuffle-partitions tag never stamps" "" ""
fi

# The SF100 failure mode: an explicit 32 IS stamped (operator intent) but must warn loudly.
SHUFFLE_PARTITIONS="32"
out="$(driver_env)"
assert_eq "explicit 32 stamps (operator intent wins)" \
  "$(grep -c '^OXIDANT_SHUFFLE_PARTITIONS=32$' <<<"${out}" || true)" "1"
err="$(driver_err)"
assert_eq "explicit 32 logs a loud boot warning" \
  "$(grep -c 'WARNING: oxidant:shuffle-partitions=32 is below the engine' <<<"${err}" || true)" "1"

# An explicit value at/above the floor stamps without a warning.
SHUFFLE_PARTITIONS="400"
out="$(driver_env)"
assert_eq "400 stamps" \
  "$(grep -c '^OXIDANT_SHUFFLE_PARTITIONS=400$' <<<"${out}" || true)" "1"
err="$(driver_err)"
assert_eq "400 warns nothing" \
  "$(grep -c 'shuffle-partitions' <<<"${err}" || true)" "0"

# Garbage never reaches the env file — the engine default applies instead.
SHUFFLE_PARTITIONS="abc"
out="$(driver_env)"
if grep -q '^OXIDANT_SHUFFLE_PARTITIONS=' <<<"${out}"; then
  assert_eq "non-numeric shuffle-partitions never stamps" "stamped" ""
else
  assert_eq "non-numeric shuffle-partitions never stamps" "" ""
fi
err="$(driver_err)"
assert_eq "non-numeric shuffle-partitions warns" \
  "$(grep -c "WARNING: ignoring non-numeric oxidant:shuffle-partitions='abc'" <<<"${err}" || true)" "1"

# Zero is not a valid fan-out: ignored like garbage.
SHUFFLE_PARTITIONS="0"
out="$(driver_env)"
if grep -q '^OXIDANT_SHUFFLE_PARTITIONS=' <<<"${out}"; then
  assert_eq "zero shuffle-partitions never stamps" "stamped" ""
else
  assert_eq "zero shuffle-partitions never stamps" "" ""
fi
SHUFFLE_PARTITIONS=""

# --- other env vars: empty tags must not stamp either ---------------------------

MEMORY_LIMIT_BYTES=""
SHUFFLE_SPILL_BYTES=""
out="$(render_env worker 2>/dev/null)"
if grep -qE '^OXIDANT_(MEMORY_LIMIT_BYTES|SHUFFLE_SPILL_BYTES)=' <<<"${out}"; then
  assert_eq "empty memory/spill tags never stamp (worker)" "stamped" ""
else
  assert_eq "empty memory/spill tags never stamp (worker)" "" ""
fi
MEMORY_LIMIT_BYTES="None"
SHUFFLE_SPILL_BYTES="None"
out="$(render_env worker 2>/dev/null)"
if grep -qE '^OXIDANT_(MEMORY_LIMIT_BYTES|SHUFFLE_SPILL_BYTES)=' <<<"${out}"; then
  assert_eq "None memory/spill tags never stamp (worker)" "stamped" ""
else
  assert_eq "None memory/spill tags never stamp (worker)" "" ""
fi
MEMORY_LIMIT_BYTES="28895544320"
SHUFFLE_SPILL_BYTES="8589934592"
out="$(render_env worker 2>/dev/null)"
assert_eq "worker memory limit stamps when set" \
  "$(grep -c '^OXIDANT_MEMORY_LIMIT_BYTES=28895544320$' <<<"${out}" || true)" "1"
assert_eq "worker spill bytes stamp when set" \
  "$(grep -c '^OXIDANT_SHUFFLE_SPILL_BYTES=8589934592$' <<<"${out}" || true)" "1"
MEMORY_LIMIT_BYTES=""
SHUFFLE_SPILL_BYTES=""

# --- OXIDANT_WORKER_COUNT: fail closed on clustered roles -----------------------

assert_eq "worker-count passes through when valid" \
  "$(worker_count_or_fail driver 2)" "2"
assert_eq "worker-count passes through for workers" \
  "$(worker_count_or_fail worker 8)" "8"
assert_fail "empty worker-count tag fails closed (driver)" worker_count_or_fail driver ""
assert_fail "None worker-count tag fails closed (driver)" worker_count_or_fail driver None
assert_fail "garbage worker-count tag fails closed (worker)" worker_count_or_fail worker abc
assert_eq "standalone keeps the 1 default" "$(worker_count_or_fail standalone "")" "1"

# W24 single-node: a driver legitimately carries worker-count 0 (driver-local boot);
# a worker never does. Empty/garbage still fails closed for both roles (KAN-139 regression).
assert_eq "driver worker-count 0 is accepted (single-node)" \
  "$(worker_count_or_fail driver 0)" "0"
assert_fail "worker worker-count 0 is refused" worker_count_or_fail worker 0
assert_fail "empty worker-count tag still fails closed (driver)" worker_count_or_fail driver ""
assert_fail "garbage worker-count tag still fails closed (driver)" worker_count_or_fail driver abc

# Driver env still pins membership and stamps the count it was given.
out="$(driver_env)"
assert_eq "driver env stamps OXIDANT_WORKER_COUNT" \
  "$(grep -c '^OXIDANT_WORKER_COUNT=2$' <<<"${out}" || true)" "1"
assert_eq "driver env pins OXIDANT_WORKERS" \
  "$(grep -c '^OXIDANT_WORKERS=10.0.0.1:50561,10.0.0.2:50561$' <<<"${out}" || true)" "1"

# A driver without a worker list still fails (no silent local fallback) — the SF100
# driver-local regression this guard caught. Explicit about WORKER_COUNT staying
# nonzero here, since that's the branch the guard must still refuse.
WORKER_COUNT=2
DRIVER_WORKERS_CSV=""
assert_fail "driver with empty worker list and nonzero worker-count still fails" render_env driver
DRIVER_WORKERS_CSV="10.0.0.1:50561,10.0.0.2:50561"

# --- W24 single-node driver: worker-count=0, no OXIDANT_WORKERS pinned ----------

WORKER_COUNT=0
DRIVER_WORKERS_CSV=""
assert_ok "single-node driver (worker-count=0) render_env succeeds" render_env driver
out="$(render_env driver 2>/tmp/oxidant-env-test.err)"
if grep -q '^OXIDANT_WORKERS=' <<<"${out}"; then
  assert_eq "single-node driver emits no OXIDANT_WORKERS" "stamped" ""
else
  assert_eq "single-node driver emits no OXIDANT_WORKERS" "" ""
fi
assert_eq "single-node driver stamps OXIDANT_WORKER_COUNT=0" \
  "$(grep -c '^OXIDANT_WORKER_COUNT=0$' <<<"${out}" || true)" "1"
err="$(cat /tmp/oxidant-env-test.err)"
assert_eq "single-node driver logs the driver-local line" \
  "$(grep -c 'single-node driver (worker-count=0): no OXIDANT_WORKERS pinned, driver-local' <<<"${err}" || true)" "1"
WORKER_COUNT=2
DRIVER_WORKERS_CSV="10.0.0.1:50561,10.0.0.2:50561"

echo
echo "${PASS} passed, ${FAIL} failed"
[[ "${FAIL}" -eq 0 ]]

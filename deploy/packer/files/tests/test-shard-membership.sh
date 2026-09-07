#!/usr/bin/env bash
# Pure-function tests for shard-membership.sh (OxidantData/Oxidant#184).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
# shellcheck source=../shard-membership.sh
source "${ROOT}/shard-membership.sh"

fail() { echo "FAIL: $*" >&2; exit 1; }

expect_index() {
  local want="$1"
  shift
  local got rc
  got="$(oxidant_resolve_shard_index "$@" )" && rc=0 || rc=$?
  [[ "${rc}" -eq 0 ]] || fail "expected index ${want}, rc=${rc} for $*"
  [[ "${got}" == "${want}" ]] || fail "expected index ${want}, got ${got} for $*"
}

expect_rc() {
  local want="$1"
  shift
  local rc=0
  oxidant_resolve_shard_index "$@" >/dev/null || rc=$?
  [[ "${rc}" -eq "${want}" ]] || fail "expected rc ${want}, got ${rc} for $*"
}

# Healthy control: count=2, peers i-a,i-b, self i-b → 1
expect_index 1 2 i-b i-a i-b

# Sorted order: i-a is ordinal 0
expect_index 0 2 i-a i-a i-b

# Excess member i-c with self i-c would have been ordinal 2; refuse.
expect_rc 2 2 i-c i-a i-b i-c

# Absent self must not be repaired by insertion (would become ordinal 2).
expect_rc 3 2 i-c i-a i-b

# Too few members
expect_rc 2 2 i-a i-a

# Duplicate IDs
expect_rc 4 2 i-a i-a i-a

# Malformed count
expect_rc 5 not-a-number i-a i-a i-b
expect_rc 5 0 i-a
expect_rc 5 -1 i-a

# Empty self
expect_rc 3 2 "" i-a i-b

echo "ok: test-shard-membership.sh"

#!/usr/bin/env bash
# Pure membership → shard-index mapping for the fixed-size worker ASG.
# Used by the periodic resolver. Do not insert an unobserved self; do not
# accept a peer set larger or smaller than WORKER_COUNT (OxidantData/Oxidant#184).
#
# oxidant_resolve_shard_index COUNT SELF_ID PEER_ID...
#   stdout: ordinal in [0, COUNT)
#   0 success
#   2 wrong peer-set size
#   3 self not observed
#   4 duplicate instance ids
#   5 malformed count
#   6 ordinal out of range (defensive)

oxidant_resolve_shard_index() {
  local worker_count="${1-}"
  local self_id="${2-}"
  shift 2 || true
  local -a peers=("$@")

  if ! [[ "${worker_count}" =~ ^[1-9][0-9]*$ ]]; then
    return 5
  fi
  if [[ -z "${self_id}" ]]; then
    return 3
  fi

  local n="${#peers[@]}"
  if (( n != worker_count )); then
    return 2
  fi

  local uniq
  uniq="$(printf '%s\n' "${peers[@]}" | sort -u | grep -c . || true)"
  if (( uniq != n )); then
    return 4
  fi

  local -a sorted
  IFS=$'\n' sorted=($(printf '%s\n' "${peers[@]}" | sort -u))
  local i resolved=-1
  for i in "${!sorted[@]}"; do
    if [[ "${sorted[$i]}" == "${self_id}" ]]; then
      resolved=$i
      break
    fi
  done
  if (( resolved < 0 )); then
    return 3
  fi
  if (( resolved >= worker_count )); then
    return 6
  fi
  printf '%s\n' "${resolved}"
  return 0
}

oxidant_membership_fingerprint() {
  printf '%s\n' "$@" | sort -u | paste -sd, -
}

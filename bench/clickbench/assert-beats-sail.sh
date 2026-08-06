#!/usr/bin/env bash
# CI gate: fail if Oxidant's total hot runtime exceeds the Sail baseline.
# Usage: assert-beats-sail.sh <sail_total_seconds>   (default 56.3, c6a.4xlarge 2026-05-11)
set -euo pipefail
cd "$(dirname "$0")"
SAIL_TOTAL="${1:-56.3}"

LATEST="$(ls -1d results/*/ 2>/dev/null | tail -n1 || true)"
if [[ -z "$LATEST" ]]; then
  echo "no results found under results/; run benchmark.sh first" >&2
  exit 2
fi
JSON="$(ls -1 "$LATEST"*.json | head -n1)"

# Sum the hot number (min of the 2nd and 3rd element) across all 43 queries.
OXIDANT_TOTAL="$(python3 - "$JSON" <<'PY'
import json, sys
data = json.load(open(sys.argv[1]))
total = 0.0
for row in data["result"]:
    hots = [x for x in row[1:] if x is not None]
    if not hots:
        print("MISSING", file=sys.stderr); sys.exit(3)
    total += min(hots)
print(f"{total:.3f}")
PY
)"

echo "Oxidant hot total: ${OXIDANT_TOTAL}s   Sail baseline: ${SAIL_TOTAL}s" >&2
awk -v w="$OXIDANT_TOTAL" -v s="$SAIL_TOTAL" 'BEGIN { exit !(w <= s) }' \
  && { echo "PASS: Oxidant beats Sail"; exit 0; } \
  || { echo "FAIL: Oxidant slower than Sail"; exit 1; }

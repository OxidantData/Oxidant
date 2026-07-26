#!/usr/bin/env bash
# Daily-maintenance scan for the Weft engine repo.
#
# This is the DETERMINISTIC half of the daily automation: it runs the cheap-core
# quality + security gates and writes machine-readable reports under
# target/daily-maintenance/. The Cursor agent (.cursor/rules/daily-maintenance.mdc)
# then reads those reports and does the triage / PR / advisory work.
#
# Deliberately does NOT run the heavy bench/parity gates (tpch, clickbench x2,
# correctness, parity) — those live in scripts/ci-local.sh and are too slow for a
# daily pass. Run those in a separate weekly agent if you want deeper coverage.
#
# Each step is wrapped so one failure does not abort the rest: we want the full
# picture in one run, not a stop at the first red gate. Individual exit codes are
# recorded in target/daily-maintenance/summary.txt.

set -uo pipefail
# Must run from the repo root; abort loudly rather than scan the wrong directory.
cd "$(dirname "$0")/.." || { echo "FATAL: cannot cd to repo root" >&2; exit 1; }

OUT="target/daily-maintenance"
mkdir -p "$OUT"
# Wipe reports from any prior run first: target/ is a persistent cache in the Cursor
# env, and a step that is skipped this run (e.g. a MISSING tool) must not leave a
# stale report the agent would read as today's signal.
rm -f "$OUT"/*.txt "$OUT"/*.log "$OUT"/*.json "$OUT"/*.diff 2>/dev/null || true

# The agent has no clock; stamp the real run date here so it can title the daily
# triage issue correctly even on days the repo has no new commit.
RUN_DATE="$(date -u +%Y-%m-%d)"
echo "$RUN_DATE" > "$OUT/run-date.txt"
: > "$OUT/summary.txt"

# run <label> <report-file> <cmd...> — run a scan step, capture stdout+stderr to the
# report, record status. Use for human-readable logs and JSON tools whose stderr is
# empty on success (cargo audit/deny).
run() {
  local label="$1"; shift
  local report="$1"; shift
  echo "==> $label"
  local status=0
  "$@" > "$OUT/$report" 2>&1 || status=$?
  _record "$label" "$report" "$status"
}

# run_split <label> <stdout-report> <cmd...> — like run() but keeps stderr OUT of the
# report (stderr -> <report>.stderr.log). Use for machine-readable stdout (clippy JSON)
# that must stay parseable — cargo interleaves "Checking …" / "error: could not compile"
# progress lines on stderr that would otherwise corrupt the JSON stream.
run_split() {
  local label="$1"; shift
  local report="$1"; shift
  echo "==> $label"
  local status=0
  "$@" > "$OUT/$report" 2>"$OUT/$report.stderr.log" || status=$?
  _record "$label" "$report" "$status"
}

_record() {
  local label="$1" report="$2" status="$3"
  if [ "$status" -eq 0 ]; then
    echo "  OK    $label -> $report"
    printf '%-24s OK    (%s)\n' "$label" "$report" >> "$OUT/summary.txt"
  else
    echo "  FLAG  $label (exit $status) -> $report"
    printf '%-24s FLAG  exit=%s (%s)\n' "$label" "$status" "$report" >> "$OUT/summary.txt"
  fi
  return 0
}

echo "# daily-maintenance scan ($RUN_DATE) — reports in $OUT/"

# --- formatting: non-zero exit means the tree is unformatted (a finding) ---
run "rustfmt-check"   "fmt.diff"        cargo fmt --all -- --check

# --- weft-cli MUST be built before tests/clippy (binary-only crate; AGENTS.md).
#     If this FLAGs, test.log below is unreliable (CLI integration tests can't spawn
#     the binary) — the agent must read build-cli.log before triaging test failures.
#     --locked so the scan never silently rewrites Cargo.lock. ---
run "build-weft-cli"  "build-cli.log"   cargo build --locked -p weft-cli

# --- clippy: JSON on stdout for per-lint parsing; stderr split out so it stays valid.
#     -D warnings = the gate. ---
run_split "clippy"    "clippy.json"     cargo clippy --locked --workspace --all-targets --message-format=json -- -D warnings

# --- test suite: failures are bugs to triage ---
run "test"            "test.log"        cargo test --locked --workspace

# --- dependency CVEs: RUSTSEC advisories (first real security signal for this repo) ---
if command -v cargo-audit >/dev/null 2>&1; then
  run "cargo-audit"   "audit.json"      cargo audit --json
else
  echo "  MISSING cargo-audit — install with: cargo install cargo-audit --locked"
  printf '%-24s MISSING (install cargo-audit)\n' "cargo-audit" >> "$OUT/summary.txt"
fi

# --- advisories + licenses + bans + yanked, per deny.toml ---
if command -v cargo-deny >/dev/null 2>&1; then
  run "cargo-deny"    "deny.log"        cargo deny check
else
  echo "  MISSING cargo-deny — install with: cargo install cargo-deny --locked"
  printf '%-24s MISSING (install cargo-deny)\n' "cargo-deny" >> "$OUT/summary.txt"
fi

# --- within-semver dependency updates (informational; drives chore(deps) PRs).
#     NOTE: this only surfaces bumps allowed by current Cargo.toml constraints — it
#     does NOT report newer *major* versions pinned out by those constraints. For that,
#     `cargo outdated` would be needed (extra tool, not installed here). Cross-major
#     security-relevant upgrades still surface via cargo-audit/cargo-deny above. ---
run "dep-updates"     "updates.log"     cargo update --dry-run --verbose

echo
echo "==> summary"
cat "$OUT/summary.txt"
echo
echo "Reports written to $OUT/. Hand these to the daily-maintenance playbook."

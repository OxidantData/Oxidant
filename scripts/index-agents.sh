#!/usr/bin/env bash
# Rebuild local CodeGraph + GitNexus indexes for this repo.
# Indexes are gitignored (.codegraph/, .gitnexus/). See docs/AGENT_INDEXING.md.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

missing=0
if ! command -v codegraph >/dev/null 2>&1; then
  echo "warn: codegraph not on PATH — install: npm i -g @colbymchenry/codegraph" >&2
  missing=1
fi
if ! command -v gitnexus >/dev/null 2>&1; then
  echo "warn: gitnexus not on PATH — install: npm i -g gitnexus" >&2
  missing=1
fi

if [[ "$missing" -eq 1 ]]; then
  echo "error: install missing CLIs, then re-run (or see docs/AGENT_INDEXING.md)" >&2
  exit 1
fi

echo "==> codegraph init ($ROOT)"
codegraph init

echo "==> gitnexus analyze ($ROOT)"
# Preserve committed AGENTS.md (Cursor Cloud + daily-maintenance sections).
gitnexus analyze --skip-agents-md

echo "done. Verify with: codegraph status && gitnexus doctor"
echo "Multi-repo: after indexing oxidant-platform too, see docs/AGENT_INDEXING.md (group oxidant)."

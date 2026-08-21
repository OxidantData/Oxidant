#!/usr/bin/env bash
# One-shot brand rename: weft -> oxidant (case-aware: Weft -> Oxidant, WEFT -> OXIDANT).
# Executed exactly once for the rebrand PR; kept in history so the mega-diff is
# auditable as "this script + its output", not 200 hand edits.
#
# Handles: file contents (tracked text files), directory + file names, Cargo.lock
# regeneration, and fixups for URLs/emails the blanket replace would corrupt.
#
# NOT touched (intentional "Weft" mentions): NOTICE, TRADEMARK.md, COMMERCIAL.md,
# this script. Historical benchmark data under bench/**/results (untracked) is
# left as-is.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

if [ -n "$(git status --porcelain --untracked-files=no)" ]; then
  echo "error: working tree must be clean (tracked files)" >&2
  exit 1
fi

# --- 1. Content replace (case-aware) in every tracked text file containing weft
git grep -Iil weft | while read -r f; do
  case "$f" in
    NOTICE|TRADEMARK.md|COMMERCIAL.md|scripts/rename-to-oxidant.sh|Cargo.lock) continue ;;
  esac
  perl -pi -e 's/WEFT/OXIDANT/g; s/Weft/Oxidant/g; s/weft/oxidant/g' "$f"
done

# --- 2. Fixups for URLs / emails / org names the blanket gets wrong
# NOTE: during the one-shot run this step matched the script's own literal
# patterns and partially self-rewrote it (e.g. 'Oxidant Labs' -> 'Oxidant Data'
# inside the pattern list). The block below is the corrected, complete rule set
# as actually needed; the fixes it documents all landed in the rename PR.
(git grep -Il -e oxidantlabs -e 'vamzi/oxidant' -e 'github.io/oxidant' -e 'Oxidant Labs' || true) \
  | while read -r f; do
      case "$f" in
        scripts/rename-to-oxidant.sh) continue ;;
      esac
      perl -pi -e '
        s|gitlab\.com/oxidantlabs/oxidant-platform|gitlab.com/weftlabs/weft-platform|g;
        s|gitlab\.com/oxidantlabs/oxidant|github.com/OxidantData/Oxidant|g;
        s|github\.com/oxidantlabs/oxidant|github.com/OxidantData/Oxidant|g;
        s|github\.com/vamzi/oxidant|github.com/OxidantData/Oxidant|g;
        s|repos/vamzi/oxidant/|repos/OxidantData/Oxidant/|g;
        s|vamzi\.github\.io/oxidant|oxidantdata.com|g;
        s/oxidantlabs\.dev/oxidantdata.com/g;
        s/Oxidant Labs/Oxidant Data/g;
      ' "$f"
    done

# --- 3. Rename directories (deepest first), then remaining files with weft in the name
find . -depth -type d -iname '*weft*' \
  -not -path './.git/*' -not -path './target/*' -not -path '*/node_modules/*' \
  | while read -r d; do
      nd="$(dirname "$d")/$(basename "$d" | sed -e 's/weft/oxidant/g' -e 's/Weft/Oxidant/g')"
      git mv "$d" "$nd" 2>/dev/null || mv "$d" "$nd"
    done

git ls-files | grep -i weft | while read -r f; do
  nf="$(dirname "$f")/$(basename "$f" | sed -e 's/weft/oxidant/g' -e 's/Weft/Oxidant/g')"
  git mv "$f" "$nf"
done

# --- 4. Regenerate Cargo.lock against the renamed packages
cargo metadata --no-deps --format-version 1 >/dev/null

echo "== rename pass complete =="
echo "residual case-insensitive 'weft' mentions (should be intentional-only):"
git grep -in weft || echo "(none)"

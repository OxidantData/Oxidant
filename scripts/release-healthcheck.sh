#!/usr/bin/env bash
# Probe the install paths Oxidant's docs actually publish, against the release GitHub
# currently serves as `latest`.
#
# Every other release check we have looks at the pipeline's own state; this one looks at
# what a new user hits. v0.2.3-v0.2.5 were tagged by hand at an unbumped Cargo.toml, so
# cargo-dist's `plan` job aborted and all three releases went public with zero assets.
# `binaries.yml` was red, but `latest` pointed at an empty release for days and the only
# signal was a user reporting that the curl|sh line did nothing.
#
# Usage: scripts/release-healthcheck.sh [tag]     (default: whatever /releases/latest resolves to)
# Exit 0 = every documented install path is live. Exit 1 = at least one is broken.
set -uo pipefail

cd "$(dirname "$0")/.." || exit 1

REPO="${OXIDANT_REPO:-OxidantData/Oxidant}"
TAP_REPO="${OXIDANT_TAP_REPO:-OxidantData/homebrew-tap}"
IMAGE_REPO="${OXIDANT_IMAGE_REPO:-oxidantdata/oxidant}"

failed=0
fail() { printf '  FAIL  %s\n' "$*"; failed=1; }
ok()   { printf '  ok    %s\n' "$*"; }

# ---------------------------------------------------------------- resolve the release
TAG="${1:-}"
PINNED="$TAG"
if [ -z "$TAG" ]; then
  if command -v gh >/dev/null 2>&1; then
    TAG=$(gh api "repos/$REPO/releases/latest" --jq .tag_name 2>/dev/null)
  else
    TAG=$(curl -sSL -H 'Accept: application/vnd.github+json' \
            "https://api.github.com/repos/$REPO/releases/latest" | jq -r .tag_name)
  fi
fi
if [ -z "$TAG" ] || [ "$TAG" = "null" ]; then
  echo "could not resolve the latest release of $REPO" >&2
  exit 1
fi
VERSION="${TAG#v}"

# With no argument we probe `releases/latest/download/...`, because that redirect is the
# thing the docs tell users to curl and the thing that broke. Given an explicit tag we
# probe that release directly, so this can be pointed at a known-good release as a control.
if [ -n "$PINNED" ]; then
  DOWNLOAD_BASE="https://github.com/$REPO/releases/download/$TAG"
else
  DOWNLOAD_BASE="https://github.com/$REPO/releases/latest/download"
fi
echo "checking $REPO $TAG (version $VERSION) via $DOWNLOAD_BASE"

# Shared with the `verify-release` job in .github/workflows/binaries.yml, so the set this
# checks in the wild is the same set the pipeline refuses to publish without.
# (while-read rather than `mapfile`: macOS ships bash 3.2, so this has to run there too.)
expected=()
while IFS= read -r name; do
  [ -n "$name" ] && expected+=("$name")
done < <(./scripts/release-expected-assets.sh "$VERSION")
if [ "${#expected[@]}" -eq 0 ]; then
  echo "release-expected-assets.sh produced nothing" >&2
  exit 1
fi

# ------------------------------------------------- assets are attached and not truncated
echo "release assets"
assets=$(gh api "repos/$REPO/releases/tags/$TAG" --jq '.assets[] | "\(.name) \(.size)"' 2>/dev/null)
if [ -z "$assets" ]; then
  fail "$TAG has no assets attached at all (this is the v0.2.5 failure mode)"
else
  for name in "${expected[@]}"; do
    size=$(awk -v n="$name" '$1 == n {print $2}' <<<"$assets")
    if   [ -z "$size" ]; then fail "asset missing: $name"
    elif [ "$size" -eq 0 ]; then fail "asset is zero bytes: $name"
    fi
  done
  [ "$failed" -eq 0 ] && ok "all ${#expected[@]} expected assets present and non-empty"
fi

# --------------------------------------- the literal URLs README/getting-started publish
echo "documented download URLs"
probe() {
  local url="$1" code
  code=$(curl -sSL -o /dev/null -w '%{http_code}' "$url")
  if [ "$code" = 200 ]; then ok "${url##*/} ($code)"; else fail "${url##*/} -> HTTP $code"; fi
}
for name in "${expected[@]}"; do
  probe "$DOWNLOAD_BASE/$name"
done

# -------------------------------------------------------------- homebrew tap tracks it
echo "homebrew tap"
formula=$(curl -sSL "https://raw.githubusercontent.com/$TAP_REPO/HEAD/Formula/oxidant.rb")
tap_version=$(sed -n 's/^[[:space:]]*version "\([^"]*\)".*/\1/p' <<<"$formula" | head -1)
if [ "$tap_version" = "$VERSION" ]; then
  ok "$TAP_REPO formula is $tap_version"
else
  # This is the quiet one: `brew install` keeps succeeding and keeps handing out the old
  # version, so nobody reports a failure.
  fail "$TAP_REPO formula is ${tap_version:-unparseable}, expected $VERSION"
fi

# --------------------------------------------------------------------- container image
echo "container image"
token=$(curl -sSL "https://ghcr.io/token?scope=repository:${IMAGE_REPO}:pull&service=ghcr.io" | jq -r .token)
code=$(curl -sSL -o /dev/null -w '%{http_code}' \
  -H "Authorization: Bearer $token" \
  -H 'Accept: application/vnd.oci.image.index.v1+json,application/vnd.docker.distribution.manifest.list.v2+json' \
  "https://ghcr.io/v2/${IMAGE_REPO}/manifests/$VERSION")
if [ "$code" = 200 ]; then ok "ghcr.io/$IMAGE_REPO:$VERSION ($code)"
else fail "ghcr.io/$IMAGE_REPO:$VERSION -> HTTP $code"; fi

echo
if [ "$failed" -ne 0 ]; then
  echo "FAILED - at least one documented install path for $TAG is broken."
  exit 1
fi
echo "OK - every documented install path for $TAG is live."

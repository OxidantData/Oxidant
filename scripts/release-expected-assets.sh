#!/usr/bin/env bash
# Print the asset names a complete Oxidant release must carry, one per line.
#
# Single source of truth for two consumers that must agree: the `verify-release` job in
# .github/workflows/binaries.yml (asserts them at publish time) and
# scripts/release-healthcheck.sh (asserts them against whatever is live).
#
# Usage: scripts/release-expected-assets.sh <version>     e.g. 0.2.6
set -euo pipefail

cd "$(dirname "$0")/.."

VERSION="${1:?usage: release-expected-assets.sh <version>}"
VERSION="${VERSION#v}"

# From dist: the curl|sh installer, the Homebrew formula, the manifest and checksums,
# and the source archive.
cat <<EOF
oxidant-installer.sh
oxidant.rb
dist-manifest.json
sha256.sum
source.tar.gz
source.tar.gz.sha256
EOF

# From the hand-added package-linux job: nfpm packages plus the sample-data archive that
# docs/getting-started.md tells users to curl. sample-data is uploaded only from the
# x86_64 leg, so a single-arch failure silently drops it.
cat <<EOF
sample-data.tar.gz
oxidant_${VERSION}_amd64.deb
oxidant_${VERSION}_arm64.deb
oxidant-${VERSION}-1.x86_64.rpm
oxidant-${VERSION}-1.aarch64.rpm
EOF

# One tarball + checksum per shipped target. Read from dist-workspace.toml rather than
# hardcoded, so adding a target cannot leave this list stale.
# (while-read rather than `mapfile`: macOS ships bash 3.2, so this has to run there too.)
awk '/^targets = \[/ {inside=1; next} inside && /^\]/ {inside=0} inside' dist-workspace.toml |
  tr -d '", ' | grep -v '^$' |
  while IFS= read -r target; do
    echo "oxidant-${target}.tar.xz"
    echo "oxidant-${target}.tar.xz.sha256"
  done

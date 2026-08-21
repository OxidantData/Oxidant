#!/usr/bin/env bash
# Gluten + Velox backend for Spark 3.5 — a single prebuilt "bundle" JAR layered onto the same
# Spark install from install-spark.sh. Gluten offloads Spark SQL operators to the native Velox
# engine, so this is the "Spark made fast with a native vectorized backend" data point.
#
# Apache Gluten publishes per-(Spark, OS, arch) bundle jars on its releases page. The exact asset
# name moves with each release, so it is overridable via env. Defaults target Spark 3.5 / x86_64.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
GLUTEN_VERSION="${GLUTEN_VERSION:-1.3.0}"
# Apache Gluten stopped attaching prebuilt bundle jars to GitHub releases and moved the repo to
# apache/gluten; the velox bundle now ships *inside* the official binary tarball on dist.apache.
# The CentOS-7 build links against an older glibc and runs fine on Ubuntu 24.04 (libvelox.so
# resolves its deps once libnuma1 is present). Override GLUTEN_TARBALL_URL / GLUTEN_JAR_GLOB if a
# newer release renames things.
GLUTEN_TARBALL="apache-gluten-${GLUTEN_VERSION}-incubating-bin-spark35.tar.gz"
GLUTEN_TARBALL_URL="${GLUTEN_TARBALL_URL:-https://dlcdn.apache.org/incubator/gluten/${GLUTEN_VERSION}-incubating/${GLUTEN_TARBALL}}"
GLUTEN_JAR_GLOB="${GLUTEN_JAR_GLOB:-gluten-velox-bundle-spark3.5_2.12-*_x86_64-${GLUTEN_VERSION}.jar}"

DEST="$HERE/jars"
mkdir -p "$DEST"
# Gluten needs libnuma at runtime.
sudo apt-get install -y --no-install-recommends libnuma1 >/dev/null 2>&1 || true

if ! ls "$DEST"/gluten-velox-bundle-*.jar >/dev/null 2>&1; then
  if [ -n "${GLUTEN_URL:-}" ]; then          # explicit direct-jar override still honored
    echo "[gluten] downloading bundle jar from GLUTEN_URL …"
    curl -fSL -o "$DEST/$(basename "$GLUTEN_URL")" "$GLUTEN_URL"
  else
    echo "[gluten] fetching $GLUTEN_TARBALL …"
    TMP="$(mktemp -d)"
    if curl -fSL -o "$TMP/$GLUTEN_TARBALL" "$GLUTEN_TARBALL_URL" \
       || curl -fSL -o "$TMP/$GLUTEN_TARBALL" "https://archive.apache.org/dist/incubator/gluten/${GLUTEN_VERSION}-incubating/${GLUTEN_TARBALL}"; then
      tar -xzf "$TMP/$GLUTEN_TARBALL" -C "$TMP"
      JAR="$(find "$TMP" -name "$GLUTEN_JAR_GLOB" | head -1)"
      [ -n "$JAR" ] && cp "$JAR" "$DEST/" || echo "[gluten] WARN: bundle jar not found in tarball ($GLUTEN_JAR_GLOB)"
    else
      echo "[gluten] WARN: could not download tarball — gluten will be skipped (recorded pending)"
    fi
    rm -rf "$TMP"
  fi
fi
ls "$DEST"/gluten-velox-bundle-*.jar 2>/dev/null && echo "[gluten] ready" \
  || echo "[gluten] no bundle jar — run-engine.sh gluten will exit and the engine stays pending"

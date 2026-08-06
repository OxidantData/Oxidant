#!/usr/bin/env bash
# Sail (LakeSail OSS) — a Rust Spark Connect server, `pip install pysail`. This is the engine
# Oxidant's headline result is measured against, so a fresh same-box Sail number is the apples-to-
# apples baseline. Sail speaks Spark Connect, so the same PySpark client drives it.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
SAIL_VERSION="${SAIL_VERSION:-}"   # empty = latest on PyPI
PYSPARK_FOR_SAIL="${PYSPARK_FOR_SAIL:-3.5.3}"

VENV="$HERE/.venv-sail"
if [ ! -d "$VENV" ]; then
  python3 -m venv "$VENV"
  "$VENV/bin/pip" install --quiet --upgrade pip
  if [ -n "$SAIL_VERSION" ]; then
    "$VENV/bin/pip" install --quiet "pysail==${SAIL_VERSION}"
  else
    "$VENV/bin/pip" install --quiet pysail
  fi
  # Stock PySpark client to drive Sail's Connect endpoint (see install-spark.sh for the pins).
  "$VENV/bin/pip" install --quiet \
    "pyspark[connect]==${PYSPARK_FOR_SAIL}" "setuptools<81" "pandas<2.2" "pyarrow<16" \
    grpcio grpcio-status protobuf
fi
echo "[sail] ready: $("$VENV/bin/python" -c 'import pysail,sys; print("pysail", pysail.__version__)' 2>/dev/null || echo 'pysail installed')  client=$VENV"

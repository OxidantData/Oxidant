#!/usr/bin/env bash
# Apache Spark 3.5.x with the Spark Connect server + a matching PySpark client venv.
# Spark 3.5 is the lingua franca here: it has a stable Spark Connect server AND is the version
# Gluten/Velox targets, so vanilla-Spark and Spark+Gluten share one install for a fair A/B.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
SPARK_VERSION="${SPARK_VERSION:-3.5.3}"
HADOOP_PROFILE="${HADOOP_PROFILE:-hadoop3}"
SPARK_HOME="${SPARK_HOME:-$HOME/spark-${SPARK_VERSION}-bin-${HADOOP_PROFILE}}"
ARCHIVE="spark-${SPARK_VERSION}-bin-${HADOOP_PROFILE}.tgz"

if [ ! -d "$SPARK_HOME" ]; then
  echo "[spark] downloading $ARCHIVE …"
  curl -fSL -o "/tmp/$ARCHIVE" \
    "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${ARCHIVE}"
  tar -xzf "/tmp/$ARCHIVE" -C "$HOME"
fi
echo "SPARK_HOME=$SPARK_HOME"

# PySpark client venv (matches the 3.5 server protocol).
VENV="$HERE/.venv-spark"
if [ ! -d "$VENV" ]; then
  python3 -m venv "$VENV"
  "$VENV/bin/pip" install --quiet --upgrade pip
  # setuptools<81 restores the `distutils` shim PySpark 3.5 imports (removed from Python 3.12,
  # the default on Ubuntu 24.04); pandas<2.2 keeps the Arrow conversion path compatible. pyarrow
  # is left at the Spark-3.5-era (<16) on purpose — Oxidant's server now materializes StringView, so
  # an older-Arrow client (what real Spark ships) must work too.
  "$VENV/bin/pip" install --quiet \
    "pyspark[connect]==${SPARK_VERSION}" "setuptools<81" "pandas<2.2" "pyarrow<16" \
    grpcio grpcio-status protobuf
fi
echo "[spark] ready: server=$SPARK_HOME  client=$VENV"

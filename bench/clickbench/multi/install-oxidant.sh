#!/usr/bin/env bash
# Oxidant — build the `oxidant` binary (oxidant-cli) in release mode; it serves Spark Connect via
# `oxidant spark server --port 50051 --foreground` (run-engine.sh is the supervisor). A stock
# PySpark client drives it like any other engine.
# oxidant-proto targets Spark 4.x, so the client venv pins PySpark 4.0 to match the protocol.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
REPO="$(cd "$HERE/../../.." && pwd)"
PYSPARK_FOR_OXIDANT="${PYSPARK_FOR_OXIDANT:-3.5.3}"   # 3.5 client works for the basic Connect ops; override to 4.0.0 if needed

# shellcheck disable=SC1091
[ -f "$HOME/.cargo/env" ] && source "$HOME/.cargo/env"
echo "[oxidant] building release binary (this takes a while) …"
( cd "$REPO" && cargo build --release -p oxidant-cli )
echo "OXIDANT_BIN=$REPO/target/release/oxidant"

VENV="$HERE/.venv-oxidant"
if [ ! -d "$VENV" ]; then
  python3 -m venv "$VENV"
  "$VENV/bin/pip" install --quiet --upgrade pip
  "$VENV/bin/pip" install --quiet \
    "pyspark[connect]==${PYSPARK_FOR_OXIDANT}" "setuptools<81" "pandas<2.2" "pyarrow<16" \
    grpcio grpcio-status protobuf
fi
echo "[oxidant] ready: bin=$REPO/target/release/oxidant  client=$VENV"

#!/usr/bin/env bash
# One-shot bootstrap for a fresh Ubuntu 24.04 c6a.4xlarge: system deps + JDK + Rust, then each
# engine's installer. Idempotent-ish (safe to re-run). Run from the repo root:
#   bash bench/clickbench/multi/bootstrap.sh
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"

echo "== apt deps =="
sudo apt-get update -y
sudo apt-get install -y --no-install-recommends \
  openjdk-17-jdk-headless python3 python3-venv python3-pip \
  build-essential cmake pkg-config libssl-dev curl unzip ca-certificates git

# Rust 1.90+ for building oxidant.
if ! command -v cargo >/dev/null 2>&1; then
  echo "== rustup (1.90) =="
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain 1.90.0
fi
# shellcheck disable=SC1091
[ -f "$HOME/.cargo/env" ] && source "$HOME/.cargo/env"

echo "== per-engine installers =="
bash "$HERE/install-spark.sh"
bash "$HERE/install-gluten.sh"   || echo "[bootstrap] gluten install failed — will record as pending"
bash "$HERE/install-sail.sh"     || echo "[bootstrap] sail install failed — will record as pending"
bash "$HERE/install-oxidant.sh"

echo "== bootstrap done. Next: bash bench/clickbench/multi/run-all.sh =="

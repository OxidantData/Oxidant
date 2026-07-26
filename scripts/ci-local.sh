#!/usr/bin/env bash
# Run the same Rust CI gates as .github/workflows/ci.yml (subset that fits a dev laptop).
set -euo pipefail

cd "$(dirname "$0")/.."

echo "==> rustfmt"
cargo fmt --all -- --check

echo "==> clippy"
cargo clippy --workspace --all-targets -- -D warnings

echo "==> build weft CLI (required by weft-cli integration tests)"
cargo build -p weft-cli

echo "==> local-cluster CLI parse smoke"
cargo test -p weft-cli local_cluster_mode_parses --bin weft

echo "==> test"
cargo test --workspace

echo "==> tpch"
cargo run -p weft-bench -- tpch

echo "==> tpch-distributed"
WEFT_TPCH_DIST_REQUIRE_ALL=1 cargo run -p weft-bench -- tpch-distributed --sf 0.01 --workers 2

echo "==> tpcds (requires duckdb on PATH)"
if ! command -v duckdb >/dev/null 2>&1; then
  echo "duckdb CLI not found — install from https://duckdb.org/docs/installation/ (needed for dsdgen)" >&2
  exit 1
fi
cargo run -p weft-bench -- tpcds --sf 0.01

echo "==> clickbench (engine-direct)"
cargo run -p weft-bench -- clickbench --rows 20000

echo "==> clickbench-grpc"
cargo run -p weft-bench -- clickbench-grpc --rows 20000

echo "==> correctness"
cargo run -p weft-bench -- correctness --rows 5000

echo "==> correctness-distributed"
cargo run -p weft-bench -- correctness-distributed --rows 2000

echo "==> parity ratchet"
cargo build -p weft-spark-compat --bin weft-parity
./target/debug/weft-parity ratchet --baseline parity/baseline.json --out-dir parity

echo "All local CI gates passed."

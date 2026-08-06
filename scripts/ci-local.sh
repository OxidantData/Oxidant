#!/usr/bin/env bash
# Run the same Rust CI gates as .github/workflows/ci.yml (subset that fits a dev laptop).
set -euo pipefail

cd "$(dirname "$0")/.."

echo "==> rustfmt"
cargo fmt --all -- --check

echo "==> clippy"
cargo clippy --workspace --all-targets -- -D warnings

echo "==> build oxidant CLI (required by oxidant-cli integration tests)"
cargo build -p oxidant-cli

echo "==> local-cluster CLI parse smoke"
cargo test -p oxidant-cli local_cluster_mode_parses --bin oxidant

echo "==> test"
cargo test --workspace

echo "==> tpch"
cargo run -p oxidant-bench -- tpch

echo "==> tpch-distributed"
OXIDANT_TPCH_DIST_REQUIRE_ALL=1 cargo run -p oxidant-bench -- tpch-distributed --sf 0.01 --workers 2

echo "==> tpcds (requires duckdb on PATH)"
if ! command -v duckdb >/dev/null 2>&1; then
  echo "duckdb CLI not found — install from https://duckdb.org/docs/installation/ (needed for dsdgen)" >&2
  exit 1
fi
cargo run -p oxidant-bench -- tpcds --sf 0.01

echo "==> tpcds-distributed planner ratchet"
cargo run -p oxidant-bench -- tpcds-distributed --sf 0.01

echo "==> tpcds-distributed execute correctness ratchet"
cargo run -p oxidant-bench -- tpcds-distributed --execute --sf 0.01 --workers 2

echo "==> clickbench (engine-direct)"
cargo run -p oxidant-bench -- clickbench --rows 20000

echo "==> clickbench-grpc"
cargo run -p oxidant-bench -- clickbench-grpc --rows 20000

echo "==> correctness"
cargo run -p oxidant-bench -- correctness --rows 5000

echo "==> correctness-distributed"
OXIDANT_DIST_TPCDS_SAMPLE=8 cargo run -p oxidant-bench -- correctness-distributed --rows 2000

echo "==> parity ratchet"
cargo build -p oxidant-spark-compat --bin oxidant-parity
./target/debug/oxidant-parity ratchet --baseline parity/baseline.json --out-dir parity

echo "All local CI gates passed."

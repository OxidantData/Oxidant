#!/usr/bin/env bash
# Run the same Rust CI gates as .github/workflows/ci.yml (subset that fits a dev laptop).
set -euo pipefail

cd "$(dirname "$0")/.."

echo "==> ASG membership bootstrap tests"
bash deploy/packer/tests/test_asg_membership.sh

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

echo "==> tpch (official dbgen SF1)"
export OXIDANT_TPC_KITS="${OXIDANT_TPC_KITS:-${HOME}/.cache/oxidant/tpc-kits}"
export OXIDANT_TPC_DATA="${OXIDANT_TPC_DATA:-${HOME}/.cache/oxidant/tpc-data}"
mkdir -p "$OXIDANT_TPC_DATA"
if [[ ! -x "${OXIDANT_TPC_KITS}/tpch-kit/dbgen/dbgen" ]]; then
  DATA_ROOT="$(dirname "$OXIDANT_TPC_KITS")" KITS_DIR="$OXIDANT_TPC_KITS" ./bench/tpc/fetch-kits.sh
  DATA_ROOT="$(dirname "$OXIDANT_TPC_KITS")" KITS_DIR="$OXIDANT_TPC_KITS" ./bench/tpc/build-kits.sh
fi
cargo run -p oxidant-bench -- tpch --sf 1 --data "${OXIDANT_TPC_DATA}/tpch-sf1"

echo "==> tpch-distributed"
OXIDANT_TPCH_DIST_REQUIRE_ALL=1 cargo run -p oxidant-bench -- tpch-distributed --sf 1 --workers 2 \
  --data "${OXIDANT_TPC_DATA}/tpch-sf1"

echo "==> tpcds (official dsdgen SCALE=1; DuckDB optional oracle)"
if ! command -v duckdb >/dev/null 2>&1; then
  echo "duckdb CLI not found — TPC-DS will run execute-only (OXIDANT_TPCDS_ALLOW_NO_ORACLE=1)"
  export OXIDANT_TPCDS_ALLOW_NO_ORACLE=1
fi
cargo run -p oxidant-bench -- tpcds --sf 1 --data "${OXIDANT_TPC_DATA}/tpcds-sf1"

echo "==> tpcds-distributed planner ratchet"
cargo run -p oxidant-bench -- tpcds-distributed --sf 1 --data "${OXIDANT_TPC_DATA}/tpcds-sf1"

echo "==> tpcds-distributed execute correctness ratchet"
cargo run -p oxidant-bench -- tpcds-distributed --execute --sf 1 --workers 2 \
  --data "${OXIDANT_TPC_DATA}/tpcds-sf1"

echo "==> clickbench (engine-direct)"
cargo run -p oxidant-bench -- clickbench --rows 20000

echo "==> clickbench-grpc"
cargo run -p oxidant-bench -- clickbench-grpc --rows 20000

echo "==> correctness"
cargo run -p oxidant-bench -- correctness --rows 5000

echo "==> correctness-distributed"
OXIDANT_DIST_TPCDS_SAMPLE=8 cargo run -p oxidant-bench -- correctness-distributed --rows 2000 --sf 1 \
  --data "${OXIDANT_TPC_DATA}/tpcds-sf1"

echo "==> parity ratchet"
cargo build -p oxidant-spark-compat --bin oxidant-parity
./target/debug/oxidant-parity ratchet --baseline parity/baseline.json --out-dir parity

echo "All local CI gates passed."

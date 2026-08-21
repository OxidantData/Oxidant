#!/usr/bin/env bash
# Run the 43 ClickBench queries through Oxidant and diff each result against a DuckDB oracle
# over the same hits.parquet. Correctness gate (separate from the timing gate).
set -euo pipefail
cd "$(dirname "$0")"
echo "TODO(issue #2/#3): for each query, compare Oxidant (sc://) output to" >&2
echo "  duckdb -c \"<query>\" over hits.parquet, asserting equal result sets." >&2
exit 0

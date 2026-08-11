# TPC-H harness

Runs the full TPC-H suite (Q1–Q22) through Oxidant for correctness and timing.

## Published scales (official TPC `dbgen` → Snappy Parquet)

| SF | Approx. Parquet | Prepare |
|---:|----------------:|---------|
| 1 | ~500 MiB | `SF=1 ./bench/tpch/prepare.sh` |
| 100 | ~10 GiB | `SF=100 ./bench/tpch/prepare.sh` (default) |
| 300 | ~31 GiB | `SF=300 ./bench/tpch/prepare.sh` |
| 1000 | ~130 GiB | `SF=1000 ./bench/tpch/prepare.sh` |

See [`../tpc/README.md`](../tpc/README.md) for kits, Iceberg/Glue registration, and size checks.
Queries under `queries/` are regenerated from official TPC `qgen` via
`./bench/tpc/generate-queries.sh` — do not hand-edit.
Do **not** use DuckDB blobs for publishable numbers.

```bash
DATA_ROOT=/data SF=100 ./bench/tpch/prepare.sh
cargo run -p oxidant-bench --release -- tpch-bench --sf 100 \
  --data /data/tpch-sf100/parquet --no-duckdb

# After register-iceberg-glue.sh:
cargo run -p oxidant-bench --release -- tpch-bench --sf 100 \
  --glue-database tpch_sf100 --no-duckdb
```

## CI / local smoke

Date predicates use official-style SQL-92 arithmetic. Fixed substitution parameters match
historical cutoffs so row counts stay stable.

- Single-node: `cargo run -p oxidant-bench -- tpch --sf 1`
- Distributed gate: `cargo run -p oxidant-bench -- tpch-distributed --sf 1 --workers 2`
  (CI sets `OXIDANT_TPCH_DIST_REQUIRE_ALL=1` → 22/22 distributed-ok)
- `run-correctness.sh` — optional Spark/DuckDB oracle diff (when wired).

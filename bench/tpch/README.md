# TPC-H harness

Runs the full TPC-H suite (Q1–Q22) through Oxidant for correctness and timing.

Date predicates use official-style SQL-92 arithmetic — typed `date '…'` literals and
`interval 'N' {day|month|year}` (including ANSI leading precision on Q1:
`interval '90' day (3)`). The engine strips unsupported leading precision before planning.
Fixed substitution parameters match the historical CAST cutoffs so row counts stay stable.

- Single-node: `cargo run -p oxidant-bench -- tpch --sf 0.01`
- Distributed gate: `cargo run -p oxidant-bench -- tpch-distributed --sf 0.01 --workers 2`
  (CI sets `OXIDANT_TPCH_DIST_REQUIRE_ALL=1` → 22/22 distributed-ok)
- `run-correctness.sh` — optional Spark/DuckDB oracle diff (when wired).

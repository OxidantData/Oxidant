# TPC-DS harness

Runs the full TPC-DS suite (Q1–Q99) through Oxidant for correctness and timing.

## Published scales (official TPC `dsdgen` → Snappy Parquet)

| SF | Approx. Parquet | Prepare |
|---:|----------------:|---------|
| 1 | ~500 MiB | `SF=1 ./bench/tpcds/prepare.sh` |
| 100 | ~10 GiB | `SF=100 ./bench/tpcds/prepare.sh` (default) |
| 300 | ~31 GiB | `SF=300 ./bench/tpcds/prepare.sh` |
| 1000 | ~130 GiB | `SF=1000 ./bench/tpcds/prepare.sh` |

Pipeline + Glue Iceberg: [`../tpc/README.md`](../tpc/README.md). Publishable datasets must
come from official `dsdgen`, not DuckDB. Queries under `queries/` are regenerated from
official TPC `dsqgen -QUALIFY Y` via `./bench/tpc/generate-queries.sh`.

```bash
# Kits once
DATA_ROOT=$HOME/.cache/oxidant KITS_DIR=$HOME/.cache/oxidant/tpc-kits \
  ./bench/tpc/fetch-kits.sh && ./bench/tpc/build-kits.sh
export OXIDANT_TPC_KITS=$HOME/.cache/oxidant/tpc-kits

# CI / local (integer SCALE only — official dsdgen rejects fractional SF)
cargo run -p oxidant-bench -- tpcds --sf 1

DATA_ROOT=/data SF=100 ./bench/tpcds/prepare.sh
SF=100 SUITE=tpcds BUCKET=oxidant-artifacts-… ./bench/tpc/register-iceberg-glue.sh
```

## Ratchet

If a PR improves the pass set, re-run the suite and copy the printed `passed_json=…` list into
`baseline.json` (keep numeric order). The gate fails if any previously green query regresses.

## Notes

- Engineering harness (timing + optional DuckDB oracle), not an audited TPC Fair Use publication.
- Distributed TPC-DS: see `tpcds-distributed` planner/execute ratchets.

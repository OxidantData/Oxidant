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

## Connect qualification and resume identity

Both Connect callers use the shared relation-position qualifier and record
input (`sql_sha256` / `original_sha256`) and outgoing (`transformed_sha256`)
SQL hashes. The bare interval rewrite remains TPC-DS-only. Resume checks the
outgoing hash as well as input/config identity; the source digest binds both
loaded shared helpers and the caller. A helper change invalidates the complete
prior envelope rather than relabelling unread rows with new provenance.
See the [shared contract and migration limits](../tpch/README.md#connect-qualification-and-resume-identity),
including the same-source, no-op-only compatibility projection. This does not
establish an immutable engine/dataset manifest or full SQL correctness.

## Connect checkpoints and exit status

For `run-ec2-connect.py`, each checkpoint merges the selected query results with
prior rows whose run identity matches. The JSON `failures` count covers **all rows
in that merged artifact**, including unread missing/failed queries outside
`--start`/`--end`. A row with an error or neither an elapsed nor hot timing counts
as one failure; exhausted reconnect attempts count even without an error string.
A successful rerun replaces the failed row rather than keeping its failure count.

The runner returns exit status 1 while the merged artifact contains failures, even
when every selected query succeeds or is skipped. The terminal summary labels
this count `artifact_failures`; `selected_elapsed_total` covers only the selected
range, while JSON `elapsed_total_s` covers the merged rows. Zero failures does not
establish coverage of queries absent from the artifact.

Offline checkpoint regressions (in-process Spark fixture, not performance data):

```bash
python3 -B -m unittest bench.tests.test_resume_identity -v
```

## Ratchet

If a PR improves the pass set, re-run the suite and copy the printed `passed_json=…` list into
`baseline.json` (keep numeric order). The gate fails if any previously green query regresses.

## Notes

- Engineering harness (timing + optional DuckDB oracle), not an audited TPC Fair Use publication.
- Distributed TPC-DS: see `tpcds-distributed` planner/execute ratchets.

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

## Connect qualification and resume identity

`run-ec2-connect.py` shares the relation-position qualifier with TPC-DS. It
preserves the Q16 `'%Customer%Complaints%'` predicate, literal/comment text,
and the existing corpus alias behavior. TPC-DS alone applies the separate bare
interval transformation. This bounded lexer is not a general SQL resolver:
CTE shadowing, additional quote forms and full SQL result equivalence remain
outside this slice's acceptance.

Executed rows record `sql_sha256` and `original_sha256` for the input SQL, plus
`transformed_sha256` for the SQL sent to Spark. Reuse requires matching input
and outgoing SQL hashes, a successful timing, and no error. The run envelope
must match endpoint, Glue dataset label, machine, tries, and `source_sha`.
`source_sha` is a versioned digest of the caller and its loaded `qualify_sql.py`
and `resume_identity.py` files, so changing a shared helper invalidates the
whole prior envelope, including unread sibling rows. It is **not** an engine
build, dataset snapshot, client, topology or cache-state identity.

Compatibility is narrow: under the same current source/config envelope, a row
may omit the extra original/transformed fields only when the saved input hash
also equals the outgoing SQL hash (a byte-for-byte no-op transformation).
An explicit null or mismatched hash is not accepted. Reused rows are retained
unchanged, never assigned invented provenance. Q16 qualification and TPC-DS
interval rewrites require an explicit matching transformed hash.

Retired runner-only source digests and artifacts without the matching envelope
are not migrated or adopted. The existing behavior is to execute selected
queries anew and replace the working `--out` checkpoint, not to archive it or
hard-exit on incompatibility. Preserve historical JSON byte-for-byte elsewhere;
use a separate working output path (or copy) for a new run. Do not relabel old
entries with the new digest. SQL hashes alone are not correctness certificates.

The merged-artifact failure/partial-resume contract is currently TPC-DS-only
([details](../tpcds/README.md#connect-checkpoints-and-exit-status)). TPC-H's
existing missing-query exception, selected-result checkpoint and selected
failure summary are not expanded here. Neither runner's zero exit proves
full-suite completeness. Range validation, atomic/concurrent checkpoint writes,
full immutable manifests and live SQL acceptance are separate work.

Focused offline tests (controlled Spark boundary; synthetic checkpoint fixtures,
not engine execution or performance evidence):

```bash
python3 -B -m unittest bench.tests.test_qualify bench.tests.test_resume_identity bench.tests.test_connect_contract -v
```

## CI / local smoke

Date predicates use official-style SQL-92 arithmetic. Fixed substitution parameters match
historical cutoffs so row counts stay stable.

- Single-node: `cargo run -p oxidant-bench -- tpch --sf 1`
- Distributed gate: `cargo run -p oxidant-bench -- tpch-distributed --sf 1 --workers 2`
  (CI sets `OXIDANT_TPCH_DIST_REQUIRE_ALL=1` → 22/22 distributed-ok)
- `run-correctness.sh` — optional Spark/DuckDB oracle diff (when wired).

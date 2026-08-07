# sample-data/ — bundled TPC-H samples

TPC-H SF 0.01 in four physical formats, committed to the repo so the engine can serve a
preloaded `samples` schema with zero setup (see `--sample-data` in `docs/cli.md` and the
quickstart in `docs/getting-started.md`):

```text
sample-data/
  csv/tpch_<t>.csv          all 8 TPC-H tables, headered
  parquet/tpch_<t>.parquet  all 8 tables, snappy-compressed (the primary tables)
  delta/tpch_<t>/           nation, customer, orders, lineitem (_delta_log + one parquet part)
  iceberg/tpch_<t>/         nation, customer, orders, lineitem (metadata/ + data/)
```

With `oxidant spark server --sample-data sample-data` (or `OXIDANT_SAMPLE_DATA_DIR`), these
register under the built-in `spark_catalog` catalog as:

| SQL table | Source |
|---|---|
| `samples.tpch_<t>` (all 8) | `parquet/tpch_<t>.parquet` |
| `samples.tpch_<t>_csv` (all 8) | `csv/tpch_<t>.csv` |
| `samples.tpch_<t>_delta` (4 headline) | `delta/tpch_<t>/` |
| `samples.tpch_<t>_iceberg` (4 headline) | `iceberg/tpch_<t>/` |

Row counts (SF 0.01): nation 25, region 5, supplier 100, customer 1500, part 2000,
partsupp 8000, orders 15000, lineitem 60175.

## Regenerating

Everything is produced by one Rust binary — no Python/JVM toolchain:

```sh
cargo run -p oxidant-bench -- sample-data            # writes ./sample-data (idempotent)
cargo run -p oxidant-bench -- sample-data --data /tmp/sample-data   # other output dir
```

Each phase (csv → parquet → delta → iceberg) skips itself when its output already exists, so
a regeneration from scratch means deleting the tree first:

```sh
rm -rf sample-data && cargo run -p oxidant-bench -- sample-data
```

Output is deterministic (fixed file names, UUIDs and timestamps): a regeneration produces an
empty git diff.

## Why the lakehouse metadata is written by hand

The committed tree must be **relocatable**: it is read from arbitrary checkouts and from the
Docker image at `/opt/oxidant/sample-data`. Delta `add` actions and every Iceberg path (data
files, manifest, manifest list) are therefore **table-root-relative**, which the engine's
readers resolve against the table location. Writers such as pyiceberg or delta-spark bake
absolute `file://` URIs into Iceberg manifests, producing tables that only read back on the
machine that generated them — so the generator
(`crates/oxidant-bench/src/sample_data.rs`) writes the minimal spec-compliant Delta commit
and Iceberg v2 metadata directly (delta-rs commits test tables to git the same way).
`crates/oxidant-loom/tests/sample_data.rs` verifies every format reads back its SF 0.01 row
counts, so an incompatible generator change fails CI.

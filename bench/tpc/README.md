# Official TPC data pipeline (TPC-H / TPC-DS)

Builds publishable benchmark datasets with the **TPC.org toolkit** (`dbgen` / `dsdgen`),
not DuckDB. Output is **multi-file Snappy Parquet** (~128 MiB parts, Spark
`spark.sql.files.maxPartitionBytes` default), then optionally **Glue EXTERNAL Parquet**,
**Iceberg**, and **Delta**.

**Why multi-file:** Apache Spark can split a *single* Parquet file across tasks (row-group
ownership under `maxPartitionBytes`). Oxidant’s distributed scan shards by **file list**,
so one giant `store_sales.parquet` leaves all but one worker idle. `generate.sh` keeps
parallel `dbgen`/`dsdgen` parts by default (`KEEP_PARTS=1`); `tbl_to_parquet.py` also
rotates parts at `--target-part-bytes`.

Engineering / CI smokes (`cargo run -p oxidant-bench -- tpch --sf 1`) use the same official
kits at SCALE/SF **1** (~500 MiB Parquet). **Any larger published number** must come from this tree.

## Target on-disk sizes (Snappy Parquet, approx.)

| Scale factor | Approx. Parquet footprint | Typical use |
|-------------:|--------------------------:|-------------|
| **1** | **~500 MiB** | Laptop / smoke publish |
| **100** | **~10 GiB** | Canonical EC2 SF100 (KAN-14) |
| **300** | **~31 GiB** | Mid-scale stress |
| **1000** | **~130 GiB** | Large publish |

Sizes are compressed Parquet (not raw `.tbl`/`.dat`). Raw TPC text is roughly 5–10× larger.
`prepare.sh` prints a size report and warns if a scale lands outside a ±40% band of the
target (generators, codecs, and partitioning can move the number).

## Layout

```text
$DATA_ROOT/
  kits/                         # cloned/built TPC toolkits (gitignored locally)
  tpch-sf{N}/                   # flat → parquet staging
    raw/                        # dbgen *.tbl
    parquet/{nation,lineitem,…}/
  tpcds-sf{N}/
    raw/                        # dsdgen *.dat
    parquet/{store_sales,…}/
```

Glue registration (optional):

| Format | Script | Catalog DB | S3 prefix |
|--------|--------|------------|-----------|
| Parquet EXTERNAL | `register-parquet-glue.sh` | `tpch_sf{N}` / `tpcds_sf{N}` | `s3://$BUCKET/{suite}-sf{N}/` |
| Iceberg | `register-iceberg-glue.sh` | `{suite}_sf{N}_iceberg` | `s3://$BUCKET/{suite}-sf{N}-iceberg/` |
| Delta | `register-delta-glue.sh` | `{suite}_sf{N}_delta` | `s3://$BUCKET/{suite}-sf{N}-delta/` |

Wipe prior TPC test data: `BUCKET=… ./bench/tpc/delete-tpc-datasets.sh`

## Quick start

```bash
# 1) Kits (one-time). Prefer official zips from tpc.org after accepting the TPC license.
#    Default mirrors are community builds of those same tools (see fetch-kits.sh).
./bench/tpc/fetch-kits.sh
./bench/tpc/build-kits.sh

# 1b) Regenerate committed query text from official qgen / dsqgen (qualification binds)
./bench/tpc/generate-queries.sh

# 2) Generate + convert (SF100 ≈ 10 GiB Parquet; needs ~disk = 3–4× target while converting)
SF=100 SUITE=tpch DATA_ROOT=/data ./bench/tpc/prepare.sh
SF=100 SUITE=tpcds DATA_ROOT=/data ./bench/tpc/prepare.sh

# 3) Register on S3 + Glue (Parquet / Iceberg / Delta)
BUCKET=weft-artifacts-$(aws sts get-caller-identity --query Account --output text)
SF=10 SUITE=tpcds BUCKET=$BUCKET ./bench/tpc/register-parquet-glue.sh
SF=10 SUITE=tpcds BUCKET=$BUCKET SKIP_SYNC=1 ./bench/tpc/register-iceberg-glue.sh
SF=10 SUITE=tpcds BUCKET=$BUCKET ./bench/tpc/register-delta-glue.sh
```

## Queries

Harness SQL under `bench/tpch/queries/` and `bench/tpcds/queries/` is produced by:

| Suite | Tool | Flags |
|-------|------|-------|
| TPC-H | `qgen` | `-d` (default/qualification substitutions), `-s $SF` |
| TPC-DS | `dsqgen` | `-QUALIFY Y`, `-DIALECT oxidant` (`bench/tpc/dialects/oxidant.tpl`), `-SCALE $SF` |

TPC-H Q11 keeps `__OXIDANT_SF__` for the spec fraction `0.0001/SF` so one file works at every scale.
TPC-DS Q14/Q23/Q24/Q39 templates emit two statements; the harness keeps the first (engineering
parity with the prior single-statement files). Do not hand-edit the `.sql` files — re-run
`./bench/tpc/generate-queries.sh`.

Run the published harness against local Parquet:

```bash
cargo run -p oxidant-bench --release -- tpch-bench --sf 100 \
  --data /data/tpch-sf100/parquet --no-duckdb \
  --json bench/tpch/results/tpch-sf100.json
```

Or against Glue Iceberg (three-part names via local views):

```bash
export OXIDANT_CATALOG_CONF="spark.sql.catalog.glue.type=glue;spark.sql.catalog.glue.region=us-west-2;spark.sql.catalog.glue.warehouse=s3://${BUCKET}/warehouse"
cargo run -p oxidant-bench --release -- tpch-bench --sf 100 \
  --glue-database tpch_sf100 --no-duckdb \
  --json bench/tpch/results/tpch-sf100-glue.json
```

## Conversion completion and resume

`tbl_to_parquet.py` converts a table in staging before replacing its published
output. Both suite converters and `--check-complete` use the same table validator.
The version 2 `_oxidant_complete.json` binds source names/sizes/SHA-256 hashes to
an exact, flat set of `part-*.parquet` files. Each output record contains its name,
byte size, SHA-256 and Parquet row count; the marker also records the table row
total. Footer validation and content hashing happen before publication and on
reuse. A changed readable file, corrupt footer, missing/extra file, nested output
or symlink cannot qualify as that completed table.

A successful check leaves files untouched. A failed check exits nonzero; ordinary
conversion rebuilds the affected table from the selected raw inputs, preserving
other completed tables. Version 1 markers and missing/invalid markers are
unverified and must be rebuilt, not upgraded by assigning hashes to existing
parts. Keep any historical benchmark dataset/receipt separately before rebuilding
it; this command is not an archival migration. `--force` also rebuilds.

Checks read all source and output bytes (streamed hashing, not full tables in
memory), plus output footers. This adds linear disk I/O; no scale-performance
claim follows from the small regression fixtures. Run the local converter gates
with an existing PyArrow installation:

```sh
python3 -m unittest discover -s bench/tpc/tests -p 'test_*.py' -v
```

This is **table-output integrity**, not a full dataset or benchmark certificate.
The marker does not yet bind expected generator child indices, generator/scale,
converter/schema/settings identity or a dataset-wide committed generation.
TPC-DS `--only` validates only selected input tables, not the full suite; required
table coverage, concurrent publishers and interruption during directory promotion
remain separate acceptance work. Registration/other readers are not changed to
enforce this validator. A checksum detects drift relative to the emitted marker,
not malicious coordinated replacement of both metadata and data, or SQL result
correctness. Existing historical timings and parity baselines are unchanged.

## Kits / license

- Official downloads: [TPC current specifications](https://www.tpc.org/tpc_documents_current_versions/current_specifications.asp)
  (email gate; accept the TPC license).
- Default fetch uses public mirrors of those tools:
  - TPC-H: `https://github.com/gregrahn/tpch-kit`
  - TPC-DS: `https://github.com/databricks/tpcds-kit`
- Override with `TPCH_KIT_URL` / `TPCDS_KIT_URL`, or point `KITS_DIR` at an already-extracted
  official tree (`…/dbgen` and `…/tools` with `dbgen` / `dsdgen`).

This pipeline is for engineering and Fair Use–style disclosure — not a substitute for an
audited TPC publication.

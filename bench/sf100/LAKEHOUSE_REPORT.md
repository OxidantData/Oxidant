# D1 — SF100 Iceberg + Delta datagen (scripts only)

## What shipped

| Path | Role |
|------|------|
| `bench/sf100/build-lakehouse.py` | Lay Iceberg (`add_files`) + Delta (`convert_to_deltalake`) over existing Parquet; optional Glue register |
| `bench/sf100/register-glue.sh` | Extended with `FORMAT=parquet\|delta\|iceberg` |
| `bench/sf100/teardown-lakehouse.sh` | Drop Glue DBs; optional `DELETE_DATA=1` for `_delta_log` + Iceberg warehouse |
| `bench/sf100/rehearse-local.sh` | Local SF0.01-style smoke (no AWS); skips if deps missing |
| `bench/sf100/requirements.txt` | Pinned PyPI versions |
| `bench/sf100/README.md` | Runbook, Glue DB convention, cost/teardown |

## API verification (not assumed)

| Library | Version | API checked in a real venv |
|---------|---------|----------------------------|
| `pyiceberg` | **0.11.1** | `Table.add_files(file_paths, check_duplicate_files=True)` present; works with `schema.name-mapping.default` for Parquet without field-ids; external file URIs accepted |
| `deltalake` | **1.6.2** | `convert_to_deltalake(uri, mode='ignore')` present; writes only `_delta_log/` |
| `SQLAlchemy` | 2.0.51 | Required by PyIceberg `SqlCatalog` for local rehearsal |
| `boto3` | 1.43.46 | Pinned under `aiobotocore`/`s3fs` botocore upper bound |

Assumed (not executed against AWS): GlueCatalog + S3 `FileIO` behaviour for the operator SF100 run; dry-run covers the control flow without credentials.

## Glue parameters ↔ A1 `detect_format`

Detector order (branch `vamzi/lakehouse-s3-formats`): `table_type=ICEBERG` → `spark.sql.sources.provider` / `provider` → `classification` → Parquet.

| Format | Glue DB | Parameters |
|--------|---------|------------|
| Parquet | `{suite}_sf{SF}` | `classification=parquet` |
| Iceberg | `{suite}_sf{SF}_iceberg` | `table_type=ICEBERG`, `metadata_location=<current metadata json>` (+ `classification=parquet` unused by detector) |
| Delta | `{suite}_sf{SF}_delta` | `classification=delta`, `provider=delta`, `spark.sql.sources.provider=delta` |

Same S3 Parquet prefix for Parquet + Delta; Iceberg warehouse is `{source}-iceberg` (metadata + manifests; data files referenced via `add_files`).

## Local rehearsal

**Ran and passed** (`./bench/sf100/rehearse-local.sh`):

- nation: parquet=3, iceberg=3, delta=3
- region: parquet=2, iceberg=2, delta=2

No AWS commands were executed in this worker session.

## Follow-up: `_delta_log` checkpoint contamination

**Finding (rehearsal):** After planting
`_delta_log/00000000000000000010.checkpoint.parquet` with a foreign schema, a naive
`.parquet` extension listing **is contaminated** — `pyarrow` fails with
`ArrowInvalid: Schema at index 1 was different` (checkpoint schema vs table schema).
Excluding `_delta_log/` restores the original row count (**MITIGATION OK**).

Fresh `convert_to_deltalake` (commit 0, JSON only) does **not** contaminate yet.
The hazard appears once a checkpoint exists.

Documented in `README.md`. Engine-side exclude of `_delta_log` is owned by the
`catalog_bridge` worker; this rehearsal is the proof.
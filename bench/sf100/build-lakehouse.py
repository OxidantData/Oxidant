#!/usr/bin/env python3
"""Lay Iceberg + Delta metadata over an existing SF100 Parquet dump (shared bytes).

Design
------
`dump-to-s3.sh` already writes one Parquet copy per table under
``s3://$BUCKET/{suite}-sf{SF}/{table}/``. This script does **not** rewrite those
files. Instead:

* **Delta** — ``deltalake.convert_to_deltalake`` writes only ``_delta_log/`` into
  the same table prefix (mode=``ignore`` when already a Delta table).
* **Iceberg** — PyIceberg ``Table.add_files`` registers the existing Parquet
  object URIs into a new Iceberg table whose *warehouse* holds metadata only.
  Verified against ``pyiceberg==0.11.1`` (``Table.add_files`` exists) and
  ``deltalake==1.6.2`` (``convert_to_deltalake(..., mode='ignore')`` exists).

Glue databases (coexist; harness picks one)::

    {suite}_sf{SF}          parquet   (existing register-glue.sh)
    {suite}_sf{SF}_iceberg  iceberg   table_type=ICEBERG + metadata_location
    {suite}_sf{SF}_delta    delta     classification=delta + provider=delta

Weft's Glue detector (branch ``vamzi/lakehouse-s3-formats``) precedence:
``table_type=ICEBERG`` → ``spark.sql.sources.provider`` / ``provider`` →
``classification`` → Parquet. Parameters below are chosen for that order.

No AWS calls are made unless you pass a real ``s3://`` prefix *and* omit
``--dry-run``. Local ``file://`` / plain paths are for the SF0.01 rehearsal.

Examples::

  # Dry-run against an existing dump prefix (touches nothing)
  python3 bench/sf100/build-lakehouse.py \\
    --suite tpcds --sf 100 \\
    --source-prefix s3://weft-artifacts-ACCOUNT/tpcds-sf100 \\
    --formats iceberg,delta --dry-run

  # Local rehearsal (see rehearse-local.sh)
  python3 bench/sf100/build-lakehouse.py \\
    --suite tpch --sf 0.01 \\
    --source-prefix /tmp/sf001/tpch-sf0.01 \\
    --iceberg-warehouse /tmp/sf001/tpch-sf0.01-iceberg \\
    --formats iceberg,delta --skip-glue
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import traceback
from dataclasses import dataclass, field
from pathlib import Path
from typing import Sequence
from urllib.parse import urlparse

TPCH_TABLES = [
    "nation",
    "region",
    "supplier",
    "customer",
    "part",
    "partsupp",
    "orders",
    "lineitem",
]
TPCDS_TABLES = [
    "call_center",
    "catalog_page",
    "catalog_returns",
    "catalog_sales",
    "customer",
    "customer_address",
    "customer_demographics",
    "date_dim",
    "household_demographics",
    "income_band",
    "inventory",
    "item",
    "promotion",
    "reason",
    "ship_mode",
    "store",
    "store_returns",
    "store_sales",
    "time_dim",
    "warehouse",
    "web_page",
    "web_returns",
    "web_sales",
    "web_site",
]


@dataclass
class ActionResult:
    table: str
    format: str
    status: str  # ok | skip | dry-run | error
    detail: str = ""


@dataclass
class Summary:
    results: list[ActionResult] = field(default_factory=list)

    def add(self, r: ActionResult) -> None:
        self.results.append(r)
        tag = {"ok": "OK", "skip": "SKIP", "dry-run": "DRY", "error": "ERR"}.get(
            r.status, r.status.upper()
        )
        print(f"[{tag}] {r.format:7} {r.table}: {r.detail}")

    def print_final(self) -> int:
        counts: dict[str, int] = {}
        for r in self.results:
            counts[r.status] = counts.get(r.status, 0) + 1
        print("\n=== summary ===")
        for k in sorted(counts):
            print(f"  {k}: {counts[k]}")
        return 1 if counts.get("error") else 0


def tables_for(suite: str) -> list[str]:
    if suite == "tpch":
        return list(TPCH_TABLES)
    if suite == "tpcds":
        return list(TPCDS_TABLES)
    raise SystemExit(f"unknown suite {suite!r} (want tpch|tpcds)")


def parse_formats(s: str) -> list[str]:
    out = []
    for part in s.split(","):
        p = part.strip().lower()
        if not p:
            continue
        if p not in ("parquet", "iceberg", "delta"):
            raise SystemExit(f"unknown format {p!r} (want parquet|iceberg|delta)")
        if p not in out:
            out.append(p)
    if not out:
        raise SystemExit("empty --formats")
    return out


def join_uri(prefix: str, *parts: str) -> str:
    prefix = prefix.rstrip("/")
    for p in parts:
        prefix = f"{prefix}/{p.strip('/')}"
    return prefix


def is_s3(uri: str) -> bool:
    return uri.startswith("s3://")


def to_fs_path(uri: str) -> Path:
    """Local path from file:// URI or bare path."""
    if uri.startswith("file://"):
        return Path(urlparse(uri).path)
    if is_s3(uri):
        raise ValueError(f"not a local path: {uri}")
    return Path(uri)


def list_parquet_files(table_uri: str, *, include_delta_log: bool = False) -> list[str]:
    """Return full URIs/paths of *.parquet under table_uri.

    By default skips ``_delta_log/`` and Iceberg ``metadata/`` (data files only).
    Pass ``include_delta_log=True`` to mimic a naive engine listing that only filters
    on the ``.parquet`` extension (Weft ``ListingOptions.with_file_extension``).
    """
    if is_s3(table_uri):
        import s3fs  # noqa: WPS433 — optional until S3 path used

        fs = s3fs.S3FileSystem(anon=False)
        # s3fs wants bucket/key without scheme for find
        bare = table_uri[len("s3://") :].rstrip("/")
        paths = []
        for p in fs.find(bare):
            if not p.endswith(".parquet"):
                continue
            if "/metadata/" in p:
                continue
            if not include_delta_log and "/_delta_log/" in p:
                continue
            paths.append(f"s3://{p}")
        return sorted(paths)

    root = to_fs_path(table_uri)
    if not root.exists():
        return []
    if root.is_file() and root.suffix == ".parquet":
        return [str(root.resolve())]
    files = []
    for p in sorted(root.rglob("*.parquet")):
        s = str(p.resolve())
        if "/metadata/" in s:
            continue
        if not include_delta_log and "/_delta_log/" in s:
            continue
        files.append(s)
    return files


def arrow_type_to_iceberg(field_name: str, field_id: int, arrow_type):
    """Map a PyArrow type to an optional Iceberg NestedField (TPC dumps are nullable)."""
    import pyarrow as pa
    from pyiceberg.types import (
        BooleanType,
        DateType,
        DoubleType,
        FloatType,
        IntegerType,
        LongType,
        NestedField,
        StringType,
        TimestampType,
        TimestamptzType,
        DecimalType,
        BinaryType,
    )

    t = arrow_type
    if pa.types.is_boolean(t):
        ice = BooleanType()
    elif pa.types.is_int8(t) or pa.types.is_int16(t) or pa.types.is_int32(t):
        ice = IntegerType()
    elif pa.types.is_int64(t):
        ice = LongType()
    elif pa.types.is_float32(t):
        ice = FloatType()
    elif pa.types.is_float64(t):
        ice = DoubleType()
    elif pa.types.is_string(t) or pa.types.is_large_string(t):
        ice = StringType()
    elif pa.types.is_binary(t) or pa.types.is_large_binary(t):
        ice = BinaryType()
    elif pa.types.is_date(t):
        ice = DateType()
    elif pa.types.is_timestamp(t):
        ice = TimestamptzType() if t.tz is not None else TimestampType()
    elif pa.types.is_decimal(t):
        ice = DecimalType(t.precision, t.scale)
    else:
        # Fallback: stringify — still readable for bench smoke.
        ice = StringType()
    return NestedField(field_id, field_name, ice, required=False)


def iceberg_schema_from_parquet(parquet_uri: str):
    """Build Iceberg Schema + name-mapping JSON from a Parquet footer (no field-ids)."""
    import pyarrow.parquet as pq
    from pyiceberg.schema import Schema

    if is_s3(parquet_uri):
        import s3fs

        fs = s3fs.S3FileSystem(anon=False)
        with fs.open(parquet_uri, "rb") as f:
            schema = pq.read_schema(f)
    else:
        schema = pq.read_schema(parquet_uri)

    fields = []
    mapping = []
    for i, field in enumerate(schema, start=1):
        fields.append(arrow_type_to_iceberg(field.name, i, field.type))
        mapping.append({"field-id": i, "names": [field.name]})
    return Schema(*fields), json.dumps(mapping, separators=(",", ":"))


def ensure_namespace(catalog, name: str, dry_run: bool) -> None:
    try:
        catalog.create_namespace(name)
    except Exception as e:  # noqa: BLE001 — catalog-specific "already exists"
        if "already" in str(e).lower() or "exists" in str(e).lower():
            return
        # Some catalogs raise NamespaceAlreadyExistsError
        if e.__class__.__name__.endswith("AlreadyExistsError"):
            return
        if dry_run:
            return
        raise


def build_delta(
    table: str,
    table_uri: str,
    dry_run: bool,
    summary: Summary,
) -> None:
    files = list_parquet_files(table_uri)
    if not files:
        summary.add(ActionResult(table, "delta", "skip", f"no parquet under {table_uri}"))
        return
    if dry_run:
        summary.add(
            ActionResult(
                table,
                "delta",
                "dry-run",
                f"convert_to_deltalake({table_uri!r}, mode='ignore')  # {len(files)} parquet files",
            )
        )
        return
    from deltalake import convert_to_deltalake

    storage_options = None
    if is_s3(table_uri):
        # Pick up standard AWS env; deltalake uses object_store underneath.
        storage_options = {
            k: v
            for k, v in {
                "AWS_REGION": os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION"),
                "AWS_DEFAULT_REGION": os.environ.get("AWS_DEFAULT_REGION")
                or os.environ.get("AWS_REGION"),
            }.items()
            if v
        } or None
    convert_to_deltalake(table_uri, mode="ignore", storage_options=storage_options)
    summary.add(
        ActionResult(table, "delta", "ok", f"_delta_log at {table_uri} ({len(files)} files)")
    )


def build_iceberg(
    table: str,
    table_uri: str,
    warehouse: str,
    catalog,
    namespace: str,
    dry_run: bool,
    summary: Summary,
) -> str | None:
    """Create/load Iceberg table and add_files. Returns metadata_location or None."""
    files = list_parquet_files(table_uri)
    if not files:
        summary.add(ActionResult(table, "iceberg", "skip", f"no parquet under {table_uri}"))
        return None
    ident = f"{namespace}.{table}"
    if dry_run:
        summary.add(
            ActionResult(
                table,
                "iceberg",
                "dry-run",
                f"create {ident} in warehouse={warehouse}; add_files({len(files)} uris from {table_uri})",
            )
        )
        return None

    schema, name_mapping = iceberg_schema_from_parquet(files[0])
    ensure_namespace(catalog, namespace, dry_run=False)

    from pyiceberg.exceptions import TableAlreadyExistsError

    try:
        tbl = catalog.create_table(ident, schema=schema)
        with tbl.transaction() as tx:
            tx.set_properties({"schema.name-mapping.default": name_mapping})
        tbl = catalog.load_table(ident)
    except TableAlreadyExistsError:
        tbl = catalog.load_table(ident)
    except Exception as e:
        # Some catalog backends raise generic errors
        if "already" in str(e).lower():
            tbl = catalog.load_table(ident)
        else:
            raise

    # Relink name mapping if missing (resumable runs on half-created tables).
    if "schema.name-mapping.default" not in tbl.properties:
        with tbl.transaction() as tx:
            tx.set_properties({"schema.name-mapping.default": name_mapping})
        tbl = catalog.load_table(ident)

    tbl.add_files(files, check_duplicate_files=True)
    tbl = catalog.load_table(ident)
    md = tbl.metadata_location
    summary.add(
        ActionResult(
            table,
            "iceberg",
            "ok",
            f"{ident} metadata_location={md} files={len(files)}",
        )
    )
    return md


def glue_db_name(suite: str, sf: str, fmt: str) -> str:
    # SF may be "100" or "0.01" — Glue names need [a-z0-9_].
    sf_token = str(sf).replace(".", "_")
    base = f"{suite}_sf{sf_token}"
    if fmt == "parquet":
        return base
    return f"{base}_{fmt}"


def register_glue_parquet_style(
    *,
    region: str,
    database: str,
    table: str,
    location: str,
    fmt: str,
    metadata_location: str | None,
    dry_run: bool,
    summary: Summary,
) -> None:
    """Create/replace a Glue EXTERNAL_TABLE with parameters Weft detect_format understands."""
    location = location if location.endswith("/") else location + "/"
    if fmt == "iceberg":
        if not metadata_location:
            summary.add(
                ActionResult(table, "glue", "skip", "iceberg register needs metadata_location")
            )
            return
        params = {
            "table_type": "ICEBERG",
            "metadata_location": metadata_location,
            # classification kept for Athena-ish UIs; detector prefers table_type.
            "classification": "parquet",
            "EXTERNAL": "TRUE",
        }
        # Iceberg-on-Glue StorageDescriptor is often a stub; location = table root.
        input_format = "org.apache.iceberg.mr.hive.HiveIcebergInputFormat"
        output_format = "org.apache.iceberg.mr.hive.HiveIcebergOutputFormat"
        serde = "org.apache.iceberg.mr.hive.HiveIcebergSerDe"
    elif fmt == "delta":
        params = {
            "classification": "delta",
            "provider": "delta",
            "spark.sql.sources.provider": "delta",
            "EXTERNAL": "TRUE",
        }
        input_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
        output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
        serde = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    else:
        params = {"classification": "parquet", "EXTERNAL": "TRUE"}
        input_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
        output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
        serde = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"

    table_input = {
        "Name": table,
        "TableType": "EXTERNAL_TABLE",
        "Parameters": params,
        "StorageDescriptor": {
            "Location": location,
            "InputFormat": input_format,
            "OutputFormat": output_format,
            "SerdeInfo": {"SerializationLibrary": serde},
            "Columns": [],
        },
    }
    if dry_run:
        summary.add(
            ActionResult(
                table,
                "glue",
                "dry-run",
                f"create-table {database}.{table} params={params} location={location}",
            )
        )
        return

    import boto3

    glue = boto3.client("glue", region_name=region)
    try:
        glue.create_database(
            DatabaseInput={"Name": database, "Description": f"Weft SF lakehouse ({fmt})"}
        )
    except glue.exceptions.AlreadyExistsException:
        pass
    try:
        glue.delete_table(DatabaseName=database, Name=table)
    except Exception:
        pass
    glue.create_table(DatabaseName=database, TableInput=table_input)
    summary.add(
        ActionResult(
            table,
            "glue",
            "ok",
            f"{database}.{table} → detect via {fmt} params",
        )
    )


def make_iceberg_catalog(warehouse: str, sqlite_path: Path | None):
    """SqlCatalog for local; GlueCatalog when warehouse is s3 and AWS is intended."""
    if is_s3(warehouse):
        from pyiceberg.catalog.glue import GlueCatalog

        # Catalog name is logical; Glue databases are namespaces.
        return GlueCatalog(
            "glue",
            **{
                "warehouse": warehouse,
                "region_name": os.environ.get("AWS_REGION")
                or os.environ.get("AWS_DEFAULT_REGION")
                or "us-west-2",
            },
        )
    from pyiceberg.catalog.sql import SqlCatalog

    assert sqlite_path is not None
    sqlite_path.parent.mkdir(parents=True, exist_ok=True)
    wh = warehouse
    if wh.startswith("file://"):
        wh = to_fs_path(wh).as_posix()
    Path(wh).mkdir(parents=True, exist_ok=True)
    return SqlCatalog(
        "local",
        **{"uri": f"sqlite:///{sqlite_path}", "warehouse": wh},
    )


def parquet_row_count(files: Sequence[str]) -> int:
    import pyarrow.parquet as pq

    total = 0
    for f in files:
        if is_s3(f):
            import s3fs

            fs = s3fs.S3FileSystem(anon=False)
            with fs.open(f, "rb") as fh:
                total += pq.ParquetFile(fh).metadata.num_rows
        else:
            total += pq.ParquetFile(f).metadata.num_rows
    return total


def plant_dummy_delta_checkpoint(table_uri: str) -> str:
    """Write a foreign-schema Parquet under ``_delta_log/`` (simulates a Delta checkpoint)."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    if is_s3(table_uri):
        raise NotImplementedError("checkpoint planting is local-only (rehearsal)")
    root = to_fs_path(table_uri)
    log_dir = root / "_delta_log"
    log_dir.mkdir(parents=True, exist_ok=True)
    # Real Delta checkpoints look like 00000000000000000010.checkpoint.parquet
    path = log_dir / "00000000000000000010.checkpoint.parquet"
    # Schema deliberately unrelated to the table (Delta action-log shape, simplified).
    pq.write_table(
        pa.table(
            {
                "txn": pa.array([1, 2], type=pa.int64()),
                "add": pa.array(["x", "y"], type=pa.string()),
            }
        ),
        path,
    )
    return str(path.resolve())


def try_plain_parquet_row_count(
    table_uri: str, *, include_delta_log: bool
) -> tuple[int | None, str]:
    """Read directory as plain Parquet (no Delta/Iceberg reader).

    Returns ``(row_count, note)``. ``row_count`` is None when the scan fails
    (typical when a checkpoint Parquet has a conflicting schema).
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    files = list_parquet_files(table_uri, include_delta_log=include_delta_log)
    if not files:
        return 0, "no parquet files"
    try:
        # Mimic ListingTable: every path the .parquet extension filter allows.
        tables = [pq.read_table(f) for f in files]
        combined = pa.concat_tables(tables)
        return combined.num_rows, f"read {len(files)} file(s)"
    except Exception as e:  # noqa: BLE001
        return None, f"{type(e).__name__}: {e}"


def verify_local_reads(table_uri: str, iceberg_table, delta_uri: str) -> None:
    """Assert Iceberg + Delta + plain-Parquet isolation after in-place ``_delta_log``."""
    files = list_parquet_files(table_uri, include_delta_log=False)
    expected = parquet_row_count(files)
    ice_rows = iceberg_table.scan().to_arrow().num_rows
    from deltalake import DeltaTable

    dt = DeltaTable(delta_uri)
    delta_rows = dt.to_pyarrow_table().num_rows
    if ice_rows != expected or delta_rows != expected:
        raise AssertionError(
            f"row count mismatch parquet={expected} iceberg={ice_rows} delta={delta_rows}"
        )
    print(f"[verify] rows match: {expected}")

    # 1) Plain Parquet read of the shared prefix after convert (commit-0 has no checkpoint yet).
    plain_rows, plain_note = try_plain_parquet_row_count(table_uri, include_delta_log=True)
    if plain_rows != expected:
        raise AssertionError(
            "plain Parquet read after Delta convert contaminated: "
            f"got={plain_rows} expected={expected} ({plain_note})"
        )
    print(f"[verify] plain Parquet after Delta convert: {plain_rows} rows ({plain_note})")

    # 2) Plant a dummy Delta checkpoint.parquet and re-read with the same extension filter
    #    the engine uses today (ListingOptions.with_file_extension(".parquet")).
    ckpt = plant_dummy_delta_checkpoint(table_uri)
    print(f"[verify] planted dummy checkpoint: {ckpt}")
    naive_files = list_parquet_files(table_uri, include_delta_log=True)
    if not any(f.endswith("checkpoint.parquet") for f in naive_files):
        raise AssertionError("dummy checkpoint not visible to .parquet extension listing")
    contam_rows, contam_note = try_plain_parquet_row_count(table_uri, include_delta_log=True)
    if contam_rows == expected:
        # Unexpected: schemas happened to concat cleanly and row count matched by luck.
        print(
            f"[verify] CONTAMINATION NOT OBSERVED: naive .parquet listing still "
            f"{contam_rows} rows with checkpoint present ({contam_note}). "
            "Hazard may still exist for schema-incompatible checkpoints; "
            "engine should still exclude _delta_log."
        )
    elif contam_rows is None:
        print(f"[verify] CONTAMINATION MANIFEST (scan error): {contam_note}")
    else:
        print(
            f"[verify] CONTAMINATION MANIFEST (wrong row count): "
            f"got={contam_rows} expected={expected} ({contam_note})"
        )

    # Mitigation check: excluding _delta_log (the engine fix) restores the original rows.
    safe_rows, safe_note = try_plain_parquet_row_count(table_uri, include_delta_log=False)
    if safe_rows != expected:
        raise AssertionError(
            f"data-only Parquet listing (exclude _delta_log) still wrong: "
            f"got={safe_rows} expected={expected} ({safe_note})"
        )
    print(
        f"[verify] MITIGATION OK: exclude _delta_log → {safe_rows} rows ({safe_note})"
    )


def main(argv: Sequence[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--suite", required=True, choices=["tpch", "tpcds"])
    p.add_argument("--sf", default=os.environ.get("SF", "100"))
    p.add_argument(
        "--source-prefix",
        required=True,
        help="Parquet dump root: s3://bucket/tpcds-sf100 or /path/tpcds-sf100",
    )
    p.add_argument(
        "--iceberg-warehouse",
        default=None,
        help="Iceberg warehouse root (metadata). Default: {source-prefix}-iceberg",
    )
    p.add_argument(
        "--formats",
        default="iceberg,delta",
        help="Comma list: parquet,iceberg,delta (parquet = Glue-only / no metadata write)",
    )
    p.add_argument("--tables", default=None, help="Comma subset of tables (default: full suite)")
    p.add_argument("--dry-run", action="store_true", help="Print actions; touch nothing")
    p.add_argument(
        "--skip-glue",
        action="store_true",
        help="Do not create/update Glue databases/tables",
    )
    p.add_argument(
        "--register-glue",
        action="store_true",
        help="Force Glue registration (default: register when source is s3 and not --skip-glue)",
    )
    p.add_argument("--region", default=os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION") or "us-west-2")
    p.add_argument(
        "--sqlite-catalog",
        default=None,
        help="Sqlite path for local SqlCatalog (default: {iceberg-warehouse}/catalog.db)",
    )
    p.add_argument(
        "--verify",
        action="store_true",
        help="After local build, assert Iceberg/Delta row counts == Parquet",
    )
    args = p.parse_args(argv)

    formats = parse_formats(args.formats)
    tables = (
        [t.strip() for t in args.tables.split(",") if t.strip()]
        if args.tables
        else tables_for(args.suite)
    )
    source = args.source_prefix.rstrip("/")
    warehouse = (args.iceberg_warehouse or f"{source}-iceberg").rstrip("/")
    register_glue = (args.register_glue or is_s3(source)) and not args.skip_glue
    if args.dry_run:
        # Still allow printing glue actions when not --skip-glue
        pass

    summary = Summary()
    print(
        f"[lakehouse] suite={args.suite} sf={args.sf} source={source}\n"
        f"            warehouse={warehouse} formats={formats} dry_run={args.dry_run}\n"
        f"            glue={register_glue} region={args.region}"
    )
    print(
        "[lakehouse] Glue DB convention: "
        f"{glue_db_name(args.suite, args.sf, 'parquet')} | "
        f"{glue_db_name(args.suite, args.sf, 'iceberg')} | "
        f"{glue_db_name(args.suite, args.sf, 'delta')}"
    )

    iceberg_catalog = None
    iceberg_ns = glue_db_name(args.suite, args.sf, "iceberg")
    if "iceberg" in formats:
        sqlite = Path(args.sqlite_catalog) if args.sqlite_catalog else None
        if not is_s3(warehouse):
            sqlite = sqlite or (to_fs_path(warehouse) / "catalog.db")
        if not args.dry_run:
            iceberg_catalog = make_iceberg_catalog(warehouse, sqlite)
        else:
            summary.add(
                ActionResult(
                    "*",
                    "iceberg",
                    "dry-run",
                    f"would open catalog warehouse={warehouse} namespace={iceberg_ns}",
                )
            )

    for table in tables:
        table_uri = join_uri(source, table)
        md_loc: str | None = None
        try:
            if "delta" in formats:
                build_delta(table, table_uri, args.dry_run, summary)
            if "iceberg" in formats:
                if args.dry_run:
                    build_iceberg(
                        table, table_uri, warehouse, None, iceberg_ns, True, summary
                    )
                else:
                    assert iceberg_catalog is not None
                    md_loc = build_iceberg(
                        table,
                        table_uri,
                        warehouse,
                        iceberg_catalog,
                        iceberg_ns,
                        False,
                        summary,
                    )
            if "parquet" in formats and register_glue:
                register_glue_parquet_style(
                    region=args.region,
                    database=glue_db_name(args.suite, args.sf, "parquet"),
                    table=table,
                    location=table_uri,
                    fmt="parquet",
                    metadata_location=None,
                    dry_run=args.dry_run,
                    summary=summary,
                )
            if register_glue and "delta" in formats:
                register_glue_parquet_style(
                    region=args.region,
                    database=glue_db_name(args.suite, args.sf, "delta"),
                    table=table,
                    location=table_uri,
                    fmt="delta",
                    metadata_location=None,
                    dry_run=args.dry_run,
                    summary=summary,
                )
            if register_glue and "iceberg" in formats:
                # GlueCatalog.create_table already writes table_type=ICEBERG + metadata_location.
                # Only use boto3 when the Iceberg catalog is local (SqlCatalog).
                using_glue_catalog = is_s3(warehouse)
                if using_glue_catalog and not args.dry_run:
                    summary.add(
                        ActionResult(
                            table,
                            "glue",
                            "ok",
                            f"{iceberg_ns}.{table} registered via PyIceberg GlueCatalog",
                        )
                    )
                else:
                    ice_loc = join_uri(warehouse, iceberg_ns, table)
                    register_glue_parquet_style(
                        region=args.region,
                        database=iceberg_ns,
                        table=table,
                        location=ice_loc,
                        fmt="iceberg",
                        metadata_location=md_loc,
                        dry_run=args.dry_run,
                        summary=summary,
                    )
            if args.verify and not args.dry_run and not is_s3(source):
                if "iceberg" in formats and "delta" in formats and iceberg_catalog is not None:
                    ice_tbl = iceberg_catalog.load_table(f"{iceberg_ns}.{table}")
                    verify_local_reads(table_uri, ice_tbl, table_uri)
        except Exception as e:  # noqa: BLE001
            summary.add(ActionResult(table, "error", "error", f"{type(e).__name__}: {e}"))
            traceback.print_exc()

    return summary.print_final()


if __name__ == "__main__":
    sys.exit(main())

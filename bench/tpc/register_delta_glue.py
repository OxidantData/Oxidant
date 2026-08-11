#!/usr/bin/env python3
"""Write Delta Lake tables from local Parquet dirs to S3 and register in Glue.

Env: DB, REGION, BUCKET, PREFIX, LOCAL_PARQUET, SUITE
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import boto3
import pyarrow.dataset as ds

try:
    from deltalake import write_deltalake
except ImportError as e:
    sys.stderr.write(
        "register_delta_glue.py needs deltalake — pip install 'deltalake>=0.17'\n"
        f"import error: {e}\n"
    )
    sys.exit(1)

# Reuse Hive Parquet SerDe for Spark/Oxidant Delta catalog entries that point
# at a Delta table root (directory containing `_delta_log/`).
PARQUET_INPUT = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
PARQUET_OUTPUT = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
PARQUET_SERDE = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"

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


def arrow_type_to_hive(t) -> str:
    import pyarrow as pa

    if pa.types.is_int32(t):
        return "int"
    if pa.types.is_int64(t) or pa.types.is_integer(t):
        return "bigint"
    if pa.types.is_float32(t):
        return "float"
    if pa.types.is_float64(t):
        return "double"
    if pa.types.is_boolean(t):
        return "boolean"
    if pa.types.is_date(t):
        return "date"
    if pa.types.is_timestamp(t):
        return "timestamp"
    if pa.types.is_decimal(t):
        return f"decimal({t.precision},{t.scale})"
    if pa.types.is_binary(t) or pa.types.is_large_binary(t):
        return "binary"
    return "string"


def ensure_database(glue, name: str, location: str) -> None:
    try:
        glue.get_database(Name=name)
        print(f"[delta] database {name} exists")
    except glue.exceptions.EntityNotFoundException:
        glue.create_database(
            DatabaseInput={
                "Name": name,
                "Description": f"Oxidant TPC Delta ({name})",
                "LocationUri": location,
            }
        )
        print(f"[delta] created database {name}")


def upsert_delta_table(glue, db: str, name: str, location: str, columns: list) -> None:
    storage = {
        "Columns": columns,
        "Location": location,
        "InputFormat": PARQUET_INPUT,
        "OutputFormat": PARQUET_OUTPUT,
        "SerdeInfo": {"SerializationLibrary": PARQUET_SERDE, "Parameters": {}},
        "StoredAsSubDirectories": False,
    }
    params = {
        "EXTERNAL": "TRUE",
        "classification": "delta",
        "provider": "delta",
        "spark.sql.sources.provider": "delta",
        "table_type": "DELTA",
    }
    table_input = {
        "Name": name,
        "TableType": "EXTERNAL_TABLE",
        "Parameters": params,
        "StorageDescriptor": storage,
    }
    try:
        glue.get_table(DatabaseName=db, Name=name)
        glue.update_table(DatabaseName=db, TableInput=table_input)
        print(f"[delta] updated {db}.{name} -> {location}")
    except glue.exceptions.EntityNotFoundException:
        glue.create_table(DatabaseName=db, TableInput=table_input)
        print(f"[delta] created {db}.{name} -> {location}")


def main() -> None:
    db = os.environ["DB"]
    region = os.environ.get("REGION", "us-west-2")
    bucket = os.environ["BUCKET"]
    prefix = os.environ["PREFIX"].strip("/")
    local = Path(os.environ["LOCAL_PARQUET"])
    suite = os.environ.get("SUITE", "tpch")

    if not local.is_dir():
        raise SystemExit(f"missing LOCAL_PARQUET={local}")

    glue = boto3.client("glue", region_name=region)
    ensure_database(glue, db, f"s3://{bucket}/{prefix}/")

    if suite == "tpch":
        names = TPCH_TABLES
    else:
        names = sorted(p.name for p in local.iterdir() if p.is_dir())

    storage_opts = {"AWS_REGION": region}
    for name in names:
        src = local / name
        if not src.is_dir():
            raise SystemExit(f"missing table dir {src}")
        dest = f"s3://{bucket}/{prefix}/{name}"
        print(f"[delta] write {src} -> {dest}")
        dataset = ds.dataset(str(src), format="parquet")
        # Stream via RecordBatchReader so large facts stay off-heap as much as possible.
        reader = dataset.scanner().to_reader()
        write_deltalake(
            dest,
            reader,
            mode="overwrite",
            storage_options=storage_opts,
        )
        columns = [
            {"Name": f.name, "Type": arrow_type_to_hive(f.type), "Comment": ""}
            for f in dataset.schema
        ]
        upsert_delta_table(glue, db, name, dest.rstrip("/") + "/", columns)

    print("[delta] done")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[delta] ERROR: {e}", file=sys.stderr)
        raise

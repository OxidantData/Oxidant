#!/usr/bin/env python3
"""Register Parquet directories on S3 as EXTERNAL Hive Parquet tables in Glue.

Env: DB, REGION, BUCKET, PREFIX, SUITE
"""

from __future__ import annotations

import os
import sys

import boto3
import pyarrow.parquet as pq


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

PARQUET_INPUT = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
PARQUET_OUTPUT = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
PARQUET_SERDE = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"


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


def schema_from_s3_prefix(bucket: str, key_prefix: str, region: str):
    s3 = boto3.client("s3", region_name=region)
    paginator = s3.get_paginator("list_objects_v2")
    sample_key = None
    for page in paginator.paginate(Bucket=bucket, Prefix=key_prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                sample_key = obj["Key"]
                break
        if sample_key:
            break
    if not sample_key:
        raise SystemExit(f"no parquet under s3://{bucket}/{key_prefix}")
    import io

    buf = io.BytesIO()
    s3.download_fileobj(bucket, sample_key, buf)
    buf.seek(0)
    schema = pq.ParquetFile(buf).schema_arrow
    return [
        {"Name": f.name, "Type": arrow_type_to_hive(f.type), "Comment": ""}
        for f in schema
    ]


def ensure_database(glue, name: str, location: str) -> None:
    try:
        glue.get_database(Name=name)
        print(f"[glue] database {name} exists")
    except glue.exceptions.EntityNotFoundException:
        glue.create_database(
            DatabaseInput={
                "Name": name,
                "Description": f"Oxidant TPC Parquet ({name})",
                "LocationUri": location,
            }
        )
        print(f"[glue] created database {name}")


def upsert_table(glue, db: str, name: str, location: str, columns: list) -> None:
    storage = {
        "Columns": columns,
        "Location": location,
        "InputFormat": PARQUET_INPUT,
        "OutputFormat": PARQUET_OUTPUT,
        "SerdeInfo": {"SerializationLibrary": PARQUET_SERDE, "Parameters": {}},
        "StoredAsSubDirectories": False,
    }
    params = {"EXTERNAL": "TRUE", "classification": "parquet"}
    table_input = {
        "Name": name,
        "TableType": "EXTERNAL_TABLE",
        "Parameters": params,
        "StorageDescriptor": storage,
    }
    try:
        glue.get_table(DatabaseName=db, Name=name)
        glue.update_table(DatabaseName=db, TableInput=table_input)
        print(f"[glue] updated {db}.{name} -> {location}")
    except glue.exceptions.EntityNotFoundException:
        glue.create_table(DatabaseName=db, TableInput=table_input)
        print(f"[glue] created {db}.{name} -> {location}")


def list_table_prefixes(bucket: str, prefix: str, region: str) -> list[str]:
    s3 = boto3.client("s3", region_name=region)
    tables: set[str] = set()
    for page in s3.get_paginator("list_objects_v2").paginate(
        Bucket=bucket, Prefix=prefix.rstrip("/") + "/", Delimiter="/"
    ):
        for cp in page.get("CommonPrefixes", []):
            tables.add(cp["Prefix"].rstrip("/").split("/")[-1])
    return sorted(tables)


def main() -> None:
    db = os.environ["DB"]
    region = os.environ.get("REGION", "us-west-2")
    bucket = os.environ["BUCKET"]
    prefix = os.environ["PREFIX"].strip("/")
    suite = os.environ.get("SUITE", "tpch")

    glue = boto3.client("glue", region_name=region)
    ensure_database(glue, db, f"s3://{bucket}/{prefix}/")

    if suite == "tpch":
        names = TPCH_TABLES
    else:
        names = list_table_prefixes(bucket, prefix, region)
        if not names:
            raise SystemExit(f"no table prefixes under s3://{bucket}/{prefix}/")

    for name in names:
        key_prefix = f"{prefix}/{name}/"
        location = f"s3://{bucket}/{key_prefix}"
        columns = schema_from_s3_prefix(bucket, key_prefix, region)
        upsert_table(glue, db, name, location, columns)

    print("[glue] parquet registration done")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[glue] ERROR: {e}", file=sys.stderr)
        raise

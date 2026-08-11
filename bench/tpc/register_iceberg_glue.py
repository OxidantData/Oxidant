#!/usr/bin/env python3
"""Register Parquet directories already on S3 as Iceberg tables in the Glue catalog.

Env:
  DB, WAREHOUSE, REGION, BUCKET, PREFIX, SUITE
"""

from __future__ import annotations

import os
import sys

import pyarrow as pa
import pyarrow.dataset as ds
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, TableAlreadyExistsError
from pyiceberg.schema import Schema
from pyiceberg.types import (
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    NestedField,
    StringType,
)

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


def tpch_schema(name: str) -> Schema:
    # Parquet from Arrow marks columns nullable; Iceberg required=True rejects overwrite.
    schemas = {
        "nation": Schema(
            NestedField(1, "n_nationkey", LongType(), required=False),
            NestedField(2, "n_name", StringType(), required=False),
            NestedField(3, "n_regionkey", LongType(), required=False),
            NestedField(4, "n_comment", StringType(), required=False),
        ),
        "region": Schema(
            NestedField(1, "r_regionkey", LongType(), required=False),
            NestedField(2, "r_name", StringType(), required=False),
            NestedField(3, "r_comment", StringType(), required=False),
        ),
        "supplier": Schema(
            NestedField(1, "s_suppkey", LongType(), required=False),
            NestedField(2, "s_name", StringType(), required=False),
            NestedField(3, "s_address", StringType(), required=False),
            NestedField(4, "s_nationkey", LongType(), required=False),
            NestedField(5, "s_phone", StringType(), required=False),
            NestedField(6, "s_acctbal", DecimalType(15, 2), required=False),
            NestedField(7, "s_comment", StringType(), required=False),
        ),
        "customer": Schema(
            NestedField(1, "c_custkey", LongType(), required=False),
            NestedField(2, "c_name", StringType(), required=False),
            NestedField(3, "c_address", StringType(), required=False),
            NestedField(4, "c_nationkey", LongType(), required=False),
            NestedField(5, "c_phone", StringType(), required=False),
            NestedField(6, "c_acctbal", DecimalType(15, 2), required=False),
            NestedField(7, "c_mktsegment", StringType(), required=False),
            NestedField(8, "c_comment", StringType(), required=False),
        ),
        "part": Schema(
            NestedField(1, "p_partkey", LongType(), required=False),
            NestedField(2, "p_name", StringType(), required=False),
            NestedField(3, "p_mfgr", StringType(), required=False),
            NestedField(4, "p_brand", StringType(), required=False),
            NestedField(5, "p_type", StringType(), required=False),
            NestedField(6, "p_size", IntegerType(), required=False),
            NestedField(7, "p_container", StringType(), required=False),
            NestedField(8, "p_retailprice", DecimalType(15, 2), required=False),
            NestedField(9, "p_comment", StringType(), required=False),
        ),
        "partsupp": Schema(
            NestedField(1, "ps_partkey", LongType(), required=False),
            NestedField(2, "ps_suppkey", LongType(), required=False),
            NestedField(3, "ps_availqty", IntegerType(), required=False),
            NestedField(4, "ps_supplycost", DecimalType(15, 2), required=False),
            NestedField(5, "ps_comment", StringType(), required=False),
        ),
        "orders": Schema(
            NestedField(1, "o_orderkey", LongType(), required=False),
            NestedField(2, "o_custkey", LongType(), required=False),
            NestedField(3, "o_orderstatus", StringType(), required=False),
            NestedField(4, "o_totalprice", DecimalType(15, 2), required=False),
            NestedField(5, "o_orderdate", DateType(), required=False),
            NestedField(6, "o_orderpriority", StringType(), required=False),
            NestedField(7, "o_clerk", StringType(), required=False),
            NestedField(8, "o_shippriority", IntegerType(), required=False),
            NestedField(9, "o_comment", StringType(), required=False),
        ),
        "lineitem": Schema(
            NestedField(1, "l_orderkey", LongType(), required=False),
            NestedField(2, "l_partkey", LongType(), required=False),
            NestedField(3, "l_suppkey", LongType(), required=False),
            NestedField(4, "l_linenumber", IntegerType(), required=False),
            NestedField(5, "l_quantity", DecimalType(15, 2), required=False),
            NestedField(6, "l_extendedprice", DecimalType(15, 2), required=False),
            NestedField(7, "l_discount", DecimalType(15, 2), required=False),
            NestedField(8, "l_tax", DecimalType(15, 2), required=False),
            NestedField(9, "l_returnflag", StringType(), required=False),
            NestedField(10, "l_linestatus", StringType(), required=False),
            NestedField(11, "l_shipdate", DateType(), required=False),
            NestedField(12, "l_commitdate", DateType(), required=False),
            NestedField(13, "l_receiptdate", DateType(), required=False),
            NestedField(14, "l_shipinstruct", StringType(), required=False),
            NestedField(15, "l_shipmode", StringType(), required=False),
            NestedField(16, "l_comment", StringType(), required=False),
        ),
    }
    return schemas[name]


def arrow_to_iceberg_schema(arrow_schema) -> Schema:
    """Best-effort Arrow → Iceberg schema for TPC-DS inferred Parquet (no field-ids)."""
    from pyiceberg.io.pyarrow import pyarrow_to_schema
    from pyiceberg.table.name_mapping import MappedField, NameMapping

    mapping = NameMapping(
        [
            MappedField(field_id=i, names=[field.name])
            for i, field in enumerate(arrow_schema, start=1)
        ]
    )
    return pyarrow_to_schema(arrow_schema, name_mapping=mapping)


def ensure_overwrite(catalog, ident, schema: Schema, location: str, parquet_loc: str) -> None:
    try:
        table = catalog.create_table(ident, schema=schema, location=location)
        print(f"[glue] created {'.'.join(ident)}")
    except TableAlreadyExistsError:
        table = catalog.load_table(ident)
        print(f"[glue] exists {'.'.join(ident)} — overwrite")
    # Commit in ~512 MiB Arrow chunks (not per micro-batch) to keep Glue/S3 commits sane.
    chunk_target = 512 * 1024 * 1024
    scanner = ds.dataset(parquet_loc, format="parquet").scanner()
    pending: list[pa.RecordBatch] = []
    pending_bytes = 0
    first = True
    rows = 0

    def flush() -> None:
        nonlocal pending, pending_bytes, first, rows
        if not pending:
            return
        arrow_table = pa.Table.from_batches(pending)
        if first:
            table.overwrite(arrow_table)
            first = False
        else:
            table.append(arrow_table)
        rows += arrow_table.num_rows
        pending = []
        pending_bytes = 0

    for batch in scanner.to_batches():
        pending.append(batch)
        pending_bytes += batch.nbytes
        if pending_bytes >= chunk_target:
            flush()
    flush()
    if first:
        empty = pa.Table.from_batches([], schema=scanner.projected_schema)
        table.overwrite(empty)
    print(f"[glue] wrote {'.'.join(ident)} ({rows} rows) from {parquet_loc}")


def main() -> None:
    db = os.environ["DB"]
    warehouse = os.environ["WAREHOUSE"].rstrip("/")
    region = os.environ.get("REGION", "us-west-2")
    bucket = os.environ["BUCKET"]
    prefix = os.environ["PREFIX"].strip("/")
    suite = os.environ.get("SUITE", "tpch")
    local = os.environ.get("LOCAL_PARQUET", "").rstrip("/")

    catalog = load_catalog(
        "glue",
        **{
            "type": "glue",
            "warehouse": warehouse,
            "client.region": region,
        },
    )

    try:
        catalog.create_namespace(db)
        print(f"[glue] created namespace {db}")
    except NamespaceAlreadyExistsError:
        print(f"[glue] namespace {db} exists")
    # Glue can lag after delete; ensure the database row is present for CreateTable.
    import boto3

    glue = boto3.client("glue", region_name=region)
    try:
        glue.get_database(Name=db)
    except glue.exceptions.EntityNotFoundException:
        glue.create_database(
            DatabaseInput={
                "Name": db,
                "Description": f"Oxidant TPC Iceberg ({db})",
                "LocationUri": warehouse + "/",
            }
        )
        print(f"[glue] created Glue database {db}")
        try:
            catalog.create_namespace(db)
        except NamespaceAlreadyExistsError:
            pass

    def src_for(name: str) -> str:
        if local:
            return f"{local}/{name}"
        return f"s3://{bucket}/{prefix}/{name}/"

    if suite == "tpch":
        for name in TPCH_TABLES:
            ensure_overwrite(
                catalog,
                (db, name),
                tpch_schema(name),
                f"{warehouse}/{name}",
                src_for(name),
            )
    else:
        if local:
            from pathlib import Path

            tables = sorted(p.name for p in Path(local).iterdir() if p.is_dir())
        else:
            import boto3

            s3 = boto3.client("s3", region_name=region)
            tables_set: set[str] = set()
            for page in s3.get_paginator("list_objects_v2").paginate(
                Bucket=bucket, Prefix=prefix + "/", Delimiter="/"
            ):
                for cp in page.get("CommonPrefixes", []):
                    tables_set.add(cp["Prefix"].rstrip("/").split("/")[-1])
            tables = sorted(tables_set)
        if not tables:
            raise SystemExit(f"no tables under {local or f's3://{bucket}/{prefix}/'}")
        for name in tables:
            parquet_loc = src_for(name)
            sample = ds.dataset(parquet_loc, format="parquet").scanner().head(1)
            ensure_overwrite(
                catalog,
                (db, name),
                arrow_to_iceberg_schema(sample.schema),
                f"{warehouse}/{name}",
                parquet_loc,
            )

    print("[glue] done")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[glue] ERROR: {e}", file=sys.stderr)
        raise

#!/usr/bin/env python3
"""Stream a Kafka topic into a live Delta table registered in the AWS Glue Data Catalog.

This is the reference shape of an Oxidant streaming pipeline, written against the *stock* PySpark
Connect client — nothing here is Oxidant-specific API. Point ``--remote`` at real Spark and the
same script runs there.

Driven by ``scripts/validate-streaming-glue.sh``; also runnable directly:

    pip install 'pyspark-client>=4.0'
    KAFKA_BOOTSTRAP=b-1.msk.example:9092 KAFKA_TOPIC=orders \\
    GLUE_DATABASE=streaming_live GLUE_TABLE=orders_live \\
    CHECKPOINT=s3://my-bucket/checkpoints/orders_live \\
    python3 python/examples/kafka_to_glue_delta.py
"""

from __future__ import annotations

import os
import sys
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, from_unixtime, to_date
from pyspark.sql.types import LongType, StringType, StructField, StructType

# The producer's JSON payload. Declared rather than inferred: a streaming query has no sample to
# infer from, and an inferred schema that shifts between batches would silently corrupt the table.
PAYLOAD = StructType(
    [
        StructField("order_id", LongType()),
        StructField("customer", StringType()),
        StructField("amount", LongType()),
        StructField("event_ts", LongType()),
    ]
)


def env(name: str, default: str | None = None) -> str:
    value = os.environ.get(name, default)
    if value is None:
        sys.exit(f"{name} is required")
    return value


def main() -> int:
    port = env("OXIDANT_PORT", "50051")
    remote = env("OXIDANT_REMOTE", f"sc://localhost:{port}")
    database = env("GLUE_DATABASE", "streaming_live")
    table = env("GLUE_TABLE", "orders_live")
    checkpoint = env("CHECKPOINT")

    spark = SparkSession.builder.remote(remote).getOrCreate()

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", env("KAFKA_BOOTSTRAP"))
        .option("subscribe", env("KAFKA_TOPIC"))
        # Read the topic from the beginning so a validation run sees the records it produced,
        # rather than only what arrives after the query starts (Spark's `latest` default).
        .option("startingOffsets", env("STARTING_OFFSETS", "earliest"))
        .option("maxOffsetsPerTrigger", env("MAX_OFFSETS_PER_TRIGGER", "10000"))
        .load()
    )

    orders = (
        raw.select(from_json(col("value").cast("string"), PAYLOAD).alias("payload"))
        .select("payload.*")
        # A partition column a dashboard filters on. Partitioning is the single biggest lever on
        # query cost for a live table: without it every dashboard query scans every file the
        # stream has ever written. Low cardinality on purpose — never partition on a raw
        # timestamp or an id.
        .withColumn("event_date", to_date(from_unixtime(col("event_ts"))))
    )

    target = f"glue.{database}.{table}"
    print(f"[stream] {env('KAFKA_TOPIC')} -> {target} (checkpoint {checkpoint})")

    query = (
        orders.writeStream.format("delta")
        .outputMode("append")
        .partitionBy("event_date")
        .option("checkpointLocation", checkpoint)
        # Iceberg metadata is published over the same Parquet files, so Athena, Trino, and
        # DuckDB can read this table too. On by default; shown here because it is the point.
        .option("icebergCompat", "true")
        # `availableNow` drains what is on the topic and stops, which is what a validation run
        # wants. A production pipeline uses `.trigger(processingTime="30 seconds")` instead.
        .trigger(availableNow=True)
        .toTable(target)
    )

    deadline = time.time() + float(env("TIMEOUT_SECONDS", "300"))
    while query.isActive and time.time() < deadline:
        progress = query.lastProgress
        if progress:
            print(
                f"[stream] batch {progress.get('batchId')}: "
                f"{progress.get('numInputRows')} rows"
            )
        time.sleep(2)
    query.stop()

    # The whole point: the streamed rows are queryable as a catalog table, by name, from a
    # completely separate session — which is what a dashboard does.
    reader = SparkSession.builder.remote(remote).getOrCreate()
    total = reader.sql(f"SELECT count(*) AS n FROM {target}").collect()[0]["n"]
    print(f"[verify] {target} has {total} rows")
    reader.sql(
        f"SELECT customer, count(*) AS orders, sum(amount) AS revenue "
        f"FROM {target} GROUP BY customer ORDER BY customer"
    ).show()

    # The same rows, resolved through Iceberg metadata instead of the Delta log. One copy of the
    # data; whichever engine a team already runs can read it.
    iceberg_target = f"{target}_iceberg"
    try:
        iceberg_total = reader.sql(
            f"SELECT count(*) AS n FROM {iceberg_target}"
        ).collect()[0]["n"]
        print(f"[verify] {iceberg_target} (Iceberg view) has {iceberg_total} rows")
        if iceberg_total != total:
            print(
                f"[verify] FAIL: Delta sees {total} rows, Iceberg sees {iceberg_total}",
                file=sys.stderr,
            )
            return 1
    except Exception as exc:  # noqa: BLE001 - the Delta table is still the primary result
        print(f"[verify] WARN: Iceberg view unreadable: {exc}", file=sys.stderr)

    if total == 0:
        print("[verify] FAIL: the table is empty", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

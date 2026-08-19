#!/usr/bin/env python3
"""Stock pyspark.pipelines client e2e: DefineSqlGraphElements + StartRun over Kafka spool."""

from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.pipelines.spark_connect_graph_element_registry import (
    SparkConnectGraphElementRegistry,
)
from pyspark.pipelines.spark_connect_pipeline import (
    create_dataflow_graph,
    handle_pipeline_events,
    start_run,
)

REPO_ROOT = Path(os.environ["OXIDANT_REPO_ROOT"]).resolve()
SPOOL_DIR = REPO_ROOT / "examples" / "spool" / "orders"
WAREHOUSE = Path(os.environ["OXIDANT_WAREHOUSE"])
CHECKPOINTS = Path(os.environ["OXIDANT_CHECKPOINTS"])
CONNECT_URL = os.environ.get("OXIDANT_CONNECT_URL", "sc://localhost:50051")
EXPECTED_REVENUE = 725


def pipeline_sql(spool: Path) -> str:
    spool = spool.resolve()
    return f"""CREATE STREAMING TABLE orders_bronze
TBLPROPERTIES (
  'subscribe' = 'orders',
  'oxidant.spool.dir' = '{spool}',
  'startingOffsets' = 'earliest'
)
USING DELTA
AS SELECT
  CAST(get_json_object(CAST(value AS STRING), '$.order_id') AS BIGINT) AS order_id,
  get_json_object(CAST(value AS STRING), '$.customer') AS customer,
  CAST(get_json_object(CAST(value AS STRING), '$.amount') AS BIGINT) AS amount
FROM stream;

CREATE MATERIALIZED VIEW revenue_gold AS
SELECT customer, sum(amount) AS revenue, count(*) AS orders
FROM orders_bronze WHERE amount > 0 GROUP BY customer;
"""


def main() -> int:
    if not SPOOL_DIR.is_dir():
        print(f"spool dir missing: {SPOOL_DIR}", file=sys.stderr)
        return 1

    WAREHOUSE.mkdir(parents=True, exist_ok=True)
    CHECKPOINTS.mkdir(parents=True, exist_ok=True)

    spark = (
        SparkSession.builder.remote(CONNECT_URL)
        .config("spark.sql.catalog.local.type", "local")
        .config("spark.sql.catalog.local.warehouse", str(WAREHOUSE))
        .config("spark.sql.defaultCatalog", "local")
        .getOrCreate()
    )

    graph_id = create_dataflow_graph(
        spark,
        default_catalog="local",
        default_database="live",
        sql_conf={},
    )
    print(f"created graph: {graph_id}")

    registry = SparkConnectGraphElementRegistry(spark, graph_id)
    registry.register_sql(pipeline_sql(SPOOL_DIR), Path("pipeline.sql"))
    print("defined SQL graph elements")

    events = start_run(
        spark,
        graph_id,
        full_refresh=None,
        full_refresh_all=False,
        refresh=None,
        dry=False,
        storage=str(CHECKPOINTS),
    )
    handle_pipeline_events(events)
    print("StartRun complete")

    total = spark.sql("SELECT sum(revenue) AS total FROM local.live.revenue_gold").collect()[0][
        "total"
    ]
    print(f"sum(revenue) = {total}")
    if total != EXPECTED_REVENUE:
        print(f"expected {EXPECTED_REVENUE}, got {total}", file=sys.stderr)
        return 1

    print("PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())

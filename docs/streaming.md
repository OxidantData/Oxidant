# Structured Streaming: Kafka → live tables in the lake

Oxidant runs Spark Structured Streaming queries against the Spark Connect protocol, so a stock
PySpark client's `spark.readStream…writeStream` works unchanged. The pipeline this page covers is
the one that makes streaming useful to a dashboard:

```
Kafka topic  →  micro-batch  →  your DataFrame transformation  →  Delta table in Glue  →  BI query
```

Each micro-batch is committed as one Delta transaction, so a dashboard polling the table always
sees a whole number of batches — never a half-written file.

- Catalog setup (auth, region, warehouse): [`catalogs-glue.md`](catalogs-glue.md)
- The engine's overall design: [`architecture.md`](architecture.md)

## Quick start

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType

spark = SparkSession.builder.remote("sc://localhost:50051").getOrCreate()

payload = StructType([
    StructField("order_id", LongType()),
    StructField("customer", StringType()),
    StructField("amount", LongType()),
    StructField("event_time", TimestampType()),
])

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "b-1.msk.example:9092")
    .option("subscribe", "orders")
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", 50000)
    .load()
)

orders = (
    raw.select(from_json(col("value").cast("string"), payload).alias("o"))
       .select("o.*")
)

query = (
    orders.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "s3://my-bucket/checkpoints/orders_live")
    .trigger(processingTime="30 seconds")
    .toTable("glue.streaming_live.orders")
)
```

Then, from anywhere that reads Glue — Oxidant, Spark, Athena, Trino:

```sql
SELECT customer, sum(amount) FROM glue.streaming_live.orders GROUP BY customer;
```

## Keep streaming tables in their own schema

`streaming_live` above is a **separate Glue database**, and that is the intended layout rather
than a stylistic preference. A live table is written continuously and read at seconds-old
freshness; a batch-loaded table is rewritten on a schedule and read at hours-old freshness. Giving
them separate databases means:

- a permissions boundary — the streaming writer's IAM role needs `CreateTable`/`UpdateTable` on
  one database, not on your curated warehouse;
- a blast radius — a runaway stream cannot collide with a table a batch job owns;
- an obvious place for a reader to look for "the fresh copy".

Oxidant **creates the database if it does not exist** when the query starts, so nothing has to be
pre-provisioned. It is created with the catalog's warehouse convention
(`{warehouse}/{database}.db/`) unless the Glue database already carries a `LocationUri`.

## The Kafka source

`readStream.format("kafka")` produces Spark's exact schema, so any Spark job's projections port
over unchanged:

| Column | Type | Notes |
|--------|------|-------|
| `key` | `binary` | nullable |
| `value` | `binary` | nullable — cast it (`col("value").cast("string")`) |
| `topic` | `string` | |
| `partition` | `int` | |
| `offset` | `bigint` | |
| `timestamp` | `timestamp` | producer timestamp, milliseconds |
| `timestampType` | `int` | `0` = `CreateTime` |

### Options

| Option | Default | Meaning |
|--------|---------|---------|
| `kafka.bootstrap.servers` | — (required) | Comma-separated broker list |
| `subscribe` | — (required) | Comma-separated topic list |
| `startingOffsets` | `latest` | `earliest`, `latest`, or `{"topic":{"0":12,"1":-2}}` (`-2` earliest, `-1` latest) |
| `maxOffsetsPerTrigger` | unlimited | Records per micro-batch, across all partitions |
| `kafka.fetch.max.wait.ms` | `500` | Broker-side wait before returning an empty fetch |
| `kafka.max.partition.fetch.bytes` | `1048576` | Fetch ceiling per partition per batch |

**No consumer groups.** Like Spark, Oxidant assigns partitions directly and keeps offsets in the
query's own `checkpointLocation`. That is what makes a query replayable: the checkpoint, not the
broker, is the source of truth for where you are — list your topics with `subscribe`.

**Offsets are committed after the sink write, never before.** A crash between the two replays the
last micro-batch (at-least-once). A crash never skips one.

Not supported yet: `subscribePattern`, `assign`, `includeHeaders`, `kafka.group.id`, per-record
`timestampType` discrimination, and Kafka as a *sink*. `subscribePattern` and `assign` are
rejected rather than silently reinterpreted.

### Running without a broker

Setting `OXIDANT_KAFKA_SPOOL` (or the `oxidant.spool.dir` option) to a directory of
newline-delimited files makes the source read `batch-0`, `batch-1`, … from disk instead of a
broker, one file per micro-batch. This exists so tests and demos can exercise the whole pipeline
offline. It is not a broker substitute — it has one partition and replays whole files on restart.

## The sink

| `writeStream.format(...)` | Behaviour |
|---------------------------|-----------|
| `delta` (default for `toTable`) | One Parquet file + one `_delta_log` commit per batch. Atomic; readable by Spark, Athena, Trino. |
| `parquet` | One Parquet file per batch in the table directory (Hive-style). No commit protocol — a reader listing mid-write can see a partial file. |
| `json` / `csv` | Local file directory. Development only. |
| `memory` | In-process. Tests only. |

Both `toTable("catalog.database.table")` and `start("s3://bucket/path")` work; only the former
declares the table in the catalog. `s3://` and local paths behave identically — writes go through
the same object store, credentials, and assumed role that a `SELECT` from the table would use.

**Iceberg is not a supported streaming sink yet.** An Iceberg commit has to write Avro manifests
and update the catalog's `metadata_location` atomically; until that exists, `format("iceberg")`
returns a clear error pointing at Delta rather than writing a table nothing can read. Oxidant
*reads* Iceberg tables fine — this is a write-side gap only.

### The table Oxidant registers in Glue

A Delta table's SerDe is indistinguishable from a plain Parquet table's, so the metastore entry
carries `spark.sql.sources.provider = delta` (plus `classification = delta`) — the same convention
Spark, EMR, and Athena use to know they should read `_delta_log/` instead of listing the
directory.

Schema is declared once, at query start, from the *transformed* plan's output — not from the
source's seven Kafka columns. Once created, a batch whose schema drifts from the table's is
rejected before anything is written; schema evolution is not automatic.

## Triggers

| `trigger(...)` | Behaviour |
|----------------|-----------|
| `processingTime="30 seconds"` | Fire every interval. Accepts `ms`/`seconds`/`minutes`/`hours`. |
| `once=True` | Drain everything available, then stop. |
| `availableNow=True` | Same, then idle. |
| *(omitted)* | Every 1 second. |

A batch that fails terminates the query with the error on its status, rather than retrying the
same failure every interval — check `query.status()` / `query.lastProgress`.

## Checkpoints

`checkpointLocation` holds `offsets.json`: the committed batch id, the source's replay position,
and the event-time watermark. Point a restarted query at the same location and it resumes; the
query `id` survives the restart while `run_id` is new, exactly as in Spark. Delete the directory
to start over.

Today the checkpoint is written through the local filesystem, so a driver that moves hosts needs
that path on shared storage (EFS/NFS) — object-store checkpoints are not implemented yet.

## Validating against real Glue

`scripts/validate-streaming-glue.sh` runs the whole thing against a real broker, a real S3
bucket, and a real Glue database, then queries the table back and prints the row count:

```sh
export KAFKA_BOOTSTRAP=b-1.msk.example:9092
export KAFKA_TOPIC=orders
export GLUE_DATABASE=streaming_live       # created if missing
export S3_WAREHOUSE=s3://my-bucket/streaming
export AWS_REGION=us-east-1
./scripts/validate-streaming-glue.sh
```

The CI suite covers everything above the broker socket
(`crates/oxidant-connect/tests/streaming_kafka_lakehouse.rs`); this script is what covers the AWS
leg, which needs credentials CI does not have.

## Limits worth knowing before you deploy

- **Micro-batches run on the driver.** The streaming path does not use the Flight worker cluster,
  so throughput is bounded by one process. Size `maxOffsetsPerTrigger` accordingly.
- **Append output mode only.** `complete` and `update` are accepted but behave as `append`.
- **No stateful aggregation across batches.** Watermarks drop late rows and `dedupColumns`
  deduplicates within a bounded window, but streaming joins and windowed aggregations that need
  cross-batch state are not implemented.
- **One writer per table.** Concurrent Delta writers are detected (the commit is
  create-if-not-exists and retries at the next free version), but the sink is designed for a
  single streaming query per table.
- **No compaction.** One file per micro-batch means a fast trigger produces many small files. Run
  a periodic compaction job, or use a slower trigger.

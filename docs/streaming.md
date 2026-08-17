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
| `kafka.metadata.max.age.ms` | `30000` | How often the partition assignment is re-resolved |

**Partitions are fetched concurrently**, so a batch costs one round trip's latency rather than one
per partition. On a mostly-idle topic that is the difference between a trigger that fires on time
and one that spends `partitions x fetch.max.wait.ms` waiting.

**`maxOffsetsPerTrigger` is divided across partitions**, not spent in partition order: every
partition gets a floor of one record and the remainder is split in proportion to each partition's
lag. Spending the budget in order would let one busy partition consume it every batch and leave
the rest of the topic permanently unread.

**New partitions are picked up automatically.** The assignment is re-resolved every
`kafka.metadata.max.age.ms`; a partition added after the query started is read from `earliest`,
because every record in it postdates the query.

**No consumer groups.** Like Spark, Oxidant assigns partitions directly and keeps offsets in the
query's own `checkpointLocation`. That is what makes a query replayable: the checkpoint, not the
broker, is the source of truth for where you are — list your topics with `subscribe`.

**Offsets are committed after the sink write, never before**, and the Delta sink stamps each
micro-batch id into its commit as a `txn` action. So a crash between the two replays the batch,
the log recognizes the replay, and the rows are *not* written twice — exactly-once into the table,
not merely at-least-once. A crash never skips a batch either.

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
| `delta` (default for `toTable`) | One snappy Parquet file + one `_delta_log` commit per batch, with per-column statistics and periodic checkpoints. Atomic; readable by Spark, Athena, Trino — and, via the Iceberg metadata published alongside it, by Iceberg engines too. |
| `parquet` | One snappy Parquet file per batch in the table directory (Hive-style). No commit protocol — a reader listing mid-write can see a partial file. |
| `json` / `csv` | Local file directory. Development only. |
| `memory` | In-process. Tests only. |

Both `toTable("catalog.database.table")` and `start("s3://bucket/path")` work; only the former
declares the table in the catalog. `s3://` and local paths behave identically — writes go through
the same object store, credentials, and assumed role that a `SELECT` from the table would use.

`format("iceberg")` is not a sink format — but you almost certainly do not need it to, because a
Delta sink publishes Iceberg metadata over the same files. See
[Reading one table from any engine](#reading-one-table-from-any-engine) below.

### Partitioning

`writeStream.partitionBy("event_date")` writes Hive-style directories
(`event_date=2026-08-17/part-….parquet`) and records the values in the Delta `add` action, the way
Spark does. The partition columns live in the path, not in the data files.

Partitioning is the single biggest lever on dashboard query cost: without it every query scans
every file the stream has ever written. Partition on something a dashboard filters on and that
does not explode in cardinality — a date, a region, a tenant — never on a timestamp or an id.

Every data file also carries per-column `stats` (min/max/null counts) in its `add` action, so
readers can skip files within a partition too.

### The table Oxidant registers in Glue

A Delta table's SerDe is indistinguishable from a plain Parquet table's, so the metastore entry
carries `spark.sql.sources.provider = delta` (plus `classification = delta`) — the same convention
Spark, EMR, and Athena use to know they should read `_delta_log/` instead of listing the
directory.

Schema is declared once, at query start, from the *transformed* plan's output — not from the
source's seven Kafka columns. Once created, a batch whose schema drifts from the table's is
rejected before anything is written; schema evolution is not automatic.

## Reading one table from any engine

A live table is worth more when everything in the building can query it. Delta and Iceberg are
both *metadata over Parquet* — the data files are identical, and only the description of which
files are live differs — so Oxidant writes one copy of the data and publishes **both** metadata
trees over it:

```
              part-00000-….parquet   part-00001-….parquet     <- written once
                     |                       |
   _delta_log/*.json + checkpoints           metadata/*.avro + vN.metadata.json
   (Spark, Databricks, Athena, Oxidant)      (Trino, Athena, DuckDB, Snowflake, Oxidant)
```

This is on by default for Delta sinks. Turn it off with `.option("icebergCompat", "false")`.

- In a catalog, the Delta table keeps its name and a **sibling Iceberg entry** is registered
  alongside it: `orders` and `orders_iceberg`. One metastore entry cannot be both, because Athena
  decides how to read a table from its `table_type`. Change the suffix with
  `.option("icebergTableSuffix", "_ice")`.
- Without a catalog, `metadata/version-hint.text` is written, which is how Trino's hadoop catalog
  and DuckDB's Iceberg extension find the current snapshot.
- The published metadata sets `schema.name-mapping.default`. Parquet written for Delta carries no
  Iceberg field ids, and without a name mapping an Iceberg reader opens the table and returns
  every column as null. This is the detail that makes the whole thing work.

**Iceberg readers trail Delta readers.** The Iceberg tree is published on the first commit — so
the table is readable as Iceberg from its first micro-batch, rather than only once it has written
ten — and republished every `checkpointInterval` commits (10 by default) after that. Republishing
per batch would cost more than the data write. Delta readers always see the newest batch; Iceberg
readers see the table as of the last publish. Lower `checkpointInterval` to shorten the lag, at
the cost of more metadata writes.

This is verified against Amazon Athena, not just against Oxidant's own Iceberg reader: a streamed
table's `GROUP BY customer` aggregation in Athena matches Oxidant's row for row.

```python
query = (
    orders.writeStream
    .format("delta")
    .partitionBy("event_date")
    .option("checkpointLocation", "s3://my-bucket/checkpoints/orders_live")
    .option("checkpointInterval", "10")     # Delta checkpoints + Iceberg publishes
    .trigger(processingTime="30 seconds")
    .toTable("glue.streaming_live.orders")
)
```

```sql
-- Delta readers
SELECT count(*) FROM glue.streaming_live.orders;
-- Iceberg readers, same files
SELECT count(*) FROM glue.streaming_live.orders_iceberg;
```

## Triggers

| `trigger(...)` | Behaviour |
|----------------|-----------|
| `processingTime="30 seconds"` | Fire every interval. Accepts `ms`/`seconds`/`minutes`/`hours`. |
| `once=True` | Drain everything available, then stop. |
| `availableNow=True` | Same. The query goes inactive once drained, so `awaitTermination()` returns and a `while query.isActive` loop exits. |
| *(omitted)* | Every 1 second. |

Triggers fire on a **fixed schedule**: a batch that overruns its interval is followed immediately
by the next one, rather than the interval being added on top of the batch's own duration.

Transient I/O failures — an S3 5xx, a broker leader election, a throttled catalog call — are
retried with exponential backoff (4 attempts). A batch that fails anyway, or that fails for a
reason retrying cannot fix (a schema error, an offset aged out of retention), terminates the query
with the error on its status rather than spinning on it every interval. Check `query.status()` /
`query.lastProgress`.

## Checkpoints

`checkpointLocation` holds `offsets.json`: the committed batch id, the source's replay position,
and the event-time watermark. It is written as a single atomic object write, so a crash mid-write
can never leave a half-parsed checkpoint that silently resets the query's position. Point a
restarted query at the same location and it resumes; the query `id` survives the restart while
`run_id` is new, exactly as in Spark. Delete the directory to start over.

The checkpoint is written through the same object store as the table, so `s3://` works and a
driver that restarts on another host resumes from where the last one committed. A bare filesystem
path still works and is fine for a single-host deployment.

The single write is the atomicity story: an object-store `PUT` is atomic, so a reader sees either
the whole previous checkpoint or the whole new one, never a truncated one that would parse as "no
committed offsets" and silently replay (or, with `startingOffsets=latest`, skip) a run's worth of
data.

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

**S3 credentials resolve through the AWS default chain**, in the same order the AWS CLI uses:
environment variables, then web identity (IRSA), then the shared profile in `~/.aws/credentials`
and `~/.aws/config` (including SSO and `credential_process`), then the ECS container endpoint, then
EC2 instance metadata. So `AWS_PROFILE=myprofile` works on a laptop and an instance role works on
EC2, with no extra setup for either.

A table can still pin its own identity, and doing so outranks the chain entirely: static keys in
`storage_options` (`s3.access-key-id` / `fs.s3a.access.key` and friends), `fs.s3a.assumed.role.arn`
to assume a second role on top of the ambient one, or `s3.skip-signature` to read a public bucket
unsigned.

## Throughput

What the pipeline does per micro-batch, and what bounds it:

| Stage | Behaviour |
|-------|-----------|
| Fetch | All partitions concurrently, capped at `kafka.max.partition.fetch.bytes` each |
| Transform | The batch is sliced into 8192-row chunks across one partition per core |
| Write | One snappy Parquet file per partition value, one Delta commit |
| Commit | The next version is remembered, so the log is not listed per commit |

Two ceilings are worth knowing. The default `kafka.max.partition.fetch.bytes` of 1 MiB caps a
batch at roughly `partitions x 1 MiB`, so raise it (or add partitions) before raising the trigger
rate. And micro-batches run **on the driver** — the streaming path does not use the Flight worker
cluster — so a single process bounds throughput.

## Limits worth knowing before you deploy

- **Micro-batches run on the driver.** The streaming path does not use the Flight worker cluster,
  so throughput is bounded by one process. Size `maxOffsetsPerTrigger` accordingly.
- **Append output mode only.** `complete` and `update` are accepted but behave as `append`.
- **No stateful aggregation across batches.** Watermarks drop late rows and `dedupColumns`
  deduplicates within a bounded window, but streaming joins and windowed aggregations that need
  cross-batch state are not implemented.
- **One writer per table.** Concurrent Delta writers are detected (the commit is
  create-if-not-exists and retries at the next free version), and the `txn` stamp is per query, so
  the sink is designed for a single streaming query per table.
- **No compaction.** One file per micro-batch means a fast trigger produces many small files.
  Checkpoints keep the *log* bounded, but the file count still grows — run a periodic compaction
  job, or use a slower trigger. If another writer compacts the table, Oxidant notices the `remove`
  actions and stops checkpointing rather than writing a checkpoint from a stale file list.
- **Iceberg metadata is a snapshot, not a mirror.** It is republished every `checkpointInterval`
  commits, so Iceberg readers lag Delta readers by up to that many batches.
- **The Iceberg side is append-only.** Deletes or updates applied to the Delta table by another
  writer are not reflected in the published Iceberg metadata.

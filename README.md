<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/assets/brand/logo-lockup-white.svg">
    <img alt="Oxidant" src="docs/assets/brand/logo-lockup-black.svg" width="300">
  </picture>
</p>

<h3 align="center">The lakehouse engine in a single binary.</h3>

<p align="center">
  SQL · Streaming · Declarative Pipelines — over open table formats, on your catalogs, with no JVM.<br/>
  Speaks <a href="https://spark.apache.org/docs/latest/spark-connect-overview.html">Spark Connect</a>, so stock PySpark clients work with a one-line URL change.
</p>

<p align="center">
  <a href="https://github.com/OxidantData/Oxidant/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/OxidantData/Oxidant/actions/workflows/ci.yml/badge.svg"></a>
  <a href="https://github.com/OxidantData/Oxidant/releases/latest"><img alt="Latest release" src="https://img.shields.io/github/v/release/OxidantData/Oxidant"></a>
  <a href="LICENSE"><img alt="License: AGPL-3.0" src="https://img.shields.io/badge/license-AGPL--3.0-black"></a>
</p>

---

## Why Oxidant

**One binary, nothing to operate.** No JVM, no driver/executor ceremony, no cluster manager to
babysit before your first query. Download one binary (or pull one container) and you have a SQL
engine, a streaming engine, and a pipeline runner. Scale out to a driver/worker cluster over
Arrow Flight when you outgrow one machine — same binary, two subcommands.

**Pipelines are data, not code.** Declare a Kafka → bronze → silver → gold DAG in one YAML file
or in Spark Declarative Pipelines SQL — `oxidant pipeline run` or a stock `pyspark.pipelines`
client takes it from there: exactly-once micro-batches, checkpoints, expectations
(`drop` / `warn` / `fail`), incremental streaming tables, and materialized views that only
recompute when something upstream actually moved.

**Your tables stay open.** Writes land as Delta with Iceberg metadata published over the same
Parquet files — one copy of the data, readable by any engine, with no lock-in. Read and write
through the catalogs you already have: AWS Glue (with Lake Formation column/row security),
Hive, Unity/REST, or a plain local warehouse.

**Wire-compatible where it counts.** Oxidant serves the Spark Connect protocol: unmodified
PySpark, Spark SQL, and `spark-pipelines` clients connect to it today. It's a capability, not a
crutch — everything above works from the built-in CLI, REST API, and web UI without touching a
Spark client at all.

## Quickstart

```sh
# 1. Install (macOS + Linux, x86_64/arm64)
curl --proto '=https' --tlsv1.2 -LsSf \
  https://github.com/OxidantData/Oxidant/releases/latest/download/oxidant-installer.sh | sh
# or: brew install oxidantdata/tap/oxidant · or: docker pull ghcr.io/oxidantdata/oxidant

# 2. Start the engine (gRPC on 50051, Web UI + REST on 4040)
oxidant start --port 50051
```

```python
# 3. Query it from stock PySpark — no JVM on either side
from pyspark.sql import SparkSession          # pip install "pyspark-client>=4.0"
spark = SparkSession.builder.remote("sc://localhost:50051").getOrCreate()
spark.sql("SELECT 1 AS hello").show()
```

Or open the SQL editor + notebooks at **http://localhost:4040**, run `oxidant sql -e "SELECT 1"`,
or drive it from an LLM agent with `oxidant mcp`. Full walkthrough:
**[docs/getting-started.md](docs/getting-started.md)**.

## Declarative pipelines, two doors

The same DAG, your choice of surface — one engine underneath:

```sql
-- pipelines.sql, run by a stock spark-pipelines client (or pyspark.pipelines)
CREATE STREAMING TABLE orders_bronze
TBLPROPERTIES ('subscribe' = 'orders', 'kafka.bootstrap.servers' = 'b-1.msk:9092');

CREATE MATERIALIZED VIEW revenue_gold AS
SELECT region, sum(amount) AS revenue FROM orders_bronze GROUP BY region;
```

```yaml
# oxidant.yaml — same DAG, config-file door: oxidant pipeline run -c oxidant.yaml
pipeline: { name: sales, catalog: local, schema: live, trigger: 30 seconds, format: delta }
tables:
  - name: orders_bronze
    source: { format: kafka, options: { subscribe: orders } }
    expect: { amount_set: { check: "amount IS NOT NULL", action: fail } }
  - name: revenue_gold
    sql: SELECT region, sum(amount) AS revenue FROM orders_bronze GROUP BY region
```

Streaming ingest is exactly-once (write-ahead offset log + atomic Delta commits), AUTO CDC
(SCD 1) merges are built in, and every table lands in the catalog queryable by anything that
reads it. Details: **[docs/pipelines.md](docs/pipelines.md)** ·
**[docs/streaming.md](docs/streaming.md)**.

## What's in the box

| | |
|---|---|
| **Spark Connect server** | Stock PySpark / Spark SQL / `spark-pipelines` clients, unmodified |
| **Declarative pipelines** | SQL (`CREATE STREAMING TABLE`, `CREATE MATERIALIZED VIEW`, `CREATE FLOW`, `AUTO CDC`), Python `pyspark.pipelines`, or YAML — one engine |
| **Structured streaming** | Kafka sources, exactly-once, watermarks, expectations, checkpointed state |
| **Lakehouse I/O** | Delta (read + write), Iceberg (read + compat publish), Parquet/CSV/JSON |
| **Catalogs** | AWS Glue (+ Lake Formation column/row security), Hive, Unity/REST, local |
| **Interfaces** | Web UI (SQL editor, notebooks, monitoring) · REST statement API · CLI · MCP server |
| **Distributed** | Driver/worker cluster over Arrow Flight; fixed-size EC2 deploy via CloudFormation |
| **Performance** | Vectorized Arrow-native CPU core (Loom); self-hosted, reproducible ClickBench/TPC gates |

## Architecture (one screen)

```
PySpark / spark-pipelines / REST / CLI ──▶ oxidant-connect
                                              │
                     oxidant-plan (warp) ─ analyzer ─ optimizer (heddle) ─ physical
                                              │
                                       oxidant-loom (CPU)
                              vectorized Arrow, DataFusion → native
                                              │
                     oxidant-execution (local | driver/worker + Arrow Flight)
                                              │
                     oxidant-datasource (Parquet/Delta/Iceberg) ─ catalogs (Glue/Hive/Unity)
```

Everything between operators is Apache Arrow — no operator, present or planned, leaves it.
Deep dive: **[docs/architecture.md](docs/architecture.md)** · crate map:
**[docs/CODEMAP.md](docs/CODEMAP.md)**.

## Status

**Pre-alpha and runnable.** The engine executes SQL, streaming, and pipelines end-to-end today;
the Spark-SQL parity ratchet (2,900+ corpus queries passing strict) runs in CI on every PR.
Expect rough edges; the issue tracker is the honest list of them. Benchmarks are self-hosted
and reproducible — see the site for the current ClickBench/TPC numbers.

## Deploy

Free Community AMI on AWS Marketplace (listing in progress) ·
`docker pull ghcr.io/oxidantdata/oxidant` · fixed-size EC2 clusters:
**[docs/distributed-ec2.md](docs/distributed-ec2.md)** · self-hosted platform:
**[docs/deployment.md](docs/deployment.md)**.

## Contributing

Issues and PRs welcome — **[CONTRIBUTING.md](CONTRIBUTING.md)** covers the gates (fmt, clippy,
the parity ratchet, the benchmark suites). Good first contributions are tagged in the tracker.

## License

GNU Affero General Public License v3.0 — see [`LICENSE`](LICENSE). Commercial licensing:
[`COMMERCIAL.md`](COMMERCIAL.md). Trademark policy: [`TRADEMARK.md`](TRADEMARK.md).

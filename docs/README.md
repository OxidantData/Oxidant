# Oxidant docs

Oxidant is a drop-in Apache Spark replacement: it speaks the Spark Connect gRPC protocol, so
unmodified PySpark and Spark SQL clients connect with a one-line URL change — no JVM.

**Start here:** [`getting-started.md`](getting-started.md) — install, run, and your first query
in a few minutes.

## User guides

| Doc | Purpose |
|-----|---------|
| [getting-started.md](getting-started.md) | Install/run (binary or Docker) and first query via Web UI, `oxidant sql`, or PySpark Connect |
| [web-ui.md](web-ui.md) | Monitoring UI tour, SQL editor, and notebooks on `:4040` |
| [api.md](api.md) | REST statement API reference (`/api/v1/statements`, cluster status, authenticated `/api/status`) with curl examples |
| [config.md](config.md) | `oxidant.yaml` reference: catalogs (including a local catalog over directories), engine tuning, config precedence |
| [pipelines.md](pipelines.md) | Declarative Kafka → lakehouse table DAG run by the binary (`oxidant pipeline`) |
| [cli.md](cli.md) | `oxidant sql` command-line SQL client reference |
| [sql-writes.md](sql-writes.md) | Writing tables from SQL: `CREATE TABLE … USING delta AS SELECT`, `INSERT INTO`/`OVERWRITE`, per-format support |
| [mcp.md](mcp.md) | `oxidant mcp` MCP server setup for Claude Desktop / Cursor + tool reference |
| [workers.md](workers.md) | Adding workers: single-node default, local-cluster, multi-host, Docker |
| [catalogs-glue.md](catalogs-glue.md) | AWS Glue Data Catalog end-to-end (IAM, config, CTAS, troubleshooting) |
| [catalogs-lakeformation.md](catalogs-lakeformation.md) | AWS Lake Formation column/row security on Glue tables (identity modes, fail-closed behavior, security boundary) |
| [catalogs-unity.md](catalogs-unity.md) | Unity Catalog over its Iceberg REST surface (exact config, verified behavior, and the column-mapping gap that blocks reads) |
| [streaming.md](streaming.md) | Structured Streaming: Kafka → live Delta tables in Glue, readable as Iceberg |
| [catalogs.md](catalogs.md) | External catalog SPI overview (Hive / Glue / REST) and bring-your-own catalog |

## Deployment & operations

| Doc | Purpose |
|-----|---------|
| [distributed-ec2.md](distributed-ec2.md) | EC2 ASG data plane: Packer AMI + CloudFormation, Glue on EC2 |
| [deployment.md](deployment.md) | Self-hosted platform deploy outline (control plane + EKS data plane) |
| [runtime-contract.md](runtime-contract.md) | Engine image env contract (K8s/EndpointSlice discovery, spill, tuning) |

## Contributor / internals

| Doc | Purpose |
|-----|---------|
| [architecture.md](architecture.md) | Engine design: Loom vectorized core, HVM backend, Connect path |
| [CODEMAP.md](CODEMAP.md) | Crate / bench / deploy ownership map |
| [databricks-coverage.md](databricks-coverage.md) | Databricks SQL manual coverage matrix: statements / functions / types / operators, with probe evidence |
| [databricks-functions.md](databricks-functions.md) | Exact builtin-function coverage: oxidant's live registry vs. all 606 documented Databricks functions. **Generated** — regenerate with `oxidant-parity functions --markdown`, never hand-edit |
| [databricks-parity-plan.md](databricks-parity-plan.md) | Phased plan for Databricks SQL parity on Glue + Lake Formation (epic KAN-89..KAN-108) |

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
| [api.md](api.md) | REST statement API reference (`/api/v1/statements`, cluster status) with curl examples |
| [cli.md](cli.md) | `oxidant sql` command-line SQL client reference |
| [mcp.md](mcp.md) | `oxidant mcp` MCP server setup for Claude Desktop / Cursor + tool reference |
| [workers.md](workers.md) | Adding workers: single-node default, local-cluster, multi-host, Docker |
| [catalogs-glue.md](catalogs-glue.md) | AWS Glue Data Catalog end-to-end (IAM, config, CTAS, troubleshooting) |
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

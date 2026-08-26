# Getting started

Install the Oxidant engine, run the server, then run your first query three ways: the
Web UI, the `oxidant sql` CLI, or a stock PySpark client.

## Install Oxidant

Pick whichever fits your platform — every path installs the same `oxidant` binary
(except Docker, which needs no install). Prebuilt binaries cover Apple Silicon and
Intel Macs plus x86_64 and arm64 Linux (glibc). The Docker image ships with sample
tables preloaded, so it is the zero-setup path.

### 1. Shell installer (macOS + Linux)

```sh
curl --proto '=https' --tlsv1.2 -LsSf https://github.com/OxidantData/Oxidant/releases/latest/download/oxidant-installer.sh | sh
```

Installs `oxidant` into `~/.cargo/bin` (or `$CARGO_HOME/bin` if set); the script
prints the exact path and how to add it to `PATH`. This path installs the binary
only — for the sample tables, also grab the standalone archive (see
[Sample data](#sample-data)):

```sh
curl --proto '=https' --tlsv1.2 -LsSf https://github.com/OxidantData/Oxidant/releases/latest/download/sample-data.tar.gz | tar -xz
```

### 2. Homebrew (macOS + Linux)

```sh
brew install oxidantdata/tap/oxidant
```

### 3. Debian / Ubuntu (.deb)

Download `oxidant_<ver>_amd64.deb` (or `_arm64.deb`) from
[GitHub Releases](https://github.com/OxidantData/Oxidant/releases/latest), then:

```sh
sudo dpkg -i oxidant_<ver>_amd64.deb
```

The package installs `oxidant` to `/usr/bin` (sample tables included, auto-discovered
from `/usr/share/oxidant/sample-data`). Each release also attaches an `.rpm`
(`sudo dnf install ./oxidant-<ver>-1.x86_64.rpm`). The Linux binaries are built in
manylinux_2_34 containers and need **glibc ≥ 2.34** (RHEL 9+, Amazon Linux 2023,
Ubuntu 22.04+, Debian 12+, Fedora 36+) — on older distro glibcs (Debian 11 and
below) use the Docker image below.

### 4. Docker (no install, sample data included)

```sh
docker run --rm -p 4040:4040 -p 50051:50051 ghcr.io/oxidantdata/oxidant:latest
```

- The monitoring UI, SQL editor, notebook, and REST API listen on <http://localhost:4040>.
- Spark Connect gRPC listens on `sc://localhost:50051`.
- A `samples` schema (TPC-H tables in four formats) is preloaded — see
  [Sample data](#sample-data) below.

Open <http://localhost:4040>, go to the **SQL Editor**, and run:

```sql
SELECT count(*) FROM samples.tpch_nation   -- 25
```

### 5. Build from source

Rust 1.90 is pinned by `rust-toolchain.toml` and installs automatically via rustup. No
`protoc` needed.

```sh
git clone https://github.com/OxidantData/Oxidant.git
cd Oxidant
cargo build -p oxidant-cli        # binary at ./target/debug/oxidant

./target/debug/oxidant start --port 50051 --sample-data sample-data
```

`--sample-data sample-data` points at the committed sample tables in the repo (same contents
as the Docker image); drop the flag for a clean server without the `samples` schema.

### Coming soon: AWS AMI / Marketplace

A free Community AMI on AWS Marketplace is in progress (listing pending) — until
then, use any install path above on an EC2 instance. Fixed-size EC2 clusters via
CloudFormation are documented in [distributed-ec2.md](distributed-ec2.md).

> **Glue catalog users:** no AWS CLI needed — the engine uses `aws-sdk-glue` in-process with
> the standard AWS credential chain (env vars, shared config, instance role / IRSA). See
> [catalogs-glue.md](catalogs-glue.md).

## Run the server

```sh
oxidant start --port 50051
```

(From a source build: `./target/debug/oxidant start --port 50051`.)

The server is a **daemon**: `start` spawns it detached, waits until it answers, and prints
where it went.

```text
oxidant started (pid 57235)
  spark connect:  sc://0.0.0.0:50051
  ui + rest:      http://127.0.0.1:4040
  log:            ~/.local/share/oxidant/run/oxidant.log
  pidfile:        ~/.local/share/oxidant/run/oxidant.pid
```

Three more commands drive it, and starting twice is safe — it reports the running one and
spawns nothing:

```sh
oxidant status     # pid, uptime, ports, log path, health probe
oxidant stop       # SIGTERM, then SIGKILL if it will not go
oxidant restart    # same flags, new process
```

`oxidant status` is written for scripts: it exits `0` when the server is running and healthy,
`3` when it is not running, and `4` when the process is alive but not answering.

```text
oxidant is running
  pid:            57235
  uptime:         2 hours
  spark connect:  sc://0.0.0.0:50051
  ui + rest:      http://127.0.0.1:4040
  health:         ok (single-node, version 0.2.0)
  log:            ~/.local/share/oxidant/run/oxidant.log
  pidfile:        ~/.local/share/oxidant/run/oxidant.pid
  flags:          --port 50051
```

> **Running under a supervisor?** systemd, Docker and CI harnesses already own the process, so
> they take the other door: `oxidant spark server … --foreground` runs in the foreground and
> writes no pidfile. A bare `oxidant spark server` with neither is refused, because that is how
> orphaned engines accumulate. Same for `oxidant worker --foreground`.

Useful server flags — they are the same for `start` and for `spark server --foreground`
(full text: `oxidant` with no args):

```text
oxidant start --port <PORT> [--ui-port <PORT>] [--ui-bind <ADDR>] [--no-ui]
              [--mode local|local-cluster] [--workers <N|host:port,...>]
              [--sample-data <DIR>] [--catalog-conf key=value]...
```

- `--ui-port` — HTTP port for the UI + REST API (default `4040`).
- `--ui-bind` — interface for the UI (default `0.0.0.0`; use `127.0.0.1` on shared hosts — the UI has no auth).
- `--no-ui` — disable the HTTP UI + REST API entirely.
- `--workers host:port,...` — attach remote Flight workers; the driver then routes
  distributable queries across them (static list — see [workers.md](workers.md)).
- `--mode local-cluster --workers N` — embed N in-process workers instead of remote ones
  (see [workers.md](workers.md)). With no workers at all, the driver runs every query itself.
- `--sample-data` — register a sample-data tree as the `samples` schema at startup (env: `OXIDANT_SAMPLE_DATA_DIR`).
- `--catalog-conf` — register an external catalog at startup (see [catalogs-glue.md](catalogs-glue.md)).

## Sample data

The bundled sample data is TPC-H SF 0.01, all 8 tables, in four physical formats — same rows
in every format. Tables live in the `samples` schema of the built-in `spark_catalog` catalog:

| Format | Tables |
|---|---|
| Parquet (primary) | `samples.tpch_{nation,region,supplier,customer,part,partsupp,orders,lineitem}` |
| CSV | same 8, suffixed `_csv` (e.g. `samples.tpch_nation_csv`) |
| Delta Lake | `samples.tpch_{nation,customer,orders,lineitem}_delta` |
| Apache Iceberg | `samples.tpch_{nation,customer,orders,lineitem}_iceberg` |

Row counts: nation 25, region 5, supplier 100, customer 1500, part 2000, partsupp 8000,
orders 15000, lineitem 60175. The data files are committed under
[`sample-data/`](../sample-data/README.md) in the repo (~19 MB) and baked into the Docker
image at `/opt/oxidant/sample-data`.

Most binary installs ship the same tree, and the server auto-discovers it at startup
with no flags at all: release tarballs carry `sample-data/` next to the binary, the
`.deb`/`.rpm` place it at `/usr/share/oxidant/sample-data`, and Homebrew installs it
under the formula prefix at `share/oxidant/sample-data`. The curl|sh installer is the
one exception — it installs the binary only, so point the server at the standalone
archive (above) with `--sample-data sample-data`. `--sample-data` /
`OXIDANT_SAMPLE_DATA_DIR` override the discovery; with no bundled tree installed (e.g. a
source build), the server starts clean with no `samples` schema, as before.

## First query — pick a client

### 1. Web UI SQL editor

Open <http://localhost:4040>, go to the **SQL Editor** page, type:

```sql
SELECT count(*) FROM samples.tpch_nation
```

Press **Cmd/Ctrl+Enter** — the results table renders below, and the statement appears in the
recent-statements list. More in [web-ui.md](web-ui.md).

### 2. `oxidant sql` CLI

```sh
oxidant sql -e "SELECT count(*) FROM samples.tpch_nation"
```

Point at a non-default server with `--url http://host:4040` or `OXIDANT_URL`. Formats, files,
and stdin: [cli.md](cli.md).

### 3. PySpark (Spark Connect)

Install the stock PySpark Connect client (pure Python, no JVM):

```sh
pip install "pyspark-client>=4.0"
```

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.remote("sc://localhost:50051").getOrCreate()
spark.sql("SELECT count(*) FROM samples.tpch_nation").show()
```

## Smoke-test SQL quirk (pre-alpha)

`range(5)` returns a column named `range().value`, not Spark's conventional `id`, so
`SELECT id FROM range(5)` errors. Prefer plain `SELECT` literals or `VALUES` in smoke tests:

```sql
SELECT 1 AS hello;
SELECT * FROM VALUES (1, 'a'), (2, 'b') AS t(num, letter);
```

Also, `CREATE DATABASE` / `CREATE SCHEMA` via SQL is not implemented yet (the Spark Catalog
RPC is unimplemented) — schemas arrive via external catalogs or `--sample-data`, and tables
via `CREATE TABLE` / `CREATE EXTERNAL TABLE`.

## Next steps

- [web-ui.md](web-ui.md) — monitoring pages, SQL editor, notebooks
- [api.md](api.md) — run SQL over REST (`POST /api/v1/statements`)
- [mcp.md](mcp.md) — drive Oxidant from Claude Desktop / Cursor via MCP
- [workers.md](workers.md) — scale out beyond single-node
- [catalogs-glue.md](catalogs-glue.md) — query tables in the AWS Glue Data Catalog

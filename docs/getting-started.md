# Getting started

Run the Oxidant engine, then run your first query three ways: the Web UI, the `oxidant sql`
CLI, or a stock PySpark client.

## Option A — Docker (no build)

```sh
docker run -p 50051:50051 -p 4040:4040 ghcr.io/oxidantdata/oxidant
```

- Spark Connect gRPC listens on `sc://localhost:50051`.
- The monitoring UI, SQL editor, notebook, and REST API listen on <http://localhost:4040>.

## Option B — build the binary from source

Rust 1.90 is pinned by `rust-toolchain.toml` and installs automatically via rustup. No
`protoc` needed.

```sh
git clone https://github.com/OxidantData/Oxidant.git
cd Oxidant
cargo build -p oxidant-cli        # binary at ./target/debug/oxidant

./target/debug/oxidant spark server --port 50051
```

The server starts Spark Connect gRPC on `50051` and the HTTP UI + REST API on `4040`:

```text
Oxidant Spark Connect server listening on sc://0.0.0.0:50051
Oxidant UI at http://0.0.0.0:4040
```

Useful server flags (full text: `oxidant` with no args):

```text
oxidant spark server --port <PORT> [--ui-port <PORT>] [--ui-bind <ADDR>] [--no-ui]
                     [--mode local|local-cluster] [--workers <N>]
                     [--catalog-conf key=value]...
```

- `--ui-port` — HTTP port for the UI + REST API (default `4040`).
- `--ui-bind` — interface for the UI (default `0.0.0.0`; use `127.0.0.1` on shared hosts — the UI has no auth).
- `--no-ui` — disable the HTTP UI + REST API entirely.
- `--mode local-cluster --workers N` — embed N in-process workers (see [workers.md](workers.md)).
- `--catalog-conf` — register an external catalog at startup (see [catalogs-glue.md](catalogs-glue.md)).

## First query — pick a client

### 1. Web UI SQL editor

Open <http://localhost:4040>, go to the **SQL Editor** page, type:

```sql
SELECT 1 AS hello
```

Press **Cmd/Ctrl+Enter** — the results table renders below, and the statement appears in the
recent-statements list. More in [web-ui.md](web-ui.md).

### 2. `oxidant sql` CLI

```sh
oxidant sql -e "SELECT 1 AS hello"
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
spark.sql("SELECT 1 AS hello").show()
```

## Smoke-test SQL quirk (pre-alpha)

`range(5)` returns a column named `range().value`, not Spark's conventional `id`, so
`SELECT id FROM range(5)` errors. Prefer plain `SELECT` literals or `VALUES` in smoke tests:

```sql
SELECT 1 AS hello;
SELECT * FROM VALUES (1, 'a'), (2, 'b') AS t(num, letter);
```

## Next steps

- [web-ui.md](web-ui.md) — monitoring pages, SQL editor, notebooks
- [api.md](api.md) — run SQL over REST (`POST /api/v1/statements`)
- [mcp.md](mcp.md) — drive Oxidant from Claude Desktop / Cursor via MCP
- [workers.md](workers.md) — scale out beyond single-node
- [catalogs-glue.md](catalogs-glue.md) — query tables in the AWS Glue Data Catalog

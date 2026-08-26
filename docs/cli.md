# `oxidant sql` — command-line SQL client

`oxidant sql` runs SQL two ways: **in-process by default**, and against a running server's
[REST API](api.md) when you point it at one.

```text
oxidant sql [-c <config.yaml>] [--url http://host:4040] (-e "<sql>" | -f <file.sql> | stdin)
            [--format table|csv|json] [--timeout <secs>]
```

## Embedded (the default)

With no `--url` and no `OXIDANT_URL`, the statement runs **in this process**. No server is
started and no port is bound: the catalogs declared in [`oxidant.yaml`](config.md) are bridged
into a local engine and the query executes there.

```sh
# Query a directory of Parquet/Delta/Iceberg/CSV/JSON files, with nothing running
oxidant sql -c oxidant.yaml -e "SELECT count(*) FROM local.live.orders"

# Zero config at all still works
oxidant sql -e "SELECT 1 AS hello"
```

The config file is found by `--config` / `-c`, then `$OXIDANT_CONFIG`, then `./oxidant.yaml`.
An explicit `--config` path that does not exist is an error rather than a silent fallback — a
typo must not quietly run your statement against different catalogs.

## Against a running server

Passing `--url`, or setting `OXIDANT_URL`, sends the statement to that server's REST API
instead. Long-running statements are polled until they finish; `--timeout` caps the total wait
(default 300 seconds, after which the command exits non-zero while the statement keeps running
server-side — cancel it via the API or Web UI).

```sh
oxidant sql --url http://driver.internal:4040 -e "SELECT count(*) FROM glue.oxidant_demo.orders"
```

`--timeout` applies only to this path; an embedded statement runs to completion locally.

## SQL input

Exactly one source per invocation:

```sh
# Inline
oxidant sql -e "SELECT 1 AS hello"

# From a file
oxidant sql -f queries/daily-report.sql

# From stdin
echo "SELECT 1 AS hello" | oxidant sql
```

## Output formats

```sh
# Default: aligned table
oxidant sql -e "SELECT * FROM VALUES (1,'a'),(2,'b') AS t(num, letter)"

# CSV — pipe into other tools
oxidant sql -e "SELECT * FROM VALUES (1,'a'),(2,'b') AS t(num, letter)" --format csv

# JSON — one object per row, script-friendly
oxidant sql -e "SELECT 1 AS hello" --format json | jq .
```

## Point at a remote server

```sh
oxidant sql --url http://driver.internal:4040 -e "SELECT count(*) FROM glue.oxidant_demo.orders"

# or persist it for the shell session — note this switches every later `oxidant sql`
# in the shell to the remote path, embedded execution included
export OXIDANT_URL=http://driver.internal:4040
oxidant sql -e "SELECT 1 AS hello"
```

## Examples

```sh
# Quick engine smoke test (see the range() note in getting-started.md)
oxidant sql -e "SELECT 1 AS hello"

# Catalog exploration against Glue
oxidant sql -e "SHOW DATABASES IN glue"
oxidant sql -e "SHOW TABLES IN glue.oxidant_demo"

# Save a result as CSV
oxidant sql -f report.sql --format csv > report.csv
```

## Exit behavior

- A `succeeded` statement prints rows in the selected format and exits `0`.
- A `failed` statement prints the server-side error to stderr and exits non-zero, so
  `oxidant sql -e ... && next-step` works in scripts.

## Other subcommands

| Command | What it does |
|---|---|
| `oxidant start` / `stop` / `status` / `restart` | Daemon control for the Spark Connect server (`sc://host:port`) plus the web UI. `start` is idempotent; `status` exits `0` running / `3` stopped / `4` alive-but-not-answering |
| `oxidant spark server --foreground` | The same server in the foreground, for a supervisor that owns the process (systemd, Docker, CI). **Bare `oxidant spark server` is refused** — long-lived processes are daemons. A release build also refuses to be the *second* server on the machine; set `OXIDANT_SINGLE_INSTANCE=0` to allow it (or `=1` to enforce it in a debug build) |
| `oxidant worker --foreground` / `oxidant driver` | A distributed Flight worker (supervised, same rule), and a two-stage distributed aggregation across workers (one-shot: it runs a query and exits) |
| `oxidant history-server` | Serves completed application event logs |
| `oxidant mcp` | stdio MCP server over the same REST statement API |
| `oxidant pipeline (run\|validate\|show)` | Builds, checks or prints the table DAG in `oxidant.yaml` — see [pipelines.md](pipelines.md) |
| `oxidant pipeline reconcile` | Read-only drift report between a `postgres_cdc` source and the lakehouse tables it feeds. `--table <NAME>` scopes it, `--sample <KEYS>` widens the key walk (default 10,000), `--cron '<EXPR>'` registers a schedule (`--cron off` clears it). **Exits 0 in sync, 1 on drift, 2 when it could not run** (unreachable publisher, a `--table` that names nothing, a key type the walk refuses), so `reconcile || page_the_data_team` does not page for a network blip — see [postgres-cdc.md](postgres-cdc.md) §4 |

## Server flags: `--sample-data` (bundled sample tables)

`oxidant start` accepts `--sample-data <DIR>` (env: `OXIDANT_SAMPLE_DATA_DIR`). When
set, the server registers the sample-data tree at `<DIR>` as the `samples` schema of the
built-in `spark_catalog` catalog at startup — parquet tables under their bare names
(`samples.tpch_nation`, …), CSV as `…_csv`, Delta as `…_delta`, Iceberg as `…_iceberg`.
Registration is best-effort: a missing directory or unreadable table is logged and skipped,
never a boot failure.

When neither the flag nor the env var is set, the server auto-discovers a bundled tree,
checking in order: `<dir of the oxidant binary>/sample-data` (release tarballs),
`<binary dir>/../share/oxidant/sample-data` (prefix installs — e.g. Homebrew), and
`/usr/share/oxidant/sample-data` (deb/rpm). The curl|sh installer installs the binary
only — download `sample-data.tar.gz` from the release and pass `--sample-data` there. The first directory that exists and contains a
`parquet/` subdir wins; with no match there is no `samples` schema and no behavior change.
So binary installs (curl tarball / deb) load the `samples` schema with zero flags —
pass `--sample-data` / `OXIDANT_SAMPLE_DATA_DIR` to override, and omit both for a clean
server.

```sh
# From a repo checkout (the committed tree is ./sample-data):
./target/debug/oxidant start --port 50051 --sample-data sample-data

# Or via the environment (this is how the Docker image preloads it):
OXIDANT_SAMPLE_DATA_DIR=/opt/oxidant/sample-data oxidant start --port 50051
```

Then from this CLI: `oxidant sql -e "SELECT count(*) FROM samples.tpch_nation"`. See
[getting-started.md](getting-started.md#sample-data) for the table list and
[sample-data/README.md](../sample-data/README.md) for regeneration.

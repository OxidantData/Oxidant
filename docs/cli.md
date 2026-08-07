# `oxidant sql` — command-line SQL client

`oxidant sql` runs SQL against a running server's [REST API](api.md) — a lightweight
alternative to the [Web UI editor](web-ui.md) and to a full PySpark client.

```text
oxidant sql [--url http://localhost:4040] (-e "<sql>" | -f <file.sql> | stdin)
            [--format table|csv|json] [--timeout <secs>]
```

The server URL comes from `--url`, or from the `OXIDANT_URL` environment variable; the default
is `http://localhost:4040`. Long-running statements are polled until they finish; `--timeout`
caps the total wait (default 300 seconds, after which the command exits non-zero while the
statement keeps running server-side — cancel it via the API or Web UI).

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

# or persist it for the shell session
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

## Server flags: `--sample-data` (bundled sample tables)

`oxidant spark server` accepts `--sample-data <DIR>` (env: `OXIDANT_SAMPLE_DATA_DIR`). When
set, the server registers the sample-data tree at `<DIR>` as the `samples` schema of the
built-in `spark_catalog` catalog at startup — parquet tables under their bare names
(`samples.tpch_nation`, …), CSV as `…_csv`, Delta as `…_delta`, Iceberg as `…_iceberg`.
Registration is best-effort: a missing directory or unreadable table is logged and skipped,
never a boot failure.

When neither the flag nor the env var is set, the server auto-discovers a bundled tree,
checking in order: `<dir of the oxidant binary>/sample-data` (release tarballs and the
curl|sh installer), `<binary dir>/../share/oxidant/sample-data` (prefix installs), and
`/usr/share/oxidant/sample-data` (deb/rpm). The first directory that exists and contains a
`parquet/` subdir wins; with no match there is no `samples` schema and no behavior change.
So binary installs (curl tarball / deb) load the `samples` schema with zero flags —
pass `--sample-data` / `OXIDANT_SAMPLE_DATA_DIR` to override, and omit both for a clean
server.

```sh
# From a repo checkout (the committed tree is ./sample-data):
./target/debug/oxidant spark server --port 50051 --sample-data sample-data

# Or via the environment (this is how the Docker image preloads it):
OXIDANT_SAMPLE_DATA_DIR=/opt/oxidant/sample-data oxidant spark server --port 50051
```

Then from this CLI: `oxidant sql -e "SELECT count(*) FROM samples.tpch_nation"`. See
[getting-started.md](getting-started.md#sample-data) for the table list and
[sample-data/README.md](../sample-data/README.md) for regeneration.

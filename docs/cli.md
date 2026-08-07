# `oxidant sql` — command-line SQL client

`oxidant sql` runs SQL against a running server's [REST API](api.md) — a lightweight
alternative to the [Web UI editor](web-ui.md) and to a full PySpark client.

```text
oxidant sql [--url http://localhost:4040] (-e "<sql>" | -f <file.sql> | stdin)
            [--format table|csv|json]
```

The server URL comes from `--url`, or from the `OXIDANT_URL` environment variable; the default
is `http://localhost:4040`.

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

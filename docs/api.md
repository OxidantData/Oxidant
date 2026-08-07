# REST API

The server's HTTP port (default `4040`) hosts a small REST API for running SQL statements and
checking cluster state — no Spark client needed. Base URL below is `http://localhost:4040`.

> **No auth.** These endpoints are unauthenticated. Bind the UI port to loopback on shared
> hosts (`--ui-bind 127.0.0.1`), or disable HTTP with `--no-ui`.

## Endpoints

| Method | Path | Purpose |
|--------|------|---------|
| `POST` | `/api/v1/statements` | Submit a SQL statement (async, or synchronous with `?wait=true`) |
| `GET` | `/api/v1/statements` | Recent statements, newest first |
| `GET` | `/api/v1/statements/{id}` | Statement status, and error/rowCount/schema when done |
| `GET` | `/api/v1/statements/{id}/result` | Result rows as JSON or CSV |
| `POST` | `/api/v1/statements/{id}/cancel` | Cancel a pending/running statement |
| `GET` | `/api/v1/cluster/status` | Cluster mode, workers, engine version |

## Submit a statement

```sh
curl -s -X POST http://localhost:4040/api/v1/statements \
  -H 'Content-Type: application/json' \
  -d '{"sql": "SELECT 1 AS hello"}'
```

`202 Accepted`:

```json
{"statementId": "9f2c1a…", "status": "pending"}
```

To block until the statement reaches a terminal state instead of polling:

```sh
curl -s -X POST "http://localhost:4040/api/v1/statements?wait=true&timeout=60" \
  -H 'Content-Type: application/json' \
  -d '{"sql": "SELECT count(*) AS n FROM parquet.`hits.parquet`"}'
```

`timeout` is in seconds; on expiry you get the statement back in its last known state and can
keep polling.

## Check status

```sh
curl -s http://localhost:4040/api/v1/statements/9f2c1a…
```

```json
{
  "statementId": "9f2c1a…",
  "status": "succeeded",
  "rowCount": 1,
  "schema": {"fields": [{"name": "hello", "type": "integer"}]}
}
```

`status` is one of `pending`, `running`, `succeeded`, `failed`, `canceled`. `failed`
statements carry an `error` field; `succeeded` statements carry `rowCount` and `schema`.

List recent statements (newest first):

```sh
curl -s http://localhost:4040/api/v1/statements
```

## Fetch results

```sh
# JSON (default)
curl -s "http://localhost:4040/api/v1/statements/9f2c1a…/result?format=json&limit=100"

# CSV (text/csv)
curl -s "http://localhost:4040/api/v1/statements/9f2c1a…/result?format=csv"
```

JSON shape:

```json
{
  "schema": {"fields": [{"name": "hello", "type": "integer"}]},
  "rows": [{"hello": 1}],
  "rowCount": 1,
  "truncated": false
}
```

`truncated` is `true` when `limit` cut the result short — re-fetch with a higher `limit`.

## Cancel

```sh
curl -s -X POST http://localhost:4040/api/v1/statements/9f2c1a…/cancel
```

## Cluster status

```sh
curl -s http://localhost:4040/api/v1/cluster/status
```

```json
{
  "mode": "single-node",
  "workers": [],
  "version": "0.x.y"
}
```

`mode` is `single-node`, `local-cluster`, or `distributed`; `workers` lists the connected
worker endpoints (see [workers.md](workers.md)).

## Full async flow (submit → poll → result → cancel)

```sh
# 1. Submit
ID=$(curl -s -X POST http://localhost:4040/api/v1/statements \
  -H 'Content-Type: application/json' \
  -d '{"sql": "SELECT * FROM VALUES (1,'"'"'a'"'"'),(2,'"'"'b'"'"') AS t(num, letter)"}' \
  | jq -r .statementId)

# 2. Poll until terminal
until curl -s http://localhost:4040/api/v1/statements/$ID | jq -e '.status | IN("succeeded","failed","canceled")' >/dev/null; do
  sleep 1
done

# 3. Read the result
curl -s "http://localhost:4040/api/v1/statements/$ID/result?format=json" | jq .

# 4. Cancel a still-running statement (when polling shows pending/running)
curl -s -X POST http://localhost:4040/api/v1/statements/$ID/cancel
```

The [`oxidant sql`](cli.md) CLI and the [MCP server](mcp.md) are thin clients over this same
API.

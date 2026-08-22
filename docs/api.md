# REST API

The server's HTTP port (default `4040`) hosts a small REST API for running SQL statements and
checking cluster state — no Spark client needed. Base URL below is `http://localhost:4040`.

> **No auth.** The `/api/v1` endpoints are unauthenticated. Bind the UI port to loopback on
> shared hosts (`--ui-bind 127.0.0.1`), or disable HTTP with `--no-ui`. The one exception is
> [`/api/status`](#driver-status), which requires a bearer token and is off by default.

## Endpoints

| Method | Path | Purpose |
|--------|------|---------|
| `POST` | `/api/v1/statements` | Submit a SQL statement (async, or synchronous with `?wait=true`) |
| `GET` | `/api/v1/statements` | Recent statements, newest first |
| `GET` | `/api/v1/statements/{id}` | Statement status, and error/rowCount/schema when done |
| `GET` | `/api/v1/statements/{id}/result` | Result rows as JSON or CSV |
| `POST` | `/api/v1/statements/{id}/cancel` | Cancel a pending/running statement |
| `GET` | `/api/v1/cluster/status` | Cluster mode, workers, engine version |
| `GET` | `/api/status` | Driver status for a control plane — **bearer token required** |

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

## Driver status

`GET /api/status` is the operational endpoint a control plane polls to decide when to
auto-terminate or autoscale a cluster. It is served by the same HTTP listener as everything
above, on both a single-node `oxidant spark server` and the driver of a distributed cluster
(the driver *is* an `oxidant spark server` — see [distributed-ec2.md](distributed-ec2.md)).

**It is disabled unless `OXIDANT_STATUS_TOKEN` is set**, and requests must present that token
as a bearer credential:

| `OXIDANT_STATUS_TOKEN` | `Authorization` header | Response |
|------------------------|------------------------|----------|
| unset, or empty        | anything               | `404 Not Found` — the route does not exist |
| set                    | missing or wrong       | `401 Unauthorized` + `WWW-Authenticate: Bearer` |
| set                    | `Bearer <token>`       | `200 OK` |

The token is compared in constant time. Restart the server to change it.

```sh
OXIDANT_STATUS_TOKEN=$(openssl rand -hex 32) oxidant spark server --port 50051
curl -s http://localhost:4040/api/status -H "Authorization: Bearer $OXIDANT_STATUS_TOKEN"
```

```json
{
  "version": "0.x.y",
  "uptime_secs": 123,
  "last_query_at": "2026-08-22T01:23:45.678Z",
  "active_queries": 0,
  "queued_queries": 0,
  "queries": [
    {
      "id": "9f2c1a…",
      "tag": "SELECT count(*) FROM events",
      "state": "finished",
      "started_at": "2026-08-22T01:23:45.101Z",
      "duration_ms": 577,
      "rows": 1,
      "bytes": 0
    }
  ]
}
```

| Field | Meaning |
|-------|---------|
| `uptime_secs` | Seconds since the driver process started |
| `last_query_at` | Most recent query **start or finish**, RFC3339, `null` before the first query. With `active_queries: 0` this is the "idle since" timestamp auto-termination keys off |
| `active_queries` | Queries running right now, counted across every query the driver still remembers |
| `queued_queries` | Always `0` — the engine admits queries on arrival and has no queue. The field exists so a poller keeps working if one is added |
| `queries` | The 20 most recent queries, newest first. `?limit=N` returns up to 200; the counters above are never truncated |
| `queries[].tag` | The query's description: its truncated SQL, or `DataFrame` for a Connect plan |
| `queries[].state` | `running`, `finished`, or `failed` |
| `queries[].rows` | Output rows reported by the last stage to finish (0 while running) |
| `queries[].bytes` | Bytes shuffled across the query's stages — `0` for a single-node query, which never shuffles |

Every field comes from the same query lifecycle events that back the [monitoring UI](web-ui.md);
nothing is sampled or estimated separately.

### Trust model

The token authenticates the *caller*, not the transport. Oxidant serves plain HTTP, so a
bearer token on the wire is only as private as the network under it, and a poller that trusts
the response is trusting the network too. Treat `/api/status` as a **network-perimeter**
endpoint:

- Keep the driver's HTTP port inside a private subnet or a security group that admits only the
  control plane — the same requirement the unauthenticated `/api/v1` routes already impose.
- Do not expose port 4040 to the internet, with or without a token.
- Terminate TLS at a proxy in front of the driver if the poll crosses an untrusted hop.
- Give each cluster its own token so a leak is scoped to one cluster.

The token itself is redacted from `/api/v1/applications/{id}/environment`, which otherwise
echoes every `OXIDANT_*` variable.

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

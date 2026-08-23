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
| `GET` | `/api/v1/logs` | The driver's recent log lines — an in-memory ring buffer of the last 1000 |
| `GET` | `/api/dashboards` | Dashboards, newest-updated first |
| `POST` | `/api/dashboards` | Create a dashboard |
| `GET` | `/api/dashboards/{id}` | One dashboard document |
| `PATCH` | `/api/dashboards/{id}` | Update name, widgets, layout or refresh interval |
| `DELETE` | `/api/dashboards/{id}` | Delete a dashboard |
| `GET` | `/api/status` | Driver status for a control plane — **bearer token required** |
| `GET` | `/api/v1/pipelines` | Streaming pipelines with a connector log on this driver — **bearer token required** |
| `GET` | `/api/v1/pipelines/{name}/logs` | Tail of a streaming connector's JSONL log — **bearer token required** |

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

## Dashboards

A dashboard is a JSON document: a [react-grid-layout](https://github.com/react-grid-layout/react-grid-layout)
`layout` array plus a list of widget specs, each of which is a SQL statement the browser runs
through the statement API above. The server stores and validates them; it never executes a
widget's SQL itself. See [web-ui.md](web-ui.md#dashboards) for the page and the
SQL-to-chart convention.

```sh
curl -s -X POST http://localhost:4040/api/dashboards \
  -H 'content-type: application/json' -d '{
    "name": "Sales overview",
    "widgets": [
      {"id": "w1", "type": "bar", "title": "Revenue by region",
       "sql": "SELECT region, revenue FROM sales", "options": {"stacked": false}}
    ],
    "layout": [{"i": "w1", "x": 0, "y": 0, "w": 6, "h": 8}],
    "refreshIntervalSecs": 30
  }'
```

```json
{
  "id": "d3f1…",
  "name": "Sales overview",
  "widgets": [{"id": "w1", "type": "bar", "title": "Revenue by region", "sql": "…", "options": {}}],
  "layout": [{"i": "w1", "x": 0, "y": 0, "w": 6, "h": 8}],
  "refreshIntervalSecs": 30,
  "createdAtMs": 1787405314595,
  "updatedAtMs": 1787405314595
}
```

`type` is one of `bar`, `line`, `area`, `pie`, `scatter`, `table`, `kpi`; anything else is a
`400` naming the accepted set. Empty SQL, an empty name, duplicate widget ids, unknown fields
and a `layout` entry pointing at no widget are refused the same way, with
`{"error": "..."}` explaining which. `PATCH` replaces only the fields it is given; sending
`"refreshIntervalSecs": null` turns auto-refresh off, while omitting the key leaves it alone.
`DELETE` answers `204`.

Documents live under `OXIDANT_DASHBOARD_DIR` (default `$XDG_DATA_HOME/oxidant/dashboards`,
else `~/.oxidant/dashboards`), one file per dashboard.

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

The same token gates [the pipeline list](#pipelines) and [connector logs](#connector-logs);
a leak is a leak of all three.

## Pipelines

`GET /api/v1/pipelines` lists the streaming connectors that have written a log under
`<OXIDANT_CHECKPOINT_DIR>/logs`, newest write first. It is the closest thing to a
streaming-query registry this port exposes, and it is what the
[**Pipelines** page](web-ui.md#pipelines) enumerates before tailing each log.

```sh
curl -s http://localhost:4040/api/v1/pipelines \
  -H "Authorization: Bearer $OXIDANT_STATUS_TOKEN"
```

```json
{
  "pipelines": [
    {"name": "orders_live", "sizeBytes": 48213, "modifiedMs": 1755912345678},
    {"name": "clicks", "sizeBytes": 1204, "modifiedMs": 1755912100000}
  ],
  "truncated": false
}
```

| Field | Meaning |
|-------|---------|
| `name` | The connector's name — the value to pass as `{name}` to [the tail route](#connector-logs) |
| `sizeBytes` | Size of the live log file. Not growing between polls is a connector that is not doing anything |
| `modifiedMs` | Last write, epoch milliseconds on the **driver's** clock, or `null` where the filesystem reports none |
| `truncated` | `true` when more than 200 logs were found and the list was cut |

Only `<name>.jsonl` is listed. Rotated generations (`<name>.jsonl.1` …) are history, not
pipelines, and a name the tail route would reject is skipped here too — the list never offers a
row that `400`s when followed.

Same gate as [`/api/status`](#driver-status), and the same 404s as the tail route (no token, no
`OXIDANT_CHECKPOINT_DIR`, no `logs/` directory). The one difference: a `logs/` directory that
exists but is empty answers `200` with an empty list, because "there are no pipelines" is a
different fact from "this driver cannot tell you", and the UI says different things about them.

> **Streaming work is not in the execution store.** `oxidant-streaming` does not register a
> micro-batch with the observability store, so `/api/v1/applications/{app}/sql`, `/jobs` and
> `/stages` contain no streaming executions at all. The connector log is the only per-batch
> record this API serves. See [web-ui.md](web-ui.md#pipelines).

## Connector logs

`GET /api/v1/pipelines/{name}/logs?tail=N` returns the tail of one streaming connector's own
log — the `postgres_cdc` connector's slot lifecycle, a Kafka reader's rebalances, the messages
that exist nowhere else because a failed micro-batch records *that* it failed, not why. The
**Pipelines** page in the [monitoring UI](web-ui.md#pipelines) shows it in a pipeline's detail
drawer.

Connectors append one JSON object per line to `<checkpoints>/logs/<name>.jsonl`, alongside the
offset and commit logs they already keep. `OXIDANT_CHECKPOINT_DIR` names that checkpoint root —
set it to the same absolute path as `pipeline.checkpoints` in
[`oxidant.yaml`](config.md). Nothing here writes: the endpoint only reads what a connector left
behind.

It is guarded by **the same bearer token as [`/api/status`](#driver-status)** — a connector log
names slots, tables and hosts, so it is operational data, not monitoring decoration.

| Situation | Response |
|---|---|
| `OXIDANT_STATUS_TOKEN` unset or empty | `404` — the route does not exist |
| token set, credential missing or wrong | `401` + `WWW-Authenticate: Bearer` |
| `OXIDANT_CHECKPOINT_DIR` unset | `404` |
| `<checkpoints>/logs` does not exist | `404` |
| no `<name>.jsonl` in it | `404` |
| `name` is not a plain `[A-Za-z0-9._-]` filename | `400` |
| otherwise | `200` |

Every absence is the same `404` on purpose: a caller learns "there is nothing here" and nothing
more, and the UI reads it as "this driver serves no connector logs" and hides the section.

```sh
curl -s "http://localhost:4040/api/v1/pipelines/orders_live/logs?tail=5" \
  -H "Authorization: Bearer $OXIDANT_STATUS_TOKEN"
```

```json
{
  "name": "orders_live",
  "tail": 5,
  "events": [
    {"ts": "2026-08-22T01:23:45.101Z", "level": "info", "event": "slot_created", "slot": "orders_slot"},
    {"ts": "2026-08-22T01:23:46.882Z", "level": "info", "event": "snapshot_complete", "rows": 120400}
  ],
  "malformed": 0,
  "truncated": false
}
```

| Field | Meaning |
|-------|---------|
| `events` | The parsed lines, **oldest first** — newest last, the order a log reads in |
| `tail` | The count actually applied: `?tail=N` defaults to 100 and is clamped to 1000 |
| `malformed` | Lines that were not valid JSON. A log being appended to right now normally has none; they are counted rather than dropped silently or failed on |
| `truncated` | `true` when the file was larger than the 1 MiB window read back from its end. The tail still ends at the newest record; only older history is out of view |

The read is a bounded window on the end of the file, so the cost is the same whether the log is
2 KiB or 2 GiB. Event *shape* is the connector's, not this endpoint's — it parses JSON and does
not interpret it.

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

# REST API

The server's HTTP port (default `4040`) hosts a small REST API for running SQL statements and
checking cluster state — no Spark client needed. Base URL below is `http://localhost:4040`.

> **No auth.** Most `/api/v1` endpoints are unauthenticated. Bind the UI port to loopback on
> shared hosts (`--ui-bind 127.0.0.1`), or disable HTTP with `--no-ui`. The exceptions are the
> routes marked **bearer token required** below — [`/api/status`](#driver-status), the
> [pipeline list](#pipelines), a [connector log](#connector-logs) and the whole
> [log browser](#driver-logs) — which share one token and are off by default.

## Endpoints

| Method | Path | Purpose |
|--------|------|---------|
| `POST` | `/api/v1/statements` | Submit a SQL statement (async, or synchronous with `?wait=true`) |
| `GET` | `/api/v1/statements` | Recent statements, newest first |
| `GET` | `/api/v1/statements/{id}` | Statement status, and error/rowCount/schema when done |
| `GET` | `/api/v1/statements/{id}/result` | Result rows as JSON or CSV |
| `POST` | `/api/v1/statements/{id}/cancel` | Cancel a pending/running statement |
| `GET` | `/api/v1/cluster/status` | Cluster mode, workers, engine version |
| `GET` | `/api/dashboards` | Dashboards, newest-updated first |
| `POST` | `/api/dashboards` | Create a dashboard |
| `GET` | `/api/dashboards/{id}` | One dashboard document |
| `PATCH` | `/api/dashboards/{id}` | Update name, widgets, layout or refresh interval |
| `DELETE` | `/api/dashboards/{id}` | Delete a dashboard |
| `GET` | `/api/status` | Driver status for a control plane — **bearer token required** |
| `GET` | `/api/v1/pipelines` | Streaming pipelines with a connector log on this driver — **bearer token required** |
| `GET` | `/api/v1/pipelines/{name}/logs` | Tail of a streaming connector's JSONL log — **bearer token required** |
| `GET` | `/api/v1/logs` | One page of a node's logs — the memory ring, or a file with `?file=`, filtered by `?level=`/`?target=`/`?q=`/`?from=`/`?to=` and paged by `?before=` — **bearer token required** |
| `GET` | `/api/v1/logs/files` | Every log file a node still has, newest period first — **bearer token required** |
| `GET` | `/api/v1/logs/workers` | The nodes that can be browsed, and which are answering — **bearer token required** |
| `GET` | `/api/v1/logs/tail` | SSE follow of a node's log, under the same filters — **bearer token required** |
| `POST` | `/api/v1/logs/dump` | Assemble a bounded diagnostic bundle — **bearer token required** |
| `GET` | `/api/v1/logs/dump/{id}` | Collect the bundle — **bearer token required** |

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

Every statement also carries:

- `source` — `rest` for this API, `connect` for a PySpark / Spark Connect `ExecutePlan`. The two
  now share one rail, so a `spark.sql(...)` a client ran over gRPC is listed here too
  (issue #134).
- `tier` — `hot` (live: results in memory, still cancellable) or `history` (replayed from the
  durable journal after a restart, or aged out of memory: listable and inspectable, but
  `POST …/cancel` answers `409` and `…/result` answers `410`).
- `clientOperationId` — the client's own `operation_id`, when it supplied one matching
  `^[A-Za-z0-9._:-]{1,128}$`. It is an alias only: `statementId` is always engine-minted
  `stmt-<uuid>` and is the only identity that reaches a filesystem path.
- `history: "degraded"` — present on a `?wait=true` response only when the statement's terminal
  record could not be made durable within `OXIDANT_HISTORY_ACK_TIMEOUT_MS`. Its absence is the
  promise that what you were just told is on disk.

List recent statements (newest first). Statements survive a restart when history is on
(`OXIDANT_HISTORY`, see [runtime-contract.md](runtime-contract.md)):

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

**`410 result_expired` (new).** `404` still means "no such statement id" and `409`
still means "not succeeded yet"; `410 {"error":"result_expired"}` is a third answer, meaning the
statement is known and succeeded but its rows are gone — it was replayed from the journal after a
restart, or it aged past `OXIDANT_HISTORY_HOT_TTL_SECS`. Reading rows back off disk is the next
increment of the durability work; today this is the honest answer rather than an empty result set.

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

With durable statement history on (the default — see
[runtime-contract.md](runtime-contract.md)), six more fields are flattened into the same object.
They are **absent entirely** under `OXIDANT_HISTORY=off`:

| Field | Meaning |
|-------|---------|
| `history_writes` | `ok` or `degraded`, aggregated over three subsystems: the statement journal, the result spill writer, and the disk sweep. Each is sticky until a success *of its own* clears it, and none needs a restart to flip back |
| `history_dropped_events` | Work history gave up on under backpressure — journal records the writer had no room for, plus spill jobs the spill queue had no room for. Neither loses a statement |
| `results_on_disk_bytes` | Total size of the spilled results under `history/results/` |
| `result_writes` | The spill writer alone: `degraded` once a spill was refused by the disk or dropped, cleared by the next spill that lands |
| `result_write_failures` | Spills the disk refused outright (ENOSPC/EIO/…) |
| `disk` | `ok`; `over_budget` when the engine's own subtree is past `OXIDANT_DISK_MAX_BYTES` with nothing left to prune; or `low_free` when a volume holding a managed directory is below `OXIDANT_DISK_MIN_FREE_BYTES`. `low_free` pauses result spill and deletes **nothing** — it is very often a co-tenant's shortfall. `over_budget` wins when both hold |

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

The same token gates [the pipeline list](#pipelines), [connector logs](#connector-logs) and
[the driver's log buffer](#driver-logs); a leak is a leak of all four.

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

`{name}` chooses a filename, never a path: it must match `[A-Za-z0-9._-]` and may not start
with a dot, and the file it names must be a **regular file inside** `<checkpoints>/logs` — a
symlink is refused (`404`) even when it points at a readable file, and the resolved path is
checked against the resolved logs directory. The listing applies the same rule, so everything
`GET /api/v1/pipelines` offers is tailable and nothing else is. A `logs` directory that is
itself a symlink is fine: that is the operator's own configuration, and it becomes the
boundary.

## Driver logs

Six routes, one token, and one question asked of any node in the cluster: *what did this process
log?* They are what the [**Observability** page](web-ui.md#observability) is built on.

| Route | Answers |
|---|---|
| `GET /api/v1/logs` | one page — [the ring](#driver-logs), [a file](#rolled-files-file), [filtered and cursor-paged](#filters-and-the-backward-cursor) |
| `GET /api/v1/logs/files` | [what files exist](#the-file-listing-apiv1logsfiles) |
| `GET /api/v1/logs/workers` | [which nodes can be browsed](#worker-logs-through-the-driver-workerid) |
| `GET /api/v1/logs/tail` | [SSE follow](#tail-follow-apiv1logstail) |
| `POST /api/v1/logs/dump`, `GET /api/v1/logs/dump/{id}` | [a support bundle](#diagnostic-dumps) |

**Logs stay where they are written.** Every node writes its own files; browsing another node's
is a *read* federated at query time (`?worker=<id>`), never a copy onto the driver's disk. The
one exception is the diagnostic dump, and it is a route of its own so that it has to be asked
for.

With no parameters at all, `GET /api/v1/logs` returns the node's in-memory `tracing` ring buffer
— the last 1000 lines, oldest first.

```sh
curl -s http://localhost:4040/api/v1/logs -H "Authorization: Bearer $OXIDANT_STATUS_TOKEN"
```

```json
{"logs": ["2026-08-23T14:00:00.500Z [INFO] oxidant_connect::rest - message=listening on 0.0.0.0:4040"]}
```

Every line leads with an **RFC-3339 UTC timestamp**. That is a change: the lines used to start
at `[LEVEL]`, and a line with no time in it gives a rolled log no column to filter on.

It is guarded by **the same bearer token as [`/api/status`](#driver-status)**, with the same
three answers (`404` unset, `401` wrong, `200` right). The buffer captures every event at every
enabled level *including field values*, so it names hosts, slots, tables and query text; and
this port is served under a permissive CORS layer, which means an ungated buffer is readable
cross-site by any origin an operator's browser visits. It is served by the engine's REST
router, so a standalone history server has no buffer and answers `404` whatever the token says.

### Rolled files: `?file=`

The ring holds minutes. `?file=` reads the durable exec log the engine writes under
`$OXIDANT_DATA_DIR/logs/` — `OXIDANT_LOG_KEEP_DAYS` of it, default 30 (see
[the runtime contract](runtime-contract.md)). Same route, same token.

```sh
# the live file on disk, rather than the memory ring
curl -s 'http://localhost:4040/api/v1/logs?file=current' -H "Authorization: Bearer $TOKEN"
# one rolled UTC day
curl -s 'http://localhost:4040/api/v1/logs?file=2026-08-23' -H "Authorization: Bearer $TOKEN"
# the second size split of one UTC hour, under OXIDANT_LOG_ROLL=hourly
curl -s 'http://localhost:4040/api/v1/logs?file=2026-08-23-14.2' -H "Authorization: Bearer $TOKEN"
```

```json
{
  "file": "2026-08-23",
  "format": "parquet",
  "dedup": true,
  "offset": 0,
  "limit": 1000,
  "next_offset": 1000,
  "logs": ["…"]
}
```

```text
file := "current"
      | YYYY "-" MM "-" DD          [ "." N ]        # daily
      | YYYY "-" MM "-" DD "-" HH   [ "." N ]        # hourly
      | YYYY "-W" ww                [ "." N ]        # weekly, ISO year + ISO week
YYYY := 4DIGIT   MM,DD,HH,ww := 2DIGIT   N := 2..999
```

- **Names are UTC** and carry no offset; `ww` is the ISO week, so a week spanning New Year is
  one file. `.N` is the size-split sequence and appears only on the second and later files of a
  period.
- **You never name an extension.** The server serves the `.parquet` if the conversion has run
  and the `.log` if it has not, and reports which in `format`. Anything outside the grammar —
  an extension, `..`, a `/`, an absolute path, `2026-8-23` — is `400 invalid file`, never a
  `404`: the value is parsed into a typed period and the filename is *reconstructed* from it,
  so no caller-supplied string ever reaches a path join. A well-formed period with no file on
  disk is the `404`.
- **`dedup`** says whether the file collapsed repeated lines (`OXIDANT_LOG_DEDUP`, on by
  default) into `… repeated N times`. The memory ring does not dedup, so the same window can
  read differently through the two; the file is authoritative. An *unfiltered, uncursored*
  no-`?file=` read has no `dedup` key and is byte-identical to what it was before — see
  [the cursor bullet below](#filters-and-the-backward-cursor) for what a filter changes.
- A node with no rolling writer (`OXIDANT_LOG_ROLL=off`, or `OXIDANT_HISTORY=off`) answers
  `404` with a reason, for every `?file=` value.
- **The answer is one page, always.** `?limit=` (default **1000**, the same number the ring
  serves; clamped to **10,000**) and `?offset=` (default 0) walk the file, and the response
  echoes both plus `next_offset` — the offset of the next page, or `null` at the end of the
  file. A rolled file can hold `OXIDANT_LOG_MAX_FILE_BYTES` (256 MiB, ~2M lines); serving one
  whole would build every line in memory and then serialise a second copy into the body.
  **Follow `next_offset` rather than counting lines**: a page is also cut short by an internal
  byte budget, so it can come back with fewer than `limit` lines and still have a successor.
- **The line strings a converted file returns are normalized, not byte-verbatim.** `format`
  tells you which form you got. A `.log` is served as written; a `.parquet` is *reconstructed*
  from its columns, so `message=` leads unless the original had it elsewhere, the remaining
  fields keep the order they were rendered in, and a value the field parser read as a `k=v`
  pair inside a message (`"failed to bind, addr=0.0.0.0:4040"`) comes back as the same string
  but is stored as `message` + a field. Time, level, target and text are preserved either way.

```sh
# page through a big day
curl -s 'http://localhost:4040/api/v1/logs?file=2026-08-23&offset=2000&limit=500' \
  -H "Authorization: Bearer $TOKEN"
```

### Filters and the backward cursor

`?offset=` walks a file from the top. That is the right answer for "show me this file" and the
wrong one for the question an operator actually arrives with — *the errors from
`oxidant_execution` in the last hour, out of a 256 MiB day*. Four filters compose, and a cursor
pages **backward from the newest line**:

| Parameter | Means |
|---|---|
| `level=` | `error`, `warn`, `info`, `debug` or `trace` — a **floor**, not an equality. `level=warn` keeps `warn` *and* `error` |
| `target=` | `tracing` target **prefix**: `target=oxidant_execution` matches `oxidant_execution::stage` |
| `q=` | free text over the rendered line, case-insensitive |
| `from=`, `to=` | RFC-3339 instants, matched against the `ts` column. **Half-open** — `from=T&to=T+1h` and the next hour's `from=T+1h` tile a day with no line served twice |
| `before=` | the cursor: serve the lines *before* this row index |
| `order=desc` | ask for the newest-first page without passing a filter to imply it |

```sh
# the last hour's warnings and errors out of yesterday's rolled day
curl -s 'http://localhost:4040/api/v1/logs?file=2026-08-23&level=warn&target=oxidant_execution' \
     -G --data-urlencode 'from=2026-08-23T13:00:00Z' --data-urlencode 'to=2026-08-23T14:00:00Z' \
     -H "Authorization: Bearer $TOKEN"
```

```json
{
  "file": "2026-08-23", "format": "parquet", "dedup": true, "limit": 500,
  "before": null, "next_before": 41822,
  "logs": ["…"]
}
```

- **The cursor chooses the shape.** A `before=`, an `order=desc`, or *any* filter gives you the
  newest-first page and `next_before`; nothing at all gives you PR3's oldest-first `?offset=`
  page, byte-identical to what it answered before this route grew filters. Two shapes on one
  route is the price of not breaking a released contract, and which you get is decided by what
  you asked for rather than by a version flag.
- **`next_before` is a row index from the start of the named file**, and `null` when the page
  reached the start. Log files are append-only and conversion preserves row order one for one,
  so a cursor minted against `oxidant-2026-08-23.log` still names the same line after the
  background converter has replaced it with `oxidant-2026-08-23.parquet`. **Follow it rather
  than counting**: a page is also cut short by an internal byte budget.
- **The memory ring is the one exception, and it says so.** A no-`?file=` page carries
  `"cursor": "best-effort"`; a file's page carries no `cursor` key at all. The ring is not
  append-only — it *rolls*, so every line the node logs between two requests shifts every index
  by one, and a `before=` walk over it repeats lines at one end and loses them at the other. The
  ring holds 1000 lines and one page holds 10,000, so **read it whole rather than paging it**
  (the Observability pane does exactly that, and hides its *Load older lines* button); if you
  want a cursor that names the same line tomorrow, ask for `?file=current`.
- **`?limit=` clamps at 10,000 on every log route**, including the no-`?file=` ring read. PR3's
  read path had no cap: `?file=current` on a 256 MiB file built every line in memory and then
  serialised a second copy into the body, on an endpoint the Observability page polls.
- **A filter never hides a line it cannot judge.** A line no parser could decompose — something
  else wrote it into `logs/`, or a `tracing` value that was already two physical lines — has no
  level and no timestamp, and dropping it from a `level=error` page would lose the *tail* of the
  multi-line error you were searching for. So an unjudgeable line passes `level`, `from` and
  `to`, and is still judged by `q` and `target`, which read the rendered string it does have.
- **A bad filter is a `400` naming the value**, never a silent no-op: a `level=warining` that
  quietly matched nothing reads as "there were no warnings".
- **Pushdown, and what it does not buy.** On a `.parquet` file, `from`/`to` prune whole row
  groups from the footer statistics, and `level`/`target` are evaluated against a three-column
  projection (`ts, level, target`) before `message`/`fields_json` are decoded at all. `q` is
  free text over the *rendered* line, so it cannot be pushed down and is applied last — a
  `q`-only query still decodes every candidate group, which is the honest cost of a substring
  search and exactly what `grep` would have paid on the text file.

### The file listing: `/api/v1/logs/files`

```sh
curl -s http://localhost:4040/api/v1/logs/files -H "Authorization: Bearer $TOKEN"
```

```json
{
  "dir": "/var/lib/oxidant/driver-4040/logs",
  "dedup": true,
  "files": [
    {"file": "current",    "rolled": false, "format": "text",    "size_bytes": 1048576,
     "first_ts": "2026-08-24T00:00:01.004Z", "last_ts": "2026-08-24T09:14:22.881Z"},
    {"file": "2026-08-23", "rolled": true,  "format": "parquet", "size_bytes": 20971520,
     "first_ts": "2026-08-23T00:00:00.512Z", "last_ts": "2026-08-23T23:59:58.117Z"}
  ]
}
```

`file` is the `?file=` value that reads it. The listing is a **directory read**, not a computed
range — the visible history is always honestly what exists, so a file retention took is simply
absent rather than offered and then `404`ing. Ordering is by `(period end, split)` and never
lexicographic: `2026-08-23.2` is the *newer* generation of that period even though `'2' < 'l'`.
`first_ts`/`last_ts` are `null` for a file whose first or last line carries no parseable
timestamp — a guess would be worse. The live file's bounds are read from its two ends only, so
listing a 256 MiB file does not read 256 MiB.

### Worker logs, through the driver: `?worker=<id>`

**Workers write the same files**, under their own `$OXIDANT_DATA_DIR` — `oxidant worker` runs
the same process-level logging init, which is the whole point of hoisting it out of the REST
router a standalone worker never builds. Reading them goes through the driver, and **no worker
log bytes touch the driver's disk**: the driver forwards the query, forwards one bounded page
back, and keeps nothing. The one exception is [the diagnostic dump](#diagnostic-dumps), which
says so in its own name.

```sh
# which nodes can be browsed, and which are answering
curl -s http://localhost:4040/api/v1/logs/workers -H "Authorization: Bearer $TOKEN"
# the same query, against one of them
curl -s 'http://localhost:4040/api/v1/logs?worker=10.0.0.7:50051&file=current&level=error' \
  -H "Authorization: Bearer $TOKEN"
```

```json
{"workers": [
  {"worker_id": "driver",           "address": null,               "reachable": true,  "error": null},
  {"worker_id": "10.0.0.7:50051",   "address": "http://10.0.0.7:50051", "reachable": true,  "error": null},
  {"worker_id": "10.0.0.8:50051",   "address": "http://10.0.0.8:50051", "reachable": false, "error": "timed out"}
]}
```

- `?worker=` takes a **worker id** — `host:port`, the same string `/api/v1/cluster/status`
  prints — and it is matched against this driver's own configured workers. It is never an
  address you supply: this route's token is one an operator hands to a monitoring page, and a
  `?worker=` that named an arbitrary host would turn the driver into a request forwarder for
  anything its network can reach. An unknown id is a `404` listing the ids that exist. `driver`
  (or no `?worker=` at all) is this node.
- **"Configured" means the deployment, not the session.** The log routes —
  `?worker=`, `/api/v1/logs/workers`, `/api/v1/logs/tail` and
  `POST /api/v1/logs/dump {"worker":"all"}` — resolve against `OXIDANT_WORKERS` /
  `OXIDANT_WORKER_SERVICE` (falling back to the boot `--workers` list), and deliberately **not**
  against `spark.oxidant.workers`. That key is [per-session worker pinning](workers.md) for
  *query routing*, it lives in one process-wide map, and the Spark Connect port that writes it
  is unauthenticated — so honouring it here would let any client that can reach that port choose
  an address the driver dials, and would silently drop the real nodes out of the picker and out
  of a support bundle. Pin a session's workers all you like; the log browser still shows you the
  cluster.
- **A worker that does not answer is listed `reachable: false` with the reason, never silently
  skipped**, and a query against it is a `502` (or `504` past 30 s) naming the node — never an
  empty page. "No errors on worker 2" and "worker 2 is dead" must not read alike, because the
  second is the thing you were looking for.
- **A worker's own refusal keeps its status.** Its `400 invalid file` comes back as a `400`, not
  flattened into a `502`, so you can tell which node objected and to what.
- **The transport is a Flight action**, not a second HTTP surface on the worker. A worker speaks
  Flight and nothing else; an HTTP listener would mean a second port to open, a second bind to
  configure, a second CORS decision and a second place to get the token gate right. The Flight
  interconnect is a **trusted network boundary** — it already accepts arbitrary stage SQL from
  anyone who can reach it, so serving that same peer a log page is not a new privilege — and
  keeping it off the public internet is the operator's job exactly as it was before. Driver and
  worker run *one* implementation of the query, so `level=warn` cannot come to mean two things.

### Tail-follow: `/api/v1/logs/tail`

Server-sent events, taking the same filters (`level`, `target`, `q`, `from`, `to`, `worker`).

```sh
curl -N 'http://localhost:4040/api/v1/logs/tail?level=warn' -H "Authorization: Bearer $TOKEN"
```

```text
event: open
data: {"mode":"follow","worker":"driver","dedup":false}

event: line
data: 2026-08-24T09:14:22.881Z [WARN] oxidant_execution - message=spilling stage 4

event: dropped
data: {"dropped":37}
```

| Event | Data |
|---|---|
| `open` | `mode`, `worker`, `dedup`, and `poll_ms` when polled. Always first |
| `line` | one rendered line (driver) |
| `lines` | a JSON array of rendered lines (worker) |
| `dropped` | `{dropped: N}` — the fan-out fell behind this reader by N lines |
| `rolled` | the followed worker rolled its live file; re-read the listing and the page |
| `error` | `{error, status}` — a worker poll failed. **The stream stays open**: a worker restart is exactly when someone is watching, and the cursor does not move, so a worker that comes back resumes at the end of its file rather than replaying it |

- **The driver's tail is `tracing` itself, not a file poll.** The rolling writer's queue, its
  dedup hold and its 5 s timer all sit between an event and the file, so a follow that re-read
  `oxidant.log` would lag by that timer. It therefore reports `"dedup": false` — it is the
  *ring*'s view, and [the file is authoritative](#rolled-files-file).
- **A worker's tail is honestly a poll**, and `mode` says so: the driver re-asks the worker's
  `?file=current` every 2 s over Flight and forwards what is new. A long-lived Flight stream
  would pin a worker-side task to a browser tab. "New" is a **forward cursor** — a row index,
  so two identical lines a second apart are two events.
- **A tail follows exactly what you named, or it is a `400`.** One source per node is still
  being written to, and there is no silent substitution:

  | `?worker=` | `?file=` | |
  |---|---|---|
  | absent (driver) | absent (the ring) | **follows** — the driver's tail *is* the `tracing` stream the ring holds |
  | absent (driver) | `current` | **follows** |
  | a worker | `current` | **polls** every 2 s |
  | a worker | absent (the ring) | **`400`** — a rolling buffer has no forward cursor a poll could resume from, so an index into it names a different line every time the node logs one |
  | either | a rolled period | **`400`** — it will never grow again |

  The worker case used to override `file` to `current` regardless, so following a worker's
  memory ring painted a page from the ring and appended a tail from the worker's `oxidant.log`
  — two sources with different `dedup` under one `open` event claiming `"dedup": true` — and on
  an `OXIDANT_LOG_ROLL=off` worker it polled a file that does not exist, emitting an `error`
  every 2 s under a caption reading "following".
- **The first poll asks for a position, not a page.** It requests the rows *after the end* of
  the live file, which names the end and returns nothing: a follow emits only what arrives
  after you started following, exactly as the driver's own tail does. So the stream never
  repeats lines you already fetched with `/api/v1/logs`, and — because a scan position is not a
  match position — a selective `level=` cannot make a poll re-emit the matches it already sent.
- **The gap is never silent.** A reader the fan-out outruns is told exactly how many lines it
  lost, in its own stream.
- `EventSource` cannot carry an `Authorization` header. Read the stream with `fetch` and a
  `ReadableStream` (which is what the [Observability page](web-ui.md#observability) does) rather
  than moving the token into the query string, where it lands in proxy logs and `Referer`.

### Diagnostic dumps

**The one time log bytes move.** Everywhere else the driver federates reads; a support bundle is
the stated exception, and it is a separate, explicitly-named, token-guarded route rather than a
mode of the browser — an operator who copies a cluster's logs onto the driver's disk should have
had to say so.

```sh
ID=$(curl -s -X POST http://localhost:4040/api/v1/logs/dump \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"worker":"all","from":"2026-08-23T13:00:00Z","to":"2026-08-23T14:00:00Z","level":"warn"}' \
  | jq -r .dumpId)

# 202 while it assembles, 200 with the bundle when it is done
curl -s -o bundle.parquet -w '%{http_code}' \
  "http://localhost:4040/api/v1/logs/dump/$ID" -H "Authorization: Bearer $TOKEN"
```

```json
{"dumpId": "dump-3f2a…", "status": "building",
 "from": "2026-08-23T13:00:00.000Z", "to": "2026-08-23T14:00:00.000Z",
 "nodes": ["driver", "10.0.0.7:50051"], "maxBytes": 1073741824}
```

- The body is optional and every field is: `worker` (`all`, `driver`, or one worker id —
  default `all`), `from`/`to`, and the same `level`/`target`/`q` the browser takes. **Both
  instants absent means the last hour**, and the `202` echoes the window it used. With no
  default, an empty body would mean "every node, thirty days", which the cap would then refuse
  after minutes of Flight round-trips — a refusal that is correct and useless.
- **The window decides which files are opened**, not just which rows are kept: a node's file
  whose `first_ts`/`last_ts` (from [the listing](#the-file-listing-apiv1logsfiles)) put it
  wholly outside the window is never read, and on a `.parquet` file the row groups the window
  excludes are skipped on their footer statistics. So a one-hour bundle costs an hour, not the
  whole retention. This file-level rule is *coarser* than the row filter, deliberately: a rolled
  file whose every parseable timestamp is outside the window is skipped along with any
  unjudgeable line it holds, which are the continuations of lines that are themselves outside
  the window. A `?file=` read of that same file still serves them.
- **It answers `202` and assembles on a task.** Six nodes and a day is minutes of round-trips,
  and a client that gave up halfway would leave a half-written file with nobody to finish it.
  Collection has four answers, each a distinct fact: `202` still assembling, `200` here it is,
  the assembly's own failure with its reason, `404` no such dump.
- **The bundle is one Parquet with a `node` column** — `(node, ts, level, target, message,
  fields_json)` — not an archive of per-node files, so it is queryable as it stands:
  `SELECT * FROM dump WHERE level = 'ERROR' ORDER BY ts`. The rows are re-rendered through the
  same normalization a converted file already documents, so a bundle is faithful to what the
  browser shows rather than byte-identical to the file.
- **A node that could not be reached is named in the bundle**, and the dump still completes:
  each node contributes one `oxidant.dump` row, so
  `SELECT DISTINCT node, message FROM dump WHERE target = 'oxidant.dump'` is the manifest. A
  bundle that silently omitted the node that died would be worse than no bundle — the missing
  node is the one the case is about.
- **Bounded and refused, never truncated.** `OXIDANT_LOG_DUMP_MAX_BYTES` (default 1 GiB) plus
  the [disk budget and free-space floor](runtime-contract.md); a request that would breach
  either is a `507`, and a dump that breaches the cap mid-write is abandoned and reports the
  `507` on collection. A smaller bundle would be carried to a support case in the belief that it
  held the window that was asked for.
- **Dumps already assembling hold their headroom.** The check reserves the whole cap — the size
  of a bundle is not knowable until the logs have been read — and every dump still `building`
  counts as one such reservation, so N requests arriving together cannot each be admitted
  against the same free space. The `507` says how many are in flight.
- Bundles land in `$OXIDANT_DATA_DIR/dumps/` as `dump-<uuid>.parquet`, **expire after 24 h**,
  and are swept by the same prune pass that sweeps spilled results. The directory is resolved by
  the same code that resolves every other subtree under the data dir — so with
  `OXIDANT_DATA_DIR_PER_PROCESS=1` it is `$OXIDANT_DATA_DIR/<role>-<port>/dumps/`, the tree the
  sweeper prunes and the disk budget bills, and `$OXIDANT_DUMP_DIR` overrides it outright.

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

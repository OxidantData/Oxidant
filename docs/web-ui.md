# Web UI

When the server runs with the UI enabled (the default), open <http://localhost:4040>. The same
HTTP port serves the monitoring pages, the SQL editor, notebooks, and the
[REST API](api.md).

```sh
oxidant spark server --port 50051 --ui-port 4040
```

> **No auth.** The UI and REST API have no authentication or authorization. On shared or
> reachable hosts, bind loopback and tunnel instead:
>
> ```sh
> oxidant spark server --port 50051 --ui-bind 127.0.0.1
> ssh -L 4040:localhost:4040 user@host
> ```
>
> `--no-ui` disables the HTTP server entirely.
>
> The authenticated routes on this port are the operational ones —
> [`GET /api/status`](api.md#driver-status), the [pipeline list and connector
> logs](api.md#pipelines), and the whole [log browser](api.md#driver-logs). They share one
> token and stay off unless `OXIDANT_STATUS_TOKEN` is set.

## Two consoles

There are two front ends on this port, and which one you get is a deployment choice:

| | Served when | Pages |
|---|---|---|
| **Embedded console** | The default — nothing to build, nothing to install | Everything in the table below, including **Pipelines** and **Observability** |
| **Dashboards app** (`ui/`) | `OXIDANT_UI_DIR` points at a built `ui/dist` | Jobs, Stages, SQL, Executors, Environment, Cluster, Catalog, Editor, Notebook, **Dashboards** |

The embedded console is a single hand-written HTML file compiled into the binary
(`crates/oxidant-ui-server/src/embedded_ui.html`): it can import nothing, which is why
dashboards — a charting library, a grid engine and a query cache — live in the React app
instead. The two are not the same set of pages, and this document says which console carries
each one.

**[Pipelines](#pipelines) and [Observability](#observability) are embedded-console pages.** They
are absent from the React app: setting `OXIDANT_UI_DIR` swaps the embedded page out entirely,
so on that deployment those two pages are gone even though `GET /api/v1/pipelines` and
`GET /api/v1/logs` still answer on the same port. Convergence is the intent, not the state.
Until then: leave `OXIDANT_UI_DIR` unset to watch a streaming pipeline, or read the routes
directly — everything both pages show comes from [the REST API](api.md), by design.

Both consoles share one credential. The React app's **Cluster** page reads the gated
[`/api/v1/logs`](api.md#driver-logs), so its log pane takes the status token and stores it
under the same `oxidant.statusToken` key the embedded console uses — paste it into either and
both have it. Nothing else in the React app is gated.

## Monitoring pages

Carried by both consoles unless a row says otherwise. The UI mirrors the Spark UI layout and is
backed by the Spark-compatible `/api/v1/applications/...` endpoints, so existing tooling
against those routes works too.

| Page | What it shows |
|------|---------------|
| **Jobs** | One row per action/query execution: status, duration, stages |
| **Stages** | Stage detail — tasks, input/shuffle sizes, per-stage timing |
| **SQL** | Every SQL/DataFrame execution with its plan and duration |
| **Executors** | The driver plus any connected workers (see [workers.md](workers.md)) |
| **Environment** | Runtime info, Spark/Oxidant properties, catalog config |
| **Pipelines** | Streaming pipelines running right now — see [below](#pipelines). *Embedded console only* |
| **Observability** | Any node's logs, jobs expanded into their stages, and SQL executions on one screen — see [below](#observability). *Embedded console only* |
| **Dashboards** | Grids of SQL-backed widgets — see [below](#dashboards). *React app only* |

## Pipelines

The **Pipelines** page watches streaming pipelines while they run: one row per pipeline, with
its source, state, observed trigger interval, last batch and its rows and duration, rows/sec
over the last minute, the confirmed flush LSN and how much WAL its replication slot is holding.
Click a row for a detail drawer: batch history, slot and snapshot positions, connector
warnings, the error text, and the connector's own log tail.

### Streaming work is not in the execution store

A streaming micro-batch is **never registered with the observability store**. `oxidant-streaming`
does not depend on `oxidant-observability`; the scheduler runs each batch straight through
`Engine::execute_logical_plan` and records the result as an in-process `QueryProgress` on the
`StreamingQuery`. Nothing calls `QueryTracker::begin` for a batch.

So the **Jobs**, **Stages**, **SQL** and **Observability** pages hold no streaming work — not
"until the first batch lands", but permanently, and a page derived from them would be
permanently empty against a running `postgres_cdc` pipeline. Closing that gap means a per-batch
observer on `StreamingQueryManager` plus wiring in `oxidant-connect`; that is a change to the
streaming engine's contract, not to the UI.

Until it lands, this page reads **the connector's own JSONL log**, which is not a consolation
prize — it carries more than the execution store would have:

| Connector event | What the page reads from it |
|-----------------|-----------------------------|
| `batch` | Rows, duration, and the LSN range the batch covered. A `replay: true` line marks a re-read (`↻` in the drawer) |
| `slot_metrics` | WAL retained by the slot, replication lag, server flush LSN, reader position |
| `commit` | The confirmed flush LSN — and whether it was actually **announced to the publisher**, because a position committed locally but never sent leaves the slot growing |
| `snapshot_start` / `snapshot_done` | The introspected table list (which is where the `PostgresCDC[…]` source string comes from), backfill rows, and the consistent point |
| `schema_change`, `large_transaction`, `value_dropped` | The connector-warnings list in the drawer |
| `error` | The failure banner. `will_retry` is the difference between *retrying* and *stopped* |

### Where the numbers come from

| Source | What it contributes |
|--------|---------------------|
| [`/api/v1/pipelines`](api.md#pipelines) | Which pipelines exist. There is no streaming-query registry this server can reach, so the set of connector logs on disk is the registry |
| [`/api/v1/pipelines/{name}/logs`](api.md#connector-logs) | Everything in the table above. Tailed once per pipeline **per 5 s poll** — the interval the page's caption quotes, for the 12 most recently written |
| [`/api/status`](api.md#driver-status) | A cross-check that annotates and never overrules the connector log, consulted only where the log has no opinion. Its `tag` is a query's *description* — truncated SQL text, not a name — so a match must be a whole-tag streaming identity; matching a pipeline's name as a substring of SQL text reported healthy pipelines as stopped |

All three are bearer-token guarded, so **this page needs the status token** — without one it
says so rather than showing "no pipelines".

Three numbers are **observed rather than reported**, and the page labels them:

- **Trigger interval** — the median gap between batch starts, not a configured value.
- **Rows/sec** — rows in the last minute over the span those batches covered.
- **Liveness** — how long since the newest logged event. Those stamps are on the *driver's*
  clock and `now` is the browser's, so the "running" window is floored at 30 seconds rather
  than cut fine.

Batch numbers shown `#3` in the drawer's history are the page's ordinals over the log tail it
holds: the connector log records LSN ranges, not batch ids. The list's **Last batch** column
shows the newest batch's end LSN (or its timestamp) for that reason — a value that moves when
the pipeline does. **Sink is absent, not blank** — the connector log does not
record one, and the page does not guess.

A query started through Spark Connect `readStream()` has no checkpoint root to log under and so
will not appear; a pipeline declared in [`oxidant.yaml`](config.md) will.

### The status token and the checkpoint root

Paste `OXIDANT_STATUS_TOKEN` into the field at the bottom of the page; it is kept in this
browser's `localStorage` and sent to this driver only. The **Observability** page reuses the
same stored token.

The driver additionally needs `OXIDANT_CHECKPOINT_DIR` set to the pipeline checkpoint root —
the same absolute path as `pipeline.checkpoints` in [`oxidant.yaml`](config.md). Unset,
`/api/v1/pipelines` answers `404` and the page says the driver serves no connector logs.

## Observability

The **Observability** page is "what is this cluster doing right now", on three surfaces:

| Section | Source | Shows |
|---------|--------|-------|
| **Exec logs** | [the log browser](api.md#driver-logs) | Any node's logs on the terminal surface — see [the pane's controls](#the-log-pane) below |
| **Jobs & stages** | `/api/v1/applications/{app}/jobs` + `/stages` | One row per job — id, name, status, duration, stages done/total — expanding in place into its stages, with shuffle read/write where the stage reports any |
| **SQL queries** | `/api/v1/applications/{app}/sql` | Every execution the store holds — Spark Connect sessions, the REST statement API, the Editor — with status, duration and a row count |

Only the log pane has routes of its own; jobs, stages and SQL already ride the page's 2 s
refresh and the SSE event stream, and fetching them twice would double the load for no extra
freshness.

One number is **derived**, and the column says so: an execution's row count. The store records
a plan, a status and a duration for an execution but not its cardinality, so rows are summed
over the stages that execution's jobs ran.

Statements run from the **Editor** appear both here and in the Editor's own recent-statements
rail, and the two are not yet one query history —
[issue #134](https://github.com/OxidantData/oxidant/issues/134).

### The log pane

| Control | Does |
|---------|------|
| **Node** | Every node the driver can browse, `driver` first. **An unreachable worker stays in the list**, marked — dropping it would read as "there is no such node", which is not what happened |
| **File** | The memory ring, `current`, and every rolled generation this node still has, newest first, with its format and size. Picking a rolled file turns **Follow** off: following a file that will never grow again is not following anything |
| **Target** | `tracing` target prefix |
| **Search** | Free text over the whole file, not over the page already fetched |
| **From** / **To** | Your own wall clock, sent as RFC-3339 instants. `To` is exclusive, so two adjacent windows tile a day without a line appearing twice |
| **Level chips** | `error` / `warn` / `info` / `debug` — a **floor**, not four independent toggles: `warn` means warn *and* error, which is what [`level=`](api.md#filters-and-the-backward-cursor) means to `curl` |
| **Follow** | Tail-follow. Live on the driver, a 2 s poll against a worker, and the caption says which |
| **Pause** | Stops both the follow and the 5 s fallback poll |
| **Dump** | Copies the selected node's logs for the filtered window into a [support bundle](api.md#diagnostic-dumps) and downloads it — the one action on this page that moves log bytes |

Every filter is evaluated by the API over the whole file, not by the browser over what it
already has, and **Load older lines** walks the API's backward cursor — so a 256 MiB day is
readable without ever loading it. The pane stays pinned to the newest line unless you scroll up
into it. Following for a long time does not open a hole in the scroll-back: as the live page
fills, its oldest lines move into the scrolled-back pages with the cursor that belongs to them,
and when the browser eventually releases scroll-back it releases a whole page at a time, so
**Load older lines** fetches it again rather than skipping past it.

The **memory ring** is the one view with no **Load older lines**, and the line under the pane
says why: it is not a file but a 1000-entry buffer that rolls as the node logs, so its cursor
names a different line every time the node writes one. The pane asks for all 1000 lines in a
single page — the page cap is 10,000 — so there is nothing left to walk backward into. For a
history that still names the same line tomorrow, pick a file.

Four things the pane says out loud rather than hiding:

- **The token.** Every log route is gated by the same `OXIDANT_STATUS_TOKEN` as `/api/status`,
  because the files carry every logged field — hosts, slots, tables, query text. The pane sends
  the token the Pipelines page stores; without one it says so instead of showing an empty box.
- **A node with no rolled files.** `OXIDANT_LOG_ROLL=off` keeps durable statement history with
  stderr-only logs, so every `?file=` answers `404` while the memory ring still answers. The
  pane drops to the ring and the caption says why. `404` is also the answer when no status token
  is configured, and on a standalone history server, which carries no log routes at all.
- **Dedup.** The caption says `deduped` or `not deduped`. A file collapses an identical
  consecutive line into `… repeated N times`; the ring and the live tail do not. Without the
  label, a collapsed run reads as a gap.
- **Dropped lines.** If the tail's fan-out outruns the browser, the count of lost lines is in
  the caption rather than the jump being unexplained.

Streaming micro-batches are **not** on this page; see [Pipelines](#pipelines) for why.

## Theme

The UI carries the Oxidant brand theme from <https://www.oxidantdata.com>: dark-first
monochrome — layered near-blacks, hairline borders, Geist typography, and no decorative accent
colour. Emphasis comes from contrast and weight; the inverted white slab is what a primary
button gets instead of a coloured fill.

Both consoles share one component vocabulary, so a page here reads as a sibling of the
platform console rather than a different product:

| Component | Where it shows up |
|-----------|-------------------|
| **Chip** — a dot plus mono text on a raised pill | Every lifecycle state: job, stage, statement, executor, pipeline. The *dot* carries the hue, so a column of chips reads as words with a colour cue rather than a row of coloured labels |
| **ErrorState** — a tinted, hairlined banner with the raw text on the terminal surface | Every failure. Deliberately louder than a chip: an error is never just a colour change in a table cell |
| **EmptyState** — a dashed hairline, not a filled card | "Nothing here *yet*" — no jobs, no stages, no pipelines |
| **Card** with an eyebrow over its title | Every section |
| **Metric tiles** — hairline-separated cells sharing one border | Rows of numbers that belong together |
| **Detail drawer** — a right-hand sheet over a scrim | Pipeline detail. The only shadow in the UI, and it exists only while the sheet is open |
| **Filter chip** — a chip that is also a control | Log level filters. Off is a hairline outline, on is the raised slab; the dot keeps the level's hue in both, dimmed when off |

Colour is reserved for status, and only for status:

| Colour | Means |
|--------|-------|
| Green | Succeeded / completed |
| Amber | Running, pending, truncated results, a `warn` log line |
| Red | Failed — a failed job, a rejected statement, a stopped pipeline, an `error` log line |

`danger` and the chip tints are the only tokens the engine adds to the website's set: the
marketing site renders nothing that can fail or be in flight. Both are semantic, never
decorative.

A toggle in the header switches to the light theme. The choice persists in `localStorage`
under `oxidant-theme` and is applied before first paint, so there is no flash on reload.

Nothing is fetched from a CDN — no external fonts, no external assets — so the pages render
identically on a driver with no egress. The React build in [`ui/`](../ui) bundles Geist and
JetBrains Mono as self-hosted woff2 files; the single-file page compiled into the binary has no
asset pipeline, so it falls through to the system sans stack unless Geist is installed locally.

## SQL Editor

The **SQL Editor** page runs ad-hoc SQL over the REST statement API — no client install needed.

- Type SQL into the textarea, press **Cmd/Ctrl+Enter** (or the Run button).
- Results render as a table under the editor.
- A **recent statements** list (newest first) shows status — `pending`, `running`,
  `succeeded`, `failed`, `canceled` — with the error message for failures; click one to
  re-inspect its result.
- The rail is **one history for the whole engine**, not just for this page: a `spark.sql(...)`
  a PySpark client ran over Spark Connect is listed here too, badged `connect`
  ([issue #134](https://github.com/OxidantData/oxidant/issues/134)). Statements also survive a
  restart — they are replayed from the durable journal under `$OXIDANT_DATA_DIR` — so the rail
  after a driver restart shows what ran before it. A replayed statement can be inspected but no
  longer has its rows: fetching its result answers `410 result_expired`. `OXIDANT_HISTORY=off`
  restores the old volatile behaviour; see
  [runtime-contract.md](runtime-contract.md).

## Dashboards

The **Dashboards** page is a grid of SQL-backed widgets. Each widget is one statement, run
against this engine on demand through the same [statement API](api.md) the SQL Editor uses —
so a widget refresh appears on the **Jobs** and **SQL** pages like any other query, with the
same distributed routing.

- **List page** — every dashboard with its widget count and when it last changed. Name one and
  press **Create** to start.
- **View mode** — **Refresh** re-runs every widget; each card has its own **Refresh** too. The
  **Auto** dropdown sets a per-dashboard refresh interval (off, 5s … 15m) and is saved with the
  dashboard.
- **Edit mode** — drag a card by its title bar, resize from its bottom-right corner, **Add
  widget**, **Edit** or **Remove** one, then **Save**. **Cancel** discards the whole draft.

### Widgets

| Type | Draws |
|------|-------|
| **Bar** | One bar group per label; optionally stacked or horizontal |
| **Line** | One line per numeric column; optionally smoothed or stacked |
| **Area** | A line with its area filled |
| **Pie** | A donut of the first numeric column |
| **Scatter** | `[x, y]` points against two value axes |
| **Table** | Every column, sortable, paginated |
| **KPI counter** | A single number |

Funnel, gauge, sankey, heatmap, combo and pivot widgets — plus cross-filters, parameters,
scheduled refresh, share/embed and export — are [Oxidant Platform](../COMMERCIAL.md) features,
not part of the engine UI.

### How a result becomes a chart

One rule covers every widget type:

> **The first column labels the point. Every numeric column after it is a series.**

```sql
SELECT region, revenue, orders FROM sales GROUP BY region
--     ^label   ^series  ^series
```

- Column 1 is the category: the bar's label, the point on the line, the pie slice's name.
- Every column after it whose type is numeric becomes a series named after the column.
  Non-numeric trailing columns are ignored by charts; the **Table** widget shows everything.
- **Pie** uses only the first numeric column — a pie has one dimension. **Scatter** reads
  column 1 as the x value, falling back to the row number when it is not numeric.
- **KPI** takes the first numeric cell of the first row. A one-column, one-row result is used
  whatever its type, so `SELECT 'healthy' AS state` is a legal KPI.
- A lone numeric column is plotted against the row number.

NULL is absence, not zero:

| Where | Renders as |
|-------|------------|
| A value column | A gap — the line breaks, no bar is drawn |
| The label column | `∅` |
| A pie slice | Dropped |
| A table cell | `NULL`, and sorts last in **both** directions |

Whether a column counts as numeric comes from the Arrow type the statement API reports
(`Int64`, `Float64`, `Decimal128(10, 2)`, …), so an all-NULL `Int64` is still a series rather
than being mistaken for text. Widgets fetch at most **1000 rows** — aggregate in SQL rather
than shipping raw rows to a chart.

### Where dashboards are stored

There is no metadata database in the engine, so each dashboard is a JSON file under
`OXIDANT_DASHBOARD_DIR` (default `$XDG_DATA_HOME/oxidant/dashboards`, else
`~/.oxidant/dashboards`), written atomically. They are plain documents — check them into git,
copy them between servers, or edit them over
`GET`/`POST`/`PATCH`/`DELETE /api/dashboards`. If the directory cannot be written, dashboards
still work for the life of the process but are not persisted.

### Serving the dashboards page

The page compiled into the binary is a single self-contained HTML file, which cannot carry a
charting library or a grid engine. Dashboards therefore live in the React app under
[`ui/`](../ui); point the server at a build of it:

```sh
cd ui && npm install && npm run build
OXIDANT_UI_DIR=$PWD/dist oxidant spark server --port 50051 --ui-port 4040
```

Unset, nothing changes and the embedded monitoring page is served as before.

## Notebook

The **Notebook** page is a lightweight SQL notebook that runs entirely in your browser against
the statement API.

- **SQL cells** execute against the server; **Markdown cells** render documentation between
  queries.
- Run cells individually, or **run all** top-to-bottom.
- The notebook persists to browser **localStorage** automatically.
- **Export/import** as JSON to share a notebook or move it between browsers/machines.

### Python notebooks (Jupyter)

No Python runs on the driver — Oxidant executes SQL and DataFrame plans, not Python code. For a
Python notebook experience, run your own Jupyter locally and connect with the stock PySpark
Connect client:

```sh
pip install "pyspark-client>=4.0" jupyterlab
jupyter lab
```

In a notebook cell:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.remote("sc://localhost:50051").getOrCreate()
spark.sql("SELECT 1 AS hello").show()
df = spark.sql("SELECT * FROM VALUES (1,'a'),(2,'b') AS t(num, letter)")
df.filter("num > 1").show()
```

Every `spark.sql(...)` call here shows up on the **SQL** monitoring page and in the statement
list, so the UI remains useful as a query history/monitor alongside Jupyter.

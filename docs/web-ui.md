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
> The one authenticated route on this port is
> [`GET /api/status`](api.md#driver-status) — the control-plane status endpoint, which stays
> off unless `OXIDANT_STATUS_TOKEN` is set.

## Monitoring pages

The UI mirrors the Spark UI layout and is backed by the Spark-compatible
`/api/v1/applications/...` endpoints, so existing tooling against those routes works too.

| Page | What it shows |
|------|---------------|
| **Jobs** | One row per action/query execution: status, duration, stages |
| **Stages** | Stage detail — tasks, input/shuffle sizes, per-stage timing |
| **SQL** | Every SQL/DataFrame execution with its plan and duration |
| **Executors** | The driver plus any connected workers (see [workers.md](workers.md)) |
| **Environment** | Runtime info, Spark/Oxidant properties, catalog config |

## Theme

The UI carries the Oxidant brand theme from <https://www.oxidantdata.com>: dark-first
monochrome — layered near-blacks, hairline borders, Geist typography, and no decorative accent
colour. Emphasis comes from contrast and weight; the inverted white slab is what a primary
button gets instead of a coloured fill.

Colour is reserved for status, and only for status:

| Colour | Means |
|--------|-------|
| Green | Succeeded / completed, and "faster than Spark" on the Compare page |
| Amber | Running, pending, truncated results, "slower than Spark" |
| Red | Failed — a failed job, a rejected statement, a statement error pane |

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

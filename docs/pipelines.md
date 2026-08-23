# Declarative pipelines: Kafka → lakehouse, from one config file

`oxidant pipeline` builds a table DAG described in [`oxidant.yaml`](config.md). One binary, one
file, no PySpark client and no server:

```sh
oxidant pipeline validate -c oxidant.yaml   # parse, plan, topologically sort — run nothing
oxidant pipeline show     -c oxidant.yaml   # print the resolved DAG
oxidant pipeline run      -c oxidant.yaml   # build the tables
```

The engine underneath is the same one a PySpark `writeStream` drives — the same Kafka source,
the same checkpoints, the same exactly-once commit ordering, the same Delta sink with Iceberg
metadata published over it. This is a second door into it, not a second implementation.

Runnable example, needing neither a broker nor AWS:
[`examples/oxidant.yaml`](../examples/oxidant.yaml).

## Spark Declarative Pipelines (Connect)

Stock Spark 4.x clients can define and run pipelines over `oxidant spark server` without a
YAML file:

- **`pyspark.pipelines`** (`@dp.table`, `@dp.materialized_view`, …) and **`spark-pipelines run`**
  on `.sql` pipeline sources both speak the Spark Connect `PipelineCommand` surface
  (`CreateDataflowGraph` → `DefineOutput` / `DefineFlow` / `DefineSqlGraphElements` → `StartRun`).
- **What works today:** graph registry + SDP SQL parsing (`CREATE STREAMING TABLE`,
  `CREATE MATERIALIZED VIEW`, `CREATE TEMPORARY VIEW`, `CREATE [ONCE] FLOW`, `REFRESH MATERIALIZED
  VIEW`), dry-run validation (`StartRun.dry`), non-dry execution with `PipelineEvent` streaming,
  `full_refresh_all` / `full_refresh_selection` (drop pipeline state), `refresh_selection` and SQL
  `REFRESH` / `OR REFRESH` requests (subgraph + ancestors), `once` flows (skip after first
  successful completion unless refreshed), external **sinks** (`DefineOutput` with
  `output_type=SINK`), and **`ExecuteOutputFlows`** (one-shot remote run of a single output plus
  its flows, from the payload alone — no pre-registered graph; `storage` is the checkpoint root
  and `full_refresh` drops pipeline state first). The one-shot run's id is derived from the
  resolved output name, so re-runs share a Delta `appId`, and the *default* checkpoint root
  (`$TMPDIR/oxidant-sdp-execute-output-flows-<catalog.database.name>`) is derived from that same
  id — stable across sessions and clients, so a re-run resumes rather than replaying its source
  into the same location. The corollary is that callers sharing a catalog and an output name
  share checkpoint state; a deployment that wants isolation must pass `storage` explicitly.
  `output_type` must be set, and must not be `TEMPORARY_VIEW` — a view is registered but never
  written. Kafka spool sources (`oxidant.spool.dir` in `TBLPROPERTIES` or `readStream` options)
  exercise the same path as the YAML runner. Python source functions whose relation is empty at
  `DefineFlow` time round-trip via `GetQueryFunctionExecutionSignalStream` → client-side
  evaluation → `DefineFlowQueryFunctionResult` backfill before `StartRun`.
  `CREATE FLOW … AS AUTO CDC INTO …` and `AutoCdcFlowDetails` merge a change stream into a target
  table (SCD Type 1) — see [AUTO CDC (SCD Type 1)](#auto-cdc-scd-type-1).
- **Sinks** are write targets, not datasets. `SinkDetails.options.path` is required, and the sink
  is **not** registered in the catalog — so a flow that reads one is refused at `StartRun`
  (`flow ... cannot read sink ...`) rather than failing later as a missing table. That covers an
  AUTO CDC flow too: its source is `auto_cdc.source`, not a relation, and it is checked the same
  way. Two formats: `delta` (default; one atomic transaction per micro-batch) and `parquet`
  (**one file per batch, no commit protocol** — each batch lands atomically, but a reader can see
  a partially written *run*, a replayed batch is not deduplicated, and there is no atomic
  replace; the run emits a warning event naming the sink). `kafka`, `json`, and `csv` sinks are
  refused at definition time — see [TODOS.md](TODOS.md). A sink that carries its own streaming
  source in `options` (`subscribe` / `oxidant.spool.dir`) is written incrementally from the
  checkpoint like a streaming table; a sink whose flow reads other pipeline tables is a derived
  output — recomputed and **replaced** on every pass, which only `delta` can do, so a `parquet`
  sink with no source of its own is refused when the graph is lowered rather than failing
  mid-run.
- **Limits (deferred):** no Kafka sink.
  `APPLY CHANGES INTO` (Databricks legacy syntax) is rejected — use `CREATE FLOW … AS AUTO CDC …` instead.
  Interactive `spark.sql("CREATE STREAMING TABLE …")` still correctly rejects those statements;
  use `DefineSqlGraphElements` or the Python decorators instead.

**Python query-function signal stream**

When `pyspark.pipelines` cannot analyze a flow's Python function at definition time, it sends
`DefineFlow` with an empty `relation_flow_details.relation`. The client then opens
`GetQueryFunctionExecutionSignalStream` (scoped by `client_id`, matching the value on
`DefineFlow`) and receives `PipelineQueryFunctionExecutionSignal` responses naming pending flows.
After evaluating the Python function locally, the client sends `DefineFlowQueryFunctionResult` to
backfill the stored relation; `StartRun` then plans those flows like any other. Flows still
relation-less at `StartRun` fail with `failed_precondition` naming the flow.

A flow is "pending" while it has neither a relation nor SDP SQL text. Signals are routed by
`client_id`: a stream request only sees flows whose `DefineFlow.client_id` matches it, plus flows
registered without a `client_id` (which the stock client does not set today). Pending state lives
in the session's dataflow graph, so `DropDataflowGraph` or closing the session clears it.

**Backfill is accepted once, and only for a pending flow.** A `DefineFlowQueryFunctionResult`
naming a flow that already has a relation (an earlier backfill) or SDP SQL text (a flow from
`DefineSqlGraphElements`) is rejected with `failed_precondition`; an unknown flow is rejected with
`invalid_argument`. Re-evaluation is deliberately not supported: `StartRun` prefers a flow's
relation over its SQL, so accepting a second result would let a stale or misaddressed one replace
a `.sql` file's query — different table contents, no diagnostic, no source location. Send another
`DefineFlow` if a flow genuinely needs redefining.

> **Load-bearing caveat — register empty-relation flows *before* opening the signal stream.**
> Oxidant's `execute_plan` handler materializes the full response list before streaming, so each
> signal-stream RPC emits whatever is pending *at call time* in one
> `PipelineQueryFunctionExecutionSignal` and then completes; it does not hold the stream open.
> Upstream Spark's model is the inverse — the client opens the stream and holds it while the
> server pushes signals during graph resolution at `StartRun`. A client following *that* order
> (open the stream, then define flows) gets an empty, already-closed stream, evaluates nothing,
> and fails at `StartRun` with `failed_precondition`. Define the flows first, or call the stream
> again after late arrivals. Holding the stream open is tracked in
> [docs/TODOS.md](TODOS.md#sdp-phase-4a-follow-ups).

Both the current `flow_identifiers` field and the pre-4.2 `flow_names` field are populated;
backfill requests are accepted against either.

A backfilled relation is normally planned through DataFusion and unparsed, so a bad flow fails at
`StartRun` with its source location attached. The exception is a plain `SQL` relation that reads a
table this graph builds: that table does not exist until the run creates it, so the text is
forwarded to the runner verbatim, exactly like an SDP-SQL flow's `query_sql`. Deferring is
all-or-nothing per statement — one graph-built reference forwards the whole query — so the tables
it names that the graph does *not* build are checked against the catalog at `StartRun` instead,
and a typo among them is rejected there with the flow's source location rather than surfacing from
inside the runner. Two known rough edges on this path: leaf-name matching means
`other_catalog.other_db.orders_bronze` counts as the graph's `orders_bronze` (the runner's DAG
builder matches the same way, so the two agree), and the forwarded text is only checked for
parseability, not for being a single `SELECT`.

`PipelineAnalysisContext` rides along as a packed `google.protobuf.Any` in
`ExecutePlanRequest.user_context.extensions` (see `pyspark.pipelines.add_pipeline_analysis_context`),
not as a top-level request field. Oxidant ignores user-context extensions, so requests carrying it
are accepted unchanged; pipeline-scoped analysis (resolving names against the in-flight graph
rather than the catalog) is not implemented.

Note that `pyspark` 4.2 always evaluates the Python query function at `DefineFlow` time and sends a
populated relation, so this round-trip is exercised by
`crates/oxidant-connect/tests/pipeline_phase4a.rs` rather than by the client e2e gate below.

**Client e2e gate** (stock `pyspark.pipelines` / `spark-pipelines run`, no broker):

```sh
./tests/sdp-client-e2e.sh
```

The script builds `oxidant-cli`, starts a local-catalog server, runs
`DefineSqlGraphElements` + `StartRun` over the committed `examples/spool/orders` fixture
(`sum(revenue)=725`), then repeats via `python -m pyspark.pipelines.cli run` on a `.sql` file.
`StartRun.storage` must live under the catalog warehouse parent (e.g. `warehouse/_checkpoints`)
so pipeline table data and catalog registration share the same root.

**Temp views and refresh semantics**

- Chained temporary views resolve in **definition order** only. A `CREATE TEMPORARY VIEW` that
  references another temp view defined later in the same graph fails loudly at `StartRun` (Spark
  resolves by dependency; oxidant does not reorder definitions).
- `REFRESH MATERIALIZED VIEW` / `OR REFRESH` requests queued on the graph are drained at the
  next non-dry `StartRun` and are **at-most-once**: if that run fails after the drain, the refresh
  is not retried automatically — issue another `REFRESH` or run again with `refresh_selection`.
- **Dry-run temp views:** validation registers graph temporary views in the session so downstream
  SQL can be planned, then drops only what it registered on every exit (success, failure, or panic).
  A dry run errors instead of `CREATE OR REPLACE` when a same-named session view already exists,
  so interactive temp views are never clobbered.
- **`sql_conf` scope:** graph-level `sql_conf` (on `CreateDataflowGraph`) is applied for the whole
  `StartRun` and catalog keys (`spark.sql.catalog.*`, `spark.sql.defaultCatalog`) take effect via
  `sync_catalogs()`. Flow-level `sql_conf` (on `DefineFlow`) is scoped to that flow's planning only;
  session keys such as `spark.sql.session.*` apply there, but catalog keys are ignored with a
  `PipelineEvent` — register catalogs on the graph instead.

## Kinds of table

```yaml
pipeline:
  name: sales
  catalog: local            # must support write DDL: `local` or `glue`
  schema: live              # created if missing
  storage: ${CONFIG_DIR}/warehouse/live      # local paths must be absolute; see config.md
  checkpoints: ${CONFIG_DIR}/warehouse/_checkpoints
  trigger: 30 seconds       # or: once | available_now | 500ms | 5 minutes
  format: delta
  iceberg_compat: true

tables:
  - name: orders_bronze     # STREAMING: it declares a `source`
    source:
      format: kafka
      options:
        kafka.bootstrap.servers: b-1.msk.example:9092
        subscribe: orders
        startingOffsets: earliest
        maxOffsetsPerTrigger: 50000
    sql: SELECT CAST(value AS STRING) AS raw, timestamp FROM stream
    partition_by: [event_date]

  - name: orders_silver     # DERIVED: defined by SQL over other tables
    sql: SELECT * FROM orders_bronze WHERE amount > 0
```

**A streaming table** reads a source and **appends** one micro-batch per trigger. Its `sql:` runs
over the source, which is in scope under the fixed alias **`stream`** with Spark's exact Kafka
schema (`key`, `value`, `topic`, `partition`, `offset`, `timestamp`, `timestampType`). Omit the
`sql:` and those seven raw columns land in the table unchanged — which is what Spark does, and
almost never what you want.

**A derived table** is defined purely by SQL over other tables and is **recomputed in full** on
each trigger, then written in one atomic commit that retires the previous files and adds the new
ones together. A reader sees either the whole old table or the whole new one.

### Idle passes are skipped

A derived table is only recomputed when it could produce something different: when something it
reads moved this pass, when its own declaration changed, or when it has never been built. An idle
pipeline reports `unchanged` and touches nothing.

Without this a 30-second trigger rebuilds every gold table 2,880 times a day, each rebuild a full
recompute plus a Delta commit retiring every live file — so the version history fills with commits
that change nothing and each one leaves a fresh set of small files.

The skip is deliberately conservative. A table that reads anything the pipeline does **not** build
is always recomputed: an outside lake table can be rewritten by anyone, so its freshness is
unknowable from here. And the table's declaration is fingerprinted, so editing its `sql:` or adding
an expectation forces a recompute even when no upstream moved — otherwise a check you just added
would sit inert until the next batch happened to arrive.

State lives in `{pipeline.checkpoints}/_pipeline-state.json`. Delete it to force a full rebuild.

### Full recompute is a real cost

A derived table is rebuilt from scratch every update. That is always correct and needs no
cross-batch state — which the engine does not have (see
[streaming.md](streaming.md), "no stateful aggregation across batches"). It is also
O(whole table) per update, so a gold aggregate over a large bronze table will dominate the
trigger interval. Size the trigger to the recompute, not to the ingestion rate. Incremental
derived tables are [not implemented](TODOS.md).

### AUTO CDC (SCD Type 1)

`CREATE FLOW … AS AUTO CDC INTO … FROM …` (Connect `DefineFlow.auto_cdc_flow_details`, or the
same clause in SDP SQL) merges each micro-batch from a streaming CDC source into a target table
by key, ordered by `sequence_by` (latest wins). Optional
`APPLY AS DELETE WHEN …` and `APPLY AS TRUNCATE WHEN …` clauses remove rows; `IGNORE NULL UPDATES`
preserves existing target values when the batch carries nulls. Only **SCD Type 1** is supported
today (`STORED AS SCD TYPE 1` / `SCD_TYPE_1`).

There is **no Delta `MERGE` operator** in the engine yet. Each micro-batch is applied as a
**read–modify–write** over the whole target table: read the current Delta snapshot, merge in
memory/SQL, then commit a replacement version via the existing Delta sink (same atomic replace
path derived tables use). That is **O(target table size) per batch** — honest for correctness,
not for large targets at fast triggers. It is also O(target table size) in *memory*: the whole
target is held between batches so a run of N batches reads the table once instead of N times, and
the merge builds a second copy while it runs. Budget roughly twice the target for the worker —
in practice that ceiling, not the per-batch time, is what decides whether a target fits. Native
Delta merge is future work.

The target is declared with **no query** — `CREATE STREAMING TABLE cdc_target;` — and is defined
entirely by its CDC flow: it inherits the *source* table's stream, per-batch transformation,
expectations and `dedup_columns`, then merges instead of appending. (Format and partitioning
`TBLPROPERTIES` on the target are fine; what is rejected is a target that has its own `source:`,
query, append flow, or expectations — those are append-only sinks, and one table cannot be both.)
The `KEYS` and `SEQUENCE BY` columns must survive `COLUMNS` / `COLUMNS * EXCEPT` into the target
— the next batch compares against the values stored there, so dropping them would let an older
event overwrite newer state.

One source is a change stream by itself and needs no separate bronze table: a
`format: postgres_cdc` source emits `__oxidant_op` / `__oxidant_lsn` / `__oxidant_ts` alongside the
table's own columns, so `auto_cdc.source: <table>_changes` names the stream that source declares
rather than a second entry in `tables:`. See [postgres-cdc.md](postgres-cdc.md) and
`examples/postgres-cdc.yaml`.

#### What "out of order safe" does and does not cover

A record whose `sequence_by` value is older than the target's loses, and a truncate only removes
rows at or below its own sequence. But three cases are worth stating outright:

- **A delete leaves no tombstone.** SCD Type 1 removes the row, and nothing records *when*. A
  record for that key arriving in a **later** micro-batch is therefore treated as new, even if
  its `sequence_by` value is older than the delete's — the key comes back. Within one batch the
  delete still wins. This matches Databricks' SCD Type 1; retaining tombstones would mean
  carrying a column the target does not declare, with no rule for ever dropping it.
- **A NULL `sequence_by` value fails the batch.** A row with no ordering value cannot be placed
  against the target, and dropping it silently would lose a change event — including a delete or
  a truncate. Databricks fails the same way.
- **NULL key values compare equal.** A NULL-keyed row is one key like any other, matched with
  `IS NOT DISTINCT FROM`. This is the one case here that **deliberately diverges from
  Databricks**: Spark's `MERGE` matches keys with `=`, so a NULL-keyed row never matches the
  target and is re-inserted on every micro-batch — an unbounded set of duplicates for one key,
  which is a leak rather than a semantic. Expect a Databricks parity test to differ on this row.

Two rows for the same key with the *same* `sequence_by` value have no natural winner. The merge
breaks the tie deterministically — a delete first, then the remaining target columns descending
— so a replay recomputes the table that was committed rather than a different one.

`APPLY AS DELETE WHEN` / `APPLY AS TRUNCATE WHEN` expressions end at the next AUTO CDC clause
keyword (`SEQUENCE`, `APPLY`, `COLUMNS`, `IGNORE`, `STORED`, `TRACK`). An unquoted column with
one of those names truncates the clause — spell it with backticks (`` `stored` ``). Each clause
may appear only once; a repeat is an error rather than a silent overwrite.

The same merge is reachable from a YAML pipeline with `auto_cdc:` on a table that declares the
streaming source itself:

```yaml
  - name: customers_scd1
    source:                       # the CDC stream, same shape as any streaming table
      format: kafka
      options: {subscribe: customers, startingOffsets: earliest}
    sql: SELECT id, name, seq, op FROM stream
    auto_cdc:
      source: customers_bronze    # declared table this stream feeds — the DAG edge
      keys: [id]
      sequence_by: seq
      apply_as_deletes: "op = 'D'"
      except_column_list: [op]    # or column_list: [...], not both
      ignore_null_updates_columns: [name]   # or ignore_null_updates_except: [...], not both
```

`auto_cdc.source` must name a table declared in the same `tables:` list — it is what puts the
dependency edge in the graph, so a typo would leave the merge running in an arbitrary order. The
target's `source:`, `sql:`, `expect:` and `dedup_columns:` must be **identical** to that table's:
both re-read the same stream, and letting them drift means the CDC target silently merges a
different projection, or a different set of rows, than the table it claims to follow.

## Dependencies and ordering

Edges come from resolving each `sql:` with DataFusion's own parser, so:

- a reference to another declared table is an edge, **including** a fully-qualified one
  (`local.live.orders_bronze` orders the same as `orders_bronze`);
- a CTE is **not** an edge — `WITH orders AS (...)` is a local definition, not a read of a table
  called `orders`;
- a similar name is **not** an edge — `orders_archive_2024` does not depend on `orders`;
- a table the pipeline does not build is not an edge, which is how a bronze table reads an
  existing lake table.

Tables are then topologically sorted. A cycle is rejected before anything runs, with the path
that closes it (`a -> b -> c -> a`). `oxidant pipeline validate` does all of this and stops —
it is the fast feedback loop, and it touches neither the broker nor the lake.

Within one pass, each table is updated in order. A table whose upstream **failed** this pass is
skipped rather than computed from stale data, and the rest of the graph still runs: a broken
gold table must not stop bronze ingestion. `--table <name>` restricts a run to that table **and
its ancestors** — refreshing gold from stale silver would report success over old numbers.

## Expectations

```yaml
    expect:
      amount_positive:
        check: amount > 0
        action: drop          # drop | warn | fail
```

| action | effect |
|---|---|
| `drop` | failing rows are filtered out before the write |
| `warn` *(default)* | every row is written; violations are counted and logged as `table=X expectation=Y failed_records=N` |
| `fail` | the update is aborted; the table stays at its **last good version** |

All three work on both kinds of table. `drop` costs nothing extra — it is a predicate on the query
that already runs. `warn` and `fail` each cost one violation count.

`warn` is the default deliberately: a constraint added to a running pipeline should surface the
problem before it stops the ingestion someone is relying on.

**On a streaming table the countable unit is the micro-batch.** `warn` logs the violation count
for that batch; `fail` aborts it before anything reaches the sink, so the table stays at its last
good version and the batch's records stay in the offset log, unread. Fix the data or the check and
the next trigger replays exactly those records — see
[streaming.md](streaming.md#exactly-once-and-what-makes-it-true).

That is a narrower claim than the same action on a derived table, and the difference matters when
you write the check: a derived table is recomputed in full, so its count is over the whole table,
while a streaming table's is over the rows that arrived in that trigger. `count(*) > 1000` means
"this batch", not "this table".

```yaml
tables:
  - name: orders_bronze
    source: { format: kafka, options: { subscribe: orders } }
    expect:
      parseable:  { check: "order_id IS NOT NULL", action: drop }   # per row
      amount_set: { check: "amount IS NOT NULL",   action: fail }   # per micro-batch

  - name: orders_silver
    sql: SELECT * FROM orders_bronze
    expect:
      amount_positive: { check: "amount > 0", action: fail }        # over the whole table
```

A `drop` on a streaming table works whether or not the table declares its own `sql:` — one is
synthesized to hang the filter on when it does not.

A row where the check evaluates to **NULL** counts as a violation. `amount > 0` is NULL when
`amount` is NULL, and treating that as "not a failure" is how a column of nulls passes a
quality gate.

`fail` and `warn` are evaluated against the query *before* any `drop` filtering, so a row a
`drop` would remove still counts for a `fail` on the same table — otherwise the two would cancel
out and `fail` could never fire.

Each `fail` or `warn` expectation costs one extra execution of the table's query, to count the
violations. `drop` costs nothing extra — it is a predicate on the query that already runs. On an
expensive derived table, prefer one expectation over several, or use `drop` where the semantics
allow it.

## Triggers

| `trigger:` | behaviour |
|---|---|
| `30 seconds` / `5 minutes` / `500ms` | one pass over the DAG per interval |
| `once` | one pass, draining every source, then exit |
| `available_now` | the same |

Passes run on a **fixed schedule**: a pass that overruns its interval is followed immediately by
the next, rather than the interval being added on top of the pass's own duration. A bare number
(`trigger: 30`) is rejected rather than assigned a unit — guessing milliseconds where seconds
were meant is a 1000× error that only shows up as a hammered broker.

`--once` forces a single pass regardless of the configured trigger. A `once` run exits non-zero
if any table failed, so `oxidant pipeline run --once && next-step` works in a script.

## Where the tables land

Each table is written to `{pipeline.catalog}.{pipeline.schema}.{name}` and registered in the
catalog, so anything that reads that catalog can query it — including `oxidant sql`:

```sh
oxidant sql -c oxidant.yaml -e "SELECT * FROM local.live.revenue_gold"
```

Only `local` and `glue` can be a pipeline sink: the Hive provider has no `create_database` and
the REST provider has no write DDL at all, so a pipeline pointed at either is rejected **at
config load** rather than failing mid-run after a source has already been read.

`storage:` pins the root (`{storage}/{table}/`). Omit it and each table lands wherever the
catalog's warehouse convention puts it, which is what makes the same config work against a local
warehouse and an S3 one unchanged.

Delta tables also get Iceberg metadata published over the same Parquet files, so
`local.live.orders_bronze_iceberg` is readable by Iceberg engines. One copy of the data. The
Iceberg side is republished every `checkpoint_interval` commits, so it trails the Delta view —
see [streaming.md](streaming.md#reading-one-table-from-any-engine).

## Running without a broker

Set the source option `oxidant.spool.dir` (or `OXIDANT_KAFKA_SPOOL`) to a directory of
newline-delimited JSON files named `batch-0.json`, `batch-1.json`, … — one file per micro-batch,
read from disk instead of from a broker. `subscribe` is still required; the spool replaces the
broker, not the topic name.

This exists so the whole pipeline can be exercised offline, and it is **not** a broker
substitute: it has one partition, no retention, and no concurrent producer. It does resume
correctly — the file it reached is part of the checkpoint, so re-running a `once` pipeline
against the same spool ingests **nothing** the second time, exactly as a Kafka source would.

## Limits worth knowing before you deploy

- **Derived tables are recomputed in full**, as above.
- **A derived table's result is materialized in driver memory** before it is written, so a
  recompute is bounded by the driver's RAM rather than streamed to the sink. This is fine for
  the aggregates a gold table usually holds; it is not fine for a silver table that is a
  near-copy of a large bronze one. Streaming that write is [open work](TODOS.md).
- **Micro-batches run in one process.** The streaming path does not use the Flight worker
  cluster, so throughput is bounded by a single driver; `maxOffsetsPerTrigger` is the lever.
- **One writer per table.** The sink is designed for a single pipeline per table.
- **No compaction.** One file per micro-batch means a fast trigger produces many small files.
- **Append output mode only** for streaming tables.
- **`local` and `glue` only** as sink catalogs.

# Writing tables with SQL

Oxidant writes tables three ways: the declarative [pipeline](pipelines.md), a PySpark
`writeStream` over Spark Connect, and plain SQL — `CREATE TABLE … AS SELECT` and `INSERT`.
This page is the third one.

Everything here works from `oxidant sql` with no server running, against any catalog that
implements write DDL (`local` and `glue` — see [catalogs.md](catalogs.md)).

## What each format supports

| Format | Read | `CREATE TABLE … AS SELECT` | `INSERT INTO` | `INSERT OVERWRITE` |
|--------|------|---------------------------|---------------|--------------------|
| Delta | yes | yes | yes | yes — atomic |
| Parquet | yes | yes | yes | no (see below) |
| CSV | yes | yes | — | — |
| JSON | yes | yes | — | — |
| Iceberg | yes | no | no | no |

**Delta is the one to use for anything a reader looks at.** Every write is a transaction-log
commit, so a reader sees the table before the write or after it, never mid-write. An
`INSERT OVERWRITE` retires the old files in the *same* commit that adds the new ones.

**A plain Parquet table takes `INSERT INTO` but not `INSERT OVERWRITE`.** It has no
transaction log, so replacing its files could not be made atomic — a reader listing the
directory mid-overwrite would see a half-empty table. Oxidant refuses rather than fakes it.

**Iceberg is not a write target, deliberately.** A Delta table written with `icebergCompat`
publishes Iceberg metadata over the *same* Parquet files, so one copy of the data is readable
by both engines. Writing a separate Iceberg table would be a second copy with its own
divergent history. Naming `USING iceberg` on a write gets an error that says this.

## `CREATE TABLE … AS SELECT`

```sql
CREATE TABLE lake.live.orders USING delta AS
  SELECT order_id, amount, event_date FROM lake.raw.orders_json;
```

The table is created in the catalog (`CatalogProvider::create_table`), the rows are written
at the resolved location, and the table is immediately queryable by name.

Two optional clauses:

```sql
CREATE TABLE lake.live.orders USING delta
  LOCATION 's3://my-bucket/live/orders/'
  PARTITIONED BY (event_date)
AS SELECT order_id, amount, event_date FROM lake.raw.orders_json;
```

- `LOCATION` — where the files go. Without it the catalog picks: the `local` catalog uses
  `{warehouse}/{database}.db/{table}/`, Glue uses its configured `warehouse` root.
- `PARTITIONED BY` — columns written as Hive-style directories
  (`event_date=2026-01-01/…`) rather than into the data files, exactly as Spark writes them.
  Name columns only; Spark's typed form (`PARTITIONED BY (event_date DATE)`) is refused rather
  than half-understood. **Delta only** on this path: the flat formats are written as a single
  file, so partition columns would never reach the directory the reader looks in — a table
  unreadable through its own metadata. `USING parquet PARTITIONED BY (…)` is an error naming
  Delta as the alternative, and `INSERT` into an already-partitioned Parquet table is refused
  for the same reason.

`OPTIONS(…)`, `COMMENT`, and `TBLPROPERTIES(…)` are **refused** on this path rather than
accepted and dropped — `CatalogProvider::create_table` has nowhere to put them, and a table
that silently does not match its own DDL is worse than a statement that does not run.

## `INSERT INTO` and `INSERT OVERWRITE`

```sql
INSERT INTO lake.live.orders SELECT * FROM lake.raw.late_arrivals;
INSERT OVERWRITE lake.live.orders SELECT * FROM lake.raw.orders_json;
```

An insert is visible to the next `SELECT` in the same session — the cached table provider is
evicted as part of the statement.

Columns are cast to the table's own types positionally, because the rows do not always arrive
in those types — a Spark integer literal plans as `INT` even when the column is `BIGINT`. The
cast **fails the statement rather than losing the value**: a string that is not a number, a
number past the column's range, a timestamp that does not parse, or a decimal too big for the
declared precision is an error naming the column, and nothing is committed. It is never a
`NULL` written in the value's place — the overflow behaviour matches Spark's ANSI store
assignment, and it is the same enforcement the [pipeline](pipelines.md) and streaming write
paths use. The full ANSI policy is stricter still: Spark rejects a `STRING` → numeric store
assignment at *analysis* time, where this engine plans the conversion and is strict only about
the value surviving it, so `INSERT INTO t(int_col) SELECT '42'` succeeds here and does not in
Spark under ANSI.

A value that was already `NULL` is not a lost value and writes through as `NULL`. A `NULL` into
a column the table declares `NOT NULL` is an error, for the same reason.

The table's schema is not widened: adding a column needs `ALTER TABLE`, not an `INSERT`.

A store assignment is not an expression `CAST`, but the two are strict in the same direction
here: this engine's expression `CAST` errors on an invalid cast rather than yielding `NULL`
(`SELECT CAST('x' AS INT)` is an error, where Spark with `spark.sql.ansi.enabled=false` returns
`NULL` — the Spark-SQL parity baseline records that divergence against `nonansi/cast.sql`).
There is no non-ANSI mode to switch to; `TRY_CAST` is the lenient form. Cast-or-fail
on the write path is therefore consistent with the rest of the engine, not an exception to it.

`REPLACE INTO` is not supported — that is row-level matching on a key, which Delta expresses
as `MERGE`.

## Local-warehouse tables (no catalog)

An unqualified `CREATE TABLE t USING <fmt> …` writes to Oxidant's own local warehouse as a
DataFusion `ListingTable`, not through a catalog. That path takes `parquet`, `csv`, `json`,
and `orc`, and is the one Spark-SQL parity tests exercise. It does not take `delta` — use a
catalog-qualified name for that.

It accepts the same storage clauses:

```sql
CREATE TABLE events (id INT, region STRING) USING csv
  PARTITIONED BY (region)
  LOCATION '/data/events'
  OPTIONS (header 'true', delimiter '|');

INSERT INTO events VALUES (1, 'eu'), (2, 'us');
```

- `LOCATION` must be a filesystem path (bare or `file://`). An `s3://` location belongs to a
  catalog table, so it is left to the normal path rather than half-handled here.
- `OPTIONS(…)` is translated to DataFusion's own vocabulary — `header` → `format.has_header`,
  `delimiter`/`sep` → `format.delimiter`, plus `quote`, `escape`, `compression`, and only for
  CSV. An option outside that set fails the statement rather than being dropped: a CSV table
  that silently ignores `header` reads its header row as data.
- `PARTITIONED BY` works on the declare-then-`INSERT` form. The local **CTAS** form
  (`CREATE TABLE t USING csv AS SELECT …`) takes `LOCATION` but not `PARTITIONED BY` — its
  writer emits one flat file and cannot partition, so it refuses rather than claiming to.

## Gotchas

- **A catalog named `local` needs no special handling for `INSERT`, but that took work.**
  `LOCAL` is a keyword in the `INSERT` grammar (Hive's `INSERT OVERWRITE LOCAL DIRECTORY`), so
  `INSERT INTO local.live.t` would fail at parse. Oxidant quotes the catalog segment for you
  when it names a registered catalog. Other reserved words in *namespace* or *table* position
  still need backticks.
- **A zero-row table cannot be read back.** `CREATE TABLE … AS SELECT` over an empty result
  writes no data files, and the lakehouse reader treats a table with no active files as an
  error rather than an empty result.
- **Writes run on the driver.** The rows are collected before the commit, because a Delta
  commit is atomic over the file set it carries. A `SELECT` that does not fit in driver memory
  is not yet a safe `INSERT` source — see [TODOS.md](TODOS.md).

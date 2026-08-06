# ClickBench coverage gate — known pre-existing failures

**Status: RESOLVED** — all 43 queries pass; `coverage` job is a required gate again.

## Root cause (identified and fixed)

The `Engine` uses the **Databricks SQL dialect** (`opts.sql_parser.dialect = Dialect::Databricks`),
which treats `"..."` as a **string literal**, not an identifier (matching Spark's default
`spark.sql.ansi.double_quoted_identifiers=false`). The ClickBench `queries.sql` file used
ANSI-standard double-quoted column identifiers (e.g. `"CounterID"`, `"EventDate"`).

DataFusion therefore parsed `WHERE "CounterID" = 62` as `WHERE 'CounterID' = 62` (a string
constant compared to an integer), and the `simplify_expressions` optimizer tried to constant-fold
the comparison by casting `'CounterID'` to Int32 — producing the error
`Cannot cast string 'CounterID' to value of Int32 type`.

The same root cause produced every other failure category:
- `Function 'sum' failed to match any signature … received String (Utf8)` — `SUM('IsRefresh')` instead of the column
- `Cannot coerce arithmetic expression Utf8 - Int64` — `'ClientIP' - 1`
- `Error parsing timestamp from 'EventTime'` — `to_timestamp_seconds('EventTime')`

Queries that "passed" before the fix (e.g. Q4 `COUNT(DISTINCT "UserID")`, Q11
`"MobilePhoneModel" <> ''`) happened not to trigger a type error under the wrong interpretation,
but returned semantically wrong results.

## Fix

Converted all `"ColumnName"` identifiers in `bench/clickbench/queries.sql` to
`` `ColumnName` `` (backtick-quoted). Backtick-quoted identifiers are recognized as identifiers by
both the Databricks and Generic dialects. Also fixed the two inline SQL strings in
`crates/oxidant-bench/src/main.rs` that used `\"EventDate\"` for the same reason.

## Acceptance criterion (met)

`cargo run -p oxidant-bench -- clickbench --rows 20000` exits 0 (43/43).

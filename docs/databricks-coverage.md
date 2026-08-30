# Databricks SQL coverage matrix

Section-by-section map of the [Databricks SQL language
manual](https://docs.databricks.com/aws/en/sql/language-manual/) onto what the Oxidant engine
actually does today, across four axes: **statements**, **functions**, **data types**, and
**operators**.

This is the KAN-89 deliverable for the "Oxidant SQL parity with Databricks SQL on AWS Glue + Lake
Formation" epic. The plan behind the epic is [`databricks-parity-plan.md`](databricks-parity-plan.md);
every **Missing** or **Partial** row below names the ticket that owns closing it (original epic
stories KAN-89..KAN-108, plus follow-on tickets KAN-110..KAN-118 filed from this matrix).

## How this was measured

| | |
|--|--|
| Engine commit | `b3f3f2e` (`chore: release v0.1.2`), the `main` this branch is based on |
| Build | `cargo build -p oxidant-cli` (dev profile) |
| Server | `./target/debug/oxidant start --port 50051 --sample-data sample-data` |
| Probe transport | `POST /api/v1/statements?wait=true` (the [REST API](api.md) that `oxidant sql` drives) |
| Probes run | 578 statements over three passes (508 first pass = 505 scored across the four axes below + 3 setup/DDL warm-ups not attributed to an axis, 55 follow-ups, 15 semantic checks). Reconstructible statement probes are listed in [`parity/databricks-probes.md`](../parity/databricks-probes.md); function-category ratios are category aggregates (see that file). |
| Date | 2026-08-09 |

Every row's Evidence column is one of two things and nothing else:

- **a probe** — real SQL that was sent to that server, with the real observed result or the
  verbatim leading text of the real error; or
- **a code path** — `file → symbol`, read in this worktree.

Rows I could not settle either way are not given a status. They are collected under
[Not verified](#not-verified) instead of being guessed at.

### Status legend

| Status | Meaning |
|--------|---------|
| **Supported** | Probe ran and returned Spark-shaped results, or the owning code path implements the section. |
| **Partial** | Some of the section works; a named part does not, or it parses but the semantics diverge from Databricks. |
| **Missing** | Probe failed outright (parser or planner rejects it) and no code path implements it. |
| **N/A** | Requires the Databricks control plane (Unity Catalog *securables and DDL*, Delta Sharing, serverless, workspace identities, AI/Model Serving). Out of scope per the plan's scope boundaries. Unity Catalog as a read-only *metadata source* is a separate, partially working thing — see [catalogs-unity.md](catalogs-unity.md). |

## Baseline counts (freshly read)

Read from `parity/baseline.json` in this worktree, the Spark v4.0.0 golden corpus that the
`Spark SQL parity ratchet` CI job gates on:

| Metric | Value |
|--------|-------|
| Spark version | v4.0.0 |
| Files total / skipped | 303 / 25 |
| Blocks total | 14,706 |
| Strict pass | 2,854 (19.4%) |
| Semantic pass | 7,890 (53.7%) |

Failure buckets, largest first — these are what the epic's dialect/function tickets are aimed at.
(The file also carries a `pass` bucket of 2,854, which is the same number as strict pass above.)

| Bucket | Blocks |
|--------|-------:|
| error-parity | 2,880 |
| schema-only | 2,141 |
| exec-error | 1,830 |
| parser-unsupported | 1,433 |
| function-missing | 916 |
| missing-relation | 887 |
| correctness | 601 |
| feature-unsupported | 592 |
| decimal-precision | 209 |
| missing-error | 137 |
| requires-udf-registration | 87 |
| null-semantics | 83 |
| datetime | 32 |
| ordering | 15 |
| nondeterministic | 5 |
| engine-panic | 4 |

> **The plan document's figures are superseded.** `databricks-parity-plan.md` §"Current state"
> reports strict 536 / semantic 4,984 of 12,641. Those numbers are stale; the file it cites
> (`parity/baseline.json`) now reads 2,854 / 7,890 of 14,706 as tabulated above. Read the baseline
> file, not the plan, for parity numbers.

The Spark corpus measures Spark-SQL parity, not Databricks-specific surface. It is the right
denominator for the function/dialect tickets and the wrong one for Databricks-only statements
(`COPY INTO`, Delta maintenance, Unity Catalog DDL), which is why the matrix below exists.

## Headline

200 manual sections are scored below: **75 Supported** (74 probed, one parse-only), **29 Partial**,
**79 Missing**, **17 N/A**. Every one of the 108 Missing/Partial rows names an owning ticket.

Raw first-pass probe pass rates, by axis (a probe "passes" if the statement succeeded — it does
not assert Databricks-identical values, so these are upper bounds). Denominators sum to 505; the
other 3 first-pass statements were setup/DDL warm-ups (see methodology table).

| Axis | Probes passing |
|------|---------------:|
| Data types | 23/32 |
| Operators | 31/43 |
| Statements | 90/186 |
| Functions | 124/244 |

## A. Statements — DDL

| Manual section | Status | Evidence | Ticket |
|---|---|---|---|
| `CREATE TABLE [USING]` (plain) | Supported | `CREATE TABLE cov_t (id INT, name STRING, amt DECIMAL(10,2)) USING parquet` → OK; writes a real format-backed table (`crates/oxidant-loom/src/spark_create_table.rs` → `lower_create_table_using`, dispatched from `crates/oxidant-loom/src/lib.rs` → `Engine::sql`) | — |
| `CREATE TABLE … COMMENT` / `TBLPROPERTIES` | Supported | `CREATE TABLE cov_tp (a INT) USING parquet TBLPROPERTIES ('k'='v')` → OK; `SHOW TBLPROPERTIES cov_ddl2` → `k \| v` | — |
| `CREATE TABLE … LOCATION` | Missing | `CREATE TABLE cov_loc2 (a INT) USING parquet LOCATION '/tmp/probe/cov_loc2'` → `ParserError("Expected: end of statement, found: USING at Line: 1, Column: 31")` | KAN-94 |
| `CREATE TABLE … PARTITIONED BY` | Missing | `CREATE TABLE cov_part (a INT, p STRING) USING parquet PARTITIONED BY (p)` → `ParserError("Expected: end of statement, found: USING at Line: 1, Column: 41")` | KAN-94 |
| `CREATE TABLE … CLUSTER BY` (liquid clustering) | Missing | `CREATE TABLE cov_ddl4 (a INT) USING delta CLUSTER BY (a)` → `ParserError("Expected: end of statement, found: USING at Line: 1, Column: 31")` | KAN-94 |
| `CREATE TABLE … AS SELECT` (CTAS) | Supported | `CREATE TABLE cov_ctas USING parquet AS SELECT id FROM cov_t` → OK (`spark_create_table.rs` → `lower_create_table_ctas`, streamed to Parquet by `Engine::run_create_table_ctas`) | — |
| `CREATE TABLE LIKE` | Missing | `CREATE TABLE cov_like LIKE cov_t` → `This feature is not implemented: Like not supported` | KAN-94 |
| `CREATE TABLE CLONE` (shallow/deep) | Missing | `CREATE TABLE cov_clone SHALLOW CLONE cov_t` → `ParserError("Expected: end of statement, found: SHALLOW at Line: 1, Column: 24")` | KAN-105 |
| `CREATE TABLE` with Hive format | Missing | `CREATE TABLE cov_hive (a INT) STORED AS PARQUET` → `Hive formats not supported: Some(HiveFormat { … storage: Some(FileFormat { format: PARQUET }) … })` | KAN-94 |
| `CREATE TABLE` constraints (`PRIMARY KEY`, `CHECK`) | Supported (parse-only) | `CREATE TABLE cov_pk (a INT NOT NULL, CONSTRAINT pk PRIMARY KEY (a)) USING parquet` → OK. Not verified that the constraint is enforced or reported back. | — |
| `CREATE SCHEMA` / `CREATE DATABASE` | Supported | `CREATE SCHEMA IF NOT EXISTS cov_db` → OK; `SHOW DATABASES` then lists `cov_db` | — |
| `CREATE VIEW` / `CREATE OR REPLACE VIEW` | Supported | `CREATE OR REPLACE VIEW cov_v3 AS SELECT id FROM cov_t` → OK; `SELECT count(*) FROM cov_v3` → 3 | — |
| `CREATE TEMPORARY VIEW` | Supported | `CREATE TEMPORARY VIEW cov_tv AS SELECT id FROM cov_t` → OK; `SHOW VIEWS` reports it with `isTemporary = true`. Temp/persistent distinction is tracked in `Engine::sql` (`temp_views`, `analyze_create_view`) since DataFusion has none. | — |
| `CREATE MATERIALIZED VIEW` | N/A (interactive) / Supported (SDP) | Interactive `spark.sql`: `Materialized views not supported`. **Pipeline context:** `DefineSqlGraphElements` / `StartRun` materializes MVs via the declarative runner (full recompute + atomic replace). | — |
| `CREATE STREAMING TABLE` | N/A (interactive) / Supported (SDP) | Interactive `spark.sql`: parser rejects `STREAMING`. **Pipeline context:** `DefineSqlGraphElements` + `StartRun` builds streaming tables from Kafka/spool sources and SQL flows. | — |
| `CREATE FUNCTION` (SQL body) | Partial | `CREATE OR REPLACE FUNCTION cov_add2(x INT) RETURNS INT RETURN x + 100` → OK, but `SELECT cov_add2(1)` → `1` and `SELECT cov_add2(5)` → `1`. The definition registers (`crates/oxidant-loom/src/udf_registry.rs` → `try_create_function`) but the body is not evaluating the argument. | KAN-110 |
| `CREATE CATALOG` | Missing | `CREATE CATALOG IF NOT EXISTS cov_cat` → `ParserError("Expected: an object type after CREATE, found: CATALOG")` | KAN-100 |
| `CREATE CONNECTION` | N/A | `CREATE CONNECTION cov_conn TYPE mysql …` → `ParserError("… found: CONNECTION")`. Unity Catalog Lakehouse Federation object. | — |
| `CREATE EXTERNAL LOCATION` / `CREATE CREDENTIAL` | N/A | `CREATE EXTERNAL LOCATION cov_loc URL 's3://b/p' …` → `ParserError("Expected: TABLE, found: LOCATION")`. Unity Catalog securable. | — |
| `CREATE VOLUME` | N/A | `CREATE VOLUME cov_vol` → `ParserError("… found: VOLUME")`. Unity Catalog volume. | — |
| `CREATE SHARE` / `CREATE RECIPIENT` / `CREATE PROVIDER` | N/A | `CREATE SHARE cov_share` → `ParserError("… found: SHARE")`. Delta Sharing control plane. | — |
| `CREATE BLOOMFILTER INDEX` | Missing | `CREATE BLOOMFILTER INDEX ON TABLE cov_t FOR COLUMNS (id)` → `ParserError("… found: BLOOMFILTER")` | KAN-105 |
| `ALTER TABLE` (add/drop/rename column, `SET TBLPROPERTIES`, `ADD PARTITION`) | Missing | All four probes → `This feature is not implemented: Unsupported SQL statement: ALTER TABLE …`, e.g. `ALTER TABLE cov_ddl1 ADD COLUMN b STRING` | KAN-100 |
| `ALTER VIEW` | Missing | `ALTER VIEW cov_v RENAME TO cov_v2` → `ParserError("Expected: AS, found: RENAME at Line: 1, Column: 18")` | KAN-100 |
| `ALTER SCHEMA` / `ALTER DATABASE` | Missing | `ALTER SCHEMA cov_db SET DBPROPERTIES ('k'='v')` → `ParserError("Expected: ALTER SCHEMA operation, found: SET")` | KAN-100 |
| `COMMENT ON` | Missing | `COMMENT ON TABLE cov_t IS 'hello'` → `ParserError("Expected: an SQL statement, found: COMMENT at Line: 1, Column: 1")` | KAN-100 |
| `DROP TABLE` / `DROP VIEW` / `DROP SCHEMA` / `DROP FUNCTION` | Supported | `DROP TABLE cov_cm`, `DROP VIEW IF EXISTS cov_tv`, `DROP SCHEMA IF EXISTS cov_db2`, `DROP FUNCTION IF EXISTS cov_add` → all OK | — |
| `TRUNCATE TABLE` | Missing | `TRUNCATE TABLE cov_ctas` → `TRUNCATE operation on table 'cov_ctas' caused by This feature is not implemented: TRUNCATE not supported for Base table` | KAN-100 |
| `REPAIR TABLE` / `MSCK REPAIR TABLE` | Missing | `MSCK REPAIR TABLE cov_ddl3` → `Unsupported SQL statement: MSCK REPAIR TABLE cov_ddl3` | KAN-100 |
| `UNDROP TABLE` | N/A | `UNDROP TABLE cov_ddl_json` → `ParserError("Expected: an SQL statement, found: UNDROP")`. Unity Catalog table-recovery feature. | — |
| `SYNC` | N/A | `SYNC SCHEMA cov_db FROM cov_db` → `ParserError("Expected: an SQL statement, found: SYNC")`. Hive-metastore→Unity-Catalog upgrade command. | — |
| `REFRESH` (table / foreign / MV) | Missing | `REFRESH TABLE cov_t` → `ParserError("Expected: an SQL statement, found: REFRESH at Line: 1, Column: 1")` | KAN-100 |
| `DECLARE VARIABLE` / `SET VARIABLE` | Missing | `DECLARE VARIABLE cov_var INT DEFAULT 1` → `ParserError("Expected: CURSOR, found: cov_var")`; `SET VARIABLE cov_var = 2` → `ParserError("Expected: equals sign or TO, found: cov_var")` | KAN-91 |

## B. Statements — DML

| Manual section | Status | Evidence | Ticket |
|---|---|---|---|
| `INSERT INTO … VALUES` | Supported | `INSERT INTO cov_dml VALUES (1, 'x')` → OK; `SELECT count(*) FROM cov_dml` reflects the write. Spark's empty-result shape is reproduced by the `is_insert` branch in `Engine::sql` (`crates/oxidant-loom/src/spark_create_table.rs` → `is_insert`). | — |
| `INSERT INTO … SELECT` | Supported | `INSERT INTO cov_dml SELECT id, name FROM cov_t` → OK | — |
| `INSERT INTO` with column list | Supported | `INSERT INTO cov_dml (a, b) VALUES (9, 'z')` → OK | — |
| `INSERT INTO … BY NAME` | Missing | `INSERT INTO cov_dml BY NAME SELECT 1 AS a, 'q' AS b` → `ParserError("Expected: SELECT, VALUES, or a subquery in the query body, found: BY at Line: 1, Column: 23")` | KAN-91 |
| `INSERT OVERWRITE` (table) | Missing | `INSERT OVERWRITE cov_dml SELECT id, name FROM cov_t` and the `OVERWRITE TABLE` spelling → `Overwrites are not implemented yet for Parquet` | KAN-94 |
| `INSERT OVERWRITE DIRECTORY` | Missing | `INSERT OVERWRITE DIRECTORY '/tmp/probe/out' USING parquet SELECT id FROM cov_t` → `ParserError("Expected: SELECT, VALUES, or a subquery in the query body, found: USING at Line: 1, Column: 45")` | KAN-94 |
| `INSERT INTO … REPLACE WHERE` | Missing | `INSERT INTO cov_dml REPLACE WHERE a = 1 SELECT …` → `ParserError("… found: REPLACE at Line: 1, Column: 23")` | KAN-106 |
| `DELETE FROM` | Missing | `DELETE FROM cov_dml WHERE a = 9` → `DELETE operation on table 'cov_dml' caused by This feature is not implemented: DELETE not supported` | KAN-106 |
| `UPDATE` | Missing | `UPDATE cov_dml SET b = 'y' WHERE a = 1` → `UPDATE operation on table 'cov_dml' caused by This feature is not implemented: UPDATE not supported` | KAN-106 |
| `MERGE INTO` | Missing | Full `MERGE INTO cov_dml t USING cov_t s ON t.a = s.id WHEN MATCHED … WHEN NOT MATCHED …` → `Unsupported SQL statement: MERGE INTO …` | KAN-106 |
| `COPY INTO` | Missing | `COPY INTO cov_dml FROM '/tmp/probe/out' FILEFORMAT = PARQUET` → `ParserError("Expected: FROM or TO, found: cov_dml at Line: 1, Column: 11")` (the parser only knows PostgreSQL `COPY`) | KAN-112 |
| `LOAD DATA` | Missing | `LOAD DATA INPATH '/tmp/probe/out' INTO TABLE cov_dml` → `ParserError("Expected: `DATA` or an extension name after `LOAD`, found: INPATH")` | KAN-94 |
| `PUT INTO` | N/A | Not probed. Databricks volume-ingest statement, control-plane only. | — |

## C. Statements — Query (`SELECT` and its clauses)

| Manual section | Status | Evidence | Ticket |
|---|---|---|---|
| `SELECT` / projections / aliases | Supported | `SELECT 1 AS a, 'x' AS b` → `a:Int32, b:Utf8`; output naming goes through `crates/oxidant-loom/src/spark_names.rs` → `project_spark_names` | — |
| `WHERE` | Supported | `SELECT id FROM cov_t WHERE amt > 15` → 2 rows | — |
| `DISTINCT` | Supported | `SELECT DISTINCT name FROM cov_t` → 3 rows | — |
| `SELECT * EXCEPT (…)` | Supported | `SELECT * EXCEPT (amt) FROM cov_t` → `id:Int32, name:Utf8View` | — |
| `SELECT * REPLACE (…)` | Missing | `SELECT * REPLACE (id + 1 AS id) FROM cov_t` → `ParserError("Expected: end of statement, found: REPLACE at Line: 1, Column: 10")` | KAN-91 |
| `GROUP BY` (incl. ordinals, `GROUP BY ALL`) | Supported | `GROUP BY name`, `GROUP BY 1`, and `GROUP BY ALL` all → correct 3-group results | — |
| `GROUP BY ROLLUP` / `CUBE` / `GROUPING SETS` | Supported | All three probes → OK; `SELECT name, grouping(name) … GROUP BY ROLLUP(name)` also OK | — |
| `HAVING` | Supported | `… GROUP BY name HAVING sum(amt) > 5` → OK | — |
| `ORDER BY` (incl. `NULLS FIRST/LAST`, ordinals) | Supported | `ORDER BY id DESC`, `ORDER BY id ASC NULLS LAST`, `ORDER BY 1` → all OK | — |
| `LIMIT` / `OFFSET` | Supported | `SELECT id FROM cov_t ORDER BY id LIMIT 2 OFFSET 1` → OK | — |
| `SORT BY` | Missing | `SELECT id FROM cov_t SORT BY id` → `This feature is not implemented: SORT BY` | KAN-113 |
| `CLUSTER BY` (query clause) | Missing | `SELECT id FROM cov_t CLUSTER BY id` → `This feature is not implemented: CLUSTER BY` | KAN-113 |
| `DISTRIBUTE BY` | Missing | `SELECT id FROM cov_t DISTRIBUTE BY id` → `Physical plan does not support DistributeBy partitioning` | KAN-113 |
| `JOIN` — inner/left/right/full/cross | Supported | All five probes → OK | — |
| `JOIN` — `LEFT SEMI` / `LEFT ANTI` | Supported | `SELECT a.id FROM cov_t a LEFT SEMI JOIN cov_t b ON a.id = b.id` → OK; same for `LEFT ANTI` | — |
| `JOIN` — `NATURAL`, `USING` | Supported | `NATURAL JOIN` and `JOIN … USING (id)` → both OK | — |
| `LATERAL` subquery join | Missing | `SELECT t.id, s.x FROM cov_t t, LATERAL (SELECT t.id + 1 AS x) s` → `Physical plan does not support logical expression OuterReferenceColumn(…)` | KAN-116 |
| Set operators `UNION` / `UNION ALL` / `INTERSECT` / `EXCEPT` | Supported | All four probes → OK | — |
| Set operator `MINUS` | Missing | `SELECT 1 AS a MINUS SELECT 2` → `This feature is not implemented: MINUS  not implemented` | KAN-91 |
| Common table expressions (`WITH`, column list) | Supported | `WITH c AS (SELECT 1 AS a) SELECT a FROM c` and `WITH c(a) AS (SELECT 1) SELECT a FROM c` → OK | — |
| Recursive CTE (`WITH RECURSIVE`) | Missing | `WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM c WHERE n < 3) …` → `Schema error: No field named n. Valid fields are c."Int64(1)".`; the un-aliased variant → `Cannot project plan column 0 ('c.n + Int64(1)') to expected output field` | KAN-116 |
| Subqueries — scalar, `IN`, `EXISTS` (uncorrelated) | Supported | `SELECT (SELECT max(id) FROM cov_t)`, `… WHERE id IN (SELECT …)`, `… WHERE EXISTS (SELECT 1 …)` → OK | — |
| Correlated scalar subquery in `SELECT` | Missing | `SELECT id, (SELECT count(*) FROM cov_t u WHERE u.id <= t.id) AS c FROM cov_t t` → `Physical plan does not support logical expression ScalarSubquery(<subquery>)`. Correlated `IN` in `WHERE` does work. | KAN-116 |
| Window functions + `OVER` + frames | Supported | `row_number()/rank()/lag()/ntile()` over `ORDER BY`; `ROWS BETWEEN 1 PRECEDING AND CURRENT ROW` and `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` → all OK | — |
| `WINDOW` clause (named windows) | Supported | `SELECT id, rank() OVER w FROM cov_t WINDOW w AS (ORDER BY id)` → OK | — |
| `QUALIFY` | Supported | `SELECT id FROM cov_t QUALIFY row_number() OVER (ORDER BY id) = 1` → 1 row; `… <= 2` → 2 rows | — |
| `LATERAL VIEW` (incl. `OUTER`) | Missing | `… LATERAL VIEW explode(t.arr) e AS v` → `This feature is not implemented: LATERAL VIEWS` | KAN-114 |
| `PIVOT` | Missing | `… PIVOT (sum(amt) FOR name IN ('a','b'))` → `Unsupported ast node Pivot { … }` | KAN-97 |
| `UNPIVOT` | Missing | `… UNPIVOT (val FOR col IN (x, y))` → `Unsupported ast node Unpivot { … }` | KAN-97 |
| `TABLESAMPLE` | Partial | Parses but is ignored: `SELECT count(*) FROM cov_t TABLESAMPLE (1 ROWS)` → `3` on a 3-row table (Databricks returns 1). `TABLESAMPLE (50 PERCENT)` likewise returns everything. | KAN-111 |
| `VALUES` clause | Supported | `SELECT * FROM VALUES (1,'a'),(2,'b') AS t(num, letter)` → OK | — |
| Table-valued function `range()` | Partial | `SELECT * FROM range(3)` → works but the column is `value`, not Spark's `id`; `SELECT id FROM range(5)` → `Schema error: No field named id. Valid fields are "range()".value.` | KAN-99 |
| Table-valued functions `explode` / `inline` / `posexplode` in `FROM` | Missing | `SELECT * FROM explode(array(1,2))` → `table function 'explode' not found` (same for `inline`, `posexplode`) | KAN-93 |
| `IDENTIFIER()` clause | Missing | `SELECT * FROM IDENTIFIER('cov_t')` → `table function 'IDENTIFIER' not found` | KAN-91 |
| Hints (`/*+ BROADCAST(t) */`, `REPARTITION`) | Partial | Both probes return correct rows, so the comment is tolerated, but nothing consumes it — no code path reads Spark hints, and join strategy is chosen by `crates/oxidant-loom/src/lib.rs` → `join_preference` from env/statistics instead. | KAN-91 |
| Aggregate `FILTER (WHERE …)` | Supported | `SELECT count(*) FILTER (WHERE id > 1) FROM cov_t` → OK | — |
| `WITHIN GROUP (ORDER BY …)` | Supported | `SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY id) FROM cov_t` → `2.0` | — |
| `TRANSFORM … USING` (script transform) | Missing | `SELECT TRANSFORM(id) USING 'cat' AS (x) FROM cov_t` → `ParserError("Expected: end of statement, found: 'cat' at Line: 1, Column: 28")` | KAN-115 |
| `TABLE <name>` statement | Missing | `TABLE cov_t` → `ParserError("Expected: an SQL statement, found: TABLE at Line: 1, Column: 1")` | KAN-91 |
| Time travel `VERSION AS OF` / `TIMESTAMP AS OF` | Partial | Works on Delta: `SELECT count(*) FROM samples.tpch_nation_delta VERSION AS OF 0` → `25`. But it is silently ignored on non-Delta tables — `SELECT count(*) FROM cov_t VERSION AS OF 99` (a Parquet table, no version 99) → `3` instead of an error. | KAN-105 |
| `EXPLAIN` | Partial | `EXPLAIN SELECT 1 AS a` → `plan_type`/`plan` rows (DataFusion shape, not Spark's single `plan` column); `EXPLAIN FORMAT INDENT …` → OK; `EXPLAIN EXTENDED SELECT 1 AS a` → `ParserError("Expected: an SQL statement, found: EXTENDED at Line: 1, Column: 9")` | KAN-91 |
| Named parameter markers (`:param`) | N/A | Client-side binding in the Databricks SQL connector, not engine SQL surface. | — |
| Reading files directly (``parquet.`path` ``, sample tables) | Supported | `SELECT count(*) FROM samples.tpch_nation` → OK; Delta and Iceberg sample tables also read (`samples.tpch_nation_delta` → 25, `samples.tpch_nation_iceberg` → OK) | — |

## D. Statements — Auxiliary (`SHOW`, `DESCRIBE`, session, maintenance)

The `SHOW`/`DESCRIBE`/`USE` families are intercepted before DataFusion planning in
`crates/oxidant-loom/src/lib.rs` → `Engine::sql`, which dispatches to `parse_show` → `run_show`,
`parse_describe` → `run_describe`, and `parse_use` → `run_use` so the result *shape* matches
Spark's rather than DataFusion's.

| Manual section | Status | Evidence | Ticket |
|---|---|---|---|
| `SHOW CATALOGS` | Supported | `SHOW CATALOGS` → `catalog:Utf8` = `spark_catalog` | — |
| `SHOW DATABASES` / `SHOW SCHEMAS` | Supported | `SHOW DATABASES` → `namespace:Utf8` = `samples`, `cov_db`, `default` | — |
| `SHOW TABLES` (incl. `IN`, `LIKE`) | Supported | `SHOW TABLES` → `namespace`/`tableName`/`isTemporary`; `SHOW TABLES IN samples` and `SHOW TABLES LIKE 'cov*'` → OK | — |
| `SHOW COLUMNS` | Supported | `SHOW COLUMNS IN cov_t` → `col_name` = `id`, `name`, `amt` | — |
| `SHOW VIEWS` | Supported | `SHOW VIEWS` → `namespace`/`viewName`/`isTemporary`, with the temp view flagged | — |
| `SHOW TBLPROPERTIES` | Supported | `SHOW TBLPROPERTIES cov_ddl2` → `key`/`value` = `k`/`v` | — |
| `SHOW CREATE TABLE` | Supported | `SHOW CREATE TABLE cov_t` → `createtab_stmt` = ``CREATE TABLE spark_catalog.default.cov_t (\n  id INT,\n  name STRING,\n  amt DECIMAL(10,2))\nUSING parquet`` | — |
| `SHOW TABLE EXTENDED` | Supported | `SHOW TABLE EXTENDED LIKE 'cov_t'` → `namespace`/`tableName`/`isTemporary`/`information`, with `Catalog:`/`Database:`/`Provider:` lines | — |
| `SHOW PARTITIONS` | Partial | Returns the Spark `partition` column shape, but `SHOW PARTITIONS no_such_table_xyz` → 0 rows instead of a `TABLE_OR_VIEW_NOT_FOUND` error, and partitioned tables can't be created yet (see `CREATE TABLE … PARTITIONED BY`), so it has nothing real to list. | KAN-98, KAN-100 |
| `SHOW FUNCTIONS` | Partial | `SHOW FUNCTIONS` → 1 row per registered function (`abs`, `acos`, …), but `SHOW FUNCTIONS LIKE 'up*'` → 0 rows even though `upper` is registered, so the pattern filter is wrong. | KAN-98 |
| `SHOW TABLES DROPPED` | N/A | `SHOW TABLES DROPPED` → `SHOW TABLES FILTER not supported`. Unity Catalog `UNDROP` companion. | — |
| `SHOW GRANTS`, `SHOW USERS`, `SHOW GROUPS`, `SHOW SHARES`, `SHOW VOLUMES`, `SHOW CONNECTIONS` | N/A | All → `SHOW [VARIABLE] is not supported unless information_schema is enabled`. These enumerate Unity Catalog / workspace principals and securables. Lake Formation grants are the in-scope analogue and are KAN-102's job, not a `SHOW` statement. | — |
| `DESCRIBE TABLE` (incl. bare `DESCRIBE`) | Supported | `DESCRIBE TABLE cov_t` → `col_name`/`data_type`/`comment` with Spark type spellings (`int`, `string`, `decimal(10,2)`) | — |
| `DESCRIBE TABLE EXTENDED` | Partial | `DESCRIBE TABLE EXTENDED cov_t` → OK but returns exactly the same 3 column rows as plain `DESCRIBE`; Databricks appends the `# Detailed Table Information` block. | KAN-98 |
| `DESCRIBE QUERY` | Supported | `DESCRIBE QUERY SELECT 1 AS a` → `a`/`int`/`` | — |
| `DESCRIBE SCHEMA` / `DESCRIBE DATABASE` | Supported | `DESCRIBE SCHEMA default` → `info_name`/`info_value` rows (`Namespace Name`, `Comment`, `Location`) | — |
| `DESCRIBE CATALOG` | Supported | `DESCRIBE CATALOG spark_catalog` → `Catalog Name` / `spark_catalog` | — |
| `DESCRIBE FUNCTION` | Partial | `DESCRIBE FUNCTION upper` → `Function: upper`, but `Class: N/A` and `Usage: N/A` where Databricks returns the real class and usage string. | KAN-98 |
| `DESCRIBE DETAIL` | Missing | `DESCRIBE DETAIL cov_t` → `ParserError("Expected: end of statement, found: cov_t at Line: 1, Column: 17")` | KAN-105 |
| `DESCRIBE HISTORY` | Missing | `DESCRIBE HISTORY cov_t` → `ParserError("Expected: end of statement, found: cov_t at Line: 1, Column: 18")`; also fails on a real Delta table (`DESCRIBE HISTORY samples.tpch_nation_delta` → `ParserError("… found: samples at Line: 1, Column: 18")`) | KAN-105 |
| `USE` / `USE CATALOG` | Supported | `USE default` and `USE CATALOG spark_catalog` → OK (`crates/oxidant-loom/src/lib.rs` → `parse_use` / `run_use`) | — |
| `USE DATABASE` / `USE SCHEMA` keyword forms | Missing | `USE DATABASE default` → `Unsupported SQL statement: USE DATABASE default` — the bare and `CATALOG` spellings work, the `DATABASE`/`SCHEMA` keyword spellings do not | KAN-95 |
| `SET` (Spark conf) | Missing | `SET spark.sql.shuffle.partitions = 8` → `Invalid or Unsupported Configuration: Could not find config namespace "spark"`; bare `SET` (list all) → `ParserError("Expected: identifier, found: EOF")`. DataFusion's own namespace works: `SET datafusion.execution.batch_size = 4096` → OK. | KAN-91 |
| `SET TIME ZONE` | Missing | `SET TIME ZONE 'UTC'` → `SET variant not implemented yet: SetTimeZone { … }` | KAN-91 |
| `RESET` | Missing | `RESET spark.sql.shuffle.partitions` → `Invalid or Unsupported Configuration: Could not find config namespace "spark"` | KAN-91 |
| `ANALYZE TABLE` | Missing | `ANALYZE TABLE cov_t COMPUTE STATISTICS` and the `FOR COLUMNS` form → `Unsupported SQL statement: ANALYZE TABLE …` | KAN-101 |
| `CACHE TABLE` / `UNCACHE TABLE` / `CLEAR CACHE` / `CACHE SELECT` | Missing | `CACHE TABLE cov_cache AS SELECT …` → `Unsupported SQL statement`; `CLEAR CACHE` → `ParserError("Expected: an SQL statement, found: CLEAR")`; `CACHE SELECT …` → `ParserError("Expected: a `TABLE` keyword, found: id")` | KAN-91 |
| `OPTIMIZE` | Missing | `OPTIMIZE cov_t` → `Unsupported SQL statement: OPTIMIZE cov_t` | KAN-105 |
| `VACUUM` | Missing | `VACUUM cov_t` → `Unsupported SQL statement: VACUUM cov_t` | KAN-105 |
| `RESTORE` | Missing | `RESTORE TABLE cov_t TO VERSION AS OF 0` → `ParserError("Expected: an SQL statement, found: RESTORE")` | KAN-105 |
| `CONVERT TO DELTA` | Missing | ``CONVERT TO DELTA parquet.`/tmp/probe/out` `` → `ParserError("Expected: an SQL statement, found: CONVERT")` | KAN-105 |
| `GENERATE` (symlink manifest) | Missing | `GENERATE symlink_format_manifest FOR TABLE cov_t` → `ParserError("Expected: an SQL statement, found: GENERATE")` | KAN-105 |
| `FSCK REPAIR TABLE` | Missing | `FSCK REPAIR TABLE cov_t` → `ParserError("Expected: an SQL statement, found: FSCK")` | KAN-105 |
| `LIST` / `GET` / `PUT` / `REMOVE` (volumes) | N/A | `LIST '/Volumes/x/y/z'` → `ParserError("Expected: an SQL statement, found: LIST")`. Unity Catalog volumes. | — |

## E. Statements — Security and privileges

Databricks expresses privileges through Unity Catalog. The in-scope analogue for Oxidant is AWS
Lake Formation, which the epic tackles as a plan-rewrite/authorization layer (KAN-102..KAN-104)
rather than as SQL `GRANT` statements.

> **Updated.** This section previously read "There is no Lake Formation code in the tree today".
> That is no longer true. `crates/oxidant-catalog-lakeformation` resolves Lake Formation
> authorization, and column/row enforcement is applied to scans via
> `oxidant-loom/src/lakeformation_provider.rs` — see
> [`catalogs-lakeformation.md`](catalogs-lakeformation.md). The SQL `GRANT`/`REVOKE`/`DENY`
> statements below are still unimplemented; enforcement is configured, not expressed in SQL.

| Manual section | Status | Evidence | Ticket |
|---|---|---|---|
| `GRANT` | Missing | ``GRANT SELECT ON TABLE cov_t TO `u@example.com` `` → `Unsupported SQL statement: GRANT SELECT ON cov_t TO …` | KAN-102 |
| `REVOKE` | Missing | ``REVOKE SELECT ON TABLE cov_t FROM `u@example.com` `` → `Unsupported SQL statement: REVOKE SELECT ON cov_t FROM …` | KAN-102 |
| `DENY` | Missing | ``DENY SELECT ON TABLE cov_t TO `u@example.com` `` → `Unsupported SQL statement: DENY SELECT ON cov_t TO …` | KAN-102 |
| Row filters / column masks | **Supported** (not via SQL) | Enforced from Lake Formation, not granted in SQL: `glue:GetUnfilteredTableMetadata` resolves the caller's authorized columns + row filter and `LakeFormationTableProvider` applies them to every scan. Denied columns are absent from the schema; row filters are `AND`-ed into the scan. Opt in with `spark.sql.catalog.<n>.lakeformation=true`. | KAN-103 |
| Cross-account shared catalogs (resource links) | Missing | Same: no Lake Formation client in the tree. | KAN-104 |
| `CREATE GROUP` / `ALTER GROUP` / `DROP GROUP` | N/A | `CREATE GROUP cov_grp` → `ParserError("Expected: an object type after CREATE, found: GROUP")`. Workspace identity management. | — |
| `SHOW GRANTS ON SHARE` / `TO RECIPIENT`, `GRANT SHARE` | N/A | Delta Sharing control plane. | — |
| `REPAIR PRIVILEGES` | N/A | `MSCK REPAIR TABLE cov_t SYNC METADATA` → `ParserError("Expected: end of statement, found: SYNC")`. Unity Catalog privilege sync. | — |

## F. Functions

> **Superseded for this axis.** The per-category ratios below were probe-sampled by hand against a
> 244-function subset at engine commit `b3f3f2e`. [`databricks-functions.md`](databricks-functions.md)
> now measures the same axis **exactly and reproducibly** — it diffs oxidant's *live* function
> registry against all 606 documented Databricks function names, and is regenerated by
> `oxidant-parity functions --markdown`. Read that file for the current number; the rows here are
> kept for their per-function detail and their probe evidence, and are marked where they are stale.
>
> Current exact coverage: **325 / 440 in-scope functions (73.9%)**. The 166 out-of-scope names, and
> why each is excluded, are enumerated in that file and in
> `crates/oxidant-spark-compat/databricks-functions.json`.

Oxidant gets functions from three places: DataFusion's built-ins, Spark-name aliases onto
DataFusion built-ins (`crates/oxidant-loom/src/lib.rs` → `register_spark_function_aliases`), and
Spark-only UDFs implemented in `crates/oxidant-loom/src/spark_functions/` (`mod.rs` → `register`).
A missing function surfaces as `Error during planning: Invalid function '<name>'`.

A source-level grep cannot substitute for the live registry: DataFusion generates much of its
function set through macros (`make_math_unary_udf!`) and `aliases()`, so a static diff under-counts
what is registered by roughly eighty names. `Engine::registered_function_names` (which also answers
`SHOW FUNCTIONS`) is the authority.

Ratios below are "probes passing / probes run" for that category in the first pass; the named
functions are the ones that actually failed.

| Manual category | Status | Evidence | Ticket |
|---|---|---|---|
| Aggregate functions | Partial | 21/30. Work: `count`, `sum`, `avg`, `min`/`max`, `stddev*`, `var*`, `corr`, `covar_*`, `collect`-via-`array_agg`, `any_value`, `bool_and`/`bool_or`, `approx_count_distinct`, `percentile`, `median`, `mode`, `count_if`, `bit_and/or/xor`, `regr_*`, `try_sum`/`try_avg`, `sum(DISTINCT …)`. Missing: `first`, `last`, `collect_list`, `collect_set`, `approx_percentile`, `percentile_approx`, `kurtosis`, `skewness`, `max_by`, `min_by`, `listagg`, `histogram_numeric`. Also `SELECT every(x) , some(y), any(y)` → `Schema contains duplicate unqualified field name "bool_or(…)"` because the aliases collapse onto one name. | KAN-92, KAN-93 |
| Window / analytic functions | Supported | 6/6: `row_number`, `rank`, `dense_rank`, `percent_rank`, `cume_dist`, `ntile`, `lag`, `lead`, `nth_value` all evaluate over `OVER (ORDER BY …)` | — |
| Array functions | Partial | 14/22. Missing: `size`, `sequence`, `shuffle`, `slice`, `array_insert`, `get`, plus `array_prepend` (arity error) and one `array_append` planning error. | KAN-93 |
| Map functions | Partial | 6/10. Missing: `map_concat`, `map_from_arrays`, `map_from_entries`, `str_to_map`. Working: `map_keys`, `map_values`, `map_contains_key`, `map_entries`, `element_at`, `try_element_at`, `cardinality`. | KAN-93 |
| Struct functions | Partial | 1/5 probes (one probe bundled `named_struct`+`struct`+`to_json` as a single pass). Those three construct/serialize fine, but field access via a dot on a constructed struct fails (see Operators), and `schema_of_json` is missing. | KAN-91, KAN-93 |
| Lambda / higher-order functions | Missing | 0/11 — `transform`, `filter`, `exists`, `forall`, `aggregate`, `reduce`, `zip_with`, `transform_keys`, `transform_values`, `map_filter`, `map_zip_with` all → `Invalid function`. The `->` lambda operator itself is therefore also untested against a real callee. | KAN-93 |
| String functions | Partial | 29/41. Missing: `locate`, `format_number`, `printf`, `soundex`, `base64`, `unbase64`, `sentences`, `space`, `collate`, `is_valid_utf8`, `luhn_check`. Diverging: `encode('a','utf-8')` → `There is no built-in encoding named 'utf-8'` (only `base64`/`base64pad`/`hex`); `regexp_extract_all('aab','a')` → `[INVALID_PARAMETER_VALUE.REGEX_GROUP_INDEX] … expected a group index between 0 and 0, got 1` where Databricks defaults to group 0. | KAN-92, KAN-93 |
| Math functions | Partial | **Stale — was 13/20.** `e`, `bit_get`, `negative`, `width_bucket`, `hypot`, `bit_reverse`, `rint`, `expm1`, `log1p`, `sec`, `csc`, `mod` and `pmod` are now registered (`spark_functions/spark_math2.rs`, plus the alias tables). Still missing: `randn`, `uniform`, and `ceiling` — the last deliberately: DataFusion's `ceil` returns DOUBLE for a non-DECIMAL argument where Databricks returns BIGINT, so aliasing `ceiling` onto it added four fresh wrong answers in `operators.sql.out` rather than closing a gap. `ceil`/`floor` carry the same pre-existing divergence; fixing the return type the way `spark_math.rs` fixes `round` would close all three at once. Diverging: `bround(2.5)` → `bround on non-integral input (Float64) is not supported`. Working: `abs`, `ceil`, `floor`, `round`, `exp`/`ln`/`log`/`log2`/`log10`, `pow`/`power`/`sqrt`/`cbrt`, all trig and hyperbolic, `degrees`/`radians`/`pi`, `sign`/`signum`, `greatest`/`least`, `factorial`, `conv`, `shiftleft`/`shiftright`/`shiftrightunsigned`, `bit_count`, `try_add`/`try_subtract`/`try_multiply`/`try_divide`, `isnan`, `nanvl`. | KAN-92, KAN-93 |
| Date/time functions | Partial | **Stale — was 8/28, now 62/82** (see [`databricks-functions.md`](databricks-functions.md)). Added since: `year`, `month`, `day`, `dayofmonth`, `quarter`, `hour`, `minute`, `second`, `dayofweek`, `weekday`, `dayofyear`, `weekofyear`, `dayname` (`spark_functions/spark_datetime4.rs`); `datediff`, `date_diff`, `dateadd`, `add_months`, `last_day`, `months_between` (`spark_datetime5.rs`); `current_timezone`, `from_utc_timestamp`, `to_utc_timestamp`, `convert_timezone` (`spark_timezone.rs`); `unix_micros`, `curdate`, `getdate`. Still missing: `timestampadd`, `timestampdiff`, and the three-argument unit forms of `datediff`/`dateadd` — **not** a registration gap: sqlparser's Databricks dialect parses the bare unit keyword as a column reference, so `datediff(MONTH, a, b)` fails with `No field named month` before any UDF runs, and closing it needs a parser/`ExprPlanner` change. Also missing: `make_interval`, `window`, `session_window`, the `time_*` family. Working: `current_date`, `current_timestamp`, `now`, `date_add`/`date_sub`, `date_format`, `date_trunc`, `to_date`, `to_timestamp`, `from_unixtime`, `make_date`, `make_timestamp`, `extract`/`date_part`, `unix_date`, `try_to_timestamp`, `unix_timestamp(expr, fmt)`. Diverging: `trunc(DATE, 'MM')` coerces only numerics; `to_date('01/02/2024','MM/dd/yyyy')` → `input contains invalid characters` (Java format patterns aren't understood — the strftime spelling `'%m/%d/%Y'` works); `unix_timestamp()` rejects zero arguments. | KAN-92, KAN-93 |
| Conditional functions | Supported | **Stale — was 6/8.** `isnull`, `isnotnull`, `equal_null` and `iff` are now registered (`spark_functions/spark_conditional.rs`, `spark_if.rs`), each lowered by `simplify()` into the native `IS NULL` / `<=>` / `CASE` expression so the optimizer can still push and fold them. Working: `coalesce`, `nvl`, `nvl2`, `nullif`, `ifnull`, `if`, `nullifzero`, `zeroifnull`. | KAN-92 |
| Conversion functions | Supported | 7/7: `cast`, `try_cast`, the constructor aliases (`bigint`/`int`/`string`/`double`/`boolean`/`date`/`timestamp`/`binary`/`decimal`), and `typeof` (`crates/oxidant-loom/src/spark_functions/spark_cast_constructors.rs`) | — |
| Predicate functions | Supported | **Stale — was 2/5.** `regexp_like`, `regexp` and `rlike` already worked; `isnull` and the function spellings `like(x, p)` / `ilike(x, p)` are now registered (`spark_functions/spark_conditional.rs`). The optional third `escape` argument accepts only a backslash — DataFusion's LIKE kernel implements no other, and a planning error beats matching against the wrong escape. | KAN-92 |
| Hash functions | Partial | 4/6. `sha`, `sha1`, `sha2`, `crc32` now work (`crates/oxidant-loom/src/spark_functions/spark_hash.rs`); `hash` and `xxhash64` remain missing. **The earlier note that this was "alias work, not new implementation" was wrong:** DataFusion 54's `DigestAlgorithm` (`datafusion-functions-54.1.0/src/crypto/basic.rs`) is `Md5, Sha224, Sha256, Sha384, Sha512, Blake2s, Blake2b, Blake3` — it has **no SHA-1 and no CRC-32** to alias onto. `hash`/`xxhash64` are deferred for a stronger reason: they hash Spark's *internal row representation*, and Spark's Murmur3 uses a non-standard tail rule, so they need a byte-level port verified against a real Spark. | KAN-92, KAN-93 |
| JSON functions | Partial | 4/6. Working: `get_json_object`, `from_json`, `to_json`, `json_array_length`/`json_object_keys` (`crates/oxidant-loom/src/spark_functions/spark_json.rs`, `spark_from_json.rs`). Missing: `json_tuple`, `schema_of_json`. | KAN-93 |
| CSV functions | Partial | 1/3. `to_csv` works; `schema_of_csv` missing; `from_csv('1,a','x INT, y STRING')` → `Internal error: Assertion failed: result_data_type == *expected_type: Function 'from_csv' returned value of type 'Utf8' while … expected: 'Struct()'` — an outright bug in `crates/oxidant-loom/src/spark_functions/spark_csv.rs`. | KAN-93 |
| XML functions | Missing | 0/4 — `xpath_string`, `from_xml`, `to_xml`, `schema_of_xml` all `Invalid function`. | KAN-93 |
| VARIANT functions | Missing | 0/6 — `parse_json`, `variant_get`, `try_variant_get`, `is_variant_null`, `schema_of_variant`, `to_variant_object` all `Invalid function`. Blocked on the VARIANT type itself (see Data types). | KAN-93 |
| URL functions | Partial | 2/3. `parse_url`, `url_encode`/`url_decode` work; `try_parse_url` missing. | KAN-93 |
| Generator (table-valued) functions | Missing | 0/5 — `explode`, `explode_outer`, `posexplode`, `inline`, `stack` all `Invalid function`, in both `SELECT` and `FROM` position. | KAN-93 |
| Misc / system functions | Partial | 4/15. Working: `current_catalog`, `current_database`, `current_schema`, `version`, `uuid`, `assert_true`. Missing: `current_user`/`user`/`session_user`, `current_version`, `monotonically_increasing_id`, `spark_partition_id`, `input_file_name`, `raise_error`, `aes_encrypt`, `bitmap_count`, `java_method`, `reflect`, `stack`. | KAN-93 |
| Geospatial functions (`ST_*`) | Missing | 0/3 — `st_point`, `st_area`, `st_astext` → `Invalid function`. | KAN-93 |
| AI functions (`ai_query`, `ai_analyze_sentiment`, `vector_search`) | N/A | 0/3 — all `Invalid function`. Require Databricks Model Serving / Vector Search. | — |
| SQL UDFs (`CREATE FUNCTION … RETURN`) | Partial | Registers, but evaluates wrong: after `CREATE OR REPLACE FUNCTION cov_add2(x INT) RETURNS INT RETURN x + 100`, both `SELECT cov_add2(1)` and `SELECT cov_add2(5)` → `1`. | KAN-110 |

## G. Data types

| Manual section | Status | Evidence | Ticket |
|---|---|---|---|
| `BOOLEAN` | Supported | `SELECT CAST('true' AS BOOLEAN)` → `true` | — |
| `TINYINT` / `SMALLINT` / `INT` / `BIGINT` | Supported | All four casts → OK with the matching Arrow width | — |
| `FLOAT` / `DOUBLE` | Supported | `CAST(1.5 AS FLOAT)`, `CAST(1.5 AS DOUBLE)` → OK | — |
| `DECIMAL(p,s)` | Supported | `CAST(1.5 AS DECIMAL(10,2))` → `v:Decimal128(10, 2)` = `1.5`; decimal division follows Spark's rules (`DECIMAL(10,2) / DECIMAL(10,2)` → `Decimal128(16, 6)` = `0.333333`) | — |
| `STRING` | Supported | `SELECT typeof(CAST('a' AS STRING))` → `string` | — |
| `CHAR(n)` / `VARCHAR(n)` | Partial | Both parse and cast to `Utf8View`, but the length is not applied: `SELECT concat('[', CAST('ab' AS CHAR(4)), ']')` → `[ab]`, where Databricks pads to `[ab  ]`. | KAN-117 |
| `BINARY` | Missing | `SELECT CAST('ab' AS BINARY)` → `This feature is not implemented: Unsupported SQL type BINARY` | KAN-117 |
| `DATE` | Supported | `SELECT DATE '2024-01-02'` → OK | — |
| `TIMESTAMP` | Supported | `SELECT TIMESTAMP '2024-01-02 03:04:05'` → OK | — |
| `TIMESTAMP_NTZ` | Missing | `SELECT CAST('2024-01-02 03:04:05' AS TIMESTAMP_NTZ)` → `Unsupported SQL type TIMESTAMP_NTZ` | KAN-117 |
| `INTERVAL` — year-month | Partial | Single-unit works (`INTERVAL '3' MONTH` → `Interval(MonthDayNano)` = `3 mons`); the compound form does not: `INTERVAL '1-2' YEAR TO MONTH` → `Unsupported Interval Expression with last_field Some(Month)`. Arrow also has one interval type where Spark has two distinct ones. | KAN-117 |
| `INTERVAL` — day-time | Partial | Same split: `INTERVAL '5' DAY` → OK; `INTERVAL '1 02:03:04' DAY TO SECOND` → `Unsupported Interval Expression with last_field Some(Second)` | KAN-117 |
| `ARRAY<T>` | Supported | `SELECT array(1,2,3)` → `List(Int32)` = `[1, 2, 3]`; `CAST(array(1,2) AS ARRAY<BIGINT>)` → OK; `typeof(array(1))` → `array<int>` | — |
| `MAP<K,V>` | Partial | Values are right — `SELECT map('a',1,'b',2)` → `{"a": 1, "b": 2}` — but the type name is not: `typeof(map('a',1))` → ``map(field { name: "entries", data_type: struct([…]) }, false)`` instead of Databricks' `MAP<STRING, INT>`. | KAN-99 |
| `STRUCT<…>` | Supported | `SELECT named_struct('a',1,'b','x')` → `Struct("a": Int32, "b": Utf8)`; `struct(1 AS a, 'x' AS b)` also OK | — |
| `VARIANT` | Missing | `SELECT CAST(NULL AS VARIANT)` → `Unsupported SQL type VARIANT`; `parse_json` → `Invalid function 'parse_json'` | KAN-117, KAN-93 |
| `OBJECT` | Missing | `SELECT CAST(NULL AS OBJECT<a: INT>)` → `ParserError("Expected: ), found: < at Line: 1, Column: 27")` | KAN-117 |
| `GEOGRAPHY` / `GEOMETRY` | Missing | `st_geogfromtext` / `st_geomfromtext` → `Invalid function` | KAN-93 |
| `VOID` / `NULL` | Supported | `SELECT NULL` → OK; `SELECT typeof(NULL)` → OK | — |

## H. Operators

| Manual section | Status | Evidence | Ticket |
|---|---|---|---|
| Arithmetic `+` `-` `*` `%` and unary `-` | Supported | `SELECT 1+2, 5-2, 3*4, 7 % 2, -(3)` → all OK | — |
| Division `/` (Spark true division) | Supported | `SELECT 7 / 2` → `Float64` `3.5` (not integer `3`), via `crates/oxidant-loom/src/lib.rs` → `SparkDividePlanner`; `SELECT 5 / 0` → `[DIVIDE_BY_ZERO] … SQLSTATE: 22012`, matching Spark ANSI | — |
| Integer division `DIV` | Missing | `SELECT 7 DIV 2` → `ParserError("No infix parser for token Word(Word { value: \"DIV\" … })")`; lowercase `div` fails identically | KAN-91 |
| Comparison `=` `==` `<>` `!=` `<` `<=` `>` `>=` | Supported | All probes → OK, including Spark's `==` spelling | — |
| Null-safe equality `<=>` | Supported | `SELECT 1 <=> NULL, NULL <=> NULL` → `false`, `true` | — |
| Logical `AND` / `OR` / `NOT` | Supported | `SELECT true AND false, true OR false, NOT true` → OK | — |
| `!` (prefix NOT) | Missing | `SELECT !true` → `Unsupported SQL unary operator BangNot` | KAN-91 |
| String concatenation `\|\|` | Supported | `SELECT 'a' \|\| 'b'` → `ab` | — |
| Bitwise `&` `\|` `^` | Supported | `SELECT 6 & 3, 6 \| 3, 6 ^ 3` → OK | — |
| Bitwise `~` (NOT) | Missing | `SELECT ~6` → `Unsupported SQL unary operator BitwiseNot` | KAN-91 |
| Shift `<<` `>>` `>>>` | Missing | `SELECT 1 << 3` → `ParserError("No infix parser for token ShiftLeft")`; `>>>` likewise. The function spellings do work: `SELECT shiftleft(1,3), shiftright(16,2)` → OK, so this is operator syntax only. | KAN-91 |
| `BETWEEN` | Supported | `SELECT 2 BETWEEN 1 AND 3` → `true` | — |
| `IN` (list and subquery) | Supported | `SELECT 2 IN (1,2,3)` → OK; `… WHERE id IN (SELECT …)` → OK, including the correlated form | — |
| `EXISTS` | Supported | `SELECT id FROM cov_t t WHERE EXISTS (SELECT 1 FROM cov_t u WHERE u.id = t.id)` → OK | — |
| `= ANY (subquery)` | Missing | `SELECT 1 = ANY (SELECT id FROM cov_t)` → `Physical plan does not support logical expression Exists(…)` | KAN-116 |
| `LIKE` (incl. `ESCAPE`) | Supported | `SELECT 'abc' LIKE 'a%'` → `true`; `'a_c' LIKE 'a\_c'` → OK | — |
| `ILIKE` | Supported | `SELECT 'ABC' ILIKE 'a%'` → `true` | — |
| `RLIKE` / `REGEXP` (operator form) | Missing | `SELECT 'abc' RLIKE '^a'` → `Unsupported ast node in sqltorel: RLike { … }` (same for `REGEXP`). The function form works: `SELECT regexp_like('abc','^a')` → OK. | KAN-91 |
| `LIKE ANY` / `LIKE ALL` | Supported | `SELECT 'abc' LIKE ANY ('a%','z%')` → `true`; `SELECT 'abc' LIKE ALL ('a%','%c')` → `true`. Lowered to an OR/AND chain in `crates/oxidant-loom/src/lib.rs` → `lower_like_quantifiers`. KAN-96's acceptance criteria appear already met. | — |
| `IS NULL` / `IS NOT NULL` / `IS TRUE` / `IS DISTINCT FROM` | Supported | All probes → OK | — |
| `CASE` (searched and simple) | Supported | Both forms → OK | — |
| Cast operator `::` | Supported | `SELECT '1'::INT` → OK | — |
| Field access `.` on a struct column | Supported | `SELECT s.a FROM (SELECT named_struct('a',1) AS s) t` → `1` | — |
| Field access `.` on an inline struct expression | Missing | `SELECT named_struct('a',1).a` → `Dot access not supported for non-string expr: Identifier(…)`. This is the same round-trip hazard `sanitize_generated_sql` works around in `crates/oxidant-execution/src/plan/stage_planner.rs`. | KAN-91 |
| Subscript `[]` on arrays and maps | Partial | Map subscript works (`SELECT map('a',1)['a']` → `1`). Array subscript is **1-based** here: `SELECT array(10,20,30)[0] AS z, array(10,20,30)[1] AS o` → `z=null, o=10`. Spark/Databricks `[]` is 0-based (expected `z=10, o=20`); `element_at` is the 1-based form. | KAN-118 |
| Lambda `->` | Missing | `SELECT transform(array(1,2), x -> x + 1)` → `Invalid function 'transform'`. No higher-order function exists to apply a lambda to, so the operator is untestable today. | KAN-93 |
| JSON path `:` | Missing | `SELECT '{"a":1}':a` → `Binary operator 'Colon' is not supported in the physical expr`; the VARIANT spelling fails earlier at `parse_json`. | KAN-117, KAN-93 |

## Ticket map

[`docs/databricks-parity-plan.md`](databricks-parity-plan.md) §Stories assigns the placeholder
IDs `OXIDANT-DBR-001..020`. They map in order onto both the Jira keys used above and the story
rows (lines 3–22) of `jira/databricks-parity-tickets.csv`, which has no ID column of its own —
mapping is positional by CSV row order.

| Plan ID | Jira | Summary |
|---------|------|---------|
| OXIDANT-DBR-001 | KAN-89 | Build Databricks SQL coverage matrix (this document) |
| OXIDANT-DBR-002 | KAN-90 | Add Databricks-specific SQL corpus to parity harness |
| OXIDANT-DBR-003 | KAN-91 | Implement staged `oxidant-sql` dialect pipeline |
| OXIDANT-DBR-004 | KAN-92 | Register Wave A function aliases |
| OXIDANT-DBR-005 | KAN-93 | Implement high-value scalar UDF backlog |
| OXIDANT-DBR-006 | KAN-94 | `CREATE TABLE … USING <format>` for Glue/local catalog |
| OXIDANT-DBR-007 | KAN-95 | `USE CATALOG` / `USE DATABASE` / `USE SCHEMA` |
| OXIDANT-DBR-008 | KAN-96 | `LIKE ANY` / `LIKE ALL` |
| OXIDANT-DBR-009 | KAN-97 | `PIVOT` / `UNPIVOT` |
| OXIDANT-DBR-010 | KAN-98 | `SHOW` / `DESCRIBE` metadata statements |
| OXIDANT-DBR-011 | KAN-99 | Spark output-name reconciliation pass |
| OXIDANT-DBR-012 | KAN-100 | Extend Glue catalog DDL: ALTER/DROP/CREATE DATABASE/REPAIR |
| OXIDANT-DBR-013 | KAN-101 | Glue column statistics via `ANALYZE TABLE` |
| OXIDANT-DBR-014 | KAN-102 | Add Lake Formation authorization crate |
| OXIDANT-DBR-015 | KAN-103 | Apply Lake Formation row filters and column masks to scans |
| OXIDANT-DBR-016 | KAN-104 | Lake Formation cross-account resource links |
| OXIDANT-DBR-017 | KAN-105 | Delta Lake SQL on Glue: CONVERT/VACUUM/OPTIMIZE/RESTORE |
| OXIDANT-DBR-018 | KAN-106 | Delta `MERGE`/`UPDATE`/`DELETE` on Glue tables |
| OXIDANT-DBR-019 | KAN-107 | Databricks parity ratchet CI job |
| OXIDANT-DBR-020 | KAN-108 | Document Glue + Lake Formation SQL support |

Follow-on tickets filed from this matrix (and its review), also under epic KAN-88:

| Jira | Type | Summary | Owns matrix rows |
|------|------|---------|------------------|
| KAN-110 | Bug | SQL UDF body not evaluated | `CREATE FUNCTION`, SQL UDFs |
| KAN-111 | Bug | `TABLESAMPLE` parsed then ignored | `TABLESAMPLE` |
| KAN-112 | Story | `COPY INTO` file ingest | `COPY INTO` |
| KAN-113 | Story | `SORT BY` / `CLUSTER BY` / `DISTRIBUTE BY` | those three query clauses |
| KAN-114 | Story | `LATERAL VIEW` (incl. `OUTER`) | `LATERAL VIEW` |
| KAN-115 | Story | `TRANSFORM … USING` | `TRANSFORM … USING` |
| KAN-116 | Story | Planner: correlated / `LATERAL` / recursive CTE / `= ANY` | recursive CTE, correlated scalar, `LATERAL` join, `= ANY` |
| KAN-117 | Story | Type system: `BINARY`, `TIMESTAMP_NTZ`, `VARIANT`/`OBJECT`, compound `INTERVAL`, `CHAR(n)` | those type rows (+ JSON `:` with KAN-93) |
| KAN-118 | Bug | Array `[]` is 1-based; Spark/Databricks are 0-based | array/map subscript |

KAN-91 keeps dialect/AST surface only (operators, `SELECT * REPLACE`, `MINUS`, session/`CACHE`/`EXPLAIN` forms, hints, `IDENTIFIER`, `TABLE`, struct-dot AST). Planner and type-system gaps that a pipeline shell cannot close were moved to KAN-116 / KAN-117.

KAN-94 / KAN-100 / KAN-105 acceptance criteria in the plan and `jira/` import artifacts were broadened so the matrix rows citing them stay inside those tickets' stated scope (see plan §Stories).

Five tickets have no **Missing**/**Partial** rows pointing at them, for different reasons:
KAN-89 is this document; KAN-90 adds golden coverage but does not implement language features;
KAN-96 (`LIKE ANY`/`LIKE ALL`) is already implemented and both probes pass, so it looks
closeable on inspection; KAN-107 and KAN-108 are process/docs tickets that gate the work
rather than appearing as a language-surface gap.

## Where the plan document is out of date

Beyond the baseline numbers, three of the plan's premises no longer hold, because work landed
between the plan being written and `b3f3f2e`:

- The plan calls `oxidant-sql` a "stub" and treats `SHOW`/`DESCRIBE`/`USE` as unimplemented.
  They are implemented today in `crates/oxidant-loom/src/lib.rs` (`parse_show`/`run_show`,
  `parse_describe`/`run_describe`, `parse_use`/`run_use`) and the probes above pass. KAN-98 and
  KAN-95 are smaller than scoped — mostly filters, `EXTENDED` detail, and keyword spellings.
- `CREATE TABLE … USING <format>` is described as unimplemented and "the largest parser/missing-relation
  driver". The plain form works (`spark_create_table.rs`), including CTAS. What is still missing
  is the modifier grammar: `LOCATION`, `PARTITIONED BY`, `CLUSTER BY`.
- The output-naming pass (KAN-99) exists as `crates/oxidant-loom/src/spark_names.rs`
  (`project_spark_names`); the `schema-only` bucket is 2,141 blocks with it already in place.

## Not verified

Listed rather than guessed:

- **Constraint enforcement.** `CREATE TABLE … CONSTRAINT pk PRIMARY KEY (a)` parses, but I did not
  test whether a duplicate insert is rejected or whether the constraint is reported by
  `SHOW CREATE TABLE`. Marked Supported (parse-only) on the parse evidence alone.
- **Glue-backed behavior for every DDL/DML row.** All probes ran against the built-in
  `spark_catalog` on local Parquet. Rows that a Glue catalog might answer differently
  (`SHOW PARTITIONS`, `ANALYZE TABLE`, `ALTER TABLE`) were not re-run against Glue — that needs
  AWS credentials this environment does not have.
- **Lake Formation rows.** Originally scored Missing from the absence of any `aws-sdk-lakeformation`
  dependency or filter/mask plan rewrite in the tree — a code path, not a probe. Both now exist and
  are covered by tests against a stub Lake Formation/Glue endpoint plus end-to-end SQL over a real
  Parquet table, so the row-filter/column-mask row above is scored from tests rather than from a
  live AWS probe.
- **`PUT INTO`.** Not probed; marked N/A from the manual's description as a Unity Catalog volume
  operation.
- **Value-level Databricks equivalence.** A probe "passing" means the statement executed and
  returned a plausible Spark-shaped result. Except where a row says otherwise, results were not
  diffed against a real Databricks warehouse. The `parity/baseline.json` ratchet is what does
  value-level comparison, and it does it against Spark v4.0.0, not Databricks — which is exactly
  the hole KAN-90 and KAN-107 exist to fill.

## Reproducing

```sh
cargo build -p oxidant-cli
./target/debug/oxidant spark server --port 50051 --sample-data sample-data --foreground &

# Any row's Evidence column is a statement you can replay:
./target/debug/oxidant sql -e "SELECT 'abc' LIKE ANY ('a%','z%') AS v"
./target/debug/oxidant sql -e "SELECT 7 DIV 2 AS v"   # expect the DIV ParserError
./target/debug/oxidant sql -e "SHOW TABLES IN samples"
```

Baseline counts come straight from the checked-in file:

```sh
python3 -c "import json;d=json.load(open('parity/baseline.json'));print(d['strict_pass'],d['semantic_pass'],d['blocks_total'])"
```

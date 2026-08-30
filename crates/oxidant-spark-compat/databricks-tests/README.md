# databricks-tests — authored Databricks SQL parity corpus

Companion corpus to the vendored Spark `sql-tests` (`../spark-tests/`). Where the Spark corpus
measures parity against Apache Spark's own golden outputs, this corpus measures parity against
the **Databricks SQL language manual** statement categories
(<https://docs.databricks.com/en/sql/language-manual/>). The files are *authored*, not vendored:
each `inputs/*.sql` exercises one manual category and each `results/*.sql.out` records the
expected Databricks result in Spark's `SQLQueryTestSuite` golden format (`-- !query` /
`-- !query schema` / `-- !query output`), so the existing runner/classifier/reporter pipeline
scores them unchanged.

## Layout

- `inputs/` — one `.sql` file per manual category. Setup (`CREATE … TEMPORARY VIEW …`) runs
  before golden replay exactly like the Spark corpus.
- `results/` — authored goldens. Statements that are valid Databricks SQL carry the expected
  `struct<…>` schema + tab-separated rows.
- Skipped files start with a `--SKIP <reason>` directive; the harness records the skip and the
  reason in the report (never silently dropped). The audit table below records every skip,
  including catalog/storage dependencies and non-deterministic output ordering.

## Golden audit and sources

These goldens are deliberately not treated as a substitute for a Databricks warehouse run.
Every scored file below has an auditable source for both its supported syntax and expected
result. The scored row-producing statements include an `ORDER BY` where the source clause
does not promise a total output order.

| file | status | defensible source |
|---|---|---|
| `lateral-view.sql` | scored | [Databricks `LATERAL VIEW`](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-qry-select-lateral-view) specifies `OUTER`, `explode`, and generator aliases; its empty-array example establishes the `NULL` row. The equivalent generator behavior is also exercised by vendored `spark-tests/results/table-valued-functions.sql.out`. |
| `pivot-unpivot.sql` | scored | [Databricks `PIVOT`](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-qry-select-pivot) documents aggregate result columns and aliases; [Databricks `UNPIVOT`](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-qry-select-unpivot) documents name-column strings and `INCLUDE NULLS`. Equivalent exact Spark goldens are vendored in `spark-tests/results/pivot.sql.out` and `spark-tests/results/unpivot.sql.out`. |
| `qualify.sql` | scored | [Databricks `QUALIFY`](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-qry-select-qualify) documents filtering window-function results and gives equivalent `RANK()` examples. Explicit ordering makes the selected rows deterministic. |
| `match-recognize.sql` | scored | [Databricks `MATCH_RECOGNIZE`](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-qry-select-match-recognize) consecutive-rising-run example is copied into this file with its documented two output rows. It is beta and applies to Databricks Runtime 19.0+. |
| `functions-datetime.sql` | scored | The [date and timestamp functions](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-functions-builtin) section documents each accessor and its origin. The two day-of-week conventions (`dayofweek` counting from Sunday=1, `weekday` from Monday=0) are pinned exactly by the vendored `spark-tests/results/datetime-legacy.sql.out`, and the `convert_timezone('Europe/Moscow', 'America/Los_Angeles', …)` row is copied from `spark-tests/results/timestamp-ntz.sql.out`. Every other value is a calendar fact (leap years, ISO week numbering, IANA offsets). `current_timezone()` is oxidant's session zone, UTC. |
| `functions-hash.sql` | scored | SHA-1, SHA-2 and CRC-32 are public standards; every expected digest is the reference value for `abc`, computed independently (`printf 'abc' \| shasum -a N`, `python3 -c "import zlib; print(zlib.crc32(b'abc'))"`), never read back out of oxidant. The `sha2(expr, 100)` → `NULL` row is the manual's documented rule for an unsupported bit width. |
| `functions-conditional.sql` | scored | [`equal_null`](https://docs.databricks.com/aws/en/sql/language-manual/functions/equal_null), [`isnull`](https://docs.databricks.com/aws/en/sql/language-manual/functions/isnull), [`like`](https://docs.databricks.com/aws/en/sql/language-manual/functions/like) and [`iff`](https://docs.databricks.com/aws/en/sql/language-manual/functions/iff) document these as the function spellings of `IS NULL`, `<=>`, `LIKE` and `IF`, whose truth tables the manual fixes. **One row is a recorded gap, not a pass:** `nullifzero`/`zeroifnull` must return the argument's own type (`int`), and oxidant widens to `bigint` — it scores `schema-only`, keeping the divergence visible instead of hiding it. |
| `functions-math.sql` | scored | The manual fixes `rint` as half-to-even, `pmod` as the non-negative modulo, and `width_bucket`'s out-of-range conventions (`0` below the range, `numBuckets + 1` at or above it). The remaining values are IEEE-754 identities (`hypot(3,4) = 5`, `expm1(0) = 0`) and exact two's-complement bit reversals. |
| `create-table-using.sql` | skipped (`requires-catalog-and-delta-storage`) | The [CREATE TABLE manual](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-create-table-using) supports the syntax, but success and data-source errors require a configured catalog and Delta storage. The prior authored success/error outputs therefore were not defensible. |
| `cluster-distribute-sort-by.sql` | skipped (`output-order-is-not-guaranteed`) | The [CLUSTER BY manual](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-qry-select-clusterby) explicitly says it does not guarantee a total order. The authored global row sequences for `CLUSTER BY`/`DISTRIBUTE BY`/`SORT BY` cannot be scored as fixed goldens. |
| `copy-into.sql` | skipped (`requires-delta-storage`) | `COPY INTO` needs a Delta target and cloud storage. |
| `merge-into.sql` | skipped (`requires-delta-storage`) | `MERGE INTO` needs a Delta target table. |
| `delta-lake-sql.sql` | skipped (`requires-delta-storage`) | Time travel and maintenance commands need Delta metadata and storage. |
| `lake-formation-grants.sql` | skipped (`requires-lake-formation-catalog`) | GRANT/REVOKE behavior needs Lake Formation-aware catalog authorization. |

The previous `MATCH_RECOGNIZE` golden was outright wrong: current Databricks documentation
supports the clause (beta in Databricks Runtime 19.0+), so it now records the documented
successful example instead of a guessed parse rejection.

## Running

```bash
# Score the corpus (writes parity/databricks/{parity.json,report.md,parity.html,scoreboard.json})
cargo run -p oxidant-spark-compat --bin oxidant-parity -- golden --corpus databricks

# CI gate against the committed baseline
./target/debug/oxidant-parity ratchet --corpus databricks   # defaults to parity/baseline-databricks.json

# Debug one file
./target/debug/oxidant-parity file --corpus databricks qualify.sql.out
```

Re-baseline after intentional improvements: run `golden --corpus databricks` and copy the
headline counts into `parity/baseline-databricks.json` (same shape as `parity/baseline.json`).

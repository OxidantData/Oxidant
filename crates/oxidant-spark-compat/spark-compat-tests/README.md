# spark-compat-tests — authored extended Spark SQL corpus

Companion corpus to the vendored Spark `sql-tests` (`../spark-tests/`). Where the Spark corpus
measures parity against Apache Spark's own golden outputs, this corpus measures parity against
Spark SQL language-manual statement categories that the vendored corpus does not exercise. The
files are *authored*, not vendored: each `inputs/*.sql` exercises one manual category and each
`results/*.sql.out` records the expected result in Spark's `SQLQueryTestSuite` golden format
(`-- !query` / `-- !query schema` / `-- !query output`), so the existing runner/classifier/reporter
pipeline scores them unchanged.

## Layout

- `inputs/` — one `.sql` file per manual category. Setup (`CREATE … TEMPORARY VIEW …`) runs
  before golden replay exactly like the Spark corpus.
- `results/` — authored goldens. Statements that are valid Spark SQL carry the expected
  `struct<…>` schema + tab-separated rows.
- Skipped files start with a `--SKIP <reason>` directive; the harness records the skip and the
  reason in the report (never silently dropped). The audit table below records every skip,
  including catalog/storage dependencies and non-deterministic output ordering.

## Golden audit and sources

These goldens are deliberately not treated as a substitute for a live warehouse run. Every
scored file below has an auditable source for both its supported syntax and expected result.
The scored row-producing statements include an `ORDER BY` where the source clause does not
promise a total output order.

| file | status | defensible source |
|---|---|---|
| `lateral-view.sql` | scored | Spark's `LATERAL VIEW` syntax specifies `OUTER`, `explode`, and generator aliases; its empty-array example establishes the `NULL` row. The equivalent generator behavior is also exercised by vendored `spark-tests/results/table-valued-functions.sql.out`. |
| `pivot-unpivot.sql` | scored | Spark's `PIVOT` syntax documents aggregate result columns and aliases; `UNPIVOT` documents name-column strings and `INCLUDE NULLS`. Equivalent exact Spark goldens are vendored in `spark-tests/results/pivot.sql.out` and `spark-tests/results/unpivot.sql.out`. |
| `qualify.sql` | scored | Spark's `QUALIFY` syntax documents filtering window-function results and gives equivalent `RANK()` examples. Explicit ordering makes the selected rows deterministic. |
| `match-recognize.sql` | scored | A reference `MATCH_RECOGNIZE` consecutive-rising-run example is copied into this file with its documented two output rows. `MATCH_RECOGNIZE` is a beta extension in the reference implementations that document it. |
| `functions-datetime.sql` | scored | The date and timestamp functions section documents each accessor and its origin. The two day-of-week conventions (`dayofweek` counting from Sunday=1, `weekday` from Monday=0) are pinned exactly by the vendored `spark-tests/results/datetime-legacy.sql.out`, and the `convert_timezone('Europe/Moscow', 'America/Los_Angeles', …)` row is copied from `spark-tests/results/timestamp-ntz.sql.out`. Every other value is a calendar fact (leap years, ISO week numbering, IANA offsets). `current_timezone()` is oxidant's session zone, UTC. |
| `functions-hash.sql` | scored | SHA-1, SHA-2 and CRC-32 are public standards; every expected digest is the reference value for `abc`, computed independently (`printf 'abc' \| shasum -a N`, `python3 -c "import zlib; print(zlib.crc32(b'abc'))"`), never read back out of oxidant. The `sha2(expr, 100)` → `NULL` row is the manual's documented rule for an unsupported bit width. |
| `functions-conditional.sql` | scored | `equal_null`, `isnull`, `like` and `iff` document these as the function spellings of `IS NULL`, `<=>`, `LIKE` and `IF`, whose truth tables the manual fixes. **One row is a recorded gap, not a pass:** `nullifzero`/`zeroifnull` must return the argument's own type (`int`), and oxidant widens to `bigint` — it scores `schema-only`, keeping the divergence visible instead of hiding it. |
| `functions-math.sql` | scored | The manual fixes `rint` as half-to-even, `pmod` as the non-negative modulo, and `width_bucket`'s out-of-range conventions (`0` below the range, `numBuckets + 1` at or above it). `negative('-1.11')` → `1.11` is pinned by the vendored `spark-tests/results/operators.sql.out`. The remaining values are IEEE-754 identities (`hypot(3,4) = 5`, `expm1(0) = 0`) and exact two's-complement bit reversals. **`ceiling` is deliberately not in this file:** the reference `ceil`/`ceiling` returns BIGINT for a non-DECIMAL argument, while the `("ceiling", "ceil")` alias inherits DataFusion's DOUBLE return type. Scoring that as a pass would have certified a known divergence as correct — it belongs in `docs/spark-coverage.md` as a gap, not in the gate. |
| `create-table-using.sql` | skipped (`requires-catalog-and-delta-storage`) | The `CREATE TABLE … USING` syntax is supported, but success and data-source errors require a configured catalog and Delta storage. The prior authored success/error outputs therefore were not defensible. |
| `cluster-distribute-sort-by.sql` | skipped (`output-order-is-not-guaranteed`) | `CLUSTER BY` explicitly does not guarantee a total order. The authored global row sequences for `CLUSTER BY`/`DISTRIBUTE BY`/`SORT BY` cannot be scored as fixed goldens. |
| `copy-into.sql` | skipped (`requires-delta-storage`) | `COPY INTO` needs a Delta target and cloud storage. |
| `merge-into.sql` | skipped (`requires-delta-storage`) | `MERGE INTO` needs a Delta target table. |
| `delta-lake-sql.sql` | skipped (`requires-delta-storage`) | Time travel and maintenance commands need Delta metadata and storage. |
| `lake-formation-grants.sql` | skipped (`requires-lake-formation-catalog`) | GRANT/REVOKE behavior needs Lake Formation-aware catalog authorization. |

The previous `MATCH_RECOGNIZE` golden was outright wrong: the clause is supported (as a beta
extension) by reference implementations, so it now records the documented successful example
instead of a guessed parse rejection.

## Running

```bash
# Score the corpus (writes parity/spark-compat/{parity.json,report.md,parity.html,scoreboard.json})
cargo run -p oxidant-spark-compat --bin oxidant-parity -- golden --corpus spark-compat

# CI gate against the committed baseline
./target/debug/oxidant-parity ratchet --corpus spark-compat   # defaults to parity/baseline-spark.json

# Debug one file
./target/debug/oxidant-parity file --corpus spark-compat qualify.sql.out
```

Re-baseline after intentional improvements: run `golden --corpus spark-compat` and copy the
headline counts into `parity/baseline-spark.json` (same shape as `parity/baseline.json`).

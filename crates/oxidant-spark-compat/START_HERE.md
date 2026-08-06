# START HERE — Spark parity harness

> Read this first: what the parity harness measures, how to run it, the faithfulness
> rules every parity change must follow, and the ranked backlog. For depth, follow the
> **Doc map** at the bottom.

---

## 1. What this is

Oxidant is a drop-in Apache Spark replacement on DataFusion 54. We measure Spark compatibility by replaying
**Apache Spark v4.0.0's own golden SQL corpus** (`crates/oxidant-spark-compat/spark-tests/{inputs,results}`,
304 files / **12,641 queries**, vendored verbatim from `sql/core/src/test/resources/sql-tests`) through
`oxidant_loom::Engine`, formatting output Spark-style, and diffing against Spark's committed `.sql.out`. A CI
ratchet (`parity/baseline.json`) means parity can only rise. This *is* the faithful way to "run Spark's
actual unit tests" — it's Spark's `SQLQueryTestSuite` corpus. (The Scala/JVM-internal suites test
Catalyst/codegen/RDD internals Oxidant doesn't have — they'd validate DataFusion, not parity.)

Two numbers: **strict** = byte-for-byte identical to the golden (schema line + rows); **semantic** =
right answer / right rejection, crediting benign column-name / error-text / tie-order divergence.

## 2. Where the numbers live

`parity/baseline.json` holds the committed strict/semantic floor, and the `parity` job in
`.github/workflows/ci.yml` fails any PR that drops below it — parity can only rise. Run the harness
(§3) for the current measurement; it writes `parity/{parity.json,report.md,parity.html,scoreboard.json}`.

## 3. How to run (≈10s incremental build; golden ≈30–40s)

```bash
cargo build -p oxidant-spark-compat --bin oxidant-parity
./target/debug/oxidant-parity golden     # measure; writes parity/{parity.json,report.md,parity.html,scoreboard.json}
./target/debug/oxidant-parity file group-by.sql.out   # uncapped per-block verdicts for ONE file
./target/debug/oxidant-parity ratchet --baseline parity/baseline.json   # CI gate (strict & semantic must not drop)
cargo test -p oxidant-loom -p oxidant-spark-compat                      # unit tests
```

`golden` writes real files to a per-engine temp warehouse (torn down on `Engine` drop), so a single
run can exceed a 2-minute shell timeout — give it room or run it in the background.

## 4. The non-negotiables (what keeps parity work honest)

1. **FAITHFULNESS.** Anything in `Engine::sql` is on the production path for real users. ✅ alias a Spark
   fn to an identical DF builtin; lower Spark syntax to an *equivalent* DF plan; emit Spark-compatible
   names. ❌ lossy rewrites — the canonical sin is stripping `USING parquet` (turns a persistent table
   into an in-memory MemTable). If the only way to pass is lossy, it's **needs-feature** → report, don't
   ship. A faithful 70% beats a lossy 95%.
2. **The real regression gate is NOT "no bad bucket rose" — it's "no file lost a strict pass."** Unblocking
   a cascade (CREATE TABLE USING, a function wave) makes previously-unrunnable rows execute and hit
   *pre-existing* downstream gaps, so correctness/exec-error/decimal/etc. **rise — that is honest
   unmasking, not regression.** Verify the real line with the **stash audit** (§7).
3. **The ratchet only gates strict + semantic + blocks_total** (`src/bin/parity.rs`). Both must not drop;
   bad buckets are not gated.
4. **Stay in lane.** Parity changes belong in
   `crates/oxidant-loom/src/{lib.rs,spark_functions/**,spark_names.rs,spark_int_literals.rs,spark_create_table.rs}`,
   `crates/oxidant-loom/Cargo.toml`, `crates/oxidant-spark-compat/**`, `parity/`, `site/public/parity.*`.
   Keep parity PRs out of `schema_adapt.rs`, `catalog_bridge.rs`, `gateway/*`.

## 5. Known gaps / ranked backlog

Highest leverage first:

1. **Decimal-precision pass (blocked — investigated and reverted).** Typing unsuffixed `1.5`
   as `decimal(2,1)` IS faithful and the rewrite is a one-branch add in
   `lib.rs::rewrite_spark_typed_literals` (reuse `decimal_ps`; gate on `num.contains('.') && !has_exp`
   so `2.35E10` stays double, matching Spark). Measured: strict +15 / semantic +17 — BUT it regressed
   2 files (6 byte-correct strict passes: `predicate-functions.sql` −5, `inline-table.sql` −1) and
   raised exec-error. Root cause = **DataFusion 54 coercion/overflow gaps the decimal type exposes**,
   NOT the literal typing: (a) no `Utf8`↔`Decimal128` comparison coercion — `'1.5' > 0.5` fails
   `simplify_expressions` where string-vs-`double` worked (Spark coerces; golden `(1.5 > 0.5):boolean`);
   (b) decimal-multiply overflow errors (Arrow "Arithmetic overflow") where Spark returns NULL with
   `allowPrecisionLoss`; (c) decimal in window-frame bounds → "Invalid window frame". Unblock = add
   those coercions (an analyzer/`ExprPlanner` string→numeric rule + Spark-style decimal overflow→NULL),
   THEN re-apply the one-branch literal typing.
2. **Unmasked correctness + missing-error + null-semantics.** Pre-existing gaps, concentrated in
   `collations.sql`, `postgreSQL/numeric.sql`, `window.sql`, `charvarchar.sql`, `postgreSQL/int4/int8.sql`.
   missing-error = Oxidant too lenient now that tables exist (accepts queries Spark rejects) — needs
   analyzer validations.
3. **Function wave (function-missing):** `listagg` (needs `WITHIN GROUP` plan support), `from_xml`/
   `from_csv`/`to_csv` (extend the `spark_from_json.rs` schema-string parser), `percentile_disc`,
   `grouping_id`, `to_timestamp_ltz`. (uniform/randn = nondeterministic, excluded; udaf/foo*/udtf already
   excluded as test fixtures.)
4. **CREATE TABLE USING follow-ons** (`CREATE_TABLE_USING_DESIGN.md`): CTAS (`USING fmt AS SELECT`,
   needs COPY-then-CREATE-EXTERNAL materialization), `PARTITIONED BY`, `OPTIONS`/`LOCATION`, exotic column
   types (varchar(n)/timestamp_ntz/nested struct). Each currently returns `None` → fails as before.
5. **Structural residual (the honest distance to 100%, per `ROADMAP.md` §0/§4):** exact Spark error-text
   (`error-parity`→strict — low value, brittle, partly anti-faithful), and Spark-internal behaviors Oxidant
   legitimately differs on. Faithful ceiling ≈ 85–95% semantic / 55–75% strict. Don't chase strict at the
   cost of correctness; present the residual as an itemized opt-in list.

## 6. How to add a Spark UDF (the proven pattern)

Each function is additive: a new file `crates/oxidant-loom/src/spark_functions/<name>.rs` with a
`pub fn register(ctx)`, plus one `mod` line and one `register` call in `spark_functions/mod.rs`.
**Templates to copy:** `spark_functions/mod.rs` (`typeof`, minimal scalar), `spark_encoding.rs`
(array/per-row scalar), `try_arithmetic.rs` (numeric, NULL-on-error), `spark_aggregates.rs`
(AggregateUDF).

**DataFusion 54 `ScalarUDFImpl` gotchas (already in the templates):**
- `#[derive(Debug, PartialEq, Eq, Hash)]` on the struct (the trait requires `Eq` + `Hash`).
- Exactly four methods: `name`, `signature`, `return_type`, `invoke_with_args` — **no `as_any`**.
- Materialize args: `args.args[i].clone().into_array(args.number_rows)?` then downcast.
- **MSRV is 1.72**: no `Arc::unwrap_or_clone` (use `(*arc).clone()`), no other >1.72 APIs.
- Tests: DataFusion's parser rejects Spark literal suffixes (`1L`, `1.0D`); use `CAST(...)`.
- When unsure of exact Spark output, read the golden: grep the function in `spark-tests/inputs/`,
  read the matching `spark-tests/results/*.sql.out`, match it byte-for-byte.

## 7. The stash audit (run this to prove faithfulness after any cascade-unblocking change)

```bash
cp parity/parity.json /tmp/after.json                 # your built tree's result
git stash && cargo build -q -p oxidant-spark-compat --bin oxidant-parity && ./target/debug/oxidant-parity golden
cp parity/parity.json /tmp/before.json && git stash pop && cargo build -q -p oxidant-spark-compat --bin oxidant-parity
# then per-file: assert no file's `pass` (strict) count dropped before→after; confirm every bad-bucket
# rise sits on a missing-relation/function-missing drop in the SAME file (= unmasking, not regression).
```

(After `git stash` the new untracked `spark_*.rs` files orphan harmlessly — their `mod` lines are stashed.)

## 8. Doc map

- **`ROADMAP.md`** — per-cluster verdicts, the oxidant-sql dialect-layer architecture, the honest-ceiling §0/§4.
- **`CREATE_TABLE_USING_DESIGN.md`** — CTU spec; non-CTAS subset landed, follow-ons specced.
- **`COLUMN_NAMING_PASS.md`** — output column-naming deep-dive.
- **`README.md`** — harness internals.

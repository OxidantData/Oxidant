# Spark-parity work — handoff for a fresh session

> Read this first, then `ROADMAP.md` (the adversarially-verified per-cluster plan) and the
> committed example UDFs. Everything here is on branch `feat/spark-parity-harness`.

## TL;DR

Oxidant is a drop-in Apache Spark replacement on DataFusion 54. We **measure** Spark compatibility by
replaying Apache Spark v4.0.0's *own* golden SQL tests through oxidant and diffing against Spark's
committed `.sql.out` outputs, with a CI ratchet so parity can only rise.

**Current parity (deterministic): semantic 58.5% (7,397 / 12,641), strict 22.1% (2,793 floor).**
Up from 25.5% / 2.2% at the start.

**Coordinator iteration 2 (2026-06-26)** — landed the two biggest cascade levers (strict 1,322→2,793
(+1,471), semantic 5,767→7,397 (+1,630)):
1. **CREATE TABLE … USING** (`spark_create_table.rs`): lowers `CREATE TABLE [IF NOT EXISTS] name (cols)
   USING {parquet|orc|csv|json}` to a REAL format-backed `CREATE EXTERNAL TABLE … STORED AS <fmt>
   LOCATION '<per-engine warehouse>/name/'` (genuine durable storage, NOT the forbidden MemTable strip;
   round-trip proven by unit tests incl. the CSV NULL-vs-empty trap). INSERT returns `vec![]` so DML
   renders Spark's empty `struct<>`. Falls through to the original parse error for anything it can't
   faithfully lower (CTAS, PARTITIONED BY, OPTIONS, exotic types) — never a regression. **missing-relation
   2,572→900 (−1,672).** Deferred follow-ons: CTAS, PARTITIONED BY, OPTIONS/LOCATION (see
   `CREATE_TABLE_USING_DESIGN.md`).
2. **cast-constructors** (`spark_functions/spark_cast_constructors.rs`): `float()/double()/string()/
   boolean()/binary()/date()/int()/…(x)` → faithful Spark CAST + `positive(x)`. **function-missing −238.**
3. **LIKE ALL/ANY/SOME** (lib.rs): quantified `expr [I]LIKE {ALL|ANY} (p1,…)` → AND/OR chains.
4. **from_json family** (`spark_functions/spark_from_json.rs`): a Spark DDL/DataType schema-string parser +
   `from_json` scalar (partial — highest-value subset).
5. **Correctness:** `spark_divide.rs` (literal-zero integral `/` carries DOUBLE type + ANSI error) and a
   Spark `unescapeSQLString` pass over single-quoted literals (ilike) — correctness fixes, refutation-checked.

> **Cascade unmasking (read this before reacting to the bucket table).** Unblocking ~1,900 rows that
> *could not run* before (missing-relation/function-missing) exposed pre-existing downstream gaps, so
> several "bad" buckets ROSE (correctness 169→277, exec-error 957→1,093, decimal 143→189, missing-error
> 126→166, null-semantics 47→71, datetime 6→13, engine-panic 1→3). A per-file audit confirmed **NO file
> lost a strict (byte-correct) pass** — these are honest now-visible backlog, not regressions. Next
> iterations target exactly this surfaced backlog (decimal-precision, the typeCoercion correctness rows,
> aggregate output names — the column-naming-wave-2 agent died on an API stall and is re-queued).

**Coordinator iteration (2026-06-26)** — a multi-swarm pass landed six faithful levers (strict
988→1,322 (+334), semantic 5,599→5,767 (+168), correctness 244→169 (−75), function-missing 1,133→959):
1. **int-vs-bigint literal-default-type** (`spark_int_literals.rs`, the +278-strict lever): integer
   literals now plan as `Int32` when in 32-bit range (Spark's `IntegerType`), `Int64` otherwise; the
   retype runs on the **raw pre-analysis plan** so DataFusion's `TypeCoercion` re-derives all
   downstream types exactly as Spark does. A `NamePreserver` keeps `HAVING`/`EXCEPT`/`INTERSECT`/
   lateral by-name references resolvable; any node that can't re-validate aborts the whole rewrite to
   the original plan. `schema-only` 2,138→1,966; zero bad-bucket movement.
2. **`if(c,a,b)`** (`spark_functions/spark_if.rs`): a `ScalarUDF` whose `simplify()` rewrites to
   `Expr::Case`, so Spark's short-circuit + `CASE` branch-coercion come for free (a 3-arg UDF could
   not — it would eagerly evaluate both branches). function-missing −67.
3. **Spark `/` true-division** (`SparkDividePlanner` `ExprPlanner` in `lib.rs`): integral `/` lowers
   to `CAST(.. AS DOUBLE) / CAST(.. AS DOUBLE)` (Spark's `Divide` contract; `7/2`=`3.5` not `3`).
   Literal-zero divisor stays on the integer path to preserve Spark ANSI `DIVIDE_BY_ZERO`. (Residual:
   a runtime *column*-valued zero divisor yields `Inf` rather than Spark's error — narrow, untested,
   strictly less corrupting than the integer truncation it replaces; follow-up.)
4. **Spark `round`/`bround`** (`spark_functions/spark_math.rs`): integral-typed, `HALF_UP`/`HALF_EVEN`
   in exact i128, ANSI overflow — overrides DataFusion's float-coercing `round`. correctness −20.
5. **regexp** (`spark_regex_misc.rs` + harness `format.rs`): `regexp_extract_all` Hive-style render +
   engine fixes. correctness −29.
6. **interval** (harness `format.rs`): render Arrow intervals as Spark's `CalendarInterval.toString`.
   correctness −9.
Plus a **denominator-honesty** classifier fix (`classify.rs`/`report.rs`): 87 Scala/JVM/Python
test-fixture functions (`udaf(`/`udtf`/`mydoubleavg(` in `udaf/*.sql`, `udtf/udtf.sql`) move from
`function-missing` to the excluded `requires-udf-registration` bucket (0 pass change — these are
harness scaffolding, not oxidant SQL gaps). **The `cast-constructors` lever (`float()`/`double()`/… →
CAST, ~110 rows) did NOT land — its swarm agent died on the account spend limit; it is the top
queued lever for the next iteration, alongside the designed `CREATE TABLE … USING` front-end (§6.5,
biggest remaining cascade, ~+600–1000 strict).** (The "session-timezone quick win" was tested and
disproven — see §6.1.)

**Column-naming wave 1 (2026-06-25).** New `crates/oxidant-loom/src/spark_names.rs`: wraps the top
result projection in an outer renaming projection emitting Spark's `Expression.sql`/`prettyName`
output names (bare literals, unqualified columns, `make_array`→`array`, parenthesized binary ops,
comma-space arg lists, `X'…'`/`DATE '…'` literals). Wired into `Engine::sql`/`Engine::schema` via
`plan_spark`. The *outer*-projection design (not in-place rename of the inner projection) is the
load-bearing correctness choice — a `Sort`/`Filter`/CTE/window above the projection references its
columns by name, so an in-place rename breaks `ORDER BY 1`/`GROUP BY ALL`/window plans (measured:
+57 exec-errors before the redesign). Commit `74f36a2`.

**Wave 4b (2026-06-25): Spark typed-literal parser.** `normalize_spark_sql` now rewrites Spark's
suffixed numeric literals — `1L`/`2Y`/`3S`/`1.0F`/`1.0D`/`1.0BD` — into `CAST(<n> AS <type>)`
(`lib.rs::rewrite_spark_typed_literals` + `decimal_ps` for BigDecimal precision/scale). DataFusion's
lexer read the suffixed forms as identifiers (`No field named "1l"`); the cast is exactly Spark's
semantics, so it's faithful. The scanner is string-/identifier-/comment-aware (never touches `'…'`,
`"…"`, `` `…` ``, `--`/`/* */`, `col1`, `0x1F`, `1e5`). Net: exec-error 1069→955, semantic +168,
strict +44, zero new wrong answers (verified: no typed-literal query is in `correctness`; the small
`correctness` rise is cascade — setup statements that now parse expose pre-existing downstream gaps).

**Last wave (2026-06-25): function wave 4 + Spark binary rendering.** A 4-agent worktree swarm added
`spark_array.rs` (array_size, sort_array, map_contains_key, try_element_at + `array`→`make_array`
alias), `spark_datetime3.rs` (make_timestamp[_ntz], to_timestamp_ntz, try_to_timestamp, unix_*,
date_from_unix_date, date_add), `spark_misc.rs` (hex/unhex/to_binary/try_to_binary, current_*,
assert_true, 2-arg replace), `spark_aggregates2.rs` (try_sum, try_avg, skewness). Then `format.rs`
learned Spark's binary rendering (`new String(bytes, UTF_8)` ≈ `from_utf8_lossy`, not Arrow hex).
Net: function-missing 1340→1099; semantic +134, strict +15. One real wrong-answer bug found and
fixed during integration: `map_contains_key` needed least-common-type key coercion (Spark widens
int↔double, rejects string↔numeric). **Lessons for the next swarm:** (1) always re-run the full
corpus and watch the `correctness` bucket — a newly-registered fn that returns a *wrong* answer is
worse than a missing one (faithfulness); survey `correctness`/`exec-error` for the new fn names and
fix or drop. (2) Some Spark calls are **parser-blocked**, not UDF-able: `timestampdiff(MONTH,…)`
(bare unit keyword parses as a column ref), `percentile_disc/approx` and `listagg` (`WITHIN GROUP`),
and **typed literal suffixes** `1L`/`1.0D`/`2Y`/`2S`/`98…BD` (parsed as identifiers — this blocks
the try_sum/try_avg overflow goldens and many `array(2Y,…)` rows). A Spark typed-literal parser pass
is now a clean, high-value next target. (3) JSON/CSV/XML (`from_json`/`from_csv`/`from_xml`) still
need a Spark DDL/DataType schema-string parser (Wave F) — deferred, substantial.

Reproduce in ~10s:
```bash
cargo run -p oxidant-spark-compat --bin oxidant-parity -- golden   # writes parity/{parity.json,report.md,parity.html,scoreboard.json}
```

## 1. The mission and the honest framing

"100% Spark parity" is pursued as a **measured pass-rate against Spark's portable corpora**, NOT by
porting Spark's ~30k Scala/JVM-internal suites (those test Catalyst/codegen/RDD internals oxidant
doesn't have — they'd validate DataFusion, not Spark).

Two honest numbers (see `report.rs`):
- **strict** — byte-for-byte identical to Spark's golden (schema line + rows). The hard claim.
- **semantic** — right answer / right rejection, crediting benign divergences: column-name spelling
  (`schema-only`), "both engines reject" (`error-parity`), and same-rows-different-tie-order
  (`ordering`). The fair claim for a drop-in replacement.

**Realistic ceilings (from `ROADMAP.md`): ~85–95% semantic, ~55–75% strict.** 100% strict is not
reachable without trading against faithfulness — error-text matching and the column-naming long tail
are structural. Do not chase strict at the cost of correctness.

### The load-bearing rule: FAITHFULNESS
Anything that runs in `Engine::sql` is on the **production path**, not just tests. A change must not
alter results/semantics for real users. Examples:
- ✅ Faithful: dropping `TEMPORARY` from `CREATE TEMPORARY VIEW` (Spark temp views ≡ DataFusion
  session views); registering a Spark function name as an alias of an identical DataFusion builtin.
- ❌ Forbidden: stripping `USING parquet` from `CREATE TABLE` (silently turns a persistent table
  into an in-memory one). If the only way to pass a query is a lossy rewrite, it is **needs-feature**,
  not a shortcut. The harness measures reality; never inflate the number with a lossy hack.

## 2. What exists

**`crates/oxidant-spark-compat/`** — the harness (read its `README.md`).
- `spark-tests/{inputs,results}` — vendored Spark v4.0.0 golden corpus (304 files, 12,641 queries).
- `golden.rs` parse `.sql.out` → blocks; `format.rs` render oxidant Arrow output Spark-style
  (`hiveResultString`); `normalize.rs` row-sort for unordered queries; `classify.rs` triage
  taxonomy; `runner.rs` replay (one `Engine` per file, panic-isolated per block); `report.rs`
  scoreboard (JSON + markdown + HTML + the ratchet JSON).
- `bin/parity.rs` — `oxidant-parity {golden|ratchet|file}`.
- `tests/golden_sql.rs` — fast wiring tests + the `#[ignore]`d full-corpus ratchet test.

**Engine changes in `crates/oxidant-loom/src/`** (all faithful):
- `lib.rs::normalize_spark_sql` — rewrites `CREATE [OR REPLACE] [GLOBAL] {TEMPORARY|TEMP} VIEW` →
  `CREATE … VIEW` (keyword-only, the body is preserved verbatim).
- `lib.rs::register_spark_function_aliases` — Wave-A aliases (Spark name → identical DataFusion
  builtin: `startswith`→`starts_with`, `len`→`length`, `any`→`bool_or`, …).
- `lib.rs::Engine::new` sets `datafusion.sql_parser.dialect = Databricks` (Spark SQL: `"..."` is a
  STRING literal, backticks quote identifiers).
- `spark_functions/` — additive module of Spark-only UDFs (see §7). Modules: `try_arithmetic`,
  `spark_strings`, `spark_encoding`, `spark_datetime`, `spark_convert`, `spark_regex_misc`,
  `spark_datetime2`, `spark_json`, `spark_aggregates`, plus `typeof` in `mod.rs` (the template).

**CI**: `.gitlab-ci.yml` job `spark-parity` runs `oxidant-parity ratchet` against
`parity/baseline.json`. **Scoreboard**: `site/public/parity.{html,json}`.

## 3. How to run things
```bash
# Measure + write artifacts (parity/):
cargo run -p oxidant-spark-compat --bin oxidant-parity -- golden
# CI gate — fails if below parity/baseline.json:
cargo run -p oxidant-spark-compat --bin oxidant-parity -- ratchet --baseline parity/baseline.json
# Debug one file's per-block verdicts:
cargo run -p oxidant-spark-compat --bin oxidant-parity -- file group-by.sql.out
# Tests (fast) and full-corpus ratchet test:
cargo test -p oxidant-loom -p oxidant-spark-compat
cargo test -p oxidant-spark-compat -- --ignored
```
After any improvement: re-run `golden`, then **re-baseline** by copying the new headline+buckets into
`parity/baseline.json` (strict has a ±1 tie-flake — baseline strict to the *minimum* over 3 runs),
and refresh `site/public/parity.{html,json}` from the run's `parity.html`/`scoreboard.json`.

## 4. Current state (Spark v4.0.0, 12,641 queries)

| bucket | count | meaning / where the work is |
|---|---:|---|
| `pass` | 2,793 | ✓ strict (floor 2,793; was 1,322 pre-iteration-2; ±1 union.sql tie) |
| `error-parity` | 2,406 | ✓ both engines reject (semantic pass) |
| `schema-only` | 2,147 | ✓ right values, divergent column name — aggregate names (column-naming wave 2, agent died) + decimal/typed-null display + the unmasked CREATE/INSERT rows (§6.4) |
| `parser-unsupported` | 1,238 | Spark syntax DataFusion rejects (PIVOT, USE, CTAS/PARTITIONED-BY CREATE TABLE) |
| `exec-error` | 1,093 | misc execution failures (rose +136 — unmasked rows from numeric/float8/timestamp now plan deeper) |
| `missing-relation` | 900 | cascade from a failed setup stmt (was 2,572; CREATE TABLE USING landed — remaining are CTAS/exotic-type CREATEs) |
| `function-missing` | 721 | functions still unimplemented — listagg, from_xml/csv, percentile_disc, grouping_id (§6.3) |
| `feature-unsupported` | 482 | PIVOT, `USE db`, SHOW CREATE TABLE, … |
| `correctness` | 277 | **genuine wrong answers — highest trust** (rose 169→277 via cascade unmasking; pre-existing gaps in collations/numeric/window now visible) |
| `decimal-precision` | 189 | precision/scale/rounding (rose +46 unmasked — next target, the decimalArith/window-literal pass was deferred) |
| `missing-error` | 166 | oxidant too lenient (rose +40 unmasked: tables now exist, oxidant accepts queries Spark rejects) |
| `requires-udf-registration` | 87 | excluded: Scala/JVM/Python test fixtures (`udaf`/`udtf`/`mydoubleavg`) — not oxidant gaps |
| `null-semantics` | 71 | three-valued-logic (rose +24 unmasked) |
| `ordering` | 51 | ✓ counted semantic |
| `datetime` | 13 | tz-naive TIMESTAMP gap (rose +7 unmasked in postgreSQL/timestamp) — §6.1 |
| `nondeterministic` | 4 | rand/uuid/shuffle — excluded from scoring by design |
| `engine-panic` | 3 | DataFusion `panic!` (rose +2 unmasked in view-schema-binding/compensation; panic-isolated per block) — §6.6 |

## 5. Mine the backlog (how to pick the next target)
```bash
# Cluster the most common oxidant errors driving function-missing/exec/parser:
python3 - <<'PY'
import json, collections, re
r=json.load(open("parity/parity.json"))
c=collections.Counter()
for f in r["files"]:
    for x in f.get("failures",[]):
        d=re.sub(r"'[^']*'","'X'",x["detail"]); d=re.sub(r'"[^"]*"','"X"',d); d=re.sub(r'\d+','N',d)
        c[(x["bucket"], d[:90])]+=1
for (b,m),n in c.most_common(30): print(f"{n:4} [{b}] {m}")
PY
```
Note: per-file `failures` are capped at 20; bucket *totals* are exact.

## 6. Ranked next steps

### 6.1 Session timezone — DISPROVEN (2026-06-25): NOT a quick win on DataFusion 54
Hypothesis was: Spark generated the goldens in `America/Los_Angeles`, oxidant renders in UTC, so
setting the session tz would flip a batch of timestamp renders. **Tested and false.** Setting
`opts.execution.time_zone = Some("America/Los_Angeles")` *does* take effect (verified: `now()`'s
type flips to `Timestamp(ns, Some("America/Los_Angeles"))`), but produces **zero parity movement**
because DataFusion 54 produces bare `TIMESTAMP` literals, `CAST(x AS timestamp)`, `to_timestamp`,
`from_unixtime` as `Timestamp(_, None)` — **timezone-naive (NTZ)**. The session tz only governs
`now()`/`current_*` (nondeterministic → excluded from scoring), so the deterministic corpus values
render identically regardless of session tz. `cast(0 as timestamp)` → `1970-01-01 00:00:00` in both
UTC and LA; Spark (LTZ) would give `1969-12-31 16:00:00`.

The *actual* gap is **Spark `TIMESTAMP` ≡ timestamp-with-local-time-zone (LTZ)** vs DataFusion
`TIMESTAMP` ≡ NTZ. Closing it is a real type-semantics feature (coerce literals/casts to
`Timestamp(_, Some(session_tz))` on the production path; affects comparisons/joins/storage), **not** a
config flip — and its yield is small: across `timestamp/date/interval/timestamp-ntz` the failures are
dominated by `function-missing`/`schema-only`/`parser-unsupported`/`exec-error`, with the
tz-sensitive `datetime` bucket only ~3 in `timestamp.sql` and ~0 elsewhere. Skip unless doing a
dedicated LTZ-correctness pass. The real levers in these files are §6.3 (functions: `unix_seconds`,
`unix_millis`, `make_timestamp`, `make_timestamp_ltz`, `date_add`, `convert_timezone`) and §6.4
(column-naming).

### 6.2 `array(...)` / type-constructor function syntax (parser/alias layer)
Spark uses `array(1,2,3)` (DataFusion: `make_array`), and cast-constructors `int(x)`/`double(x)`/
`string(x)` (= `CAST(x AS …)`). These hit `function-missing`/`parser-unsupported` and block
`to_json(array(...))` etc. `array`→`make_array` is an alias (add to `register_spark_function_aliases`).
The cast-constructors need an `ExprPlanner` (recognize `TYPE(expr)` → `CAST`) — bigger; see ROADMAP
"type-and-cast" / function-registration "OUT OF THIS CLUSTER".

### 6.3 More function waves (diminishing but steady; use the swarm — §7)
Remaining backlog (`ROADMAP.md` → function-registration notes, Waves B–F): UDAFs
`listagg`/`percentile_cont`/`percentile_disc`/`histogram_numeric`; `to_number`/`to_char` format
coverage; `mask` variants; `regexp_replace`/`regexp_substr`; `from_csv`/`to_csv`. Per-wave yield is
now ~+30–60 semantic — worthwhile but no longer the dominant lever.

### 6.4 Column-naming pass — wave 1 LANDED (`spark_names.rs`); two follow-ons remain
Wave 1 (commit `74f36a2`) converted **+319** schema-only→strict via `crates/oxidant-loom/src/spark_names.rs`
(scalar literals/columns/functions/binary-ops/casts). The remaining `schema-only` (2,138) splits into:

1. **int-vs-bigint literal-default-type** (~228 + nested-in-array cases): Spark integer literals
   default to `INT`, DataFusion to `Int64`/`BIGINT`, so `k:int`/`array<int>` (golden) shows as
   `k:bigint`/`array<bigint>` (oxidant). This is **not a name issue** — it's a type-spelling divergence
   and the *single biggest* remaining schema-only chunk. Fixing it (coerce integer literal default to
   Int32) is risky: it affects arithmetic/overflow semantics and could move the `correctness` bucket.
   Own investigation, own ratchet. Many aggregate rows are *double-blocked* (name **and** type), so
   this must land for aggregate-naming to pay off.
2. **Aggregate output names** (`count(testdata.a)`→`count(a)`, `count(*)`→`count(1)`, `max(t.c)`→`max(c)`):
   I prototyped a `render_aggregate` path (count(*)→count(1), unqualified args, FILTER) but it moved
   **≈0** and was reverted, because in `SELECT k, count(*) … GROUP BY k` the plan is
   `Projection → Aggregate` — the projection references the aggregate by a bare **`Column`** named
   `count(*)`, while the `AggregateFunction` expr lives in the `Aggregate` node, which the
   outer-projection renamer deliberately never enters. To fix: when a top-projection column is a bare
   `Column` resolving to an `Aggregate` output, look up that aggregate's expr in the child `Aggregate`
   node and render *it* (Spark-style) for the output name. Combine with (1) since most are double-blocked.

Also deferred from wave 1 (low yield, listed in `COLUMN_NAMING_PASS.md`): typed-null `CAST(NULL AS T)`
spelling, explicit-cast retention (`bit_count(CAST(1 AS TINYINT))` — Spark keeps *user* casts in the
name but strips coercion casts; hard to distinguish from the plan), Spark-name reverse-aliases
(`var_samp`→`variance`). **Full data-grounded plan + the implemented rules: `COLUMN_NAMING_PASS.md`.**
See also ROADMAP §1d/§2 (stage-3 "project_spark_names").

### 6.5 `CREATE TABLE … USING <format>` — biggest cascade (needs a real feature)
~120 direct `parser-unsupported` + thousands of downstream `missing-relation`. **Do NOT** shim by
stripping `USING` (lossy — see §1). The faithful fix is a Spark-DDL front end that lowers
`CREATE TABLE … USING fmt [OPTIONS/PARTITIONED BY/AS SELECT]` to a real format-backed table
(`CREATE EXTERNAL TABLE … STORED AS fmt LOCATION <managed-warehouse-path>`; write CTAS results
first). This belongs in the planned `oxidant-sql` dialect layer, not `normalize_spark_sql`. ROADMAP
"create-table-using" has the full spec.

### 6.6 Correctness + robustness (small count, high trust)
`correctness` (209) = wrong answers — triage these directly (`oxidant-parity file <f>`), they matter most
for trust. `engine-panic` (1): DataFusion panics on multi-arg `COUNT(DISTINCT a,b)` — implement a
real multi-arg distinct or special-case it.

### 6.7 The other parity pillars (not yet started — original plan Wave 3)
- **PySpark Connect suite**: run Spark's own `python/pyspark/sql/tests/connect/` unmodified against a
  live oxidant Spark Connect server — the strongest drop-in proof. Harness dir: `bench/spark-compat/`.
- **TPC-H/DS correctness** vs DuckDB oracle (wire `bench/{tpch,clickbench}/run-correctness.sh`).
- **Type/Arrow conformance matrix** (Spark↔Arrow DataType round-trips).

## 7. How to add a Spark UDF (the proven pattern)

Each function is additive: a new file `spark_functions/<name>.rs` with a `pub fn register(ctx)`, plus
one `mod` line and one `register` call in `spark_functions/mod.rs`. **Templates to copy:**
`spark_functions/mod.rs` (`typeof`, minimal scalar), `spark_encoding.rs` (array/per-row scalar),
`try_arithmetic.rs` (numeric, NULL-on-error), `spark_aggregates.rs` (AggregateUDF).

**DataFusion 54 ScalarUDFImpl gotchas (already in the templates):**
- `#[derive(Debug, PartialEq, Eq, Hash)]` on the struct (the trait requires Eq+Hash).
- Exactly four methods: `name`, `signature`, `return_type`, `invoke_with_args` — **no `as_any`**.
- Materialize args: `args.args[i].clone().into_array(args.number_rows)?` then downcast.
- **MSRV is 1.72**: no `Arc::unwrap_or_clone` (use `(*arc).clone()`), no other >1.72 APIs.
- Tests: DataFusion's parser rejects Spark literal suffixes (`1L`,`1.0D`); use `CAST(...)`.
- When unsure of exact Spark output, READ THE GOLDEN: grep the fn in `spark-tests/inputs/`, read the
  matching `spark-tests/results/*.sql.out`, match it byte-for-byte.

**Running an implementation swarm** (optional; needs explicit opt-in / "ultracode"): see the saved
scripts in the session's `workflows/scripts/oxidant-spark-udf-swarm*.js`. Pattern: one agent per batch,
`isolation: 'worktree'` so each compiles against real DataFusion, returns the verified file source as
text; you integrate into the main tree and gate through the ratchet. Worktrees branch from **committed
HEAD**, so commit any new template before launching. Watch for: JSON round-trip can double-escape
quotes in returned source (`\"` → check it compiles; `spark_datetime.rs` needed an unescape once);
agents may add deps (`regex`, `serde_json`) — declare them in `oxidant-loom/Cargo.toml`.

## 8. Guardrails (do not skip)
- **Faithfulness** (§1) over the score. A faithful 70% beats a lossy 95%.
- **Ratchet is the arbiter.** Integrate a change only if the full corpus holds/raises. Re-baseline to
  lock gains (strict to the 3-run minimum — there's a ±1 tie-order flake on `postgreSQL/union.sql`).
- **Determinism.** `rand/uuid/shuffle` → `nondeterministic` bucket (excluded). If you see the score
  flake, find the unstable query and either exclude it or fix the comparison — never ship a flaky gate.
- **Concurrent work on this branch.** Another session is actively committing the platform/gateway
  control plane (catalog `schema_adapt`, OIDC/SCIM — commits `39a55e9`, `2aa9723`, `ed2dc1b`, …). Keep
  parity commits limited to `crates/oxidant-loom/src/{lib.rs,spark_functions/**}`, `crates/oxidant-spark-compat/**`,
  `parity/`, `site/public/parity.*`. Never stage their files (`schema_adapt.rs`, `catalog_bridge.rs`,
  gateway/*). If a build error points at a file you didn't touch, it's likely their in-progress WIP —
  confirm all errors are outside your files (the compiler lists every error) before worrying.

## 9. Pointers
- **`crates/oxidant-spark-compat/PARITY_SWARM_PLAYBOOK.md` — coordinator playbook to drive parity to
  its faithful ceiling in one multi-swarm campaign (dependency-ordered waves, the faithfulness
  contract every agent inherits, the ratchet loop). Start here to run the whole push.**
- **`crates/oxidant-spark-compat/COLUMN_NAMING_PASS.md` — output column-naming deep-dive; wave 1 landed,
  two follow-ons (int/bigint type, aggregate names) remain.**
- `crates/oxidant-spark-compat/ROADMAP.md` — the per-cluster verdicts + dialect-layer architecture.
- `crates/oxidant-spark-compat/README.md` — harness internals + how to run.
- Parity commits: `c9a6dd6`, `1c4694f`, `f927cbe`, `cb81580`, `f0c1947`, `55b4c54`, `8458824`,
  `070429b`, `e8057e3` (UDF wave 4 + binary rendering), `57e7aa5` (typed literals) — interleaved
  with the concurrent platform commits.

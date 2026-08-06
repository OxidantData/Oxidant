# Oxidant → Spark SQL parity — implementation roadmap

_Produced by the `oxidant-sql-dialect-swarm` workflow (10 design agents + adversarial verifiers + synthesis lead). Baseline at generation: strict 536, semantic 4,984 of 12,641._

## Per-cluster verdicts

| cluster | verdict | unlock (est) | effort | faithful-rewrite? |
|---|---|---:|---|---|
| create-table-using | needs-feature | 900 | L | n/a |
| use-database | faithful-rewrite | 33 | S | verifier rejected |
| like-any-all | needs-plan-change | 52 | M | n/a |
| pivot-unpivot | needs-plan-change | 12 | XL | n/a |
| show-and-misc-ddl | needs-plan-change | 120 | XL | n/a |
| function-registration | needs-function-registration | 1100 | XL | n/a |
| column-naming | needs-plan-change | 950 | XL | n/a |
| type-and-cast | needs-feature | 50 | M | n/a |
| leniency-and-datetime | needs-feature | 130 | XL | n/a |

## Synthesized roadmap

I have what I need. The architecture is clear: `normalize_spark_sql` is a string-level prefilter applied at line 204 (`sql`) and 218 (`schema`), wrapping `ctx.sql()`. Now I'll synthesize the roadmap.

# OXIDANT → SPARK SQL PARITY: IMPLEMENTATION ROADMAP

Baseline (measured): strict 4.2% (536), semantic 39.4% (4,984) of 12,641 queries. The `unlock_estimate` numbers below are the spec authors' figures; I flag throughout which are **measured from parity.json buckets** vs **estimated**.

---

## 1. Ranked clusters by leverage (unlock / effort)

Effort scale S<M<L<XL. ROI = unlock ÷ effort-weight (S=1, M=2, L=4, XL=8). "Strict" = exact `.sql.out` match; "semantic" = same rows, schema/error-text may differ.

| Rank | Cluster | Unlock | Effort | ROI | Layer | Verdict | Faithful-now? |
|---|---|---|---|---|---|---|---|
| **TIER (a) — READY NOW: verified-faithful, land in `Engine::sql`** |
| — | *(strip-temporary-view — already shipped)* | — | — | — | (a) | done | yes |
| A1 | **use-database** | 33 | S | 33 | (a)→(b) | faithful-rewrite (`has_code`) | **NO as written** — 4 bugs |
| **TIER (b) — oxidant-sql dialect layer: AST intercepts / rewrites** |
| B1 | **type-and-cast** | 50 | M | 25 | (b)+(c) | needs-feature | partial |
| B2 | **like-any-all** | 52 | M | 26 | (b) | needs-plan-change | no (rejected as string rewrite) |
| B3 | **pivot-unpivot** | 12 | XL | 1.5 | (b)/(e) | needs-plan-change | no |
| **TIER (c) — function registration backlog** |
| C1 | **function-registration** | **1100** | XL | 137 | (c) | needs-function-registration | n/a (additive UDFs) |
| **TIER (d) — column-naming (the strict lever)** |
| D1 | **column-naming** | **950** | XL | 119 | (d) | needs-plan-change | n/a (output-shaping) |
| **TIER (e) — real feature / correctness work** |
| E1 | **show-and-misc-ddl** | 120 | XL | 15 | (e) | needs-plan-change (catalog metadata) | no |
| E2 | **leniency-and-datetime** | 130 | XL | 16 | (e) | needs-feature | no (lossy rewrite forbidden) |
| E3 | **create-table-using** | 900 | L | — | (e) | needs-feature | **FORBIDDEN as rewrite** |

**Read of the table.** By raw ROI the order is C1 (function-reg, 137) > D1 (column-naming, 119) > A1 (use, 33) > B2≈B1 (~25) > E2/E1/B3 ≈ low. But ROI conflates two parity axes:

- **C1 and D1 are the only two billion-dollar levers.** function-missing=1653 is the *semantic* ceiling; schema-only=1985 is the single biggest *strict* lever. Everything else is rounding error against these two. C1 raises semantic; D1 converts already-semantically-correct rows into strict passes.
- **create-table-using (E3, unlock 900) is a trap.** The headline number is large, but the spec's verdict is explicit: the only string rewrite that passes it is the exact lossy `USING parquet`→MemTable rewrite the FAITHFULNESS PRINCIPLE forbids by name. Its 900 is **only redeemable via real managed-table/format-backed storage (tier e)**, not via any rewrite. Do not let its unlock_estimate pull it up the queue.

---

## 1d. Column-naming — explicit feasibility verdict

**Verdict: FEASIBLE and the highest-value strict work, but it is output-shaping, not a SQL rewrite — it belongs in a result-schema reconciliation pass, not in `normalize_spark_sql`.**

schema-only=1985 means: oxidant already computes the **correct rows** for ~1,985 queries but emits a **different column name / nullability / type-display** than Spark's golden. The divergences (per the spec's 497-row sample across all 95 files) are deterministic and rule-shaped: DataFusion names an unaliased expression column `count(...)` / `Int64(1)` where Spark prints `count(1)` / `1`; casing and the `col#NNN` exprId suffix differ; nullability flags differ. These are **mechanical column-header rewrites on the output schema**, with no row recompute.

Why it's still XL and "needs-plan-change": you cannot do it as a blind string map. Correct Spark column naming requires walking the **logical plan's projection list** and reproducing Spark's `Expression.sql`/`prettyName` naming algorithm (the same algorithm that produces `count(1)`, `(a + b)`, `CAST(x AS INT)` headers). That is a real reimplementation of Spark's output-name derivation, fed by the analyzed plan — hence a plan-level pass in oxidant-sql, not a text hack. But it is bounded, deterministic, and unlocks the largest strict bucket. **Build it; just budget it as a plan-output naming pass, and expect it to land incrementally (each naming rule flips a sub-bucket of the 1985).**

---

## 2. oxidant-sql dialect-layer architecture

Today's surface: `Engine::sql` (lib.rs:203) and `Engine::schema` (lib.rs:217) both call `normalize_spark_sql(query)` (a `&str → Cow<str>` prefilter) immediately before `ctx.sql()`. That single chokepoint is where the dialect layer plugs in. Replace the lone `strip_temporary_view` call with a staged pipeline.

```
Engine::sql(query)
   │
   ▼
oxidant_sql::dialect::lower(query, &ctx)   ──►  LowerOutcome
   │                                          ├─ Sql(Cow<str>)        → ctx.sql()
   │                                          ├─ Rewritten(LogicalPlan) → ctx.execute_logical_plan()
   │                                          └─ Direct(Vec<RecordBatch>) → return (e.g. USE, SHOW)
   ▼
ctx.sql() → df → (NEW) project_spark_names(df.schema) → collect
```

Three additive stages, each a registry of independent rules. A rule's contract: **either it fires and fully owns the statement, or it returns `None` and the next rule sees the original input unchanged.** No rule mutates shared state; ordering is by specificity, and a conflict (two rules claim the same statement) is a hard error in debug, first-wins in release — so additions stay conflict-free by construction.

**Stage 1 — String prefilter registry (`Vec<dyn StrRule>`), pre-parse.**
`fn try_rewrite(&self, &str) -> Option<Cow<str>>`. This is exactly today's `strip_temporary_view` generalized. Home for **verified-faithful, purely-lexical, leading-keyword** rewrites only (strip-temporary-view; type-alias normalization `AS long`→`AS BIGINT` once verified faithful). Gated on a leading-token match so it can never fire inside literals/joins. **A1 (use-database) does NOT belong here** — its 4 bugs (semicolon glued to token, double-quote-vs-backtick dialect sensitivity, comment-defeat, keyword-named namespaces) are precisely the failures a whitespace tokenizer produces. Move USE to Stage 2.

**Stage 2 — Statement-intercept registry (AST-level), post-parse.**
Parse once with a Spark-configured `sqlparser` dialect; dispatch on the `Statement` variant. `fn intercept(&self, &Statement, &ctx) -> Option<LowerOutcome>`. Home for: `USE` (resolve namespace correctly, handle `;`/comments/quoting via the real parser, emit the verified `SET datafusion.catalog.default_schema='…'` — and crucially *reject* what Spark rejects), `PIVOT`/`UNPIVOT` (needs child-schema resolution → must run after binding; emits a rewritten `LogicalPlan`), `LIKE ANY/ALL` (expression-tree rewrite to OR/AND chains — faithful at AST level where the byte-span rewrite was not), `SHOW`/`DESCRIBE` (emit `Direct(batches)` from catalog metadata). This stage is where the spec's three "needs-plan-change, rejected-as-string-rewrite" clusters (like-any-all, pivot, show-ddl) actually become tractable.

**Stage 3 — Output naming pass (`project_spark_names`), post-plan.**
The column-naming lever. Walks the analyzed projection list, renames output columns per Spark's `Expression.sql` algorithm. Pure schema/header transform, zero row recompute. Registry of per-expression naming rules so each Spark naming idiom (`count(1)`, parenthesized arithmetic, cast display) is an independent, separately-testable rule that flips its own sub-bucket of the 1985.

**Cross-cutting — function registry (C1)** is orthogonal to all three stages: it's a one-time `register_udf` loop at `SessionContext` construction (lib.rs ~196), populated from the mined 1653-name list, deferred to DF54's registry where it already has an impl.

This structure makes every future gap a new rule in one of three `Vec<Rule>`s, with a uniform "fire-and-own or pass-through" contract — additive and conflict-free.

---

## 3. Recommended execution order for the next swarm (cumulative parity)

Distinguishing **measured** (from parity.json buckets) vs **estimated** (spec author's projection; real yield is usually lower because buckets overlap and a query can fail for several reasons at once).

| Step | Work | Layer | Δ semantic | Δ strict | Cumulative (realistic) | Basis |
|---|---|---|---|---|---|---|
| 0 | *baseline* | — | — | — | sem 4,984 / strict 536 | measured |
| 1 | **C1 function registration** (defer-to-DF54 backlog; biggest single lever, fully additive, low-risk) | (c) | **+1,653 ceiling → realistically +900–1,200** | small | **sem ~6,000–6,200** | bucket measured 1653; yield estimated |
| 2 | **D1 column-naming output pass** (the strict lever; only meaningful once rows are correct, so it must follow C1) | (d) | 0 | **+1,985 ceiling → realistically +1,000–1,500 incrementally** | **strict ~1,800–2,200** | bucket measured 1985; yield estimated, lands rule-by-rule |
| 3 | **Stage-1/2 quick faithful rewrites**: build the registry shell, migrate strip-temporary-view, add type-alias normalization (B1, the faithful subset ~30–40), land **USE via AST intercept** (A1 done correctly) | (a)/(b) | +~80 | +~40 | sem ~6,100 / strict ~2,250 | est; A1 measured 33 |
| 4 | **B2 like-any-all** (AST expression rewrite) + **B1 remaining cast/type features** | (b) | +~100 | +~60 | sem ~6,200 / strict ~2,300 | est |
| 5 | **E1 show-and-misc-ddl** (catalog-metadata feature — the real gate behind 506 queries) + **B3 pivot** | (b)/(e) | +~130 | +~110 | sem ~6,330 / strict ~2,400 | est; cluster measured 506, unlock 120 |
| 6 | **E2 leniency-and-datetime** (overflow kernels, Spark datetime formatter, analyzer validations — real semantic enforcement) | (e) | +~130 | +~120 | sem ~6,460 / strict ~2,520 | est |
| 7 | **E3 create-table-using** — ONLY via real managed/format-backed table storage, never a rewrite | (e) | +~900 ceiling → realistically +400–600 | +~300 | sem ~7,000 / strict ~2,900 | unlock measured-ish, yield estimated; large feature |

**Ordering rationale:** C1 before D1 is load-bearing — column-naming can only convert a row-correct result into a strict pass, so it has nothing to act on until function-registration has made more results row-correct. Both are XL but **C1 is the lowest-risk XL** (additive UDF registration, can't regress existing passes), so it leads. Steps 3–4 build the dialect-layer scaffolding the spec calls for while banking the easy faithful wins. Steps 5–7 are genuine feature work with diminishing ROI; E3 is last despite its big headline because it's the most expensive *and* the most dangerous (faithfulness trap).

---

## 4. Honesty note — how reachable is 100% strict parity?

**100% strict is not realistically reachable, and the gap is structural, not a backlog.** Two divergence classes cap it:

1. **Column-naming / schema display (D1).** Even after the Stage-3 naming pass, exact reproduction of Spark's output-name algorithm has long-tail cases (nested struct field naming, exprId `#NNN` suffixes, `lateral`/generator naming, version-specific `prettyName` quirks). We can take the 1,985 bucket from ~0 to a large fraction, but the last few percent require bug-for-bug mimicry of an evolving Spark internal. Expect to asymptote, not close.

2. **Error-text divergence.** Many goldens are *expected failures* whose `.sql.out` contains Spark's exact exception string (`PARSE_SYNTAX_ERROR`, `SCHEMA_NOT_FOUND`, `[CAST_INVALID_INPUT] …` with Spark's SQLSTATE and message). Oxidant surfaces DataFusion's error text (e.g. `"Unsupported SQL statement: USE"`). Strict-matching these requires a **full Spark error-message-catalog translation layer** mapping every DF error to Spark's exact string — high effort, brittle, and pure strict-only value (semantic parity already counts these as matches when the *outcome* is "error"). The A1/B-class verdicts already flag this: turning an expected parse error into a passing statement (use-database BUG 2) is a *faithfulness regression*, so chasing error-text strict matches can actively conflict with correctness.

**Realistic ceilings:**
- **Semantic parity → 85–95% is reachable** and is the right north star. function-registration + dialect intercepts + real features (show/pivot/datetime/create-table) close most of the 60% gap on outcome-correctness.
- **Strict parity → realistically 55–75%.** The column-naming pass is the dominant mover; beyond it, the residual is error-text and naming long-tail that costs more than it's worth and sometimes opposes faithfulness.

**Recommendation:** treat **semantic parity as the product goal and column-naming-driven strict as the headline metric**, and explicitly *cap* investment in error-text strict matching once it starts trading against the faithfulness principle. The faithfulness constraint (no lossy rewrites — create-table-using and the leniency cluster are the live examples) is itself a permanent ceiling on strict-via-shortcut, and that is the correct trade: a faithful 70% strict engine is worth more than a 95% strict engine that silently returns wrong results for real users.

**Relevant file:** `crates/oxidant-loom/src/lib.rs` — `normalize_spark_sql` (L70), `next_token` (L79), `strip_temporary_view` (L93); plug-in points `Engine::sql` (L203-213) and `Engine::schema` (L217-225); `SessionContext` construction for the function-registry loop (~L196).

## Function-registration backlog (mined)

EVIDENCE: parity.json (whole-corpus run) buckets: function-missing=1653 of 12641 (the cluster ceiling). I mined every "Invalid function 'X'" detail (regex on the JSON `detail` field; per-file `failures` is capped at FAILURE_CAP=20 in report.rs:77, but the per-file `function-missing` BUCKET tally is uncapped). I cross-checked the rejected set against the LIVE DF54 registry by adding a throwaway crates/oxidant-loom/examples/list_funcs.rs that printed ctx.state().scalar/aggregate/window_functions().keys() (303 names) — then deleted it (no tracked files modified). True per-function demand measured by grepping inputs/*.sql for `name(` occurrences (scratchpad/usage_rank.txt).

PRIORITIZED BACKLOG (Spark fn | usage-count | classification). Wave A — PURE ALIASES (trivial, alias-table only, strict-safe): startswith/endswith->starts_with/ends_with, variance->var_samp, len->length, approx_count_distinct->approx_distinct, any/some/every->bool_or/bool_and, sign->signum, power->pow, ucase/lcase/char. Wave B — HIGH-VALUE SCALAR UDFs: split(regex,128), mask(67), to_char/to_varchar/to_number(103/36/32), format_string(45), typeof(31), elt(20), bit_count(20), luhn_check(19), size/array_size(26/5), sort_array(15), map_contains_key(8), parse_url(8), url_encode/decode(11), soundex/sentences/str_to_map, conv/bin/bround/getbit. Wave C — try_* family (one shared overflow/parse-safe wrapper pattern): try_add/subtract/multiply/divide/sum/avg(~90 combined), try_to_binary(35), try_to_timestamp, try_element_at, try_reflect, try_url_decode, nullifzero/zeroifnull. Wave D — DATETIME UDFs: timestamp_seconds(35), to_timestamp_ntz/ltz(25), make_timestamp(14), timestampdiff/add(16), next_day(10), unix_*/timestamp_* converters, convert_timezone. Wave E — UDAFs: mode(36), listagg(37, needs WITHIN GROUP plan support), percentile/percentile_disc/percentile_cont(106/62 — exact), histogram_numeric(20), any_value(36), count_if, hll_sketch_agg/estimate/union(needs Datasketches HLL binary format). Wave F — SUBSTANTIAL (own sub-projects): JSON family from_json(68)/to_json(20)/json_tuple(19)/schema_of_json/get_json_object/json_array_length — from_json needs a Spark-DDL/DataType schema-string parser; XML/CSV from_xml(18)/to_csv/from_csv(22)/to_xml; regexp_extract(_all)(16+16). 

OUT OF THIS CLUSTER (do NOT register; route to other clusters): (a) cast-as-function type constructors double()/boolean()/string()/binary()/array()/float()/decimal() — ~250-300 queries, verdict needs-feature (parser/ExprPlanner recognizing TYPE(expr)=CAST(expr AS TYPE)); concentrated in postgreSQL/* and typeCoercion/native/* files. (b) if() and the HOF lambdas (transform/filter/aggregate/reduce/exists/forall/zip_with/map_filter/map_zip_with/transform_keys/transform_values, ~120 queries) — needs-feature: DF has no `if` builtin and no lambda-argument support; requires an ExprPlanner/logical rewrite, not register_udf. (c) IDENTIFIER()/TABLE() clauses and udaf/foo1d1 test-defined UDFs (udf*/sql-udf/udtf files, ~150 queries) — the harness should SKIP these (requires-udf-registration), they are not oxidant gaps.

UNLOCK ESTIMATE basis: of the 1653 bucket, the captured (capped) sample is ~74% genuine functions, ~16% cast-constructors, ~10% test-UDFs; extrapolated genuine-registration share ~1,100 queries direct. Cascade is positive but bounded — many function-missing queries co-occur with schema-only/error-parity gaps, and several function-heavy files build views with functions so fixing the setup unlocks downstream rows (json-functions, array, mask, bitwise, string-functions, hll are near-fully gated on this cluster). Realistic strict-parity gain is lower than 1,100 because many will land in schema-only (benign column-name divergence in the generated `struct<fn(args):type>` header) — but those count as semantic passes, which is the bigger headline lever (semantic 39.4% today).

REGISTRATION MECHANISM DESIGN: add a `oxidant-functions` registration entrypoint `fn register_spark_functions(ctx:&SessionContext)` called once in Engine::new() after SessionContext construction. Two tables: (1) ALIASES: `&[(&str /*spark*/, &str /*df target*/)]` — for each, fetch the target ScalarUDF/AggregateUDF from ctx.state() and re-register a delegating wrapper carrying the Spark name (or, where DF exposes with_aliases on the impl, register the impl with the extra alias). (2) IMPLS: each real UDF is a `struct XxxUdf; impl ScalarUDFImpl` (return_type + invoke_with_args over Arrow arrays) registered via ctx.register_udf(ScalarUDF::from(XxxUdf::new())). Keep it OFF the oxidant-loom faithfulness-rewrite path — this is additive registration, semantically faithful by construction (each UDF must match Spark's documented contract incl. null/overflow edge cases, which is why try_* and size/sort_array can't be naive aliases). FILE REFS: crates/oxidant-loom/src/lib.rs:152 (Engine::new — insertion point), crates/oxidant-spark-compat/src/report.rs:77 (FAILURE_CAP caveat), inputs at crates/oxidant-spark-compat/spark-tests/inputs/{mask-functions,json-functions,higher-order-functions,array,bitwise,hll,string-functions,math}.sql with goldens in ../results/*.sql.out.
# Spark output column-naming (the strict-parity lever)

> Read `HANDOFF.md` first for the overall mission, how to run the harness, and the faithfulness rule;
> this doc is the deep dive for the biggest **strict** lever. Everything is on branch
> `feat/spark-parity-harness`.

## DONE so far — wave 1 (commit `74f36a2`, 2026-06-25): strict 669→988 (+319), semantic held

Implemented in **`crates/oxidant-loom/src/spark_names.rs`**, wired into `Engine::sql`/`Engine::schema`
through the new `Engine::plan_spark` helper. The design diverged from the original sketch below in
one load-bearing way — read this before continuing:

- **Outer projection, NOT in-place rename.** The original plan (§"Where to hook it") was to descend
  to the top `Projection` and re-alias its expressions in place. **That is unsafe and was measured to
  cost +57 exec-errors:** a `Sort`/`Filter`/CTE/window *above* the projection references its output
  columns *by name*, so renaming them in place breaks `ORDER BY 1`, `GROUP BY ALL`, window plans, etc.
  The shipped approach instead reads the top projection's exprs (to know the Spark names) but wraps
  the **whole** plan in one new *outer* `Projection` (`SELECT col0 AS spark0, … FROM (orig plan)`).
  The inner plan stays byte-identical, so nothing internal can break. Duplicate output names
  (`SELECT 1, 1`) → bail to the original plan (DataFusion projections forbid dup names; Spark allows).
- **Rules shipped:** literal type-wrappers stripped (Rule 1), table qualifiers stripped (Rule 2),
  `make_array`→`array` (Rule 3), binary ops parenthesized (Rule 4), comma-space arg lists, `Cast`
  unwrap, `Negative`/`Not`/`IS [NOT] NULL`, full-length `X'…'` binary + `DATE '…'` (Rule 6). Anything
  not modelled falls back to DataFusion's own name (safe — stays `schema-only`, never regresses).
- **Verified faithful & regression-free:** semantic held at 5,599 exactly, no `correctness`/
  `exec-error`/etc. bucket rose. Ratchet re-based to the 3-run strict floor (987; 988/987/987).

### What's left (two follow-ons) — and why wave 1 stopped here
The remaining `schema-only` (2,138) is **no longer dominated by name divergences**:

- **int-vs-bigint type spelling (~228 + array-nested)** — the biggest remaining chunk, and **not a
  name problem** (`k:int` vs `k:bigint`). Out of scope for a *naming* pass; it's a literal-default-type
  change (Spark `INT` vs DataFusion `Int64`) that touches arithmetic/overflow → its own investigation.
- **Aggregate output names** (`count(*)`→`count(1)`, `count(t.a)`→`count(a)`, `max(t.c)`→`max(c)`) —
  a `render_aggregate` path was prototyped and **reverted (moved ≈0)**: in `Projection → Aggregate`
  the projection references the aggregate as a bare `Column` named `count(*)`, and the
  `AggregateFunction` expr lives in the child `Aggregate` node, which the outer-projection renamer
  never enters. The fix is to special-case a top-projection `Column` that resolves to an `Aggregate`
  output: find that aggregate's expr in the child node and render *it*. **Most aggregate rows are also
  int/bigint-double-blocked**, so do this *together with* the int/bigint fix or it won't move the
  number. The reverted prototype (count-star→`count(1)`, unqualified agg args, FILTER rendering,
  `Negative(literal)`→`-1`) is in this session's history if useful.

Everything below is the original plan; the rules are now mostly shipped (§ DONE), kept for reference.

---

## TL;DR

`schema-only` is **2,456** queries (the largest bucket after the two cascade pools): oxidant computes the
**right rows** but labels the output columns differently from Spark, so they pass *semantic* and fail
*strict*. Converting them is the only path that materially moves the **strict** number
(currently 5.3% / 669). Realistic reachable strict ceiling per `ROADMAP.md` is ~55–75%, and most of
that gap is exactly this pass.

The job: make oxidant **emit Spark's auto-generated column names** for the final result projection, by
reproducing Spark's `Expression.sql` / `toPrettySQL` / `prettyName` naming on the analyzed plan.

## The load-bearing design decision: this is an ENGINE feature, not a harness hack

Do **NOT** rewrite oxidant's column names inside the harness (`format.rs` / a comparison-time renamer).
The *strict* claim is "oxidant's output is byte-for-byte identical to Spark's golden". If the harness
massaged oxidant's names before diffing, the strict number would be a lie. A drop-in Spark replacement
*should* return the same result column names as Spark (BI tools, `df.columns`, `CREATE TABLE AS`
all depend on them), so emitting Spark-style names is **correct production behavior** — implement it
on the production path and the strict pass is real.

⇒ Implement in `crates/oxidant-loom/`, on the `Engine::sql` path. The harness then measures reality.

## Where to hook it

`crates/oxidant-loom/src/lib.rs`:
- `Engine::sql` (currently **line 443–453**): `df = ctx.sql(...)`, then `df.collect()`. Insert the
  rename **between** them — take `df.logical_plan()`, rewrite the top output projection's names,
  rebuild the `DataFrame` (e.g. `DataFrame::new(self.ctx.state(), new_plan)` or
  `ctx.execute_logical_plan`), then `collect()`.
- `Engine::schema` (**line 457–465**): apply the **same** rewrite so Spark Connect
  `AnalyzePlan(Schema)` / `df.schema` agree with the executed result. Factor the rewrite into one
  helper both call.
- Leave `physical_plan` (line 469) alone — distributed planning doesn't need output cosmetics.

## HARD correctness constraints (read before writing code)

Column resolution depends on names. Renaming the wrong thing changes results. So:

1. **Only the true top-level output projection.** Descend from the plan root through result-shaping
   wrappers that don't rename (`Sort`, `Limit`, `Distinct`, `Offset`) until the first `Projection`,
   rename **its** expressions, and stop. **Never recurse into subqueries, CTEs, joins, or
   sub-projections** — their column names are referenced internally; renaming them breaks the query.
2. **Preserve user aliases.** `SELECT a AS foo` → keep `foo`. In DataFusion an explicit alias is
   `Expr::Alias`; only rename projection items that are **not** already `Expr::Alias` (those carry
   DataFusion's auto-name, which is what diverges). Re-alias each such expr to its Spark name.
3. **Don't change types, only names.** The `int` vs `bigint` divergence (below) is a *separate*
   issue — do not try to fix it here.
4. **Gate every increment on the full-corpus ratchet.** This pass touches the production path; a
   bad rename rule can regress currently-passing queries. Run `golden`, confirm `strict`/`semantic`
   only rise, before committing. Re-baseline as you go (strict to the 3-run minimum — there's a ±1
   tie flake).

## The naming rules, prioritized by measured frequency

Counts are from a capped 20-failures-per-file sample of the current `schema-only` bucket (so they
under-count totals but rank correctly). Re-mine anytime with the script in **§ Measure** below.

### Rule 1 — strip DataFusion literal type-wrappers  ·  ~435 (by far #1)
oxidant prints a literal as `Utf8("x")` / `Int64(11)` / `Float64(2.1)` / `Boolean(true)`; Spark prints
the bare literal text.

| golden | oxidant |
|---|---|
| `array_contains(c, array(111, 112, 113))` | `array_contains(...,make_array(Int64(111),...))` |
| `elt(NULL, 123, 456)` | `elt(NULL,Utf8("123"),Utf8("456"))` |
| `count(DISTINCT 1)` | `count(DISTINCT Int64(1))` |
| `ifnull(1, 2.1)` | `ifnull(Int64(1),CAST(Float64(2.1) AS Float64))` |

Transform: `Utf8("s")`→`s` (no quotes), `Int64(n)`/`Int32(n)`→`n`, `Float64(x)`/`Float32(x)`→`x`
(Spark text form), `Boolean(b)`→`true`/`false`, `NULL`→`NULL`. Also **drop redundant
type-coercion casts** Spark doesn't surface (`CAST(Float64(2.1) AS Float64)`→`2.1`).

### Rule 2 — strip the table qualifier from column refs  ·  ~121
oxidant prints `testdata.a` / `data.b` / `emp.id`; Spark prints the bare column `a` / `b` / `id` in the
auto-name (note: this is the *output-name* spelling only; resolution is unaffected).

| golden | oxidant |
|---|---|
| `k:int,count(b):bigint` | `k:bigint,count(testdata.b):bigint` |
| `count(id) FILTER (WHERE (hiredate = DATE '2001-01-01'))` | `count(emp.id) FILTER (WHERE emp.hiredate = ...)` |

Transform: in the rendered name, a `Column` is its unqualified name.

### Rule 3 — render `make_array` as `array`  ·  ~23
Side effect of our `array`→`make_array` alias (wave 4): the function runs correctly but the auto-name
uses the canonical DataFusion name.

| golden | oxidant |
|---|---|
| `to_json(array(map(a, 1)))` | `to_json(array(map(make_array(Utf8("a")), ...)))` |

Transform: name-render `make_array(...)` as `array(...)`. (Watch the `map(...)` shape too — Spark
prints `map(a, 1)` where DataFusion's map takes key/value arrays; lower priority.)

### Rule 4 — parenthesize binary operations  ·  ~28
Spark wraps binary ops in parens in the auto-name; DataFusion doesn't.

| golden | oxidant |
|---|---|
| `(count((id + (power / 2))) * 3)` | `count(data.id + data.power / Int64(2)) * Int64(3)` |
| `(a = 1)` | `a = 1` |

Transform: when rendering a `BinaryExpr`, wrap it in `( … )` (matching Spark's fully-parenthesized
form, including nested).

### Rule 5 — `count(*)` → `count(1)`  ·  small but trivial
Spark names `COUNT(*)` as `count(1)`. oxidant prints `count(*)`.

### Rule 6 — literal date / binary spelling  ·  small
`DATE '2001-01-01'` (golden) vs oxidant's `CAST(Utf8("2001-01-01") AS Date32)`; `X'4561…'` (golden) vs
oxidant's `Binary("69,97,…")` **in the column name**. (The binary *value* rendering was already fixed in
`format.rs`; this is the schema-line name.) Lower priority, do after 1–4.

## Spark's algorithm (reference)

Spark derives an output name from each top projection expression via `Expression.sql` /
`Column.named` / `toPrettySQL`, then `prettyName` per expression. Key behaviors to mirror (all
visible in the tables above): bare literals, unqualified columns, fully-parenthesized binary ops,
lower-case function `prettyName`, `count(1)` for `count(*)`. You don't need Spark's source — the
goldens are the spec; render to match them byte-for-byte and let the ratchet adjudicate.

## Implementation sketch

```rust
// crates/oxidant-loom/src/lib.rs (new module or inline)

/// Spark's auto-generated output name for a top-projection expression (the inverse of the
/// divergences in COLUMN_NAMING_PASS.md). Pure function over a DataFusion `Expr`.
fn spark_expr_name(e: &Expr) -> String { /* literals bare, columns unqualified,
    BinaryExpr parenthesized, make_array->array, count(*)->count(1), ... */ }

/// Re-alias the top-level output projection's auto-named columns to their Spark names. Descends
/// only through Sort/Limit/Distinct/Offset; renames the first Projection it reaches; never enters
/// subqueries. Explicit `Expr::Alias` items are left untouched.
fn project_spark_names(plan: LogicalPlan) -> LogicalPlan { /* match root; rebuild Projection
    with expr -> if matches!(expr, Expr::Alias(_)) { expr } else { expr.alias(spark_expr_name(&expr)) } */ }
```

Build incrementally: implement Rule 1 alone, run `golden`, confirm a big jump and **no regression**,
commit, then add Rule 2, etc. Each rule is independently ratchet-gated. Unit-test `spark_expr_name`
against the example rows above before running the corpus.

Heads-up: DataFusion already exposes its own name via `Expr::schema_name()` / `Expr::display_name`
— you are *replacing* that for the top projection, not extending it. Check the DataFusion-54 `Expr`
variants (`Literal`, `Column`, `BinaryExpr`, `ScalarFunction`, `Cast`, `Alias`, `AggregateFunction`,
`Case`, …) so the renderer is exhaustive; fall back to DataFusion's name for any variant you haven't
special-cased (safe — it just stays in `schema-only`).

## Separate issue, do NOT fix here: `int` vs `bigint`

Many `schema-only` rows are a *type-spelling* divergence, not a name one: Spark integer literals
default to `INT` (32-bit), DataFusion to `Int64`/`BIGINT`, so `array<int>`/`k:int` (golden) shows as
`array<bigint>`/`k:bigint` (oxidant). This lives in the `other` (~322 sampled) cluster. It's a literal
**default-type** change (risky — affects arithmetic/overflow) and belongs in its own investigation,
not the naming pass. Flagged here only so you recognize and skip it.

## Measure / iterate / ratchet

```bash
# Re-cluster the schema-only divergences (counts rank the rules):
python3 - <<'PY'
import json, re, collections
r=json.load(open("parity/parity.json"))
pat=re.compile(r"golden `struct<(.*)>` vs oxidant `struct<(.*)>`")
c=collections.Counter()
for f in r["files"]:
  for x in f.get("failures",[]):
    if x["bucket"]!="schema-only": continue
    m=pat.search(x.get("detail","")); 
    if not m: continue
    g,w=m.group(1),m.group(2)
    if re.search(r'(Utf8|Int64|Int32|Float64|Float32|Boolean)\(',w): c['literal-wrapper']+=1
    if re.search(r'[A-Za-z0-9_]+\.[a-z]',w): c['qualifier']+=1
    if 'make_array(' in w: c['make_array']+=1
for k,n in c.most_common(): print(f"{n:5} {k}")
PY

# Full measure + the per-file debugger:
cargo run -p oxidant-spark-compat --bin oxidant-parity -- golden
cargo run -p oxidant-spark-compat --bin oxidant-parity -- file group-by.sql.out   # name-heavy file
# Gate (must hold/raise) and re-baseline after each rule:
cargo run -p oxidant-spark-compat --bin oxidant-parity -- ratchet --baseline parity/baseline.json
#   -> then copy new headline+buckets into parity/baseline.json (strict to 3-run min),
#      cp parity/parity.html site/public/parity.html ; cp parity/scoreboard.json site/public/parity.json
```

Good test files (name-divergence dense): `group-by.sql.out`, `array.sql.out`, `count.sql.out`,
`columnresolution*.sql.out`, `json-functions.sql.out`.

## Definition of done / expected impact

Each rule converts a sub-bucket of `schema-only` (right rows) into `pass` (strict). Rule 1 alone
should move several hundred. Don't expect every `schema-only` to flip — some have multiple
divergences (a name *and* the int/bigint type) and need both fixed. Target: land Rules 1–5, watch
`schema-only` fall and `pass` climb, ratchet green throughout. This is the work that takes strict
from ~5% toward the ROADMAP's ~55–75% ceiling.

## Guardrails recap (from HANDOFF.md §8)
- Faithful production behavior over the score — Spark-compatible names are correct, a comparison-time
  renamer is not.
- Ratchet is the arbiter; integrate a rule only if the full corpus holds/raises.
- Determinism: re-baseline strict to the 3-run minimum (±1 tie flake on `postgreSQL/union.sql`).
- Don't touch the concurrent platform files (`schema_adapt.rs`, `catalog_bridge.rs`, gateway/*);
  keep changes to `crates/oxidant-loom/src/**`, `crates/oxidant-spark-compat/**`, `parity/`,
  `site/public/parity.*`.

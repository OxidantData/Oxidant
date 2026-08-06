# Parity swarm playbook — drive Spark parity to its ceiling in one coordinated pass

> **You are the coordinator.** This doc tells you how to run the Spark-parity push as a sequence of
> dependency-ordered agent swarms, each fanned out with the `Workflow` tool, each gated by the
> ratchet and the faithfulness contract, looping until the corpus stops moving. Read `HANDOFF.md`
> (mission + how to run), `ROADMAP.md` (per-cluster verdicts), and `COLUMN_NAMING_PASS.md` (the
> naming deep-dive) first. Everything is on branch `feat/spark-parity-harness`.

---

## 0. The honest target (read before you promise "100%")

The goal is **maximum *faithful* parity**, measured by the harness, driven as hard as parallel
agents can take it. Be decisive about the ceiling rather than chasing a number that lies:

- **Faithful ceiling (from `ROADMAP.md`): ~85–95% semantic, ~55–75% strict.** That is the real
  objective of this playbook.
- **Literal 100% strict is *not* faithfully reachable.** The last mile is structural and trades
  against correctness for real users:
  - **Exact Spark error-text** — `error-parity` (~2,443) already *passes semantic* (both engines
    reject). Making it *strict* means reproducing Spark's exact `AnalysisException`/`SparkException`
    message strings. Low user value, high churn, and brittle. Bracket it.
  - **int-vs-bigint literal-default-type** — Spark integer literals default to `INT`, DataFusion to
    `Int64`. Matching the *spelling* (`k:int`) means changing the literal default type, which
    changes arithmetic/overflow semantics. Faithful only if done as a real type change with its own
    correctness gate, not a cosmetic one.
  - **Spark-internal behaviors** oxidant legitimately differs on (codegen, RDD, Catalyst-specific).
- **Therefore:** treat "100%" as *drive every faithful lever to exhaustion, then stop and report the
  residual as an explicit, itemized, opt-in list* — don't inflate the number with lossy rewrites.
  A faithful 70% strict beats a lossy 95%. (Memory: [[user-prefers-decisive-research-grounded-pushback]].)

The current floor (post coordinator iteration 2, 2026-06-26): **strict 22.1% (2,793), semantic 58.5% (7,397).**
(Iteration 1 floor was strict 10.5% (1,322) / semantic 45.6% (5,767); column-naming wave 1 was 7.8% / 44.3%.)

> **Iteration 2 note — cascade unmasking.** Landing `CREATE TABLE … USING` (missing-relation 2,572→900,
> −1,672) and cast-constructors (function-missing −238) unblocked ~1,900 rows that *could not run* before.
> Most became passes (strict +1,471, semantic +1,630), but ~360 now hit **pre-existing** downstream gaps,
> so several "bad" buckets rose (correctness 169→277, exec-error 957→1,093, decimal-precision 143→189,
> missing-error 126→166, null-semantics 47→71, datetime 6→13, engine-panic 1→3). These are honest
> unmaskings, NOT regressions: a per-file check confirmed **no file lost a strict (byte-correct) pass**.
> The rises are now-visible backlog (the next iterations' correctness/decimal/null targets), not new bugs.

---

## 1. The coordinator loop (this is the whole job)

Run this loop. Each iteration is one `Workflow` (or a short chain of them). You stay in the loop
between waves — read each result, re-mine, decide the next fan-out.

```
1. MEASURE   cargo run -p oxidant-spark-compat --bin oxidant-parity -- golden
2. MINE      cluster the failing buckets (§3 script) → ranked work-list
3. FAN OUT   one swarm per independent cluster, dependency-ordered (§4), via Workflow
4. INTEGRATE pull each agent's verified artifact into the main tree
5. GATE      cargo run … ratchet --baseline parity/baseline.json   (MUST hold/raise)
6. RE-BASELINE copy new headline+buckets into parity/baseline.json (strict = 3-run min),
               refresh site/public/parity.{html,json}, commit
7. REPEAT    until a full MINE pass yields no faithful, ratchet-positive work (loop-until-dry)
```

The non-negotiables that make this safe at swarm scale are in §2. The swarm decomposition is §4.
The orchestration mechanics (how to actually call `Workflow`) are §5.

---

## 2. The faithfulness contract — every agent inherits this verbatim

Paste this into every agent prompt. It is the reason a 30-agent swarm can't wreck the engine.

> **Faithfulness:** anything that runs in `Engine::sql` is on the production path for real users. A
> change must not alter results/semantics. ✅ allowed: registering a Spark function name as an alias
> of an identical DataFusion builtin; lowering Spark syntax to *equivalent* DataFusion plans;
> emitting Spark-compatible output names. ❌ forbidden: lossy rewrites (e.g. stripping `USING parquet`
> from `CREATE TABLE`, which silently turns a persistent table into an in-memory one). If the only
> way to pass a query is a lossy rewrite, it is **needs-feature**, not a shortcut — report it, don't
> ship it.
>
> **Correctness over coverage:** a newly-registered function that returns a *wrong* answer is worse
> than a missing one. After adding anything, survey the `correctness` and `exec-error` buckets for
> your new names and fix or drop. Never let `correctness`, `missing-error`, `null-semantics`,
> `decimal-precision`, or `datetime` rise.
>
> **Ratchet is the arbiter:** your change integrates only if the full corpus holds or raises strict
> AND semantic AND no "bad" bucket grows. Determinism: strict has a ±1 tie-flake
> (`postgreSQL/union.sql`); baseline strict to the 3-run minimum.
>
> **Stay in your lane:** edit only `crates/oxidant-loom/src/{lib.rs,spark_functions/**,spark_names.rs}`,
> `crates/oxidant-spark-compat/**`, `parity/`, `site/public/parity.*`. NEVER touch the concurrent
> platform files (`schema_adapt.rs`, `catalog_bridge.rs`, gateway/*) — if a build error names a file
> you didn't touch, it's another session's WIP; confirm all errors are outside your files.
>
> **MSRV 1.72 / DataFusion 54 gotchas:** no `Arc::unwrap_or_clone` (use `(*arc).clone()`);
> `ScalarUDFImpl` needs `#[derive(Debug,PartialEq,Eq,Hash)]` + exactly `name`/`signature`/
> `return_type`/`invoke_with_args` (no `as_any`); DataFusion's parser rejects Spark literal suffixes
> in tests (use `CAST(...)`). When unsure of exact Spark output, **read the golden** — grep the
> feature in `spark-tests/inputs/`, read the matching `spark-tests/results/*.sql.out`, match it
> byte-for-byte.

---

## 3. Mine the backlog (the work-list generator)

```bash
# Bucket totals are exact; per-file `failures` are capped at 20.
cargo run -p oxidant-spark-compat --bin oxidant-parity -- golden

# Cluster the dominant error signatures across function-missing / exec / parser:
python3 - <<'PY'
import json, collections, re
r=json.load(open("parity/parity.json")); c=collections.Counter()
for f in r["files"]:
  for x in f.get("failures",[]):
    d=re.sub(r"'[^']*'","'X'",x["detail"]); d=re.sub(r'"[^"]*"','"X"',d); d=re.sub(r'\d+','N',d)
    c[(x["bucket"], d[:90])]+=1
for (b,m),n in c.most_common(40): print(f"{n:4} [{b}] {m}")
PY

# Debug one file's per-block verdicts (every block, not capped):
cargo run -p oxidant-spark-compat --bin oxidant-parity -- file group-by.sql.out
```

Re-run MINE after every wave — the denominator shifts as cascades unblock.

---

## 4. The swarm decomposition (dependency-ordered)

Current buckets (post wave 1) and the swarm that owns each. **Order matters: cascade-unblockers
first, because they change every downstream number.**

| wave | swarm | buckets targeted (size) | shape |
|---|---|---|---|
| **W1** | **A · CREATE TABLE USING front-end** | `missing-relation` 2,572 + `parser-unsupported` ~120 direct | 1 design agent → small impl swarm; **sequential before W2** |
| **W2** | **B · function waves** | `function-missing` 1,133 | wide fan-out, 1 agent/batch, worktree-isolated |
| **W2** | **C · parser & features** | `parser-unsupported` 1,348, `feature-unsupported` 459 | medium fan-out per syntax family |
| **W2** | **D · column-naming wave 2** | `schema-only` 2,138 | 2 agents (int/bigint type fix; aggregate names) — see below |
| **W2** | **E · correctness hardening** | `correctness` 244, `decimal-precision` 143, `null-semantics` 47, `datetime` 6, `engine-panic` 1 | adversarial-verify swarm, highest trust |
| **W3** | **F · last-mile (opt-in)** | `error-parity` strict, `missing-error` 131 | only after W1–W2 dry; bracket per §0 |

### W1 — Swarm A: `CREATE TABLE … USING <format>` (the single biggest lever)
This one cascade drives `missing-relation` (2,572) **and** a large slice of `parser-unsupported`.
**Do NOT shim by stripping `USING`** (lossy — §2). The faithful fix is a Spark-DDL front-end that
lowers `CREATE TABLE … USING fmt [OPTIONS/PARTITIONED BY/AS SELECT]` to a real format-backed table
(`CREATE EXTERNAL TABLE … STORED AS fmt LOCATION <managed-warehouse-path>`; materialize CTAS results
first). Belongs in the planned `oxidant-sql` dialect layer, **not** `normalize_spark_sql`. Spec in
`ROADMAP.md` "create-table-using". Run this as: 1 `Plan` agent to lock the lowering design → 2–3 impl
agents (DDL parse, table provider wiring, CTAS) → integrate → **re-MINE before W2** (the corpus
reshapes massively here).

### W2 — runs in parallel once W1 has landed and been re-mined
- **B (functions):** the proven additive-UDF pattern (`HANDOFF.md` §7). One agent per function batch,
  `isolation: 'worktree'` so each compiles against real DataFusion and returns verified file source;
  you integrate and ratchet. **Loop-until-dry:** keep spawning function batches until a MINE pass
  surfaces no new implementable functions. Watch `correctness` every integration.
- **C (parser/features):** group by syntax family — PIVOT, `USE db`, `WITHIN GROUP`/ordered-set
  aggregates, remaining typed-literal gaps, `LIKE ANY`, lateral/table-valued. Each family = one
  agent. Some are parser-blocked and need the `oxidant-sql` layer, not a UDF — agents must classify
  "UDF-able vs parser-blocked" and report the latter up, not force it.
- **D (column-naming wave 2):** two independent agents. (1) **int-vs-bigint type pass** — the largest
  remaining `schema-only` chunk, but it's a *type-semantics* change (literal default `Int64`→`Int32`)
  with its own correctness risk; gate hard on `correctness`/arithmetic goldens. (2) **aggregate output
  names** — blocked because `SELECT k, count(*) … GROUP BY k` plans as `Projection→Aggregate` and the
  projection references the aggregate as a bare `Column`; fix by resolving such a column to its
  `Aggregate`-node expr and rendering *that* in `spark_names.rs`. Most aggregate rows are
  int/bigint-double-blocked, so **sequence D2 after D1** or they won't move. Full context:
  `COLUMN_NAMING_PASS.md`.
- **E (correctness):** the highest-trust swarm. For each `correctness`/`null-semantics`/
  `decimal-precision` row, one agent reproduces and fixes, then **a second, adversarial agent tries to
  refute the fix** (perspective-diverse: does it match the golden? does it break a sibling row? is the
  rounding/3VL right?). Only integrate fixes that survive refutation. `engine-panic` (multi-arg
  `COUNT(DISTINCT a,b)`) lives here.

### W3 — Swarm F: last-mile (opt-in, only after W1–W2 are dry)
`error-parity`→strict (exact Spark exception text) and `missing-error` (oxidant too lenient). Per §0
this is structural and partly non-faithful. Present the residual as an itemized list and get an
explicit decision before spending swarm budget here.

---

## 5. Orchestration mechanics (how to actually fan out)

Use the `Workflow` tool. Default to `pipeline()` (find → verify per item, no barrier); use a barrier
only when a stage genuinely needs all prior results (dedup, count-zero early-exit). The canonical
shapes for this project:

**Function/feature discovery — loop-until-dry with adversarial integration check:**
```js
export const meta = {
  name: 'parity-function-wave',
  description: 'Discover + implement Spark UDFs until dry, ratchet-gated',
  phases: [{title:'Discover'},{title:'Implement'},{title:'Verify'}],
}
const seen = new Set(); let dry = 0
while (dry < 2) {
  const batches = await agent('MINE function-missing; return up to 8 implementable Spark fns not in: '
    + [...seen].join(','), {phase:'Discover', schema: BATCH_SCHEMA})
  const fresh = batches.fns.filter(f => !seen.has(f.name))
  if (!fresh.length) { dry++; continue }
  dry = 0; fresh.forEach(f => seen.add(f.name))
  // one worktree agent per fn → it returns verified source; a verifier checks it vs the golden
  await pipeline(fresh,
    f => agent(`Implement Spark ${f.name} as an additive UDF (HANDOFF §7). `+FAITHFULNESS,
               {phase:'Implement', isolation:'worktree', schema: SRC_SCHEMA}),
    (src, f) => agent(`Adversarially verify ${f.name} vs spark-tests goldens; refute if wrong.`,
               {phase:'Verify', schema: VERDICT_SCHEMA}).then(v => ({f, src, v})))
}
```
You (the coordinator) integrate surviving artifacts into the main tree, then run the ratchet
yourself — **agents propose, the ratchet disposes.** Worktrees branch from committed HEAD, so commit
any new template before launching a swarm.

**Correctness — perspective-diverse refutation (no lossy "fix" survives):**
```js
const verdicts = await parallel(['matches-golden','breaks-sibling','semantics-3VL/rounding']
  .map(lens => () => agent(`Judge fix for "${row}" via the ${lens} lens — real?`, {schema: V})))
const keep = verdicts.filter(Boolean).filter(v => v.real).length >= 2
```

**Completeness critic** between waves: one agent asks "what bucket/cluster did we *not* touch, what
claim is unverified?" — its answer seeds the next MINE.

Scale the fleet to the work, not to a fixed number. Budget-aware: if the user set a token target,
size each wave's fan-out to `budget.remaining()`.

---

## 6. Gate, re-baseline, commit (after every wave)

```bash
cargo run -p oxidant-spark-compat --bin oxidant-parity -- ratchet --baseline parity/baseline.json
# If green: re-baseline (strict to the 3-run minimum — ±1 tie-flake), refresh the scoreboard:
#   edit parity/baseline.json headline+buckets
#   cp parity/parity.html site/public/parity.html ; cp parity/scoreboard.json site/public/parity.json
git add crates/oxidant-loom/src/** crates/oxidant-spark-compat/** parity/baseline.json site/public/parity.*
git commit   # one commit per wave; message states the bucket deltas
```
Commit per wave (not per agent) so the history reads as ratchet steps. Never stage `.claude/` or the
concurrent platform files.

---

## 7. Definition of done

Stop when a **full MINE pass produces no faithful, ratchet-positive work** across W1–W2. At that
point:
- Report the final headline (strict/semantic %) and the residual bucket table.
- Itemize the **structural residual** (§0): exact-error-text rows, the int/bigint type decision if
  not taken, and any feature explicitly deferred — each with its size and why it's bracketed.
- That itemized residual *is* the honest distance to "100%", and the decision to close any of it is
  the user's, because each item trades against faithfulness or is a large standalone feature.

This is the maximal faithful push, run as one coordinated swarm campaign — not a promise the harness
can't keep.

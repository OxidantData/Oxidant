# START HERE — Spark-parity push, fresh-session handoff

> Read this first. It is the launchpad: current state, how to run, the next moves, the orchestration
> recipe, and the gotchas that will bite you. For depth, follow the **Doc map** at the bottom.
> Everything is on branch `feat/spark-parity-harness` (pushed to `github.com/OxidantData/Oxidant`, remote name
> `github`). Last parity commit at this handoff: `5570287` (aggregate output names, +126 strict). The
> GitHub CI parity gate (`.github/workflows/ci.yml`) was merged to `main` separately via PR #4.

---

## 1. What this is

oxidant is a drop-in Apache Spark replacement on DataFusion 54. We measure Spark compatibility by replaying
**Apache Spark v4.0.0's own golden SQL corpus** (`crates/oxidant-spark-compat/spark-tests/{inputs,results}`,
304 files / **12,641 queries**, vendored verbatim from `sql/core/src/test/resources/sql-tests`) through
`oxidant_loom::Engine`, formatting output Spark-style, and diffing against Spark's committed `.sql.out`. A CI
ratchet (`parity/baseline.json`) means parity can only rise. This *is* the faithful way to "run Spark's
actual unit tests" — it's Spark's `SQLQueryTestSuite` corpus. (The Scala/JVM-internal suites test
Catalyst/codegen/RDD internals oxidant doesn't have — they'd validate DataFusion, not parity.)

Two numbers: **strict** = byte-for-byte identical to the golden (schema line + rows); **semantic** =
right answer / right rejection, crediting benign column-name / error-text / tie-order divergence.

## 2. Current state (deterministic)

**strict 23.1% (2,919 / 12,641) · semantic 58.5% (7,397 / 12,641).** Up from 2.2% / 25.5% at the start.

| bucket | n | | bucket | n |
|---|--:|---|---|--:|
| **pass** (strict) | 2,919 | | exec-error | 1,093 |
| error-parity (sem) | 2,406 | | missing-relation | 900 |
| schema-only (sem*) | 2,021 | | function-missing | 721 |
| parser-unsupported | 1,238 | | feature-unsupported | 482 |
| correctness | 277 | | missing-error | 166 |
| decimal-precision | 189 | | requires-udf-registration (excluded) | 87 |
| null-semantics | 71 | | ordering (sem) | 48 |
| datetime | 13 | | engine-panic | 3 |
| nondeterministic (excluded) | 4 | | | |

Trajectory: column-naming-w1 (7.8/44.3) → iter-1 six levers (10.5/45.6) → iter-2 CREATE TABLE USING +
cast-constructors (22.1/58.5) → iter-3 aggregate output names (**23.1/58.5**, +126 strict). See
`HANDOFF.md` for the per-iteration changelog.

> **CI gate is now live on GitHub.** `.github/workflows/ci.yml` (merged to `main`, PR #4) runs the
> parity ratchet on every PR and fails it if strict/semantic parity drops below `parity/baseline.json`
> — so the work here is protected. (Its `fmt`/`build-test` jobs are currently red on pre-existing
> `oxidant-bench`/`oxidant-gateway` debt, surfaced not introduced; the `parity` job is green.)

## 3. How to run (≈10s build incremental, golden ≈30–40s now that CTU writes real files)

```bash
cargo build -p oxidant-spark-compat --bin oxidant-parity
./target/debug/oxidant-parity golden     # measure; writes parity/{parity.json,report.md,parity.html,scoreboard.json}
./target/debug/oxidant-parity file group-by.sql.out   # uncapped per-block verdicts for ONE file
./target/debug/oxidant-parity ratchet --baseline parity/baseline.json   # CI gate (strict & semantic must not drop)
cargo test -p oxidant-loom -p oxidant-spark-compat        # 177 + 23 unit tests, all green
```

## 4. The coordinator loop (the whole job)

`MEASURE → MINE → FAN OUT (swarm) → INTEGRATE → GATE (ratchet) → RE-BASELINE → REPEAT until dry.`
Full recipe + the faithfulness contract every agent inherits: **`PARITY_SWARM_PLAYBOOK.md`**. MINE script
(cluster failing signatures into a ranked work-list): `HANDOFF.md` §5 / playbook §3.

## 5. THE non-negotiables (these are what keep a 30-agent swarm from wrecking the engine)

1. **FAITHFULNESS.** Anything in `Engine::sql` is on the production path for real users. ✅ alias a Spark
   fn to an identical DF builtin; lower Spark syntax to an *equivalent* DF plan; emit Spark-compatible
   names. ❌ lossy rewrites — the canonical sin is stripping `USING parquet` (turns a persistent table
   into an in-memory MemTable). If the only way to pass is lossy, it's **needs-feature** → report, don't
   ship. A faithful 70% beats a lossy 95%.
2. **The real regression gate is NOT "no bad bucket rose" — it's "no file lost a strict pass."** Unblocking
   a cascade (CREATE TABLE USING, a function wave) makes previously-unrunnable rows execute and hit
   *pre-existing* downstream gaps, so correctness/exec-error/decimal/etc. **rise — that is honest
   unmasking, not regression.** Verify the real line with the **stash audit** (§7). Iter-2 rose correctness
   169→277 etc. and was correct to ship because **zero files lost a strict (byte-correct) pass.**
3. **The ratchet only gates strict + semantic + blocks_total** (`bin/parity.rs`). Both must not drop; bad
   buckets are not gated. Agents propose, the ratchet + your stash-audit dispose.
4. **Stay in lane.** Edit only `crates/oxidant-loom/src/{lib.rs,spark_functions/**,spark_names.rs,spark_int_literals.rs,spark_create_table.rs}`,
   `crates/oxidant-loom/Cargo.toml`, `crates/oxidant-spark-compat/**`, `parity/`, `site/public/parity.*`. NEVER
   touch `schema_adapt.rs`, `catalog_bridge.rs`, `gateway/*` — a **concurrent session** owns the platform
   control plane on this same branch (its worktree: `/Users/vamsi/projects/oxidant-platform-k8s`).

## 6. Next moves (ranked — this is where to point the next swarm)

The iter-2 cascade surfaced its own backlog. Highest leverage first:

1. **✅ DONE (iter-3) — Column-naming wave 2: aggregate output names.** `count(*)`→`count(1)`,
   `count(testdata.a)`→`count(a)`, `sum(t.power)`→`sum(power)`, DISTINCT/FILTER preserved, nested in
   binary ops. Landed in `spark_names.rs` (commit `5570287`): builds an output-name→expr map from the
   `Aggregate` feeding the top projection (descend Filter/Sort/Limit/Distinct/SubqueryAlias) and renders
   such bare `Column`s as the aggregate expr. **strict 2,793→2,919 (+126); semantic held; per-file audit
   = no file lost a strict pass.** Pure output-shaping. (DataFusion builds `count(*)` as `count(Int64(1))`
   aliased `"count(*)"`, so its arg renders `1`.)
2. **⛔ BLOCKED (iter-3, investigated + reverted) — Decimal-precision pass (189).** Typing unsuffixed `1.5`
   as `decimal(2,1)` IS faithful and the rewrite is a 1-branch add in `lib.rs::rewrite_spark_typed_literals`
   (reuse `decimal_ps`; gate on `num.contains('.') && !has_exp` so `2.35E10` stays double, matching Spark).
   Measured: **strict +15 / semantic +17, decimal-precision −22, correctness −4, missing-error −12** — BUT
   it regressed **2 files (6 byte-correct strict passes): `predicate-functions.sql` −5, `inline-table.sql`
   −1**, and raised exec-error +24. Root cause = **DataFusion 54 coercion/overflow gaps the decimal type
   exposes**, NOT the literal typing: (a) **no `Utf8`↔`Decimal128` comparison coercion** — `'1.5' > 0.5`
   fails `simplify_expressions` where string-vs-`double` worked (Spark coerces; golden `(1.5 > 0.5):boolean`);
   (b) **decimal-multiply overflow errors** (Arrow "Arithmetic overflow") where Spark returns NULL with
   `allowPrecisionLoss`; (c) **decimal in window-frame bounds** → "Invalid window frame". This is a real
   needs-feature (the three coercion/overflow fixes), not a quick win — per the faithfulness rule it was
   reverted rather than shipped as a 6-pass regression. Unblock = add those coercions (an analyzer/`ExprPlanner`
   string→numeric rule + Spark-style decimal overflow→NULL), THEN re-apply the 1-branch literal typing.
3. **Unmasked correctness (277) + missing-error (166) + null-semantics (71).** Now-visible pre-existing
   gaps, concentrated in `collations.sql`, `postgreSQL/numeric.sql`, `window.sql`, `charvarchar.sql`,
   `postgreSQL/int4/int8.sql`. Diagnose→fix→**adversarially refute** (the highest-trust swarm; only ship
   refutation survivors). missing-error = oxidant too lenient now that tables exist (accepts queries Spark
   rejects) — needs analyzer validations.
4. **Function wave (function-missing 721):** `listagg` (needs `WITHIN GROUP` plan support), `from_xml`/
   `from_csv`/`to_csv` (extend the `spark_from_json.rs` schema-string parser), `percentile_disc`,
   `grouping_id`, `to_timestamp_ltz`. (uniform/randn = nondeterministic, excluded; udaf/foo*/udtf already
   excluded as test fixtures.)
5. **CREATE TABLE USING deferred follow-ons** (`CREATE_TABLE_USING_DESIGN.md`): CTAS (`USING fmt AS SELECT`,
   needs COPY-then-CREATE-EXTERNAL materialization), `PARTITIONED BY`, `OPTIONS`/`LOCATION`, exotic column
   types (varchar(n)/timestamp_ntz/nested struct). Each currently returns `None` → fails as before.
6. **Structural residual (the honest distance to 100%, per `ROADMAP.md` §0/§4):** exact Spark error-text
   (`error-parity`→strict — low value, brittle, partly anti-faithful), and Spark-internal behaviors oxidant
   legitimately differs on. Faithful ceiling ≈ 85–95% semantic / 55–75% strict. Don't chase strict at the
   cost of correctness; present the residual as an itemized opt-in list.

## 7. The stash audit (run this to prove faithfulness after any cascade-unblocking change)

```bash
cp parity/parity.json /tmp/after.json                 # your built tree's result
git stash && cargo build -q -p oxidant-spark-compat --bin oxidant-parity && ./target/debug/oxidant-parity golden
cp parity/parity.json /tmp/before.json && git stash pop && cargo build -q -p oxidant-spark-compat --bin oxidant-parity
# then per-file: assert no file's `pass` (strict) count dropped before→after; confirm every bad-bucket
# rise sits on a missing-relation/function-missing drop in the SAME file (= unmasking, not regression).
```
(After `git stash` the new untracked `spark_*.rs` files orphan harmlessly — their `mod` lines are stashed.)

## 8. Integration mechanics (coordinator replays agent artifacts into the main tree)

Worktree agents return structured artifacts: `new_files` (full content) + `edits` ({path, old_str, new_str})
+ `added_deps`. You replay them: Write new files, apply edits with exact-match verification, then build +
ratchet. Gotchas learned the hard way:
- Agent JSON may be **HTML-escaped** (`&gt;`/`&amp;`) — unescape before writing or it won't compile.
- `spark_functions/mod.rs` **register-call anchors displace** when multiple agents add a `mod`+`register`
  line — apply those manually. Same for any shared file (`lib.rs`, `spark_names.rs`, `format.rs`).
- Each agent compiled in isolation from HEAD; the **combined** tree is the truth — build once, run the
  full golden, run the stash audit. Don't trust summed isolated deltas.
- Re-baseline strict to the **3-run minimum** (±1 `postgreSQL/union.sql` tie-flake); semantic is stable.
  Then `cp parity/scoreboard.json site/public/parity.json; cp parity/parity.html site/public/parity.html`.

## 9. Orchestration recipe (how to launch the next swarm — needs "ultracode"/explicit opt-in)

Use the `Workflow` tool. Two iteration scripts are saved at (session scratchpad — copy out if you want them):
`parity_iteration.js`, `parity_iteration2.js`. Shape that worked:
- Each impl agent runs `isolation: 'worktree'` and **self-verifies via the harness** (build →
  `./target/debug/oxidant-parity golden` → compare buckets to `parity/baseline.json` → confirm no bad bucket
  rose / report honestly) before returning its artifact. Gold standard.
- Risky levers (correctness, decimal, type changes): `diagnose → fix → 3-lens adversarial refute`; ship
  only refutation survivors. A refuter's abstract doubt loses to the harness golden-diff ground truth, but
  read its objection — it catches out-of-corpus gaps (e.g. divide-by-zero → Inf vs Spark NULL).
- Big features (CREATE TABLE USING): dedicated `impl → 3-lens verify` pipeline, `effort: 'high'`.

## 10. OPS gotchas (will bite you)

- **Disk fills up.** Each worktree agent does a cold full build (~5 GB). A swarm leaves
  `.claude/worktrees/wf_*` — once **121 GB**, which failed a link with `errno 28`. After extracting
  artifacts: `git worktree list --porcelain | awk '/^worktree /{print $2}' | grep '/.claude/worktrees/wf_' |
  while read w; do git worktree remove --force "$w"; done; git worktree prune`. (Leave `oxidant-platform-k8s`.)
- **Spend limit** killed agents mid-swarm in iter-1 (cast-constructors, refuters). If agents fail with
  "monthly spend limit," raise it (claude.ai/settings/usage) before launching a full swarm — or
  coordinator-verify the survivors yourself.
- CTU makes `golden` slower (writes real files to a per-engine temp warehouse, torn down on `Engine` drop);
  a single golden run can exceed a 2-min Bash timeout — give it room or run in background.

## 11. Doc map

- **`PARITY_SWARM_PLAYBOOK.md`** — the coordinator playbook (the loop, faithfulness contract, swarm waves,
  Workflow patterns). Run the campaign from here.
- **`HANDOFF.md`** — detailed per-iteration changelog + current bucket table + ranked next steps + the
  "how to add a Spark UDF" template (§7).
- **`ROADMAP.md`** — per-cluster verdicts, the oxidant-sql dialect-layer architecture, the honest-ceiling §0/§4.
- **`CREATE_TABLE_USING_DESIGN.md`** — CTU spec; non-CTAS subset LANDED, follow-ons specced.
- **`COLUMN_NAMING_PASS.md`** — output column-naming deep-dive (w1 landed; aggregate-names is move #2 above).
- **`README.md`** — harness internals.
- Memory: `~/.claude/projects/-Users-vamsi-projects-oxidant/memory/spark-parity-harness.md`.

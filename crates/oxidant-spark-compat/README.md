# oxidant-spark-compat — Apache Spark parity harness

Turns "Oxidant is a drop-in Spark replacement" from a slogan into a **measured, gated number**.

It replays Apache Spark's *own* golden SQL tests (`sql/core/src/test/resources/sql-tests/`,
vendored under `spark-tests/` from Spark **v4.0.0**) through Oxidant, formats the results exactly the
way Spark's `SQLQueryTestSuite` does, and diffs them against Spark's committed `.sql.out` golden
outputs. Every mismatch is filed into a triage bucket so the result is an actionable backlog.

## Run it

```bash
# Full corpus → parity/{parity.json, report.md, parity.html, scoreboard.json} + headline.
cargo run -p oxidant-spark-compat --bin oxidant-parity -- golden

# CI gate: fail if parity dropped below parity/baseline.json (the ratchet).
cargo run -p oxidant-spark-compat --bin oxidant-parity -- ratchet --baseline parity/baseline.json

# Debug one file's per-block verdicts.
cargo run -p oxidant-spark-compat --bin oxidant-parity -- file group-by.sql.out

# Tests (fast wiring tests + the #[ignore]d full-corpus ratchet).
cargo test -p oxidant-spark-compat            # fast
cargo test -p oxidant-spark-compat -- --ignored   # full corpus, ~7s
```

## Two honest numbers

- **strict** — byte-for-byte identical to Spark's golden (schema line + rows).
- **semantic** — right answer / right rejection, crediting two benign divergences: column-name
  spelling in the `struct<...>` schema line, and "both engines reject this query" (different JVM
  vs Rust error text). The fair reading for a drop-in replacement.

## Current state (Spark v4.0.0, 12,641 queries across 303 files)

| metric | first baseline | now | Δ |
|---|---|---|---|
| strict parity | 2.2% (274) | **4.2% (536)** | +262 |
| semantic parity | 25.5% (3,223) | **39.4% (4,984)** | +1,761 |

The jump came from the first parity fixes: **`CREATE [OR REPLACE] [GLOBAL] {TEMPORARY|TEMP} VIEW`
support** in Oxidant's engine (`oxidant_loom::normalize_spark_sql`). DataFusion's `create_view` rejects
*only* the `temporary` flag, so the rewrite drops the keyword (Spark temp views and DataFusion
session-catalog views are both session-scoped — semantically equivalent), and also accepts Spark's
`TEMP` abbreviation. That cleared ~2,650 cascade failures; ~1,500 of those queries now run and
return the right values.

These rewrites are deliberately **semantically faithful** (drop a keyword that doesn't change
results). The next-biggest parser cluster — `CREATE TABLE … USING <format>` — is intentionally
*not* shimmed in the core engine: stripping `USING parquet` would silently turn a persistent table
into an in-memory one, which is lossy in the production path. Adaptations like that belong in the
planned `oxidant-sql` Spark-dialect front end, not in `Engine::sql`.

### Triage backlog — failures by bucket (current)

| bucket | count | what it means |
|---|---:|---|
| `missing-relation` | 2,916 | remaining setup cascades (`CREATE TABLE … USING`, `INSERT`, …) |
| `error-parity` | 2,464 | ✓ both engines reject (counts as semantic pass) |
| `schema-only` | 1,984 | ✓ right values, divergent column name (semantic pass; blocks *strict*) |
| `function-missing` | 1,653 | functions Oxidant/DataFusion lacks or names differently |
| `parser-unsupported` | 1,304 | Spark SQL syntax DataFusion's parser rejects (mostly `CREATE TABLE … USING`) |
| `exec-error` | 938 | miscellaneous execution failures |
| `feature-unsupported` | 378 | `PIVOT`, `USE db`, `SHOW CREATE TABLE`, … |
| `correctness` | 180 | **genuine wrong answers — top priority** |
| `decimal-precision` | 132 | precision/scale/rounding diverged |
| `missing-error` | 110 | Oxidant accepted a query Spark rejects (too lenient) |
| `null-semantics` | 23 | three-valued-logic divergence |
| `ordering` | 21 | right multiset, wrong row order |
| `datetime` | 1 | calendar/timezone/format |
| `engine-panic` | 1 | DataFusion `panic!` (multi-arg `COUNT(DISTINCT a,b)`) — isolated, not fatal |

### Next levers — and which layer each belongs in

The remaining gains split by *where* the work has to live:

- **`oxidant-sql` Spark-dialect front end (not core `Engine`)** — `CREATE TABLE … USING <format>`
  (the largest parser + missing-relation driver, ~120 direct + cascades), `PIVOT`, `USE db`,
  `LIKE ANY`. These need real parsing/planning, and several are *lossy* as naive string shims, so
  they don't belong in `Engine::sql`.
- **DataFusion column-naming (`schema_name`)** — `schema-only` (1,984) is the biggest *strict*
  lever: right rows, but DataFusion names columns `Utf8("hello")` / `count(testdata.a)` where Spark
  uses `hello` / `count(a)`. Aligning generated names converts ~1,900 semantic passes into strict
  ones — but it means overriding DataFusion's expr display and is correctness-sensitive.
- **Real feature/function work** — `function-missing` (1,653), `decimal-precision`, multi-arg
  `COUNT(DISTINCT)` (the `engine-panic`), `correctness` (180 — fewest but highest-trust).

## How it works

`golden.rs` parses `.sql.out` into `(sql, schema, output)` blocks → one `oxidant_loom::Engine` per
file replays them in order (so `CREATE … VIEW` setup persists) → `format.rs` renders Oxidant's Arrow
output Spark-style → `normalize.rs` applies Spark's row-sorting for unordered queries →
`classify.rs` buckets the result → `report.rs` aggregates the scoreboard. The golden file is the
authoritative statement list, so we never re-implement Spark's `.sql` splitter.

Files needing machinery Oxidant lacks (registered UDFs, `--IMPORT` chains) are **skipped with a
recorded reason**, never silently dropped — see the "Skipped files" section of `report.md`.

## Re-baselining

When Oxidant improves, lock the gain in: `oxidant-parity golden` then commit the regenerated
`parity/baseline.json`. The CI `spark-parity` job and the `#[ignore]`d ratchet test both read it.

The scoreboard is published to the site at `site/public/parity.{html,json}`.

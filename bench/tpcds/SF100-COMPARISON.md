# TPC-DS SF100 three-engine comparison: Oxidant vs EMR Spark vs Athena

Date: 2026-08-11. Dataset: TPC-DS SF100 (~100 GB scale factor 100) in Glue
`tpcds_sf100` (canonical typed Parquet, `s3://weft-artifacts-810738286322/tpcds-sf100-typed/`),
us-west-2. Same dsqgen query texts (`bench/tpcds/queries/q1..q99.sql`) for all three
engines, single cold run per query (`tries=1`), `.collect()` timing.

## Headline

| Engine | Pass | Total (s, passing queries) | Notes |
|--------|------|---------------------------:|-------|
| **Oxidant v0.1.5** | **99/99** | **6669** | Spark Connect, 2-worker distributed, `OXIDANT_DISTRIBUTED_STRICT=1` |
| **EMR Spark 3.5 (emr-7.13.0)** | **99/99** | **5753** | hive catalog, AQE defaults |
| **Athena engine v3 (Trino)** | **89/99** | **1028** | serverless; 10 dialect failures (see below); 0.37 TB scanned (~$2 at $5/TB) |

## Topologies (not apples-to-apples — read before comparing numbers)

| | Oxidant | EMR Spark | Athena |
|--|---------|-----------|--------|
| Driver/master | c6g.2xlarge (8 vCPU Graviton3) | c6g.2xlarge | — (serverless) |
| Workers | 2 × **m8g.8xlarge** (32 vCPU / 128 GiB each) | 2 × **m8g.4xlarge** (16 vCPU / 64 GiB each) | — |
| Shuffle partitions | 32 | 200 (Spark default) + AQE | engine-managed |
| Catalog | Glue via Oxidant catalog bridge | Glue via hive-metastore (`spark.sql.catalogImplementation=hive`) | Glue native |
| Memory | `OXIDANT_MEMORY_LIMIT_BYTES` ~27 GiB + 8 GiB shuffle spill per worker | EMR defaults | — |

EMR core nodes are half the size of Oxidant's workers (m8g.8xlarge was blocked by an
account vCPU quota on the EMR path). Oxidant's totals were achieved with **2× the
worker hardware** — see "Performance findings".

## Per-query results

| Query | Oxidant (s) | EMR Spark (s) | Athena (s) |
|-------|------------:|--------------:|-----------:|
| Q1 | 10.9 | 29.9 | 6.4 |
| Q2 | 55.4 | 51.2 | 12.0 |
| Q3 | 0.9 | 35.7 | 8.8 |
| Q4 | 116.3 | 170.7 | 24.3 |
| Q5 | 172.5 | 103.2 | 35.1 |
| Q6 | 76.2 | 24.6 | 5.2 |
| Q7 | 60.7 | 79.1 | 7.8 |
| Q8 | 0.9 | 33.0 | 10.9 |
| Q9 | 1.2 | 32.8 | 7.5 |
| Q10 | 171.9 | 37.0 | 7.0 |
| Q11 | 79.5 | 128.4 | 12.2 |
| Q12 | 1.3 | 23.1 | 4.0 |
| Q13 | 1.2 | 67.7 | 10.4 |
| Q14 | 115.6 | 200.9 | 21.2 |
| Q15 | 55.3 | 37.8 | 5.7 |
| Q16 | 11.7 | 106.3 | FAIL |
| Q17 | 316.6 | 97.4 | 9.3 |
| Q18 | 1.1 | 35.0 | 4.9 |
| Q19 | 74.6 | 50.1 | 11.3 |
| Q20 | 0.9 | 9.6 | 6.3 |
| Q21 | 11.6 | 4.9 | 4.5 |
| Q22 | 1.1 | 8.3 | 4.4 |
| Q23 | 195.7 | 135.3 | 21.9 |
| Q24 | 20.6 | 54.9 | 9.6 |
| Q25 | 316.3 | 107.8 | 12.0 |
| Q26 | 37.8 | 28.1 | 7.2 |
| Q27 | 1.9 | 215.6 | 10.9 |
| Q28 | 1.4 | 22.8 | 16.2 |
| Q29 | 391.2 | 98.8 | 9.0 |
| Q30 | 3.5 | 6.9 | 4.2 |
| Q31 | 54.4 | 143.9 | 10.2 |
| Q32 | 1.1 | 40.8 | FAIL |
| Q33 | 302.2 | 71.5 | 7.5 |
| Q34 | 1.3 | 61.1 | 9.9 |
| Q35 | 174.5 | 41.8 | 6.9 |
| Q36 | 1.2 | 55.7 | 10.5 |
| Q37 | 24.2 | 18.1 | 4.6 |
| Q38 | 150.9 | 44.0 | 8.3 |
| Q39 | 1.7 | 5.5 | 4.1 |
| Q40 | 12.0 | 56.1 | 5.2 |
| Q41 | 0.7 | 2.5 | 10.1 |
| Q42 | 74.7 | 31.9 | 7.0 |
| Q43 | 1.1 | 34.8 | 9.5 |
| Q44 | 1.0 | 43.7 | 15.2 |
| Q45 | 27.5 | 25.2 | 4.0 |
| Q46 | 1.0 | 68.4 | 10.4 |
| Q47 | 2.2 | 48.1 | 10.9 |
| Q48 | 1.2 | 68.0 | 8.7 |
| Q49 | 151.0 | 147.0 | 29.5 |
| Q50 | 19.9 | 52.7 | 6.9 |
| Q51 | 131.8 | 56.8 | 9.9 |
| Q52 | 1.3 | 34.9 | 6.4 |
| Q53 | 1.1 | 44.3 | 8.9 |
| Q54 | 447.0 | 46.5 | 7.7 |
| Q55 | 1.3 | 33.9 | 10.9 |
| Q56 | 158.9 | 69.8 | 24.7 |
| Q57 | 26.3 | 22.0 | 7.2 |
| Q58 | 148.7 | 52.7 | FAIL |
| Q59 | 75.0 | 64.5 | 18.4 |
| Q60 | 84.5 | 79.4 | 8.6 |
| Q61 | 1.7 | 85.5 | 41.3 |
| Q62 | 29.5 | 21.7 | 30.0 |
| Q63 | 1.2 | 48.3 | 7.3 |
| Q64 | 204.2 | 132.9 | 50.6 |
| Q65 | 4.2 | 50.8 | 12.9 |
| Q66 | 110.8 | 52.4 | 35.4 |
| Q67 | 21.5 | 71.0 | 11.7 |
| Q68 | 1.1 | 71.9 | 10.0 |
| Q69 | 141.2 | 38.9 | 6.5 |
| Q70 | 78.2 | 70.1 | FAIL |
| Q71 | 74.8 | 46.6 | 6.5 |
| Q72 | 29.6 | 32.5 | FAIL |
| Q73 | 73.4 | 60.8 | 9.4 |
| Q74 | 57.1 | 97.9 | 10.3 |
| Q75 | 223.2 | 174.8 | 14.1 |
| Q76 | 53.2 | 117.4 | 8.7 |
| Q77 | 204.6 | 81.6 | 11.7 |
| Q78 | 248.4 | 135.7 | 13.1 |
| Q79 | 1.3 | 62.0 | 11.2 |
| Q80 | 104.6 | 157.6 | 16.7 |
| Q81 | 6.3 | 9.7 | 4.4 |
| Q82 | 97.3 | 13.2 | 4.6 |
| Q83 | 9.3 | 14.3 | FAIL |
| Q84 | 0.5 | 5.1 | 2.1 |
| Q85 | 33.7 | 41.6 | 6.1 |
| Q86 | 0.6 | 22.0 | FAIL |
| Q87 | 140.5 | 42.2 | 11.0 |
| Q88 | 74.1 | 33.9 | 9.5 |
| Q89 | 1.1 | 47.7 | 9.6 |
| Q90 | 27.2 | 21.2 | 6.5 |
| Q91 | 5.5 | 10.1 | 3.5 |
| Q92 | 0.5 | 41.0 | FAIL |
| Q93 | 1.5 | 0.5 | 7.6 |
| Q94 | 3.9 | 61.2 | FAIL |
| Q95 | 4.3 | 53.9 | FAIL |
| Q96 | 0.7 | 32.5 | 36.7 |
| Q97 | 68.6 | 35.8 | 7.1 |
| Q98 | 89.0 | 33.5 | 6.4 |
| Q99 | 55.2 | 20.5 | 12.9 |
| **pass** | **99/99** | **99/99** | **89/99** |
| **total** | **6669** | **5753** | **1028** |

Athena total data scanned: 0.37 TB (~$2 at $5/TB). Athena total covers only the 89
passing queries.

## Findings

### 1. Correctness / coverage

- **Oxidant passes 99/99 at SF100** with distributed execution enforced
  (`OXIDANT_DISTRIBUTED_STRICT=1` — no driver-local fallback). This is the first
  full clean SF100 run.
- **EMR Spark passes 99/99** after harness fixes (below).
- **Athena fails 10 queries on TPC-DS dialect strictness** (Trino does not coerce
  the way Spark/dsqgen expects):
  - `date` vs `varchar` literal comparison — Q16, Q32, Q58, Q83, Q92, Q94, Q95
    (`Cannot check if date is BETWEEN varchar(10) and date`, etc.)
  - `date + integer` arithmetic — Q72
  - select-alias from a `grouping()` expression referenced in `ORDER BY` — Q70, Q86

  Per the run decision these stand as engine gaps: Athena did not honor the
  dsqgen TPC-DS SQL as written. (A `DATE '…'` literal / interval / ORDER BY
  rewrite would unblock all 10; not applied.)

### 2. The headline "engine failures" were harness bugs, not engine bugs

The first Oxidant run showed 95/99 with Q31/Q49/Q51/Q91 "refused by the
distributed splitter", and the first EMR run showed 87/99. **Both were the same
client-side bug**, present independently in both runner scripts:

- `qualify()` rewrote table names with a bare regex, mangling anything colliding
  with a TPC-DS table name: column aliases (`AS store_sales` Q31, `AS item` Q49,
  `store_v1 store` Q51, `Call_Center` Q91) and string literals (`'store'` Q49).
- The same bug **silently corrupted 6 more queries** (Q5/Q14/Q76/Q77/Q80/Q93) —
  they "passed" but compared against literals like `'glue.tpcds_sf100.store'`,
  returning wrong results. Silent wrong-results are worse than loud failures;
  this is the strongest argument for result-set validation in bench harnesses.
- EMR had a second harness issue: dsqgen `AS "order count"` double-quoted
  aliases are parse errors on Spark with ANSI off
  (`spark.sql.ansi.doubleQuotedIdentifiers` requires `ansi.enabled=true`, which
  we did not want to enable). Fixed by rewriting to backticks (8 queries:
  Q16/Q32/Q50/Q62/Q92/Q94/Q95/Q99).

Fixes: `bench/tpcds/run-ec2-connect.py` and `scratchpad/emr_tpcds_run.py` now use
a from-list-aware tokenizer `qualify()` (string literals, quoted identifiers and
comments are opaque; only table-reference positions are rewritten). After the
fix: 99/99 on both engines.

### 3. Engine diagnostics gap (fixed in tree)

The Oxidant strict-refusal message was misleading: `oxidant-connect` built the
driver logical plan with `.ok()`, swallowing the parse error, so a client-side
SQL defect surfaced as "query did not run distributed; refusing driver-local
fallback" — pointing at the splitter, which was never even consulted (all four
plan shapes were already pinned in `bench/distributed/tpcds-*-baseline.json`).
`crates/oxidant-connect/src/lib.rs` now logs the plan-build failure and carries
it into the strict refusal message (+ unit tests).

### 4. Performance findings (Oxidant vs EMR)

- Oxidant wins 55 of 99 queries head-to-head, but **loses the total: 6669s vs
  5753s (~16% slower) — with double the worker hardware** (2× m8g.8xlarge vs
  2× m8g.4xlarge) and 32 shuffle partitions vs EMR's 200+AQE.
- 23 queries are >2× slower than EMR. Worst offenders:
  Q54 (447.0s vs 46.5s, 9.6×), Q82 (97.3 vs 13.2, 7.4×), Q10 (171.9 vs 37.0, 4.7×),
  Q33 (302.2 vs 71.5, 4.2×), Q35 (174.5 vs 41.8, 4.2×), Q29 (391.2 vs 98.8, 4.0×),
  Q69 (141.2 vs 38.9, 3.6×), Q38 (150.9 vs 44.0, 3.4×), Q17 (316.6 vs 97.4, 3.3×),
  Q87 (140.5 vs 42.2, 3.3×).
- Pattern: long multi-fact join aggregations with big intermediate results.
  Candidate causes to investigate: shuffle partition count (32 vs 200), no AQE
  equivalent (EMR coalesces/switches join strategies at runtime), join order on
  the uncorrelated-subquery-heavy queries (Q54/Q82 are `IN`-subquery shapes).
- Oxidant's wins are concentrated in short interactive-class queries (1–2s where
  EMR pays 20–70s of Spark scheduling/planning overhead) and a few large ones
  (Q31 54.4 vs 143.9, Q27 1.9 vs 215.6, Q76 53.2 vs 117.4, Q80 104.6 vs 157.6,
  Q14 115.6 vs 200.9).
- Athena is fastest on most individual queries (serverless Trino, engine-managed
  parallelism) but is not comparable on cost model — it scanned 0.37 TB total
  (~$2) with zero cluster management.

### 5. Pre-existing issues observed during the runs (not caused by harness fixes)

- **Q97** transient `stage cancelled by driver` on the first Oxidant attempt;
  passed on reconnect (runner's `session_dead` retry). Worth a reliability ticket.
- **Q66** float summation-order mismatch in the local SF1 distributed execute
  gate (last-digit differences in `*_sales_per_sq_foot`), exposed by the typed
  data WIP. Reproduces with the harness fixes reverted — separate engine issue.
- **Q44** `EXPLAIN` panics in **debug builds only** (`attempt to multiply with
  overflow` in datafusion-physical-plan 54.1.0 join cardinality estimation).
  Release builds wrap arithmetic; EC2 unaffected.

### 6. Glue catalog cache staleness (bug class found + fixed this session)

Oxidant cached Glue table metadata in-process indefinitely; after the SF100
dataset was re-typed (new S3 prefix), the driver/workers kept serving the stale
locations until restart. Fixed in tree: TTL revalidation for cached tables
(`OXIDANT_CATALOG_CACHE_TTL_MS`, default 60s, fail-open), catalog version bump
propagation, and `spark.catalog.refreshTable` RPC support. A follow-up audit of
all engine caches produced 18 Jira tickets (KAN-119…KAN-136); top items:
KAN-119 (DataFusion ListFilesCache unbounded TTL), KAN-120 (lakehouse snapshot
pinned forever on driver), KAN-121 (replicated tables frozen as MemTables).

## Artifacts / repro

- Results: `bench/tpcds/results/tpcds-sf100-ec2-v0.1.5.json` (Oxidant),
  `bench/tpcds/results/emr-tpcds-sf100.json` (EMR),
  `bench/tpcds/results/tpcds-sf100-athena.json` (Athena).
- Runners: `bench/tpcds/run-ec2-connect.py` (Oxidant Connect),
  `scratchpad/emr_tpcds_run.py` (EMR spark-submit, script at
  `s3://weft-artifacts-810738286322/emr-tpcds-sf100/emr_tpcds_run.py`),
  `scratchpad/athena_tpcds_run.py` (Athena, workgroup `tpcds-sf100-comparison`).
- Table generation: `scratchpad/compare_sf100.py`.
- Oxidant cluster: CFN stack `oxidant-sf100`, AMI v0.1.5 `ami-02492f989f64d9473`.
- EMR cluster: `j-099757134N721JVRMM4N` (terminated after the run); step
  `s-03696673K9QRZOR3F1L9` was the final (clean) run.
- Session handoff with full operational detail: `scratchpad/SF100-HANDOFF.md`.

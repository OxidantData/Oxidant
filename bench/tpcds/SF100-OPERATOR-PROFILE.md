# KAN-153 — Operator-level profile of the worst SF100 TPC-DS queries (v0.1.8)

Date: 2026-08-12. Cluster: driver 8 vCPU `54.189.36.172`,
workers 2 × m8g.4xlarge (16 vCPU / 61 GiB) `32.184.22.163` (w63, shard 0),
`52.41.113.67` (w67, shard 1). Env (workers): `OXIDANT_MEMORY_LIMIT_BYTES=40Gi`,
`OXIDANT_SHUFFLE_SPILL_BYTES=8Gi`, `OXIDANT_AQE=1`, S3 cache 21 GiB/worker at
`/var/lib/oxidant/s3cache`, `OXIDANT_PREFER_HASH_JOIN=auto`. Driver: `OXIDANT_DISTRIBUTED_STRICT=1`,
shuffle partitions default 200.

Method: each query run once via `bench/tpcds/run-ec2-connect.py` (warm cache, `--tries 1`),
then worker journals (`Oxidant stage summary:` per task: duration_ms, batches, spill_bytes)
and driver journals (`dispatch stage_id=… exchange=… upstreams=… sql=<first 99 chars>`,
`barrier complete`) aggregated per stage per query attempt. Stage SQL reconstructed from
dispatch prefixes + the planner code (`crates/oxidant-execution/src/plan/gather_shapes.rs`,
`stage_planner.rs`, `dag_splitter.rs`).

## Cross-cutting findings (the short version)

1. **Only `store_sales` is sharded at SF100.** Auto-broadcast classification
   (`resolve_replicated_tables`, `OXIDANT_AUTO_BROADCAST_THRESHOLD_BYTES` default **32 GiB**,
   largest table stays sharded) replicates everything else. Actual S3 sizes:
   `store_sales` 19.3 GB (520 files, sharded), `catalog_sales` **14.5 GB (replicated!)**,
   `web_sales` **7.2 GB (replicated!)**, `store_returns` 2.4 GB, `catalog_returns` 1.5 GB,
   `web_returns` 0.8 GB, all dims ≤ 0.15 GB (replicated). Glue tables carry **no numRows
   stats**, so the row-aware rule (`OXIDANT_REPLICATE_MAX_ROW_MULTIPLE=4.0`) never fires.
   Consequences, observed repeatedly below:
   - Every stage touching `catalog_sales`/`web_sales` scans them **in full, per worker or as
     one single `Forward` task** (Forward = exactly one task, always on the *first* worker w67).
   - Whole join chains (`store_sales ⋈ store_returns ⋈ catalog_sales` in Q25/Q17) collapse
     into a **single producer stage with 2 tasks**, each re-scanning + re-joining the full
     replicated facts — zero join parallelism beyond 2.
2. **Effective scan throughput is catastrophically asymmetric: ~210 MB/s/task warm vs
   ~10–20 MB/s/task cold/thrashed.** The per-worker cache (21 GiB) is smaller than the
   per-worker working set of the plan-bound queries (store shard 9.6 GB + replicated
   catalog 14.5 + web 7.2 + returns 4.7 ≈ **36 GB**), so the LRU thrashes and most big-fact
   scans are effectively cold. This is why the warm full-99 pass barely beat cold (0.94×).
   The cache is not a fix-all: even warm, single-task Forward legs are the critical path.
3. **Spill is a non-issue on these six queries.** Zero shuffle spill on every stage of
   Q14/Q23/Q25/Q17 except one odd 2.9 GB on a trivial Q23 combine stage (stage 5, see below).
   Sort-merge vs hash join is not the current bottleneck.
4. **First-attempt fail-fast + full retry is real.** Q14 (and the Q1 sanity run) failed a
   first attempt within ~2 ms on one stage, cancelled all siblings, and re-ran the whole
   query from scratch (runner reconnect+retry). Root error is not visible at info log level
   (stage summary shows only `status=error duration_ms=2`). Adds up to ~1 full query
   duration when it hits a long query — worth its own ticket; looks like stale task slots /
   membership from the previous query (`worker has no free task slots` class, see
   driver.rs:1514-1516 comments).
5. **Task-slot oversubscription inflates small consumer tasks.** 16 slots/worker, each task's
   DataFusion plan parallelizes across all 16 cores: when 200-600 consumer tasks queue
   behind monster producer/Forward tasks, median consumer task time runs ~9–12 s for
   near-empty inputs (Q14 stages 10/13/16: 200 tasks × ~12 s ≈ 225 s of wall for a
   recombine over ~50k rows). Queue wait is NOT counted in duration_ms (slot acquired
   before the timer starts, flight.rs:1383→1390) — this is genuine CPU oversubscription.

## Per-query stage profiles

(filled below; wall = driver dispatch→barrier; tot = sum of task durations; skew = max/median task)

### Q14 — 1072 s measured (EMR 201 s) — 18 stages

Shape: `try_rollup_union_derived_subqueries` (KAN-49 wave-4, dedicated Q14 shape).
Store arm sharded; catalog + web legs become **Forward single tasks** (replicated-only arms).

| stage | exchange | upstreams | wall_s | tasks | tot_s | med_s | max_s | skew | what it is |
|------:|---------|-----------|-------:|------:|------:|------:|------:|-----:|------------|
| 0 | Hash | [] | 613 | 2 | 1199 | 600 | 612 | 1.0 | INTERSECT store leg: scan `store_sales`⋈item⋈date_dim (d_year 1999–2001), project (brand,class,category) per sale row (~173M rows), hash-shuffle by all 3 cols |
| 1 | **Forward** | [] | **756** | 1 | 756 | 756 | 756 | – | INTERSECT catalog leg: **full `catalog_sales` scan (14.5 GB) + joins in ONE task on w67** (~86M rows out) |
| 2 | Forward | [] | 76 | 1 | 75 | 75 | 75 | – | INTERSECT web leg: full `web_sales` scan, one task |
| 3 | Hash | [0,1,2] | 80 | 200 | 2074 | 9.4 | 30.8 | 3.3 | `shuffle0 INTERSECT shuffle1 INTERSECT shuffle2` (~265M rows of 3-int tuples: distinct + 3-way semi) |
| 4 | Hash | [3] | 1 | 2 | 1.4 | – | – | – | key-set join-back with `item` → item_sk set (small) |
| 5 | Hash | [] | 51 | 2 | 52 | 26 | 51 | 1.9 | avg_sales store leg partial (sum,count over qty·price, 1999–2001) |
| 6 | **Forward** | [] | 224 | 1 | 223 | – | – | – | avg_sales catalog+web legs in ONE task (14.5+7.2 GB scan) |
| 7 | Hash | [5,6] | <1 | 1 | 0.1 | – | – | – | scalar combine → m0 |
| 8 | Hash | [] | 54 | 2 | 60 | 30 | 54 | 1.8 | store arm export (Nov-2001 slice ⋈ item, cols gc0-2, aa0-1, j0=item_sk) |
| 9 | Hash | [8,4] | 89 | 2* | 160 | 80 | 83 | 1.0 | semi (j0 IN cross_items) + partial agg, gathers to partition 0 (*AQE-coalesced read) |
| 10 | Hash | [9,7] | 133 | 200 | 2269 | 12.5 | 22 | 1.8 | recombine + HAVING vs scalar — tiny input, oversubscription-inflated |
| 11 | **Forward** | [] | 221 | 1 | 220 | – | – | – | catalog arm export: full `catalog_sales` Nov-2001 scan, one task |
| 12 | Hash | [11,4] | 76 | 2* | 152 | 76 | 76 | 1.0 | catalog semi + partial |
| 13 | Hash | [12,7] | 82 | 200 | 2236 | 12.5 | 24 | 1.9 | catalog recombine (tiny input, inflated) |
| 14 | Forward | [] | 2 | 1 | 2 | – | – | – | web arm export (cache-warm this time) |
| 15 | Hash | [14,4] | 2 | 2* | 3.2 | – | – | – | web semi + partial |
| 16 | Hash | [15,7] | 74 | 200 | 1945 | 9.1 | 20 | 2.2 | web recombine |
| 17 | (output) | [10,13,16,7] | ~1 | 200 | 2.4 | – | – | – | ROLLUP grouping-set recombine |

Critical path: stage 1 (**756 s**, Forward catalog leg) → 3 (80) → 4 (1) → 9 (89) → 10 (133) → out.
Worker task-time totals: w63 4878 s, w67 6554 s over 1072 s wall × 32 slots = 33 % slot utilization.

**Smoking gun, fully detailed:** Q14's first ~13 minutes are two parallel monsters on
disjoint resources: the `Forward` catalog INTERSECT leg (stage 1, 756 s, one task on w67,
~19 MB/s effective — S3-thrashed single-task scan of 14.5 GB) and the sharded store leg
(stage 0, 612 s, one task per worker, same ~16 MB/s throughput). Everything downstream
(INTERSECT combine, arms, recombines) adds another ~5 min, of which ~225 s is
oversubscription noise on tiny recombine tasks.
Fixes in order of leverage: (a) parallelize the replicated/Forward legs (shard the scan
across workers + 200 partitions; INTERSECT/AVG/arm-partial all dedup or combine
associatively, so sharding a "replicated" arm is semantics-safe for this shape) —
756+224+221 ≈ 1200 task-seconds of single-task work would drop to ~30–60 s of wall;
(b) fix S3 scan throughput / cache thrash (see cross-cutting #2) — stage 0's 612 s would
drop toward Q23-stage-2's demonstrated 46 s warm rate;
(c) dedup the three full fact scans per channel (cross_items leg, avg leg, arm export all
read the same facts with different projections — one scan could feed all three).
Note the runner-level first-attempt failure added ~1 s only here, but the same pattern cost
a full rerun on the Q1 sanity.

### Q23 — 702 s measured (EMR 135 s) — 11 stages

Shape: `try_union_over_derived_ctes` (KAN-49 wave-3f). store_sales-derived CTEs plan once;
the two channel arms (catalog/web replicated) become **Forward** stages.

| stage | exchange | upstreams | wall_s | tasks | tot_s | med_s | max_s | what |
|------:|---------|-----------|-------:|------:|------:|------:|------:|------|
| 0 | Hash | [] | **503** | 2 | 560 | 280 | **503/57** | `frequent_ss_items`: scan store_sales⋈date_dim⋈item, GROUP BY (itemdesc,item_sk,d_date) — partial agg emits ~53M groups/worker (HAVING count>4 can't filter pre-combine). w63 503 s vs w67 57 s (see note) |
| 1 | Hash | [0] | 26 | 200 | 682 | 3.4 | 7.1 | combine + HAVING |
| 2 | Hash | [] | 46 | 2 | 50 | 25 | 46 | `max_store_sales` per-customer agg (scan store_sales) — **46 s warm-scan rate** |
| 3 | Hash | [2] | 1 | 200 | 24 | 0.11 | – | combine per-customer |
| 4 | Hash | [3] | 1 | 200 | 19 | – | – | max(csales) partial |
| 5 | Hash | [4] | 1 | 200 | 22 | 0.11 | – | max combine — **spilled 2.9 GB for a one-row aggregate** (odd; cheap in time) |
| 6 | Hash | [] | 46 | 2 | 50 | 25 | 46 | `best_ss_customer` per-customer agg — **same store_sales scan+agg as stage 2, computed twice** (no CSE across the HAVING boundary) |
| 7 | Hash | [6] | 1 | 200 | 22 | 0.11 | – | combine + HAVING > 0.5·max |
| 8 | **Forward** | [1,7] | **170** | 1 | 169 | – | – | catalog arm: full `catalog_sales` ⋈ customer ⋈ date_dim ⋈ frequent_ss_items ⋈ best_ss_customer, GROUP BY name — ONE task |
| 9 | **Forward** | [1,7] | 110 | 1 | 109 | – | – | web arm: same shape over full `web_sales` — ONE task |
| 10 | (output) | | | 1 | | | | union + final |

Critical path: stage 0 (503) → 1 (26) → 8 (170) ≈ 700 s.
**Stage-0 asymmetry**: w67 finished in 57 s with `batches=0` while w63 ran 503 s emitting
6662 batches (~53M partial-agg rows). Shard indices are correct (env verified). Not
explained by cache warmth alone (both scanned the same table minutes apart in Q14); the
~100M-group partial hash-agg on (itemdesc,item_sk,d_date) is the prime suspect for the
503 s (hash table of tens of millions of groups under the 40 GiB pool), but the w67
57 s/0-batch task implies the two tasks did NOT do symmetric work — could not root-cause
from available telemetry (no per-operator timings; needs a repro with metrics).
**Dup work**: stages 2 and 6 rescan store_sales for structurally identical per-customer
aggregates (46 s × 2).

### Q25 — 381 s measured (EMR 108 s) — 2 stages

| stage | exchange | upstreams | wall_s | tasks | what |
|------:|---------|-----------|-------:|------:|------|
| 0 | Hash | [] | **381** | 2 | **The entire query**: `store_sales ⋈ store_returns ⋈ catalog_sales ⋈ date_dim×3 ⋈ store ⋈ item` partial GROUP BY (i_item_id, i_item_desc, s_store_id, s_store_name), hash-shuffled. Each task: ½ store_sales (9.6 GB) ⋈ **FULL store_returns (2.4 GB)** ⋈ **FULL catalog_sales (14.5 GB)**. w67 299 s, w63 381 s |
| 1 | Hash | [0] | <1 | 2 | final combine, 14 rows |

The ss⋈sr⋈cs 3-fact chain does not distribute at all: with only store_sales sharded, the
"one sharded table → broadcast everything else" path runs the whole chain **twice** (once
per worker shard task), each re-scanning 17 GB of replicated facts and hash-joining a
~144M-row catalog_sales build side per task. EMR runs this as shuffle joins with dynamic
filters. This is the purest demonstration of cross-cutting finding #1.

### Q17 — 410 s measured (EMR 97 s) — 2 stages

Identical pattern to Q25 (same join chain, `s_state` + stddev aggs): stage 0 = 2 tasks
(w63 409.5 s, w67 409.2 s), stage 1 combine <1 s. Same root cause.

### Q10 — 227 s measured (EMR 37 s) — 3 stages

Shape: KAN-55 semi/anti subqueries. The `store_sales` EXISTS leg is extracted once as a
co-located key shuffle (stage 0); the `web_sales`/`catalog_sales` EXISTS legs stay **inline
per partition task** because those tables are replicated.

| stage | exchange | upstreams | wall_s | tasks | tot_s | med_s | max_s | skew | what |
|------:|---------|-----------|-------:|------:|------:|------:|------:|-----:|------|
| 0 | Hash | [] | 2 | 2 | 2 | ~1 | 1.1 | – | distinct `ss_customer_sk` from store_sales⋈date_dim (2002, moy 1–4), hash by k0. **1 s/worker**: the worker-local hash join's build side (90 date rows) publishes a KAN-2 R2 dynamic filter (`enable_join_dynamic_filter_pushdown`, oxidant-loom/src/lib.rs:3285-3300) into the store_sales scan → row-group pruning on date_sk-contiguous parquet skips ~95 % of the table. Proof that runtime filters pay off enormously **where they fire** — they currently only fire inside a single task, not across stage boundaries (the KAN-150 gap) |
| 1 | Hash | [0] | **224** | 202 | **6855** | 8.7 | 171.3 | **19.7** | main agg: `customer ⋈ customer_address ⋈ customer_demographics` (5-county filter) ⋈ semi(store keys from shuffle) ⋈ **`EXISTS(web_sales Q1-2002) OR EXISTS(catalog_sales Q1-2002)` rebuilt inside every one of the 200 partition tasks** — each task re-scans and re-hash-builds the full replicated web+catalog legs (21.7 GB × 200 ≈ 4.3 TB of task-local scans) |
| 2 | (output) | [1] | <1 | 1 | | | | | combine, 2374 rows |

Skew 19.7 on stage 1 = cache thrash across tasks (some tasks warm ~9 s, cold ones 171 s).
Fix: materialize the web/catalog semi key sets ONCE (Forward or sharded distinct-key
stages, same pattern KAN-55 already uses for the sharded store leg) instead of per-task
full rebuilds. Stage 0 demonstrates the engine *can* do this cheaply.

### Q78 — 347 s measured (EMR 136 s) — 5 stages

Shape: `dag_splitter` branch DAG with keyed outer (ss LEFT JOIN ws LEFT JOIN cs on
(year,item,customer)). ss arm sharded; ws/cs arms are replicated-only → **Forward**.

| stage | exchange | upstreams | wall_s | tasks | tot_s | med_s | max_s | what |
|------:|---------|-----------|-------:|------:|------:|------:|------:|------|
| 0 | Hash | [] | **313** | 2 | 581 | 291 | 313 | ss arm partial: `store_sales LEFT JOIN store_returns … IS NULL` ⋈ date_dim, GROUP BY (d_year, ss_item_sk, ss_customer_sk) — 288M rows → tens of millions of groups per task; store_returns (2.4 GB) fully scanned per task |
| 1 | Hash | [0] | 27 | 200 | 713 | 3.3 | 6.8 | ss arm combine |
| 2 | Forward | [] | 41 | 1 | 41 | – | – | ws arm: full `web_sales` LEFT ANTI web_returns + GROUP BY (year,item,customer), ONE task |
| 3 | Forward | [] | **292** | 1 | 292 | – | – | cs arm: full `catalog_sales` (14.5 GB) LEFT ANTI catalog_returns + GROUP BY, ONE task on w67 |
| 4 | (output) | [1,2,3] | ~7 | 200 | 100 | 0.5 | 0.7 | keyed 3-way outer join of arm outputs + ratio projection |

Critical path: stage 0 (313) → 1 (27) → 4 (7) ≈ 347 s; the Forward cs arm (292 s) overlaps
stage 0 and is nearly co-critical. Both are the same species of monster: giant GROUP BYs
over full fact scans concentrated in 1–2 tasks. Worker task-time: w63 638 s vs w67 1089 s
(Forward always lands on w67 → systematic imbalance).

## Ranked top-5 operator/plan fixes (expected impact from measured critical paths)

1. **Stop replicating mid-size facts — or make "replicated" scans parallel.**
   `OXIDANT_AUTO_BROADCAST_THRESHOLD_BYTES=32 GiB` replicates catalog_sales (14.5 GB) and
   web_sales (7.2 GB) at SF100; every specialized shape then either (a) emits **Forward
   single-task stages** over them (Q14 stages 1/6/11 ≈ 1200 s of one-task work; Q23 stages
   8/9 = 279 s; Q78 stage 3 = 292 s), or (b) **re-scans them inside every shard task**
   (Q25/Q17 stage 0: each of 2 tasks re-reads 17 GB; Q10 stage 1: 200 tasks re-read 21.7 GB
   each). Two attack angles, complementary:
   - *Planner:* teach the Forward/"replicated arm" paths to fan the scan out (200-partition
     producers; INTERSECT dedups, AVG sum/count combines, arm GROUP BY partials all merge
     associatively, so sharding a replicated arm is semantics-safe for these shapes).
     Estimated: Q14 1072→~450 s, Q23 702→~430 s, Q78 347→~200 s. Also kills the
     everything-on-w67 imbalance (Forward always dispatches to worker[0]).
   - *Classification:* shard catalog_sales/web_sales outright (lower the threshold to ~4 GiB
     and/or populate Glue `numRows` so the row-aware rule fires — Glue currently has **no
     row stats**, so `OXIDANT_REPLICATE_MAX_ROW_MULTIPLE` is dead). This unlocks real
     shuffle-join chains for Q25/Q17 (the whole-chain-in-one-stage pattern → EMR-style
     distributed joins; est. 380–410 s → ~100–150 s, EMR 97–108 s). **Coverage caveat:**
     Q14's shape requires *exactly one* sharded table and Q23's expects replicated channel
     facts — reclassifying without extending those shapes will bounce Q14/Q23 off strict
     mode. The planner-side fix is the safer first step.
2. **Fix cold-scan throughput (~10–20 MB/s/task) and cache thrash.** Warm scans hit
   ~210 MB/s/task (Q23 stages 2/6 = 46 s for a full store_sales scan; Q10 stage 0 pruned to
   1 s); cold/thrashed scans crawl (Q14 stage 0 612 s, Q23 stage 0 503 s, Q17 409 s).
   Per-worker cache (21 GiB) < per-worker working set (~36 GB: 9.6 store shard + 14.5
   catalog + 7.2 web + 4.7 returns + dims) → LRU thrash every query. Options: bigger cache,
   read-ahead/prefetch, more range-read concurrency in the S3 object-store path. Could not
   decompose S3-vs-decode from available telemetry — measure before sizing the fix.
   Expected: 2–5× on every scan-bound stage of the plan-bound queries; this is also why the
   warm full-99 pass only bought 0.94×.
3. **Materialize replicated semi/EXISTS legs once per query, not once per task** (Q10 stage
   1: 6855 task-seconds, wall 224 s → est. ~60–80 s, EMR 37 s). The machinery already
   exists for sharded legs (KAN-55 co-located key shuffle, and Q14's Forward key-set
   stages); apply it to replicated-fact EXISTS/IN legs instead of inlining the full table
   scan + hash build into every partition task. KAN-150's dynamic filters will further cut
   the build side, but the per-task rebuild is the structural cost.
4. **CSE repeated scans/aggregates of the same fact within one query.** Q14 scans each
   channel fact **3×** (INTERSECT leg + AVG leg + arm export = 9 full fact scans/query);
   Q23 computes the same per-customer store_sales aggregate twice (stages 2≡6, 46 s × 2).
   Identical-stage CSE (stage_planner.rs:423) only merges byte-identical stages; extend the
   branch-fingerprint merge (dag_splitter) so one leaf scan feeds multiple downstream
   consumers, or emit multi-consumer partials. Q14 alone: ~3× scan-volume reduction
   (compounds with fixes 1–2).
5. **Coalesce/shrink the 200-wide consumer fan-out for small inputs.** Q14 stages 10/13/16:
   600 recombine tasks × ~12 s median (16 slots × 16 DataFusion threads oversubscription;
   queue time is excluded from duration_ms, so this is real CPU contention) ≈ 225 s of wall
   for recombines over ~50k rows. AQE already coalesces some reads (stages 9/12/15 ran as
   2 tasks) but the gathered recombine stages stayed 200-wide. Tighten the coalesce
   criterion to gathered/small-input stages. Expected: ~3–4 min off Q14, smaller wins on
   Q23/Q78 combines.

Honorable mentions (not top-5 but ticket-worthy):
- **First-attempt fail-fast + full-query retry**: Q14's attempt 0 died in 2 ms on stage 2
  (and the Q1 sanity query failed once identically); root error invisible at info level.
  Looks like the previous query's task-slot/membership residue (driver.rs:1514 comment
  names the `worker has no free task slots` class). When it hits a long query it doubles it.
- **Q23 stage-0 asymmetry** (w63 503 s/6662 batches vs w67 57 s/0 batches, same table, same
  stage, correct shard env): unexplained; the ~100M-group partial hash agg on
  (itemdesc,item_sk,d_date) is the prime suspect. Pre-aggregating (item_sk, d_date) and
  joining `itemdesc` after the HAVING would slash group cardinality regardless.
- **Q23 stage 5 spilled 2.9 GB** computing a one-row `max` combine — harmless here (1 s
  wall) but indicates the shuffle-write threshold mis-sizing on tiny stages.
- Glue `tpcds_sf100` facts point at `s3://…/tpcds-sf100/` while dims point at
  `…/tpcds-sf100-typed/` — cosmetic, but the absence of Glue column/row statistics on all
  tables also starves any future cost-based classification.

## What I could NOT measure

- Per-operator (join vs agg vs scan vs shuffle-write) time inside a task: the only worker
  telemetry is per-task duration/batches/shuffle-spill. No rows-per-stage, no scan bytes,
  no S3 GET counts, no CPU/IO histograms. The ~16–20 MB/s cold-scan throughput could be
  S3 range-read latency, parquet decode, or join build — undecidable from these journals.
- The Q14/Q1 first-attempt failure root cause (error text not logged at info level).
- Q23 stage-0 w67 57 s/0-batch anomaly (see Q23 note).
- Full stage SQL beyond the 99-char driver log prefix (dispatch log truncates;
  `OXIDANT_TPCDS_DEBUG=1` requires a driver restart I was not allowed to do).
- Correctness cross-check of results vs EMR (not requested; runner reported rows=100/LIMIT).

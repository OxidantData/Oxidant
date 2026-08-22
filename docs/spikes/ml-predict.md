# SPIKE: ONNX batch inference in the Oxidant engine

**Issue:** [OxidantData/Oxidant#118](https://github.com/OxidantData/Oxidant/issues/118)
**Branch:** `ml-predict-spike` (throwaway — never merged, never pushed)
**Date:** 2026-08-22
**Verdict:** **Ship the mechanism, do not ship tract's tree support.** See
[Recommendation](#recommendation).

---

## TL;DR

* A pure-Rust ONNX UDF works. `ml_predict('<uri>', f1, …, fk)` scores a real sklearn
  `GradientBoostingClassifier` and a real torch MLP inside `oxidant sql`, agreeing with
  onnxruntime to within one `f32` ULP (max absolute difference 6.0e-8) with identical labels.
* **Batching is the whole game for neural nets: 44x** (5.79M vs 132k rows/sec pure inference;
  25x end-to-end in SQL over 1M rows).
* **Batching does nothing for tract's tree ensembles (1.03x over 1M rows)** — because tract's
  `TreeEnsembleClassifier` is a scalar per-row loop whose leaf lookup is **quadratic in
  ensemble size**. A 100-tree GBDT scores at ~4,000 rows/sec; a 200-tree one at ~1,000.
* tract has **two correctness bugs and one hard load failure** on the standard skl2onnx binary
  classifier export. One of them — a silently inverted class label — would have shipped wrong
  answers. All three are worked around in `crates/oxidant-ml/src/compat.rs`.

---

## 1. What was built

| Path | What |
|---|---|
| `crates/oxidant-ml/` | ONNX load + compile + score (tract), compat rewrites, model cache, blob source trait |
| `crates/oxidant-ml/src/compat.rs` | The tract-compat ONNX graph rewrites — **read this one** |
| `crates/oxidant-loom/src/spark_functions/ml_predict.rs` | The three scalar UDFs |
| `crates/oxidant-loom/src/ml_blob_source.rs` | `s3://` model URIs via the engine's own object store |
| `crates/oxidant-ml/src/bin/ml-probe.rs` | tract op-coverage probe / ORT cross-check |
| `crates/oxidant-ml/src/bin/ml-bench.rs` | Pure-inference benchmark: row vs batch |
| `crates/oxidant-ml/src/bin/ml-lifecycle.rs` | Load time, cache behaviour, memory footprint |

Three UDFs are registered, because the spike had to *measure* two strategies:

| function | strategy | returns |
|---|---|---|
| `ml_predict(uri, f…)` | one tensor per RecordBatch | `DOUBLE` — positive-class probability, or the model's single output |
| `ml_predict_class(uri, f…)` | one tensor per RecordBatch | `BIGINT` — class label; `NULL` for models with no label output |
| `ml_predict_rowwise(uri, f…)` | one tensor **per row** | `DOUBLE` — identical values, benchmark control arm only |

`ml_predict_rowwise` is not an API proposal. It exists so the row-vs-batch number is a
measurement of two strategies for the same computation rather than an estimate; the probe
asserts the two arms agree before reporting a speedup.

---

## 2. Real output in `oxidant sql`

Both models are trained on the **real** UCI `winequality-red.csv` (1,599 rows, 11 features,
binary target `quality >= 6`) — the Databricks-tutorial shape the issue asked for.

### sklearn `GradientBoostingClassifier` (100 estimators, depth 3, train acc 0.8630)

```
$ oxidant sql -f wine.sql
+---------+------------------+----------------------+------------+
| alcohol | volatile_acidity | gbdt_p               | gbdt_class |
+---------+------------------+----------------------+------------+
| 9.4     | 0.7              | 0.15888100862503052  | 0          |
| 9.8     | 0.88             | 0.15947037935256958  | 0          |
| 9.8     | 0.76             | 0.3290609121322632   | 0          |
| 9.8     | 0.28             | 0.6259010434150696   | 1          |
| 9.4     | 0.7              | 0.15888100862503052  | 0          |
| 9.4     | 0.66             | 0.12256816029548644  | 0          |
| 9.4     | 0.6              | 0.052787333726882935 | 0          |
| 10.0    | 0.65             | 0.46756044030189514  | 0          |
+---------+------------------+----------------------+------------+
```

onnxruntime on the same 8 rows: `p1 = [0.158881, 0.159470, 0.329061, 0.625901, 0.158881,
0.122568, 0.052787, 0.467560]`, `label = [0, 0, 0, 1, 0, 0, 0, 0]`. **Labels identical;
probabilities agree to within 6.0e-8 absolute on every row** — that is 2⁻²⁴, one ULP of an
`f32` near 1.0, i.e. last-bit rounding from a different accumulation order. Two of the eight
rows are bit-identical, the rest differ in the final mantissa bit.

### torch MLP (11→32→16→1, ReLU/Sigmoid, train acc 0.9325)

```
$ oxidant sql -f wine_mlp.sql
+---------+----------------------+-----------+
| alcohol | mlp_score            | mlp_class |
+---------+----------------------+-----------+
| 9.4     | 0.001953303813934326 | null      |
| 9.8     | 0.006038486957550049 | null      |
| 9.8     | 0.3209840655326843   | null      |
| 9.8     | 0.8526582717895508   | null      |
| 9.4     | 0.001953303813934326 | null      |
| 9.4     | 0.006515681743621826 | null      |
| 9.4     | 0.02300018072128296  | null      |
| 10.0    | 0.928717851638794    | null      |
+---------+----------------------+-----------+
```

onnxruntime: `[0.001953303813934326, 0.006038546562194824, 0.3209840655326843,
0.8526582717895508, 0.001953303813934326, 0.006515681743621826, 0.023000210523605347,
0.928717851638794]`. **Six of eight rows are bit-identical**; rows 2 and 7 differ by 6.0e-8
and 3.0e-8 — the same one-ULP `f32` rounding as above. `mlp_class` is `NULL` by
design: the graph has no label output, and returning NULL rather than erroring lets one query
select both columns across model kinds.

---

## 3. tract feasibility — the ops and dtype gaps

This was the #1 risk in the brief, and it is where the spike's value is. Every finding below
was verified by reading tract's source, not inferred from an error message.

### 3.1 MSRV: tract 0.23.x needs rustc 1.91; the workspace pins 1.90

Every published `tract-onnx` in the 0.23 line (0.23.0 … 0.23.5) declares `rust-version = 1.91`.
`rust-toolchain.toml` pins **1.90**. Pinned `tract-onnx = "=0.22.0"`, which additionally
required walking `kstring` back from 2.0.4 → 2.0.2 (2.0.4 wants 1.96).

**This costs nothing today.** I reproduced every finding below against **0.23.5** on rustc
1.96.1 in a scratch crate outside the workspace: identical behaviour, identical error strings.
Bumping the toolchain buys no tract fixes.

### 3.2 `ZipMap` is unimplemented — the *default* skl2onnx export is unloadable

```
tract optimize: Translating node #3 "ZipMap" Unimplemented(ZipMap) ToTypedTranslator
```

skl2onnx wraps classifier probabilities in `ZipMap`, producing
`seq(map(int64, tensor(float)))`. tract has no sequence/map support. Models **must** be
exported with `options={id(clf): {'zipmap': False}}`, which yields a plain `[n, n_classes]`
float tensor. This is a documentation/tooling constraint on whoever exports the model, not
something the engine can rewrite around cheaply.

### 3.3 `base_values` arity — a hard load failure on every binary sklearn GBDT

```
Node TreeEnsembleClassifier, attribute 'base_values': expected length 1 (or undefined), got 2
```

The message reads backwards ("expected {actual}, got {wanted}") and cost real time. The model
has **one** base value; tract wants `n_classes` = 2.

skl2onnx is right: a binary `GradientBoostingClassifier` is a *single* series of trees plus one
initial raw score, so `base_values = [0.139]` alongside `classlabels_int64s = [0, 1]`. tract
parses it with `get_vec_attr_opt::<f32>(node, "base_values", ensemble.n_classes())`, which
hard-requires `len == n_classes`.

**Worked around** in `compat::pad_base_values` by padding the attribute to length 2 at the
protobuf level, before tract's parser sees it. This is semantics-preserving because tract
already implements the binary layout correctly further down: it detects `binary_result_layout`
(≤2 class labels, every leaf's `class_id == 0`), broadcasts `base_values` into rank 2, adds,
applies the `LOGISTIC` post-transform, then **slices column 0** and emits `[1 - p, p]`. Only
column 0 is ever read, so the padded slot is discarded.

**Upstream fix:** compute `binary_result_layout` *before* parsing `base_values` and accept
length 1 in that case. A few lines.

### 3.4 Binary class labels are silently WRONG — the dangerous one

After fixing 3.3, probabilities were exact but **every label was inverted**:

| | rows 1–8 |
|---|---|
| onnxruntime | `[0, 0, 0, 1, 0, 0, 0, 0]` |
| tract (raw) | `[1, 1, 1, 0, 1, 1, 1, 1]` |

tract's `wire()` computes the argmax over `processed_scores` — the **pre-slice** `[n, 2]`
tensor, whose column 1 carries only `base_values` with no tree contribution — instead of over
the `[1 - p, p]` it actually returns:

```rust
let processed_scores = scores.clone();      // <-- captured BEFORE the binary fix-up
if self.binary_result_layout { /* slice col 0, complement, concat */ }
let winners = model.wire_node(…, Reducer::ArgMax(false), &processed_scores)?;  // <-- wrong input
```

So the label is `argmax([p, sigmoid(base)])`, and flips whenever `p < sigmoid(base)`. With
`base = 0.139`, `sigmoid(base) ≈ 0.535`, which is why all 8 rows were wrong. No error, no
warning — just wrong answers. This is exactly the failure class that costs credibility.

**Worked around** by dropping tract's label output for binary tree ensembles entirely and
taking the argmax over the probabilities ourselves (`OnnxModel::read_outputs`). Multi-class
graphs take `binary_result_layout = false`, where the argmax is over the correct tensor, so
they keep tract's label.

### 3.5 The tree evaluator is quadratic in ensemble size — the blocker

`TreeEnsembleData::eval_unchecked` resolves a tree's leaf contributions with:

```rust
self.leaves.to_array_view_unchecked::<u32>().outer_iter()
    .skip(leaf.start_id).take(leaf.end_id - leaf.start_id)
```

`Iterator::skip` on ndarray's `AxisIter` walks element by element, so **each leaf lookup is
O(total leaves in the entire ensemble)**, performed once per tree per row. Total work is
therefore O(n_trees × total_leaves × rows) ≈ O(n_trees² × rows).

Measured (release-ci, single-threaded, 5,000 rows, batch 8192, Apple M4 Pro):

| n_estimators | rows/sec (batched) | slower than n=10 |
|---|---|---|
| 10 | 382,801 | 1.0x |
| 25 | 92,855 | 4.1x |
| 50 | 17,538 | 21.8x |
| 100 | 3,952 | 96.9x |
| 200 | 1,064 | **359.8x** |

20x the trees → 360x slower. Clean quadratic. A realistic production GBDT (500–1000 trees,
depth 6) would be another 1–2 orders of magnitude worse — call it tens of rows/sec.

This is not a workaround-able bug at the graph level; it is tract's data layout and inner
loop. Fixing it upstream means giving `TreeEnsembleData` an offset index (or a flat
`[start, end)` slice instead of `skip`) — a genuine but self-contained upstream contribution.

### 3.6 dtype: the "11 doubles" shape does not survive export

The issue asked for the Databricks tutorial's 11-`double` feature vector. Exporting that
literally with skl2onnx `DoubleTensorType` produces a graph **onnxruntime itself refuses to
load**, because `ai.onnx.ml`'s `TreeEnsembleClassifier` outputs are float by spec:

```
Type Error: Type (tensor(double)) of output arg (probabilities) of node (N1)
does not match expected type (tensor(float))
```

(and with ZipMap on, the same error against `seq(map(int64, tensor(double)))`).

Curiously **tract accepts it** — `TreeEnsembleClassifier::eval` does `input.cast_to::<f32>()`
unconditionally — and returns numbers identical to the float export. So tract is *more*
permissive than ORT here, but we should not rely on a graph the reference runtime rejects.

**Consequence for the API:** features are f32 at the ONNX boundary regardless of what the
column type is. `ml_predict` coerces every feature argument to `Float64` in SQL (so `INT`,
`DECIMAL`, and `FLOAT` columns all work without the caller writing casts) and narrows to f32
when filling the tensor. That narrowing is lossy and silent — see [Risks](#risks).

### 3.7 What worked with no gymnastics at all

The torch MLP needed **zero** rewrites. 13 nodes, all standard ops
(`Gemm`, `Clip`, `Sigmoid`, `Const`), `into_optimized().into_runnable()` first try, dynamic
batch axis honoured. tract's neural-net coverage is not the problem; its `ai.onnx.ml`
coverage is.

---

## 4. Benchmarks

**Machine:** Apple M4 Pro, 12 cores, 24 GiB RAM, macOS (Darwin 25.6.0).
**Build:** `--profile release-ci` (opt-level 3, no LTO, 16 codegen units), rustc 1.90.
**Data:** 1,000,000 rows resampled with 5% jitter from the real 1,599-row UCI wine set, so
tree paths are realistically distributed. Single parquet file, 90.6 MB.

### 4.1 Pure inference (no SQL engine) — `ml-bench`

Single-threaded, 100,000 rows, batch vs one tract call per row.

**torch MLP:**

| strategy | time | rows/sec | vs row-wise |
|---|---|---|---|
| row-wise | 759.863 ms | 131,603 | 1.0x |
| batch = 512 | 17.701 ms | 5,649,372 | 42.9x |
| **batch = 1024** | **17.286 ms** | **5,785,015** | **44.0x** |
| batch = 4096 | 19.850 ms | 5,037,741 | 38.3x |
| batch = 8192 | 20.986 ms | 4,765,119 | 36.2x |
| batch = 32768 | 21.166 ms | 4,724,465 | 35.9x |

Batching wins by **44x**, and the optimum is ~1024 rows — large batches lose a little to cache
pressure. DataFusion's default 8192-row batch sits within 20% of optimal, so no tuning is
needed.

**sklearn GBDT (100 trees), 20,000 rows:**

| strategy | time | rows/sec | vs row-wise |
|---|---|---|---|
| row-wise | 5.341 s | 3,745 | 1.0x |
| batch = 1024 | 5.159 s | 3,876 | 1.0x |
| batch = 8192 | 5.002 s | 3,999 | 1.1x |

**Batching does not help.** This is the "if it doesn't, find out why" case from the brief, and
the answer is §3.5: tract's tree op is a scalar `for i in 0..n` loop over rows with no
vectorization, so the only thing batching saves is per-call plan-invocation overhead — which is
noise next to the quadratic leaf scan.

### 4.2 End-to-end in `oxidant sql` — 1,000,000 rows

Best-of-N wall clock of the whole `oxidant sql` invocation, `--format csv`.

| query | best | rows/sec | batch speedup |
|---|---|---|---|
| baseline (scan + decode all 11 columns) | 0.102 s | 9,802,891 | — |
| `ml_predict` MLP | **0.271 s** | **3,689,684** | **25.2x** |
| `ml_predict_rowwise` MLP | 6.834 s | 146,331 | |
| `ml_predict` GBDT (10 trees) | 3.915 s | 255,452 | 2.0x |
| `ml_predict_rowwise` GBDT (10 trees) | 7.947 s | 125,836 | |
| `ml_predict` GBDT (100 trees) | 128.743 s | 7,767 | 1.03x |
| `ml_predict_rowwise` GBDT (100 trees) | 132.178 s | 7,566 | |

All arms returned identical checksums to their counterpart, so the speedups compare equal work.

Three things to read carefully:

* **The 100-tree GBDT arms were re-measured on an idle machine, interleaved.** A first pass
  had `ml_predict` at 207.7 s and `ml_predict_rowwise` at 133.0 s — batching apparently making
  things *slower*, which contradicts both the micro-benchmark and every mechanism I could find
  in tract's code. It was contention: that pass ran while a `release-ci` compile and a
  `cargo clippy` were saturating all 12 cores (15-minute load average 28). Re-run interleaved
  on a quiet box — `batch, rowwise, batch, rowwise` — the two arms land at 128.7 s / 132.2 s,
  i.e. **1.03x**, matching the 1.0–1.1x measured standalone. The lesson is banked, not just
  the number: on this hardware a benchmark sharing the box with a build is worth nothing.

* **These are close to per-core numbers, despite a 12-way plan.** `EXPLAIN` shows
  `DataSourceExec: file_groups={12 groups: …}` — DataFusion splits the file into 12 byte
  ranges. But pyarrow wrote the fixture as a **single row group**, so only one of those ranges
  contains a row-group start and one partition does all 1M rows. That is why the SQL numbers
  line up with the single-threaded micro-benchmark (MLP: 0.271 s − 0.102 s baseline = 0.169 s
  of inference ⇒ 5.92M rows/sec, against 5.79M measured standalone). The GBDT arm runs about
  1.9x its single-threaded micro-benchmark, so a little parallelism does survive. A properly
  row-grouped table multiplies these by the partition count; the *ratios* are what this table
  is for.
* **The row-wise arm converges to ~130–150k rows/sec regardless of model.** The MLP (146k) and
  the 10-tree GBDT (126k) land in the same band despite a 3x difference in inference cost,
  because per-row scoring is dominated by ~7 µs/row of fixed per-call overhead (plan
  invocation, input tensor allocation, output copy). **That is the ceiling on any row-at-a-time
  UDF design**, and it is the single strongest argument for the batched shape.

### 4.3 Model lifecycle — `ml-lifecycle`

| | cold load | hot lookup |
|---|---|---|
| GBDT (53 KB) | 2.77 ms | 3.6 µs |
| MLP (4.4 KB) | 1.90 ms | 4.0 µs |
| GBDT-200 (107 KB) | 1.12 ms | 4.3 µs |

Cold load is fetch + protobuf parse + compat rewrite + `into_optimized()` + `into_runnable()`.
The cache turns that into a ~4 µs hash lookup — a **~700x** difference, and without it every
RecordBatch would re-compile the model. The cache is not an optimization, it is a requirement.

**Memory footprint.** Marginal RSS per cached model, after the allocator warms up on the first
load (which costs ~4 MiB of arena regardless of model):

| ONNX bytes | marginal RSS |
|---|---|
| 14,293 | +0.36 MiB |
| 27,534 | +0.38 MiB |
| 53,428 | +0.72 MiB |
| 107,248 | +0.91 MiB |

Roughly **8–10x the serialized size**, so ~1 MiB resident for a 100 KB model. Small enough
that the cache needs no eviction policy for tens of models — but there **is** no eviction
policy today, which is a gap (see Risks).

**Cache design.** Keyed on `uri` + a version token (S3 ETag, or local `mtime:size`), so
republishing a model to the same URI is picked up without a restart. Re-probing that version
is an S3 HEAD, which is far too expensive per batch, so a successful probe is trusted for
`OXIDANT_ML_MODEL_TTL_MS` (default 60 s). Fetch and compile happen *outside* the lock, so a
cold GET of a large model does not stall every other model's lookups on that executor.

### 4.4 Loading from `s3://`

`crates/oxidant-loom/src/ml_blob_source.rs` routes `s3://` model URIs through
`catalog_bridge::ensure_remote_store` — **the same function the table read and write paths
use**. A model in a bucket therefore resolves with the same region resolution, the same
default/assumed-role credential chain, and the same `s3_io` instrumentation and
concurrent-range wrapper a `SELECT` from that bucket would get, rather than a second,
divergent S3 client.

One implementation note worth carrying forward: `ScalarUDFImpl::invoke_with_args` runs on a
tokio worker thread, where both creating a runtime and `Handle::block_on` panic. The blob
source hands the object-store future to a dedicated runtime and drives it from a *fresh*
thread. That is only reached on a cache miss, so the thread spawn is amortized across a whole
query at worst.

**Verification status — read this before quoting the S3 path as working.** The local-path
lifecycle above is measured. The `s3://` path is implemented and compiles, and it reuses a
function that is already exercised by the engine's own MinIO integration test
(`crates/oxidant-loom/tests/minio_lakehouse.rs`), but **I did not exercise it end-to-end in
this session** — the MinIO image pull did not complete on this machine. Treat "loads a model
from S3" as designed-and-plumbed, not proven. Standing up MinIO and adding an
`OXIDANT_MINIO_TEST=1`-gated test alongside the existing one is under an hour of work and
should happen before anyone relies on the claim.

---

## 5. API recommendation

The spike implemented the **scalar UDF** shape the issue proposed:

```sql
ml_predict('s3://bucket/model.onnx', col1, col2, …, colk)
```

**It works, and I would not ship it as the only shape.** Four problems, in the order they
will bite:

1. **The feature list is positional and unchecked.** Nothing ties `col1, …, col11` to the
   order the model was trained on. Swap two columns and you get plausible numbers that are
   silently wrong — the same failure class as §3.4, but caused by us. The engine can only
   check the *count* (it does), never the *order*.
2. **The URI is a string literal repeated in every query.** It is the cache key, so it must be
   constant; that is fine, but it means the model's location, and therefore its version, is
   copy-pasted across every query that scores it. There is no way to atomically move a whole
   estate to a new model.
3. **One scalar output per call.** Getting probability *and* label means two UDF calls, which
   means scoring the batch twice unless DataFusion CSEs them (it does not, across different
   function names). That is a 2x waste that the UDF shape makes hard to avoid.
4. **No place to hang metadata.** Feature names, the normalization the torch MLP needs
   (currently written by hand as 11 z-score expressions in SQL — see `wine_mlp.sql`), the
   class labels, the expected dtype: all of it lives outside the query, in a human's head.

**Recommended shape: DDL-registered model + a struct-returning UDF.**

```sql
CREATE MODEL wine_quality
  USING ONNX
  LOCATION 's3://bucket/models/wine/v7.onnx'
  FEATURES (fixed_acidity, volatile_acidity, …, alcohol);

SELECT ml_predict(wine_quality, *).probability AS p,
       ml_predict(wine_quality, *).label       AS class
FROM   wine;
```

This fixes all four: the catalog binds feature *names* to positions (1), gives one place to
repoint a version (2), returns a struct so one inference serves every output (3), and is the
natural home for normalization and metadata (4). It also matches how the rest of the engine
already works — models become catalog objects like tables, so `SHOW MODELS` / `DESCRIBE MODEL`
and the existing catalog SPI carry them.

Keep the URI-literal UDF as the **escape hatch** for ad-hoc scoring and for tests. It is ~200
lines and already written; it costs nothing to keep and it is what makes the DDL form easy to
implement (the DDL path resolves to exactly the same `oxidant_ml::cache::get(uri)`).

---

## Recommendation

**Ship the mechanism. Do not ship tree-model support on tract.**

| | verdict |
|---|---|
| Pure-Rust ONNX inference in the engine | **Ship.** No C++ toolchain, no `libonnxruntime` to package, ~1 MiB per cached model, 5.8M rows/sec on a neural net, single core. |
| Batched (per-RecordBatch) scoring | **Ship.** 44x over per-row, and the per-row ceiling (~150k rows/sec) is a property of the design, not of the model. |
| Neural-net models (torch export) | **Ship.** Zero rewrites needed, exact agreement with onnxruntime. |
| **Tree ensembles (sklearn / XGBoost / LightGBM) on tract** | **Do not ship** until §3.5 is fixed upstream. 4,000 rows/sec on a *toy* 100-tree model, quadratic in tree count, and the label path is wrong by default. |
| The URI-literal UDF as the *primary* API | **No.** Ship it as an escape hatch; make DDL-registered models the primary shape. |

Tree models are the majority of what customers actually score in a warehouse, so "ship the
mechanism, no trees" is a partial product. The three routes out, in the order I would take
them:

1. **Fix tract upstream** (§3.5 + §3.3 + §3.4). Self-contained, all three are small, and it
   buys back the entire sklearn/XGBoost/LightGBM family on a pure-Rust stack. Best
   effort:reward ratio and it is a good open-source contribution.
2. **Implement `TreeEnsemble*` ourselves** in `oxidant-ml` and let tract handle everything
   else. We control the layout, can go SIMD/column-major, and would land far above tract's
   corrected speed. More code, no upstream dependency.
3. **Add onnxruntime as an optional backend.** Fastest to correct-and-fast, but it drags a
   C++ dependency into a Rust binary that currently has none — which is a real part of the
   product's story. I would not do this first.

### Estimated build effort

| Work | Effort |
|---|---|
| Harden what exists (error paths, nulls, tests, docs) | 3–5 days |
| `CREATE MODEL` DDL + catalog object + struct-returning UDF | 1.5–2.5 weeks |
| Upstream tract fixes (§3.3–3.5) + version bump | 1–2 weeks, plus upstream review latency |
| *or* our own `TreeEnsemble` op (vectorized) | 2–3 weeks |
| Distributed: ship model bytes to workers / warm caches | 1 week |
| **Total to a credible GA** | **6–9 weeks** |

### Risks

* **Silent wrongness is the dominant risk in this whole area**, and we have already hit it once
  (§3.4). Any shipping version needs a golden-model parity harness that scores fixtures against
  onnxruntime in CI — the same shape as `oxidant-spark-compat`'s ratchet. Without it, a tract
  upgrade can change answers with a green build.
* **f32 narrowing is silent** (§3.6). A `DECIMAL(38,10)` feature loses precision on its way
  into the tensor with no warning. Needs at minimum a documented contract, ideally a check.
* **Positional feature binding** (API §1) — mitigated by the DDL shape, not by the UDF shape.
* **No cache eviction.** A long-lived executor scoring many model versions grows without
  bound. ~1 MiB per model makes this slow, not harmless.
* **Model version can change mid-query.** The 60 s TTL means a long query spanning the
  boundary can score early batches with v1 and later batches with v2. Correct-looking output,
  two models. The DDL shape (pinned version per statement) is the real fix.
* **tract is a small project.** Upstream responsiveness is a schedule dependency for route 1.
* **`ZipMap`** (§3.2) means we cannot consume a model exported by someone who did not know our
  constraint. Either document it loudly or teach `compat.rs` to strip `ZipMap` nodes.

### What I would do next

Fix §3.5 in tract and open the PR — it is the one change that turns this from "a neural-net
feature" into "an ML feature". Everything else on the list is ordinary work; that one is the
hinge.

---

## Reproducing

```sh
# 1. Export the models (python venv: scikit-learn, skl2onnx, torch, onnx, onnxruntime)
python export_models.py            # writes /tmp/mlspike/models/*.onnx + golden.json

# 2. tract op-coverage probe + onnxruntime cross-check
cargo run -p oxidant-ml --bin ml-probe -- golden_raw.csv wine_gbdt_float.onnx --dump

# 3. Row vs batch
cargo run --profile release-ci -p oxidant-ml --bin ml-bench -- rows.csv model.onnx 1024 8192

# 4. Lifecycle
cargo run --profile release-ci -p oxidant-ml --bin ml-lifecycle -- model.onnx

# 5. In SQL
oxidant sql -e "SELECT ml_predict('/path/model.onnx', 7.4, 0.7, …, 9.4)"
```

The python export script and SQL fixtures live in the issue thread; they are not committed
because this branch is throwaway and the models are 4 KB–107 KB binaries.

# HVM2/Bend backend — verdict and removal record

**Date:** 2026-08-06 · **Status:** decided — backend removed · **Archive:** the last
HVM-containing tree is tagged [`archive/hvm-backend`](https://github.com/OxidantData/Oxidant/tree/archive/hvm-backend).

## Verdict

HVM2 cannot run Oxidant's workload class — not "is slow on it", but *cannot express it
by construction*:

- **24-bit numerics only** (`u24/i24/f24`): a `SUM`/`COUNT` overflows at 16,777,216
  rows. TPC-H SF100 `lineitem` alone is ~600M rows; every aggregate over real data is
  wrong, not merely slow.
- **No hash-table primitive**: `Map` is an immutable binary tree — path-copying
  allocation per group insert makes hash aggregation/join O(N·log G) *with* allocation.
- **No array/columnar/SIMD type**: a column becomes O(N) boxed cons nodes,
  pointer-chased — the exact opposite of the vectorized hot loop.
- **No I/O, no FFI** (experimental): it cannot read a Parquet file or be handed an
  Arrow buffer; the host must marshal bounded inputs in and results back out.
- **4 GB / 32-bit heap** per instance; GPU path is **CUDA-only, Nvidia-only,
  effectively RTX-4090-only**, and maintainer-flagged "less stable".

Measured consequence: the HVM path contributes **0 of 43 ClickBench, 0 of 22 TPC-H,
and 0 of 99 TPC-DS queries**. There is no query Oxidant ships where it wins.

## Evidence in-repo (the scaffold never ran)

- `crates/oxidant-hvm` was a 27-line stub: an `ENABLED` const plus `run_fragment()`
  returning `Err(Unsupported)` on every call.
- The `hvm = "2"` dependency was never added — a `TODO(deps)` in its `Cargo.toml`.
- The `hvm` feature was off by default; nothing in the workspace enabled it.
- `oxidant_optimizer::route()` defaulted every fragment to `Backend::Loom`; no plan
  ever routed to `Backend::Hvm`.

Removing it therefore changes no behavior: nothing executed HVM, so no benchmark
number (ClickBench / TPC-H / TPC-DS / parity) can move.

## Decision

Remove the `oxidant-hvm` scaffold and the two-backend routing seam. Oxidant has a
**single vectorized execution backend: Loom.**

The workload class HVM2 was meant for — irregular, graph-shaped, recursive compute —
is redirected to **native Loom vectorized operators**, as a future separate program:
CSR adjacency representations, factorized / worst-case-optimal joins, and semi-naive
recursive evaluation (Kuzu-lineage techniques). These need no second runtime: they are
operators over Arrow, inside the engine we already have.

## Guardrails going forward

- **The Arrow boundary is sacred.** Everything between operators is Apache Arrow; no
  operator gets a private in-memory format.
- **No new runtimes.** Execution innovation lands as Loom operators, not as an
  embedded foreign evaluator.
- **Benchmark honesty.** Every performance claim rides with a reproducible
  ClickBench / TPC-H number; a new operator class must win on a published suite or a
  clearly-defined workload before it is wired onto a query path.

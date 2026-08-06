//! Native operators — the Phase-1 carve-outs that replace DataFusion's generic physical
//! operators on the handful of ClickBench queries that dominate the total runtime.
//!
//! Each operator here consumes and produces Arrow [`RecordBatch`](crate::arrow::array::RecordBatch),
//! so it slots in behind the same `Engine` surface while DataFusion remains the planner and the
//! correctness fallback for everything not yet carved out (architecture §"The decision that
//! shapes everything", decision D2).
//!
//! Status: kernels land first as standalone, unit-tested functions cross-checked against
//! DataFusion; the physical-plan rewrite that routes matching plans through them is wired
//! separately so the build stays green and all 43 queries stay correct.

pub mod hash_agg;

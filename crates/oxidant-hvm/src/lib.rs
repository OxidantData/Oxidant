//! `oxidant-hvm` — the parallel/GPU backend (codename the second front).
//!
//! Compiles *routed* plan fragments to Bend and reduces them on the HVM2 runtime (C, or
//! CUDA on RTX-4090-class GPUs). It is **off by default** and **off the critical path**:
//! the engine is correct and competitive on `oxidant-loom` alone.
//!
//! Hard constraints that shape everything here (see architecture §3):
//! - HVM2 has **no I/O and no FFI**, so it is driven as a Rust *library*: the host
//!   marshals a small, bounded input into an interaction net, reduces, and reads results
//!   back. Marshalling only pays when per-element compute is high and parallelism massive.
//! - 24-bit numerics, no hash table, no columnar/SIMD type, 4 GB heap, CUDA/4090-only.
//!   Therefore this backend **never** runs the columnar hot loop — only embarrassingly-
//!   parallel, compute-bound, irregular fragments (recursive/graph UDFs, symbolic
//!   transforms, the tree-shaped *combine* of partial aggregates).
//!
//! Phase 2 ships a go/no-go gate: demonstrate ≥2× over `oxidant-loom` on a bounded workload
//! class, or shelve this crate as research with the engine unaffected.

/// Whether the HVM backend was compiled in.
pub const ENABLED: bool = cfg!(feature = "hvm");

/// Codegen + reduce a routed fragment. Stubbed; lives behind the `hvm` feature when real.
pub fn run_fragment() -> oxidant_common::Result<()> {
    Err(oxidant_common::Error::Unsupported(
        "oxidant-hvm backend is not enabled (build with --features oxidant-hvm/hvm)".into(),
    ))
}

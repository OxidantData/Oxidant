//! `oxidant-physical` — physical planning and the execution contract.
//!
//! Lowers a resolved logical plan to physical operators, carrying the [`Backend`] tag
//! that heddle assigned. [`ExecutionPlan`] streams Arrow `RecordBatch`es — the
//! universal currency between operators — and is implemented by the single vectorized
//! backend, `oxidant-loom`.

use oxidant_common::Result;
use oxidant_optimizer::Backend;

/// A physical operator that produces a stream of Arrow record batches.
///
/// The Arrow type is left abstract in the stub (the real trait returns a
/// `SendableRecordBatchStream`); this keeps the seam visible without pulling `arrow` in.
pub trait ExecutionPlan: Send + Sync {
    /// The backend this operator executes on.
    fn backend(&self) -> Backend;

    /// Human-readable operator name for `EXPLAIN`.
    fn name(&self) -> &str;
}

/// Plan a resolved logical plan into a physical plan. Implemented in Phase 0.
pub fn plan_physical() -> Result<()> {
    Ok(())
}

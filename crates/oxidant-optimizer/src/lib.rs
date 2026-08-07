//! `oxidant-optimizer` (codename **heddle**) — logical optimization.
//!
//! Beyond the usual rewrites (predicate/projection pushdown, constant folding, join
//! reorder), heddle owns plan-level decisions for Oxidant's single execution backend:
//! the vectorized CPU core ([`oxidant-loom`](../oxidant_loom/index.html)). The
//! Bend→HVM2 second-backend bet was evaluated and removed — see
//! `docs/HVM_VERDICT.md`; irregular/graph compute is planned as Loom-native
//! operators, not a second runtime.

use oxidant_plan::LogicalPlan;

/// Which execution backend a plan fragment runs on. Oxidant has exactly one backend —
/// the vectorized Loom engine — so this is a tag (kept for the
/// `oxidant-physical` [`ExecutionPlan`](../oxidant_physical/index.html) contract and
/// `EXPLAIN` output), not a routing choice.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Backend {
    /// Vectorized Arrow CPU engine. The only backend; home of the columnar hot loop.
    Loom,
}

/// Decide the backend for a (sub)plan. Single-backend engine: everything is `Loom`.
pub fn route(_plan: &LogicalPlan) -> Backend {
    Backend::Loom
}

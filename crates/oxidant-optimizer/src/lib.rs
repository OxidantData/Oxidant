//! `oxidant-optimizer` (codename **heddle**) — logical optimization and backend routing.
//!
//! Beyond the usual rewrites (predicate/projection pushdown, constant folding, join
//! reorder), heddle owns Oxidant's distinctive job: deciding, per plan fragment, whether it
//! runs on the vectorized CPU core ([`oxidant-loom`](../oxidant_loom/index.html)) or is routed
//! to the parallel backend ([`oxidant-hvm`](../oxidant_hvm/index.html)).
//!
//! Routing default is **CPU**. A fragment is eligible for `oxidant-hvm` only if it is
//! compute-bound, massively fine-grained-parallel, fits 24-bit numerics, and is NOT a
//! columnar scan/filter/hash-aggregate (those always stay on Loom — see architecture §3).

use oxidant_plan::LogicalPlan;

/// Which execution backend a plan fragment should run on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Backend {
    /// Vectorized Arrow CPU engine. The default and the home of the columnar hot loop.
    Loom,
    /// Bend → HVM2 backend. Opt-in, routed fragments only, gated behind a feature flag.
    Hvm,
}

/// Decide the backend for a (sub)plan. Conservative default: everything is `Loom` until
/// the HVM2 go/no-go gate (Phase 2) proves a fragment class where `Hvm` wins.
pub fn route(_plan: &LogicalPlan) -> Backend {
    Backend::Loom
}

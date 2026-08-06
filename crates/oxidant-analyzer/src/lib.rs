//! `oxidant-analyzer` — name/type/function resolution.
//!
//! Turns the *unresolved* warp IR into a *resolved* logical plan: binds
//! `UnresolvedAttribute`/`UnresolvedFunction`/`UnresolvedStar` against the catalog and a
//! function registry, computes output schemas, and inserts implicit casts following
//! Spark type-coercion rules. This (plus behavioral parity) is the bulk of the real
//! engineering effort — not the gRPC transport.

use oxidant_catalog::CatalogProvider;
use oxidant_common::Result;
use oxidant_plan::LogicalPlan;

/// Resolve a plan against a catalog.
///
/// Phase 0 is a no-op: table-name resolution happens lazily in the DataFusion catalog bridge
/// (`oxidant-loom::catalog_bridge`), not here. This seam stays for when analysis moves out of
/// DataFusion (typed coercion, function binding) and needs the catalog directly.
pub fn resolve(plan: LogicalPlan, _catalog: &dyn CatalogProvider) -> Result<LogicalPlan> {
    Ok(plan)
}

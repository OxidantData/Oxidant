//! Derive a distributed [`StageDef`] DAG automatically from a SQL query.
//!
//! ## Supported shape (v1)
//!
//! A single **grouped aggregation** over one table (optionally filtered, sorted, limited):
//!
//! ```sql
//! SELECT <group cols>, <aggregates> FROM t [WHERE ...] GROUP BY <cols> [ORDER BY ...] [LIMIT n]
//! ```
//!
//! It lowers to the canonical two stages — *partial aggregate per worker → hash shuffle by the
//! group key → final combine*:
//!
//! - re-combinable aggregates lower directly (`SUM→SUM`, `COUNT→SUM`, `MIN→MIN`, `MAX→MAX`);
//! - `AVG(x)` is split into `SUM(x)`/`COUNT(x)` partials and recombined as `Σsum / Σcount`;
//! - `COUNT(DISTINCT x)` (and any other `DISTINCT` aggregate) can't pre-aggregate, so the partial
//!   stage instead *projects* the grouping + argument columns and shuffles the raw rows by the
//!   group key; the final stage runs the original aggregate over the co-located rows (exact,
//!   because every group lands wholly on one worker).
//!
//! ## Joins (broadcast)
//!
//! A join is auto-derived when every base table but one is **replicated** (passed in `replicated` —
//! present in full on every worker): the join then runs locally per worker over the single sharded
//! table's shard, so it folds straight into the partial stage's FROM tail with no extra shuffle.
//! This covers star schemas (a sharded fact + replicated dimensions, including multi-dim
//! join chains folded into the partial). Joins between two or more *sharded* tables lower to a
//! **left-deep shuffle-join chain** (pairwise equijoin stages, then partial/final aggregate)
//! when each join is a single equijoin key. Two sharded-table compositions layer on top of that:
//!
//! - **comma-joins** (`FROM a, b WHERE a.k = b.k`, TPC-H Q12) — which DataFusion leaves as a
//!   `CROSS JOIN` + `Filter` — are normalized up front into a connected chain of keyed inner
//!   equijoins by [`super::join_order::connect_comma_join_chain`] (so stage SQL never emits a
//!   plain cross join between large tables — TPC-DS Q6) and planned by the ordinary paths;
//!   [`super::join_chain::rewrite_comma_join_filters`] remains as the retry for shapes the
//!   up-front rewrite declines.
//! - an **aggregate over a pre-aggregated derived table** (TPC-H Q13's count-distribution over a
//!   LEFT JOIN group-by, TPC-DS Q54's revenue bands over a per-customer `GROUP BY` CTE) is
//!   composed from the inner distributed aggregation plus one exact outer-aggregate stage
//!   hash-shuffled by the outer group key ([`aggregate_over_aggregate_stages`]).
//!
//! Also supported: ungrouped/global aggregates, `HAVING` over the aggregated result,
//! scalar / IN / EXISTS subqueries **over replicated tables only**, distributable set operations,
//! and **narrow window** support: re-combinable aggregate
//! windows (`SUM`/`COUNT`/`MIN`/`MAX`/`AVG`) with a non-empty `PARTITION BY` over one
//! sharded table (hash-shuffle by the partition key, then compute the window locally).
//! CTE-heavy outer cross joins are lowered recursively by [`super::dag_splitter`]: each sharded
//! aggregate branch becomes its own sub-DAG, and a gathered outer stage combines the branch
//! outputs with any replicated-only inputs.
//! Ranking windows distribute too (KAN-49a/KAN-49b): with a `PARTITION BY` they compute after
//! the partition hash-shuffle; a *global* ranking window (no `PARTITION BY`) gathers the tiny
//! combined aggregate output to partition 0 and computes there. Other unsupported window shapes
//! return an explicit [`Error::Unsupported`] so the caller falls back to single-node execution.
//! Correlated scalar subqueries over sharded tables are either decorrelated into a distributed
//! per-key aggregate + shuffle join (equality-correlated `min`/`max`/`sum`/`count`, TPC-H Q2 —
//! see [`super::shape_extensions::try_decorrelate_scalar_subquery`]) or rejected (not
//! broadcast-safe). **Uncorrelated** scalar-aggregate thresholds in HAVING (TPC-H Q11) get a
//! one-row broadcast — scalar partial/combine stages plus driver-side literal injection into the
//! outer stages ([`super::shape_extensions::try_uncorrelated_scalar_threshold`]).

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

use datafusion::common::{Column, TableReference};
use datafusion::logical_expr::{
    Aggregate, Expr, GroupingSet, JoinType, LogicalPlan, Projection, Union,
};
use datafusion::sql::unparser::Unparser;
use oxidant_common::{Error, Result};
use oxidant_loom::Engine;

use super::shape_extensions::{
    collect_subquery_tables, ensure_having_subquery_tables_replicated,
    ensure_subquery_tables_replicated, reject_explicit_unsupported,
    try_decorrelate_scalar_subquery, try_derived_scalar_equality, try_in_agg_semi_join,
    try_materialize_complex_fact, try_materialize_subquery_fact, try_nested_in_semi,
    try_non_aggregate, try_scalar_subquery_projection, try_semi_anti_subqueries,
    try_uncorrelated_scalar_threshold, try_union_all, try_window,
};
use crate::driver::{ExchangeMode, StageDef};

/// A query lowered to a distributed [`StageDef`] DAG.
#[derive(Debug, Clone)]
pub struct DistributedQuery {
    /// Topologically-ordered stages; the last is the output stage. Its result is the grouped
    /// aggregation, **unordered** — a global `ORDER BY` / `LIMIT` can't be applied per-worker.
    pub stages: Vec<StageDef>,
    /// Optional global finalize to run on the *gathered* result (registered as table `result`):
    /// the query's `ORDER BY` / `LIMIT`, which must run once over all workers' output, not per
    /// worker. `None` when the query has neither.
    pub finalize_sql: Option<String>,
}

/// Derive a distributed plan for `sql`, or [`Error::Unsupported`] if its shape isn't handled yet.
///
/// `replicated` names base tables that are present in **full** on every worker (small dimension
/// tables). A join is auto-derived as a **broadcast join** — it runs locally per worker — as long as
/// every table but one is replicated (so exactly one table is sharded). Joins between two or more
/// *sharded* tables are auto-derived as a left-deep shuffle-join chain when each join is a single
/// equijoin.
/// Derive a distributed plan from an already-built logical plan.
pub fn plan_distributed_logical(lp: &LogicalPlan, replicated: &[&str]) -> Result<DistributedQuery> {
    // Comma-join chains (`CROSS JOIN` + WHERE equijoins — TPC-DS Q6) become connected keyed
    // inner joins before anything else: DataFusion has no join reordering, so a cross join
    // emitted into stage SQL executes as one on the worker, and `CrossJoinExec` buffers its
    // whole left input (Q6 at SF10: 16 GB of `date_dim × customer ⋈ customer_address`)
    // outside the KAN-25 hash-join build guard. The rewrite is semantics-preserving and a
    // no-op for every other shape (keyed chains, genuine cross products, branch-DAG joins).
    let connected = super::join_order::connect_comma_join_chain(lp, replicated);
    let lp = connected.as_ref().unwrap_or(lp);
    // Re-root a dim-leftmost inner chain at a sharded leaf (TPC-DS Q37/Q82 under the row-aware
    // classification): the shuffle-join-chain planner requires a sharded leftmost table, and
    // inner joins are symmetric, so rotating the chain preserves semantics. Runs before the
    // filter-first reorder so the reorder permutes the chain around its final root; a no-op
    // (returns `None`) for sharded-leftmost and single-sharded chains, so those plans stay
    // byte-for-byte stable.
    let rerooted = super::join_order::reroot_inner_chain_at_sharded(lp, replicated);
    let lp = rerooted.as_ref().unwrap_or(lp);
    // Filter-first inner-join reorder (TPC-DS Q72 at SF10): DataFusion executes inner joins in
    // written order, so a fact⋈fact join placed before the selective dimension filters must
    // stream (or, under the workers' unknown-stats sort-merge reroute, external-sort) the
    // exploded intermediate — Q72's stage 0 wrote 36-38 GB of sort spill per worker and never
    // finished. Pulling filtered leaves ahead is semantics-preserving and shrinks the
    // intermediate before the expensive join; a no-op (returns `None`) for plans whose order
    // already matches, so unaffected queries keep their plans byte-for-byte.
    let reordered = super::join_order::reorder_filtered_dims_first(lp);
    let lp = reordered.as_ref().unwrap_or(lp);
    // KAN-2 (TPC-DS Q72 at the 2-sharded classification): distribute a WHERE `Filter` above a
    // keyed chain with two or more sharded leaves onto the chain — single-table conjuncts push
    // onto their leaf's scan, cross-table conjuncts become the residual of the join placing
    // their last referenced leaf. The chain extractor walks past `Filter` nodes and would
    // silently drop them; with trailing replicated joins now folded into the final chain stage
    // (Q72's LEFT JOIN promotion / catalog_returns) instead of rejected, a dropped `Filter`
    // would be a wrong answer. A no-op for one-sharded chains (the broadcast path unparses the
    // filter into its stage tail) and for every shape the rewrite declines (subquery-bearing
    // conjuncts, outer-join-side references, ambiguous columns).
    let distributed = super::join_order::distribute_chain_filter(lp, replicated);
    let lp = distributed.as_ref().unwrap_or(lp);
    let primary: Result<DistributedQuery> = (|| {
        // Correlated scalar min/max/sum/count subqueries (TPC-H Q2) get a real shuffle-join
        // plan here instead of the whole-fact gather they'd otherwise fall into below.
        if let Some(dq) = try_decorrelate_scalar_subquery(lp, replicated)? {
            return Ok(dq);
        }
        // Uncorrelated scalar-aggregate thresholds (TPC-H Q11's global HAVING fraction) get a
        // one-row broadcast: scalar partial/combine stages, then the driver inlines the single
        // computed value into the outer stages' SQL before dispatch (literal injection).
        if let Some(dq) = try_uncorrelated_scalar_threshold(lp, replicated)? {
            return Ok(dq);
        }
        // KAN-37: TPC-H Q18's grouped `IN` aggregate fused with the identical outer aggregate
        // joins the tiny per-key aggregate stream to the replicated dims instead of shuffling
        // the full 3-way join output (~60M wide rows at SF10 → 600s stage-timeout blowout).
        if let Some(dq) = try_in_agg_semi_join(lp, replicated)? {
            return Ok(dq);
        }
        // EXISTS / NOT EXISTS / IN predicates over a sharded fact (TPC-H Q4/Q18/Q21) plan as
        // co-located semi/anti key shuffles feeding the ordinary two-stage aggregation,
        // replacing the whole-fact single-partition gather they'd otherwise fall into below.
        if let Some(dq) = try_semi_anti_subqueries(lp, replicated)? {
            return Ok(dq);
        }
        // A non-aggregate top with one nested IN / correlated-scalar IN predicate (TPC-H Q20)
        // plans as a co-located semi cascade.
        if let Some(dq) = try_nested_in_semi(lp, replicated)? {
            return Ok(dq);
        }
        // An uncorrelated scalar over a derived per-key aggregate (TPC-H Q15's
        // `total_revenue = (SELECT max(total_revenue) FROM revenue)`) plans as a distributed
        // derived table + the KAN-27 one-row scalar broadcast.
        if let Some(dq) = try_derived_scalar_equality(lp, replicated)? {
            return Ok(dq);
        }
        // KAN-55: uncorrelated global-aggregate scalar subqueries in the projection over an
        // all-replicated outer (TPC-DS Q9) plan as per-scalar partial/combine pairs plus a
        // gated single-partition outer evaluation.
        if let Some(dq) = try_scalar_subquery_projection(lp, replicated)? {
            return Ok(dq);
        }
        // KAN-49 wave-3b: a UNION (distinct) of per-channel arms carrying global rank()
        // windows (TPC-DS Q49) — must run before `try_union_all`, whose arm-peel errors on
        // the windowed arms.
        if let Some(dq) = super::gather_shapes::try_ranked_union(lp, replicated)? {
            return Ok(dq);
        }
        if let Some(dq) = try_materialize_subquery_fact(lp, replicated)? {
            return Ok(dq);
        }
        // KAN-49 wave-3f: a UNION ALL of per-channel arms whose sharded inputs are shared
        // derived CTEs (TPC-DS Q23 at the SF10 classification — the channel facts replicate,
        // the store_sales-derived CTEs plan once and gather into each arm's stage). Must run
        // before `try_union_all`, whose arm planner errors on these arms instead of declining.
        if let Some(dq) = super::gather_shapes::try_union_over_derived_ctes(lp, replicated)? {
            return Ok(dq);
        }
        // KAN-49 wave-4 (TPC-DS Q14): a grouping-set (ROLLUP) aggregate over a per-channel
        // UNION ALL whose arms carry an INTERSECT-derived `IN` key set and a global-AVG HAVING
        // threshold — both derived tables over the sharded fact. Must run before
        // `try_union_all`, whose arm planner rejects these subqueries, and before the peel
        // path, whose subquery safety checks refuse them.
        if let Some(dq) = super::gather_shapes::try_rollup_union_derived_subqueries(lp, replicated)?
        {
            return Ok(dq);
        }
        if let Some(dq) = try_union_all(lp, replicated)? {
            return Ok(dq);
        }
        if let Some(dq) = try_window(lp, replicated)? {
            return Ok(dq);
        }
        if let Some(dq) = try_non_aggregate(lp, replicated)? {
            return Ok(dq);
        }
        // KAN-49 wave-3b ("gather" wave): the shapes that previously fell into the
        // strict-refused whole-fact gather — set-op chains under a global count (Q38/Q87),
        // a FULL OUTER JOIN of distinct-key aggregates (Q97), a HAVING scalar threshold over
        // a shared derived aggregate (Q24), and IN keys from a self-join of the fact (Q95).
        if let Some(dq) = super::gather_shapes::try_global_count_over_set_op(lp, replicated)? {
            return Ok(dq);
        }
        if let Some(dq) = super::gather_shapes::try_full_outer_join_global_agg(lp, replicated)? {
            return Ok(dq);
        }
        if let Some(dq) = super::gather_shapes::try_derived_having_scalar_threshold(lp, replicated)?
        {
            return Ok(dq);
        }
        if let Some(dq) = super::gather_shapes::try_self_join_in_keys(lp, replicated)? {
            return Ok(dq);
        }
        reject_explicit_unsupported(lp)?;
        let mut dq = match peel(lp) {
            Ok(peeled) => aggregation_stages_for(&peeled, replicated),
            Err(linear_error) => match super::dag_splitter::try_branch_dag(lp, replicated)? {
                Some(dq) => Ok(dq),
                None => Err(linear_error),
            },
        }?;
        validate_stage_sql(&mut dq)?;
        Ok(dq)
    })();

    let mut dq = match primary {
        Ok(dq) => dq,
        Err(primary_error) => {
            // Debug hook: the gather fallback below replaces this error in strict mode, which
            // hides *why* no parallel shape matched. `OXIDANT_TPCDS_DEBUG=1` surfaces it.
            if std::env::var("OXIDANT_TPCDS_DEBUG").is_ok() {
                eprintln!("[plan-debug] primary shape error: {primary_error}");
            }
            // KAN-26: a comma-join (`CROSS JOIN` + `WHERE` equijoin — TPC-H Q12) becomes a
            // shuffleable inner equijoin once the filter conjuncts are pushed into the join.
            // Retry the whole planner on the normalized plan before the gather / rejection
            // fallbacks; `rewrite_comma_join_filters` returns `None` (and the retry simply
            // fails) for anything else, so those paths see the original error unchanged.
            let reason = primary_error.to_string();
            if reason.contains("shuffle join needs an equijoin key")
                || reason.contains("Cross Join")
            {
                if let Some(rewritten) = crate::plan::join_chain::rewrite_comma_join_filters(lp) {
                    // The recursive call validates and stamps; on failure the original error
                    // drives the usual fallbacks below.
                    if let Ok(dq) = plan_distributed_logical(&rewritten, replicated) {
                        return Ok(dq);
                    }
                }
            }
            // KAN-49a: a correlated scalar-aggregate subquery left in the branch-aware outer
            // skeleton (TPC-DS Q1/Q30/Q81's per-key avg threshold) decorrelates into a derived
            // per-key aggregate join — which the retry then materializes as its own branch
            // instead of rejecting the leftover sharded scan.
            if reason.contains("still scans unmaterialized sharded table") {
                if let Some(rewritten) =
                    super::shape_extensions::rewrite_correlated_scalar_subqueries(lp)
                {
                    if let Ok(dq) = plan_distributed_logical(&rewritten, replicated) {
                        return Ok(dq);
                    }
                }
            }
            let materializable_rejection = reason.contains("scanned multiple times")
                || reason.contains("scanned 2×")
                || reason.contains("scanned 3×")
                || reason.contains("scanned 4×")
                || reason.contains("scanned 5×")
                || reason.contains("scanned 6×")
                || reason.contains("scanned 7×")
                || reason.contains("FULL OUTER JOIN is not broadcast-safe")
                || reason.contains("shuffle join needs an equijoin key")
                || reason.contains("arm 0 is not a distributable aggregation")
                || reason.contains("unsupported top-level plan node")
                || reason.contains("Cross Join")
                || reason.contains("window over an aggregation")
                || reason.contains("window function")
                || reason.contains("global aggregation over DISTINCT")
                || reason.contains("COUNT(DISTINCT)")
                || reason.contains("UNION ALL arm does not scan sharded table")
                || reason.contains("branch-aware CrossJoin")
                || reason.contains("expected left-deep equijoin chain")
                // KAN-12: subquery-only / correlated fact scans that cannot stay shard-local
                // (TPC-H Q20) — gather the fact via try_materialize_complex_fact.
                || reason.contains("subquery over")
                || reason.contains("preserved side does not scan sharded table");
            if !materializable_rejection {
                return Err(primary_error);
            }
            match try_materialize_complex_fact(lp, replicated) {
                Ok(Some(mut dq)) => {
                    validate_stage_sql(&mut dq)?;
                    dq
                }
                Ok(None) => return Err(primary_error),
                Err(gather_err) => return Err(gather_err),
            }
        }
    };
    // Identical-stage CSE runs over the fully assembled DAG, whatever shape path built it: the
    // peel / branch-DAG block above, every early-returning shape handler, and the gather
    // fallback. It preserves each path's byte-for-byte stages unless two are wholly identical.
    cse_identical_stages(&mut dq.stages);
    stamp_replicated_tables(&mut dq, replicated);
    Ok(dq)
}

fn stamp_replicated_tables(dq: &mut DistributedQuery, replicated: &[&str]) {
    let csv = replicated.join(",");
    for stage in &mut dq.stages {
        // A stage whose stamp was set deliberately keeps it: the replicated-slice producer of
        // a union split (`split_union_finish`) drops its per-worker-sliced tables so the
        // workers' file sharder slices those scans for that stage only.
        if stage.replicated_tables.is_empty() {
            stage.replicated_tables = csv.clone();
        }
    }
}

/// Infer replicate/broadcast tables from file sizes + optional `OXIDANT_REPLICATED_TABLES` override.
///
/// See [`oxidant_loom::shard::classify_replicated_tables`]: the largest known table in the plan stays
/// sharded; smaller tables under `OXIDANT_AUTO_BROADCAST_THRESHOLD_BYTES` (default 32 GiB) replicate.
/// With catalog row counts available, the row-aware rule (`OXIDANT_REPLICATE_MAX_ROW_MULTIPLE`, on
/// by default at 4.0) keeps a byte-eligible candidate sharded when it has more than multiple ×
/// the largest table's rows — see [`oxidant_loom::shard::classify_replicated_tables_with_rows`].
///
/// KAN-55: tables scanned only inside expression subqueries (EXISTS / IN / scalar) are sized too.
/// They were previously invisible here and defaulted to *sharded*, which made e.g. TPC-DS Q10's
/// 500 MB `web_sales` shard-by-default at SF10 simply because it appears only inside a subquery —
/// blocking plans that are provably safe once the table replicates. Sizing them keeps the
/// per-query rule uniform: the largest table the query reads anywhere stays sharded.
pub async fn resolve_replicated_tables(engine: &Engine, lp: &LogicalPlan) -> Vec<String> {
    use oxidant_loom::shard::{
        auto_broadcast_threshold_bytes, classify_replicated_tables_with_rows,
        replicate_max_row_multiple, replicated_tables_override_from_env,
    };
    use std::collections::HashSet;

    let mut seen = HashSet::new();
    let mut names = base_tables(lp);
    collect_subquery_tables(lp, &mut names);
    let names: Vec<String> = names
        .into_iter()
        .filter(|name| seen.insert(name.to_ascii_lowercase()))
        .collect();
    // Size tables concurrently: a cache miss lists files (and, for external catalogs,
    // calls the catalog's metadata API) — serializing those multiplies the first-query
    // latency by the table count. The per-engine estimate cache keeps repeats cheap. Row
    // counts ride the same walk (catalog table properties; no extra I/O).
    //
    // The bare `names` stay the classification key below (sized/stamp_replicated_tables and
    // friends key off bare names); the sizing walk itself goes through the scan's full
    // TableReference when one owns the name (KAN-81), so a catalog-qualified `glue.db.t`
    // probes exactly its own namespace instead of fanning out across every catalog. The
    // string entry point is only the no-scan-owner fallback (local MemTables).
    let estimates = futures::future::join_all(names.iter().map(|name| {
        let reference = find_table_ref(lp, name);
        async move {
            match reference {
                Some(reference) => engine.estimate_table_stats_ref(&reference).await,
                None => engine.estimate_table_stats(name).await,
            }
        }
    }))
    .await;
    let mut sized: Vec<(String, Option<u64>)> = Vec::with_capacity(estimates.len());
    let mut rows: Vec<Option<u64>> = Vec::with_capacity(estimates.len());
    for (name, (bytes, row_count)) in names.into_iter().zip(estimates) {
        sized.push((name, bytes));
        rows.push(row_count);
    }
    let override_names = replicated_tables_override_from_env();
    let override_refs: Vec<&str> = override_names.iter().map(String::as_str).collect();
    classify_replicated_tables_with_rows(
        &sized,
        &rows,
        &override_refs,
        auto_broadcast_threshold_bytes(),
        replicate_max_row_multiple(),
    )
}

/// Last-line check on the SQL every stage will hand to a worker.
///
/// Individual shape handlers each splice Unparser output into their own stage SQL, so a
/// generated-SQL defect has to be caught in each of them or in one place after the fact. This is
/// that one place — it runs on whatever the chosen path produced.
///
/// Before rejecting, tries [`rewrite_out_of_scope_join_alias_refs`] on each stage's SQL / the
/// finalize SQL in place: most dangling `left`/`right` join-side references are just the
/// Unparser failing to substitute an outer alias it already emitted, so they can be patched up
/// rather than falling back to single-node execution. The reject call after the rewrite is a
/// safety net for shapes the rewrite can't fix (no definition at all, sibling-scope leaks).
fn validate_stage_sql(dq: &mut DistributedQuery) -> Result<()> {
    for s in dq.stages.iter_mut() {
        s.sql = rewrite_out_of_scope_join_alias_refs(&s.sql)?;
        reject_out_of_scope_join_alias_refs(&s.sql)?;
    }
    if let Some(f) = dq.finalize_sql.take() {
        let rewritten = rewrite_out_of_scope_join_alias_refs(&f)?;
        reject_out_of_scope_join_alias_refs(&rewritten)?;
        dq.finalize_sql = Some(rewritten);
    }
    Ok(())
}

/// Identical-stage CSE over the assembled stage DAG (TPC-DS Q44/Q36).
///
/// Shape-aware dedup happens earlier, at the *plan* level ([`super::dag_splitter::branch_fingerprint`]):
/// structurally identical branches plan once. That fingerprint covers whole branches, but two
/// branches that differ anywhere above the leaves — Q44's `rank() ASC` / `rank() DESC` window
/// ORDER BYs — still re-plan byte-identical sub-DAGs underneath (the same aggregate partials and
/// combines, the same HAVING-scalar partial/combine pairs): Q44 assembled 11 stages where stages
/// 0≡5, 1≡6, 2≡7, 3≡8, running 4 `store_sales` scans for 2 distinct inputs.
///
/// This pass merges stages whose entire dispatch contract is identical — same SQL, same exchange
/// mode, same hash key, same upstream ids — and rewrites every consumer's `upstream_stage_ids` at
/// the survivor. It runs to a **fixpoint**: merging leaves makes their (previously distinct only
/// in upstream id) combines identical, which then merge on the next pass.
///
/// Soundness rules:
///
/// - Consumer SQL names upstream outputs **positionally** (`shuffle_input` / `shuffle_input_N`
///   bind to the Nth listed upstream, not to a stage id), so retargeting an upstream id needs no
///   SQL rewrite; a stage read by several consumers (or twice by one — Q39's `[1, 1]`) is already
///   a supported scheduler shape.
/// - Only **consumed** stages merge. Unconsumed stages carry positional meaning the driver
///   pattern-matches on: the output stage (`run_stages_obs_inner` requires exactly one) and the
///   KAN-27 scalar combine (the unique non-output stage nobody lists as an upstream). Merging
///   either away would break those contracts.
/// - Volatile SQL never merges: a `rand()`-bearing stage must re-evaluate per reference, the
///   stage-level analog of [`super::dag_splitter::plan_contains_volatile`].
/// - Stages carrying a physical `plan_fragment` never merge (the SQL dispatch path this pass
///   reasons about does not apply to them).
///
/// Stage ids stay sparse (a merged-away id simply disappears); uniqueness and topological order
/// are preserved because a duplicate always merges into an *earlier* stage, so every remaining
/// consumer still follows its upstreams.
fn cse_identical_stages(stages: &mut Vec<StageDef>) {
    while cse_merge_pass(stages) {}
}

/// One CSE pass: merge every mergeable stage into the earliest identical stage, returning whether
/// anything merged (so the caller can re-run — newly identical consumers surface only after their
/// upstream ids are rewritten).
fn cse_merge_pass(stages: &mut Vec<StageDef>) -> bool {
    use std::collections::HashSet;
    let consumed: HashSet<u32> = stages
        .iter()
        .flat_map(|s| s.upstream_stage_ids.iter().copied())
        .collect();
    // (sql, exchange, hash key, upstreams, replicate stamp) — the full dispatch contract of a
    // stage. The stamp is query-uniform for most stages, but a union split's sliced producer
    // carries a deliberate per-stage stamp (`split_union_finish`) that changes what its workers
    // scan — such a stage is only identical to one stamped the same way.
    type CseKey = (String, u8, Vec<u32>, Vec<u32>, String);
    let mut representative: HashMap<CseKey, u32> = HashMap::new();
    let mut merge_into: HashMap<u32, u32> = HashMap::new();
    for s in stages.iter() {
        if !consumed.contains(&s.stage_id)
            || s.plan_fragment.is_some()
            || sql_contains_volatile(&s.sql)
        {
            continue;
        }
        let key = (
            s.sql.clone(),
            s.exchange as u8,
            s.hash_key_cols.clone(),
            s.upstream_stage_ids.clone(),
            s.replicated_tables.clone(),
        );
        match representative.entry(key) {
            std::collections::hash_map::Entry::Occupied(e) => {
                merge_into.insert(s.stage_id, *e.get());
            }
            std::collections::hash_map::Entry::Vacant(e) => {
                e.insert(s.stage_id);
            }
        }
    }
    if merge_into.is_empty() {
        return false;
    }
    stages.retain(|s| !merge_into.contains_key(&s.stage_id));
    for s in stages.iter_mut() {
        for u in &mut s.upstream_stage_ids {
            if let Some(&rep) = merge_into.get(u) {
                *u = rep;
            }
        }
    }
    true
}

/// Whether generated stage SQL invokes a volatile function (`rand()`, `now()`, …) whose result
/// may differ across evaluations. Conservative substring scan over the lowercased SQL: a false
/// positive only forgoes the merge, while the functions DataFusion marks volatile unparse to
/// exactly these spellings. SQL-level CSE cannot resolve UDF signatures the way the plan-level
/// [`super::dag_splitter::plan_contains_volatile`] can, so planner-emitted SQL is the scope.
pub(crate) fn sql_contains_volatile(sql: &str) -> bool {
    const VOLATILE: [&str; 8] = [
        "rand(",
        "random(",
        "now(",
        "today(",
        "uuid(",
        "current_time", // also prefixes current_timestamp
        "current_date",
        "localtimestamp",
    ];
    let lower = sql.to_ascii_lowercase();
    VOLATILE.iter().any(|f| lower.contains(f))
}

/// SQL convenience wrapper around [`plan_distributed_logical`].
///
/// When the shape-based planner cannot lower the query, falls back to a single
/// [`ExchangeMode::Forward`] stage via [`super::physical_splitter::plan_forward`] (Sail-like
/// coverage: any locally-plannable SQL still gets a distributed job graph on one worker that
/// has a full view of the tables). Planner coverage / ratchets must call
/// [`plan_distributed_logical`] directly so Forward does not inflate the supported count.
pub async fn plan_distributed(
    engine: &Engine,
    sql: &str,
    replicated: &[&str],
) -> Result<DistributedQuery> {
    let (lp, lakehouse_snapshot_pins) = engine.logical_plan_with_lakehouse_snapshots(sql).await?;
    // Optimize before splitting: stage SQL is unparsed from this plan, and no pushdown can
    // cross a stage boundary once the plan is cut. The stock optimizer moves outer filters
    // into the scans/group-bys below (TPC-DS Q78's year predicate, KAN-2 throughput).
    let lp = engine.optimize_logical_plan(lp)?;
    let mut query = match plan_distributed_logical(&lp, replicated) {
        Ok(dq) => Ok(dq),
        Err(Error::Unsupported(_)) => {
            crate::plan::physical_splitter::plan_forward(engine, sql).await
        }
        Err(e) => Err(e),
    }?;
    for stage in &mut query.stages {
        stage.lakehouse_snapshot_pins = lakehouse_snapshot_pins.clone();
        if stage.replicated_tables.is_empty() {
            stage.replicated_tables = replicated.join(",");
        }
    }
    Ok(query)
}

/// The top of the plan above the aggregate: the output projection (if any) plus the trailing
/// `ORDER BY` / `LIMIT`, which the final stage must reproduce.
pub(crate) struct Peeled<'a> {
    /// Output projection exprs (the SELECT list), if the plan has a `Projection` over the aggregate.
    pub(crate) projection: Option<&'a [Expr]>,
    /// `ORDER BY` exprs to apply on the final output, if any.
    pub(crate) sort: Option<&'a [datafusion::logical_expr::SortExpr]>,
    /// `LIMIT` fetch count, if any.
    pub(crate) limit: Option<usize>,
    /// Post-aggregate (`HAVING`) predicates, outermost first. Every `Filter` above the `Aggregate`
    /// lands here — see [`peel`].
    pub(crate) having: Vec<&'a Expr>,
    /// `Projection`s found *below* the output projection and above the `Aggregate`, which only
    /// rename the aggregate's output columns. TPC-DS Q21's `SELECT * FROM (SELECT … sum(…) AS
    /// inv_before … GROUP BY …) x WHERE …` puts one here: the inner subquery aliases the aggregate
    /// output before the `HAVING` or the outer projection ever names it. Ordered innermost-first so
    /// [`build_remap`] can fold them in the order the aliases were introduced.
    pub(crate) alias_projections: Vec<&'a [Expr]>,
    /// The aggregate node itself.
    pub(crate) agg: &'a Aggregate,
}

/// Strip an optional `Limit` / `Sort` / `Projection` off the top and require an `Aggregate` under
/// them. Rejects anything else (the caller falls back to single-node).
///
/// Every `Filter` crossed on the way down is a post-aggregate predicate: this loop only descends
/// through `Limit` / `Sort` / `Projection` / `Filter` / `SubqueryAlias`, so if it reaches the
/// `Aggregate` at all, nothing it passed could have filtered pre-aggregation rows. They are
/// therefore all collected as `HAVING` rather than matched positionally — an earlier version
/// required the `Filter` to sit *directly* on the `Aggregate` and silently discarded the predicate
/// otherwise, which made TPC-DS Q21 (`Filter` → `SubqueryAlias` → `Projection` → `Aggregate`)
/// return unfiltered rows.
pub(crate) fn peel(lp: &LogicalPlan) -> Result<Peeled<'_>> {
    let mut limit = None;
    let mut sort = None;
    let mut projection = None;
    let mut having = Vec::new();
    let mut alias_projections: Vec<&[Expr]> = Vec::new();
    let mut node = lp;
    loop {
        match node {
            LogicalPlan::Limit(l) => {
                // Only a plain `LIMIT n` (no OFFSET) is supported; fetch is an Expr in DF54.
                if let Some(Expr::Literal(scalar, _)) = l.fetch.as_deref() {
                    limit = scalar_as_usize(scalar);
                }
                node = &l.input;
            }
            LogicalPlan::Sort(s) => {
                sort = Some(s.expr.as_slice());
                node = &s.input;
            }
            LogicalPlan::Projection(p) => {
                // Scanning outer→inner, the first `Projection` is the query's real output
                // projection. Anything below it only renames aggregate output on the way to the
                // `Aggregate`, and is folded into the remap instead of replacing the output list.
                if projection.is_none() {
                    projection = Some(p.expr.as_slice());
                } else {
                    alias_projections.push(p.expr.as_slice());
                }
                node = &p.input;
            }
            LogicalPlan::Filter(f) => {
                having.push(f.predicate.as_ref());
                node = f.input.as_ref();
            }
            LogicalPlan::SubqueryAlias(s) => node = s.input.as_ref(),
            LogicalPlan::Aggregate(agg) => {
                alias_projections.reverse();
                return Ok(Peeled {
                    projection,
                    sort,
                    limit,
                    having,
                    alias_projections,
                    agg,
                });
            }
            other => {
                return Err(Error::Unsupported(format!(
                    "auto-distribute: unsupported top-level plan node `{}`",
                    other.display().to_string().lines().next().unwrap_or("")
                )));
            }
        }
    }
}

/// Build the distributed plan for a (possibly global) aggregation.
pub(crate) fn aggregation_stages_for(
    p: &Peeled<'_>,
    replicated: &[&str],
) -> Result<DistributedQuery> {
    let agg = p.agg;
    let tables = base_tables(&agg.input);
    let sharded: Vec<&str> = tables
        .iter()
        .filter(|t| !replicated.contains(&t.as_str()))
        .map(|t| t.as_str())
        .collect();

    // Subqueries (IN / EXISTS / scalar) only over replicated dims — never over unreplicated tables.
    // Also check HAVING: whatever reaches here with a sharded-table subquery in HAVING was
    // declined by the one-row-broadcast path (TPC-H Q11) and must gather instead.
    ensure_subquery_tables_replicated(&agg.input, &sharded, replicated)?;
    ensure_having_subquery_tables_replicated(&p.having, replicated)?;

    if agg.group_expr.is_empty() {
        return global_aggregation_stages(p, &sharded);
    }

    // Two or more sharded tables → left-deep shuffle-join chain + aggregate.
    if sharded.len() >= 2 {
        // KAN-26: the aggregate's input may itself be a pre-aggregated derived table (TPC-H
        // Q13's count-distribution over a LEFT JOIN group-by). The chain builder only accepts
        // raw scan inputs and would reject with "expected left-deep equijoin chain", so compose
        // instead: distribute the inner aggregation, then hash-shuffle its output by the outer
        // group key into an exact single-stage outer aggregate.
        if peels_to_inner_aggregate(&agg.input) {
            return aggregate_over_aggregate_stages(p, replicated);
        }
        // KAN-162: a UNION ALL whose arms each scan at most one sharded table (exactly once,
        // in a broadcast-safe tree) splits into one partial producer per sharded table plus an
        // associative recombine — the two-producer `try_split_broadcast_union` shape generalized
        // to N sharded arms. Pure admission gate: when the predicate doesn't hold the shuffle
        // join chain below keeps its current decline behavior.
        if let Some(dq) = try_split_multi_sharded_union(p, replicated)? {
            return Ok(dq);
        }
        return crate::plan::join_chain::plan_shuffle_join_chain(p, &sharded, replicated);
    }

    // KAN-36/KAN-44: a *single* sharded table needs the same composition whenever the
    // aggregate's input is itself an aggregation (a pre-aggregated derived table / CTE). The
    // flat broadcast path below would splice the inner aggregation into the partial stage's
    // FROM tail and run its GROUP BY per worker: an inner group whose rows span workers emits
    // one partial row per worker, and the outer aggregate reads those partials as final —
    // TPC-DS Q54's `my_revenue` CTE emitted two rows for one customer, splitting the outer
    // revenue-band count. The composition plans the inner aggregation exactly (partial →
    // shuffle by the inner key → combine) before the outer aggregate ever sees a row. This
    // subsumes the original KAN-36 divert (TPC-H Q13's null-extended outer join at the
    // auto-broadcast configuration, whose per-worker repetition the inner recombine absorbs).
    if sharded.len() == 1 && peels_to_inner_aggregate(&agg.input) {
        return aggregate_over_aggregate_stages(p, replicated);
    }

    // Broadcast-join safety: exactly one base table may be sharded; others must be replicated.
    if sharded.len() != 1 {
        return Err(Error::Unsupported(format!(
            "auto-distribute: need exactly one sharded base table (others replicated), \
             found {} sharded among {tables:?}",
            sharded.len()
        )));
    }
    let sharded_name = sharded[0];
    if let Some(dq) = aggregate_over_distinct_union_stages(p, sharded_name, replicated)? {
        return Ok(dq);
    }
    if let Some(dq) = try_split_broadcast_union(p, sharded_name, replicated)? {
        return Ok(dq);
    }
    if let Some(join) = sharded_null_extended_outer_join(&agg.input, sharded_name) {
        // KAN-36: the blanket preserved-side rejection does not apply to the relaxed outer-join
        // shape — but only aggregates blind to the preserved side's per-worker repetition may
        // use it (see the helper).
        ensure_null_extended_aggregate_args(agg, join)?;
    } else {
        reject_unsafe_broadcast_shapes(&agg.input, sharded_name)?;
    }
    // The aggregate's input must unparse to a plain `SELECT * FROM …` so we can splice our own
    // SELECT list onto its FROM/WHERE tail without losing column qualifiers.
    let input_sql = Unparser::default()
        .plan_to_sql(&agg.input)
        .map_err(|e| Error::Unsupported(format!("auto-distribute: unparse input: {e}")))?
        .to_string();
    // Unparser emits `SELECT * FROM …` for a single scan, but multi-join inputs can be
    // `SELECT *, * FROM …` (one star per join input). Extract the FROM/WHERE/JOIN tail either way.
    let tail = extract_from_tail(&input_sql)?;
    let tail = sanitize_generated_sql(&tail);

    // Broadcast is only correct if the sharded table is *scanned* exactly once (the driving fact).
    // A second scan — a self-join or a correlated EXISTS/IN subquery over it — would see only the
    // local shard per worker and silently lose cross-shard rows, so reject it. (`base_tables` counts
    // the plan-input scan only; subquery scans live in expressions, so descend into those too.)
    let scans = count_table_scans(&agg.input, sharded_name);
    if scans > 1 {
        return Err(Error::Unsupported(format!(
            "auto-distribute: sharded table `{sharded_name}` scanned {scans}× \
             (self-join / subquery) — not broadcast-safe"
        )));
    }

    let up = Unparser::default();
    // A DataFusion grouping set occupies one `group_expr` slot but represents several output
    // columns. Partial aggregation must use the union of those columns as its finest grouping
    // level; the final stage reconstructs the requested ROLLUP/CUBE/GROUPING SETS levels.
    let group_sql: Vec<String> = flattened_group_exprs(&agg.group_expr)
        .into_iter()
        .map(|g| expr_sql(&up, g))
        .collect::<Result<_>>()?;

    let aggs = agg
        .aggr_expr
        .iter()
        .map(AggSpec::classify)
        .collect::<Result<Vec<_>>>()?;
    let mut aggs = aggs;
    resolve_grouping_specs(&mut aggs, &agg.group_expr)?;
    let distinct = aggs.iter().any(|a| a.distinct);

    let remap = build_remap(p);

    // Two-phase grouping sets (TPC-DS Q67 at SF10: the gather below funnelled 4.84M finest-level
    // partial rows into ONE partition for a single-threaded ROLLUP): hash the partial by the
    // first grouping column instead, roll up per partition, and fix up the grand total.
    if !distinct {
        if let Some(dq) = grouping_set_two_phase_stages(p, &group_sql, &aggs, &tail, &remap)? {
            return Ok(dq);
        }
    }

    let (partial_sql, final_sql) = if distinct {
        distinct_stage_sql(&up, p, &group_sql, &aggs, &tail, &remap)?
    } else {
        recombine_stage_sql(p, &group_sql, &aggs, &tail, &remap)?
    };

    // Coarser grouping-set levels span multiple finest-level keys. Hashing by all `g{j}` columns
    // would therefore split (for example) a ROLLUP grand total across every worker. The shapes
    // [`grouping_set_two_phase_stages`] cannot distribute key by `g0` instead (handled above);
    // everything else gathers the already-compressed finest-level partials to one partition for
    // the final grouping set.
    let hash_key_cols: Vec<u32> = if is_grouping_set(&agg.group_expr) {
        vec![]
    } else {
        (0..group_sql.len() as u32).collect()
    };
    Ok(DistributedQuery {
        stages: vec![
            StageDef::new(0, partial_sql, vec![], hash_key_cols),
            StageDef::new(1, final_sql, vec![0], vec![]),
        ],
        finalize_sql: build_finalize(p)?,
    })
}

/// True when `lp` reaches an `Aggregate` through only column-renaming nodes (`Projection` /
/// `SubqueryAlias`) — i.e. the query aggregates an already-aggregated derived table (TPC-H Q13).
/// A `Filter` on the way down would be a pre-aggregation predicate on the derived table, which
/// [`aggregate_over_aggregate_stages`] cannot re-apply, so it deliberately does not descend
/// through one.
fn peels_to_inner_aggregate(lp: &LogicalPlan) -> bool {
    let mut node = lp;
    loop {
        match node {
            LogicalPlan::Projection(p) => node = p.input.as_ref(),
            LogicalPlan::SubqueryAlias(s) => node = s.input.as_ref(),
            LogicalPlan::Aggregate(_) => return true,
            _ => return false,
        }
    }
}

/// TPC-H Q13 at the auto-broadcast configuration (KAN-36): the single sharded table sits on the
/// **null-extended** side of a `LEFT` / `RIGHT` outer join whose preserved side scans only
/// replicated tables (`replicated customer LEFT JOIN sharded orders`). Per-worker broadcast
/// evaluation then repeats every preserved row on every worker — sound only under an aggregate
/// whose recombine absorbs that repetition (see [`ensure_null_extended_aggregate_args`]), which
/// is why the blanket preserved-side rejection in [`reject_unsafe_broadcast_shapes`] is relaxed
/// for exactly this shape. Returns the join when the shape matches.
fn sharded_null_extended_outer_join<'a>(
    lp: &'a LogicalPlan,
    sharded_name: &str,
) -> Option<&'a datafusion::logical_expr::Join> {
    let mut node = lp;
    loop {
        match node {
            LogicalPlan::Projection(p) => node = p.input.as_ref(),
            LogicalPlan::Filter(f) => node = f.input.as_ref(),
            LogicalPlan::SubqueryAlias(s) => node = s.input.as_ref(),
            LogicalPlan::Join(j) => {
                // Exactly one scan of the sharded table on the null-extended side, none on the
                // preserved side. Semi/anti joins stay rejected: they emit a replicated preserved
                // row on every worker with a *local* match, which no recombine absorbs.
                let matches = match j.join_type {
                    JoinType::Left => {
                        count_table_scans(&j.right, sharded_name) == 1
                            && count_table_scans(&j.left, sharded_name) == 0
                    }
                    JoinType::Right => {
                        count_table_scans(&j.left, sharded_name) == 1
                            && count_table_scans(&j.right, sharded_name) == 0
                    }
                    _ => false,
                };
                return matches.then_some(j);
            }
            _ => return None,
        }
    }
}

/// The relaxed outer-join broadcast shape is exact only when no aggregate can observe the
/// preserved side's per-worker repetition: every aggregate argument must reference at least one
/// column, and only columns of the null-extended (sharded) side — NULL-extended rows contribute
/// nothing to `count` / `sum` / `min` / `max` / `avg` partials over those columns, so the
/// ordinary recombine stays exact. `count(1)` / `count(*)` or any aggregate over a preserved-side
/// column would count every preserved row once per worker, and DISTINCT aggregates compose with
/// the raw-row shuffle, not this recombine — all stay rejected.
fn ensure_null_extended_aggregate_args(
    agg: &Aggregate,
    join: &datafusion::logical_expr::Join,
) -> Result<()> {
    let extended = match join.join_type {
        JoinType::Left => &join.right,
        _ => &join.left,
    };
    let scope = crate::plan::join_chain::JoinSideScope::of(extended);
    for e in &agg.aggr_expr {
        if AggSpec::classify(e)?.distinct {
            return Err(Error::Unsupported(format!(
                "auto-distribute: DISTINCT aggregate `{e}` over a sharded null-extended join \
                 side is not supported"
            )));
        }
        let mut cols = Vec::new();
        collect_expr_columns(strip_alias(e), &mut cols);
        if cols.is_empty() || !cols.iter().all(|c| scope.contains(c)) {
            return Err(Error::Unsupported(format!(
                "auto-distribute: aggregate `{e}` reads columns outside the sharded \
                 null-extended join side — broadcasting the join would repeat the replicated \
                 preserved side's rows on every worker"
            )));
        }
    }
    Ok(())
}

/// Aggregation over a pre-aggregated derived table (TPC-H Q13):
///
/// ```sql
/// SELECT c_count, count(*) FROM (
///     SELECT c_custkey, count(o_orderkey) FROM customer LEFT JOIN orders … GROUP BY c_custkey
/// ) AS c_orders GROUP BY c_count
/// ```
///
/// Reached either with 2+ sharded base tables (the inner LEFT JOIN planned as a shuffle-join
/// chain, KAN-26) or with a single sharded table (KAN-36/KAN-44) — originally only TPC-H Q13's
/// null-extended outer join at the auto-broadcast configuration, now every single-sharded
/// agg-over-agg input, since the flat broadcast path would otherwise run the inner GROUP BY per
/// worker and leak un-combined partial groups to the outer aggregate (TPC-DS Q54).
///
/// The inner aggregation is planned by the ordinary machinery (which handles the sharded LEFT
/// JOIN chain). Its terminal combine stage emits exactly one row per inner group, so
/// re-targeting that stage's hash key at the outer `GROUP BY` column(s) co-locates every row of
/// an outer group on one worker; the outer aggregate then runs **exactly** in a single stage —
/// the same co-location argument the DISTINCT path uses — and the query's HAVING / output
/// projection wrap it via the ordinary [`wrap_output`] remap.
///
/// Only outer group keys that are plain inner output columns can serve as hash keys, and the
/// outer aggregate arguments must reference inner output columns only; anything else returns
/// [`Error::Unsupported`] so the caller's strict-mode rejection / gather fallback decides.
fn aggregate_over_aggregate_stages(
    p: &Peeled<'_>,
    replicated: &[&str],
) -> Result<DistributedQuery> {
    let unsupported = |why: String| {
        Error::Unsupported(format!("auto-distribute: aggregate over aggregate: {why}"))
    };
    let inner = peel(&p.agg.input)?;
    if inner.sort.is_some() || inner.limit.is_some() {
        return Err(unsupported(
            "inner aggregation with ORDER BY / LIMIT is not supported".into(),
        ));
    }
    if !inner.having.is_empty() {
        return Err(unsupported(
            "a FILTER between the two aggregations is not supported".into(),
        ));
    }
    if is_grouping_set(&inner.agg.group_expr) || is_grouping_set(&p.agg.group_expr) {
        return Err(unsupported(
            "ROLLUP / CUBE / GROUPING SETS on either level are not supported".into(),
        ));
    }
    let mut dq = aggregation_stages_for(&inner, replicated)?;

    // Output column names of the inner terminal stage, in order: the aliased projection names
    // when the inner query has an output projection, otherwise the raw `g{j}` / `r{i}` names.
    let inner_out: Vec<String> = match inner.projection {
        Some(exprs) => exprs.iter().map(output_name).collect(),
        None => (0..inner.agg.group_expr.len())
            .map(|j| format!("g{j}"))
            .chain((0..inner.agg.aggr_expr.len()).map(|i| format!("r{i}")))
            .collect(),
    };

    // Outer group keys must be plain inner output columns — only those can be hash keys.
    let mut hash_key_cols = Vec::with_capacity(p.agg.group_expr.len());
    let mut group_names = Vec::with_capacity(p.agg.group_expr.len());
    for g in &p.agg.group_expr {
        let Expr::Column(c) = g else {
            return Err(unsupported(format!(
                "outer group key `{g}` is not a plain inner output column"
            )));
        };
        let idx = inner_out.iter().position(|n| n == &c.name).ok_or_else(|| {
            unsupported(format!(
                "outer group key `{}` is not an inner output column",
                c.flat_name()
            ))
        })?;
        hash_key_cols.push(idx as u32);
        group_names.push(c.name.clone());
    }

    // Every column the outer aggregates read must come from the inner aggregation's output.
    let up = Unparser::default();
    let mut aggs = Vec::with_capacity(p.agg.aggr_expr.len());
    for a in &p.agg.aggr_expr {
        let Expr::AggregateFunction(af) = strip_alias(a) else {
            return Err(unsupported(format!(
                "non-aggregate in outer aggregate list: {a}"
            )));
        };
        if af.params.args.len() > 1 {
            return Err(unsupported(format!(
                "multi-argument outer aggregate `{a}` is not supported"
            )));
        }
        for arg in &af.params.args {
            let mut cols = Vec::new();
            collect_expr_columns(arg, &mut cols);
            if let Some(bad) = cols.iter().find(|c| !inner_out.contains(&c.name)) {
                return Err(unsupported(format!(
                    "outer aggregate argument `{}` is not an inner output column",
                    bad.flat_name()
                )));
            }
        }
        aggs.push(af);
    }

    // Re-target the inner terminal stage at the outer group key so outer groups co-locate.
    let combine = dq
        .stages
        .last_mut()
        .ok_or_else(|| unsupported("inner aggregation produced no stages".into()))?;
    combine.hash_key_cols = hash_key_cols;
    let combine_id = combine.stage_id;

    // Exact single-stage outer aggregate over the co-located rows. With no outer group key the
    // shuffle gathers to partition 0, so suppress the synthetic zero-input row on the others
    // (mirrors `global_aggregation_stages`).
    let mut sel: Vec<String> = group_names
        .iter()
        .enumerate()
        .map(|(j, n)| format!("{n} AS g{j}"))
        .collect();
    for (i, af) in aggs.iter().enumerate() {
        let func = af.func.name().to_ascii_lowercase();
        let distinct = if af.params.distinct { "DISTINCT " } else { "" };
        let arg_sql = match af.params.args.first() {
            Some(arg) => expr_sql(&up, &unqualify(arg))?,
            None => "1".to_string(), // count(*) carries no arg
        };
        sel.push(format!("{func}({distinct}{arg_sql}) AS r{i}"));
    }
    let inner_sql = if group_names.is_empty() {
        format!(
            "SELECT {} FROM shuffle_input HAVING COUNT(*) > 0",
            sel.join(", ")
        )
    } else {
        format!(
            "SELECT {} FROM shuffle_input GROUP BY {}",
            sel.join(", "),
            group_names.join(", ")
        )
    };
    let remap = build_remap(p);
    let outer_sql = wrap_output(p, &inner_sql, &remap)?;

    let next_id = dq.stages.iter().map(|s| s.stage_id).max().unwrap_or(0) + 1;
    dq.stages
        .push(StageDef::new(next_id, outer_sql, vec![combine_id], vec![]));
    dq.finalize_sql = build_finalize(p)?;
    Ok(dq)
}

/// Every `Column` referenced anywhere in `e`.
fn collect_expr_columns(e: &Expr, out: &mut Vec<datafusion::common::Column>) {
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
    let _ = e.apply(|node| {
        if let Expr::Column(c) = node {
            out.push(c.clone());
        }
        Ok(TreeNodeRecursion::Continue)
    });
}

/// Ungrouped aggregation: partials per worker, gather to partition 0, recombine.
fn global_aggregation_stages(p: &Peeled<'_>, sharded: &[&str]) -> Result<DistributedQuery> {
    if sharded.len() != 1 {
        return Err(Error::Unsupported(format!(
            "auto-distribute: global aggregation needs exactly one sharded table, found {}",
            sharded.len()
        )));
    }
    let sharded_name = sharded[0];
    if count_table_scans(&p.agg.input, sharded_name) > 1 {
        return Err(Error::Unsupported(format!(
            "auto-distribute: sharded table `{sharded_name}` scanned multiple times"
        )));
    }
    // Per-shard DISTINCT + partial COUNT/SUM, then combine, double-counts keys that land on
    // more than one worker (TPC-DS Q87: 496 vs 494). Needs a shuffle-by-distinct-key stage
    // before the global aggregate; until then decline.
    if plan_contains_distinct(&p.agg.input) {
        return Err(Error::Unsupported(
            "auto-distribute: global aggregation over DISTINCT of a sharded table is not \
             supported (per-shard DISTINCT would double-count cross-shard keys)"
                .into(),
        ));
    }
    reject_unsafe_broadcast_shapes(&p.agg.input, sharded_name)?;
    let input_sql = Unparser::default()
        .plan_to_sql(&p.agg.input)
        .map_err(|e| Error::Unsupported(format!("auto-distribute: unparse input: {e}")))?
        .to_string();
    let tail = extract_from_tail(&input_sql)?;
    let tail = sanitize_generated_sql(&tail);

    let aggs = p
        .agg
        .aggr_expr
        .iter()
        .map(AggSpec::classify)
        .collect::<Result<Vec<_>>>()?;
    if aggs.iter().any(|a| a.distinct) {
        return global_distinct_aggregation_stages(p, &tail, &aggs);
    }

    let remap = build_remap(p);

    let mut psel = Vec::new();
    let mut combine = Vec::new();
    for (i, a) in aggs.iter().enumerate() {
        let (sel, comb) = partial_combine_sql(&a.func, i, &a.arg_sql)?;
        psel.extend(sel);
        combine.push(comb);
    }

    let partial_sql = sanitize_generated_sql(&format!("SELECT {} {tail}", psel.join(", ")));
    // HAVING COUNT(*) > 0 drops the all-null row workers with an empty shuffle bucket would emit.
    let inner = format!(
        "SELECT {} FROM shuffle_input HAVING COUNT(*) > 0",
        combine.join(", ")
    );
    let final_sql = wrap_output(p, &inner, &remap)?;
    Ok(DistributedQuery {
        stages: vec![
            StageDef::new(0, partial_sql, vec![], vec![]),
            StageDef::new(1, final_sql, vec![0], vec![]),
        ],
        finalize_sql: build_finalize(p)?,
    })
}

/// Global (ungrouped) aggregation with a `COUNT(DISTINCT x)` in the list (a DISTINCT-carrying
/// CrossJoin branch no shared-scan merge group claims — e.g. TPC-DS Q28's bucket aggregates
/// before merging, or a singleton / DISTINCT-incompatible branch — see [`super::dag_splitter`]).
///
/// A per-shard `DISTINCT` + recombine would double-count values that land on more than one
/// worker, so rows are hash-shuffled **by the DISTINCT argument** instead — every equal value
/// co-locates on one partition, making each partition's `COUNT(DISTINCT x)` exact:
///
/// 1. **Partial dedup**: `GROUP BY` the DISTINCT argument (emitted as `c{i}`) and pre-aggregate
///    the recombinable partial state (`sum`/`count`/`min`/`max`/`avg` pieces) of the
///    non-DISTINCT aggregates per value. One row per *locally distinct* value crosses the
///    exchange instead of every matching fact row (Q28 at SF10: ~440k raw rows per branch
///    shrink to the per-worker distinct-value count).
/// 2. **Per-partition aggregate**: `count(DISTINCT c{i})` (exact by co-location) plus a
///    recombine of the pre-aggregated state columns. A global aggregate emits exactly one row
///    per partition — identity values over an empty bucket (0 counts, NULL sums), so nothing is
///    double- or mis-counted.
/// 3. **Gather-combine**: `sum` the per-partition distinct counts and recombine the partials on
///    partition 0; `HAVING COUNT(*) > 0` suppresses the synthetic empty-partition row, same as
///    the non-DISTINCT global path.
///
/// Restricted to DISTINCT **count** aggregates that all share one argument expression; anything
/// else keeps the honest [`Error::Unsupported`].
fn global_distinct_aggregation_stages(
    p: &Peeled<'_>,
    tail: &str,
    aggs: &[AggSpec],
) -> Result<DistributedQuery> {
    let distinct_args: Vec<&str> = aggs
        .iter()
        .filter(|a| a.distinct)
        .map(|a| a.arg_sql.as_str())
        .collect();
    if distinct_args.is_empty()
        || distinct_args.iter().any(|arg| *arg != distinct_args[0])
        || aggs.iter().any(|a| a.distinct && a.func != "count")
    {
        return Err(Error::Unsupported(
            "auto-distribute: global DISTINCT aggregates are only supported as \
             COUNT(DISTINCT x) over a single shared argument"
                .into(),
        ));
    }

    // Stage 0: per-worker partial dedup — the DISTINCT argument column(s) first (they all share
    // one argument, so the first position hashes identically to any), then each non-DISTINCT
    // aggregate's partial state, grouped by the argument.
    let mut psel: Vec<String> = Vec::new();
    let mut group_sql: Vec<String> = Vec::new();
    for (i, a) in aggs.iter().enumerate() {
        if a.distinct {
            psel.push(format!("{} AS c{i}", a.arg_sql));
            if !group_sql.contains(&a.arg_sql) {
                group_sql.push(a.arg_sql.clone());
            }
        }
    }
    for (i, a) in aggs.iter().enumerate() {
        if a.distinct {
            continue;
        }
        let (sel, _comb) = partial_combine_sql(&a.func, i, &a.arg_sql)?;
        psel.extend(sel);
    }
    let partial_sql = sanitize_generated_sql(&format!(
        "SELECT {} {tail} GROUP BY {}",
        psel.join(", "),
        group_sql.join(", ")
    ));

    // Stage 1: exact per-partition distinct counts + a recombine of the stage-0 partial state
    // (under the same `a{i}…` names the gather-combine has always read).
    let mut mid_sel = Vec::with_capacity(aggs.len());
    let mut combine = Vec::with_capacity(aggs.len());
    for (i, a) in aggs.iter().enumerate() {
        if a.distinct {
            mid_sel.push(format!("count(DISTINCT c{i}) AS d{i}"));
            combine.push(format!("sum(d{i}) AS r{i}"));
        } else {
            mid_sel.extend(recombine_partial_state_sql(&a.func, i)?);
            let (_sel, comb) = partial_combine_sql(&a.func, i, &format!("c{i}"))?;
            combine.push(comb);
        }
    }
    let mid_sql =
        sanitize_generated_sql(&format!("SELECT {} FROM shuffle_input", mid_sel.join(", ")));

    // Stage 2: gather and recombine. The empty-bucket synthetic row reads as NULLs / zero
    // counts; HAVING COUNT(*) > 0 keeps only partition 0's real row.
    let inner = format!(
        "SELECT {} FROM shuffle_input HAVING COUNT(*) > 0",
        combine.join(", ")
    );
    let remap = build_remap(p);
    let final_sql = wrap_output(p, &inner, &remap)?;
    Ok(DistributedQuery {
        stages: vec![
            // The DISTINCT argument column(s) lead the stage-0 select list; all share one
            // argument, so hashing by the first position co-locates every equal value.
            StageDef::new(0, partial_sql, vec![], vec![0]),
            StageDef::new(1, mid_sql, vec![0], vec![]),
            StageDef::new(2, final_sql, vec![1], vec![]),
        ],
        finalize_sql: build_finalize(p)?,
    })
}

/// Recombine one aggregate's partial state columns (the `a{i}…` emitted by the stage-0 partial
/// dedup) into per-partition totals under the same names, so the final gather-combine reads
/// them unchanged. Mirrors the state layout of [`partial_combine_sql`].
pub(crate) fn recombine_partial_state_sql(func: &str, i: usize) -> Result<Vec<String>> {
    match func {
        "sum" | "count" => Ok(vec![format!("sum(a{i}) AS a{i}")]),
        "min" => Ok(vec![format!("min(a{i}) AS a{i}")]),
        "max" => Ok(vec![format!("max(a{i}) AS a{i}")]),
        "avg" => Ok(vec![format!("sum(a{i}s) AS a{i}s, sum(a{i}c) AS a{i}c")]),
        "stddev" | "var" | "stddev_pop" | "var_pop" => Ok(vec![format!(
            "sum(a{i}s) AS a{i}s, sum(a{i}q) AS a{i}q, sum(a{i}c) AS a{i}c"
        )]),
        other => Err(Error::Unsupported(format!(
            "auto-distribute: aggregate `{other}` not supported"
        ))),
    }
}

/// Shuffle-join two sharded tables, then run the grouped aggregation.
pub(crate) fn shuffle_join_two_tables(
    p: &Peeled<'_>,
    sharded: &[&str],
) -> Result<DistributedQuery> {
    let join = find_inner_equijoin(&p.agg.input)?;
    let (key_pairs, residual_filter) = collect_equijoin_keys(&join.on, join.filter.as_ref())?;

    let left_scan = simple_table_scan(join.left.as_ref())?;
    let right_scan = simple_table_scan(join.right.as_ref())?;
    let left_name = left_scan.table;
    let right_name = right_scan.table;
    if !(sharded.contains(&left_name) && sharded.contains(&right_name)) {
        return Err(Error::Unsupported(
            "auto-distribute: shuffle join sides must be the two sharded tables".into(),
        ));
    }

    let mut left_key_idxs = Vec::with_capacity(key_pairs.len());
    let mut right_key_idxs = Vec::with_capacity(key_pairs.len());
    let mut on_parts = Vec::with_capacity(key_pairs.len());
    let left_alias = left_scan.alias.unwrap_or(left_name);
    let right_alias = right_scan.alias.unwrap_or(right_name);
    for (left_key_expr, right_key_expr) in &key_pairs {
        let left_key_name = column_name(left_key_expr)?;
        let right_key_name = column_name(right_key_expr)?;
        left_key_idxs.push(column_index_in_scan(&left_scan, &left_key_name)?);
        right_key_idxs.push(column_index_in_scan(&right_scan, &right_key_name)?);
        on_parts.push(format!(
            "{left_alias}.{left_key_name} = {right_alias}.{right_key_name}"
        ));
    }

    let left_sql = match &left_scan.filter_sql {
        Some(f) => format!("SELECT * FROM {} WHERE {f}", left_scan.table_sql),
        None => format!("SELECT * FROM {}", left_scan.table_sql),
    };
    let right_sql = match &right_scan.filter_sql {
        Some(f) => format!("SELECT * FROM {} WHERE {f}", right_scan.table_sql),
        None => format!("SELECT * FROM {}", right_scan.table_sql),
    };

    let up = Unparser::default();
    let group_sql: Vec<String> = p
        .agg
        .group_expr
        .iter()
        .map(|g| expr_sql(&up, g))
        .collect::<Result<_>>()?;
    let aggs = p
        .agg
        .aggr_expr
        .iter()
        .map(AggSpec::classify)
        .collect::<Result<Vec<_>>>()?;

    let remap = build_remap(p);

    let on_sql = on_parts.join(" AND ");
    let mut join_tail = format!(
        "FROM shuffle_input_0 AS {left_alias} JOIN shuffle_input_1 AS {right_alias} ON {on_sql}"
    );
    if let Some(residual) = residual_filter.as_ref() {
        join_tail.push_str(&format!(" WHERE {}", expr_sql(&up, residual)?));
    }

    let (partial_sql, final_sql) = if aggs.iter().any(|a| a.distinct) {
        distinct_stage_sql(&up, p, &group_sql, &aggs, &join_tail, &remap)?
    } else {
        recombine_stage_sql(p, &group_sql, &aggs, &join_tail, &remap)?
    };

    // Stage 3 has a single upstream, so Flight registers it as `shuffle_input` (not `_2`).
    let hash_group: Vec<u32> = (0..group_sql.len() as u32).collect();
    Ok(DistributedQuery {
        stages: vec![
            StageDef::new(0, sanitize_generated_sql(&left_sql), vec![], left_key_idxs),
            StageDef::new(
                1,
                sanitize_generated_sql(&right_sql),
                vec![],
                right_key_idxs,
            ),
            StageDef::new(2, partial_sql, vec![0, 1], hash_group),
            StageDef::new(3, final_sql, vec![2], vec![]),
        ],
        finalize_sql: build_finalize(p)?,
    })
}

/// A leaf table scan, optionally filtered, with an optional SQL alias.
pub(crate) struct SimpleScan<'a> {
    /// Bare table name (used for replicate/shard policy matching).
    pub(crate) table: &'a str,
    /// Catalog-qualified SQL relation text for stage `FROM` clauses (KAN-4).
    ///
    /// Workers resolve unqualified names to `spark_catalog.default.*`; Glue SF100 tables live
    /// under `glue.<db>.<table>`, so leaf stage SQL must preserve the logical plan's
    /// [`TableReference`] qualification.
    pub(crate) table_sql: String,
    pub(crate) alias: Option<&'a str>,
    pub(crate) filter_sql: Option<String>,
    pub(crate) schema: datafusion::common::DFSchemaRef,
    /// The scan's logical-plan row-count statistic (KAN-160), from the table provider's
    /// `TableProvider::statistics()` — exact parquet-footer counts for lakehouse tables,
    /// `Absent` for providers without statistics (e.g. `MemTable`). Read by the semi-join
    /// filter admission gate in `join_chain`; plan consumers must treat it as the
    /// UNFILTERED table cardinality (an upper bound once `filter_sql` applies).
    pub(crate) stats_num_rows: datafusion::common::stats::Precision<usize>,
}

fn find_inner_equijoin(lp: &LogicalPlan) -> Result<&datafusion::logical_expr::Join> {
    let mut node = lp;
    loop {
        match node {
            LogicalPlan::Projection(p) => node = p.input.as_ref(),
            LogicalPlan::Filter(f) => node = f.input.as_ref(),
            LogicalPlan::Join(j) => {
                use datafusion::logical_expr::JoinType;
                if j.join_type != JoinType::Inner {
                    return Err(Error::Unsupported(
                        "auto-distribute: only INNER shuffle joins are supported".into(),
                    ));
                }
                return Ok(j);
            }
            other => {
                return Err(Error::Unsupported(format!(
                    "auto-distribute: expected a join under aggregate, found `{}`",
                    other.display().to_string().lines().next().unwrap_or("")
                )));
            }
        }
    }
}

pub(crate) fn simple_table_scan(lp: &LogicalPlan) -> Result<SimpleScan<'_>> {
    match lp {
        LogicalPlan::TableScan(s) => Ok(SimpleScan {
            table: s.table_name.table(),
            table_sql: table_ref_sql(&s.table_name),
            alias: None,
            filter_sql: None,
            schema: s.projected_schema.clone(),
            stats_num_rows: datafusion::datasource::source_as_provider(&s.source)
                .ok()
                .and_then(|p| p.statistics())
                .map(|stats| stats.num_rows)
                .unwrap_or(datafusion::common::stats::Precision::Absent),
        }),
        LogicalPlan::SubqueryAlias(sa) => {
            let mut inner = simple_table_scan(sa.input.as_ref())?;
            inner.alias = Some(sa.alias.table());
            Ok(inner)
        }
        LogicalPlan::Filter(f) => {
            let mut inner = simple_table_scan(f.input.as_ref())?;
            let up = Unparser::default();
            let pred = expr_sql(&up, f.predicate.as_ref())?;
            inner.filter_sql = Some(match inner.filter_sql {
                Some(prev) => format!("({prev}) AND ({pred})"),
                None => pred,
            });
            Ok(inner)
        }
        LogicalPlan::Projection(p) => simple_table_scan(p.input.as_ref()),
        other => Err(Error::Unsupported(format!(
            "auto-distribute: shuffle join side must be a table scan, found `{}`",
            other.display().to_string().lines().next().unwrap_or("")
        ))),
    }
}

pub(crate) fn flatten_and_conjuncts(expr: &Expr, out: &mut Vec<Expr>) {
    use datafusion::logical_expr::Operator;
    match expr {
        Expr::BinaryExpr(b) if b.op == Operator::And => {
            flatten_and_conjuncts(&b.left, out);
            flatten_and_conjuncts(&b.right, out);
        }
        _ => out.push(expr.clone()),
    }
}

/// Equijoin key pairs `(left, right)` plus optional non-equality residual filter.
pub(crate) type EquijoinKeys = (Vec<(Expr, Expr)>, Option<Expr>);

/// Collect equijoin key pairs from `ON` plus every equality conjunct in `filter` (KAN-10).
///
/// DataFusion often parks composite `ON a=b AND c=d` as a single `on` pair plus an equality
/// residual in `join.filter`. Those residual equalities must become hash keys too — leaving them
/// as a post-shuffle `WHERE` is correct for INNER only when the first key alone co-locates rows,
/// but hashing the full composite key is what D-2.7 requires and avoids skew-sensitive bugs.
pub(crate) fn collect_equijoin_keys(
    on: &[(Expr, Expr)],
    filter: Option<&Expr>,
) -> Result<EquijoinKeys> {
    use datafusion::logical_expr::Operator;

    let mut keys: Vec<(Expr, Expr)> = on.to_vec();
    let mut residual_parts: Vec<Expr> = Vec::new();

    if let Some(filter) = filter {
        let mut conjuncts = Vec::new();
        flatten_and_conjuncts(filter, &mut conjuncts);
        for expr in conjuncts {
            match expr {
                Expr::BinaryExpr(b) if b.op == Operator::Eq => {
                    keys.push((*b.left, *b.right));
                }
                other => residual_parts.push(other),
            }
        }
    }

    if keys.is_empty() {
        return Err(Error::Unsupported(
            "auto-distribute: shuffle join needs an equijoin key (on or filter)".into(),
        ));
    }
    let residual = residual_parts.into_iter().reduce(Expr::and);
    Ok((keys, residual))
}

#[cfg(test)]
mod equijoin_filter_tests {
    use super::collect_equijoin_keys;
    use datafusion::prelude::{col, lit};

    #[test]
    fn extracts_equality_keys_and_preserves_non_equality_residual() {
        let filter = col("a").eq(col("b")).and(col("c").gt(lit(1_i64)));

        let (keys, residual) = collect_equijoin_keys(&[], Some(&filter))
            .expect("equality conjunct should be accepted");

        assert_eq!(keys, vec![(col("a"), col("b"))]);
        assert_eq!(residual, Some(col("c").gt(lit(1_i64))));
    }

    #[test]
    fn promotes_all_filter_equalities_to_composite_keys() {
        let filter = col("a")
            .eq(col("b"))
            .and(col("c").eq(col("d")))
            .and(col("e").gt(lit(1_i64)));
        let (keys, residual) = collect_equijoin_keys(&[], Some(&filter)).unwrap();
        assert_eq!(keys.len(), 2);
        assert_eq!(residual, Some(col("e").gt(lit(1_i64))));
    }
}

pub(crate) fn column_name(e: &Expr) -> Result<String> {
    match e {
        Expr::Column(c) => Ok(c.name.clone()),
        other => Err(Error::Unsupported(format!(
            "auto-distribute: join key must be a column, found {other}"
        ))),
    }
}

fn column_index_in_scan(scan: &SimpleScan<'_>, name: &str) -> Result<u32> {
    for (i, f) in scan.schema.fields().iter().enumerate() {
        if f.name() == name {
            return Ok(i as u32);
        }
    }
    let needle = name.to_ascii_lowercase();
    for (i, f) in scan.schema.fields().iter().enumerate() {
        if f.name().to_ascii_lowercase() == needle {
            return Ok(i as u32);
        }
    }
    Err(Error::Unsupported(format!(
        "auto-distribute: join key `{name}` not found in table `{}`",
        scan.table
    )))
}

/// Build the global finalize query (`ORDER BY` / `LIMIT` over the gathered `result` table), or
/// `None` when the query has neither. Sort exprs reference output columns; `result` carries those
/// under their unqualified output names, so column refs are unqualified (e.g. `lineitem.l_returnflag`
/// → `l_returnflag`, matching `wrap_output`'s aliasing) before unparsing.
pub(crate) fn build_finalize(p: &Peeled) -> Result<Option<String>> {
    if p.sort.is_none() && p.limit.is_none() {
        return Ok(None);
    }
    let up = Unparser::default();
    let mut sql = String::from("SELECT * FROM result");
    if let Some(sorts) = p.sort {
        let parts = sorts
            .iter()
            .map(|s| {
                let dir = if s.asc { "ASC" } else { "DESC" };
                let nulls = if s.nulls_first {
                    "NULLS FIRST"
                } else {
                    "NULLS LAST"
                };
                Ok(format!(
                    "{} {dir} {nulls}",
                    finalize_expr_sql(&up, &unqualify(&s.expr))?
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        if !parts.is_empty() {
            sql.push_str(&format!(" ORDER BY {}", parts.join(", ")));
        }
    }
    if let Some(n) = p.limit {
        sql.push_str(&format!(" LIMIT {n}"));
    }
    Ok(Some(sql))
}

/// One aggregate in the SELECT list, classified for partial/final decomposition.
pub(crate) struct AggSpec {
    /// Lowercased function name (`sum`/`count`/`min`/`max`/`avg`).
    pub(crate) func: String,
    /// SQL of the (single) argument, e.g. `t.v` (or `1` for `count(*)`).
    pub(crate) arg_sql: String,
    /// Whether the aggregate is `DISTINCT`.
    pub(crate) distinct: bool,
    /// For a `grouping(col)` aggregate under a grouping set: the flattened group-column index
    /// `j` the final stage recomputes as `grouping(g{j})` (resolved by
    /// [`resolve_grouping_specs`]). The partial stage emits nothing for it — a finest-level
    /// `grouping()` value is always 0 and cannot recombine into the rolled-up levels.
    pub(crate) grouping_target: Option<u32>,
}

/// Partial-stage `SELECT` fragment(s) and final-stage combine expression for one aggregate at
/// output position `i`, given its (DataFusion-canonical, lowercased) function name and argument
/// SQL. Shared by `global_aggregation_stages` and `recombine_stage_sql`, which differ only in
/// group-by handling around this per-aggregate decomposition.
pub(crate) fn partial_combine_sql(
    func: &str,
    i: usize,
    arg_sql: &str,
) -> Result<(Vec<String>, String)> {
    match func {
        "sum" => Ok((
            vec![format!("sum({arg_sql}) AS a{i}")],
            format!("sum(a{i}) AS r{i}"),
        )),
        "count" => Ok((
            vec![format!("count({arg_sql}) AS a{i}")],
            format!("sum(a{i}) AS r{i}"), // counts recombine by summing
        )),
        "min" => Ok((
            vec![format!("min({arg_sql}) AS a{i}")],
            format!("min(a{i}) AS r{i}"),
        )),
        "max" => Ok((
            vec![format!("max({arg_sql}) AS a{i}")],
            format!("max(a{i}) AS r{i}"),
        )),
        "avg" => Ok((
            vec![format!(
                "sum({arg_sql}) AS a{i}s, count({arg_sql}) AS a{i}c"
            )],
            // No cast: SUM/COUNT keep DataFusion's own AVG result type (a DECIMAL average
            // stays DECIMAL at the same scale). Forcing DOUBLE here made TPC-DS Q7/Q26 return
            // numerically-right values at the wrong scale (`120.65` vs `120.650000`).
            format!("(sum(a{i}s) / NULLIF(sum(a{i}c), 0)) AS r{i}"),
        )),
        // stddev/stddev_samp/var/var_samp (Spark's `stddev`, `stddev_samp`, `variance`, `var_samp`,
        // `var_sample` all resolve to these DataFusion-canonical names) recombine from the partial
        // sum, sum-of-squares, and count via the parallel variance identity
        // `Var = (Σx² - (Σx)²/n) / (n-1)` (sample) or `/ n` (population); stddev is `sqrt(Var)`.
        "stddev" | "var" => {
            let sel = vec![format!(
                "sum({arg_sql}) AS a{i}s, sum(({arg_sql})*({arg_sql})) AS a{i}q, count({arg_sql}) AS a{i}c"
            )];
            let combine = format!(
                "(sum(a{i}q) - (sum(a{i}s)*sum(a{i}s))/NULLIF(sum(a{i}c),0)) / NULLIF(sum(a{i}c)-1, 0)"
            );
            let combine = if func == "stddev" {
                format!("sqrt({combine}) AS r{i}")
            } else {
                format!("{combine} AS r{i}")
            };
            Ok((sel, combine))
        }
        "stddev_pop" | "var_pop" => {
            let sel = vec![format!(
                "sum({arg_sql}) AS a{i}s, sum(({arg_sql})*({arg_sql})) AS a{i}q, count({arg_sql}) AS a{i}c"
            )];
            let combine = format!(
                "(sum(a{i}q) - (sum(a{i}s)*sum(a{i}s))/NULLIF(sum(a{i}c),0)) / NULLIF(sum(a{i}c), 0)"
            );
            let combine = if func == "stddev_pop" {
                format!("sqrt({combine}) AS r{i}")
            } else {
                format!("{combine} AS r{i}")
            };
            Ok((sel, combine))
        }
        other => Err(Error::Unsupported(format!(
            "auto-distribute: aggregate `{other}` not supported"
        ))),
    }
}

impl AggSpec {
    pub(crate) fn classify(e: &Expr) -> Result<AggSpec> {
        // An aggregate written `sum(x) AS total` arrives wrapped in an alias.
        let e = match e {
            Expr::Alias(a) => a.expr.as_ref(),
            other => other,
        };
        let Expr::AggregateFunction(af) = e else {
            return Err(Error::Unsupported(format!(
                "auto-distribute: non-aggregate in aggregate list: {e}"
            )));
        };
        let func = af.func.name().to_ascii_lowercase();
        let up = Unparser::default();
        let arg_sql = match af.params.args.first() {
            Some(a) => expr_sql(&up, a)?,
            None => "1".to_string(), // count(*) carries no arg
        };
        Ok(AggSpec {
            func,
            arg_sql,
            distinct: af.params.distinct,
            grouping_target: None,
        })
    }
}

/// Resolve `grouping(col)` aggregate specs (valid only under a grouping set) to the flattened
/// group-column index the final combine recomputes as `grouping(g{j})`. The partial stage
/// cannot compute a meaningful value — at the finest level `grouping()` is always 0 — so it
/// emits nothing and the combine evaluates `grouping()` against the real `GROUP BY ROLLUP`.
pub(crate) fn resolve_grouping_specs(aggs: &mut [AggSpec], group_expr: &[Expr]) -> Result<()> {
    if !aggs.iter().any(|a| a.func == "grouping") {
        return Ok(());
    }
    if !is_grouping_set(group_expr) {
        return Err(Error::Unsupported(
            "auto-distribute: grouping() aggregate without a grouping set".into(),
        ));
    }
    let up = Unparser::default();
    let flattened = flattened_group_exprs(group_expr)
        .into_iter()
        .map(|g| expr_sql(&up, g))
        .collect::<Result<Vec<_>>>()?;
    for spec in aggs.iter_mut() {
        if spec.func != "grouping" {
            continue;
        }
        let j = flattened
            .iter()
            .position(|g| g == &spec.arg_sql)
            .ok_or_else(|| {
                Error::Unsupported(format!(
                    "auto-distribute: grouping() argument `{}` is not a flattened group column",
                    spec.arg_sql
                ))
            })?;
        spec.grouping_target = Some(j as u32);
    }
    Ok(())
}

/// Re-combinable path (no DISTINCT): partial aggregates per worker, final recombines.
pub(crate) fn recombine_stage_sql(
    p: &Peeled,
    group_sql: &[String],
    aggs: &[AggSpec],
    tail: &str,
    remap: &HashMap<String, String>,
) -> Result<(String, String)> {
    // Partial SELECT list: group cols as g{j}, then per-aggregate partial state. Final combine
    // SELECT list (over `shuffle_input`): g{j} group cols + recombined aggregates.
    let (psel, combine) = partial_and_combine_lists(group_sql, aggs)?;

    let group_by = group_sql.join(", ");
    let partial_sql = sanitize_generated_sql(&format!(
        "SELECT {} {tail} GROUP BY {group_by}",
        psel.join(", ")
    ));
    let final_group_by = final_group_by_sql(&p.agg.group_expr, group_sql.len())?;
    let reject_empty_partition = if is_grouping_set(&p.agg.group_expr) {
        // Empty shuffle buckets would otherwise each emit the empty grouping-set row (the ROLLUP
        // grand total). Only partition 0 receives rows on the grouping-set gather.
        " HAVING COUNT(*) > 0"
    } else {
        ""
    };
    let inner = format!(
        "SELECT {} FROM shuffle_input GROUP BY {final_group_by}{reject_empty_partition}",
        combine.join(", "),
    );
    let final_sql = wrap_output(p, &inner, remap)?;
    Ok((partial_sql, final_sql))
}

/// Whether a grouping set distributes as a **two-phase** rollup: every grouping level except the
/// grand total contains the first flattened group column `g0`.
///
/// `ROLLUP(a, b, …)` always qualifies — its levels are prefixes of the written list, so every
/// non-grand-total level starts with `a`. Explicit `GROUPING SETS` qualify level-by-level (any
/// level containing `g0` co-locates when hashed by `g0`), except a duplicated `()` level: the
/// fixup *sums* per-partition grand-total partials, so it cannot reproduce the duplicate
/// grand-total rows single-node emits for a repeated empty set — that shape keeps the gather
/// plan (duplicate non-empty levels are fine: their rows pass through the fixup verbatim).
/// `CUBE` never qualifies (it emits sibling levels like `(b)` that a `g0` hash would split).
/// When this returns false the caller keeps the partition-0 gather plan.
fn grouping_set_shares_first_column(group_expr: &[Expr]) -> bool {
    let [Expr::GroupingSet(gs)] = group_expr else {
        return false;
    };
    let Some(first) = flattened_group_exprs(group_expr).into_iter().next() else {
        return false;
    };
    match gs {
        GroupingSet::Rollup(_) => true,
        GroupingSet::Cube(_) => false,
        GroupingSet::GroupingSets(levels) => {
            levels
                .iter()
                .all(|level| level.is_empty() || level.iter().any(|e| e == first))
                && levels.iter().filter(|level| level.is_empty()).count() <= 1
        }
    }
}

/// Two-phase distributed grouping sets (TPC-DS Q67), replacing the partition-0 gather
/// [`aggregation_stages_for`] otherwise falls into for a grouping set (see
/// [`recombine_stage_sql`]'s empty-partition note): the gather funnelled every finest-level
/// partial row into one partition for a single-threaded `ROLLUP` (Q67 at SF10: 4.84M rows, a
/// 3.8s one-core stage).
///
/// Three stages instead of two:
///
/// 1. **Partial** (unchanged SQL): finest-level `GROUP BY` per worker — but hash-shuffled by
///    `g0` only. Every level of the grouping set except the grand total contains `g0` (see
///    [`grouping_set_shares_first_column`]), so every such level's groups land wholly on one
///    partition; finest-level rows with `g0 NULL` hash-consistently co-locate too.
/// 2. **Per-partition rollup**: the same `GROUP BY ROLLUP (…)` recombine the gather plan runs
///    once, now parallel — each partition rolls up only its `g0` slice. Levels containing `g0`
///    come out **exact** (no cross-partition combine needed); the grand-total level yields one
///    *partial* row per partition (its slice's total), tagged `grouping(g0) AS __gid` = 1 so the
///    fixup can find it. The `HAVING COUNT(*) > 0` empty-partition guard carries over verbatim:
///    it drops the synthetic grand-total row of a partition whose bucket is empty, so an empty
///    input still yields zero rows cluster-wide. Output is hash-shuffled by `g0` again: exact
///    rows stay in their partition class, and every grand-total partial (`g0 NULL`, `__gid = 1`)
///    lands in the single NULL bucket — the "grand-total partials hash to one bucket" funnel.
/// 3. **Grand-total fixup / output**: per-partition `UNION ALL` of the exact rows
///    (`WHERE __gid = 0`, a passthrough) and the combined grand total (`WHERE __gid = 1` —
///    ≤ #partitions rows, all in the NULL bucket, recombined there by one task; every other
///    partition's synthetic empty-input row is dropped by the same `HAVING COUNT(*) > 0` guard).
///    Recombining rides the ordinary [`partial_combine_sql`] decomposition: `sum`/`count`/`min`/
///    `max` re-aggregate the partials' `r{i}` directly; `avg`/`stddev`/`var*` re-aggregate the
///    component columns stage 2 carried along (`a{i}s`/`a{i}q`/`a{i}c`); `grouping()` outputs
///    take `max(r{i})` (every grand-total partial has grouping = 1, and `max` preserves the
///    `grouping()` return type).
///
/// Returns `Ok(None)` for shapes the two-phase plan cannot reproduce exactly — no grouping set,
/// no grouping columns, or a level that does not share `g0` (CUBE, or explicit sets like
/// `(b), (a, b)`) — so the caller keeps the gather plan. HAVING / output projections need no
/// special casing: stage 3 emits the same `g{j}`/`r{i}` schema the gather plan's combine emitted,
/// so [`wrap_output`] composes unchanged.
fn grouping_set_two_phase_stages(
    p: &Peeled<'_>,
    group_sql: &[String],
    aggs: &[AggSpec],
    tail: &str,
    remap: &HashMap<String, String>,
) -> Result<Option<DistributedQuery>> {
    if group_sql.is_empty() || !grouping_set_shares_first_column(&p.agg.group_expr) {
        return Ok(None);
    }

    // Stage 0: the ordinary finest-level partial (identical SQL to the gather plan's), keyed by
    // g0 — select position 0 — instead of gathered.
    let (psel, _combine) = partial_and_combine_lists(group_sql, aggs)?;
    let group_by = group_sql.join(", ");
    let partial_sql = sanitize_generated_sql(&format!(
        "SELECT {} {tail} GROUP BY {group_by}",
        psel.join(", ")
    ));

    // Stage 1: per-partition rollup. `r{i}` is the final value for the exact rows and the
    // per-partition value for the grand-total partial; avg/stddev/var additionally carry their
    // recombine components for the stage-2 grand-total fixup.
    let mut rollup_sel: Vec<String> = (0..group_sql.len()).map(|j| format!("g{j}")).collect();
    for (i, a) in aggs.iter().enumerate() {
        if let Some(j) = a.grouping_target {
            rollup_sel.push(format!("grouping(g{j}) AS r{i}"));
            continue;
        }
        let (_sel, comb) = partial_combine_sql(&a.func, i, &a.arg_sql)?;
        rollup_sel.push(comb);
        if matches!(
            a.func.as_str(),
            "avg" | "stddev" | "var" | "stddev_pop" | "var_pop"
        ) {
            rollup_sel.extend(recombine_partial_state_sql(&a.func, i)?);
        }
    }
    rollup_sel.push("grouping(g0) AS __gid".to_string());
    let final_group_by = final_group_by_sql(&p.agg.group_expr, group_sql.len())?;
    let rollup_sql = sanitize_generated_sql(&format!(
        "SELECT {} FROM shuffle_input GROUP BY {final_group_by} HAVING COUNT(*) > 0",
        rollup_sel.join(", ")
    ));

    // Stage 2: exact rows pass through; grand-total partials combine on the NULL bucket.
    let mut passthrough: Vec<String> = (0..group_sql.len()).map(|j| format!("g{j}")).collect();
    let mut grand: Vec<String> = (0..group_sql.len())
        .map(|j| format!("NULL AS g{j}"))
        .collect();
    for (i, a) in aggs.iter().enumerate() {
        passthrough.push(format!("r{i}"));
        let comb = if a.grouping_target.is_some() {
            format!("max(r{i}) AS r{i}")
        } else {
            match a.func.as_str() {
                "sum" | "count" => format!("sum(r{i}) AS r{i}"),
                "min" => format!("min(r{i}) AS r{i}"),
                "max" => format!("max(r{i}) AS r{i}"),
                // The component columns stage 1 carried are the ordinary recombine input.
                "avg" | "stddev" | "var" | "stddev_pop" | "var_pop" => {
                    partial_combine_sql(&a.func, i, &a.arg_sql)?.1
                }
                other => {
                    return Err(Error::Unsupported(format!(
                        "auto-distribute: aggregate `{other}` not supported"
                    )))
                }
            }
        };
        grand.push(comb);
    }
    let inner = format!(
        "SELECT {} FROM shuffle_input WHERE __gid = 0 UNION ALL \
         SELECT {} FROM shuffle_input WHERE __gid = 1 HAVING COUNT(*) > 0",
        passthrough.join(", "),
        grand.join(", ")
    );
    let final_sql = wrap_output(p, &inner, remap)?;

    Ok(Some(DistributedQuery {
        stages: vec![
            StageDef::new(0, partial_sql, vec![], vec![0]),
            StageDef::new(1, rollup_sql, vec![0], vec![0]),
            StageDef::new(2, final_sql, vec![1], vec![]),
        ],
        finalize_sql: build_finalize(p)?,
    }))
}

/// DISTINCT path: shuffle the raw grouping + argument columns by group key, run the original
/// aggregate in the final stage (exact, since each group is co-located on one worker).
pub(crate) fn distinct_stage_sql(
    _up: &Unparser,
    p: &Peeled,
    group_sql: &[String],
    aggs: &[AggSpec],
    tail: &str,
    remap: &HashMap<String, String>,
) -> Result<(String, String)> {
    // Partial: project group cols (g{j}) and each aggregate's argument column (c{i}); no aggregation.
    let mut psel: Vec<String> = group_sql
        .iter()
        .enumerate()
        .map(|(j, g)| format!("{g} AS g{j}"))
        .collect();
    for (i, a) in aggs.iter().enumerate() {
        if a.grouping_target.is_some() {
            continue; // recomputed on the final stage — see resolve_grouping_specs
        }
        psel.push(format!("{} AS c{i}", a.arg_sql));
    }
    let partial_sql = sanitize_generated_sql(&format!("SELECT {} {tail}", psel.join(", ")));

    // Final: re-run each aggregate over the projected columns, grouped by g{j}.
    let mut combine: Vec<String> = (0..group_sql.len()).map(|j| format!("g{j}")).collect();
    for (i, a) in aggs.iter().enumerate() {
        if let Some(j) = a.grouping_target {
            combine.push(format!("grouping(g{j}) AS r{i}"));
            continue;
        }
        let d = if a.distinct { "DISTINCT " } else { "" };
        combine.push(format!("{}({d}c{i}) AS r{i}", a.func));
    }
    let final_group_by = final_group_by_sql(&p.agg.group_expr, group_sql.len())?;
    let reject_empty_partition = if is_grouping_set(&p.agg.group_expr) {
        " HAVING COUNT(*) > 0"
    } else {
        ""
    };
    let inner = format!(
        "SELECT {} FROM shuffle_input GROUP BY {final_group_by}{reject_empty_partition}",
        combine.join(", "),
    );
    let final_sql = wrap_output(p, &inner, remap)?;
    Ok((partial_sql, final_sql))
}

/// Map the aggregate's output column names to the safe stage names (`g{j}` group, `r{i}` result).
///
/// Keyed three ways, because callers reach these columns under different names: the expression's
/// `schema_name` (how the plan refers to it), an explicit `AS` alias on the group/aggregate expr,
/// and the `Aggregate`'s own schema field names.
pub(crate) fn build_agg_remap(agg: &Aggregate) -> HashMap<String, String> {
    let mut remap: HashMap<String, String> = HashMap::new();
    let flattened_groups = flattened_group_exprs(&agg.group_expr);
    for (j, g) in flattened_groups.iter().enumerate() {
        remap.insert(g.schema_name().to_string(), format!("g{j}"));
        if let Expr::Alias(a) = g {
            remap.insert(a.name.clone(), format!("g{j}"));
        }
    }
    for (i, a) in agg.aggr_expr.iter().enumerate() {
        remap.insert(a.schema_name().to_string(), format!("r{i}"));
        if let Expr::Alias(al) = a {
            remap.insert(al.name.clone(), format!("r{i}"));
        }
    }
    let n_group = flattened_groups.len();
    for (j, field) in agg.schema.fields().iter().take(n_group).enumerate() {
        remap.insert(field.name().clone(), format!("g{j}"));
    }
    // DataFusion inserts a hidden `__grouping_id` field between the flattened group fields and the
    // aggregate fields. It is not part of `aggr_expr` and must not consume an `r{i}` position.
    let agg_field_offset = n_group + usize::from(is_grouping_set(&agg.group_expr));
    for (i, field) in agg
        .schema
        .fields()
        .iter()
        .skip(agg_field_offset)
        .take(agg.aggr_expr.len())
        .enumerate()
    {
        remap.insert(field.name().clone(), format!("r{i}"));
    }
    remap
}

/// [`build_agg_remap`] extended with [`Peeled::alias_projections`], so a `HAVING` written against
/// an intervening subquery's aliases (TPC-DS Q21's `inv_before`) still resolves to `r{i}` / `g{j}`.
///
/// KAN-162 q64: a post-aggregate `Filter` can also sit ABOVE the output projection (the
/// branch-aware CrossJoin splitter pushes `cs1.syear = …` onto a branch whose SELECT-list
/// projection introduced `syear`), in which case the peel makes that projection the output
/// projection and the predicate can only resolve through its aliases. Fold those in too —
/// insert-if-absent, so every mapping a below-the-projection `HAVING` can legitimately resolve
/// keeps winning exactly as before (only previously-declining references gain a mapping).
pub(crate) fn build_remap(p: &Peeled<'_>) -> HashMap<String, String> {
    let mut remap = build_agg_remap(p.agg);
    for proj in &p.alias_projections {
        for e in proj.iter() {
            let Expr::Alias(a) = e else { continue };
            let mapped = match a.expr.as_ref() {
                Expr::Column(c) => remap
                    .get(&c.flat_name())
                    .or_else(|| remap.get(&c.name))
                    .cloned(),
                other => remap.get(&other.schema_name().to_string()).cloned(),
            };
            if let Some(mapped) = mapped {
                remap.insert(a.name.clone(), mapped);
            }
        }
    }
    if let Some(exprs) = p.projection {
        for e in exprs.iter() {
            let Expr::Alias(a) = e else { continue };
            if remap.contains_key(&a.name) {
                continue;
            }
            let mapped = match a.expr.as_ref() {
                Expr::Column(c) => remap
                    .get(&c.flat_name())
                    .or_else(|| remap.get(&c.name))
                    .cloned(),
                other => remap.get(&other.schema_name().to_string()).cloned(),
            };
            if let Some(mapped) = mapped {
                remap.insert(a.name.clone(), mapped);
            }
        }
    }
    remap
}

/// Expression substitutions for alias-projection names that do **not** map to a single
/// `g{j}`/`r{i}` column under [`build_remap`]: an alias of an *expression* over aggregate outputs
/// (TPC-DS Q39's `stddev_samp(inv_quantity_on_hand)*1.000 AS stdev`, referenced by the CTE's
/// HAVING and output projection as `foo.stdev`) can still be evaluated on the final stage once
/// its aggregate references are replaced by the recombined `r{i}` columns. Each entry inlines the
/// fully-remapped expression wherever the alias is referenced; aliases whose expression still
/// references anything unmapped are omitted (the caller's [`ensure_all_columns_remapped`] then
/// declines, same as before).
pub(crate) fn build_expr_substs(
    p: &Peeled<'_>,
    remap: &HashMap<String, String>,
) -> HashMap<String, Expr> {
    use datafusion::common::tree_node::{Transformed, TreeNode};
    let mut substs = HashMap::new();
    for proj in &p.alias_projections {
        for e in proj.iter() {
            let Expr::Alias(a) = e else { continue };
            // The plain name remap already covers this alias — leave it on the cheaper path.
            if remap.contains_key(&a.name) {
                continue;
            }
            let mut resolved = true;
            let mapped = a
                .expr
                .clone()
                .transform(|node| {
                    match &node {
                        // An aggregate whose recombine lands in `r{i}` (matched by schema name,
                        // the same key `build_agg_remap` uses) evaluates there.
                        Expr::AggregateFunction(_) => {
                            let key = node.schema_name().to_string();
                            match remap.get(&key) {
                                Some(safe) => {
                                    return Ok(Transformed::yes(datafusion::prelude::col(safe)));
                                }
                                None => resolved = false,
                            }
                        }
                        Expr::Column(c) => {
                            match remap.get(&c.flat_name()).or_else(|| remap.get(&c.name)) {
                                Some(safe) => {
                                    return Ok(Transformed::yes(datafusion::prelude::col(safe)));
                                }
                                None => resolved = false,
                            }
                        }
                        // Aliases the planner sprinkles inside an expression (the schema-name
                        // alias on a bare aggregate) carry no value — drop them or the inlined
                        // expression would unparse `x AS name` in operand position.
                        Expr::Alias(alias) => {
                            return Ok(Transformed::yes(alias.expr.as_ref().clone()));
                        }
                        _ => {}
                    }
                    Ok(Transformed::no(node))
                })
                .map(|t| t.data)
                .unwrap_or_else(|_| a.expr.as_ref().clone());
            if resolved {
                substs.insert(a.name.clone(), mapped);
            }
        }
    }
    substs
}

/// Wrap the combined inner query so the final stage's output matches the original query's columns:
/// re-apply the output projection with aggregate/group columns remapped to `r{i}`/`g{j}`, each
/// item explicitly aliased back to its original output name (so a bare `t.k` stays column `k`, and
/// downstream `ORDER BY` over those names resolves). `ORDER BY` / `LIMIT` are *not* applied here —
/// they're global and run in [`build_finalize`].
pub(crate) fn wrap_output(
    p: &Peeled<'_>,
    inner: &str,
    remap: &HashMap<String, String>,
) -> Result<String> {
    wrap_output_impl(p, inner, remap, false)
}

/// `wrap_output` for the union-split recombine stages (the `merged_arms` combine): identical,
/// except that when no output projection sits above the aggregate the internal `g{j}` / `r{i}`
/// columns are re-aliased back to the aggregate's schema field names (see [`agg_output_select`])
/// instead of `SELECT *`. Scoped to these call sites because the sub-DAG output invariant
/// ([`super::dag_splitter::placeholder_plan`]) is what a name-based consumer — the derived-leg
/// export stage — binds by; every other `wrap_output` consumer either applies an explicit
/// projection or reads the internal names (e.g. a stacked window stage references `g{j}`).
pub(crate) fn wrap_output_recombine(
    p: &Peeled<'_>,
    inner: &str,
    remap: &HashMap<String, String>,
) -> Result<String> {
    wrap_output_impl(p, inner, remap, true)
}

fn wrap_output_impl(
    p: &Peeled<'_>,
    inner: &str,
    remap: &HashMap<String, String>,
    realias_none: bool,
) -> Result<String> {
    let up = Unparser::default();
    let substs = build_expr_substs(p, remap);
    // Apply HAVING against remapped `g{j}`/`r{i}` columns *before* the output projection aliases
    // them back to original names (otherwise `WHERE r0 > …` fails against `having_in.sv`).
    let from_sql = if p.having.is_empty() {
        format!("({inner}) AS combined")
    } else {
        let mut preds = Vec::with_capacity(p.having.len());
        for pred in &p.having {
            let mapped = remap_columns(&unqualify(pred), remap, &substs);
            ensure_all_columns_remapped(&mapped)?;
            preds.push(format!("({})", expr_sql(&up, &mapped)?));
        }
        let having_sql = preds.join(" AND ");
        format!("(SELECT * FROM ({inner}) AS combined WHERE {having_sql}) AS having_in")
    };
    let select = match p.projection {
        Some(exprs) => exprs
            .iter()
            .map(|e| {
                let name = output_name(e);
                let sql = expr_sql(&up, &remap_columns(strip_alias(e), remap, &substs))?;
                Ok(format!("{sql} AS \"{name}\""))
            })
            .collect::<Result<Vec<_>>>()?
            .join(", "),
        None if realias_none => agg_output_select(p).unwrap_or_else(|| "*".to_string()),
        None => "*".to_string(),
    };
    Ok(format!("SELECT {select} FROM {from_sql}"))
}

/// The `wrap_output` select list when no output projection sits above the aggregate (e.g. a
/// derived leg's `DISTINCT` rewritten to its group-by equivalent): the plan's output columns
/// are the aggregate's own schema fields, so re-alias the internal `g{j}` / `r{i}` columns
/// back to those field names. Stage outputs must carry the logical plan's field names — a
/// name-based consumer (the derived-leg export stage) binds by them, and emitting the raw
/// `g{j}` names only ever worked for positional consumers.
///
/// Returns `None` — caller keeps the historical `SELECT *` — when the field names can't be
/// re-aliased safely: an unexpected field count, or duplicate field names (DataFusion permits
/// duplicate unqualified names under different qualifiers; re-aliasing would collide).
fn agg_output_select(p: &Peeled<'_>) -> Option<String> {
    let n_group = flattened_group_exprs(&p.agg.group_expr).len();
    let offset = n_group + usize::from(is_grouping_set(&p.agg.group_expr));
    let fields = p.agg.schema.fields();
    if fields.len() != offset + p.agg.aggr_expr.len() {
        return None;
    }
    let names: Vec<&str> = fields
        .iter()
        .take(n_group)
        .chain(fields.iter().skip(offset))
        .map(|f| f.name().as_str())
        .collect();
    if names.iter().collect::<HashSet<_>>().len() != names.len() {
        return None;
    }
    let mut items = Vec::with_capacity(names.len());
    for (j, name) in names.iter().take(n_group).enumerate() {
        items.push(format!("g{j} AS \"{name}\""));
    }
    for (i, name) in names.iter().skip(n_group).enumerate() {
        items.push(format!("r{i} AS \"{name}\""));
    }
    Some(items.join(", "))
}

pub(crate) fn output_name(e: &Expr) -> String {
    match e {
        Expr::Alias(a) => a.name.clone(),
        Expr::Column(c) => c.name.clone(),
        other => other.schema_name().to_string(),
    }
}

/// The expr without its alias layer(s) (so we can re-alias after remapping). Stacked aliases —
/// TPC-H Q13's `count(Int64(1)) AS count(*) AS custdist` — strip fully: keeping an inner alias
/// would splice `r0 AS "count(*)" AS "custdist"`, which is not valid SQL.
pub(crate) fn strip_alias(e: &Expr) -> &Expr {
    match e {
        Expr::Alias(a) => strip_alias(&a.expr),
        other => other,
    }
}

/// Drop the table qualifier from every column reference (e.g. `lineitem.l_returnflag` →
/// `l_returnflag`), so a sort over the gathered `result` table resolves against its unqualified
/// output column names.
pub(crate) fn unqualify(e: &Expr) -> Expr {
    use datafusion::common::tree_node::{Transformed, TreeNode};
    e.clone()
        .transform(|node| {
            if let Expr::Column(c) = &node {
                return Ok(Transformed::yes(datafusion::prelude::col(c.name.clone())));
            }
            Ok(Transformed::no(node))
        })
        .map(|t| t.data)
        .unwrap_or(e.clone())
}

/// Replace any column reference whose flat name is in `remap` with the safe-named column; a
/// column naming an entry of `substs` (an expression-aliased aggregate output — see
/// [`build_expr_substs`]) is replaced by that expression instead.
fn remap_columns(
    e: &Expr,
    remap: &HashMap<String, String>,
    substs: &HashMap<String, Expr>,
) -> Expr {
    use datafusion::common::tree_node::{Transformed, TreeNode};
    e.clone()
        .transform(|node| {
            if let Expr::Column(c) = &node {
                if let Some(sub) = substs.get(&c.flat_name()).or_else(|| substs.get(&c.name)) {
                    return Ok(Transformed::yes(sub.clone()));
                }
                if let Some(safe) = remap.get(&c.flat_name()).or_else(|| remap.get(&c.name)) {
                    return Ok(Transformed::yes(datafusion::prelude::col(safe)));
                }
            }
            Ok(Transformed::no(node))
        })
        .map(|t| t.data)
        .unwrap_or(e.clone())
}

/// Require every column in an already-remapped predicate to name a `g{j}` / `r{i}` stage column.
///
/// Anything left un-remapped refers to a name that only existed in the original plan, so the
/// predicate would either fail on the worker or — worse, if the name happens to collide — filter
/// on the wrong column. Decline the query instead.
fn ensure_all_columns_remapped(e: &Expr) -> Result<()> {
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
    let mut bad: Option<String> = None;
    let _ = e.apply(|node| {
        if let Expr::Column(c) = node {
            let safe = c.relation.is_none()
                && matches!(c.name.as_bytes(), [b'g' | b'r', rest @ ..]
                    if !rest.is_empty() && rest.iter().all(u8::is_ascii_digit));
            if !safe {
                bad = Some(c.flat_name());
                return Ok(TreeNodeRecursion::Stop);
            }
        }
        Ok(TreeNodeRecursion::Continue)
    });
    match bad {
        Some(name) => Err(Error::Unsupported(format!(
            "auto-distribute: HAVING references `{name}`, which does not map to an aggregate or \
             group output column"
        ))),
        None => Ok(()),
    }
}

/// Unparse an expr to SQL text.
pub(crate) fn expr_sql(up: &Unparser, e: &Expr) -> Result<String> {
    up.expr_to_sql(e)
        .map(|ast| sanitize_generated_sql(&ast.to_string()))
        .map_err(|err| Error::Unsupported(format!("auto-distribute: unparse expr: {err}")))
}

/// [`expr_sql`] for expression positions in a **finalize** query (which the engine parses under
/// the Databricks dialect). The Unparser double-quotes identifiers that collide with reserved
/// words (`"value"`, TPC-H Q11's `ORDER BY value DESC`), but the Databricks dialect reads double
/// quotes as *string literals* — so `ORDER BY "value"` silently sorted by a constant and
/// returned the gather order. The Unparser only double-quotes identifiers (its string literals
/// are single-quoted), so re-quoting them as backticks — which the dialect treats as identifiers
/// — is faithful.
pub(crate) fn finalize_expr_sql(up: &Unparser, e: &Expr) -> Result<String> {
    let sql = expr_sql(up, e)?;
    let mut out = String::with_capacity(sql.len());
    let mut chars = sql.chars();
    while let Some(c) = chars.next() {
        if c != '"' {
            out.push(c);
            continue;
        }
        let mut ident = String::new();
        for c in chars.by_ref() {
            if c == '"' {
                break;
            }
            ident.push(c);
        }
        out.push('`');
        out.push_str(&ident);
        out.push('`');
    }
    Ok(out)
}

/// Extract the `FROM …` tail from an unparsed aggregate input.
///
/// DataFusion's unparser yields `SELECT * FROM …` for a plain scan, but a join of N inputs can
/// become `SELECT *, *, … FROM …`. We only need the FROM/JOIN/WHERE suffix to splice a new SELECT.
pub(crate) fn extract_from_tail(input_sql: &str) -> Result<String> {
    if let Some(rest) = input_sql.strip_prefix("SELECT * ") {
        // Only accept when the remainder starts with FROM (not `*, * FROM`).
        if rest.starts_with("FROM ") || rest.starts_with("from ") {
            return Ok(rest.to_string());
        }
    }
    let bytes = input_sql.as_bytes();
    let upper = input_sql.to_ascii_uppercase();
    let mut depth = 0i32;
    let mut i = 0;
    while i + 6 <= upper.len() {
        match bytes[i] {
            b'(' => depth += 1,
            b')' => depth = depth.saturating_sub(1),
            _ if depth == 0 && upper[i..].starts_with(" FROM ") => {
                return Ok(input_sql[i + 1..].to_string()); // "FROM …"
            }
            _ => {}
        }
        i += 1;
    }
    Err(Error::Unsupported(
        "auto-distribute: non-trivial aggregate input (no FROM tail)".into(),
    ))
}

/// If `agg.input` contains a `UNION` where at least one arm scans `sharded_name` and at least one
/// other arm scans it zero times, plan the two arm groups with different **placements** instead
/// of flatly rejecting the query (see [`reject_unsafe_broadcast_shapes`] for why a uniform
/// broadcast is unsafe here — TPC-DS Q33/Q56/Q60/Q66/Q71/Q76's per-channel `UNION ALL`, one arm
/// per fact table):
///
/// - the sharded arm(s) become an ordinary partial-aggregate producer stage (one per worker,
///   hash-shuffled by the outer `GROUP BY` key) — unchanged from the single-arm path.
/// - the replicated-only arm(s) become a **second** producer stage using the very same partial
///   SQL shape and the same hash key. Its placement is chosen by [`replicated_slice_tables`]:
///   on a multi-worker cluster every worker computes the partial over a disjoint 1/W file
///   slice of each replicated arm's anchor table (TPC-DS Q71 — the `store_sales`/`web_sales`
///   arms no longer serialize on one worker); otherwise exactly one worker computes it
///   ([`ExchangeMode::Forward`] — see the driver's producer loop): every worker holds identical
///   data for these arms, so computing the (already-exact) partial once per disjoint slice —
///   or once total for `Forward` — and shuffling it by the shared group key merges correctly
///   with the sharded arms' genuine per-worker partials, instead of being replicated once per
///   worker and multiplying the total.
/// - the final combine stage reads *both* producer stages (`shuffle_input_0`/`shuffle_input_1`,
///   the same multi-upstream shape [`crate::plan::join_chain`] uses for shuffle joins) and
///   recombines exactly as the single-arm path would.
///
/// `Ok(None)` — falling through to the flat [`reject_unsafe_broadcast_shapes`] guard — when: no
/// `Union` is found under `agg.input`; every arm (or no arm) scans `sharded_name`, so there is
/// nothing to place differently; the aggregate has a `DISTINCT` aggregate (not yet composed with
/// this split); the `Union` sits under a plan node this function does not know how to rebuild
/// with a narrowed child (only single-child nodes and `Join` are supported — see
/// [`split_union_by_sharding`]); or the narrowed sharded side still contains an aggregate and the
/// SUM-only guard below does not hold (KAN-54).
///
/// Grouping sets (`ROLLUP`/`CUBE`/`GROUPING SETS`) compose exactly with this split: the partials
/// gather to partition 0 (empty hash key), so the final stage sees every finest-level group whole
/// before rebuilding the super-aggregate levels — the same argument as the single-arm path, with
/// `HAVING COUNT(*) > 0` suppressing the empty partitions' synthetic grand-total row (KAN-49d,
/// TPC-DS Q5/Q77/Q80). Two sharded-arm shapes need more than the flat tail-splice and are
/// planned by dedicated compositions before the naive path runs:
///
/// - **a replicated aggregate joined into the sharded arm above its own aggregate**
///   (Q77's `ss LEFT JOIN sr` — sales-per-key `LEFT JOIN` returns-per-key): splicing the whole
///   arm per worker attaches the replicated side's per-key *totals* to every worker's partial row
///   for that key, and the outer SUM re-adds them once per worker (the KAN-54 doubling trap —
///   verified wrong at sf0.01 before this composition existed). Planned by
///   [`try_split_union_agg_join`]: per-worker partials of the sharded aggregate co-locate with
///   the replicated aggregate (computed once), and the join runs **after** the per-key recombine.
/// - **an aggregate over another mixed union nested inside the sharded arm** (Q5's
///   `store_sales UNION ALL store_returns` feeding the per-store GROUP BY): planned by
///   [`try_split_union_nested`], which distributes the arm with the ordinary machinery and
///   adapts its exact output rows into the outer partial schema.
fn try_split_broadcast_union(
    p: &Peeled<'_>,
    sharded_name: &str,
    replicated: &[&str],
) -> Result<Option<DistributedQuery>> {
    let agg = p.agg;
    let aggs = agg
        .aggr_expr
        .iter()
        .map(AggSpec::classify)
        .collect::<Result<Vec<_>>>()?;
    if aggs.iter().any(|a| a.distinct) {
        return Ok(None);
    }
    let Some((sharded_input, replicated_input)) =
        split_union_by_sharding(&agg.input, sharded_name)?
    else {
        return Ok(None);
    };

    // KAN-49d (TPC-DS Q77): a replicated aggregate joined into the sharded arm above the arm's
    // own aggregate would silently double under the naive path — route it to the join-deferral
    // composition (or refuse) before anything else runs. The CROSS JOIN spelling (Q77's catalog
    // arm, `FROM cs, cr`) routes the same way: DataFusion 54 has no `LogicalPlan::CrossJoin` —
    // a cross join is a `LogicalPlan::Join` with `join_type: Inner`, empty `on`, and no filter
    // (its Display prints "Cross Join:"), so this finder already matches it.
    if find_replicated_agg_join(&sharded_input, sharded_name).is_some() {
        return try_split_union_agg_join(
            p,
            &sharded_input,
            sharded_name,
            &replicated_input,
            replicated,
        );
    }

    // KAN-49d (TPC-DS Q5): the sharded arm is itself an aggregate over another mixed union.
    // Distribute the arm recursively, then adapt its exact rows into the outer partial schema.
    if let Ok(arm_peeled) = peel(&sharded_input) {
        if arm_peeled.sort.is_none()
            && arm_peeled.limit.is_none()
            && split_union_by_sharding(&arm_peeled.agg.input, sharded_name)?.is_some()
        {
            return try_split_union_nested(p, &arm_peeled, &replicated_input, replicated);
        }
    }

    // The sharded side keeps every safety check the single-arm path would have run on the whole
    // input — nested Unions, self-joins, outer joins whose preserved side misses the sharded
    // table, etc. — just scoped to the narrower subtree. The replicated side is, by construction,
    // untouched by any of those (it scans `sharded_name` zero times, so every check below is a
    // vacuous pass), so it needs no further validation here.
    reject_unsafe_broadcast_shapes(&sharded_input, sharded_name)?;
    let scans = count_table_scans(&sharded_input, sharded_name);
    if scans > 1 {
        return Err(Error::Unsupported(format!(
            "auto-distribute: sharded table `{sharded_name}` scanned {scans}× \
             (self-join / subquery) — not broadcast-safe"
        )));
    }

    // KAN-54 (TPC-DS Q33/Q56/Q60): when the narrowed sharded side still contains an aggregate
    // (a pre-aggregated per-channel arm), the partial stage recomputes that inner GROUP BY per
    // worker. A key present on w workers then contributes w inner rows where the single-node
    // plan has one, and each carries a per-worker partial value rather than the key's total.
    // An outer SUM still composes exactly — partial sums re-add to the key total regardless of
    // row multiplicity — but COUNT/AVG read the inflated multiplicity and MIN/MAX compare
    // partials instead of totals, so those must keep refusing. The inner aggregates must
    // decompose additively for the same reason (SUM/COUNT only, never DISTINCT), must be
    // leaf-level (an aggregate under an aggregate is KAN-44's composition, not this one's),
    // and must not use grouping sets (per-worker ROLLUP levels do not re-add so naively).
    let mut inner_aggs = Vec::new();
    collect_aggregates(&sharded_input, &mut inner_aggs);
    if !inner_aggs.is_empty() {
        let inner_ok = inner_aggs.iter().all(|a| {
            let mut nested = Vec::new();
            collect_aggregates(&a.input, &mut nested);
            nested.is_empty()
                && !is_grouping_set(&a.group_expr)
                && a.aggr_expr.iter().all(|e| {
                    AggSpec::classify(e)
                        .map(|s| !s.distinct && matches!(s.func.as_str(), "sum" | "count"))
                        .unwrap_or(false)
                })
        });
        let outer_ok = agg.aggr_expr.iter().all(|e| {
            AggSpec::classify(e)
                .map(|s| !s.distinct && s.func == "sum")
                .unwrap_or(false)
        });
        if !inner_ok || !outer_ok {
            return Ok(None);
        }
    }

    let up = Unparser::default();
    let group_sql: Vec<String> = flattened_group_exprs(&agg.group_expr)
        .into_iter()
        .map(|g| expr_sql(&up, g))
        .collect::<Result<_>>()?;
    let remap = build_remap(p);

    let sharded_tail = union_split_tail(&sharded_input)?;
    let (psel, _combine) = partial_and_combine_lists(&group_sql, &aggs)?;
    let group_by = group_sql.join(", ");

    let sharded_partial = sanitize_generated_sql(&format!(
        "SELECT {} {sharded_tail} GROUP BY {group_by}",
        psel.join(", ")
    ));
    let stages = vec![StageDef::new(0, sharded_partial, vec![], vec![])];
    split_union_finish(
        p,
        &group_sql,
        &aggs,
        &remap,
        stages,
        &replicated_input,
        replicated,
    )
    .map(Some)
}

/// Shared tail of every union-split plan: the replicated-side partial producer plus the final
/// combine over the two producer streams.
///
/// `stages` is the sharded side's stage DAG; its last stage must already emit the `g{j}`/`a{i}`
/// partial schema (group columns first, then per-aggregate partials — exactly what
/// [`partial_and_combine_lists`] produces). This helper assigns that stage's hash key (group-key
/// hash, or the partition-0 gather for grouping sets), appends the replicated-side partial with
/// the identical select list, and closes with the recombine + output wrap.
///
/// The replicated-side partial has two placements, chosen by [`replicated_slice_tables`]:
///
/// - **one worker** ([`ExchangeMode::Forward`], the original design): every worker holds
///   identical data for these arms, so computing the (already-exact) partial once and
///   shuffling it by the shared group key merges correctly with the sharded arms' genuine
///   per-worker partials, instead of being replicated once per worker and multiplying totals.
/// - **replicated-slice producers** (multi-worker clusters): the stage runs on EVERY worker,
///   each scanning a disjoint 1/W slice of each replicated arm's anchor table — the same
///   size-weighted file assignment sharded tables use (`oxidant_loom::shard`). The stage's
///   replicate stamp drops the sliced tables so the workers' file sharder treats them as
///   sharded for this stage only; every other replicated table in the stage (the shared
///   dimensions) is still scanned in full, so each arm's joins stay co-located within its
///   slice. Per-slice partials have the same shape as the sharded side's per-worker partials
///   and recombine exactly in the unchanged combine below. The result is deterministic
///   regardless of worker count: the file→worker assignment is a pure function of the
///   (location-sorted) listing, so every worker derives the same disjoint cover, and exact
///   recombination makes the output independent of how the rows were partitioned.
#[allow(clippy::too_many_arguments)]
fn split_union_finish(
    p: &Peeled<'_>,
    group_sql: &[String],
    aggs: &[AggSpec],
    remap: &HashMap<String, String>,
    mut stages: Vec<StageDef>,
    replicated_input: &LogicalPlan,
    replicated: &[&str],
) -> Result<DistributedQuery> {
    let replicated_tail = union_split_tail(replicated_input)?;
    let (psel, combine) = partial_and_combine_lists(group_sql, aggs)?;
    let group_by = group_sql.join(", ");
    let replicated_partial = sanitize_generated_sql(&format!(
        "SELECT {} {replicated_tail} GROUP BY {group_by}",
        psel.join(", ")
    ));

    // Grouping sets (ROLLUP/CUBE/GROUPING SETS — TPC-DS Q77/Q80) gather everything to partition 0
    // instead of hashing by key, same as the single-arm path (see `aggregation_stages_for`): a
    // grand-total level spans multiple finest-level keys, which a per-key hash can't co-locate.
    let grouping_set = is_grouping_set(&p.agg.group_expr);
    let hash_key_cols: Vec<u32> = if grouping_set {
        vec![]
    } else {
        (0..group_sql.len() as u32).collect()
    };
    let sharded_output = stages.last_mut().ok_or_else(|| {
        Error::Unsupported("auto-distribute: union split has no sharded-side stages".into())
    })?;
    sharded_output.hash_key_cols = hash_key_cols.clone();
    let sharded_output_id = sharded_output.stage_id;

    let replicated_id = sharded_output_id + 1;
    let mut replicated_stage =
        StageDef::new(replicated_id, replicated_partial, vec![], hash_key_cols);
    match sliced_replicate_stamp(replicated_input, replicated) {
        Some(stamp) => {
            // Sliced placement: dispatch to every worker (default worker-indexed exchange) and
            // drop the sliced tables from this stage's replicate stamp so their scans shard.
            // `stamp_replicated_tables` preserves a non-empty stamp.
            replicated_stage.replicated_tables = stamp;
        }
        None => replicated_stage.exchange = ExchangeMode::Forward,
    }

    let final_group_by = final_group_by_sql(&p.agg.group_expr, group_sql.len())?;
    // Matches `recombine_stage_sql`: with an empty hash key, every rendezvous partition but 0
    // gets zero gathered rows yet would still emit the ROLLUP grand-total row for that emptiness.
    let reject_empty_partition = if grouping_set {
        " HAVING COUNT(*) > 0"
    } else {
        ""
    };
    let inner = format!(
        "SELECT {} FROM (SELECT * FROM shuffle_input_0 UNION ALL SELECT * FROM shuffle_input_1) \
         AS merged_arms GROUP BY {final_group_by}{reject_empty_partition}",
        combine.join(", ")
    );
    let final_sql = wrap_output(p, &inner, remap)?;
    let combine_stage = StageDef::new(
        replicated_id + 1,
        final_sql,
        vec![sharded_output_id, replicated_id],
        vec![],
    );

    stages.push(replicated_stage);
    stages.push(combine_stage);
    Ok(DistributedQuery {
        stages,
        finalize_sql: build_finalize(p)?,
    })
}

/// KAN-162: aggregate over a `UNION ALL` whose arms collectively scan two or more sharded
/// tables — e.g. TPC-DS Q2's `web_sales`+`catalog_sales` union or Q33/Q56/Q60/Q76's
/// per-channel unions at the all-facts-sharded SF100 classification (KAN-161's 4 GiB
/// auto-broadcast threshold shards every sales fact, so these no longer fit
/// [`try_split_broadcast_union`]'s exactly-one-sharded-arm shape, and the shuffle join chain
/// has no UNION vocabulary).
///
/// Admission (per union arm, reusing KAN-161's predicate): the arm scans **at most one**
/// sharded table, **exactly once**, in a broadcast-safe tree
/// (`count_table_scans(arm, t) == 1 && reject_unsafe_broadcast_shapes(arm, t).is_ok()`).
/// Arms are bucketed by that table; arms scanning only replicated tables form one optional
/// replicated bucket. The plan generalizes the two-producer union split to N sharded arms:
///
/// 1. **one partial-aggregate producer per bucket**: the bucket's (rebuilt) arm slice —
///    arm-local sharded scan + broadcast dims — partially aggregated by the group key,
///    hash-shuffled by that key. The replicated bucket keeps the single-sharded path's
///    placement (sliced per-worker partials when sliceable, else one `Forward` worker).
///    A bucket whose arm joins a REPLICATED aggregate into the sharded arm's own aggregate
///    (KAN-49d's Q77 shape) cannot use the flat partial — its per-key totals would attach
///    once per worker partial — so it gets the join-deferral stage chain of
///    [`union_agg_join_bucket_stages`] (per-bucket sharded partial + once-computed replicated
///    aggregate co-located by the join key, recombined per key BEFORE the join), whose stage-D
///    terminal is the bucket's producer.
/// 2. **one associative recombine**: `UNION ALL` of every producer stream, re-aggregated by
///    the group key — exact because union arms partition the input rows and hash co-location
///    puts every partial for a group key on one partition.
///
/// Every check failure returns `Ok(None)` — this is a pure admission gate layered in front of
/// the shuffle join chain, so a shape it declines keeps the chain's existing refusal.
fn try_split_multi_sharded_union(
    p: &Peeled<'_>,
    replicated: &[&str],
) -> Result<Option<DistributedQuery>> {
    let agg = p.agg;
    let aggs = agg
        .aggr_expr
        .iter()
        .map(AggSpec::classify)
        .collect::<Result<Vec<_>>>()?;
    if aggs.iter().any(|a| a.distinct) {
        return Ok(None);
    }
    let Some(groups) = split_union_by_sharding_multi(&agg.input, replicated)? else {
        return Ok(None);
    };
    // This composition only exists for the multi-sharded union; a single sharded bucket (or
    // none) belongs to `try_split_broadcast_union` / the flat broadcast path below.
    if groups.iter().filter(|(key, _)| key.is_some()).count() < 2 {
        return Ok(None);
    }

    // Validate each rebuilt producer the way the single-sharded path validates its narrowed
    // side: the ancestor chain above the union (broadcast dim joins, projections, filters) is
    // now folded into the bucket's plan, so re-derive the bucket's sharded set and re-run the
    // broadcast-safe tree check on the result. A join above the union that pulls in a second
    // sharded table, or an unsafe preserved-side outer join up there, declines the whole shape.
    let mut agg_join_bucket: Vec<bool> = Vec::with_capacity(groups.len());
    let mut left_join_bucket: Vec<bool> = Vec::with_capacity(groups.len());
    for (key, plan) in &groups {
        let mut is_agg_join = false;
        let mut is_left_join = false;
        match key {
            Some(t) => {
                let plan_tables = base_tables(plan);
                let mut plan_sharded: Vec<&str> = plan_tables
                    .iter()
                    .map(String::as_str)
                    .filter(|tb| !replicated.contains(tb))
                    .collect();
                plan_sharded.sort_unstable();
                plan_sharded.dedup();
                if plan_sharded != [t.as_str()] {
                    return Ok(None);
                }
                // KAN-162 (TPC-DS Q5's web leg): a bucket chaining over a co-locatable LEFT
                // JOIN (see [`find_co_locatable_left_join`]) skips the broadcast-safe tree
                // check — [`union_left_join_branch_stages`]'s own gates replace it. The
                // `plan_sharded == [t]` re-derivation above and the KAN-54 additive check
                // below still run for it.
                if find_co_locatable_left_join(plan, t).is_some() {
                    is_left_join = true;
                } else {
                    if reject_unsafe_broadcast_shapes(plan, t).is_err() {
                        return Ok(None);
                    }
                    // KAN-162: KAN-49d's reroute (a replicated aggregate joined into the arm above
                    // the arm's own aggregate — TPC-DS Q77's `ss LEFT JOIN sr`) generalizes to the
                    // N-producer shape: each such bucket gets its own join-deferral stage chain
                    // (stage A–D per bucket, built and re-validated by
                    // [`union_agg_join_bucket_stages`], which declines the whole shape on a shape
                    // mismatch) instead of the flat partial below. The KAN-54 additive guard does
                    // not apply to it — the deferral is exact by the per-key-recombine-first
                    // argument, not by per-worker additivity. The CROSS JOIN spelling (Q77's
                    // catalog arm) is a `LogicalPlan::Join` in DataFusion 54 (Inner, empty `on`,
                    // no filter), so the one finder covers both spellings.
                    is_agg_join = find_replicated_agg_join(plan, t).is_some();
                }
            }
            None => {
                if base_tables(plan)
                    .iter()
                    .any(|tb| !replicated.contains(&tb.as_str()))
                {
                    return Ok(None);
                }
            }
        }
        if is_agg_join {
            agg_join_bucket.push(true);
            left_join_bucket.push(false);
            continue;
        }
        agg_join_bucket.push(false);
        left_join_bucket.push(is_left_join);
        // KAN-54's additive constraint, per producer: an inner aggregate recomputes per worker,
        // so it (and the outer aggregate over it) must re-associatively recombine — inner
        // SUM/COUNT only, leaf-level, no grouping sets; outer SUM only.
        let mut inner_aggs = Vec::new();
        collect_aggregates(plan, &mut inner_aggs);
        if !inner_aggs.is_empty() {
            let inner_ok = inner_aggs.iter().all(|a| {
                let mut nested = Vec::new();
                collect_aggregates(&a.input, &mut nested);
                nested.is_empty()
                    && !is_grouping_set(&a.group_expr)
                    && a.aggr_expr.iter().all(|e| {
                        AggSpec::classify(e)
                            .map(|s| !s.distinct && matches!(s.func.as_str(), "sum" | "count"))
                            .unwrap_or(false)
                    })
            });
            let outer_ok = agg.aggr_expr.iter().all(|e| {
                AggSpec::classify(e)
                    .map(|s| !s.distinct && s.func == "sum")
                    .unwrap_or(false)
            });
            if !inner_ok || !outer_ok {
                return Ok(None);
            }
        }
    }

    let up = Unparser::default();
    let group_sql: Vec<String> = flattened_group_exprs(&agg.group_expr)
        .into_iter()
        .map(|g| expr_sql(&up, g))
        .collect::<Result<_>>()?;
    let remap = build_remap(p);
    let (psel, combine) = partial_and_combine_lists(&group_sql, &aggs)?;
    let group_by = group_sql.join(", ");

    // Grouping sets gather everything to partition 0, same as `split_union_finish`.
    let grouping_set = is_grouping_set(&agg.group_expr);
    let hash_key_cols: Vec<u32> = if grouping_set {
        vec![]
    } else {
        (0..group_sql.len() as u32).collect()
    };

    let mut stages: Vec<StageDef> = Vec::with_capacity(groups.len() + 1);
    let mut producers: Vec<u32> = Vec::with_capacity(groups.len());
    for (((key, plan), is_agg_join), is_left_join) in
        groups.iter().zip(&agg_join_bucket).zip(&left_join_bucket)
    {
        if *is_left_join {
            // KAN-162: one co-located LEFT JOIN chain (R1 key-shuffle + R2 Forward shuffle +
            // R3 per-partition join producer) per such bucket; the terminal (R3) is the
            // bucket's producer into the shared recombine and takes the same group-key hash
            // (or partition-0 gather for grouping sets) as the flat producers.
            let t = key
                .as_deref()
                .expect("a co-located LEFT JOIN bucket is sharded");
            let Some(mut bucket) = union_left_join_branch_stages(p, plan, t, stages.len() as u32)?
            else {
                return Ok(None);
            };
            let terminal = bucket.last_mut().expect("co-located join bucket stages");
            terminal.hash_key_cols = hash_key_cols.clone();
            producers.push(terminal.stage_id);
            stages.extend(bucket);
            continue;
        }
        if *is_agg_join {
            // KAN-162: one stage-A–D join-deferral chain per agg-join bucket; the terminal
            // (stage D) is the bucket's producer into the shared recombine and takes the same
            // group-key hash (or partition-0 gather for grouping sets) as the flat producers.
            let t = key.as_deref().expect("an agg-join bucket is sharded");
            let Some(mut bucket) = union_agg_join_bucket_stages(p, plan, t, stages.len() as u32)?
            else {
                return Ok(None);
            };
            let terminal = bucket.last_mut().expect("join-deferral bucket stages");
            terminal.hash_key_cols = hash_key_cols.clone();
            producers.push(terminal.stage_id);
            stages.extend(bucket);
            continue;
        }
        let tail = union_split_tail(plan)?;
        let partial = sanitize_generated_sql(&format!(
            "SELECT {} {tail} GROUP BY {group_by}",
            psel.join(", ")
        ));
        let mut stage = StageDef::new(stages.len() as u32, partial, vec![], hash_key_cols.clone());
        if key.is_none() {
            // The replicated bucket is identical on every worker: slice it across workers when
            // the anchor tables allow, else compute it once and forward (see
            // `split_union_finish`).
            match sliced_replicate_stamp(plan, replicated) {
                Some(stamp) => stage.replicated_tables = stamp,
                None => stage.exchange = ExchangeMode::Forward,
            }
        }
        producers.push(stage.stage_id);
        stages.push(stage);
    }

    let final_group_by = final_group_by_sql(&agg.group_expr, group_sql.len())?;
    let reject_empty_partition = if grouping_set {
        " HAVING COUNT(*) > 0"
    } else {
        ""
    };
    let arm_reads: Vec<String> = (0..producers.len())
        .map(|i| format!("SELECT * FROM shuffle_input_{i}"))
        .collect();
    let inner = format!(
        "SELECT {} FROM ({}) AS merged_arms GROUP BY {final_group_by}{reject_empty_partition}",
        combine.join(", "),
        arm_reads.join(" UNION ALL ")
    );
    let final_sql = wrap_output_recombine(p, &inner, &remap)?;
    let combine_id = stages.len() as u32;
    stages.push(StageDef::new(combine_id, final_sql, producers, vec![]));

    Ok(Some(DistributedQuery {
        stages,
        finalize_sql: build_finalize(p)?,
    }))
}

/// Split a `Union` reachable from `lp` into one rebuilt plan per sharded table its arms scan
/// (plus one plan for the arms scanning only replicated tables), or `Ok(None)` when the shape
/// isn't the multi-sharded union split's: no `Union`, an arm scanning two or more sharded
/// tables, an arm scanning its sharded table more than once, or an arm failing the
/// broadcast-safe tree check (KAN-161's admission predicate, applied per arm).
///
/// One bucket of the multi-sharded union split: the bucket's single sharded table (`None` =
/// the replicated-only bucket) paired with its rebuilt arm-slice plan.
type ShardedUnionBucket = (Option<String>, LogicalPlan);

/// The descent/rebuild mirrors [`split_union_by_sharding`]: single-child nodes recurse and
/// rebuild via [`with_new_child`]; a multi-child node (a broadcast join above the union —
/// TPC-DS Q71's `item`/`time_dim` wrapper) is descended on the one child containing the
/// `Union`, with the other children cloned unchanged into every rebuilt plan.
fn split_union_by_sharding_multi(
    lp: &LogicalPlan,
    replicated: &[&str],
) -> Result<Option<Vec<ShardedUnionBucket>>> {
    if let LogicalPlan::Union(u) = lp {
        // Flatten nested unions before bucketing (TPC-DS keeps three-way set ops nested), same
        // as `split_union_by_sharding`.
        let mut arms = Vec::new();
        for input in &u.inputs {
            flatten_union_all(input, &mut arms);
        }
        // Arms are admitted FIFO. An arm that fails the direct per-arm admission may still
        // split: a nested MIXED union inside the arm (TPC-DS Q5's per-channel
        // `Aggregate over Join(Union(sales, returns) ⋈ dims)`) distributes exactly over the
        // enclosing chain — see [`distribute_over_nested_union`]. Each distributed branch is
        // pushed back onto the queue and re-admitted, so a branch that itself fails (or
        // contains a deeper nested union) recurses; every distribution removes one `Union`
        // node from the arm, so the queue drains.
        let mut pending: VecDeque<Arc<LogicalPlan>> = arms.into_iter().collect();
        let mut groups: Vec<(Option<String>, Vec<Arc<LogicalPlan>>)> = Vec::new();
        while let Some(arm) = pending.pop_front() {
            let mut sharded: Vec<String> = base_tables(&arm)
                .into_iter()
                .filter(|t| !replicated.contains(&t.as_str()))
                .collect();
            sharded.sort_unstable();
            sharded.dedup();
            let key = match sharded.as_slice() {
                [] => None,
                [t] if count_table_scans(&arm, t) == 1
                    && reject_unsafe_broadcast_shapes(&arm, t).is_ok() =>
                {
                    Some(t.clone())
                }
                // KAN-162 (TPC-DS Q5's web leg): an arm chaining over
                // `LeftJoin(replicated preserved, sharded null-extended)` fails the
                // broadcast-safe check above for exactly one reason — the co-locatable LEFT
                // JOIN. Admit it as its own bucket kind: push a SINGLETON group, bypassing
                // the same-key merge below, so it never folds into the flat `web_sales`
                // bucket (the flat producer would unparse the LEFT JOIN verbatim — the
                // inexact per-worker shape). `union_of_arms` collapses the singleton.
                [t] if count_table_scans(&arm, t) == 1
                    && find_co_locatable_left_join(&arm, t).is_some() =>
                {
                    groups.push((Some(t.clone()), vec![Arc::clone(&arm)]));
                    continue;
                }
                _ => {
                    match distribute_over_nested_union(&arm, replicated)? {
                        Some(branches) => {
                            // Re-admit each branch in place (order preserved) so the
                            // sales-side branches bucket by their sharded table and the
                            // all-replicated returns-side branches land in the replicated
                            // bucket.
                            for branch in branches.into_iter().rev() {
                                pending.push_front(branch);
                            }
                            continue;
                        }
                        None => return Ok(None),
                    }
                }
            };
            match groups.iter_mut().find(|(k, _)| *k == key) {
                Some((_, bucket)) => bucket.push(Arc::clone(&arm)),
                None => groups.push((key, vec![Arc::clone(&arm)])),
            }
        }
        // Rebuild each bucket. A rebuild failure (arm type coercion the strict `Union`
        // validator rejects even loosely) declines the shape rather than guessing.
        let mut out = Vec::with_capacity(groups.len());
        for (key, bucket) in groups {
            match union_of_arms(bucket) {
                Ok(plan) => out.push((key, plan)),
                Err(_) => return Ok(None),
            }
        }
        return Ok(Some(out));
    }

    let children = lp.inputs();
    if children.is_empty() {
        return Ok(None);
    }
    if children.len() == 1 {
        return match split_union_by_sharding_multi(children[0], replicated)? {
            Some(groups) => groups
                .into_iter()
                .map(|(key, child)| with_new_child(lp, child).map(|plan| (key, plan)))
                .collect::<Result<Vec<_>>>()
                .map(Some),
            None => Ok(None),
        };
    }
    // Multi-child node: the Union must live under exactly one child; the rest are cloned
    // unchanged into every rebuilt plan.
    let mut found: Option<(usize, Vec<ShardedUnionBucket>)> = None;
    for (idx, child) in children.iter().enumerate() {
        if let Some(groups) = split_union_by_sharding_multi(child, replicated)? {
            if found.is_some() {
                return Ok(None); // ambiguous: more than one child has a splittable Union
            }
            found = Some((idx, groups));
        }
    }
    let Some((idx, groups)) = found else {
        return Ok(None);
    };
    let mut out = Vec::with_capacity(groups.len());
    for (key, new_child) in groups {
        let mut new_children: Vec<LogicalPlan> = children.iter().map(|c| (*c).clone()).collect();
        new_children[idx] = new_child;
        let rebuilt = lp
            .with_new_exprs(lp.expressions(), new_children)
            .map_err(|e| {
                Error::Unsupported(format!("auto-distribute: rebuild union-split join: {e}"))
            })?;
        out.push((key, rebuilt));
    }
    Ok(Some(out))
}

/// KAN-162 (TPC-DS Q5): distribute a plan node's ancestor chain over a nested `Union`
/// reachable inside it, returning one rebuilt plan per union branch — or `Ok(None)` when the
/// shape has no splittable nested union, keeping the caller's decline exactly.
///
/// The nested mixed union: a `Union` whose branches sit under a chain of
///
/// - **single-child nodes** (`Projection` / `Aggregate` / `Filter` / `SubqueryAlias`) — each
///   distributes over `UNION ALL` exactly (bag semantics) except `Aggregate`, see below; and
/// - **`Inner` joins** whose other children scan only replicated tables — an inner join
///   distributes over a disjoint union on any one side exactly. A non-inner join in the
///   chain, a sharded table in any other join child, a `Union` reachable under more than one
///   child of the same join, or any other node type (`Window`, `Limit`, `Sort`, `Distinct`,
///   …) is not distributable and returns `Ok(None)`.
///
/// Exactness: `UNION ALL` distributes over inner join / filter / projection verbatim, so
/// each rebuilt branch covers exactly its share of the arm's rows; the top-level split then
/// partitions input rows by bucket exactly as it does for undistributed arms, and
/// replicated-bucket placement (sliced stamp or `Forward`) is the existing, already-correct
/// mechanism. An `Aggregate` in the chain does NOT distribute over a union in isolation
/// (`Agg(X ∪ Y)` ≠ `Agg(X) ∪ Agg(Y)` when a group spans both) — it is admitted here only
/// because the split's recombine re-aggregates every producer's partials by the group key:
/// the per-branch inner aggregates feed the outer aggregate's SUM recombine, which is exact
/// under precisely the KAN-54 additive guard (inner leaf-level SUM/COUNT, no grouping sets;
/// outer all-SUM) that `try_split_multi_sharded_union` re-runs on every rebuilt bucket plan.
/// The chain — including the arm's own inner aggregate — is rebuilt UNCHANGED around each
/// branch, so per-channel aggregates are neither merged nor reordered across channels.
fn distribute_over_nested_union(
    lp: &LogicalPlan,
    replicated: &[&str],
) -> Result<Option<Vec<Arc<LogicalPlan>>>> {
    match lp {
        LogicalPlan::Union(u) => {
            let mut arms = Vec::new();
            for input in &u.inputs {
                flatten_union_all(input, &mut arms);
            }
            // Re-alias each branch positionally to the UNION's output names: the union schema
            // takes the first branch's field names, so once the union is gone the enclosing
            // chain's references (e.g. `salesreturns.return_amt`) only resolve on a branch
            // whose own projection used different names if the columns are renamed. A branch
            // whose columns can't be re-aliased cleanly (duplicate qualified names) declines.
            let names: Vec<&str> = u
                .schema
                .fields()
                .iter()
                .map(|f| f.name().as_str())
                .collect();
            let mut out = Vec::with_capacity(arms.len());
            for arm in arms {
                let fields = arm.schema().fields();
                if fields.len() != names.len() {
                    return Ok(None);
                }
                let exprs: Vec<Expr> = fields
                    .iter()
                    .zip(&names)
                    .map(|(f, name)| {
                        // Unqualified reference to the branch's own output column: a branch
                        // whose names are ambiguous fails `Projection::try_new` → decline.
                        Expr::Column(Column::new_unqualified(f.name())).alias(*name)
                    })
                    .collect();
                match Projection::try_new(exprs, Arc::clone(&arm)) {
                    Ok(p) => out.push(Arc::new(LogicalPlan::Projection(p))),
                    Err(_) => return Ok(None),
                }
            }
            Ok(Some(out))
        }
        LogicalPlan::Projection(_)
        | LogicalPlan::Aggregate(_)
        | LogicalPlan::Filter(_)
        | LogicalPlan::SubqueryAlias(_) => {
            let child = lp.inputs()[0];
            match distribute_over_nested_union(child, replicated)? {
                Some(branches) => branches
                    .into_iter()
                    .map(|branch| with_new_child(lp, (*branch).clone()).map(Arc::new))
                    .collect::<Result<Vec<_>>>()
                    .map(Some),
                None => Ok(None),
            }
        }
        LogicalPlan::Join(j) => {
            if j.join_type != JoinType::Inner {
                return Ok(None);
            }
            let children = lp.inputs();
            let mut found: Option<(usize, Vec<Arc<LogicalPlan>>)> = None;
            for (idx, child) in children.iter().enumerate() {
                if let Some(branches) = distribute_over_nested_union(child, replicated)? {
                    if found.is_some() {
                        return Ok(None); // ambiguous: unions under two join children
                    }
                    found = Some((idx, branches));
                }
            }
            let Some((idx, branches)) = found else {
                return Ok(None);
            };
            // Every other join child must be fully replicated: a sharded table there would
            // belong to the distributed branches' bucketing, not ride along cloned.
            for (i, child) in children.iter().enumerate() {
                if i != idx
                    && base_tables(child)
                        .iter()
                        .any(|t| !replicated.contains(&t.as_str()))
                {
                    return Ok(None);
                }
            }
            let mut out = Vec::with_capacity(branches.len());
            for branch in branches {
                let mut new_children: Vec<LogicalPlan> =
                    children.iter().map(|c| (*c).clone()).collect();
                new_children[idx] = (*branch).clone();
                let rebuilt = lp
                    .with_new_exprs(lp.expressions(), new_children)
                    .map_err(|e| {
                        Error::Unsupported(format!(
                            "auto-distribute: rebuild nested-union distribution join: {e}"
                        ))
                    })?;
                out.push(Arc::new(rebuilt));
            }
            Ok(Some(out))
        }
        _ => Ok(None),
    }
}

/// One sliced anchor table per replicated arm for the replicated-slice producer placement, or
/// `None` to keep the single-`Forward` placement. Slicing is **all-or-nothing** across the
/// replicated arms: the stage SQL computes every replicated arm, so an arm without a sliced
/// anchor would be scanned in full on every worker and its partials would multiply in the
/// combine — any arm that cannot provide a safe anchor keeps the whole stage on `Forward`.
///
/// An anchor is safe when, within its arm:
///
/// - it is scanned exactly once in the whole replicated side (arm-unique — a table shared by
///   two arms, or self-joined within one, would need co-located slices the independent
///   per-table file assignment cannot give);
/// - every join on the path from its scan to the arm root keeps it on the inner / preserved
///   side (slicing the null-extended side of an outer join would re-emit the preserved side's
///   unmatched rows on every worker);
/// - it is not force-included by `OXIDANT_REPLICATED_TABLES` (the workers' env override would
///   replicate it anyway and multiply the arm).
///
/// Everything else in the arm stays fully replicated, so the anchor's file slice alone
/// partitions the arm's joined rows: an inner join distributes over a disjoint union on any
/// one side, so the per-slice join outputs form a disjoint cover of the full arm output.
/// When several tables qualify, the first in scan order wins (TPC-DS per-channel arms write
/// the channel fact first); the choice is performance-only — correctness holds for any single
/// anchor per arm.
///
/// Guards beyond the arm shape: the driver must know of more than one worker
/// (`OXIDANT_WORKER_COUNT` — the same env the workers shard files by; helm and the EC2 bootstrap
/// both set it on the driver/connect server), and the replicated side must be raw row sources
/// outside the `Union` itself ([`replicated_arms_for_slicing`]) — an inner aggregate, window,
/// distinct, or expression subquery would read per-slice state that does not recombine (the
/// KAN-54 trap shifted to the replicated side). The outer aggregate's own partials already
/// recombine exactly, or [`partial_and_combine_lists`] would have rejected the query before
/// this split was chosen.
pub(crate) fn replicated_slice_tables(replicated_input: &LogicalPlan) -> Option<Vec<String>> {
    if crate::driver::expected_worker_count_from_env().unwrap_or(1) <= 1 {
        return None;
    }
    let forced = oxidant_loom::shard::replicated_tables_override_from_env();
    let arms = replicated_arms_for_slicing(replicated_input)?;
    let mut picked = Vec::with_capacity(arms.len());
    for arm in &arms {
        // Scan order, deduplicated: the written-first table (the channel fact) wins ties.
        let mut seen = std::collections::HashSet::new();
        let tables: Vec<String> = base_tables(arm)
            .into_iter()
            .filter(|t| seen.insert(t.clone()))
            .collect();
        let anchor = tables.into_iter().find(|t| {
            !forced.iter().any(|f| f.eq_ignore_ascii_case(t))
                && count_table_scans(replicated_input, t) == 1
                && slice_placement_safe(arm, t)
        });
        picked.push(anchor?);
    }
    Some(picked)
}

/// The reduced replicate stamp for a stage whose replicated-only scan region fans out across
/// workers — `replicated` minus the anchor tables [`replicated_slice_tables`] chose to slice —
/// or `None` to keep the single-[`ExchangeMode::Forward`] placement (single/unknown worker
/// count, no safe anchor, or an anchor force-included by `OXIDANT_REPLICATED_TABLES`). Setting
/// the returned non-empty stamp on a stage makes `stamp_replicated_tables` leave it alone, so
/// the workers' file sharder slices exactly those tables' scans for that stage only; every
/// other replicated table is still scanned in full, keeping the region's joins co-located
/// within each worker's disjoint slice.
///
/// `None` when the kept stamp would be EMPTY: an empty stamp reads as "unset" and
/// `stamp_replicated_tables` would refill it with the full replicate list, silently un-slicing
/// the anchors and multiplying the stage's rows by the worker count. `Forward` is the safe
/// placement for that (degenerate — every replicated table sliced) case.
pub(crate) fn sliced_replicate_stamp(plan: &LogicalPlan, replicated: &[&str]) -> Option<String> {
    let slice_tables = replicated_slice_tables(plan)?;
    let kept: Vec<&str> = replicated
        .iter()
        .filter(|t| !slice_tables.iter().any(|s| s.eq_ignore_ascii_case(t)))
        .copied()
        .collect();
    if kept.is_empty() {
        return None;
    }
    Some(kept.join(","))
}

/// The replicated side's leaf union arms for slice analysis, or `None` when any node outside
/// the `Union` itself is not a raw row source (an aggregate / window / distinct / expression
/// subquery) — per-slice evaluation of those does not recombine, so slicing must not happen.
/// Without a `Union` (a single replicated arm) the whole side is the one arm; dims joined
/// above the union stay out of the arm list (they are never slice anchors).
fn replicated_arms_for_slicing(lp: &LogicalPlan) -> Option<Vec<&LogicalPlan>> {
    fn raw_except_union(lp: &LogicalPlan) -> bool {
        use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
        match lp {
            LogicalPlan::Aggregate(_) | LogicalPlan::Window(_) | LogicalPlan::Distinct(_) => false,
            LogicalPlan::Union(u) => u.inputs.iter().all(|i| raw_except_union(i)),
            _ => {
                let mut has_subquery = false;
                for e in lp.expressions() {
                    let _ = e.apply(|node| {
                        if matches!(
                            node,
                            Expr::Exists(_) | Expr::InSubquery(_) | Expr::ScalarSubquery(_)
                        ) {
                            has_subquery = true;
                            return Ok(TreeNodeRecursion::Stop);
                        }
                        Ok(TreeNodeRecursion::Continue)
                    });
                }
                !has_subquery && lp.inputs().iter().all(|c| raw_except_union(c))
            }
        }
    }
    if !raw_except_union(lp) {
        return None;
    }
    let mut node = lp;
    loop {
        match node {
            LogicalPlan::Union(u) => {
                // `split_union_by_sharding` flattens nested unions before bucketing, but its
                // rebuild fallback can keep one nested — flatten defensively so every
                // returned arm is a leaf subtree.
                fn flatten_union_refs<'a>(lp: &'a LogicalPlan, out: &mut Vec<&'a LogicalPlan>) {
                    match lp {
                        LogicalPlan::Union(u) => {
                            for input in &u.inputs {
                                flatten_union_refs(input, out);
                            }
                        }
                        other => out.push(other),
                    }
                }
                let mut arms: Vec<&LogicalPlan> = Vec::new();
                for input in &u.inputs {
                    flatten_union_refs(input, &mut arms);
                }
                return Some(arms);
            }
            LogicalPlan::Join(_) => {
                let children = node.inputs();
                match (contains_union(children[0]), contains_union(children[1])) {
                    (true, false) => node = children[0],
                    (false, true) => node = children[1],
                    // No union below: the whole side is one arm. A union under both join
                    // sides is not a shape this split produces — refuse to analyze it.
                    (false, false) => return Some(vec![lp]),
                    (true, true) => return None,
                }
            }
            _ => {
                let children = node.inputs();
                if children.len() == 1 {
                    node = children[0];
                } else {
                    return Some(vec![lp]);
                }
            }
        }
    }
}

/// Any `Union` node in the subtree.
fn contains_union(lp: &LogicalPlan) -> bool {
    if matches!(lp, LogicalPlan::Union(_)) {
        return true;
    }
    lp.inputs().iter().any(|c| contains_union(c))
}

/// Whether slicing `table`'s files partitions the arm's output exactly: every join between
/// the table's scan and the arm root must keep the scan's side inner or preserved. Cross
/// joins and inner joins distribute over a disjoint union on either side; for outer and
/// semi/anti joins only the preserved (left for Left\*, right for Right\*) side may be
/// sliced — slicing the null-extended side of an outer join would re-emit the preserved
/// side's unmatched rows once per worker.
fn slice_placement_safe(lp: &LogicalPlan, table: &str) -> bool {
    match lp {
        LogicalPlan::TableScan(s) => s.table_name.table() == table,
        LogicalPlan::Join(j) => {
            let left_has = count_table_scans(&j.left, table) > 0;
            let right_has = count_table_scans(&j.right, table) > 0;
            match (left_has, right_has) {
                (true, false) => {
                    matches!(
                        j.join_type,
                        JoinType::Inner | JoinType::Left | JoinType::LeftSemi | JoinType::LeftAnti
                    ) && slice_placement_safe(&j.left, table)
                }
                (false, true) => {
                    matches!(
                        j.join_type,
                        JoinType::Inner
                            | JoinType::Right
                            | JoinType::RightSemi
                            | JoinType::RightAnti
                    ) && slice_placement_safe(&j.right, table)
                }
                // Scanned on both sides (a self-join): independent slices would lose matches.
                _ => false,
            }
        }
        other => {
            let children = other.inputs();
            children.len() == 1 && slice_placement_safe(children[0], table)
        }
    }
}

/// The outer-partial adapter stage for the two KAN-49d composition paths: regroup the exact arm
/// rows the sharded-side sub-DAG emits (named as the union's output columns) into the
/// `g{j}`/`a{i}` partial schema the final combine expects. Summing keeps any row multiplicity
/// exact — one row per outer key in the common case.
///
/// Group keys and aggregate arguments are emitted as **bare column names**, never through the
/// Unparser: it double-quotes identifiers that collide with reserved words (`channel`), and the
/// workers' Databricks dialect reads a double-quoted token as a *string literal* — which turned
/// Q77's `channel` group column into the constant `'channel'` (the qualified naive path is saved
/// by [`sanitize_generated_sql`]'s dot+quote rewrite, but this stage's columns are unqualified).
/// [`union_split_outer_cols`] has already guaranteed these are plain columns.
fn union_split_outer_partial(up: &Unparser, agg: &Aggregate, aggs: &[AggSpec]) -> Result<String> {
    let bare_col = |e: &Expr| -> Result<String> {
        match unqualify(e) {
            Expr::Column(c) => Ok(c.name),
            other => expr_sql(up, &other),
        }
    };
    let group_unq: Vec<String> = flattened_group_exprs(&agg.group_expr)
        .into_iter()
        .map(bare_col)
        .collect::<Result<_>>()?;
    let mut unq_aggs = Vec::with_capacity(aggs.len());
    for (spec, e) in aggs.iter().zip(&agg.aggr_expr) {
        let arg_sql = match strip_alias(e) {
            Expr::AggregateFunction(af) => match af.params.args.first() {
                Some(Expr::Column(c)) => c.name.clone(),
                _ => agg_arg_sql_unqualified(up, spec, e)?,
            },
            _ => agg_arg_sql_unqualified(up, spec, e)?,
        };
        unq_aggs.push(AggSpec {
            func: spec.func.clone(),
            arg_sql,
            distinct: spec.distinct,
            grouping_target: None,
        });
    }
    let (psel, _combine) = partial_and_combine_lists(&group_unq, &unq_aggs)?;
    Ok(sanitize_generated_sql(&format!(
        "SELECT {} FROM shuffle_input GROUP BY {}",
        psel.join(", "),
        group_unq.join(", ")
    )))
}

/// The composition paths feed the outer partial from a sub-DAG whose output columns are the
/// union's own — so the outer aggregate's group keys and aggregate arguments must all be plain
/// union output columns, each present among `arm_out_names`. Anything else (expression group
/// keys, `count(*)`, computed arguments) declines.
fn union_split_outer_cols(agg: &Aggregate, arm_out_names: &[String]) -> bool {
    let mut cols = Vec::new();
    for g in flattened_group_exprs(&agg.group_expr) {
        let Expr::Column(c) = g else { return false };
        cols.push(&c.name);
    }
    for e in &agg.aggr_expr {
        let Expr::AggregateFunction(af) = strip_alias(e) else {
            return false;
        };
        for arg in &af.params.args {
            let Expr::Column(c) = arg else { return false };
            cols.push(&c.name);
        }
    }
    cols.iter().all(|n| arm_out_names.iter().any(|o| o == *n))
}

/// True when `lp` is an aggregate at its output (possibly under projections/aliases/filters) —
/// i.e. its rows are per-key aggregate rows, not raw scan rows. A replicated aggregate joined
/// against per-worker partial aggregate rows attaches its per-key totals once per worker (the
/// KAN-54 doubling trap); joined against raw sharded scan rows it attaches once per row, which
/// the ordinary recombine absorbs — so only the aggregate-output case is unsafe.
fn peels_to_aggregate_shallow(lp: &LogicalPlan) -> bool {
    let mut node = lp;
    loop {
        match node {
            LogicalPlan::Projection(p) => node = p.input.as_ref(),
            LogicalPlan::SubqueryAlias(s) => node = s.input.as_ref(),
            LogicalPlan::Filter(f) => node = f.input.as_ref(),
            LogicalPlan::Aggregate(_) => return true,
            _ => return false,
        }
    }
}

/// Find a join where one side is a fully-replicated subtree producing aggregate rows and the
/// other side scans the sharded table and also produces aggregate rows (TPC-DS Q77's
/// `ss LEFT JOIN sr` arm shape). The naive union-split tail would attach the replicated side's
/// per-key totals to every worker's partial rows, doubling them under the outer SUM.
fn find_replicated_agg_join<'a>(
    lp: &'a LogicalPlan,
    sharded_name: &str,
) -> Option<&'a datafusion::logical_expr::Join> {
    if let LogicalPlan::Join(j) = lp {
        for (maybe_repl, maybe_sharded) in [(&j.left, &j.right), (&j.right, &j.left)] {
            if count_table_scans(maybe_repl, sharded_name) == 0
                && count_table_scans(maybe_sharded, sharded_name) >= 1
                && peels_to_aggregate_shallow(maybe_repl)
                && peels_to_aggregate_shallow(maybe_sharded)
            {
                return Some(j);
            }
        }
    }
    lp.inputs()
        .iter()
        .find_map(|c| find_replicated_agg_join(c, sharded_name))
}

/// KAN-162 (TPC-DS Q5's web leg): find a `LEFT JOIN` whose PRESERVED side scans the sharded
/// table zero times (fully replicated in the bucket context — the bucket's only sharded table
/// is `sharded_name`) and whose NULL-EXTENDED side scans it — the shape
/// [`reject_unsafe_broadcast_shapes`] correctly refuses to run per worker: every worker holds
/// every preserved row but only a slice of the null-extended side, so an unmatched preserved
/// row would null-extend once per worker. The bucket instead runs the co-located composition
/// built by [`union_left_join_branch_stages`]: both sides are hash-shuffled by the join key
/// and the LEFT JOIN runs per partition, each preserved row landing in exactly one bucket.
///
/// The caller guarantees the bucket plan's only sharded table is `sharded_name`, scanned
/// exactly once overall — so the preserved side is necessarily all-replicated and the
/// null-extended side holds that single scan. Only `JoinType::Left` qualifies: Right/Full
/// spellings are a different shape (and q80's `fact LEFT JOIN returns`, sharded side
/// preserved, is already broadcast-safe and never reaches here).
fn find_co_locatable_left_join<'a>(
    lp: &'a LogicalPlan,
    sharded_name: &str,
) -> Option<&'a datafusion::logical_expr::Join> {
    if let LogicalPlan::Join(j) = lp {
        if j.join_type == JoinType::Left
            && count_table_scans(&j.left, sharded_name) == 0
            && count_table_scans(&j.right, sharded_name) == 1
        {
            return Some(j);
        }
    }
    lp.inputs()
        .iter()
        .find_map(|c| find_co_locatable_left_join(c, sharded_name))
}

/// Column-remap scope for [`try_split_union_agg_join`]'s stage C: left-aggregate output columns
/// resolve to the recombined `laq.g{j}` / `laq.r{i}`, the replicated aggregate's output columns
/// to `raq.<name>`. Qualified references disambiguate by the two sides' subquery aliases; a bare
/// name must exist on exactly one side.
struct ArmSideMaps {
    left_alias: Option<String>,
    left_map: HashMap<String, String>,
    right_alias: Option<String>,
    right_map: HashMap<String, String>,
}

fn arm_side_lookup(maps: &ArmSideMaps, c: &datafusion::common::Column) -> Option<String> {
    match &c.relation {
        Some(r) => {
            let rel = r.to_string();
            if Some(&rel) == maps.left_alias.as_ref() {
                maps.left_map.get(&c.name).cloned()
            } else if Some(&rel) == maps.right_alias.as_ref() {
                maps.right_map.get(&c.name).cloned()
            } else {
                None
            }
        }
        None => match (maps.left_map.get(&c.name), maps.right_map.get(&c.name)) {
            (Some(v), None) => Some(v.clone()),
            (None, Some(v)) => Some(v.clone()),
            _ => None,
        },
    }
}

fn remap_arm_side_expr(up: &Unparser, maps: &ArmSideMaps, e: &Expr) -> Result<String> {
    use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
    let mut unmapped = None;
    let _ = e.apply(|node| {
        if let Expr::Column(c) = node {
            if arm_side_lookup(maps, c).is_none() {
                unmapped = Some(c.flat_name());
                return Ok(TreeNodeRecursion::Stop);
            }
        }
        Ok(TreeNodeRecursion::Continue)
    });
    if let Some(bad) = unmapped {
        return Err(Error::Unsupported(format!(
            "auto-distribute: union-arm join expression references `{bad}`, which does not map \
             to either aggregate side"
        )));
    }
    let mapped = e
        .clone()
        .transform(|node| {
            if let Expr::Column(c) = &node {
                if let Some(t) = arm_side_lookup(maps, c) {
                    return Ok(Transformed::yes(datafusion::prelude::col(t)));
                }
            }
            Ok(Transformed::no(node))
        })
        .map(|t| t.data)
        .unwrap_or_else(|_| e.clone());
    expr_sql(up, &mapped)
}

/// One side of [`try_split_union_agg_join`]'s arm join: an aggregate reached through an optional
/// `SubqueryAlias` and an optional pure-renaming `Projection`, with the side's final output column
/// names positionally aligned to the aggregate's schema fields (group fields first, then the
/// aggregate outputs in `aggr_expr` order).
struct JoinSide<'a> {
    alias: Option<String>,
    agg: &'a Aggregate,
    out_names: Vec<String>,
}

fn join_side_agg(side: &LogicalPlan) -> Option<JoinSide<'_>> {
    let (alias, mut node) = match side {
        LogicalPlan::SubqueryAlias(s) => (Some(s.alias.to_string()), s.input.as_ref()),
        other => (None, other),
    };
    let renames: Option<&[Expr]> = match node {
        LogicalPlan::Projection(p) => {
            node = p.input.as_ref();
            Some(p.expr.as_slice())
        }
        _ => None,
    };
    let LogicalPlan::Aggregate(agg) = node else {
        return None;
    };
    let fields = agg.schema.fields();
    let out_names: Vec<String> = match renames {
        Some(exprs) => {
            if exprs.len() != fields.len() {
                return None;
            }
            exprs
                .iter()
                .zip(fields.iter())
                .map(|(e, f)| {
                    let (base, alias) = match e {
                        Expr::Alias(a) => (a.expr.as_ref(), Some(a.name.clone())),
                        other => (other, None),
                    };
                    // The projection must be a pure renaming of the aggregate's own outputs:
                    // either a column reference to the field, or the field's own expression
                    // (the analyzer keeps `sum(x) AS sales` over `aggr=[[sum(x)]]` — the field
                    // at that position is by construction that expression's output).
                    let matches_field = match base {
                        Expr::Column(c) => c.name == *f.name(),
                        other => other.schema_name().to_string() == *f.name(),
                    };
                    if !matches_field {
                        return None;
                    }
                    Some(alias.unwrap_or_else(|| f.name().clone()))
                })
                .collect::<Option<Vec<_>>>()?
        }
        None => fields.iter().map(|f| f.name().clone()).collect(),
    };
    Some(JoinSide {
        alias,
        agg,
        out_names,
    })
}

/// TPC-DS Q77's sharded arm: `Projection over Join(Aggregate(sharded …), Aggregate(replicated …))`
/// — sales per key `LEFT JOIN` returns per key, both pre-aggregated. The arm must not run per
/// worker (the replicated side's per-key totals would attach once per worker partial — see
/// [`find_replicated_agg_join`]). The join-deferral composition is built by
/// [`union_agg_join_bucket_stages`]; this entry point closes it with the replicated-side partial
/// producer + recombine via [`split_union_finish`] (the exactly-one-sharded-arm case).
///
/// `Ok(None)` — a safe refusal — when the arm is not exactly the composition's shape.
fn try_split_union_agg_join(
    p: &Peeled<'_>,
    sharded_input: &LogicalPlan,
    sharded_name: &str,
    replicated_input: &LogicalPlan,
    replicated: &[&str],
) -> Result<Option<DistributedQuery>> {
    let Some(stages) = union_agg_join_bucket_stages(p, sharded_input, sharded_name, 0)? else {
        return Ok(None);
    };
    let up = Unparser::default();
    let aggs = p
        .agg
        .aggr_expr
        .iter()
        .map(AggSpec::classify)
        .collect::<Result<Vec<_>>>()?;
    let group_sql: Vec<String> = flattened_group_exprs(&p.agg.group_expr)
        .into_iter()
        .map(|g| expr_sql(&up, g))
        .collect::<Result<_>>()?;
    let remap = build_remap(p);
    split_union_finish(
        p,
        &group_sql,
        &aggs,
        &remap,
        stages,
        replicated_input,
        replicated,
    )
    .map(Some)
}

/// Stages A–D of the join-deferral composition for ONE sharded union bucket whose arm is
/// `Projection over Join(Aggregate(sharded …), Aggregate(replicated …))` — or, for TPC-DS
/// Q77's catalog arm (`FROM cs, cr`), the same shape with a `CrossJoin` of the two per-key
/// aggregates:
///
/// 1. **stage A**: the sharded aggregate's ordinary per-worker partial, hash-shuffled by its
///    group key (equijoin arm) or gathered whole to partition 0 (cross join — no co-location
///    key exists);
/// 2. **stage B**: the replicated aggregate evaluated in full exactly once
///    ([`ExchangeMode::Forward`]), hash-shuffled by the join key so equal keys co-locate
///    (equijoin) or gathered whole to partition 0 (cross join);
/// 3. **stage C**: recombine the left partials **per key first** (`GROUP BY g{j}` over the
///    co-located partials), then run the join against the replicated side and apply the arm's
///    projection — the join sees exactly one left row per key, so the replicated totals attach
///    exactly once. For the cross join, partition 0 holds laq (the whole recombined left
///    aggregate) and raq (the whole replicated aggregate) and computes the full cross product;
///    every other partition sees two empty inputs and emits nothing. Output gathers to
///    partition 0 (empty hash key).
/// 4. **stage D**: a [`union_split_outer_partial`] adapter regrouping the arm rows into the
///    outer partial schema.
///
/// Stage D is the last stage returned and emits the `g{j}`/`a{i}` partial schema with an empty
/// hash key; the caller assigns its shuffle (the single-sharded path lets
/// [`split_union_finish`] set the group-key hash; the KAN-162 multi-sharded path assigns the
/// shared group-key hash itself). Stage ids are consecutive starting at `first_id`; stage C's
/// `shuffle_input_0`/`shuffle_input_1` are its two positional upstreams (A, B).
///
/// `Ok(None)` — a safe refusal — when the arm is not exactly this shape: non-equi/full-key join,
/// a join filter, a non-LEFT/INNER join type, a sharded side that is not a single-scan aggregate
/// over raw rows (either join spelling), or outer group/aggregate expressions that are not plain
/// union columns.
fn union_agg_join_bucket_stages(
    p: &Peeled<'_>,
    sharded_input: &LogicalPlan,
    sharded_name: &str,
    first_id: u32,
) -> Result<Option<Vec<StageDef>>> {
    let up = Unparser::default();
    // Arm shape: (SubqueryAlias)* → Projection → (SubqueryAlias)* → Join | CrossJoin.
    let mut node = sharded_input;
    while let LogicalPlan::SubqueryAlias(s) = node {
        node = s.input.as_ref();
    }
    let LogicalPlan::Projection(proj) = node else {
        return Ok(None);
    };
    let mut jnode = proj.input.as_ref();
    while let LogicalPlan::SubqueryAlias(s) = jnode {
        jnode = s.input.as_ref();
    }
    // The sharded side must be the LEFT input as written; the flipped spelling declines.
    // `equi` is the equijoin node, or `None` for a genuine cross product of the two per-key
    // aggregates (Q77's catalog arm, `FROM cs, cr`). The cross product is exact by the same
    // recombine-left-first argument — laq holds exactly one row per key before the cross — but
    // with no equijoin key there is no co-location to exploit: BOTH producers gather to
    // partition 0 (empty hash keys) and stage C's partition-0 task computes the whole cross
    // product; every other partition sees two empty inputs and emits nothing (laq's GROUP BY
    // over an empty input yields zero rows because the left aggregate is keyed — the
    // `group_expr.is_empty()` refusal below is what keeps that true).
    //
    // DataFusion 54 has no `LogicalPlan::CrossJoin`: a cross join is a `Join` with
    // `join_type: Inner`, empty `on`, and no filter (its Display prints "Cross Join:"). An
    // Inner join whose equality conjuncts sit in `filter` (DataFusion parks CTE-arm join
    // conditions there) is NOT a cross product — it takes the equijoin path.
    let (left_plan, right_plan, equi) = match jnode {
        LogicalPlan::Join(join)
            if join.join_type == JoinType::Inner && join.on.is_empty() && join.filter.is_none() =>
        {
            (join.left.as_ref(), join.right.as_ref(), None)
        }
        LogicalPlan::Join(join) => {
            if !matches!(join.join_type, JoinType::Left | JoinType::Inner) {
                return Ok(None);
            }
            (join.left.as_ref(), join.right.as_ref(), Some(join))
        }
        _ => return Ok(None),
    };
    // Sides: (SubqueryAlias)? → (renaming Projection)? → Aggregate; left scans the sharded table
    // exactly once, right zero (fully replicated).
    let Some(left) = join_side_agg(left_plan) else {
        return Ok(None);
    };
    let Some(right) = join_side_agg(right_plan) else {
        return Ok(None);
    };
    let left_agg = left.agg;
    let right_agg = right.agg;
    if count_table_scans(right_plan, sharded_name) != 0
        || count_table_scans(left_plan, sharded_name) != 1
    {
        return Ok(None);
    }
    // The left partial must be the ordinary broadcast case: raw scans / row-level joins only
    // under the left aggregate (no nested aggregate, no union).
    let mut nested = Vec::new();
    collect_aggregates(&left_agg.input, &mut nested);
    if !nested.is_empty() || split_union_by_sharding(&left_agg.input, sharded_name)?.is_some() {
        return Ok(None);
    }
    if left_agg.group_expr.is_empty() || is_grouping_set(&left_agg.group_expr) {
        return Ok(None);
    }
    let left_aggs = left_agg
        .aggr_expr
        .iter()
        .map(AggSpec::classify)
        .collect::<Result<Vec<_>>>()?;
    if left_aggs.iter().any(|a| a.distinct) {
        return Ok(None);
    }
    // Equijoin keys (the `Join` arm only): from `ON` plus equality conjuncts parked in
    // `join.filter` (DataFusion keeps CTE-arm join conditions there). Any non-equality
    // residual declines — it belongs to a different shape. A cross join has no keys to
    // validate; its stage C fans out by construction (see the `equi` comment above).
    let mut left_key_names = Vec::new();
    let mut right_key_names = Vec::new();
    if let Some(join) = equi {
        let Ok((keys, residual)) = collect_equijoin_keys(&join.on, join.filter.as_ref()) else {
            return Ok(None);
        };
        if residual.is_some() || keys.len() != left_agg.group_expr.len() {
            return Ok(None);
        }
        // Orient each pair (left side first) and require plain columns both sides. One pair per
        // left group column, and every right group column equated — otherwise a left row can
        // match several right rows and the outer SUM fans out.
        let col_name = |e: &Expr| -> Option<String> {
            match e {
                Expr::Column(c) => Some(c.name.clone()),
                _ => None,
            }
        };
        let side_has = |side: &JoinSide, e: &Expr| -> bool {
            match e {
                Expr::Column(c) => match &c.relation {
                    Some(r) => Some(r.to_string()) == side.alias,
                    None => side.out_names.iter().any(|n| n == &c.name),
                },
                _ => false,
            }
        };
        for (a, b) in &keys {
            let (Some(an), Some(bn)) = (col_name(a), col_name(b)) else {
                return Ok(None);
            };
            let (a_l, a_r) = (side_has(&left, a), side_has(&right, a));
            let (b_l, b_r) = (side_has(&left, b), side_has(&right, b));
            if a_l && b_r && !(a_r && b_l) {
                left_key_names.push(an);
                right_key_names.push(bn);
            } else if b_l && a_r && !(b_r && a_l) {
                left_key_names.push(bn);
                right_key_names.push(an);
            } else {
                return Ok(None);
            }
        }
        let mut left_group_out: Vec<String> = left.out_names[..left_agg.group_expr.len()].to_vec();
        let mut left_keys_sorted = left_key_names.clone();
        left_group_out.sort();
        left_keys_sorted.sort();
        if left_group_out != left_keys_sorted {
            return Ok(None);
        }
        let right_group_out = &right.out_names[..right_agg.group_expr.len()];
        if right_agg.group_expr.is_empty()
            || !right_group_out.iter().all(|n| right_key_names.contains(n))
        {
            return Ok(None);
        }
    }
    // The outer aggregate must read plain union columns that the arm projection emits.
    let arm_out_names: Vec<String> = proj.expr.iter().map(output_name).collect();
    if !union_split_outer_cols(p.agg, &arm_out_names) {
        return Ok(None);
    }

    // Stage A: left partial per worker, hash-shuffled by the left group key — or, for a cross
    // join, gathered whole to partition 0 (no co-location key exists; the cross product runs on
    // the one partition holding the fully recombined left aggregate).
    let left_group_sql: Vec<String> = left_agg
        .group_expr
        .iter()
        .map(|g| expr_sql(&up, g))
        .collect::<Result<_>>()?;
    let left_tail = union_split_tail(&left_agg.input)?;
    let (left_psel, left_combine) = partial_and_combine_lists(&left_group_sql, &left_aggs)?;
    let stage_a_sql = sanitize_generated_sql(&format!(
        "SELECT {} {left_tail} GROUP BY {}",
        left_psel.join(", "),
        left_group_sql.join(", ")
    ));
    let stage_a_keys: Vec<u32> = if equi.is_some() {
        (0..left_group_sql.len() as u32).collect()
    } else {
        vec![]
    };
    let stage_a = StageDef::new(first_id, stage_a_sql, vec![], stage_a_keys);

    // Stage B: the replicated aggregate in full, once, hash-co-located by the right join key —
    // or, for a cross join, gathered whole to partition 0 alongside the left partials.
    let right_sql = sanitize_generated_sql(
        &up.plan_to_sql(right_plan)
            .map_err(|e| {
                Error::Unsupported(format!(
                    "auto-distribute: unparse union-arm replicated aggregate: {e}"
                ))
            })?
            .to_string(),
    );
    let right_key_pos: Vec<u32> = if equi.is_some() {
        let pos: Option<Vec<u32>> = right_key_names
            .iter()
            .map(|n| {
                right_plan
                    .schema()
                    .fields()
                    .iter()
                    .position(|f| f.name() == n)
                    .map(|i| i as u32)
            })
            .collect();
        let Some(pos) = pos else {
            return Ok(None);
        };
        pos
    } else {
        vec![]
    };
    let mut stage_b = StageDef::new(first_id + 1, right_sql, vec![], right_key_pos);
    stage_b.exchange = ExchangeMode::Forward;

    // Stage C: recombine left partials per key, then join, then the arm's projection.
    let mut left_map = HashMap::new();
    for (j, name) in left
        .out_names
        .iter()
        .take(left_agg.group_expr.len())
        .enumerate()
    {
        left_map.insert(name.clone(), format!("laq.g{j}"));
    }
    for (i, name) in left
        .out_names
        .iter()
        .skip(left_agg.group_expr.len())
        .take(left_agg.aggr_expr.len())
        .enumerate()
    {
        left_map.insert(name.clone(), format!("laq.r{i}"));
    }
    // Also accept the aggregate's internal names (the arm projection may reference either the
    // renaming projection's outputs or the aggregate's own field/alias names).
    for (i, a) in left_agg.aggr_expr.iter().enumerate() {
        left_map
            .entry(a.schema_name().to_string())
            .or_insert_with(|| format!("laq.r{i}"));
        if let Expr::Alias(al) = a {
            left_map
                .entry(al.name.clone())
                .or_insert_with(|| format!("laq.r{i}"));
        }
    }
    let mut right_map = HashMap::new();
    for name in &right.out_names {
        right_map.insert(name.clone(), format!("raq.{name}"));
    }
    let maps = ArmSideMaps {
        left_alias: left.alias.clone(),
        left_map,
        right_alias: right.alias.clone(),
        right_map,
    };

    let group_aliases: Vec<String> = (0..left_group_sql.len()).map(|j| format!("g{j}")).collect();
    let laq = format!(
        "SELECT {} FROM shuffle_input_0 GROUP BY {}",
        left_combine.join(", "),
        group_aliases.join(", ")
    );
    let mut on_parts = Vec::new();
    for (l, r) in left_key_names.iter().zip(&right_key_names) {
        let (Some(ls), Some(rs)) = (maps.left_map.get(l), maps.right_map.get(r)) else {
            return Ok(None);
        };
        on_parts.push(format!("{ls} = {rs}"));
    }
    // Equijoin arm: `… laq JOIN|LEFT JOIN raq ON <keys>`. CrossJoin arm: `… laq CROSS JOIN raq`
    // (no ON) — exact because laq is the whole recombined left aggregate on this partition (or
    // empty off partition 0).
    let join_clause = match equi {
        Some(join) => {
            let join_kw = if join.join_type == JoinType::Left {
                "LEFT JOIN"
            } else {
                "JOIN"
            };
            format!(
                "{join_kw} (SELECT * FROM shuffle_input_1) AS raq ON {}",
                on_parts.join(" AND ")
            )
        }
        None => "CROSS JOIN (SELECT * FROM shuffle_input_1) AS raq".to_string(),
    };
    let select = proj
        .expr
        .iter()
        .map(|e| {
            let name = output_name(e);
            let sql = remap_arm_side_expr(&up, &maps, strip_alias(e))?;
            Ok(format!("{sql} AS \"{name}\""))
        })
        .collect::<Result<Vec<_>>>()?
        .join(", ");
    let stage_c_sql = sanitize_generated_sql(&format!(
        "SELECT {select} FROM ({laq}) AS laq {join_clause}"
    ));
    let stage_c = StageDef::new(
        first_id + 2,
        stage_c_sql,
        vec![first_id, first_id + 1],
        vec![],
    );

    // Stage D: regroup the exact arm rows into the outer partial schema. Its shuffle key is the
    // caller's to assign (see the doc comment above).
    let aggs = p
        .agg
        .aggr_expr
        .iter()
        .map(AggSpec::classify)
        .collect::<Result<Vec<_>>>()?;
    let stage_d_sql = union_split_outer_partial(&up, p.agg, &aggs)?;
    let stage_d = StageDef::new(first_id + 3, stage_d_sql, vec![first_id + 2], vec![]);

    Ok(Some(vec![stage_a, stage_b, stage_c, stage_d]))
}

/// Which side of a co-located LEFT JOIN a column reference resolves to.
#[derive(Clone, Copy, PartialEq, Eq)]
enum LjSide {
    Left,
    Right,
    Neither,
    Ambiguous,
}

/// A LEFT JOIN input peeled through catalog-style wrappers down to its [`TableScan`].
///
/// Glue/Hive view expansion yields `SubqueryAlias → Projection → TableScan` with a
/// catalog-qualified scan name (`glue.tpcds_sf100.web_sales`). [`alias`] is the FROM-item
/// name the Unparser emits for that side (the outermost `SubqueryAlias`, else the bare
/// table name) — R1/R2 SQL and R3's FROM-item substitution must keep it so column
/// resolution in the bucket still lines up.
struct PeeledJoinScan<'a> {
    scan: &'a datafusion::logical_expr::TableScan,
    alias: String,
}

/// True when every projection expr is a column (optionally aliased) — pure rename/reorder,
/// no computed expressions. Anything else is not a catalog passthrough wrapper.
fn is_passthrough_column_projection(p: &Projection) -> bool {
    p.expr
        .iter()
        .all(|e| matches!(strip_alias(e), Expr::Column(_)))
}

/// Peel `SubqueryAlias` and passthrough `Projection` wrappers on a LEFT JOIN input down to
/// the underlying [`TableScan`]. Declines (`None`) on any other node or a non-passthrough
/// projection (expression in the select list).
fn peel_join_input_scan(lp: &LogicalPlan) -> Option<PeeledJoinScan<'_>> {
    let mut node = lp;
    let mut alias: Option<String> = None;
    loop {
        match node {
            LogicalPlan::SubqueryAlias(s) => {
                if alias.is_none() {
                    alias = Some(s.alias.table().to_string());
                }
                node = s.input.as_ref();
            }
            LogicalPlan::Projection(p) if is_passthrough_column_projection(p) => {
                node = p.input.as_ref();
            }
            LogicalPlan::TableScan(s) => {
                let alias = alias.unwrap_or_else(|| s.table_name.table().to_string());
                return Some(PeeledJoinScan { scan: s, alias });
            }
            _ => return None,
        }
    }
}

/// FROM-item SQL for a peeled join scan: bare alias when that is the whole table reference,
/// otherwise `qualified AS alias` so workers resolve the catalog table while R3 keeps the
/// Unparser's relation name.
fn peeled_scan_from_sql(peeled: &PeeledJoinScan<'_>) -> String {
    let table_sql = table_ref_sql(&peeled.scan.table_name);
    if table_sql == peeled.alias {
        peeled.alias.clone()
    } else {
        format!("{table_sql} AS {}", peeled.alias)
    }
}

/// Relation qualifier matching for a peeled join side: Unparser alias, bare table name, or
/// the fully-qualified scan string (catalog plans may emit any of the three).
fn peeled_relation_matches(rel: &str, peeled: &PeeledJoinScan<'_>) -> bool {
    rel == peeled.alias
        || rel == peeled.scan.table_name.table()
        || rel == peeled.scan.table_name.to_string()
}

/// Stages R1–R3 of the co-located LEFT JOIN composition for ONE sharded union bucket whose
/// arm chains over `LeftJoin(replicated preserved, sharded null-extended)` — TPC-DS Q5's web
/// leg (`web_returns LEFT JOIN web_sales`, the join existing only to recover
/// `ws_web_site_sk` for return rows). See [`find_co_locatable_left_join`] for why the bucket
/// cannot run per worker. The composition (q77's stage-A/B pattern at row level):
///
/// 1. **R1**: a narrow key-shuffle of the null-extended side — join keys first (so
///    `hash_key_cols = 0..k`), then exactly the right-side columns referenced above the
///    join. An ordinary sliced sharded leaf stage: a per-partition bucket holds ALL the
///    side's rows for its key slice.
/// 2. **R2**: the preserved side projected in join-schema order from the peeled scan
///    (q77's stage-B recipe), computed once ([`ExchangeMode::Forward`]) and hash-shuffled
///    by the left key positions — each preserved row routed to exactly one bucket, NULL
///    keys included (NULLs hash to one deterministic bucket and null-extend there exactly
///    once).
/// 3. **R3 (the bucket's producer)**: the flat producer's own construction — the outer
///    partial SELECT over [`union_split_tail`] — with the tail's two FROM items
///    token-substituted to the positional shuffle inputs (see
///    [`substitute_co_located_join_inputs`]). Replicated dims in the chain scan locally (the
///    KAN-55 consumer-stage precedent). R3's shuffle key is the caller's to assign (the
///    shared group-key hash, or the partition-0 gather for grouping sets).
///
/// Exactness: equal keys co-locate, so partition p reproduces `preserved_p LEFT JOIN
/// null_ext_p` over exactly the rows hashing to p — matched rows expand exactly once
/// (arbitrary fanout; no uniqueness assumption), unmatched and NULL-keyed preserved rows
/// null-extend exactly once. The sharded fact's measures stay with the separate flat
/// producer (the union's sales branch), so nothing double-counts.
///
/// Join inputs may be bare [`LogicalPlan::TableScan`]s or catalog wrappers
/// (`SubqueryAlias` / passthrough `Projection` over a — possibly catalog-qualified —
/// scan). Comparison against `sharded_name` uses the bare [`TableReference::table`]
/// component. `Ok(None)` — a safe refusal — when either input does not peel to a scan, the
/// null-extended bare name is not `sharded_name`, no equijoin key or a residual
/// (non-equality) join filter, a key that is not a plain column, a non-INNER join above
/// the LEFT JOIN in the bucket's chain, a right-side reference above the join that does
/// not resolve unambiguously, or a tail whose FROM items do not substitute exactly once
/// each.
fn union_left_join_branch_stages(
    p: &Peeled<'_>,
    plan: &LogicalPlan,
    sharded_name: &str,
    first_id: u32,
) -> Result<Option<Vec<StageDef>>> {
    let up = Unparser::default();
    let Some(join) = find_co_locatable_left_join(plan, sharded_name) else {
        return Ok(None);
    };
    // Peel catalog wrappers (SubqueryAlias / passthrough Projection) down to the scan.
    // R1/R2 SQL and the R3 FROM-item substitution use the Unparser alias, not the
    // fully-qualified scan string.
    let Some(left) = peel_join_input_scan(join.left.as_ref()) else {
        return Ok(None);
    };
    let Some(right) = peel_join_input_scan(join.right.as_ref()) else {
        return Ok(None);
    };
    let preserved = left.alias.clone();
    let null_ext = right.alias.clone();
    if right.scan.table_name.table() != sharded_name {
        return Ok(None);
    }
    // Every other join in the bucket's chain must be INNER (DataFusion's CrossJoin spelling
    // included — the KAN-26 normalization converts those before the cascade, but admit
    // either): an outer join above the LEFT JOIN is a different composition. Its other
    // children are replicated — the caller's `plan_sharded == [t]` re-derivation has run.
    if !other_joins_inner(plan, join) {
        return Ok(None);
    }

    let left_fields: Vec<&str> = join
        .left
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    let right_fields: Vec<&str> = join
        .right
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    let side_of = |c: &Column| -> LjSide {
        match &c.relation {
            Some(r) => {
                let rel = r.to_string();
                if peeled_relation_matches(&rel, &left) {
                    LjSide::Left
                } else if peeled_relation_matches(&rel, &right) {
                    LjSide::Right
                } else {
                    LjSide::Neither
                }
            }
            None => {
                match (
                    left_fields.contains(&c.name.as_str()),
                    right_fields.contains(&c.name.as_str()),
                ) {
                    (true, false) => LjSide::Left,
                    (false, true) => LjSide::Right,
                    (true, true) => LjSide::Ambiguous,
                    (false, false) => LjSide::Neither,
                }
            }
        }
    };

    // Equijoin keys: ON plus equality conjuncts parked in `filter` (DataFusion keeps the
    // analyzed plan's join conditions there). Any non-equality residual declines.
    let Ok((keys, residual)) = collect_equijoin_keys(&join.on, join.filter.as_ref()) else {
        return Ok(None);
    };
    if residual.is_some() {
        return Ok(None);
    }
    // Orient each pair (preserved side first) and require plain columns both sides.
    let mut left_key_names: Vec<String> = Vec::new();
    let mut right_key_names: Vec<String> = Vec::new();
    for (a, b) in &keys {
        let (Expr::Column(ca), Expr::Column(cb)) = (a, b) else {
            return Ok(None);
        };
        match (side_of(ca), side_of(cb)) {
            (LjSide::Left, LjSide::Right) => {
                left_key_names.push(ca.name.clone());
                right_key_names.push(cb.name.clone());
            }
            (LjSide::Right, LjSide::Left) => {
                left_key_names.push(cb.name.clone());
                right_key_names.push(ca.name.clone());
            }
            _ => return Ok(None),
        }
    }

    // The right-side columns referenced anywhere above the join (the join's payload — Q5's
    // `ws_web_site_sk`). Column references inside the null-extended subtree itself are
    // excluded; a bare name resolving to BOTH sides is ambiguous and declines.
    let mut referenced: Vec<String> = Vec::new();
    {
        let mut cols: Vec<Column> = Vec::new();
        collect_columns_outside(plan, join.right.as_ref(), &mut cols);
        for c in &cols {
            match side_of(c) {
                LjSide::Right => {
                    if !referenced.contains(&c.name) {
                        referenced.push(c.name.clone());
                    }
                }
                LjSide::Ambiguous => return Ok(None),
                LjSide::Left | LjSide::Neither => {}
            }
        }
    }

    // R1: narrow key-shuffle of the null-extended side (join keys first).
    let mut extra: Vec<String> = referenced
        .iter()
        .filter(|c| !right_key_names.contains(c))
        .cloned()
        .collect();
    extra.sort();
    for name in right_key_names.iter().chain(&extra) {
        if !right_fields.contains(&name.as_str()) {
            return Ok(None);
        }
    }
    let right_from = peeled_scan_from_sql(&right);
    let r1_cols: Vec<String> = right_key_names
        .iter()
        .chain(&extra)
        .map(|c| format!("{null_ext}.{c}"))
        .collect();
    let r1_sql =
        sanitize_generated_sql(&format!("SELECT {} FROM {right_from}", r1_cols.join(", ")));
    let stage_r1 = StageDef::new(
        first_id,
        r1_sql,
        vec![],
        (0..right_key_names.len() as u32).collect(),
    );

    // R2: the preserved side in full (join-schema column order), once, hash-co-located by
    // the left join key. Explicit columns keep left_key_pos aligned when a passthrough
    // Projection reorders the scan.
    let left_from = peeled_scan_from_sql(&left);
    let r2_cols: Vec<String> = left_fields
        .iter()
        .map(|c| format!("{preserved}.{c}"))
        .collect();
    let r2_sql = sanitize_generated_sql(&format!("SELECT {} FROM {left_from}", r2_cols.join(", ")));
    let left_key_pos: Option<Vec<u32>> = left_key_names
        .iter()
        .map(|n| {
            join.left
                .schema()
                .fields()
                .iter()
                .position(|f| f.name() == n)
                .map(|i| i as u32)
        })
        .collect();
    let Some(left_key_pos) = left_key_pos else {
        return Ok(None);
    };
    let mut stage_r2 = StageDef::new(first_id + 1, r2_sql, vec![], left_key_pos);
    stage_r2.exchange = ExchangeMode::Forward;

    // R3: the bucket's producer — the flat producer's construction over the substituted
    // tail. upstreams = [R1, R2], so `shuffle_input_0` is the null-extended side and
    // `shuffle_input_1` the preserved side. The shuffle key is the caller's to assign.
    // Substitution keys are the Unparser aliases (not the qualified scan strings).
    let tail = union_split_tail(plan)?;
    let Some(tail) = substitute_co_located_join_inputs(&tail, &preserved, &null_ext) else {
        return Ok(None);
    };
    let aggs = p
        .agg
        .aggr_expr
        .iter()
        .map(AggSpec::classify)
        .collect::<Result<Vec<_>>>()?;
    let group_sql: Vec<String> = flattened_group_exprs(&p.agg.group_expr)
        .into_iter()
        .map(|g| expr_sql(&up, g))
        .collect::<Result<_>>()?;
    let (psel, _) = partial_and_combine_lists(&group_sql, &aggs)?;
    let r3_sql = sanitize_generated_sql(&format!(
        "SELECT {} {tail} GROUP BY {}",
        psel.join(", "),
        group_sql.join(", ")
    ));
    let stage_r3 = StageDef::new(first_id + 2, r3_sql, vec![first_id, first_id + 1], vec![]);

    Ok(Some(vec![stage_r1, stage_r2, stage_r3]))
}

/// Every `Join` node in `lp`'s subtree other than the co-located LEFT JOIN `lj` (identified
/// by pointer — it is borrowed out of the same plan tree) is an INNER join.
fn other_joins_inner(lp: &LogicalPlan, lj: &datafusion::logical_expr::Join) -> bool {
    let here_ok = match lp {
        LogicalPlan::Join(j) => std::ptr::eq(j, lj) || j.join_type == JoinType::Inner,
        _ => true,
    };
    here_ok && lp.inputs().iter().all(|c| other_joins_inner(c, lj))
}

/// All expression columns in `lp`'s subtree except those under `skip` (pointer-identified —
/// the co-located LEFT JOIN's null-extended input, whose own columns the join keys already
/// cover).
fn collect_columns_outside(lp: &LogicalPlan, skip: &LogicalPlan, out: &mut Vec<Column>) {
    if std::ptr::eq(lp, skip) {
        return;
    }
    for e in lp.expressions() {
        collect_expr_columns(&e, out);
    }
    for c in lp.inputs() {
        collect_columns_outside(c, skip, out);
    }
}

/// Token-aware rewrite of a co-located LEFT JOIN bucket's FROM tail: the preserved side's
/// FROM item becomes `(SELECT * FROM shuffle_input_1) AS <preserved>` (R2) and the
/// null-extended side's JOIN item becomes `(SELECT * FROM shuffle_input_0) AS <null_ext>`
/// (R1). `None` unless each substitutes exactly once — the bucket's count==1 scan
/// guarantees each table token appears exactly once as a FROM/JOIN item, so anything else
/// means the shape was not the expected one.
///
/// Whole-identifier, keyword-anchored: only a FROM/JOIN item qualifies. Bare MemTable scans
/// unparse as `FROM web_returns` / `JOIN web_sales`; Glue/Hive view expansion unparses as
/// `(SELECT … FROM glue.tpcds_sf100.web_returns) AS web_returns` — both forms are matched by
/// the relation alias (never by rewriting qualified column references in the ON clause, or
/// table mentions inside string literals / quoted identifiers / comments — the
/// `localize_shuffle_input_sql` discipline).
fn substitute_co_located_join_inputs(
    tail: &str,
    preserved: &str,
    null_ext: &str,
) -> Option<String> {
    let bytes = tail.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(tail.len() + 64);
    let mut n_preserved = 0usize;
    let mut n_null_ext = 0usize;
    let mut i = 0;

    // Skip ASCII whitespace; return the new index.
    let skip_ws = |mut j: usize| -> usize {
        while j < bytes.len() && bytes[j].is_ascii_whitespace() {
            j += 1;
        }
        j
    };
    // Read a bare identifier starting at `j`; return `(ident, end)`.
    let read_ident = |j: usize| -> Option<(&str, usize)> {
        if j >= bytes.len() || !(bytes[j] == b'_' || bytes[j].is_ascii_alphabetic()) {
            return None;
        }
        let mut end = j + 1;
        while end < bytes.len() && (bytes[end] == b'_' || bytes[end].is_ascii_alphanumeric()) {
            end += 1;
        }
        Some((&tail[j..end], end))
    };
    // Skip a balanced `(…)` span starting at `j` (which must be `(`); return the index
    // past the closing `)`, respecting quotes/comments inside.
    let skip_balanced_paren = |mut j: usize| -> Option<usize> {
        if j >= bytes.len() || bytes[j] != b'(' {
            return None;
        }
        let mut depth = 0usize;
        while j < bytes.len() {
            let c = bytes[j];
            if c == b'\'' || c == b'"' || c == b'`' {
                let quote = c;
                j += 1;
                while j < bytes.len() {
                    if bytes[j] == quote {
                        if j + 1 < bytes.len() && bytes[j + 1] == quote {
                            j += 2;
                            continue;
                        }
                        j += 1;
                        break;
                    }
                    j += 1;
                }
                continue;
            }
            if c == b'-' && j + 1 < bytes.len() && bytes[j + 1] == b'-' {
                while j < bytes.len() && bytes[j] != b'\n' {
                    j += 1;
                }
                continue;
            }
            if c == b'/' && j + 1 < bytes.len() && bytes[j + 1] == b'*' {
                j += 2;
                while j + 1 < bytes.len() && !(bytes[j] == b'*' && bytes[j + 1] == b'/') {
                    j += 1;
                }
                j = j.saturating_add(2);
                continue;
            }
            if c == b'(' {
                depth += 1;
            } else if c == b')' {
                depth -= 1;
                j += 1;
                if depth == 0 {
                    return Some(j);
                }
                continue;
            }
            j += 1;
        }
        None
    };

    while i < bytes.len() {
        let c = bytes[i];
        // Quoted spans: 'string' / "identifier" / `identifier`, each self-escaped by doubling.
        if c == b'\'' || c == b'"' || c == b'`' {
            let quote = c;
            out.push(c);
            i += 1;
            while i < bytes.len() {
                out.push(bytes[i]);
                if bytes[i] == quote {
                    if i + 1 < bytes.len() && bytes[i + 1] == quote {
                        out.push(bytes[i + 1]);
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                i += 1;
            }
            continue;
        }
        // Line comment: copy through end-of-line verbatim.
        if c == b'-' && i + 1 < bytes.len() && bytes[i + 1] == b'-' {
            while i < bytes.len() && bytes[i] != b'\n' {
                out.push(bytes[i]);
                i += 1;
            }
            continue;
        }
        // Block comment: copy through the closing marker verbatim.
        if c == b'/' && i + 1 < bytes.len() && bytes[i + 1] == b'*' {
            out.extend_from_slice(b"/*");
            i += 2;
            while i < bytes.len() {
                out.push(bytes[i]);
                if bytes[i] == b'*' && i + 1 < bytes.len() && bytes[i + 1] == b'/' {
                    out.push(b'/');
                    i += 2;
                    break;
                }
                i += 1;
            }
            continue;
        }
        if c == b'_' || c.is_ascii_alphabetic() {
            let start = i;
            while i < bytes.len() && (bytes[i] == b'_' || bytes[i].is_ascii_alphanumeric()) {
                i += 1;
            }
            let ident = &tail[start..i];
            let is_from = ident.eq_ignore_ascii_case("from");
            let is_join = ident.eq_ignore_ascii_case("join");
            if is_from || is_join {
                let item_start = skip_ws(i);
                // Catalog wrappers unparse as `(SELECT …) AS <alias>`; bare scans as `<alias>`.
                let parsed = if item_start < bytes.len() && bytes[item_start] == b'(' {
                    (|| {
                        let after_paren = skip_balanced_paren(item_start)?;
                        let mut j = skip_ws(after_paren);
                        // Optional AS keyword.
                        if let Some((as_kw, after_as)) = read_ident(j) {
                            if as_kw.eq_ignore_ascii_case("as") {
                                j = skip_ws(after_as);
                            }
                        }
                        let (alias, end) = read_ident(j)?;
                        Some((alias, end))
                    })()
                } else if let Some((alias, end)) = read_ident(item_start) {
                    // Bare identifier, optionally renamed: `web_returns` or
                    // `glue.tpcds_sf100.web_returns AS web_returns`. Do NOT treat the
                    // trailing component of an unqualified multi-part name as the alias —
                    // that would also match the scan inside `(SELECT … FROM glue.….t) AS t`
                    // and double-count.
                    let mut end = end;
                    let mut alias = alias;
                    let mut j = end;
                    while j < bytes.len() && bytes[j] == b'.' {
                        if let Some((_, next_end)) = read_ident(j + 1) {
                            j = next_end;
                            end = next_end;
                        } else {
                            break;
                        }
                    }
                    // Multi-part without AS is not a relation alias match.
                    let is_multipart = end != item_start + alias.len();
                    let mut j = skip_ws(end);
                    let mut has_as = false;
                    if let Some((as_kw, after_as)) = read_ident(j) {
                        if as_kw.eq_ignore_ascii_case("as") {
                            j = skip_ws(after_as);
                            if let Some((renamed, renamed_end)) = read_ident(j) {
                                alias = renamed;
                                end = renamed_end;
                                has_as = true;
                            }
                        }
                    }
                    if is_multipart && !has_as {
                        None
                    } else {
                        Some((alias, end))
                    }
                } else {
                    None
                };
                if let Some((alias, item_end)) = parsed {
                    let replacement = if is_from && alias == preserved {
                        n_preserved += 1;
                        Some(format!("(SELECT * FROM shuffle_input_1) AS {preserved}"))
                    } else if is_join && alias == null_ext {
                        n_null_ext += 1;
                        Some(format!("(SELECT * FROM shuffle_input_0) AS {null_ext}"))
                    } else {
                        None
                    };
                    if let Some(rep) = replacement {
                        out.extend_from_slice(ident.as_bytes());
                        out.extend_from_slice(&tail.as_bytes()[i..item_start]);
                        out.extend_from_slice(rep.as_bytes());
                        i = item_end;
                        continue;
                    }
                }
            }
            out.extend_from_slice(ident.as_bytes());
            continue;
        }
        out.push(c);
        i += 1;
    }
    if n_preserved == 1 && n_null_ext == 1 {
        // Input was valid UTF-8; all inserted text is ASCII and quoted spans are copied whole.
        Some(String::from_utf8(out).unwrap_or_else(|_| tail.to_string()))
    } else {
        None
    }
}

/// TPC-DS Q5's sharded arm: an aggregate over *another* mixed union nested inside it
/// (`(store_sales UNION ALL store_returns) ⋈ date_dim ⋈ store GROUP BY s_store_id`). The arm
/// cannot run per worker — the nested union's replicated arm would repeat on every worker — so
/// distribute the arm with the ordinary machinery (which plans the inner mixed union with this
/// same split, minus the grouping-set level) and adapt its exact output rows into the outer
/// partial schema via [`union_split_outer_partial`].
fn try_split_union_nested(
    p: &Peeled<'_>,
    arm_peeled: &Peeled<'_>,
    replicated_input: &LogicalPlan,
    replicated: &[&str],
) -> Result<Option<DistributedQuery>> {
    let arm_out_names: Vec<String> = match arm_peeled.projection {
        Some(exprs) => exprs.iter().map(output_name).collect(),
        None => arm_peeled
            .agg
            .schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect(),
    };
    if !union_split_outer_cols(p.agg, &arm_out_names) {
        return Ok(None);
    }
    let arm_dq = aggregation_stages_for(arm_peeled, replicated)?;
    if arm_dq.finalize_sql.is_some() {
        // The caller already declined arm-level ORDER BY/LIMIT; belt-and-braces.
        return Ok(None);
    }
    let up = Unparser::default();
    let aggs = p
        .agg
        .aggr_expr
        .iter()
        .map(AggSpec::classify)
        .collect::<Result<Vec<_>>>()?;
    let mut stages = arm_dq.stages;
    let arm_out_id = stages.last().map(|s| s.stage_id).ok_or_else(|| {
        Error::Unsupported("auto-distribute: nested union arm produced no stages".into())
    })?;
    let d_sql = union_split_outer_partial(&up, p.agg, &aggs)?;
    stages.push(StageDef::new(
        arm_out_id + 1,
        d_sql,
        vec![arm_out_id],
        vec![],
    ));
    let group_sql: Vec<String> = flattened_group_exprs(&p.agg.group_expr)
        .into_iter()
        .map(|g| expr_sql(&up, g))
        .collect::<Result<_>>()?;
    let remap = build_remap(p);
    split_union_finish(
        p,
        &group_sql,
        &aggs,
        &remap,
        stages,
        replicated_input,
        replicated,
    )
    .map(Some)
}

/// Unparse `lp` and extract its `FROM …` tail — the same unparse-then-slice `agg.input` handling
/// as the single-arm path, factored out so [`try_split_broadcast_union`] can apply it to each of
/// the two narrowed sub-plans [`split_union_by_sharding`] produces.
fn union_split_tail(lp: &LogicalPlan) -> Result<String> {
    let sql = Unparser::default()
        .plan_to_sql(lp)
        .map_err(|e| Error::Unsupported(format!("auto-distribute: unparse union-split arm: {e}")))?
        .to_string();
    let tail = extract_from_tail(&sql)?;
    Ok(sanitize_generated_sql(&tail))
}

/// Aggregate over a `DISTINCT` union of raw per-channel projections with the sharded fact in only
/// some arms (KAN-49a, TPC-DS Q75's `all_sales` CTE):
///
/// ```sql
/// SELECT d_year, i_brand_id, …, SUM(sales_cnt), SUM(sales_amt)
/// FROM (SELECT … FROM catalog_sales … UNION SELECT … FROM store_sales … UNION SELECT … FROM web_sales …)
/// GROUP BY d_year, i_brand_id, …
/// ```
///
/// [`try_split_broadcast_union`] must **not** see this shape: splitting a distinct union's arms
/// by sharding dedups within each half only, so a row present in both halves survives twice
/// (the guard [`super::dag_splitter::reject_mixed_union_branch`] exists for exactly this). The
/// exact composition instead co-locates duplicates *before* dedup:
///
/// 1. **one producer stage per leaf arm** exporting the arm's raw rows (no aggregation),
///    hash-shuffled on the **full row**. A sharded arm reads its local shard per worker; an arm
///    over only replicated tables is computed exactly once on one worker
///    ([`ExchangeMode::Forward`] — the same placement a replicated `UNION ALL` arm gets).
/// 2. **dedup + partial aggregate**: `SELECT DISTINCT *` over the per-partition union of all arm
///    streams, then the partial `GROUP BY`. Identical rows always hash to the same partition
///    whichever arm (or worker) produced them, so the per-partition dedup is globally exact and
///    the partials over the deduplicated rows recombine exactly.
/// 3. **final combine** over the partials, hash-shuffled by the group key — the ordinary
///    recombine, with the query's HAVING / output projection re-applied via [`wrap_output`].
///
/// Arms must be raw row sources (no aggregate / window / distinct / union / expression subquery
/// of their own) and a sharded arm must scan the sharded table exactly once with a
/// broadcast-safe join tree. Anything else returns `Ok(None)` when the shape simply isn't this
/// one (no mixed-sharding distinct union at the aggregate input) and `Err` when it is the shape
/// but an arm is unsafe — so [`try_split_broadcast_union`] never silently splits a distinct
/// union this composition declines.
fn aggregate_over_distinct_union_stages(
    p: &Peeled<'_>,
    sharded_name: &str,
    replicated: &[&str],
) -> Result<Option<DistributedQuery>> {
    let agg = p.agg;
    let mut node = agg.input.as_ref();
    while let LogicalPlan::SubqueryAlias(s) = node {
        node = s.input.as_ref();
    }
    let LogicalPlan::Distinct(distinct) = node else {
        return Ok(None);
    };
    let LogicalPlan::Union(union) = distinct.input().as_ref() else {
        return Ok(None);
    };

    // Leaf arms, flattening nested UNION (distinct is idempotent, so nesting is transparent).
    let mut arms: Vec<&LogicalPlan> = Vec::new();
    flatten_distinct_union(node, &mut arms);
    if arms.len() < 2 {
        return Ok(None);
    }
    let mut saw_sharded = false;
    let mut saw_replicated = false;
    for arm in &arms {
        if base_tables(arm).iter().any(|t| t == sharded_name) {
            saw_sharded = true;
        } else {
            saw_replicated = true;
        }
    }
    // Only the *mixed* shape belongs here — the shape [`super::dag_splitter`]'s mixed-union
    // guard would otherwise reject. An all-sharded or all-replicated distinct union keeps its
    // existing handling.
    if !saw_sharded || !saw_replicated {
        return Ok(None);
    }

    let unsupported = |why: String| {
        Error::Unsupported(format!(
            "auto-distribute: aggregate over DISTINCT union: {why}"
        ))
    };
    if is_grouping_set(&agg.group_expr) {
        return Err(unsupported(
            "ROLLUP / CUBE / GROUPING SETS over a distinct union are not supported".into(),
        ));
    }
    let aggs = agg
        .aggr_expr
        .iter()
        .map(AggSpec::classify)
        .collect::<Result<Vec<_>>>()?;
    if aggs.iter().any(|a| a.distinct) {
        return Err(unsupported(
            "DISTINCT aggregates over a distinct union are not supported".into(),
        ));
    }

    // The union's output column names (taken from its schema) are what the dedup stage reads.
    let union_cols: Vec<String> = union
        .schema
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    if union_cols.is_empty() {
        return Err(unsupported("union has an empty schema".into()));
    }

    let up = Unparser::default();
    // Group keys and aggregate arguments must be readable off the deduplicated union row: they
    // unqualify to plain union column names.
    let group_sql: Vec<String> = flattened_group_exprs(&agg.group_expr)
        .into_iter()
        .map(|g| expr_sql(&up, &unqualify(g)))
        .collect::<Result<_>>()?;
    {
        let mut cols = Vec::new();
        for g in flattened_group_exprs(&agg.group_expr) {
            collect_expr_columns(g, &mut cols);
        }
        for e in &agg.aggr_expr {
            if let Expr::AggregateFunction(af) = strip_alias(e) {
                for arg in &af.params.args {
                    collect_expr_columns(arg, &mut cols);
                }
            }
        }
        if let Some(bad) = cols
            .iter()
            .find(|c| !union_cols.iter().any(|u| u == &c.name))
        {
            return Err(unsupported(format!(
                "column `{}` is not a union output column",
                bad.flat_name()
            )));
        }
    }

    // Producer stages: one per arm, raw rows hash-shuffled on the full union row.
    let mut stages: Vec<StageDef> = Vec::new();
    let n_cols = union_cols.len() as u32;
    for (arm_i, arm) in arms.iter().enumerate() {
        // Raw arms only: a nested aggregate / window / distinct / union belongs to a different
        // composition, and an expression subquery could re-scan the sharded fact shard-locally.
        if !arm_is_raw_row_source(arm) {
            return Err(unsupported(format!(
                "arm {arm_i} contains an aggregate, window, distinct, union, or subquery"
            )));
        }
        let arm_tables = base_tables(arm);
        let arm_is_sharded = arm_tables.iter().any(|t| t == sharded_name);
        let mut stage_sql = up
            .plan_to_sql(arm)
            .map_err(|e| {
                Error::Unsupported(format!(
                    "auto-distribute: unparse distinct-union arm {arm_i}: {e}"
                ))
            })?
            .to_string();
        if arm_is_sharded {
            // The ordinary broadcast-safety checks, scoped to this arm: a single scan of the
            // sharded table, no unsafe preserved-side outer join.
            reject_unsafe_broadcast_shapes(arm, sharded_name)?;
            if count_table_scans(arm, sharded_name) != 1 {
                return Err(unsupported(format!(
                    "arm {arm_i} scans sharded table `{sharded_name}` more than once"
                )));
            }
        } else if arm_tables.iter().any(|t| !replicated.contains(&t.as_str())) {
            return Err(unsupported(format!(
                "arm {arm_i} scans a table that is neither sharded nor replicated"
            )));
        }
        stage_sql = sanitize_generated_sql(&stage_sql);
        let mut stage = StageDef::new(
            stages.len() as u32,
            stage_sql,
            vec![],
            (0..n_cols).collect(),
        );
        if !arm_is_sharded {
            // A replicated-only arm is identical on every worker: compute it once.
            stage.exchange = ExchangeMode::Forward;
        }
        stages.push(stage);
    }

    // Dedup + partial aggregate over the co-located arm rows, hash-shuffled by the group key.
    let arm_reads: Vec<String> = (0..arms.len())
        .map(|i| format!("SELECT * FROM shuffle_input_{i}"))
        .collect();
    let mut psel: Vec<String> = group_sql
        .iter()
        .enumerate()
        .map(|(j, g)| format!("{g} AS g{j}"))
        .collect();
    let mut combine: Vec<String> = (0..group_sql.len()).map(|j| format!("g{j}")).collect();
    for (i, a) in aggs.iter().enumerate() {
        let arg_sql = agg_arg_sql_unqualified(&up, a, &agg.aggr_expr[i])?;
        let (sel, comb) = partial_combine_sql(&a.func, i, &arg_sql)?;
        psel.extend(sel);
        combine.push(comb);
    }
    let dedup_id = stages.len() as u32;
    let partial_sql = sanitize_generated_sql(&format!(
        "SELECT {} FROM (SELECT DISTINCT * FROM ({}) AS all_arms) AS deduped GROUP BY {}",
        psel.join(", "),
        arm_reads.join(" UNION ALL "),
        group_sql.join(", "),
    ));
    stages.push(StageDef::new(
        dedup_id,
        partial_sql,
        (0..dedup_id).collect(),
        (0..group_sql.len() as u32).collect(),
    ));

    // Final combine + output wrap (HAVING / projection), the ordinary recombine shape.
    let final_group_by = final_group_by_sql(&agg.group_expr, group_sql.len())?;
    let inner = format!(
        "SELECT {} FROM shuffle_input GROUP BY {final_group_by}",
        combine.join(", ")
    );
    let remap = build_remap(p);
    let final_sql = wrap_output(p, &inner, &remap)?;
    let combine_id = dedup_id + 1;
    stages.push(StageDef::new(combine_id, final_sql, vec![dedup_id], vec![]));

    Ok(Some(DistributedQuery {
        stages,
        finalize_sql: build_finalize(p)?,
    }))
}

/// Flatten a `DISTINCT`-over-`UNION` tree into its leaf arms. Nested distinct unions dedup the
/// same bag (dedup is idempotent), so the leaf list is equivalent to the tree.
pub(crate) fn flatten_distinct_union<'a>(lp: &'a LogicalPlan, out: &mut Vec<&'a LogicalPlan>) {
    match lp {
        LogicalPlan::Distinct(d) => flatten_distinct_union(d.input().as_ref(), out),
        LogicalPlan::Union(u) => {
            for input in &u.inputs {
                flatten_distinct_union(input, out);
            }
        }
        other => out.push(other),
    }
}

/// A raw row source: no aggregate, window, distinct, union, or expression subquery anywhere in
/// the subtree (joins / filters / projections / scans only).
fn arm_is_raw_row_source(lp: &LogicalPlan) -> bool {
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
    match lp {
        LogicalPlan::Aggregate(_)
        | LogicalPlan::Window(_)
        | LogicalPlan::Distinct(_)
        | LogicalPlan::Union(_) => return false,
        _ => {}
    }
    let mut has_subquery = false;
    for e in lp.expressions() {
        let _ = e.apply(|node| {
            if matches!(
                node,
                Expr::Exists(_) | Expr::InSubquery(_) | Expr::ScalarSubquery(_)
            ) {
                has_subquery = true;
                return Ok(TreeNodeRecursion::Stop);
            }
            Ok(TreeNodeRecursion::Continue)
        });
    }
    !has_subquery && lp.inputs().iter().all(|c| arm_is_raw_row_source(c))
}

/// The aggregate argument's SQL with its table qualifier dropped, so it resolves against the
/// deduplicated union row's plain column names. `count(*)` carries no argument — the classified
/// spec's literal `1` is reused.
fn agg_arg_sql_unqualified(up: &Unparser, spec: &AggSpec, original: &Expr) -> Result<String> {
    if let Expr::AggregateFunction(af) = strip_alias(original) {
        if let Some(arg) = af.params.args.first() {
            return expr_sql(up, &unqualify(arg));
        }
    }
    Ok(spec.arg_sql.clone())
}

/// The partial-stage `SELECT` list (`g{j}` group columns + each aggregate's partial state) and
/// the corresponding final-stage combine expressions, shared by every partial/combine caller in
/// this module ([`recombine_stage_sql`], [`global_aggregation_stages`], and
/// [`try_split_broadcast_union`]).
pub(crate) fn partial_and_combine_lists(
    group_sql: &[String],
    aggs: &[AggSpec],
) -> Result<(Vec<String>, Vec<String>)> {
    let mut psel: Vec<String> = group_sql
        .iter()
        .enumerate()
        .map(|(j, g)| format!("{g} AS g{j}"))
        .collect();
    let mut combine: Vec<String> = (0..group_sql.len()).map(|j| format!("g{j}")).collect();
    for (i, a) in aggs.iter().enumerate() {
        if let Some(j) = a.grouping_target {
            // Recomputed against the combine's real `GROUP BY ROLLUP` — see
            // [`resolve_grouping_specs`]. The partial stage emits nothing for it.
            combine.push(format!("grouping(g{j}) AS r{i}"));
            continue;
        }
        let (sel, comb) = partial_combine_sql(&a.func, i, &a.arg_sql)?;
        psel.extend(sel);
        combine.push(comb);
    }
    Ok((psel, combine))
}

/// Split a `Union` reachable from `lp` into two rebuilt plans — one keeping only the arms that
/// scan `sharded_name` at least once, one keeping only the arms that scan it zero times — or
/// `Ok(None)` when there is nothing useful to split (no `Union`, or every/no arm scans it).
///
/// Descends through any node with exactly one child (`Projection`/`Filter`/`SubqueryAlias`/…) by
/// recursing and rebuilding via [`LogicalPlan::with_new_exprs`] with the same expressions and a
/// narrowed child — the standard "rewrite one subtree, keep the rest" pattern. A `Join` (TPC-DS
/// Q71 wraps its per-channel `UNION ALL` in `item`/`time_dim` broadcast joins before the outer
/// aggregate) is descended into on whichever single side contains the `Union`; the other side
/// (small replicated dimensions) is cloned unchanged into both rebuilt plans. Any other multi-
/// child node, or a `Union` reachable from more than one child, returns `Ok(None)` rather than
/// guessing.
fn split_union_by_sharding(
    lp: &LogicalPlan,
    sharded_name: &str,
) -> Result<Option<(LogicalPlan, LogicalPlan)>> {
    if let LogicalPlan::Union(u) = lp {
        // Flatten nested unions before bucketing arms (TPC-DS Q33/Q56/Q60/Q76 keep their
        // three-way set op nested as `Union(Union(ss, cs), ws)`). Without this, the inner
        // `Union(ss, cs)` buckets as one "sharded" arm and its replicated-only `cs` leaf trips
        // [`reject_unsafe_broadcast_shapes`] below.
        let mut arms = Vec::new();
        for input in &u.inputs {
            flatten_union_all(input, &mut arms);
        }
        let bucket = |arms: &[Arc<LogicalPlan>]| {
            let mut sharded_arms = Vec::new();
            let mut replicated_arms = Vec::new();
            for arm in arms {
                if count_table_scans(arm, sharded_name) > 0 {
                    sharded_arms.push(Arc::clone(arm));
                } else {
                    replicated_arms.push(Arc::clone(arm));
                }
            }
            (sharded_arms, replicated_arms)
        };
        let (sharded_arms, replicated_arms) = bucket(&arms);
        if !sharded_arms.is_empty() && !replicated_arms.is_empty() {
            // A flattened rebuild can fail where the original nested union planned fine: the SQL
            // planner coerces arm types (TPC-DS Q77's `coalesce(returns, 0)` widens a decimal in
            // two arms but not the third), and `Union::try_new` validates strictly. Fall back to
            // bucketing only the top-level inputs in that case — the pre-flatten behavior.
            if let (Ok(s), Ok(r)) = (union_of_arms(sharded_arms), union_of_arms(replicated_arms)) {
                return Ok(Some((s, r)));
            }
        }
        let (sharded_arms, replicated_arms) = bucket(&u.inputs);
        if sharded_arms.is_empty() || replicated_arms.is_empty() {
            return Ok(None);
        }
        return Ok(Some((
            union_of_arms(sharded_arms)?,
            union_of_arms(replicated_arms)?,
        )));
    }

    let children = lp.inputs();
    if children.is_empty() {
        return Ok(None);
    }
    if children.len() == 1 {
        return match split_union_by_sharding(children[0], sharded_name)? {
            Some((s, r)) => Ok(Some((with_new_child(lp, s)?, with_new_child(lp, r)?))),
            None => Ok(None),
        };
    }
    // Multi-child node (Join, …): the Union must live under exactly one child; the rest are
    // cloned unchanged into both rebuilt plans.
    let mut found: Option<(usize, LogicalPlan, LogicalPlan)> = None;
    for (idx, child) in children.iter().enumerate() {
        if let Some((s, r)) = split_union_by_sharding(child, sharded_name)? {
            if found.is_some() {
                return Ok(None); // ambiguous: more than one child has a splittable Union
            }
            found = Some((idx, s, r));
        }
    }
    let Some((idx, s_child, r_child)) = found else {
        return Ok(None);
    };
    let mut s_children: Vec<LogicalPlan> = children.iter().map(|c| (*c).clone()).collect();
    let mut r_children = s_children.clone();
    s_children[idx] = s_child;
    r_children[idx] = r_child;
    Ok(Some((
        lp.with_new_exprs(lp.expressions(), s_children)
            .map_err(|e| {
                Error::Unsupported(format!("auto-distribute: rebuild union-split join: {e}"))
            })?,
        lp.with_new_exprs(lp.expressions(), r_children)
            .map_err(|e| {
                Error::Unsupported(format!("auto-distribute: rebuild union-split join: {e}"))
            })?,
    )))
}

/// Rebuild `lp` with its single existing child replaced by `child`, keeping `lp`'s own
/// expressions unchanged — the "rewrite one subtree" step [`split_union_by_sharding`] applies at
/// every single-child node on the way down to (or up from) a splittable `Union`.
fn with_new_child(lp: &LogicalPlan, child: LogicalPlan) -> Result<LogicalPlan> {
    lp.with_new_exprs(lp.expressions(), vec![child])
        .map_err(|e| Error::Unsupported(format!("auto-distribute: rebuild union-split node: {e}")))
}

/// Flatten a nested `Union` tree into its leaf arms. A bare `LogicalPlan::Union` is always a bag
/// union (`UNION DISTINCT` is a `Distinct` node above a `Union`; INTERSECT/EXCEPT lower to
/// semi/anti joins), and bag union is associative, so the leaf arm list is equivalent to the
/// tree. Shared by [`split_union_by_sharding`] and `shape_extensions::plan_union`.
pub(crate) fn flatten_union_all(lp: &Arc<LogicalPlan>, arms: &mut Vec<Arc<LogicalPlan>>) {
    match lp.as_ref() {
        LogicalPlan::Union(u) => {
            for input in &u.inputs {
                flatten_union_all(input, arms);
            }
        }
        _ => arms.push(Arc::clone(lp)),
    }
}

/// Every `Aggregate` node in the subtree, at any depth.
fn collect_aggregates<'a>(lp: &'a LogicalPlan, out: &mut Vec<&'a Aggregate>) {
    if let LogicalPlan::Aggregate(a) = lp {
        out.push(a);
    }
    for c in lp.inputs() {
        collect_aggregates(c, out);
    }
}

/// Rebuild a `Union` from a (possibly single-element) arm subset, collapsing to the bare plan
/// when only one arm remains — matches how a single-arm `agg.input` unparses today (no `UNION`
/// wrapper for one input).
fn union_of_arms(mut arms: Vec<Arc<LogicalPlan>>) -> Result<LogicalPlan> {
    if arms.len() == 1 {
        return Ok((*arms.remove(0)).clone());
    }
    // Strict first: identical schemas rebuild cleanly. When the SQL analyzer's arm coercion
    // widened a type in only some arms (TPC-DS Q77's `coalesce(returns_, 0)` — Decimal128(17,2)
    // in one arm vs Decimal128(22,2) in another), the strict rebuild rejects and the loose
    // rebuild takes the first arm's schema. Loose is safe here because the narrowed side is only
    // ever *unparsed* for a stage-SQL tail — the worker's SQL planner re-derives the union type
    // coercion from the text, exactly as it did for the original query.
    Union::try_new(arms.clone())
        .or_else(|_| Union::try_new_with_loose_types(arms))
        .map(LogicalPlan::Union)
        .map_err(|e| Error::Unsupported(format!("auto-distribute: rebuild split union: {e}")))
}

/// Reject plan shapes where broadcasting the replicated tables to every worker duplicates output
/// rows instead of partitioning them.
///
/// The single-sharded-table broadcast model is correct when every output row is produced by
/// matching against the (partitioned) sharded table, which a plain inner-join chain guarantees.
/// Two shapes break that invariant, and both go wrong silently — the query returns a number that
/// is a multiple of the right one:
///
/// - a `UNION ALL` arm with no path to the sharded table — only when [`try_split_broadcast_union`]
///   above could not place it separately (e.g. the `Union` sits under a shape it can't rebuild).
/// - an outer join whose preserved side does not reach the sharded table. TPC-DS Q97 `FULL OUTER
///   JOIN`s two independently-aggregated fact tables; the side without the sharded table is
///   replicated, so its unmatched rows (and under `FULL`, all of its rows) survive once per worker
///   rather than once overall.
///
/// A subtree that never scans the sharded table is uniformly replicated and harmless on its own —
/// it only becomes a duplication bug where a parent combines it additively with sharded data, and
/// that parent is the node this catches. So skip such subtrees rather than flagging them, and stop
/// at a nested `Aggregate`: below one, the replicated subtree's result is identical and complete on
/// every worker, which is what lets TPC-DS Q54's `UNION ALL` of two non-sharded facts feed a
/// `DISTINCT` customer filter safely.
pub(crate) fn reject_unsafe_broadcast_shapes(lp: &LogicalPlan, sharded_name: &str) -> Result<()> {
    if count_table_scans(lp, sharded_name) == 0 {
        return Ok(());
    }
    match lp {
        // Nested aggregates (TPC-H Q13: count distribution over a LEFT JOIN group-by) must still
        // validate joins below — stopping here previously allowed sharding the null-supplying side.
        LogicalPlan::Aggregate(a) => reject_unsafe_broadcast_shapes(&a.input, sharded_name),
        LogicalPlan::Union(u) => {
            for arm in &u.inputs {
                if count_table_scans(arm, sharded_name) == 0 {
                    return Err(Error::Unsupported(format!(
                        "auto-distribute: UNION ALL arm does not scan sharded table \
                         `{sharded_name}` — broadcasting it would repeat that arm's rows on \
                         every worker"
                    )));
                }
                reject_unsafe_broadcast_shapes(arm, sharded_name)?;
            }
            Ok(())
        }
        LogicalPlan::Join(j) => {
            match j.join_type {
                JoinType::Full => {
                    return Err(Error::Unsupported(
                        "auto-distribute: FULL OUTER JOIN is not broadcast-safe with a single \
                         sharded table"
                            .into(),
                    ));
                }
                JoinType::Left | JoinType::LeftSemi | JoinType::LeftAnti | JoinType::LeftMark
                    if count_table_scans(&j.left, sharded_name) == 0 =>
                {
                    return Err(Error::Unsupported(format!(
                        "auto-distribute: LEFT join's preserved side does not scan sharded table \
                         `{sharded_name}` — its unmatched rows would repeat on every worker"
                    )));
                }
                JoinType::Right | JoinType::RightSemi | JoinType::RightAnti
                    if count_table_scans(&j.right, sharded_name) == 0 =>
                {
                    return Err(Error::Unsupported(format!(
                        "auto-distribute: RIGHT join's preserved side does not scan sharded table \
                         `{sharded_name}` — its unmatched rows would repeat on every worker"
                    )));
                }
                _ => {}
            }
            reject_unsafe_broadcast_shapes(&j.left, sharded_name)?;
            reject_unsafe_broadcast_shapes(&j.right, sharded_name)
        }
        other => {
            for c in other.inputs() {
                reject_unsafe_broadcast_shapes(c, sharded_name)?;
            }
            Ok(())
        }
    }
}

/// Reject stage SQL that references the Unparser's `left` / `right` join-side alias from outside
/// the lexical scope that defined it.
///
/// DataFusion's Unparser names a decorrelated subquery's join sides `"left"` / `"right"`. When
/// that side is *also* wrapped in another alias one level out — TPC-DS Q8/Q38/Q87's chained
/// `EXISTS`, which unparses to `(SELECT … FROM (…) AS "left" WHERE EXISTS (…)) AS hot_cust WHERE
/// EXISTS (… `left`.c_last_name …)` — the trailing reference sits in a sibling scope where
/// `left` was never bound, and the row it means is only reachable through the outer alias the
/// Unparser failed to substitute. The SQL parses fine; it fails at name resolution on the worker
/// (`No field named left.c_last_name`). Renaming the alias uniformly would not help, since the
/// reference is dangling rather than merely awkwardly quoted, so reject and fall back.
///
/// Scope is tracked as a stack of paren depths: `AS "left"` binds `left` in the frame that is open
/// where the alias appears, and stays visible to every nested subquery (which is what makes the
/// legitimate correlation work) until that frame's paren closes.
pub(crate) fn reject_out_of_scope_join_alias_refs(sql: &str) -> Result<()> {
    const DEFS: [(&str, &str); 4] = [
        ("\"left\"", "left"),
        ("\"right\"", "right"),
        ("`left`", "left"),
        ("`right`", "right"),
    ];
    const USES: [(&str, &str); 4] = [
        ("\"left\".", "left"),
        ("\"right\".", "right"),
        ("`left`.", "left"),
        ("`right`.", "right"),
    ];
    let bytes = sql.as_bytes();
    let mut stack: Vec<Vec<&str>> = vec![Vec::new()];
    let mut i = 0usize;
    while i < bytes.len() {
        match bytes[i] {
            b'(' => {
                stack.push(Vec::new());
                i += 1;
            }
            b')' => {
                if stack.len() > 1 {
                    stack.pop();
                }
                i += 1;
            }
            _ => {
                if let Some((pat, name)) = USES.iter().find(|(pat, _)| sql[i..].starts_with(*pat)) {
                    if !stack.iter().any(|frame| frame.contains(name)) {
                        return Err(Error::Unsupported(format!(
                            "auto-distribute: generated SQL references join-side alias `{name}` \
                             outside the scope that defines it (Unparser aliasing)"
                        )));
                    }
                    i += pat.len();
                    continue;
                }
                if let Some((pat, name)) = DEFS.iter().find(|(pat, _)| sql[i..].starts_with(*pat)) {
                    stack.last_mut().expect("stack never empty").push(name);
                    i += pat.len();
                    continue;
                }
                i += 1;
            }
        }
    }
    Ok(())
}

/// Rewrite dangling `left`/`right` join-side aliases (see
/// [`reject_out_of_scope_join_alias_refs`]) to the outer alias that actually owns them, when the
/// SQL shape makes that alias recoverable.
///
/// The Unparser's `(… AS "left" …) AS hot_cust WHERE EXISTS (… `left`.col …)` shape closes the
/// frame that bound `left` and immediately re-aliases it as `hot_cust` one level out — the
/// dangling reference is really `` `hot_cust`.col ``, the Unparser just didn't substitute it.
/// This walks the same paren-scope stack as the rejector, but:
///
/// - when a frame that bound `left`/`right` closes, remembers those names as *pending*;
/// - if the very next significant token is `AS <alias>` (quoted or bare), maps each pending name
///   to that alias — this is the only case that resolves pending names, so sibling scopes
///   (`), (SELECT …)` with no `AS` in between) never absorb into an unrelated alias;
///   anything else clears pending without recording a mapping;
/// - an out-of-scope use rewrites to the mapped outer alias (matching the use's own quote style)
///   if a mapping exists, otherwise it's still dangling and returns the same
///   [`Error::Unsupported`] the rejector would.
///
/// In-scope uses are copied through unchanged.
pub(crate) fn rewrite_out_of_scope_join_alias_refs(sql: &str) -> Result<String> {
    const DEFS: [(&str, &str); 4] = [
        ("\"left\"", "left"),
        ("\"right\"", "right"),
        ("`left`", "left"),
        ("`right`", "right"),
    ];
    const USES: [(&str, &str, u8); 4] = [
        ("\"left\".", "left", b'"'),
        ("\"right\".", "right", b'"'),
        ("`left`.", "left", b'`'),
        ("`right`.", "right", b'`'),
    ];
    let bytes = sql.as_bytes();
    let mut stack: Vec<Vec<&str>> = vec![Vec::new()];
    let mut absorbed: HashMap<&str, String> = HashMap::new();
    let mut out = String::with_capacity(sql.len());
    let mut i = 0usize;
    while i < bytes.len() {
        match bytes[i] {
            b'(' => {
                stack.push(Vec::new());
                out.push('(');
                i += 1;
            }
            b')' => {
                let closed = if stack.len() > 1 { stack.pop() } else { None };
                out.push(')');
                i += 1;
                if let Some(closed) = closed {
                    if !closed.is_empty() {
                        if let Some(alias) = peek_as_alias(sql, i) {
                            for name in closed {
                                absorbed.insert(name, alias.clone());
                            }
                        }
                        // Anything other than `AS <alias>` right after the close: pending is
                        // dropped without recording a mapping (sibling scopes must not leak).
                    }
                }
            }
            _ => {
                if let Some(&(pat, name, quote)) =
                    USES.iter().find(|(pat, _, _)| sql[i..].starts_with(*pat))
                {
                    if stack.iter().any(|frame| frame.contains(&name)) {
                        out.push_str(pat);
                    } else if let Some(alias) = absorbed.get(name) {
                        out.push(quote as char);
                        out.push_str(alias);
                        out.push(quote as char);
                        out.push('.');
                    } else {
                        return Err(Error::Unsupported(format!(
                            "auto-distribute: generated SQL references join-side alias `{name}` \
                             outside the scope that defines it (Unparser aliasing)"
                        )));
                    }
                    i += pat.len();
                    continue;
                }
                if let Some((pat, name)) = DEFS.iter().find(|(pat, _)| sql[i..].starts_with(*pat)) {
                    stack.last_mut().expect("stack never empty").push(name);
                    out.push_str(pat);
                    i += pat.len();
                    continue;
                }
                out.push(bytes[i] as char);
                i += 1;
            }
        }
    }
    Ok(out)
}

/// Look just past a just-closed `)` at byte offset `pos` for `AS <alias>` and, if found, return
/// the alias's identifier text (quotes stripped). Returns `None` for anything else — a sibling
/// separator (`,`), a keyword (`WHERE`, …), another `)`, or end of input.
fn peek_as_alias(sql: &str, pos: usize) -> Option<String> {
    let bytes = sql.as_bytes();
    let mut j = pos;
    while j < bytes.len() && bytes[j].is_ascii_whitespace() {
        j += 1;
    }
    if j + 2 > bytes.len() || !bytes[j..j + 2].eq_ignore_ascii_case(b"AS") {
        return None;
    }
    let after_as = j + 2;
    if after_as < bytes.len()
        && (bytes[after_as].is_ascii_alphanumeric() || bytes[after_as] == b'_')
    {
        return None; // e.g. `ASC`, or an identifier starting with "as"
    }
    let mut k = after_as;
    while k < bytes.len() && bytes[k].is_ascii_whitespace() {
        k += 1;
    }
    match bytes.get(k)? {
        b'"' | b'`' => {
            let quote = bytes[k];
            let start = k + 1;
            let mut end = start;
            while end < bytes.len() && bytes[end] != quote {
                end += 1;
            }
            if end < bytes.len() {
                Some(sql[start..end].to_string())
            } else {
                None
            }
        }
        c if c.is_ascii_alphabetic() || *c == b'_' => {
            let start = k;
            let mut end = k;
            while end < bytes.len() && (bytes[end].is_ascii_alphanumeric() || bytes[end] == b'_') {
                end += 1;
            }
            Some(sql[start..end].to_string())
        }
        _ => None,
    }
}

/// Whether DataFusion's aggregate uses its single-expression grouping-set representation.
pub(crate) fn is_grouping_set(group_expr: &[Expr]) -> bool {
    matches!(group_expr, [Expr::GroupingSet(_)])
}

/// Expand DataFusion's positional `group_expr` representation into the actual output columns.
///
/// `ROLLUP(a, b)` and `CUBE(a, b)` become `[a, b]`. Explicit grouping sets become the stable,
/// de-duplicated union returned by DataFusion's own [`GroupingSet::distinct_expr`].
pub(crate) fn flattened_group_exprs(group_expr: &[Expr]) -> Vec<&Expr> {
    match group_expr {
        [Expr::GroupingSet(grouping_set)] => grouping_set.distinct_expr(),
        ordinary => ordinary.iter().collect(),
    }
}

/// Render the final stage's group-by over safe `g{j}` columns.
///
/// The grouping construct appears only in `GROUP BY`; unlike the old positional lowering, it is
/// never emitted into the partial SELECT list where Databricks would resolve `ROLLUP` as a scalar
/// function. Keeping the explicit space (`ROLLUP (...)`) also matches the syntax accepted by the
/// worker parser and by the original TPC-DS queries.
pub(crate) fn final_group_by_sql(group_expr: &[Expr], flattened_len: usize) -> Result<String> {
    let flattened = flattened_group_exprs(group_expr);
    if flattened.len() != flattened_len {
        return Err(Error::Unsupported(format!(
            "auto-distribute: grouping set has {} flattened columns but stage SQL has {flattened_len}",
            flattened.len()
        )));
    }

    let safe_name = |expr: &Expr| -> Result<String> {
        flattened
            .iter()
            .position(|candidate| *candidate == expr)
            .map(|j| format!("g{j}"))
            .ok_or_else(|| {
                Error::Unsupported(format!(
                    "auto-distribute: grouping-set expression `{expr}` is not in its flattened columns"
                ))
            })
    };

    match group_expr {
        [Expr::GroupingSet(GroupingSet::Rollup(exprs))] => Ok(format!(
            "ROLLUP ({})",
            exprs
                .iter()
                .map(safe_name)
                .collect::<Result<Vec<_>>>()?
                .join(", ")
        )),
        [Expr::GroupingSet(GroupingSet::Cube(exprs))] => Ok(format!(
            "CUBE ({})",
            exprs
                .iter()
                .map(safe_name)
                .collect::<Result<Vec<_>>>()?
                .join(", ")
        )),
        [Expr::GroupingSet(GroupingSet::GroupingSets(levels))] => {
            let levels = levels
                .iter()
                .map(|level| {
                    let names = level.iter().map(safe_name).collect::<Result<Vec<_>>>()?;
                    Ok(format!("({})", names.join(", ")))
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(format!("GROUPING SETS ({})", levels.join(", ")))
        }
        ordinary => {
            if ordinary.len() != flattened_len {
                return Err(Error::Unsupported(format!(
                    "auto-distribute: expected {} ordinary group columns, got {flattened_len}",
                    ordinary.len()
                )));
            }
            Ok((0..flattened_len)
                .map(|j| format!("g{j}"))
                .collect::<Vec<_>>()
                .join(", "))
        }
    }
}

/// Fix SQL fragments from DataFusion's Unparser that the Databricks-dialect re-parser rejects.
///
/// Two common failure modes when generated stage SQL is sent to workers:
/// - `alias."col"` — dot access with a double-quoted column name;
/// - `"table".col` — dot access on a double-quoted table name (e.g. reserved `part`).
pub(crate) fn sanitize_generated_sql(sql: &str) -> String {
    fix_interval_pg_style(&fix_quoted_column_after_dot(&fix_quoted_table_dot_access(
        sql,
    )))
}

/// DataFusion's Unparser emits Postgres-style combined interval literals
/// (`INTERVAL '12 MONS'`, `INTERVAL '90 DAYS'`). Workers re-parse under the Databricks dialect,
/// which requires a unit *after* the quoted value (`INTERVAL '12' MONTH`). Rewrite the combined
/// form so stage SQL round-trips. Case-insensitive on the keyword and unit abbreviation.
fn fix_interval_pg_style(sql: &str) -> String {
    let b = sql.as_bytes();
    let n = b.len();
    let mut out = String::with_capacity(n);
    let mut i = 0;
    while i < n {
        // Skip quoted strings so we never rewrite interval-looking content inside literals.
        if b[i] == b'\'' || b[i] == b'"' {
            let quote = b[i];
            let start = i;
            i += 1;
            while i < n {
                if b[i] == quote {
                    if i + 1 < n && b[i + 1] == quote {
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                i += 1;
            }
            out.push_str(&sql[start..i]);
            continue;
        }

        if interval_kw_at(b, i) {
            let after_kw = i + 8;
            let mut j = after_kw;
            while j < n && b[j].is_ascii_whitespace() {
                j += 1;
            }
            if j < n && b[j] == b'\'' {
                let lit_open = j;
                j += 1;
                let lit_body = j;
                while j < n && b[j] != b'\'' {
                    j += 1;
                }
                if j < n {
                    let body = &sql[lit_body..j];
                    if let Some((num, unit)) = split_pg_interval_body(body) {
                        out.push_str(&sql[i..lit_open]);
                        out.push('\'');
                        out.push_str(num);
                        out.push('\'');
                        out.push(' ');
                        out.push_str(unit);
                        i = j + 1; // past closing quote
                        continue;
                    }
                }
            }
        }

        out.push(b[i] as char);
        i += 1;
    }
    out
}

fn interval_kw_at(b: &[u8], i: usize) -> bool {
    const KW: &[u8] = b"interval";
    if i + KW.len() > b.len() {
        return false;
    }
    if i > 0 {
        let prev = b[i - 1];
        if prev.is_ascii_alphanumeric() || prev == b'_' {
            return false;
        }
    }
    if !b[i..i + KW.len()].eq_ignore_ascii_case(KW) {
        return false;
    }
    let after = i + KW.len();
    if after < b.len() {
        let next = b[after];
        if next.is_ascii_alphanumeric() || next == b'_' {
            return false;
        }
    }
    true
}

/// Split `"12 MONS"` / `"-90 DAYS"` into (`"-90"`, `"DAY"`). Returns `None` when the body is already
/// a bare numeric literal (no unit inside the quotes).
fn split_pg_interval_body(body: &str) -> Option<(&str, &'static str)> {
    let body = body.trim();
    let mut parts = body.split_whitespace();
    let num = parts.next()?;
    let unit_raw = parts.next()?;
    if parts.next().is_some() {
        return None; // multi-unit combined forms — leave alone
    }
    // Number may be signed / decimal; require it to look numeric.
    if !num
        .bytes()
        .enumerate()
        .all(|(i, c)| c.is_ascii_digit() || ((c == b'+' || c == b'-') && i == 0) || c == b'.')
    {
        return None;
    }
    let unit = match unit_raw.to_ascii_uppercase().as_str() {
        "YEAR" | "YEARS" | "YR" | "YRS" => "YEAR",
        "MONTH" | "MONTHS" | "MON" | "MONS" => "MONTH",
        "DAY" | "DAYS" | "D" => "DAY",
        "HOUR" | "HOURS" | "HR" | "HRS" => "HOUR",
        "MINUTE" | "MINUTES" | "MIN" | "MINS" => "MINUTE",
        "SECOND" | "SECONDS" | "SEC" | "SECS" => "SECOND",
        _ => return None,
    };
    Some((num, unit))
}

/// `"table".col` → `` `table`.col `` so dot access parses under the Databricks dialect.
fn fix_quoted_table_dot_access(sql: &str) -> String {
    let mut out = String::with_capacity(sql.len());
    let mut i = 0;
    let bytes = sql.as_bytes();
    while i < bytes.len() {
        if bytes[i] == b'"' {
            let start = i;
            i += 1;
            while i < bytes.len() && bytes[i] != b'"' {
                i += 1;
            }
            if i < bytes.len() {
                let ident = &sql[start + 1..i];
                i += 1; // closing quote
                if i < bytes.len() && bytes[i] == b'.' && is_simple_ident(ident) {
                    out.push('`');
                    out.push_str(ident);
                    out.push('`');
                    out.push('.');
                    i += 1;
                    continue;
                }
                out.push_str(&sql[start..i]);
                continue;
            }
        }
        out.push(bytes[i] as char);
        i += 1;
    }
    out
}

/// `alias."col"` → `alias.col` when `col` is a plain identifier.
fn fix_quoted_column_after_dot(sql: &str) -> String {
    let mut out = String::with_capacity(sql.len());
    let mut i = 0;
    let bytes = sql.as_bytes();
    while i < bytes.len() {
        let start = i;
        if bytes[i].is_ascii_alphabetic() || bytes[i] == b'_' {
            while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                i += 1;
            }
            if i < bytes.len() && bytes[i] == b'.' && i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                let qstart = i + 2;
                let mut j = qstart;
                while j < bytes.len() && bytes[j] != b'"' {
                    j += 1;
                }
                if j < bytes.len() {
                    let ident = &sql[qstart..j];
                    if is_simple_ident(ident) {
                        out.push_str(&sql[start..=i]);
                        out.push_str(ident);
                        i = j + 1;
                        continue;
                    }
                }
            }
            out.push_str(&sql[start..i]);
            continue;
        }
        out.push(bytes[i] as char);
        i += 1;
    }
    out
}

fn is_simple_ident(s: &str) -> bool {
    let mut chars = s.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Extract a non-negative integer `LIMIT` value from a literal scalar.
fn scalar_as_usize(s: &datafusion::scalar::ScalarValue) -> Option<usize> {
    use datafusion::scalar::ScalarValue::*;
    match s {
        Int64(Some(v)) if *v >= 0 => Some(*v as usize),
        Int32(Some(v)) if *v >= 0 => Some(*v as usize),
        UInt64(Some(v)) => Some(*v as usize),
        UInt32(Some(v)) => Some(*v as usize),
        _ => None,
    }
}

/// True when `lp` (or any nested subquery plan) contains a `Distinct` / `DistinctOn` node.
pub(crate) fn plan_contains_distinct(lp: &LogicalPlan) -> bool {
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
    matches!(lp, LogicalPlan::Distinct(_))
        || lp.inputs().iter().any(|c| plan_contains_distinct(c))
        || {
            let mut found = false;
            for e in lp.expressions() {
                let _ = e.apply(|node| {
                    let sub = match node {
                        Expr::Exists(ex) => Some(ex.subquery.subquery.as_ref()),
                        Expr::InSubquery(iq) => Some(iq.subquery.subquery.as_ref()),
                        Expr::ScalarSubquery(sq) => Some(sq.subquery.as_ref()),
                        _ => None,
                    };
                    if let Some(plan) = sub {
                        if plan_contains_distinct(plan) {
                            found = true;
                            return Ok(TreeNodeRecursion::Stop);
                        }
                    }
                    Ok(TreeNodeRecursion::Continue)
                });
                if found {
                    break;
                }
            }
            found
        }
}

/// Count scans of table `name` anywhere in `lp` — across plan inputs **and** subquery plans nested
/// in expressions (EXISTS / IN / scalar subqueries), so a correlated subquery over the table counts.
pub(crate) fn count_table_scans(lp: &LogicalPlan, name: &str) -> usize {
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
    let mut n = match lp {
        LogicalPlan::TableScan(s) if s.table_name.table() == name => 1,
        _ => 0,
    };
    for c in lp.inputs() {
        n += count_table_scans(c, name);
    }
    for e in lp.expressions() {
        let _ = e.apply(|node| {
            let sub = match node {
                Expr::Exists(ex) => Some(&ex.subquery.subquery),
                Expr::InSubquery(iq) => Some(&iq.subquery.subquery),
                Expr::ScalarSubquery(sq) => Some(&sq.subquery),
                _ => None,
            };
            if let Some(plan) = sub {
                n += count_table_scans(plan, name);
            }
            Ok(TreeNodeRecursion::Continue)
        });
    }
    n
}

/// Collect the base (scanned) table names referenced anywhere in `lp`.
pub fn base_tables(lp: &LogicalPlan) -> Vec<String> {
    let mut out = Vec::new();
    collect_tables(lp, &mut out);
    out
}

fn collect_tables(lp: &LogicalPlan, out: &mut Vec<String>) {
    if let LogicalPlan::TableScan(s) = lp {
        out.push(s.table_name.table().to_string());
    }
    for c in lp.inputs() {
        collect_tables(c, out);
    }
}

/// SQL relation text preserving catalog/schema qualification from a logical [`TableReference`].
pub(crate) fn table_ref_sql(reference: &TableReference) -> String {
    reference.to_string()
}

/// Look up the catalog-qualified SQL text for a bare table name in `lp` (and expression
/// subqueries). Falls back to the bare name when no matching scan exists (local MemTables).
pub(crate) fn qualified_table_sql(lp: &LogicalPlan, bare: &str) -> String {
    find_qualified_table_sql(lp, bare).unwrap_or_else(|| bare.to_string())
}

/// Look up the full logical [`TableReference`] for a bare table name in `lp` (and expression
/// subqueries). Returns `None` when no scan owns the name (local MemTables).
fn find_table_ref(lp: &LogicalPlan, bare: &str) -> Option<TableReference> {
    if let LogicalPlan::TableScan(s) = lp {
        if s.table_name.table() == bare {
            return Some(s.table_name.clone());
        }
    }
    for e in lp.expressions() {
        use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
        let mut found = None;
        let _ = e.apply(|node| {
            let sub = match node {
                Expr::Exists(ex) => Some(ex.subquery.subquery.as_ref()),
                Expr::InSubquery(iq) => Some(iq.subquery.subquery.as_ref()),
                Expr::ScalarSubquery(sq) => Some(sq.subquery.as_ref()),
                _ => None,
            };
            if let Some(plan) = sub {
                if let Some(reference) = find_table_ref(plan, bare) {
                    found = Some(reference);
                    return Ok(TreeNodeRecursion::Stop);
                }
            }
            Ok(TreeNodeRecursion::Continue)
        });
        if found.is_some() {
            return found;
        }
    }
    for c in lp.inputs() {
        if let Some(reference) = find_table_ref(c, bare) {
            return Some(reference);
        }
    }
    None
}

fn find_qualified_table_sql(lp: &LogicalPlan, bare: &str) -> Option<String> {
    find_table_ref(lp, bare).map(|r| table_ref_sql(&r))
}

#[cfg(test)]
mod guard_tests {
    use super::{
        find_qualified_table_sql, qualified_table_sql, reject_out_of_scope_join_alias_refs,
        rewrite_out_of_scope_join_alias_refs, substitute_co_located_join_inputs, table_ref_sql,
    };
    use datafusion::common::TableReference;
    use datafusion::logical_expr::LogicalPlanBuilder;
    use datafusion::prelude::lit;
    use oxidant_loom::arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    #[test]
    fn substitute_co_located_joins_accepts_catalog_subquery_aliases() {
        // Glue/Hive view expansion unparses SubqueryAlias→Projection→TableScan as
        // `(SELECT … FROM glue.….t) AS t` rather than a bare FROM item.
        let tail = "FROM (SELECT wr_item_sk FROM glue.tpcds_sf100.web_returns) AS web_returns LEFT OUTER JOIN (SELECT ws_item_sk FROM glue.tpcds_sf100.web_sales) AS web_sales ON web_returns.wr_item_sk = web_sales.ws_item_sk";
        let out = substitute_co_located_join_inputs(tail, "web_returns", "web_sales")
            .expect("catalog subquery FROM items must substitute");
        assert!(
            out.contains("(SELECT * FROM shuffle_input_1) AS web_returns"),
            "{out}"
        );
        assert!(
            out.contains("(SELECT * FROM shuffle_input_0) AS web_sales"),
            "{out}"
        );
        assert!(
            !out.contains("glue.tpcds_sf100"),
            "qualified scans must be fully replaced: {out}"
        );
    }

    #[test]
    fn substitute_co_located_joins_finds_nested_catalog_join_under_outer_from() {
        // After distribute_over_nested_union rebuilds the arm, union_split_tail unparses
        // the whole chain — the co-located LEFT JOIN sits inside outer `FROM (… ) AS x`.
        let tail = "FROM (SELECT 'web' AS channel FROM (SELECT ws_web_site_sk FROM (SELECT wr_item_sk FROM glue.tpcds_sf100.web_returns) AS web_returns LEFT OUTER JOIN (SELECT ws_item_sk FROM glue.tpcds_sf100.web_sales) AS web_sales ON web_returns.wr_item_sk = web_sales.ws_item_sk) AS salesreturns) AS x";
        let out = substitute_co_located_join_inputs(tail, "web_returns", "web_sales")
            .expect("nested catalog LEFT JOIN must still substitute once");
        assert!(
            out.contains("(SELECT * FROM shuffle_input_1) AS web_returns"),
            "{out}"
        );
        assert!(
            out.contains("(SELECT * FROM shuffle_input_0) AS web_sales"),
            "{out}"
        );
    }

    #[test]
    fn substitute_real_q5_tail_fragment() {
        let tail = r#"FROM (SELECT 'web' AS "channel", wsr.web_site_id AS id, wsr.sales FROM (SELECT web_site.web_site_id, sum(salesreturns.sales_price) AS sales FROM (SELECT ws_web_site_sk AS k, wr_returned_date_sk AS dsk, sales_price AS sales_price FROM (SELECT web_sales.ws_web_site_sk, web_returns.wr_returned_date_sk, CAST(0 AS DOUBLE) AS sales_price FROM (SELECT web_returns.wr_returned_date_sk, web_returns.wr_web_site_sk, web_returns.wr_item_sk, web_returns.wr_order_number, web_returns.wr_return_amt, web_returns.wr_net_loss FROM glue.tpcds_sf100.web_returns) AS web_returns LEFT OUTER JOIN (SELECT web_sales.ws_sold_date_sk, web_sales.ws_web_site_sk, web_sales.ws_item_sk, web_sales.ws_order_number, web_sales.ws_ext_sales_price, web_sales.ws_net_profit FROM glue.tpcds_sf100.web_sales) AS web_sales ON ((web_returns.wr_item_sk = web_sales.ws_item_sk) AND (web_returns.wr_order_number = web_sales.ws_order_number)))) AS salesreturns INNER JOIN date_dim ON salesreturns.dsk = date_dim.d_date_sk INNER JOIN web_site ON salesreturns.k = web_site.web_site_sk WHERE (date_dim.d_year = 2001) GROUP BY web_site.web_site_id) AS wsr) AS x"#;
        let out = substitute_co_located_join_inputs(tail, "web_returns", "web_sales");
        assert!(out.is_some(), "expected Some, got None");
        let out = out.unwrap();
        assert!(
            out.contains("shuffle_input_1") && out.contains("shuffle_input_0"),
            "{out}"
        );
    }

    #[test]
    fn substitute_co_located_joins_still_accepts_bare_idents() {
        let tail = "FROM web_returns LEFT OUTER JOIN web_sales ON web_returns.wr_item_sk = web_sales.ws_item_sk";
        let out = substitute_co_located_join_inputs(tail, "web_returns", "web_sales").unwrap();
        assert!(
            out.contains("(SELECT * FROM shuffle_input_1) AS web_returns"),
            "{out}"
        );
        assert!(
            out.contains("(SELECT * FROM shuffle_input_0) AS web_sales"),
            "{out}"
        );
    }

    #[test]
    fn table_ref_sql_preserves_qualification() {
        assert_eq!(table_ref_sql(&TableReference::bare("lineitem")), "lineitem");
        assert_eq!(
            table_ref_sql(&TableReference::partial("tpch_sf100", "lineitem")),
            "tpch_sf100.lineitem"
        );
        assert_eq!(
            table_ref_sql(&TableReference::full("glue", "tpch_sf100", "lineitem")),
            "glue.tpch_sf100.lineitem"
        );
    }

    #[test]
    fn qualified_table_sql_reads_full_table_reference_from_scan() {
        use datafusion::logical_expr::logical_plan::builder::LogicalTableSource;

        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let source = Arc::new(LogicalTableSource::new(schema));
        let lp = LogicalPlanBuilder::scan(
            TableReference::full("glue", "tpch_sf100", "lineitem"),
            source,
            None,
        )
        .unwrap()
        .filter(lit(true))
        .unwrap()
        .build()
        .unwrap();
        assert_eq!(
            qualified_table_sql(&lp, "lineitem"),
            "glue.tpch_sf100.lineitem"
        );
        assert_eq!(
            find_qualified_table_sql(&lp, "orders"),
            None,
            "missing bare name must not invent a qualification"
        );
    }

    #[test]
    fn plain_sql_without_join_side_aliases_is_accepted() {
        assert!(reject_out_of_scope_join_alias_refs(
            "SELECT a, sum(b) FROM t WHERE c > 1 GROUP BY a"
        )
        .is_ok());
    }

    #[test]
    fn correlated_reference_into_a_nested_subquery_is_accepted() {
        // `left` is bound one level out and used inside the EXISTS — the legitimate shape.
        let sql = r#"SELECT count(1) FROM (SELECT * FROM t) AS "left" WHERE EXISTS (SELECT 1 FROM u WHERE (`left`.k = u.k))"#;
        assert!(reject_out_of_scope_join_alias_refs(sql).is_ok());
        // The rewrite is a no-op on already-in-scope SQL.
        assert_eq!(rewrite_out_of_scope_join_alias_refs(sql).unwrap(), sql);
    }

    #[test]
    fn dangling_reference_to_the_enclosing_alias_is_rewritten() {
        // TPC-DS Q38/Q87 shape: `left` is bound inside the parens that `AS hot_cust` closes, so
        // the second EXISTS's `left` is dangling — but the Unparser did emit the outer alias
        // that owns it (`hot_cust`), so the reference can be rewritten rather than rejected.
        let sql = r#"SELECT count(1) FROM (SELECT * FROM (SELECT * FROM t) AS "left" WHERE EXISTS (SELECT 1 FROM u WHERE (`left`.k = u.k))) AS hot_cust WHERE EXISTS (SELECT 1 FROM v WHERE (`left`.k = v.k))"#;

        // Unrewritten, this is still the unsupported dangling shape.
        let err = reject_out_of_scope_join_alias_refs(sql).expect_err("dangling `left`");
        assert!(err.to_string().contains("outside the scope"), "{err}");

        let rewritten = rewrite_out_of_scope_join_alias_refs(sql).expect("recoverable");
        let expected = r#"SELECT count(1) FROM (SELECT * FROM (SELECT * FROM t) AS "left" WHERE EXISTS (SELECT 1 FROM u WHERE (`left`.k = u.k))) AS hot_cust WHERE EXISTS (SELECT 1 FROM v WHERE (`hot_cust`.k = v.k))"#;
        assert_eq!(rewritten, expected);

        // The in-scope correlation inside the first EXISTS is untouched; only the dangling
        // second reference was rewritten. The rejector, run as a safety net, now passes.
        assert!(reject_out_of_scope_join_alias_refs(&rewritten).is_ok());
    }

    #[test]
    fn rewrite_absorbs_a_double_quoted_outer_alias_matching_the_uses_quote_style() {
        // Same shape, but the outer alias is `AS "hot_cust"` and the dangling use is
        // double-quoted — the rewrite must preserve that quote style rather than always
        // emitting backticks.
        let sql = r#"SELECT * FROM (SELECT * FROM t AS "left" WHERE EXISTS (SELECT 1 FROM u WHERE ("left".k = u.k))) AS "hot_cust" WHERE EXISTS (SELECT 1 FROM v WHERE ("left".k = v.k))"#;
        let rewritten = rewrite_out_of_scope_join_alias_refs(sql).expect("recoverable");
        let expected = r#"SELECT * FROM (SELECT * FROM t AS "left" WHERE EXISTS (SELECT 1 FROM u WHERE ("left".k = u.k))) AS "hot_cust" WHERE EXISTS (SELECT 1 FROM v WHERE ("hot_cust".k = v.k))"#;
        assert_eq!(rewritten, expected);
        assert!(reject_out_of_scope_join_alias_refs(&rewritten).is_ok());
    }

    #[test]
    fn reference_with_no_definition_at_all_is_rejected() {
        assert!(reject_out_of_scope_join_alias_refs(r#"SELECT "left".a FROM t"#).is_err());
        // No definition ever appears, so there's nothing to absorb into — still unfixable.
        assert!(rewrite_out_of_scope_join_alias_refs(r#"SELECT "left".a FROM t"#).is_err());
    }

    #[test]
    fn a_sibling_scopes_definition_does_not_leak() {
        let sql = r#"SELECT * FROM (SELECT 1 FROM x AS "left" WHERE `left`.a = 1), (SELECT `left`.b FROM y)"#;
        assert!(reject_out_of_scope_join_alias_refs(sql).is_err());
        // The first scope closes into a sibling `,`, not an `AS <alias>`, so pending is cleared
        // rather than absorbed — the second, unrelated `left` use in the sibling stays unfixable.
        assert!(rewrite_out_of_scope_join_alias_refs(sql).is_err());
    }
}

#[cfg(test)]
mod sanitize_tests {
    use super::{fix_quoted_column_after_dot, fix_quoted_table_dot_access, sanitize_generated_sql};

    #[test]
    fn quoted_column_after_dot_becomes_unquoted() {
        let sql = r#"sum(shipping."volume")"#;
        assert_eq!(fix_quoted_column_after_dot(sql), "sum(shipping.volume)");
    }

    #[test]
    fn quoted_table_dot_access_uses_backticks() {
        let sql = r#""part".p_partkey = lineitem.l_partkey"#;
        assert_eq!(
            fix_quoted_table_dot_access(sql),
            "`part`.p_partkey = lineitem.l_partkey"
        );
    }

    #[test]
    fn sanitize_composes_both_fixes() {
        let sql = r#"SELECT sum(shipping."volume") FROM "part" WHERE "part".p_partkey = 1"#;
        let got = sanitize_generated_sql(sql);
        assert!(got.contains("shipping.volume"));
        assert!(got.contains("`part`.p_partkey"));
        assert!(!got.contains(r#""volume""#));
    }

    #[test]
    fn sanitize_rewrites_pg_style_interval_literals() {
        // Unparser form that broke TPC-H Q6 distributed stage SQL under Databricks dialect.
        assert_eq!(
            sanitize_generated_sql(
                "SELECT * FROM t WHERE d < (CAST('1994-01-01' AS DATE) + INTERVAL '12 MONS')"
            ),
            "SELECT * FROM t WHERE d < (CAST('1994-01-01' AS DATE) + INTERVAL '12' MONTH)"
        );
        assert_eq!(
            sanitize_generated_sql("x + INTERVAL '90 DAYS'"),
            "x + INTERVAL '90' DAY"
        );
        // Already-legal form is left alone.
        assert_eq!(
            sanitize_generated_sql("x + INTERVAL '1' YEAR"),
            "x + INTERVAL '1' YEAR"
        );
        // Content inside string literals is not rewritten.
        assert_eq!(
            sanitize_generated_sql("SELECT 'INTERVAL ''12 MONS''' AS s"),
            "SELECT 'INTERVAL ''12 MONS''' AS s"
        );
    }

    #[test]
    fn sanitize_rewrites_signed_and_abbreviated_interval_units() {
        assert_eq!(
            sanitize_generated_sql("x - INTERVAL '-90 DAYS'"),
            "x - INTERVAL '-90' DAY"
        );
        assert_eq!(
            sanitize_generated_sql("x + Interval '2 YR'"),
            "x + Interval '2' YEAR"
        );
        assert_eq!(
            sanitize_generated_sql("x + INTERVAL '3 MON'"),
            "x + INTERVAL '3' MONTH"
        );
        assert_eq!(
            sanitize_generated_sql("x + INTERVAL '4 HRS'"),
            "x + INTERVAL '4' HOUR"
        );
        assert_eq!(
            sanitize_generated_sql("x + INTERVAL '5 MINS'"),
            "x + INTERVAL '5' MINUTE"
        );
        assert_eq!(
            sanitize_generated_sql("x + INTERVAL '6 SECS'"),
            "x + INTERVAL '6' SECOND"
        );
    }

    #[test]
    fn sanitize_leaves_multi_unit_pg_interval_bodies_alone() {
        // Multi-unit combined forms are not safely rewritable — leave the Unparser output as-is.
        assert_eq!(
            sanitize_generated_sql("x + INTERVAL '1 YEAR 2 MONS'"),
            "x + INTERVAL '1 YEAR 2 MONS'"
        );
    }
}

#[cfg(test)]
mod agg_combine_tests {
    use super::partial_combine_sql;

    #[test]
    fn stddev_samp_combine_uses_nminus1_and_sqrt() {
        // `stddev`/`stddev_samp` resolve to DataFusion's canonical `stddev` name.
        let (sel, combine) = partial_combine_sql("stddev", 0, "t.v").expect("supported");
        assert_eq!(
            sel,
            vec!["sum(t.v) AS a0s, sum((t.v)*(t.v)) AS a0q, count(t.v) AS a0c"]
        );
        assert_eq!(
            combine,
            "sqrt((sum(a0q) - (sum(a0s)*sum(a0s))/NULLIF(sum(a0c),0)) / NULLIF(sum(a0c)-1, 0)) AS r0"
        );
    }

    #[test]
    fn stddev_pop_combine_divides_by_n() {
        let (_, combine) = partial_combine_sql("stddev_pop", 2, "x").expect("supported");
        assert_eq!(
            combine,
            "sqrt((sum(a2q) - (sum(a2s)*sum(a2s))/NULLIF(sum(a2c),0)) / NULLIF(sum(a2c), 0)) AS r2"
        );
    }

    #[test]
    fn var_samp_combine_matches_stddev_without_sqrt() {
        // `var`/`var_samp`/`var_sample`/`variance` all resolve to DataFusion's canonical `var` name.
        let (_, combine) = partial_combine_sql("var", 0, "x").expect("supported");
        assert_eq!(
            combine,
            "(sum(a0q) - (sum(a0s)*sum(a0s))/NULLIF(sum(a0c),0)) / NULLIF(sum(a0c)-1, 0) AS r0"
        );
    }

    #[test]
    fn var_pop_combine_matches_stddev_pop_without_sqrt() {
        let (_, combine) = partial_combine_sql("var_pop", 0, "x").expect("supported");
        assert_eq!(
            combine,
            "(sum(a0q) - (sum(a0s)*sum(a0s))/NULLIF(sum(a0c),0)) / NULLIF(sum(a0c), 0) AS r0"
        );
    }

    #[test]
    fn existing_aggregates_are_unchanged() {
        assert_eq!(
            partial_combine_sql("sum", 0, "x").unwrap(),
            (
                vec!["sum(x) AS a0".to_string()],
                "sum(a0) AS r0".to_string()
            )
        );
        assert_eq!(
            partial_combine_sql("avg", 1, "x").unwrap(),
            (
                vec!["sum(x) AS a1s, count(x) AS a1c".to_string()],
                "(sum(a1s) / NULLIF(sum(a1c), 0)) AS r1".to_string()
            )
        );
    }

    #[test]
    fn unsupported_aggregate_is_an_honest_error_not_a_wrong_answer() {
        assert!(partial_combine_sql("median", 0, "x").is_err());
    }
}

#[cfg(test)]
mod grouping_set_tests {
    use std::sync::Arc;

    use datafusion::logical_expr::{Expr, GroupingSet};
    use datafusion::prelude::col;
    use oxidant_loom::arrow::array::{Int64Array, RecordBatch};
    use oxidant_loom::arrow::datatypes::{DataType, Field, Schema};
    use oxidant_loom::arrow::util::pretty::pretty_format_batches;
    use oxidant_loom::Engine;

    use super::{
        final_group_by_sql, flattened_group_exprs, grouping_set_shares_first_column,
        plan_distributed_logical,
    };

    #[test]
    fn two_phase_admission_rule() {
        // ROLLUP always qualifies (every non-grand-total level starts with the first column).
        assert!(grouping_set_shares_first_column(&[Expr::GroupingSet(
            GroupingSet::Rollup(vec![col("a"), col("b")])
        )]));
        // CUBE never does (sibling levels like (b) split under a g0 hash).
        assert!(!grouping_set_shares_first_column(&[Expr::GroupingSet(
            GroupingSet::Cube(vec![col("a"), col("b")])
        )]));
        // Explicit sets qualify level-by-level on the first *flattened* column: here the levels
        // are written (b), (a, b) so b is first and shared — hash by b.
        assert!(grouping_set_shares_first_column(&[Expr::GroupingSet(
            GroupingSet::GroupingSets(vec![vec![col("b")], vec![col("a"), col("b")], vec![]])
        )]));
        // …but (b), (a, b) written with a first does not share a.
        assert!(!grouping_set_shares_first_column(&[Expr::GroupingSet(
            GroupingSet::GroupingSets(vec![vec![col("a")], vec![col("b")]])
        )]));
        // A duplicated grand-total level cannot be reproduced by summing per-partition partials.
        assert!(!grouping_set_shares_first_column(&[Expr::GroupingSet(
            GroupingSet::GroupingSets(vec![vec![col("a")], vec![], vec![]])
        )]));
        // No grouping columns at all: the caller's global-aggregate path owns this.
        assert!(!grouping_set_shares_first_column(&[Expr::GroupingSet(
            GroupingSet::GroupingSets(vec![vec![]])
        )]));
    }

    fn table() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k1", DataType::Int64, false),
            Field::new("k2", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 1, 2])),
                Arc::new(Int64Array::from(vec![10, 20, 10])),
                Arc::new(Int64Array::from(vec![5, 7, 11])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn renders_rollup_cube_and_explicit_grouping_sets_over_safe_columns() {
        let rollup = vec![Expr::GroupingSet(GroupingSet::Rollup(vec![
            col("a"),
            col("b"),
        ]))];
        assert_eq!(flattened_group_exprs(&rollup).len(), 2);
        assert_eq!(final_group_by_sql(&rollup, 2).unwrap(), "ROLLUP (g0, g1)");

        let cube = vec![Expr::GroupingSet(GroupingSet::Cube(vec![
            col("a"),
            col("b"),
        ]))];
        assert_eq!(final_group_by_sql(&cube, 2).unwrap(), "CUBE (g0, g1)");

        let grouping_sets = vec![Expr::GroupingSet(GroupingSet::GroupingSets(vec![
            vec![col("a"), col("b")],
            vec![col("b")],
            vec![],
        ]))];
        assert_eq!(flattened_group_exprs(&grouping_sets).len(), 2);
        assert_eq!(
            final_group_by_sql(&grouping_sets, 2).unwrap(),
            "GROUPING SETS ((g0, g1), (g1), ())"
        );
    }

    #[tokio::test]
    async fn rollup_two_phase_keyed_partial_rollup_and_fixup_match_single_node() {
        let engine = Engine::new();
        engine.register_batches("t", vec![table()]).unwrap();
        let sql = "SELECT k1, k2, SUM(v) AS total FROM t \
                   GROUP BY ROLLUP (k1, k2) \
                   ORDER BY k1 NULLS FIRST, k2 NULLS FIRST";
        let logical = engine.logical_plan(sql).await.unwrap();
        let dq = plan_distributed_logical(&logical, &[]).expect("ROLLUP should distribute");

        // Two-phase plan: keyed partial → per-partition rollup → grand-total fixup.
        assert_eq!(dq.stages.len(), 3, "{dq:?}");
        assert_eq!(
            dq.stages[0].hash_key_cols,
            vec![0],
            "the finest-level partial hashes by g0 so every g0-bearing rollup level co-locates"
        );
        assert!(!dq.stages[0].sql.contains("ROLLUP"), "{}", dq.stages[0].sql);
        assert!(dq.stages[0].sql.contains("AS g0"), "{}", dq.stages[0].sql);
        assert!(dq.stages[0].sql.contains("AS g1"), "{}", dq.stages[0].sql);
        assert_eq!(dq.stages[1].upstream_stage_ids, vec![0]);
        assert_eq!(
            dq.stages[1].hash_key_cols,
            vec![0],
            "exact rows stay in their g0 class; grand-total partials (g0 NULL) funnel to one bucket"
        );
        assert!(
            dq.stages[1]
                .sql
                .contains("GROUP BY ROLLUP (g0, g1) HAVING COUNT(*) > 0"),
            "the per-partition rollup keeps the empty-partition guard: {}",
            dq.stages[1].sql
        );
        assert!(
            dq.stages[1].sql.contains("grouping(g0) AS __gid"),
            "grand-total partials are tagged for the fixup: {}",
            dq.stages[1].sql
        );
        assert_eq!(dq.stages[2].upstream_stage_ids, vec![1]);
        assert!(
            dq.stages[2].sql.contains("__gid = 0")
                && dq.stages[2].sql.contains("__gid = 1 HAVING COUNT(*) > 0"),
            "the fixup passes exact rows through and combines ≤ #partitions grand totals: {}",
            dq.stages[2].sql
        );

        // Single-partition simulation of the three-stage pipeline.
        let partial = engine.sql(&dq.stages[0].sql).await.unwrap();
        let partial_schema = partial[0].schema();
        let mid_engine = Engine::new();
        mid_engine
            .register_batches("shuffle_input", partial)
            .unwrap();
        let mid = mid_engine.sql(&dq.stages[1].sql).await.unwrap();
        let mid_schema = mid[0].schema();
        let final_engine = Engine::new();
        final_engine.register_batches("shuffle_input", mid).unwrap();
        let combined = final_engine.sql(&dq.stages[2].sql).await.unwrap();
        final_engine.register_batches("result", combined).unwrap();
        let actual = final_engine
            .sql(dq.finalize_sql.as_deref().expect("ORDER BY finalize"))
            .await
            .unwrap();
        let expected = engine.sql(sql).await.unwrap();
        assert_eq!(
            pretty_format_batches(&actual).unwrap().to_string(),
            pretty_format_batches(&expected).unwrap().to_string()
        );

        // A partition with a typed empty shuffle bucket must not manufacture a ROLLUP
        // grand-total row at either the rollup or the fixup stage.
        let empty_engine = Engine::new();
        empty_engine
            .register_batches(
                "shuffle_input",
                vec![RecordBatch::new_empty(partial_schema)],
            )
            .unwrap();
        let empty = empty_engine.sql(&dq.stages[1].sql).await.unwrap();
        assert_eq!(empty.iter().map(RecordBatch::num_rows).sum::<usize>(), 0);
        let empty_fix = Engine::new();
        empty_fix
            .register_batches("shuffle_input", vec![RecordBatch::new_empty(mid_schema)])
            .unwrap();
        let empty = empty_fix.sql(&dq.stages[2].sql).await.unwrap();
        assert_eq!(empty.iter().map(RecordBatch::num_rows).sum::<usize>(), 0);
    }

    #[tokio::test]
    async fn rollup_two_phase_avg_count_and_grouping_match_single_node() {
        // Non-associative aggregates (AVG rides SUM/COUNT components) and grouping() outputs
        // recompute correctly through the grand-total fixup.
        let engine = Engine::new();
        engine.register_batches("t", vec![table()]).unwrap();
        let sql = "SELECT k1, k2, grouping(k1) AS gk, COUNT(*) AS c, AVG(v) AS av \
                   FROM t GROUP BY ROLLUP (k1, k2) \
                   ORDER BY k1 NULLS FIRST, k2 NULLS FIRST";
        let logical = engine.logical_plan(sql).await.unwrap();
        let dq = plan_distributed_logical(&logical, &[]).expect("ROLLUP should distribute");
        assert_eq!(dq.stages.len(), 3, "{dq:?}");
        assert!(
            dq.stages[1]
                .sql
                .contains("sum(a2s) AS a2s, sum(a2c) AS a2c"),
            "the per-partition rollup carries AVG's recombine components: {}",
            dq.stages[1].sql
        );
        assert!(
            dq.stages[2].sql.contains("max(r0) AS r0"),
            "grouping() recombines as max (1 on every grand-total partial): {}",
            dq.stages[2].sql
        );

        let partial = engine.sql(&dq.stages[0].sql).await.unwrap();
        let mid_engine = Engine::new();
        mid_engine
            .register_batches("shuffle_input", partial)
            .unwrap();
        let mid = mid_engine.sql(&dq.stages[1].sql).await.unwrap();
        let final_engine = Engine::new();
        final_engine.register_batches("shuffle_input", mid).unwrap();
        let combined = final_engine.sql(&dq.stages[2].sql).await.unwrap();
        final_engine.register_batches("result", combined).unwrap();
        let actual = final_engine
            .sql(dq.finalize_sql.as_deref().expect("ORDER BY finalize"))
            .await
            .unwrap();
        let expected = engine.sql(sql).await.unwrap();
        assert_eq!(
            pretty_format_batches(&actual).unwrap().to_string(),
            pretty_format_batches(&expected).unwrap().to_string()
        );
    }

    #[tokio::test]
    async fn unsafe_grouping_set_shapes_keep_the_gather_plan() {
        // A level that does not contain the first flattened column ((k2) here, plus every CUBE
        // sibling level) would split across a g0 hash — the partition-0 gather stays.
        for sql in [
            "SELECT k1, k2, SUM(v) AS total FROM t \
             GROUP BY GROUPING SETS ((k1, k2), (k2), ())",
            "SELECT k1, k2, SUM(v) AS total FROM t GROUP BY CUBE (k1, k2)",
        ] {
            let engine = Engine::new();
            engine.register_batches("t", vec![table()]).unwrap();
            let logical = engine.logical_plan(sql).await.unwrap();
            let dq = plan_distributed_logical(&logical, &[]).expect("should distribute");
            assert_eq!(dq.stages.len(), 2, "gather plan kept for {sql}: {dq:?}");
            assert!(
                dq.stages[0].hash_key_cols.is_empty(),
                "partial still gathers to partition 0 for {sql}"
            );
            assert!(
                dq.stages[1].sql.contains("HAVING COUNT(*) > 0"),
                "the gather combine keeps its empty-partition guard for {sql}: {}",
                dq.stages[1].sql
            );
        }
    }
}

#[cfg(test)]
mod cse_tests {
    use crate::driver::{ExchangeMode, StageDef};

    use super::{cse_identical_stages, sql_contains_volatile};

    fn stage(id: u32, sql: &str, upstreams: &[u32]) -> StageDef {
        StageDef::new(id, sql, upstreams.to_vec(), vec![])
    }

    fn summary(stages: &[StageDef]) -> Vec<(u32, String, Vec<u32>)> {
        stages
            .iter()
            .map(|s| (s.stage_id, s.sql.clone(), s.upstream_stage_ids.clone()))
            .collect()
    }

    #[test]
    fn identical_stages_merge_and_consumers_retarget() {
        // Leaves 0≡2 merge immediately; their combines 1≡3 become identical only once stage 3's
        // upstream rewrites 2→0 — the fixpoint cascade. Stage 4 then reads stage 1 twice (the
        // Q39-style multi-consumer pull the scheduler already supports).
        let mut stages = vec![
            stage(0, "SELECT k, sum(v) AS a0 FROM t GROUP BY k", &[]),
            stage(1, "SELECT sum(a0) AS r0 FROM shuffle_input", &[0]),
            stage(2, "SELECT k, sum(v) AS a0 FROM t GROUP BY k", &[]),
            stage(3, "SELECT sum(a0) AS r0 FROM shuffle_input", &[2]),
            stage(
                4,
                "SELECT * FROM shuffle_input_0 JOIN shuffle_input_1 USING (k)",
                &[1, 3],
            ),
        ];
        cse_identical_stages(&mut stages);
        let expected: Vec<(u32, String, Vec<u32>)> = vec![
            (
                0,
                "SELECT k, sum(v) AS a0 FROM t GROUP BY k".to_string(),
                vec![],
            ),
            (
                1,
                "SELECT sum(a0) AS r0 FROM shuffle_input".to_string(),
                vec![0],
            ),
            (
                4,
                "SELECT * FROM shuffle_input_0 JOIN shuffle_input_1 USING (k)".to_string(),
                vec![1, 1],
            ),
        ];
        assert_eq!(summary(&stages), expected);
    }

    #[test]
    fn same_sql_with_different_upstreams_does_not_merge() {
        let mut stages = vec![
            stage(0, "SELECT * FROM a", &[]),
            stage(1, "SELECT * FROM b", &[]),
            stage(2, "SELECT sum(x) FROM shuffle_input", &[0]),
            stage(3, "SELECT sum(x) FROM shuffle_input", &[1]),
            stage(
                4,
                "SELECT * FROM shuffle_input_0 JOIN shuffle_input_1 USING (k)",
                &[2, 3],
            ),
        ];
        let before = summary(&stages);
        cse_identical_stages(&mut stages);
        assert_eq!(summary(&stages), before);
    }

    #[test]
    fn hash_key_or_exchange_difference_does_not_merge() {
        let mut keyed = stage(0, "SELECT k, sum(v) AS a0 FROM t GROUP BY k", &[]);
        keyed.hash_key_cols = vec![0];
        let gathered = stage(1, "SELECT k, sum(v) AS a0 FROM t GROUP BY k", &[]);
        let mut forwarded = stage(2, "SELECT k, sum(v) AS a0 FROM t GROUP BY k", &[]);
        forwarded.hash_key_cols = vec![0];
        forwarded.exchange = ExchangeMode::Forward;
        let mut stages = vec![
            keyed,
            gathered,
            forwarded,
            stage(3, "SELECT * FROM shuffle_input", &[0]),
        ];
        // All three producers are consumed by stage 3, so consumption itself is not what
        // protects them here.
        stages[3].upstream_stage_ids = vec![0, 1, 2];
        let before = summary(&stages);
        cse_identical_stages(&mut stages);
        assert_eq!(summary(&stages), before);
    }

    #[test]
    fn volatile_stage_sql_never_merges() {
        assert!(sql_contains_volatile("SELECT rand() AS r FROM t"));
        assert!(sql_contains_volatile("SELECT x FROM t WHERE ts < now()"));
        assert!(sql_contains_volatile(
            "SELECT CURRENT_TIMESTAMP AS ts FROM t"
        ));
        assert!(!sql_contains_volatile("SELECT k, sum(v) FROM t GROUP BY k"));
        let mut stages = vec![
            stage(0, "SELECT rand() AS r, k FROM t", &[]),
            stage(1, "SELECT rand() AS r, k FROM t", &[]),
            stage(
                2,
                "SELECT * FROM shuffle_input_0 JOIN shuffle_input_1 USING (k)",
                &[0, 1],
            ),
        ];
        let before = summary(&stages);
        cse_identical_stages(&mut stages);
        assert_eq!(summary(&stages), before);
    }

    #[test]
    fn unconsumed_output_stage_never_merges() {
        // Stages 0 and 2 are byte-identical, but stage 2 is the (unconsumed) output stage:
        // merging it away — or merging its consumed twin into it — would break the driver's
        // exactly-one-output contract.
        let mut stages = vec![
            stage(0, "SELECT sum(v) AS r0 FROM t", &[]),
            stage(1, "SELECT r0 + 1 FROM shuffle_input", &[0]),
            stage(2, "SELECT sum(v) AS r0 FROM t", &[]),
        ];
        let before = summary(&stages);
        cse_identical_stages(&mut stages);
        assert_eq!(summary(&stages), before);
    }
}

/// Regression locks for PR #52 planner fixes: Q21-shaped HAVING above an aliasing projection,
/// fail-loud unmapped HAVING columns, and AVG recombine without a forced DOUBLE cast.
#[cfg(test)]
mod peel_remap_tests {
    use std::sync::Arc;

    use datafusion::prelude::col;
    use oxidant_loom::arrow::array::{Int64Array, RecordBatch};
    use oxidant_loom::arrow::datatypes::{DataType, Field, Schema};
    use oxidant_loom::Engine;

    use super::{build_remap, ensure_all_columns_remapped, peel, plan_distributed_logical};

    fn tiny_table() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![0i64, 1, 0])),
                Arc::new(Int64Array::from(vec![10i64, 20, 30])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn ensure_all_columns_remapped_accepts_stage_names_only() {
        ensure_all_columns_remapped(&col("r0")).expect("r0 is a remapped aggregate output");
        ensure_all_columns_remapped(&col("g0")).expect("g0 is a remapped group key");
        let err = ensure_all_columns_remapped(&col("inv_before"))
            .expect_err("original alias must not slip through");
        assert!(err.to_string().contains("HAVING references"), "got: {err}");
    }

    #[tokio::test]
    async fn q21_shaped_having_above_alias_projection_remaps() {
        // Filter → SubqueryAlias → Projection → Aggregate. An earlier peel required the Filter to
        // sit directly on Aggregate and silently dropped the predicate (unfiltered Q21 rows).
        let engine = Engine::new();
        engine.register_batches("t", vec![tiny_table()]).unwrap();
        let lp = engine
            .logical_plan(
                "SELECT * FROM (\
                     SELECT k, SUM(v) AS inv_before FROM t GROUP BY k\
                 ) x WHERE inv_before > 10",
            )
            .await
            .unwrap();

        let peeled = peel(&lp).expect("Q21-shaped plan must peel");
        assert!(
            !peeled.having.is_empty(),
            "intervening Filter must be collected as HAVING"
        );
        assert!(
            !peeled.alias_projections.is_empty(),
            "inner SUM alias projection must be retained for remap"
        );
        let remap = build_remap(&peeled);
        assert_eq!(
            remap.get("inv_before").map(String::as_str),
            Some("r0"),
            "alias inv_before must map to aggregate slot r0; got {remap:?}"
        );

        let dq = plan_distributed_logical(&lp, &[]).expect("must distribute");
        let final_sql = &dq.stages.last().expect("stages").sql;
        // Predicate must use remapped `r0`; the output projection may still alias it back
        // to `"inv_before"` for schema fidelity.
        assert!(
            final_sql.contains("WHERE") && final_sql.contains("(r0 > 10)"),
            "final stage must filter on remapped r0; got: {final_sql}"
        );
        assert!(
            !final_sql.contains("inv_before >"),
            "HAVING must not filter on the pre-remap alias name; got: {final_sql}"
        );
    }

    #[tokio::test]
    async fn avg_recombine_does_not_force_double_cast() {
        // Forcing CAST(... AS DOUBLE) made TPC-DS Q7/Q26 return the right number at the wrong scale.
        let engine = Engine::new();
        engine.register_batches("t", vec![tiny_table()]).unwrap();
        let lp = engine
            .logical_plan("SELECT k, AVG(v) AS av FROM t GROUP BY k")
            .await
            .unwrap();
        let dq = plan_distributed_logical(&lp, &[]).expect("avg must distribute");
        let sql = dq
            .stages
            .iter()
            .map(|s| s.sql.as_str())
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            sql.contains("(sum(a0s) / NULLIF(sum(a0c), 0))"),
            "expected SUM/COUNT recombine; got:\n{sql}"
        );
        let upper = sql.to_uppercase();
        assert!(
            !upper.contains("AS DOUBLE") && !upper.contains("AS FLOAT64"),
            "AVG recombine must not force DOUBLE; got:\n{sql}"
        );
    }
}

/// The driver optimizes the logical plan BEFORE the stage split (`plan_distributed`):
/// otherwise stage SQL is unparsed from the raw SQL shape and no pushdown can cross a
/// stage boundary (TPC-DS Q78's `ss_sold_year=2000` stayed in the final stage while leaf
/// stages scanned and grouped every year of all three fact tables — 6.3s vs 1.5s at SF10).
#[cfg(test)]
mod driver_side_optimization_tests {
    use std::sync::Arc;

    use oxidant_loom::arrow::array::{Int64Array, RecordBatch};
    use oxidant_loom::arrow::datatypes::{DataType, Field, Schema};
    use oxidant_loom::Engine;

    use super::plan_distributed;

    fn fact(date_col: &str, item_col: &str, cust_col: &str, qty_col: &str) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new(date_col, DataType::Int64, false),
            Field::new(item_col, DataType::Int64, false),
            Field::new(cust_col, DataType::Int64, false),
            Field::new(qty_col, DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(Int64Array::from(vec![10, 20, 10])),
                Arc::new(Int64Array::from(vec![100, 200, 100])),
                Arc::new(Int64Array::from(vec![5, 7, 9])),
            ],
        )
        .unwrap()
    }

    fn date_dim() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("d_date_sk", DataType::Int64, false),
            Field::new("d_year", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(Int64Array::from(vec![1999, 2000, 2000])),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn outer_filter_pushes_into_cte_leaf_stage_before_split() {
        let engine = Engine::new();
        engine
            .register_batches(
                "store_sales",
                vec![fact(
                    "ss_sold_date_sk",
                    "ss_item_sk",
                    "ss_customer_sk",
                    "ss_quantity",
                )],
            )
            .unwrap();
        engine
            .register_batches("date_dim", vec![date_dim()])
            .unwrap();
        let sql = "WITH ss AS (\
                       SELECT d_year AS ss_sold_year, ss_item_sk, ss_customer_sk, \
                              SUM(ss_quantity) AS ss_qty \
                       FROM store_sales JOIN date_dim ON ss_sold_date_sk = d_date_sk \
                       GROUP BY d_year, ss_item_sk, ss_customer_sk) \
                   SELECT ss_sold_year, ss_item_sk, ss_customer_sk, ss_qty FROM ss \
                   WHERE ss_sold_year = 2000 \
                   ORDER BY ss_sold_year, ss_item_sk, ss_customer_sk LIMIT 100";
        let dq = plan_distributed(&engine, sql, &[])
            .await
            .expect("CTE agg shape should distribute");
        assert!(
            dq.stages.len() >= 2,
            "expected a multi-stage plan, got: {dq:?}"
        );
        // The fact shard scan stays unfiltered (the inner join with the filtered date_dim
        // eliminates non-2000 rows); the win is the pushed predicate on the date_dim leaf.
        let dim_stages: Vec<_> = dq
            .stages
            .iter()
            .filter(|s| s.sql.contains("FROM date_dim"))
            .collect();
        assert!(!dim_stages.is_empty(), "{dq:?}");
        for st in &dim_stages {
            assert!(
                st.sql.contains("d_year = 2000"),
                "year filter not pushed into the date_dim leaf stage: {}",
                st.sql
            );
        }
    }

    #[tokio::test]
    async fn outer_filter_propagates_through_left_join_equality_into_right_cte() {
        let engine = Engine::new();
        engine
            .register_batches(
                "store_sales",
                vec![fact(
                    "ss_sold_date_sk",
                    "ss_item_sk",
                    "ss_customer_sk",
                    "ss_quantity",
                )],
            )
            .unwrap();
        engine
            .register_batches(
                "web_sales",
                vec![fact(
                    "ws_sold_date_sk",
                    "ws_item_sk",
                    "ws_bill_customer_sk",
                    "ws_quantity",
                )],
            )
            .unwrap();
        engine
            .register_batches("date_dim", vec![date_dim()])
            .unwrap();
        let sql = "WITH ss AS (\
                       SELECT d_year AS ss_sold_year, ss_item_sk, ss_customer_sk, \
                              SUM(ss_quantity) AS ss_qty \
                       FROM store_sales JOIN date_dim ON ss_sold_date_sk = d_date_sk \
                       GROUP BY d_year, ss_item_sk, ss_customer_sk), \
                   ws AS (\
                       SELECT d_year AS ws_sold_year, ws_item_sk, ws_bill_customer_sk AS ws_customer_sk, \
                              SUM(ws_quantity) AS ws_qty \
                       FROM web_sales JOIN date_dim ON ws_sold_date_sk = d_date_sk \
                       GROUP BY d_year, ws_item_sk, ws_bill_customer_sk) \
                   SELECT ss_sold_year, ss_item_sk, ss_customer_sk, ss_qty, ws_qty \
                   FROM ss LEFT JOIN ws ON ws_sold_year = ss_sold_year \
                       AND ws_item_sk = ss_item_sk AND ws_customer_sk = ss_customer_sk \
                   WHERE ss_sold_year = 2000 \
                   ORDER BY ss_sold_year, ss_item_sk, ss_customer_sk LIMIT 100";
        let dq = plan_distributed(&engine, sql, &[])
            .await
            .expect("two-CTE left-join shape should distribute");
        // Both CTEs' date_dim scans must carry the filter: the ss side via the plain
        // group-key pushdown, the ws side via the LEFT JOIN equality inference
        // (ws_sold_year = ss_sold_year = 2000). Identical scans may CSE-merge, so assert
        // the invariant over every date_dim stage rather than counting them.
        let dim_stages: Vec<_> = dq
            .stages
            .iter()
            .filter(|s| s.sql.contains("FROM date_dim"))
            .collect();
        assert!(
            !dim_stages.is_empty(),
            "no date_dim leaf stage in the split: {dq:?}"
        );
        for st in &dim_stages {
            assert!(
                st.sql.contains("d_year = 2000"),
                "year filter not pushed into a date_dim leaf stage: {}",
                st.sql
            );
        }
        for table in ["store_sales", "web_sales"] {
            assert!(
                dq.stages.iter().any(|s| s.sql.contains(table)),
                "no stage scans {table}: {dq:?}"
            );
        }
    }

    /// SQL table aliases (`JOIN date_dim d1`) are the class the splitter cannot re-render
    /// after predicate re-qualification (TPC-DS Q72 broke with "No field named
    /// date_dim.d_year. Did you mean 'd1.d_year'?" on workers). The shape gate must leave
    /// such plans unoptimized — no pushed year filter in any date_dim stage — while the
    /// split itself still succeeds.
    #[tokio::test]
    async fn aliased_dim_shape_is_left_unoptimized() {
        let engine = Engine::new();
        engine
            .register_batches(
                "catalog_sales",
                vec![fact(
                    "cs_sold_date_sk",
                    "cs_item_sk",
                    "cs_bill_customer_sk",
                    "cs_quantity",
                )],
            )
            .unwrap();
        engine
            .register_batches("date_dim", vec![date_dim()])
            .unwrap();
        let sql = "SELECT d1.d_year, cs_item_sk, SUM(cs_quantity) AS q \
                   FROM catalog_sales JOIN date_dim d1 ON cs_sold_date_sk = d1.d_date_sk \
                   WHERE d1.d_year = 2000 \
                   GROUP BY d1.d_year, cs_item_sk \
                   ORDER BY d1.d_year, cs_item_sk LIMIT 100";
        let dq = plan_distributed(&engine, sql, &[])
            .await
            .expect("aliased-dim shape should still distribute");
        for st in dq.stages.iter().filter(|s| s.sql.contains("FROM date_dim")) {
            assert!(
                !st.sql.contains("date_dim.d_year"),
                "the shape gate must keep the optimizer's base-qualified pushdown out of \
                 aliased-dim stages (the splitter's own alias-qualified rendering is the \
                 pre-existing behavior): {}",
                st.sql
            );
        }
    }
}

//! KAN-49 wave-3b ("gather" wave): distributed shapes for the TPC-DS queries that previously
//! fell into the strict-refused whole-fact gather (`try_materialize_complex_fact`). Each shape
//! here replaces that gather with hash-shuffle co-location: rows that must compare equal land on
//! the same shuffle partition, so a per-partition `INTERSECT` / `EXCEPT` / full outer join /
//! self-join / global aggregate is exact, and only the tiny post-aggregate result moves to a
//! single partition.
//!
//! Covered shapes (TPC-DS at the SF10 strict configuration):
//!
//! - [`try_global_count_over_set_op`] — Q38/Q87: global `count(*)` over an `INTERSECT` / `EXCEPT`
//!   (DISTINCT) chain of per-channel `SELECT DISTINCT key…` branches.
//! - [`try_full_outer_join_global_agg`] — Q97: global aggregates over a `FULL OUTER JOIN` of two
//!   per-channel distinct-key aggregates.
//! - [`try_derived_having_scalar_threshold`] — Q24: grouped aggregate over a derived per-key
//!   aggregate with a `HAVING <agg> > (SELECT 0.05*avg(…) FROM <same derived>)` threshold.
//! - [`try_self_join_in_keys`] — Q95: global aggregate whose `IN` key sets come from a self-join
//!   of the sharded fact (the "shuffle-first distinct-key producer").
//! - [`try_ranked_union`] — Q49: `UNION` (distinct) of per-channel arms that carry global
//!   `rank()` windows over a tiny per-item aggregate.
//! - [`try_union_over_derived_ctes`] — Q23 at the SF10 classification: `UNION ALL` of
//!   per-channel arms whose only sharded inputs are shared derived CTEs; each distinct CTE is
//!   planned once (fingerprint dedup) and gathers, and each arm runs exactly once as a
//!   `Forward` stage pulling the CTEs' bucket 0 — or, on a multi-worker cluster, as a
//!   fanned-out export/semi/partial/recombine pipeline with the CTEs re-keyed to the arms'
//!   join columns (KAN-156).
//! - [`try_cross_scalar_threshold`] (used by the Q23 shape) — a grouped aggregate that
//!   cross-joins a single-row derived scalar (`best_ss_customer ⋈ max_store_sales`): the
//!   scalar's own grouped input distributes, a KAN-27 one-row broadcast computes the value,
//!   and the outer combine's HAVING compares against the injected literal.
//!   KAN-158: when the scalar's per-key input (sq2) is a filter-restriction of the outer
//!   per-key aggregate (same group keys + measure; sq2 adds only INNER joins to replicated
//!   dims and filters on those dims — Q23's `date_dim` year window), both share **one** raw
//!   fact-scan export; two consumer partials derive the restricted and unrestricted aggs.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::common::{Column, ScalarValue};
use datafusion::logical_expr::{
    Aggregate, BinaryExpr, Expr, Filter, JoinType, LogicalPlan, LogicalPlanBuilder, Operator,
};
use datafusion::sql::unparser::Unparser;
use oxidant_common::{Error, Result};

use super::dag_splitter::{
    node_id, only_inner_joins, plan_contains_volatile, replace_branches, strip_filters,
};
use super::shape_extensions::{
    build_outer_finalize, collect_subquery_tables, expr_columns, expr_contains_subquery,
    flatten_conjuncts, peek_sort_limit, per_key_agg_parts, plan_contains_outer_reference,
    remap_expr_columns,
};
use super::stage_planner::{
    aggregation_stages_for, base_tables, build_agg_remap, build_finalize, build_remap,
    collect_equijoin_keys, count_table_scans, expr_sql, extract_from_tail, final_group_by_sql,
    finalize_expr_sql, flatten_union_all, flattened_group_exprs, is_grouping_set, output_name,
    partial_and_combine_lists, partial_combine_sql, peel, plan_distributed_logical,
    qualified_table_sql, reject_unsafe_broadcast_shapes, replicated_slice_tables,
    sanitize_generated_sql, sliced_replicate_stamp, strip_alias, unqualify, wrap_output,
    wrap_output_recombine, AggSpec, DistributedQuery, Peeled,
};
use crate::driver::{scalar_literal_supported, ExchangeMode, StageDef, SCALAR_TOKEN};

fn unsupported(why: impl Into<String>) -> Error {
    Error::Unsupported(format!("auto-distribute: {}", why.into()))
}

/// Decline a shape with a debug trace (gated on `OXIDANT_TPCDS_DEBUG`): strict-mode debugging
/// otherwise sees only the gather fallback's refusal, which masks which check rejected.
macro_rules! decline {
    ($($arg:tt)*) => {{
        if std::env::var("OXIDANT_TPCDS_DEBUG").is_ok() {
            eprintln!(
                "[gather-shapes] decline at {}:{}: {}",
                file!(),
                line!(),
                format!($($arg)*)
            );
        }
        return Ok(None);
    }};
}

/// Strip any `SubqueryAlias` layers off a plan node.
fn strip_aliases(lp: &LogicalPlan) -> &LogicalPlan {
    let mut node = lp;
    while let LogicalPlan::SubqueryAlias(s) = node {
        node = s.input.as_ref();
    }
    node
}

/// Unparse a subplan to sanitized SQL, or decline the shape when the Unparser round-trip fails.
fn plan_sql(lp: &LogicalPlan, what: &str) -> Result<String> {
    let sql = Unparser::default()
        .plan_to_sql(lp)
        .map_err(|e| unsupported(format!("unparse {what}: {e}")))?
        .to_string();
    Ok(sanitize_generated_sql(&sql))
}

/// Distinct sharded base tables scanned by `lp` (sorted).
fn sharded_tables(lp: &LogicalPlan, replicated: &[&str]) -> Vec<String> {
    let mut tables: Vec<String> = base_tables(lp)
        .into_iter()
        .filter(|t| !replicated.contains(&t.as_str()))
        .collect();
    tables.sort();
    tables.dedup();
    tables
}

/// True when the subtree contains an `Aggregate` or `Window` node (per-shard evaluation of
/// either would not be shard-local-safe for the set-op leaf producers).
fn contains_agg_or_window(lp: &LogicalPlan) -> bool {
    if matches!(lp, LogicalPlan::Aggregate(_) | LogicalPlan::Window(_)) {
        return true;
    }
    lp.inputs().iter().any(|i| contains_agg_or_window(i))
}

/// Rewrite every column in `e` through `f`; `f` returns `None` for columns to keep as-is.
/// Pre-validates with `check` so unresolvable columns decline the shape instead of silently
/// producing wrong SQL.
fn remap_columns_with(
    e: &Expr,
    check: &dyn Fn(&Column) -> bool,
    f: &dyn Fn(&Column) -> Column,
) -> Result<Expr> {
    use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
    let mut bad: Option<String> = None;
    let _ = e.apply(|node| {
        if let Expr::Column(c) = node {
            if !check(c) {
                bad = Some(c.flat_name());
                return Ok(TreeNodeRecursion::Stop);
            }
        }
        Ok(TreeNodeRecursion::Continue)
    });
    if let Some(name) = bad {
        return Err(unsupported(format!(
            "column `{name}` does not belong to an expected relation"
        )));
    }
    let mapped = e
        .clone()
        .transform(|node| {
            if let Expr::Column(c) = &node {
                return Ok(Transformed::yes(Expr::Column(f(c))));
            }
            Ok(Transformed::no(node))
        })
        .map(|t| t.data)
        .unwrap_or_else(|_| e.clone());
    Ok(mapped)
}

/// A structural-equality fingerprint for two plan subtrees (DataFusion inlines CTEs, so the
/// "same" derived table appears as independent but identical subtrees). Inner-join regions are
/// canonicalized because two rewrites reshape only the copy in the main plan tree, never the
/// copy inside an expression subquery: `join_order`'s reorder pass may reorder comma-join
/// children, and `connect_comma_join_chain` (KAN-49 Q6) may rewrite a `Filter` over key-less
/// comma joins into a keyed inner-join chain (equijoin conjuncts become ON keys, single-table
/// conjuncts push onto leaf scans, non-equality cross-table conjuncts become residual ON
/// filters). Both forms reduce to the same `JoinSet`: every conjunct (filter conjuncts, ON
/// key pairs, residual ON filters, leaf filters) plus every leaf, each order-insensitive.
fn plan_fingerprint(lp: &LogicalPlan) -> String {
    fn collect_join_region<'a>(
        lp: &'a LogicalPlan,
        leaves: &mut Vec<&'a LogicalPlan>,
        preds: &mut Vec<String>,
    ) {
        match lp {
            LogicalPlan::Filter(f) => {
                let mut conjuncts = Vec::new();
                flatten_conjuncts(&f.predicate, &mut conjuncts);
                for c in conjuncts {
                    preds.push(normalize_conjunct(c));
                }
                collect_join_region(&f.input, leaves, preds);
            }
            LogicalPlan::Join(j) if j.join_type == JoinType::Inner => {
                for (l, r) in &j.on {
                    preds.push(normalize_eq(l, r));
                }
                if let Some(filter) = &j.filter {
                    let mut conjuncts = Vec::new();
                    flatten_conjuncts(filter, &mut conjuncts);
                    for c in conjuncts {
                        preds.push(normalize_conjunct(c));
                    }
                }
                collect_join_region(&j.left, leaves, preds);
                collect_join_region(&j.right, leaves, preds);
            }
            other => leaves.push(other),
        }
    }
    if matches!(lp, LogicalPlan::Filter(_))
        || matches!(lp, LogicalPlan::Join(j) if j.join_type == JoinType::Inner)
    {
        let mut leaves = Vec::new();
        let mut preds = Vec::new();
        collect_join_region(lp, &mut leaves, &mut preds);
        // Only canonicalize a genuine join region; a lone filter over a non-join keeps its
        // structural fingerprint below.
        if leaves.len() >= 2 {
            let mut parts: Vec<String> = leaves.iter().map(|l| plan_fingerprint(l)).collect();
            parts.sort();
            preds.sort();
            return format!("JoinSet[{}][{}]", preds.join(" & "), parts.join(" | "));
        }
    }
    let mut s = lp.display().to_string();
    for i in lp.inputs() {
        s.push_str(&format!("\n  ({})", plan_fingerprint(i)));
    }
    s
}

/// A conjunct for the join-region fingerprint: equality sides are ordered (equality commutes,
/// and the keyed-join rewrite may place a conjunct's sides in either ON position); every other
/// conjunct fingerprints by its display text.
fn normalize_conjunct(e: &Expr) -> String {
    if let Expr::BinaryExpr(b) = e {
        if b.op == Operator::Eq {
            return normalize_eq(&b.left, &b.right);
        }
    }
    e.to_string()
}

fn normalize_eq(l: &Expr, r: &Expr) -> String {
    let (ls, rs) = (l.to_string(), r.to_string());
    if ls <= rs {
        format!("{ls} = {rs}")
    } else {
        format!("{rs} = {ls}")
    }
}

/// Assign fresh stage ids to a sub-DAG starting at `offset`, returning the shifted stages and
/// the id of the sub-DAG's terminal stage.
fn shift_stages(stages: &[StageDef], offset: u32) -> (Vec<StageDef>, u32) {
    let mut out = Vec::with_capacity(stages.len());
    let mut last = offset;
    for s in stages {
        let new_id = s.stage_id + offset;
        out.push(StageDef {
            stage_id: new_id,
            sql: s.sql.clone(),
            upstream_stage_ids: s.upstream_stage_ids.iter().map(|u| u + offset).collect(),
            hash_key_cols: s.hash_key_cols.clone(),
            exchange: s.exchange,
            plan_fragment: s.plan_fragment.clone(),
            lakehouse_snapshot_pins: s.lakehouse_snapshot_pins.clone(),
            replicated_tables: String::new(),
        });
        last = new_id;
    }
    (out, last)
}

// ---------------------------------------------------------------------------
// Q38 / Q87: global count(*) over an INTERSECT / EXCEPT (DISTINCT) chain.
// ---------------------------------------------------------------------------

/// Distribute a global `count(*)` over a chain of `INTERSECT` / `EXCEPT` set operations
/// (TPC-DS Q38's three-channel `INTERSECT`, Q87's `EXCEPT … EXCEPT`):
///
/// ```sql
/// SELECT count(*) FROM (
///   SELECT DISTINCT k… FROM store_sales … INTERSECT
///   SELECT DISTINCT k… FROM catalog_sales … INTERSECT
///   SELECT DISTINCT k… FROM web_sales …) hot_cust
/// ```
///
/// DataFusion lowers each set op to a semi/anti join over `Distinct` branch inputs with
/// full-row equality keys. Instead of gathering one channel's whole fact, every branch
/// exports its (already branch-local) rows as a leaf stage hash-shuffled **on the full row**,
/// so equal rows from every branch co-locate on one partition. A branch reading only
/// replicated tables emits its full row set on every worker — hash-routed duplicates that the
/// set op's own DISTINCT semantics absorb. The per-partition `INTERSECT` / `EXCEPT` (rebuilt
/// as SQL with each branch replaced by its shuffle input) is then exact, and the global
/// `count(*)` recombines per-partition counts.
///
/// Restricted to: a single non-DISTINCT `count` aggregate, no HAVING, left-deep
/// LeftSemi/LeftAnti chains whose join keys cover the full branch row positionally, and
/// `Distinct` branches that are per-worker computable (no aggregate/window, at most one
/// sharded table scanned once, subquery tables replicated).
pub(crate) fn try_global_count_over_set_op(
    lp: &LogicalPlan,
    replicated: &[&str],
) -> Result<Option<DistributedQuery>> {
    let Ok(p) = peel(lp) else {
        decline!("q38/q87");
    };
    if !p.agg.group_expr.is_empty() || !p.having.is_empty() {
        decline!("q38/q87");
    }
    let [agg_expr] = p.agg.aggr_expr.as_slice() else {
        decline!("q38/q87");
    };
    let spec = match AggSpec::classify(agg_expr) {
        Ok(s) => s,
        Err(_) => return Ok(None),
    };
    if spec.distinct || spec.func != "count" {
        decline!("q38/q87");
    }

    let mut leaves: Vec<&LogicalPlan> = Vec::new();
    let Some(chain_sql) = walk_set_op_chain(strip_aliases(p.agg.input.as_ref()), &mut leaves)?
    else {
        decline!("q38/q87");
    };
    if leaves.len() < 2 {
        decline!("q38/q87");
    }
    let n_cols = leaves[0].schema().fields().len();
    if n_cols == 0 || leaves.iter().any(|l| l.schema().fields().len() != n_cols) {
        decline!("q38/q87");
    }
    for leaf in &leaves {
        if sharded_tables(leaf, replicated).len() > 1 {
            decline!("q38/q87");
        }
        let mut subq_tables = Vec::new();
        collect_subquery_tables(leaf, &mut subq_tables);
        if subq_tables
            .iter()
            .any(|t| !replicated.contains(&t.as_str()))
        {
            decline!("q38/q87");
        }
        if contains_agg_or_window(leaf) {
            decline!("q38/q87");
        }
        for t in sharded_tables(leaf, replicated) {
            if count_table_scans(leaf, &t) != 1 {
                decline!("q38/q87");
            }
        }
    }

    let mut stages = Vec::with_capacity(leaves.len() + 2);
    for (i, leaf) in leaves.iter().enumerate() {
        let sql = plan_sql(leaf, "set-op branch")?;
        stages.push(StageDef::new(
            i as u32,
            sql,
            vec![],
            (0..n_cols as u32).collect(),
        ));
    }
    let chain_id = leaves.len() as u32;
    let partial_sql = sanitize_generated_sql(&format!(
        "SELECT count(*) AS a0 FROM ({chain_sql}) AS set_chain"
    ));
    stages.push(StageDef::new(
        chain_id,
        partial_sql,
        (0..chain_id).collect(),
        vec![],
    ));
    let remap = build_remap(&p);
    let inner = "SELECT sum(a0) AS r0 FROM shuffle_input HAVING COUNT(*) > 0".to_string();
    let final_sql = sanitize_generated_sql(&wrap_output(&p, &inner, &remap)?);
    stages.push(StageDef::new(
        chain_id + 1,
        final_sql,
        vec![chain_id],
        vec![],
    ));
    Ok(Some(DistributedQuery {
        stages,
        finalize_sql: build_finalize(&p)?,
    }))
}

/// Rebuild the per-partition set-op SQL for a left-deep LeftSemi/LeftAnti chain, pushing each
/// `Distinct` branch onto `leaves` in order. Returns `Ok(None)` when the tree is any other
/// shape (which keeps the query on the existing rejection / gather paths).
fn walk_set_op_chain<'a>(
    node: &'a LogicalPlan,
    leaves: &mut Vec<&'a LogicalPlan>,
) -> Result<Option<String>> {
    match strip_aliases(node) {
        LogicalPlan::Join(j) => {
            let op_sql = match j.join_type {
                JoinType::LeftSemi => "INTERSECT",
                JoinType::LeftAnti => "EXCEPT",
                _ => return Ok(None),
            };
            if j.filter.is_some() {
                decline!("q38/q87");
            }
            let Some(left_sql) = walk_set_op_chain(&j.left, leaves)? else {
                decline!("q38/q87");
            };
            let right_node = strip_aliases(&j.right);
            let LogicalPlan::Distinct(_) = right_node else {
                decline!("q38/q87");
            };
            // The semi/anti join must equate the full branch row, positionally — that is what
            // makes `left OP right` with positional set semantics equal to this join.
            let n = right_node.schema().fields().len();
            if !positional_full_row_keys(j, n) {
                decline!("q38/q87");
            }
            leaves.push(right_node);
            let idx = leaves.len() - 1;
            Ok(Some(format!(
                "({left_sql}) {op_sql} (SELECT * FROM shuffle_input_{idx})"
            )))
        }
        LogicalPlan::Distinct(d) => {
            // An internal `Distinct` above the nested chain is redundant (the left input of an
            // INTERSECT/EXCEPT DISTINCT is already distinct); skip it. A `Distinct` whose input
            // is not a set-op join is a branch leaf.
            match strip_aliases(d.input()) {
                LogicalPlan::Join(_) => walk_set_op_chain(d.input(), leaves),
                _ => {
                    leaves.push(strip_aliases(node));
                    let idx = leaves.len() - 1;
                    Ok(Some(format!("(SELECT * FROM shuffle_input_{idx})")))
                }
            }
        }
        _ => Ok(None),
    }
}

/// The semi/anti join's `on` pairs must be exactly the positional identity over both inputs'
/// `n` output columns (DataFusion's lowering of `INTERSECT` / `EXCEPT` DISTINCT).
fn positional_full_row_keys(j: &datafusion::logical_expr::Join, n: usize) -> bool {
    if j.on.len() != n {
        return false;
    }
    let mut pairs = Vec::with_capacity(n);
    for (l, r) in &j.on {
        let (Expr::Column(lc), Expr::Column(rc)) = (l, r) else {
            return false;
        };
        let (Ok(li), Ok(ri)) = (
            j.left.schema().index_of_column(lc),
            j.right.schema().index_of_column(rc),
        ) else {
            return false;
        };
        pairs.push((li, ri));
    }
    pairs.sort_unstable();
    pairs == (0..n).map(|i| (i, i)).collect::<Vec<_>>()
}

// ---------------------------------------------------------------------------
// Q97: global aggregates over a FULL OUTER JOIN of two distinct-key aggregates.
// ---------------------------------------------------------------------------

/// Distribute a global aggregate over a `FULL OUTER JOIN` of two per-channel distinct-key
/// derived tables (TPC-DS Q97's `ssci`/`csci` store-vs-catalog bucket counts):
///
/// ```sql
/// SELECT sum(CASE WHEN ssci.k IS NOT NULL AND csci.k IS NULL THEN 1 ELSE 0 END), …
/// FROM (SELECT ss_customer_sk customer_sk, ss_item_sk item_sk FROM store_sales … GROUP BY …) ssci
/// FULL OUTER JOIN (SELECT … FROM catalog_sales … GROUP BY …) csci
///   ON ssci.customer_sk = csci.customer_sk AND ssci.item_sk = csci.item_sk
/// ```
///
/// Each side is distributed as its own partial/combine pair (per-worker `GROUP BY`, hash
/// shuffle by the group key, recombine-dedup), so each side's distinct keys land wholly on one
/// partition. A side scanning only replicated tables emits the full key set per worker; the
/// recombine `GROUP BY` absorbs those duplicates. The per-partition `FULL OUTER JOIN` is then
/// exact — every row with key K from either side sits on partition h(K), and NULL keys never
/// match in either plan — and the global aggregates on top recombine per-partition partials.
///
/// Restricted to: a global (ungrouped) aggregate, non-DISTINCT aggregates whose arguments read
/// only the two sides' output columns, sides that are plain distinct-key grouped aggregates
/// (empty aggregate list), and full-width equijoin keys with no residual predicate.
pub(crate) fn try_full_outer_join_global_agg(
    lp: &LogicalPlan,
    replicated: &[&str],
) -> Result<Option<DistributedQuery>> {
    let Ok(p) = peel(lp) else {
        decline!("q97");
    };
    if !p.agg.group_expr.is_empty() || !p.having.is_empty() {
        decline!("q97");
    }
    let LogicalPlan::Join(j) = strip_aliases(p.agg.input.as_ref()) else {
        decline!("q97");
    };
    if j.join_type != JoinType::Full {
        decline!("q97");
    }
    let Some(left) = distinct_key_side(&j.left, replicated)? else {
        decline!("q97");
    };
    let Some(right) = distinct_key_side(&j.right, replicated)? else {
        decline!("q97");
    };

    // Join keys: a full-width bijection between the two sides' outputs, no residual predicate.
    let Ok((keys, residual)) = collect_equijoin_keys(&j.on, j.filter.as_ref()) else {
        decline!("q97");
    };
    if residual.is_some() || keys.len() != left.out_names.len() || keys.is_empty() {
        decline!("q97");
    }
    let mut on_parts = Vec::with_capacity(keys.len());
    let mut l_positions = Vec::with_capacity(keys.len());
    let mut r_positions = Vec::with_capacity(keys.len());
    for (le, re) in &keys {
        let (Expr::Column(lc), Expr::Column(rc)) = (le, re) else {
            decline!("q97");
        };
        let Some(li) = side_column_position(&left, lc) else {
            decline!("q97");
        };
        let Some(ri) = side_column_position(&right, rc) else {
            decline!("q97");
        };
        l_positions.push(li);
        r_positions.push(ri);
        on_parts.push(format!(
            "l.{} = r.{}",
            left.out_names[li], right.out_names[ri]
        ));
    }
    l_positions.sort_unstable();
    r_positions.sort_unstable();
    if l_positions != (0..keys.len()).collect::<Vec<_>>()
        || r_positions != (0..keys.len()).collect::<Vec<_>>()
    {
        decline!("q97");
    }

    // Aggregates: non-DISTINCT, arguments read only the two sides' columns (remapped to the
    // join stage's `l` / `r` aliases).
    let up = Unparser::default();
    let mut partials = Vec::with_capacity(p.agg.aggr_expr.len());
    let mut combines = Vec::with_capacity(p.agg.aggr_expr.len());
    for (i, e) in p.agg.aggr_expr.iter().enumerate() {
        let spec = match AggSpec::classify(e) {
            Ok(s) => s,
            Err(_) => return Ok(None),
        };
        if spec.distinct {
            decline!("q97");
        }
        let arg = match strip_alias(e) {
            Expr::AggregateFunction(af) if af.params.args.len() == 1 => &af.params.args[0],
            _ => return Ok(None),
        };
        let remapped = remap_columns_with(
            arg,
            &|c| {
                c.relation
                    .as_ref()
                    .is_some_and(|r| r.table() == left.alias || r.table() == right.alias)
            },
            &|c| {
                let side = if c.relation.as_ref().is_some_and(|r| r.table() == left.alias) {
                    "l"
                } else {
                    "r"
                };
                Column::new(Some(side), c.name.clone())
            },
        )?;
        let arg_sql = expr_sql(&up, &remapped)?;
        let (sel, comb) = partial_combine_sql(&spec.func, i, &arg_sql)?;
        partials.extend(sel);
        combines.push(comb);
    }

    let (left_stages, left_out) = shift_stages(&left.dq.stages, 0);
    let (right_stages, right_out) = shift_stages(&right.dq.stages, left_out + 1);
    let join_id = right_out + 1;
    let join_sql = sanitize_generated_sql(&format!(
        "SELECT {} FROM shuffle_input_0 AS l FULL OUTER JOIN shuffle_input_1 AS r ON {}",
        partials.join(", "),
        on_parts.join(" AND ")
    ));
    let remap = build_remap(&p);
    let inner = format!(
        "SELECT {} FROM shuffle_input HAVING COUNT(*) > 0",
        combines.join(", ")
    );
    let final_sql = sanitize_generated_sql(&wrap_output(&p, &inner, &remap)?);
    let mut stages = left_stages;
    stages.extend(right_stages);
    stages.push(StageDef::new(
        join_id,
        join_sql,
        vec![left_out, right_out],
        vec![],
    ));
    stages.push(StageDef::new(join_id + 1, final_sql, vec![join_id], vec![]));
    Ok(Some(DistributedQuery {
        stages,
        finalize_sql: build_finalize(&p)?,
    }))
}

/// One side of the Q97 full outer join: a `SubqueryAlias` over a projection over a grouped
/// aggregate with an empty aggregate list (a distinct-key producer), distributed.
struct DistinctKeySide {
    alias: String,
    out_names: Vec<String>,
    dq: DistributedQuery,
}

fn distinct_key_side(lp: &LogicalPlan, replicated: &[&str]) -> Result<Option<DistinctKeySide>> {
    let LogicalPlan::SubqueryAlias(a) = lp else {
        decline!("q97");
    };
    let alias = a.alias.table().to_string();
    let Ok(side_p) = peel(a.input.as_ref()) else {
        decline!("q97");
    };
    if side_p.sort.is_some() || side_p.limit.is_some() || !side_p.having.is_empty() {
        decline!("q97");
    }
    if side_p.agg.group_expr.is_empty() || !side_p.agg.aggr_expr.is_empty() {
        decline!("q97");
    }
    let n_group = side_p.agg.group_expr.len() as u32;
    let out_names: Vec<String> = match side_p.projection {
        Some(exprs) => exprs.iter().map(output_name).collect(),
        None => (0..n_group).map(|j| format!("g{j}")).collect(),
    };
    if sharded_tables(&side_p.agg.input, replicated).is_empty() {
        // A fully replicated side is computed once on a single worker (Forward — per-worker
        // evaluation would multiply the key set by the worker count) and hash-shuffled by the
        // join key, so it co-locates with the genuinely sharded side's recombine output.
        let sql = plan_sql(a.input.as_ref(), "replicated distinct-key side")?;
        let mut stage = StageDef::new(0, sql, vec![], (0..n_group).collect());
        stage.exchange = crate::driver::ExchangeMode::Forward;
        return Ok(Some(DistinctKeySide {
            alias,
            out_names,
            dq: DistributedQuery {
                stages: vec![stage],
                finalize_sql: None,
            },
        }));
    }
    let mut dq = match aggregation_stages_for(&side_p, replicated) {
        Ok(dq) => dq,
        Err(_) => return Ok(None),
    };
    if dq.finalize_sql.is_some() {
        decline!("q97");
    }
    // Keep the combine hash-partitioned by the group (= join) key so both sides co-locate;
    // `aggregation_stages_for` leaves its terminal stage gathered for output duty.
    let Some(combine) = dq.stages.last_mut() else {
        decline!("q97");
    };
    combine.hash_key_cols = (0..n_group).collect();
    Ok(Some(DistinctKeySide {
        alias,
        out_names,
        dq,
    }))
}

/// Position of `c` in the side's output columns (matched by alias relation and column name).
fn side_column_position(side: &DistinctKeySide, c: &Column) -> Option<usize> {
    if c.relation.as_ref().is_some_and(|r| r.table() != side.alias) {
        return None;
    }
    side.out_names.iter().position(|n| n == &c.name)
}

// ---------------------------------------------------------------------------
// Q24: HAVING scalar threshold over a shared derived per-key aggregate.
// ---------------------------------------------------------------------------

/// Distribute a grouped aggregate over a derived per-key aggregate whose HAVING carries an
/// **uncorrelated** scalar-aggregate threshold over the *same* derived table (TPC-DS Q24):
///
/// ```sql
/// WITH ssales AS (SELECT …, sum(ss_net_paid) netpaid FROM store_sales, … GROUP BY …)
/// SELECT c_last_name, c_first_name, s_store_name, sum(netpaid) paid
/// FROM ssales WHERE i_color = 'peach'
/// GROUP BY c_last_name, c_first_name, s_store_name
/// HAVING sum(netpaid) > (SELECT 0.05*avg(netpaid) FROM ssales)
/// ```
///
/// The derived table is distributed **once** (partial per worker, hash shuffle by the derived
/// group key, recombine — the ordinary machinery via [`aggregation_stages_for`]). Off that
/// combine output:
///
/// - the scalar decomposes into a per-partition partial + one-row combine (avg splits into
///   sum/count partials), gathered to partition 0; the driver inlines the single value into
///   the outer combine's HAVING as a literal before dispatch (the KAN-27 one-row broadcast —
///   [`SCALAR_TOKEN`]), so the threshold sees the *global* average;
/// - the outer aggregate runs as a second partial/combine pair (partial over the local slice
///   of the derived rows, re-shuffled by the outer group key; the recombine applies the HAVING
///   against the injected literal).
///
/// Exactness: the derived combine emits each derived group exactly once (co-located), so the
/// scalar over it and any recombinable aggregate over it are both exact. Restricted to a
/// single scalar comparison conjunct (other conjuncts subquery-free), a non-DISTINCT
/// min/max/sum/count/avg scalar over the same derived table the outer aggregates, and
/// non-DISTINCT outer aggregates reading derived output columns only.
pub(crate) fn try_derived_having_scalar_threshold(
    lp: &LogicalPlan,
    replicated: &[&str],
) -> Result<Option<DistributedQuery>> {
    let Ok(p) = peel(lp) else {
        decline!("q24");
    };
    if p.agg.group_expr.is_empty() || p.having.is_empty() {
        decline!("q24");
    }
    let mut conjuncts: Vec<&Expr> = Vec::new();
    for h in &p.having {
        flatten_conjuncts(h, &mut conjuncts);
    }
    // Exactly one `<expr> <cmp> <scalar subquery>` conjunct; every other conjunct subquery-free.
    let mut found: Option<(usize, &LogicalPlan)> = None;
    for (i, c) in conjuncts.iter().enumerate() {
        let Expr::BinaryExpr(b) = *c else {
            continue;
        };
        if !matches!(
            b.op,
            Operator::Eq
                | Operator::NotEq
                | Operator::Lt
                | Operator::LtEq
                | Operator::Gt
                | Operator::GtEq
        ) {
            continue;
        }
        let (compare, subquery) = match (b.left.as_ref(), b.right.as_ref()) {
            (Expr::ScalarSubquery(s), other) | (other, Expr::ScalarSubquery(s)) => {
                (other, s.subquery.as_ref())
            }
            _ => continue,
        };
        if found.is_some() || expr_contains_subquery(compare) {
            decline!("q24");
        }
        found = Some((i, subquery));
    }
    let Some((sub_idx, subplan)) = found else {
        decline!("q24");
    };
    if conjuncts
        .iter()
        .enumerate()
        .any(|(i, c)| i != sub_idx && expr_contains_subquery(c))
    {
        decline!("q24");
    }
    if plan_contains_outer_reference(subplan) {
        decline!("q24");
    }
    let fields = subplan.schema().fields();
    if fields.len() != 1 || !scalar_literal_supported(fields[0].data_type()) {
        decline!("q24");
    }

    // The scalar: at most one single-expression projection layer over a bare global aggregate
    // with one non-DISTINCT min/max/sum/count/avg (Q24's `0.05 * avg(netpaid)`).
    let mut projection: Option<&[Expr]> = None;
    let mut sp = subplan;
    while let LogicalPlan::Projection(pr) = sp {
        if projection.is_some() || pr.expr.len() != 1 {
            decline!("q24");
        }
        projection = Some(pr.expr.as_slice());
        sp = pr.input.as_ref();
    }
    let LogicalPlan::Aggregate(sub_agg) = sp else {
        decline!("q24");
    };
    if !sub_agg.group_expr.is_empty() || sub_agg.aggr_expr.len() != 1 {
        decline!("q24");
    }
    let scalar_spec = match AggSpec::classify(&sub_agg.aggr_expr[0]) {
        Ok(s) => s,
        Err(_) => return Ok(None),
    };
    if scalar_spec.distinct
        || !matches!(
            scalar_spec.func.as_str(),
            "min" | "max" | "sum" | "count" | "avg"
        )
    {
        decline!("q24");
    }

    // The scalar's input and the outer aggregate's input (minus the outer filters) must be the
    // same derived grouped aggregate — Q24's shared `ssales` CTE, inlined twice by DataFusion.
    let mut filter_preds: Vec<&Expr> = Vec::new();
    let mut outer_body = p.agg.input.as_ref();
    while let LogicalPlan::Filter(f) = outer_body {
        flatten_conjuncts(&f.predicate, &mut filter_preds);
        outer_body = f.input.as_ref();
    }
    let derived = strip_aliases(outer_body);
    if plan_fingerprint(strip_aliases(sub_agg.input.as_ref())) != plan_fingerprint(derived) {
        if std::env::var("OXIDANT_TPCDS_DEBUG").is_ok() {
            eprintln!(
                "[gather-shapes] q24 scalar derived:\n{}",
                plan_fingerprint(strip_aliases(sub_agg.input.as_ref()))
            );
            eprintln!(
                "[gather-shapes] q24 outer derived:\n{}",
                plan_fingerprint(derived)
            );
        }
        decline!("q24");
    }
    let Ok(inner_p) = peel(derived) else {
        decline!("q24");
    };
    if inner_p.sort.is_some()
        || inner_p.limit.is_some()
        || !inner_p.having.is_empty()
        || inner_p.agg.group_expr.is_empty()
    {
        decline!("q24");
    }
    let inner_dq = match aggregation_stages_for(&inner_p, replicated) {
        Ok(dq) => dq,
        Err(_) => return Ok(None),
    };
    if inner_dq.finalize_sql.is_some() {
        decline!("q24");
    }
    let inner_out: Vec<String> = match inner_p.projection {
        Some(exprs) => exprs.iter().map(output_name).collect(),
        None => (0..inner_p.agg.group_expr.len())
            .map(|j| format!("g{j}"))
            .chain((0..inner_p.agg.aggr_expr.len()).map(|i| format!("r{i}")))
            .collect(),
    };

    let up = Unparser::default();
    // Outer group keys and filters must read derived output columns only.
    let mut group_names = Vec::with_capacity(p.agg.group_expr.len());
    for g in &p.agg.group_expr {
        let Expr::Column(c) = g else {
            decline!("q24");
        };
        if !inner_out.contains(&c.name) {
            decline!("q24");
        }
        group_names.push(c.name.clone());
    }
    for pred in &filter_preds {
        let mut cols = Vec::new();
        expr_columns(pred, &mut cols);
        if !cols.iter().all(|c| inner_out.contains(&c.name)) {
            decline!("q24");
        }
    }
    let mut outer_partials = Vec::with_capacity(p.agg.aggr_expr.len());
    let mut outer_combines = Vec::with_capacity(p.agg.aggr_expr.len());
    for (i, e) in p.agg.aggr_expr.iter().enumerate() {
        let spec = match AggSpec::classify(e) {
            Ok(s) => s,
            Err(_) => return Ok(None),
        };
        if spec.distinct {
            decline!("q24");
        }
        let arg = match strip_alias(e) {
            Expr::AggregateFunction(af) if af.params.args.len() == 1 => &af.params.args[0],
            _ => return Ok(None),
        };
        let arg = unqualify(arg);
        let mut cols = Vec::new();
        expr_columns(&arg, &mut cols);
        if !cols.iter().all(|c| inner_out.contains(&c.name)) {
            decline!("q24");
        }
        let arg_sql = expr_sql(&up, &arg)?;
        let (sel, comb) = partial_combine_sql(&spec.func, i, &arg_sql)?;
        outer_partials.extend(sel);
        outer_combines.push(comb);
    }
    // The scalar's argument must be a plain derived output column.
    let scalar_arg = match strip_alias(&sub_agg.aggr_expr[0]) {
        Expr::AggregateFunction(af) if af.params.args.len() == 1 => unqualify(&af.params.args[0]),
        _ => return Ok(None),
    };
    let Expr::Column(sc) = &scalar_arg else {
        decline!("q24");
    };
    if !inner_out.contains(&sc.name) {
        decline!("q24");
    }

    // Stages 0..=inner_last: the derived table, distributed.
    let mut stages = inner_dq.stages;
    let inner_last = stages.last().map(|s| s.stage_id).unwrap_or(0);

    // Scalar partial / one-row combine (KAN-27 one-row broadcast).
    let (scalar_sels, scalar_comb) = per_key_agg_parts(&scalar_spec.func, &sc.name, 0)?;
    let guard_col = if scalar_spec.func == "avg" {
        "a0c"
    } else {
        "a0"
    };
    let scalar_partial_id = inner_last + 1;
    stages.push(StageDef::new(
        scalar_partial_id,
        sanitize_generated_sql(&format!(
            "SELECT {} FROM shuffle_input",
            scalar_sels.join(", ")
        )),
        vec![inner_last],
        vec![],
    ));
    let mut s0v_remap = HashMap::new();
    s0v_remap.insert(
        sub_agg.aggr_expr[0].schema_name().to_string(),
        "s0v".to_string(),
    );
    if let Some(f) = sub_agg.schema.fields().first() {
        s0v_remap.insert(f.name().clone(), "s0v".to_string());
    }
    let proj_sql = match projection {
        Some(exprs) => {
            if expr_contains_subquery(&exprs[0]) {
                decline!("q24");
            }
            let mapped = remap_expr_columns(strip_alias(&exprs[0]), &s0v_remap);
            let mut cols = Vec::new();
            expr_columns(&mapped, &mut cols);
            if !cols.iter().all(|c| c.relation.is_none() && c.name == "s0v") {
                decline!("q24");
            }
            expr_sql(&up, &mapped)?
        }
        None => "s0v".to_string(),
    };
    let scalar_combine_id = scalar_partial_id + 1;
    stages.push(StageDef::new(
        scalar_combine_id,
        sanitize_generated_sql(&format!(
            "SELECT {proj_sql} AS m0 FROM \
             (SELECT {scalar_comb} AS s0v FROM shuffle_input HAVING COUNT({guard_col}) > 0) AS cs"
        )),
        vec![scalar_partial_id],
        vec![],
    ));

    // Outer partial over the derived combine output, re-shuffled by the outer group key.
    let mut psel: Vec<String> = group_names
        .iter()
        .enumerate()
        .map(|(j, n)| format!("{n} AS g{j}"))
        .collect();
    psel.extend(outer_partials);
    let where_sql = if filter_preds.is_empty() {
        String::new()
    } else {
        let parts = filter_preds
            .iter()
            .map(|pr| expr_sql(&up, &unqualify(pr)))
            .collect::<Result<Vec<_>>>()?;
        format!(" WHERE {}", parts.join(" AND "))
    };
    let outer_partial_id = scalar_combine_id + 1;
    stages.push(StageDef::new(
        outer_partial_id,
        sanitize_generated_sql(&format!(
            "SELECT {} FROM shuffle_input{where_sql} GROUP BY {}",
            psel.join(", "),
            group_names.join(", ")
        )),
        vec![inner_last],
        (0..group_names.len() as u32).collect(),
    ));

    // Outer combine: the recombine applies the HAVING with the scalar swapped for the
    // placeholder literal the driver substitutes before dispatch.
    let token = SCALAR_TOKEN.to_string();
    let placeholder = Expr::Literal(ScalarValue::Utf8(Some(token.clone())), None);
    let mut having_exprs: Vec<Expr> = Vec::with_capacity(conjuncts.len());
    for (i, c) in conjuncts.iter().enumerate() {
        if i == sub_idx {
            let Expr::BinaryExpr(b) = *c else {
                decline!("q24");
            };
            let (left, right) = if matches!(b.left.as_ref(), Expr::ScalarSubquery(_)) {
                (Box::new(placeholder.clone()), b.right.clone())
            } else {
                (b.left.clone(), Box::new(placeholder.clone()))
            };
            having_exprs.push(Expr::BinaryExpr(BinaryExpr {
                left,
                op: b.op,
                right,
            }));
        } else {
            having_exprs.push((*c).clone());
        }
    }
    let modified = Peeled {
        projection: p.projection,
        sort: p.sort,
        limit: p.limit,
        having: having_exprs.iter().collect(),
        alias_projections: p.alias_projections.clone(),
        agg: p.agg,
    };
    let group_by = (0..group_names.len())
        .map(|j| format!("g{j}"))
        .collect::<Vec<_>>()
        .join(", ");
    let inner_combine = format!(
        "SELECT {group_by}, {} FROM shuffle_input GROUP BY {group_by}",
        outer_combines.join(", ")
    );
    let remap = build_remap(&modified);
    let final_sql = sanitize_generated_sql(&wrap_output(&modified, &inner_combine, &remap)?);
    let outer_combine_id = outer_partial_id + 1;
    stages.push(StageDef::new(
        outer_combine_id,
        final_sql,
        vec![outer_partial_id],
        vec![],
    ));
    let dq = DistributedQuery {
        stages,
        finalize_sql: build_finalize(&p)?,
    };
    // Self-check (same as KAN-27): the placeholder must survive as a quoted literal in exactly
    // one stage's SQL and nowhere in the finalize, or the driver could not substitute it.
    let quoted = format!("'{token}'");
    if dq.stages.iter().filter(|s| s.sql.contains(&quoted)).count() != 1
        || dq.finalize_sql.as_ref().is_some_and(|f| f.contains(&token))
    {
        decline!("q24");
    }
    Ok(Some(dq))
}

// ---------------------------------------------------------------------------
// Q23: UNION ALL of per-channel arms over shared sharded derived CTEs.
// ---------------------------------------------------------------------------

/// Distribute a `UNION ALL` of per-channel aggregate arms whose only sharded inputs are
/// **shared derived CTEs** (TPC-DS Q23 at the SF10 classification, where `store_sales` is the
/// sharded table and the channel facts replicate):
///
/// ```sql
/// WITH frequent_ss_items AS (SELECT …, count(*) cnt FROM store_sales, … GROUP BY … HAVING count(*) > 4),
///      max_store_sales  AS (SELECT max(csales) tpcds_cmax FROM (per-customer aggregate) sq2),
///      best_ss_customer AS (SELECT …, sum(…) ssales FROM store_sales, customer, max_store_sales
///                           GROUP BY … HAVING sum(…) > 0.5 * max(tpcds_cmax))
/// SELECT … FROM (catalog arm joining the CTEs UNION ALL web arm joining the CTEs) sq3
/// ```
///
/// At this classification neither arm scans a sharded base table directly — the sharded fact
/// is read only inside the CTEs, which both arms share (DataFusion inlines each CTE per arm).
/// The composition:
///
/// 1. **Each distinct derived CTE is planned once** (fingerprint dedup, the dag_splitter
///    pattern): `frequent_ss_items` through the ordinary recursive machinery;
///    `best_ss_customer` through [`try_cross_scalar_threshold`], which splits its single-row
///    `max_store_sales` cross-join leaf into a distributed per-customer aggregate plus a KAN-27
///    one-row scalar broadcast ([`SCALAR_TOKEN`] literal injection into the combine's HAVING).
///    Every CTE's terminal stage gathers (empty hash key), so its whole output sits in
///    bucket 0 of every endpoint.
/// 2. **Each arm becomes one `ExchangeMode::Forward` stage**: the arm plan with its CTE
///    references replaced by `shuffle_input` placeholders (dag_splitter's
///    `replace_branches`). The driver runs a Forward stage exactly once (on worker 0),
///    where the full replicated channel/dim tables and a bucket-0 pull of every CTE make
///    the arm's grouped aggregate exact — a Hash exchange would instead re-run the whole
///    arm once per shuffle partition, with partitions 1..n-1 guaranteed empty (correct
///    but ~partitions× the work; see the note at the emission site).
///
///    KAN-156: on a multi-worker cluster, [`try_q23_fanned_arms`] first attempts to replace
///    these single-task arms with fanned-out pipelines (sliced scan export → hash-co-located
///    semi per CTE → partial aggregate → per-arm recombine), re-keying the CTE terminal
///    stages from the gather to the arms' join columns. It is all-or-nothing and strictly
///    admitted; any decline keeps the Forward arms here byte-identical.
/// 3. The arms concatenate in one final `UNION ALL` stage; the query's `ORDER BY`/`LIMIT`
///    rides the usual driver-side finalize.
///
/// Restricted to: a top `UNION ALL` (distinct unions decline); arms whose sharded inputs are
/// all peel-able grouped derived aggregates (no direct sharded base scans); skeletons of
/// projections / filters / inner joins / grouped aggregates, where every skeleton aggregate
/// sits above at least one CTE (conservative: a Forward arm would compute a replicated-only
/// aggregate exactly once, but an arm with no gathered CTE has no business in this
/// composition); and CTEs whose distributed plans gather their output. At most one CTE may
/// use the scalar-token machinery (the driver supports a single one-row broadcast per plan).
pub(crate) fn try_union_over_derived_ctes(
    lp: &LogicalPlan,
    replicated: &[&str],
) -> Result<Option<DistributedQuery>> {
    let (inner, sort, limit) = peek_sort_limit(lp);
    let mut top = inner;
    loop {
        match top {
            LogicalPlan::Projection(p) => top = p.input.as_ref(),
            LogicalPlan::SubqueryAlias(s) => top = s.input.as_ref(),
            _ => break,
        }
    }
    let LogicalPlan::Union(u) = top else {
        decline!("q23");
    };
    if u.inputs.len() < 2 {
        decline!("q23");
    }

    // Per arm: collect the derived sharded CTE leaves (occurrence level — each arm inlines its
    // own copy of every CTE). An arm that is *itself* one aggregate over direct sharded scans
    // (the per-channel fact-union families — TPC-DS Q33/Q54/Q75 & friends) has no skeleton for
    // this composition and stays with `try_union_all`.
    let mut arm_leaves: Vec<Vec<&LogicalPlan>> = Vec::with_capacity(u.inputs.len());
    for arm in &u.inputs {
        let mut leaves = Vec::new();
        let Some(()) = collect_arm_leaves(arm, replicated, &mut leaves, true, None) else {
            decline!("q23");
        };
        if leaves.is_empty() || leaves.iter().any(|l| std::ptr::eq(*l, arm.as_ref())) {
            decline!("q23");
        }
        arm_leaves.push(leaves);
    }

    // Fingerprint-dedup the leaves across arms (the dag_splitter CTE-reuse rule): identical
    // CTE copies plan once and every occurrence reads the same shuffle output. Volatile leaves
    // never share an evaluation — each occurrence plans separately, as single-node would.
    let mut rep_of: Vec<usize> = (0..arm_leaves.iter().map(Vec::len).sum::<usize>()).collect();
    let mut fp_to_rep: HashMap<String, usize> = HashMap::new();
    {
        let mut i = 0usize;
        for leaves in &arm_leaves {
            for leaf in leaves {
                if !super::dag_splitter::plan_contains_volatile(leaf) {
                    let fp = plan_fingerprint(leaf);
                    if let Some(&r) = fp_to_rep.get(&fp) {
                        rep_of[i] = r;
                    } else {
                        fp_to_rep.insert(fp, i);
                    }
                }
                i += 1;
            }
        }
    }
    let mut reps: Vec<usize> = Vec::new();
    for &r in &rep_of {
        if !reps.contains(&r) {
            reps.push(r);
        }
    }
    let leaf_at = |occ: usize| -> &LogicalPlan {
        let mut base = 0usize;
        for leaves in &arm_leaves {
            if occ < base + leaves.len() {
                return leaves[occ - base];
            }
            base += leaves.len();
        }
        unreachable!("occurrence index within total leaf count")
    };

    // Plan each distinct CTE once; its terminal stage must gather (the arm stages rely on the
    // empty-other-partitions invariant).
    let mut stages: Vec<StageDef> = Vec::new();
    let mut rep_output: HashMap<usize, u32> = HashMap::with_capacity(reps.len());
    let mut next_id = 0u32;
    for &r in &reps {
        let Some(dq) = plan_derived_cte(leaf_at(r), replicated)? else {
            decline!("q23");
        };
        if dq.finalize_sql.is_some()
            || dq
                .stages
                .last()
                .map_or(true, |s| !s.hash_key_cols.is_empty())
        {
            decline!("q23");
        }
        let (shifted, last) = shift_stages(&dq.stages, next_id);
        stages.extend(shifted);
        rep_output.insert(r, last);
        next_id = last + 1;
    }

    // Each arm: replace its CTE references with `shuffle_input` placeholders (numbered locally
    // per arm, matching that stage's upstream list) and emit the arm as one gathered stage.
    //
    // KAN-156: first try to fan the replicated channel arms out across workers (sliced scan
    // exports + hash-co-located CTE joins + partial/recombine) — all-or-nothing, since the CTE
    // terminal stages are re-keyed from the partition-0 gather to the arms' join keys. `None`
    // keeps the original per-arm `Forward` stages below.
    let arm_stage_ids = match try_q23_fanned_arms(
        &u.inputs,
        &arm_leaves,
        &rep_of,
        &rep_output,
        replicated,
        &mut next_id,
        &mut stages,
    ) {
        Some(ids) => ids,
        None => {
            let mut arm_stage_ids = Vec::with_capacity(arm_leaves.len());
            let mut base = 0usize;
            for (arm, leaves) in u.inputs.iter().zip(&arm_leaves) {
                let branch_by_node: HashMap<usize, usize> = leaves
                    .iter()
                    .enumerate()
                    .map(|(j, l)| (super::dag_splitter::node_id(l), j))
                    .collect();
                let (rewritten, changed) =
                    super::dag_splitter::replace_branches(arm, &branch_by_node, leaves.len())?;
                if !changed {
                    decline!("q23");
                }
                // No sharded scan may remain in the arm skeleton (a scan left behind would read only
                // partition 0's local shard of the fact).
                let mut remaining = base_tables(&rewritten);
                collect_subquery_tables(&rewritten, &mut remaining);
                if remaining.iter().any(|t| {
                    t != "shuffle_input"
                        && !t.starts_with("shuffle_input_")
                        && !replicated.contains(&t.as_str())
                }) {
                    decline!("q23");
                }
                let sql = plan_sql(&rewritten, "union arm")?;
                let upstreams: Vec<u32> = (0..leaves.len())
                    .map(|j| rep_output[&rep_of[base + j]])
                    .collect();
                // Run the arm exactly once (`ExchangeMode::Forward` → one task on worker 0), not
                // once per shuffle partition: every worker holds the full replicated channel/dim
                // tables, and each CTE gather leaves its whole output in bucket 0 of every
                // endpoint, so a single task pulling bucket 0 from every endpoint computes the
                // exact arm. A Hash exchange here is correct-but-slow — the empty hash key makes
                // partitions 1..n-1 emit zero rows while every partition re-runs the full
                // replicated scan + join (Q23 at SF10: 169s vs Spark's 5.8s, 71.6 GB of spill
                // from the duplicated work). Per-worker partials over the replicated fact would
                // instead multiply the arm's rows by the worker count (KAN-54), so Forward
                // (compute once) is the only correct fast placement.
                let mut arm = StageDef::new(next_id, sql, upstreams, vec![]);
                arm.exchange = ExchangeMode::Forward;
                stages.push(arm);
                arm_stage_ids.push(next_id);
                next_id += 1;
                base += leaves.len();
            }
            arm_stage_ids
        }
    };

    // Final stage: the UNION ALL of the arms under the original output projection/alias.
    let arm_map: HashMap<usize, usize> = u
        .inputs
        .iter()
        .enumerate()
        .map(|(i, a)| (super::dag_splitter::node_id(a), i))
        .collect();
    let (rewritten_top, _) =
        super::dag_splitter::replace_branches(inner, &arm_map, u.inputs.len())?;
    let union_sql = plan_sql(&rewritten_top, "union over arms")?;
    stages.push(StageDef::new(next_id, union_sql, arm_stage_ids, vec![]));

    // The driver substitutes at most one scalar literal per plan (Q24 self-check rule).
    let quoted = format!("'{SCALAR_TOKEN}'");
    if stages.iter().filter(|s| s.sql.contains(&quoted)).count() > 1 {
        decline!("q23");
    }
    Ok(Some(DistributedQuery {
        stages,
        finalize_sql: build_outer_finalize(sort, limit)?,
    }))
}

// ---------------------------------------------------------------------------
// KAN-156: fanned-out Q23 channel arms.
//
// The original composition runs each replicated channel arm as ONE `Forward` stage (one task,
// always on worker 0) scanning the full replicated channel fact — SF100 profile: 170 s + 110 s
// of single-task work for the catalog/web arms. The fanned-out form replaces each arm with a
// short pipeline whose every stage is parallel:
//
// 1. **Export** (leaf producer, one task per worker): the arm's scan region — the replicated
//    channel fact + dimensions with the arm's non-CTE conjuncts — projecting the group columns
//    (`gc{j}`), aggregate arguments (`aa{i}`), and per-CTE join keys (`j{m}_{k}`). The region's
//    anchor table (the channel fact) is sliced across workers via the stage's reduced replicate
//    stamp, so each worker exports a disjoint 1/W file slice; hash-shuffled by the first CTE's
//    join key.
// 2. **One semi stage per CTE**: an inner equijoin against the CTE's terminal output, which is
//    re-keyed from the partition-0 gather to the arm's join columns, so equal keys co-locate
//    and the per-partition join is the exact single-node join (multiplicity preserved; NULL
//    keys never match on either side). Intermediate semis passthrough and re-shuffle by the
//    next CTE's key; the last folds in the partial aggregate (`sum`/`count`/`min`/`max` over
//    the exported `aa{i}`), hash-shuffled by the group key.
// 3. **Recombine** per arm: the associative combine over the co-located per-slice partials,
//    re-applying the arm's output projection via the ordinary remap machinery. Arms recombine
//    SEPARATELY and the final `UNION ALL` stage concatenates them — equal group keys across
//    arms must not merge (single-node concatenates arm outputs too).
//
// Exactness: the export is row-level over the region, so the disjoint per-slice union equals
// the full region output; inner equijoins distribute over hash co-location; the partial
// aggregates re-add associatively once equal groups co-locate. The result is
// semantics-identical to the single-`Forward` arm.
//
// Admission is deliberately narrow (anything else keeps the `Forward` arms): every arm must be
// a plain grouped aggregate (no sort/limit/having/alias-projection/grouping-set/DISTINCT, only
// sum/count/min/max) over a strip-able scan region whose CTE references are all plain-column
// equi keys, every CTE occurrence must be key-joined (never cross-joined), the re-key target
// per CTE must agree across arms, and every arm's region must offer a safe slice anchor
// (multi-worker cluster — single-worker keeps `Forward` byte-identical).
// ---------------------------------------------------------------------------

/// One arm's join against one CTE occurrence in the Q23 fan-out.
struct Q23CteJoin {
    /// The deduplicated representative this occurrence plans from.
    rep: usize,
    /// CTE output key column positions (the terminal stage's re-key target).
    key_idx: Vec<u32>,
    /// CTE output key column names, aligned with `key_idx`.
    key_cols: Vec<String>,
    /// Arm-side key expression SQL over the scan-region columns, aligned with `key_cols`.
    arm_key_sql: Vec<String>,
}

/// One arm's fanned-out analysis (see the module comment above).
struct Q23ArmFanout<'a> {
    /// The arm's peeled projection + aggregate.
    p: Peeled<'a>,
    /// Classified arm aggregates (all non-DISTINCT sum/count/min/max).
    aggs: Vec<AggSpec>,
    /// Group column SQL (flattened).
    group_sql: Vec<String>,
    /// The arm's scan region: `agg.input` with every CTE leaf removed (join keys recorded).
    region: LogicalPlan,
    /// Per-CTE-occurrence joins, in leaf order.
    joins: Vec<Q23CteJoin>,
    /// The export stage's reduced replicate stamp (sliced anchors dropped).
    stamp: String,
}

/// State for [`q23_strip_cte`]: the CTE leaf identity/alias maps plus the equi keys recorded
/// while stripping them out of an arm's scan region.
struct Q23StripCtx {
    leaf_ids: HashSet<usize>,
    alias_of: HashMap<usize, String>,
    occ_of: HashMap<usize, usize>,
    /// Recorded equi keys per leaf node id: (CTE-side column name, arm-side expression).
    keys: HashMap<usize, Vec<(String, Expr)>>,
    /// Unqualified names of every CTE output column (ambiguity guard).
    leaf_fields: HashSet<String>,
}

impl Q23StripCtx {
    /// The CTE leaf a column belongs to, by its relation qualifier matching the leaf's alias.
    fn leaf_of(&self, c: &Column) -> Option<usize> {
        let relation = c.relation.as_ref()?;
        self.alias_of
            .iter()
            .find(|(_, alias)| alias.as_str() == relation.table())
            .map(|(id, _)| *id)
    }
}

/// The distinct CTE leaves `e` references. Err on anything the analysis cannot rule out: an
/// unqualified column whose name collides with a CTE output column, or columns of two
/// different leaves in one expression.
fn q23_leaf_refs(e: &Expr, ctx: &Q23StripCtx) -> std::result::Result<Vec<usize>, ()> {
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
    let mut refs: Vec<usize> = Vec::new();
    let mut bad = false;
    let _ = e.apply(|node| {
        if let Expr::Column(c) = node {
            match ctx.leaf_of(c) {
                Some(id) => {
                    if !refs.contains(&id) {
                        refs.push(id);
                    }
                }
                None if c.relation.is_none() && ctx.leaf_fields.contains(&c.name) => {
                    bad = true;
                    return Ok(TreeNodeRecursion::Stop);
                }
                None => {}
            }
        }
        Ok(TreeNodeRecursion::Continue)
    });
    if bad || refs.len() > 1 {
        return Err(());
    }
    Ok(refs)
}

/// Rebuild an arm's scan region with every CTE leaf removed, recording the equi keys that
/// connected it. `None` for any structure outside the admitted shape (a removed CTE leaf
/// bubbles up through the join cases, so a `None` return at the top level means "decline the
/// fan-out", never "the whole region was a leaf" — the arm itself is never a bare CTE).
fn q23_strip_cte(node: &LogicalPlan, ctx: &mut Q23StripCtx) -> Option<LogicalPlan> {
    if ctx.leaf_ids.contains(&node_id(node)) {
        return None;
    }
    match node {
        LogicalPlan::TableScan(_) => Some(node.clone()),
        LogicalPlan::Filter(f) => {
            let input = q23_strip_cte(&f.input, ctx)?;
            let mut conjuncts = Vec::new();
            flatten_conjuncts(&f.predicate, &mut conjuncts);
            let mut kept: Vec<Expr> = Vec::new();
            for c in conjuncts {
                let refs = q23_leaf_refs(c, ctx).ok()?;
                if refs.is_empty() {
                    kept.push(c.clone());
                    continue;
                }
                // A CTE-referencing conjunct is admitted only as a plain equi key:
                // `<cte>.<col> = <arm expr>` in either operand order.
                let Expr::BinaryExpr(be) = c else { return None };
                if be.op != Operator::Eq {
                    return None;
                }
                let leaf_id = refs[0];
                let no_refs = |e: &Expr| q23_leaf_refs(e, ctx).map(|r| r.is_empty()) == Ok(true);
                let (key_col, arm_expr) = match (be.left.as_ref(), be.right.as_ref()) {
                    (Expr::Column(col), other)
                        if ctx.leaf_of(col) == Some(leaf_id) && no_refs(other) =>
                    {
                        (col, other)
                    }
                    (other, Expr::Column(col))
                        if ctx.leaf_of(col) == Some(leaf_id) && no_refs(other) =>
                    {
                        (col, other)
                    }
                    _ => return None,
                };
                if expr_contains_subquery(arm_expr) {
                    return None;
                }
                ctx.keys
                    .entry(leaf_id)
                    .or_default()
                    .push((key_col.name.clone(), arm_expr.clone()));
            }
            if kept.is_empty() {
                return Some(input);
            }
            let predicate = kept.into_iter().reduce(|a, b| a.and(b))?;
            Some(LogicalPlan::Filter(
                Filter::try_new(predicate, Arc::new(input)).ok()?,
            ))
        }
        LogicalPlan::Join(j) if j.join_type == JoinType::Inner => {
            let left_leaf = ctx.leaf_ids.contains(&node_id(&j.left));
            let right_leaf = ctx.leaf_ids.contains(&node_id(&j.right));
            match (left_leaf, right_leaf) {
                (true, true) => None,
                (false, false) => {
                    // No CTE on either side: the ON keys / residual must stay leaf-free.
                    let no_refs =
                        |e: &Expr| q23_leaf_refs(e, ctx).map(|r| r.is_empty()) == Ok(true);
                    for (le, re) in &j.on {
                        if !no_refs(le) || !no_refs(re) {
                            return None;
                        }
                    }
                    if let Some(f) = &j.filter {
                        if !no_refs(f) {
                            return None;
                        }
                    }
                    let nl = q23_strip_cte(&j.left, ctx)?;
                    let nr = q23_strip_cte(&j.right, ctx)?;
                    Some(LogicalPlan::Join(datafusion::logical_expr::Join {
                        left: Arc::new(nl),
                        right: Arc::new(nr),
                        ..j.clone()
                    }))
                }
                (left_is_leaf, _) => {
                    // The CTE sits directly on one side of the join: record the ON keys
                    // (a key-less cross join records nothing here — the comma-join form's
                    // conjuncts above contribute them instead — and the per-leaf key check
                    // at the end declines a genuinely un-keyed CTE).
                    let (leaf_id, other_side) = if left_is_leaf {
                        (node_id(&j.left), &j.right)
                    } else {
                        (node_id(&j.right), &j.left)
                    };
                    if j.filter.is_some() {
                        return None;
                    }
                    for (le, re) in &j.on {
                        let (key_expr, arm_expr) = if left_is_leaf { (le, re) } else { (re, le) };
                        let Expr::Column(col) = key_expr else {
                            return None;
                        };
                        if ctx.leaf_of(col) != Some(leaf_id)
                            || q23_leaf_refs(arm_expr, ctx).map(|r| r.is_empty()) != Ok(true)
                            || expr_contains_subquery(arm_expr)
                        {
                            return None;
                        }
                        ctx.keys
                            .entry(leaf_id)
                            .or_default()
                            .push((col.name.clone(), arm_expr.clone()));
                    }
                    q23_strip_cte(other_side, ctx)
                }
            }
        }
        _ => None,
    }
}

/// Analyze one Q23 channel arm for the fan-out pipeline (see the KAN-156 module comment).
fn q23_arm_fanout<'a>(
    arm: &'a LogicalPlan,
    leaves: &[&'a LogicalPlan],
    base: usize,
    rep_of: &[usize],
    replicated: &[&str],
) -> Option<Q23ArmFanout<'a>> {
    let up = Unparser::default();
    let p = peel(arm).ok()?;
    if p.sort.is_some()
        || p.limit.is_some()
        || !p.having.is_empty()
        || !p.alias_projections.is_empty()
    {
        return None;
    }
    if p.agg.group_expr.is_empty() || is_grouping_set(&p.agg.group_expr) {
        return None;
    }
    let aggs = p
        .agg
        .aggr_expr
        .iter()
        .map(AggSpec::classify)
        .collect::<Result<Vec<_>>>()
        .ok()?;
    if aggs
        .iter()
        .any(|a| a.distinct || !matches!(a.func.as_str(), "sum" | "count" | "min" | "max"))
    {
        return None;
    }

    let mut ctx = Q23StripCtx {
        leaf_ids: HashSet::new(),
        alias_of: HashMap::new(),
        occ_of: HashMap::new(),
        keys: HashMap::new(),
        leaf_fields: HashSet::new(),
    };
    for (j, leaf) in leaves.iter().enumerate() {
        // The leaf must carry its CTE alias: the join conjuncts address it by relation.
        let LogicalPlan::SubqueryAlias(s) = leaf else {
            return None;
        };
        let id = node_id(leaf);
        ctx.leaf_ids.insert(id);
        ctx.alias_of.insert(id, s.alias.table().to_string());
        ctx.occ_of.insert(id, base + j);
        ctx.leaf_fields
            .extend(leaf.schema().fields().iter().map(|f| f.name().clone()));
    }

    // Group keys and aggregate arguments are computed in the export, before any CTE join —
    // they must read region columns only.
    for e in p.agg.group_expr.iter().chain(p.agg.aggr_expr.iter()) {
        if !q23_leaf_refs(e, &ctx).ok()?.is_empty() || expr_contains_subquery(e) {
            return None;
        }
    }

    let region = q23_strip_cte(p.agg.input.as_ref(), &mut ctx)?;
    // The region must evaluate per worker with exact per-slice output: either all-replicated
    // (sliced across workers via a safe anchor, KAN-156) or — KAN-161 — exactly one sharded
    // anchor scanned once in a broadcast-safe join tree, which each worker already holds as
    // its disjoint local shard (no slicing; the stamp keeps the full replicated list). Two
    // sharded tables in one region would join local shards only — WRONG; keep rejecting that.
    let region_tables = base_tables(&region);
    let mut region_sharded: Vec<&str> = region_tables
        .iter()
        .map(String::as_str)
        .filter(|t| !replicated.contains(t))
        .collect();
    region_sharded.sort_unstable();
    region_sharded.dedup();
    let stamp = match region_sharded.as_slice() {
        [] => sliced_replicate_stamp(&region, replicated)?,
        [t] => {
            if count_table_scans(&region, t) != 1 {
                return None;
            }
            reject_unsafe_broadcast_shapes(&region, t).ok()?;
            replicated.join(",")
        }
        _ => return None,
    };
    let group_sql: Vec<String> = flattened_group_exprs(&p.agg.group_expr)
        .into_iter()
        .map(|g| expr_sql(&up, g))
        .collect::<Result<_>>()
        .ok()?;

    let mut joins = Vec::with_capacity(leaves.len());
    for leaf in leaves {
        let id = node_id(leaf);
        let keys = ctx.keys.get(&id).filter(|k| !k.is_empty())?;
        let field_names: Vec<&str> = leaf
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        // The semi+partial stage groups by the unqualified exported `gc{j}` aliases; a CTE
        // output column literally named `gc<N>` would make that GROUP BY ambiguous.
        if field_names.iter().any(|n| {
            let b = n.as_bytes();
            b.len() > 2 && b[0] == b'g' && b[1] == b'c' && b[2..].iter().all(u8::is_ascii_digit)
        }) {
            return None;
        }
        let mut key_idx = Vec::with_capacity(keys.len());
        let mut key_cols = Vec::with_capacity(keys.len());
        let mut arm_key_sql = Vec::with_capacity(keys.len());
        for (col, expr) in keys {
            // The key column must address the CTE output schema as a plain identifier (it is
            // spliced into the semi stage's ON clause).
            if !col.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_') {
                return None;
            }
            key_idx.push(field_names.iter().position(|n| n == col)? as u32);
            key_cols.push(col.clone());
            arm_key_sql.push(expr_sql(&up, expr).ok()?);
        }
        let occ = ctx.occ_of[&id];
        joins.push(Q23CteJoin {
            rep: rep_of[occ],
            key_idx,
            key_cols,
            arm_key_sql,
        });
    }
    Some(Q23ArmFanout {
        p,
        aggs,
        group_sql,
        region,
        joins,
        stamp,
    })
}

/// The partial-aggregate select item over the exported `aa{i}` (semi+partial stage).
fn q23_fanout_partial(func: &str, i: usize) -> Option<String> {
    Some(match func {
        "sum" => format!("sum(semi_s.aa{i}) AS c{i}"),
        "count" => format!("count(semi_s.aa{i}) AS c{i}"),
        "min" => format!("min(semi_s.aa{i}) AS c{i}"),
        "max" => format!("max(semi_s.aa{i}) AS c{i}"),
        _ => return None,
    })
}

/// The recombine select item over the `c{i}` partials (counts re-add by summing).
fn q23_fanout_combine(func: &str, i: usize) -> Option<String> {
    Some(match func {
        "sum" | "count" => format!("sum(c{i}) AS r{i}"),
        "min" => format!("min(c{i}) AS r{i}"),
        "max" => format!("max(c{i}) AS r{i}"),
        _ => return None,
    })
}

/// Build one arm's pipeline stages with ids allocated from `next_id` (nothing is pushed to the
/// plan's stage list until every arm built successfully — the caller commits atomically).
/// Returns the built stages and the recombine (arm output) stage id.
fn build_q23_fanned_arm(
    fan: &Q23ArmFanout,
    rep_output: &HashMap<usize, u32>,
    next_id: &mut u32,
) -> Option<(Vec<StageDef>, u32)> {
    let n_group = fan.group_sql.len();
    let first_join = fan.joins.first()?;

    // Export: group cols, aggregate args, then per-join key columns.
    let mut export_cols: Vec<String> = Vec::new();
    for (j, g) in fan.group_sql.iter().enumerate() {
        export_cols.push(format!("{g} AS gc{j}"));
    }
    for (i, spec) in fan.aggs.iter().enumerate() {
        export_cols.push(format!("{} AS aa{i}", spec.arg_sql));
    }
    // (join position, key position) -> export column index; passthrough semis preserve these.
    let mut j_col: HashMap<(usize, usize), u32> = HashMap::new();
    for (m, join) in fan.joins.iter().enumerate() {
        for (k, sql) in join.arm_key_sql.iter().enumerate() {
            j_col.insert((m, k), export_cols.len() as u32);
            export_cols.push(format!("{sql} AS j{m}_{k}"));
        }
    }
    let region_sql = Unparser::default()
        .plan_to_sql(&fan.region)
        .map_err(|e| unsupported(format!("unparse q23 arm region: {e}")))
        .ok()?
        .to_string();
    let tail = sanitize_generated_sql(&extract_from_tail(&region_sql).ok()?);
    let export_sql = sanitize_generated_sql(&format!("SELECT {} {tail}", export_cols.join(", ")));
    let first_keys: Vec<u32> = (0..first_join.key_idx.len())
        .map(|k| j_col[&(0, k)])
        .collect();
    let export_id = *next_id;
    *next_id += 1;
    let mut export_stage = StageDef::new(export_id, export_sql, vec![], first_keys);
    export_stage.replicated_tables = fan.stamp.clone();

    let mut stages = vec![export_stage];
    let mut prev_id = export_id;

    // One semi stage per CTE; the last folds in the partial aggregate.
    for (m, join) in fan.joins.iter().enumerate() {
        let cte_stage = rep_output[&join.rep];
        let cond = join
            .key_cols
            .iter()
            .enumerate()
            .map(|(k, col)| format!("semi_s.j{m}_{k} = semi_k.{col}"))
            .collect::<Vec<_>>()
            .join(" AND ");
        let upstreams = vec![prev_id, cte_stage];
        let id = *next_id;
        *next_id += 1;
        if m + 1 < fan.joins.len() {
            let next_keys: Vec<u32> = (0..fan.joins[m + 1].key_idx.len())
                .map(|k| j_col[&(m + 1, k)])
                .collect();
            let sql = sanitize_generated_sql(&format!(
                "SELECT semi_s.* FROM (SELECT * FROM shuffle_input_0) AS semi_s \
                 INNER JOIN (SELECT * FROM shuffle_input_1) AS semi_k ON {cond}"
            ));
            stages.push(StageDef::new(id, sql, upstreams, next_keys));
        } else {
            let mut sel: Vec<String> = (0..n_group)
                .map(|j| format!("semi_s.gc{j} AS gc{j}"))
                .collect();
            for (i, spec) in fan.aggs.iter().enumerate() {
                sel.push(q23_fanout_partial(&spec.func, i)?);
            }
            let group_by = (0..n_group)
                .map(|j| format!("gc{j}"))
                .collect::<Vec<_>>()
                .join(", ");
            let sql = sanitize_generated_sql(&format!(
                "SELECT {} FROM (SELECT * FROM shuffle_input_0) AS semi_s \
                 INNER JOIN (SELECT * FROM shuffle_input_1) AS semi_k ON {cond} GROUP BY {group_by}",
                sel.join(", ")
            ));
            let hash: Vec<u32> = (0..n_group as u32).collect();
            stages.push(StageDef::new(id, sql, upstreams, hash));
        }
        prev_id = id;
    }

    // Recombine the co-located per-slice partials; re-apply the arm's output projection.
    let mut sel: Vec<String> = (0..n_group).map(|j| format!("gc{j} AS g{j}")).collect();
    for (i, spec) in fan.aggs.iter().enumerate() {
        sel.push(q23_fanout_combine(&spec.func, i)?);
    }
    let group_by = (0..n_group)
        .map(|j| format!("gc{j}"))
        .collect::<Vec<_>>()
        .join(", ");
    let inner = format!(
        "SELECT {} FROM shuffle_input GROUP BY {group_by}",
        sel.join(", ")
    );
    let remap = build_remap(&fan.p);
    let sql = wrap_output(&fan.p, &inner, &remap).ok()?;
    let id = *next_id;
    *next_id += 1;
    stages.push(StageDef::new(
        id,
        sanitize_generated_sql(&sql),
        vec![prev_id],
        vec![],
    ));
    Some((stages, id))
}

/// Try to replace the per-arm `Forward` stages of [`try_union_over_derived_ctes`] with the
/// fanned-out pipelines (see the KAN-156 module comment). All-or-nothing: on success the CTE
/// terminal stages are re-keyed from the partition-0 gather to the arms' join keys and every
/// arm reads them hash-co-located; on any decline nothing is mutated and the caller emits the
/// original `Forward` arms against the gathered CTEs.
#[allow(clippy::too_many_arguments)]
fn try_q23_fanned_arms(
    union_inputs: &[Arc<LogicalPlan>],
    arm_leaves: &[Vec<&LogicalPlan>],
    rep_of: &[usize],
    rep_output: &HashMap<usize, u32>,
    replicated: &[&str],
    next_id: &mut u32,
    stages: &mut Vec<StageDef>,
) -> Option<Vec<u32>> {
    // Analyze every arm first — the re-keying below is not reversible per arm, so nothing may
    // fail after it starts.
    let mut fanouts = Vec::with_capacity(union_inputs.len());
    let mut base = 0usize;
    for (arm, leaves) in union_inputs.iter().zip(arm_leaves) {
        fanouts.push(q23_arm_fanout(arm, leaves, base, rep_of, replicated)?);
        base += leaves.len();
    }
    // One re-key target per CTE representative, identical across every arm that joins it (a
    // stage has exactly one output hash key).
    let mut rep_keys: HashMap<usize, Vec<u32>> = HashMap::new();
    for fan in &fanouts {
        for join in &fan.joins {
            match rep_keys.get(&join.rep) {
                Some(prev) if *prev != join.key_idx => return None,
                Some(_) => {}
                None => {
                    rep_keys.insert(join.rep, join.key_idx.clone());
                }
            }
        }
    }
    // Build every arm's stages with a scratch id cursor (pure — no plan mutation yet).
    let mut scratch = *next_id;
    let mut built: Vec<(Vec<StageDef>, u32)> = Vec::with_capacity(fanouts.len());
    for fan in &fanouts {
        built.push(build_q23_fanned_arm(fan, rep_output, &mut scratch)?);
    }

    // Commit: re-key the CTE terminal stages (equal keys now co-locate with the arm exports),
    // then append the arm pipelines.
    for (rep, key_idx) in &rep_keys {
        let terminal = rep_output[rep];
        let stage = stages.iter_mut().find(|s| s.stage_id == terminal)?;
        stage.hash_key_cols = key_idx.clone();
    }
    let mut ids = Vec::with_capacity(built.len());
    for (arm_stages, out_id) in built {
        stages.extend(arm_stages);
        ids.push(out_id);
    }
    *next_id = scratch;
    Some(ids)
}

/// Plan one derived CTE for [`try_union_over_derived_ctes`]: the single-row-scalar cross-join
/// threshold shape first (it never matches a plain grouped aggregate), then the ordinary
/// recursive machinery.
fn plan_derived_cte(leaf: &LogicalPlan, replicated: &[&str]) -> Result<Option<DistributedQuery>> {
    if let Some(dq) = try_cross_scalar_threshold(leaf, replicated)? {
        return Ok(Some(dq));
    }
    Ok(plan_distributed_logical(leaf, replicated).ok())
}

/// Collect an arm's derived sharded CTEs (occurrence level), or `None` when the arm's shape is
/// outside the [`try_union_over_derived_ctes`] composition. `at_root` is set only for the arm
/// node itself (KAN-161): with the channel facts sharded, the arm's own scan region reads one
/// sharded fact — the root then stays the skeleton instead of classifying as a CTE leaf.
/// `anchor` carries that decision down the recursion: the descent must not re-classify the
/// root's own aggregate as a CTE leaf once it reaches it (a bare `Aggregate` leaf is unplannable
/// here — `q23_arm_fanout` needs `SubqueryAlias` leaves, and the skeleton above it still
/// references the aggregate's original output names), and below that aggregate the region's
/// scans of the single sharded anchor table are legitimate per-worker local-shard reads.
#[derive(Clone)]
struct RootAnchor<'a> {
    /// The arm root's own aggregate, kept as the skeleton.
    agg: &'a Aggregate,
    /// The one sharded table its scan region reads (verified scanned exactly once in a
    /// broadcast-safe join tree at the root).
    table: String,
    /// Flips once the recursion crosses `agg`: only below it is `table` tolerated in the
    /// replicated-only skeleton check.
    inside: bool,
}

fn collect_arm_leaves<'a>(
    node: &'a LogicalPlan,
    replicated: &[&str],
    out: &mut Vec<&'a LogicalPlan>,
    at_root: bool,
    mut anchor: Option<RootAnchor<'a>>,
) -> Option<()> {
    // A peel-able grouped aggregate whose own body directly scans a sharded base table is a
    // materialized derived CTE (frequent_ss_items / best_ss_customer). The arm's own aggregate
    // is not: its input region (down to the nested CTE aggregates) scans replicated tables only.
    let stripped = strip_aliases(node);
    if let Ok(p) = peel(stripped) {
        if p.sort.is_none() && p.limit.is_none() && !p.agg.group_expr.is_empty() {
            let mut region = Vec::new();
            skeleton_region_tables(p.agg.input.as_ref(), &mut region);
            if region.iter().any(|t| !replicated.contains(&t.as_str())) {
                // KAN-161: at the arm root the region may scan the arm's own channel fact
                // (catalog_sales / web_sales now shard). Keep the root as the skeleton when the
                // region holds exactly one sharded table, scanned once, in a broadcast-safe
                // join tree — the fanned-out arm pipeline then evaluates it per worker over
                // the local shard (exact: row-level export + hash co-location downstream).
                // Deeper nodes keep leaf semantics: they are the materialized derived CTEs,
                // and a sharded region there must still classify as a leaf.
                let mut region_sharded: Vec<&str> = region
                    .iter()
                    .map(String::as_str)
                    .filter(|t| !replicated.contains(t))
                    .collect();
                region_sharded.sort_unstable();
                region_sharded.dedup();
                let root_anchor = at_root
                    && matches!(
                        region_sharded.as_slice(),
                        [t] if count_table_scans(p.agg.input.as_ref(), t) == 1
                            && reject_unsafe_broadcast_shapes(p.agg.input.as_ref(), t).is_ok()
                    );
                if root_anchor {
                    anchor = Some(RootAnchor {
                        agg: p.agg,
                        table: region_sharded[0].to_string(),
                        inside: false,
                    });
                } else if anchor
                    .as_ref()
                    .map_or(true, |a| !std::ptr::eq(a.agg, p.agg))
                {
                    out.push(node);
                    return Some(());
                }
            }
        }
    }
    // A skeleton node: its own region may read replicated tables only, including tables inside
    // expression subqueries. (The arm root admitted above is an Aggregate/Projection node, so
    // `skeleton_region_tables` does not descend past its aggregate boundary into the region —
    // the sharded anchor is not re-rejected here. Below the anchored aggregate the region's
    // scans of the anchor table itself are the admitted per-worker local-shard reads.)
    let anchor_table = anchor
        .as_ref()
        .filter(|a| a.inside)
        .map(|a| a.table.as_str());
    let mut region = Vec::new();
    skeleton_region_tables(node, &mut region);
    collect_subquery_tables(node, &mut region);
    if region
        .iter()
        .any(|t| !replicated.contains(&t.as_str()) && Some(t.as_str()) != anchor_table)
    {
        return None;
    }
    match node {
        LogicalPlan::Projection(_) | LogicalPlan::SubqueryAlias(_) | LogicalPlan::Filter(_) => {
            for i in node.inputs() {
                collect_arm_leaves(i, replicated, out, false, anchor.clone())?;
            }
            Some(())
        }
        LogicalPlan::Aggregate(a) if !a.group_expr.is_empty() => {
            let before = out.len();
            let mut child_anchor = anchor.clone();
            if let Some(an) = child_anchor.as_mut() {
                if std::ptr::eq(an.agg, a) {
                    an.inside = true;
                }
            }
            for i in node.inputs() {
                collect_arm_leaves(i, replicated, out, false, child_anchor.clone())?;
            }
            // A replicated-only aggregate in the arm skeleton is declined conservatively
            // (the arm must read at least one gathered CTE to belong in this composition);
            // the Forward placement above would compute it exactly once, but there is no
            // query shape today that needs the relaxation.
            if out.len() == before {
                return None;
            }
            Some(())
        }
        LogicalPlan::Join(j) if j.join_type == JoinType::Inner => {
            for i in node.inputs() {
                collect_arm_leaves(i, replicated, out, false, anchor.clone())?;
            }
            Some(())
        }
        LogicalPlan::TableScan(_) => Some(()),
        _ => None,
    }
}

/// Base-table scans reachable from `lp` without crossing an aggregate / window / set-op boundary
/// (the scan region a skeleton node or a CTE's own aggregate body evaluates per partition).
fn skeleton_region_tables(lp: &LogicalPlan, out: &mut Vec<String>) {
    match lp {
        LogicalPlan::TableScan(scan) => out.push(scan.table_name.table().to_string()),
        LogicalPlan::Projection(_)
        | LogicalPlan::Filter(_)
        | LogicalPlan::SubqueryAlias(_)
        | LogicalPlan::Join(_) => {
            for i in lp.inputs() {
                skeleton_region_tables(i, out);
            }
        }
        _ => {}
    }
}

/// Leaves of a cross-join region: descend `Inner` joins with no equijoin keys and no residual
/// filter (DataFusion's representation of a comma join); every other node is a leaf.
fn collect_cross_leaves<'a>(node: &'a LogicalPlan, out: &mut Vec<&'a LogicalPlan>) -> Option<()> {
    match node {
        LogicalPlan::Join(j)
            if j.join_type == JoinType::Inner && j.on.is_empty() && j.filter.is_none() =>
        {
            collect_cross_leaves(&j.left, out)?;
            collect_cross_leaves(&j.right, out)?;
            Some(())
        }
        other => {
            out.push(other);
            Some(())
        }
    }
}

/// Distribute a grouped aggregate whose FROM cross-joins a **single-row derived scalar**
/// (TPC-DS Q23's `best_ss_customer ⋈ max_store_sales`):
///
/// ```sql
/// SELECT c_customer_sk, sum(ss_quantity * ss_sales_price) ssales
/// FROM store_sales, customer, max_store_sales          -- max_store_sales: one row
/// WHERE ss_customer_sk = c_customer_sk
/// GROUP BY c_customer_sk
/// HAVING sum(ss_quantity * ss_sales_price) > 0.5 * max(tpcds_cmax)
/// ```
///
/// A global (ungrouped) aggregate always yields exactly one row, so cross-joining it preserves
/// the outer aggregate's groups, and any `min/max/sum/avg(<its column>)` wrapping inside the
/// outer aggregate list is just that single value (referenced from HAVING by name). The
/// composition:
///
/// 1. the scalar's own input (a grouped derived aggregate — Q23's per-customer `sq2`) plans
///    through the ordinary recursive machinery and gathers;
/// 2. a scalar partial/combine pair (the KAN-27 one-row broadcast) computes the single value;
/// 3. the outer aggregate plans through the ordinary machinery with the scalar leaf dropped
///    from its FROM and the wrapped references replaced by the driver's literal placeholder
///    ([`SCALAR_TOKEN`]), so its combine applies the HAVING against the *global* value.
///
/// KAN-158: when sq2 is a **filter-restriction** of the outer per-key aggregate (same group
/// keys and measure; sq2 only adds INNER joins to replicated dims + filters on those dims),
/// step 1+3 share one raw fact-scan export via [`try_kan158_share_restricted_agg`] instead of
/// scanning the fact twice (SF100 profile: stages 2≡6 at ~46 s each before CSE).
///
/// Restricted to: exactly one single-row derived leaf (a global aggregate over a peel-able
/// grouped aggregate, one non-DISTINCT min/max/sum/count/avg over a plain column); every
/// reference to it is a single-argument min/max/sum/avg aggregate in the outer aggregate list
/// referenced from HAVING (the projection and the pre-aggregation filters must not mention it);
/// no grouping sets; no alias projections.
fn try_cross_scalar_threshold(
    lp: &LogicalPlan,
    replicated: &[&str],
) -> Result<Option<DistributedQuery>> {
    use datafusion::common::tree_node::{Transformed, TreeNode};

    let Ok(p) = peel(strip_aliases(lp)) else {
        decline!("q23-scalar");
    };
    if p.sort.is_some()
        || p.limit.is_some()
        || p.having.is_empty()
        || p.agg.group_expr.is_empty()
        || !p.alias_projections.is_empty()
        || p.agg
            .group_expr
            .iter()
            .any(|e| matches!(e, Expr::GroupingSet(_)))
    {
        decline!("q23-scalar");
    }

    // Cross-region leaves under the aggregate's filters.
    let mut filter_preds: Vec<&Expr> = Vec::new();
    let mut body = p.agg.input.as_ref();
    while let LogicalPlan::Filter(f) = body {
        flatten_conjuncts(&f.predicate, &mut filter_preds);
        body = f.input.as_ref();
    }
    let mut leaves: Vec<&LogicalPlan> = Vec::new();
    let Some(()) = collect_cross_leaves(body, &mut leaves) else {
        decline!("q23-scalar");
    };

    // The single-row leaf: an optional one-column passthrough projection over a global
    // aggregate over a peel-able grouped derived aggregate. Exactly one per shape (the driver
    // substitutes a single scalar literal).
    let mut scalar_idx: Option<usize> = None;
    for (i, leaf) in leaves.iter().enumerate() {
        let mut node = strip_aliases(leaf);
        if let LogicalPlan::Projection(pr) = node {
            if pr.expr.len() == 1 && matches!(strip_alias(&pr.expr[0]), Expr::Column(_)) {
                node = pr.input.as_ref();
            }
        }
        let LogicalPlan::Aggregate(m) = node else {
            continue;
        };
        if !m.group_expr.is_empty() {
            continue;
        }
        if scalar_idx.is_some() {
            decline!("q23-scalar");
        }
        scalar_idx = Some(i);
    }
    let Some(mi) = scalar_idx else {
        decline!("q23-scalar");
    };
    let m_leaf = leaves[mi];
    let m_out: Vec<String> = m_leaf
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    if m_out.len() != 1 {
        decline!("q23-scalar");
    }
    let mut m_node = strip_aliases(m_leaf);
    let mut m_proj: Option<&[Expr]> = None;
    if let LogicalPlan::Projection(pr) = m_node {
        if pr.expr.len() != 1 || !matches!(strip_alias(&pr.expr[0]), Expr::Column(_)) {
            decline!("q23-scalar");
        }
        m_proj = Some(pr.expr.as_slice());
        m_node = pr.input.as_ref();
    }
    let LogicalPlan::Aggregate(m_agg) = m_node else {
        decline!("q23-scalar");
    };
    if m_agg.aggr_expr.len() != 1 {
        decline!("q23-scalar");
    }
    let m_spec = match AggSpec::classify(&m_agg.aggr_expr[0]) {
        Ok(s) => s,
        Err(_) => return Ok(None),
    };
    if m_spec.distinct
        || !matches!(
            m_spec.func.as_str(),
            "min" | "max" | "sum" | "count" | "avg"
        )
    {
        decline!("q23-scalar");
    }
    let m_arg = match strip_alias(&m_agg.aggr_expr[0]) {
        Expr::AggregateFunction(af) if af.params.args.len() == 1 => &af.params.args[0],
        _ => return Ok(None),
    };
    let Expr::Column(m_arg_col) = m_arg else {
        decline!("q23-scalar");
    };
    // The scalar's input must be a grouped derived aggregate (planned recursively below).
    let sq2 = m_agg.input.as_ref();
    let Ok(sq2_p) = peel(strip_aliases(sq2)) else {
        decline!("q23-scalar");
    };
    if sq2_p.agg.group_expr.is_empty() || sq2_p.sort.is_some() || sq2_p.limit.is_some() {
        decline!("q23-scalar");
    }

    // Partition the outer aggregate list: `min/max/sum/avg(<scalar column>)` wraps (dropped —
    // the cross join guarantees each is the single scalar value) and the real aggregates.
    let mut real_aggs: Vec<Expr> = Vec::with_capacity(p.agg.aggr_expr.len());
    let mut dropped_refs: Vec<String> = Vec::new();
    let group_w = p.agg.group_expr.len();
    for (i, e) in p.agg.aggr_expr.iter().enumerate() {
        let is_wrap = match strip_alias(e) {
            Expr::AggregateFunction(af) if af.params.args.len() == 1 => {
                let fname = AggSpec::classify(e).map(|s| s.func).unwrap_or_default();
                matches!(fname.as_str(), "min" | "max" | "sum" | "avg")
                    && matches!(&af.params.args[0], Expr::Column(c) if c.name == m_out[0])
            }
            _ => false,
        };
        if is_wrap {
            dropped_refs.push(e.schema_name().to_string());
            if let Some(f) = p.agg.schema.fields().get(group_w + i) {
                dropped_refs.push(f.name().clone());
            }
        } else {
            real_aggs.push(e.clone());
        }
    }
    if dropped_refs.is_empty() {
        decline!("q23-scalar");
    }

    // The projection and the pre-aggregation filters must not touch the scalar leaf at all.
    let touches_scalar = |e: &Expr| {
        let mut cols = Vec::new();
        expr_columns(e, &mut cols);
        cols.iter().any(|c| c.name == m_out[0])
            || dropped_refs
                .iter()
                .any(|n| *n == e.schema_name().to_string())
    };
    if filter_preds.iter().any(|pr| touches_scalar(pr))
        || p.projection
            .is_some_and(|exprs| exprs.iter().any(touches_scalar))
    {
        decline!("q23-scalar");
    }

    // Rewrite HAVING: every reference to a dropped wrap becomes the driver's placeholder
    // literal. At least one reference must exist, or the scalar leaf is dead weight and this
    // shape adds machinery for nothing.
    let token = SCALAR_TOKEN.to_string();
    let mut replaced = 0usize;
    let mut new_having: Vec<Expr> = Vec::with_capacity(p.having.len());
    for h in &p.having {
        let rewritten = (*h)
            .clone()
            .transform(|node| {
                let hit = match &node {
                    Expr::Column(c) => dropped_refs
                        .iter()
                        .any(|n| *n == c.flat_name() || *n == c.name),
                    Expr::AggregateFunction(_) => dropped_refs
                        .iter()
                        .any(|n| *n == node.schema_name().to_string()),
                    _ => false,
                };
                if hit {
                    replaced += 1;
                    return Ok(Transformed::yes(Expr::Literal(
                        ScalarValue::Utf8(Some(token.clone())),
                        None,
                    )));
                }
                Ok(Transformed::no(node))
            })
            .map(|t| t.data)
            .unwrap_or_else(|_| (*h).clone());
        new_having.push(rewritten);
    }
    if replaced == 0
        || new_having.iter().any(|e| {
            let mut cols = Vec::new();
            expr_columns(e, &mut cols);
            cols.iter().any(|c| c.name == m_out[0])
        })
    {
        decline!("q23-scalar");
    }

    // Rebuild the outer aggregate without the scalar leaf and plan it through the ordinary
    // machinery: partial per worker, hash shuffle by the group key, combine applying the
    // HAVING against the placeholder literal the driver substitutes before dispatch.
    let remaining: Vec<&LogicalPlan> = leaves
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != mi)
        .map(|(_, l)| *l)
        .collect();
    if remaining.is_empty() {
        decline!("q23-scalar");
    }
    let mut cur: LogicalPlan = remaining[0].clone();
    for l in &remaining[1..] {
        cur = LogicalPlanBuilder::from(cur)
            .cross_join((*l).clone())
            .and_then(|b| b.build())
            .map_err(|e| unsupported(format!("rebuild cross region: {e}")))?;
    }
    if let Some(pred) = and_all(filter_preds.iter().map(|pr| (*pr).clone()).collect()) {
        cur = LogicalPlan::Filter(
            Filter::try_new(pred, Arc::new(cur))
                .map_err(|e| unsupported(format!("rebuild cross filters: {e}")))?,
        );
    }
    let new_agg = Aggregate::try_new(Arc::new(cur), p.agg.group_expr.to_vec(), real_aggs)
        .map_err(|e| unsupported(format!("rebuild outer aggregate: {e}")))?;
    let having_pred = and_all(new_having).ok_or_else(|| unsupported("empty HAVING"))?;
    let mut plan = LogicalPlan::Filter(
        Filter::try_new(having_pred, Arc::new(LogicalPlan::Aggregate(new_agg)))
            .map_err(|e| unsupported(format!("rebuild outer HAVING: {e}")))?,
    );
    if let Some(proj) = p.projection {
        plan = LogicalPlanBuilder::from(plan)
            .project(proj.to_vec())
            .and_then(|b| b.build())
            .map_err(|e| unsupported(format!("rebuild outer projection: {e}")))?;
    }

    // Scalar partial / one-row combine projection SQL (KAN-27), shared by both paths below.
    let up = Unparser::default();
    let (scalar_sels, scalar_comb) = per_key_agg_parts(&m_spec.func, &m_arg_col.name, 0)?;
    let guard_col = if m_spec.func == "avg" { "a0c" } else { "a0" };
    let proj_sql = match m_proj {
        Some(exprs) => {
            let mut s0v_remap = HashMap::new();
            s0v_remap.insert(
                m_agg.aggr_expr[0].schema_name().to_string(),
                "s0v".to_string(),
            );
            if let Some(f) = m_agg.schema.fields().first() {
                s0v_remap.insert(f.name().clone(), "s0v".to_string());
            }
            let mapped = remap_expr_columns(strip_alias(&exprs[0]), &s0v_remap);
            let mut cols = Vec::new();
            expr_columns(&mapped, &mut cols);
            if !cols.iter().all(|c| c.relation.is_none() && c.name == "s0v") {
                decline!("q23-scalar");
            }
            expr_sql(&up, &mapped)?
        }
        None => "s0v".to_string(),
    };

    // KAN-158: try one shared raw scan feeding both the restricted (sq2) and unrestricted
    // (outer) per-key aggregates before falling back to two independent fact scans.
    if let Some(dq) = try_kan158_share_restricted_agg(
        &sq2_p,
        &plan,
        replicated,
        &scalar_sels,
        &scalar_comb,
        guard_col,
        &proj_sql,
        &token,
    )? {
        return Ok(Some(dq));
    }

    let dq = match plan_distributed_logical(&plan, replicated) {
        Ok(dq) => dq,
        Err(_) => return Ok(None),
    };
    if dq.finalize_sql.is_some() {
        decline!("q23-scalar");
    }
    // The placeholder must survive as a quoted literal in exactly one stage's SQL.
    let quoted = format!("'{token}'");
    if dq.stages.iter().filter(|s| s.sql.contains(&quoted)).count() != 1 {
        decline!("q23-scalar");
    }

    // The scalar's own derived aggregate, distributed; it must gather so the scalar partial
    // sees every row on partition 0.
    let sq2_dq = match plan_distributed_logical(sq2, replicated) {
        Ok(dq) => dq,
        Err(_) => {
            let stripped = strip_aliases(sq2);
            if std::ptr::eq(stripped, sq2) {
                decline!("q23-scalar");
            }
            match plan_distributed_logical(stripped, replicated) {
                Ok(dq) => dq,
                Err(_) => return Ok(None),
            }
        }
    };
    if sq2_dq.finalize_sql.is_some()
        || sq2_dq
            .stages
            .last()
            .map_or(true, |s| !s.hash_key_cols.is_empty())
    {
        decline!("q23-scalar");
    }

    let mut stages = sq2_dq.stages;
    let sq2_last = stages.last().map(|s| s.stage_id).unwrap_or(0);
    let scalar_partial_id = sq2_last + 1;
    stages.push(StageDef::new(
        scalar_partial_id,
        sanitize_generated_sql(&format!(
            "SELECT {} FROM shuffle_input",
            scalar_sels.join(", ")
        )),
        vec![sq2_last],
        vec![],
    ));
    let scalar_combine_id = scalar_partial_id + 1;
    stages.push(StageDef::new(
        scalar_combine_id,
        sanitize_generated_sql(&format!(
            "SELECT {proj_sql} AS m0 FROM \
             (SELECT {scalar_comb} AS s0v FROM shuffle_input HAVING COUNT({guard_col}) > 0) AS cs"
        )),
        vec![scalar_partial_id],
        vec![],
    ));
    let (shifted, _) = shift_stages(&dq.stages, scalar_combine_id + 1);
    stages.extend(shifted);
    Ok(Some(DistributedQuery {
        stages,
        finalize_sql: None,
    }))
}

/// KAN-158: share one raw fact-scan export between a **filter-restricted** per-key aggregate
/// (`sq2_p`, the scalar's input) and an **unrestricted** sibling (`outer_plan`, the rebuilt
/// best-customer aggregate).
///
/// Admission (all required — anything weaker is not provably exact):
///
/// 1. Both peels have the same group-key count and identical group-expr SQL, one non-DISTINCT
///    `sum`/`count`/`min`/`max`/`avg` each, and identical argument SQL.
/// 2. Stripping filters, the outer tail's base tables are a **proper subset** of sq2's; every
///    extra table is replicated; every join below both tails is INNER (so filters commute).
/// 3. The extra tables contribute a single equijoin key from the outer tail's schema onto a
///    column of the extra dim (Q23: `ss_sold_date_sk = d_date_sk`); sq2's residual filters
///    mention only columns of the extra dims (the year window).
/// 4. The outer body is broadcast-safe over exactly one sharded fact.
///
/// Plan shape when admitted:
///
/// ```text
/// shared leaf  — export (g0.., dsk, amt) from the outer body, hash by group keys
/// dated partial ← shared — join extra dims + sq2 filters, partial agg (feeds the scalar)
/// dated combine → scalar partial/combine (KAN-27 one-row broadcast)
/// wide partial  ← shared — unrestricted partial agg
/// wide combine  — HAVING against the scalar token (outer output)
/// ```
///
/// Declines (returns `Ok(None)`) when any check fails — the caller keeps the two-scan path.
#[allow(clippy::too_many_arguments)] // scalar partial/combine fragments + token travel with the plan peels
fn try_kan158_share_restricted_agg(
    sq2_p: &Peeled<'_>,
    outer_plan: &LogicalPlan,
    replicated: &[&str],
    scalar_sels: &[String],
    scalar_comb: &str,
    guard_col: &str,
    proj_sql: &str,
    token: &str,
) -> Result<Option<DistributedQuery>> {
    let dbg = std::env::var("OXIDANT_TPCDS_DEBUG").is_ok();
    macro_rules! decline158 {
        ($($arg:tt)*) => {{
            if dbg {
                eprintln!("[kan158] decline: {}", format!($($arg)*));
            }
            return Ok(None);
        }};
    }
    let Ok(outer_p) = peel(strip_aliases(outer_plan)) else {
        decline158!("outer peel failed");
    };
    if outer_p.agg.group_expr.len() != sq2_p.agg.group_expr.len()
        || outer_p.agg.aggr_expr.len() != 1
        || sq2_p.agg.aggr_expr.len() != 1
        || !sq2_p.having.is_empty()
        || outer_p.sort.is_some()
        || sq2_p.sort.is_some()
        || plan_contains_volatile(outer_plan)
        || plan_contains_volatile(sq2_p.agg.input.as_ref())
    {
        decline158!(
            "shape mismatch groups={}vs{} aggs={}vs{} sq2_having={} volatile",
            outer_p.agg.group_expr.len(),
            sq2_p.agg.group_expr.len(),
            outer_p.agg.aggr_expr.len(),
            sq2_p.agg.aggr_expr.len(),
            sq2_p.having.len()
        );
    }
    let up = Unparser::default();
    for (a, b) in outer_p.agg.group_expr.iter().zip(&sq2_p.agg.group_expr) {
        let (asql, bsql) = (expr_sql(&up, a)?, expr_sql(&up, b)?);
        if asql != bsql {
            decline158!("group expr mismatch {asql} vs {bsql}");
        }
    }
    let outer_agg = AggSpec::classify(&outer_p.agg.aggr_expr[0])?;
    let sq2_agg = AggSpec::classify(&sq2_p.agg.aggr_expr[0])?;
    if outer_agg.distinct
        || sq2_agg.distinct
        || outer_agg.func != sq2_agg.func
        || outer_agg.arg_sql != sq2_agg.arg_sql
        || !matches!(
            outer_agg.func.as_str(),
            "sum" | "count" | "min" | "max" | "avg"
        )
    {
        decline158!(
            "agg mismatch {}/{} vs {}/{}",
            outer_agg.func,
            outer_agg.arg_sql,
            sq2_agg.func,
            sq2_agg.arg_sql
        );
    }

    let (outer_tail, outer_filters) = match strip_filters(outer_p.agg.input.as_ref()) {
        Some(v) => v,
        None => decline158!("strip_filters outer failed"),
    };
    let (sq2_tail, sq2_filters) = match strip_filters(sq2_p.agg.input.as_ref()) {
        Some(v) => v,
        None => decline158!("strip_filters sq2 failed"),
    };
    // The unrestricted body may carry residual filters (DataFusion often leaves equijoin
    // predicates as Filter-over-CrossJoin); they ride the shared leaf's WHERE. Sq2 must carry
    // at least one *additional* filter (the year window) that becomes the dated consumer's
    // restriction.
    if sq2_filters.is_empty() {
        decline158!("sq2 has no residual filters");
    }
    if !only_inner_joins(&outer_tail) || !only_inner_joins(&sq2_tail) {
        decline158!("non-inner joins");
    }

    let outer_tables = base_tables(&outer_tail);
    let sq2_tables = base_tables(&sq2_tail);
    let outer_set: HashSet<&str> = outer_tables.iter().map(String::as_str).collect();
    let mut extra: Vec<&str> = sq2_tables
        .iter()
        .map(String::as_str)
        .filter(|t| !outer_set.contains(t))
        .collect();
    extra.sort_unstable();
    extra.dedup();
    if extra.is_empty() || extra.iter().any(|t| !replicated.contains(t)) {
        decline158!("extra={extra:?} replicated check");
    }
    // Every outer table must appear in sq2 (sq2 is a restriction, not a rewrite).
    if outer_tables
        .iter()
        .any(|t| !sq2_tables.iter().any(|s| s == t))
    {
        decline158!("outer tables not subset of sq2: {outer_tables:?} vs {sq2_tables:?}");
    }

    let mut outer_sharded: Vec<&str> = outer_tables
        .iter()
        .map(String::as_str)
        .filter(|t| !replicated.contains(t))
        .collect();
    outer_sharded.sort_unstable();
    outer_sharded.dedup();
    let [fact] = outer_sharded.as_slice() else {
        decline158!("outer_sharded={outer_sharded:?}");
    };
    if count_table_scans(&outer_tail, fact) != 1
        || reject_unsafe_broadcast_shapes(&outer_tail, fact).is_err()
    {
        decline158!("broadcast-unsafe or multi-scan fact={fact}");
    }

    // Exactly one extra replicated dim (Q23: date_dim). Multi-dim restrictions need per-dim
    // join keys and are declined until a caller needs them.
    if extra.len() != 1 {
        decline158!("extra len {} {:?}", extra.len(), extra);
    }
    let extra_table = extra[0];

    // The equijoin key from the outer body onto the extra dim — required so the dated
    // consumer can re-attach with `INNER JOIN dim ON dsk = dim.<key>` without inventing
    // join conditions. Q23: `ss_sold_date_sk = d_date_sk`.
    let Some((dsk_sql, dim_key)) = fact_side_join_key_to_extra(&sq2_tail, &outer_tail, extra_table)
    else {
        decline158!("no join key to extra={extra_table}");
    };
    // Sq2 residual filters may only touch the extra dim. A filter on the fact/customer
    // columns would not be a pure restriction of the shared leaf's row set.
    for f in &sq2_filters {
        let mut cols = Vec::new();
        expr_columns(f, &mut cols);
        if cols
            .iter()
            .any(|c| match c.relation.as_ref().map(|r| r.table()) {
                Some(t) => t != extra_table,
                None => !table_has_column(&sq2_tail, extra_table, &c.name),
            })
        {
            decline158!("sq2 filter touches non-extra cols");
        }
    }

    let n_group = outer_p.agg.group_expr.len();
    let mut export_cols: Vec<String> = Vec::with_capacity(n_group + 2);
    for (j, g) in outer_p.agg.group_expr.iter().enumerate() {
        export_cols.push(format!("{} AS g{j}", expr_sql(&up, g)?));
    }
    export_cols.push(format!("{dsk_sql} AS dsk"));
    export_cols.push(format!("{} AS amt", outer_agg.arg_sql));
    let outer_sql = Unparser::default()
        .plan_to_sql(&outer_tail)
        .map_err(|e| unsupported(format!("kan158 unparse outer tail: {e}")))?
        .to_string();
    let tail = sanitize_generated_sql(&extract_from_tail(&outer_sql)?);
    let shared_where = if outer_filters.is_empty() {
        String::new()
    } else {
        let parts: Result<Vec<_>> = outer_filters.iter().map(|f| expr_sql(&up, f)).collect();
        format!(" WHERE {}", parts?.join(" AND "))
    };
    let shared_sql = sanitize_generated_sql(&format!(
        "SELECT {} {tail}{shared_where}",
        export_cols.join(", ")
    ));
    let hash_keys: Vec<u32> = (0..n_group as u32).collect();

    // Dated restriction = sq2 filters that are not already on the shared leaf. Prefer the
    // full sq2_filters list when outer had no filters; otherwise require every sq2 filter to
    // mention only the extra dim (outer's join-eq filters live on outer tables and must not
    // be re-applied after the dim join).
    let dated_filters: Vec<&Expr> = if outer_filters.is_empty() {
        sq2_filters.iter().collect()
    } else {
        sq2_filters
            .iter()
            .filter(|f| {
                let mut cols = Vec::new();
                expr_columns(f, &mut cols);
                cols.iter().any(|c| {
                    c.relation
                        .as_ref()
                        .is_some_and(|r| r.table() == extra_table)
                        || table_has_column(&sq2_tail, extra_table, &c.name)
                })
            })
            .collect()
    };
    if dated_filters.is_empty() {
        decline158!("no dated restriction filters after subtracting outer");
    }
    let filter_sql = {
        let parts: Result<Vec<_>> = dated_filters.iter().map(|f| expr_sql(&up, f)).collect();
        parts?.join(" AND ")
    };
    let join_extra = {
        let dim_sql = qualified_table_sql(&sq2_tail, extra_table);
        // Alias the dim back to its bare name so residual filter SQL (which names
        // `date_dim.d_year` via the plan's Column relation) still resolves after the
        // shared-leaf projection dropped the original scan.
        if dim_sql == extra_table {
            format!("INNER JOIN {dim_sql} ON dsk = {extra_table}.{dim_key}")
        } else {
            format!("INNER JOIN {dim_sql} AS {extra_table} ON dsk = {extra_table}.{dim_key}")
        }
    };
    let group_by = (0..n_group)
        .map(|j| format!("g{j}"))
        .collect::<Vec<_>>()
        .join(", ");
    let (partial_sels, _) = partial_combine_sql(&outer_agg.func, 0, "amt")?;
    let dated_sql = sanitize_generated_sql(&format!(
        "SELECT {group_by}, {} FROM shuffle_input {join_extra} WHERE {filter_sql} GROUP BY {group_by}",
        partial_sels.join(", ")
    ));
    let wide_sql = sanitize_generated_sql(&format!(
        "SELECT {group_by}, {} FROM shuffle_input GROUP BY {group_by}",
        partial_sels.join(", ")
    ));

    // Plan the outer through the ordinary machinery only to reuse its combine/HAVING SQL and
    // output projection — then discard its leaf (replaced by the shared-scan wide partial).
    let outer_dq = match plan_distributed_logical(outer_plan, replicated) {
        Ok(dq) => dq,
        Err(_) => return Ok(None),
    };
    if outer_dq.finalize_sql.is_some() || outer_dq.stages.len() < 2 {
        return Ok(None);
    }
    let quoted = format!("'{token}'");
    if outer_dq
        .stages
        .iter()
        .filter(|s| s.sql.contains(&quoted))
        .count()
        != 1
    {
        return Ok(None);
    }
    // The outer leaf must be the only empty-upstream stage; its combine (and any projection
    // stages) follow. We keep everything after the leaf.
    let outer_leaf = &outer_dq.stages[0];
    if !outer_leaf.upstream_stage_ids.is_empty() {
        return Ok(None);
    }
    let outer_rest = &outer_dq.stages[1..];

    // Likewise plan sq2 for its combine SQL (output names `c_customer_sk` / `csales`).
    let sq2_plan = {
        // Rebuild sq2 as a bare aggregate (no HAVING) matching sq2_p.
        let agg = Aggregate::try_new(
            Arc::new(sq2_p.agg.input.as_ref().clone()),
            sq2_p.agg.group_expr.to_vec(),
            sq2_p.agg.aggr_expr.to_vec(),
        )
        .map_err(|e| unsupported(format!("kan158 rebuild sq2: {e}")))?;
        let mut plan = LogicalPlan::Aggregate(agg);
        if let Some(proj) = sq2_p.projection {
            plan = LogicalPlanBuilder::from(plan)
                .project(proj.to_vec())
                .and_then(|b| b.build())
                .map_err(|e| unsupported(format!("kan158 sq2 projection: {e}")))?;
        }
        plan
    };
    let sq2_dq = match plan_distributed_logical(&sq2_plan, replicated) {
        Ok(dq) => dq,
        Err(_) => return Ok(None),
    };
    if sq2_dq.finalize_sql.is_some()
        || sq2_dq.stages.len() < 2
        || !sq2_dq.stages[0].upstream_stage_ids.is_empty()
        || sq2_dq
            .stages
            .last()
            .map_or(true, |s| !s.hash_key_cols.is_empty())
    {
        return Ok(None);
    }
    let sq2_rest = &sq2_dq.stages[1..];

    // Assemble: shared → dated partial → sq2 combines → scalar → wide partial → outer rest.
    let mut stages = Vec::new();
    let shared_id = 0u32;
    stages.push(StageDef::new(
        shared_id,
        shared_sql,
        vec![],
        hash_keys.clone(),
    ));
    let dated_partial_id = 1u32;
    stages.push(StageDef::new(
        dated_partial_id,
        dated_sql,
        vec![shared_id],
        hash_keys.clone(),
    ));
    // Shift sq2's post-leaf stages to follow the dated partial (their upstream 0 → dated).
    let mut next_id = dated_partial_id + 1;
    let id_shift = next_id; // old id 0 (leaf) maps conceptually to dated_partial; old 1 → next_id
    let mut sq2_last = dated_partial_id;
    for s in sq2_rest {
        let new_id = s.stage_id + id_shift - 1; // leaf was 0; first rest was 1 → id_shift
        let mut upstreams: Vec<u32> = s
            .upstream_stage_ids
            .iter()
            .map(|&u| {
                if u == 0 {
                    dated_partial_id
                } else {
                    u + id_shift - 1
                }
            })
            .collect();
        if upstreams.is_empty() {
            upstreams.push(dated_partial_id);
        }
        stages.push(StageDef {
            stage_id: new_id,
            sql: s.sql.clone(),
            upstream_stage_ids: upstreams,
            hash_key_cols: s.hash_key_cols.clone(),
            exchange: s.exchange,
            plan_fragment: s.plan_fragment.clone(),
            lakehouse_snapshot_pins: s.lakehouse_snapshot_pins.clone(),
            replicated_tables: String::new(),
        });
        sq2_last = new_id;
        next_id = new_id + 1;
    }

    let scalar_partial_id = next_id;
    stages.push(StageDef::new(
        scalar_partial_id,
        sanitize_generated_sql(&format!(
            "SELECT {} FROM shuffle_input",
            scalar_sels.join(", ")
        )),
        vec![sq2_last],
        vec![],
    ));
    let scalar_combine_id = scalar_partial_id + 1;
    stages.push(StageDef::new(
        scalar_combine_id,
        sanitize_generated_sql(&format!(
            "SELECT {proj_sql} AS m0 FROM \
             (SELECT {scalar_comb} AS s0v FROM shuffle_input HAVING COUNT({guard_col}) > 0) AS cs"
        )),
        vec![scalar_partial_id],
        vec![],
    ));
    next_id = scalar_combine_id + 1;

    let wide_partial_id = next_id;
    stages.push(StageDef::new(
        wide_partial_id,
        wide_sql,
        vec![shared_id],
        hash_keys,
    ));
    next_id = wide_partial_id + 1;
    // Shift outer's post-leaf stages; their upstream 0 → wide_partial.
    let outer_shift = next_id;
    for s in outer_rest {
        let new_id = s.stage_id + outer_shift - 1;
        let mut upstreams: Vec<u32> = s
            .upstream_stage_ids
            .iter()
            .map(|&u| {
                if u == 0 {
                    wide_partial_id
                } else {
                    u + outer_shift - 1
                }
            })
            .collect();
        if upstreams.is_empty() {
            upstreams.push(wide_partial_id);
        }
        stages.push(StageDef {
            stage_id: new_id,
            sql: s.sql.clone(),
            upstream_stage_ids: upstreams,
            hash_key_cols: s.hash_key_cols.clone(),
            exchange: s.exchange,
            plan_fragment: s.plan_fragment.clone(),
            lakehouse_snapshot_pins: s.lakehouse_snapshot_pins.clone(),
            replicated_tables: String::new(),
        });
    }

    Ok(Some(DistributedQuery {
        stages,
        finalize_sql: None,
    }))
}

/// True when a scan of `table` under `lp` exposes `column` in its schema.
fn table_has_column(lp: &LogicalPlan, table: &str, column: &str) -> bool {
    match lp {
        LogicalPlan::TableScan(s) if s.table_name.table() == table => {
            s.projected_schema
                .fields()
                .iter()
                .any(|f| f.name() == column)
                || s.source
                    .schema()
                    .fields()
                    .iter()
                    .any(|f| f.name() == column)
        }
        _ => lp
            .inputs()
            .iter()
            .any(|i| table_has_column(i, table, column)),
    }
}

/// Fact-side column SQL + dim-side key name for the equijoin onto `extra` in `sq2_tail`.
///
/// Walks `sq2_tail`'s INNER join tree for an equijoin between a column belonging to
/// `outer_tail`'s tables and a column belonging to `extra`. Returns
/// `(fact_side_sql, dim_key_name)` so the shared leaf can export the fact-side column as
/// `dsk` and the dated consumer can write `INNER JOIN extra ON dsk = extra.dim_key`.
fn fact_side_join_key_to_extra(
    sq2_tail: &LogicalPlan,
    outer_tail: &LogicalPlan,
    extra: &str,
) -> Option<(String, String)> {
    let outer_table_set: HashSet<String> = base_tables(outer_tail).into_iter().collect();
    let mut found: Option<(String, String)> = None;
    let mut stack = vec![sq2_tail];
    while let Some(node) = stack.pop() {
        if let LogicalPlan::Join(j) = node {
            if j.join_type == JoinType::Inner {
                for (l, r) in &j.on {
                    let (Expr::Column(lc), Expr::Column(rc)) = (l, r) else {
                        continue;
                    };
                    let l_extra = lc.relation.as_ref().is_some_and(|rel| rel.table() == extra);
                    let r_extra = rc.relation.as_ref().is_some_and(|rel| rel.table() == extra);
                    let l_outer = lc
                        .relation
                        .as_ref()
                        .is_some_and(|rel| outer_table_set.contains(rel.table()));
                    let r_outer = rc
                        .relation
                        .as_ref()
                        .is_some_and(|rel| outer_table_set.contains(rel.table()));
                    let candidate = match (l_outer && r_extra, r_outer && l_extra) {
                        (true, false) => Some((l, rc.name.clone())),
                        (false, true) => Some((r, lc.name.clone())),
                        _ => None,
                    };
                    if let Some((fact_side, dim_key)) = candidate {
                        let sql = expr_sql(&Unparser::default(), fact_side).ok()?;
                        if found
                            .as_ref()
                            .is_some_and(|(prev, prev_k)| prev != &sql || prev_k != &dim_key)
                        {
                            return None; // ambiguous / conflicting keys
                        }
                        found = Some((sql, dim_key));
                    }
                }
            }
            stack.push(j.left.as_ref());
            stack.push(j.right.as_ref());
            continue;
        }
        for i in node.inputs() {
            stack.push(i);
        }
    }
    found
}

/// Conjunctive AND of `exprs` (`None` for an empty list).
fn and_all(exprs: Vec<Expr>) -> Option<Expr> {
    let mut it = exprs.into_iter();
    let mut acc = it.next()?;
    for e in it {
        acc = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(acc),
            op: Operator::And,
            right: Box::new(e),
        });
    }
    Some(acc)
}

// ---------------------------------------------------------------------------
// Q95: IN key sets produced by a shuffle-first self-join of the sharded fact.
// ---------------------------------------------------------------------------

/// The self-join CTE behind Q95's `IN` subqueries (`ws_wh`): two scans of the same sharded
/// fact equijoined on a key, with residual conjuncts over the two aliases' columns.
struct SelfJoinCte {
    /// The equijoin key column name (also the `IN` target column name).
    key: String,
    /// Every filter conjunct over the self-join, cloned.
    conjuncts: Vec<Expr>,
    a_alias: String,
    b_alias: String,
    fingerprint: String,
}

enum InBody {
    /// `SELECT k FROM <self-join CTE>` — key set of the fact's self-join.
    SelfJoin(SelfJoinCte),
    /// `SELECT r.rk FROM <replicated r>, <self-join CTE> WHERE r.rk = cte.k`.
    ReplicatedJoin {
        table: String,
        r_key: String,
        cte_key: String,
        cte_fingerprint: String,
    },
    No,
}

/// Distribute a global aggregate whose WHERE carries `IN` subqueries over a self-join of the
/// sharded fact (TPC-DS Q95 — orders shipped from two warehouses and returned):
///
/// ```sql
/// SELECT count(DISTINCT ws_order_number), sum(ws_ext_ship_cost), sum(ws_net_profit)
/// FROM web_sales ws1, date_dim, customer_address, web_site
/// WHERE … AND ws1.ws_order_number IN (SELECT ws_order_number FROM ws_wh)
///         AND ws1.ws_order_number IN (SELECT wr_order_number FROM web_returns, ws_wh …)
/// -- ws_wh = web_sales ws1 JOIN web_sales ws2 ON order_number, warehouse_sk <> warehouse_sk
/// ```
///
/// The ordinary semi/anti path declines because the `IN` bodies scan the fact twice. Instead:
///
/// 1. **Key producer**: the fact exported as `(k0 = order key, ic{i} = residual columns)` and
///    hash-shuffled by `k0`, so every order's rows co-locate on one partition.
/// 2. **Self-join keys** (the "shuffle-first distinct-key producer" the KAN-55 refusal test
///    documented as missing): `SELECT DISTINCT a.k0 FROM shuffle_input a JOIN shuffle_input b
///    ON a.k0 = b.k0 AND <residuals>` per partition — exact, since all candidate pairs
///    co-locate. The replicated-table `IN` (`web_returns ⋈ ws_wh`) joins the replicated table
///    against that co-located key stream per partition.
/// 3. **Outer export**: the fact scan with its non-subquery predicates, exporting the order
///    key (`ok0`) and the aggregate argument columns, hash-shuffled by `ok0` — co-located with
///    both key streams, so the `IN` filters keep their exact three-valued semantics per
///    partition (`IN` keeps its `IN` spelling, never `EXISTS`).
/// 4. **Global aggregate**: `count(DISTINCT ok0)` is exact per partition (each order lands
///    wholly on one partition); sums recombine; the combine gathers one row per partition.
pub(crate) fn try_self_join_in_keys(
    lp: &LogicalPlan,
    replicated: &[&str],
) -> Result<Option<DistributedQuery>> {
    let Ok(p) = peel(lp) else {
        decline!("q95");
    };
    if !p.agg.group_expr.is_empty() || !p.having.is_empty() {
        decline!("q95");
    }
    let mut conjuncts: Vec<&Expr> = Vec::new();
    let mut body = p.agg.input.as_ref();
    while let LogicalPlan::Filter(f) = body {
        flatten_conjuncts(&f.predicate, &mut conjuncts);
        body = f.input.as_ref();
    }
    let mut in_conjuncts: Vec<(&Column, &LogicalPlan)> = Vec::new();
    let mut plain: Vec<&Expr> = Vec::new();
    for c in &conjuncts {
        match c {
            Expr::InSubquery(iq) if !iq.negated => {
                let Expr::Column(col) = iq.expr.as_ref() else {
                    decline!("q95");
                };
                in_conjuncts.push((col, iq.subquery.subquery.as_ref()));
            }
            other if expr_contains_subquery(other) => return Ok(None),
            other => plain.push(*other),
        }
    }
    if in_conjuncts.is_empty() {
        decline!("q95");
    }
    let outer_key = in_conjuncts[0].0;
    if in_conjuncts
        .iter()
        .any(|(c, _)| c.name != outer_key.name || c.relation != outer_key.relation)
    {
        decline!("q95");
    }
    // The outer body: one sharded fact scanned once, comma-joined to replicated tables only.
    let body_sharded = sharded_tables(body, replicated);
    let [fact] = body_sharded.as_slice() else {
        return Ok(None);
    };
    if count_table_scans(body, fact) != 1 || !is_plain_cross_join_body(body) {
        decline!("q95");
    }

    // Classify the IN bodies: exactly one self-join CTE shape shared by all of them.
    let mut cte: Option<SelfJoinCte> = None;
    let mut rep_bodies: Vec<(String, String, String)> = Vec::new();
    for (col, subq) in &in_conjuncts {
        match classify_in_body(subq, fact, replicated)? {
            InBody::SelfJoin(c) => {
                if c.key != col.name {
                    decline!("q95");
                }
                match &cte {
                    None => cte = Some(c),
                    Some(prev) if prev.fingerprint == c.fingerprint => {}
                    _ => return Ok(None),
                }
            }
            InBody::ReplicatedJoin {
                table,
                r_key,
                cte_key,
                cte_fingerprint,
            } => {
                if cte_key != col.name {
                    decline!("q95");
                }
                rep_bodies.push((table, r_key, cte_fingerprint));
            }
            InBody::No => return Ok(None),
        }
    }
    let Some(cte) = cte else {
        decline!("q95");
    };
    // Every replicated-join body must reference the same self-join CTE the direct body matched.
    let mut rep_bodies2: Vec<(String, String)> = Vec::new();
    for (table, r_key, fingerprint) in &rep_bodies {
        if *fingerprint != cte.fingerprint {
            decline!("q95");
        }
        rep_bodies2.push((table.clone(), r_key.clone()));
    }
    drop(rep_bodies);

    // Aggregate classification: DISTINCT only as count(DISTINCT <the order key>); every other
    // aggregate's argument must be a plain column of the sharded fact (exported as oc{j}).
    let fact_alias = fact_alias_of(body, fact);
    let up = Unparser::default();
    let mut export_cols: Vec<String> = Vec::new();
    let mut semi_sels: Vec<String> = Vec::new();
    let mut combines: Vec<String> = Vec::new();
    for (i, e) in p.agg.aggr_expr.iter().enumerate() {
        let spec = match AggSpec::classify(e) {
            Ok(s) => s,
            Err(_) => return Ok(None),
        };
        let arg = match strip_alias(e) {
            Expr::AggregateFunction(af) if af.params.args.len() <= 1 => af.params.args.first(),
            _ => return Ok(None),
        };
        if spec.distinct {
            if spec.func != "count" {
                decline!("q95");
            }
            let Some(Expr::Column(c)) = arg else {
                decline!("q95");
            };
            if c.name != outer_key.name || c.relation != outer_key.relation {
                decline!("q95");
            }
            semi_sels.push(format!("count(DISTINCT o.ok0) AS d{i}"));
            combines.push(format!("sum(d{i}) AS r{i}"));
            continue;
        }
        let arg_sql = match arg {
            None => "1".to_string(),
            Some(Expr::Column(c)) => {
                let Some(alias) = &fact_alias else {
                    decline!("q95");
                };
                if c.relation.as_ref().map(|r| r.table()) != Some(alias.as_str()) {
                    decline!("q95");
                }
                let pos = match export_cols.iter().position(|n| n == &c.name) {
                    Some(pos) => pos,
                    None => {
                        export_cols.push(c.name.clone());
                        export_cols.len() - 1
                    }
                };
                format!("o.oc{pos}")
            }
            _ => return Ok(None),
        };
        let (sel, comb) = partial_combine_sql(&spec.func, i, &arg_sql)?;
        semi_sels.extend(sel);
        combines.push(comb);
    }

    // Stage 0: the fact exported as (k0, ic{j}), hash-shuffled by the order key.
    let mut residual_cols: Vec<String> = Vec::new();
    for conj in &cte.conjuncts {
        let mut cols = Vec::new();
        expr_columns(conj, &mut cols);
        for c in cols {
            if c.name != cte.key && !residual_cols.contains(&c.name) {
                residual_cols.push(c.name.clone());
            }
        }
    }
    let mut producer_sel = vec![format!("{} AS k0", cte.key)];
    for (j, n) in residual_cols.iter().enumerate() {
        producer_sel.push(format!("{n} AS ic{j}"));
    }
    let fact_sql = qualified_table_sql(lp, fact);
    let producer_sql = sanitize_generated_sql(&format!(
        "SELECT {} FROM {fact_sql}",
        producer_sel.join(", ")
    ));

    // Stage 1: the self-join distinct keys, per partition over the co-located rows.
    let rename = |c: &Column| -> Column {
        let side = if c
            .relation
            .as_ref()
            .is_some_and(|r| r.table() == cte.a_alias)
        {
            "a"
        } else {
            "b"
        };
        let name = if c.name == cte.key {
            "k0".to_string()
        } else {
            let pos = residual_cols
                .iter()
                .position(|n| n == &c.name)
                .expect("residual col collected above");
            format!("ic{pos}")
        };
        Column::new(Some(side), name)
    };
    let mut self_join_preds = Vec::with_capacity(cte.conjuncts.len());
    for conj in &cte.conjuncts {
        let mapped = remap_columns_with(
            conj,
            &|c| {
                c.relation
                    .as_ref()
                    .is_some_and(|r| r.table() == cte.a_alias || r.table() == cte.b_alias)
            },
            &rename,
        )?;
        self_join_preds.push(expr_sql(&up, &mapped)?);
    }
    let keys_sql = sanitize_generated_sql(&format!(
        "SELECT DISTINCT a.k0 AS k0 FROM shuffle_input a JOIN shuffle_input b ON {}",
        self_join_preds.join(" AND ")
    ));

    let mut stages = vec![
        StageDef::new(0, producer_sql, vec![], vec![0]),
        StageDef::new(1, keys_sql, vec![0], vec![0]),
    ];
    // Per replicated-join IN body: keys of the replicated table present in the co-located
    // self-join key stream.
    let mut key_stage_ids = vec![1u32];
    for (table, r_key) in &rep_bodies2 {
        let id = stages.len() as u32;
        let r_sql = qualified_table_sql(lp, table);
        let sql = sanitize_generated_sql(&format!(
            "SELECT DISTINCT {table}.{r_key} AS k0 FROM {r_sql} \
             JOIN shuffle_input wh ON {table}.{r_key} = wh.k0"
        ));
        stages.push(StageDef::new(id, sql, vec![1], vec![0]));
        key_stage_ids.push(id);
    }

    // Outer export: the fact with its non-subquery predicates, exporting the order key and the
    // aggregate argument columns, hash-shuffled by the order key.
    let body_sql = plan_sql(body, "outer body")?;
    let tail = extract_from_tail(&body_sql)?;
    let mut out_sel = vec![format!(
        "{} AS ok0",
        expr_sql(&up, &Expr::Column(outer_key.clone()))?
    )];
    for (j, n) in export_cols.iter().enumerate() {
        let alias = fact_alias.as_deref().unwrap_or(fact);
        out_sel.push(format!("{alias}.{n} AS oc{j}"));
    }
    let where_sql = if plain.is_empty() {
        String::new()
    } else {
        let parts = plain
            .iter()
            .map(|pr| expr_sql(&up, pr))
            .collect::<Result<Vec<_>>>()?;
        format!(" WHERE {}", parts.join(" AND "))
    };
    let outer_id = stages.len() as u32;
    stages.push(StageDef::new(
        outer_id,
        sanitize_generated_sql(&format!("SELECT {} {tail}{where_sql}", out_sel.join(", "))),
        vec![],
        vec![0],
    ));

    // Semi + per-partition global aggregate: the IN filters see co-located key streams.
    let mut upstreams = key_stage_ids.clone();
    upstreams.push(outer_id);
    let outer_idx = upstreams.len() - 1;
    let in_filters = (0..key_stage_ids.len())
        .map(|i| format!("o.ok0 IN (SELECT k0 FROM shuffle_input_{i})"))
        .collect::<Vec<_>>()
        .join(" AND ");
    let semi_id = stages.len() as u32;
    stages.push(StageDef::new(
        semi_id,
        sanitize_generated_sql(&format!(
            "SELECT {} FROM shuffle_input_{outer_idx} AS o WHERE {in_filters}",
            semi_sels.join(", ")
        )),
        upstreams,
        vec![],
    ));
    let remap = build_remap(&p);
    let inner = format!(
        "SELECT {} FROM shuffle_input HAVING COUNT(*) > 0",
        combines.join(", ")
    );
    let final_sql = sanitize_generated_sql(&wrap_output(&p, &inner, &remap)?);
    stages.push(StageDef::new(semi_id + 1, final_sql, vec![semi_id], vec![]));
    Ok(Some(DistributedQuery {
        stages,
        finalize_sql: build_finalize(&p)?,
    }))
}

/// The outer body is a plain comma join: only `CrossJoin` / `TableScan` / `SubqueryAlias`
/// nodes below the stripped filters (joins/aggregates/windows would need their own shape).
fn is_plain_cross_join_body(lp: &LogicalPlan) -> bool {
    match lp {
        LogicalPlan::TableScan(_) | LogicalPlan::SubqueryAlias(_) => {
            lp.inputs().iter().all(|i| is_plain_cross_join_body(i))
        }
        LogicalPlan::Join(j) if j.join_type == JoinType::Inner && j.on.is_empty() => {
            is_plain_cross_join_body(&j.left) && is_plain_cross_join_body(&j.right)
        }
        _ => false,
    }
}

/// The alias the outer body gives the sharded fact scan, if any (`ws1` for Q95).
fn fact_alias_of(body: &LogicalPlan, fact: &str) -> Option<String> {
    fn walk(lp: &LogicalPlan, fact: &str, out: &mut Option<String>) {
        if let LogicalPlan::SubqueryAlias(a) = lp {
            if let LogicalPlan::TableScan(t) = a.input.as_ref() {
                if t.table_name.table() == fact {
                    *out = Some(a.alias.table().to_string());
                }
            }
        }
        for i in lp.inputs() {
            walk(i, fact, out);
        }
    }
    let mut out = None;
    walk(body, fact, &mut out);
    out
}

/// Match an `IN` body: either the self-join CTE itself (`SELECT k FROM <cte>`) or a replicated
/// table joined to it (`SELECT r.rk FROM r, <cte> WHERE r.rk = cte.k`).
fn classify_in_body(subq: &LogicalPlan, fact: &str, replicated: &[&str]) -> Result<InBody> {
    let LogicalPlan::Projection(p1) = strip_aliases(subq) else {
        return Ok(InBody::No);
    };
    if p1.expr.len() != 1 {
        return Ok(InBody::No);
    }
    let Expr::Column(out_col) = strip_alias(&p1.expr[0]) else {
        return Ok(InBody::No);
    };
    // Body A: the projection sits directly on the self-join CTE.
    if let Some(cte) = match_self_join_cte(&p1.input, fact, &out_col.name)? {
        return Ok(InBody::SelfJoin(cte));
    }
    // Body B: projection over a filter over a cross join of a replicated table and the CTE.
    let LogicalPlan::Filter(f) = strip_aliases(&p1.input) else {
        return Ok(InBody::No);
    };
    let LogicalPlan::Join(cj) = strip_aliases(&f.input) else {
        return Ok(InBody::No);
    };
    if cj.join_type != JoinType::Inner || !cj.on.is_empty() {
        return Ok(InBody::No);
    }
    // Split the two sides: a replicated table scan and the self-join CTE (in either order).
    let scan_relation = |lp: &LogicalPlan| -> Option<(String, String)> {
        match strip_aliases(lp) {
            LogicalPlan::TableScan(t) => {
                let n = t.table_name.table().to_string();
                Some((n.clone(), n))
            }
            LogicalPlan::SubqueryAlias(a) => match a.input.as_ref() {
                LogicalPlan::TableScan(t) => Some((
                    a.alias.table().to_string(),
                    t.table_name.table().to_string(),
                )),
                _ => None, // the CTE side
            },
            _ => None,
        }
    };
    let (rep_relation, rep_table, cte_side) =
        match (scan_relation(&cj.left), scan_relation(&cj.right)) {
            (Some((rel, tab)), None) => (rel, tab, cj.right.as_ref()),
            (None, Some((rel, tab))) => (rel, tab, cj.left.as_ref()),
            _ => return Ok(InBody::No),
        };
    if !replicated.contains(&rep_table.as_str()) {
        return Ok(InBody::No);
    }
    let LogicalPlan::SubqueryAlias(cte_a) = cte_side else {
        return Ok(InBody::No);
    };
    let cte_alias = cte_a.alias.table().to_string();
    // The CTE side must be the self-join shape.
    if !is_self_join_cte_shape(strip_aliases(cte_side), fact) {
        return Ok(InBody::No);
    }
    // Filter conjuncts: columns only from the replicated table or the CTE alias; the linking
    // equality pairs the replicated output key with the CTE's key column.
    let mut conjuncts = Vec::new();
    flatten_conjuncts(&f.predicate, &mut conjuncts);
    let mut cte_key: Option<String> = None;
    for conj in &conjuncts {
        if expr_contains_subquery(conj) {
            return Ok(InBody::No);
        }
        let mut cols = Vec::new();
        expr_columns(conj, &mut cols);
        if !cols.iter().all(|c| {
            c.relation
                .as_ref()
                .is_some_and(|r| r.table() == rep_relation || r.table() == cte_alias)
        }) {
            return Ok(InBody::No);
        }
        if let Expr::BinaryExpr(b) = conj {
            if b.op == Operator::Eq {
                if let (Expr::Column(l), Expr::Column(r)) = (b.left.as_ref(), b.right.as_ref()) {
                    for (maybe_rep, maybe_cte) in [(l, r), (r, l)] {
                        if maybe_rep.relation.as_ref().map(|x| x.table())
                            == Some(rep_relation.as_str())
                            && maybe_rep.name == out_col.name
                            && maybe_cte.relation.as_ref().map(|x| x.table())
                                == Some(cte_alias.as_str())
                        {
                            cte_key = Some(maybe_cte.name.clone());
                        }
                    }
                }
            }
        }
    }
    let Some(cte_key) = cte_key else {
        return Ok(InBody::No);
    };
    Ok(InBody::ReplicatedJoin {
        table: rep_table,
        r_key: out_col.name.clone(),
        cte_key,
        cte_fingerprint: plan_fingerprint(cte_side),
    })
}

/// The `ws_wh` CTE shape behind an IN body: `Projection` over `Filter` over a `CrossJoin` of
/// two aliased scans of the same sharded fact, with an equijoin on the key column and
/// arbitrary residual conjuncts over the two aliases' columns.
fn match_self_join_cte(
    cte_input: &LogicalPlan,
    fact: &str,
    key_name: &str,
) -> Result<Option<SelfJoinCte>> {
    let LogicalPlan::Projection(p2) = strip_aliases(cte_input) else {
        decline!("q95");
    };
    let LogicalPlan::Filter(f) = strip_aliases(&p2.input) else {
        decline!("q95");
    };
    let LogicalPlan::Join(cj) = strip_aliases(&f.input) else {
        decline!("q95");
    };
    if cj.join_type != JoinType::Inner || !cj.on.is_empty() {
        decline!("q95");
    }
    let (a_alias, b_alias) = match (strip_aliases_side(&cj.left), strip_aliases_side(&cj.right)) {
        (Some((a, ta)), Some((b, tb))) if ta == fact && tb == fact && a != b => (a, b),
        _ => return Ok(None),
    };
    // The CTE's key output must be a plain column named `key_name` from one of the aliases.
    let key_projected = p2.expr.iter().any(|e| {
        matches!(strip_alias(e), Expr::Column(c) if c.name == key_name
            && c.relation.as_ref().is_some_and(|r| r.table() == a_alias || r.table() == b_alias))
            && output_name(e) == key_name
    });
    if !key_projected {
        decline!("q95");
    }
    // Conjuncts: an equijoin `a.key = b.key` plus residuals over the two aliases only.
    let mut conjuncts: Vec<Expr> = Vec::new();
    {
        let mut refs: Vec<&Expr> = Vec::new();
        flatten_conjuncts(&f.predicate, &mut refs);
        refs.iter().for_each(|e| conjuncts.push((*e).clone()));
    }
    let mut has_key_eq = false;
    for conj in &conjuncts {
        if expr_contains_subquery(conj) {
            decline!("q95");
        }
        let mut cols = Vec::new();
        expr_columns(conj, &mut cols);
        if !cols.iter().all(|c| {
            c.relation
                .as_ref()
                .is_some_and(|r| r.table() == a_alias || r.table() == b_alias)
        }) {
            decline!("q95");
        }
        if let Expr::BinaryExpr(b) = conj {
            if b.op == Operator::Eq {
                if let (Expr::Column(l), Expr::Column(r)) = (b.left.as_ref(), b.right.as_ref()) {
                    let la = l.relation.as_ref().map(|x| x.table());
                    let ra = r.relation.as_ref().map(|x| x.table());
                    if l.name == key_name
                        && r.name == key_name
                        && ((la == Some(a_alias.as_str()) && ra == Some(b_alias.as_str()))
                            || (la == Some(b_alias.as_str()) && ra == Some(a_alias.as_str())))
                    {
                        has_key_eq = true;
                    }
                }
            }
        }
    }
    if !has_key_eq {
        decline!("q95");
    }
    Ok(Some(SelfJoinCte {
        key: key_name.to_string(),
        conjuncts,
        a_alias,
        b_alias,
        fingerprint: plan_fingerprint(cte_input),
    }))
}

/// Confirm a CTE subtree is the self-join shape: `Projection` over `Filter` over a
/// `CrossJoin` of two aliased scans of the same sharded fact.
fn is_self_join_cte_shape(cte_side: &LogicalPlan, fact: &str) -> bool {
    let LogicalPlan::Projection(p2) = cte_side else {
        return false;
    };
    let LogicalPlan::Filter(f) = strip_aliases(&p2.input) else {
        return false;
    };
    let LogicalPlan::Join(cj) = strip_aliases(&f.input) else {
        return false;
    };
    cj.join_type == JoinType::Inner
        && cj.on.is_empty()
        && matches!(
            (strip_aliases_side(&cj.left), strip_aliases_side(&cj.right)),
            (Some((_, ta)), Some((_, tb))) if ta == fact && tb == fact
        )
}

/// `SubqueryAlias(alias) → TableScan` as `(alias, table)`.
fn strip_aliases_side(lp: &LogicalPlan) -> Option<(String, String)> {
    let LogicalPlan::SubqueryAlias(a) = lp else {
        return None;
    };
    let LogicalPlan::TableScan(t) = a.input.as_ref() else {
        return None;
    };
    Some((
        a.alias.table().to_string(),
        t.table_name.table().to_string(),
    ))
}

// ---------------------------------------------------------------------------
// Q49: UNION (distinct) of per-channel arms carrying global rank() windows.
// ---------------------------------------------------------------------------

/// Distribute a `UNION` (distinct) of per-channel arms where each arm computes global
/// `rank()` windows over a tiny per-item aggregate and keeps its top-N (TPC-DS Q49):
///
/// ```sql
/// SELECT 'web' AS channel, item, return_ratio, rank() OVER (ORDER BY return_ratio) …
/// FROM (SELECT ws_item_sk item, sum(…) / sum(…) return_ratio, …
///       FROM web_sales LEFT JOIN web_returns … GROUP BY ws_item_sk)
/// WHERE return_rank <= 10 OR currency_rank <= 10
/// UNION SELECT 'catalog', … UNION SELECT 'store', …
/// ```
///
/// Each arm's per-item aggregate distributes as the ordinary partial/combine pair (the join
/// with the returns table is a broadcast against the sharded channel fact). The combined
/// per-item relation is tiny — one row per item — so it gathers to one partition (empty hash
/// key) where the global `rank()` windows and the top-N filter compute exactly; the arm
/// outputs concatenate (`UNION ALL`), hash-shuffle on the full row, and dedup per partition
/// for the `UNION` (distinct) semantics. The query's `ORDER BY` / `LIMIT` stay in the
/// driver-side finalize.
///
/// Restricted to: a `Distinct`-wrapped union tree; arms of the form `Projection → Filter →
/// Projection(with rank-style windows, no PARTITION BY) → WindowAggr* → distributable grouped
/// aggregate`; a plain-column top projection. Anything else declines to the existing paths.
pub(crate) fn try_ranked_union(
    lp: &LogicalPlan,
    replicated: &[&str],
) -> Result<Option<DistributedQuery>> {
    let (body, sort, limit) = peek_sort_limit(lp);
    let mut top_proj: Option<&[Expr]> = None;
    let mut node = body;
    loop {
        match node {
            LogicalPlan::Projection(p) => {
                if top_proj.is_some() {
                    decline!("q49");
                }
                top_proj = Some(p.expr.as_slice());
                node = p.input.as_ref();
            }
            LogicalPlan::SubqueryAlias(s) => node = s.input.as_ref(),
            _ => break,
        }
    }
    let LogicalPlan::Distinct(d) = node else {
        decline!("q49");
    };
    let mut arms: Vec<&LogicalPlan> = Vec::new();
    if !collect_union_arms(d.input(), &mut arms) || arms.len() < 2 {
        decline!("q49");
    }

    let mut stages: Vec<StageDef> = Vec::new();
    let mut arm_outs: Vec<u32> = Vec::new();
    let mut width: Option<usize> = None;
    for arm in &arms {
        let Some((arm_stages, arm_width)) = plan_ranked_arm(arm, replicated)? else {
            decline!("q49");
        };
        match width {
            None => width = Some(arm_width),
            Some(w) if w == arm_width => {}
            _ => return Ok(None),
        }
        let offset = stages.last().map(|s| s.stage_id + 1).unwrap_or(0);
        let (shifted, last) = shift_stages(&arm_stages, offset);
        stages.extend(shifted);
        arm_outs.push(last);
    }
    let n_cols = width.unwrap_or(0);
    if n_cols == 0 {
        decline!("q49");
    }

    let union_sql = (0..arm_outs.len())
        .map(|i| format!("SELECT * FROM shuffle_input_{i}"))
        .collect::<Vec<_>>()
        .join(" UNION ALL ");
    let union_id = stages.last().map(|s| s.stage_id + 1).unwrap_or(0);
    stages.push(StageDef::new(
        union_id,
        union_sql,
        arm_outs,
        (0..n_cols as u32).collect(),
    ));

    let up = Unparser::default();
    let dedup_sql = match top_proj {
        Some(exprs) => {
            let mut items = Vec::with_capacity(exprs.len());
            for e in exprs {
                let Expr::Column(_) = strip_alias(e) else {
                    decline!("q49");
                };
                items.push(format!(
                    "{} AS \"{}\"",
                    finalize_expr_sql(&up, &unqualify(strip_alias(e)))?,
                    output_name(e)
                ));
            }
            format!(
                "SELECT {} FROM (SELECT DISTINCT * FROM shuffle_input) AS dd",
                items.join(", ")
            )
        }
        None => "SELECT DISTINCT * FROM shuffle_input".to_string(),
    };
    let dedup_id = union_id + 1;
    stages.push(StageDef::new(
        dedup_id,
        sanitize_generated_sql(&dedup_sql),
        vec![union_id],
        vec![],
    ));
    Ok(Some(DistributedQuery {
        stages,
        finalize_sql: build_outer_finalize(sort, limit)?,
    }))
}

/// Collect the leaf arms of a (possibly nested, distinct) union tree. Nested `Distinct` nodes
/// wrapping a `Union` are redundant for the concatenated-dedup rebuild and are skipped.
fn collect_union_arms<'a>(node: &'a LogicalPlan, arms: &mut Vec<&'a LogicalPlan>) -> bool {
    match strip_aliases(node) {
        LogicalPlan::Union(u) => u.inputs.iter().all(|i| collect_union_arms(i, arms)),
        LogicalPlan::Distinct(d) if matches!(strip_aliases(d.input()), LogicalPlan::Union(_)) => {
            collect_union_arms(d.input(), arms)
        }
        _ => {
            arms.push(node);
            true
        }
    }
}

/// One Q49 channel arm: distribute the per-item aggregate, then gather the tiny result and
/// compute the global rank windows + top-N filter on partition 0.
fn plan_ranked_arm(
    arm: &LogicalPlan,
    replicated: &[&str],
) -> Result<Option<(Vec<StageDef>, usize)>> {
    let LogicalPlan::Projection(out_p) = strip_aliases(arm) else {
        decline!("q49");
    };
    let LogicalPlan::Filter(f) = strip_aliases(&out_p.input) else {
        decline!("q49");
    };
    let LogicalPlan::Projection(win_p) = strip_aliases(&f.input) else {
        decline!("q49");
    };
    // Descend the WindowAggr chain to the per-item aggregate, collecting the window
    // definitions. DataFusion splits window evaluation: the `WindowAggr` nodes hold the
    // `WindowFunction` exprs and the projection above references their results by schema name.
    let mut wf_exprs: Vec<&datafusion::logical_expr::expr::WindowFunction> = Vec::new();
    let mut w = win_p.input.as_ref();
    loop {
        match w {
            LogicalPlan::Window(wn) => {
                for e in &wn.window_expr {
                    let Expr::WindowFunction(wf) = strip_alias(e) else {
                        decline!("q49");
                    };
                    use datafusion::logical_expr::WindowFunctionDefinition;
                    let name = match &wf.fun {
                        WindowFunctionDefinition::AggregateUDF(fun) => {
                            fun.name().to_ascii_lowercase()
                        }
                        WindowFunctionDefinition::WindowUDF(fun) => fun.name().to_ascii_lowercase(),
                    };
                    // Only rank-style windows without PARTITION BY: their input is tiny (the
                    // per-item aggregate), so gathering to one partition is the exact plan.
                    if !matches!(name.as_str(), "rank" | "dense_rank" | "row_number")
                        || !wf.params.partition_by.is_empty()
                    {
                        decline!("q49");
                    }
                    wf_exprs.push(wf);
                }
                w = wn.input.as_ref();
            }
            LogicalPlan::SubqueryAlias(s) => w = s.input.as_ref(),
            _ => break,
        }
    }
    if wf_exprs.is_empty() {
        decline!("q49");
    }
    // The window projection: plain inner columns plus schema-named references to the collected
    // window results (aliased to the arm's output names).
    let up = Unparser::default();
    let mut win_items = Vec::with_capacity(win_p.expr.len());
    for e in &win_p.expr {
        let Expr::Column(c) = strip_alias(e) else {
            decline!("q49");
        };
        let name = &c.name;
        if let Some(wf) = wf_exprs.iter().find(|wf| {
            Expr::WindowFunction(Box::new((**wf).clone()))
                .schema_name()
                .to_string()
                == *name
        }) {
            let mut order_parts = Vec::with_capacity(wf.params.order_by.len());
            for s in &wf.params.order_by {
                let dir = if s.asc { "ASC" } else { "DESC" };
                let nulls = if s.nulls_first {
                    "NULLS FIRST"
                } else {
                    "NULLS LAST"
                };
                order_parts.push(format!(
                    "{} {dir} {nulls}",
                    finalize_expr_sql(&up, &unqualify(&s.expr))?
                ));
            }
            let wf_name = match &wf.fun {
                datafusion::logical_expr::WindowFunctionDefinition::AggregateUDF(fun) => {
                    fun.name().to_ascii_lowercase()
                }
                datafusion::logical_expr::WindowFunctionDefinition::WindowUDF(fun) => {
                    fun.name().to_ascii_lowercase()
                }
            };
            win_items.push(format!(
                "{wf_name}() OVER (ORDER BY {}) AS \"{}\"",
                order_parts.join(", "),
                output_name(e)
            ));
        } else {
            win_items.push(format!(
                "{} AS \"{}\"",
                finalize_expr_sql(&up, &unqualify(strip_alias(e)))?,
                output_name(e)
            ));
        }
    }

    let Ok(inner_p) = peel(w) else {
        decline!("q49");
    };
    if inner_p.sort.is_some() || inner_p.limit.is_some() || !inner_p.having.is_empty() {
        decline!("q49");
    }
    let mut stages = if sharded_tables(&inner_p.agg.input, replicated).is_empty() {
        // A channel arm reading only replicated tables is computed once on a single worker
        // (Forward) — per-worker partials would multiply the arm's rows by the worker count.
        // The window gather above it collects everything to partition 0 either way.
        let sql = plan_sql(w, "replicated ranked arm")?;
        let mut stage = StageDef::new(0, sql, vec![], vec![]);
        stage.exchange = crate::driver::ExchangeMode::Forward;
        vec![stage]
    } else {
        match aggregation_stages_for(&inner_p, replicated) {
            Ok(dq) if dq.finalize_sql.is_none() => dq.stages,
            _ => return Ok(None),
        }
    };

    let filter_sql = expr_sql(&up, &unqualify(&f.predicate))?;
    let mut out_items = Vec::with_capacity(out_p.expr.len());
    for e in &out_p.expr {
        out_items.push(format!(
            "{} AS \"{}\"",
            finalize_expr_sql(&up, &unqualify(strip_alias(e)))?,
            output_name(e)
        ));
    }
    let gather_sql = sanitize_generated_sql(&format!(
        "SELECT {} FROM (SELECT {} FROM shuffle_input) AS w WHERE {filter_sql}",
        out_items.join(", "),
        win_items.join(", ")
    ));
    let inner_last = stages.last().map(|s| s.stage_id).unwrap_or(0);
    stages.push(StageDef::new(
        inner_last + 1,
        gather_sql,
        vec![inner_last],
        vec![],
    ));
    Ok(Some((stages, out_p.expr.len())))
}

// ---------------------------------------------------------------------------
// Q14: ROLLUP over a per-channel UNION ALL whose arms read the sharded fact
//      through two derived-CTE subqueries — an INTERSECT key set (`IN`) and a
//      global-AVG threshold (`HAVING`).
// ---------------------------------------------------------------------------

/// Distribute TPC-DS Q14:
///
/// ```sql
/// WITH cross_items AS (SELECT i_item_sk ss_item_sk
///                      FROM item, (store ⨏ catalog ⨏ web over (brand,class,category)) sq1 …),
///      avg_sales   AS (SELECT avg(quantity*list_price) average_sales
///                      FROM (store arm UNION ALL catalog arm UNION ALL web arm) sq2)
/// SELECT channel, i_brand_id, …, sum(sales), sum(number_sales)
/// FROM (  SELECT 'store' channel, …, sum(ss_quantity*ss_list_price) sales, count(*) number_sales
///         FROM store_sales, item, date_dim
///         WHERE ss_item_sk IN (SELECT ss_item_sk FROM cross_items) AND …
///         GROUP BY i_brand_id, i_class_id, i_category_id
///         HAVING sum(ss_quantity*ss_list_price) > (SELECT average_sales FROM avg_sales)
///         UNION ALL <catalog arm> UNION ALL <web arm>) y
/// GROUP BY ROLLUP(channel, i_brand_id, i_class_id, i_category_id)
/// ORDER BY … LIMIT 100
/// ```
///
/// The two subqueries are **derived** tables (DataFusion inlines one copy per arm) that
/// themselves scan the sharded fact, so the base-table subquery safety checks refuse the query
/// whole, and the whole-fact gather is documented-broken for ROLLUP + UNION/INTERSECT. The
/// composition plans each derived table **once** (the inlined copies are fingerprint-checked
/// identical) and rewrites every arm to read those stage outputs:
///
/// 1. **cross_items** — the INTERSECT chain's raw arms run as leaf producers hash-shuffled on
///    the full (brand, class, category) triple (the sharded channel's arm per worker, the
///    replicated channels' arms once via `ExchangeMode::Forward` — or, on a multi-worker
///    cluster, fanned out per worker over disjoint file slices of the arm's anchor table;
///    KAN-156. Equal triples still co-locate, so the per-partition INTERSECT — dedup
///    included — stays exact), so equal triples co-locate
///    and the per-partition `INTERSECT` is exact. The `item` join-back is a per-partition
///    broadcast join against the co-located triple stream; its one-column `ss_item_sk` output
///    hash-shuffles on the key, becoming the co-located IN key stream for every channel arm.
/// 2. **avg_sales** — one per-worker `sum`/`count` partial stage per sharded arm plus one
///    optional partial over the replicated arms (KAN-161: any subset of channel arms may be
///    sharded, each scanning at most one sharded table exactly once; multi-worker: per-worker
///    partials over disjoint slices of the replicated anchors — the one-row-per-task global
///    partials re-add in the unchanged combine) combine into the one-row global AVG, gathered
///    to partition 0 (the Q24 threshold decomposition). The arms consume it as a **co-located
///    stream** (`SELECT m0 FROM shuffle_input_N`, the Q44 pattern) rather than a `SCALAR_TOKEN`
///    literal: the driver substitutes at most one such token per plan, and the literal would
///    also force the scalar stage to stay unconsumed.
/// 3. **Each channel arm** (the Q70 `agg_pipeline_with_in_producer` pattern) — a scan export of
///    the group columns, aggregate arguments, and `xx_item_sk`, hash-shuffled by that key
///    (replicated-only arms: one `Forward` task, or multi-worker fanned-out slices as above); a
///    per-partition semi join against the key stream feeding the partial aggregate (co-location
///    makes the `IN` globally exact, three-valued logic included: a NULL key never matches on
///    either side, and an unmatched non-NULL key is FALSE-or-NULL — filtered either way); then a
///    **gathered** recombine that completes the groups and applies the HAVING against the
///    co-located one-row scalar (a per-partition HAVING would read an empty scalar everywhere
///    but partition 0). The arm output is the exact union-arm row stream, named as the outer
///    recombine's `g{j}`/`a{i}` partial schema.
/// 4. **The outer ROLLUP** gathers the three tiny arm streams (grouping-set levels span keys, so
///    only the partition-0 gather is exact — the KAN-49d argument) and rebuilds every level.
///    `HAVING COUNT(*) > 0 OR EXISTS (<scalar stream>)` suppresses the synthetic grand-total row
///    on the empty partitions while keeping it on partition 0 even when every arm is empty —
///    single-node ROLLUP emits that row over an empty input.
///
/// Declines (`Ok(None)`) for anything outside this exact family: a non-grouping-set top, arms
/// without exactly one plain-column `IN` plus one scalar-compare HAVING, fingerprint or
/// volatility mismatches between the inlined subquery copies, any component scanning more than
/// one sharded base table or scanning its sharded table more than once (subqueries included;
/// KAN-161 admits arms/legs with at most one sharded table each), non-raw
/// INTERSECT arms, a semi join whose keys are not the full-row match of the DISTINCT-INTERSECT
/// lowering, or a scalar that is not a single global min/max/sum/count/avg over a raw UNION ALL.
pub(crate) fn try_rollup_union_derived_subqueries(
    lp: &LogicalPlan,
    replicated: &[&str],
) -> Result<Option<DistributedQuery>> {
    Ok(build_rollup_union_derived(lp, replicated))
}

/// One matched channel arm of the Q14 shape (see
/// [`try_rollup_union_derived_subqueries`]).
struct Q14ChannelArm<'a> {
    /// The arm's peeled projection + aggregate (the projection is the union arm's select list).
    peeled: Peeled<'a>,
    /// Classified arm aggregates (all non-DISTINCT sum/count/min/max).
    aggs: Vec<AggSpec>,
    /// Arm aggregate index the HAVING compares against the scalar.
    having_agg: usize,
    /// HAVING comparison operator, already mirrored so the aggregate is on the left.
    having_op: Operator,
    /// The uncorrelated scalar subquery plan (this arm's inlined `avg_sales` copy).
    having_subquery: &'a LogicalPlan,
    /// The plain-column outer key of the arm's IN predicate (`xx_item_sk`).
    in_outer: &'a Expr,
    /// The IN subquery plan (this arm's inlined `cross_items` copy).
    in_subquery: &'a LogicalPlan,
    /// Subquery-free WHERE conjuncts of the arm body.
    regular: Vec<&'a Expr>,
    /// The arm body below its filter chain.
    body: &'a LogicalPlan,
    /// Whether the body scans the sharded fact (exactly once, checked at match time).
    sharded: bool,
}

fn build_rollup_union_derived(lp: &LogicalPlan, replicated: &[&str]) -> Option<DistributedQuery> {
    let up = Unparser::default();

    // --- Top: a grouping-set aggregate over a UNION ALL ----------------------
    let p = peel(lp).ok()?;
    if !is_grouping_set(&p.agg.group_expr)
        || !p.having.is_empty()
        || !p.alias_projections.is_empty()
    {
        return None;
    }
    let outer_group_exprs = flattened_group_exprs(&p.agg.group_expr);
    let mut outer_group_names: Vec<String> = Vec::with_capacity(outer_group_exprs.len());
    for g in &outer_group_exprs {
        let Expr::Column(c) = unqualify(g) else {
            return None;
        };
        outer_group_names.push(c.name);
    }
    // The outer aggregates must re-add exact per-arm values: SUM over a plain union column only.
    let mut outer_specs: Vec<AggSpec> = Vec::with_capacity(p.agg.aggr_expr.len());
    let mut outer_arg_names: Vec<String> = Vec::with_capacity(p.agg.aggr_expr.len());
    for e in &p.agg.aggr_expr {
        let spec = AggSpec::classify(e).ok()?;
        if spec.distinct || spec.func != "sum" {
            return None;
        }
        let Expr::AggregateFunction(af) = strip_alias(e) else {
            return None;
        };
        let [arg] = af.params.args.as_slice() else {
            return None;
        };
        let Expr::Column(c) = unqualify(arg) else {
            return None;
        };
        outer_arg_names.push(c.name);
        outer_specs.push(spec);
    }

    let LogicalPlan::Union(u) = strip_aliases(p.agg.input.as_ref()) else {
        return None;
    };
    let mut arm_plans: Vec<Arc<LogicalPlan>> = Vec::new();
    for input in &u.inputs {
        flatten_union_all(input, &mut arm_plans);
    }
    if arm_plans.len() < 2 {
        return None;
    }
    let union_fields: Vec<String> = u.schema.fields().iter().map(|f| f.name().clone()).collect();
    // The outer group keys + summed columns must cover the union row exactly (each arm's
    // projection maps positionally onto it below).
    if outer_group_names.len() + outer_arg_names.len() != union_fields.len() {
        return None;
    }
    let union_pos = |name: &str| union_fields.iter().position(|f| f == name);
    let mut outer_group_pos: Vec<usize> = Vec::with_capacity(outer_group_names.len());
    for n in &outer_group_names {
        outer_group_pos.push(union_pos(n)?);
    }
    let mut outer_arg_pos: Vec<usize> = Vec::with_capacity(outer_arg_names.len());
    for n in &outer_arg_names {
        outer_arg_pos.push(union_pos(n)?);
    }

    // --- At least one sharded base table across the whole plan ---------------
    // KAN-161: several facts may be sharded at once (catalog_sales/web_sales above the
    // auto-broadcast threshold). Admission is per arm below: each arm of the key set /
    // scalar / channel union may scan at most ONE sharded table, exactly once, in a
    // broadcast-safe tree — the per-stage exactness arguments (full-row hash co-location,
    // associative partials) do not depend on which channel is sharded.
    let mut all_tables = base_tables(lp);
    collect_subquery_tables(lp, &mut all_tables);
    let mut sharded: Vec<&str> = all_tables
        .iter()
        .map(String::as_str)
        .filter(|t| !replicated.contains(t))
        .collect();
    sharded.sort_unstable();
    sharded.dedup();
    if sharded.is_empty() {
        return None;
    }

    // --- Per-arm shape -------------------------------------------------------
    let mut arms: Vec<Q14ChannelArm> = Vec::with_capacity(arm_plans.len());
    for arm in &arm_plans {
        let a = match_q14_channel_arm(arm, replicated)?;
        if a.peeled.projection.map_or(0, |proj| proj.len()) != union_fields.len() {
            return None;
        }
        arms.push(a);
    }
    if !arms.iter().any(|a| a.sharded) {
        return None;
    }

    // The inlined derived-table copies must be structurally identical across arms (plan once,
    // feed every arm) and deterministic (a volatile copy would re-evaluate per reference
    // single-node, so deduplicating it would change results).
    let in_fp = plan_fingerprint(arms[0].in_subquery);
    let having_fp = plan_fingerprint(arms[0].having_subquery);
    if arms.iter().any(|a| {
        plan_fingerprint(a.in_subquery) != in_fp || plan_fingerprint(a.having_subquery) != having_fp
    }) {
        return None;
    }
    if plan_contains_volatile(arms[0].in_subquery)
        || plan_contains_volatile(arms[0].having_subquery)
    {
        return None;
    }

    // --- Stages: the two derived tables first (topological order) ------------
    let mut stages: Vec<StageDef> = Vec::new();
    let mut next_id = 0u32;
    let (key_stage, key_name) =
        plan_q14_intersect_key_set(arms[0].in_subquery, replicated, &mut stages, &mut next_id)?;
    let scalar_stage = plan_q14_global_scalar(
        arms[0].having_subquery,
        replicated,
        &mut stages,
        &mut next_id,
    )?;

    // --- Per channel arm: export -> co-located semi + partial -> gathered recombine --------
    let mut arm_out_ids: Vec<u32> = Vec::with_capacity(arms.len());
    for a in &arms {
        let ap = &a.peeled;
        let arm_remap = build_agg_remap(ap.agg);
        let n_group = ap.agg.group_expr.len();

        // Scan export: group columns, aggregate arguments, and the IN key — hashed by the key.
        let mut export_cols: Vec<String> = Vec::new();
        for (j, g) in ap.agg.group_expr.iter().enumerate() {
            export_cols.push(format!("{} AS gc{j}", expr_sql(&up, g).ok()?));
        }
        for (i, spec) in a.aggs.iter().enumerate() {
            export_cols.push(format!("{} AS aa{i}", spec.arg_sql));
        }
        export_cols.push(format!("{} AS j0", expr_sql(&up, a.in_outer).ok()?));
        let j0_idx = (export_cols.len() - 1) as u32;
        let body_sql = up
            .plan_to_sql(a.body)
            .map_err(|e| unsupported(format!("unparse q14 arm body: {e}")))
            .ok()?
            .to_string();
        let tail = sanitize_generated_sql(&extract_from_tail(&body_sql).ok()?);
        let where_sql = if a.regular.is_empty() {
            String::new()
        } else {
            let parts = a
                .regular
                .iter()
                .map(|pr| expr_sql(&up, pr))
                .collect::<Result<Vec<_>>>()
                .ok()?;
            format!(" WHERE {}", parts.join(" AND "))
        };
        let export_sql = sanitize_generated_sql(&format!(
            "SELECT {} {tail}{where_sql}",
            export_cols.join(", ")
        ));
        let export_id = next_id;
        next_id += 1;
        let mut export_stage = StageDef::new(export_id, export_sql, vec![], vec![j0_idx]);
        if !a.sharded {
            // A replicated-only arm is identical on every worker: fan the scan out when a safe
            // slice anchor exists (KAN-156 — each worker exports a disjoint file slice and the
            // hash shuffle on j0 still co-locates equal IN keys, so the co-located semi below
            // stays exact); otherwise produce it once (the driver's Forward rule) instead of
            // multiplying it by the worker count.
            place_replicated_stage(&mut export_stage, &[a.body], replicated);
        }
        stages.push(export_stage);

        // Co-located semi + partial aggregate (gathers: the recombine must see the one-row
        // scalar, which lands on partition 0 only).
        let mut psel: Vec<String> = (0..n_group).map(|j| format!("gc{j} AS b{j}")).collect();
        for (i, spec) in a.aggs.iter().enumerate() {
            psel.push(q14_partial(&spec.func, i)?);
        }
        let group_by = (0..n_group)
            .map(|j| format!("gc{j}"))
            .collect::<Vec<_>>()
            .join(", ");
        let semi_sql = sanitize_generated_sql(&format!(
            "SELECT {} FROM (SELECT * FROM shuffle_input_0 WHERE j0 IN \
             (SELECT {key_name} FROM shuffle_input_1)) AS semi_in GROUP BY {group_by}",
            psel.join(", ")
        ));
        let semi_id = next_id;
        next_id += 1;
        stages.push(StageDef::new(
            semi_id,
            semi_sql,
            vec![export_id, key_stage],
            vec![],
        ));

        // Gathered recombine + HAVING against the co-located scalar row; emits the outer
        // partial schema (`g{j}` group columns, `a{i}` complete arm values).
        let mut sel: Vec<String> = Vec::new();
        for (j, &pos) in outer_group_pos.iter().enumerate() {
            let e = strip_alias(&ap.projection?[pos]);
            match e {
                Expr::Literal(..) => sel.push(format!("{} AS g{j}", expr_sql(&up, e).ok()?)),
                Expr::Column(c) => {
                    let name = arm_remap
                        .get(&c.flat_name())
                        .or_else(|| arm_remap.get(&c.name))?;
                    let t = q14_remap_index(name, 'g')?;
                    sel.push(format!("b{t} AS g{j}"));
                }
                _ => return None,
            }
        }
        for (i, &pos) in outer_arg_pos.iter().enumerate() {
            let t = q14_arm_agg_index(strip_alias(&ap.projection?[pos]), ap.agg, &arm_remap)?;
            sel.push(format!("{} AS a{i}", q14_combine(&a.aggs[t].func, t)?));
        }
        let arm_group_by = (0..n_group)
            .map(|t| format!("b{t}"))
            .collect::<Vec<_>>()
            .join(", ");
        let having_sql = format!(
            "{} {} (SELECT m0 FROM shuffle_input_1)",
            q14_combine(&a.aggs[a.having_agg].func, a.having_agg)?,
            q14_op_sql(a.having_op)?
        );
        let recombine_sql = sanitize_generated_sql(&format!(
            "SELECT {} FROM shuffle_input_0 GROUP BY {arm_group_by} HAVING {having_sql}",
            sel.join(", ")
        ));
        let recombine_id = next_id;
        next_id += 1;
        stages.push(StageDef::new(
            recombine_id,
            recombine_sql,
            vec![semi_id, scalar_stage],
            vec![],
        ));
        arm_out_ids.push(recombine_id);
    }

    // --- Outer grouping-set recombine over the exact arm streams -------------
    let n_group = outer_group_names.len();
    let g_names: Vec<String> = (0..n_group).map(|j| format!("g{j}")).collect();
    let (_psel, combine) = partial_and_combine_lists(&g_names, &outer_specs).ok()?;
    let final_group_by = final_group_by_sql(&p.agg.group_expr, n_group).ok()?;
    let n_arms = arm_out_ids.len();
    let arm_union = (0..n_arms)
        .map(|i| format!("SELECT * FROM shuffle_input_{i}"))
        .collect::<Vec<_>>()
        .join(" UNION ALL ");
    // Partition-0 gate: the gathered scalar stream exists only there, so empty partitions
    // suppress their synthetic ROLLUP grand-total row while partition 0 keeps its own — even
    // when every arm is empty (single-node ROLLUP emits the grand total over an empty input).
    let gate_pos = n_arms;
    let inner = format!(
        "SELECT {} FROM ({arm_union}) AS merged_arms GROUP BY {final_group_by} \
         HAVING COUNT(*) > 0 OR EXISTS (SELECT * FROM shuffle_input_{gate_pos})",
        combine.join(", ")
    );
    let final_sql =
        sanitize_generated_sql(&wrap_output_recombine(&p, &inner, &build_remap(&p)).ok()?);
    let mut upstreams = arm_out_ids.clone();
    upstreams.push(scalar_stage);
    stages.push(StageDef::new(next_id, final_sql, upstreams, vec![]));

    Some(DistributedQuery {
        stages,
        finalize_sql: build_finalize(&p).ok()?,
    })
}

/// Match one Q14 channel arm: `Projection -> Filter(HAVING <agg> <cmp> <scalar>) -> Aggregate
/// -> Filter(<col> IN (<key set>) AND <regular…>) -> raw join/scan body`.
fn match_q14_channel_arm<'a>(
    arm: &'a LogicalPlan,
    replicated: &[&str],
) -> Option<Q14ChannelArm<'a>> {
    let peeled = peel(arm).ok()?;
    if peeled.sort.is_some() || !peeled.alias_projections.is_empty() {
        return None;
    }
    peeled.projection?;
    if peeled.agg.group_expr.is_empty() || is_grouping_set(&peeled.agg.group_expr) {
        return None;
    }
    for g in &peeled.agg.group_expr {
        if !matches!(strip_alias(g), Expr::Column(_)) {
            return None;
        }
    }
    let aggs = peeled
        .agg
        .aggr_expr
        .iter()
        .map(AggSpec::classify)
        .collect::<Result<Vec<_>>>()
        .ok()?;
    if aggs
        .iter()
        .any(|a| a.distinct || !matches!(a.func.as_str(), "sum" | "count" | "min" | "max"))
    {
        return None;
    }
    let remap = build_agg_remap(peeled.agg);

    // HAVING: exactly one conjunct, `<arm aggregate> <cmp> <uncorrelated scalar subquery>`.
    let mut having_conjuncts: Vec<&Expr> = Vec::new();
    for h in &peeled.having {
        flatten_conjuncts(h, &mut having_conjuncts);
    }
    let [conjunct] = having_conjuncts.as_slice() else {
        return None;
    };
    let Expr::BinaryExpr(b) = conjunct else {
        return None;
    };
    let (compare, subquery, mirrored) = match (b.left.as_ref(), b.right.as_ref()) {
        (Expr::ScalarSubquery(s), other) => (other, s.subquery.as_ref(), true),
        (other, Expr::ScalarSubquery(s)) => (other, s.subquery.as_ref(), false),
        _ => return None,
    };
    let having_op = if mirrored { q14_mirror_op(b.op)? } else { b.op };
    q14_op_sql(having_op)?;
    if expr_contains_subquery(compare) || plan_contains_outer_reference(subquery) {
        return None;
    }
    let having_agg = q14_arm_agg_index(compare, peeled.agg, &remap)?;

    // WHERE: exactly one non-negated `<plain column> IN (<uncorrelated subquery>)`; every other
    // conjunct subquery-free.
    let mut conjuncts: Vec<&Expr> = Vec::new();
    let mut body = peeled.agg.input.as_ref();
    while let LogicalPlan::Filter(f) = body {
        flatten_conjuncts(&f.predicate, &mut conjuncts);
        body = f.input.as_ref();
    }
    let mut in_split: Option<(&Expr, &LogicalPlan)> = None;
    let mut regular: Vec<&Expr> = Vec::new();
    for c in &conjuncts {
        if let Expr::InSubquery(iq) = c {
            if iq.negated || in_split.is_some() || expr_contains_subquery(&iq.expr) {
                return None;
            }
            if plan_contains_outer_reference(&iq.subquery.subquery) {
                return None;
            }
            if !matches!(iq.expr.as_ref(), Expr::Column(_)) {
                return None;
            }
            in_split = Some((iq.expr.as_ref(), iq.subquery.subquery.as_ref()));
            continue;
        }
        if expr_contains_subquery(c) {
            return None;
        }
        regular.push(*c);
    }
    let (in_outer, in_subquery) = in_split?;
    if !q14_raw_row_source(body) {
        return None;
    }
    let body_tables = base_tables(body);
    let mut body_sharded: Vec<&str> = body_tables
        .iter()
        .map(String::as_str)
        .filter(|t| !replicated.contains(t))
        .collect();
    body_sharded.sort_unstable();
    body_sharded.dedup();
    let sharded = match body_sharded.as_slice() {
        [] => false,
        [t] => {
            if count_table_scans(body, t) != 1 {
                return None;
            }
            reject_unsafe_broadcast_shapes(body, t).ok()?;
            true
        }
        _ => return None,
    };
    Some(Q14ChannelArm {
        peeled,
        aggs,
        having_agg,
        having_op,
        having_subquery: subquery,
        in_outer,
        in_subquery,
        regular,
        body,
        sharded,
    })
}

/// A raw scan/join tree: no aggregates, windows, set ops, or expression subqueries — safe to
/// evaluate per worker against the local shard plus the replicated dimensions.
fn q14_raw_row_source(lp: &LogicalPlan) -> bool {
    if lp.expressions().iter().any(expr_contains_subquery) {
        return false;
    }
    match lp {
        LogicalPlan::TableScan(_) => true,
        LogicalPlan::Projection(_) | LogicalPlan::Filter(_) | LogicalPlan::SubqueryAlias(_) => {
            lp.inputs().iter().all(|i| q14_raw_row_source(i))
        }
        LogicalPlan::Join(j) => {
            j.join_type == JoinType::Inner && lp.inputs().iter().all(|i| q14_raw_row_source(i))
        }
        _ => false,
    }
}

/// KAN-156: choose the placement for a stage computing a replicated-only row source or partial.
/// On a multi-worker cluster with a safe slice anchor per arm, the stage keeps the default
/// worker-indexed exchange and runs on EVERY worker, each scanning a disjoint 1/W file slice of
/// the anchor tables (the reduced replicate stamp makes the workers' file sharder treat them as
/// sharded for this stage only); otherwise the stage computes exactly once
/// (`ExchangeMode::Forward` — one task on worker 0). Slicing is all-or-nothing across `arms`:
/// an arm without a safe anchor would be scanned in full on every worker and its contribution
/// would multiply by the worker count. Exactness for the sliced placement is the caller's
/// argument, and it is always the same shape: the stage's output is a row-level stream or a
/// recombine-safe partial (INTERSECT legs dedup after hash co-location, AVG legs combine
/// sum/count, arm GROUP BYs are partial aggregates), so the disjoint per-slice outputs merge
/// associatively downstream — byte-identical semantics to the single-task `Forward` version.
fn place_replicated_stage(stage: &mut StageDef, arms: &[&LogicalPlan], replicated: &[&str]) {
    let mut anchors: Vec<String> = Vec::new();
    for arm in arms {
        match replicated_slice_tables(arm) {
            Some(mut a) => anchors.append(&mut a),
            None => {
                stage.exchange = ExchangeMode::Forward;
                return;
            }
        }
    }
    let kept: Vec<&str> = replicated
        .iter()
        .filter(|t| !anchors.iter().any(|s| s.eq_ignore_ascii_case(t)))
        .copied()
        .collect();
    // An empty stamp reads as "unset" and would be refilled with the full replicate list,
    // silently un-slicing the anchors — keep `Forward` for that degenerate case.
    if kept.is_empty() {
        stage.exchange = ExchangeMode::Forward;
        return;
    }
    stage.replicated_tables = kept.join(",");
}

/// Plan the `cross_items` key set: the INTERSECT chain as full-row key-shuffles plus the `item`
/// join-back, emitting the one-column key stream hash-shuffled on that key. Returns the key
/// stream's terminal stage id and its key column name.
fn plan_q14_intersect_key_set(
    sub: &LogicalPlan,
    replicated: &[&str],
    stages: &mut Vec<StageDef>,
    next_id: &mut u32,
) -> Option<(u32, String)> {
    // One output column: the IN key.
    let fields = sub.schema().fields();
    if fields.len() != 1 {
        return None;
    }
    let key_name = fields[0].name().clone();

    // Descend single-column passthrough/rename projections and aliases to the join region.
    let mut node = sub;
    loop {
        let stripped = strip_aliases(node);
        if !std::ptr::eq(stripped, node) {
            node = stripped;
            continue;
        }
        match node {
            LogicalPlan::Projection(pj)
                if pj.expr.len() == 1 && matches!(strip_alias(&pj.expr[0]), Expr::Column(_)) =>
            {
                node = pj.input.as_ref();
            }
            _ => break,
        }
    }
    // The join region: optional (subquery-free) equijoin filters over a cross-join tree whose
    // leaves are replicated raw scans plus exactly one INTERSECT semi chain.
    let mut conjuncts: Vec<&Expr> = Vec::new();
    let mut body = node;
    while let LogicalPlan::Filter(f) = body {
        flatten_conjuncts(&f.predicate, &mut conjuncts);
        body = f.input.as_ref();
    }
    if conjuncts.iter().any(|c| expr_contains_subquery(c)) {
        return None;
    }
    let mut leaves: Vec<&LogicalPlan> = Vec::new();
    collect_cross_leaves(body, &mut leaves)?;
    let mut chain: Option<&LogicalPlan> = None;
    for leaf in leaves {
        if matches!(strip_aliases(leaf), LogicalPlan::Join(j) if j.join_type == JoinType::LeftSemi)
        {
            if chain.is_some() {
                return None;
            }
            chain = Some(leaf);
            continue;
        }
        if !q14_raw_row_source(leaf)
            || base_tables(leaf)
                .iter()
                .any(|t| !replicated.contains(&t.as_str()))
        {
            return None;
        }
    }
    let chain = chain?;

    // Decompose the semi chain into its raw INTERSECT arms (leftmost first); every join must be
    // the full-row semi match of the DISTINCT-INTERSECT lowering.
    let mut set_arms: Vec<&LogicalPlan> = Vec::new();
    q14_intersect_arms(strip_aliases(chain), &mut set_arms)?;
    if set_arms.len() < 2 {
        return None;
    }
    let n_cols = set_arms[0].schema().fields().len();
    if n_cols == 0 {
        return None;
    }
    let hash_key: Vec<u32> = (0..n_cols as u32).collect();
    let mut sharded_arms = 0usize;
    let mut arm_stage_ids: Vec<u32> = Vec::with_capacity(set_arms.len());
    for arm in set_arms {
        if !q14_raw_row_source(arm) || arm.schema().fields().len() != n_cols {
            return None;
        }
        // KAN-161: each arm may scan at most one sharded table, exactly once (per-channel
        // facts shard independently — the full-row hash co-location makes the per-partition
        // INTERSECT exact no matter which arms are sharded).
        let arm_tables = base_tables(arm);
        let mut arm_sharded: Vec<&str> = arm_tables
            .iter()
            .map(String::as_str)
            .filter(|t| !replicated.contains(t))
            .collect();
        arm_sharded.sort_unstable();
        arm_sharded.dedup();
        let scans = match arm_sharded.as_slice() {
            [] => 0,
            [t] => {
                let n = count_table_scans(arm, t);
                if n != 1 {
                    return None;
                }
                reject_unsafe_broadcast_shapes(arm, t).ok()?;
                sharded_arms += 1;
                n
            }
            _ => return None,
        };
        let sql = plan_sql(arm, "q14 intersect arm").ok()?;
        let mut stage = StageDef::new(*next_id, sql, vec![], hash_key.clone());
        if scans == 0 {
            // Replicated arm: fan the scan out across workers when a safe slice anchor exists
            // (each worker scans a disjoint file slice and hash-shuffles by the full row, so
            // duplicates still co-locate and the per-partition INTERSECT stays exact);
            // otherwise produce once (Forward) instead of once per worker.
            place_replicated_stage(&mut stage, &[arm], replicated);
        }
        arm_stage_ids.push(*next_id);
        *next_id += 1;
        stages.push(stage);
    }
    if sharded_arms == 0 {
        return None;
    }

    // The set op: the full-row hash co-locates equal triples, so the per-partition INTERSECT is
    // globally exact (dedup included — duplicates hash to the same partition).
    let set_sql = (0..arm_stage_ids.len())
        .map(|i| format!("SELECT * FROM shuffle_input_{i}"))
        .collect::<Vec<_>>()
        .join(" INTERSECT ");
    let set_id = *next_id;
    *next_id += 1;
    stages.push(StageDef::new(set_id, set_sql, arm_stage_ids, hash_key));

    // The join-back: the key-set plan with the semi chain replaced by the co-located triple
    // stream (the `item` side is replicated — every partition joins its own slice locally).
    let branch_by_node = HashMap::from([(node_id(chain), 0usize)]);
    let (rewritten, changed) = replace_branches(sub, &branch_by_node, 1).ok()?;
    if !changed {
        return None;
    }
    let sql = plan_sql(&rewritten, "q14 key-set join-back").ok()?;
    let out_id = *next_id;
    *next_id += 1;
    stages.push(StageDef::new(out_id, sql, vec![set_id], vec![0]));
    Some((out_id, key_name))
}

/// Decompose a `LeftSemi(Distinct(left), right)` chain — DataFusion's DISTINCT-INTERSECT
/// lowering — into its raw arms, leftmost first. `None` when any join is not exactly the
/// full-row semi match of that lowering (rebuilding it as `INTERSECT` SQL would change
/// semantics).
fn q14_intersect_arms<'a>(node: &'a LogicalPlan, out: &mut Vec<&'a LogicalPlan>) -> Option<()> {
    let LogicalPlan::Join(j) = node else {
        out.push(node);
        return Some(());
    };
    if j.join_type != JoinType::LeftSemi || j.filter.is_some() {
        return None;
    }
    let LogicalPlan::Distinct(d) = j.left.as_ref() else {
        return None;
    };
    if !q14_semi_keys_cover_full_row(j) {
        return None;
    }
    q14_intersect_arms(strip_aliases(d.input()), out)?;
    out.push(strip_aliases(j.right.as_ref()));
    Some(())
}

/// Every join key is a plain column; both sides' keys cover their full output rows exactly once
/// (the DISTINCT-INTERSECT lowering fingerprint — a semi match on the whole row).
fn q14_semi_keys_cover_full_row(j: &datafusion::logical_expr::Join) -> bool {
    let left_names: HashSet<&str> = j
        .left
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    let right_names: HashSet<&str> = j
        .right
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    if left_names.len() != right_names.len() || j.on.len() != left_names.len() {
        return false;
    }
    let mut l_used = HashSet::new();
    let mut r_used = HashSet::new();
    for (le, re) in &j.on {
        let (Expr::Column(lc), Expr::Column(rc)) = (le, re) else {
            return false;
        };
        if !left_names.contains(lc.name.as_str()) || !right_names.contains(rc.name.as_str()) {
            return false;
        }
        if !l_used.insert(&lc.name) || !r_used.insert(&rc.name) {
            return false;
        }
    }
    true
}

/// Plan the HAVING scalar (Q14's `avg_sales`): one global non-DISTINCT min/max/sum/count/avg
/// over a raw UNION ALL of channel projections, each scanning at most one sharded table
/// (exactly once), with at least one sharded arm overall. The mixed-sharding global decomposes
/// as one per-worker partial per sharded arm plus one partial over the replicated arms (when
/// any), then a one-row combine gathered to partition 0.
/// Returns the combine stage id; consumers read it as `(SELECT m0 FROM shuffle_input_N)` where
/// their own main input also gathers (partition-0 co-location).
fn plan_q14_global_scalar(
    sub: &LogicalPlan,
    replicated: &[&str],
    stages: &mut Vec<StageDef>,
    next_id: &mut u32,
) -> Option<u32> {
    // Strip aliases and single-expression passthrough projections down to the global aggregate.
    let mut node = sub;
    loop {
        let stripped = strip_aliases(node);
        if !std::ptr::eq(stripped, node) {
            node = stripped;
            continue;
        }
        match node {
            LogicalPlan::Projection(pj) if pj.expr.len() == 1 => {
                if !matches!(
                    strip_alias(&pj.expr[0]),
                    Expr::Column(_) | Expr::AggregateFunction(_)
                ) {
                    return None;
                }
                node = pj.input.as_ref();
            }
            _ => break,
        }
    }
    let LogicalPlan::Aggregate(agg) = node else {
        return None;
    };
    if !agg.group_expr.is_empty() || agg.aggr_expr.len() != 1 {
        return None;
    }
    let spec = AggSpec::classify(&agg.aggr_expr[0]).ok()?;
    if spec.distinct || !matches!(spec.func.as_str(), "min" | "max" | "sum" | "count" | "avg") {
        return None;
    }

    let LogicalPlan::Union(u) = strip_aliases(agg.input.as_ref()) else {
        return None;
    };
    let mut scalar_arms: Vec<Arc<LogicalPlan>> = Vec::new();
    for input in &u.inputs {
        flatten_union_all(input, &mut scalar_arms);
    }
    if scalar_arms.len() < 2 {
        return None;
    }
    let union_fields: HashSet<&str> = u
        .schema
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();

    // The aggregate argument must read union output columns only.
    let up = Unparser::default();
    let arg = match strip_alias(&agg.aggr_expr[0]) {
        Expr::AggregateFunction(af) if af.params.args.len() == 1 => unqualify(&af.params.args[0]),
        Expr::AggregateFunction(af) if af.params.args.is_empty() && spec.func == "count" => {
            Expr::Literal(ScalarValue::Int64(Some(1)), None)
        }
        _ => return None,
    };
    let mut arg_cols = Vec::new();
    expr_columns(&arg, &mut arg_cols);
    if !arg_cols
        .iter()
        .all(|c| union_fields.contains(c.name.as_str()))
    {
        return None;
    }
    let arg_sql = expr_sql(&up, &arg).ok()?;
    let (partial_sels, _comb) = partial_combine_sql(&spec.func, 0, &arg_sql).ok()?;
    let comb_expr = match spec.func.as_str() {
        "sum" | "count" => "sum(a0)".to_string(),
        "min" => "min(a0)".to_string(),
        "max" => "max(a0)".to_string(),
        "avg" => "(sum(a0s) / NULLIF(sum(a0c), 0))".to_string(),
        _ => return None,
    };

    let mut sharded_sqls: Vec<String> = Vec::new();
    let mut repl_sqls: Vec<String> = Vec::new();
    let mut repl_arms: Vec<&LogicalPlan> = Vec::new();
    for arm in &scalar_arms {
        if !q14_raw_row_source(arm) {
            return None;
        }
        // KAN-161: per arm at most one sharded table, scanned exactly once.
        let arm_tables = base_tables(arm);
        let mut arm_sharded: Vec<&str> = arm_tables
            .iter()
            .map(String::as_str)
            .filter(|t| !replicated.contains(t))
            .collect();
        arm_sharded.sort_unstable();
        arm_sharded.dedup();
        let sharded_arm = match arm_sharded.as_slice() {
            [] => false,
            [t] => {
                if count_table_scans(arm, t) != 1 {
                    return None;
                }
                reject_unsafe_broadcast_shapes(arm, t).ok()?;
                true
            }
            _ => return None,
        };
        let sql = plan_sql(arm, "q14 scalar arm").ok()?;
        if sharded_arm {
            sharded_sqls.push(sql);
        } else {
            repl_sqls.push(sql);
            repl_arms.push(arm.as_ref());
        }
    }
    if sharded_sqls.is_empty() {
        return None;
    }

    // Per-worker partial over each sharded arm (global partial: exactly one row per worker,
    // even over an empty shard — so the combine's one row is the exact single-node value,
    // NULLs and empty-input included), gathered; when replicated arms exist, one Forward
    // partial over them — or, when every replicated arm has a safe slice anchor, a per-worker
    // partial over disjoint file slices of the anchors (KAN-156): each task still emits its
    // one global-partial row (empty slices included), and the per-slice partials re-add in the
    // unchanged combine exactly like the sharded side's per-worker partials.
    let psel = partial_sels.join(", ");
    let mut partial_ids: Vec<u32> = Vec::with_capacity(sharded_sqls.len() + 1);
    for sql in &sharded_sqls {
        let id = *next_id;
        *next_id += 1;
        stages.push(StageDef::new(
            id,
            sanitize_generated_sql(&format!("SELECT {psel} FROM ({sql}) AS sq2")),
            vec![],
            vec![],
        ));
        partial_ids.push(id);
    }
    if !repl_sqls.is_empty() {
        let repl_union = repl_sqls.join(" UNION ALL ");
        let repl_id = *next_id;
        *next_id += 1;
        let mut repl_stage = StageDef::new(
            repl_id,
            sanitize_generated_sql(&format!("SELECT {psel} FROM ({repl_union}) AS sq2")),
            vec![],
            vec![],
        );
        place_replicated_stage(&mut repl_stage, &repl_arms, replicated);
        stages.push(repl_stage);
        partial_ids.push(repl_id);
    }
    let comb_id = *next_id;
    *next_id += 1;
    // `HAVING COUNT(*) > 0`: the empty partitions (gathered inputs land on partition 0 only)
    // would still emit the global aggregate's identity row; partition 0's input always carries
    // the per-worker / Forward partial rows, so its one row survives.
    let comb_inputs = (0..partial_ids.len())
        .map(|i| format!("SELECT * FROM shuffle_input_{i}"))
        .collect::<Vec<_>>()
        .join(" UNION ALL ");
    stages.push(StageDef::new(
        comb_id,
        sanitize_generated_sql(&format!(
            "SELECT {comb_expr} AS m0 FROM ({comb_inputs}) AS avg_in HAVING COUNT(*) > 0"
        )),
        partial_ids,
        vec![],
    ));
    Some(comb_id)
}

/// The arm-aggregate partial in the Q14 semi+partial stage (`c{i}` over the exported `aa{i}`).
fn q14_partial(func: &str, i: usize) -> Option<String> {
    Some(match func {
        "sum" => format!("sum(aa{i}) AS c{i}"),
        "count" => format!("count(aa{i}) AS c{i}"),
        "min" => format!("min(aa{i}) AS c{i}"),
        "max" => format!("max(aa{i}) AS c{i}"),
        _ => return None,
    })
}

/// The arm-aggregate recombine over the `c{t}` partials (counts re-add by summing).
fn q14_combine(func: &str, t: usize) -> Option<String> {
    Some(match func {
        "sum" | "count" => format!("sum(c{t})"),
        "min" => format!("min(c{t})"),
        "max" => format!("max(c{t})"),
        _ => return None,
    })
}

/// SQL text for a comparison operator (`<>` for NotEq — safe in every worker dialect).
fn q14_op_sql(op: Operator) -> Option<&'static str> {
    Some(match op {
        Operator::Eq => "=",
        Operator::NotEq => "<>",
        Operator::Lt => "<",
        Operator::LtEq => "<=",
        Operator::Gt => ">",
        Operator::GtEq => ">=",
        _ => return None,
    })
}

/// The operator with its sides swapped (a subquery on the left of the comparison).
fn q14_mirror_op(op: Operator) -> Option<Operator> {
    Some(match op {
        Operator::Eq => Operator::Eq,
        Operator::NotEq => Operator::NotEq,
        Operator::Lt => Operator::Gt,
        Operator::LtEq => Operator::GtEq,
        Operator::Gt => Operator::Lt,
        Operator::GtEq => Operator::LtEq,
        _ => return None,
    })
}

/// Parse a `g{t}` / `r{t}` remap target back to its index.
fn q14_remap_index(name: &str, prefix: char) -> Option<usize> {
    let rest = name.strip_prefix(prefix)?;
    if rest.is_empty() || !rest.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    rest.parse().ok()
}

/// Which aggregate of `agg` an expression refers to: an exact (alias-stripped) expression match
/// against the aggregate list, or a plain column naming one of its outputs (`r{t}` through the
/// aggregate remap).
fn q14_arm_agg_index(e: &Expr, agg: &Aggregate, remap: &HashMap<String, String>) -> Option<usize> {
    let stripped = strip_alias(e);
    for (i, a) in agg.aggr_expr.iter().enumerate() {
        if stripped == strip_alias(a) {
            return Some(i);
        }
    }
    if let Expr::Column(c) = stripped {
        return q14_remap_index(
            remap.get(&c.flat_name()).or_else(|| remap.get(&c.name))?,
            'r',
        );
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use oxidant_loom::arrow::array::{Int64Array, StringArray};
    use oxidant_loom::arrow::datatypes::{DataType, Field, Schema};
    use oxidant_loom::arrow::record_batch::RecordBatch;
    use oxidant_loom::Engine;

    const Q38: &str = include_str!("../../../../bench/tpcds/queries/q38.sql");
    const Q87: &str = include_str!("../../../../bench/tpcds/queries/q87.sql");
    const Q97: &str = include_str!("../../../../bench/tpcds/queries/q97.sql");
    const Q24: &str = include_str!("../../../../bench/tpcds/queries/q24.sql");

    fn i64f(name: &str) -> Field {
        Field::new(name, DataType::Int64, false)
    }

    fn strf(name: &str) -> Field {
        Field::new(name, DataType::Utf8, false)
    }

    /// An all-Int64 single-row table (plan-shape tests never read the rows, but the tables
    /// must exist for `logical_plan`).
    fn i64_table(cols: &[&str]) -> RecordBatch {
        let schema = Arc::new(Schema::new(
            cols.iter().map(|c| i64f(c)).collect::<Vec<_>>(),
        ));
        let arrays: Vec<oxidant_loom::arrow::array::ArrayRef> = (0..cols.len())
            .map(|_| Arc::new(Int64Array::from(vec![1i64])) as oxidant_loom::arrow::array::ArrayRef)
            .collect();
        RecordBatch::try_new(schema, arrays).unwrap()
    }

    /// Miniature TPC-DS schema covering Q24/Q38/Q87/Q97 (one row per table).
    async fn tpcds_engine() -> Engine {
        let engine = Engine::new();
        engine
            .register_batches(
                "store_sales",
                vec![i64_table(&[
                    "ss_customer_sk",
                    "ss_sold_date_sk",
                    "ss_item_sk",
                    "ss_ticket_number",
                    "ss_store_sk",
                    "ss_net_paid",
                ])],
            )
            .unwrap();
        engine
            .register_batches(
                "catalog_sales",
                vec![i64_table(&[
                    "cs_bill_customer_sk",
                    "cs_sold_date_sk",
                    "cs_item_sk",
                ])],
            )
            .unwrap();
        engine
            .register_batches(
                "web_sales",
                vec![i64_table(&[
                    "ws_bill_customer_sk",
                    "ws_sold_date_sk",
                    "ws_item_sk",
                ])],
            )
            .unwrap();
        engine
            .register_batches(
                "store_returns",
                vec![i64_table(&["sr_ticket_number", "sr_item_sk"])],
            )
            .unwrap();
        engine
            .register_batches(
                "store",
                vec![i64_table(&[
                    "s_store_sk",
                    "s_store_name",
                    "s_state",
                    "s_zip",
                    "s_market_id",
                ])],
            )
            .unwrap();
        engine
            .register_batches(
                "item",
                vec![i64_table(&[
                    "i_item_sk",
                    "i_color",
                    "i_current_price",
                    "i_manager_id",
                    "i_units",
                    "i_size",
                ])],
            )
            .unwrap();
        engine
            .register_batches(
                "customer",
                vec![RecordBatch::try_new(
                    Arc::new(Schema::new(vec![
                        i64f("c_customer_sk"),
                        strf("c_last_name"),
                        strf("c_first_name"),
                        i64f("c_current_addr_sk"),
                        strf("c_birth_country"),
                    ])),
                    vec![
                        Arc::new(Int64Array::from(vec![1i64])),
                        Arc::new(StringArray::from(vec!["smith"])),
                        Arc::new(StringArray::from(vec!["ann"])),
                        Arc::new(Int64Array::from(vec![1i64])),
                        Arc::new(StringArray::from(vec!["US"])),
                    ],
                )
                .unwrap()],
            )
            .unwrap();
        engine
            .register_batches(
                "customer_address",
                vec![RecordBatch::try_new(
                    Arc::new(Schema::new(vec![
                        i64f("ca_address_sk"),
                        strf("ca_state"),
                        strf("ca_country"),
                        strf("ca_zip"),
                    ])),
                    vec![
                        Arc::new(Int64Array::from(vec![1i64])),
                        Arc::new(StringArray::from(vec!["CA"])),
                        Arc::new(StringArray::from(vec!["US"])),
                        Arc::new(StringArray::from(vec!["94000"])),
                    ],
                )
                .unwrap()],
            )
            .unwrap();
        engine
            .register_batches(
                "date_dim",
                vec![i64_table(&["d_date_sk", "d_month_seq", "d_date"])],
            )
            .unwrap();
        engine
    }

    async fn plan(sql: &str) -> LogicalPlan {
        tpcds_engine().await.logical_plan(sql).await.unwrap()
    }

    // ------------------------------------------------------------------
    // Q38 / Q87: global count(*) over an INTERSECT / EXCEPT chain.
    // ------------------------------------------------------------------

    /// The SF10 post-classification configuration for the set-op shapes: `store_sales` is the
    /// sharded fact; the other two channels and the dims replicate.
    const REPL_SET_OP: [&str; 4] = ["date_dim", "customer", "catalog_sales", "web_sales"];

    #[tokio::test]
    async fn q38_global_count_over_intersect_plans_distributed() {
        let lp = plan(Q38).await;
        let dq = try_global_count_over_set_op(&lp, &REPL_SET_OP)
            .unwrap()
            .expect("Q38 must admit the set-op shape");
        // Three branch exports (hash-shuffled on the full 3-column row) + per-partition
        // INTERSECT + global recombine.
        assert_eq!(dq.stages.len(), 5, "{dq:?}");
        for (i, stage) in dq.stages[..3].iter().enumerate() {
            assert_eq!(stage.stage_id, i as u32);
            assert_eq!(
                stage.hash_key_cols,
                vec![0, 1, 2],
                "branch exports shuffle on the full row: {stage:?}"
            );
            assert!(stage.upstream_stage_ids.is_empty());
        }
        assert!(dq.stages[0].sql.contains("store_sales"), "{dq:?}");
        let chain = &dq.stages[3];
        assert_eq!(chain.upstream_stage_ids, vec![0, 1, 2]);
        assert!(
            chain.sql.contains("INTERSECT") && chain.sql.contains("count(*)"),
            "per-partition set-op stage: {}",
            chain.sql
        );
        let global = &dq.stages[4];
        assert_eq!(global.upstream_stage_ids, vec![3]);
        assert!(
            global.sql.contains("sum(a0)"),
            "global recombine sums per-partition counts: {}",
            global.sql
        );
    }

    #[tokio::test]
    async fn q87_global_count_over_except_plans_distributed() {
        let lp = plan(Q87).await;
        let dq = try_global_count_over_set_op(&lp, &REPL_SET_OP)
            .unwrap()
            .expect("Q87 must admit the set-op shape");
        assert_eq!(dq.stages.len(), 5, "{dq:?}");
        assert!(
            dq.stages[3].sql.contains("EXCEPT"),
            "per-partition EXCEPT stage: {}",
            dq.stages[3].sql
        );
    }

    #[tokio::test]
    async fn set_op_shape_declines_without_set_op() {
        let lp = plan("SELECT count(*) FROM store_sales").await;
        assert!(
            try_global_count_over_set_op(&lp, &REPL_SET_OP)
                .unwrap()
                .is_none(),
            "a plain scan aggregate has no semi/anti chain to distribute"
        );
    }

    #[tokio::test]
    async fn set_op_shape_declines_on_distinct_count() {
        let lp = plan(&Q38.replace("count(*)", "count(DISTINCT c_last_name)")).await;
        assert!(
            try_global_count_over_set_op(&lp, &REPL_SET_OP)
                .unwrap()
                .is_none(),
            "count(DISTINCT …) is not recombinable by summing per-partition counts"
        );
    }

    #[tokio::test]
    async fn set_op_shape_declines_on_non_count_aggregate() {
        let lp = plan(&Q38.replace("count(*)", "sum(1)")).await;
        assert!(
            try_global_count_over_set_op(&lp, &REPL_SET_OP)
                .unwrap()
                .is_none(),
            "only count recombines exactly over a set-op chain"
        );
    }

    #[tokio::test]
    async fn set_op_shape_declines_on_grouped_aggregate() {
        let lp = plan(
            "SELECT k, count(*) FROM (\
                 SELECT DISTINCT ss_customer_sk AS k FROM store_sales INTERSECT \
                 SELECT DISTINCT cs_bill_customer_sk AS k FROM catalog_sales) x \
             GROUP BY k",
        )
        .await;
        assert!(
            try_global_count_over_set_op(&lp, &REPL_SET_OP)
                .unwrap()
                .is_none(),
            "grouped aggregates belong to the ordinary aggregation shapes, not this one"
        );
    }

    #[tokio::test]
    async fn set_op_shape_declines_on_two_sharded_tables_in_one_leaf() {
        // One branch scanning two sharded facts cannot be evaluated per-worker.
        let lp = plan(
            "SELECT count(*) FROM (\
                 SELECT DISTINCT ss_customer_sk FROM store_sales \
                 JOIN web_sales ON ss_sold_date_sk = ws_sold_date_sk INTERSECT \
                 SELECT DISTINCT cs_bill_customer_sk FROM catalog_sales) x",
        )
        .await;
        assert!(
            try_global_count_over_set_op(&lp, &["catalog_sales"])
                .unwrap()
                .is_none(),
            "a leaf with two sharded scans is not shard-local-computable"
        );
    }

    // ------------------------------------------------------------------
    // Q97: global aggregates over a FULL OUTER JOIN of distinct-key aggregates.
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn q97_full_outer_global_agg_both_sides_sharded() {
        let lp = plan(Q97).await;
        let dq = try_full_outer_join_global_agg(&lp, &["date_dim"])
            .unwrap()
            .expect("Q97 must admit the full-outer shape");
        // Left partial/combine + right partial/combine + per-partition FULL OUTER JOIN +
        // global recombine.
        assert_eq!(dq.stages.len(), 6, "{dq:?}");
        let join = &dq.stages[4];
        assert_eq!(join.upstream_stage_ids, vec![1, 3], "{dq:?}");
        assert!(
            join.sql.contains("FULL OUTER JOIN"),
            "per-partition join stage: {}",
            join.sql
        );
        let global = &dq.stages[5];
        assert_eq!(global.upstream_stage_ids, vec![4]);
        assert!(
            global.sql.contains("HAVING COUNT(*) > 0"),
            "the empty-input guard must gate the global aggregate: {}",
            global.sql
        );
    }

    #[tokio::test]
    async fn q97_full_outer_global_agg_replicated_side_is_forwarded() {
        // With `catalog_sales` replicated, the right side's distinct keys are computed once
        // (Forward — per-worker evaluation would multiply the key set) and hash-shuffled to
        // co-locate with the sharded side.
        let lp = plan(Q97).await;
        let dq = try_full_outer_join_global_agg(&lp, &["date_dim", "catalog_sales"])
            .unwrap()
            .expect("Q97 with a replicated side must still admit");
        assert_eq!(dq.stages.len(), 5, "{dq:?}");
        let forwards = dq
            .stages
            .iter()
            .filter(|s| s.exchange == ExchangeMode::Forward)
            .count();
        assert_eq!(
            forwards, 1,
            "exactly the fully-replicated side is a Forward stage: {dq:?}"
        );
    }

    #[tokio::test]
    async fn q97_shape_declines_on_inner_join() {
        let lp = plan(&Q97.replace("FULL OUTER JOIN", "JOIN")).await;
        assert!(
            try_full_outer_join_global_agg(&lp, &["date_dim"])
                .unwrap()
                .is_none(),
            "an inner join of the two sides is the broadcast/shuffle-join planner's shape"
        );
    }

    #[tokio::test]
    async fn q97_shape_declines_on_distinct_aggregate() {
        let lp = plan(
            "SELECT count(DISTINCT ssci.customer_sk) FROM \
             (SELECT ss_customer_sk customer_sk, ss_item_sk item_sk FROM store_sales \
              GROUP BY ss_customer_sk, ss_item_sk) ssci \
             FULL OUTER JOIN \
             (SELECT cs_bill_customer_sk customer_sk, cs_item_sk item_sk FROM catalog_sales \
              GROUP BY cs_bill_customer_sk, cs_item_sk) csci \
             ON (ssci.customer_sk = csci.customer_sk AND ssci.item_sk = csci.item_sk)",
        )
        .await;
        assert!(
            try_full_outer_join_global_agg(&lp, &["date_dim"])
                .unwrap()
                .is_none(),
            "count(DISTINCT …) over a full outer join does not recombine per-partition"
        );
    }

    // ------------------------------------------------------------------
    // Q24: HAVING scalar threshold over a shared derived per-key aggregate.
    // ------------------------------------------------------------------

    /// The SF10 classification for Q24: `store_sales` sharded, the returns table and every
    /// dim replicated.
    const REPL_Q24: [&str; 5] = [
        "store_returns",
        "store",
        "item",
        "customer",
        "customer_address",
    ];

    #[tokio::test]
    async fn q24_derived_having_scalar_threshold_plans_distributed() {
        let lp = plan(Q24).await;
        let dq = try_derived_having_scalar_threshold(&lp, &REPL_Q24)
            .unwrap()
            .expect("Q24 must admit the scalar-threshold shape");
        // The derived aggregate (partial + combine), the scalar partial + one-row combine,
        // and the outer partial + combine.
        assert_eq!(dq.stages.len(), 6, "{dq:?}");
        let quoted = format!("'{SCALAR_TOKEN}'");
        let token_stages = dq.stages.iter().filter(|s| s.sql.contains(&quoted)).count();
        assert_eq!(
            token_stages, 1,
            "the scalar placeholder must survive in exactly the outer combine: {dq:?}"
        );
        // The scalar combine produces the global threshold off the derived combine's output.
        assert!(
            dq.stages.iter().any(|s| s.sql.contains(" AS m0 FROM")),
            "one-row scalar combine stage missing: {dq:?}"
        );
    }

    #[tokio::test]
    async fn q24_shape_declines_when_scalar_reads_a_different_table() {
        // The threshold must be over the SAME derived table the outer aggregate reads —
        // a scalar over the raw fact cannot reuse the distributed derived stages.
        let lp = plan(&Q24.replace("FROM ssales)", "FROM store_sales)")).await;
        assert!(
            try_derived_having_scalar_threshold(&lp, &REPL_Q24)
                .unwrap()
                .is_none(),
            "a threshold over a different input than the outer aggregate must decline"
        );
    }

    #[tokio::test]
    async fn q24_shape_declines_without_having() {
        let lp = plan(
            "SELECT ss_store_sk, sum(ss_net_paid) FROM store_sales \
             GROUP BY ss_store_sk",
        )
        .await;
        assert!(
            try_derived_having_scalar_threshold(&lp, &REPL_Q24)
                .unwrap()
                .is_none(),
            "no HAVING scalar threshold — nothing for this shape to do"
        );
    }
}

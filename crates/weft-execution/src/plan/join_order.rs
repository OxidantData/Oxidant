//! Filter-first inner-join reordering for distributed stage planning.
//!
//! DataFusion has no cost-based join reordering: the physical plan executes inner joins in
//! the order the SQL wrote them (only the build/probe *side* ever swaps, and only with
//! statistics). When a query joins two large tables before its selective dimension filters —
//! TPC-DS Q72 at SF10 joins `catalog_sales` (14.4M rows) to `inventory` (~77 rows per item)
//! second, exploding to ~1.1B joined rows that the quantity/week filters only cut *after* the
//! fact — every execution strategy suffers: the all sort-merge plan the workers' unknown-stats
//! reroute picks must external-sort that intermediate (observed live: 36-38 GB of spill per
//! worker in 600 s, stage killed by the timeout without emitting a batch), and a hash plan
//! would build on the exploded chain.
//!
//! Reordering the *inner* joins so leaves carrying single-table filter conjuncts join first is
//! semantics-preserving and collapses the intermediate before the expensive join (Q72: the
//! `cd_marital_status` / `hd_buy_potential` / `d_year` filters shrink `catalog_sales` to ~10⁵
//! rows before `inventory` is ever touched). Outer joins, non-inner chain members, and
//! everything above the chain stay exactly where they were; a chain whose computed order is
//! unchanged returns `None` so unaffected queries keep their plans byte-for-byte.

use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{Column, NullEquality};
use datafusion::logical_expr::{
    Expr, Filter, Join, JoinConstraint, JoinType, LogicalPlan, Projection, SubqueryAlias,
};

use super::stage_planner::flatten_and_conjuncts;

/// Rewrite every inner-join chain that sits under a `Filter` so leaves with single-table
/// filter conjuncts join before leaves without. `None` when no chain changes order — the
/// common case — so callers keep the original plan untouched.
pub(crate) fn reorder_filtered_dims_first(lp: &LogicalPlan) -> Option<LogicalPlan> {
    let out = lp
        .clone()
        .transform_up(|node| {
            let LogicalPlan::Filter(f) = &node else {
                return Ok(Transformed::no(node));
            };
            let Some(input) = reorder_under_filter(f) else {
                return Ok(Transformed::no(node));
            };
            let rebuilt =
                Filter::try_new(f.predicate.clone(), Arc::new(input)).map(LogicalPlan::Filter)?;
            Ok(Transformed::yes(rebuilt))
        })
        .ok()?;
    out.transformed.then_some(out.data)
}

/// One step of a flattened left-deep inner-join chain: the original join with its right leaf.
/// Reordering permutes steps; each step keeps its own right input and ON expressions, so the
/// chain stays valid as long as a step's referenced left-side leaves are placed before it.
struct Step {
    right: Arc<LogicalPlan>,
    on: Vec<(Expr, Expr)>,
    filter: Option<Expr>,
    join_constraint: JoinConstraint,
    null_equality: NullEquality,
    null_aware: bool,
}

/// A node passed through on the way from the `Filter` down to the inner-join chain, re-applied
/// unchanged above the reordered chain. Only transparent-in-input shapes are allowed: a
/// projection, a subquery alias, or the preserved side of an outer join (reordering the inner
/// product below a LEFT/RIGHT join does not change which rows null-extend).
enum Cap {
    Projection(Projection),
    SubqueryAlias(SubqueryAlias),
    /// Descend into `left` (LEFT join): the reordered chain replaces the left input.
    OuterJoinLeft(Join),
    /// Descend into `right` (RIGHT join): the reordered chain replaces the right input.
    OuterJoinRight(Join),
}

/// A chain leaf the reorder knows how to place: a plain table scan, optionally under an alias
/// (self-joins like Q72's `date_dim d1/d2/d3` are distinct leaves through their aliases).
/// Anything else (a subquery, an aggregation, a join of a different type) makes the chain
/// opaque and the rewrite bails — it never guesses at plan shapes it cannot reassemble.
fn is_simple_leaf(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::TableScan(_) => true,
        LogicalPlan::SubqueryAlias(a) => matches!(a.input.as_ref(), LogicalPlan::TableScan(_)),
        _ => false,
    }
}

/// Flatten a left-deep inner-join chain into its leftmost leaf + one [`Step`] per join,
/// preserving the original step order. `None` when the chain contains anything but inner joins
/// over simple leaves.
fn flatten_chain(node: &LogicalPlan, steps: &mut Vec<Step>) -> Option<Arc<LogicalPlan>> {
    match node {
        LogicalPlan::Join(j) if j.join_type == JoinType::Inner => {
            let leftmost = flatten_chain(&j.left, steps)?;
            if !is_simple_leaf(&j.right) {
                return None;
            }
            steps.push(Step {
                right: j.right.clone(),
                on: j.on.clone(),
                filter: j.filter.clone(),
                join_constraint: j.join_constraint,
                null_equality: j.null_equality,
                null_aware: j.null_aware,
            });
            Some(leftmost)
        }
        leaf if is_simple_leaf(leaf) => Some(Arc::new(node.clone())),
        _ => None,
    }
}

/// The single leaf whose schema contains `col`, or `None` when zero or several leaves match
/// (a duplicate unqualified name is ambiguous — the rewrite bails rather than guess).
fn leaf_of(col: &Column, leaves: &[Arc<LogicalPlan>]) -> Option<usize> {
    let mut found = None;
    for (i, leaf) in leaves.iter().enumerate() {
        if leaf.schema().index_of_column(col).is_ok() {
            if found.is_some() {
                return None;
            }
            found = Some(i);
        }
    }
    found
}

/// Reorder the inner-join chain under `filter` (descending through caps) so filtered leaves
/// join first. See the module docs for the full rationale.
fn reorder_under_filter(filter: &Filter) -> Option<LogicalPlan> {
    let mut conjuncts = Vec::new();
    flatten_and_conjuncts(&filter.predicate, &mut conjuncts);

    // Pass through transparent nodes down to the contiguous inner-join chain.
    let mut caps: Vec<Cap> = Vec::new();
    let mut cursor = filter.input.as_ref();
    let chain_root = loop {
        match cursor {
            LogicalPlan::Join(j) if j.join_type == JoinType::Inner => break cursor,
            LogicalPlan::Projection(p) => {
                caps.push(Cap::Projection(p.clone()));
                cursor = p.input.as_ref();
            }
            LogicalPlan::SubqueryAlias(a) => {
                caps.push(Cap::SubqueryAlias(a.clone()));
                cursor = a.input.as_ref();
            }
            LogicalPlan::Join(j) if j.join_type == JoinType::Left => {
                caps.push(Cap::OuterJoinLeft(j.clone()));
                cursor = j.left.as_ref();
            }
            LogicalPlan::Join(j) if j.join_type == JoinType::Right => {
                caps.push(Cap::OuterJoinRight(j.clone()));
                cursor = j.right.as_ref();
            }
            _ => return None,
        }
    };

    let mut steps: Vec<Step> = Vec::new();
    let leftmost = flatten_chain(chain_root, &mut steps)?;
    // Fewer than two joins leaves nothing to permute.
    if steps.len() < 2 {
        return None;
    }
    let mut leaves: Vec<Arc<LogicalPlan>> = vec![leftmost];
    leaves.extend(steps.iter().map(|s| s.right.clone()));

    // Score each leaf by the number of filter conjuncts fully attributable to it alone.
    let mut scores = vec![0usize; leaves.len()];
    for conjunct in &conjuncts {
        let refs = conjunct.column_refs();
        if refs.is_empty() {
            continue;
        }
        let mut owner = None;
        let mut single = true;
        for col in refs {
            match leaf_of(col, &leaves) {
                Some(i) if owner.is_none() => owner = Some(i),
                Some(i) if owner == Some(i) => {}
                _ => {
                    // Cross-table or ambiguous conjunct: it filters no single leaf.
                    single = false;
                    break;
                }
            }
        }
        if single {
            if let Some(i) = owner {
                scores[i] += 1;
            }
        }
    }

    // Placement dependencies for each step: the leaves (other than its own right leaf) its ON
    // expressions reference. A step may only be placed once those are in the chain. Any column
    // that resolves to no leaf (or ambiguously) bails the whole rewrite.
    let mut deps: Vec<Vec<usize>> = Vec::with_capacity(steps.len());
    for (si, step) in steps.iter().enumerate() {
        let own_leaf = si + 1;
        let mut step_deps = Vec::new();
        let mut exprs: Vec<&Expr> = step.on.iter().flat_map(|(l, r)| [l, r]).collect();
        if let Some(f) = &step.filter {
            exprs.push(f);
        }
        for expr in exprs {
            for col in expr.column_refs() {
                let i = leaf_of(col, &leaves)?;
                if i != own_leaf && !step_deps.contains(&i) {
                    step_deps.push(i);
                }
            }
        }
        deps.push(step_deps);
    }

    // Greedy, deterministic placement: keep the original leftmost leaf; repeatedly place the
    // highest-scoring step whose dependencies are all placed (ties: original order). This is a
    // stable sort by score under validity constraints, so a chain with no scoring conjuncts
    // comes out in its original order and returns `None` below.
    let mut placed: Vec<usize> = vec![0];
    let mut remaining: Vec<usize> = (0..steps.len()).collect();
    let mut new_order: Vec<usize> = Vec::with_capacity(steps.len());
    while !remaining.is_empty() {
        let mut best: Option<usize> = None; // position within `remaining`
        for (pos, &si) in remaining.iter().enumerate() {
            if !deps[si].iter().all(|d| placed.contains(d)) {
                continue;
            }
            let better = match best {
                None => true,
                Some(b) => {
                    (scores[si + 1], std::cmp::Reverse(si))
                        > (scores[remaining[b] + 1], std::cmp::Reverse(remaining[b]))
                }
            };
            if better {
                best = Some(pos);
            }
        }
        let pos = best?; // dependency cycle — bail rather than emit an invalid chain
        let si = remaining.remove(pos);
        placed.push(si + 1);
        new_order.push(si);
    }
    if new_order.iter().copied().eq(0..steps.len()) {
        return None;
    }

    // Rebuild the chain in the new order, preserving each step's own join definition.
    let mut acc = leaves[0].clone();
    for &si in &new_order {
        let step = &steps[si];
        let join = Join::try_new(
            acc,
            step.right.clone(),
            step.on.clone(),
            step.filter.clone(),
            JoinType::Inner,
            step.join_constraint,
            step.null_equality,
            step.null_aware,
        )
        .ok()?;
        acc = Arc::new(LogicalPlan::Join(join));
    }

    // Re-apply the caps above the reordered chain, innermost last.
    let mut out = acc;
    for cap in caps.into_iter().rev() {
        out = match cap {
            Cap::Projection(p) => Arc::new(LogicalPlan::Projection(
                Projection::try_new(p.expr.clone(), out).ok()?,
            )),
            Cap::SubqueryAlias(a) => Arc::new(LogicalPlan::SubqueryAlias(
                SubqueryAlias::try_new(out, a.alias.clone()).ok()?,
            )),
            Cap::OuterJoinLeft(j) => Arc::new(LogicalPlan::Join(
                Join::try_new(
                    out,
                    j.right.clone(),
                    j.on.clone(),
                    j.filter.clone(),
                    j.join_type,
                    j.join_constraint,
                    j.null_equality,
                    j.null_aware,
                )
                .ok()?,
            )),
            Cap::OuterJoinRight(j) => Arc::new(LogicalPlan::Join(
                Join::try_new(
                    j.left.clone(),
                    out,
                    j.on.clone(),
                    j.filter.clone(),
                    j.join_type,
                    j.join_constraint,
                    j.null_equality,
                    j.null_aware,
                )
                .ok()?,
            )),
        };
    }
    Some((*out).clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use weft_loom::arrow::array::{Int64Array, StringArray};
    use weft_loom::arrow::datatypes::{DataType, Field, Schema};
    use weft_loom::arrow::record_batch::RecordBatch;
    use weft_loom::Engine;

    fn table(cols: &[(&str, DataType)]) -> RecordBatch {
        let schema = Arc::new(Schema::new(
            cols.iter()
                .map(|(n, t)| Field::new(*n, t.clone(), true))
                .collect::<Vec<_>>(),
        ));
        let columns = cols
            .iter()
            .map(|(_, t)| -> Arc<dyn weft_loom::arrow::array::Array> {
                match t {
                    DataType::Int64 => Arc::new(Int64Array::from(vec![1, 2, 3])),
                    DataType::Utf8 => Arc::new(StringArray::from(vec!["x", "y", "z"])),
                    other => panic!("unsupported test column type {other}"),
                }
            })
            .collect();
        RecordBatch::try_new(schema, columns).unwrap()
    }

    /// The Q72 shape in miniature: sharded fact `f` joined to an unfiltered second large table
    /// `big` first, then to filtered dims `a` / `b`, with an outer join above the inner chain.
    const Q: &str = "SELECT a.id, sum(f.v) AS s \
                     FROM f \
                     JOIN big ON (f.big_sk = big.k) \
                     JOIN a ON (f.a_sk = a.k) \
                     JOIN b ON (f.b_sk = b.k) \
                     LEFT JOIN c ON (f.c_sk = c.k) \
                     WHERE a.flag = 'x' AND b.flag = 'y' \
                     GROUP BY a.id";

    async fn logical_plan(sql: &str) -> LogicalPlan {
        let engine = Engine::new();
        let int = DataType::Int64;
        engine
            .register_batches(
                "f",
                vec![table(&[
                    ("a_sk", int.clone()),
                    ("b_sk", int.clone()),
                    ("c_sk", int.clone()),
                    ("big_sk", int.clone()),
                    ("v", int.clone()),
                ])],
            )
            .unwrap();
        engine
            .register_batches("big", vec![table(&[("k", int.clone())])])
            .unwrap();
        engine
            .register_batches(
                "a",
                vec![table(&[
                    ("k", int.clone()),
                    ("id", int.clone()),
                    ("flag", DataType::Utf8),
                ])],
            )
            .unwrap();
        engine
            .register_batches(
                "b",
                vec![table(&[("k", int.clone()), ("flag", DataType::Utf8)])],
            )
            .unwrap();
        engine
            .register_batches("c", vec![table(&[("k", int.clone())])])
            .unwrap();
        engine.logical_plan(sql).await.unwrap()
    }

    /// Table names of the leaves of the inner-join chain under the plan's filter, in join
    /// order — the shape the reorder permutes.
    fn chain_leaf_names(lp: &LogicalPlan) -> Vec<String> {
        fn scan_name(plan: &LogicalPlan) -> String {
            match plan {
                LogicalPlan::TableScan(t) => t.table_name.to_string(),
                LogicalPlan::SubqueryAlias(a) => a.alias.to_string(),
                other => panic!("not a leaf: {}", other.display_indent()),
            }
        }
        fn walk(node: &LogicalPlan, names: &mut Vec<String>) {
            match node {
                LogicalPlan::Join(j) if j.join_type == JoinType::Inner => {
                    walk(&j.left, names);
                    names.push(scan_name(&j.right));
                }
                LogicalPlan::Join(j) if j.join_type == JoinType::Left => walk(&j.left, names),
                leaf => names.push(scan_name(leaf)),
            }
        }
        fn find_filter(node: &LogicalPlan) -> &Filter {
            match node {
                LogicalPlan::Filter(f) => f,
                other => find_filter(other.inputs().into_iter().next().expect("leaf")),
            }
        }
        let f = find_filter(lp);
        let mut names = Vec::new();
        walk(&f.input, &mut names);
        names
    }

    #[tokio::test]
    async fn filtered_dims_move_ahead_of_unfiltered_fact_joins() {
        let lp = logical_plan(Q).await;
        assert_eq!(chain_leaf_names(&lp), vec!["f", "big", "a", "b"]);
        let reordered = reorder_filtered_dims_first(&lp).expect("must reorder");
        assert_eq!(chain_leaf_names(&reordered), vec!["f", "a", "b", "big"]);
        // Reordering must be stable: a second pass changes nothing.
        assert!(reorder_filtered_dims_first(&reordered).is_none());
    }

    #[tokio::test]
    async fn no_filter_conjuncts_keeps_original_order() {
        let lp = logical_plan(
            "SELECT a.id, sum(f.v) AS s FROM f \
             JOIN big ON (f.big_sk = big.k) JOIN a ON (f.a_sk = a.k) JOIN b ON (f.b_sk = b.k) \
             GROUP BY a.id",
        )
        .await;
        // No Filter node at all — nothing to reorder by.
        assert!(reorder_filtered_dims_first(&lp).is_none());
    }

    #[tokio::test]
    async fn already_optimal_order_is_left_untouched() {
        let lp = logical_plan(
            "SELECT a.id, sum(f.v) AS s FROM f \
             JOIN a ON (f.a_sk = a.k) JOIN b ON (f.b_sk = b.k) JOIN big ON (f.big_sk = big.k) \
             LEFT JOIN c ON (f.c_sk = c.k) \
             WHERE a.flag = 'x' AND b.flag = 'y' GROUP BY a.id",
        )
        .await;
        assert!(reorder_filtered_dims_first(&lp).is_none());
    }

    #[tokio::test]
    async fn non_inner_chain_member_blocks_reorder() {
        let lp = logical_plan(
            "SELECT a.id, sum(f.v) AS s FROM f \
             JOIN big ON (f.big_sk = big.k) FULL JOIN a ON (f.a_sk = a.k) \
             WHERE a.flag = 'x' GROUP BY a.id",
        )
        .await;
        assert!(reorder_filtered_dims_first(&lp).is_none());
    }

    #[tokio::test]
    async fn distributed_stage_sql_uses_reordered_joins() {
        let lp = logical_plan(Q).await;
        let dq =
            super::super::stage_planner::plan_distributed_logical(&lp, &["big", "a", "b", "c"])
                .expect("plannable");
        let stage0 = &dq.stages[0].sql;
        let a_pos = stage0.find("JOIN a ON").expect(stage0);
        let big_pos = stage0.find("JOIN big ON").expect(stage0);
        assert!(
            a_pos < big_pos,
            "filtered dim `a` must join before unfiltered `big` in stage SQL: {stage0}"
        );
    }
}

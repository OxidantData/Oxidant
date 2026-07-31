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
//!
//! A second normalization runs before the reorder: [`connect_comma_join_chain`] rewrites a
//! comma-join chain (`CROSS JOIN`s with the equijoin predicates parked in the `Filter` above)
//! into a *connected* chain of keyed inner equijoins, so generated stage SQL never hands a
//! worker a plain cross join between large tables (TPC-DS Q6 at SF10).

use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{Column, NullEquality};
use datafusion::logical_expr::{
    Expr, Filter, Join, JoinConstraint, JoinType, LogicalPlan, Operator, Projection, SubqueryAlias,
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

/// KAN-49 (TPC-DS Q6 at SF10): normalize a comma-join chain — a `Filter` over a left-deep
/// chain of key-less inner joins — into a *connected* chain of keyed inner equijoins, so
/// generated stage SQL carries every join as `INNER JOIN ... ON` and no worker ever plans a
/// `CrossJoinExec` between large tables.
///
/// DataFusion has no cost-based join reordering: the physical plan executes joins in the order
/// the stage SQL writes them. Q6's stage-0 SQL listed `customer_address CROSS JOIN date_dim
/// CROSS JOIN item CROSS JOIN customer CROSS JOIN store_sales` with all four equijoin
/// predicates in `WHERE`; `date_dim` and `item` equijoin only against `store_sales`, so the
/// emitted order left them as genuine cross joins under the fact join. A worker-side
/// `CrossJoinExec` buffers its *entire* left input in memory — at SF10 `date_dim` filtered to
/// one `d_month_seq` × `customer ⋈ customer_address` was 16 GB — outside the KAN-25 hash-join
/// build guard, which only inspects `HashJoinExec` builds and cannot reroute a cross join.
///
/// The rewrite moves each cross-table `l = r` filter conjunct onto the join that connects its
/// two leaves and reorders the chain as a greedy spanning walk of the join graph, so every join
/// after the first has at least one hash key. The walk roots at the *original* leftmost leaf —
/// keeping the established KAN-26 shuffle-chain shapes byte-stable — and attaches the
/// most-filtered reachable leaf next (ties: written order). Remaining conjuncts are distributed
/// exactly like the KAN-26 comma-join rewrite distributes them: single-table predicates push
/// onto their leaf's scan, non-equality cross-table conjuncts become the residual ON filter of
/// the join placing the last leaf they reference. Nothing may stay in a `Filter` above the
/// rebuilt chain: the shuffle-join chain extractor walks past `Filter` nodes and would silently
/// drop such a predicate. The rewrite is semantics-preserving: inner joins commute and the
/// conjunctive predicate evaluates over the same row set either way.
///
/// Returns `None` for anything outside the narrow shape — a chain with a keyed step or a
/// non-simple leaf (the KAN-49a branch-DAG cross joins above aggregates keep their shape), an
/// ambiguously-resolving column, no usable equality edge, a join graph not connected from the
/// leftmost leaf (a genuine cross product, left exactly as written), or a subquery-bearing
/// conjunct the dedicated subquery paths own (a conjunct correlated across several leaves, or
/// one whose subquery scans a non-`replicated` table — TPC-H Q2/Q4/Q20 decorrelate or
/// materialize those; only single-leaf subquery predicates over replicated tables, like Q6's
/// two thresholds, may push down) — so unaffected queries keep their plans byte-for-byte.
pub(crate) fn connect_comma_join_chain(
    lp: &LogicalPlan,
    replicated: &[&str],
) -> Option<LogicalPlan> {
    let out = lp
        .clone()
        .transform_up(|node| {
            let LogicalPlan::Filter(f) = &node else {
                return Ok(Transformed::no(node));
            };
            let Some(rebuilt) = connect_under_filter(f, replicated) else {
                return Ok(Transformed::no(node));
            };
            Ok(Transformed::yes(rebuilt))
        })
        .ok()?;
    out.transformed.then_some(out.data)
}

/// Whether `e` contains any subquery expression (scalar / IN / EXISTS).
fn expr_has_subquery(e: &Expr) -> bool {
    let mut found = false;
    let _ = e.apply(|node| {
        if matches!(
            node,
            Expr::Exists(_) | Expr::InSubquery(_) | Expr::ScalarSubquery(_)
        ) {
            found = true;
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    });
    found
}

/// Every base table scanned inside `e`'s subquery plans, matching
/// [`super::shape_extensions::collect_subquery_tables`]'s naming.
fn expr_subquery_tables(e: &Expr, out: &mut Vec<String>) {
    visit_subquery_plans(e, &mut |lp| {
        if let LogicalPlan::TableScan(s) = lp {
            let name = s.table_name.table().to_string();
            if !out.iter().any(|t| t == &name) {
                out.push(name);
            }
        }
    });
}

/// Every [`Expr::OuterReferenceColumn`] anywhere inside `e`'s subquery plans. `column_refs`
/// does not see them, but they attribute the conjunct to the chain leaves they correlate with
/// just like its plain columns — over-collecting (a nested subquery's outer ref may name its
/// immediate scope, not the chain) only ever makes the rewrite decline, never mis-place.
fn expr_subquery_outer_refs(e: &Expr, out: &mut Vec<Column>) {
    visit_subquery_plans(e, &mut |lp| {
        for expr in lp.expressions() {
            let _ = expr.apply(|node| {
                if let Expr::OuterReferenceColumn(_, col) = node {
                    if !out.contains(col) {
                        out.push(col.clone());
                    }
                }
                Ok(TreeNodeRecursion::Continue)
            });
        }
    });
}

/// Run `f` on every subquery plan in `e`, recursing into nested subqueries.
fn visit_subquery_plans(e: &Expr, f: &mut impl FnMut(&LogicalPlan)) {
    let _ = e.apply(|node| {
        let plan = match node {
            Expr::Exists(ex) => Some(ex.subquery.subquery.as_ref()),
            Expr::InSubquery(iq) => Some(iq.subquery.subquery.as_ref()),
            Expr::ScalarSubquery(sq) => Some(sq.subquery.as_ref()),
            _ => None,
        };
        if let Some(p) = plan {
            fn walk(lp: &LogicalPlan, f: &mut impl FnMut(&LogicalPlan)) {
                f(lp);
                for expr in lp.expressions() {
                    visit_subquery_plans(&expr, f);
                }
                for c in lp.inputs() {
                    walk(c, f);
                }
            }
            walk(p, f);
        }
        Ok(TreeNodeRecursion::Continue)
    });
}

/// One step of the [`connect_comma_join_chain`] rewrite: see the function for the contract.
fn connect_under_filter(filter: &Filter, replicated: &[&str]) -> Option<LogicalPlan> {
    // The chain root must be the filter's direct input — no caps, keeping the shape narrow.
    let mut steps: Vec<Step> = Vec::new();
    let leftmost = flatten_chain(&filter.input, &mut steps)?;
    // Only pure comma chains: every step key-less (a keyed step is a different normalization —
    // `rewrite_comma_join_filters` / the ordinary keyed-chain paths own those).
    if steps.is_empty() || steps.iter().any(|s| !s.on.is_empty() || s.filter.is_some()) {
        return None;
    }
    let mut leaves: Vec<Arc<LogicalPlan>> = vec![leftmost];
    leaves.extend(steps.iter().map(|s| s.right.clone()));

    let mut conjuncts = Vec::new();
    flatten_and_conjuncts(&filter.predicate, &mut conjuncts);

    // Classify every conjunct: a cross-table plain-column equality becomes a join edge (later
    // an ON key), a single-table conjunct pushes onto its leaf's scan, any other cross-table
    // conjunct becomes a residual ON filter of the join placing the last leaf it references.
    // A column that resolves to no leaf (or ambiguously) bails the whole rewrite rather than
    // guess. Subquery-bearing conjuncts attribute their plain columns *and* their subqueries'
    // outer references (below), so Q6's two self-correlated thresholds land on their own leaf
    // while a cross-correlated one declines the rewrite entirely.
    let mut edges: Vec<(usize, usize, Expr, Expr)> = Vec::new();
    let mut leaf_preds: Vec<Vec<Expr>> = vec![Vec::new(); leaves.len()];
    let mut cross_residual: Vec<(Vec<usize>, Expr)> = Vec::new();
    let mut scores = vec![0usize; leaves.len()];
    for conjunct in conjuncts {
        let mut edge = None;
        if let Expr::BinaryExpr(b) = &conjunct {
            if b.op == Operator::Eq {
                if let (Expr::Column(l), Expr::Column(r)) = (b.left.as_ref(), b.right.as_ref()) {
                    let li = leaf_of(l, &leaves)?;
                    let ri = leaf_of(r, &leaves)?;
                    if li != ri {
                        edge = Some((li, ri, b.left.as_ref().clone(), b.right.as_ref().clone()));
                    }
                }
            }
        }
        if let Some(e) = edge {
            edges.push(e);
            continue;
        }
        let has_subquery = expr_has_subquery(&conjunct);
        let mut owners: Vec<usize> = Vec::new();
        for col in conjunct.column_refs() {
            let i = leaf_of(col, &leaves)?;
            if !owners.contains(&i) {
                owners.push(i);
            }
        }
        if has_subquery {
            // `column_refs` does not see the outer references inside subquery plans; they
            // attribute the conjunct to the leaves it correlates with exactly like its plain
            // columns. A conjunct correlated across leaves — or whose outer reference resolves
            // to no chain leaf (a scope above this filter) — is left for the dedicated paths.
            let mut outer_refs = Vec::new();
            expr_subquery_outer_refs(&conjunct, &mut outer_refs);
            for col in &outer_refs {
                let i = leaf_of(col, &leaves)?;
                if !owners.contains(&i) {
                    owners.push(i);
                }
            }
            // A subquery-bearing conjunct may only push onto its leaf's scan when the dedicated
            // subquery paths provably do not own it: it is not correlated across leaves, and
            // every table its subquery scans is replicated (present in full on every worker).
            // Anything else — TPC-H Q2's per-key min, the semi/anti cascades, the
            // materialization fallbacks — keeps its original shape for those paths.
            if owners.len() > 1 {
                return None;
            }
            let mut sub_tables = Vec::new();
            expr_subquery_tables(&conjunct, &mut sub_tables);
            if !sub_tables.iter().all(|t| replicated.contains(&t.as_str())) {
                return None;
            }
        }
        match owners.len() {
            // No columns at all (a constant predicate): leftmost side, matching the KAN-26
            // rewrite's convention — it filters every row identically either way.
            0 => leaf_preds[0].push(conjunct),
            1 => {
                scores[owners[0]] += 1;
                leaf_preds[owners[0]].push(conjunct);
            }
            _ => cross_residual.push((owners, conjunct)),
        }
    }
    if edges.is_empty() {
        return None; // no equijoin structure to expose — a genuine cross product
    }

    // Greedy spanning walk rooted at the ORIGINAL leftmost leaf (KAN-26 byte-stability),
    // attaching the most-filtered reachable leaf next (ties: written order). `on` pairs are
    // oriented (accumulated-chain expr, new-leaf expr) as `Join::try_new` expects.
    let mut placed: Vec<usize> = vec![0];
    while placed.len() < leaves.len() {
        let mut best: Option<usize> = None;
        for (j, &score_j) in scores.iter().enumerate() {
            if placed.contains(&j) {
                continue;
            }
            let connected = edges.iter().any(|(l, r, _, _)| {
                (*l == j) != (*r == j) && (placed.contains(l) || placed.contains(r))
            });
            if !connected {
                continue;
            }
            let better = match best {
                None => true,
                Some(b) => (score_j, std::cmp::Reverse(j)) > (scores[b], std::cmp::Reverse(b)),
            };
            if better {
                best = Some(j);
            }
        }
        placed.push(best?); // disconnected from the leftmost leaf — a genuine cross product
    }

    // Wrap each filtered leaf, then chain the joins in placed order.
    let wrap = |idx: usize| -> Option<Arc<LogicalPlan>> {
        match leaf_preds[idx].iter().cloned().reduce(Expr::and) {
            None => Some(leaves[idx].clone()),
            Some(p) => Some(Arc::new(LogicalPlan::Filter(
                Filter::try_new(p, leaves[idx].clone()).ok()?,
            ))),
        }
    };
    let mut acc: Arc<LogicalPlan> = wrap(0)?;
    for (pos, &j) in placed.iter().enumerate().skip(1) {
        let prior = &placed[..pos];
        let mut on: Vec<(Expr, Expr)> = Vec::new();
        for (l, r, lexpr, rexpr) in &edges {
            if *r == j && prior.contains(l) {
                on.push((lexpr.clone(), rexpr.clone()));
            } else if *l == j && prior.contains(r) {
                on.push((rexpr.clone(), lexpr.clone()));
            }
        }
        if on.is_empty() {
            return None; // unreachable from the walk above; never emit a key-less join
        }
        // A cross-table residual joins at the step placing the last leaf it references.
        let mut residual_parts: Vec<Expr> = Vec::new();
        for (owners, expr) in &cross_residual {
            if owners.contains(&j) && owners.iter().all(|o| o == &j || prior.contains(o)) {
                residual_parts.push(expr.clone());
            }
        }
        let join_filter = residual_parts.into_iter().reduce(Expr::and);
        let join = Join::try_new(
            acc,
            wrap(j)?,
            on,
            join_filter,
            JoinType::Inner,
            steps[0].join_constraint,
            steps[0].null_equality,
            steps[0].null_aware,
        )
        .ok()?;
        acc = Arc::new(LogicalPlan::Join(join));
    }

    Some((*acc).clone())
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

    /// `on`-key counts of every inner join in the plan (any depth), in no particular order.
    fn all_inner_join_key_counts(lp: &LogicalPlan) -> Vec<usize> {
        let mut counts = Vec::new();
        if let LogicalPlan::Join(j) = lp {
            if j.join_type == JoinType::Inner {
                counts.push(j.on.len());
            }
        }
        for c in lp.inputs() {
            counts.extend(all_inner_join_key_counts(c));
        }
        counts
    }

    /// The Q6 shape in miniature: a comma chain (`f, a, b`) whose equijoins live in WHERE.
    /// The rewrite must connect every join with hash keys, rooted at the original leftmost
    /// leaf, with single-table predicates pushed onto their leaf's scan (KAN-26 convention).
    const COMMA_Q: &str = "SELECT a.id, sum(f.v) AS s \
                           FROM f, a, b \
                           WHERE f.a_sk = a.k AND f.b_sk = b.k AND a.flag = 'x' \
                           GROUP BY a.id";

    /// Table names of the chain leaves in join order, seeing through pushed-down leaf filters.
    fn join_tree_leaf_names(lp: &LogicalPlan) -> Vec<String> {
        fn scan_name(plan: &LogicalPlan) -> String {
            match plan {
                LogicalPlan::TableScan(t) => t.table_name.to_string(),
                LogicalPlan::SubqueryAlias(a) => a.alias.to_string(),
                LogicalPlan::Filter(f) => scan_name(f.input.as_ref()),
                other => panic!("not a leaf: {}", other.display_indent()),
            }
        }
        fn walk(node: &LogicalPlan, names: &mut Vec<String>) {
            match node {
                LogicalPlan::Join(j) if j.join_type == JoinType::Inner => {
                    walk(&j.left, names);
                    names.push(scan_name(&j.right));
                }
                leaf => names.push(scan_name(leaf)),
            }
        }
        fn chain_root(node: &LogicalPlan) -> &LogicalPlan {
            match node {
                LogicalPlan::Join(j) if j.join_type == JoinType::Inner => node,
                other => chain_root(other.inputs().into_iter().next().expect("leaf")),
            }
        }
        let mut names = Vec::new();
        walk(chain_root(lp), &mut names);
        names
    }

    fn count_filters(lp: &LogicalPlan) -> usize {
        usize::from(matches!(lp, LogicalPlan::Filter(_)))
            + lp.inputs().iter().map(|c| count_filters(c)).sum::<usize>()
    }

    #[tokio::test]
    async fn comma_chain_becomes_connected_keyed_joins() {
        let lp = logical_plan(COMMA_Q).await;
        // The raw plan has no keys on any chain join.
        assert_eq!(all_inner_join_key_counts(&lp), vec![0, 0]);
        let connected = connect_comma_join_chain(&lp, &[]).expect("must rewrite");
        // Every join carries its equijoin keys; the chain stays rooted at the original
        // leftmost leaf, then attaches the filtered dim before the unfiltered one.
        assert_eq!(all_inner_join_key_counts(&connected), vec![1, 1]);
        assert_eq!(join_tree_leaf_names(&connected), vec!["f", "a", "b"]);
        // The single-table predicate pushes onto leaf `a`'s scan — exactly one Filter, and
        // none above the chain (the shuffle-chain extractor walks past such filters).
        assert_eq!(count_filters(&connected), 1);
        let leaf_a = chain_root_input(&connected, 1);
        assert!(matches!(leaf_a, LogicalPlan::Filter(_)), "{leaf_a:?}");
        // The rewrite converges: a second pass finds no comma chain left.
        assert!(connect_comma_join_chain(&connected, &[]).is_none());
    }

    /// A single-leaf scalar-subquery predicate over a REPLICATED table (the Q6 pattern)
    /// pushes onto its leaf's scan like any single-table predicate.
    #[tokio::test]
    async fn subquery_over_replicated_table_pushes_down_and_connects() {
        let lp = logical_plan(
            "SELECT a.id, sum(f.v) AS s FROM f, a \
             WHERE f.a_sk = a.k AND a.k > (SELECT avg(big.k) FROM big) \
             GROUP BY a.id",
        )
        .await;
        let connected = connect_comma_join_chain(&lp, &["big"]).expect("must rewrite");
        assert_eq!(all_inner_join_key_counts(&connected), vec![1]);
        assert_eq!(join_tree_leaf_names(&connected), vec!["f", "a"]);
        assert_eq!(count_filters(&connected), 1);
        assert!(matches!(
            chain_root_input(&connected, 1),
            LogicalPlan::Filter(_)
        ));
    }

    /// The same shape with the subquery's table NOT replicated stays exactly as written — the
    /// dedicated subquery paths own it (a per-shard evaluation would be wrong).
    #[tokio::test]
    async fn subquery_over_sharded_table_keeps_original_shape() {
        let lp = logical_plan(
            "SELECT a.id, sum(f.v) AS s FROM f, a \
             WHERE f.a_sk = a.k AND a.k > (SELECT avg(big.k) FROM big) \
             GROUP BY a.id",
        )
        .await;
        assert!(connect_comma_join_chain(&lp, &[]).is_none());
    }

    /// A subquery correlated across two chain leaves (the TPC-H Q2 pattern) keeps its shape
    /// for the decorrelation paths, even when its subquery tables are replicated.
    #[tokio::test]
    async fn cross_correlated_subquery_keeps_original_shape() {
        let lp = logical_plan(
            "SELECT a.id, sum(f.v) AS s FROM f, a \
             WHERE f.a_sk = a.k AND f.v > (SELECT avg(big.k) FROM big WHERE big.k = a.k) \
             GROUP BY a.id",
        )
        .await;
        assert!(connect_comma_join_chain(&lp, &["big"]).is_none());
    }

    /// The `idx`-th chain leaf (0 = leftmost), for asserting pushed-down filter placement.
    fn chain_root_input(lp: &LogicalPlan, idx: usize) -> &LogicalPlan {
        fn chain_root(node: &LogicalPlan) -> &LogicalPlan {
            match node {
                LogicalPlan::Join(j) if j.join_type == JoinType::Inner => node,
                other => chain_root(other.inputs().into_iter().next().expect("leaf")),
            }
        }
        fn leaf_at<'a>(
            node: &'a LogicalPlan,
            idx: usize,
            seen: &mut usize,
        ) -> Option<&'a LogicalPlan> {
            match node {
                LogicalPlan::Join(j) if j.join_type == JoinType::Inner => {
                    if let Some(found) = leaf_at(&j.left, idx, seen) {
                        return Some(found);
                    }
                    if *seen == idx {
                        *seen += 1;
                        return Some(&j.right);
                    }
                    *seen += 1;
                    None
                }
                leaf => {
                    if *seen == idx {
                        *seen += 1;
                        return Some(leaf);
                    }
                    *seen += 1;
                    None
                }
            }
        }
        let mut seen = 0usize;
        leaf_at(chain_root(lp), idx, &mut seen).expect("leaf index in range")
    }

    #[tokio::test]
    async fn comma_chain_without_equality_edges_is_untouched() {
        // A genuine cross product (no cross-table equality) keeps its original shape.
        let lp =
            logical_plan("SELECT a.id, sum(f.v) AS s FROM f, a WHERE a.flag = 'x' GROUP BY a.id")
                .await;
        assert!(connect_comma_join_chain(&lp, &[]).is_none());
    }

    #[tokio::test]
    async fn disconnected_comma_chain_is_untouched() {
        // `b` equijoins nothing: the join graph is disconnected — a genuine cross product.
        let lp = logical_plan(
            "SELECT a.id, sum(f.v) AS s FROM f, a, b WHERE f.a_sk = a.k GROUP BY a.id",
        )
        .await;
        assert!(connect_comma_join_chain(&lp, &[]).is_none());
    }

    #[tokio::test]
    async fn mixed_keyed_and_cross_chain_is_untouched() {
        // One keyed step + one cross step: a different normalization owns that shape.
        let lp = logical_plan(
            "SELECT a.id, sum(f.v) AS s FROM f JOIN a ON (f.a_sk = a.k), b \
             WHERE f.b_sk = b.k GROUP BY a.id",
        )
        .await;
        assert!(connect_comma_join_chain(&lp, &[]).is_none());
    }
}

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

    reapply_caps(acc, caps)
}

/// Re-apply the caps a rewrite descended through above the rebuilt chain, innermost last.
fn reapply_caps(chain: Arc<LogicalPlan>, caps: Vec<Cap>) -> Option<LogicalPlan> {
    let mut out = chain;
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

/// KAN-2 (TPC-DS Q72 at the 2-sharded classification): distribute a `Filter` sitting above a
/// **keyed** inner-join chain (with optional projection / alias / outer-join caps) onto the
/// chain itself — single-table conjuncts push onto their leaf's scan, cross-table conjuncts
/// become the residual of the join placing their last referenced leaf — so nothing remains in
/// a `Filter` above the chain. The shuffle-join chain extractor walks *past* `Filter` nodes
/// and would silently drop such a predicate; once the chain planner folds trailing replicated
/// joins (Q72's `LEFT JOIN promotion` / `LEFT JOIN catalog_returns`) into the final stage
/// instead of rejecting them, a dropped `Filter` becomes a wrong answer rather than a
/// rejection. This is the keyed-chain counterpart of [`connect_comma_join_chain`]'s
/// distribution (which the comma-join shape already gets): Q72's six WHERE conjuncts
/// (`cd_marital_status`, `hd_buy_potential`, `d1.d_year`, the `d_week_seq` equality, the
/// quantity comparison, the ship-date comparison) ride ONE `Filter` above the
/// `catalog_sales ⋈ inventory` chain because the driver-side planner is analyzer-only — no
/// predicate pushdown.
///
/// The rewrite fires only when the chain holds **at least two sharded leaves** — a
/// one-sharded chain belongs to the broadcast path, which unparses the `Filter` into its
/// stage tail and must keep it. It is semantics-preserving: inner joins commute and a
/// conjunctive predicate evaluates over the same row set whether applied above the chain or
/// at the placing join.
///
/// Returns `None` — plan untouched, original rejection/fallback paths preserved — for
/// anything outside the narrow shape: a non-inner chain member or non-simple leaf, an
/// ambiguously-resolving column, a conjunct referencing an outer-join cap's null-extended
/// leaf (pushing it below the outer join would change which rows null-extend), a
/// subquery-bearing conjunct (the dedicated subquery paths own those), or a cross-table
/// conjunct mixing replicated and sharded leaves whose last referenced leaf is sharded (a
/// folded replicated dim's columns are not in the shuffle stream the sharded step's ON/WHERE
/// can reference).
pub(crate) fn distribute_chain_filter(
    lp: &LogicalPlan,
    replicated: &[&str],
) -> Option<LogicalPlan> {
    let out = lp
        .clone()
        .transform_up(|node| {
            let LogicalPlan::Filter(f) = &node else {
                return Ok(Transformed::no(node));
            };
            let Some(rebuilt) = distribute_under_filter(f, replicated) else {
                return Ok(Transformed::no(node));
            };
            Ok(Transformed::yes(rebuilt))
        })
        .ok()?;
    out.transformed.then_some(out.data)
}

/// One step of the [`distribute_chain_filter`] rewrite: see the function for the contract.
fn distribute_under_filter(filter: &Filter, replicated: &[&str]) -> Option<LogicalPlan> {
    // Descend the same chain-transparent caps the filter-first reorder allows.
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
    let leftmost = flatten_chain_scans(chain_root, &mut steps)?;
    if steps.is_empty() {
        return None;
    }
    let mut leaves: Vec<Arc<LogicalPlan>> = vec![leftmost];
    leaves.extend(steps.iter().map(|s| s.right.clone()));
    let sharded: Vec<bool> = leaves
        .iter()
        .map(|l| leaf_table_name(l).is_some_and(|t| !replicated.contains(&t)))
        .collect();
    // Only a chain the shuffle-join-chain planner will own; the broadcast path keeps its
    // Filter (it unparses into the single sharded stage's tail).
    if sharded.iter().filter(|&&s| s).count() < 2 {
        return None;
    }

    // Classify every conjunct: a single-table conjunct pushes onto its leaf's scan (exactly
    // the KAN-26 comma-join convention), any other conjunct becomes a residual of the step
    // placing the last leaf it references. A column resolving to no inner-chain leaf — an
    // outer-join cap's null-extended side, a projected-away name, or an ambiguous duplicate —
    // declines the whole rewrite rather than mis-place the predicate.
    let mut conjuncts = Vec::new();
    flatten_and_conjuncts(&filter.predicate, &mut conjuncts);
    let mut leaf_preds: Vec<Vec<Expr>> = vec![Vec::new(); leaves.len()];
    let mut step_residuals: Vec<Vec<Expr>> = vec![Vec::new(); steps.len()];
    for conjunct in conjuncts {
        // The dedicated subquery paths own subquery-bearing conjuncts (decorrelation,
        // materialization, semi/anti cascades) — never redistribute those.
        if expr_has_subquery(&conjunct) {
            return None;
        }
        let mut owners: Vec<usize> = Vec::new();
        for col in conjunct.column_refs() {
            let i = leaf_of(col, &leaves)?;
            if !owners.contains(&i) {
                owners.push(i);
            }
        }
        match owners.len() {
            // No columns at all (a constant predicate): leftmost side, matching the KAN-26 /
            // connector convention — it filters every row identically either way.
            0 => leaf_preds[0].push(conjunct),
            1 => leaf_preds[owners[0]].push(conjunct),
            _ => {
                let last = *owners.iter().max().expect("owners is non-empty");
                // A folded replicated dim is joined into the stage as the raw table — its
                // columns are not in the shuffle stream a *sharded* step's ON/WHERE can
                // reference. Decline rather than emit a dangling reference.
                if sharded[last] && owners.iter().any(|&o| !sharded[o]) {
                    return None;
                }
                step_residuals[last - 1].push(conjunct);
            }
        }
    }

    // Wrap each filtered leaf, AND each step's new residuals into its own join filter, and
    // rebuild the chain in its ORIGINAL order — placement distributes the predicate, it
    // never permutes the chain.
    let wrap = |idx: usize| -> Option<Arc<LogicalPlan>> {
        match leaf_preds[idx].iter().cloned().reduce(Expr::and) {
            None => Some(leaves[idx].clone()),
            Some(p) => Some(Arc::new(LogicalPlan::Filter(
                Filter::try_new(p, leaves[idx].clone()).ok()?,
            ))),
        }
    };
    let mut acc: Arc<LogicalPlan> = wrap(0)?;
    for (si, step) in steps.iter().enumerate() {
        let new_filter = step
            .filter
            .clone()
            .into_iter()
            .chain(step_residuals[si].iter().cloned())
            .reduce(Expr::and);
        let join = Join::try_new(
            acc,
            wrap(si + 1)?,
            step.on.clone(),
            new_filter,
            JoinType::Inner,
            step.join_constraint,
            step.null_equality,
            step.null_aware,
        )
        .ok()?;
        acc = Arc::new(LogicalPlan::Join(join));
    }

    // The outer caps re-wrap unchanged; the Filter itself is fully consumed — every conjunct
    // landed on a scan or a step, so nothing may stay above the rebuilt chain.
    reapply_caps(acc, caps)
}

/// KAN-2 (TPC-DS Q37/Q82 at the row-aware classification): re-root an inner-join chain whose
/// **written leftmost leaf is replicated** so a sharded leaf becomes the chain root, which the
/// shuffle-join-chain planner requires.
///
/// The row-aware replicate/shard classification (`OXIDANT_REPLICATE_MAX_ROW_MULTIPLE`, on by
/// default) can keep a mid-chain table sharded while the query's written-first table stays a
/// replicated dim — Q37's `FROM item, inventory, date_dim, catalog_sales` with `inventory` and
/// `catalog_sales` sharded. [`connect_comma_join_chain`] and [`reorder_filtered_dims_first`]
/// both deliberately root the chain at the written leftmost leaf, so the chain planner then
/// rejected the query ("requires a sharded leftmost table") and it fell back to single-node
/// execution. Inner joins are symmetric, so rotating the chain to start at a sharded leaf is
/// semantics-preserving — with two care points this rewrite handles explicitly:
///
/// - **No re-rooting across non-inner joins**: only a contiguous *inner* chain is rotated
///   (outer/semi/anti members make the chain opaque, exactly like the reorder above); an
///   outer join *above* the chain is a cap — the inner product on its preserved side may be
///   rotated without changing which rows null-extend (same argument as the reorder's caps).
/// - **Side-specific join conditions**: a step may only be placed once every leaf its ON /
///   residual expressions reference is in the chain, and each ON pair is re-oriented so its
///   right expression references the leaf that step brings in. When the re-oriented left key
///   references a *replicated* dim (which folds into the stage and never becomes a shuffle
///   input), it is substituted with an equivalent column from the query's own equality web —
///   Q37's `cs_item_sk = i_item_sk` becomes `cs_item_sk = inv_item_sk` through
///   `inv_item_sk = i_item_sk`. Conjunctive inner-join equality is transitive, so the result
///   set is unchanged; without a carried equivalent the rewrite declines.
///
/// The rewrite fires only when the written leftmost leaf is replicated **and at least two
/// leaves are sharded** — a single-sharded chain is the broadcast path's, which does not care
/// about join order, so those plans stay byte-for-byte stable. Anything unexpected (an
/// ambiguously-resolving column, a placement cycle, an ON pair that does not touch the
/// incoming leaf, a trailing replicated leaf) declines the rewrite and the query keeps its
/// original plan and error/fallback path.
pub(crate) fn reroot_inner_chain_at_sharded(
    lp: &LogicalPlan,
    replicated: &[&str],
) -> Option<LogicalPlan> {
    reroot_node(lp, replicated)
}

/// Descend the plan spine through chain-transparent nodes and re-root the first inner-join
/// chain found. Caps re-wrap unchanged above the rotated chain.
fn reroot_node(node: &LogicalPlan, replicated: &[&str]) -> Option<LogicalPlan> {
    match node {
        LogicalPlan::Join(j) => match j.join_type {
            JoinType::Inner => reroot_chain(node, replicated),
            JoinType::Left => {
                let new_left = reroot_node(&j.left, replicated)?;
                node.with_new_exprs(node.expressions(), vec![new_left, j.right.as_ref().clone()])
                    .ok()
            }
            JoinType::Right => {
                let new_right = reroot_node(&j.right, replicated)?;
                node.with_new_exprs(node.expressions(), vec![j.left.as_ref().clone(), new_right])
                    .ok()
            }
            _ => None,
        },
        LogicalPlan::Limit(_)
        | LogicalPlan::Sort(_)
        | LogicalPlan::Projection(_)
        | LogicalPlan::SubqueryAlias(_)
        | LogicalPlan::Filter(_)
        | LogicalPlan::Aggregate(_)
        | LogicalPlan::Distinct(_) => {
            let input = node.inputs().into_iter().next()?;
            let new_input = reroot_node(input, replicated)?;
            node.with_new_exprs(node.expressions(), vec![new_input])
                .ok()
        }
        _ => None,
    }
}

/// A chain leaf for the re-root: like [`is_simple_leaf`] but also seeing through the `Filter`
/// wrappers [`connect_comma_join_chain`] pushes single-table predicates down into.
fn is_chain_leaf(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Filter(f) => is_chain_leaf(f.input.as_ref()),
        _ => is_simple_leaf(plan),
    }
}

/// The base table a chain leaf scans, through `Filter` / `SubqueryAlias` wrappers.
fn leaf_table_name(plan: &LogicalPlan) -> Option<&str> {
    match plan {
        LogicalPlan::TableScan(s) => Some(s.table_name.table()),
        LogicalPlan::SubqueryAlias(a) => leaf_table_name(a.input.as_ref()),
        LogicalPlan::Filter(f) => leaf_table_name(f.input.as_ref()),
        _ => None,
    }
}

/// [`flatten_chain`] over [`is_chain_leaf`] leaves, so chains whose leaves carry pushed-down
/// filters (every connected comma chain with single-table predicates) stay re-rootable.
fn flatten_chain_scans(node: &LogicalPlan, steps: &mut Vec<Step>) -> Option<Arc<LogicalPlan>> {
    match node {
        LogicalPlan::Join(j) if j.join_type == JoinType::Inner => {
            let leftmost = flatten_chain_scans(&j.left, steps)?;
            if !is_chain_leaf(&j.right) {
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
        leaf if is_chain_leaf(leaf) => Some(Arc::new(node.clone())),
        _ => None,
    }
}

/// The `(leaf index, column)` a join expression resolves to, for dependency / equality-web
/// analysis. Non-column expressions never resolve (they are not join keys here).
fn column_leaf<'e>(e: &'e Expr, leaves: &[Arc<LogicalPlan>]) -> Option<(usize, &'e Column)> {
    let Expr::Column(c) = e else { return None };
    let i = leaf_of(c, leaves)?;
    Some((i, c))
}

/// `a.k = b.k` with plain columns on both sides — a join-edge conjunct whether it sits in
/// `on` or is parked in `join.filter`. Returns cloned `(left, right)` column exprs.
fn as_column_equality(e: &Expr) -> Option<(Expr, Expr)> {
    let Expr::BinaryExpr(b) = e else { return None };
    if b.op != Operator::Eq {
        return None;
    }
    if !matches!(b.left.as_ref(), Expr::Column(_)) || !matches!(b.right.as_ref(), Expr::Column(_)) {
        return None;
    }
    Some((b.left.as_ref().clone(), b.right.as_ref().clone()))
}

/// Union-find over the chain's equality web: `(leaf index, column name)` keys joined by every
/// column-equality the query's ON clauses / join filters state. Used to substitute a join key
/// that references a folded replicated dim with an equivalent column a shuffle input carries.
#[derive(Default)]
struct EqualityWeb {
    parent: std::collections::HashMap<(usize, String), (usize, String)>,
    member: std::collections::HashMap<(usize, String), Column>,
}

impl EqualityWeb {
    fn find(&self, key: &(usize, String)) -> (usize, String) {
        let mut root = key.clone();
        while let Some(p) = self.parent.get(&root) {
            root = p.clone();
        }
        root
    }

    fn link(&mut self, a: (usize, String), b: (usize, String), ca: &Column, cb: &Column) {
        self.member.entry(a.clone()).or_insert_with(|| ca.clone());
        self.member.entry(b.clone()).or_insert_with(|| cb.clone());
        let (ra, rb) = (self.find(&a), self.find(&b));
        if ra != rb {
            self.parent.insert(rb, ra);
        }
    }

    /// The equality-web peer of `key` carried by an already-placed shuffle input: a qualified
    /// column on a placed sharded leaf (smallest leaf index wins, so the chain root — always
    /// placed — is preferred). `skip` excludes the join step's own right leaf.
    fn carried_peer(
        &self,
        key: &(usize, String),
        placed: &[usize],
        sharded: &[bool],
        skip: usize,
    ) -> Option<Column> {
        let root = self.find(key);
        self.member
            .iter()
            .filter(|(k, c)| {
                self.find(k) == root
                    && k.0 != skip
                    && sharded[k.0]
                    && placed.contains(&k.0)
                    && c.relation.is_some()
            })
            .min_by_key(|(k, _)| k.0)
            .map(|(_, c)| c.clone())
    }
}

fn reroot_chain(chain_root: &LogicalPlan, replicated: &[&str]) -> Option<LogicalPlan> {
    let mut steps: Vec<Step> = Vec::new();
    let leftmost = flatten_chain_scans(chain_root, &mut steps)?;
    let mut leaves: Vec<Arc<LogicalPlan>> = vec![leftmost];
    leaves.extend(steps.iter().map(|s| s.right.clone()));
    let n = leaves.len();

    let sharded: Vec<bool> = leaves
        .iter()
        .map(|l| leaf_table_name(l).is_some_and(|t| !replicated.contains(&t)))
        .collect();
    // Byte-stability: a chain already rooted at a sharded leaf keeps its written shape, and a
    // chain with fewer than two sharded leaves belongs to the broadcast path (order-free).
    if sharded[0] || sharded.iter().filter(|&&s| s).count() < 2 {
        return None;
    }
    // The new root: the first sharded leaf in written order.
    let root_idx = (1..n).find(|&i| sharded[i])?;

    // Placement dependencies per step (every leaf its ON / residual exprs reference, other
    // than the leaf it brings in) plus the equality web for key substitution. Any column that
    // resolves to no leaf (or ambiguously) bails the whole rewrite rather than guess.
    // Equality conjuncts parked in `join.filter` register too: the chain planner promotes them
    // to hash keys downstream, so they are part of the query's equality web.
    let mut web = EqualityWeb::default();
    let mut refs: Vec<Vec<usize>> = Vec::with_capacity(steps.len());
    let link_pair = |web: &mut EqualityWeb, l: &Expr, r: &Expr, leaves: &[Arc<LogicalPlan>]| {
        if let (Some((li, lc)), Some((ri, rc))) = (column_leaf(l, leaves), column_leaf(r, leaves)) {
            web.link((li, lc.name.clone()), (ri, rc.name.clone()), lc, rc);
        }
    };
    for step in &steps {
        // A subquery in a step's ON / filter correlates through outer references that
        // `column_refs` cannot see — placement could miss them, so decline (the dedicated
        // subquery paths own those shapes).
        if step
            .on
            .iter()
            .any(|(l, r)| expr_has_subquery(l) || expr_has_subquery(r))
            || step.filter.as_ref().is_some_and(expr_has_subquery)
        {
            return None;
        }
        let mut step_refs = Vec::new();
        let mut exprs: Vec<&Expr> = step.on.iter().flat_map(|(l, r)| [l, r]).collect();
        if let Some(f) = &step.filter {
            exprs.push(f);
        }
        for expr in exprs {
            for col in expr.column_refs() {
                let i = leaf_of(col, &leaves)?;
                if !step_refs.contains(&i) {
                    step_refs.push(i);
                }
            }
        }
        for (l, r) in &step.on {
            link_pair(&mut web, l, r, &leaves);
        }
        if let Some(f) = &step.filter {
            let mut conjuncts = Vec::new();
            flatten_and_conjuncts(f, &mut conjuncts);
            for c in &conjuncts {
                if let Some((a, b)) = as_column_equality(c) {
                    link_pair(&mut web, &a, &b, &leaves);
                }
            }
        }
        refs.push(step_refs);
    }

    // The old leftmost leaf is no longer the root: the first step referencing it adopts it as
    // its incoming right leaf (a connected chain always has one — the first step).
    let adopt = refs.iter().position(|r| r.contains(&0))?;
    let new_right: Vec<usize> = (0..steps.len())
        .map(|j| if j == adopt { 0 } else { j + 1 })
        .collect();
    let deps: Vec<Vec<usize>> = refs
        .iter()
        .enumerate()
        .map(|(j, r)| r.iter().copied().filter(|&i| i != new_right[j]).collect())
        .collect();

    // Greedy, deterministic placement from the new root: placeable steps go in original
    // order; an unplaceable remainder (dependency cycle) declines the rewrite.
    let mut placed: Vec<usize> = vec![root_idx];
    let mut remaining: Vec<usize> = (0..steps.len()).collect();
    let mut order: Vec<usize> = Vec::with_capacity(steps.len());
    while !remaining.is_empty() {
        let pos = remaining.iter().position(|&j| {
            !placed.contains(&new_right[j]) && deps[j].iter().all(|d| placed.contains(d))
        })?;
        let j = remaining.remove(pos);
        placed.push(new_right[j]);
        order.push(j);
    }
    // The shuffle chain builder cannot fold trailing replicated-only joins: the last placed
    // step must bring in a sharded leaf.
    if !sharded[*placed.last()?] {
        return None;
    }

    // Rebuild the chain in placed order, re-orienting each equijoin pair (whether it sits in
    // `on` or is parked as a `join.filter` equality conjunct) so its right expression
    // references the incoming leaf, and substituting folded-dim left keys through the web.
    let orient = |a: &Expr,
                  b: &Expr,
                  rj: usize,
                  placed_so_far: &[usize],
                  leaves: &[Arc<LogicalPlan>],
                  sharded: &[bool],
                  web: &EqualityWeb|
     -> Option<(Expr, Expr)> {
        let (mut left, right) = match (column_leaf(a, leaves), column_leaf(b, leaves)) {
            (Some((ai, _)), Some((bi, _))) if bi == rj && ai != rj => (a.clone(), b.clone()),
            (Some((ai, _)), Some((bi, _))) if ai == rj && bi != rj => (b.clone(), a.clone()),
            // A pair that never touches the incoming leaf (or twice) cannot key this join.
            _ => return None,
        };
        let (li, lname) = {
            let (i, c) = column_leaf(&left, leaves)?;
            (i, c.name.clone())
        };
        if !sharded[li] {
            left = Expr::Column(web.carried_peer(&(li, lname), placed_so_far, sharded, rj)?);
        }
        Some((left, right))
    };
    let mut acc = leaves[root_idx].clone();
    let mut placed_so_far: Vec<usize> = vec![root_idx];
    for &j in &order {
        let rj = new_right[j];
        let mut on: Vec<(Expr, Expr)> = Vec::with_capacity(steps[j].on.len());
        for (a, b) in &steps[j].on {
            on.push(orient(a, b, rj, &placed_so_far, &leaves, &sharded, &web)?);
        }
        // Filter conjuncts: column equalities get the same re-orientation/substitution (the
        // chain planner re-promotes them to hash keys); everything else stays residual.
        let mut new_filter: Option<Expr> = None;
        if let Some(f) = &steps[j].filter {
            let mut conjuncts = Vec::new();
            flatten_and_conjuncts(f, &mut conjuncts);
            for c in conjuncts {
                let part = match as_column_equality(&c) {
                    Some((a, b)) => {
                        let (l, r) = orient(&a, &b, rj, &placed_so_far, &leaves, &sharded, &web)?;
                        l.eq(r)
                    }
                    None => c,
                };
                new_filter = Some(match new_filter {
                    None => part,
                    Some(prev) => prev.and(part),
                });
            }
        }
        let join = Join::try_new(
            acc,
            leaves[rj].clone(),
            on,
            new_filter,
            JoinType::Inner,
            steps[j].join_constraint,
            steps[j].null_equality,
            steps[j].null_aware,
        )
        .ok()?;
        acc = Arc::new(LogicalPlan::Join(join));
        placed_so_far.push(rj);
    }
    Some((*acc).clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use oxidant_loom::arrow::array::{Int64Array, StringArray};
    use oxidant_loom::arrow::datatypes::{DataType, Field, Schema};
    use oxidant_loom::arrow::record_batch::RecordBatch;
    use oxidant_loom::Engine;
    use std::sync::Arc;

    fn table(cols: &[(&str, DataType)]) -> RecordBatch {
        let schema = Arc::new(Schema::new(
            cols.iter()
                .map(|(n, t)| Field::new(*n, t.clone(), true))
                .collect::<Vec<_>>(),
        ));
        let columns = cols
            .iter()
            .map(|(_, t)| -> Arc<dyn oxidant_loom::arrow::array::Array> {
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

    // --- reroot_inner_chain_at_sharded ----------------------------------------

    /// The Q37 shape in miniature: replicated `item` dim written first, sharded `inventory`
    /// mid-chain, sharded `catalog_sales` last; the two sharded tables equijoin only through
    /// `item`. All-int columns keep the fixture tiny.
    async fn star_plan(sql: &str) -> LogicalPlan {
        let engine = Engine::new();
        let int = DataType::Int64;
        engine
            .register_batches(
                "item",
                vec![table(&[
                    ("i_item_sk", int.clone()),
                    ("i_item_id", int.clone()),
                ])],
            )
            .unwrap();
        engine
            .register_batches(
                "inventory",
                vec![table(&[
                    ("inv_item_sk", int.clone()),
                    ("inv_date_sk", int.clone()),
                    ("inv_quantity_on_hand", int.clone()),
                ])],
            )
            .unwrap();
        engine
            .register_batches("date_dim", vec![table(&[("d_date_sk", int.clone())])])
            .unwrap();
        engine
            .register_batches(
                "catalog_sales",
                vec![table(&[
                    ("cs_item_sk", int.clone()),
                    ("cs_quantity", int.clone()),
                ])],
            )
            .unwrap();
        engine.logical_plan(sql).await.unwrap()
    }

    /// The outermost inner join of the re-rooted chain (its last-placed step).
    fn top_inner_join(lp: &LogicalPlan) -> datafusion::logical_expr::Join {
        fn walk(node: &LogicalPlan) -> Option<datafusion::logical_expr::Join> {
            match node {
                LogicalPlan::Join(j) if j.join_type == JoinType::Inner => Some(j.clone()),
                other => walk(other.inputs().into_iter().next()?),
            }
        }
        walk(lp).expect("an inner join in the plan")
    }

    /// The equijoin pair of the chain's last-placed step that references `relation`, whether
    /// it sits in `on` or is parked as a `join.filter` equality conjunct (DataFusion's SQL
    /// planner parks written `JOIN ... ON` equalities in the filter).
    fn top_join_key(lp: &LogicalPlan, relation: &str) -> (Expr, Expr) {
        let top = top_inner_join(lp);
        let mut candidates: Vec<(Expr, Expr)> = top.on.clone();
        if let Some(f) = &top.filter {
            let mut conjuncts = Vec::new();
            flatten_and_conjuncts(f, &mut conjuncts);
            candidates.extend(conjuncts.iter().filter_map(as_column_equality));
        }
        candidates
            .into_iter()
            .find(|(a, b)| {
                [a, b].iter().any(|e| {
                    matches!(e, Expr::Column(c)
                        if c.relation.as_ref().map(|r| r.table()) == Some(relation))
                })
            })
            .unwrap_or_else(|| panic!("no join key referencing {relation}: {top:?}"))
    }

    fn assert_col(e: &Expr, relation: &str, name: &str) {
        let Expr::Column(c) = e else {
            panic!("expected a column, found {e}")
        };
        assert_eq!(
            c.relation.as_ref().map(|r| r.table()),
            Some(relation),
            "column {e} relation"
        );
        assert_eq!(c.name, name);
    }

    /// Dim-leftmost keyed chain with two sharded tables: re-rooted at the first sharded leaf
    /// (`inventory`), and the trailing `catalog_sales` join's key — written against the folded
    /// `item` dim — is substituted through the equality web to the carried `inventory` key.
    #[tokio::test]
    async fn reroots_dim_leftmost_chain_to_sharded_fact() {
        let lp = star_plan(
            "SELECT i_item_id, sum(cs_quantity) AS q \
             FROM item \
             JOIN inventory ON (inventory.inv_item_sk = item.i_item_sk) \
             JOIN date_dim ON (date_dim.d_date_sk = inventory.inv_date_sk) \
             JOIN catalog_sales ON (catalog_sales.cs_item_sk = item.i_item_sk) \
             GROUP BY i_item_id",
        )
        .await;
        let replicated = ["item", "date_dim"];
        let rerooted =
            reroot_inner_chain_at_sharded(&lp, &replicated).expect("must re-root to inventory");
        assert_eq!(
            join_tree_leaf_names(&rerooted),
            vec!["inventory", "item", "date_dim", "catalog_sales"]
        );
        // `cs_item_sk = i_item_sk` became `inv_item_sk = cs_item_sk`: conjunctive inner-join
        // equality is transitive, and `item` folds into the stage (never a shuffle input).
        let (left, right) = top_join_key(&rerooted, "catalog_sales");
        assert_col(&left, "inventory", "inv_item_sk");
        assert_col(&right, "catalog_sales", "cs_item_sk");
        // Re-rooting converges: the new leftmost leaf is sharded, so a second pass is a no-op.
        assert!(reroot_inner_chain_at_sharded(&rerooted, &replicated).is_none());
    }

    /// The same shape as a comma chain (Q37's written form): after the connector attaches the
    /// WHERE equijoins (rooted at the written leftmost `item`), the re-root rotates the
    /// connected chain to `inventory`.
    #[tokio::test]
    async fn reroots_connected_comma_chain() {
        let lp = star_plan(
            "SELECT i_item_id, sum(cs_quantity) AS q \
             FROM item, inventory, date_dim, catalog_sales \
             WHERE inventory.inv_item_sk = item.i_item_sk \
               AND date_dim.d_date_sk = inventory.inv_date_sk \
               AND catalog_sales.cs_item_sk = item.i_item_sk \
               AND inventory.inv_quantity_on_hand BETWEEN 100 AND 500 \
             GROUP BY i_item_id",
        )
        .await;
        let replicated = ["item", "date_dim"];
        let connected = connect_comma_join_chain(&lp, &replicated).expect("must connect");
        assert_eq!(
            join_tree_leaf_names(&connected),
            vec!["item", "inventory", "date_dim", "catalog_sales"]
        );
        let rerooted =
            reroot_inner_chain_at_sharded(&connected, &replicated).expect("must re-root");
        assert_eq!(
            join_tree_leaf_names(&rerooted),
            vec!["inventory", "item", "date_dim", "catalog_sales"]
        );
        let (left, right) = top_join_key(&rerooted, "catalog_sales");
        assert_col(&left, "inventory", "inv_item_sk");
        assert_col(&right, "catalog_sales", "cs_item_sk");
    }

    /// A chain containing a non-inner member is opaque: no re-rooting across it.
    #[tokio::test]
    async fn non_inner_chain_member_blocks_reroot() {
        let lp = star_plan(
            "SELECT i_item_id, sum(cs_quantity) AS q \
             FROM item \
             JOIN inventory ON (inventory.inv_item_sk = item.i_item_sk) \
             FULL JOIN catalog_sales ON (catalog_sales.cs_item_sk = item.i_item_sk) \
             GROUP BY i_item_id",
        )
        .await;
        assert!(reroot_inner_chain_at_sharded(&lp, &["item", "date_dim"]).is_none());
    }

    /// A chain already rooted at a sharded leaf keeps its written shape (byte-stability).
    #[tokio::test]
    async fn sharded_leftmost_chain_is_untouched() {
        let lp = star_plan(
            "SELECT i_item_id, sum(cs_quantity) AS q \
             FROM inventory \
             JOIN item ON (inventory.inv_item_sk = item.i_item_sk) \
             JOIN catalog_sales ON (catalog_sales.cs_item_sk = item.i_item_sk) \
             GROUP BY i_item_id",
        )
        .await;
        assert!(reroot_inner_chain_at_sharded(&lp, &["item", "date_dim"]).is_none());
    }

    /// Fewer than two sharded leaves: the broadcast path owns the chain — leave its order alone.
    #[tokio::test]
    async fn single_sharded_chain_is_untouched() {
        let lp = star_plan(
            "SELECT i_item_id, sum(inv_quantity_on_hand) AS q \
             FROM item \
             JOIN inventory ON (inventory.inv_item_sk = item.i_item_sk) \
             JOIN date_dim ON (date_dim.d_date_sk = inventory.inv_date_sk) \
             GROUP BY i_item_id",
        )
        .await;
        assert!(reroot_inner_chain_at_sharded(&lp, &["item", "date_dim"]).is_none());
    }

    /// An outer join above the inner chain is a cap: the preserved-side inner product re-roots
    /// while the outer join itself is never crossed.
    #[tokio::test]
    async fn reroots_inner_chain_below_outer_join_cap() {
        let lp = star_plan(
            "SELECT i_item_id, sum(cs_quantity) AS q \
             FROM item \
             JOIN inventory ON (inventory.inv_item_sk = item.i_item_sk) \
             JOIN catalog_sales ON (catalog_sales.cs_item_sk = item.i_item_sk) \
             LEFT JOIN date_dim ON (date_dim.d_date_sk = inventory.inv_date_sk) \
             GROUP BY i_item_id",
        )
        .await;
        let replicated = ["item", "date_dim"];
        let rerooted =
            reroot_inner_chain_at_sharded(&lp, &replicated).expect("must re-root below the cap");
        assert_eq!(
            join_tree_leaf_names(&rerooted),
            vec!["inventory", "item", "catalog_sales"]
        );
    }

    // --- distribute_chain_filter -----------------------------------------------

    /// The `join.filter` of the inner chain step whose right leaf scans `table` (where the
    /// rewrite parks cross-table residuals), seeing through any outer-join cap above.
    fn step_filter_for(lp: &LogicalPlan, table: &str) -> Option<Expr> {
        fn walk(node: &LogicalPlan, table: &str) -> Option<Expr> {
            match node {
                LogicalPlan::Join(j) if j.join_type == JoinType::Inner => {
                    if leaf_table_name(&j.right) == Some(table) {
                        return j.filter.clone();
                    }
                    walk(&j.left, table)
                }
                other => walk(other.inputs().into_iter().next()?, table),
            }
        }
        walk(lp, table)
    }

    fn conjunct_count(e: &Expr) -> usize {
        let mut conjuncts = Vec::new();
        flatten_and_conjuncts(e, &mut conjuncts);
        conjuncts.len()
    }

    /// The Q72 distribution shape in miniature: a keyed chain with two sharded leaves and a
    /// `WHERE` mixing single-table conjuncts (push onto their dim scans) with a cross-table
    /// comparison (residual of the step placing its last referenced leaf).
    const DISTRIBUTE_Q: &str = "SELECT a.id, sum(f.v) AS s \
         FROM f \
         JOIN big ON (f.big_sk = big.k) \
         JOIN a ON (f.a_sk = a.k) \
         JOIN b ON (f.b_sk = b.k) \
         LEFT JOIN c ON (f.c_sk = c.k) \
         WHERE a.flag = 'x' AND b.flag = 'y' AND f.v > big.k \
         GROUP BY a.id";

    #[tokio::test]
    async fn distributes_where_conjuncts_onto_keyed_chain() {
        let lp = logical_plan(DISTRIBUTE_Q).await;
        let rewritten =
            distribute_chain_filter(&lp, &["a", "b", "c"]).expect("must distribute the filter");
        // The chain keeps its written order (placement never permutes).
        assert_eq!(join_tree_leaf_names(&rewritten), vec!["f", "big", "a", "b"]);
        // Exactly the two pushed-down leaf filters remain — nothing stays above the chain.
        assert_eq!(count_filters(&rewritten), 2);
        assert!(matches!(
            chain_root_input(&rewritten, 2),
            LogicalPlan::Filter(_)
        ));
        assert!(matches!(
            chain_root_input(&rewritten, 3),
            LogicalPlan::Filter(_)
        ));
        assert!(!matches!(
            chain_root_input(&rewritten, 1),
            LogicalPlan::Filter(_)
        ));
        // The cross-table comparison joins the step placing its last leaf (`big`) as a
        // residual in its join filter, alongside the written equijoin.
        let step_filter = step_filter_for(&rewritten, "big").expect("big step has a filter");
        assert_eq!(conjunct_count(&step_filter), 2, "{step_filter}");
        // Fully consumed: a second pass finds no chain filter left to distribute.
        assert!(distribute_chain_filter(&rewritten, &["a", "b", "c"]).is_none());
    }

    /// A one-sharded chain belongs to the broadcast path, which unparses the `Filter` into its
    /// stage tail — the rewrite must leave it exactly where it is.
    #[tokio::test]
    async fn one_sharded_chain_keeps_its_filter() {
        let lp = logical_plan(DISTRIBUTE_Q).await;
        assert!(distribute_chain_filter(&lp, &["big", "a", "b", "c"]).is_none());
    }

    /// A subquery-bearing conjunct belongs to the dedicated subquery paths: the rewrite
    /// declines and the filter stays parked (the chain planner then refuses to fold).
    #[tokio::test]
    async fn subquery_conjunct_declines_distribution() {
        let lp = logical_plan(
            "SELECT a.id, sum(f.v) AS s FROM f \
             JOIN big ON (f.big_sk = big.k) JOIN a ON (f.a_sk = a.k) \
             WHERE a.flag = 'x' AND f.v > (SELECT avg(b.k) FROM b) \
             GROUP BY a.id",
        )
        .await;
        assert!(distribute_chain_filter(&lp, &["a", "b"]).is_none());
    }

    /// A conjunct referencing an outer-join cap's null-extended leaf cannot move below the
    /// outer join (it would re-filter null-extended rows): the rewrite declines.
    #[tokio::test]
    async fn outer_join_cap_side_conjunct_declines() {
        let lp = logical_plan(
            "SELECT a.id, sum(f.v) AS s FROM f \
             JOIN big ON (f.big_sk = big.k) JOIN a ON (f.a_sk = a.k) \
             LEFT JOIN c ON (f.c_sk = c.k) \
             WHERE c.k IS NOT NULL \
             GROUP BY a.id",
        )
        .await;
        assert!(distribute_chain_filter(&lp, &["a", "c"]).is_none());
    }

    /// A cross-table conjunct mixing a replicated leaf with a LATER sharded leaf would have
    /// to reference the folded dim from the sharded step's ON/WHERE, where its columns are
    /// not bound: the rewrite declines rather than dangle the reference.
    #[tokio::test]
    async fn cross_conjunct_on_folded_dim_and_later_sharded_declines() {
        let lp = logical_plan(
            "SELECT a.id, sum(f.v) AS s FROM f \
             JOIN a ON (f.a_sk = a.k) JOIN big ON (f.big_sk = big.k) \
             WHERE a.k = big.k \
             GROUP BY a.id",
        )
        .await;
        assert!(distribute_chain_filter(&lp, &["a"]).is_none());
    }
}

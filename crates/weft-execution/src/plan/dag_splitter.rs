//! Recursive branch-aware lowering for plans whose aggregates live below a `CrossJoin` (or a
//! `LEFT`/`RIGHT`/`FULL` join chaining several independently-aggregated branches together).
//!
//! The core splitter's [`super::stage_planner::peel`] is deliberately a fast linear walk to one
//! aggregate. CTE-heavy plans instead have a tree of independently distributable aggregate
//! branches under one or more joins. This module materializes those sharded branches as stage
//! sub-DAGs, replaces them with the worker's `shuffle_input[_i]` tables, and unparses the
//! remaining outer plan as one gathered output stage.
//!
//! ## Why any join type at the outer skeleton is safe
//!
//! Each materialized branch's final stage always writes with empty `hash_key_cols`, so — per
//! [`crate::shuffle::partition::hash_partition`]'s "empty key list = global gather" rule — its
//! *entire* output lands in partition 0 and every other rendezvous partition of the gathered
//! outer stage sees zero rows from it. A `CROSS`/`INNER` join of such branches is therefore
//! trivially empty on partitions `1..w` (whichever side is empty makes an inner-style join
//! empty), which is why the original cross-join-only admission never needed to check anything
//! beyond "at least one branch exists". A `LEFT`/`RIGHT`/`FULL` join is *not* automatically safe:
//! it must additionally have its *preserved* side(s) gated to zero on `1..w`, or a replicated
//! (never-empty) preserved side would re-emit its full contents once per worker. See
//! [`is_branch_gated`], which [`try_branch_dag`] enforces before accepting a non-cross outer
//! skeleton.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::logical_expr::logical_plan::builder::LogicalTableSource;
use datafusion::logical_expr::{Expr, LogicalPlan, LogicalPlanBuilder};
use datafusion::sql::unparser::Unparser;
use weft_common::{Error, Result};

use super::stage_planner::{
    base_tables, peel, plan_distributed_logical, sanitize_generated_sql, DistributedQuery,
};
use crate::driver::{ExchangeMode, StageDef};

/// Try to lower a plan containing a cross-join tree into independent branch sub-DAGs followed by
/// one gathered outer stage. Returns `Ok(None)` when no cross join (or no sharded branch) exists.
pub(crate) fn try_branch_dag(
    lp: &LogicalPlan,
    replicated: &[&str],
) -> Result<Option<DistributedQuery>> {
    // Only split when the first branching node under the outer unary chain is a combinable join
    // (CROSS/INNER, or a LEFT/RIGHT/FULL chaining several aggregate branches — e.g. TPC-DS Q78's
    // `ss LEFT JOIN ws LEFT JOIN cs`). A join buried inside an aggregate or UNION arm belongs to
    // that branch's own planner; lifting it would either gather raw fact rows or duplicate
    // replicated UNION arms.
    if !first_branching_node(lp).is_some_and(is_combinable_join) {
        return Ok(None);
    }

    let mut branch_nodes = Vec::new();
    collect_sharded_branches(lp, replicated, &mut branch_nodes);
    if branch_nodes.is_empty() {
        return Ok(None);
    }

    let branch_count = branch_nodes.len();
    let branch_by_node: HashMap<usize, usize> = branch_nodes
        .iter()
        .enumerate()
        .map(|(i, node)| (node_id(node), i))
        .collect();

    let mut branch_queries = Vec::with_capacity(branch_count);
    for (i, branch) in branch_nodes.iter().enumerate() {
        reject_mixed_union_branch(branch, replicated)?;
        let dq = plan_branch(branch, replicated).map_err(|e| {
            Error::Unsupported(format!(
                "auto-distribute: branch-aware CrossJoin branch {i} is not distributable: {e}"
            ))
        })?;
        // Only the branch's own *output* stage matters here: it is the one whose stage id
        // becomes the outer skeleton's upstream (via `append_branch`), so it alone must satisfy
        // the "empty hash key ⇒ everything gathers to partition 0" invariant the rest of this
        // module relies on (see the module doc). An *intermediate* Forward stage feeding that
        // output — e.g. a UNION arm scanning only replicated tables, run once via
        // `try_split_broadcast_union` / `plan_union`'s zero-sharded-arm path and then combined
        // with the sharded arm(s) inside this same sub-DAG — is exactly the safe "run once,
        // shuffle its (already-exact) contribution" pattern the driver already implements for
        // Forward producer stages, so it is not rejected here.
        if dq
            .stages
            .last()
            .is_some_and(|s| s.exchange == ExchangeMode::Forward)
        {
            return Err(Error::Unsupported(format!(
                "auto-distribute: branch-aware CrossJoin branch {i} outputs via Forward exchange; \
                 a replicated-only branch must remain in the outer stage"
            )));
        }
        branch_queries.push(dq);
    }

    let rewritten = replace_branches(lp, &branch_by_node, branch_count)?.0;
    if !is_branch_gated(&rewritten) {
        return Err(Error::Unsupported(
            "auto-distribute: branch-aware outer join is not guaranteed empty on non-driving \
             worker partitions (a replicated-only input sits on a LEFT/RIGHT/FULL join's \
             preserved side) — would duplicate rows once per worker"
                .into(),
        ));
    }
    reject_remaining_sharded_scans(&rewritten, replicated)?;
    let outer_sql = Unparser::default()
        .plan_to_sql(&rewritten)
        .map_err(|e| {
            Error::Unsupported(format!(
                "auto-distribute: unparse branch-aware CrossJoin outer plan: {e}"
            ))
        })?
        .to_string();

    let mut stages = Vec::new();
    let mut upstream_stage_ids = Vec::with_capacity(branch_count);
    let mut next_id = 0u32;
    for (i, dq) in branch_queries.into_iter().enumerate() {
        let output = append_branch(&mut stages, &mut next_id, dq, i)?;
        upstream_stage_ids.push(output);
    }

    stages.push(StageDef::new(
        next_id,
        sanitize_generated_sql(&outer_sql),
        upstream_stage_ids,
        vec![],
    ));
    Ok(Some(DistributedQuery {
        stages,
        finalize_sql: None,
    }))
}

/// A `UNION ALL` arm that scans only replicated tables is not itself unsafe to mix with a sharded
/// arm: [`plan_branch`] (via [`super::stage_planner::try_split_broadcast_union`] when the union
/// feeds a `GROUP BY`, or [`super::shape_extensions::try_union_all`]'s zero-sharded-arm handling
/// when the union sits at the branch's own top) places that arm on exactly one worker
/// (`ExchangeMode::Forward`) and shuffles its single, already-exact contribution alongside the
/// sharded arm's genuine per-worker partials — the same "run once, don't replicate" composition
/// [`is_branch_gated`]'s docs describe for a replicated `LEFT`/`RIGHT` join side. That combine is
/// only sound for `UNION ALL`: it never needs to notice a row that also appears in another arm.
///
/// A plain `UNION` (`DISTINCT`) is different — DataFusion lowers it to `Distinct` wrapping
/// `Union` — because deduplication is defined over *all* arms together. Splitting a mixed
/// sharded/replicated union into two independently-planned halves and only then `UNION ALL`-ing
/// their results (as the two paths above do) could keep a row that should have been deduplicated
/// against a row that landed in the other half. Keep that shape an honest rejection.
fn reject_mixed_union_branch(branch: &LogicalPlan, replicated: &[&str]) -> Result<()> {
    if let LogicalPlan::Distinct(distinct) = branch {
        if let LogicalPlan::Union(union) = distinct.input().as_ref() {
            if union_has_mixed_sharding(union, replicated) {
                return Err(Error::Unsupported(
                    "auto-distribute: branch-aware CrossJoin UNION (DISTINCT) has a \
                     replicated-table-only arm plus a sharded-table arm; splitting a distinct \
                     union by sharding cannot preserve cross-arm deduplication"
                        .into(),
                ));
            }
        }
    }
    for input in branch.inputs() {
        reject_mixed_union_branch(input, replicated)?;
    }
    Ok(())
}

fn union_has_mixed_sharding(union: &datafusion::logical_expr::Union, replicated: &[&str]) -> bool {
    let mut has_sharded_arm = false;
    let mut has_replicated_arm = false;
    for arm in &union.inputs {
        let tables = base_tables(arm);
        if tables
            .iter()
            .any(|table| !replicated.contains(&table.as_str()))
        {
            has_sharded_arm = true;
        } else if !tables.is_empty() {
            has_replicated_arm = true;
        }
    }
    has_sharded_arm && has_replicated_arm
}

fn plan_branch(branch: &LogicalPlan, replicated: &[&str]) -> Result<DistributedQuery> {
    match plan_distributed_logical(branch, replicated) {
        ok @ Ok(_) => ok,
        Err(primary) => {
            // Set-op detection intentionally looks through only its own top-level wrappers. CTE
            // references add one or more SubqueryAlias nodes around the same schema; stripping
            // those is safe because qualifiers are restored on the outer placeholder.
            let mut inner = branch;
            while let LogicalPlan::SubqueryAlias(alias) = inner {
                inner = alias.input.as_ref();
            }
            if std::ptr::eq(inner, branch) {
                Err(primary)
            } else {
                plan_distributed_logical(inner, replicated)
            }
        }
    }
}

/// Walk down through plain pass-through wrappers (`Limit`/`Sort`/`Projection`/`Filter`/
/// `SubqueryAlias` — exactly the node types [`super::stage_planner::peel`] also tunnels through)
/// looking for the first multi-input node. Stops (returns `None`) at any *other* single-input node
/// — notably `Aggregate` / `Window` / `Distinct` — instead of tunneling through it: those are plan
/// boundaries whose own input belongs to that node's own branch planner, not to a join skeleton
/// several levels up. Blindly tunneling through them used to let [`collect_sharded_branches`] dig
/// past a branch's `WindowAggr` straight into its raw pre-aggregation join, silently dropping the
/// window computation and materializing a branch with the wrong (pre-window) schema.
fn first_branching_node(mut lp: &LogicalPlan) -> Option<&LogicalPlan> {
    loop {
        let is_wrapper = matches!(
            lp,
            LogicalPlan::Limit(_)
                | LogicalPlan::Sort(_)
                | LogicalPlan::Projection(_)
                | LogicalPlan::Filter(_)
                | LogicalPlan::SubqueryAlias(_)
        );
        let inputs = lp.inputs();
        match inputs.as_slice() {
            [] => return None,
            [input] if is_wrapper => lp = input,
            [_] => return None,
            _ => return Some(lp),
        }
    }
}

/// A join whose outer-skeleton use is (subject to [`is_branch_gated`] for the non-`Inner` cases)
/// safe to leave in the gathered outer stage: `CROSS`/`INNER` (the original shape this splitter
/// was built for) or a `LEFT`/`RIGHT`/`FULL` chaining independently-materialized branches (e.g.
/// TPC-DS Q78's `ss LEFT JOIN ws LEFT JOIN cs`, one arm per fact table).
fn is_combinable_join(lp: &LogicalPlan) -> bool {
    use datafusion::logical_expr::JoinType;
    matches!(
        lp,
        LogicalPlan::Join(join)
            if matches!(join.join_type, JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Full)
    )
}

/// Whether `lp` — a node in the *rewritten* outer skeleton, after branches have been substituted
/// with `shuffle_input`/`shuffle_input_N` placeholders — is guaranteed to produce zero rows
/// whenever every placeholder is empty (see the module doc for why every non-zero placeholder
/// partition other than partition 0 needs this). `CROSS`/`INNER`/semi joins only need *either*
/// side gated (matches the pre-existing cross-join behavior, which never needed this check because
/// the non-empty `branch_nodes` guard above already guarantees at least one side of an all-`Inner`
/// join tree is a branch). `LEFT`/`RIGHT` additionally require the *preserved* side gated — the
/// non-preserved side may safely stay a fully replicated table computed fresh every partition,
/// since it never survives into the output once the preserved side is empty. `FULL` needs both.
fn is_branch_gated(lp: &LogicalPlan) -> bool {
    use datafusion::logical_expr::JoinType;
    match lp {
        LogicalPlan::TableScan(scan) => {
            let name = scan.table_name.table();
            name == "shuffle_input" || name.starts_with("shuffle_input_")
        }
        LogicalPlan::Join(join) => match join.join_type {
            JoinType::Inner | JoinType::LeftSemi | JoinType::RightSemi => {
                is_branch_gated(&join.left) || is_branch_gated(&join.right)
            }
            JoinType::Left | JoinType::LeftAnti | JoinType::LeftMark => is_branch_gated(&join.left),
            JoinType::Right | JoinType::RightAnti | JoinType::RightMark => {
                is_branch_gated(&join.right)
            }
            JoinType::Full => is_branch_gated(&join.left) && is_branch_gated(&join.right),
        },
        LogicalPlan::Union(union) => union.inputs.iter().all(|i| is_branch_gated(i)),
        LogicalPlan::Filter(f) => is_branch_gated(&f.input),
        LogicalPlan::Projection(p) => is_branch_gated(&p.input),
        LogicalPlan::SubqueryAlias(s) => is_branch_gated(&s.input),
        LogicalPlan::Limit(l) => is_branch_gated(&l.input),
        LogicalPlan::Sort(s) => is_branch_gated(&s.input),
        // Conservative default: an unrecognized node sitting where gating matters (e.g. an
        // Aggregate re-grouping a branch — a global `COUNT(*)` over zero rows is one row, not
        // zero) is treated as *not* gated rather than risk a false "safe".
        _ => false,
    }
}

/// Collect the maximal sharded subplans below the cross-join skeleton. Unary nodes above or between
/// cross joins remain in the skeleton so their expressions retain the original branch qualifiers.
/// Replicated-only leaves also remain there: evaluating them once beside the gathered sharded
/// inputs is correct and avoids duplicating a Forward intermediate on every worker.
fn collect_sharded_branches<'a>(
    lp: &'a LogicalPlan,
    replicated: &[&str],
    out: &mut Vec<&'a LogicalPlan>,
) {
    // Stop at an aggregate branch the linear planner already understands. Its input often contains
    // fact/dimension cross joins of its own; descending through those would gather raw fact rows
    // before aggregating and defeat the purpose of this splitter.
    if peel(lp).is_ok() {
        let tables = base_tables(lp);
        if tables
            .iter()
            .any(|table| !replicated.contains(&table.as_str()))
        {
            out.push(lp);
        }
        return;
    }

    let inputs = lp.inputs();
    let unary_leads_to_join = matches!(
        inputs.as_slice(),
        [input] if first_branching_node(input).is_some_and(is_combinable_join)
    );
    if is_combinable_join(lp) || unary_leads_to_join {
        for input in inputs {
            collect_sharded_branches(input, replicated, out);
        }
        return;
    }

    let tables = base_tables(lp);
    if tables
        .iter()
        .any(|table| !replicated.contains(&table.as_str()))
    {
        out.push(lp);
    }
}

fn node_id(lp: &LogicalPlan) -> usize {
    lp as *const LogicalPlan as usize
}

/// A materialized branch must remove every scan of its sharded fact from the outer SQL. Scans left
/// in expression subqueries are especially dangerous: they would read only partition 0's local
/// shard and silently return incomplete results.
fn reject_remaining_sharded_scans(lp: &LogicalPlan, replicated: &[&str]) -> Result<()> {
    let mut tables = HashSet::new();
    collect_tables_with_subqueries(lp, &mut tables);
    let mut sharded: Vec<_> = tables
        .into_iter()
        .filter(|table| {
            table != "shuffle_input"
                && !table.starts_with("shuffle_input_")
                && !replicated.contains(&table.as_str())
        })
        .collect();
    sharded.sort();
    if sharded.is_empty() {
        Ok(())
    } else {
        Err(Error::Unsupported(format!(
            "auto-distribute: branch-aware CrossJoin outer plan still scans unmaterialized \
             sharded table(s) {sharded:?} (possibly in a scalar/correlated subquery)"
        )))
    }
}

fn collect_tables_with_subqueries(lp: &LogicalPlan, out: &mut HashSet<String>) {
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};

    if let LogicalPlan::TableScan(scan) = lp {
        out.insert(scan.table_name.table().to_string());
    }
    for input in lp.inputs() {
        collect_tables_with_subqueries(input, out);
    }
    for expr in lp.expressions() {
        let _ = expr.apply(|node| {
            let subquery = match node {
                Expr::Exists(exists) => Some(&exists.subquery.subquery),
                Expr::InSubquery(in_subquery) => Some(&in_subquery.subquery.subquery),
                Expr::ScalarSubquery(scalar) => Some(&scalar.subquery),
                _ => None,
            };
            if let Some(plan) = subquery {
                collect_tables_with_subqueries(plan, out);
            }
            Ok(TreeNodeRecursion::Continue)
        });
    }
}

/// Replace selected branches while preserving every other node. The bool reports whether this
/// subtree changed, avoiding unnecessary reconstruction (and schema validation) of untouched
/// replicated branches.
fn replace_branches(
    lp: &LogicalPlan,
    branch_by_node: &HashMap<usize, usize>,
    branch_count: usize,
) -> Result<(LogicalPlan, bool)> {
    if let Some(&branch_i) = branch_by_node.get(&node_id(lp)) {
        return Ok((placeholder_plan(lp, branch_i, branch_count)?, true));
    }

    let inputs = lp.inputs();
    if inputs.is_empty() {
        return Ok((lp.clone(), false));
    }

    let mut changed = false;
    let mut rewritten_inputs = Vec::with_capacity(inputs.len());
    for input in inputs {
        let (rewritten, input_changed) = replace_branches(input, branch_by_node, branch_count)?;
        changed |= input_changed;
        rewritten_inputs.push(rewritten);
    }
    if !changed {
        return Ok((lp.clone(), false));
    }

    let rebuilt = lp
        .with_new_exprs(lp.expressions(), rewritten_inputs)
        .map_err(|e| {
            Error::Unsupported(format!(
                "auto-distribute: rebuild branch-aware CrossJoin outer plan: {e}"
            ))
        })?;
    Ok((rebuilt, true))
}

fn placeholder_plan(
    branch: &LogicalPlan,
    branch_i: usize,
    branch_count: usize,
) -> Result<LogicalPlan> {
    let table_name = if branch_count == 1 {
        "shuffle_input".to_string()
    } else {
        format!("shuffle_input_{branch_i}")
    };
    let source = Arc::new(LogicalTableSource::new(Arc::new(
        branch.schema().as_arrow().clone(),
    )));
    let scan = LogicalPlanBuilder::scan(table_name, source, None)
        .and_then(|builder| builder.build())
        .map_err(|e| {
            Error::Unsupported(format!(
                "auto-distribute: build CrossJoin branch {branch_i} placeholder: {e}"
            ))
        })?;

    // CTE / derived-table branches normally have one qualifier. Reapply it so expressions in the
    // untouched outer plan still resolve, while the unparser emits `shuffle_input_i AS old_alias`.
    let qualifiers: HashSet<_> = branch
        .schema()
        .iter()
        .filter_map(|(qualifier, _)| qualifier.cloned())
        .collect();
    match qualifiers.len() {
        0 => Ok(scan),
        1 => {
            let alias = qualifiers.into_iter().next().expect("one qualifier");
            LogicalPlanBuilder::from(scan)
                .alias(alias)
                .and_then(|builder| builder.build())
                .map_err(|e| {
                    Error::Unsupported(format!(
                        "auto-distribute: alias CrossJoin branch {branch_i} placeholder: {e}"
                    ))
                })
        }
        n => Err(Error::Unsupported(format!(
            "auto-distribute: CrossJoin branch {branch_i} output spans {n} qualifiers; \
             add a projection/alias before the branch boundary"
        ))),
    }
}

fn append_branch(
    stages: &mut Vec<StageDef>,
    next_id: &mut u32,
    dq: DistributedQuery,
    branch_i: usize,
) -> Result<u32> {
    if dq.stages.is_empty() {
        return Err(Error::Unsupported(format!(
            "auto-distribute: branch-aware CrossJoin branch {branch_i} produced no stages"
        )));
    }

    let id_offset = *next_id;
    let mut output_id = None;
    for stage in dq.stages {
        let new_id = stage.stage_id.checked_add(id_offset).ok_or_else(|| {
            Error::Unsupported("auto-distribute: branch stage id overflow".into())
        })?;
        let upstream_stage_ids = stage
            .upstream_stage_ids
            .into_iter()
            .map(|id| {
                id.checked_add(id_offset).ok_or_else(|| {
                    Error::Unsupported("auto-distribute: branch upstream id overflow".into())
                })
            })
            .collect::<Result<Vec<_>>>()?;
        stages.push(StageDef {
            stage_id: new_id,
            sql: stage.sql,
            upstream_stage_ids,
            hash_key_cols: stage.hash_key_cols,
            exchange: stage.exchange,
            plan_fragment: stage.plan_fragment,
            lakehouse_snapshot_pins: stage.lakehouse_snapshot_pins,
            replicated_tables: String::new(),
        });
        output_id = Some(new_id);
        *next_id = (*next_id).max(new_id.checked_add(1).ok_or_else(|| {
            Error::Unsupported("auto-distribute: branch stage id overflow".into())
        })?);
    }

    let mut output_id = output_id.expect("non-empty stages");
    if let Some(finalize_sql) = dq.finalize_sql {
        let rewritten = finalize_sql.replacen("FROM result", "FROM shuffle_input", 1);
        if rewritten == finalize_sql {
            return Err(Error::Unsupported(format!(
                "auto-distribute: CrossJoin branch {branch_i} finalize does not read `result`"
            )));
        }
        let finalize_id = *next_id;
        stages.push(StageDef::new(
            finalize_id,
            sanitize_generated_sql(&rewritten),
            vec![output_id],
            vec![],
        ));
        output_id = finalize_id;
        *next_id = (*next_id).checked_add(1).ok_or_else(|| {
            Error::Unsupported("auto-distribute: branch finalize stage id overflow".into())
        })?;
    }
    Ok(output_id)
}

#[cfg(test)]
mod tests {
    use weft_loom::arrow::array::{Int64Array, RecordBatch, StringArray};
    use weft_loom::arrow::datatypes::{DataType, Field, Schema};
    use weft_loom::Engine;

    use super::*;

    fn fact() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![0, 1, 0])),
                Arc::new(Int64Array::from(vec![10, 20, 30])),
            ],
        )
        .unwrap()
    }

    fn dim() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("dk", DataType::Int64, false),
            Field::new("label", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![0, 1])),
                Arc::new(StringArray::from(vec!["zero", "one"])),
            ],
        )
        .unwrap()
    }

    async fn logical_plan(sql: &str) -> LogicalPlan {
        let engine = Engine::new();
        engine.register_batches("t", vec![fact()]).unwrap();
        engine.register_batches("d", vec![dim()]).unwrap();
        engine.logical_plan(sql).await.unwrap()
    }

    #[tokio::test]
    async fn two_aggregate_ctes_form_independent_sub_dags() {
        let lp = logical_plan(
            "WITH a AS (SELECT k, SUM(v) AS s FROM t GROUP BY k), \
             b AS (SELECT COUNT(*) AS n FROM t) \
             SELECT a.k, a.s, b.n FROM a CROSS JOIN b",
        )
        .await;
        let dq = plan_distributed_logical(&lp, &[]).expect("public planner should split");

        assert_eq!(dq.stages.len(), 5);
        assert_eq!(dq.stages.last().unwrap().upstream_stage_ids, vec![1, 3]);
        let outer = &dq.stages.last().unwrap().sql;
        assert!(outer.contains("shuffle_input_0"), "{outer}");
        assert!(outer.contains("shuffle_input_1"), "{outer}");
        assert!(dq.finalize_sql.is_none());
    }

    #[tokio::test]
    async fn replicated_cross_join_leaf_stays_in_outer_stage() {
        let lp = logical_plan(
            "WITH a AS (SELECT k, SUM(v) AS s FROM t GROUP BY k) \
             SELECT a.k, a.s, d.label FROM a CROSS JOIN d",
        )
        .await;
        let dq = try_branch_dag(&lp, &["d"])
            .expect("split")
            .expect("cross join plan");

        assert_eq!(dq.stages.len(), 3);
        assert_eq!(dq.stages.last().unwrap().upstream_stage_ids, vec![1]);
        let outer = &dq.stages.last().unwrap().sql;
        assert!(outer.contains("shuffle_input"), "{outer}");
        assert!(outer.contains(" d"), "{outer}");
    }

    #[tokio::test]
    async fn unsupported_branch_reports_its_index_and_cause() {
        // A branch that the *inner* shape planners reject for a reason the gather fallback also
        // declines (ROLLUP+UNION) must surface as a branch-indexed error from try_branch_dag —
        // not as a silent Ok(None) or an unindexed shape error.
        let lp = logical_plan(
            "WITH a AS (\
                 SELECT k, SUM(v) AS s FROM (\
                   SELECT k, v FROM t WHERE k = 1 \
                   UNION ALL \
                   SELECT k, v FROM t WHERE k = 2\
                 ) AS x GROUP BY ROLLUP(k)\
             ), \
             b AS (SELECT COUNT(*) AS n FROM t) \
             SELECT a.k, a.s, b.n FROM a CROSS JOIN b",
        )
        .await;
        let err = match try_branch_dag(&lp, &[]) {
            Err(e) => e,
            Ok(None) => panic!("expected branch-dag to attempt and fail the ROLLUP+UNION branch"),
            Ok(Some(_)) => panic!("ROLLUP+UNION branch must not distribute via branch-dag"),
        };
        let msg = err.to_string();
        assert!(msg.contains("branch-aware CrossJoin branch"), "{msg}");
    }

    #[tokio::test]
    async fn count_distinct_cross_join_gathers_via_materialize_fallback() {
        // plan_distributed_logical must not leave COUNT(DISTINCT)×CrossJoin as a hard failure:
        // the gather fallback evaluates the full fact once on partition 0 (TPC-DS Q28).
        let lp = logical_plan(
            "WITH a AS (SELECT COUNT(DISTINCT v) AS n FROM t), \
             b AS (SELECT k, SUM(v) AS s FROM t GROUP BY k) \
             SELECT a.n, b.k, b.s FROM a CROSS JOIN b",
        )
        .await;
        let dq = plan_distributed_logical(&lp, &[]).expect("gather fallback");
        assert!(
            dq.stages.len() >= 3,
            "expected gather/gate/eval stages, got {dq:?}"
        );
    }

    /// `UNION ALL` of two per-arm aggregates — one over the sharded table, one over a replicated
    /// table (TPC-DS Q4/Q11/Q74's `year_total` CTE in miniature) — is safe to split: the
    /// replicated arm has zero sharded tables in its own `GROUP BY` input, so
    /// [`super::super::shape_extensions::try_union_all`]'s zero-sharded-arm handling runs it once
    /// (`ExchangeMode::Forward`) and shuffles its (already-exact) output alongside the sharded
    /// arm's genuine per-worker partials. Only the branch's own *output* stage matters to the
    /// outer skeleton (see [`try_branch_dag`]'s Forward check), and that stage still gathers with
    /// an empty hash key like any other branch.
    #[tokio::test]
    async fn mixed_union_all_branch_is_split_by_sharding() {
        let lp = logical_plan(
            "WITH a AS (\
                 SELECT k, SUM(v) AS s FROM t GROUP BY k \
                 UNION ALL \
                 SELECT dk AS k, SUM(1) AS s FROM d GROUP BY dk\
             ), \
             b AS (SELECT COUNT(*) AS n FROM t) \
             SELECT a.k, a.s, b.n FROM a CROSS JOIN b",
        )
        .await;
        let dq = try_branch_dag(&lp, &["d"])
            .expect("split")
            .expect("cross join plan");
        assert!(
            dq.stages
                .iter()
                .any(|s| s.exchange == ExchangeMode::Forward),
            "expected a Forward stage for the replicated-only UNION ALL arm: {dq:?}"
        );
        let branch_a_output = dq
            .stages
            .iter()
            .find(|s| s.sql.contains("shuffle_input_0") && s.sql.contains("shuffle_input_1"))
            .expect("branch a's UNION ALL gather stage");
        assert!(
            branch_a_output.hash_key_cols.is_empty(),
            "{branch_a_output:?}"
        );
    }

    /// A plain `UNION` (`DISTINCT`) mixing a sharded arm with a replicated-only arm is *not* safe
    /// to split the same way: DataFusion lowers it to `Distinct` wrapping `Union`, and splitting
    /// into two independently-planned halves combined with `UNION ALL` would not reproduce
    /// deduplication across the two halves. Keep this shape an honest rejection.
    #[tokio::test]
    async fn mixed_union_distinct_branch_is_rejected() {
        let lp = logical_plan(
            "WITH a AS (\
                 SELECT k, SUM(v) AS s \
                 FROM (\
                     SELECT k, v FROM t \
                     UNION \
                     SELECT dk AS k, 1 AS v FROM d\
                 ) mixed \
                 GROUP BY k\
             ), \
             b AS (SELECT COUNT(*) AS n FROM t) \
             SELECT a.k, a.s, b.n FROM a CROSS JOIN b",
        )
        .await;
        let err =
            try_branch_dag(&lp, &["d"]).expect_err("distinct UNION arm can't be split safely");
        let msg = err.to_string();
        assert!(msg.contains("UNION (DISTINCT)"), "{msg}");
        assert!(msg.contains("cross-arm deduplication"), "{msg}");
    }

    #[tokio::test]
    async fn union_of_only_replicated_arms_is_not_rejected() {
        let lp = logical_plan(
            "SELECT dk AS k, 1 AS v FROM d \
             UNION ALL \
             SELECT dk AS k, 2 AS v FROM d",
        )
        .await;
        reject_mixed_union_branch(&lp, &["d"]).expect("both UNION arms are replicated-only");
    }

    #[tokio::test]
    async fn first_branching_node_stops_at_aggregate_boundary() {
        // Projection -> Aggregate -> Filter -> Join(t, d). The join sits *below* the aggregate, so
        // it belongs to the aggregate's own branch planner, not to a join skeleton several levels
        // up. Before the fix, `first_branching_node` treated every single-input node (including
        // `Aggregate`) as a pass-through wrapper and would tunnel straight through it to this join.
        let lp = logical_plan("SELECT k, SUM(v) AS s FROM t, d WHERE t.k = d.dk GROUP BY k").await;
        assert!(
            first_branching_node(&lp).is_none(),
            "must stop at the Aggregate boundary instead of tunneling into its input"
        );
    }

    #[tokio::test]
    async fn left_join_of_two_aggregate_branches_is_distributed() {
        // TPC-DS Q78's shape in miniature: two independently-aggregated branches over the same
        // sharded fact, chained with a LEFT JOIN instead of a CROSS JOIN.
        let lp = logical_plan(
            "WITH a AS (SELECT k, SUM(v) AS s FROM t GROUP BY k), \
             b AS (SELECT k, COUNT(*) AS n FROM t GROUP BY k) \
             SELECT a.k, a.s, b.n FROM a LEFT JOIN b ON a.k = b.k",
        )
        .await;
        let dq =
            plan_distributed_logical(&lp, &[]).expect("left join of two branches should split");

        assert_eq!(dq.stages.len(), 5);
        let outer = &dq.stages.last().unwrap().sql;
        assert!(outer.to_uppercase().contains("LEFT OUTER JOIN"), "{outer}");
        assert!(outer.contains("shuffle_input_0"), "{outer}");
        assert!(outer.contains("shuffle_input_1"), "{outer}");
    }

    #[tokio::test]
    async fn left_join_with_branch_on_preserved_side_is_accepted() {
        // The aggregate branch sits on the LEFT (preserved) side and the replicated dimension sits
        // on the non-preserved side — safe, since the branch is gated to empty on partitions 1..w.
        let lp = logical_plan(
            "WITH a AS (SELECT k, SUM(v) AS s FROM t GROUP BY k) \
             SELECT a.k, a.s, d.label FROM a LEFT JOIN d ON a.k = d.dk",
        )
        .await;
        let dq = try_branch_dag(&lp, &["d"])
            .expect("split")
            .expect("left join plan");

        assert_eq!(dq.stages.len(), 3);
        let outer = &dq.stages.last().unwrap().sql;
        assert!(outer.to_uppercase().contains("LEFT OUTER JOIN"), "{outer}");
        assert!(outer.contains("shuffle_input"), "{outer}");
        assert!(outer.contains(" d"), "{outer}");
    }

    #[tokio::test]
    async fn left_join_with_replicated_preserved_side_is_rejected() {
        // The replicated dimension sits on the LEFT (preserved) side; it is never empty, so every
        // worker would re-emit its full contents once per worker if this were accepted.
        let lp = logical_plan(
            "WITH a AS (SELECT k, SUM(v) AS s FROM t GROUP BY k) \
             SELECT d.dk, a.s FROM d LEFT JOIN a ON d.dk = a.k",
        )
        .await;
        let err = try_branch_dag(&lp, &["d"])
            .expect_err("replicated preserved side must not be accepted as gated");
        let msg = err.to_string();
        assert!(msg.contains("preserved side"), "{msg}");
        assert!(msg.contains("duplicate rows once per worker"), "{msg}");
    }

    #[tokio::test]
    async fn remaining_sharded_scan_in_outer_plan_is_rejected() {
        // After materializing branches, any leftover scan of a sharded fact (including inside an
        // EXISTS/scalar subquery) would read only partition 0 and silently drop rows.
        let lp = logical_plan("SELECT k FROM t WHERE EXISTS (SELECT 1 FROM t t2 WHERE t2.k = t.k)")
            .await;
        let err = reject_remaining_sharded_scans(&lp, &[]).expect_err("sharded t still present");
        let msg = err.to_string();
        assert!(msg.contains("unmaterialized sharded table"), "{msg}");
        assert!(
            msg.contains("\"t\"") || msg.contains("`t`") || msg.contains("t"),
            "{msg}"
        );
    }

    #[tokio::test]
    async fn remaining_scan_of_only_replicated_tables_is_allowed() {
        let lp = logical_plan("SELECT dk FROM d").await;
        reject_remaining_sharded_scans(&lp, &["d"]).expect("replicated dim may remain in outer");
    }
}

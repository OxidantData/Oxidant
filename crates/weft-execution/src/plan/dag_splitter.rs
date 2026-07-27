//! Recursive branch-aware lowering for plans whose aggregates live below a `CrossJoin`.
//!
//! The core splitter's [`super::stage_planner::peel`] is deliberately a fast linear walk to one
//! aggregate. CTE-heavy plans instead have a tree of independently distributable aggregate
//! branches under one or more cross joins. This module materializes those sharded branches as
//! stage sub-DAGs, replaces them with the worker's `shuffle_input[_i]` tables, and unparses the
//! remaining outer plan as one gathered output stage.

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
    // Only split when the first branching node under the outer unary chain is a CrossJoin. A
    // CrossJoin buried inside an aggregate or UNION arm belongs to that branch's own planner;
    // lifting it would either gather raw fact rows or duplicate replicated UNION arms.
    if !first_branching_node(lp).is_some_and(is_cross_join) {
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
        if dq
            .stages
            .iter()
            .any(|s| s.exchange == ExchangeMode::Forward)
        {
            return Err(Error::Unsupported(format!(
                "auto-distribute: branch-aware CrossJoin branch {i} uses Forward exchange; \
                 replicated-only intermediate branches must remain in the outer stage"
            )));
        }
        branch_queries.push(dq);
    }

    let rewritten = replace_branches(lp, &branch_by_node, branch_count)?.0;
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

/// A replicated UNION arm is not a broadcast dimension: it contributes independent rows and would
/// therefore be emitted in full by every worker beside the sharded arm. The normal aggregate
/// splitter's "one sharded table, others replicated" rule is only safe for joins driven by the
/// sharded rows, so keep this mixed set-op shape an honest rejection.
fn reject_mixed_union_branch(branch: &LogicalPlan, replicated: &[&str]) -> Result<()> {
    if let LogicalPlan::Union(union) = branch {
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
        if has_sharded_arm && has_replicated_arm {
            return Err(Error::Unsupported(
                "auto-distribute: branch-aware CrossJoin UNION has a replicated-table-only arm \
                 plus a sharded-table arm; the replicated arm would be duplicated per worker"
                    .into(),
            ));
        }
    }
    for input in branch.inputs() {
        reject_mixed_union_branch(input, replicated)?;
    }
    Ok(())
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

fn first_branching_node(mut lp: &LogicalPlan) -> Option<&LogicalPlan> {
    loop {
        let inputs = lp.inputs();
        match inputs.as_slice() {
            [] => return None,
            [input] => lp = input,
            _ => return Some(lp),
        }
    }
}

fn is_cross_join(lp: &LogicalPlan) -> bool {
    matches!(
        lp,
        LogicalPlan::Join(join)
            if join.join_type == datafusion::logical_expr::JoinType::Inner
                && join.on.is_empty()
                && join.filter.is_none()
    )
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
    let unary_leads_to_cross_join = matches!(
        inputs.as_slice(),
        [input] if first_branching_node(input).is_some_and(is_cross_join)
    );
    if is_cross_join(lp) || unary_leads_to_cross_join {
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
        let lp = logical_plan(
            "WITH a AS (SELECT COUNT(DISTINCT v) AS n FROM t), \
             b AS (SELECT k, SUM(v) AS s FROM t GROUP BY k) \
             SELECT a.n, b.k, b.s FROM a CROSS JOIN b",
        )
        .await;
        let err = plan_distributed_logical(&lp, &[]).expect_err("global distinct branch");
        let msg = err.to_string();
        assert!(msg.contains("branch-aware CrossJoin branch 0"), "{msg}");
        assert!(msg.contains("COUNT(DISTINCT)"), "{msg}");
    }

    #[tokio::test]
    async fn mixed_sharded_and_replicated_union_is_rejected() {
        let lp = logical_plan(
            "WITH a AS (\
                 SELECT k, SUM(v) AS s \
                 FROM (\
                     SELECT k, v FROM t \
                     UNION ALL \
                     SELECT dk AS k, 1 AS v FROM d\
                 ) mixed \
                 GROUP BY k\
             ), \
             b AS (SELECT COUNT(*) AS n FROM t) \
             SELECT a.k, a.s, b.n FROM a CROSS JOIN b",
        )
        .await;
        let err = try_branch_dag(&lp, &["d"]).expect_err("replicated UNION arm would duplicate");
        let msg = err.to_string();
        assert!(
            msg.contains("replicated-table-only arm plus a sharded-table arm"),
            "{msg}"
        );
        assert!(msg.contains("duplicated per worker"), "{msg}");
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
}

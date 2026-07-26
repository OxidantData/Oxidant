//! Extra plan shapes layered on top of the core aggregation splitter.
//!
//! Kept as a sibling module so parallel edits to [`super::stage_planner`] (broadcast / shuffle
//! joins, HAVING, …) stay low-conflict. Covers:
//!
//! - **Subquery safety**: IN / EXISTS / scalar subqueries are only legal over **replicated**
//!   tables (broadcast-correct); sharded-table subqueries stay rejected by scan counting.
//! - **UNION ALL** of two (or more) distributable aggregations.
//! - Explicit **Unsupported** messages for window functions and `UNION` (distinct).

use datafusion::logical_expr::{Expr, LogicalPlan, Union};
use weft_common::{Error, Result};

use super::stage_planner::{aggregation_stages_for, peel, DistributedQuery};
use crate::driver::StageDef;

/// If `lp` is a `UNION ALL` of distributable aggregations (optionally under `ORDER BY` / `LIMIT`),
/// lower it; otherwise `Ok(None)` so the caller falls through to the aggregation path.
pub(crate) fn try_union_all(
    lp: &LogicalPlan,
    replicated: &[&str],
) -> Result<Option<DistributedQuery>> {
    let (inner, sort, limit) = peek_sort_limit(lp);
    match inner {
        LogicalPlan::Distinct(d) => {
            if matches!(d.input().as_ref(), LogicalPlan::Union(_)) {
                return Err(Error::Unsupported(
                    "auto-distribute: UNION (distinct) is not supported — use UNION ALL, \
                     or fall back to local execution"
                        .into(),
                ));
            }
            Ok(None)
        }
        LogicalPlan::Union(u) => Ok(Some(plan_union_all(u, replicated, sort, limit)?)),
        _ => Ok(None),
    }
}

/// Reject window / distinct shapes with an explicit message before the generic peel error.
pub(crate) fn reject_explicit_unsupported(lp: &LogicalPlan) -> Result<()> {
    let mut node = lp;
    loop {
        match node {
            LogicalPlan::Limit(l) => node = l.input.as_ref(),
            LogicalPlan::Sort(s) => node = s.input.as_ref(),
            LogicalPlan::Projection(p) => node = p.input.as_ref(),
            LogicalPlan::Filter(f) => node = f.input.as_ref(),
            LogicalPlan::Window(_) => {
                return Err(Error::Unsupported(
                    "auto-distribute: window functions are not supported \
                     (no partitioned-window shuffle yet) — falling back to local execution"
                        .into(),
                ));
            }
            LogicalPlan::Distinct(_) => {
                return Err(Error::Unsupported(
                    "auto-distribute: DISTINCT (or UNION distinct) is not supported"
                        .into(),
                ));
            }
            _ => return Ok(()),
        }
    }
}

/// Every table scanned inside expression subqueries (EXISTS / IN / scalar) must be **replicated**,
/// unless it is already the driving sharded fact (that case is rejected separately by scan
/// counting — a second shard-local scan would silently drop cross-shard rows).
pub(crate) fn ensure_subquery_tables_replicated(
    lp: &LogicalPlan,
    sharded: &[&str],
    replicated: &[&str],
) -> Result<()> {
    let mut tables = Vec::new();
    collect_subquery_tables(lp, &mut tables);
    for t in &tables {
        if sharded.iter().any(|s| *s == t.as_str()) {
            continue;
        }
        if !replicated.contains(&t.as_str()) {
            return Err(Error::Unsupported(format!(
                "auto-distribute: subquery over `{t}` is only safe when that table is replicated"
            )));
        }
    }
    Ok(())
}

fn plan_union_all(
    u: &Union,
    replicated: &[&str],
    sort: Option<&[datafusion::logical_expr::SortExpr]>,
    limit: Option<usize>,
) -> Result<DistributedQuery> {
    if u.inputs.len() < 2 {
        return Err(Error::Unsupported(
            "auto-distribute: UNION ALL needs at least two arms".into(),
        ));
    }

    let mut stages: Vec<StageDef> = Vec::new();
    let mut arm_output_ids: Vec<u32> = Vec::new();
    let mut next_id: u32 = 0;

    for (arm_i, arm) in u.inputs.iter().enumerate() {
        let peeled = peel(arm).map_err(|e| {
            Error::Unsupported(format!(
                "auto-distribute: UNION ALL arm {arm_i} is not a distributable aggregation: {e}"
            ))
        })?;
        // Per-arm ORDER BY / LIMIT under UNION is unexpected (optimizer lifts them); reject rather
        // than silently drop.
        if peeled.sort.is_some() || peeled.limit.is_some() {
            return Err(Error::Unsupported(format!(
                "auto-distribute: UNION ALL arm {arm_i} has ORDER BY/LIMIT; apply them outside the UNION"
            )));
        }
        let arm_dq = aggregation_stages_for(&peeled, replicated)?;
        let id_offset = next_id;
        for s in arm_dq.stages {
            let new_id = s.stage_id + id_offset;
            stages.push(StageDef {
                stage_id: new_id,
                sql: s.sql,
                upstream_stage_ids: s
                    .upstream_stage_ids
                    .into_iter()
                    .map(|u| u + id_offset)
                    .collect(),
                hash_key_cols: s.hash_key_cols,
            });
            next_id = next_id.max(new_id + 1);
        }
        // Last stage of the arm is its output; keep it as an intermediate gather (empty hash →
        // partition 0) feeding the union stage.
        let arm_out = stages.last().map(|s| s.stage_id).ok_or_else(|| {
            Error::Unsupported(format!("auto-distribute: UNION ALL arm {arm_i} produced no stages"))
        })?;
        arm_output_ids.push(arm_out);
    }

    let union_sql = if arm_output_ids.len() == 1 {
        "SELECT * FROM shuffle_input".to_string()
    } else {
        let parts: Vec<String> = (0..arm_output_ids.len())
            .map(|i| format!("SELECT * FROM shuffle_input_{i}"))
            .collect();
        parts.join(" UNION ALL ")
    };

    let union_id = next_id;
    stages.push(StageDef {
        stage_id: union_id,
        sql: union_sql,
        upstream_stage_ids: arm_output_ids,
        hash_key_cols: vec![],
    });

    let finalize_sql = build_outer_finalize(sort, limit)?;
    Ok(DistributedQuery {
        stages,
        finalize_sql,
    })
}

fn build_outer_finalize(
    sort: Option<&[datafusion::logical_expr::SortExpr]>,
    limit: Option<usize>,
) -> Result<Option<String>> {
    if sort.is_none() && limit.is_none() {
        return Ok(None);
    }
    // Mirror stage_planner::build_finalize but without Peeled — sort exprs already reference
    // output column names of the UNION.
    let up = datafusion::sql::unparser::Unparser::default();
    let mut sql = String::from("SELECT * FROM result");
    if let Some(sorts) = sort {
        let parts = sorts
            .iter()
            .map(|s| {
                let dir = if s.asc { "ASC" } else { "DESC" };
                let nulls = if s.nulls_first {
                    "NULLS FIRST"
                } else {
                    "NULLS LAST"
                };
                let expr = up
                    .expr_to_sql(&s.expr)
                    .map_err(|e| Error::Unsupported(format!("auto-distribute: unparse sort: {e}")))?
                    .to_string();
                Ok(format!("{expr} {dir} {nulls}"))
            })
            .collect::<Result<Vec<_>>>()?;
        if !parts.is_empty() {
            sql.push_str(&format!(" ORDER BY {}", parts.join(", ")));
        }
    }
    if let Some(n) = limit {
        sql.push_str(&format!(" LIMIT {n}"));
    }
    Ok(Some(sql))
}

fn peek_sort_limit(
    lp: &LogicalPlan,
) -> (
    &LogicalPlan,
    Option<&[datafusion::logical_expr::SortExpr]>,
    Option<usize>,
) {
    let mut limit = None;
    let mut sort = None;
    let mut node = lp;
    loop {
        match node {
            LogicalPlan::Limit(l) => {
                if let Some(Expr::Literal(scalar, _)) = l.fetch.as_deref() {
                    limit = scalar_as_usize(scalar);
                }
                node = l.input.as_ref();
            }
            LogicalPlan::Sort(s) => {
                sort = Some(s.expr.as_slice());
                node = s.input.as_ref();
            }
            other => return (other, sort, limit),
        }
    }
}

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

fn collect_subquery_tables(lp: &LogicalPlan, out: &mut Vec<String>) {
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
    for e in lp.expressions() {
        let _ = e.apply(|node| {
            let sub = match node {
                Expr::Exists(ex) => Some(&ex.subquery.subquery),
                Expr::InSubquery(iq) => Some(&iq.subquery.subquery),
                Expr::ScalarSubquery(sq) => Some(&sq.subquery),
                _ => None,
            };
            if let Some(plan) = sub {
                collect_all_tables(plan, out);
                collect_subquery_tables(plan, out);
            }
            Ok(TreeNodeRecursion::Continue)
        });
    }
    for c in lp.inputs() {
        collect_subquery_tables(c, out);
    }
}

fn collect_all_tables(lp: &LogicalPlan, out: &mut Vec<String>) {
    if let LogicalPlan::TableScan(s) = lp {
        let name = s.table_name.table().to_string();
        if !out.iter().any(|t| t == &name) {
            out.push(name);
        }
    }
    for c in lp.inputs() {
        collect_all_tables(c, out);
    }
}

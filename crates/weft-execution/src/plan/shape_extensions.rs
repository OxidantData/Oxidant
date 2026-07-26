//! Extra plan shapes layered on top of the core aggregation splitter.
//!
//! Kept as a sibling module so parallel edits to [`super::stage_planner`] (broadcast / shuffle
//! joins, HAVING, …) stay low-conflict. Covers:
//!
//! - **Subquery safety**: IN / EXISTS / scalar subqueries are only legal over **replicated**
//!   tables (broadcast-correct); sharded-table subqueries stay rejected by scan counting.
//! - **UNION ALL** of two (or more) distributable aggregations.
//! - **Narrow windows**: aggregate `OVER (PARTITION BY …)` over one sharded table (shuffle by
//!   partition key, then compute locally). Ranking / global windows stay Unsupported.
//! - Explicit **Unsupported** messages for unsupported window shapes and `UNION` (distinct).

use std::collections::HashMap;

use datafusion::logical_expr::expr::{WindowFunction, WindowFunctionDefinition};
use datafusion::logical_expr::{Expr, LogicalPlan, Union, Window};
use datafusion::sql::unparser::Unparser;
use weft_common::{Error, Result};

use super::stage_planner::{
    aggregation_stages_for, base_tables, column_name, count_table_scans, expr_sql,
    extract_from_tail, peel, sanitize_generated_sql, DistributedQuery,
};
use crate::driver::StageDef;

/// If `lp` is a distributable aggregate window over one sharded table, lower it; otherwise
/// `Ok(None)` so the caller falls through (unsupported window shapes return `Err`).
pub(crate) fn try_window(
    lp: &LogicalPlan,
    replicated: &[&str],
) -> Result<Option<DistributedQuery>> {
    let Some(p) = peel_window(lp) else {
        return Ok(None);
    };
    Ok(Some(window_stages_for(&p, replicated)?))
}

/// The top of the plan above a `Window` node: optional projection plus trailing sort/limit.
pub(crate) struct WindowPeeled<'a> {
    pub(crate) projection: Option<&'a [Expr]>,
    pub(crate) sort: Option<&'a [datafusion::logical_expr::SortExpr]>,
    pub(crate) limit: Option<usize>,
    pub(crate) window: &'a Window,
}

fn peel_window(lp: &LogicalPlan) -> Option<WindowPeeled<'_>> {
    let mut limit = None;
    let mut sort = None;
    let mut projection = None;
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
            LogicalPlan::Projection(p) => {
                projection = Some(p.expr.as_slice());
                node = &p.input;
            }
            LogicalPlan::Window(w) => {
                return Some(WindowPeeled {
                    projection,
                    sort,
                    limit,
                    window: w,
                });
            }
            _ => return None,
        }
    }
}

fn window_stages_for(p: &WindowPeeled<'_>, replicated: &[&str]) -> Result<DistributedQuery> {
    let w = p.window;
    if w.window_expr.is_empty() {
        return Err(Error::Unsupported(
            "auto-distribute: window plan has no window expressions".into(),
        ));
    }

    let tables = base_tables(&w.input);
    let sharded: Vec<&str> = tables
        .iter()
        .filter(|t| !replicated.contains(&t.as_str()))
        .map(|t| t.as_str())
        .collect();
    ensure_subquery_tables_replicated(&w.input, &sharded, replicated)?;
    if sharded.len() != 1 {
        return Err(Error::Unsupported(format!(
            "auto-distribute: window over {} sharded tables (need exactly one)",
            sharded.len()
        )));
    }
    let sharded_name = sharded[0];
    if count_table_scans(&w.input, sharded_name) > 1 {
        return Err(Error::Unsupported(format!(
            "auto-distribute: sharded table `{sharded_name}` scanned multiple times under window"
        )));
    }

    let mut partition_by: Option<Vec<Expr>> = None;
    for e in &w.window_expr {
        let Expr::WindowFunction(wf) = e else {
            return Err(Error::Unsupported(format!(
                "auto-distribute: non-window expression in window list: {e}"
            )));
        };
        validate_window_func(wf)?;
        match &partition_by {
            None => partition_by = Some(wf.params.partition_by.clone()),
            Some(prev) if prev == &wf.params.partition_by => {}
            Some(_) => {
                return Err(Error::Unsupported(
                    "auto-distribute: mixed PARTITION BY clauses across window functions".into(),
                ))
            }
        }
    }
    let partition_by = partition_by.unwrap_or_default();
    if partition_by.is_empty() {
        return Err(Error::Unsupported(
            "auto-distribute: window without PARTITION BY cannot be distributed \
             (no partition shuffle key) — falling back to local execution"
                .into(),
        ));
    }

    let up = Unparser::default();
    let part_names: Vec<String> = partition_by
        .iter()
        .map(|e| column_name(e))
        .collect::<Result<_>>()?;

    let input_sql = up
        .plan_to_sql(&w.input)
        .map_err(|e| Error::Unsupported(format!("auto-distribute: unparse window input: {e}")))?
        .to_string();
    let tail = extract_from_tail(&input_sql)?;
    let tail = sanitize_generated_sql(&tail);

    let mut select_cols = part_names.clone();
    for field in w.input.schema().fields() {
        let name = field.name();
        if !part_names.iter().any(|p| p == name) {
            select_cols.push(name.clone());
        }
    }
    let partial_sql = sanitize_generated_sql(&format!(
        "SELECT {} {tail}",
        select_cols.join(", ")
    ));
    let hash_key_cols: Vec<u32> = (0..part_names.len() as u32).collect();

    let mut remap: HashMap<String, String> = HashMap::new();
    for (i, e) in w.window_expr.iter().enumerate() {
        remap.insert(e.schema_name().to_string(), format!("w{i}"));
    }
    let inner = build_window_inner(&up, w, &remap)?;
    let final_sql = wrap_window_output(p, &inner, &remap)?;

    Ok(DistributedQuery {
        stages: vec![
            StageDef::new(0, partial_sql, vec![], hash_key_cols),
            StageDef::new(1, final_sql, vec![0], vec![]),
        ],
        finalize_sql: build_outer_finalize(p.sort, p.limit)?,
    })
}

fn validate_window_func(wf: &WindowFunction) -> Result<()> {
    let name = match &wf.fun {
        WindowFunctionDefinition::AggregateUDF(f) => f.name().to_ascii_lowercase(),
        WindowFunctionDefinition::WindowUDF(f) => f.name().to_ascii_lowercase(),
    };
    if !matches!(name.as_str(), "sum" | "count" | "min" | "max" | "avg") {
        return Err(Error::Unsupported(format!(
            "auto-distribute: window function `{name}` is not supported for distribution \
             (only SUM/COUNT/MIN/MAX/AVG aggregate windows)"
        )));
    }
    if wf.params.distinct {
        return Err(Error::Unsupported(
            "auto-distribute: DISTINCT window aggregates are not supported".into(),
        ));
    }
    if wf.params.filter.is_some() {
        return Err(Error::Unsupported(
            "auto-distribute: FILTER on window functions is not supported".into(),
        ));
    }
    if !wf.params.order_by.is_empty() {
        return Err(Error::Unsupported(
            "auto-distribute: window ORDER BY / ranking frames are not supported \
             (only partition-wide aggregate windows)"
                .into(),
        ));
    }
    Ok(())
}

fn build_window_inner(
    up: &Unparser,
    w: &Window,
    _remap: &HashMap<String, String>,
) -> Result<String> {
    let mut parts = Vec::new();
    for field in w.input.schema().fields() {
        parts.push(field.name().clone());
    }
    for (i, e) in w.window_expr.iter().enumerate() {
        let sql = expr_sql(up, e)?;
        parts.push(format!("{sql} AS w{i}"));
    }
    Ok(format!(
        "SELECT {} FROM shuffle_input",
        parts.join(", ")
    ))
}

fn wrap_window_output(
    p: &WindowPeeled<'_>,
    inner: &str,
    remap: &HashMap<String, String>,
) -> Result<String> {
    let up = Unparser::default();
    let from_sql = format!("({inner}) AS combined");
    let select = match p.projection {
        Some(exprs) => exprs
            .iter()
            .map(|e| {
                let name = output_name(e);
                let sql = expr_sql(&up, &remap_window_columns(strip_alias(e), remap))?;
                Ok(format!("{sql} AS \"{name}\""))
            })
            .collect::<Result<Vec<_>>>()?
            .join(", "),
        None => "*".to_string(),
    };
    Ok(format!("SELECT {select} FROM {from_sql}"))
}

fn remap_window_columns(e: &Expr, remap: &HashMap<String, String>) -> Expr {
    use datafusion::common::tree_node::{Transformed, TreeNode};
    e.clone()
        .transform(|node| {
            if let Expr::Column(c) = &node {
                if let Some(safe) = remap.get(&c.flat_name()) {
                    return Ok(Transformed::yes(datafusion::prelude::col(safe)));
                }
            }
            Ok(Transformed::no(node))
        })
        .map(|t| t.data)
        .unwrap_or(e.clone())
}

fn output_name(e: &Expr) -> String {
    match e {
        Expr::Alias(a) => a.name.clone(),
        Expr::Column(c) => c.name.clone(),
        other => other.schema_name().to_string(),
    }
}

fn strip_alias(e: &Expr) -> &Expr {
    match e {
        Expr::Alias(a) => &a.expr,
        other => other,
    }
}

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
                     (no PARTITION BY shuffle path matched) — falling back to local execution"
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
                exchange: s.exchange,
                plan_fragment: s.plan_fragment,
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
    stages.push(StageDef::new(
        union_id,
        union_sql,
        arm_output_ids,
        vec![],
    ));

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

#[cfg(test)]
mod tests {
    use weft_loom::arrow::array::{Int64Array, RecordBatch};
    use weft_loom::arrow::datatypes::{DataType, Field, Schema};
    use weft_loom::Engine;

    use super::*;

    fn tiny_table() -> RecordBatch {
        let schema = std::sync::Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                std::sync::Arc::new(Int64Array::from(vec![0, 1, 0])),
                std::sync::Arc::new(Int64Array::from(vec![10, 20, 30])),
            ],
        )
        .unwrap()
    }

    async fn plan(sql: &str) -> Result<DistributedQuery> {
        let engine = Engine::new();
        engine.register_batches("t", vec![tiny_table()]).unwrap();
        let lp = engine.logical_plan(sql).await?;
        try_window(&lp, &[])
            .and_then(|o| o.ok_or_else(|| Error::Unsupported("not a window plan".into())))
    }

    #[tokio::test]
    async fn partition_by_window_plans_two_stages() {
        let dq = plan("SELECT k, SUM(v) OVER (PARTITION BY k) AS sv FROM t")
            .await
            .expect("partitioned window should plan");
        assert_eq!(dq.stages.len(), 2);
        assert_eq!(dq.stages[0].hash_key_cols, vec![0]);
        assert!(dq.stages[0].sql.contains("FROM t"));
        assert!(dq.stages[1].sql.contains("OVER"));
    }

    #[tokio::test]
    async fn global_window_is_rejected() {
        let engine = Engine::new();
        engine.register_batches("t", vec![tiny_table()]).unwrap();
        let lp = engine
            .logical_plan("SELECT SUM(v) OVER () AS sv FROM t")
            .await
            .unwrap();
        let err = try_window(&lp, &[]).expect_err("no PARTITION BY");
        let msg = format!("{err}");
        assert!(msg.contains("PARTITION BY"), "got: {msg}");
    }

    #[tokio::test]
    async fn union_distinct_rejected_before_peel() {
        let engine = Engine::new();
        engine.register_batches("t", vec![tiny_table()]).unwrap();
        let lp = engine
            .logical_plan(
                "SELECT k, SUM(v) AS sv FROM t GROUP BY k \
                 UNION SELECT k, SUM(v) AS sv FROM t WHERE v > 1 GROUP BY k",
            )
            .await
            .unwrap();
        let err = try_union_all(&lp, &[]).expect_err("UNION distinct");
        let msg = format!("{err}");
        assert!(msg.contains("UNION"), "got: {msg}");
    }
}

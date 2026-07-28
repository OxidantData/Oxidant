//! Extra plan shapes layered on top of the core aggregation splitter.
//!
//! Kept as a sibling module so parallel edits to [`super::stage_planner`] (broadcast / shuffle
//! joins, HAVING, …) stay low-conflict. Covers:
//!
//! - **Subquery safety**: a fact scanned only inside IN / EXISTS / scalar subqueries can be
//!   gathered once and evaluated on one gated partition; self-subqueries over the driving
//!   sharded fact stay rejected by scan counting.
//! - **UNION ALL** / **UNION** (distinct) / **INTERSECT** / **EXCEPT** of distributable arms
//!   (branch stages + hash-shuffle co-location, then local set / dedup).
//! - **Narrow windows**: aggregate `OVER (PARTITION BY …)` over one sharded table (shuffle by
//!   partition key, then compute locally). Ranking / global windows stay Unsupported.
//! - Explicit **Unsupported** messages for unsupported window / distinct shapes.

use std::collections::HashMap;

use datafusion::logical_expr::expr::{WindowFunction, WindowFunctionDefinition};
use datafusion::logical_expr::{Aggregate, Expr, JoinType, LogicalPlan, Union, Window};
use datafusion::sql::unparser::Unparser;
use weft_common::{Error, Result};

use super::stage_planner::{
    aggregation_stages_for, base_tables, build_agg_remap, column_name, count_table_scans, expr_sql,
    extract_from_tail, peel, qualified_table_sql, sanitize_generated_sql, unqualify,
    DistributedQuery, Peeled,
};
use crate::driver::{ExchangeMode, StageDef};

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

/// Non-aggregate queries: parallel scan on workers (one sharded table) plus gather, with global
/// `ORDER BY` / `LIMIT` in `finalize_sql`. All-replicated scans use a single Forward stage.
pub(crate) fn try_non_aggregate(
    lp: &LogicalPlan,
    replicated: &[&str],
) -> Result<Option<DistributedQuery>> {
    if plan_contains_aggregate(lp) {
        return Ok(None);
    }
    if plan_contains_window(lp) || plan_contains_distinct(lp) {
        return Ok(None);
    }

    let (body, sort, limit) = peel_scan_tail(lp);
    let tables = base_tables(body);
    let sharded: Vec<&str> = tables
        .iter()
        .filter(|t| !replicated.contains(&t.as_str()))
        .map(|t| t.as_str())
        .collect();
    ensure_subquery_tables_replicated(body, &sharded, replicated)?;

    if sharded.len() > 1 {
        return Ok(None);
    }

    if sharded.len() == 1 {
        let sharded_name = sharded[0];
        if count_table_scans(body, sharded_name) > 1 {
            return Err(Error::Unsupported(format!(
                "auto-distribute: sharded table `{sharded_name}` scanned multiple times under scan query"
            )));
        }
    }

    let up = Unparser::default();
    let worker_sql = up
        .plan_to_sql(body)
        .map_err(|e| Error::Unsupported(format!("auto-distribute: unparse scan query: {e}")))?
        .to_string();
    let worker_sql = sanitize_generated_sql(&worker_sql);
    let finalize_sql = build_outer_finalize(sort, limit)?;

    let stage = if sharded.is_empty() {
        StageDef {
            stage_id: 0,
            sql: worker_sql,
            upstream_stage_ids: vec![],
            hash_key_cols: vec![],
            exchange: ExchangeMode::Forward,
            plan_fragment: None,
            lakehouse_snapshot_pins: String::new(),
            replicated_tables: String::new(),
        }
    } else {
        StageDef::new(0, worker_sql, vec![], vec![])
    };

    Ok(Some(DistributedQuery {
        stages: vec![stage],
        finalize_sql,
    }))
}

fn peel_scan_tail(
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
            _ => return (node, sort, limit),
        }
    }
}

fn plan_contains_aggregate(lp: &LogicalPlan) -> bool {
    match lp {
        LogicalPlan::Aggregate(_) => true,
        _ => lp.inputs().iter().any(|n| plan_contains_aggregate(n)),
    }
}

fn plan_contains_window(lp: &LogicalPlan) -> bool {
    match lp {
        LogicalPlan::Window(_) => true,
        _ => lp.inputs().iter().any(|n| plan_contains_window(n)),
    }
}

fn plan_contains_distinct(lp: &LogicalPlan) -> bool {
    match lp {
        LogicalPlan::Distinct(_) => true,
        _ => lp.inputs().iter().any(|n| plan_contains_distinct(n)),
    }
}

/// The top of the plan above a `Window` node: optional projection plus trailing sort/limit.
pub(crate) struct WindowPeeled<'a> {
    /// The query's real output projection — the **first** `Projection` encountered scanning
    /// outer→inner. Anything below it (see `alias_projections`) only renames the window/aggregate
    /// output on the way down.
    pub(crate) projection: Option<&'a [Expr]>,
    pub(crate) sort: Option<&'a [datafusion::logical_expr::SortExpr]>,
    pub(crate) limit: Option<usize>,
    /// Post-window (`HAVING`-equivalent) predicates, outermost first. Every `Filter` crossed on
    /// the way down to the `Window` lands here — TPC-DS Q53/Q63/Q89's `SELECT * FROM (SELECT …
    /// avg(…) OVER (…) avg_x … ) tmp1 WHERE CASE … END > 0.1` puts one here. The original peel
    /// silently discarded these; this is a HAVING-equivalent, not a pre-window predicate, and must
    /// be re-applied over the window's own output.
    pub(crate) having: Vec<&'a Expr>,
    /// `Projection`s found *below* the output projection and above the `Window`, which only
    /// rename the window/aggregate output columns (mirrors [`super::stage_planner::Peeled`]'s
    /// field of the same name). TPC-DS Q53/Q63/Q89 alias `sum_sales`/`avg_*_sales` here, one level
    /// below the outer `SELECT tmp1.…` and the `HAVING`-equivalent `Filter`. Ordered innermost-first.
    pub(crate) alias_projections: Vec<&'a [Expr]>,
    pub(crate) window: &'a Window,
}

fn peel_window(lp: &LogicalPlan) -> Option<WindowPeeled<'_>> {
    let mut limit = None;
    let mut sort = None;
    let mut projection: Option<&[Expr]> = None;
    let mut having: Vec<&Expr> = Vec::new();
    let mut alias_projections: Vec<&[Expr]> = Vec::new();
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
                // Scanning outer→inner, the first `Projection` is the query's real output
                // projection; anything below it only renames columns on the way to the `Window`
                // and folds into the remap instead (see `Peeled`'s identical convention).
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
            LogicalPlan::Window(w) => {
                alias_projections.reverse();
                return Some(WindowPeeled {
                    projection,
                    sort,
                    limit,
                    having,
                    alias_projections,
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

    // When `w.input` is itself an `Aggregate` (TPC-DS Q12/Q20/Q53/Q63/Q89/Q98's
    // `sum(sum(x)) OVER (PARTITION BY …)`), the plain scan path below is wrong twice over: the
    // partial stage would compute a *per-shard* aggregate the final stage never recombines, and
    // the final stage would re-emit the aggregate expression as if it were a plain shuffle_input
    // column (`No field named store_sales.ss_sales_price`). Compose partial-agg → combine →
    // window instead.
    if plan_contains_aggregate(&w.input) {
        return window_over_aggregate_stages_for(p, replicated);
    }

    // The scan path below has no HAVING-equivalent handling — a `Filter` between the window and
    // the output projection would be silently dropped rather than mis-evaluated, so fail closed.
    if !p.having.is_empty() {
        return Err(Error::Unsupported(
            "auto-distribute: a FILTER between a window-over-scan and its output projection is \
             not supported"
                .into(),
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
        .map(column_name)
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
    let partial_sql = sanitize_generated_sql(&format!("SELECT {} {tail}", select_cols.join(", ")));
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

/// Window over an aggregation: `sum(sum(x)) OVER (PARTITION BY g)` / `avg(sum(x)) OVER (PARTITION
/// BY g)`-style TPC-DS shapes (Q12/Q20/Q53/Q63/Q89/Q98).
///
/// Composes three stages instead of the scan path's two:
///
/// 1. **Partial aggregate** per worker (unchanged from a plain distributed aggregation).
/// 2. **Combine**: hash-shuffled by the *aggregate's own* `GROUP BY` key and recombined, exactly
///    as [`aggregation_stages_for`] already does for a non-windowed query — reused verbatim via a
///    synthetic [`Peeled`] with no projection/HAVING/sort/limit, so its output stays in the raw
///    `g{j}`/`r{i}` naming instead of being aliased back to source names.
/// 3. **Window**: the combine stage's output is *re*-shuffled by the window's `PARTITION BY`
///    columns (a subset of the group key in every TPC-DS instance of this shape, so groups that
///    landed on different combine partitions must be gathered again before the window can see the
///    whole partition), then the window aggregate is computed locally and the query's
///    HAVING-equivalent filter / output projection are re-applied.
///
/// Only a window sitting **directly** on a plain `Aggregate` is handled — `aggregation_stages_for`
/// itself still enforces broadcast safety and rejects `ROLLUP`/`CUBE`/`GROUPING SETS`, DISTINCT
/// over 2+ sharded tables needing a join chain, etc., so those surface as their own specific
/// [`Error::Unsupported`] reasons. A window over a `UNION`/`DISTINCT` of aggregates (TPC-DS Q36)
/// is out of scope here and rejected explicitly.
fn window_over_aggregate_stages_for(
    p: &WindowPeeled<'_>,
    replicated: &[&str],
) -> Result<DistributedQuery> {
    let w = p.window;
    let LogicalPlan::Aggregate(agg) = w.input.as_ref() else {
        return Err(Error::Unsupported(
            "auto-distribute: window over an aggregation is only supported when the window sits \
             directly over a GROUP BY (no UNION / DISTINCT underneath it)"
                .into(),
        ));
    };

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

    // Reuse the ordinary aggregation planner verbatim for the partial→combine pipeline: a
    // no-projection/no-HAVING/no-sort/no-limit `Peeled` makes its final stage emit the raw
    // `g{j}`/`r{i}` row (`SELECT * FROM (…) AS combined`) instead of aliasing back to source
    // names, which is exactly the input the window stage below needs. This also gets broadcast
    // safety, ROLLUP rejection, and (if ever needed) the shuffle-join-chain / DISTINCT paths for
    // free, without duplicating any of that logic here.
    let synthetic = Peeled {
        projection: None,
        sort: None,
        limit: None,
        having: Vec::new(),
        alias_projections: Vec::new(),
        agg,
    };
    let mut dq = aggregation_stages_for(&synthetic, replicated)?;

    let n_group = agg.group_expr.len();
    let agg_remap = build_agg_remap(agg);
    let hash_key_cols: Vec<u32> = partition_by
        .iter()
        .map(|e| {
            let key = e.schema_name().to_string();
            agg_remap
                .get(&key)
                .and_then(|name| remap_name_to_index(name, n_group))
                .ok_or_else(|| {
                    Error::Unsupported(format!(
                        "auto-distribute: window PARTITION BY column `{key}` does not map to a \
                         group or aggregate output column"
                    ))
                })
        })
        .collect::<Result<_>>()?;

    // The combine stage (last stage `aggregation_stages_for` produced — stage 1 for the common
    // single-sharded-table broadcast case, or the tail of a shuffle-join chain) currently has an
    // empty hash key because it expected to be the terminal stage. Re-target it at the window's
    // partition columns so the window stage below sees whole partitions.
    let combine = dq.stages.last_mut().ok_or_else(|| {
        Error::Unsupported("auto-distribute: window-over-aggregate produced no stages".into())
    })?;
    combine.hash_key_cols = hash_key_cols;
    let combine_id = combine.stage_id;

    let window_inner = build_window_over_agg_inner(w, agg, &agg_remap)?;
    let full_remap = build_window_over_agg_remap(agg, &w.window_expr, &p.alias_projections);
    let window_sql = wrap_window_over_agg_output(p, &window_inner, &full_remap)?;

    let next_id = dq.stages.iter().map(|s| s.stage_id).max().unwrap_or(0) + 1;
    dq.stages
        .push(StageDef::new(next_id, window_sql, vec![combine_id], vec![]));
    dq.finalize_sql = build_outer_finalize(p.sort, p.limit)?;
    Ok(dq)
}

/// Parse a [`build_agg_remap`] value (`"g{j}"` / `"r{i}"`) into its position in the combine
/// stage's own output row (`g0, …, g{n_group-1}, r0, …`).
fn remap_name_to_index(name: &str, n_group: usize) -> Option<u32> {
    let (prefix, rest) = name.split_at(1);
    let idx: usize = rest.parse().ok()?;
    match prefix {
        "g" => Some(idx as u32),
        "r" => Some((n_group + idx) as u32),
        _ => None,
    }
}

/// The window stage's own `SELECT`: every combine-stage column passed through unchanged, plus one
/// `w{k}` per window expression. `validate_window_func` has already rejected DISTINCT / FILTER /
/// ORDER BY, so each window function is exactly `func(arg?) OVER (PARTITION BY …)` — safe to
/// re-emit as plain SQL text once `arg` and the partition columns are remapped to `g{j}`/`r{i}`.
fn build_window_over_agg_inner(
    w: &Window,
    agg: &Aggregate,
    remap: &HashMap<String, String>,
) -> Result<String> {
    let n_group = agg.group_expr.len();
    let n_agg = agg.aggr_expr.len();
    let mut cols: Vec<String> = (0..n_group).map(|j| format!("g{j}")).collect();
    cols.extend((0..n_agg).map(|i| format!("r{i}")));

    for (k, e) in w.window_expr.iter().enumerate() {
        let Expr::WindowFunction(wf) = e else {
            return Err(Error::Unsupported(format!(
                "auto-distribute: non-window expression in window list: {e}"
            )));
        };
        let func_name = match &wf.fun {
            WindowFunctionDefinition::AggregateUDF(f) => f.name().to_ascii_lowercase(),
            WindowFunctionDefinition::WindowUDF(f) => f.name().to_ascii_lowercase(),
        };
        let arg_sql = match wf.params.args.first() {
            Some(arg) => {
                let key = arg.schema_name().to_string();
                remap.get(&key).cloned().ok_or_else(|| {
                    Error::Unsupported(format!(
                        "auto-distribute: window argument `{key}` does not map to a group or \
                         aggregate output column"
                    ))
                })?
            }
            None => "1".to_string(), // count(*)-style window carries no arg
        };
        let part_sql: Vec<String> = wf
            .params
            .partition_by
            .iter()
            .map(|pb| {
                let key = pb.schema_name().to_string();
                remap.get(&key).cloned().ok_or_else(|| {
                    Error::Unsupported(format!(
                        "auto-distribute: window PARTITION BY column `{key}` does not map to a \
                         group or aggregate output column"
                    ))
                })
            })
            .collect::<Result<_>>()?;
        cols.push(format!(
            "{func_name}({arg_sql}) OVER (PARTITION BY {}) AS w{k}",
            part_sql.join(", ")
        ));
    }
    Ok(format!("SELECT {} FROM shuffle_input", cols.join(", ")))
}

/// [`build_agg_remap`] extended with each window expression's own schema name (→ `w{i}`) and
/// [`WindowPeeled::alias_projections`] — mirrors [`super::stage_planner::build_remap`], which does
/// the same for [`Peeled::alias_projections`], so a HAVING-equivalent filter or output projection
/// written against an intervening subquery's aliases (TPC-DS Q53/Q63/Q89's `sum_sales` /
/// `avg_*_sales`) still resolves to `g{j}`/`r{i}`/`w{i}`.
fn build_window_over_agg_remap(
    agg: &Aggregate,
    window_expr: &[Expr],
    alias_projections: &[&[Expr]],
) -> HashMap<String, String> {
    let mut remap = build_agg_remap(agg);
    for (i, e) in window_expr.iter().enumerate() {
        remap.insert(e.schema_name().to_string(), format!("w{i}"));
    }
    for proj in alias_projections {
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
    remap
}

/// Wrap the window stage's inner query so its output matches the original query's columns —
/// mirrors [`super::stage_planner::wrap_output`] (private to that module, so re-implemented here
/// rather than imported): re-apply the HAVING-equivalent filter (if any) against the remapped
/// `g{j}`/`r{i}`/`w{i}` columns, then the output projection, each item explicitly aliased back to
/// its original output name.
fn wrap_window_over_agg_output(
    p: &WindowPeeled<'_>,
    inner: &str,
    remap: &HashMap<String, String>,
) -> Result<String> {
    let up = Unparser::default();
    let from_sql = if p.having.is_empty() {
        format!("({inner}) AS combined")
    } else {
        let mut preds = Vec::with_capacity(p.having.len());
        for pred in &p.having {
            let mapped = remap_expr_columns(&unqualify(pred), remap);
            ensure_all_window_columns_remapped(&mapped)?;
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
                let sql = expr_sql(&up, &remap_expr_columns(strip_alias(e), remap))?;
                Ok(format!("{sql} AS \"{name}\""))
            })
            .collect::<Result<Vec<_>>>()?
            .join(", "),
        None => "*".to_string(),
    };
    Ok(format!("SELECT {select} FROM {from_sql}"))
}

/// Replace any column reference whose flat name is in `remap` with the safe-named column. Local
/// twin of [`super::stage_planner::remap_columns`] (private to that module).
fn remap_expr_columns(e: &Expr, remap: &HashMap<String, String>) -> Expr {
    use datafusion::common::tree_node::{Transformed, TreeNode};
    e.clone()
        .transform(|node| {
            if let Expr::Column(c) = &node {
                if let Some(safe) = remap.get(&c.flat_name()).or_else(|| remap.get(&c.name)) {
                    return Ok(Transformed::yes(datafusion::prelude::col(safe)));
                }
            }
            Ok(Transformed::no(node))
        })
        .map(|t| t.data)
        .unwrap_or(e.clone())
}

/// Require every column in an already-remapped predicate to name a `g{j}` / `r{i}` / `w{i}` stage
/// column. Local twin of [`super::stage_planner::ensure_all_columns_remapped`] (private to that
/// module), extended to also accept `w{i}` window-output columns.
fn ensure_all_window_columns_remapped(e: &Expr) -> Result<()> {
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
    let mut bad: Option<String> = None;
    let _ = e.apply(|node| {
        if let Expr::Column(c) = node {
            let safe = c.relation.is_none()
                && matches!(c.name.as_bytes(), [b'g' | b'r' | b'w', rest @ ..]
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
            "auto-distribute: window-stage filter references `{name}`, which does not map to a \
             group, aggregate, or window output column"
        ))),
        None => Ok(()),
    }
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
        // shuffle_input has no table qualifier — strip relation prefixes from the OVER clause.
        let sql = expr_sql(up, &unqualify(e))?;
        parts.push(format!("{sql} AS w{i}"));
    }
    Ok(format!("SELECT {} FROM shuffle_input", parts.join(", ")))
}

fn wrap_window_output(
    p: &WindowPeeled<'_>,
    inner: &str,
    remap: &HashMap<String, String>,
) -> Result<String> {
    let from_sql = format!("({inner}) AS combined");
    let select = match p.projection {
        Some(exprs) => exprs
            .iter()
            .map(|e| {
                let name = output_name(e);
                // Prefer remapped window alias (w0, …); otherwise the bare output column name.
                // Never emit relation-qualified refs — `combined` has no `t.` prefix.
                let stripped = strip_alias(e);
                let key = stripped.schema_name().to_string();
                let src = remap
                    .get(&key)
                    .cloned()
                    .or_else(|| column_name(stripped).ok())
                    .unwrap_or_else(|| name.clone());
                Ok::<_, Error>(format!("{src} AS \"{name}\""))
            })
            .collect::<Result<Vec<_>>>()?
            .join(", "),
        None => "*".to_string(),
    };
    Ok(format!("SELECT {select} FROM {from_sql}"))
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

/// Set ops / distinct: `UNION ALL`, `UNION` (distinct), `INTERSECT` / `EXCEPT` (as semi/anti),
/// and a top-level `DISTINCT` over a distributable aggregation. Otherwise `Ok(None)`.
pub(crate) fn try_union_all(
    lp: &LogicalPlan,
    replicated: &[&str],
) -> Result<Option<DistributedQuery>> {
    let (inner, sort, limit) = peek_sort_limit(lp);
    // Strip projections / aliases so `SELECT … FROM (… UNION …) alias` still matches.
    let mut inner = inner;
    loop {
        match inner {
            LogicalPlan::Projection(p) => inner = p.input.as_ref(),
            LogicalPlan::SubqueryAlias(s) => inner = s.input.as_ref(),
            LogicalPlan::Filter(f) => inner = f.input.as_ref(),
            _ => break,
        }
    }
    match inner {
        LogicalPlan::Distinct(d) => match d.input().as_ref() {
            LogicalPlan::Union(u) => Ok(Some(plan_union(
                u,
                replicated,
                sort,
                limit,
                SetCombine::UnionDistinct,
            )?)),
            other => {
                // `SELECT DISTINCT …` over a distributable aggregation: plan the agg, then
                // hash-shuffle the full row and dedup locally.
                if let Ok(peeled) = peel(other) {
                    if peeled.sort.is_some() || peeled.limit.is_some() {
                        return Err(Error::Unsupported(
                            "auto-distribute: DISTINCT over ORDER BY/LIMIT is not supported".into(),
                        ));
                    }
                    let n_cols = other.schema().fields().len() as u32;
                    if n_cols == 0 {
                        return Err(Error::Unsupported(
                            "auto-distribute: DISTINCT over empty schema".into(),
                        ));
                    }
                    let mut dq = aggregation_stages_for(&peeled, replicated)?;
                    append_full_row_dedup(&mut dq, n_cols, sort, limit)?;
                    return Ok(Some(dq));
                }
                // Non-aggregate DISTINCT (e.g. `SELECT DISTINCT col FROM t WHERE …`): reuse the
                // scan/forward path, then hash-shuffle full rows and dedup.
                if let Some(mut dq) = try_non_aggregate(other, replicated)? {
                    let n_cols = other.schema().fields().len() as u32;
                    if n_cols == 0 {
                        return Err(Error::Unsupported(
                            "auto-distribute: DISTINCT over empty schema".into(),
                        ));
                    }
                    // try_non_aggregate may already have set finalize for sort/limit on `other`;
                    // outer sort/limit (peeled above Distinct) wins when present.
                    if sort.is_some() || limit.is_some() {
                        dq.finalize_sql = build_outer_finalize(sort, limit)?;
                    }
                    append_full_row_dedup(&mut dq, n_cols, None, None)?;
                    return Ok(Some(dq));
                }
                Ok(None)
            }
        },
        LogicalPlan::Union(u) => Ok(Some(plan_union(
            u,
            replicated,
            sort,
            limit,
            SetCombine::UnionAll,
        )?)),
        LogicalPlan::Join(j)
            if matches!(
                j.join_type,
                JoinType::LeftSemi | JoinType::LeftAnti | JoinType::RightSemi | JoinType::RightAnti
            ) =>
        {
            // DataFusion lowers INTERSECT/EXCEPT to semi/anti joins. Plan both arms, hash the
            // full row for co-location, then run INTERSECT/EXCEPT locally.
            Ok(Some(plan_semi_anti_set_op(j, replicated, sort, limit)?))
        }
        _ => Ok(None),
    }
}

#[derive(Clone, Copy)]
enum SetCombine {
    UnionAll,
    UnionDistinct,
}

/// Hash-partition the last stage on every output column, then `SELECT DISTINCT *`.
fn append_full_row_dedup(
    dq: &mut DistributedQuery,
    n_cols: u32,
    sort: Option<&[datafusion::logical_expr::SortExpr]>,
    limit: Option<usize>,
) -> Result<()> {
    let last = dq.stages.last_mut().ok_or_else(|| {
        Error::Unsupported("auto-distribute: DISTINCT has no stages to dedup".into())
    })?;
    last.hash_key_cols = (0..n_cols).collect();
    let dedup_id = last.stage_id.saturating_add(1);
    let upstream = last.stage_id;
    dq.stages.push(StageDef::new(
        dedup_id,
        "SELECT DISTINCT * FROM shuffle_input".to_string(),
        vec![upstream],
        vec![],
    ));
    if dq.finalize_sql.is_none() {
        dq.finalize_sql = build_outer_finalize(sort, limit)?;
    }
    Ok(())
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
            LogicalPlan::SubqueryAlias(s) => node = s.input.as_ref(),
            LogicalPlan::Window(_) => {
                return Err(Error::Unsupported(
                    "auto-distribute: window functions are not supported \
                     (no PARTITION BY shuffle path matched) — falling back to local execution"
                        .into(),
                ));
            }
            LogicalPlan::Distinct(_) => {
                return Err(Error::Unsupported(
                    "auto-distribute: DISTINCT (or UNION distinct) is not supported".into(),
                ));
            }
            _ => return Ok(()),
        }
    }
}

/// Materialize one fact that is sharded **only inside expression subqueries**, then evaluate the
/// original query exactly once against the gathered rows.
///
/// Running a correlated `EXISTS` / `NOT EXISTS` independently on each fact shard is not correct:
/// existence is global, and an outer query over replicated tables would also be emitted once per
/// worker. This path instead emits:
///
/// 1. a scatter scan of the subquery fact, gathered to shuffle partition 0 (empty hash key);
/// 2. a one-row gate, likewise gathered to partition 0;
/// 3. the original aggregate query with that fact's table sources replaced by `shuffle_input_0`
///    and a top-level `HAVING` gate on `shuffle_input_1`, so every non-zero output partition is
///    empty while the original `ORDER BY` / `LIMIT` remains outermost.
///
/// The gate is separate from the fact because the fact may be globally empty: `NOT EXISTS` still
/// has to run once in that case. No stage uses [`ExchangeMode::Forward`], so workers really do read
/// their local shards before the final single-partition evaluation.
///
/// A fact also scanned by the outer plan is deliberately excluded. Gathering and replacing every
/// such scan would be correct but could centralize the entire driving query; replacing only the
/// subquery scan would not be correct for self-correlated predicates. Those shapes retain the
/// existing scan-count rejection until a key-based semi/anti shuffle is implemented.
pub(crate) fn try_materialize_subquery_fact(
    lp: &LogicalPlan,
    replicated: &[&str],
) -> Result<Option<DistributedQuery>> {
    let outer_tables = base_tables(lp);
    let mut subquery_tables = Vec::new();
    collect_subquery_tables(lp, &mut subquery_tables);

    let unsafe_tables: Vec<&str> = subquery_tables
        .iter()
        .map(String::as_str)
        .filter(|table| !replicated.contains(table))
        .collect();
    let [fact] = unsafe_tables.as_slice() else {
        return Ok(None);
    };
    if outer_tables.iter().any(|table| table == fact) {
        return Ok(None);
    }
    if !plan_contains_aggregate(lp) {
        return Ok(None);
    }

    let expected_scans = count_table_scans(lp, fact);
    if expected_scans == 0 {
        return Err(Error::Unsupported(format!(
            "auto-distribute: subquery fact `{fact}` was collected but has no table scan"
        )));
    }

    let original_sql = Unparser::default()
        .plan_to_sql(lp)
        .map_err(|e| Error::Unsupported(format!("auto-distribute: unparse subquery query: {e}")))?
        .to_string();
    let (rewritten_sql, replaced_scans) =
        rewrite_table_factors(&original_sql, fact, "shuffle_input_0")?;
    if replaced_scans != expected_scans {
        return Err(Error::Unsupported(format!(
            "auto-distribute: subquery fact `{fact}` has {expected_scans} logical scans but \
             generated SQL exposed {replaced_scans} replaceable table sources"
        )));
    }

    // The output stage runs on every rendezvous partition. Only partition 0 receives the gate,
    // including when the gathered fact itself has zero rows. A top-level HAVING suppresses the
    // otherwise-present zero-input global-aggregate row on every other partition without wrapping
    // (and thereby semantically hiding) the original ORDER BY.
    let final_sql = sanitize_generated_sql(&add_partition_gate(&rewritten_sql)?);
    let fact_sql = qualified_table_sql(lp, fact);
    Ok(Some(DistributedQuery {
        stages: vec![
            StageDef::new(
                0,
                sanitize_generated_sql(&format!("SELECT * FROM {fact_sql}")),
                vec![],
                vec![],
            ),
            StageDef::new(
                1,
                "SELECT 1 AS __weft_subquery_gate".to_string(),
                vec![],
                vec![],
            ),
            StageDef::new(2, final_sql, vec![0, 1], vec![]),
        ],
        finalize_sql: None,
    }))
}

/// Correctness-first fallback for a single fact whose shape cannot stay shard-local.
///
/// Some plans need a *global* view of one otherwise-sharded fact:
///
/// - self-joins / correlated scalar subqueries scan that fact more than once;
/// - a FULL OUTER JOIN preserves a replicated side independently of the local fact shard;
/// - a set operation containing a global/ranking window cannot distribute its arms independently.
///
/// Running those shapes per shard is incorrect. Instead, gather the fact to partition 0, replace
/// every occurrence of its table source in the original unparsed query with that gathered input,
/// and evaluate the unchanged query there. A separately-gathered one-row gate suppresses output on
/// every other partition, including the synthetic row a global aggregate emits for empty input.
///
/// This deliberately remains a bounded fallback rather than a general "gather anything" planner:
/// ordinary one-scan aggregates, joins, and set operations must continue through their parallel
/// shape-specific paths.
pub(crate) fn try_materialize_complex_fact(
    lp: &LogicalPlan,
    replicated: &[&str],
) -> Result<Option<DistributedQuery>> {
    let mut tables = base_tables(lp);
    let mut subquery_tables = Vec::new();
    collect_subquery_tables(lp, &mut subquery_tables);
    tables.extend(subquery_tables);
    tables.sort();
    tables.dedup();

    let sharded: Vec<&str> = tables
        .iter()
        .map(String::as_str)
        .filter(|table| !replicated.contains(table))
        .collect();
    let [fact] = sharded.as_slice() else {
        return Ok(None);
    };
    // COUNT(DISTINCT) is correct under gather (full fact on partition 0). Previously declined
    // to keep the parallel path's shape error visible; the gather fallback is now the deliberate
    // home for that shape (TPC-DS Q28).

    // Grouping sets are gatherable when the Unparser round-trip is faithful (TPC-DS Q67/Q70/Q86
    // ROLLUP+rank verified exact). Decline the known-broken compositions:
    // - ROLLUP + UNION ALL (Q5/Q77/Q80): gather still mismatched single-node at sf=0.01
    // - ROLLUP + INTERSECT/EXCEPT (Q14): Unparser emits out-of-scope `brand_id` aliases
    if plan_contains_grouping_set(lp)
        && (plan_contains_union(lp) || plan_contains_intersect_or_except(lp))
    {
        return Ok(None);
    }

    // Admission for the gather fallback is driven by the caller's rejection list; once we are
    // here with a single sharded fact, gather it.

    // Keep global ORDER BY / LIMIT outside the gathered worker stage. A Sort hidden inside the
    // gating subquery is not an ordering guarantee, and LIMIT must be applied only after that
    // ordering. This mirrors every other distributed shape's driver-side finalize.
    let (body, sort, limit) = peek_sort_limit(lp);
    let original_sql = Unparser::default()
        .plan_to_sql(body)
        .map_err(|e| {
            Error::Unsupported(format!("auto-distribute: unparse gathered fact query: {e}"))
        })?
        .to_string();
    let scans = count_table_scans(lp, fact);
    let (rewritten_sql, replaced_scans) =
        rewrite_table_factors(&original_sql, fact, "shuffle_input_0")?;
    if replaced_scans == 0 {
        return Err(Error::Unsupported(format!(
            "auto-distribute: gathered fact `{fact}` has {scans} logical scans but generated SQL \
             exposed 0 replaceable table sources"
        )));
    }
    // DataFusion inlines CTEs into the logical plan, so `count_table_scans` can exceed the
    // number of table factors the Unparser emits (TPC-DS Q1: two logical scans of
    // `store_returns` via a shared CTE, one SQL occurrence). As long as every SQL occurrence
    // was rewritten, the gathered stage sees the full fact once and the CTE body is correct.
    if replaced_scans > scans {
        return Err(Error::Unsupported(format!(
            "auto-distribute: gathered fact `{fact}` has {scans} logical scans but generated SQL \
             exposed {replaced_scans} replaceable table sources"
        )));
    }

    // Keep the original query in its own scope. Filtering that result by the partition-0 gate is
    // valid for grouped, global-aggregate, window, and set-op roots alike; injecting a WHERE into
    // the original query would be wrong for zero-input global aggregates.
    let final_sql = sanitize_generated_sql(&format!(
        "SELECT gathered_fact.* FROM ({rewritten_sql}) AS gathered_fact \
         WHERE EXISTS (SELECT 1 FROM shuffle_input_1)"
    ));
    let fact_sql = qualified_table_sql(lp, fact);
    Ok(Some(DistributedQuery {
        stages: vec![
            StageDef::new(
                0,
                sanitize_generated_sql(&format!("SELECT * FROM {fact_sql}")),
                vec![],
                vec![],
            ),
            StageDef::new(
                1,
                "SELECT 1 AS __weft_materialize_gate".to_string(),
                vec![],
                vec![],
            ),
            StageDef::new(2, final_sql, vec![0, 1], vec![]),
        ],
        finalize_sql: build_outer_finalize(sort, limit)?,
    }))
}

fn plan_contains_union(lp: &LogicalPlan) -> bool {
    matches!(lp, LogicalPlan::Union(_))
        || lp.inputs().iter().any(|input| plan_contains_union(input))
}

/// True when `lp` contains an INTERSECT/EXCEPT (DataFusion lowers these to semi/anti joins).
fn plan_contains_intersect_or_except(lp: &LogicalPlan) -> bool {
    let local = match lp {
        LogicalPlan::Join(j) => matches!(
            j.join_type,
            JoinType::LeftSemi | JoinType::LeftAnti | JoinType::RightSemi | JoinType::RightAnti
        ),
        _ => false,
    };
    local
        || lp
            .inputs()
            .iter()
            .any(|input| plan_contains_intersect_or_except(input))
}

fn plan_contains_grouping_set(lp: &LogicalPlan) -> bool {
    let local = match lp {
        LogicalPlan::Aggregate(aggregate) => aggregate
            .group_expr
            .iter()
            .any(|expr| matches!(expr, Expr::GroupingSet(_))),
        _ => false,
    };
    local
        || lp
            .inputs()
            .iter()
            .any(|input| plan_contains_grouping_set(input))
}

/// Parse generated SQL and rewrite only named table factors, never similarly-named columns or
/// string literals. An unaliased source receives its old name as an alias so qualified references
/// such as `web_sales.ws_order_number` keep resolving after the source becomes `shuffle_input_0`.
fn rewrite_table_factors(sql: &str, table: &str, replacement: &str) -> Result<(String, usize)> {
    use std::ops::ControlFlow;

    use datafusion::sql::sqlparser::ast::{
        Ident, ObjectName, ObjectNamePart, TableAlias, TableFactor, VisitMut, VisitorMut,
    };
    use datafusion::sql::sqlparser::dialect::GenericDialect;
    use datafusion::sql::sqlparser::parser::Parser;

    struct Rewriter<'a> {
        table: &'a str,
        replacement: &'a str,
        count: usize,
    }

    impl VisitorMut for Rewriter<'_> {
        type Break = ();

        fn pre_visit_table_factor(&mut self, factor: &mut TableFactor) -> ControlFlow<Self::Break> {
            let TableFactor::Table {
                name, alias, args, ..
            } = factor
            else {
                return ControlFlow::Continue(());
            };
            if args.is_some() {
                return ControlFlow::Continue(());
            }
            let Some(original) = name.0.last().and_then(ObjectNamePart::as_ident).cloned() else {
                return ControlFlow::Continue(());
            };
            if !original.value.eq_ignore_ascii_case(self.table) {
                return ControlFlow::Continue(());
            }

            if alias.is_none() {
                *alias = Some(TableAlias {
                    explicit: true,
                    name: original,
                    columns: vec![],
                    at: None,
                });
            }
            *name = ObjectName(vec![ObjectNamePart::Identifier(Ident::new(
                self.replacement,
            ))]);
            self.count += 1;
            ControlFlow::Continue(())
        }
    }

    let mut statements = Parser::parse_sql(&GenericDialect {}, sql).map_err(|e| {
        Error::Unsupported(format!(
            "auto-distribute: parse generated SQL for subquery materialization: {e}"
        ))
    })?;
    if statements.len() != 1 {
        return Err(Error::Unsupported(format!(
            "auto-distribute: subquery materialization expected one statement, found {}",
            statements.len()
        )));
    }
    let mut rewriter = Rewriter {
        table,
        replacement,
        count: 0,
    };
    let _ = statements.visit(&mut rewriter);
    Ok((statements.remove(0).to_string(), rewriter.count))
}

/// Add the partition-0 sentinel as a top-level HAVING predicate. This path is intentionally
/// aggregate-only: HAVING is the one gate that suppresses both grouped rows and the synthetic
/// zero-input row of a global aggregate, while leaving ORDER BY / LIMIT in their original outer
/// query scope.
fn add_partition_gate(sql: &str) -> Result<String> {
    use datafusion::sql::sqlparser::ast::{BinaryOperator, Expr as SqlExpr, SetExpr, Statement};
    use datafusion::sql::sqlparser::dialect::GenericDialect;
    use datafusion::sql::sqlparser::parser::Parser;

    let mut statements = Parser::parse_sql(&GenericDialect {}, sql).map_err(|e| {
        Error::Unsupported(format!(
            "auto-distribute: parse generated SQL for subquery partition gate: {e}"
        ))
    })?;
    if statements.len() != 1 {
        return Err(Error::Unsupported(format!(
            "auto-distribute: subquery partition gate expected one statement, found {}",
            statements.len()
        )));
    }
    let Some(Statement::Query(query)) = statements.get_mut(0) else {
        return Err(Error::Unsupported(
            "auto-distribute: subquery materialization expected a SELECT query".into(),
        ));
    };

    let mut gate_statements = Parser::parse_sql(
        &GenericDialect {},
        "SELECT 1 HAVING EXISTS (SELECT 1 FROM shuffle_input_1)",
    )
    .map_err(|e| {
        Error::Unsupported(format!(
            "auto-distribute: build subquery partition gate expression: {e}"
        ))
    })?;
    let Some(Statement::Query(gate_query)) = gate_statements.pop() else {
        return Err(Error::Unsupported(
            "auto-distribute: failed to build subquery partition gate query".into(),
        ));
    };
    let SetExpr::Select(gate_select) = gate_query.body.as_ref() else {
        return Err(Error::Unsupported(
            "auto-distribute: failed to build subquery partition gate SELECT".into(),
        ));
    };
    let gate = gate_select.having.clone().ok_or_else(|| {
        Error::Unsupported("auto-distribute: subquery partition gate has no HAVING".into())
    })?;

    let SetExpr::Select(select) = query.body.as_mut() else {
        return Err(Error::Unsupported(
            "auto-distribute: subquery materialization requires a top-level aggregate SELECT \
             (set operations remain on their dedicated planner path)"
                .into(),
        ));
    };
    select.having = Some(match select.having.take() {
        Some(existing) => SqlExpr::BinaryOp {
            left: Box::new(existing),
            op: BinaryOperator::And,
            right: Box::new(gate),
        },
        None => gate,
    });
    Ok(statements.remove(0).to_string())
}

/// Every table scanned inside expression subqueries (EXISTS / IN / scalar) must be **replicated**.
///
/// A subquery over the driving sharded fact is never shard-local-safe (HAVING thresholds like
/// TPC-H Q11, correlated filters, etc.) — those shapes gather via `try_materialize_complex_fact`
/// after this reject (see `plan_distributed_logical` materializable_rejection for `subquery over`).
pub(crate) fn ensure_subquery_tables_replicated(
    lp: &LogicalPlan,
    _sharded: &[&str],
    replicated: &[&str],
) -> Result<()> {
    let mut tables = Vec::new();
    collect_subquery_tables(lp, &mut tables);
    for t in &tables {
        if !replicated.contains(&t.as_str()) {
            return Err(Error::Unsupported(format!(
                "auto-distribute: subquery over `{t}` is only safe when that table is replicated"
            )));
        }
    }
    Ok(())
}

/// Same rule for post-aggregate `HAVING` predicates (not visible to [`collect_subquery_tables`] on
/// the aggregate input alone).
pub(crate) fn ensure_having_subquery_tables_replicated(
    having: &[&Expr],
    replicated: &[&str],
) -> Result<()> {
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
    for pred in having {
        let mut bad: Option<String> = None;
        let _ = pred.apply(|expr| {
            let subquery = match expr {
                Expr::InSubquery(iq) => Some(iq.subquery.subquery.as_ref()),
                Expr::ScalarSubquery(sq) => Some(sq.subquery.as_ref()),
                Expr::Exists(ex) => Some(ex.subquery.subquery.as_ref()),
                _ => None,
            };
            if let Some(lp) = subquery {
                let mut tables = base_tables(lp);
                collect_subquery_tables(lp, &mut tables);
                for t in &tables {
                    if !replicated.contains(&t.as_str()) {
                        bad = Some(t.clone());
                        return Ok(TreeNodeRecursion::Stop);
                    }
                }
            }
            Ok(TreeNodeRecursion::Continue)
        });
        if let Some(t) = bad {
            return Err(Error::Unsupported(format!(
                "auto-distribute: subquery over `{t}` is only safe when that table is replicated"
            )));
        }
    }
    Ok(())
}

/// Plan one arm of a top-level `UNION` / `UNION ALL` / `INTERSECT` / `EXCEPT`.
///
/// [`aggregation_stages_for`] requires *exactly one* sharded base table (it needs somewhere to
/// hash-shuffle partial aggregates from) and errors on zero — but a per-channel set op (e.g.
/// TPC-DS Q4/Q11/Q74's `year_total` CTE, `UNION ALL`-ing a `store_sales` arm with a `web_sales`
/// arm) plans each arm against the *same* `replicated` list, so whichever fact a given arm's
/// `GROUP BY` doesn't use is — for this run — a fully replicated table, not a missing one. Every
/// worker already holds that arm's complete input, so its `GROUP BY` is exact if computed once:
/// run the arm's whole SQL as a single [`ExchangeMode::Forward`] stage instead of erroring. The
/// caller unions this arm's single-row-per-worker output with the genuinely sharded arms' hash-
/// shuffled partials via a plain gather (`UNION ALL` needs no co-location), so a value produced
/// once and gathered to partition 0 combines correctly alongside partials produced per-worker and
/// gathered to the same partition.
fn arm_stages_for(
    arm: &LogicalPlan,
    peeled: &Peeled<'_>,
    replicated: &[&str],
    label: &str,
    arm_i: usize,
) -> Result<DistributedQuery> {
    let has_sharded_table = base_tables(&peeled.agg.input)
        .iter()
        .any(|t| !replicated.contains(&t.as_str()));
    if has_sharded_table {
        return aggregation_stages_for(peeled, replicated);
    }
    let sql = Unparser::default()
        .plan_to_sql(arm)
        .map_err(|e| {
            Error::Unsupported(format!(
                "auto-distribute: unparse {label} arm {arm_i} (all-replicated): {e}"
            ))
        })?
        .to_string();
    let mut stage = StageDef::new(0, sanitize_generated_sql(&sql), vec![], vec![]);
    stage.exchange = ExchangeMode::Forward;
    Ok(DistributedQuery {
        stages: vec![stage],
        finalize_sql: None,
    })
}

fn plan_union(
    u: &Union,
    replicated: &[&str],
    sort: Option<&[datafusion::logical_expr::SortExpr]>,
    limit: Option<usize>,
    combine: SetCombine,
) -> Result<DistributedQuery> {
    if u.inputs.len() < 2 {
        return Err(Error::Unsupported(
            "auto-distribute: UNION needs at least two arms".into(),
        ));
    }

    let mut stages: Vec<StageDef> = Vec::new();
    let mut arm_output_ids: Vec<u32> = Vec::new();
    let mut next_id: u32 = 0;
    let label = match combine {
        SetCombine::UnionAll => "UNION ALL",
        SetCombine::UnionDistinct => "UNION",
    };

    for (arm_i, arm) in u.inputs.iter().enumerate() {
        let peeled = peel(arm).map_err(|e| {
            Error::Unsupported(format!(
                "auto-distribute: {label} arm {arm_i} is not a distributable aggregation: {e}"
            ))
        })?;
        // Per-arm ORDER BY / LIMIT under UNION is unexpected (optimizer lifts them); reject rather
        // than silently drop.
        if peeled.sort.is_some() || peeled.limit.is_some() {
            return Err(Error::Unsupported(format!(
                "auto-distribute: {label} arm {arm_i} has ORDER BY/LIMIT; apply them outside the UNION"
            )));
        }
        let arm_dq = arm_stages_for(arm, &peeled, replicated, label, arm_i)?;
        let id_offset = next_id;
        for s in arm_dq.stages {
            let new_id = s.stage_id + id_offset;
            stages.push(StageDef {
                stage_id: new_id,
                sql: s.sql,
                upstream_stage_ids: s
                    .upstream_stage_ids
                    .into_iter()
                    .map(|uid| uid + id_offset)
                    .collect(),
                hash_key_cols: s.hash_key_cols,
                exchange: s.exchange,
                plan_fragment: s.plan_fragment,
                lakehouse_snapshot_pins: s.lakehouse_snapshot_pins,
                replicated_tables: String::new(),
            });
            next_id = next_id.max(new_id + 1);
        }
        let arm_out = stages.last().map(|s| s.stage_id).ok_or_else(|| {
            Error::Unsupported(format!(
                "auto-distribute: {label} arm {arm_i} produced no stages"
            ))
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

    let n_cols = u.schema.fields().len() as u32;
    let union_id = next_id;
    match combine {
        SetCombine::UnionAll => {
            // Gather (empty hash) — UNION ALL needs no co-location.
            stages.push(StageDef::new(union_id, union_sql, arm_output_ids, vec![]));
            Ok(DistributedQuery {
                stages,
                finalize_sql: build_outer_finalize(sort, limit)?,
            })
        }
        SetCombine::UnionDistinct => {
            // Hash-shuffle the concatenated rows on the full output row, then dedup locally.
            if n_cols == 0 {
                return Err(Error::Unsupported(
                    "auto-distribute: UNION distinct over empty schema".into(),
                ));
            }
            stages.push(StageDef::new(
                union_id,
                union_sql,
                arm_output_ids,
                (0..n_cols).collect(),
            ));
            let dedup_id = union_id + 1;
            stages.push(StageDef::new(
                dedup_id,
                "SELECT DISTINCT * FROM shuffle_input".to_string(),
                vec![union_id],
                vec![],
            ));
            Ok(DistributedQuery {
                stages,
                finalize_sql: build_outer_finalize(sort, limit)?,
            })
        }
    }
}

/// Plan INTERSECT / EXCEPT when DataFusion has lowered them to a semi/anti join of two arms.
///
/// Both sides are planned as independent stage DAGs (same offset trick as UNION). Their outputs
/// are hash-shuffled on the full row so equal rows co-locate; the final stage runs
/// `INTERSECT` / `EXCEPT` (or `… ALL`) locally.
fn plan_semi_anti_set_op(
    join: &datafusion::logical_expr::Join,
    replicated: &[&str],
    sort: Option<&[datafusion::logical_expr::SortExpr]>,
    limit: Option<usize>,
) -> Result<DistributedQuery> {
    let (op_sql, is_all) = match join.join_type {
        JoinType::LeftSemi | JoinType::RightSemi => {
            // DISTINCT INTERSECT inserts Distinct on the left before the semi join.
            let is_all = !matches!(join.left.as_ref(), LogicalPlan::Distinct(_));
            ("INTERSECT", is_all)
        }
        JoinType::LeftAnti | JoinType::RightAnti => {
            let is_all = !matches!(join.left.as_ref(), LogicalPlan::Distinct(_));
            ("EXCEPT", is_all)
        }
        other => {
            return Err(Error::Unsupported(format!(
                "auto-distribute: set-op path got unexpected join type {other:?}"
            )));
        }
    };
    let quant = if is_all { " ALL" } else { "" };
    let label = format!("{op_sql}{quant}");

    // Strip Distinct wrapper on the left (INTERSECT/EXCEPT DISTINCT).
    let left_lp = match join.left.as_ref() {
        LogicalPlan::Distinct(d) => d.input().as_ref(),
        other => other,
    };
    let right_lp = join.right.as_ref();

    let mut stages: Vec<StageDef> = Vec::new();
    let mut next_id: u32 = 0;
    let mut arm_outs = Vec::with_capacity(2);
    for (arm_i, arm) in [left_lp, right_lp].into_iter().enumerate() {
        let peeled = peel(arm).map_err(|e| {
            Error::Unsupported(format!(
                "auto-distribute: {label} arm {arm_i} is not a distributable aggregation: {e}"
            ))
        })?;
        if peeled.sort.is_some() || peeled.limit.is_some() {
            return Err(Error::Unsupported(format!(
                "auto-distribute: {label} arm {arm_i} has ORDER BY/LIMIT; apply them outside the set op"
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
                    .map(|uid| uid + id_offset)
                    .collect(),
                hash_key_cols: s.hash_key_cols,
                exchange: s.exchange,
                plan_fragment: s.plan_fragment,
                lakehouse_snapshot_pins: s.lakehouse_snapshot_pins,
                replicated_tables: String::new(),
            });
            next_id = next_id.max(new_id + 1);
        }
        let arm_out = stages.last().map(|s| s.stage_id).ok_or_else(|| {
            Error::Unsupported(format!(
                "auto-distribute: {label} arm {arm_i} produced no stages"
            ))
        })?;
        // Re-hash arm output on the full row so equal rows from both arms co-locate.
        let n_cols = arm.schema().fields().len() as u32;
        if n_cols == 0 {
            return Err(Error::Unsupported(format!(
                "auto-distribute: {label} arm {arm_i} has empty schema"
            )));
        }
        if let Some(s) = stages.last_mut() {
            s.hash_key_cols = (0..n_cols).collect();
        }
        arm_outs.push(arm_out);
    }

    let set_sql =
        format!("SELECT * FROM shuffle_input_0 {op_sql}{quant} SELECT * FROM shuffle_input_1");
    let set_id = next_id;
    stages.push(StageDef::new(set_id, set_sql, arm_outs, vec![]));
    Ok(DistributedQuery {
        stages,
        finalize_sql: build_outer_finalize(sort, limit)?,
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
                    .expr_to_sql(&unqualify(&s.expr))
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
    use weft_loom::arrow::util::pretty::pretty_format_batches;
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

    fn gate_table() -> RecordBatch {
        let schema = std::sync::Arc::new(Schema::new(vec![Field::new(
            "__weft_subquery_gate",
            DataType::Int64,
            false,
        )]));
        RecordBatch::try_new(schema, vec![std::sync::Arc::new(Int64Array::from(vec![1]))]).unwrap()
    }

    async fn plan(sql: &str) -> Result<DistributedQuery> {
        let engine = Engine::new();
        engine.register_batches("t", vec![tiny_table()]).unwrap();
        let lp = engine.logical_plan(sql).await?;
        try_window(&lp, &[])
            .and_then(|o| o.ok_or_else(|| Error::Unsupported("not a window plan".into())))
    }

    #[test]
    fn table_factor_rewrite_preserves_implicit_and_explicit_aliases() {
        let sql = "SELECT web_sales.k FROM web_sales WHERE EXISTS \
                   (SELECT 1 FROM web_sales AS ws2 WHERE ws2.k = web_sales.k)";
        let (rewritten, count) =
            rewrite_table_factors(sql, "web_sales", "shuffle_input_0").unwrap();
        assert_eq!(count, 2);
        assert!(
            rewritten.contains("shuffle_input_0 AS web_sales"),
            "{rewritten}"
        );
        assert!(rewritten.contains("shuffle_input_0 AS ws2"), "{rewritten}");
    }

    #[tokio::test]
    async fn subquery_only_sharded_fact_is_gathered_and_gated() {
        let sql = "SELECT d.k, COUNT(*) AS n FROM dim d \
                   WHERE EXISTS (SELECT 1 FROM t WHERE t.k = d.k) \
                   GROUP BY d.k ORDER BY d.k";
        let planner = Engine::new();
        planner.register_batches("t", vec![tiny_table()]).unwrap();
        planner.register_batches("dim", vec![tiny_table()]).unwrap();
        let lp = planner.logical_plan(sql).await.unwrap();
        let dq = try_materialize_subquery_fact(&lp, &["dim"])
            .expect("materialization planning")
            .expect("subquery-only sharded fact");

        assert_eq!(dq.stages.len(), 3);
        assert!(dq.stages.iter().all(|s| s.exchange == ExchangeMode::Hash));
        assert_eq!(dq.stages[2].upstream_stage_ids, vec![0, 1]);
        assert!(dq.stages[2].sql.contains("shuffle_input_0"));
        assert!(dq.stages[2].sql.contains("shuffle_input_1"));

        // Execute the final stage locally with the two gathered inputs. This checks that the
        // table-factor alias rewrite preserves correlated references and output order.
        let worker = Engine::new();
        worker.register_batches("dim", vec![tiny_table()]).unwrap();
        worker
            .register_batches("shuffle_input_0", vec![tiny_table()])
            .unwrap();
        worker
            .register_batches("shuffle_input_1", vec![gate_table()])
            .unwrap();
        let expected = planner.sql(sql).await.unwrap();
        let actual = worker.sql(&dq.stages[2].sql).await.unwrap();
        let rows = |batches: &[RecordBatch]| {
            batches
                .iter()
                .flat_map(|batch| {
                    let keys = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap();
                    let counts = batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap();
                    (0..batch.num_rows())
                        .map(|i| (keys.value(i), counts.value(i)))
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>()
        };
        assert_eq!(rows(&actual), rows(&expected));
    }

    #[tokio::test]
    async fn repeated_fact_is_gathered_once_and_evaluated_exactly() {
        let sql = "SELECT t1.k, SUM(t1.v + t2.v) AS total \
                   FROM t t1 JOIN t t2 ON t1.k = t2.k \
                   GROUP BY t1.k ORDER BY t1.k";
        let planner = Engine::new();
        planner.register_batches("t", vec![tiny_table()]).unwrap();
        let lp = planner.logical_plan(sql).await.unwrap();
        let dq = try_materialize_complex_fact(&lp, &[])
            .expect("materialization planning")
            .expect("repeated fact");

        assert_eq!(dq.stages.len(), 3);
        assert_eq!(dq.stages[2].upstream_stage_ids, vec![0, 1]);
        assert_eq!(dq.stages[0].sql, "SELECT * FROM t");
        assert!(dq.stages[2].sql.contains("shuffle_input_0 AS t1"));
        assert!(dq.stages[2].sql.contains("shuffle_input_0 AS t2"));

        let worker = Engine::new();
        worker
            .register_batches("shuffle_input_0", vec![tiny_table()])
            .unwrap();
        worker
            .register_batches("shuffle_input_1", vec![gate_table()])
            .unwrap();
        let expected = planner.sql(sql).await.unwrap();
        let gathered = worker.sql(&dq.stages[2].sql).await.unwrap();
        let finalizer = Engine::new();
        finalizer.register_batches("result", gathered).unwrap();
        let actual = finalizer
            .sql(dq.finalize_sql.as_deref().expect("ORDER BY finalize"))
            .await
            .unwrap();
        assert_eq!(
            pretty_format_batches(&actual).unwrap().to_string(),
            pretty_format_batches(&expected).unwrap().to_string()
        );
    }

    #[tokio::test]
    async fn asymmetric_full_join_materialization_is_exact_and_partition_gated() {
        let sql = "SELECT COUNT(*) AS n, SUM(COALESCE(t.v, dim.v)) AS total \
                   FROM t FULL OUTER JOIN dim ON t.k = dim.k";
        let planner = Engine::new();
        planner.register_batches("t", vec![tiny_table()]).unwrap();
        planner.register_batches("dim", vec![tiny_table()]).unwrap();
        let lp = planner.logical_plan(sql).await.unwrap();
        let dq = try_materialize_complex_fact(&lp, &["dim"])
            .expect("materialization planning")
            .expect("asymmetric full join");

        let worker = Engine::new();
        worker.register_batches("dim", vec![tiny_table()]).unwrap();
        worker
            .register_batches("shuffle_input_0", vec![tiny_table()])
            .unwrap();
        worker
            .register_batches("shuffle_input_1", vec![gate_table()])
            .unwrap();
        let expected = planner.sql(sql).await.unwrap();
        let actual = worker.sql(&dq.stages[2].sql).await.unwrap();
        assert_eq!(
            pretty_format_batches(&actual).unwrap().to_string(),
            pretty_format_batches(&expected).unwrap().to_string()
        );

        // A global aggregate over an empty fact input still emits one row. The outer gate must
        // suppress that row on every non-driving rendezvous partition.
        let empty_gate = RecordBatch::new_empty(gate_table().schema());
        let gated_worker = Engine::new();
        gated_worker
            .register_batches("dim", vec![tiny_table()])
            .unwrap();
        gated_worker
            .register_batches(
                "shuffle_input_0",
                vec![RecordBatch::new_empty(tiny_table().schema())],
            )
            .unwrap();
        gated_worker
            .register_batches("shuffle_input_1", vec![empty_gate])
            .unwrap();
        let gated = gated_worker.sql(&dq.stages[2].sql).await.unwrap();
        assert_eq!(gated.iter().map(RecordBatch::num_rows).sum::<usize>(), 0);
    }

    #[tokio::test]
    async fn plain_rollup_rank_is_gatherable() {
        let planner = Engine::new();
        planner.register_batches("t", vec![tiny_table()]).unwrap();
        let lp = planner
            .logical_plan(
                "SELECT k, SUM(v), RANK() OVER (ORDER BY SUM(v) DESC) AS r \
                 FROM t GROUP BY ROLLUP(k)",
            )
            .await
            .unwrap();
        assert!(
            try_materialize_complex_fact(&lp, &[])
                .expect("shape check")
                .is_some(),
            "ROLLUP without UNION/INTERSECT should gather"
        );
    }

    #[tokio::test]
    async fn rollup_with_union_stays_rejected() {
        let planner = Engine::new();
        planner.register_batches("t", vec![tiny_table()]).unwrap();
        let lp = planner
            .logical_plan(
                "SELECT k, SUM(v) FROM (\
                   SELECT k, v FROM t WHERE k = 1 \
                   UNION ALL \
                   SELECT k, v FROM t WHERE k = 2\
                 ) AS x GROUP BY ROLLUP(k)",
            )
            .await
            .unwrap();
        assert!(
            try_materialize_complex_fact(&lp, &[])
                .expect("shape check")
                .is_none(),
            "ROLLUP + UNION must stay declined"
        );
    }

    #[tokio::test]
    async fn self_subquery_over_outer_sharded_fact_stays_rejected_by_this_path() {
        let engine = Engine::new();
        engine.register_batches("t", vec![tiny_table()]).unwrap();
        let lp = engine
            .logical_plan(
                "SELECT t1.k, COUNT(*) FROM t t1 \
                 WHERE EXISTS (SELECT 1 FROM t t2 WHERE t2.k = t1.k AND t2.v <> t1.v) \
                 GROUP BY t1.k",
            )
            .await
            .unwrap();
        assert!(
            try_materialize_subquery_fact(&lp, &[])
                .expect("shape check")
                .is_none(),
            "self-subqueries remain on the scan-count rejection path"
        );
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
    async fn window_over_aggregate_composes_partial_combine_window_stages() {
        // TPC-DS Q12/Q20/Q53/Q63/Q89/Q98 shape: an aggregate window function over a plain GROUP
        // BY, partitioned by a *subset* of the group columns (`k` here; `v` is grouped but not
        // partitioned on), so the combine stage's per-partition groups must be re-shuffled by `k`
        // before the window can see every group sharing that partition value.
        let dq = plan(
            "SELECT k, v, sum(v) AS sv, avg(sum(v)) OVER (PARTITION BY k) AS av \
             FROM t GROUP BY k, v",
        )
        .await
        .expect("window over aggregate should plan");
        assert_eq!(dq.stages.len(), 3, "partial -> combine -> window");
        assert!(dq.stages[0].sql.contains("FROM t"));
        assert!(dq.stages[0].sql.to_uppercase().contains("GROUP BY"));
        assert!(
            !dq.stages[1].hash_key_cols.is_empty(),
            "combine stage must re-shuffle by the window's PARTITION BY columns, not gather"
        );
        let window_sql = dq.stages[2].sql.to_uppercase();
        assert!(window_sql.contains("AVG"));
        assert!(window_sql.contains("OVER (PARTITION BY"));
        assert!(dq.stages[2].sql.contains("\"sv\""));
        assert!(dq.stages[2].sql.contains("\"av\""));
    }

    #[tokio::test]
    async fn window_over_aggregate_applies_having_equivalent_filter_and_aliases() {
        // TPC-DS Q53/Q63/Q89 shape: `SELECT * FROM (SELECT … avg(…) OVER (…) alias …) tmp WHERE
        // alias > 0` — the outer Filter/alias-projection layer between the window and the query's
        // real output must survive (previously silently dropped by `peel_window`).
        let dq = plan(
            "SELECT * FROM (SELECT k, sum(v) AS sv, avg(sum(v)) OVER (PARTITION BY k) AS av \
             FROM t GROUP BY k, v) tmp WHERE av > 0",
        )
        .await
        .expect("window over aggregate with HAVING-equivalent filter should plan");
        let window_sql = &dq.stages.last().unwrap().sql;
        assert!(
            window_sql.to_uppercase().contains("WHERE"),
            "HAVING-equivalent filter must be re-applied, got: {window_sql}"
        );
        assert!(window_sql.contains("\"sv\""));
        assert!(window_sql.contains("\"av\""));
    }

    #[tokio::test]
    async fn window_over_a_union_of_aggregates_is_rejected() {
        // TPC-DS Q36 shape: the window sits over a UNION/DISTINCT of aggregates, not a plain
        // GROUP BY — out of scope for the partial->combine->window composition.
        let engine = Engine::new();
        engine.register_batches("t", vec![tiny_table()]).unwrap();
        let lp = engine
            .logical_plan(
                "SELECT k, v, avg(v) OVER (PARTITION BY k) AS av FROM \
                 (SELECT k, v FROM t GROUP BY k, v \
                  UNION SELECT k, v FROM t WHERE v > 1 GROUP BY k, v) u",
            )
            .await
            .unwrap();
        let err = try_window(&lp, &[]).expect_err("window over a UNION is not supported");
        assert!(format!("{err}").contains("auto-distribute"), "got: {err}");
    }

    #[tokio::test]
    async fn window_function_rank_over_aggregate_is_rejected() {
        let engine = Engine::new();
        engine.register_batches("t", vec![tiny_table()]).unwrap();
        let lp = engine
            .logical_plan(
                "SELECT k, sum(v) AS sv, rank() OVER (PARTITION BY k ORDER BY sum(v)) AS rk \
                 FROM t GROUP BY k, v",
            )
            .await
            .unwrap();
        let err = try_window(&lp, &[]).expect_err("rank is not a supported window aggregate");
        assert!(format!("{err}").contains("rank"), "got: {err}");
    }

    #[tokio::test]
    async fn union_distinct_plans_dedup_stage() {
        let engine = Engine::new();
        engine.register_batches("t", vec![tiny_table()]).unwrap();
        let lp = engine
            .logical_plan(
                "SELECT k, SUM(v) AS sv FROM t GROUP BY k \
                 UNION SELECT k, SUM(v) AS sv FROM t WHERE v > 1 GROUP BY k",
            )
            .await
            .unwrap();
        let dq = try_union_all(&lp, &[])
            .expect("ok")
            .expect("UNION distinct should plan");
        let last = dq.stages.last().expect("stages");
        assert!(
            last.sql.to_uppercase().contains("DISTINCT"),
            "expected dedup stage, got: {}",
            last.sql
        );
        let union_stage = &dq.stages[dq.stages.len() - 2];
        assert!(
            !union_stage.hash_key_cols.is_empty(),
            "union distinct must hash-shuffle full rows"
        );
    }

    #[tokio::test]
    async fn intersect_of_two_aggs_plans() {
        let engine = Engine::new();
        engine.register_batches("t", vec![tiny_table()]).unwrap();
        let lp = engine
            .logical_plan(
                "SELECT k FROM t GROUP BY k \
                 INTERSECT \
                 SELECT k FROM t WHERE v > 1 GROUP BY k",
            )
            .await
            .unwrap();
        let dq = try_union_all(&lp, &[])
            .expect("ok")
            .expect("INTERSECT should plan");
        let last = dq.stages.last().expect("stages");
        assert!(
            last.sql.to_uppercase().contains("INTERSECT"),
            "got: {}",
            last.sql
        );
    }

    #[tokio::test]
    async fn non_aggregate_scan_plans_single_scatter_stage() {
        let engine = Engine::new();
        engine.register_batches("t", vec![tiny_table()]).unwrap();
        let lp = engine
            .logical_plan("SELECT k, v FROM t WHERE v > 15 ORDER BY k LIMIT 2")
            .await
            .unwrap();
        let dq = try_non_aggregate(&lp, &[]).expect("ok").expect("scan plan");
        assert_eq!(dq.stages.len(), 1);
        assert_eq!(dq.stages[0].exchange, ExchangeMode::Hash);
        assert!(dq.stages[0].sql.contains("FROM t"));
        assert!(dq
            .finalize_sql
            .as_ref()
            .is_some_and(|s| s.contains("LIMIT 2")));
    }

    #[tokio::test]
    async fn non_aggregate_replicated_only_uses_forward() {
        let engine = Engine::new();
        engine.register_batches("dim", vec![tiny_table()]).unwrap();
        let lp = engine
            .logical_plan("SELECT k FROM dim WHERE v > 15 LIMIT 1")
            .await
            .unwrap();
        let dq = try_non_aggregate(&lp, &["dim"])
            .expect("ok")
            .expect("forward scan");
        assert_eq!(dq.stages.len(), 1);
        assert_eq!(dq.stages[0].exchange, ExchangeMode::Forward);
    }

    #[tokio::test]
    async fn non_aggregate_skips_when_aggregate_present() {
        let engine = Engine::new();
        engine.register_batches("t", vec![tiny_table()]).unwrap();
        let lp = engine
            .logical_plan("SELECT k, SUM(v) FROM t GROUP BY k")
            .await
            .unwrap();
        assert!(try_non_aggregate(&lp, &[]).expect("ok").is_none());
    }
}

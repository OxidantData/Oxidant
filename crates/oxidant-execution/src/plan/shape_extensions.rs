//! Extra plan shapes layered on top of the core aggregation splitter.
//!
//! Kept as a sibling module so parallel edits to [`super::stage_planner`] (broadcast / shuffle
//! joins, HAVING, …) stay low-conflict. Covers:
//!
//! - **Subquery safety**: a fact scanned only inside IN / EXISTS / scalar subqueries can be
//!   gathered once and evaluated on one gated partition; self-subqueries over the driving
//!   sharded fact stay rejected by scan counting.
//! - **KAN-55 distributed subqueries over sharded facts** (TPC-DS Q9/Q10/Q16/Q35/Q69/Q94):
//!   subquery predicates reading only replicated tables evaluate verbatim per partition;
//!   a global aggregate above the semi/anti filter re-runs exactly over the gathered filtered
//!   rows (COUNT(DISTINCT) included); uncorrelated global-aggregate scalar subqueries in the
//!   projection decompose into per-worker partials + a one-row combine the gated outer reads.
//! - **Correlated scalar subqueries** (`fact.col = (SELECT min/max/sum/count(…) FROM fact, …
//!   WHERE outer.key = fact.key …)`, TPC-H Q2) are decorrelated into a per-key distributed
//!   aggregation hash-joined against the outer scan, instead of gathering the whole fact.
//! - **Grouped `IN` fused with the outer aggregate** (TPC-H Q18, KAN-37): when the `IN`
//!   subquery's per-key aggregate is exactly the outer aggregate over the same fact, the tiny
//!   per-key stream joins the replicated dims directly instead of shuffling the full join output.
//! - **Uncorrelated scalar subqueries** used as a HAVING comparison threshold (`HAVING sum(…) >
//!   (SELECT sum(…) * frac FROM fact, …)`, TPC-H Q11) get a one-row broadcast: scalar
//!   partial/combine stages, then the driver inlines the single computed value into the outer
//!   stages' SQL before dispatch (literal injection).
//! - **UNION ALL** / **UNION** (distinct) / **INTERSECT** / **EXCEPT** of distributable arms
//!   (branch stages + hash-shuffle co-location, then local set / dedup).
//! - **Narrow windows**: aggregate `OVER (PARTITION BY …)` over one sharded table (shuffle by
//!   partition key, then compute locally). Ranking / global windows stay Unsupported.
//! - Explicit **Unsupported** messages for unsupported window / distinct shapes.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::common::Column;
use datafusion::logical_expr::expr::{BinaryExpr, WindowFunction, WindowFunctionDefinition};
use datafusion::logical_expr::{
    Aggregate, Expr, JoinType, LogicalPlan, Operator, Union, Window, WindowFrame, WindowFrameBound,
    WindowFrameUnits,
};
use datafusion::scalar::ScalarValue;
use datafusion::sql::unparser::Unparser;
use oxidant_common::{Error, Result};

use super::join_chain::{
    conjunct_side, flat_col, flat_key_index, flatten_join_residual, leaf_stage_sql, scan_alias,
    ConjunctSide, JoinSideScope,
};
use super::stage_planner::{
    aggregation_stages_for, base_tables, build_agg_remap, build_finalize, build_remap, column_name,
    count_table_scans, distinct_stage_sql, expr_sql, extract_from_tail, final_group_by_sql,
    flatten_distinct_union, flattened_group_exprs, is_grouping_set, partial_and_combine_lists,
    partial_combine_sql, peel, plan_distributed_logical, qualified_table_sql, recombine_stage_sql,
    reject_unsafe_broadcast_shapes, resolve_grouping_specs, sanitize_generated_sql,
    simple_table_scan, unqualify, wrap_output, AggSpec, DistributedQuery, Peeled,
};
use crate::driver::{scalar_literal_supported, ExchangeMode, StageDef, SCALAR_TOKEN};

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
                ));
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
/// **Stacked windows** (KAN-49a, TPC-DS Q47/Q57): several `WindowAggr` nodes may layer over the
/// same aggregate — e.g. a `rank() OVER (PARTITION BY g ORDER BY …)` feeding an
/// `avg(…) OVER (PARTITION BY g')`. Step 3 repeats per layer, innermost first: each layer's
/// input is re-shuffled by *its* `PARTITION BY` columns (ranking included — a `rank()`/`dense_rank()`/
/// `row_number()` over a co-located partition is exact, the same argument that makes partition-wide
/// aggregate windows exact), earlier `w{k}` columns pass through, and the outermost layer applies
/// the query's HAVING-equivalent filter / output projection.
///
/// **KAN-49b extensions** (TPC-DS Q36/Q44/Q51/Q67/Q70/Q86):
///
/// - The window no longer has to sit *directly* on the `Aggregate`: `SubqueryAlias` /
///   `Projection` layers between them fold into the remap, and a `Filter` there is a
///   HAVING-equivalent that applies on the combine stage's output before any window computes.
/// - **ROLLUP / CUBE / GROUPING SETS** aggregates compose (Q67/Q70/Q86): the ordinary machinery
///   distributes them two-phase — finest-level partials hashed by the first grouping column, a
///   per-partition `GROUP BY ROLLUP` for every level containing it, a tiny grand-total fixup
///   (`grouping()` outputs included — recomputed against the real rollup and combined as
///   `max`) — and gathers only the levels no single hash key co-locates (CUBE, Q70's IN-key
///   pipeline); the window then re-shuffles the tiny combined output by its
///   `PARTITION BY` key. Super-aggregate rows carry NULL grouping columns, and NULL keys
///   hash-consistently land on one partition, so per-partition window semantics match
///   single-node exactly.
/// - **Expression partition keys** (`PARTITION BY grouping(a)+grouping(b), CASE WHEN … THEN a
///   END` — Q70/Q86) materialize as computed columns on the producer stage so the shuffle can
///   hash them.
/// - **Global ranking windows** (no `PARTITION BY` — Q44): the tiny post-aggregate result
///   gathers to partition 0 (a post-aggregation gather — strict mode only forbids gathering the
///   raw sharded fact) and the global `rank()` computes there.
/// - **Framed aggregate windows** (`sum(x) OVER (… ORDER BY d ROWS BETWEEN UNBOUNDED PRECEDING
///   AND CURRENT ROW)` — Q51): any frame is exact on a co-located partition, so the frame is
///   re-emitted into the window stage's `OVER` clause.
/// - A HAVING-equivalent carrying an **uncorrelated scalar subquery over the sharded fact**
///   (Q44) plans the subquery as its own partial/combine pair whose one-row output gathers to
///   partition 0 and rides the window stage as an extra co-located input (see
///   [`plan_scalar_having_stream`]).
/// - An **uncorrelated `IN` subquery over the sharded fact** in the aggregate's input filter
///   (Q70) plans as a co-located key stream semi-filtering a scan-export stage (see
///   [`agg_pipeline_with_in_producer`]).
///
/// A window over a distributable `UNION` of aggregates (TPC-DS Q36) or over a `FULL OUTER JOIN`
/// of two windowed aggregates (TPC-DS Q51) routes to its own composition
/// ([`window_over_distinct_union_stages_for`] / [`window_over_join_stages_for`]).
fn window_over_aggregate_stages_for(
    p: &WindowPeeled<'_>,
    replicated: &[&str],
) -> Result<DistributedQuery> {
    // Collect the stack of window layers between the peeled top and the aggregate, outermost
    // first (`windows[0]` is `p.window`).
    let mut windows: Vec<&Window> = vec![p.window];
    let mut node = p.window.input.as_ref();
    while let LogicalPlan::Window(inner) = node {
        windows.push(inner);
        node = inner.input.as_ref();
    }
    // KAN-49b: look through `SubqueryAlias` / `Projection` / `Filter` layers between the
    // innermost window and the aggregate (Q44/Q67). Projections only rename aggregate outputs
    // on the way up and fold into the remap; a Filter there is a HAVING-equivalent that must
    // apply before any window computes.
    let mut between_projections: Vec<&[Expr]> = Vec::new();
    let mut between_filters: Vec<&Expr> = Vec::new();
    let agg = loop {
        match node {
            LogicalPlan::SubqueryAlias(s) => node = s.input.as_ref(),
            LogicalPlan::Projection(proj) => {
                if windows.len() == 1
                    && between_projections.is_empty()
                    && between_filters.is_empty()
                    && matches!(proj.input.as_ref(), LogicalPlan::Join(_))
                {
                    // TPC-DS Q51: framed windows over a FULL OUTER JOIN of windowed aggregates.
                    return window_over_join_stages_for(p, windows[0], proj, replicated);
                }
                between_projections.push(proj.expr.as_slice());
                node = proj.input.as_ref();
            }
            LogicalPlan::Filter(f) => {
                between_filters.push(f.predicate.as_ref());
                node = f.input.as_ref();
            }
            LogicalPlan::Distinct(_)
                if windows.len() == 1
                    && between_projections.is_empty()
                    && between_filters.is_empty() =>
            {
                // TPC-DS Q36: a ranking window over a UNION sharing one aggregate CTE.
                return window_over_distinct_union_stages_for(p, windows[0], node, replicated);
            }
            LogicalPlan::Aggregate(agg) => break agg,
            _ => {
                return Err(Error::Unsupported(
                    "auto-distribute: window over an aggregation is only supported when the \
                     window sits over a GROUP BY (possibly through renames / a HAVING filter), \
                     a distributable UNION of aggregates, or a join of windowed aggregates"
                        .into(),
                ));
            }
        }
    };

    // Pre-window HAVING-equivalent conjuncts (Q44). Conjuncts carrying an uncorrelated scalar
    // subquery over the sharded fact cannot evaluate per-partition (each partition would see
    // only its shard's scalar); they become co-located one-row streams the window stage reads
    // as extra inputs. Subquery-free conjuncts ride the combine stage as ordinary HAVING.
    let mut having_conjuncts: Vec<&Expr> = Vec::new();
    for f in &between_filters {
        flatten_conjuncts(f, &mut having_conjuncts);
    }
    let scalar_having = having_conjuncts.iter().any(|c| expr_contains_subquery(c));

    // One PARTITION BY per layer (mixed clauses within a layer stay rejected). A *global*
    // layer (no PARTITION BY) is admitted when every window function in it is a ranking
    // function — the tiny post-aggregate result gathers to partition 0 and the global rank
    // computes there (Q44); aggregate windows still require a shuffle key.
    let mut partition_bys: Vec<Vec<Expr>> = Vec::with_capacity(windows.len());
    for w in &windows {
        partition_bys.push(validate_window_layer(w)?);
    }
    if scalar_having && (windows.len() > 1 || partition_bys.iter().any(|pb| !pb.is_empty())) {
        return Err(Error::Unsupported(
            "auto-distribute: window over an aggregation: a HAVING carrying a scalar subquery \
             over the sharded fact is only supported under a single global (no PARTITION BY) \
             ranking window — the scalar stream lands on partition 0 only"
                .into(),
        ));
    }

    let up = Unparser::default();
    // Plan the scalar subquery stream(s) first: their stage ids slot between the aggregation
    // pipeline and the window stage (rebased on insertion), and the rewritten HAVING references
    // them positionally as `shuffle_input_{1+i}` (the window stage's upstream list is
    // [producer, streams…]).
    let mut scalar_streams: Vec<Vec<StageDef>> = Vec::new();
    let mut window_having_sql: Vec<String> = Vec::new();
    if scalar_having {
        let remap = build_window_over_agg_remap(agg, &[], &between_projections);
        for c in &having_conjuncts {
            let rewritten =
                rewrite_scalar_having_conjunct(&up, c, &remap, replicated, &mut scalar_streams)?;
            window_having_sql.push(format!("({rewritten})"));
        }
    }

    // The base partial→combine pipeline. Q70's uncorrelated `IN` subquery over the sharded
    // fact in the aggregate's input filter splits out into a co-located key stream plus a
    // scan-export / semi composition; everything else reuses the ordinary aggregation planner
    // verbatim: a no-projection/no-sort/no-limit `Peeled` makes its final stage emit the raw
    // `g{j}`/`r{i}` row (`SELECT * FROM (…) AS combined`) instead of aliasing back to source
    // names, which is exactly the input the window stage below needs. This also gets broadcast
    // safety, ROLLUP handling, and (if ever needed) the shuffle-join-chain / DISTINCT paths
    // for free, without duplicating any of that logic here.
    let mut dq = if let Some(split) = split_in_subquery_from_agg_input(agg, replicated)? {
        agg_pipeline_with_in_producer(agg, &split, replicated)?
    } else {
        let synthetic = Peeled {
            projection: None,
            sort: None,
            limit: None,
            having: if scalar_having {
                Vec::new()
            } else {
                having_conjuncts.clone()
            },
            alias_projections: between_projections.clone(),
            agg,
        };
        aggregation_stages_for(&synthetic, replicated)?
    };
    // Scalar stream stages go after the base pipeline (they are leaves / tiny combines) and
    // before the window stage that consumes them; ids rebase onto the pipeline's numbering.
    // The window layers' first producer is the base pipeline's *terminal* stage, which stops
    // being `stages.last()` once the streams are appended — capture its id now.
    let base_terminal_id = dq
        .stages
        .last()
        .ok_or_else(|| {
            Error::Unsupported("auto-distribute: window-over-aggregate produced no stages".into())
        })?
        .stage_id;
    let mut scalar_combine_ids: Vec<u32> = Vec::new();
    for stages in scalar_streams {
        let offset = dq.stages.iter().map(|s| s.stage_id).max().unwrap_or(0) + 1;
        for mut s in stages {
            s.stage_id += offset;
            for u in &mut s.upstream_stage_ids {
                *u += offset;
            }
            dq.stages.push(s);
        }
        scalar_combine_ids.push(dq.stages.last().expect("scalar combine appended").stage_id);
    }

    let n_group = flattened_group_exprs(&agg.group_expr).len();
    let base_remap = build_window_over_agg_remap(agg, &[], &between_projections);
    let mut stage_cols: Vec<String> = (0..n_group).map(|j| format!("g{j}")).collect();
    stage_cols.extend((0..agg.aggr_expr.len()).map(|i| format!("r{i}")));

    // Each layer re-shuffles its input by its own PARTITION BY (the previous stage's hash key is
    // re-targeted, exactly like the single-window combine retarget below) and computes locally.
    let mut next_w = 0usize;
    let mut next_c = 0usize;
    let mut chained_exprs: Vec<Expr> = Vec::new();
    let n_layers = windows.len();
    let mut producer_stage_id = base_terminal_id;
    for (depth, w) in windows.iter().rev().enumerate() {
        let partition_by = &partition_bys[n_layers - 1 - depth];
        let mut layer_remap =
            build_window_over_agg_remap(agg, &chained_exprs, &between_projections);
        // Map each partition key to a producer output column. An *expression* key (Q70/Q86's
        // `grouping(a)+grouping(b)`, `CASE WHEN grouping(b)=0 THEN a END`) materializes as a
        // computed column appended to the producer stage's SELECT so the shuffle can hash it.
        let mut hash_key_cols: Vec<u32> = Vec::with_capacity(partition_by.len());
        let mut computed: Vec<(String, String)> = Vec::new();
        for e in partition_by.iter() {
            if let Expr::Column(c) = e {
                if let Some(idx) = base_remap
                    .get(&c.flat_name())
                    .or_else(|| base_remap.get(&c.name))
                    .and_then(|name| remap_name_to_index(name, n_group))
                {
                    hash_key_cols.push(idx);
                    continue;
                }
            }
            let mapped = remap_expr_columns(e, &layer_remap);
            ensure_all_window_columns_remapped(&mapped).map_err(|_| {
                Error::Unsupported(format!(
                    "auto-distribute: window over an aggregation: PARTITION BY expression `{e}` \
                     does not map to group, aggregate, or window output columns"
                ))
            })?;
            let name = format!("c{next_c}");
            next_c += 1;
            hash_key_cols.push((stage_cols.len() + computed.len()) as u32);
            layer_remap.insert(e.schema_name().to_string(), name.clone());
            computed.push((expr_sql(&up, &mapped)?, name));
        }

        // The producer stage (the aggregate's combine for the innermost layer, the previous
        // window stage otherwise) currently has an empty hash key because it expected to be the
        // terminal stage. Re-target it at this layer's partition columns so the window stage
        // below sees whole partitions — or, for a global layer, leave it empty: everything
        // gathers to partition 0 and the global rank computes there.
        let producer = dq
            .stages
            .iter_mut()
            .find(|s| s.stage_id == producer_stage_id)
            .ok_or_else(|| {
                Error::Unsupported(
                    "auto-distribute: window-over-aggregate produced no stages".into(),
                )
            })?;
        if !computed.is_empty() {
            let extra = computed
                .iter()
                .map(|(sql, name)| format!("{sql} AS {name}"))
                .collect::<Vec<_>>()
                .join(", ");
            producer.sql = sanitize_generated_sql(&format!(
                "SELECT *, {extra} FROM ({}) AS keyed_in",
                producer.sql
            ));
            stage_cols.extend(computed.iter().map(|(_, name)| name.clone()));
        }
        if !partition_by.is_empty() {
            producer.hash_key_cols = hash_key_cols;
        }
        let producer_id = producer.stage_id;

        chained_exprs.extend(w.window_expr.iter().cloned());
        let is_outermost = depth + 1 == n_layers;
        // A scalar-bearing pre-window HAVING (Q44) filters the combine output on the window
        // stage's own input, where the co-located scalar stream is visible (partition 0).
        let (from, upstreams) = if is_outermost && !window_having_sql.is_empty() {
            let from = format!(
                "(SELECT * FROM shuffle_input_0 WHERE {}) AS having_in",
                window_having_sql.join(" AND ")
            );
            let mut ups = vec![producer_id];
            ups.extend(scalar_combine_ids.iter().copied());
            (from, ups)
        } else {
            ("shuffle_input".to_string(), vec![producer_id])
        };
        let window_inner =
            build_window_over_agg_inner(w, &layer_remap, &stage_cols, &mut next_w, &from)?;
        stage_cols.extend(
            (0..w.window_expr.len()).map(|k| format!("w{}", next_w - w.window_expr.len() + k)),
        );

        let window_sql = if is_outermost {
            let mut projections = between_projections.clone();
            projections.extend(p.alias_projections.iter().copied());
            let full_remap = build_window_over_agg_remap(agg, &chained_exprs, &projections);
            wrap_window_over_agg_output(p, &window_inner, &full_remap)?
        } else {
            window_inner
        };

        let next_id = dq.stages.iter().map(|s| s.stage_id).max().unwrap_or(0) + 1;
        dq.stages
            .push(StageDef::new(next_id, window_sql, upstreams, vec![]));
        producer_stage_id = next_id;
    }
    dq.finalize_sql = build_outer_finalize(p.sort, p.limit)?;
    Ok(dq)
}

/// Validate one window layer's expressions and return its shared `PARTITION BY`.
///
/// Two expression classes distribute exactly over a partition-co-located input:
///
/// - **partition-wide aggregate windows** (`sum`/`count`/`min`/`max`/`avg` with no `DISTINCT`,
///   no `FILTER`) — the original supported class. An `ORDER BY` with an explicit frame (Q51's
///   cumulative `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`) is also exact: the whole
///   partition is co-located, so the local framed evaluation *is* the global one.
/// - **ranking windows** (`rank`/`dense_rank`/`row_number`) — each partition's rows are wholly
///   on one worker after the hash shuffle, so the local ranking is the global ranking. Their
///   `ORDER BY` is emitted into the `OVER` clause.
///
/// A global layer (empty `PARTITION BY`) is admitted only when every window function in it is a
/// ranking function: the layer's input then gathers to partition 0 (a tiny post-aggregate
/// result — never the raw fact) and the global rank computes there (TPC-DS Q44). A global
/// aggregate window keeps the plain "no shuffle key" rejection.
fn validate_window_layer(w: &Window) -> Result<Vec<Expr>> {
    if w.window_expr.is_empty() {
        return Err(Error::Unsupported(
            "auto-distribute: window plan has no window expressions".into(),
        ));
    }
    let mut partition_by: Option<Vec<Expr>> = None;
    for e in &w.window_expr {
        let Expr::WindowFunction(wf) = e else {
            return Err(Error::Unsupported(format!(
                "auto-distribute: non-window expression in window list: {e}"
            )));
        };
        validate_window_over_agg_func(wf)?;
        match &partition_by {
            None => partition_by = Some(wf.params.partition_by.clone()),
            Some(prev) if prev == &wf.params.partition_by => {}
            Some(_) => {
                return Err(Error::Unsupported(
                    "auto-distribute: mixed PARTITION BY clauses across window functions".into(),
                ));
            }
        }
    }
    let partition_by = partition_by.unwrap_or_default();
    if partition_by.is_empty() {
        let all_ranking = w.window_expr.iter().all(|e| match e {
            Expr::WindowFunction(wf) => window_func_name(wf)
                .is_some_and(|n| matches!(n.as_str(), "rank" | "dense_rank" | "row_number")),
            _ => false,
        });
        if !all_ranking {
            return Err(Error::Unsupported(
                "auto-distribute: window without PARTITION BY cannot be distributed \
                 (no partition shuffle key) — falling back to local execution"
                    .into(),
            ));
        }
    }
    Ok(partition_by)
}

/// The lowercased function name of a window expression.
fn window_func_name(wf: &WindowFunction) -> Option<String> {
    Some(match &wf.fun {
        WindowFunctionDefinition::AggregateUDF(f) => f.name().to_ascii_lowercase(),
        WindowFunctionDefinition::WindowUDF(f) => f.name().to_ascii_lowercase(),
    })
}

/// One window expression in a window-over-aggregate layer: a partition-wide aggregate window or a
/// ranking window (see [`validate_window_layer]).
fn validate_window_over_agg_func(wf: &WindowFunction) -> Result<()> {
    let Some(name) = window_func_name(wf) else {
        return Err(Error::Unsupported(
            "auto-distribute: unsupported window function definition".into(),
        ));
    };
    let ranking = matches!(name.as_str(), "rank" | "dense_rank" | "row_number");
    if !ranking && !matches!(name.as_str(), "sum" | "count" | "min" | "max" | "avg") {
        return Err(Error::Unsupported(format!(
            "auto-distribute: window function `{name}` is not supported for distribution \
             (only SUM/COUNT/MIN/MAX/AVG aggregate windows and RANK/DENSE_RANK/ROW_NUMBER)"
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
    if !ranking && !wf.params.order_by.is_empty() {
        // A framed aggregate window is exact on a co-located partition, but only frames the
        // worker dialect can re-parse are admitted (ROWS / RANGE with renderable bounds).
        window_frame_sql(&wf.params.window_frame)?.ok_or_else(|| {
            Error::Unsupported(
                "auto-distribute: window frame is not supported \
                 (only ROWS / RANGE with unbounded or constant bounds)"
                    .into(),
            )
        })?;
    }
    Ok(())
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

/// The window stage's own `SELECT`: every producer-stage column passed through unchanged, plus
/// one `w{k}` per window expression (numbered by `next_w`, so stacked layers keep unique names).
/// `validate_window_over_agg_func` has already classified each expression, so each is either a
/// partition-wide (or framed) aggregate window `func(arg?) OVER (…)` or a ranking window
/// `rank() OVER (… ORDER BY …)` — safe to re-emit as plain SQL text once `arg`, the partition
/// columns, and the ordering columns are remapped to stage column names. `from` is the stage's
/// input reference: plain `shuffle_input`, or a filtering subquery when a pre-window HAVING
/// applies on this stage (Q44's scalar threshold).
fn build_window_over_agg_inner(
    w: &Window,
    remap: &HashMap<String, String>,
    stage_cols: &[String],
    next_w: &mut usize,
    from: &str,
) -> Result<String> {
    let mut cols: Vec<String> = stage_cols.to_vec();
    for e in w.window_expr.iter() {
        let Expr::WindowFunction(wf) = e else {
            return Err(Error::Unsupported(format!(
                "auto-distribute: non-window expression in window list: {e}"
            )));
        };
        let func_name = window_func_name(wf).ok_or_else(|| {
            Error::Unsupported("auto-distribute: unsupported window function definition".into())
        })?;
        let ranking = matches!(func_name.as_str(), "rank" | "dense_rank" | "row_number");
        let arg_sql = match wf.params.args.first() {
            Some(arg) => {
                if let Expr::Column(c) = arg {
                    if let Some(mapped) = remap.get(&c.flat_name()).or_else(|| remap.get(&c.name)) {
                        mapped.clone()
                    } else {
                        let key = arg.schema_name().to_string();
                        remap.get(&key).cloned().ok_or_else(|| {
                            Error::Unsupported(format!(
                                "auto-distribute: window argument `{key}` does not map to a \
                                 group or aggregate output column"
                            ))
                        })?
                    }
                } else {
                    let key = arg.schema_name().to_string();
                    remap.get(&key).cloned().ok_or_else(|| {
                        Error::Unsupported(format!(
                            "auto-distribute: window argument `{key}` does not map to a group or \
                             aggregate output column"
                        ))
                    })?
                }
            }
            None => "1".to_string(), // count(*)-style window carries no arg
        };
        let map_col = |e: &Expr| -> Result<String> {
            if let Expr::Column(c) = e {
                if let Some(mapped) = remap.get(&c.flat_name()).or_else(|| remap.get(&c.name)) {
                    return Ok(mapped.clone());
                }
            }
            let key = e.schema_name().to_string();
            remap.get(&key).cloned().ok_or_else(|| {
                Error::Unsupported(format!(
                    "auto-distribute: window column `{key}` does not map to a group or aggregate \
                     output column"
                ))
            })
        };
        let part_sql: Vec<String> = wf
            .params
            .partition_by
            .iter()
            .map(map_col)
            .collect::<Result<_>>()?;
        let mut over = String::new();
        if !part_sql.is_empty() {
            over.push_str(&format!("PARTITION BY {}", part_sql.join(", ")));
        }
        if !wf.params.order_by.is_empty() {
            let order_sql: Vec<String> = wf
                .params
                .order_by
                .iter()
                .map(|s| {
                    let dir = if s.asc { "ASC" } else { "DESC" };
                    let nulls = if s.nulls_first {
                        "NULLS FIRST"
                    } else {
                        "NULLS LAST"
                    };
                    Ok(format!("{} {dir} {nulls}", map_col(&s.expr)?))
                })
                .collect::<Result<Vec<_>>>()?;
            if !over.is_empty() {
                over.push(' ');
            }
            over.push_str(&format!("ORDER BY {}", order_sql.join(", ")));
            // A framed aggregate window keeps its frame (exact on a co-located partition);
            // ranking windows never need one emitted.
            if !ranking {
                if let Some(frame) = window_frame_sql(&wf.params.window_frame)? {
                    over.push(' ');
                    over.push_str(&frame);
                }
            }
        }
        let call = if wf.params.args.is_empty() {
            format!("{func_name}()")
        } else {
            format!("{func_name}({arg_sql})")
        };
        cols.push(format!("{call} OVER ({over}) AS w{next_w}"));
        *next_w += 1;
    }
    Ok(format!("SELECT {} FROM {from}", cols.join(", ")))
}

/// Render a window frame as SQL text the worker dialect re-parses, or `Ok(None)` when the frame
/// is outside the renderable set (GROUPS units, non-constant offsets) and the caller must
/// decline.
fn window_frame_sql(frame: &WindowFrame) -> Result<Option<String>> {
    let units = match frame.units {
        WindowFrameUnits::Rows => "ROWS",
        WindowFrameUnits::Range => "RANGE",
        WindowFrameUnits::Groups => return Ok(None),
    };
    let bound = |b: &WindowFrameBound| -> Result<Option<String>> {
        Ok(Some(match b {
            WindowFrameBound::CurrentRow => "CURRENT ROW".to_string(),
            WindowFrameBound::Preceding(v) if v.is_null() => "UNBOUNDED PRECEDING".to_string(),
            WindowFrameBound::Following(v) if v.is_null() => "UNBOUNDED FOLLOWING".to_string(),
            WindowFrameBound::Preceding(v) => match window_frame_offset(v) {
                Some(off) => format!("{off} PRECEDING"),
                None => return Ok(None),
            },
            WindowFrameBound::Following(v) => match window_frame_offset(v) {
                Some(off) => format!("{off} FOLLOWING"),
                None => return Ok(None),
            },
        }))
    };
    let (Some(start), Some(end)) = (bound(&frame.start_bound)?, bound(&frame.end_bound)?) else {
        return Ok(None);
    };
    Ok(Some(format!("{units} BETWEEN {start} AND {end}")))
}

/// A constant frame offset as SQL text (integers and plain numerics only).
fn window_frame_offset(v: &ScalarValue) -> Option<String> {
    match v {
        ScalarValue::UInt8(Some(x)) => Some(x.to_string()),
        ScalarValue::UInt16(Some(x)) => Some(x.to_string()),
        ScalarValue::UInt32(Some(x)) => Some(x.to_string()),
        ScalarValue::UInt64(Some(x)) => Some(x.to_string()),
        ScalarValue::Int8(Some(x)) if *x >= 0 => Some(x.to_string()),
        ScalarValue::Int16(Some(x)) if *x >= 0 => Some(x.to_string()),
        ScalarValue::Int32(Some(x)) if *x >= 0 => Some(x.to_string()),
        ScalarValue::Int64(Some(x)) if *x >= 0 => Some(x.to_string()),
        _ => None,
    }
}

/// Rewrite one scalar-subquery-bearing HAVING conjunct (Q44's
/// `avg(x) > 0.9 * (SELECT avg(x) … GROUP BY key-pinned-to-a-literal)`) for evaluation on the
/// window stage's input: the subquery is planned as a co-located one-row stream (appended to
/// `streams`) and replaced in the predicate text by `(SELECT m0 FROM shuffle_input_{1+i})` —
/// position `1+i` in the window stage's upstream list, after the combine producer. The stream
/// gathers to partition 0; on every other partition both the stream and the (gather-keyed)
/// combine are empty, so the window stage produces nothing there. A zero-row stream reads as
/// NULL, matching single-node scalar-subquery-no-rows semantics.
fn rewrite_scalar_having_conjunct(
    up: &Unparser,
    conjunct: &Expr,
    remap: &HashMap<String, String>,
    replicated: &[&str],
    streams: &mut Vec<Vec<StageDef>>,
) -> Result<String> {
    use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};

    // Find the scalar subqueries in the conjunct (exactly one supported per conjunct).
    let mut found: Vec<Expr> = Vec::new();
    let _ = conjunct.clone().apply(|node| {
        if matches!(node, Expr::ScalarSubquery(_)) {
            found.push(node.clone());
        }
        Ok(TreeNodeRecursion::Continue)
    });
    if found.is_empty() {
        let mapped = remap_expr_columns(conjunct, remap);
        return expr_sql(up, &mapped);
    }
    if found.len() > 1 {
        return Err(Error::Unsupported(
            "auto-distribute: window over an aggregation: multiple scalar subqueries in one \
             HAVING conjunct are not supported"
                .into(),
        ));
    }

    let stream = plan_scalar_having_stream(
        match &found[0] {
            Expr::ScalarSubquery(s) => s.subquery.as_ref(),
            _ => unreachable!("matched ScalarSubquery above"),
        },
        replicated,
    )?
    .ok_or_else(|| {
        Error::Unsupported(
            "auto-distribute: window over an aggregation: the HAVING's scalar subquery is not \
             an uncorrelated single-aggregate query over the sharded fact with every GROUP BY \
             key pinned to a literal"
                .into(),
        )
    })?;
    let input_pos = streams.len() + 1;
    streams.push(stream);

    // Replace the subquery with a token, remap columns to stage names, then substitute the
    // co-located stream reference for the token in the rendered SQL.
    let token = format!("__OXIDANT_SCALAR_STREAM_{}__", input_pos - 1);
    let replaced = conjunct
        .clone()
        .transform(|node| {
            if matches!(node, Expr::ScalarSubquery(_)) {
                return Ok(Transformed::yes(Expr::Literal(
                    ScalarValue::Utf8(Some(token.clone())),
                    None,
                )));
            }
            Ok(Transformed::no(node))
        })
        .map(|t| t.data)
        .map_err(|e| Error::Unsupported(format!("auto-distribute: rewrite scalar HAVING: {e}")))?;
    let mapped = remap_expr_columns(&replaced, remap);
    let sql = expr_sql(up, &mapped)?;
    Ok(sql.replace(
        &format!("'{token}'"),
        &format!("(SELECT m0 FROM shuffle_input_{input_pos})"),
    ))
}

/// Plan an uncorrelated scalar subquery from a window branch's HAVING as a partial/combine
/// stage pair whose one-row `m0` output gathers to partition 0 (empty hash key).
///
/// Shape (anything else returns `Ok(None)`): at most one single-expression projection layer
/// over a single non-DISTINCT min/max/sum/count/avg aggregate; no correlation; the aggregate's
/// `GROUP BY` keys are each pinned to a literal by an equality in the subquery's WHERE (so the
/// stream yields at most one row — Q44's `GROUP BY ss_store_sk` with `ss_store_sk = 4`); the
/// body scans exactly one sharded table exactly once with every other table replicated.
fn plan_scalar_having_stream(
    subquery: &LogicalPlan,
    replicated: &[&str],
) -> Result<Option<Vec<StageDef>>> {
    let mut sp = subquery;
    while let LogicalPlan::SubqueryAlias(s) = sp {
        sp = s.input.as_ref();
    }
    // One optional projection layer: a bare passthrough / rename of the aggregate output.
    if let LogicalPlan::Projection(pj) = sp {
        if pj.expr.len() != 1 {
            return Ok(None);
        }
        if !matches!(strip_alias(&pj.expr[0]), Expr::Column(_)) {
            return Ok(None);
        }
        sp = pj.input.as_ref();
    }
    let LogicalPlan::Aggregate(sub_agg) = sp else {
        return Ok(None);
    };
    if sub_agg.aggr_expr.len() != 1 || plan_contains_outer_reference(subquery) {
        return Ok(None);
    }
    let spec = match AggSpec::classify(&sub_agg.aggr_expr[0]) {
        Ok(s) => s,
        Err(_) => return Ok(None),
    };
    if spec.distinct || !matches!(spec.func.as_str(), "min" | "max" | "sum" | "count" | "avg") {
        return Ok(None);
    }

    // The WHERE conjuncts must all be inner-only predicates over the subquery's own FROM body.
    let mut preds: Vec<&Expr> = Vec::new();
    let mut body: &LogicalPlan = sub_agg.input.as_ref();
    while let LogicalPlan::Filter(f) = body {
        flatten_conjuncts(&f.predicate, &mut preds);
        body = f.input.as_ref();
    }
    if plan_has_filter_or_subquery_expr(body) {
        return Ok(None);
    }
    let scope = PlanScope::of(body);
    for conjunct in &preds {
        let mut cols = Vec::new();
        expr_columns_tagged(conjunct, &mut cols);
        if !cols
            .iter()
            .all(|(c, is_outer)| !is_outer && scope.contains(c))
        {
            return Ok(None);
        }
    }
    let mut arg_cols = Vec::new();
    expr_columns(&sub_agg.aggr_expr[0], &mut arg_cols);
    if !arg_cols.iter().all(|c| scope.contains(c)) {
        return Ok(None);
    }

    // Every GROUP BY key must be pinned to a literal by an equality conjunct, guaranteeing the
    // stream yields at most one row.
    for g in &sub_agg.group_expr {
        let pinned = preds.iter().any(|pr| {
            let Expr::BinaryExpr(b) = *pr else {
                return false;
            };
            if b.op != Operator::Eq {
                return false;
            }
            let matches_key =
                |e: &Expr| e == g || e.schema_name().to_string() == g.schema_name().to_string();
            (matches_key(&b.left) && matches!(b.right.as_ref(), Expr::Literal(..)))
                || (matches_key(&b.right) && matches!(b.left.as_ref(), Expr::Literal(..)))
        });
        if !pinned {
            return Ok(None);
        }
    }

    // Table safety: exactly one sharded table overall, scanned exactly once in the body.
    let body_tables = base_tables(body);
    let mut sharded: Vec<&str> = body_tables
        .iter()
        .map(String::as_str)
        .filter(|t| !replicated.contains(t))
        .collect();
    sharded.sort_unstable();
    sharded.dedup();
    let [fact] = sharded.as_slice() else {
        return Ok(None);
    };
    if count_table_scans(body, fact) != 1 {
        return Ok(None);
    }

    let up = Unparser::default();
    let body_sql = up
        .plan_to_sql(body)
        .map_err(|e| Error::Unsupported(format!("auto-distribute: unparse scalar body: {e}")))?
        .to_string();
    let tail = sanitize_generated_sql(&extract_from_tail(&body_sql)?);
    let where_sql = where_clause(&up, &preds)?;
    let mut psel: Vec<String> = Vec::new();
    for (j, g) in sub_agg.group_expr.iter().enumerate() {
        psel.push(format!("{} AS k{j}", expr_sql(&up, g)?));
    }
    let group_by = (0..sub_agg.group_expr.len())
        .map(|j| format!("k{j}"))
        .collect::<Vec<_>>()
        .join(", ");
    let (items, comb) = per_key_agg_parts(&spec.func, &spec.arg_sql, 0)?;
    psel.extend(items);
    let partial_sql = sanitize_generated_sql(&format!(
        "SELECT {} {tail}{where_sql}{}",
        psel.join(", "),
        if group_by.is_empty() {
            String::new()
        } else {
            format!(" GROUP BY {group_by}")
        }
    ));
    // The combine keeps the GROUP BY so an empty stream yields zero rows (read as NULL by the
    // window stage), then gathers to partition 0 (empty hash key).
    let combine_sql = if group_by.is_empty() {
        format!("SELECT {comb} AS m0 FROM shuffle_input")
    } else {
        format!("SELECT {comb} AS m0 FROM shuffle_input GROUP BY {group_by}")
    };
    let n_keys = sub_agg.group_expr.len() as u32;
    Ok(Some(vec![
        StageDef::new(0, partial_sql, vec![], (0..n_keys).collect()),
        StageDef::new(1, combine_sql, vec![0], vec![]),
    ]))
}

/// One uncorrelated `IN` subquery over the sharded fact, split out of the aggregate's input
/// filter chain (TPC-DS Q70's per-state top-5 filter).
struct AggInputInSplit<'a> {
    /// Remaining filter conjuncts (subquery-free), applied verbatim on the scan-export stage.
    regular: Vec<&'a Expr>,
    /// The exported outer-side key expression (a plain column).
    outer_key: &'a Expr,
    /// The subquery plan; must plan through the ordinary machinery to a one-column key stream.
    subquery: &'a LogicalPlan,
    /// The join body below the filter chain (broadcast-safe single-sharded-scan tree).
    body: &'a LogicalPlan,
}

/// Split one uncorrelated `IN (subquery over the sharded fact)` conjunct out of the aggregate
/// input's filter chain. Returns `Ok(None)` when there is no such conjunct (the ordinary
/// pipeline applies) or the shape is not exactly this one (the caller then falls back to the
/// ordinary pipeline, whose subquery safety checks reject it as before).
fn split_in_subquery_from_agg_input<'a>(
    agg: &'a Aggregate,
    replicated: &[&str],
) -> Result<Option<AggInputInSplit<'a>>> {
    let mut conjuncts: Vec<&Expr> = Vec::new();
    let mut body = agg.input.as_ref();
    while let LogicalPlan::Filter(f) = body {
        flatten_conjuncts(&f.predicate, &mut conjuncts);
        body = f.input.as_ref();
    }
    let mut outer_key: Option<&Expr> = None;
    let mut subquery: Option<&LogicalPlan> = None;
    let mut regular: Vec<&Expr> = Vec::new();
    for c in &conjuncts {
        if let Expr::InSubquery(iq) = *c {
            if iq.negated || subquery.is_some() || expr_contains_subquery(iq.expr.as_ref()) {
                return Ok(None);
            }
            if plan_contains_outer_reference(&iq.subquery.subquery) {
                return Ok(None);
            }
            // Only split when the subquery reads a sharded table; a replicated-only subquery
            // evaluates verbatim on every worker through the ordinary pipeline.
            let mut sub_tables = Vec::new();
            collect_subquery_tables(&iq.subquery.subquery, &mut sub_tables);
            for t in base_tables(&iq.subquery.subquery) {
                sub_tables.push(t);
            }
            if !sub_tables.iter().any(|t| !replicated.contains(&t.as_str())) {
                return Ok(None);
            }
            if !matches!(iq.expr.as_ref(), Expr::Column(_)) {
                return Ok(None);
            }
            outer_key = Some(iq.expr.as_ref());
            subquery = Some(iq.subquery.subquery.as_ref());
        } else {
            if expr_contains_subquery(c) {
                return Ok(None);
            }
            regular.push(*c);
        }
    }
    let (Some(outer_key), Some(subquery)) = (outer_key, subquery) else {
        return Ok(None);
    };
    Ok(Some(AggInputInSplit {
        regular,
        outer_key,
        subquery,
        body,
    }))
}

/// Build the partial→combine pipeline for a window-over-aggregate whose aggregate input filter
/// carries one uncorrelated `IN` subquery over the sharded fact (TPC-DS Q70):
///
/// 1. **Key stream**: the subquery plans through the ordinary machinery (a window-over-
///    aggregate sub-DAG for Q70's per-state rank) and its terminal stage re-targets its hash at
///    the single key column.
/// 2. **Scan export** (stage per worker): the join body with the subquery-free conjuncts,
///    projecting the flattened group columns (`gc{j}`), each aggregate's argument (`aa{i}`),
///    and the outer IN key (`j0`), hash-shuffled by `j0`.
/// 3. **Semi + partial**: rows whose `j0` matches the co-located key stream feed the ordinary
///    partial aggregate (co-location makes the per-partition `IN` globally exact); output hash
///    follows the grouping shape (gathered for ROLLUP).
/// 4. **Combine**: the ordinary recombine — `GROUP BY ROLLUP (g{j})` with `grouping()`
///    recomputation for Q70's ROLLUP, via [`final_group_by_sql`].
fn agg_pipeline_with_in_producer(
    agg: &Aggregate,
    split: &AggInputInSplit<'_>,
    replicated: &[&str],
) -> Result<DistributedQuery> {
    let up = Unparser::default();

    // 1. The key stream, planned recursively (its own strict-mode checks included).
    let mut sub_dq = plan_distributed_logical(split.subquery, replicated)?;
    if sub_dq.finalize_sql.is_some() || sub_dq.stages.is_empty() {
        return Err(Error::Unsupported(
            "auto-distribute: window over an aggregation: the IN subquery must plan to a \
             single un-ordered stream"
                .into(),
        ));
    }
    let key_fields = split.subquery.schema().fields();
    if key_fields.len() != 1 {
        return Err(Error::Unsupported(
            "auto-distribute: window over an aggregation: the IN subquery must project exactly \
             one key column"
                .into(),
        ));
    }
    let key_name = key_fields[0].name().clone();
    let key_terminal = sub_dq.stages.last_mut().expect("non-empty sub stages");
    if key_terminal.exchange == ExchangeMode::Forward {
        return Err(Error::Unsupported(
            "auto-distribute: window over an aggregation: the IN subquery must not be a \
             single-worker forward"
                .into(),
        ));
    }
    key_terminal.hash_key_cols = vec![0];
    let mut stages = sub_dq.stages;
    let key_stage_id = stages.last().expect("non-empty sub stages").stage_id;
    let mut next_id = key_stage_id + 1;

    // 2. The scan-export stage: the join body with the subquery-free conjuncts, exporting the
    // flattened group columns, aggregate arguments, and the outer IN key, hashed by the key.
    let group_sql: Vec<String> = flattened_group_exprs(&agg.group_expr)
        .into_iter()
        .map(|g| expr_sql(&up, g))
        .collect::<Result<_>>()?;
    let aggs = agg
        .aggr_expr
        .iter()
        .map(AggSpec::classify)
        .collect::<Result<Vec<_>>>()?;
    if aggs.iter().any(|a| a.distinct) {
        return Err(Error::Unsupported(
            "auto-distribute: window over an aggregation: DISTINCT aggregates do not compose \
             with the IN key stream"
                .into(),
        ));
    }
    let mut aggs = aggs;
    resolve_grouping_specs(&mut aggs, &agg.group_expr)?;
    let body_tables = base_tables(split.body);
    let body_sharded: Vec<&str> = body_tables
        .iter()
        .map(String::as_str)
        .filter(|t| !replicated.contains(t))
        .collect();
    let [sharded_name] = body_sharded.as_slice() else {
        return Err(Error::Unsupported(
            "auto-distribute: window over an aggregation: the aggregate input must scan \
             exactly one sharded table for the IN key stream"
                .into(),
        ));
    };
    reject_unsafe_broadcast_shapes(split.body, sharded_name)?;

    let mut export_cols: Vec<String> = group_sql
        .iter()
        .enumerate()
        .map(|(j, g)| format!("{g} AS gc{j}"))
        .collect();
    for (i, a) in aggs.iter().enumerate() {
        if a.grouping_target.is_some() {
            continue; // recomputed on the combine — no exported argument
        }
        export_cols.push(format!("{} AS aa{i}", a.arg_sql));
    }
    export_cols.push(format!("{} AS j0", expr_sql(&up, split.outer_key)?));
    let j0_idx = (export_cols.len() - 1) as u32;
    let body_sql = up
        .plan_to_sql(split.body)
        .map_err(|e| Error::Unsupported(format!("auto-distribute: unparse agg input: {e}")))?
        .to_string();
    let tail = sanitize_generated_sql(&extract_from_tail(&body_sql)?);
    let where_sql = where_clause(&up, &split.regular)?;
    let export_sql = sanitize_generated_sql(&format!(
        "SELECT {} {tail}{where_sql}",
        export_cols.join(", ")
    ));
    let export_id = next_id;
    next_id += 1;
    stages.push(StageDef::new(export_id, export_sql, vec![], vec![j0_idx]));

    // 3. The semi + partial stage: rows whose key matches the co-located stream feed the
    // ordinary partial aggregate.
    let n_group = group_sql.len();
    let mut psel: Vec<String> = (0..n_group).map(|j| format!("gc{j} AS g{j}")).collect();
    for (i, a) in aggs.iter().enumerate() {
        if a.grouping_target.is_some() {
            continue;
        }
        let (items, _) = partial_combine_sql(&a.func, i, &format!("aa{i}"))?;
        psel.extend(items);
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
    // Coarser grouping-set levels span multiple finest-level keys, so ROLLUP gathers (empty
    // hash key); an ordinary grouped aggregate hashes by the group key.
    let semi_hash: Vec<u32> = if is_grouping_set(&agg.group_expr) {
        vec![]
    } else {
        (0..n_group as u32).collect()
    };
    stages.push(StageDef::new(
        semi_id,
        semi_sql,
        vec![export_id, key_stage_id],
        semi_hash,
    ));

    // 4. The combine stage: the ordinary recombine (ROLLUP-aware), wrapped like the synthetic
    // aggregation pipeline's final stage (`SELECT * FROM (…) AS combined`).
    let (_, combine) = partial_and_combine_lists(
        &(0..n_group).map(|j| format!("g{j}")).collect::<Vec<_>>(),
        &aggs,
    )?;
    let final_group_by = final_group_by_sql(&agg.group_expr, n_group)?;
    let reject_empty = if is_grouping_set(&agg.group_expr) {
        " HAVING COUNT(*) > 0"
    } else {
        ""
    };
    let inner = format!(
        "SELECT {} FROM shuffle_input GROUP BY {final_group_by}{reject_empty}",
        combine.join(", ")
    );
    let synthetic = Peeled {
        projection: None,
        sort: None,
        limit: None,
        having: Vec::new(),
        alias_projections: Vec::new(),
        agg,
    };
    let combine_sql = wrap_output(&synthetic, &inner, &build_remap(&synthetic))?;
    let combine_id = next_id;
    stages.push(StageDef::new(
        combine_id,
        combine_sql,
        vec![semi_id],
        vec![],
    ));

    Ok(DistributedQuery {
        stages,
        finalize_sql: None,
    })
}

/// KAN-49b (TPC-DS Q36): a ranking window over a `UNION` (DISTINCT) whose arms are
/// independently-distributable aggregates over one shared sharded-fact CTE.
///
/// Each arm plans through the ordinary machinery (the CTE's partial aggregate never leaves the
/// workers); the arms' outputs hash-shuffle on the **full row** into a per-partition dedup
/// (identical rows co-locate, so the local `DISTINCT` is globally exact), and the window then
/// re-shuffles the tiny deduplicated result by its `PARTITION BY` key — expression keys such as
/// `CASE WHEN t_class = 0 THEN i_category END` materialize as computed columns on the dedup
/// stage.
fn window_over_distinct_union_stages_for(
    p: &WindowPeeled<'_>,
    window: &Window,
    distinct_node: &LogicalPlan,
    replicated: &[&str],
) -> Result<DistributedQuery> {
    let unsupported = |why: &str| {
        Error::Unsupported(format!(
            "auto-distribute: window over an aggregation: window over UNION: {why}"
        ))
    };
    let mut arms: Vec<&LogicalPlan> = Vec::new();
    flatten_distinct_union(distinct_node, &mut arms);
    if arms.len() < 2 {
        return Err(unsupported("fewer than two union arms"));
    }
    let partition_by = validate_window_layer(window)?;
    if partition_by.is_empty() {
        return Err(unsupported("a global window over a union is not supported"));
    }
    let up = Unparser::default();

    // Plan each arm; the terminal stage of every arm re-targets its hash at the full row.
    let mut stages: Vec<StageDef> = Vec::new();
    let mut arm_outputs: Vec<u32> = Vec::new();
    for arm in &arms {
        if !plan_contains_aggregate(arm) {
            // A raw sharded-scan arm would gather the fact wholesale.
            return Err(unsupported("a union arm without an aggregate"));
        }
        let mut arm_dq = plan_distributed_logical(arm, replicated).map_err(|e| {
            Error::Unsupported(format!(
                "auto-distribute: window over an aggregation: window over UNION arm: {e}"
            ))
        })?;
        if arm_dq.finalize_sql.is_some() {
            return Err(unsupported("a union arm with ORDER BY / LIMIT"));
        }
        let width = arm.schema().fields().len() as u32;
        let offset = stages.iter().map(|s| s.stage_id).max().map_or(0, |m| m + 1);
        let terminal = arm_dq.stages.last_mut().expect("non-empty arm stages");
        terminal.hash_key_cols = (0..width).collect();
        for mut s in arm_dq.stages {
            s.stage_id += offset;
            for u in &mut s.upstream_stage_ids {
                *u += offset;
            }
            stages.push(s);
        }
        arm_outputs.push(stages.last().expect("arm stage appended").stage_id);
    }
    let mut next_id = stages.iter().map(|s| s.stage_id).max().expect("stages") + 1;

    // Union column names come from the first arm (SQL unions match by position).
    let union_cols: Vec<String> = arms[0]
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    let mut remap: HashMap<String, String> = HashMap::new();
    for name in &union_cols {
        remap.insert(name.clone(), name.clone());
    }
    for (qualifier, field) in p.window.schema.iter() {
        if let Some(q) = qualifier {
            remap.insert(format!("{q}.{}", field.name()), field.name().clone());
        }
    }

    // Partition keys: plain columns map positionally; expressions materialize on the dedup
    // stage so the shuffle can hash them.
    let mut key_cols: Vec<u32> = Vec::with_capacity(partition_by.len());
    let mut computed: Vec<(String, String)> = Vec::new();
    for e in &partition_by {
        if let Expr::Column(c) = e {
            if let Some(name) = remap.get(&c.flat_name()).or_else(|| remap.get(&c.name)) {
                if let Some(idx) = union_cols.iter().position(|u| u == name) {
                    key_cols.push(idx as u32);
                    continue;
                }
            }
        }
        let mapped = remap_expr_columns(e, &remap);
        ensure_cols_named(&mapped, &union_cols).map_err(|bad| {
            Error::Unsupported(format!(
                "auto-distribute: window over an aggregation: window over UNION: PARTITION BY \
                 expression references `{bad}`, not a union output column"
            ))
        })?;
        let name = format!("c{}", computed.len());
        key_cols.push((union_cols.len() + computed.len()) as u32);
        remap.insert(e.schema_name().to_string(), name.clone());
        computed.push((expr_sql(&up, &mapped)?, name));
    }

    // The dedup stage: per-partition DISTINCT over the union of arm streams (identical rows
    // co-locate on the full-row hash), with computed key columns appended.
    let union_all = (0..arms.len())
        .map(|i| format!("SELECT * FROM shuffle_input_{i}"))
        .collect::<Vec<_>>()
        .join(" UNION ALL ");
    let select = if computed.is_empty() {
        "*".to_string()
    } else {
        format!(
            "*, {}",
            computed
                .iter()
                .map(|(sql, name)| format!("{sql} AS {name}"))
                .collect::<Vec<_>>()
                .join(", ")
        )
    };
    let dedup_sql = sanitize_generated_sql(&format!(
        "SELECT DISTINCT {select} FROM ({union_all}) AS all_arms"
    ));
    let dedup_id = next_id;
    next_id += 1;
    stages.push(StageDef::new(
        dedup_id,
        dedup_sql,
        arm_outputs.clone(),
        key_cols,
    ));

    // The window stage over the deduplicated rows.
    let mut stage_cols = union_cols.clone();
    stage_cols.extend(computed.iter().map(|(_, name)| name.clone()));
    let mut next_w = 0usize;
    let window_inner =
        build_window_over_agg_inner(window, &remap, &stage_cols, &mut next_w, "shuffle_input")?;
    let mut full_remap = remap;
    for (i, e) in window.window_expr.iter().enumerate() {
        full_remap.insert(e.schema_name().to_string(), format!("w{i}"));
    }
    for proj in &p.alias_projections {
        for e in proj.iter() {
            let Expr::Alias(a) = e else { continue };
            let mapped = match a.expr.as_ref() {
                Expr::Column(c) => full_remap
                    .get(&c.flat_name())
                    .or_else(|| full_remap.get(&c.name))
                    .cloned(),
                other => full_remap.get(&other.schema_name().to_string()).cloned(),
            };
            if let Some(mapped) = mapped {
                full_remap.insert(a.name.clone(), mapped);
            }
        }
    }
    let window_sql = wrap_window_over_agg_output(p, &window_inner, &full_remap)?;
    stages.push(StageDef::new(next_id, window_sql, vec![dedup_id], vec![]));

    Ok(DistributedQuery {
        stages,
        finalize_sql: build_outer_finalize(p.sort, p.limit)?,
    })
}

/// KAN-49b (TPC-DS Q51): framed windows over a `FULL OUTER JOIN` of two windowed aggregates.
///
/// The sharded side runs the ordinary window-over-aggregate pipeline (partial → combine →
/// partition-shuffled framed window); the replicated side computes once on a single `Forward`
/// worker. Both sides shuffle by the equijoin key for an exact co-located full join (rows with
/// equal keys co-locate; preserved-side rows can never straddle partitions), the CASE/renaming
/// projection over the join re-applies in the join stage, and the outer framed windows compute
/// after a final partition-keyed shuffle. KAN-162 admits BOTH sides sharded — each side's exact
/// output rows hash by the same join key, so the co-location argument is unchanged.
fn window_over_join_stages_for(
    p: &WindowPeeled<'_>,
    window: &Window,
    proj: &datafusion::logical_expr::logical_plan::Projection,
    replicated: &[&str],
) -> Result<DistributedQuery> {
    let unsupported = |why: &str| {
        Error::Unsupported(format!(
            "auto-distribute: window over an aggregation: window over join: {why}"
        ))
    };
    let LogicalPlan::Join(join) = proj.input.as_ref() else {
        return Err(unsupported("projection input is not a join"));
    };
    if join.join_type != JoinType::Full {
        return Err(unsupported(
            "only a FULL OUTER JOIN of two windowed aggregates",
        ));
    }
    let partition_by = validate_window_layer(window)?;
    if partition_by.is_empty() {
        return Err(unsupported("a global window over a join is not supported"));
    }
    let up = Unparser::default();

    // Equijoin key pairs (one column per side), from `on` and/or the join filter.
    let mut key_pairs: Vec<(Column, Column)> = Vec::new();
    for (l, r) in &join.on {
        match (l, r) {
            (Expr::Column(lc), Expr::Column(rc)) => key_pairs.push((lc.clone(), rc.clone())),
            _ => return Err(unsupported("join key must be a column pair")),
        }
    }
    if let Some(filter) = &join.filter {
        let mut conjuncts: Vec<&Expr> = Vec::new();
        flatten_conjuncts(filter, &mut conjuncts);
        for c in conjuncts {
            let Expr::BinaryExpr(b) = c else {
                return Err(unsupported("join filter must be equality keys only"));
            };
            if b.op != Operator::Eq {
                return Err(unsupported("join filter must be equality keys only"));
            }
            match (b.left.as_ref(), b.right.as_ref()) {
                (Expr::Column(lc), Expr::Column(rc)) => key_pairs.push((lc.clone(), rc.clone())),
                _ => return Err(unsupported("join key must be a column pair")),
            }
        }
    }
    if key_pairs.is_empty() {
        return Err(unsupported("no equijoin keys"));
    }

    // Classify the sides: a side scanning any sharded table is planned through the shape
    // planners; an all-replicated side is computed once on a Forward worker. Both sides may be
    // sharded (KAN-162, TPC-DS Q51 at the all-facts-sharded classification): each sharded side
    // runs the exact partial → combine → partition-shuffle window pipeline in the loop below
    // and its terminal rows hash by the same join key, so equal keys co-locate and the FULL
    // OUTER JOIN matches key-locally (a preserved-side row hashes to exactly one partition).
    // The KAN-49b both-sharded refusal was an artifact of the single-sharded classification,
    // not an exactness guard — the per-side requirements enforced in the planning loop below
    // (a windowed-aggregate branch, join keys resolvable against the side's output columns,
    // the outer PARTITION BY expressible over the join output) are the real admission and
    // still refuse every other shape.
    let side_is_sharded = |side: &LogicalPlan| {
        let mut tables = base_tables(side);
        collect_subquery_tables(side, &mut tables);
        tables.iter().any(|t| !replicated.contains(&t.as_str()))
    };
    let left_sharded = side_is_sharded(join.left.as_ref());
    let right_sharded = side_is_sharded(join.right.as_ref());

    let mut stages: Vec<StageDef> = Vec::new();
    let mut side_terminals: Vec<u32> = Vec::new();
    for (side, sharded) in [
        (join.left.as_ref(), left_sharded),
        (join.right.as_ref(), right_sharded),
    ] {
        let key_idx: Vec<u32> = key_pairs
            .iter()
            .map(|(lc, rc)| {
                let key_col = if std::ptr::eq(side, join.left.as_ref()) {
                    lc
                } else {
                    rc
                };
                side.schema()
                    .index_of_column(key_col)
                    .map(|i| i as u32)
                    .map_err(|_| {
                        Error::Unsupported(format!(
                            "auto-distribute: window over an aggregation: window over join: \
                             join key `{key_col}` is not a branch output column"
                        ))
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        let offset = stages.iter().map(|s| s.stage_id).max().map_or(0, |m| m + 1);
        if sharded {
            if !plan_contains_window(side) || !plan_contains_aggregate(side) {
                return Err(unsupported(
                    "the sharded join side is not a windowed aggregate branch",
                ));
            }
            let mut side_dq = plan_distributed_logical(side, replicated).map_err(|e| {
                Error::Unsupported(format!(
                    "auto-distribute: window over an aggregation: window over join side: {e}"
                ))
            })?;
            if side_dq.finalize_sql.is_some() {
                return Err(unsupported("a join side with ORDER BY / LIMIT"));
            }
            let terminal = side_dq.stages.last_mut().expect("non-empty side stages");
            terminal.hash_key_cols = key_idx;
            for mut s in side_dq.stages {
                s.stage_id += offset;
                for u in &mut s.upstream_stage_ids {
                    *u += offset;
                }
                stages.push(s);
            }
        } else {
            // All-replicated side: computed exactly once on one worker (every worker holds the
            // full replicated inputs), its exact output hash-shuffled by the join key.
            let sql = up
                .plan_to_sql(side)
                .map_err(|e| {
                    Error::Unsupported(format!("auto-distribute: unparse join side: {e}"))
                })?
                .to_string();
            let mut stage = StageDef::new(offset, sanitize_generated_sql(&sql), vec![], key_idx);
            stage.exchange = ExchangeMode::Forward;
            stages.push(stage);
        }
        side_terminals.push(stages.last().expect("side stage appended").stage_id);
    }
    let mut next_id = stages.iter().map(|s| s.stage_id).max().expect("stages") + 1;

    // The join stage: the projection-over-join unparsed with each side replaced by its
    // shuffle-input placeholder (`shuffle_input_0`/`shuffle_input_1`).
    let branch_by_node: HashMap<usize, usize> = [
        (join.left.as_ref() as *const LogicalPlan as usize, 0usize),
        (join.right.as_ref() as *const LogicalPlan as usize, 1usize),
    ]
    .into_iter()
    .collect();
    let proj_plan = LogicalPlan::Projection(proj.clone());
    let (rewritten, changed) =
        super::dag_splitter::replace_branches(&proj_plan, &branch_by_node, 2)?;
    if !changed {
        return Err(unsupported("join sides were not replaced by placeholders"));
    }
    let join_sql = up
        .plan_to_sql(&rewritten)
        .map_err(|e| Error::Unsupported(format!("auto-distribute: unparse join stage: {e}")))?
        .to_string();

    // Hash the join output by the outer window's partition columns (expressions materialize as
    // computed columns on the join stage).
    let join_cols: Vec<String> = rewritten
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    let mut remap: HashMap<String, String> = HashMap::new();
    for name in &join_cols {
        remap.insert(name.clone(), name.clone());
    }
    let mut key_cols: Vec<u32> = Vec::with_capacity(partition_by.len());
    let mut computed: Vec<(String, String)> = Vec::new();
    for e in &partition_by {
        if let Expr::Column(c) = e {
            if let Some(idx) = join_cols.iter().position(|n| n == &c.name) {
                key_cols.push(idx as u32);
                continue;
            }
        }
        let mapped = remap_expr_columns(&unqualify(e), &remap);
        ensure_cols_named(&mapped, &join_cols).map_err(|bad| {
            Error::Unsupported(format!(
                "auto-distribute: window over an aggregation: window over join: PARTITION BY \
                 expression references `{bad}`, not a join output column"
            ))
        })?;
        let name = format!("c{}", computed.len());
        key_cols.push((join_cols.len() + computed.len()) as u32);
        remap.insert(e.schema_name().to_string(), name.clone());
        computed.push((expr_sql(&up, &mapped)?, name));
    }
    let join_sql = if computed.is_empty() {
        sanitize_generated_sql(&join_sql)
    } else {
        sanitize_generated_sql(&format!(
            "SELECT *, {} FROM ({join_sql}) AS keyed_in",
            computed
                .iter()
                .map(|(sql, name)| format!("{sql} AS {name}"))
                .collect::<Vec<_>>()
                .join(", ")
        ))
    };
    let join_id = next_id;
    next_id += 1;
    stages.push(StageDef::new(
        join_id,
        join_sql,
        side_terminals.clone(),
        key_cols,
    ));

    // The outer window stage over the join rows.
    let mut stage_cols = join_cols.clone();
    stage_cols.extend(computed.iter().map(|(_, name)| name.clone()));
    let mut next_w = 0usize;
    let window_inner =
        build_window_over_agg_inner(window, &remap, &stage_cols, &mut next_w, "shuffle_input")?;
    let mut full_remap = remap;
    for (i, e) in window.window_expr.iter().enumerate() {
        full_remap.insert(e.schema_name().to_string(), format!("w{i}"));
    }
    for proj in &p.alias_projections {
        for e in proj.iter() {
            let Expr::Alias(a) = e else { continue };
            let mapped = match a.expr.as_ref() {
                Expr::Column(c) => full_remap
                    .get(&c.flat_name())
                    .or_else(|| full_remap.get(&c.name))
                    .cloned(),
                other => full_remap.get(&other.schema_name().to_string()).cloned(),
            };
            if let Some(mapped) = mapped {
                full_remap.insert(a.name.clone(), mapped);
            }
        }
    }
    let window_sql = wrap_window_over_agg_output(p, &window_inner, &full_remap)?;
    stages.push(StageDef::new(next_id, window_sql, vec![join_id], vec![]));

    Ok(DistributedQuery {
        stages,
        finalize_sql: build_outer_finalize(p.sort, p.limit)?,
    })
}

/// Require every column in an already-remapped expression to name one of `cols` (unqualified).
fn ensure_cols_named(e: &Expr, cols: &[String]) -> std::result::Result<(), String> {
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
    let mut bad: Option<String> = None;
    let _ = e.apply(|node| {
        if let Expr::Column(c) = node {
            if c.relation.is_some() || !cols.iter().any(|n| n == &c.name) {
                bad = Some(c.flat_name());
                return Ok(TreeNodeRecursion::Stop);
            }
        }
        Ok(TreeNodeRecursion::Continue)
    });
    match bad {
        Some(name) => Err(name),
        None => Ok(()),
    }
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
pub(crate) fn remap_expr_columns(e: &Expr, remap: &HashMap<String, String>) -> Expr {
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

/// Partial `SELECT` item(s) and the combine expression (over those partials) for one per-key
/// aggregate at output position `i`, mirroring `stage_planner::partial_combine_sql`'s
/// min/max/sum/count/avg rules (single-result aggregates only — no DISTINCT).
pub(crate) fn per_key_agg_parts(
    func: &str,
    arg_sql: &str,
    i: usize,
) -> Result<(Vec<String>, String)> {
    match func {
        "sum" => Ok((
            vec![format!("sum({arg_sql}) AS a{i}")],
            format!("sum(a{i})"),
        )),
        // counts recombine by summing
        "count" => Ok((
            vec![format!("count({arg_sql}) AS a{i}")],
            format!("sum(a{i})"),
        )),
        "min" => Ok((
            vec![format!("min({arg_sql}) AS a{i}")],
            format!("min(a{i})"),
        )),
        "max" => Ok((
            vec![format!("max({arg_sql}) AS a{i}")],
            format!("max(a{i})"),
        )),
        // No cast: SUM/COUNT keep DataFusion's own AVG result type (see partial_combine_sql).
        "avg" => Ok((
            vec![
                format!("sum({arg_sql}) AS a{i}s"),
                format!("count({arg_sql}) AS a{i}c"),
            ],
            format!("(sum(a{i}s) / NULLIF(sum(a{i}c), 0))"),
        )),
        other => Err(Error::Unsupported(format!(
            "auto-distribute: aggregate `{other}` not supported"
        ))),
    }
}

/// Decorrelate a correlated scalar min/max/sum/count subquery over the sharded fact into a
/// distributed per-key aggregation hash-joined against the outer scan (TPC-H Q2):
///
/// ```sql
/// SELECT … FROM part, supplier, partsupp, nation, region
/// WHERE <join/filter preds>
///   AND ps_supplycost = (SELECT min(ps_supplycost) FROM partsupp, supplier, nation, region
///                        WHERE p_partkey = ps_partkey AND <inner preds>)
/// ```
///
/// becomes four stages:
///
/// 1. **Partial aggregate**: `SELECT <inner keys> AS k{j}, <func>(<arg>) AS a0 FROM <inner tail>
///    WHERE <inner-only preds> GROUP BY <inner keys>` per worker (the correlation equality's inner
///    side becomes the group key), hash-shuffled by `k{j}`.
/// 2. **Combine**: re-aggregate per key (`min→min`, `sum→sum`, `count→sum`), still hashed by `k{j}`
///    so its output co-locates with the outer scan's rows.
/// 3. **Outer scan**: the original FROM/WHERE minus the subquery conjunct, exporting the outer key
///    expressions (`ok{j}`), the compared expression (`cmp0`), and every column the output
///    projection needs (`oc{i}`), hash-shuffled by `ok{j}` — the same values as `k{j}` by the
///    correlation equality, so matching rows land on the same partition.
/// 4. **Join**: `m JOIN o ON m.k{j} = o.ok{j} AND o.cmp0 = m.m0`, re-applying the output
///    projection. The per-key aggregate emits at most one row per key, so the join cannot fan out;
///    an outer row whose key has no group is dropped, exactly like the original `= NULL` outcome.
///
/// The compare may be any equality/ordering operator: a non-equality compare (TPC-H Q17's
/// `l_quantity < (SELECT 0.2 * avg(l_quantity) …)`, including the AVG partial decomposition and
/// the scalar's projection over the aggregate) becomes a residual on the same co-located join,
/// and one ungrouped aggregate layer above the filter is aggregated in a partial/combine pair
/// after the join. Three TPC-DS Q41 extensions ride the same skeleton: a `(subquery) <cmp> x`
/// compare with the subquery on the *left* (mirrored into the canonical `o.cmp0 <op> m.m0`
/// form), a correlation equality repeated inside every arm of a disjunctive residual (factored
/// out — `(c AND A) OR (c AND B)` ≡ `c AND (A OR B)`), and a `SELECT DISTINCT` wrapper over the
/// output (re-applied after a full-row hash shuffle of the join output, since duplicate output
/// rows may land on different partitions). Anything outside that shape (uncorrelated subquery —
/// TPC-H Q11's global threshold, handled by [`try_uncorrelated_scalar_threshold`] — DISTINCT
/// over an outer aggregate, a grouped aggregate on top, a second sharded table anywhere, nested
/// expression subqueries) returns `Ok(None)` so the caller falls through to the other shapes
/// and ultimately the gather / rejection paths.
pub(crate) fn try_decorrelate_scalar_subquery(
    lp: &LogicalPlan,
    replicated: &[&str],
) -> Result<Option<DistributedQuery>> {
    // Peel the query top: trailing LIMIT / ORDER BY, the output projection, then the outer
    // WHERE conjuncts over the FROM body.
    let mut sort = None;
    let mut limit = None;
    let mut projection: Option<&[Expr]> = None;
    // A `SELECT DISTINCT` wrapper (TPC-DS Q41) — records the DISTINCT node's output width. The
    // decorrelated join below keeps each outer row whose correlation key's per-key aggregate
    // satisfies the compare; duplicate *output* rows may still land on different partitions, so
    // the DISTINCT is re-applied after a full-row hash shuffle (see the tail of this function).
    let mut distinct: Option<usize> = None;
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
            LogicalPlan::SubqueryAlias(s) => node = s.input.as_ref(),
            LogicalPlan::Distinct(d) => {
                if distinct.is_some() {
                    return Ok(None);
                }
                distinct = Some(node.schema().fields().len());
                node = d.input().as_ref();
            }
            LogicalPlan::Projection(p) => {
                if projection.is_none() {
                    projection = Some(p.expr.as_slice());
                }
                node = p.input.as_ref();
            }
            _ => break,
        }
    }
    // TPC-H Q17: the correlated scalar filter may sit directly under a *global* aggregate
    // (`SELECT sum(l_extendedprice) / 7.0 FROM … WHERE l_quantity < (SELECT 0.2 * avg(…) …)`).
    // Capture one ungrouped aggregate layer so the filtered join rows can be aggregated after
    // the join; grouped aggregates and non-recombinable functions stay on the gather path.
    let outer_agg = match node {
        LogicalPlan::Aggregate(a) if a.group_expr.is_empty() => {
            let aggs = a
                .aggr_expr
                .iter()
                .map(AggSpec::classify)
                .collect::<Result<Vec<_>>>()?;
            if aggs.iter().any(|s| s.distinct)
                || !aggs
                    .iter()
                    .all(|s| matches!(s.func.as_str(), "min" | "max" | "sum" | "count"))
            {
                return Ok(None);
            }
            // Without an output projection above the aggregate there is no rename to re-apply
            // and the gathered plan preserves the schema better; decline.
            if projection.is_none() {
                return Ok(None);
            }
            node = a.input.as_ref();
            Some((a, aggs))
        }
        _ => None,
    };
    // A DISTINCT on top of an outer global aggregate is a different shape (dedup over one
    // combined row vs over the joined rows); keep it on the existing paths.
    if outer_agg.is_some() && distinct.is_some() {
        return Ok(None);
    }
    let mut conjuncts: Vec<&Expr> = Vec::new();
    let mut body = node;
    while let LogicalPlan::Filter(f) = body {
        flatten_conjuncts(&f.predicate, &mut conjuncts);
        body = f.input.as_ref();
    }
    // A grouped aggregate left in the body means the scalar sits in a HAVING (post-aggregation)
    // position — a different shape (KAN-27's threshold / the gather), not this WHERE-position
    // decorrelation.
    if conjuncts.is_empty()
        || plan_has_filter_or_subquery_expr(body)
        || plan_contains_aggregate(body)
    {
        return Ok(None);
    }

    // Find the single `<expr> <cmp> <scalar subquery>` conjunct. An equality compare joins the
    // per-key aggregate back on `=`; a non-equality compare (TPC-H Q17) keeps the compare as a
    // residual next to the key join — either way an outer row whose key has no group is dropped,
    // exactly like the original `<cmp> NULL` outcome.
    let mut found: Option<(usize, &Expr, &LogicalPlan, Operator)> = None;
    for (i, conjunct) in conjuncts.iter().enumerate() {
        let Expr::BinaryExpr(b) = *conjunct else {
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
        let (compare, subquery, op) = match (b.left.as_ref(), b.right.as_ref()) {
            // `(subquery) <cmp> x` (TPC-DS Q41's `(SELECT count(*) …) > 0`) mirrors to
            // `x <cmp'> (subquery)` so the co-located join below can always write the
            // compare as `o.cmp0 <op> m.m0`.
            (Expr::ScalarSubquery(s), other) => {
                (other, s.subquery.as_ref(), mirror_compare_op(b.op))
            }
            (other, Expr::ScalarSubquery(s)) => (other, s.subquery.as_ref(), b.op),
            _ => continue,
        };
        if found.is_some() || expr_contains_subquery(compare) {
            return Ok(None);
        }
        found = Some((i, compare, subquery, op));
    }
    let Some((sub_idx, compare_expr, subplan, compare_op)) = found else {
        return Ok(None);
    };
    if conjuncts
        .iter()
        .enumerate()
        .any(|(i, c)| i != sub_idx && expr_contains_subquery(c))
    {
        return Ok(None);
    }

    // The subquery must be a global aggregate under at most one single-expression projection
    // layer (TPC-H Q17's `0.2 * avg(l_quantity)`, re-applied over the combined per-key value
    // below). Anything more stays on the gather path rather than being silently dropped.
    let mut sub_projection: Option<&[Expr]> = None;
    let mut sp = subplan;
    while let LogicalPlan::Projection(p) = sp {
        if sub_projection.is_some() || p.expr.len() != 1 {
            return Ok(None);
        }
        sub_projection = Some(p.expr.as_slice());
        sp = p.input.as_ref();
    }
    let LogicalPlan::Aggregate(sub_agg) = sp else {
        return Ok(None);
    };
    if !sub_agg.group_expr.is_empty() || sub_agg.aggr_expr.len() != 1 {
        return Ok(None);
    }
    let spec = match AggSpec::classify(&sub_agg.aggr_expr[0]) {
        Ok(s) => s,
        Err(_) => return Ok(None),
    };
    if spec.distinct || !matches!(spec.func.as_str(), "min" | "max" | "sum" | "count" | "avg") {
        return Ok(None);
    }

    // Split the subquery's WHERE conjuncts into inner-only predicates and correlation
    // equalities (`<outer col> = <inner col>`).
    let mut inner_conjuncts: Vec<&Expr> = Vec::new();
    let mut inner_body: &LogicalPlan = sub_agg.input.as_ref();
    while let LogicalPlan::Filter(f) = inner_body {
        flatten_conjuncts(&f.predicate, &mut inner_conjuncts);
        inner_body = f.input.as_ref();
    }
    if plan_has_filter_or_subquery_expr(inner_body) {
        return Ok(None);
    }
    let scope = PlanScope::of(inner_body);
    let mut corr_pairs: Vec<(Expr, Expr)> = Vec::new(); // (outer key, inner key)
    let mut inner_preds: Vec<Expr> = Vec::new();
    let is_inner_only = |e: &Expr| {
        let mut cols = Vec::new();
        expr_columns_tagged(e, &mut cols);
        cols.iter()
            .all(|(c, is_outer)| !is_outer && scope.contains(c))
    };
    // Classify one non-inner-only conjunct as a correlation equality
    // (`<outer col> = <inner col>`), pushing the (outer, inner) key pair.
    let classify_corr = |conjunct: &Expr, corr_pairs: &mut Vec<(Expr, Expr)>| -> bool {
        let Expr::BinaryExpr(b) = conjunct else {
            return false;
        };
        if b.op != Operator::Eq {
            return false;
        }
        // A correlation side arrives as `outer_ref(col)` / an out-of-scope column; the inner
        // side must be a plain in-scope column (it becomes the per-key group key).
        let side = |e: &Expr| -> Option<(Column, bool)> {
            match e {
                Expr::Column(c) => Some((c.clone(), false)),
                Expr::OuterReferenceColumn(_, c) => Some((c.clone(), true)),
                _ => None,
            }
        };
        let (Some((lc, l_outer)), Some((rc, r_outer))) = (side(&b.left), side(&b.right)) else {
            return false;
        };
        let l_inner = !l_outer && scope.contains(&lc);
        let r_inner = !r_outer && scope.contains(&rc);
        match (l_inner, r_inner) {
            (true, false) => corr_pairs.push((Expr::Column(rc), Expr::Column(lc))),
            (false, true) => corr_pairs.push((Expr::Column(lc), Expr::Column(rc))),
            // Both-outer is not a correlation predicate we can group by; both-inner was
            // already classified as an inner-only predicate above.
            _ => return false,
        }
        true
    };
    for conjunct in inner_conjuncts {
        if is_inner_only(conjunct) {
            inner_preds.push(conjunct.clone());
            continue;
        }
        // A disjunction may repeat the correlation equality in every arm (TPC-DS Q41's
        // `(m = o.m AND A) OR (m = o.m AND B)`): factor the shared conjuncts out, then
        // classify each piece — the residual disjunction must end up inner-only.
        let (shared, residual) = factor_or_common(conjunct);
        if shared.is_empty() {
            if !classify_corr(conjunct, &mut corr_pairs) {
                return Ok(None);
            }
            continue;
        }
        for c in &shared {
            if is_inner_only(c) {
                inner_preds.push(c.clone());
            } else if !classify_corr(c, &mut corr_pairs) {
                return Ok(None);
            }
        }
        if is_inner_only(&residual) {
            inner_preds.push(residual);
        } else if !classify_corr(&residual, &mut corr_pairs) {
            return Ok(None);
        }
    }
    // Uncorrelated scalar subqueries (TPC-H Q11's global HAVING threshold) are handled upstream
    // by `try_uncorrelated_scalar_threshold` (one-row broadcast + literal injection); this path
    // requires correlation keys to group by, so it declines here.
    if corr_pairs.is_empty() {
        return Ok(None);
    }
    let mut arg_cols = Vec::new();
    expr_columns(&sub_agg.aggr_expr[0], &mut arg_cols);
    if !arg_cols.iter().all(|c| scope.contains(c)) {
        return Ok(None);
    }

    // Table safety: exactly one sharded table overall (the fact), scanned exactly once in the
    // outer body and once inside the subquery; every other table replicated.
    let inner_tables = base_tables(inner_body);
    let mut inner_sharded: Vec<&str> = inner_tables
        .iter()
        .map(String::as_str)
        .filter(|t| !replicated.contains(t))
        .collect();
    inner_sharded.sort_unstable();
    inner_sharded.dedup();
    let [fact] = inner_sharded.as_slice() else {
        return Ok(None);
    };
    if count_table_scans(inner_body, fact) != 1 {
        return Ok(None);
    }
    for t in base_tables(body) {
        if t != *fact && !replicated.contains(&t.as_str()) {
            return Ok(None);
        }
    }
    if count_table_scans(body, fact) != 1 {
        return Ok(None);
    }
    // The outer key / compare expressions must resolve against the outer body.
    let outer_scope = PlanScope::of(body);
    let mut outer_refs = Vec::new();
    expr_columns(compare_expr, &mut outer_refs);
    for (outer_key, _) in &corr_pairs {
        expr_columns(outer_key, &mut outer_refs);
    }
    if !outer_refs.iter().all(|c| outer_scope.contains(c)) {
        return Ok(None);
    }

    let up = Unparser::default();
    let n_keys = corr_pairs.len() as u32;

    // Stage 0: partial per-key aggregate over the subquery's FROM/WHERE minus correlation.
    let inner_sql = up
        .plan_to_sql(inner_body)
        .map_err(|e| Error::Unsupported(format!("auto-distribute: unparse subquery body: {e}")))?
        .to_string();
    let inner_tail = sanitize_generated_sql(&extract_from_tail(&inner_sql)?);
    let inner_where = where_clause(&up, &inner_preds.iter().collect::<Vec<_>>())?;
    let inner_key_sql: Vec<String> = corr_pairs
        .iter()
        .map(|(_, inner_key)| expr_sql(&up, inner_key))
        .collect::<Result<_>>()?;
    let mut psel: Vec<String> = inner_key_sql
        .iter()
        .enumerate()
        .map(|(j, k)| format!("{k} AS k{j}"))
        .collect();
    // AVG decorrelates into SUM/COUNT partials recombined exactly like the ordinary distributed
    // aggregation path (no cast: keeps DataFusion's own AVG result type — TPC-H Q17's
    // `0.2 * avg(l_quantity)` compares DECIMAL quantities).
    let (partial_items, combine_m0) = per_key_agg_parts(&spec.func, &spec.arg_sql, 0)?;
    psel.extend(partial_items);
    let partial_sql = sanitize_generated_sql(&format!(
        "SELECT {} {inner_tail}{inner_where} GROUP BY {}",
        psel.join(", "),
        inner_key_sql.join(", ")
    ));

    // Stage 1: combine partials per key, re-hashed by k{j} to co-locate with the outer scan.
    let mut csel: Vec<String> = (0..n_keys).map(|j| format!("k{j}")).collect();
    csel.push(format!("{combine_m0} AS m0"));
    let combine_group = (0..n_keys)
        .map(|j| format!("k{j}"))
        .collect::<Vec<_>>()
        .join(", ");
    let combine_sql = format!(
        "SELECT {} FROM shuffle_input GROUP BY {combine_group}",
        csel.join(", ")
    );

    // Stage 2: outer scan exporting join keys (ok{j}), the compared expression (cmp0), and the
    // columns the output projection reads (oc{i}).
    let outer_sql = up
        .plan_to_sql(body)
        .map_err(|e| Error::Unsupported(format!("auto-distribute: unparse outer body: {e}")))?
        .to_string();
    let outer_tail = sanitize_generated_sql(&extract_from_tail(&outer_sql)?);
    let outer_preds: Vec<&Expr> = conjuncts
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != sub_idx)
        .map(|(_, c)| *c)
        .collect();
    let outer_where = where_clause(&up, &outer_preds)?;
    let output_exprs: Vec<Expr> = match &outer_agg {
        // With an outer global aggregate the final stage aggregates again, so the scan exports
        // the aggregate *argument* columns rather than projection source columns.
        Some((agg, _)) => agg.aggr_expr.to_vec(),
        None => match projection {
            Some(exprs) => exprs.to_vec(),
            None => (0..body.schema().fields().len())
                .map(|i| {
                    let (qualifier, field) = body.schema().qualified_field(i);
                    Expr::Column(Column::new(qualifier.cloned(), field.name().clone()))
                })
                .collect(),
        },
    };
    // The DISTINCT must sit directly over the output columns (its schema width then equals the
    // final stage's select width). A DISTINCT buried between the projection and the filter would
    // dedup a different row shape — decline rather than change semantics.
    if distinct.is_some_and(|n| n != output_exprs.len()) {
        return Ok(None);
    }
    let mut exports: Vec<(String, String)> = Vec::new();
    let mut col_alias: HashMap<String, String> = HashMap::new();
    let export_cols = |e: &Expr,
                       alias: &str,
                       exports: &mut Vec<(String, String)>,
                       col_alias: &mut HashMap<String, String>|
     -> Result<()> {
        exports.push((expr_sql(&up, e)?, alias.to_string()));
        let mut cols = Vec::new();
        expr_columns(e, &mut cols);
        for c in cols {
            col_alias
                .entry(c.flat_name())
                .or_insert_with(|| alias.to_string());
        }
        Ok(())
    };
    for (j, (outer_key, _)) in corr_pairs.iter().enumerate() {
        export_cols(outer_key, &format!("ok{j}"), &mut exports, &mut col_alias)?;
    }
    export_cols(compare_expr, "cmp0", &mut exports, &mut col_alias)?;
    let mut oc_next = 0usize;
    for e in &output_exprs {
        let mut cols = Vec::new();
        expr_columns(strip_alias(e), &mut cols);
        for c in cols {
            if col_alias.contains_key(&c.flat_name()) {
                continue;
            }
            let alias = format!("oc{oc_next}");
            oc_next += 1;
            export_cols(&Expr::Column(c), &alias, &mut exports, &mut col_alias)?;
        }
    }
    let outer_select = exports
        .iter()
        .map(|(sql, alias)| format!("{sql} AS {alias}"))
        .collect::<Vec<_>>()
        .join(", ");
    let scan_sql =
        sanitize_generated_sql(&format!("SELECT {outer_select} {outer_tail}{outer_where}"));

    // Re-apply the subquery's projection over the combined per-key value as the compare's right
    // side (TPC-H Q17's `0.2 * avg(…)` → `0.2 * m.m0`); its only column reference may be the
    // aggregate itself. Without a projection the compare is against the bare combined value.
    let mut m0_remap: HashMap<String, String> = HashMap::new();
    m0_remap.insert(
        sub_agg.aggr_expr[0].schema_name().to_string(),
        "m.m0".to_string(),
    );
    if let Some(f) = sub_agg.schema.fields().first() {
        m0_remap.insert(f.name().clone(), "m.m0".to_string());
    }
    let compare_rhs = match sub_projection {
        Some(exprs) => {
            if expr_contains_subquery(&exprs[0]) {
                return Ok(None);
            }
            let mapped = remap_expr_columns(strip_alias(&exprs[0]), &m0_remap);
            let mut cols = Vec::new();
            expr_columns(&mapped, &mut cols);
            if !cols.iter().all(|c| c.name == "m0") {
                return Ok(None);
            }
            expr_sql(&up, &mapped)?
        }
        None => "m.m0".to_string(),
    };
    let op_sql = match compare_op {
        Operator::Eq => "=",
        Operator::NotEq => "!=",
        Operator::Lt => "<",
        Operator::LtEq => "<=",
        Operator::Gt => ">",
        Operator::GtEq => ">=",
        other => {
            return Err(Error::Unsupported(format!(
                "auto-distribute: unsupported scalar compare operator `{other}`"
            )));
        }
    };

    // Stage 3: hash-join the per-key aggregate against the outer rows on the correlation keys
    // plus the (possibly residual) compare, then either re-apply the output projection directly
    // or — with an outer global aggregate (Q17) — partially aggregate the filtered join rows
    // per partition and let stage 4 recombine.
    let join_conds = (0..n_keys)
        .map(|j| format!("m.k{j} = o.ok{j}"))
        .collect::<Vec<_>>()
        .join(" AND ");
    let on_sql = format!("{join_conds} AND o.cmp0 {op_sql} {compare_rhs}");
    let base_stages = vec![
        StageDef::new(0, partial_sql, vec![], (0..n_keys).collect()),
        StageDef::new(1, combine_sql, vec![0], (0..n_keys).collect()),
        StageDef::new(2, scan_sql, vec![], (0..n_keys).collect()),
    ];
    let mut stages = base_stages;
    match &outer_agg {
        None => {
            let select_list = output_exprs
                .iter()
                .map(|e| {
                    let name = output_name(e);
                    let sql = expr_sql(&up, &remap_expr_columns(strip_alias(e), &col_alias))?;
                    Ok(format!("{sql} AS \"{name}\""))
                })
                .collect::<Result<Vec<_>>>()?
                .join(", ");
            let final_sql = sanitize_generated_sql(&format!(
                "SELECT {select_list} FROM shuffle_input_0 AS m JOIN shuffle_input_1 AS o ON {on_sql}"
            ));
            stages.push(StageDef::new(3, final_sql, vec![1, 2], vec![]));
        }
        Some((agg, aggs)) => {
            // Global aggregate over the joined rows: one partial row per partition (gathered),
            // then a combine that re-applies the output projection over r{i}.
            // `HAVING COUNT(*) > 0` suppresses the synthetic row on partitions that received no
            // partials; an all-NULL gather still yields the single NULL row a global aggregate
            // over empty input produces single-node.
            let mut bsel = Vec::with_capacity(aggs.len());
            let mut combine = Vec::with_capacity(aggs.len());
            for (i, a) in aggs.iter().enumerate() {
                let remapped = remap_expr_columns(&agg.aggr_expr[i], &col_alias);
                let arg = AggSpec::classify(&remapped)?.arg_sql;
                bsel.push(format!("{}({arg}) AS b{i}", a.func));
                let combine_func = if a.func == "count" {
                    "sum"
                } else {
                    a.func.as_str()
                };
                combine.push(format!("{combine_func}(b{i}) AS r{i}"));
            }
            let join_sql = sanitize_generated_sql(&format!(
                "SELECT {} FROM shuffle_input_0 AS m JOIN shuffle_input_1 AS o ON {on_sql}",
                bsel.join(", ")
            ));
            let inner = format!(
                "SELECT {} FROM shuffle_input HAVING COUNT(*) > 0",
                combine.join(", ")
            );
            let wrap = Peeled {
                projection,
                sort: None,
                limit: None,
                having: vec![],
                alias_projections: vec![],
                agg,
            };
            let final_sql = wrap_output(&wrap, &inner, &build_agg_remap(agg))?;
            stages.push(StageDef::new(3, join_sql, vec![1, 2], vec![]));
            stages.push(StageDef::new(4, final_sql, vec![3], vec![]));
        }
    }

    let mut dq = DistributedQuery {
        stages,
        finalize_sql: build_outer_finalize(sort, limit)?,
    };
    if let Some(n_cols) = distinct {
        // Hash-shuffle the join output on the full row so duplicate output rows co-locate, then
        // dedup per partition — exact for DISTINCT. `finalize_sql` is already set (the driver's
        // global ORDER BY/LIMIT runs on the deduped gather), so the dedup leaves it alone.
        append_full_row_dedup(&mut dq, n_cols as u32, None, None)?;
    }
    Ok(Some(dq))
}

/// Rewrite `outer_ref(col)` back to a plain column reference, for re-emitting correlation
/// predicates against the outer row in generated stage SQL.
fn strip_outer_refs(e: &Expr) -> Expr {
    use datafusion::common::tree_node::{Transformed, TreeNode};
    e.clone()
        .transform(|node| {
            Ok(match node {
                Expr::OuterReferenceColumn(_, c) => Transformed::yes(Expr::Column(c)),
                other => Transformed::no(other),
            })
        })
        .map(|t| t.data)
        .unwrap_or_else(|_| e.clone())
}

/// One uncorrelated `<expr> <cmp> (SELECT min/max/sum/count/avg(…) FROM <fact> WHERE …)` WHERE
/// conjunct riding alongside the semi/anti key streams (TPC-H Q22's
/// `c_acctbal > (SELECT avg(c_acctbal) FROM customer WHERE …)`), planned as a KAN-27 one-row
/// broadcast: scalar partial / combine stages compute the single global value and the driver
/// inlines it as a SQL literal into the outer scan's WHERE before dispatch.
struct ScalarConjunct {
    /// Per-worker partial aggregate (one row each), gathered (empty hash key).
    partial_sql: String,
    /// Global combine: one row at most; the driver reads zero rows as a NULL scalar.
    combine_sql: String,
    /// The compare side of the conjunct (subquery-free), validated against the outer scope.
    compare: Expr,
    /// The original conjunct with the scalar subquery swapped for the driver's placeholder.
    token_pred: Expr,
    /// The scalar body scans only replicated tables (KAN-36: Q22 at the auto-broadcast
    /// configuration, where `customer` replicates and only the NOT EXISTS fact shards). The
    /// partial must then run **once** ([`ExchangeMode::Forward`]): per-worker partials of the
    /// identical replicated input would multiply the combined value by the worker count.
    /// (KAN-55: a fully-replicated scalar body now routes to a plain verbatim conjunct via
    /// `subquery_conjunct_all_replicated` before this classifier runs, so from the current
    /// caller this flag is set only for sharded bodies — i.e. always `false`.)
    forward_partial: bool,
}

/// `Ok(None)` when `c` is not a `<expr> <cmp> <scalar subquery>` conjunct at all; `Err` when it
/// is one but outside the plannable shape (the caller then declines the query).
fn classify_scalar_conjunct(c: &Expr, replicated: &[&str]) -> Result<Option<ScalarConjunct>> {
    let Expr::BinaryExpr(b) = c else {
        return Ok(None);
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
        return Ok(None);
    }
    let (compare, subquery) = match (b.left.as_ref(), b.right.as_ref()) {
        (Expr::ScalarSubquery(s), other) | (other, Expr::ScalarSubquery(s)) => {
            (other, s.subquery.as_ref())
        }
        _ => return Ok(None),
    };
    let unsupported =
        |why: &str| Error::Unsupported(format!("auto-distribute: scalar compare conjunct: {why}"));
    if expr_contains_subquery(compare) {
        return Err(unsupported("nested subquery in the compare side"));
    }
    // Uncorrelated only — a correlated scalar compare is try_decorrelate_scalar_subquery's shape.
    if plan_contains_outer_reference(subquery) {
        return Err(unsupported("correlated scalar subquery"));
    }
    // The driver inlines the scalar as a SQL literal; off-type results stay on the gather path.
    let fields = subquery.schema().fields();
    if fields.len() != 1 || !scalar_literal_supported(fields[0].data_type()) {
        return Err(unsupported(
            "scalar result type cannot render as a SQL literal",
        ));
    }

    // The subquery must be a bare global aggregate under at most one single-expression
    // projection: `Aggregate: groupBy=[[]]` with exactly one non-DISTINCT
    // min/max/sum/count/avg.
    let mut projection: Option<&[Expr]> = None;
    let mut sp = subquery;
    while let LogicalPlan::Projection(pj) = sp {
        if projection.is_some() || pj.expr.len() != 1 {
            return Err(unsupported("projection over the scalar aggregate"));
        }
        projection = Some(pj.expr.as_slice());
        sp = pj.input.as_ref();
    }
    let LogicalPlan::Aggregate(sub_agg) = sp else {
        return Err(unsupported("not a bare global aggregate"));
    };
    if !sub_agg.group_expr.is_empty() || sub_agg.aggr_expr.len() != 1 {
        return Err(unsupported("GROUP BY / multi-aggregate scalar"));
    }
    let spec = AggSpec::classify(&sub_agg.aggr_expr[0])?;
    if spec.distinct || !matches!(spec.func.as_str(), "min" | "max" | "sum" | "count" | "avg") {
        return Err(unsupported("aggregate function not recombinable"));
    }

    // The subquery's WHERE conjuncts must all be inner-only predicates over its own FROM body.
    let mut inner_preds: Vec<&Expr> = Vec::new();
    let mut inner_body: &LogicalPlan = sub_agg.input.as_ref();
    while let LogicalPlan::Filter(f) = inner_body {
        flatten_conjuncts(&f.predicate, &mut inner_preds);
        inner_body = f.input.as_ref();
    }
    if plan_has_filter_or_subquery_expr(inner_body) {
        return Err(unsupported("subquery inside the scalar body"));
    }
    let scope = PlanScope::of(inner_body);
    for conjunct in &inner_preds {
        let mut cols = Vec::new();
        expr_columns_tagged(conjunct, &mut cols);
        if !cols
            .iter()
            .all(|(c, is_outer)| !is_outer && scope.contains(c))
        {
            return Err(unsupported("correlated predicate in the scalar body"));
        }
    }
    let mut arg_cols = Vec::new();
    expr_columns(&sub_agg.aggr_expr[0], &mut arg_cols);
    if !arg_cols.iter().all(|c| scope.contains(c)) {
        return Err(unsupported("aggregate argument outside the scalar body"));
    }

    // Table safety: at most one sharded table in the scalar body, scanned exactly once; every
    // other table replicated. It may be the outer query's own fact (Q22): the scalar gets its
    // own stages, so the outer body's single-scan accounting is untouched. A fully-replicated
    // body (KAN-36) is planned with a run-once partial instead of per-worker partials.
    let inner_tables = base_tables(inner_body);
    let mut inner_sharded: Vec<&str> = inner_tables
        .iter()
        .map(String::as_str)
        .filter(|t| !replicated.contains(t))
        .collect();
    inner_sharded.sort_unstable();
    inner_sharded.dedup();
    let forward_partial = match inner_sharded.as_slice() {
        [] => true,
        [fact] => {
            if count_table_scans(inner_body, fact) != 1 {
                return Err(unsupported("scalar body scans its fact multiple times"));
            }
            false
        }
        _ => {
            return Err(unsupported(
                "scalar body must scan at most one sharded table",
            ))
        }
    };

    let up = Unparser::default();
    // A projection over the scalar aggregate is re-applied in the combine with the combined
    // value as `m0`; its only column reference must be the aggregate.
    let mut m0_remap: HashMap<String, String> = HashMap::new();
    m0_remap.insert(
        sub_agg.aggr_expr[0].schema_name().to_string(),
        "m0".to_string(),
    );
    if let Some(f) = sub_agg.schema.fields().first() {
        m0_remap.insert(f.name().clone(), "m0".to_string());
    }
    let proj_sql = match projection {
        Some(exprs) => {
            if expr_contains_subquery(&exprs[0]) {
                return Err(unsupported("subquery in the scalar projection"));
            }
            let mapped = remap_expr_columns(strip_alias(&exprs[0]), &m0_remap);
            let mut cols = Vec::new();
            expr_columns(&mapped, &mut cols);
            if !cols.iter().all(|c| c.relation.is_none() && c.name == "m0") {
                return Err(unsupported("projection references more than the aggregate"));
            }
            expr_sql(&up, &mapped)?
        }
        None => "m0".to_string(),
    };

    let inner_sql = up
        .plan_to_sql(inner_body)
        .map_err(|e| {
            Error::Unsupported(format!(
                "auto-distribute: unparse scalar subquery body: {e}"
            ))
        })?
        .to_string();
    let inner_tail = sanitize_generated_sql(&extract_from_tail(&inner_sql)?);
    let inner_where = where_clause(&up, &inner_preds)?;
    let (items, comb) = per_key_agg_parts(&spec.func, &spec.arg_sql, 0)?;
    let partial_sql = sanitize_generated_sql(&format!(
        "SELECT {} {inner_tail}{inner_where}",
        items.join(", ")
    ));
    // `HAVING COUNT(…) > 0` suppresses the synthetic zero-input row on empty partitions, so the
    // driver sees zero rows exactly when the scalar is NULL (same convention as
    // try_uncorrelated_scalar_threshold). AVG's count partial is 0 (not NULL) over an empty
    // input, so it is never suppressed — its NULL quotient row reads as a NULL scalar instead.
    let guard = if spec.func == "avg" { "a0c" } else { "a0" };
    let combine_sql = format!(
        "SELECT {proj_sql} AS s0 FROM \
         (SELECT {comb} AS m0 FROM shuffle_input HAVING COUNT({guard}) > 0) AS combined"
    );

    // Swap the scalar subquery for the placeholder literal the driver replaces before dispatch.
    let placeholder = Expr::Literal(ScalarValue::Utf8(Some(SCALAR_TOKEN.to_string())), None);
    let (left, right) = if matches!(b.left.as_ref(), Expr::ScalarSubquery(_)) {
        (Box::new(placeholder), b.right.clone())
    } else {
        (b.left.clone(), Box::new(placeholder))
    };
    let token_pred = Expr::BinaryExpr(BinaryExpr {
        left,
        op: b.op,
        right,
    });
    Ok(Some(ScalarConjunct {
        partial_sql,
        combine_sql,
        compare: compare.clone(),
        token_pred,
        forward_partial,
    }))
}

/// KAN-37: fuse a grouped `IN` subquery with the outer aggregation when the subquery's per-key
/// aggregate **is** the outer aggregate over the same sharded fact (TPC-H Q18):
///
/// ```sql
/// SELECT c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, sum(l_quantity)
/// FROM customer, orders, lineitem
/// WHERE o_orderkey IN (SELECT l_orderkey FROM lineitem
///                      GROUP BY l_orderkey HAVING sum(l_quantity) > 300)
///   AND c_custkey = o_custkey AND o_orderkey = l_orderkey
/// GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
/// ORDER BY o_totalprice DESC, o_orderdate LIMIT 100
/// ```
///
/// The generic semi/anti path ([`try_semi_anti_subqueries`]) shuffles the **full 3-way join
/// output** (~60M wide rows at SF10) by `o_orderkey` and hash-groups it, only for the co-located
/// `IN` filter to discard all but ~600 orders — that grind blew the 600s stage timeout at SF10.
/// This shape instead recognizes that the outer `sum(l_quantity)` grouped by a key set containing
/// `o_orderkey` is exactly the subquery's per-key `sum(l_quantity)`, so the fact never joins the
/// dimensions at all:
///
/// 1. **Partial per-key aggregate** over the fact (`k0` + `a{i}` partials), hash-shuffled by `k0`
///    — the same producer the generic path emits.
/// 2. **Combine**: re-aggregate per key, re-apply the subquery's HAVING, and carry the recombined
///    `r{i}` values alongside `k0` (still hashed by `k0`, one row per key).
/// 3. **Terminal join + final aggregate** (gather): the co-located key stream inner-joins the
///    replicated outer body on `<outer key> = s.k0` — exact `IN` semantics, because the combine
///    emits exactly one row per key (a semi join) and NULL keys never match — and re-aggregates
///    `r{i}` per outer group (`sum`/`count` → `sum(r{i})`, `min` → `min(r{i})`, `max` →
///    `max(r{i})`). Every group is wholly inside one partition (the `IN` outer key is a required
///    top-level group column, and the stream is hash-partitioned by it), so the local GROUP BY is
///    exact with no combine stage. Duplicate join keys on a replicated dimension fan the `r{i}`
///    row out exactly the way the original join fans the fact rows out, and `sum(r{i})` tracks
///    that multiplicity; a fact key whose dimension rows are missing drops out of both plans.
///
/// Shape restrictions (anything else returns `Ok(None)` → the generic semi/anti path): a
/// distributable aggregate on top (non-empty plain GROUP BY, no grouping sets, no DISTINCT, no
/// renaming projection); exactly one top-level WHERE conjunct is a non-negated `IN` whose body is
/// `<key> FROM <fact> GROUP BY <key> HAVING <min/max/sum/count>` over the same single sharded
/// fact the outer body scans (exactly once, no other sharded table, uncorrelated, plain scan);
/// the outer aggregate list is **identical** to the subquery's per-key aggregate list (same
/// funcs, same args, same order — `avg` excluded, its quotient does not recombine under join
/// multiplicity); the outer body is a comma-join tree; a regular equality conjunct links the `IN`
/// outer key column to the subquery's fact key column; that outer key column is a top-level
/// GROUP BY column on a replicated table; and no other conjunct or group expression references
/// the fact.
pub(crate) fn try_in_agg_semi_join(
    lp: &LogicalPlan,
    replicated: &[&str],
) -> Result<Option<DistributedQuery>> {
    let Ok(p) = peel(lp) else {
        return Ok(None);
    };
    if p.agg.group_expr.is_empty()
        || p.agg
            .group_expr
            .iter()
            .any(|e| matches!(e, Expr::GroupingSet(_)))
        || !p.alias_projections.is_empty()
        || p.having.iter().any(|h| expr_contains_subquery(h))
    {
        return Ok(None);
    }
    let up = Unparser::default();
    let aggs = p
        .agg
        .aggr_expr
        .iter()
        .map(AggSpec::classify)
        .collect::<Result<Vec<_>>>()?;
    if aggs.is_empty()
        || aggs
            .iter()
            .any(|a| a.distinct || !matches!(a.func.as_str(), "sum" | "count" | "min" | "max"))
    {
        return Ok(None);
    }

    // Outer body: SubqueryAlias layers over WHERE conjuncts over a comma-join tree.
    let mut body = p.agg.input.as_ref();
    while let LogicalPlan::SubqueryAlias(s) = body {
        body = s.input.as_ref();
    }
    let mut conjuncts: Vec<&Expr> = Vec::new();
    while let LogicalPlan::Filter(f) = body {
        flatten_conjuncts(&f.predicate, &mut conjuncts);
        body = f.input.as_ref();
    }
    if conjuncts.is_empty()
        || plan_has_filter_or_subquery_expr(body)
        || plan_contains_aggregate(body)
    {
        return Ok(None);
    }
    // Comma-join tree only: INNER joins with no ON/filter, plain (optionally aliased) scans.
    fn comma_join_leaves<'a>(lp: &'a LogicalPlan, out: &mut Vec<&'a LogicalPlan>) -> bool {
        match lp {
            LogicalPlan::Join(j)
                if j.join_type == JoinType::Inner && j.on.is_empty() && j.filter.is_none() =>
            {
                comma_join_leaves(&j.left, out) && comma_join_leaves(&j.right, out)
            }
            LogicalPlan::TableScan(_) | LogicalPlan::SubqueryAlias(_) => {
                out.push(lp);
                true
            }
            _ => false,
        }
    }
    let mut leaves: Vec<&LogicalPlan> = Vec::new();
    if !comma_join_leaves(body, &mut leaves) {
        return Ok(None);
    }
    // Exactly one sharded leaf (the fact); every other leaf replicated. The FROM fragments keep
    // each leaf's qualifier so conjunct / group column references still resolve.
    let mut fact: Option<String> = None;
    let mut rep_from: Vec<String> = Vec::new();
    let mut relation_names: HashSet<String> = HashSet::new();
    for leaf in &leaves {
        let Ok(scan) = simple_table_scan(leaf) else {
            return Ok(None);
        };
        if scan.filter_sql.is_some() || !relation_names.insert(scan_alias(&scan).to_string()) {
            return Ok(None);
        }
        if replicated.contains(&scan.table) {
            rep_from.push(match scan.alias {
                Some(a) => format!("{} AS {a}", scan.table_sql),
                None => scan.table_sql.clone(),
            });
        } else {
            if fact.is_some() {
                return Ok(None);
            }
            fact = Some(scan.table.to_string());
        }
    }
    let Some(fact) = fact else {
        return Ok(None);
    };

    // Exactly one non-negated IN conjunct; every other conjunct subquery-free.
    let mut in_outer: Option<&Expr> = None;
    let mut in_subquery: Option<&LogicalPlan> = None;
    let mut regular: Vec<&Expr> = Vec::new();
    for c in &conjuncts {
        match c {
            Expr::InSubquery(iq) if !iq.negated => {
                if in_outer.is_some() {
                    return Ok(None);
                }
                in_outer = Some(iq.expr.as_ref());
                in_subquery = Some(iq.subquery.subquery.as_ref());
            }
            other => {
                if expr_contains_subquery(other) {
                    return Ok(None);
                }
                regular.push(*other);
            }
        }
    }
    let (Some(in_outer), Some(in_subquery)) = (in_outer, in_subquery) else {
        return Ok(None);
    };

    // The IN outer expression must be a plain column on a replicated table.
    let outer = strip_outer_refs(in_outer);
    let Expr::Column(outer_col) = &outer else {
        return Ok(None);
    };
    let Some(outer_rel) = outer_col.relation.as_ref().map(|r| r.table().to_string()) else {
        return Ok(None);
    };
    if outer_rel == fact || !replicated.contains(&outer_rel.as_str()) {
        return Ok(None);
    }
    let outer_scope = PlanScope::of(body);
    if !outer_scope.contains(outer_col) {
        return Ok(None);
    }

    // IN subquery: `<key> FROM <fact> [WHERE …] GROUP BY <key> HAVING …` — the grouped-IN
    // producer shape, restricted to a single plain scan of the same fact.
    let mut sp = in_subquery;
    let mut in_key: Option<&Expr> = None;
    loop {
        match sp {
            LogicalPlan::SubqueryAlias(a) => sp = a.input.as_ref(),
            LogicalPlan::Projection(pj) if in_key.is_none() && pj.expr.len() == 1 => {
                in_key = Some(strip_alias(&pj.expr[0]));
                sp = pj.input.as_ref();
            }
            _ => break,
        }
    }
    let Some(key_expr) = in_key else {
        return Ok(None);
    };
    let mut mid_conjuncts: Vec<&Expr> = Vec::new();
    let mut inner_root = sp;
    while let LogicalPlan::Filter(f) = inner_root {
        flatten_conjuncts(&f.predicate, &mut mid_conjuncts);
        inner_root = f.input.as_ref();
    }
    let LogicalPlan::Aggregate(sub_agg) = inner_root else {
        return Ok(None);
    };
    if sub_agg.group_expr.len() != 1
        || strip_alias(&sub_agg.group_expr[0])
            .schema_name()
            .to_string()
            != key_expr.schema_name().to_string()
    {
        return Ok(None);
    }
    let sub_specs = sub_agg
        .aggr_expr
        .iter()
        .map(AggSpec::classify)
        .collect::<Result<Vec<_>>>()?;
    // The outer aggregate list must be exactly the subquery's per-key aggregate list: the
    // producer then computes the outer aggregate's per-key values for free.
    if sub_specs.len() != aggs.len()
        || sub_specs
            .iter()
            .zip(&aggs)
            .any(|(s, o)| s.distinct || s.func != o.func || s.arg_sql != o.arg_sql)
    {
        return Ok(None);
    }
    let mut where_preds: Vec<&Expr> = Vec::new();
    let mut scan_body = sub_agg.input.as_ref();
    while let LogicalPlan::Filter(f) = scan_body {
        flatten_conjuncts(&f.predicate, &mut where_preds);
        scan_body = f.input.as_ref();
    }
    if plan_has_filter_or_subquery_expr(scan_body)
        || plan_contains_outer_reference(scan_body)
        || mid_conjuncts.iter().any(|h| {
            if expr_contains_subquery(h) {
                return true;
            }
            let mut cols = Vec::new();
            expr_columns_tagged(h, &mut cols);
            cols.iter().any(|(_, is_outer)| *is_outer)
        })
    {
        return Ok(None);
    }
    // The subquery scans the fact exactly once and nothing else.
    let sub_tables = base_tables(scan_body);
    if sub_tables.len() != 1 || sub_tables[0] != fact || count_table_scans(scan_body, &fact) != 1 {
        return Ok(None);
    }
    // The producer key must be a plain fact column so the outer equality can link to it.
    let Expr::Column(key_col) = key_expr else {
        return Ok(None);
    };
    if key_col.relation.as_ref().map(|r| r.table()) != Some(fact.as_str()) {
        return Ok(None);
    }

    // A regular equality conjunct must link the IN outer key column to the producer's fact key
    // column (`orders.o_orderkey = lineitem.l_orderkey`); it is consumed as the co-located join
    // condition and leaves the remaining conjuncts.
    let mut key_eq_idx = None;
    for (i, c) in regular.iter().enumerate() {
        let Expr::BinaryExpr(b) = *c else {
            continue;
        };
        if b.op != Operator::Eq {
            continue;
        }
        let (Expr::Column(l), Expr::Column(r)) = (b.left.as_ref(), b.right.as_ref()) else {
            continue;
        };
        let links = |a: &Column, b: &Column| {
            a.flat_name() == outer_col.flat_name()
                && b.relation.as_ref().map(|r| r.table()) == Some(fact.as_str())
                && b.name == key_col.name
        };
        if links(l, r) || links(r, l) {
            key_eq_idx = Some(i);
            break;
        }
    }
    let Some(key_eq_idx) = key_eq_idx else {
        return Ok(None);
    };
    regular.remove(key_eq_idx);

    // The IN outer key column must be a top-level group column (co-location ⇒ exact local
    // GROUP BY), and no group expression or remaining conjunct may reference the fact.
    let key_in_group = p.agg.group_expr.iter().any(
        |g| matches!(strip_alias(g), Expr::Column(c) if c.flat_name() == outer_col.flat_name()),
    );
    if !key_in_group {
        return Ok(None);
    }
    let no_fact_cols = |e: &Expr| {
        let mut cols = Vec::new();
        expr_columns(e, &mut cols);
        cols.iter()
            .all(|c| c.relation.as_ref().is_some_and(|r| r.table() != fact))
    };
    if !p.agg.group_expr.iter().all(no_fact_cols) || !regular.iter().all(|c| no_fact_cols(c)) {
        return Ok(None);
    }

    // Producer stages: partial per-key aggregate, then a combine that re-applies the HAVING and
    // carries the recombined r{i} values (the generic grouped-IN producer projects k0 only).
    let key_sql = expr_sql(&up, key_expr)?;
    let tail = sanitize_generated_sql(&extract_from_tail(
        &up.plan_to_sql(scan_body)
            .map_err(|e| {
                Error::Unsupported(format!("auto-distribute: unparse IN subquery body: {e}"))
            })?
            .to_string(),
    )?);
    let where_sql = where_clause(&up, &where_preds)?;
    let mut psel = vec![format!("{key_sql} AS k0")];
    let mut combine = Vec::new();
    for (i, s) in sub_specs.iter().enumerate() {
        let (items, comb) = per_key_agg_parts(&s.func, &s.arg_sql, i)?;
        psel.extend(items);
        combine.push(format!("{comb} AS r{i}"));
    }
    let partial_sql = sanitize_generated_sql(&format!(
        "SELECT {} {tail}{where_sql} GROUP BY {key_sql}",
        psel.join(", ")
    ));
    let having_remap = build_agg_remap(sub_agg);
    let r_col = |name: &str| {
        matches!(name.as_bytes(), [b'r', rest @ ..]
            if !rest.is_empty() && rest.iter().all(u8::is_ascii_digit))
    };
    let mut having_sql = Vec::new();
    for h in &mid_conjuncts {
        let mapped = remap_expr_columns(h, &having_remap);
        let mut cols = Vec::new();
        expr_columns(&mapped, &mut cols);
        if !cols
            .iter()
            .all(|c| c.relation.is_none() && (c.name == "k0" || r_col(&c.name)))
        {
            return Ok(None);
        }
        having_sql.push(format!("({})", expr_sql(&up, &mapped)?));
    }
    let inner = format!(
        "SELECT k0, {} FROM shuffle_input GROUP BY k0",
        combine.join(", ")
    );
    let r_names = (0..sub_specs.len())
        .map(|i| format!("r{i}"))
        .collect::<Vec<_>>()
        .join(", ");
    let combine_sql = if having_sql.is_empty() {
        format!("SELECT k0, {r_names} FROM ({inner}) AS combined")
    } else {
        format!(
            "SELECT k0, {r_names} FROM ({inner}) AS combined WHERE {}",
            having_sql.join(" AND ")
        )
    };

    // Terminal stage: the co-located key stream joins the replicated outer body and re-aggregates
    // r{i} per group — exact because every group lands wholly on one partition.
    let group_sql = p
        .agg
        .group_expr
        .iter()
        .map(|g| expr_sql(&up, strip_alias(g)))
        .collect::<Result<Vec<_>>>()?;
    let gsel = group_sql
        .iter()
        .enumerate()
        .map(|(j, g)| format!("{g} AS g{j}"))
        .collect::<Vec<_>>();
    let combine_aggs = aggs
        .iter()
        .enumerate()
        .map(|(i, a)| {
            let f = if a.func == "count" {
                "sum"
            } else {
                a.func.as_str()
            };
            format!("{f}(s.r{i}) AS r{i}")
        })
        .collect::<Vec<_>>();
    let mut conds = vec![format!("({} = s.k0)", expr_sql(&up, &outer)?)];
    for r in &regular {
        conds.push(format!("({})", expr_sql(&up, r)?));
    }
    let join_inner = format!(
        "SELECT {}, {} FROM shuffle_input AS s CROSS JOIN {} WHERE {} GROUP BY {}",
        gsel.join(", "),
        combine_aggs.join(", "),
        rep_from.join(" CROSS JOIN "),
        conds.join(" AND "),
        group_sql.join(", ")
    );
    let remap = build_remap(&p);
    let final_sql = sanitize_generated_sql(&wrap_output(&p, &join_inner, &remap)?);

    Ok(Some(DistributedQuery {
        stages: vec![
            StageDef::new(0, partial_sql, vec![], vec![0]),
            StageDef::new(1, combine_sql, vec![0], vec![0]),
            StageDef::new(2, final_sql, vec![1], vec![]),
        ],
        finalize_sql: build_finalize(&p)?,
    }))
}

/// Distribute correlated `EXISTS` / `NOT EXISTS` and uncorrelated `IN` / `NOT IN` subquery
/// predicates over one sharded fact as **co-located semi/anti joins** (TPC-H Q4/Q18/Q21),
/// instead of gathering the whole fact to one partition:
///
/// ```sql
/// SELECT o_orderpriority, count(*) FROM orders
/// WHERE <preds> AND EXISTS (SELECT * FROM lineitem
///                         WHERE l_orderkey = o_orderkey AND l_commitdate < l_receiptdate)
/// GROUP BY o_orderpriority
/// ```
///
/// becomes:
///
/// 1. **Key producers** (one per subquery predicate): the subquery's inner side reduced to its
///    correlation keys (`k{j}`) plus any inner columns that residual (non-equality) correlation
///    predicates read (`ic{n}`, Q21's `l2.l_suppkey <> l1.l_suppkey`), hash-shuffled by `k{j}`.
///    Inner-only predicates (`l_commitdate < l_receiptdate`) stay in the producer's WHERE. A
///    grouped `IN` subquery (Q18's `GROUP BY l_orderkey HAVING sum(l_quantity) > 300`) gets a
///    partial/combine pair whose combine re-applies the HAVING before projecting the key.
/// 2. **Outer scan** (only when the outer body itself scans a sharded table): the original
///    FROM/WHERE minus the subquery predicates, exporting the outer keys (`ok{j}`), residual
///    outer columns (`oe{n}`), and the columns the GROUP BY / aggregates read (`oc{n}`),
///    hash-shuffled by `ok{j}` — the same values as `k{j}` by the correlation equality, so
///    matching rows co-locate. When the outer body is fully replicated this stage is skipped and
///    the semi stage reads the replicated tables directly: an outer row is then emitted by
///    exactly the partition its key hashes to (and only if its key is present there), so
///    nothing is double-counted.
/// 3. **Semi/anti + partial aggregate**: the (NOT) EXISTS / (NOT) IN predicates re-expressed
///    against the co-located key streams, feeding the ordinary partial aggregation, hash-
///    shuffled by the group key. `IN` keeps its `IN` spelling (never `EXISTS`) so NULL keys
///    keep their original three-valued semantics.
/// 4. **Combine**: the ordinary recombine stage, re-applying the output projection.
///
/// Shape restrictions (anything else returns `Ok(None)` → the existing gather / rejection
/// paths): a distributable aggregate on top (non-empty plain GROUP BY, no grouping sets,
/// subquery-free HAVING; DISTINCT aggregates take the exact shuffle-by-group-key path); every
/// subquery predicate is a top-level WHERE conjunct; all of them correlate on the **same** outer
/// key expressions; each subquery scans its own single sharded fact exactly once (KAN-157:
/// distinct conjuncts may touch distinct sharded facts — the Q10/Q35/Q69 all-facts-sharded
/// classification) and every other table anywhere is replicated; a top-level `OR` of non-negated
/// `EXISTS` arms counts as one conjunct whose producers are re-OR'd (parenthesized) in the semi
/// stage, while negated or mixed `OR`s decline; the outer body scans at most one sharded table
/// (exactly
/// once) — or, TPC-H Q16, is a single sharded–sharded inner equijoin planned as two flattened
/// leaf scans plus a co-located join stage that re-exports the same `ok{j}` / `oe{n}` / `oc{n}`
/// contract; `IN` subqueries are uncorrelated with either a plain scan body or a `GROUP BY
/// <key> HAVING <min/max/sum/count/avg>` body. One uncorrelated scalar min/max/sum/count/avg
/// compare conjunct (TPC-H Q22's `c_acctbal > (SELECT avg(…) …)`) rides along as a KAN-27
/// one-row broadcast: scalar partial/combine stages plus driver literal injection into the
/// outer stage. When the semi/anti WHERE sits under a derived-table projection that renames the
/// aggregated columns (Q22's `cntrycode`), at most one such renaming projection is captured and
/// group/aggregate expressions resolve through it. Correlated scalar compares are
/// [`try_decorrelate_scalar_subquery`]'s shape; global scalar thresholds are
/// [`try_uncorrelated_scalar_threshold`]'s.
pub(crate) fn try_semi_anti_subqueries(
    lp: &LogicalPlan,
    replicated: &[&str],
) -> Result<Option<DistributedQuery>> {
    let Ok(p) = peel(lp) else {
        return Ok(None);
    };
    if p.agg
        .group_expr
        .iter()
        .any(|e| matches!(e, Expr::GroupingSet(_)))
        || p.having.iter().any(|h| expr_contains_subquery(h))
    {
        return Ok(None);
    }
    // KAN-55: a global aggregate (empty GROUP BY — TPC-DS Q16/Q94's `count(DISTINCT …), sum(…)`)
    // is admissible alongside the grouped case: with no group key to hash by, the semi/anti
    // output gathers to one partition and the aggregate recomputes exactly there (see the stage
    // assembly at the bottom of this function).
    let global_agg = p.agg.group_expr.is_empty();
    let up = Unparser::default();
    let aggs = p
        .agg
        .aggr_expr
        .iter()
        .map(AggSpec::classify)
        .collect::<Result<Vec<_>>>()?;

    // TPC-H Q22: the semi/anti WHERE may sit under a derived-table projection that renames the
    // columns the outer aggregate reads (`substr(c_phone,1,2) AS cntrycode`). Strip
    // `SubqueryAlias` layers and capture at most one such renaming projection; group/aggregate
    // expressions written against its aliases resolve through it to the underlying columns.
    let mut body_projection: Option<&[Expr]> = None;
    let mut body = p.agg.input.as_ref();
    loop {
        match body {
            LogicalPlan::SubqueryAlias(s) => body = s.input.as_ref(),
            LogicalPlan::Projection(pj) if body_projection.is_none() => {
                body_projection = Some(pj.expr.as_slice());
                body = pj.input.as_ref();
            }
            _ => break,
        }
    }

    // Split the pre-aggregation WHERE conjuncts into subquery predicates (semi/anti) and
    // regular ones (which must be subquery-free).
    let mut conjuncts: Vec<&Expr> = Vec::new();
    while let LogicalPlan::Filter(f) = body {
        flatten_conjuncts(&f.predicate, &mut conjuncts);
        body = f.input.as_ref();
    }
    // The outer body must be plain scan / join leaves — a derived aggregate inside it is a
    // different shape (the gather handles those today).
    if conjuncts.is_empty()
        || plan_has_filter_or_subquery_expr(body)
        || plan_contains_aggregate(body)
    {
        return Ok(None);
    }

    // The renaming projection may only compute over the body's own columns.
    let mut body_aliases: HashMap<String, Expr> = HashMap::new();
    if let Some(proj) = body_projection {
        let scope = PlanScope::of(body);
        for e in proj {
            if expr_contains_subquery(e) {
                return Ok(None);
            }
            let mut cols = Vec::new();
            expr_columns(strip_alias(e), &mut cols);
            if !cols.iter().all(|c| scope.contains(c)) {
                return Ok(None);
            }
            match e {
                Expr::Alias(a) => {
                    body_aliases.insert(a.name.clone(), a.expr.as_ref().clone());
                }
                Expr::Column(c) => {
                    body_aliases.insert(c.flat_name(), e.clone());
                    body_aliases.insert(c.name.clone(), e.clone());
                }
                _ => {}
            }
        }
    }
    // Resolve a group / aggregate / key expression written against the derived table's aliases
    // back to the underlying body columns.
    let resolve_body_aliases = |e: &Expr| -> Expr {
        if body_aliases.is_empty() {
            return e.clone();
        }
        use datafusion::common::tree_node::{Transformed, TreeNode};
        e.clone()
            .transform(|node| {
                if let Expr::Column(c) = &node {
                    if let Some(target) = body_aliases
                        .get(&c.flat_name())
                        .or_else(|| body_aliases.get(&c.name))
                    {
                        return Ok(Transformed::yes(target.clone()));
                    }
                }
                Ok(Transformed::no(node))
            })
            .map(|t| t.data)
            .unwrap_or_else(|_| e.clone())
    };

    /// One semi/anti predicate: the subquery plan, whether a match keeps (semi) or drops
    /// (anti) the outer row, and for `IN` the outer expression compared against the key stream.
    enum SubPred<'a> {
        Exists {
            anti: bool,
            subquery: &'a LogicalPlan,
        },
        In {
            anti: bool,
            outer: &'a Expr,
            subquery: &'a LogicalPlan,
        },
        /// A top-level `OR` of non-negated `EXISTS` arms (KAN-157, TPC-DS Q10/Q35's
        /// `EXISTS (web_sales…) OR EXISTS (catalog_sales…)` at the all-facts-sharded
        /// classification): every disjunct builds its own co-located key stream and the semi
        /// condition re-joins the per-disjunct predicates with `OR`. Negated disjuncts
        /// decline — an anti arm holds on every partition but its key's own, so it cannot
        /// gate a replicated-outer row onto exactly one partition.
        ExistsOr { disjuncts: Vec<&'a LogicalPlan> },
    }

    /// A residual (non-equality) correlation predicate, re-emitted against the co-located
    /// streams: inner columns are exported as `ic{n}` on the producer, outer columns as `oe{n}`
    /// on the outer scan (or read from the replicated outer tables directly).
    struct ResidualPred {
        /// The predicate with `outer_ref(…)` stripped back to plain columns.
        expr: Expr,
        inner_cols: Vec<Column>,
        outer_cols: Vec<Column>,
    }

    /// A key-stream producer for one subquery predicate.
    struct Producer {
        anti: bool,
        is_in: bool,
        /// Outer key expressions (shared across producers — validated identical below).
        outer_keys: Vec<Expr>,
        /// Producer stage(s); the last emits `k{j}` then `ic{n}`, hash-partitioned by `k{j}`.
        stages: Vec<StageDef>,
        /// `ic{n}` alias per inner column `flat_name`, for residual remapping.
        ic_aliases: HashMap<String, String>,
        residuals: Vec<ResidualPred>,
    }

    let mut sub_preds = Vec::new();
    let mut regular: Vec<&Expr> = Vec::new();
    let mut scalar_conj: Option<ScalarConjunct> = None;
    for c in &conjuncts {
        // KAN-55: a subquery predicate whose every table is replicated is partition-independent
        // — each partition holds the same full rows for every table it reads, so it evaluates
        // exactly as it would single-node (IN / NOT EXISTS three-valued logic included). Emit it
        // verbatim as a regular conjunct wherever the outer row is read instead of forcing its
        // tables through the key-stream machinery: TPC-DS Q69's `NOT EXISTS` over replicated
        // `web_sales` / `catalog_sales`, Q10/Q35's `EXISTS(web) OR EXISTS(catalog)` arm, Q16/Q94's
        // `NOT EXISTS` over the replicated returns table.
        if expr_contains_subquery(c) && subquery_conjunct_all_replicated(c, replicated) {
            regular.push(*c);
            continue;
        }
        match c {
            Expr::Exists(ex) => sub_preds.push(SubPred::Exists {
                anti: ex.negated,
                subquery: ex.subquery.subquery.as_ref(),
            }),
            Expr::InSubquery(iq) => sub_preds.push(SubPred::In {
                anti: iq.negated,
                outer: iq.expr.as_ref(),
                subquery: iq.subquery.subquery.as_ref(),
            }),
            Expr::BinaryExpr(b) if b.op == Operator::Or && expr_contains_subquery(c) => {
                // KAN-157: a top-level `OR` of non-negated `EXISTS` arms (TPC-DS Q10/Q35's
                // `EXISTS (web…) OR EXISTS (catalog…)`) whose legs scan sharded facts. Each
                // disjunct becomes its own key-stream producer below; anything else in an
                // `OR` (a negated arm, a non-EXISTS leaf) declines.
                let mut leaves = Vec::new();
                flatten_disjuncts(c, &mut leaves);
                let mut disjuncts = Vec::with_capacity(leaves.len());
                for leaf in leaves {
                    match leaf {
                        Expr::Exists(ex) if !ex.negated => {
                            disjuncts.push(ex.subquery.subquery.as_ref());
                        }
                        _ => return Ok(None),
                    }
                }
                if disjuncts.len() < 2 {
                    return Ok(None);
                }
                sub_preds.push(SubPred::ExistsOr { disjuncts });
            }
            other => {
                if expr_contains_subquery(other) {
                    // One uncorrelated scalar-aggregate compare (TPC-H Q22) rides along as a
                    // one-row broadcast; anything else with a subquery declines.
                    let Ok(Some(sc)) = classify_scalar_conjunct(other, replicated) else {
                        return Ok(None);
                    };
                    if scalar_conj.is_some() {
                        return Ok(None);
                    }
                    scalar_conj = Some(sc);
                    continue;
                }
                regular.push(*other);
            }
        }
    }
    if sub_preds.is_empty() {
        return Ok(None);
    }
    let outer_scope = PlanScope::of(body);
    if let Some(sc) = &scalar_conj {
        let mut cols = Vec::new();
        expr_columns(&sc.compare, &mut cols);
        if !cols.iter().all(|c| outer_scope.contains(c)) {
            return Ok(None);
        }
    }

    // Table safety for one subquery's inner body: it must scan exactly one sharded fact
    // exactly once; every other table inside the subquery must be replicated. KAN-157 lifted
    // the historical "the same sharded fact across every subquery" rule: distinct predicates
    // may scan distinct sharded facts (TPC-DS Q10/Q35/Q69 at the all-facts-sharded
    // classification) — each gets its own key stream hash-shuffled by the shared correlation
    // key, so the streams co-locate per key regardless of the source fact.
    fn check_fact(inner_body: &LogicalPlan, replicated: &[&str]) -> bool {
        let tables = base_tables(inner_body);
        let mut sharded: Vec<&str> = tables
            .iter()
            .map(String::as_str)
            .filter(|t| !replicated.contains(t))
            .collect();
        sharded.sort_unstable();
        sharded.dedup();
        let [f] = sharded.as_slice() else {
            return false;
        };
        count_table_scans(inner_body, f) == 1
    }

    let mut producers: Vec<Producer> = Vec::new();
    let mut next_id: u32 = 0;
    // A scalar-broadcast conjunct takes the leading stage ids so its combine has completed by
    // the time any token-bearing stage is dispatched (the driver pulls it positionally).
    let mut scalar_stages: Vec<StageDef> = Vec::new();
    if let Some(sc) = &scalar_conj {
        let pid = next_id;
        next_id += 2;
        let mut partial = StageDef::new(pid, sc.partial_sql.clone(), vec![], vec![]);
        if sc.forward_partial {
            // Replicated body: identical on every worker, so compute the partial exactly once
            // (per-worker partials would multiply the combined scalar by the worker count).
            partial.exchange = ExchangeMode::Forward;
        }
        scalar_stages.push(partial);
        scalar_stages.push(StageDef::new(
            pid + 1,
            sc.combine_sql.clone(),
            vec![pid],
            vec![],
        ));
    }
    // Semi-stage condition composition: one entry per top-level subquery conjunct — a single
    // producer's predicate, or the KAN-157 parenthesized `OR` of a disjunct group's
    // predicates (indices into `producers`).
    enum CondGroup {
        Single(usize),
        Or(Vec<usize>),
    }

    // Build the key-stream producer for one subquery predicate (`Ok(None)` = the shape
    // declines). Shared by plain semi/anti conjuncts and by every disjunct of an `OR` group.
    let build_producer = |anti: bool,
                          in_outer: Option<&Expr>,
                          subquery: &LogicalPlan,
                          next_id: &mut u32|
     -> Result<Option<Producer>> {
        let is_in = in_outer.is_some();

        // Strip aliases; EXISTS ignores its SELECT list (strip every projection), while IN's
        // single projection column is the inner key.
        let mut sp = subquery;
        let mut in_key: Option<&Expr> = None;
        loop {
            match sp {
                LogicalPlan::SubqueryAlias(a) => sp = a.input.as_ref(),
                LogicalPlan::Projection(pj) if !is_in => sp = pj.input.as_ref(),
                LogicalPlan::Projection(pj) if in_key.is_none() && pj.expr.len() == 1 => {
                    in_key = Some(strip_alias(&pj.expr[0]));
                    sp = pj.input.as_ref();
                }
                _ => break,
            }
        }
        if is_in && in_key.is_none() {
            return Ok(None);
        }

        // Conjuncts sitting between the projection and the inner root: a plain body's WHERE
        // predicates, or a grouped IN subquery's HAVING.
        let mut mid_conjuncts: Vec<&Expr> = Vec::new();
        let mut inner_root = sp;
        while let LogicalPlan::Filter(f) = inner_root {
            flatten_conjuncts(&f.predicate, &mut mid_conjuncts);
            inner_root = f.input.as_ref();
        }

        if let LogicalPlan::Aggregate(sub_agg) = inner_root {
            // Grouped IN producer (TPC-H Q18): `IN (SELECT l_orderkey FROM lineitem GROUP BY
            // l_orderkey HAVING sum(l_quantity) > 300)`. The key must be the single group
            // column; the HAVING is re-applied over the recombined per-key aggregates.
            let Some(key_expr) = in_key else {
                return Ok(None);
            };
            if sub_agg.group_expr.len() != 1 {
                return Ok(None);
            }
            let key_name = key_expr.schema_name().to_string();
            if strip_alias(&sub_agg.group_expr[0])
                .schema_name()
                .to_string()
                != key_name
            {
                return Ok(None);
            }
            let sub_specs = sub_agg
                .aggr_expr
                .iter()
                .map(AggSpec::classify)
                .collect::<Result<Vec<_>>>()?;
            if sub_specs.iter().any(|s| {
                s.distinct || !matches!(s.func.as_str(), "min" | "max" | "sum" | "count" | "avg")
            }) {
                return Ok(None);
            }
            let mut where_preds: Vec<&Expr> = Vec::new();
            let mut scan_body = sub_agg.input.as_ref();
            while let LogicalPlan::Filter(f) = scan_body {
                flatten_conjuncts(&f.predicate, &mut where_preds);
                scan_body = f.input.as_ref();
            }
            if plan_has_filter_or_subquery_expr(scan_body)
                || plan_contains_outer_reference(scan_body)
                || mid_conjuncts.iter().any(|h| {
                    if expr_contains_subquery(h) {
                        return true;
                    }
                    let mut cols = Vec::new();
                    expr_columns_tagged(h, &mut cols);
                    cols.iter().any(|(_, is_outer)| *is_outer)
                })
            {
                return Ok(None);
            }
            let scope = PlanScope::of(scan_body);
            let mut in_scope_cols = Vec::new();
            for w in &where_preds {
                expr_columns(w, &mut in_scope_cols);
            }
            for a in &sub_agg.aggr_expr {
                expr_columns(a, &mut in_scope_cols);
            }
            if !in_scope_cols.iter().all(|c| scope.contains(c)) {
                return Ok(None);
            }
            if !check_fact(scan_body, replicated) {
                return Ok(None);
            }
            let outer = strip_outer_refs(in_outer.expect("IN predicate carries its outer expr"));
            let mut key_cols = Vec::new();
            expr_columns_tagged(key_expr, &mut key_cols);
            if !key_cols
                .iter()
                .all(|(c, is_outer)| !is_outer && scope.contains(c))
            {
                return Ok(None);
            }
            let mut outer_cols = Vec::new();
            expr_columns(&outer, &mut outer_cols);
            if !outer_cols.iter().all(|c| outer_scope.contains(c)) {
                return Ok(None);
            }

            let key_sql = expr_sql(&up, key_expr)?;
            let tail = sanitize_generated_sql(&extract_from_tail(
                &up.plan_to_sql(scan_body)
                    .map_err(|e| {
                        Error::Unsupported(format!(
                            "auto-distribute: unparse IN subquery body: {e}"
                        ))
                    })?
                    .to_string(),
            )?);
            let where_sql = where_clause(&up, &where_preds)?;
            let mut psel = vec![format!("{key_sql} AS k0")];
            let mut combine = Vec::new();
            for (i, s) in sub_specs.iter().enumerate() {
                let (items, comb) = per_key_agg_parts(&s.func, &s.arg_sql, i)?;
                psel.extend(items);
                combine.push(format!("{comb} AS r{i}"));
            }
            let partial_sql = sanitize_generated_sql(&format!(
                "SELECT {} {tail}{where_sql} GROUP BY {key_sql}",
                psel.join(", ")
            ));
            // Re-apply the HAVING over the recombined per-key aggregates (r{i} / k0 refs only).
            let having_remap = build_agg_remap(sub_agg);
            let r_col = |name: &str| {
                matches!(name.as_bytes(), [b'r', rest @ ..]
                    if !rest.is_empty() && rest.iter().all(u8::is_ascii_digit))
            };
            let mut having_sql = Vec::new();
            for h in &mid_conjuncts {
                let mapped = remap_expr_columns(h, &having_remap);
                let mut cols = Vec::new();
                expr_columns(&mapped, &mut cols);
                if !cols
                    .iter()
                    .all(|c| c.relation.is_none() && (c.name == "k0" || r_col(&c.name)))
                {
                    return Ok(None);
                }
                having_sql.push(format!("({})", expr_sql(&up, &mapped)?));
            }
            let inner = format!(
                "SELECT k0, {} FROM shuffle_input GROUP BY k0",
                combine.join(", ")
            );
            let combine_sql = if having_sql.is_empty() {
                format!("SELECT k0 FROM ({inner}) AS combined")
            } else {
                format!(
                    "SELECT k0 FROM ({inner}) AS combined WHERE {}",
                    having_sql.join(" AND ")
                )
            };
            let pid = *next_id;
            let cid = *next_id + 1;
            *next_id += 2;
            return Ok(Some(Producer {
                anti,
                is_in,
                outer_keys: vec![outer],
                stages: vec![
                    StageDef::new(pid, partial_sql, vec![], vec![0]),
                    StageDef::new(cid, combine_sql, vec![pid], vec![0]),
                ],
                ic_aliases: HashMap::new(),
                residuals: Vec::new(),
            }));
        }

        // Plain inner body (EXISTS, or an uncorrelated IN over a scan): split its WHERE
        // conjuncts into inner-only predicates, equality correlation key pairs, and residual
        // (non-equality) correlation predicates.
        let inner_body = inner_root;
        if plan_has_filter_or_subquery_expr(inner_body) || plan_contains_outer_reference(inner_body)
        {
            return Ok(None);
        }
        let scope = PlanScope::of(inner_body);
        let mut inner_preds: Vec<&Expr> = Vec::new();
        let mut corr_pairs: Vec<(Expr, Expr)> = Vec::new(); // (outer key, inner key)
        let mut ic_cols: Vec<Column> = Vec::new();
        let mut residuals: Vec<ResidualPred> = Vec::new();
        for conjunct in &mid_conjuncts {
            let mut cols = Vec::new();
            expr_columns_tagged(conjunct, &mut cols);
            if cols
                .iter()
                .all(|(c, is_outer)| !is_outer && scope.contains(c))
            {
                inner_preds.push(*conjunct);
                continue;
            }
            // An equality between a plain inner column and an outer column is a co-location key.
            let mut is_key = false;
            if let Expr::BinaryExpr(b) = *conjunct {
                if b.op == Operator::Eq {
                    let side = |e: &Expr| -> Option<(Column, bool)> {
                        match e {
                            Expr::Column(c) => Some((c.clone(), false)),
                            Expr::OuterReferenceColumn(_, c) => Some((c.clone(), true)),
                            _ => None,
                        }
                    };
                    if let (Some((lc, l_outer)), Some((rc, r_outer))) =
                        (side(&b.left), side(&b.right))
                    {
                        let l_inner = !l_outer && scope.contains(&lc);
                        let r_inner = !r_outer && scope.contains(&rc);
                        match (l_inner, r_inner) {
                            (true, false) => {
                                corr_pairs.push((Expr::Column(rc), Expr::Column(lc)));
                                is_key = true;
                            }
                            (false, true) => {
                                corr_pairs.push((Expr::Column(lc), Expr::Column(rc)));
                                is_key = true;
                            }
                            _ => {}
                        }
                    }
                }
            }
            if is_key {
                continue;
            }
            if is_in {
                // IN subquery predicates must be inner-only (uncorrelated IN only).
                return Ok(None);
            }
            // Residual correlation predicate (Q21's `l2.l_suppkey <> l1.l_suppkey`).
            let mut inner_cs = Vec::new();
            let mut outer_cs = Vec::new();
            for (c, is_outer) in cols {
                if is_outer || !scope.contains(&c) {
                    if !outer_scope.contains(&c) {
                        return Ok(None);
                    }
                    outer_cs.push(c);
                } else {
                    inner_cs.push(c);
                }
            }
            if outer_cs.is_empty() {
                return Ok(None);
            }
            for c in &inner_cs {
                if !ic_cols.iter().any(|x| x.flat_name() == c.flat_name()) {
                    ic_cols.push(c.clone());
                }
            }
            residuals.push(ResidualPred {
                expr: strip_outer_refs(conjunct),
                inner_cols: inner_cs,
                outer_cols: outer_cs,
            });
        }
        if !check_fact(inner_body, replicated) {
            return Ok(None);
        }

        let (outer_keys, inner_key_sql): (Vec<Expr>, Vec<String>) = match in_outer {
            Some(outer) => {
                let Some(key_expr) = in_key else {
                    return Ok(None);
                };
                let mut key_cols = Vec::new();
                expr_columns_tagged(key_expr, &mut key_cols);
                if !key_cols
                    .iter()
                    .all(|(c, is_outer)| !is_outer && scope.contains(c))
                {
                    return Ok(None);
                }
                let outer = strip_outer_refs(outer);
                let mut outer_cols = Vec::new();
                expr_columns(&outer, &mut outer_cols);
                if !outer_cols.iter().all(|c| outer_scope.contains(c)) {
                    return Ok(None);
                }
                (vec![outer], vec![expr_sql(&up, key_expr)?])
            }
            None => {
                if corr_pairs.is_empty() {
                    // Uncorrelated EXISTS is a global-existence check, not a per-key semi join.
                    return Ok(None);
                }
                let mut oks = Vec::new();
                let mut iks = Vec::new();
                for (ok, ik) in corr_pairs {
                    let mut cols = Vec::new();
                    expr_columns(&ok, &mut cols);
                    if !cols.iter().all(|c| outer_scope.contains(c)) {
                        return Ok(None);
                    }
                    oks.push(ok);
                    iks.push(expr_sql(&up, &ik)?);
                }
                (oks, iks)
            }
        };

        let ic_aliases: HashMap<String, String> = ic_cols
            .iter()
            .enumerate()
            .map(|(n, c)| (c.flat_name(), format!("ic{n}")))
            .collect();
        let mut sels: Vec<String> = inner_key_sql
            .iter()
            .enumerate()
            .map(|(j, k)| format!("{k} AS k{j}"))
            .collect();
        for c in &ic_cols {
            sels.push(format!(
                "{} AS {}",
                expr_sql(&up, &Expr::Column(c.clone()))?,
                ic_aliases[&c.flat_name()]
            ));
        }
        let tail = sanitize_generated_sql(&extract_from_tail(
            &up.plan_to_sql(inner_body)
                .map_err(|e| {
                    Error::Unsupported(format!("auto-distribute: unparse subquery body: {e}"))
                })?
                .to_string(),
        )?);
        let where_sql = where_clause(&up, &inner_preds)?;
        // KAN-157: the semi stage only tests membership of the exported `(k{j}, ic{n})` tuples
        // against the co-located stream, so the producer emits each distinct tuple once —
        // one materialized key set per query instead of one row per fact-row through the
        // shuffle (the Q10/Q35/Q69 EXISTS legs at the all-facts-sharded classification).
        let sql = sanitize_generated_sql(&format!(
            "SELECT DISTINCT {} {tail}{where_sql}",
            sels.join(", ")
        ));
        let n_keys = inner_key_sql.len() as u32;
        let id = *next_id;
        *next_id += 1;
        Ok(Some(Producer {
            anti,
            is_in,
            outer_keys,
            stages: vec![StageDef::new(id, sql, vec![], (0..n_keys).collect())],
            ic_aliases,
            residuals,
        }))
    };

    let mut cond_groups: Vec<CondGroup> = Vec::new();
    for pred in &sub_preds {
        match pred {
            SubPred::Exists { anti, subquery } => {
                let Some(pr) = build_producer(*anti, None, subquery, &mut next_id)? else {
                    return Ok(None);
                };
                cond_groups.push(CondGroup::Single(producers.len()));
                producers.push(pr);
            }
            SubPred::In {
                anti,
                outer,
                subquery,
            } => {
                let Some(pr) = build_producer(*anti, Some(outer), subquery, &mut next_id)? else {
                    return Ok(None);
                };
                cond_groups.push(CondGroup::Single(producers.len()));
                producers.push(pr);
            }
            SubPred::ExistsOr { disjuncts } => {
                let mut group = Vec::with_capacity(disjuncts.len());
                for disjunct in disjuncts {
                    let Some(pr) = build_producer(false, None, disjunct, &mut next_id)? else {
                        return Ok(None);
                    };
                    group.push(producers.len());
                    producers.push(pr);
                }
                cond_groups.push(CondGroup::Or(group));
            }
        }
    }

    // Co-location requires every subquery predicate to correlate on the same outer keys.
    let shared_keys: Vec<Expr> = producers[0].outer_keys.clone();
    if producers.iter().any(|pr| pr.outer_keys != shared_keys) {
        return Ok(None);
    }
    let n_keys = shared_keys.len();

    // The outer body scans at most one sharded table (exactly once) — or, TPC-H Q16, is a
    // single sharded–sharded inner equijoin, planned as two flattened leaf scans plus a
    // co-located join stage re-exporting the same `ok{j}` / `oe{n}` / `oc{n}` contract.
    // Everything else replicated. The single sharded table need not be the subquery fact
    // (multi-sharded Q4 shuffles the `orders` outer by `o_orderkey` while `lineitem` feeds the
    // key producer).
    let body_sharded: Vec<String> = base_tables(body)
        .into_iter()
        .filter(|t| !replicated.contains(&t.as_str()))
        .collect();
    if body_sharded.len() > 2 {
        return Ok(None);
    }
    for t in &body_sharded {
        if count_table_scans(body, t) != 1 {
            return Ok(None);
        }
    }
    // Keep the extensions disjoint: the scalar broadcast rides only on the single-scan outer,
    // and the renaming-projection body (Q22) composes with neither the replicated outer nor
    // the sharded–sharded equijoin.
    let join_outer = body_sharded.len() == 2;
    // KAN-36: a fully-replicated outer *does* compose with the renaming projection when the
    // export scan runs exactly once (`ExchangeMode::Forward`) — Q22 at the auto-broadcast
    // configuration, where `customer` replicates and only the NOT EXISTS fact (`orders`)
    // shards. The projection-free replicated outer keeps the inline `scan_id == None` path
    // below.
    let forward_outer = body_sharded.is_empty() && body_projection.is_some();
    if join_outer && (scalar_conj.is_some() || body_projection.is_some()) {
        return Ok(None);
    }

    let outer_sql = up
        .plan_to_sql(body)
        .map_err(|e| Error::Unsupported(format!("auto-distribute: unparse outer body: {e}")))?
        .to_string();
    let outer_tail = sanitize_generated_sql(&extract_from_tail(&outer_sql)?);
    // The scalar-broadcast threshold conjunct (with the driver's placeholder literal) filters
    // alongside the regular predicates wherever the outer rows are read.
    let mut scan_preds: Vec<&Expr> = regular.clone();
    if let Some(sc) = &scalar_conj {
        scan_preds.push(&sc.token_pred);
    }
    let regular_where = where_clause(&up, &scan_preds)?;
    let outer_key_sql: Vec<String> = shared_keys
        .iter()
        .map(|e| expr_sql(&up, e))
        .collect::<Result<_>>()?;

    // `shuffle_input` is spelled without a position when a stage has exactly one upstream.
    let input_name = |pos: usize, total: usize| {
        if total == 1 {
            "shuffle_input".to_string()
        } else {
            format!("shuffle_input_{pos}")
        }
    };
    let export_col = |e: &Expr,
                      alias: &str,
                      exports: &mut Vec<(Expr, String)>,
                      col_alias: &mut HashMap<String, String>| {
        exports.push((e.clone(), alias.to_string()));
        let mut cols = Vec::new();
        expr_columns(e, &mut cols);
        for c in cols {
            col_alias
                .entry(c.flat_name())
                .or_insert_with(|| alias.to_string());
        }
    };

    let mut stages: Vec<StageDef> = scalar_stages;
    let mut producer_out_ids: Vec<u32> = Vec::new();
    for pr in &producers {
        if let Some(last) = pr.stages.last() {
            producer_out_ids.push(last.stage_id);
        }
        stages.extend(pr.stages.iter().cloned());
    }

    // Export list shared by both outer-stage shapes: the semi/anti outer keys (`ok{j}`), the
    // residual outer columns (`oe{n}`), and the GROUP BY / aggregate argument columns (`oc{n}`).
    let mut col_alias: HashMap<String, String> = HashMap::new();
    let mut oe_aliases: HashMap<String, String> = HashMap::new();
    let mut exports: Vec<(Expr, String)> = Vec::new();
    for (j, ok) in shared_keys.iter().enumerate() {
        export_col(
            &resolve_body_aliases(ok),
            &format!("ok{j}"),
            &mut exports,
            &mut col_alias,
        );
    }
    for pr in &producers {
        for r in &pr.residuals {
            for c in &r.outer_cols {
                if col_alias.contains_key(&c.flat_name()) {
                    continue;
                }
                let alias = format!("oe{}", oe_aliases.len());
                oe_aliases.insert(c.flat_name(), alias.clone());
                export_col(
                    &Expr::Column(c.clone()),
                    &alias,
                    &mut exports,
                    &mut col_alias,
                );
            }
        }
    }
    let mut oc_next = 0usize;
    for e in p.agg.group_expr.iter().chain(p.agg.aggr_expr.iter()) {
        let mut cols = Vec::new();
        expr_columns(&resolve_body_aliases(strip_alias(e)), &mut cols);
        for c in cols {
            if col_alias.contains_key(&c.flat_name()) {
                continue;
            }
            let alias = format!("oc{oc_next}");
            oc_next += 1;
            export_col(&Expr::Column(c), &alias, &mut exports, &mut col_alias);
        }
    }

    let scan_id = if join_outer {
        // TPC-H Q16: the outer body is a sharded–sharded inner equijoin (`FROM partsupp, part
        // WHERE p_partkey = ps_partkey AND …`). Two flattened leaf scans hash-shuffled by the
        // join key feed a co-located join stage that re-exports the ok/oe/oc contract,
        // re-shuffled by the shared semi/anti outer keys.
        let LogicalPlan::Join(join) = body else {
            return Ok(None);
        };
        if join.join_type != JoinType::Inner || !join.on.is_empty() || join.filter.is_some() {
            return Ok(None);
        }
        let Ok(mut left_scan) = simple_table_scan(join.left.as_ref()) else {
            return Ok(None);
        };
        let Ok(mut right_scan) = simple_table_scan(join.right.as_ref()) else {
            return Ok(None);
        };
        if !body_sharded.iter().any(|t| t == left_scan.table)
            || !body_sharded.iter().any(|t| t == right_scan.table)
        {
            return Ok(None);
        }

        // Partition the regular conjuncts: single-side predicates fold into that side's leaf
        // scan, cross equalities become the shuffle key, anything else cross is a post-join
        // residual (equivalent for INNER).
        let left_scope = JoinSideScope::of(&join.left);
        let right_scope = JoinSideScope::of(&join.right);
        let mut left_preds: Vec<String> = Vec::new();
        let mut right_preds: Vec<String> = Vec::new();
        let mut key_pairs: Vec<(String, String)> = Vec::new(); // (left col, right col)
        let mut join_residuals: Vec<Expr> = Vec::new();
        for conjunct in &regular {
            match conjunct_side(conjunct, &left_scope, &right_scope) {
                ConjunctSide::Left => left_preds.push(expr_sql(&up, conjunct)?),
                ConjunctSide::Right => right_preds.push(expr_sql(&up, conjunct)?),
                ConjunctSide::Unknown => return Ok(None),
                ConjunctSide::Cross => {
                    let pair = match conjunct {
                        Expr::BinaryExpr(b)
                            if b.op == Operator::Eq
                                && matches!(b.left.as_ref(), Expr::Column(_))
                                && matches!(b.right.as_ref(), Expr::Column(_)) =>
                        {
                            let (Expr::Column(lc), Expr::Column(rc)) =
                                (b.left.as_ref(), b.right.as_ref())
                            else {
                                unreachable!()
                            };
                            if left_scope.contains(lc) && right_scope.contains(rc) {
                                Some((lc.name.clone(), rc.name.clone()))
                            } else if right_scope.contains(lc) && left_scope.contains(rc) {
                                Some((rc.name.clone(), lc.name.clone()))
                            } else {
                                None
                            }
                        }
                        _ => None,
                    };
                    match pair {
                        Some(p) => key_pairs.push(p),
                        None => join_residuals.push((*conjunct).clone()),
                    }
                }
            }
        }
        if key_pairs.is_empty() {
            return Ok(None);
        }
        for (scan, preds) in [(&mut left_scan, left_preds), (&mut right_scan, right_preds)] {
            if !preds.is_empty() {
                let extra = preds.join(" AND ");
                scan.filter_sql = Some(match &scan.filter_sql {
                    Some(prev) => format!("({prev}) AND ({extra})"),
                    None => extra,
                });
            }
        }

        let mut alias_by_relation: HashMap<String, String> = HashMap::new();
        let left_alias = scan_alias(&left_scan).to_string();
        alias_by_relation.insert(left_scan.table.to_string(), left_alias.clone());
        alias_by_relation.insert(left_alias.clone(), left_alias.clone());
        let right_alias = scan_alias(&right_scan).to_string();
        alias_by_relation.insert(right_scan.table.to_string(), right_alias.clone());
        alias_by_relation.insert(right_alias.clone(), right_alias.clone());

        let (left_sql, left_flats) = leaf_stage_sql(&left_scan);
        let mut left_key_idxs = Vec::with_capacity(key_pairs.len());
        for (lk, _) in &key_pairs {
            left_key_idxs.push(flat_key_index(&left_flats, &left_alias, lk)?);
        }
        let left_id = next_id;
        next_id += 1;
        stages.push(StageDef::new(left_id, left_sql, vec![], left_key_idxs));

        let (right_sql, right_flats) = leaf_stage_sql(&right_scan);
        let mut right_key_idxs = Vec::with_capacity(key_pairs.len());
        for (_, rk) in &key_pairs {
            right_key_idxs.push(flat_key_index(&right_flats, &right_alias, rk)?);
        }
        let right_id = next_id;
        next_id += 1;
        stages.push(StageDef::new(right_id, right_sql, vec![], right_key_idxs));

        let on_sql = key_pairs
            .iter()
            .map(|(lk, rk)| {
                format!(
                    "l.{} = r.{}",
                    flat_col(&left_alias, lk),
                    flat_col(&right_alias, rk)
                )
            })
            .collect::<Vec<_>>()
            .join(" AND ");
        let select = exports
            .iter()
            .map(|(e, alias)| {
                let flat = flatten_join_residual(e, &alias_by_relation, &right_alias, &[]);
                Ok(format!("{} AS {alias}", expr_sql(&up, &flat)?))
            })
            .collect::<Result<Vec<_>>>()?
            .join(", ");
        let mut join_sql = format!(
            "SELECT {select} FROM shuffle_input_0 AS l JOIN shuffle_input_1 AS r ON {on_sql}"
        );
        if !join_residuals.is_empty() {
            let preds = join_residuals
                .iter()
                .map(|r| {
                    expr_sql(
                        &up,
                        &flatten_join_residual(r, &alias_by_relation, &right_alias, &[]),
                    )
                })
                .collect::<Result<Vec<_>>>()?
                .join(" AND ");
            join_sql.push_str(&format!(" WHERE {preds}"));
        }
        let join_id = next_id;
        next_id += 1;
        stages.push(StageDef::new(
            join_id,
            sanitize_generated_sql(&join_sql),
            vec![left_id, right_id],
            (0..n_keys as u32).collect(),
        ));
        Some(join_id)
    } else if body_sharded.len() == 1 || forward_outer {
        // Outer scan: export join keys, residual outer columns, and the GROUP BY / aggregate
        // argument columns, hash-shuffled by the shared outer keys.
        let select = exports
            .iter()
            .map(|(e, alias)| Ok(format!("{} AS {alias}", expr_sql(&up, e)?)))
            .collect::<Result<Vec<_>>>()?
            .join(", ");
        let scan_sql =
            sanitize_generated_sql(&format!("SELECT {select} {outer_tail}{regular_where}"));
        let id = next_id;
        next_id += 1;
        let mut scan = StageDef::new(id, scan_sql, vec![], (0..n_keys as u32).collect());
        if forward_outer {
            // Replicated outer: every worker holds the same rows, so export them exactly once —
            // per-worker scans would deliver each outer row to its key's partition once per
            // worker and multiply the aggregates.
            scan.exchange = ExchangeMode::Forward;
        }
        stages.push(scan);
        Some(id)
    } else {
        None
    };

    // A fully-replicated outer (scan_id None) is read on *every* partition; a row is then
    // emitted exactly once only because some semi key stream gates it onto its key's partition
    // (on any other partition the `EXISTS` finds no co-located key and kills the row; anti
    // streams co-located on the same shared key are complete on that one partition). With only
    // anti producers there is no gate: `NOT EXISTS` over the partition-local key share holds on
    // every partition but the key's own, so each kept row would be emitted once per partition.
    // Decline to the gather fallback rather than multiply rows.
    if scan_id.is_none() && producers.iter().all(|pr| pr.anti) {
        return Ok(None);
    }

    // Semi/anti conditions against the co-located key streams: one condition per producer,
    // then composed per `cond_groups` — a plain conjunct passes through, a KAN-157 disjunct
    // group becomes the parenthesized `OR` of its producers' predicates.
    let total_upstreams = producer_out_ids.len() + usize::from(scan_id.is_some());
    let mut producer_conds: Vec<String> = Vec::new();
    for (i, pr) in producers.iter().enumerate() {
        let input = input_name(i + usize::from(scan_id.is_some()), total_upstreams);
        let outer_ref = |j: usize| {
            if scan_id.is_some() {
                format!("o.ok{j}")
            } else {
                outer_key_sql[j].clone()
            }
        };
        if pr.is_in {
            let kw = if pr.anti { "NOT IN" } else { "IN" };
            producer_conds.push(format!("{} {kw} (SELECT k0 FROM {input})", outer_ref(0)));
            continue;
        }
        let mut on: Vec<String> = (0..n_keys)
            .map(|j| format!("k.k{j} = {}", outer_ref(j)))
            .collect();
        for r in &pr.residuals {
            let mut remap: HashMap<String, String> = HashMap::new();
            for c in &r.inner_cols {
                remap.insert(
                    c.flat_name(),
                    format!("k.{}", pr.ic_aliases[&c.flat_name()]),
                );
            }
            if scan_id.is_some() {
                for c in &r.outer_cols {
                    remap.insert(c.flat_name(), format!("o.{}", oe_aliases[&c.flat_name()]));
                }
            }
            on.push(format!(
                "({})",
                expr_sql(&up, &remap_expr_columns(&r.expr, &remap))?
            ));
        }
        let kw = if pr.anti { "NOT EXISTS" } else { "EXISTS" };
        producer_conds.push(format!(
            "{kw} (SELECT 1 FROM {input} AS k WHERE {})",
            on.join(" AND ")
        ));
    }
    let conds: Vec<String> = cond_groups
        .iter()
        .map(|g| match g {
            CondGroup::Single(i) => producer_conds[*i].clone(),
            CondGroup::Or(idxs) => format!(
                "({})",
                idxs.iter()
                    .map(|i| producer_conds[*i].as_str())
                    .collect::<Vec<_>>()
                    .join(" OR ")
            ),
        })
        .collect();

    // The semi/anti filter feeds the ordinary partial/combine aggregation stages.
    let (tail, group_sql, stage_aggs) = if scan_id.is_some() {
        let tail = format!(
            "FROM {} AS o WHERE {}",
            input_name(0, total_upstreams),
            conds.join(" AND ")
        );
        let group_sql = p
            .agg
            .group_expr
            .iter()
            .map(|g| {
                expr_sql(
                    &up,
                    &remap_expr_columns(&resolve_body_aliases(g), &col_alias),
                )
            })
            .collect::<Result<Vec<_>>>()?;
        let stage_aggs = p
            .agg
            .aggr_expr
            .iter()
            .map(|a| AggSpec::classify(&remap_expr_columns(&resolve_body_aliases(a), &col_alias)))
            .collect::<Result<Vec<_>>>()?;
        (tail, group_sql, stage_aggs)
    } else {
        // Fully-replicated outer: each partition semi-joins the replicated outer tables against
        // its share of the key stream — a row is emitted only by the partition its key hashes
        // to, exactly once.
        let mut preds_sql = scan_preds
            .iter()
            .map(|r| expr_sql(&up, r))
            .collect::<Result<Vec<_>>>()?;
        preds_sql.extend(conds);
        let tail = format!("{outer_tail} WHERE {}", preds_sql.join(" AND "));
        let group_sql = p
            .agg
            .group_expr
            .iter()
            .map(|g| expr_sql(&up, g))
            .collect::<Result<Vec<_>>>()?;
        (tail, group_sql, aggs)
    };
    let remap = build_remap(&p);
    // DISTINCT aggregates take the exact path: the semi stage projects the raw grouping +
    // argument rows (hash-shuffled by group key, so every group lands wholly on one worker) and
    // the final stage runs the original aggregate over the co-located rows (TPC-H Q16's
    // `count(DISTINCT ps_suppkey)`).
    //
    // KAN-55: a global aggregate has no group key to hash by, so the semi/anti output gathers to
    // one partition (empty hash key) and the aggregate recomputes exactly over the complete
    // filtered row set there.
    let (partial_sql, final_sql, needs_gate) = if global_agg {
        let (ps, fs) = global_semi_stage_sql(&p, &stage_aggs, &tail, &remap)?;
        (ps, fs, stage_aggs.iter().any(|a| a.distinct))
    } else if stage_aggs.iter().any(|a| a.distinct) {
        let (ps, fs) = distinct_stage_sql(&up, &p, &group_sql, &stage_aggs, &tail, &remap)?;
        (ps, fs, false)
    } else {
        let (ps, fs) = recombine_stage_sql(&p, &group_sql, &stage_aggs, &tail, &remap)?;
        (ps, fs, false)
    };

    let mut upstreams: Vec<u32> = Vec::new();
    if let Some(id) = scan_id {
        upstreams.push(id);
    }
    upstreams.extend(producer_out_ids);
    let semi_id = next_id;
    stages.push(StageDef::new(
        semi_id,
        partial_sql,
        upstreams,
        (0..group_sql.len() as u32).collect(),
    ));
    if needs_gate {
        // DISTINCT global aggregates project raw rows, so an all-empty true result delivers zero
        // rows even to the gather partition — yet single-node still emits the synthetic
        // zero-input global row. The one-row gate lands only on partition 0, so the combine's
        // `COUNT(*) > 0 OR EXISTS (gate)` emits exactly one row cluster-wide either way.
        let gate_id = next_id + 1;
        let combine_id = next_id + 2;
        stages.push(StageDef::new(
            gate_id,
            "SELECT 1 AS __oxidant_semi_gate".to_string(),
            vec![],
            vec![],
        ));
        stages.push(StageDef::new(
            combine_id,
            final_sql,
            vec![semi_id, gate_id],
            vec![],
        ));
    } else {
        let combine_id = next_id + 1;
        stages.push(StageDef::new(combine_id, final_sql, vec![semi_id], vec![]));
    }

    let finalize_sql = build_finalize(&p)?;
    if scalar_conj.is_some() {
        // Self-check (mirrors try_uncorrelated_scalar_threshold): the placeholder must survive
        // as a quoted literal in exactly one stage's SQL, and never leak into the finalize.
        let quoted = format!("'{SCALAR_TOKEN}'");
        if stages.iter().filter(|s| s.sql.contains(&quoted)).count() != 1
            || finalize_sql
                .as_ref()
                .is_some_and(|f| f.contains(SCALAR_TOKEN))
        {
            return Ok(None);
        }
    }

    Ok(Some(DistributedQuery {
        stages,
        finalize_sql,
    }))
}

/// True when every expression subquery inside `e` scans only replicated tables (recursively).
/// Such a predicate is partition-independent: every partition holds the full rows of every table
/// the predicate reads, so its value on a given outer row cannot depend on which partition that
/// row was shuffled to — it may be evaluated verbatim wherever the outer row is read.
fn subquery_conjunct_all_replicated(e: &Expr, replicated: &[&str]) -> bool {
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
    let mut ok = true;
    let _ = e.apply(|expr| {
        let subquery = match expr {
            Expr::InSubquery(iq) => Some(iq.subquery.subquery.as_ref()),
            Expr::ScalarSubquery(sq) => Some(sq.subquery.as_ref()),
            Expr::Exists(ex) => Some(ex.subquery.subquery.as_ref()),
            _ => None,
        };
        if let Some(lp) = subquery {
            let mut tables = base_tables(lp);
            collect_subquery_tables(lp, &mut tables);
            if tables.iter().any(|t| !replicated.contains(&t.as_str())) {
                ok = false;
                return Ok(TreeNodeRecursion::Stop);
            }
        }
        Ok(TreeNodeRecursion::Continue)
    });
    ok
}

/// Global-aggregate finish for the semi/anti path (KAN-55, TPC-DS Q16/Q94): no group key exists
/// to hash by, so the semi/anti-filtered rows (or their recombinable partials) gather to one
/// partition — empty hash key — and the aggregate recomputes there.
///
/// - No DISTINCT: one global partial row per partition (count/sum decompose, avg into
///   sum/count); every partition emits its partial even over zero input rows, so the combine's
///   input is non-empty exactly on the gather partition and `HAVING COUNT(*) > 0` drops the
///   synthetic row everywhere else. An all-empty true result still sums to `(0, NULL, …)`,
///   matching single-node. The combine reads a single upstream (`shuffle_input`).
/// - DISTINCT: the exact raw-row path — every filtered row lands on the gather partition, so
///   re-running the original aggregate there is exact. Raw rows mean an all-empty true result
///   delivers zero rows; the caller adds a partition-0 gate upstream and the combine reads
///   `shuffle_input_0` (rows) + `shuffle_input_1` (gate), emitting exactly one row cluster-wide
///   via `HAVING COUNT(*) > 0 OR EXISTS (gate)`.
fn global_semi_stage_sql(
    p: &Peeled<'_>,
    aggs: &[AggSpec],
    tail: &str,
    remap: &HashMap<String, String>,
) -> Result<(String, String)> {
    if aggs.iter().any(|a| a.distinct) {
        let psel = aggs
            .iter()
            .enumerate()
            .map(|(i, a)| format!("{} AS c{i}", a.arg_sql))
            .collect::<Vec<_>>();
        let partial_sql = sanitize_generated_sql(&format!("SELECT {} {tail}", psel.join(", ")));
        let combine = aggs
            .iter()
            .enumerate()
            .map(|(i, a)| {
                let d = if a.distinct { "DISTINCT " } else { "" };
                format!("{}({d}c{i}) AS r{i}", a.func)
            })
            .collect::<Vec<_>>();
        let inner = format!(
            "SELECT {} FROM shuffle_input_0 \
             HAVING (COUNT(*) > 0) OR EXISTS (SELECT 1 FROM shuffle_input_1)",
            combine.join(", ")
        );
        let final_sql = wrap_output(p, &inner, remap)?;
        return Ok((partial_sql, final_sql));
    }

    let mut psel = Vec::new();
    let mut combine = Vec::new();
    for (i, a) in aggs.iter().enumerate() {
        let (items, comb) = per_key_agg_parts(&a.func, &a.arg_sql, i)?;
        psel.extend(items);
        combine.push(format!("{comb} AS r{i}"));
    }
    let partial_sql = sanitize_generated_sql(&format!("SELECT {} {tail}", psel.join(", ")));
    let inner = format!(
        "SELECT {} FROM shuffle_input HAVING COUNT(*) > 0",
        combine.join(", ")
    );
    let final_sql = wrap_output(p, &inner, remap)?;
    Ok((partial_sql, final_sql))
}

/// Distribute a non-aggregate query whose WHERE carries one nested `IN` semi predicate over a
/// sharded fact — TPC-H Q20:
///
/// ```sql
/// SELECT s_name, s_address FROM supplier, nation
/// WHERE s_suppkey IN (SELECT ps_suppkey FROM partsupp
///                     WHERE ps_partkey IN (SELECT p_partkey FROM part WHERE p_name LIKE 'forest%')
///                       AND ps_availqty > (SELECT 0.5 * sum(l_quantity) FROM lineitem
///                                          WHERE l_partkey = ps_partkey AND l_suppkey = ps_suppkey
///                                            AND <shipdate preds>))
///   AND s_nationkey = n_nationkey AND n_name = 'CANADA'
/// ```
///
/// The `IN` chain becomes a co-located semi cascade (every `IN` keeps its `IN` spelling, so
/// three-valued NULL semantics are unchanged; the TPC-H keys are NOT NULL FK columns anyway):
///
/// 1. **Scalar per-key partial**: the correlated scalar's fact reduced to its correlation keys
///    (`k{j}`) plus the aggregate partial (`a0`), hash-shuffled by `k{j}`.
/// 2. **Scalar combine**: recombine per key and re-apply the scalar's projection (`0.5 * …`) as
///    `thr`, still hashed by `k{j}`.
/// 3. **Nested `IN` keys**: the innermost subquery's filtered key stream (`k0`), hash-shuffled
///    by the nested key.
/// 4. **Fact scan**: the middle subquery's table (`partsupp`) exporting the nested outer key
///    (`nk0`), the correlation outer keys (`k{j}`), and the compare expression (`cmp0`),
///    hash-shuffled by `nk0` to co-locate with the nested key stream.
/// 5. **Nested semi**: `nk0 IN (SELECT k0 …)` against the co-located keys, re-shuffled by the
///    correlation keys `k{j}`.
/// 6. **Threshold semi**: join the co-located per-key threshold rows (`t.k{j} = ps.k{j}`) with
///    the compare as a residual — an inner join, so a key with no scalar group drops out exactly
///    like the original `> NULL` outcome — projecting the distinct top-`IN` key (`k0`),
///    hash-shuffled by it.
/// 7. **Outer scan**: the original FROM/WHERE minus the `IN` conjunct, exporting the outer key
///    (`ok0`) and the output columns (`oc{n}`), hash-shuffled by `ok0` — the same values as the
///    threshold semi's `k0` by the `IN` equality, so matching rows co-locate.
/// 8. **Final semi**: `o.ok0 IN (SELECT k0 …)`, re-applying the output projection; the global
///    `ORDER BY` / `LIMIT` stay in the driver-side finalize.
///
/// Shape restrictions (anything else returns `Ok(None)` → the existing gather / rejection
/// paths): the top is a plain projection (+ sort/limit) over the filtered outer body; exactly
/// one top-level `IN` / `NOT IN` conjunct, every other conjunct subquery-free; the outer body
/// scans at most one sharded table once (others replicated — a fully-replicated outer is
/// exported once via [`ExchangeMode::Forward`], KAN-55); the `IN` subquery's body is a single
/// fact scan — sharded, or replicated with a Forward export whose duplicates the threshold
/// semi's `GROUP BY` would anyway absorb — whose WHERE carries exactly one nested uncorrelated
/// `IN` (likewise a single fact scan behind a plain filter; a replicated key table is
/// Forward-exported, the `IN` being duplicate-insensitive) and exactly one equality-correlated
/// scalar min/max/sum/count compare over a **sharded** fact (correlation keys are plain columns,
/// and the top `IN`'s inner key is one of them); every other inner predicate is inner-only.
pub(crate) fn try_nested_in_semi(
    lp: &LogicalPlan,
    replicated: &[&str],
) -> Result<Option<DistributedQuery>> {
    let (mut node, sort, limit) = peel_scan_tail(lp);
    if let LogicalPlan::SubqueryAlias(s) = node {
        node = s.input.as_ref();
    }
    let LogicalPlan::Projection(out_proj) = node else {
        return Ok(None);
    };
    if out_proj.expr.iter().any(expr_contains_subquery) {
        return Ok(None);
    }
    let mut fnode = out_proj.input.as_ref();
    if let LogicalPlan::SubqueryAlias(s) = fnode {
        fnode = s.input.as_ref();
    }
    let mut conjuncts: Vec<&Expr> = Vec::new();
    let mut body = fnode;
    while let LogicalPlan::Filter(f) = body {
        flatten_conjuncts(&f.predicate, &mut conjuncts);
        body = f.input.as_ref();
    }
    if conjuncts.is_empty()
        || plan_has_filter_or_subquery_expr(body)
        || plan_contains_aggregate(body)
    {
        return Ok(None);
    }

    // Exactly one top-level `IN` / `NOT IN` conjunct; every other conjunct subquery-free.
    let mut top_in: Option<&Expr> = None;
    let mut regular: Vec<&Expr> = Vec::new();
    for c in &conjuncts {
        match c {
            Expr::InSubquery(_) => {
                if top_in.is_some() {
                    return Ok(None);
                }
                top_in = Some(*c);
            }
            other => {
                if expr_contains_subquery(other) {
                    return Ok(None);
                }
                regular.push(*other);
            }
        }
    }
    let Some(Expr::InSubquery(top)) = top_in else {
        return Ok(None);
    };

    // The outer body scans at most one sharded table (exactly once); everything else replicated.
    // A fully-replicated outer is exported exactly once below (`ExchangeMode::Forward`):
    // per-worker scans of the identical rows would deliver each outer row to its key's
    // partition once per worker and multiply the output rows. (KAN-55's per-query sizing
    // replicates e.g. Q20's `supplier` at SF10, where the subquery's `lineitem` is the largest
    // table the query reads.)
    let body_sharded: Vec<String> = base_tables(body)
        .into_iter()
        .filter(|t| !replicated.contains(&t.as_str()))
        .collect();
    if body_sharded.len() > 1 {
        return Ok(None);
    }
    let forward_outer_scan = body_sharded.is_empty();
    if let [outer_fact] = body_sharded.as_slice() {
        if count_table_scans(body, outer_fact) != 1 {
            return Ok(None);
        }
    }
    let outer_scope = PlanScope::of(body);
    let top_outer = top.expr.as_ref();
    {
        let mut cols = Vec::new();
        expr_columns(top_outer, &mut cols);
        if !cols.iter().all(|c| outer_scope.contains(c)) {
            return Ok(None);
        }
    }

    // The `IN` subquery: a single projection column (the top inner key) over a filtered scan of
    // one sharded fact (and nothing else).
    let mut sp = top.subquery.subquery.as_ref();
    let mut top_key: Option<&Expr> = None;
    loop {
        match sp {
            LogicalPlan::SubqueryAlias(a) => sp = a.input.as_ref(),
            LogicalPlan::Projection(pj) if top_key.is_none() && pj.expr.len() == 1 => {
                top_key = Some(strip_alias(&pj.expr[0]));
                sp = pj.input.as_ref();
            }
            _ => break,
        }
    }
    let Some(top_key) = top_key else {
        return Ok(None);
    };
    let mut mid_conjuncts: Vec<&Expr> = Vec::new();
    let mut mid_body = sp;
    while let LogicalPlan::Filter(f) = mid_body {
        flatten_conjuncts(&f.predicate, &mut mid_conjuncts);
        mid_body = f.input.as_ref();
    }
    if plan_has_filter_or_subquery_expr(mid_body) || plan_contains_outer_reference(mid_body) {
        return Ok(None);
    }
    let mid_tables = base_tables(mid_body);
    let mid_sharded: Vec<&str> = mid_tables
        .iter()
        .map(String::as_str)
        .filter(|t| !replicated.contains(t))
        .collect();
    if mid_tables.len() != 1 || mid_sharded.len() > 1 {
        return Ok(None);
    }
    let forward_mid_scan = mid_sharded.is_empty();
    if let [mid_fact] = mid_sharded.as_slice() {
        if count_table_scans(mid_body, mid_fact) != 1 {
            return Ok(None);
        }
    }
    // A replicated middle fact would duplicate its rows once per worker; the threshold semi's
    // `GROUP BY` (stage 5) absorbs duplicates and every `IN` downstream is duplicate-insensitive
    // — but the scan is still exported exactly once (Forward) to avoid the pointless shuffle
    // traffic.
    let mid_scope = PlanScope::of(mid_body);
    {
        let mut cols = Vec::new();
        expr_columns_tagged(top_key, &mut cols);
        if !cols
            .iter()
            .all(|(c, is_outer)| !is_outer && mid_scope.contains(c))
        {
            return Ok(None);
        }
    }

    // Split the middle WHERE: one nested uncorrelated `IN`, one equality-correlated scalar
    // compare, the rest inner-only.
    let mut nested_in: Option<(&Expr, &LogicalPlan, bool)> = None;
    let mut scalar_cmp: Option<(&Expr, &LogicalPlan, Operator, bool)> = None;
    let mut mid_preds: Vec<&Expr> = Vec::new();
    for c in &mid_conjuncts {
        match c {
            Expr::InSubquery(niq) => {
                if nested_in.is_some() {
                    return Ok(None);
                }
                nested_in = Some((
                    niq.expr.as_ref(),
                    niq.subquery.subquery.as_ref(),
                    niq.negated,
                ));
            }
            Expr::BinaryExpr(b)
                if matches!(
                    b.op,
                    Operator::Eq
                        | Operator::NotEq
                        | Operator::Lt
                        | Operator::LtEq
                        | Operator::Gt
                        | Operator::GtEq
                ) && (matches!(b.left.as_ref(), Expr::ScalarSubquery(_))
                    || matches!(b.right.as_ref(), Expr::ScalarSubquery(_))) =>
            {
                if scalar_cmp.is_some() {
                    return Ok(None);
                }
                let (compare, subquery, on_left) = match (b.left.as_ref(), b.right.as_ref()) {
                    (Expr::ScalarSubquery(s), other) => (other, s.subquery.as_ref(), true),
                    (other, Expr::ScalarSubquery(s)) => (other, s.subquery.as_ref(), false),
                    _ => unreachable!(),
                };
                if expr_contains_subquery(compare) {
                    return Ok(None);
                }
                scalar_cmp = Some((compare, subquery, b.op, on_left));
            }
            other => {
                if expr_contains_subquery(other) {
                    return Ok(None);
                }
                mid_preds.push(*other);
            }
        }
    }
    let (
        Some((nested_outer, nested_sub, nested_neg)),
        Some((compare, scalar_sub, cmp_op, cmp_on_left)),
    ) = (nested_in, scalar_cmp)
    else {
        return Ok(None);
    };
    for (e, scope) in [(nested_outer, &mid_scope), (compare, &mid_scope)] {
        let mut cols = Vec::new();
        expr_columns(e, &mut cols);
        if !cols.iter().all(|c| scope.contains(c)) {
            return Ok(None);
        }
    }
    for pred in &mid_preds {
        let mut cols = Vec::new();
        expr_columns_tagged(pred, &mut cols);
        if !cols
            .iter()
            .all(|(c, is_outer)| !is_outer && mid_scope.contains(c))
        {
            return Ok(None);
        }
    }

    // The nested `IN`: uncorrelated, a single plain-column key over a filtered scan of one
    // sharded table (no further subqueries).
    if plan_contains_outer_reference(nested_sub) {
        return Ok(None);
    }
    let mut nsp = nested_sub;
    let mut nested_key: Option<&Expr> = None;
    loop {
        match nsp {
            LogicalPlan::SubqueryAlias(a) => nsp = a.input.as_ref(),
            LogicalPlan::Projection(pj) if nested_key.is_none() && pj.expr.len() == 1 => {
                nested_key = Some(strip_alias(&pj.expr[0]));
                nsp = pj.input.as_ref();
            }
            _ => break,
        }
    }
    let Some(nested_key) = nested_key else {
        return Ok(None);
    };
    if !matches!(nested_key, Expr::Column(_)) {
        return Ok(None);
    }
    let mut nested_preds: Vec<&Expr> = Vec::new();
    let mut nested_body = nsp;
    while let LogicalPlan::Filter(f) = nested_body {
        flatten_conjuncts(&f.predicate, &mut nested_preds);
        nested_body = f.input.as_ref();
    }
    if plan_has_filter_or_subquery_expr(nested_body) {
        return Ok(None);
    }
    let nested_tables = base_tables(nested_body);
    let nested_sharded: Vec<&str> = nested_tables
        .iter()
        .map(String::as_str)
        .filter(|t| !replicated.contains(t))
        .collect();
    if nested_tables.len() != 1 || nested_sharded.len() > 1 {
        return Ok(None);
    }
    let forward_nested_scan = nested_sharded.is_empty();
    if let [nested_fact] = nested_sharded.as_slice() {
        if count_table_scans(nested_body, nested_fact) != 1 {
            return Ok(None);
        }
    }
    // A replicated nested key table would duplicate the key stream once per worker; the nested
    // semi is an `IN`, which is duplicate-insensitive — but the keys are still exported exactly
    // once (Forward) to avoid the pointless shuffle traffic.
    let nested_scope = PlanScope::of(nested_body);
    for pred in nested_preds.iter().chain(std::iter::once(&nested_key)) {
        let mut cols = Vec::new();
        expr_columns_tagged(pred, &mut cols);
        if !cols
            .iter()
            .all(|(c, is_outer)| !is_outer && nested_scope.contains(c))
        {
            return Ok(None);
        }
    }

    // The correlated scalar: a bare global min/max/sum/count under at most one
    // single-expression projection; its WHERE conjuncts split into equality correlation key
    // pairs (plain inner column = plain outer column of the middle fact) and inner-only
    // predicates.
    let mut projection: Option<&[Expr]> = None;
    let mut ssp = scalar_sub;
    while let LogicalPlan::Projection(pj) = ssp {
        if projection.is_some() || pj.expr.len() != 1 {
            return Ok(None);
        }
        projection = Some(pj.expr.as_slice());
        ssp = pj.input.as_ref();
    }
    let LogicalPlan::Aggregate(sub_agg) = ssp else {
        return Ok(None);
    };
    if !sub_agg.group_expr.is_empty() || sub_agg.aggr_expr.len() != 1 {
        return Ok(None);
    }
    let spec = AggSpec::classify(&sub_agg.aggr_expr[0])?;
    if spec.distinct || !matches!(spec.func.as_str(), "min" | "max" | "sum" | "count") {
        return Ok(None);
    }
    let mut scalar_preds: Vec<&Expr> = Vec::new();
    let mut scalar_body: &LogicalPlan = sub_agg.input.as_ref();
    while let LogicalPlan::Filter(f) = scalar_body {
        flatten_conjuncts(&f.predicate, &mut scalar_preds);
        scalar_body = f.input.as_ref();
    }
    if plan_has_filter_or_subquery_expr(scalar_body) {
        return Ok(None);
    }
    let scalar_tables = base_tables(scalar_body);
    let scalar_sharded: Vec<&str> = scalar_tables
        .iter()
        .map(String::as_str)
        .filter(|t| !replicated.contains(t))
        .collect();
    let [scalar_fact] = scalar_sharded.as_slice() else {
        return Ok(None);
    };
    if scalar_tables.len() != 1 || count_table_scans(scalar_body, scalar_fact) != 1 {
        return Ok(None);
    }
    let scalar_scope = PlanScope::of(scalar_body);
    let mut corr_pairs: Vec<(Expr, Expr)> = Vec::new(); // (outer key, inner key)
    let mut scalar_inner_preds: Vec<&Expr> = Vec::new();
    for conjunct in &scalar_preds {
        let mut cols = Vec::new();
        expr_columns_tagged(conjunct, &mut cols);
        if cols
            .iter()
            .all(|(c, is_outer)| !is_outer && scalar_scope.contains(c))
        {
            scalar_inner_preds.push(*conjunct);
            continue;
        }
        // An equality between a plain inner column and an outer (middle-fact) column is a
        // co-location key.
        let mut is_key = false;
        if let Expr::BinaryExpr(b) = *conjunct {
            if b.op == Operator::Eq {
                let side = |e: &Expr| -> Option<(Column, bool)> {
                    match e {
                        Expr::Column(c) => Some((c.clone(), false)),
                        Expr::OuterReferenceColumn(_, c) => Some((c.clone(), true)),
                        _ => None,
                    }
                };
                if let (Some((lc, l_outer)), Some((rc, r_outer))) = (side(&b.left), side(&b.right))
                {
                    let l_inner = !l_outer && scalar_scope.contains(&lc);
                    let r_inner = !r_outer && scalar_scope.contains(&rc);
                    match (l_inner, r_inner) {
                        (true, false) if r_outer && mid_scope.contains(&rc) => {
                            corr_pairs.push((Expr::Column(rc), Expr::Column(lc)));
                            is_key = true;
                        }
                        (false, true) if l_outer && mid_scope.contains(&lc) => {
                            corr_pairs.push((Expr::Column(lc), Expr::Column(rc)));
                            is_key = true;
                        }
                        _ => {}
                    }
                }
            }
        }
        if !is_key {
            return Ok(None);
        }
    }
    if corr_pairs.is_empty() {
        return Ok(None);
    }
    let mut arg_cols = Vec::new();
    expr_columns(&sub_agg.aggr_expr[0], &mut arg_cols);
    if !arg_cols.iter().all(|c| scalar_scope.contains(c)) {
        return Ok(None);
    }

    // The top `IN`'s inner key must be one of the correlation outer keys (it is the value the
    // threshold semi projects).
    let top_key_name = top_key.schema_name().to_string();
    let Some(top_pos) = corr_pairs
        .iter()
        .position(|(ok, _)| ok.schema_name().to_string() == top_key_name)
    else {
        return Ok(None);
    };

    let up = Unparser::default();
    let n_corr = corr_pairs.len();
    let kcols: Vec<String> = (0..n_corr).map(|j| format!("k{j}")).collect();

    // Stage 0: scalar per-key partial over the correlated fact.
    let scalar_sql = up
        .plan_to_sql(scalar_body)
        .map_err(|e| {
            Error::Unsupported(format!(
                "auto-distribute: unparse scalar subquery body: {e}"
            ))
        })?
        .to_string();
    let scalar_tail = sanitize_generated_sql(&extract_from_tail(&scalar_sql)?);
    let scalar_where = where_clause(&up, &scalar_inner_preds)?;
    let mut psel: Vec<String> = corr_pairs
        .iter()
        .enumerate()
        .map(|(j, (_, ik))| Ok(format!("{} AS k{j}", expr_sql(&up, ik)?)))
        .collect::<Result<_>>()?;
    let (items, comb) = per_key_agg_parts(&spec.func, &spec.arg_sql, 0)?;
    psel.extend(items);
    let group_cols: Vec<String> = corr_pairs
        .iter()
        .map(|(_, ik)| expr_sql(&up, ik))
        .collect::<Result<_>>()?;
    let partial_sql = sanitize_generated_sql(&format!(
        "SELECT {} {scalar_tail}{scalar_where} GROUP BY {}",
        psel.join(", "),
        group_cols.join(", ")
    ));

    // Stage 1: scalar combine, re-applying the scalar's projection (Q20's `0.5 * …`) as `thr`.
    let mut m0_remap: HashMap<String, String> = HashMap::new();
    m0_remap.insert(
        sub_agg.aggr_expr[0].schema_name().to_string(),
        "m0".to_string(),
    );
    if let Some(f) = sub_agg.schema.fields().first() {
        m0_remap.insert(f.name().clone(), "m0".to_string());
    }
    let proj_sql = match projection {
        Some(exprs) => {
            if expr_contains_subquery(&exprs[0]) {
                return Ok(None);
            }
            let mapped = remap_expr_columns(strip_alias(&exprs[0]), &m0_remap);
            let mut cols = Vec::new();
            expr_columns(&mapped, &mut cols);
            if !cols.iter().all(|c| c.relation.is_none() && c.name == "m0") {
                return Ok(None);
            }
            expr_sql(&up, &mapped)?
        }
        None => "m0".to_string(),
    };
    let combine_sql = format!(
        "SELECT {}, {proj_sql} AS thr FROM \
         (SELECT {}, {comb} AS m0 FROM shuffle_input GROUP BY {}) AS combined",
        kcols.join(", "),
        kcols.join(", "),
        kcols.join(", ")
    );

    // Stage 2: the nested `IN` key stream.
    let nested_sql = up
        .plan_to_sql(nested_body)
        .map_err(|e| {
            Error::Unsupported(format!(
                "auto-distribute: unparse nested IN subquery body: {e}"
            ))
        })?
        .to_string();
    let nested_tail = sanitize_generated_sql(&extract_from_tail(&nested_sql)?);
    let nested_where = where_clause(&up, &nested_preds)?;
    let nested_keys_sql = sanitize_generated_sql(&format!(
        "SELECT {} AS k0 {nested_tail}{nested_where}",
        expr_sql(&up, nested_key)?
    ));

    // Stage 3: the middle fact scan, hash-shuffled by the nested outer key.
    let mid_sql = up
        .plan_to_sql(mid_body)
        .map_err(|e| Error::Unsupported(format!("auto-distribute: unparse IN subquery body: {e}")))?
        .to_string();
    let mid_tail = sanitize_generated_sql(&extract_from_tail(&mid_sql)?);
    let mid_where = where_clause(&up, &mid_preds)?;
    let mut sels = vec![format!("{} AS nk0", expr_sql(&up, nested_outer)?)];
    for (j, (ok, _)) in corr_pairs.iter().enumerate() {
        sels.push(format!("{} AS k{j}", expr_sql(&up, ok)?));
    }
    sels.push(format!("{} AS cmp0", expr_sql(&up, compare)?));
    let scan_sql =
        sanitize_generated_sql(&format!("SELECT {} {mid_tail}{mid_where}", sels.join(", ")));

    // Stage 4: the nested semi against the co-located key stream, re-shuffled by the
    // correlation keys.
    let nested_kw = if nested_neg { "NOT IN" } else { "IN" };
    let mut pass = kcols.clone();
    pass.push("cmp0".to_string());
    let semi_sql = format!(
        "SELECT {} FROM shuffle_input_0 AS ps WHERE ps.nk0 {nested_kw} \
         (SELECT k0 FROM shuffle_input_1)",
        pass.join(", ")
    );

    // Stage 5: the threshold semi — an inner join against the co-located per-key scalar with
    // the compare as the residual (a key with no scalar group drops out exactly like the
    // original `cmp > NULL` outcome) — projecting the distinct top-`IN` key.
    let op_sql = match cmp_op {
        Operator::Eq => "=",
        Operator::NotEq => "!=",
        Operator::Lt => "<",
        Operator::LtEq => "<=",
        Operator::Gt => ">",
        Operator::GtEq => ">=",
        _ => return Ok(None),
    };
    let cmp_sql = if cmp_on_left {
        format!("t.thr {op_sql} ps.cmp0")
    } else {
        format!("ps.cmp0 {op_sql} t.thr")
    };
    let on_sql = (0..n_corr)
        .map(|j| format!("t.k{j} = ps.k{j}"))
        .collect::<Vec<_>>()
        .join(" AND ");
    let threshold_sql = format!(
        "SELECT ps.k{top_pos} AS k0 FROM shuffle_input_0 AS ps JOIN shuffle_input_1 AS t \
         ON {on_sql} AND ({cmp_sql}) GROUP BY ps.k{top_pos}"
    );

    // Stage 6: the outer scan minus the `IN` conjunct, hash-shuffled by the outer key.
    let outer_sql = up
        .plan_to_sql(body)
        .map_err(|e| Error::Unsupported(format!("auto-distribute: unparse outer body: {e}")))?
        .to_string();
    let outer_tail = sanitize_generated_sql(&extract_from_tail(&outer_sql)?);
    let outer_where = where_clause(&up, &regular)?;
    let mut col_alias: HashMap<String, String> = HashMap::new();
    let mut osel = vec![format!("{} AS ok0", expr_sql(&up, top_outer)?)];
    {
        let mut cols = Vec::new();
        expr_columns(top_outer, &mut cols);
        for c in cols {
            col_alias
                .entry(c.flat_name())
                .or_insert_with(|| "ok0".to_string());
        }
    }
    let mut oc_next = 0usize;
    for e in &out_proj.expr {
        let mut cols = Vec::new();
        expr_columns(strip_alias(e), &mut cols);
        for c in cols {
            if col_alias.contains_key(&c.flat_name()) {
                continue;
            }
            let alias = format!("oc{oc_next}");
            oc_next += 1;
            osel.push(format!(
                "{} AS {alias}",
                expr_sql(&up, &Expr::Column(c.clone()))?
            ));
            col_alias.insert(c.flat_name(), alias);
        }
    }
    let outer_scan_sql = sanitize_generated_sql(&format!(
        "SELECT {} {outer_tail}{outer_where}",
        osel.join(", ")
    ));

    // Stage 7: the final semi, re-applying the output projection.
    let top_kw = if top.negated { "NOT IN" } else { "IN" };
    let select = out_proj
        .expr
        .iter()
        .map(|e| {
            let name = output_name(e);
            let sql = expr_sql(&up, &remap_expr_columns(strip_alias(e), &col_alias))?;
            Ok(format!("{sql} AS \"{name}\""))
        })
        .collect::<Result<Vec<_>>>()?
        .join(", ");
    let final_sql = format!(
        "SELECT {select} FROM shuffle_input_0 AS o WHERE o.ok0 {top_kw} \
         (SELECT k0 FROM shuffle_input_1)"
    );

    let corr_hash: Vec<u32> = (0..n_corr as u32).collect();
    // Replicated scans (KAN-55 per-query sizing) export exactly once — see the comments at the
    // outer / middle / nested checks. Sharded scans keep the plain per-worker hash exchange.
    let mut nested_keys = StageDef::new(2, nested_keys_sql, vec![], vec![0]);
    if forward_nested_scan {
        nested_keys.exchange = ExchangeMode::Forward;
    }
    let mut mid_scan = StageDef::new(3, scan_sql, vec![], vec![0]);
    if forward_mid_scan {
        mid_scan.exchange = ExchangeMode::Forward;
    }
    let mut outer_scan = StageDef::new(6, outer_scan_sql, vec![], vec![0]);
    if forward_outer_scan {
        outer_scan.exchange = ExchangeMode::Forward;
    }
    Ok(Some(DistributedQuery {
        stages: vec![
            StageDef::new(0, partial_sql, vec![], corr_hash.clone()),
            StageDef::new(1, combine_sql, vec![0], corr_hash.clone()),
            nested_keys,
            mid_scan,
            StageDef::new(4, semi_sql, vec![3, 2], corr_hash),
            StageDef::new(5, threshold_sql, vec![4, 1], vec![0]),
            outer_scan,
            StageDef::new(7, final_sql, vec![6, 5], vec![]),
        ],
        finalize_sql: build_outer_finalize(sort, limit)?,
    }))
}

/// Route an uncorrelated scalar min/max/sum/count over a **derived per-key aggregate** — TPC-H
/// Q15's
///
/// ```sql
/// WITH revenue AS (SELECT l_suppkey AS supplier_no, sum(l_extendedprice * (1 - l_discount))
///                  AS total_revenue FROM lineitem WHERE <shipdate preds> GROUP BY supplier_no)
/// SELECT s_suppkey, …, total_revenue FROM supplier, revenue
/// WHERE s_suppkey = supplier_no AND total_revenue = (SELECT max(total_revenue) FROM revenue)
/// ```
///
/// — through the KAN-27 one-row broadcast instead of the whole-fact gather:
///
/// 1. **Derived partial** (stage 0): per-key aggregate partials over the fact shard,
///    hash-shuffled by the derived key (`k0`).
/// 2. **Derived combine** (stage 1): recombine per key, emitting the derived table under its
///    own column names (`supplier_no`, `total_revenue`), still hashed by `k0` so the outer join
///    co-locates with it.
/// 3. **Scalar partial** (stage 2): the scalar aggregate (`max(total_revenue)`) per partition —
///    one row each — gathered (empty hash key).
/// 4. **Scalar combine** (stage 3): the global value, one row at most, pulled by the driver.
///    `HAVING COUNT(s0) > 0` suppresses the all-NULL row of an empty derived table, which the
///    driver reads as "the scalar is NULL" (same convention as
///    [`try_uncorrelated_scalar_threshold`]).
/// 5. **Outer stage** (stage 4): the original FROM/WHERE with the derived table read from the
///    co-located combine output and the scalar compare against the `'__OXIDANT_SCALAR_STAGE__'`
///    placeholder the driver substitutes before dispatch (literal injection).
///
/// [`try_uncorrelated_scalar_threshold`] itself does not fit: it needs the threshold in a
/// HAVING over an outer aggregate plannable by `aggregation_stages_for`, while Q15's scalar
/// sits in a WHERE over a join against a derived table. Shape restrictions (anything else
/// returns `Ok(None)` → the existing gather / rejection paths): exactly one `<derived col>
/// <cmp> <scalar subquery>` WHERE conjunct (comparison operators only), every other conjunct
/// subquery-free; the scalar is a bare global min/max/sum/count (no GROUP BY, no DISTINCT, at
/// most Column-rename projections) over a SubqueryAlias whose inner plan is **identical** (same
/// unparsed SQL) to the one derived table in the outer body; the derived table projects a
/// single group key plus non-DISTINCT min/max/sum/count aggregates over one sharded fact
/// scanned once; every other outer table is replicated; and the scalar's output type renders as
/// a SQL literal ([`scalar_literal_supported`]).
pub(crate) fn try_derived_scalar_equality(
    lp: &LogicalPlan,
    replicated: &[&str],
) -> Result<Option<DistributedQuery>> {
    // Peel the query top: trailing LIMIT / ORDER BY, the output projection, then the WHERE
    // conjuncts over the FROM body (no aggregate on top — Q15 is a plain projection).
    let mut sort = None;
    let mut limit = None;
    let mut projection: Option<&[Expr]> = None;
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
            LogicalPlan::SubqueryAlias(s) => node = s.input.as_ref(),
            LogicalPlan::Projection(p) => {
                if projection.is_none() {
                    projection = Some(p.expr.as_slice());
                }
                node = p.input.as_ref();
            }
            _ => break,
        }
    }
    let mut conjuncts: Vec<&Expr> = Vec::new();
    let mut body = node;
    while let LogicalPlan::Filter(f) = body {
        flatten_conjuncts(&f.predicate, &mut conjuncts);
        body = f.input.as_ref();
    }
    if conjuncts.is_empty() {
        return Ok(None);
    }

    // Find the single `<derived col> <cmp> <scalar subquery>` conjunct (either side may hold
    // the subquery); every other conjunct must be subquery-free.
    let mut found: Option<(usize, &Column, &LogicalPlan, Operator)> = None;
    for (i, conjunct) in conjuncts.iter().enumerate() {
        let Expr::BinaryExpr(b) = *conjunct else {
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
        let Expr::Column(compare_col) = compare else {
            return Ok(None);
        };
        if found.is_some() {
            return Ok(None);
        }
        found = Some((i, compare_col, subquery, b.op));
    }
    let Some((sub_idx, compare_col, subplan, compare_op)) = found else {
        return Ok(None);
    };
    if conjuncts
        .iter()
        .enumerate()
        .any(|(i, c)| i != sub_idx && expr_contains_subquery(c))
    {
        return Ok(None);
    }

    // The scalar subquery: at most Column-rename projections over a bare global aggregate
    // (groupBy=[], exactly one non-DISTINCT min/max/sum/count) over the derived SubqueryAlias.
    let mut sp = subplan;
    while let LogicalPlan::Projection(p) = sp {
        if !p
            .expr
            .iter()
            .all(|e| matches!(strip_alias(e), Expr::Column(_)))
        {
            return Ok(None);
        }
        sp = p.input.as_ref();
    }
    let LogicalPlan::Aggregate(scalar_agg) = sp else {
        return Ok(None);
    };
    if !scalar_agg.group_expr.is_empty() || scalar_agg.aggr_expr.len() != 1 {
        return Ok(None);
    }
    let spec = match AggSpec::classify(&scalar_agg.aggr_expr[0]) {
        Ok(s) => s,
        Err(_) => return Ok(None),
    };
    if spec.distinct || !matches!(spec.func.as_str(), "min" | "max" | "sum" | "count") {
        return Ok(None);
    }
    let LogicalPlan::SubqueryAlias(scalar_derived) = scalar_agg.input.as_ref() else {
        return Ok(None);
    };
    // The scalar's argument must be a plain output column of that derived table (its bare name
    // is re-emitted in the scalar stages).
    let Expr::AggregateFunction(af) = strip_alias(&scalar_agg.aggr_expr[0]) else {
        return Ok(None);
    };
    let Some(Expr::Column(scalar_arg)) = af.params.args.first() else {
        return Ok(None);
    };
    if scalar_arg.relation.as_ref().map(|r| r.table()) != Some(scalar_derived.alias.table()) {
        return Ok(None);
    }
    // Uncorrelated only, and the driver must be able to render the value as a SQL literal.
    if plan_contains_outer_reference(subplan) {
        return Ok(None);
    }
    let fields = subplan.schema().fields();
    if fields.len() != 1 || !scalar_literal_supported(fields[0].data_type()) {
        return Ok(None);
    }

    // The derived table definition (shared by the scalar and the outer body — the CTE inlined
    // twice): Projection(key + agg aliases) over Aggregate(single group column, non-DISTINCT
    // min/max/sum/count) over filters over one sharded fact scanned once.
    let derived = scalar_derived.input.as_ref();
    let LogicalPlan::Projection(derived_proj) = derived else {
        return Ok(None);
    };
    let LogicalPlan::Aggregate(derived_agg) = derived_proj.input.as_ref() else {
        return Ok(None);
    };
    if derived_agg.group_expr.len() != 1
        || derived_proj.expr.len() != 1 + derived_agg.aggr_expr.len()
    {
        return Ok(None);
    }
    let derived_specs = derived_agg
        .aggr_expr
        .iter()
        .map(AggSpec::classify)
        .collect::<Result<Vec<_>>>()?;
    if derived_specs
        .iter()
        .any(|s| s.distinct || !matches!(s.func.as_str(), "min" | "max" | "sum" | "count"))
    {
        return Ok(None);
    }
    // Derived output names: the key column's alias, then each aggregate's alias, positionally
    // matched against the group column / aggregate exprs by schema name.
    let key_name = strip_alias(&derived_agg.group_expr[0])
        .schema_name()
        .to_string();
    let mut derived_out: Vec<String> = Vec::new();
    let mut expected: Vec<String> = vec![key_name.clone()];
    expected.extend(
        derived_agg
            .aggr_expr
            .iter()
            .map(|a| a.schema_name().to_string()),
    );
    for (e, want) in derived_proj.expr.iter().zip(expected.iter()) {
        if strip_alias(e).schema_name().to_string() != *want {
            return Ok(None);
        }
        derived_out.push(output_name(e));
    }
    let mut where_preds: Vec<&Expr> = Vec::new();
    let mut scan_body = derived_agg.input.as_ref();
    while let LogicalPlan::Filter(f) = scan_body {
        flatten_conjuncts(&f.predicate, &mut where_preds);
        scan_body = f.input.as_ref();
    }
    if plan_has_filter_or_subquery_expr(scan_body) || plan_contains_outer_reference(scan_body) {
        return Ok(None);
    }
    let scope = PlanScope::of(scan_body);
    let mut inner_cols = Vec::new();
    for w in &where_preds {
        expr_columns(w, &mut inner_cols);
    }
    expr_columns(&derived_agg.group_expr[0], &mut inner_cols);
    for a in &derived_agg.aggr_expr {
        expr_columns(a, &mut inner_cols);
    }
    if !inner_cols.iter().all(|c| scope.contains(c)) {
        return Ok(None);
    }
    // The derived table's fact is the one sharded table; it must be scanned exactly once.
    let fact_tables = base_tables(scan_body);
    let mut fact_sharded: Vec<&str> = fact_tables
        .iter()
        .map(String::as_str)
        .filter(|t| !replicated.contains(t))
        .collect();
    fact_sharded.sort_unstable();
    fact_sharded.dedup();
    let [fact] = fact_sharded.as_slice() else {
        return Ok(None);
    };
    if count_table_scans(scan_body, fact) != 1 {
        return Ok(None);
    }

    // The outer body: a cross-join tree whose leaves are replicated table scans plus exactly
    // one SubqueryAlias with the *same* derived definition (the CTE's other inline site). The
    // compare column must reference that alias.
    fn cross_leaves<'a>(lp: &'a LogicalPlan, out: &mut Vec<&'a LogicalPlan>) -> bool {
        match lp {
            LogicalPlan::Join(j)
                if j.join_type == JoinType::Inner && j.on.is_empty() && j.filter.is_none() =>
            {
                cross_leaves(&j.left, out) && cross_leaves(&j.right, out)
            }
            LogicalPlan::TableScan(_) | LogicalPlan::SubqueryAlias(_) => {
                out.push(lp);
                true
            }
            _ => false,
        }
    }
    let up = Unparser::default();
    let mut leaves = Vec::new();
    if !cross_leaves(body, &mut leaves) {
        return Ok(None);
    }
    let derived_sql = up
        .plan_to_sql(derived)
        .map_err(|e| Error::Unsupported(format!("auto-distribute: unparse derived table: {e}")))?
        .to_string();
    let mut outer_alias: Option<String> = None;
    let mut from_factors: Vec<String> = Vec::new();
    for leaf in leaves {
        match leaf {
            LogicalPlan::TableScan(_) => {
                let tables = base_tables(leaf);
                if tables.iter().any(|t| !replicated.contains(&t.as_str())) {
                    return Ok(None);
                }
                let sql = up
                    .plan_to_sql(leaf)
                    .map_err(|e| {
                        Error::Unsupported(format!("auto-distribute: unparse outer leaf: {e}"))
                    })?
                    .to_string();
                let tail = extract_from_tail(&sql)?;
                let Some(factor) = tail.strip_prefix("FROM ").or(tail.strip_prefix("from ")) else {
                    return Ok(None);
                };
                from_factors.push(factor.to_string());
            }
            LogicalPlan::SubqueryAlias(a) => {
                let sql = up
                    .plan_to_sql(a.input.as_ref())
                    .map_err(|e| {
                        Error::Unsupported(format!("auto-distribute: unparse outer derived: {e}"))
                    })?
                    .to_string();
                if sql != derived_sql || outer_alias.is_some() {
                    return Ok(None);
                }
                outer_alias = Some(a.alias.table().to_string());
            }
            _ => return Ok(None),
        }
    }
    let Some(outer_alias) = outer_alias else {
        return Ok(None);
    };
    if compare_col.relation.as_ref().map(|r| r.table()) != Some(outer_alias.as_str()) {
        return Ok(None);
    }

    // Stage 0/1: the derived table, distributed — per-key partials over the fact shard, then a
    // combine re-emitting the derived table under its own output column names, co-located by
    // the derived key.
    let key_sql = expr_sql(&up, &derived_agg.group_expr[0])?;
    let tail = sanitize_generated_sql(&extract_from_tail(
        &up.plan_to_sql(scan_body)
            .map_err(|e| {
                Error::Unsupported(format!("auto-distribute: unparse derived scan body: {e}"))
            })?
            .to_string(),
    )?);
    let where_sql = where_clause(&up, &where_preds)?;
    let mut psel = vec![format!("{key_sql} AS k0")];
    let mut csel = vec![format!("k0 AS \"{}\"", derived_out[0])];
    for (i, s) in derived_specs.iter().enumerate() {
        let (items, comb) = per_key_agg_parts(&s.func, &s.arg_sql, i)?;
        psel.extend(items);
        csel.push(format!("{comb} AS \"{}\"", derived_out[i + 1]));
    }
    let partial_sql = sanitize_generated_sql(&format!(
        "SELECT {} {tail}{where_sql} GROUP BY {key_sql}",
        psel.join(", ")
    ));
    let combine_sql = format!("SELECT {} FROM shuffle_input GROUP BY k0", csel.join(", "));

    // Stage 2/3: the scalar over the derived combine — per-partition partials gathered, then
    // the global combine the driver pulls for literal injection.
    let arg_name = &scalar_arg.name;
    let scalar_partial_sql = sanitize_generated_sql(&format!(
        "SELECT {}({arg_name}) AS s0 FROM shuffle_input",
        spec.func
    ));
    let combine_func = if spec.func == "count" {
        "sum"
    } else {
        spec.func.as_str()
    };
    let scalar_combine_sql =
        format!("SELECT {combine_func}(s0) AS m0 FROM shuffle_input HAVING COUNT(s0) > 0");

    // Stage 4: the original FROM/WHERE with the derived table read from the co-located combine
    // output and the scalar compare against the placeholder token.
    let token = SCALAR_TOKEN.to_string();
    let op_sql = match compare_op {
        Operator::Eq => "=",
        Operator::NotEq => "!=",
        Operator::Lt => "<",
        Operator::LtEq => "<=",
        Operator::Gt => ">",
        Operator::GtEq => ">=",
        other => {
            return Err(Error::Unsupported(format!(
                "auto-distribute: unsupported scalar compare operator `{other}`"
            )));
        }
    };
    // A bare numeric literal re-parses as FLOAT64 on the worker, and `DECIMAL = FLOAT64` never
    // matches (Q15's exact-equality on a DECIMAL(15,2)-sourced total would come back empty).
    // Wrap the token in a typed CAST so the substituted literal keeps the scalar's decimal type;
    // the driver's `'…' → literal` replacement lands inside the CAST intact.
    let token_sql = match fields[0].data_type() {
        datafusion::arrow::datatypes::DataType::Decimal128(p, s) => {
            format!("CAST('{token}' AS DECIMAL({p},{s}))")
        }
        _ => format!("'{token}'"),
    };
    let mut preds_sql = Vec::with_capacity(conjuncts.len());
    for (i, c) in conjuncts.iter().enumerate() {
        if i == sub_idx {
            preds_sql.push(format!("{outer_alias}.{arg_name} {op_sql} {token_sql}"));
        } else {
            preds_sql.push(expr_sql(&up, c)?);
        }
    }
    let mut from = format!("shuffle_input AS {outer_alias}");
    for factor in &from_factors {
        from.push_str(&format!(" CROSS JOIN {factor}"));
    }
    let select_list = match projection {
        Some(exprs) => exprs
            .iter()
            .map(|e| {
                let name = output_name(e);
                let sql = expr_sql(&up, strip_alias(e))?;
                Ok(format!("{sql} AS \"{name}\""))
            })
            .collect::<Result<Vec<_>>>()?
            .join(", "),
        None => "*".to_string(),
    };
    let outer_sql = sanitize_generated_sql(&format!(
        "SELECT {select_list} FROM {from} WHERE {}",
        preds_sql.join(" AND ")
    ));

    let dq = DistributedQuery {
        stages: vec![
            StageDef::new(0, partial_sql, vec![], vec![0]),
            StageDef::new(1, combine_sql, vec![0], vec![0]),
            StageDef::new(2, scalar_partial_sql, vec![1], vec![]),
            StageDef::new(3, scalar_combine_sql, vec![2], vec![]),
            StageDef::new(4, outer_sql, vec![1], vec![]),
        ],
        finalize_sql: build_outer_finalize(sort, limit)?,
    };
    // Self-check (same as KAN-27): the placeholder must survive as a quoted literal in exactly
    // one stage's SQL and nowhere in the finalize, or the driver could not substitute it.
    let quoted = format!("'{token}'");
    if dq.stages.iter().filter(|s| s.sql.contains(&quoted)).count() != 1
        || dq.finalize_sql.as_ref().is_some_and(|f| f.contains(&token))
    {
        return Ok(None);
    }
    Ok(Some(dq))
}

/// Decorrelate an **uncorrelated** scalar min/max/sum/count subquery used as a comparison
/// threshold in a post-aggregate (`HAVING`) predicate — TPC-H Q11:
///
/// ```sql
/// SELECT ps_partkey, sum(ps_supplycost * ps_availqty) AS value
/// FROM partsupp, supplier, nation WHERE … GROUP BY ps_partkey
/// HAVING sum(ps_supplycost * ps_availqty) >
///        (SELECT sum(ps_supplycost * ps_availqty) * 0.0001 FROM partsupp, supplier, nation WHERE …)
/// ```
///
/// The scalar computes ONE global value over the whole sharded fact, so per-shard evaluation is
/// wrong and gathering the whole fact is wasteful. Instead this emits a **one-row broadcast**
/// (Spark's subquery execution + literal injection):
///
/// 1. **Scalar partial** (stage 0): `SELECT <func>(<arg>) AS a0 FROM <inner tail> WHERE …` per
///    worker over its local shard (one row each), gathered (empty hash key).
/// 2. **Scalar combine** (stage 1): `SELECT <proj> FROM (SELECT <combine>(a0) AS m0 FROM
///    shuffle_input HAVING COUNT(a0) > 0)` — the global value, one row at most. `HAVING
///    COUNT(a0) > 0` suppresses the synthetic zero-input row on empty partitions and the
///    all-NULL-partials row of a `sum` over an empty fact, which the driver then reads as
///    "the scalar is NULL".
/// 3. **Outer stages** (stages 2+): the original query planned by the ordinary aggregation
///    machinery with the threshold conjunct rewritten to compare against the placeholder
///    literal `'__OXIDANT_SCALAR_STAGE__'`. The driver
///    ([`crate::driver::substitute_scalar_tokens`]) pulls the combine stage's single row and
///    replaces the token with the computed literal **before dispatch**, so the outer HAVING
///    filter applies the global threshold on every shuffle partition without gathering the fact.
///
/// Shape restrictions (anything else returns `Ok(None)` → the existing gather / rejection
/// paths, unchanged): exactly one threshold conjunct of the form `<expr> <cmp> <scalar
/// subquery>` (comparison operators only), every other HAVING conjunct subquery-free; the
/// subquery is a bare global aggregate (no GROUP BY, exactly one non-DISTINCT min/max/sum/count)
/// with at most one single-expression projection layer (Q11's `sum(…) * 0.0001`, re-applied in
/// the combine stage), no correlation (`OuterReferenceColumn`) anywhere, no nested subqueries,
/// exactly one sharded table across the subquery body *and* the outer aggregate input (scanned
/// once in each), and a scalar output type the driver can render as a SQL literal
/// ([`scalar_literal_supported`]). A correlated subquery (TPC-H Q2) is handled by
/// [`try_decorrelate_scalar_subquery`]; a WHERE-position (pre-aggregation) scalar threshold is
/// deliberately out of scope.
pub(crate) fn try_uncorrelated_scalar_threshold(
    lp: &LogicalPlan,
    replicated: &[&str],
) -> Result<Option<DistributedQuery>> {
    // The threshold lives in a HAVING: `peel` collects every Filter above the Aggregate.
    let Ok(peeled) = peel(lp) else {
        return Ok(None);
    };
    if peeled.having.is_empty() {
        return Ok(None);
    }
    let mut conjuncts: Vec<&Expr> = Vec::new();
    for h in &peeled.having {
        flatten_conjuncts(h, &mut conjuncts);
    }

    // Find the single `<expr> <cmp> <scalar subquery>` conjunct (either side may hold the
    // subquery); every other conjunct must be subquery-free.
    let mut found: Option<(usize, &LogicalPlan)> = None;
    for (i, conjunct) in conjuncts.iter().enumerate() {
        let Expr::BinaryExpr(b) = *conjunct else {
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
            return Ok(None);
        }
        found = Some((i, subquery));
    }
    let Some((sub_idx, subplan)) = found else {
        return Ok(None);
    };
    if conjuncts
        .iter()
        .enumerate()
        .any(|(i, c)| i != sub_idx && expr_contains_subquery(c))
    {
        return Ok(None);
    }
    // Uncorrelated only — a correlated scalar threshold is a different shape entirely.
    if plan_contains_outer_reference(subplan) {
        return Ok(None);
    }
    // The driver inlines the scalar as a SQL literal; keep off-type results on the gather path.
    let fields = subplan.schema().fields();
    if fields.len() != 1 || !scalar_literal_supported(fields[0].data_type()) {
        return Ok(None);
    }

    // The subquery must be a bare global aggregate (at most one single-expression projection
    // layer over it, e.g. Q11's `sum(…) * 0.0001`): `Aggregate: groupBy=[[]]` with exactly one
    // non-DISTINCT min/max/sum/count.
    let mut projection: Option<&[Expr]> = None;
    let mut sp = subplan;
    while let LogicalPlan::Projection(p) = sp {
        if projection.is_some() || p.expr.len() != 1 {
            return Ok(None);
        }
        projection = Some(p.expr.as_slice());
        sp = p.input.as_ref();
    }
    let LogicalPlan::Aggregate(sub_agg) = sp else {
        return Ok(None);
    };
    if !sub_agg.group_expr.is_empty() || sub_agg.aggr_expr.len() != 1 {
        return Ok(None);
    }
    let spec = match AggSpec::classify(&sub_agg.aggr_expr[0]) {
        Ok(s) => s,
        Err(_) => return Ok(None),
    };
    if spec.distinct || !matches!(spec.func.as_str(), "min" | "max" | "sum" | "count") {
        return Ok(None);
    }

    // The subquery's WHERE conjuncts must all be inner-only predicates over its own FROM body.
    let mut inner_preds: Vec<&Expr> = Vec::new();
    let mut inner_body: &LogicalPlan = sub_agg.input.as_ref();
    while let LogicalPlan::Filter(f) = inner_body {
        flatten_conjuncts(&f.predicate, &mut inner_preds);
        inner_body = f.input.as_ref();
    }
    if plan_has_filter_or_subquery_expr(inner_body) {
        return Ok(None);
    }
    let scope = PlanScope::of(inner_body);
    for conjunct in &inner_preds {
        let mut cols = Vec::new();
        expr_columns_tagged(conjunct, &mut cols);
        if !cols
            .iter()
            .all(|(c, is_outer)| !is_outer && scope.contains(c))
        {
            return Ok(None);
        }
    }
    let mut arg_cols = Vec::new();
    expr_columns(&sub_agg.aggr_expr[0], &mut arg_cols);
    if !arg_cols.iter().all(|c| scope.contains(c)) {
        return Ok(None);
    }

    // Table safety: exactly one sharded table overall (the fact), scanned exactly once in the
    // subquery body; every other table anywhere in the query replicated. (The outer aggregate's
    // own scan safety — single scan, broadcast-safe shape — is enforced by
    // `aggregation_stages_for` below.)
    let inner_tables = base_tables(inner_body);
    let mut inner_sharded: Vec<&str> = inner_tables
        .iter()
        .map(String::as_str)
        .filter(|t| !replicated.contains(t))
        .collect();
    inner_sharded.sort_unstable();
    inner_sharded.dedup();
    let [fact] = inner_sharded.as_slice() else {
        return Ok(None);
    };
    if count_table_scans(inner_body, fact) != 1 {
        return Ok(None);
    }
    for t in base_tables(&peeled.agg.input) {
        if t != *fact && !replicated.contains(&t.as_str()) {
            return Ok(None);
        }
    }

    // The projection over the scalar aggregate (Q11's `* 0.0001`) is re-applied in the combine
    // stage with the combined value as `m0`; its only column reference must be the aggregate.
    let mut m0_remap: HashMap<String, String> = HashMap::new();
    m0_remap.insert(
        sub_agg.aggr_expr[0].schema_name().to_string(),
        "m0".to_string(),
    );
    if let Some(f) = sub_agg.schema.fields().first() {
        m0_remap.insert(f.name().clone(), "m0".to_string());
    }
    let up = Unparser::default();
    let proj_sql = match projection {
        Some(exprs) => {
            if expr_contains_subquery(&exprs[0]) {
                return Ok(None);
            }
            let mapped = remap_expr_columns(strip_alias(&exprs[0]), &m0_remap);
            let mut cols = Vec::new();
            expr_columns(&mapped, &mut cols);
            if !cols.iter().all(|c| c.relation.is_none() && c.name == "m0") {
                return Ok(None);
            }
            expr_sql(&up, &mapped)?
        }
        None => "m0".to_string(),
    };

    // Rewrite the threshold conjunct, swapping the scalar subquery for the placeholder literal
    // the driver replaces with the computed value before dispatch.
    let token = SCALAR_TOKEN.to_string();
    let mut having: Vec<Expr> = Vec::with_capacity(conjuncts.len());
    for (i, c) in conjuncts.iter().enumerate() {
        if i != sub_idx {
            having.push((*c).clone());
            continue;
        }
        let Expr::BinaryExpr(b) = *c else {
            return Ok(None); // unreachable: `found` only matched BinaryExpr
        };
        let placeholder = Expr::Literal(ScalarValue::Utf8(Some(token.clone())), None);
        let (left, right) = if matches!(b.left.as_ref(), Expr::ScalarSubquery(_)) {
            (Box::new(placeholder), b.right.clone())
        } else {
            (b.left.clone(), Box::new(placeholder))
        };
        having.push(Expr::BinaryExpr(BinaryExpr {
            left,
            op: b.op,
            right,
        }));
    }
    let modified = Peeled {
        projection: peeled.projection,
        sort: peeled.sort,
        limit: peeled.limit,
        having: having.iter().collect(),
        alias_projections: peeled.alias_projections,
        agg: peeled.agg,
    };
    // The outer query minus the threshold conjunct must be plannable by the ordinary machinery
    // (for Q11: broadcast-join partial aggregate + hash-shuffled combine). On any failure leave
    // the query on the existing gather / rejection paths, unchanged.
    let Ok(mut dq) = aggregation_stages_for(&modified, replicated) else {
        return Ok(None);
    };

    // Stage 0: partial scalar aggregate per worker (one row each), gathered (empty hash key).
    let inner_sql = up
        .plan_to_sql(inner_body)
        .map_err(|e| {
            Error::Unsupported(format!(
                "auto-distribute: unparse scalar subquery body: {e}"
            ))
        })?
        .to_string();
    let inner_tail = sanitize_generated_sql(&extract_from_tail(&inner_sql)?);
    let inner_where = where_clause(&up, &inner_preds)?;
    let partial_sql = sanitize_generated_sql(&format!(
        "SELECT {}({}) AS a0 {inner_tail}{inner_where}",
        spec.func, spec.arg_sql
    ));

    // Stage 1: combine the partials into the single global value. `HAVING COUNT(a0) > 0`
    // suppresses the synthetic zero-input row on empty partitions (and the all-NULL `sum` of an
    // empty fact) so the driver sees zero rows exactly when the scalar is NULL.
    let combine_func = if spec.func == "count" {
        "sum"
    } else {
        spec.func.as_str()
    };
    let combine_sql = format!(
        "SELECT {proj_sql} AS s0 FROM \
         (SELECT {combine_func}(a0) AS m0 FROM shuffle_input HAVING COUNT(a0) > 0) AS combined"
    );

    for s in &mut dq.stages {
        s.stage_id += 2;
        for u in &mut s.upstream_stage_ids {
            *u += 2;
        }
    }
    let mut stages = vec![
        StageDef::new(0, partial_sql, vec![], vec![]),
        StageDef::new(1, combine_sql, vec![0], vec![]),
    ];
    stages.append(&mut dq.stages);
    dq.stages = stages;

    // Self-check: the placeholder must survive as a quoted literal in exactly one stage's SQL
    // (the terminal stage's HAVING). If the Unparser ever renders the string literal differently
    // the driver could not substitute it — decline to the gather path instead of shipping a
    // token a worker would try to parse.
    let quoted = format!("'{token}'");
    if dq.stages.iter().filter(|s| s.sql.contains(&quoted)).count() != 1
        || dq.finalize_sql.as_ref().is_some_and(|f| f.contains(&token))
    {
        return Ok(None);
    }
    Ok(Some(dq))
}

/// True when any expression anywhere in the subtree carries an expression subquery
/// (`EXISTS` / `IN` / scalar).
fn plan_exprs_contain_subquery(lp: &LogicalPlan) -> bool {
    lp.expressions().iter().any(expr_contains_subquery)
        || lp.inputs().iter().any(|c| plan_exprs_contain_subquery(c))
}

/// True when any expression in the subtree carries an `OuterReferenceColumn` (a correlated
/// reference into an enclosing query scope).
pub(crate) fn plan_contains_outer_reference(lp: &LogicalPlan) -> bool {
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
    let expr_has_outer = |e: &Expr| {
        let mut found = false;
        let _ = e.apply(|node| {
            if matches!(node, Expr::OuterReferenceColumn(_, _)) {
                found = true;
                return Ok(TreeNodeRecursion::Stop);
            }
            Ok(TreeNodeRecursion::Continue)
        });
        found
    };
    lp.expressions().iter().any(expr_has_outer)
        || lp.inputs().iter().any(|c| plan_contains_outer_reference(c))
}

/// The relation and column names a plan subtree brings into scope, for deciding whether a
/// predicate column is inner (local to a subquery) or an outer (correlated) reference.
struct PlanScope {
    relations: HashSet<String>,
    field_names: HashSet<String>,
}

impl PlanScope {
    fn of(lp: &LogicalPlan) -> Self {
        let mut relations = HashSet::new();
        collect_relation_names(lp, &mut relations);
        let field_names = lp
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        PlanScope {
            relations,
            field_names,
        }
    }

    fn contains(&self, c: &Column) -> bool {
        match &c.relation {
            Some(r) => self.relations.contains(r.table()),
            None => self.field_names.contains(&c.name),
        }
    }
}

fn collect_relation_names(lp: &LogicalPlan, out: &mut HashSet<String>) {
    match lp {
        LogicalPlan::TableScan(s) => {
            out.insert(s.table_name.table().to_string());
        }
        LogicalPlan::SubqueryAlias(a) => {
            out.insert(a.alias.table().to_string());
        }
        _ => {}
    }
    for c in lp.inputs() {
        collect_relation_names(c, out);
    }
}

/// Split `a AND b AND …` into its top-level conjuncts (references, no cloning).
pub(crate) fn flatten_conjuncts<'a>(e: &'a Expr, out: &mut Vec<&'a Expr>) {
    match e {
        Expr::BinaryExpr(b) if b.op == Operator::And => {
            flatten_conjuncts(&b.left, out);
            flatten_conjuncts(&b.right, out);
        }
        other => out.push(other),
    }
}

/// Split `a OR b OR …` into its top-level disjuncts (references, no cloning).
fn flatten_disjuncts<'a>(e: &'a Expr, out: &mut Vec<&'a Expr>) {
    match e {
        Expr::BinaryExpr(b) if b.op == Operator::Or => {
            flatten_disjuncts(&b.left, out);
            flatten_disjuncts(&b.right, out);
        }
        other => out.push(other),
    }
}

/// Mirror a comparison operator for swapping its operands (`a > b` ⇔ `b < a`); equality
/// operators are symmetric. Only called with the equality/ordering operators the scalar
/// compare shapes accept.
fn mirror_compare_op(op: Operator) -> Operator {
    match op {
        Operator::Lt => Operator::Gt,
        Operator::LtEq => Operator::GtEq,
        Operator::Gt => Operator::Lt,
        Operator::GtEq => Operator::LtEq,
        other => other,
    }
}

/// Factor conjuncts shared by every disjunct out of a disjunction:
/// `(c AND A) OR (c AND B)` ≡ `c AND (A OR B)` (valid in SQL three-valued logic — strong
/// Kleene AND/OR distribute, and AND is idempotent). Returns the shared conjuncts and the
/// residual disjunction; `shared` is empty (and the residual is `e` itself) when nothing
/// factors. Used to lift a correlation equality repeated in every OR arm (TPC-DS Q41's
/// `(m = o.m AND A) OR (m = o.m AND B)`) into a top-level conjunct the decorrelation can
/// group by.
fn factor_or_common(e: &Expr) -> (Vec<Expr>, Expr) {
    fn flatten_disjuncts<'a>(e: &'a Expr, out: &mut Vec<&'a Expr>) {
        match e {
            Expr::BinaryExpr(b) if b.op == Operator::Or => {
                flatten_disjuncts(&b.left, out);
                flatten_disjuncts(&b.right, out);
            }
            other => out.push(other),
        }
    }
    fn conjunct_set(e: &Expr) -> Vec<Expr> {
        let mut flat = Vec::new();
        flatten_conjuncts(e, &mut flat);
        flat.into_iter().cloned().collect()
    }
    fn and_of(parts: Vec<Expr>) -> Expr {
        // An arm made wholly of shared conjuncts leaves TRUE (`c OR (c AND B) ≡ c AND TRUE`).
        let mut it = parts.into_iter();
        let Some(mut acc) = it.next() else {
            return Expr::Literal(ScalarValue::Boolean(Some(true)), None);
        };
        for p in it {
            acc = Expr::BinaryExpr(BinaryExpr {
                left: Box::new(acc),
                op: Operator::And,
                right: Box::new(p),
            });
        }
        acc
    }
    let mut arms = Vec::new();
    flatten_disjuncts(e, &mut arms);
    if arms.len() < 2 {
        return (Vec::new(), e.clone());
    }
    let mut sets: Vec<Vec<Expr>> = arms.iter().map(|a| conjunct_set(a)).collect();
    let mut shared: Vec<Expr> = Vec::new();
    for c in &sets[0] {
        if !shared.contains(c) && sets[1..].iter().all(|s| s.contains(c)) {
            shared.push(c.clone());
        }
    }
    if shared.is_empty() {
        return (Vec::new(), e.clone());
    }
    for s in &mut sets {
        s.retain(|c| !shared.contains(c));
    }
    let mut it = sets.into_iter().map(and_of);
    let mut residual = it.next().expect("at least two disjuncts");
    for arm in it {
        residual = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(residual),
            op: Operator::Or,
            right: Box::new(arm),
        });
    }
    (shared, residual)
}

/// Every `Column` referenced anywhere in `e`.
pub(crate) fn expr_columns(e: &Expr, out: &mut Vec<Column>) {
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
    let _ = e.apply(|node| {
        if let Expr::Column(c) = node {
            out.push(c.clone());
        }
        Ok(TreeNodeRecursion::Continue)
    });
}

/// Every column referenced anywhere in `e`, tagged `true` when it arrived as an
/// [`Expr::OuterReferenceColumn`] (a correlated reference into an enclosing query scope).
fn expr_columns_tagged(e: &Expr, out: &mut Vec<(Column, bool)>) {
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
    let _ = e.apply(|node| {
        match node {
            Expr::Column(c) => out.push((c.clone(), false)),
            Expr::OuterReferenceColumn(_, c) => out.push((c.clone(), true)),
            _ => {}
        }
        Ok(TreeNodeRecursion::Continue)
    });
}

/// Mirror of oxidant-connect's strict-mode switch (`OXIDANT_DISTRIBUTED_STRICT`), read here so the
/// whole-fact gather can refuse to emit an unbounded single-partition plan (KAN-29).
fn distributed_strict() -> bool {
    std::env::var("OXIDANT_DISTRIBUTED_STRICT")
        .ok()
        .is_some_and(|v| v == "1" || v.eq_ignore_ascii_case("true"))
}

/// KAN-29 floor: the whole-fact gather centralizes a sharded fact on shuffle partition 0
/// (SF10+ Q4/Q15/Q17/Q18/Q21 wedge one worker under ~27 GB). In strict mode refuse to emit it:
/// the caller (oxidant-connect `try_run_distributed_plan`) already turns planner rejections into
/// the query error, so this fails fast with an actionable message naming the shape instead of
/// running an unbounded single-partition grind. Non-strict mode keeps the gather as the
/// correctness-first fallback. Placed at the very end of each gather path so queries the gather
/// would have *declined* keep their original rejection reason.
fn ensure_gather_not_strict(fact: &str) -> Result<()> {
    if distributed_strict() {
        return Err(Error::Unsupported(format!(
            "auto-distribute: refusing whole-fact gather of sharded table `{fact}` in strict mode \
             (OXIDANT_DISTRIBUTED_STRICT=1): it would centralize the entire fact on one shuffle \
             partition (KAN-29) and no distributed semi/anti or decorrelated shape matched \
             this query"
        )));
    }
    Ok(())
}

pub(crate) fn expr_contains_subquery(e: &Expr) -> bool {
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
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

/// True when the subtree still carries a `Filter` node (all predicates must have been lifted
/// into the conjunct runs the caller already collected) or any expression subquery.
fn plan_has_filter_or_subquery_expr(lp: &LogicalPlan) -> bool {
    matches!(lp, LogicalPlan::Filter(_))
        || lp.expressions().iter().any(expr_contains_subquery)
        || lp
            .inputs()
            .iter()
            .any(|input| plan_has_filter_or_subquery_expr(input))
}

/// ` WHERE p1 AND p2 …` for a list of predicate exprs, or an empty string when there are none.
fn where_clause(up: &Unparser, preds: &[&Expr]) -> Result<String> {
    if preds.is_empty() {
        return Ok(String::new());
    }
    let parts = preds
        .iter()
        .map(|p| expr_sql(up, p))
        .collect::<Result<Vec<_>>>()?;
    Ok(format!(" WHERE {}", parts.join(" AND ")))
}

/// KAN-55 (TPC-DS Q9): a projection carrying **uncorrelated global-aggregate scalar
/// subqueries** over the sharded fact, on top of an all-replicated outer body:
///
/// ```sql
/// SELECT CASE WHEN (SELECT count(*) FROM store_sales WHERE ss_quantity BETWEEN 1 AND 20) > 74129
///        THEN (SELECT avg(ss_ext_discount_amt) FROM store_sales WHERE ss_quantity BETWEEN 1 AND 20)
///        ELSE (SELECT avg(ss_net_paid) FROM store_sales WHERE ss_quantity BETWEEN 1 AND 20) END
/// FROM reason WHERE r_reason_sk = 1
/// ```
///
/// Equivalence argument. Each scalar is a *global* aggregate, so it decomposes exactly under any
/// row partition: count → Σ of per-worker counts, sum → Σ of per-worker sums, avg → Σsum/Σcount,
/// min/max → min/max of partials. Stage pair per scalar: a per-worker partial over the local
/// shard (run once via [`ExchangeMode::Forward`] when the body is fully replicated — per-worker
/// partials of identical input would multiply the value) and a one-row combine gathered to
/// shuffle partition 0. The combined row IS the single-node value of the scalar, so substituting
/// `(SELECT * FROM shuffle_input_{i})` for each scalar subquery leaves the projection's value
/// unchanged. The outer body reads only replicated tables plus these scalar rows — identical on
/// every partition — so a partition-0 gate (`EXISTS` on a gathered one-row stage) makes the
/// query emit exactly once cluster-wide.
///
/// Scalars whose bodies share the same `FROM` tail (Q9: all 15 read `store_sales`, differing
/// only in filter conjuncts) merge into ONE partial computing every aggregate as
/// `agg(arg) FILTER (WHERE …)` over a single shared scan and ONE combine emitting every value as
/// a column `s{j}` of a single row; scalar `i` then reads `(SELECT s{j} FROM
/// shuffle_input_{u})`. A FILTER-ed partial equals the body's own `WHERE` partial — the filter
/// removes the same rows before aggregation — with the same NULL-over-empty convention
/// (FILTER-count is 0, FILTER-sum/avg NULL). Scalars with a unique tail keep the per-scalar
/// stage pair.
///
/// Anything outside this shape (grouped / DISTINCT / correlated scalar bodies, non-scalar
/// subqueries in the projection, aggregates or subqueries in the outer body, a sharded table in
/// the outer body) returns `Ok(None)` and keeps the existing gather / rejection behavior.
pub(crate) fn try_scalar_subquery_projection(
    lp: &LogicalPlan,
    replicated: &[&str],
) -> Result<Option<DistributedQuery>> {
    let (body, sort, limit) = peek_sort_limit(lp);
    let mut node = body;
    while let LogicalPlan::SubqueryAlias(s) = node {
        node = s.input.as_ref();
    }
    let LogicalPlan::Projection(proj) = node else {
        return Ok(None);
    };

    // Collect the scalar subqueries in projection (textual) order — the same order the Unparser
    // re-emits them, so replacement `i` below pairs with scalar `i`. Any other subquery kind, or
    // a top-level aggregate/window, is not this shape.
    let mut scalars: Vec<&LogicalPlan> = Vec::new();
    {
        use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
        for e in &proj.expr {
            let mut bad = false;
            let _ = e.apply(|expr| {
                match expr {
                    Expr::ScalarSubquery(sq) => scalars.push(sq.subquery.as_ref()),
                    Expr::Exists(_)
                    | Expr::InSubquery(_)
                    | Expr::AggregateFunction(_)
                    | Expr::WindowFunction(_) => {
                        bad = true;
                        return Ok(TreeNodeRecursion::Stop);
                    }
                    _ => {}
                }
                Ok(TreeNodeRecursion::Continue)
            });
            if bad {
                return Ok(None);
            }
        }
    }
    if scalars.is_empty() {
        return Ok(None);
    }

    // The outer body: replicated tables only, and no subqueries / aggregates / windows of its
    // own (those belong to other shapes).
    fn plan_exprs_subquery_free(lp: &LogicalPlan) -> bool {
        !lp.expressions().iter().any(expr_contains_subquery)
            && lp.inputs().iter().all(|i| plan_exprs_subquery_free(i))
    }
    let input = proj.input.as_ref();
    if base_tables(input)
        .iter()
        .any(|t| !replicated.contains(&t.as_str()))
        || plan_contains_aggregate(input)
        || plan_contains_window(input)
        || !plan_exprs_subquery_free(input)
    {
        return Ok(None);
    }

    let up = Unparser::default();
    let mut bodies: Vec<ScalarBodyStages> = Vec::with_capacity(scalars.len());
    for sub in &scalars {
        let Some(body) = global_scalar_body_stages(&up, sub, replicated)? else {
            return Ok(None);
        };
        bodies.push(body);
    }

    // Group scalars by identical body tail: Q9's 15 scalars all read `FROM store_sales` and
    // differ only in filter conjuncts, so one shared scan computing every aggregate as a
    // FILTER-ed partial replaces 15 sharded scans with 1. Groups of one keep the per-scalar
    // stage pair. Group order is first appearance, so a projection whose tails are all unique
    // emits exactly the per-scalar stages it did before.
    let mut groups: Vec<Vec<usize>> = Vec::new();
    let mut group_index: HashMap<(String, bool), usize> = HashMap::new();
    for (i, body) in bodies.iter().enumerate() {
        let key = (body.inner_tail.clone(), body.forward_partial);
        let g = match group_index.get(&key) {
            Some(&g) => g,
            None => {
                let g = groups.len();
                group_index.insert(key, g);
                groups.push(Vec::new());
                g
            }
        };
        groups[g].push(i);
    }

    let mut stages: Vec<StageDef> = Vec::new();
    let mut combine_ids: Vec<u32> = Vec::new();
    // Per scalar (textual order): the one-row subquery the projection rewrite substitutes —
    // `SELECT * FROM shuffle_input_{u}` for a per-scalar combine, `SELECT s{j} FROM
    // shuffle_input_{u}` for member j of a merged combine (whose single row carries every
    // member's value as a column).
    let mut scalar_replacements: Vec<Option<String>> = vec![None; scalars.len()];
    for group in &groups {
        let pid = stages.len() as u32;
        let (partial_sql, combine_sql);
        if group.len() == 1 {
            let body = &bodies[group[0]];
            partial_sql = body.partial_sql.clone();
            combine_sql = body.combine_sql.clone();
        } else {
            let mut items: Vec<String> = Vec::new();
            let mut combs: Vec<String> = Vec::new();
            let mut projs: Vec<String> = Vec::new();
            for (j, &i) in group.iter().enumerate() {
                let body = &bodies[i];
                items.extend(filter_partial_items(body, j)?);
                let (_, comb) = per_key_agg_parts(&body.func, &body.arg_sql, j)?;
                combs.push(format!("{comb} AS m{j}"));
                let proj = match &body.projection {
                    Some(e) => {
                        let mut rename = HashMap::new();
                        rename.insert("m0".to_string(), format!("m{j}"));
                        expr_sql(&up, &remap_expr_columns(e, &rename))?
                    }
                    None => format!("m{j}"),
                };
                projs.push(format!("{proj} AS s{j}"));
            }
            partial_sql = sanitize_generated_sql(&format!(
                "SELECT {} {}",
                items.join(", "),
                bodies[group[0]].inner_tail
            ));
            // Every member's partial rides the same one row per worker, so a single
            // `HAVING COUNT(*) > 0` keeps the combined row exactly on the gather partition.
            // A member that is NULL over empty input reads as a NULL scalar either way — the
            // per-scalar convention (see global_scalar_body_stages).
            combine_sql = format!(
                "SELECT {} FROM (SELECT {} FROM shuffle_input HAVING COUNT(*) > 0) AS combined",
                projs.join(", "),
                combs.join(", ")
            );
        }
        let mut partial = StageDef::new(pid, partial_sql, vec![], vec![]);
        if bodies[group[0]].forward_partial {
            // Replicated body: identical on every worker, so compute the partial exactly once.
            partial.exchange = ExchangeMode::Forward;
        }
        stages.push(partial);
        stages.push(StageDef::new(pid + 1, combine_sql, vec![pid], vec![]));
        combine_ids.push(pid + 1);
        let upstream = combine_ids.len() - 1;
        for (j, &i) in group.iter().enumerate() {
            scalar_replacements[i] = Some(if group.len() == 1 {
                format!("SELECT * FROM shuffle_input_{upstream}")
            } else {
                format!("SELECT s{j} FROM shuffle_input_{upstream}")
            });
        }
    }

    // Re-emit the original projection with each scalar subquery swapped for its one-row input,
    // then gate output to partition 0 (the outer tables are replicated — every partition could
    // otherwise produce the row).
    let original_sql = up
        .plan_to_sql(node)
        .map_err(|e| {
            Error::Unsupported(format!(
                "auto-distribute: unparse scalar-subquery projection: {e}"
            ))
        })?
        .to_string();
    let mut replacements: Vec<String> = Vec::with_capacity(scalars.len());
    for r in scalar_replacements {
        let Some(r) = r else {
            return Err(Error::Unsupported(
                "auto-distribute: scalar subquery did not map to a combine stage".into(),
            ));
        };
        replacements.push(r);
    }
    let Ok((rewritten_sql, replaced)) = rewrite_scalar_subqueries(&original_sql, &replacements)
    else {
        // The textual and logical subquery walks did not correspond 1:1 — not this shape.
        return Ok(None);
    };
    if replaced != scalars.len() {
        return Ok(None);
    }
    let gate_id = stages.len() as u32;
    stages.push(StageDef::new(
        gate_id,
        "SELECT 1 AS __oxidant_scalar_gate".to_string(),
        vec![],
        vec![],
    ));
    let mut upstreams = combine_ids;
    upstreams.push(gate_id);
    let final_id = stages.len() as u32;
    let final_sql = sanitize_generated_sql(&format!(
        "SELECT * FROM ({rewritten_sql}) AS __oxidant_scalar_src \
         WHERE EXISTS (SELECT 1 FROM shuffle_input_{})",
        upstreams.len() - 1
    ));
    stages.push(StageDef::new(final_id, final_sql, upstreams, vec![]));

    Ok(Some(DistributedQuery {
        stages,
        finalize_sql: build_outer_finalize(sort, limit)?,
    }))
}

/// One validated uncorrelated global-aggregate scalar body: its per-scalar stage SQL (used
/// as-is when no other scalar shares the body tail) plus the pieces
/// [`try_scalar_subquery_projection`] needs to merge same-tail bodies into one shared
/// FILTER-aggregate scan.
struct ScalarBodyStages {
    partial_sql: String,
    combine_sql: String,
    forward_partial: bool,
    /// Sanitized `FROM …` tail of the body — the merge key.
    inner_tail: String,
    /// Body filter conjuncts as ` WHERE …` (empty when unfiltered); becomes each aggregate's
    /// `FILTER (WHERE …)` clause in the merged partial.
    inner_where: String,
    /// Lowercased aggregate name and argument SQL (see [`AggSpec`]).
    func: String,
    arg_sql: String,
    /// The body's projection over the aggregate, validated to reference only the combined value
    /// as the unqualified column `m0` (`None` = the value itself).
    projection: Option<Expr>,
}

/// Validate one uncorrelated global-aggregate scalar body and emit its per-scalar stage SQL plus
/// the merge pieces — `Ok(None)` when the body is outside the provable shape. Mirrors the checks
/// in [`classify_scalar_conjunct`], minus the compare-side handling.
fn global_scalar_body_stages(
    up: &Unparser,
    subquery: &LogicalPlan,
    replicated: &[&str],
) -> Result<Option<ScalarBodyStages>> {
    let mut sp = subquery;
    while let LogicalPlan::SubqueryAlias(a) = sp {
        sp = a.input.as_ref();
    }
    // At most one single-expression projection over the aggregate (`count(Int64(1)) AS
    // count(*)`), re-applied in the combine with the combined value as `m0` — the same handling
    // as classify_scalar_conjunct.
    let mut projection: Option<&[Expr]> = None;
    while let LogicalPlan::Projection(pj) = sp {
        if projection.is_some() || pj.expr.len() != 1 {
            return Ok(None);
        }
        projection = Some(pj.expr.as_slice());
        sp = pj.input.as_ref();
    }
    let LogicalPlan::Aggregate(sub_agg) = sp else {
        return Ok(None);
    };
    if !sub_agg.group_expr.is_empty() || sub_agg.aggr_expr.len() != 1 {
        return Ok(None);
    }
    let Ok(spec) = AggSpec::classify(&sub_agg.aggr_expr[0]) else {
        return Ok(None);
    };
    if spec.distinct || !matches!(spec.func.as_str(), "min" | "max" | "sum" | "count" | "avg") {
        return Ok(None);
    }
    let mut inner_preds: Vec<&Expr> = Vec::new();
    let mut inner_body: &LogicalPlan = sub_agg.input.as_ref();
    while let LogicalPlan::Filter(f) = inner_body {
        flatten_conjuncts(&f.predicate, &mut inner_preds);
        inner_body = f.input.as_ref();
    }
    if plan_has_filter_or_subquery_expr(inner_body) || plan_contains_outer_reference(inner_body) {
        return Ok(None);
    }
    let scope = PlanScope::of(inner_body);
    for conjunct in &inner_preds {
        // A nested subquery inside a body predicate carries no `Column` nodes, so the scope
        // check below would pass it vacuously — reject it explicitly instead of evaluating it
        // per shard.
        if expr_contains_subquery(conjunct) {
            return Ok(None);
        }
        let mut cols = Vec::new();
        expr_columns_tagged(conjunct, &mut cols);
        if !cols
            .iter()
            .all(|(c, is_outer)| !is_outer && scope.contains(c))
        {
            return Ok(None);
        }
    }
    let mut arg_cols = Vec::new();
    expr_columns(&sub_agg.aggr_expr[0], &mut arg_cols);
    if !arg_cols.iter().all(|c| scope.contains(c)) {
        return Ok(None);
    }

    // Table safety: at most one sharded table in the body, scanned exactly once; every other
    // table replicated. A fully-replicated body computes its partial once (Forward).
    let inner_tables = base_tables(inner_body);
    let mut inner_sharded: Vec<&str> = inner_tables
        .iter()
        .map(String::as_str)
        .filter(|t| !replicated.contains(t))
        .collect();
    inner_sharded.sort_unstable();
    inner_sharded.dedup();
    let forward_partial = match inner_sharded.as_slice() {
        [] => true,
        [fact] => {
            if count_table_scans(inner_body, fact) != 1 {
                return Ok(None);
            }
            false
        }
        _ => return Ok(None),
    };

    let inner_sql = up
        .plan_to_sql(inner_body)
        .map_err(|e| {
            Error::Unsupported(format!(
                "auto-distribute: unparse scalar projection body: {e}"
            ))
        })?
        .to_string();
    let inner_tail = sanitize_generated_sql(&extract_from_tail(&inner_sql)?);
    let inner_where = where_clause(up, &inner_preds)?;
    let (items, comb) = per_key_agg_parts(&spec.func, &spec.arg_sql, 0)?;
    let partial_sql = sanitize_generated_sql(&format!(
        "SELECT {} {inner_tail}{inner_where}",
        items.join(", ")
    ));
    // The partial is a global aggregate: one row per worker even over an empty shard, so the
    // combine's input is non-empty on the gather partition and `HAVING COUNT(…) > 0` suppresses
    // the synthetic row elsewhere. AVG guards on its count partial (0, not NULL, over empty
    // input) so its NULL quotient row still reads as a NULL scalar — the same convention as
    // classify_scalar_conjunct.
    let guard = if spec.func == "avg" { "a0c" } else { "a0" };
    // A projection over the scalar aggregate is re-applied with the combined value as `m0`; its
    // only column reference must be the aggregate.
    let mut m0_remap: HashMap<String, String> = HashMap::new();
    m0_remap.insert(
        sub_agg.aggr_expr[0].schema_name().to_string(),
        "m0".to_string(),
    );
    if let Some(f) = sub_agg.schema.fields().first() {
        m0_remap.insert(f.name().clone(), "m0".to_string());
    }
    let mut projection_mapped: Option<Expr> = None;
    let proj_sql = match projection {
        Some(exprs) => {
            if expr_contains_subquery(&exprs[0]) {
                return Ok(None);
            }
            let mapped = remap_expr_columns(strip_alias(&exprs[0]), &m0_remap);
            let mut cols = Vec::new();
            expr_columns(&mapped, &mut cols);
            if !cols.iter().all(|c| c.relation.is_none() && c.name == "m0") {
                return Ok(None);
            }
            let sql = expr_sql(up, &mapped)?;
            projection_mapped = Some(mapped);
            sql
        }
        None => "m0".to_string(),
    };
    let combine_sql = format!(
        "SELECT {proj_sql} AS s0 FROM \
         (SELECT {comb} AS m0 FROM shuffle_input HAVING COUNT({guard}) > 0) AS combined"
    );
    Ok(Some(ScalarBodyStages {
        partial_sql,
        combine_sql,
        forward_partial,
        inner_tail,
        inner_where,
        func: spec.func,
        arg_sql: spec.arg_sql,
        projection: projection_mapped,
    }))
}

/// Member `j`'s partial columns for a merged same-tail scan: the [`per_key_agg_parts`] items
/// with the body's filter conjuncts re-attached as `agg(arg) FILTER (WHERE …)` (dropped when
/// the body is unfiltered). Nullability matches the body's own `WHERE` partial exactly —
/// FILTER-count is 0 (not NULL) over an empty band, FILTER-sum/avg is NULL — because the filter
/// removes the same rows before aggregation.
fn filter_partial_items(body: &ScalarBodyStages, j: usize) -> Result<Vec<String>> {
    let (items, _) = per_key_agg_parts(&body.func, &body.arg_sql, j)?;
    if body.inner_where.is_empty() {
        return Ok(items);
    }
    items
        .into_iter()
        .map(|item| {
            // `sum(x) AS a1s` → `sum(x) FILTER (WHERE …) AS a1s`; the alias is the last
            // ` AS ` (an argument CAST's AS sits earlier).
            let Some(k) = item.rfind(" AS ") else {
                return Err(Error::Unsupported(format!(
                    "auto-distribute: partial aggregate `{item}` has no alias"
                )));
            };
            Ok(format!(
                "{} FILTER ({}){}",
                &item[..k],
                body.inner_where.trim_start(),
                &item[k..]
            ))
        })
        .collect()
}

/// Parse generated SQL and replace each scalar `(SELECT …)` expression — in textual order — with
/// the given one-row subquery (`SELECT * FROM shuffle_input_{u}` for a per-scalar combine,
/// `SELECT s{j} FROM shuffle_input_{u}` for member `j` of a merged same-tail combine), returning
/// the rewritten SQL and the replacement count. Only scalar subquery expression nodes are
/// touched (derived tables in FROM are a different AST node); the count lets the caller verify
/// the logical and textual subquery walks corresponded 1:1 before trusting the rewrite.
fn rewrite_scalar_subqueries(sql: &str, replacements: &[String]) -> Result<(String, usize)> {
    use std::ops::ControlFlow;

    use datafusion::sql::sqlparser::ast::{Expr as SqlExpr, Statement, VisitMut, VisitorMut};
    use datafusion::sql::sqlparser::dialect::GenericDialect;
    use datafusion::sql::sqlparser::parser::Parser;

    let mut replacement_exprs: Vec<SqlExpr> = Vec::with_capacity(replacements.len());
    for text in replacements {
        let mut stmts = Parser::parse_sql(&GenericDialect {}, text).map_err(|e| {
            Error::Unsupported(format!(
                "auto-distribute: build scalar replacement subquery: {e}"
            ))
        })?;
        let Some(Statement::Query(q)) = stmts.pop() else {
            return Err(Error::Unsupported(
                "auto-distribute: scalar replacement subquery did not parse".into(),
            ));
        };
        replacement_exprs.push(SqlExpr::Subquery(q));
    }

    struct Rewriter {
        replacements: Vec<SqlExpr>,
        count: usize,
    }
    impl VisitorMut for Rewriter {
        type Break = ();
        fn pre_visit_expr(&mut self, expr: &mut SqlExpr) -> ControlFlow<Self::Break> {
            if matches!(expr, SqlExpr::Subquery(_)) {
                let i = self.count;
                self.count += 1;
                if i < self.replacements.len() {
                    // Swap the whole node; the replacement contains no subquery of its own, and
                    // the original subtree is discarded unwalked (a nested subquery inside it
                    // would surface as a count mismatch at the caller).
                    *expr = self.replacements[i].clone();
                }
            }
            ControlFlow::Continue(())
        }
    }

    let mut statements = Parser::parse_sql(&GenericDialect {}, sql).map_err(|e| {
        Error::Unsupported(format!(
            "auto-distribute: parse generated SQL for scalar replacement: {e}"
        ))
    })?;
    if statements.len() != 1 {
        return Err(Error::Unsupported(format!(
            "auto-distribute: scalar replacement expected one statement, found {}",
            statements.len()
        )));
    }
    let mut rewriter = Rewriter {
        replacements: replacement_exprs,
        count: 0,
    };
    let _ = statements.visit(&mut rewriter);
    if rewriter.count != replacements.len() {
        return Err(Error::Unsupported(format!(
            "auto-distribute: scalar replacement saw {} textual subqueries for {} logical",
            rewriter.count,
            replacements.len()
        )));
    }
    Ok((statements.remove(0).to_string(), rewriter.count))
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
    ensure_gather_not_strict(fact)?;
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
                "SELECT 1 AS __oxidant_subquery_gate".to_string(),
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
    // - ROLLUP + UNION ALL: the gather round-trips the whole query through the Unparser, which
    //   is not semantics-faithful here (KAN-54 verified distributed ≠ single-node at sf=0.01).
    //   These now plan through the genuinely-distributed union split instead (KAN-49d:
    //   Q5/Q77/Q80 — see `try_split_broadcast_union`), which never reaches this fallback.
    // - ROLLUP + INTERSECT/EXCEPT with a subquery over the sharded fact (Q14): still declined —
    //   Unparser emits out-of-scope `brand_id` aliases, and the proper composition (semi-shuffle
    //   the IN over a distributed INTERSECT + a one-row broadcast of the UNION ALL average) is
    //   not built yet.
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
    ensure_gather_not_strict(fact)?;
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
                "SELECT 1 AS __oxidant_materialize_gate".to_string(),
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
    // A bare `Union` is always bag union (UNION DISTINCT is a `Distinct` over a `Union`), which
    // is associative — flatten a nested `Union(Union(a, b), c)` tree (TPC-DS Q27's hand-rolled
    // ROLLUP unions three aggregates over one CTE) so each leaf arm plans independently instead
    // of failing the arm peel on a nested `Union` node.
    let mut arms: Vec<Arc<LogicalPlan>> = Vec::new();
    for input in &u.inputs {
        super::stage_planner::flatten_union_all(input, &mut arms);
    }
    if arms.len() < 2 {
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

    for (arm_i, arm) in arms.iter().enumerate() {
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

pub(crate) fn build_outer_finalize(
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
                let expr = super::stage_planner::finalize_expr_sql(&up, &unqualify(&s.expr))?;
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

pub(crate) fn peek_sort_limit(
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

pub(crate) fn collect_subquery_tables(lp: &LogicalPlan, out: &mut Vec<String>) {
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

/// KAN-49a (TPC-DS Q1/Q30/Q81): decorrelate an equality-correlated scalar-aggregate subquery in
/// a `Filter` into a derived per-key aggregate that is inner-joined into the plan:
///
/// ```sql
/// SELECT … FROM ctr1, store, customer
/// WHERE ctr1.ctr_total_return >
///     (SELECT avg(ctr2.ctr_total_return) * 1.2
///      FROM customer_total_return ctr2
///      WHERE ctr1.ctr_store_sk = ctr2.ctr_store_sk)
/// ```
///
/// becomes, plan-wise,
///
/// ```sql
/// SELECT … FROM ctr1, store, customer
/// JOIN (SELECT ctr2.ctr_store_sk AS k0, avg(ctr2.ctr_total_return) * 1.2 AS thr0
///       FROM customer_total_return ctr2 GROUP BY ctr2.ctr_store_sk) AS __oxidant_decorr_0
///   ON ctr1.ctr_store_sk = __oxidant_decorr_0.k0
/// WHERE ctr1.ctr_total_return > __oxidant_decorr_0.thr0
/// ```
///
/// Equivalence: the derived table emits at most one row per correlation key, so the inner join
/// cannot fan out; an outer row whose key has no group is dropped, exactly like the original
/// compare against a NULL scalar (any comparison with NULL is not-true). This is only valid when
/// the scalar sits as a direct operand of a top-level comparison conjunct — under `OR`/`CASE`/
/// `NOT` a NULL scalar takes the other branch instead of dropping the row — so anything else is
/// left untouched (returns `None` when nothing was decorrelated).
///
/// The rewrite is shape-driven rather than result-driven: it exists so
/// [`super::dag_splitter`] can materialize the derived per-key aggregate as its own branch
/// (aggregate-over-aggregate over the sharded fact) instead of rejecting the correlated scalar
/// scan left in the outer skeleton.
pub(crate) fn rewrite_correlated_scalar_subqueries(lp: &LogicalPlan) -> Option<LogicalPlan> {
    let mut counter = 0usize;
    let (plan, changed) = rewrite_correlated_scalar_node(lp, &mut counter);
    changed.then_some(plan)
}

fn rewrite_correlated_scalar_node(lp: &LogicalPlan, counter: &mut usize) -> (LogicalPlan, bool) {
    let mut changed = false;
    let mut new_inputs = Vec::with_capacity(lp.inputs().len());
    for input in lp.inputs() {
        let (rewritten, child_changed) = rewrite_correlated_scalar_node(input, counter);
        changed |= child_changed;
        new_inputs.push(rewritten);
    }
    let mut node = if changed {
        match lp.with_new_exprs(lp.expressions(), new_inputs) {
            Ok(n) => n,
            Err(_) => return (lp.clone(), false),
        }
    } else {
        lp.clone()
    };
    if let LogicalPlan::Filter(f) = &node {
        if let Some(rewritten) = decorrelate_filter_scalars(f, counter) {
            node = rewritten;
            changed = true;
        }
    }
    (node, changed)
}

/// One extracted decorrelation: the derived per-key aggregate join side plus the conjunct
/// rewritten to compare against it.
struct DecorrelatedScalar {
    derived: LogicalPlan,
    alias: String,
    /// Correlation-key columns on the outer (filter-input) side, paired with the derived side's
    /// `k{j}` columns in order.
    join_outer_cols: Vec<Column>,
    /// The conjunct with the scalar subquery replaced by `alias.thr0`.
    conjunct: Expr,
}

fn decorrelate_filter_scalars(
    f: &datafusion::logical_expr::Filter,
    counter: &mut usize,
) -> Option<LogicalPlan> {
    let outer_scope = super::join_chain::JoinSideScope::of(&f.input);

    let mut conjuncts = Vec::new();
    super::stage_planner::flatten_and_conjuncts(&f.predicate, &mut conjuncts);

    let mut kept: Vec<Expr> = Vec::new();
    let mut decorrelated: Vec<DecorrelatedScalar> = Vec::new();
    for conjunct in conjuncts {
        match extract_scalar_compare(&conjunct).and_then(|(op, operand, sub)| {
            build_derived_per_key_aggregate(op, operand, sub, &outer_scope, counter)
        }) {
            Some(d) => decorrelated.push(d),
            None => kept.push(conjunct),
        }
    }
    if decorrelated.is_empty() {
        return None;
    }

    // Chain the derived per-key aggregates under the (remaining) filter as inner joins on the
    // correlation keys.
    let mut input = f.input.as_ref().clone();
    for d in &decorrelated {
        let on_left: Vec<Column> = d.join_outer_cols.to_vec();
        let on_right: Vec<Column> = (0..d.join_outer_cols.len())
            .map(|j| Column::new(Some(d.alias.as_str()), format!("k{j}")))
            .collect();
        input = datafusion::logical_expr::LogicalPlanBuilder::from(input)
            .join(
                d.derived.clone(),
                JoinType::Inner,
                (on_left, on_right),
                None,
            )
            .ok()?
            .build()
            .ok()?;
    }

    let new_conjuncts: Vec<Expr> = kept
        .into_iter()
        .chain(decorrelated.into_iter().map(|d| d.conjunct))
        .collect();
    let predicate = new_conjuncts.into_iter().reduce(Expr::and)?;
    datafusion::logical_expr::LogicalPlanBuilder::from(input)
        .filter(predicate)
        .ok()?
        .build()
        .ok()
}

/// Split a top-level conjunct into `(compare op, subquery-free operand, scalar subquery)` when it
/// is exactly `<operand> <cmp> <scalar>` or `<scalar> <cmp> <operand>` with a comparison whose
/// NULL outcome always filters the row out.
fn extract_scalar_compare(
    conjunct: &Expr,
) -> Option<(
    Operator,
    &Expr,
    &datafusion::logical_expr::logical_plan::Subquery,
)> {
    let Expr::BinaryExpr(b) = conjunct else {
        return None;
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
        return None;
    }
    let (operand, sub, op) = match (b.left.as_ref(), b.right.as_ref()) {
        (Expr::ScalarSubquery(sub), other) | (other, Expr::ScalarSubquery(sub)) => {
            (other, sub, b.op)
        }
        _ => return None,
    };
    // The non-subquery operand must not itself carry a subquery (nesting is a different shape).
    if expr_contains_subquery(operand) {
        return None;
    }
    Some((op, operand, sub))
}

/// Validate the correlated scalar subquery's shape and build the derived per-key aggregate side
/// of the decorrelation join. Returns `None` for anything outside the supported shape — the
/// conjunct then stays as written and the query keeps its original error path.
fn build_derived_per_key_aggregate(
    op: Operator,
    operand: &Expr,
    sub: &datafusion::logical_expr::logical_plan::Subquery,
    outer_scope: &super::join_chain::JoinSideScope,
    counter: &mut usize,
) -> Option<DecorrelatedScalar> {
    // The subquery-free operand must read only outer-scope columns (else it is not evaluable
    // above the join).
    let mut operand_cols = Vec::new();
    expr_columns(operand, &mut operand_cols);
    if !operand_cols.iter().all(|c| outer_scope.contains(c)) {
        return None;
    }

    // Shape: [Projection(single expr)] → Aggregate([], one agg) → [Filter] → body.
    let mut node = sub.subquery.as_ref();
    let mut scalar_proj: Option<&Expr> = None;
    while let LogicalPlan::Projection(p) = node {
        if scalar_proj.is_some() || p.expr.len() != 1 {
            return None;
        }
        scalar_proj = Some(&p.expr[0]);
        node = p.input.as_ref();
    }
    let LogicalPlan::Aggregate(agg) = node else {
        return None;
    };
    if !agg.group_expr.is_empty() || agg.aggr_expr.len() != 1 {
        return None;
    }
    let agg_expr = strip_expr_alias(&agg.aggr_expr[0]);
    let Expr::AggregateFunction(af) = agg_expr else {
        return None;
    };
    if af.params.distinct
        || !matches!(
            af.func.name().to_ascii_lowercase().as_str(),
            "min" | "max" | "sum" | "count" | "avg"
        )
    {
        return None;
    }
    let mut inner_preds: Vec<&Expr> = Vec::new();
    let mut body = agg.input.as_ref();
    while let LogicalPlan::Filter(filter) = body {
        flatten_conjuncts(&filter.predicate, &mut inner_preds);
        body = filter.input.as_ref();
    }
    // Nested expression subqueries inside the body would re-introduce a scan the derived branch
    // cannot place; plain `Filter` nodes deeper in the body are fine — the branch planner
    // handles them.
    if plan_exprs_contain_subquery(body) {
        return None;
    }

    // Correlation: equality conjuncts pairing an outer reference with a plain inner column.
    let body_scope = PlanScope::of(body);
    let mut outer_cols: Vec<Column> = Vec::new();
    let mut inner_cols: Vec<Column> = Vec::new();
    let mut remaining: Vec<&Expr> = Vec::new();
    for pred in inner_preds {
        let pair = match pred {
            Expr::BinaryExpr(b) if b.op == Operator::Eq => {
                match (b.left.as_ref(), b.right.as_ref()) {
                    (Expr::OuterReferenceColumn(_, oc), Expr::Column(ic))
                    | (Expr::Column(ic), Expr::OuterReferenceColumn(_, oc)) => {
                        Some((oc.clone(), ic.clone()))
                    }
                    _ => None,
                }
            }
            _ => None,
        };
        match pair {
            Some((oc, ic)) => {
                outer_cols.push(oc);
                inner_cols.push(ic);
            }
            None => remaining.push(pred),
        }
    }
    if outer_cols.is_empty() {
        return None;
    }
    // Every correlation key must resolve on its own side…
    if !outer_cols.iter().all(|c| outer_scope.contains(c))
        || !inner_cols.iter().all(|c| body_scope.contains(c))
    {
        return None;
    }
    // …and no other outer reference may survive anywhere in the subquery (a correlated residual
    // predicate would evaluate per shard).
    let mut tagged: Vec<(Column, bool)> = Vec::new();
    for e in remaining.iter().copied().chain(scalar_proj) {
        expr_columns_tagged(e, &mut tagged);
    }
    for e in &agg.aggr_expr {
        expr_columns_tagged(e, &mut tagged);
    }
    if tagged.iter().any(|(_, is_outer)| *is_outer) {
        return None;
    }
    if remaining.iter().any(|pred| expr_contains_subquery(pred))
        || plan_contains_outer_reference(body)
    {
        return None;
    }
    // The aggregate's argument must read body-scope columns only.
    let mut arg_cols = Vec::new();
    expr_columns(agg_expr, &mut arg_cols);
    if !arg_cols.iter().all(|c| body_scope.contains(c)) {
        return None;
    }

    // The scalar's projection over the aggregate (`avg(x) * 1.2`) re-applies per group: swap the
    // aggregate for the derived `a0` column. Any other surviving reference rejects the shape.
    let a0 = datafusion::prelude::col("a0");
    let thr_expr = match scalar_proj {
        None => a0.clone(),
        Some(proj) => {
            let swapped = substitute_agg_with_column(proj, agg_expr, &a0);
            let mut cols = Vec::new();
            expr_columns(&swapped, &mut cols);
            if !cols.iter().all(|c| c.relation.is_none() && c.name == "a0")
                || contains_aggregate_function(&swapped)
            {
                return None;
            }
            swapped
        }
    };

    let alias = format!("__oxidant_decorr_{counter}");
    *counter += 1;

    // Derived side: `SELECT <inner keys> AS k{j}, <thr> AS thr0 FROM <body> [WHERE <remaining>]
    // GROUP BY <inner keys>` — the per-key aggregate the outer skeleton joins on.
    let mut derived = body.clone();
    if !remaining.is_empty() {
        let pred = remaining.into_iter().cloned().reduce(Expr::and)?;
        derived = datafusion::logical_expr::LogicalPlanBuilder::from(derived)
            .filter(pred)
            .ok()?
            .build()
            .ok()?;
    }
    let group_exprs: Vec<Expr> = inner_cols.iter().cloned().map(Expr::Column).collect();
    let aggr_with_alias = agg.aggr_expr[0].clone().alias("a0");
    derived = datafusion::logical_expr::LogicalPlanBuilder::from(derived)
        .aggregate(group_exprs.clone(), vec![aggr_with_alias])
        .ok()?
        .build()
        .ok()?;
    let mut proj_exprs: Vec<Expr> = group_exprs
        .iter()
        .enumerate()
        .map(|(j, g)| g.clone().alias(format!("k{j}")))
        .collect();
    proj_exprs.push(thr_expr.alias("thr0"));
    derived = datafusion::logical_expr::LogicalPlanBuilder::from(derived)
        .project(proj_exprs)
        .ok()?
        .alias(alias.as_str())
        .ok()?
        .build()
        .ok()?;

    let thr_ref = Expr::Column(Column::new(Some(alias.as_str()), "thr0"));
    let conjunct = Expr::BinaryExpr(BinaryExpr {
        left: Box::new(operand.clone()),
        op,
        right: Box::new(thr_ref),
    });
    Some(DecorrelatedScalar {
        derived,
        alias,
        join_outer_cols: outer_cols,
        conjunct,
    })
}

/// The expr without its outermost alias layer, if any.
fn strip_expr_alias(e: &Expr) -> &Expr {
    match e {
        Expr::Alias(a) => a.expr.as_ref(),
        other => other,
    }
}

/// Replace occurrences of `agg` (by structural equality, or by a `Column` naming the aggregate's
/// output) with `replacement` inside `e`.
fn substitute_agg_with_column(e: &Expr, agg: &Expr, replacement: &Expr) -> Expr {
    use datafusion::common::tree_node::{Transformed, TreeNode};
    let agg_name = agg.schema_name().to_string();
    e.clone()
        .transform(|node| {
            if node == *agg {
                return Ok(Transformed::yes(replacement.clone()));
            }
            if let Expr::Column(c) = &node {
                if c.name == agg_name {
                    return Ok(Transformed::yes(replacement.clone()));
                }
            }
            Ok(Transformed::no(node))
        })
        .map(|t| t.data)
        .unwrap_or_else(|_| e.clone())
}

fn contains_aggregate_function(e: &Expr) -> bool {
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
    let mut found = false;
    let _ = e.apply(|node| {
        if matches!(node, Expr::AggregateFunction(_)) {
            found = true;
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    });
    found
}

#[cfg(test)]
mod tests {
    use oxidant_loom::arrow::array::{Int64Array, RecordBatch};
    use oxidant_loom::arrow::datatypes::{DataType, Field, Schema};
    use oxidant_loom::arrow::util::pretty::pretty_format_batches;
    use oxidant_loom::Engine;

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
            "__oxidant_subquery_gate",
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
    async fn window_over_a_union_of_aggregates_plans_dedup_then_window() {
        // KAN-49b (TPC-DS Q36 shape): the window sits over a UNION (DISTINCT) of independently
        // distributable aggregate arms — each arm plans through the ordinary machinery, the arm
        // outputs hash-shuffle on the full row into a per-partition dedup (identical rows
        // co-locate), and the window re-shuffles the deduplicated rows by its PARTITION BY key.
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
        let dq = try_window(&lp, &[])
            .expect("ok")
            .expect("window over a UNION of distributable aggregates should plan");
        assert!(
            dq.stages
                .iter()
                .any(|s| s.sql.contains("SELECT DISTINCT") && s.sql.contains("UNION ALL")),
            "the dedup stage unions the arm streams before deduplicating: {dq:?}"
        );
        let window = dq.stages.last().expect("window stage");
        assert!(
            window.sql.contains("avg(v) OVER (PARTITION BY k)"),
            "the window computes after the partition shuffle: {}",
            window.sql
        );
    }

    #[tokio::test]
    async fn window_function_rank_over_aggregate_plans_co_located() {
        // KAN-49a: a ranking window with a non-empty PARTITION BY is exact after the partition
        // hash-shuffle (every partition's rows are wholly on one worker), so it plans now —
        // where it used to be rejected. Global ranking windows (no PARTITION BY) plan too
        // (KAN-49b): the tiny post-aggregate result gathers to partition 0.
        let engine = Engine::new();
        engine.register_batches("t", vec![tiny_table()]).unwrap();
        let lp = engine
            .logical_plan(
                "SELECT k, sum(v) AS sv, rank() OVER (PARTITION BY k ORDER BY sum(v)) AS rk \
                 FROM t GROUP BY k, v",
            )
            .await
            .unwrap();
        let dq = try_window(&lp, &[])
            .expect("ok")
            .expect("rank over a partition co-located aggregate should plan");
        let window = dq.stages.last().expect("window stage");
        assert!(
            window
                .sql
                .contains("rank() OVER (PARTITION BY g0 ORDER BY r0 ASC NULLS FIRST)"),
            "the ranking window computes locally after the partition shuffle: {}",
            window.sql
        );
    }

    #[tokio::test]
    async fn window_function_global_rank_over_aggregate_plans_gathered() {
        // KAN-49b (TPC-DS Q44): a global ranking window (no PARTITION BY) over an aggregate is
        // exact once the tiny combined aggregate output gathers to partition 0 (a
        // post-aggregation gather — never the raw fact).
        let engine = Engine::new();
        engine.register_batches("t", vec![tiny_table()]).unwrap();
        let lp = engine
            .logical_plan(
                "SELECT k, sum(v) AS sv, rank() OVER (ORDER BY sum(v)) AS rk FROM t GROUP BY k, v",
            )
            .await
            .unwrap();
        let dq = try_window(&lp, &[])
            .expect("ok")
            .expect("a global ranking window over an aggregate should plan");
        let combine = &dq.stages[dq.stages.len() - 2];
        assert!(
            combine.hash_key_cols.is_empty(),
            "the combine gathers to partition 0 for the global rank: {combine:?}"
        );
        let window = dq.stages.last().expect("window stage");
        assert!(
            window
                .sql
                .contains("rank() OVER (ORDER BY r0 ASC NULLS FIRST)"),
            "the global rank computes on the gathered combine partition: {}",
            window.sql
        );
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

//! Derive a distributed [`StageDef`] DAG automatically from a SQL query.
//!
//! ## Supported shape (v1)
//!
//! A single **grouped aggregation** over one table (optionally filtered, sorted, limited):
//!
//! ```sql
//! SELECT <group cols>, <aggregates> FROM t [WHERE ...] GROUP BY <cols> [ORDER BY ...] [LIMIT n]
//! ```
//!
//! It lowers to the canonical two stages — *partial aggregate per worker → hash shuffle by the
//! group key → final combine*:
//!
//! - re-combinable aggregates lower directly (`SUM→SUM`, `COUNT→SUM`, `MIN→MIN`, `MAX→MAX`);
//! - `AVG(x)` is split into `SUM(x)`/`COUNT(x)` partials and recombined as `Σsum / Σcount`;
//! - `COUNT(DISTINCT x)` (and any other `DISTINCT` aggregate) can't pre-aggregate, so the partial
//!   stage instead *projects* the grouping + argument columns and shuffles the raw rows by the
//!   group key; the final stage runs the original aggregate over the co-located rows (exact,
//!   because every group lands wholly on one worker).
//!
//! ## Joins (broadcast)
//!
//! A join is auto-derived when every base table but one is **replicated** (passed in `replicated` —
//! present in full on every worker): the join then runs locally per worker over the single sharded
//! table's shard, so it folds straight into the partial stage's FROM tail with no extra shuffle.
//! This covers star schemas (a sharded fact + replicated dimensions, including multi-dim
//! join chains folded into the partial). Joins between two or more *sharded* tables lower to a
//! **left-deep shuffle-join chain** (pairwise equijoin stages, then partial/final aggregate)
//! when each join is a single equijoin key.
//!
//! Also supported: ungrouped/global aggregates, `HAVING` over the aggregated result,
//! scalar / IN / EXISTS subqueries **over replicated tables only**, distributable set operations,
//! and **narrow window** support: re-combinable aggregate
//! windows (`SUM`/`COUNT`/`MIN`/`MAX`/`AVG`) with a non-empty `PARTITION BY` over one
//! sharded table (hash-shuffle by the partition key, then compute the window locally).
//! CTE-heavy outer cross joins are lowered recursively by [`super::dag_splitter`]: each sharded
//! aggregate branch becomes its own sub-DAG, and a gathered outer stage combines the branch
//! outputs with any replicated-only inputs.
//! Ranking windows and global windows (no `PARTITION BY`) return an explicit
//! [`Error::Unsupported`] so the caller falls back to single-node execution.
//! Correlated subqueries over sharded tables are rejected (not broadcast-safe).

use std::collections::HashMap;

use datafusion::logical_expr::{Aggregate, Expr, JoinType, LogicalPlan};
use datafusion::sql::unparser::Unparser;
use weft_common::{Error, Result};
use weft_loom::Engine;

use super::shape_extensions::{
    ensure_subquery_tables_replicated, reject_explicit_unsupported, try_non_aggregate,
    try_union_all, try_window,
};
use crate::driver::StageDef;

/// A query lowered to a distributed [`StageDef`] DAG.
#[derive(Debug, Clone)]
pub struct DistributedQuery {
    /// Topologically-ordered stages; the last is the output stage. Its result is the grouped
    /// aggregation, **unordered** — a global `ORDER BY` / `LIMIT` can't be applied per-worker.
    pub stages: Vec<StageDef>,
    /// Optional global finalize to run on the *gathered* result (registered as table `result`):
    /// the query's `ORDER BY` / `LIMIT`, which must run once over all workers' output, not per
    /// worker. `None` when the query has neither.
    pub finalize_sql: Option<String>,
}

/// Derive a distributed plan for `sql`, or [`Error::Unsupported`] if its shape isn't handled yet.
///
/// `replicated` names base tables that are present in **full** on every worker (small dimension
/// tables). A join is auto-derived as a **broadcast join** — it runs locally per worker — as long as
/// every table but one is replicated (so exactly one table is sharded). Joins between two or more
/// *sharded* tables are auto-derived as a left-deep shuffle-join chain when each join is a single
/// equijoin.
/// Derive a distributed plan from an already-built logical plan.
pub fn plan_distributed_logical(lp: &LogicalPlan, replicated: &[&str]) -> Result<DistributedQuery> {
    if let Some(dq) = try_union_all(lp, replicated)? {
        return Ok(dq);
    }
    if let Some(dq) = try_window(lp, replicated)? {
        return Ok(dq);
    }
    if let Some(dq) = try_non_aggregate(lp, replicated)? {
        return Ok(dq);
    }
    reject_explicit_unsupported(lp)?;
    let dq = match peel(lp) {
        Ok(peeled) => aggregation_stages_for(&peeled, replicated),
        Err(linear_error) => match super::dag_splitter::try_branch_dag(lp, replicated)? {
            Some(dq) => Ok(dq),
            None => Err(linear_error),
        },
    }?;
    validate_stage_sql(&dq)?;
    Ok(dq)
}

/// Last-line check on the SQL every stage will hand to a worker.
///
/// Individual shape handlers each splice Unparser output into their own stage SQL, so a
/// generated-SQL defect has to be caught in each of them or in one place after the fact. This is
/// that one place — it runs on whatever the chosen path produced.
fn validate_stage_sql(dq: &DistributedQuery) -> Result<()> {
    for s in &dq.stages {
        reject_out_of_scope_join_alias_refs(&s.sql)?;
    }
    if let Some(f) = &dq.finalize_sql {
        reject_out_of_scope_join_alias_refs(f)?;
    }
    Ok(())
}

/// SQL convenience wrapper around [`plan_distributed_logical`].
///
/// When the shape-based planner cannot lower the query, falls back to a single
/// [`ExchangeMode::Forward`] stage via [`super::physical_splitter::plan_forward`] (Sail-like
/// coverage: any locally-plannable SQL still gets a distributed job graph on one worker that
/// has a full view of the tables). Planner coverage / ratchets must call
/// [`plan_distributed_logical`] directly so Forward does not inflate the supported count.
pub async fn plan_distributed(
    engine: &Engine,
    sql: &str,
    replicated: &[&str],
) -> Result<DistributedQuery> {
    let (lp, lakehouse_snapshot_pins) = engine.logical_plan_with_lakehouse_snapshots(sql).await?;
    let mut query = match plan_distributed_logical(&lp, replicated) {
        Ok(dq) => Ok(dq),
        Err(Error::Unsupported(_)) => {
            crate::plan::physical_splitter::plan_forward(engine, sql).await
        }
        Err(e) => Err(e),
    }?;
    for stage in &mut query.stages {
        stage.lakehouse_snapshot_pins = lakehouse_snapshot_pins.clone();
    }
    Ok(query)
}

/// The top of the plan above the aggregate: the output projection (if any) plus the trailing
/// `ORDER BY` / `LIMIT`, which the final stage must reproduce.
pub(crate) struct Peeled<'a> {
    /// Output projection exprs (the SELECT list), if the plan has a `Projection` over the aggregate.
    pub(crate) projection: Option<&'a [Expr]>,
    /// `ORDER BY` exprs to apply on the final output, if any.
    pub(crate) sort: Option<&'a [datafusion::logical_expr::SortExpr]>,
    /// `LIMIT` fetch count, if any.
    pub(crate) limit: Option<usize>,
    /// Post-aggregate (`HAVING`) predicates, outermost first. Every `Filter` above the `Aggregate`
    /// lands here — see [`peel`].
    pub(crate) having: Vec<&'a Expr>,
    /// `Projection`s found *below* the output projection and above the `Aggregate`, which only
    /// rename the aggregate's output columns. TPC-DS Q21's `SELECT * FROM (SELECT … sum(…) AS
    /// inv_before … GROUP BY …) x WHERE …` puts one here: the inner subquery aliases the aggregate
    /// output before the `HAVING` or the outer projection ever names it. Ordered innermost-first so
    /// [`build_remap`] can fold them in the order the aliases were introduced.
    pub(crate) alias_projections: Vec<&'a [Expr]>,
    /// The aggregate node itself.
    pub(crate) agg: &'a Aggregate,
}

/// Strip an optional `Limit` / `Sort` / `Projection` off the top and require an `Aggregate` under
/// them. Rejects anything else (the caller falls back to single-node).
///
/// Every `Filter` crossed on the way down is a post-aggregate predicate: this loop only descends
/// through `Limit` / `Sort` / `Projection` / `Filter` / `SubqueryAlias`, so if it reaches the
/// `Aggregate` at all, nothing it passed could have filtered pre-aggregation rows. They are
/// therefore all collected as `HAVING` rather than matched positionally — an earlier version
/// required the `Filter` to sit *directly* on the `Aggregate` and silently discarded the predicate
/// otherwise, which made TPC-DS Q21 (`Filter` → `SubqueryAlias` → `Projection` → `Aggregate`)
/// return unfiltered rows.
pub(crate) fn peel(lp: &LogicalPlan) -> Result<Peeled<'_>> {
    let mut limit = None;
    let mut sort = None;
    let mut projection = None;
    let mut having = Vec::new();
    let mut alias_projections: Vec<&[Expr]> = Vec::new();
    let mut node = lp;
    loop {
        match node {
            LogicalPlan::Limit(l) => {
                // Only a plain `LIMIT n` (no OFFSET) is supported; fetch is an Expr in DF54.
                if let Some(Expr::Literal(scalar, _)) = l.fetch.as_deref() {
                    limit = scalar_as_usize(scalar);
                }
                node = &l.input;
            }
            LogicalPlan::Sort(s) => {
                sort = Some(s.expr.as_slice());
                node = &s.input;
            }
            LogicalPlan::Projection(p) => {
                // Scanning outer→inner, the first `Projection` is the query's real output
                // projection. Anything below it only renames aggregate output on the way to the
                // `Aggregate`, and is folded into the remap instead of replacing the output list.
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
            LogicalPlan::Aggregate(agg) => {
                alias_projections.reverse();
                return Ok(Peeled {
                    projection,
                    sort,
                    limit,
                    having,
                    alias_projections,
                    agg,
                });
            }
            other => {
                return Err(Error::Unsupported(format!(
                    "auto-distribute: unsupported top-level plan node `{}`",
                    other.display().to_string().lines().next().unwrap_or("")
                )))
            }
        }
    }
}

/// Build the distributed plan for a (possibly global) aggregation.
pub(crate) fn aggregation_stages_for(
    p: &Peeled<'_>,
    replicated: &[&str],
) -> Result<DistributedQuery> {
    let agg = p.agg;
    reject_grouping_sets(&agg.group_expr)?;
    let tables = base_tables(&agg.input);
    let sharded: Vec<&str> = tables
        .iter()
        .filter(|t| !replicated.contains(&t.as_str()))
        .map(|t| t.as_str())
        .collect();

    // Subqueries (IN / EXISTS / scalar) only over replicated dims — never over unreplicated tables.
    ensure_subquery_tables_replicated(&agg.input, &sharded, replicated)?;

    if agg.group_expr.is_empty() {
        return global_aggregation_stages(p, &sharded);
    }

    // Two or more sharded tables → left-deep shuffle-join chain + aggregate.
    if sharded.len() >= 2 {
        return crate::plan::join_chain::plan_shuffle_join_chain(p, &sharded, replicated);
    }

    // Broadcast-join safety: exactly one base table may be sharded; others must be replicated.
    if sharded.len() != 1 {
        return Err(Error::Unsupported(format!(
            "auto-distribute: need exactly one sharded base table (others replicated), \
             found {} sharded among {tables:?}",
            sharded.len()
        )));
    }
    let sharded_name = sharded[0];
    reject_unsafe_broadcast_shapes(&agg.input, sharded_name)?;
    // The aggregate's input must unparse to a plain `SELECT * FROM …` so we can splice our own
    // SELECT list onto its FROM/WHERE tail without losing column qualifiers.
    let input_sql = Unparser::default()
        .plan_to_sql(&agg.input)
        .map_err(|e| Error::Unsupported(format!("auto-distribute: unparse input: {e}")))?
        .to_string();
    // Unparser emits `SELECT * FROM …` for a single scan, but multi-join inputs can be
    // `SELECT *, * FROM …` (one star per join input). Extract the FROM/WHERE/JOIN tail either way.
    let tail = extract_from_tail(&input_sql)?;
    let tail = sanitize_generated_sql(&tail);

    // Broadcast is only correct if the sharded table is *scanned* exactly once (the driving fact).
    // A second scan — a self-join or a correlated EXISTS/IN subquery over it — would see only the
    // local shard per worker and silently lose cross-shard rows, so reject it. (`base_tables` counts
    // the plan-input scan only; subquery scans live in expressions, so descend into those too.)
    let scans = count_table_scans(&agg.input, sharded_name);
    if scans > 1 {
        return Err(Error::Unsupported(format!(
            "auto-distribute: sharded table `{sharded_name}` scanned {scans}× \
             (self-join / subquery) — not broadcast-safe"
        )));
    }

    let up = Unparser::default();
    let group_sql: Vec<String> = agg
        .group_expr
        .iter()
        .map(|g| expr_sql(&up, g))
        .collect::<Result<_>>()?;

    let aggs = agg
        .aggr_expr
        .iter()
        .map(AggSpec::classify)
        .collect::<Result<Vec<_>>>()?;
    let distinct = aggs.iter().any(|a| a.distinct);

    let remap = build_remap(p);

    let (partial_sql, final_sql) = if distinct {
        distinct_stage_sql(&up, p, &group_sql, &aggs, &tail, &remap)?
    } else {
        recombine_stage_sql(p, &group_sql, &aggs, &tail, &remap)?
    };

    let hash_key_cols: Vec<u32> = (0..group_sql.len() as u32).collect();
    Ok(DistributedQuery {
        stages: vec![
            StageDef::new(0, partial_sql, vec![], hash_key_cols),
            StageDef::new(1, final_sql, vec![0], vec![]),
        ],
        finalize_sql: build_finalize(p)?,
    })
}

/// Ungrouped aggregation: partials per worker, gather to partition 0, recombine.
fn global_aggregation_stages(p: &Peeled<'_>, sharded: &[&str]) -> Result<DistributedQuery> {
    if sharded.len() != 1 {
        return Err(Error::Unsupported(format!(
            "auto-distribute: global aggregation needs exactly one sharded table, found {}",
            sharded.len()
        )));
    }
    let sharded_name = sharded[0];
    if count_table_scans(&p.agg.input, sharded_name) > 1 {
        return Err(Error::Unsupported(format!(
            "auto-distribute: sharded table `{sharded_name}` scanned multiple times"
        )));
    }
    reject_unsafe_broadcast_shapes(&p.agg.input, sharded_name)?;
    let input_sql = Unparser::default()
        .plan_to_sql(&p.agg.input)
        .map_err(|e| Error::Unsupported(format!("auto-distribute: unparse input: {e}")))?
        .to_string();
    let tail = extract_from_tail(&input_sql)?;
    let tail = sanitize_generated_sql(&tail);

    let aggs = p
        .agg
        .aggr_expr
        .iter()
        .map(AggSpec::classify)
        .collect::<Result<Vec<_>>>()?;
    if aggs.iter().any(|a| a.distinct) {
        return Err(Error::Unsupported(
            "auto-distribute: global COUNT(DISTINCT) not yet supported".into(),
        ));
    }

    let remap = build_remap(p);

    let mut psel = Vec::new();
    let mut combine = Vec::new();
    for (i, a) in aggs.iter().enumerate() {
        match a.func.as_str() {
            "sum" => {
                psel.push(format!("sum({}) AS a{i}", a.arg_sql));
                combine.push(format!("sum(a{i}) AS r{i}"));
            }
            "count" => {
                psel.push(format!("count({}) AS a{i}", a.arg_sql));
                combine.push(format!("sum(a{i}) AS r{i}"));
            }
            "min" => {
                psel.push(format!("min({}) AS a{i}", a.arg_sql));
                combine.push(format!("min(a{i}) AS r{i}"));
            }
            "max" => {
                psel.push(format!("max({}) AS a{i}", a.arg_sql));
                combine.push(format!("max(a{i}) AS r{i}"));
            }
            "avg" => {
                psel.push(format!(
                    "sum({}) AS a{i}s, count({}) AS a{i}c",
                    a.arg_sql, a.arg_sql
                ));
                // No cast: SUM/COUNT keep DataFusion's own AVG result type (a DECIMAL average
                // stays DECIMAL at the same scale). Forcing DOUBLE here made TPC-DS Q7/Q26 return
                // numerically-right values at the wrong scale (`120.65` vs `120.650000`).
                combine.push(format!("(sum(a{i}s) / NULLIF(sum(a{i}c), 0)) AS r{i}"));
            }
            other => {
                return Err(Error::Unsupported(format!(
                    "auto-distribute: aggregate `{other}` not supported"
                )))
            }
        }
    }

    let partial_sql = sanitize_generated_sql(&format!("SELECT {} {tail}", psel.join(", ")));
    // HAVING COUNT(*) > 0 drops the all-null row workers with an empty shuffle bucket would emit.
    let inner = format!(
        "SELECT {} FROM shuffle_input HAVING COUNT(*) > 0",
        combine.join(", ")
    );
    let final_sql = wrap_output(p, &inner, &remap)?;
    Ok(DistributedQuery {
        stages: vec![
            StageDef::new(0, partial_sql, vec![], vec![]),
            StageDef::new(1, final_sql, vec![0], vec![]),
        ],
        finalize_sql: build_finalize(p)?,
    })
}

/// Shuffle-join two sharded tables, then run the grouped aggregation.
pub(crate) fn shuffle_join_two_tables(
    p: &Peeled<'_>,
    sharded: &[&str],
) -> Result<DistributedQuery> {
    let join = find_inner_equijoin(&p.agg.input)?;
    let (left_key_expr, right_key_expr) = match join.on.as_slice() {
        [(l, r)] => (l.clone(), r.clone()),
        [] => equijoin_from_filter(join.filter.as_ref())?,
        _ => {
            return Err(Error::Unsupported(format!(
                "auto-distribute: shuffle join supports a single equijoin key, found {}",
                join.on.len()
            )))
        }
    };
    if join.on.len() > 1 {
        return Err(Error::Unsupported(
            "auto-distribute: multi-key shuffle joins not yet supported".into(),
        ));
    }
    // Non-equi residual filter (beyond the single equijoin) is not yet supported.
    if join.filter.is_some() && !join.on.is_empty() {
        return Err(Error::Unsupported(
            "auto-distribute: shuffle join with non-equi filter not yet supported".into(),
        ));
    }

    let left_scan = simple_table_scan(join.left.as_ref())?;
    let right_scan = simple_table_scan(join.right.as_ref())?;
    let left_name = left_scan.table;
    let right_name = right_scan.table;
    if !(sharded.contains(&left_name) && sharded.contains(&right_name)) {
        return Err(Error::Unsupported(
            "auto-distribute: shuffle join sides must be the two sharded tables".into(),
        ));
    }

    let left_key_name = column_name(&left_key_expr)?;
    let right_key_name = column_name(&right_key_expr)?;
    let left_key_idx = column_index_in_scan(&left_scan, &left_key_name)?;
    let right_key_idx = column_index_in_scan(&right_scan, &right_key_name)?;

    let left_alias = left_scan.alias.unwrap_or(left_name);
    let right_alias = right_scan.alias.unwrap_or(right_name);

    let left_sql = match &left_scan.filter_sql {
        Some(f) => format!("SELECT * FROM {left_name} WHERE {f}"),
        None => format!("SELECT * FROM {left_name}"),
    };
    let right_sql = match &right_scan.filter_sql {
        Some(f) => format!("SELECT * FROM {right_name} WHERE {f}"),
        None => format!("SELECT * FROM {right_name}"),
    };

    let up = Unparser::default();
    let group_sql: Vec<String> = p
        .agg
        .group_expr
        .iter()
        .map(|g| expr_sql(&up, g))
        .collect::<Result<_>>()?;
    let aggs = p
        .agg
        .aggr_expr
        .iter()
        .map(AggSpec::classify)
        .collect::<Result<Vec<_>>>()?;

    let remap = build_remap(p);

    let on_sql = format!("{left_alias}.{left_key_name} = {right_alias}.{right_key_name}");
    let join_tail = format!(
        "FROM shuffle_input_0 AS {left_alias} JOIN shuffle_input_1 AS {right_alias} ON {on_sql}"
    );

    let (partial_sql, final_sql) = if aggs.iter().any(|a| a.distinct) {
        distinct_stage_sql(&up, p, &group_sql, &aggs, &join_tail, &remap)?
    } else {
        recombine_stage_sql(p, &group_sql, &aggs, &join_tail, &remap)?
    };

    // Stage 3 has a single upstream, so Flight registers it as `shuffle_input` (not `_2`).
    let hash_group: Vec<u32> = (0..group_sql.len() as u32).collect();
    Ok(DistributedQuery {
        stages: vec![
            StageDef::new(
                0,
                sanitize_generated_sql(&left_sql),
                vec![],
                vec![left_key_idx],
            ),
            StageDef::new(
                1,
                sanitize_generated_sql(&right_sql),
                vec![],
                vec![right_key_idx],
            ),
            StageDef::new(2, partial_sql, vec![0, 1], hash_group),
            StageDef::new(3, final_sql, vec![2], vec![]),
        ],
        finalize_sql: build_finalize(p)?,
    })
}

/// A leaf table scan, optionally filtered, with an optional SQL alias.
pub(crate) struct SimpleScan<'a> {
    pub(crate) table: &'a str,
    pub(crate) alias: Option<&'a str>,
    pub(crate) filter_sql: Option<String>,
    pub(crate) schema: datafusion::common::DFSchemaRef,
}

fn find_inner_equijoin(lp: &LogicalPlan) -> Result<&datafusion::logical_expr::Join> {
    let mut node = lp;
    loop {
        match node {
            LogicalPlan::Projection(p) => node = p.input.as_ref(),
            LogicalPlan::Filter(f) => node = f.input.as_ref(),
            LogicalPlan::Join(j) => {
                use datafusion::logical_expr::JoinType;
                if j.join_type != JoinType::Inner {
                    return Err(Error::Unsupported(
                        "auto-distribute: only INNER shuffle joins are supported".into(),
                    ));
                }
                return Ok(j);
            }
            other => {
                return Err(Error::Unsupported(format!(
                    "auto-distribute: expected a join under aggregate, found `{}`",
                    other.display().to_string().lines().next().unwrap_or("")
                )))
            }
        }
    }
}

pub(crate) fn simple_table_scan(lp: &LogicalPlan) -> Result<SimpleScan<'_>> {
    match lp {
        LogicalPlan::TableScan(s) => Ok(SimpleScan {
            table: s.table_name.table(),
            alias: None,
            filter_sql: None,
            schema: s.projected_schema.clone(),
        }),
        LogicalPlan::SubqueryAlias(sa) => {
            let mut inner = simple_table_scan(sa.input.as_ref())?;
            inner.alias = Some(sa.alias.table());
            Ok(inner)
        }
        LogicalPlan::Filter(f) => {
            let mut inner = simple_table_scan(f.input.as_ref())?;
            let up = Unparser::default();
            let pred = expr_sql(&up, f.predicate.as_ref())?;
            inner.filter_sql = Some(match inner.filter_sql {
                Some(prev) => format!("({prev}) AND ({pred})"),
                None => pred,
            });
            Ok(inner)
        }
        LogicalPlan::Projection(p) => simple_table_scan(p.input.as_ref()),
        other => Err(Error::Unsupported(format!(
            "auto-distribute: shuffle join side must be a table scan, found `{}`",
            other.display().to_string().lines().next().unwrap_or("")
        ))),
    }
}

pub(crate) fn equijoin_from_filter(filter: Option<&Expr>) -> Result<(Expr, Expr)> {
    let Some(Expr::BinaryExpr(b)) = filter else {
        return Err(Error::Unsupported(
            "auto-distribute: shuffle join needs an equijoin key (on or filter)".into(),
        ));
    };
    use datafusion::logical_expr::Operator;
    if b.op != Operator::Eq {
        return Err(Error::Unsupported(
            "auto-distribute: shuffle join filter must be a single equality".into(),
        ));
    }
    Ok((*b.left.clone(), *b.right.clone()))
}

pub(crate) fn column_name(e: &Expr) -> Result<String> {
    match e {
        Expr::Column(c) => Ok(c.name.clone()),
        other => Err(Error::Unsupported(format!(
            "auto-distribute: join key must be a column, found {other}"
        ))),
    }
}

fn column_index_in_scan(scan: &SimpleScan<'_>, name: &str) -> Result<u32> {
    for (i, f) in scan.schema.fields().iter().enumerate() {
        if f.name() == name {
            return Ok(i as u32);
        }
    }
    let needle = name.to_ascii_lowercase();
    for (i, f) in scan.schema.fields().iter().enumerate() {
        if f.name().to_ascii_lowercase() == needle {
            return Ok(i as u32);
        }
    }
    Err(Error::Unsupported(format!(
        "auto-distribute: join key `{name}` not found in table `{}`",
        scan.table
    )))
}

/// Build the global finalize query (`ORDER BY` / `LIMIT` over the gathered `result` table), or
/// `None` when the query has neither. Sort exprs reference output columns; `result` carries those
/// under their unqualified output names, so column refs are unqualified (e.g. `lineitem.l_returnflag`
/// → `l_returnflag`, matching `wrap_output`'s aliasing) before unparsing.
pub(crate) fn build_finalize(p: &Peeled) -> Result<Option<String>> {
    if p.sort.is_none() && p.limit.is_none() {
        return Ok(None);
    }
    let up = Unparser::default();
    let mut sql = String::from("SELECT * FROM result");
    if let Some(sorts) = p.sort {
        let parts = sorts
            .iter()
            .map(|s| {
                let dir = if s.asc { "ASC" } else { "DESC" };
                let nulls = if s.nulls_first {
                    "NULLS FIRST"
                } else {
                    "NULLS LAST"
                };
                Ok(format!(
                    "{} {dir} {nulls}",
                    expr_sql(&up, &unqualify(&s.expr))?
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        if !parts.is_empty() {
            sql.push_str(&format!(" ORDER BY {}", parts.join(", ")));
        }
    }
    if let Some(n) = p.limit {
        sql.push_str(&format!(" LIMIT {n}"));
    }
    Ok(Some(sql))
}

/// One aggregate in the SELECT list, classified for partial/final decomposition.
pub(crate) struct AggSpec {
    /// Lowercased function name (`sum`/`count`/`min`/`max`/`avg`).
    pub(crate) func: String,
    /// SQL of the (single) argument, e.g. `t.v` (or `1` for `count(*)`).
    pub(crate) arg_sql: String,
    /// Whether the aggregate is `DISTINCT`.
    pub(crate) distinct: bool,
}

impl AggSpec {
    pub(crate) fn classify(e: &Expr) -> Result<AggSpec> {
        // An aggregate written `sum(x) AS total` arrives wrapped in an alias.
        let e = match e {
            Expr::Alias(a) => a.expr.as_ref(),
            other => other,
        };
        let Expr::AggregateFunction(af) = e else {
            return Err(Error::Unsupported(format!(
                "auto-distribute: non-aggregate in aggregate list: {e}"
            )));
        };
        let func = af.func.name().to_ascii_lowercase();
        let up = Unparser::default();
        let arg_sql = match af.params.args.first() {
            Some(a) => expr_sql(&up, a)?,
            None => "1".to_string(), // count(*) carries no arg
        };
        Ok(AggSpec {
            func,
            arg_sql,
            distinct: af.params.distinct,
        })
    }
}

/// Re-combinable path (no DISTINCT): partial aggregates per worker, final recombines.
pub(crate) fn recombine_stage_sql(
    p: &Peeled,
    group_sql: &[String],
    aggs: &[AggSpec],
    tail: &str,
    remap: &HashMap<String, String>,
) -> Result<(String, String)> {
    // Partial SELECT list: group cols as g{j}, then per-aggregate partial state.
    let mut psel: Vec<String> = group_sql
        .iter()
        .enumerate()
        .map(|(j, g)| format!("{g} AS g{j}"))
        .collect();
    // Final combine SELECT list (over `shuffle_input`): g{j} group cols + recombined aggregates.
    let mut combine: Vec<String> = (0..group_sql.len()).map(|j| format!("g{j}")).collect();

    for (i, a) in aggs.iter().enumerate() {
        match a.func.as_str() {
            "sum" => {
                psel.push(format!("sum({}) AS a{i}", a.arg_sql));
                combine.push(format!("sum(a{i}) AS r{i}"));
            }
            "count" => {
                psel.push(format!("count({}) AS a{i}", a.arg_sql));
                combine.push(format!("sum(a{i}) AS r{i}")); // counts recombine by summing
            }
            "min" => {
                psel.push(format!("min({}) AS a{i}", a.arg_sql));
                combine.push(format!("min(a{i}) AS r{i}"));
            }
            "max" => {
                psel.push(format!("max({}) AS a{i}", a.arg_sql));
                combine.push(format!("max(a{i}) AS r{i}"));
            }
            "avg" => {
                psel.push(format!(
                    "sum({}) AS a{i}s, count({}) AS a{i}c",
                    a.arg_sql, a.arg_sql
                ));
                // No cast: SUM/COUNT keep DataFusion's own AVG result type (a DECIMAL average
                // stays DECIMAL at the same scale). Forcing DOUBLE here made TPC-DS Q7/Q26 return
                // numerically-right values at the wrong scale (`120.65` vs `120.650000`).
                combine.push(format!("(sum(a{i}s) / NULLIF(sum(a{i}c), 0)) AS r{i}"));
            }
            other => {
                return Err(Error::Unsupported(format!(
                    "auto-distribute: aggregate `{other}` not supported"
                )))
            }
        }
    }

    let group_by = group_sql.join(", ");
    let partial_sql = sanitize_generated_sql(&format!(
        "SELECT {} {tail} GROUP BY {group_by}",
        psel.join(", ")
    ));
    let inner = format!(
        "SELECT {} FROM shuffle_input GROUP BY {}",
        combine.join(", "),
        (0..group_sql.len())
            .map(|j| format!("g{j}"))
            .collect::<Vec<_>>()
            .join(", ")
    );
    let final_sql = wrap_output(p, &inner, remap)?;
    Ok((partial_sql, final_sql))
}

/// DISTINCT path: shuffle the raw grouping + argument columns by group key, run the original
/// aggregate in the final stage (exact, since each group is co-located on one worker).
pub(crate) fn distinct_stage_sql(
    _up: &Unparser,
    p: &Peeled,
    group_sql: &[String],
    aggs: &[AggSpec],
    tail: &str,
    remap: &HashMap<String, String>,
) -> Result<(String, String)> {
    // Partial: project group cols (g{j}) and each aggregate's argument column (c{i}); no aggregation.
    let mut psel: Vec<String> = group_sql
        .iter()
        .enumerate()
        .map(|(j, g)| format!("{g} AS g{j}"))
        .collect();
    for (i, a) in aggs.iter().enumerate() {
        psel.push(format!("{} AS c{i}", a.arg_sql));
    }
    let partial_sql = sanitize_generated_sql(&format!("SELECT {} {tail}", psel.join(", ")));

    // Final: re-run each aggregate over the projected columns, grouped by g{j}.
    let mut combine: Vec<String> = (0..group_sql.len()).map(|j| format!("g{j}")).collect();
    for (i, a) in aggs.iter().enumerate() {
        let d = if a.distinct { "DISTINCT " } else { "" };
        combine.push(format!("{}({d}c{i}) AS r{i}", a.func));
    }
    let inner = format!(
        "SELECT {} FROM shuffle_input GROUP BY {}",
        combine.join(", "),
        (0..group_sql.len())
            .map(|j| format!("g{j}"))
            .collect::<Vec<_>>()
            .join(", ")
    );
    let final_sql = wrap_output(p, &inner, remap)?;
    Ok((partial_sql, final_sql))
}

/// Map the aggregate's output column names to the safe stage names (`g{j}` group, `r{i}` result).
///
/// Keyed three ways, because callers reach these columns under different names: the expression's
/// `schema_name` (how the plan refers to it), an explicit `AS` alias on the group/aggregate expr,
/// and the `Aggregate`'s own schema field names.
pub(crate) fn build_agg_remap(agg: &Aggregate) -> HashMap<String, String> {
    let mut remap: HashMap<String, String> = HashMap::new();
    for (j, g) in agg.group_expr.iter().enumerate() {
        remap.insert(g.schema_name().to_string(), format!("g{j}"));
        if let Expr::Alias(a) = g {
            remap.insert(a.name.clone(), format!("g{j}"));
        }
    }
    for (i, a) in agg.aggr_expr.iter().enumerate() {
        remap.insert(a.schema_name().to_string(), format!("r{i}"));
        if let Expr::Alias(al) = a {
            remap.insert(al.name.clone(), format!("r{i}"));
        }
    }
    let n_group = agg.group_expr.len();
    for (j, field) in agg.schema.fields().iter().take(n_group).enumerate() {
        remap.insert(field.name().clone(), format!("g{j}"));
    }
    for (i, field) in agg.schema.fields().iter().skip(n_group).enumerate() {
        remap.insert(field.name().clone(), format!("r{i}"));
    }
    remap
}

/// [`build_agg_remap`] extended with [`Peeled::alias_projections`], so a `HAVING` written against
/// an intervening subquery's aliases (TPC-DS Q21's `inv_before`) still resolves to `r{i}` / `g{j}`.
pub(crate) fn build_remap(p: &Peeled<'_>) -> HashMap<String, String> {
    let mut remap = build_agg_remap(p.agg);
    for proj in &p.alias_projections {
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

/// Wrap the combined inner query so the final stage's output matches the original query's columns:
/// re-apply the output projection with aggregate/group columns remapped to `r{i}`/`g{j}`, each
/// item explicitly aliased back to its original output name (so a bare `t.k` stays column `k`, and
/// downstream `ORDER BY` over those names resolves). `ORDER BY` / `LIMIT` are *not* applied here —
/// they're global and run in [`build_finalize`].
fn wrap_output(p: &Peeled<'_>, inner: &str, remap: &HashMap<String, String>) -> Result<String> {
    let up = Unparser::default();
    // Apply HAVING against remapped `g{j}`/`r{i}` columns *before* the output projection aliases
    // them back to original names (otherwise `WHERE r0 > …` fails against `having_in.sv`).
    let from_sql = if p.having.is_empty() {
        format!("({inner}) AS combined")
    } else {
        let mut preds = Vec::with_capacity(p.having.len());
        for pred in &p.having {
            let mapped = remap_columns(&unqualify(pred), remap);
            ensure_all_columns_remapped(&mapped)?;
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
                let sql = expr_sql(&up, &remap_columns(strip_alias(e), remap))?;
                Ok(format!("{sql} AS \"{name}\""))
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

/// The expr without its top-level alias (so we can re-alias after remapping).
fn strip_alias(e: &Expr) -> &Expr {
    match e {
        Expr::Alias(a) => &a.expr,
        other => other,
    }
}

/// Drop the table qualifier from every column reference (e.g. `lineitem.l_returnflag` →
/// `l_returnflag`), so a sort over the gathered `result` table resolves against its unqualified
/// output column names.
pub(crate) fn unqualify(e: &Expr) -> Expr {
    use datafusion::common::tree_node::{Transformed, TreeNode};
    e.clone()
        .transform(|node| {
            if let Expr::Column(c) = &node {
                return Ok(Transformed::yes(datafusion::prelude::col(c.name.clone())));
            }
            Ok(Transformed::no(node))
        })
        .map(|t| t.data)
        .unwrap_or(e.clone())
}

/// Replace any column reference whose flat name is in `remap` with the safe-named column.
fn remap_columns(e: &Expr, remap: &HashMap<String, String>) -> Expr {
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

/// Require every column in an already-remapped predicate to name a `g{j}` / `r{i}` stage column.
///
/// Anything left un-remapped refers to a name that only existed in the original plan, so the
/// predicate would either fail on the worker or — worse, if the name happens to collide — filter
/// on the wrong column. Decline the query instead.
fn ensure_all_columns_remapped(e: &Expr) -> Result<()> {
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
    let mut bad: Option<String> = None;
    let _ = e.apply(|node| {
        if let Expr::Column(c) = node {
            let safe = c.relation.is_none()
                && matches!(c.name.as_bytes(), [b'g' | b'r', rest @ ..]
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
            "auto-distribute: HAVING references `{name}`, which does not map to an aggregate or \
             group output column"
        ))),
        None => Ok(()),
    }
}

/// Unparse an expr to SQL text.
pub(crate) fn expr_sql(up: &Unparser, e: &Expr) -> Result<String> {
    up.expr_to_sql(e)
        .map(|ast| sanitize_generated_sql(&ast.to_string()))
        .map_err(|err| Error::Unsupported(format!("auto-distribute: unparse expr: {err}")))
}

/// Extract the `FROM …` tail from an unparsed aggregate input.
///
/// DataFusion's unparser yields `SELECT * FROM …` for a plain scan, but a join of N inputs can
/// become `SELECT *, *, … FROM …`. We only need the FROM/JOIN/WHERE suffix to splice a new SELECT.
pub(crate) fn extract_from_tail(input_sql: &str) -> Result<String> {
    if let Some(rest) = input_sql.strip_prefix("SELECT * ") {
        // Only accept when the remainder starts with FROM (not `*, * FROM`).
        if rest.starts_with("FROM ") || rest.starts_with("from ") {
            return Ok(rest.to_string());
        }
    }
    let bytes = input_sql.as_bytes();
    let upper = input_sql.to_ascii_uppercase();
    let mut depth = 0i32;
    let mut i = 0;
    while i + 6 <= upper.len() {
        match bytes[i] {
            b'(' => depth += 1,
            b')' => depth = depth.saturating_sub(1),
            _ if depth == 0 && upper[i..].starts_with(" FROM ") => {
                return Ok(input_sql[i + 1..].to_string()); // "FROM …"
            }
            _ => {}
        }
        i += 1;
    }
    Err(Error::Unsupported(
        "auto-distribute: non-trivial aggregate input (no FROM tail)".into(),
    ))
}

/// Reject plan shapes where broadcasting the replicated tables to every worker duplicates output
/// rows instead of partitioning them.
///
/// The single-sharded-table broadcast model is correct when every output row is produced by
/// matching against the (partitioned) sharded table, which a plain inner-join chain guarantees.
/// Two shapes break that invariant, and both go wrong silently — the query returns a number that
/// is a multiple of the right one:
///
/// - a `UNION ALL` arm with no path to the sharded table. TPC-DS Q33/Q56/Q60/Q66/Q71/Q76 union one
///   pre-aggregated arm per channel (`store_sales` / `catalog_sales` / `web_sales`). With
///   `store_sales` sharded, the other two arms scan replicated tables only, so every worker
///   computes those arms in full and the final `SUM` multiplies them by the worker count — Q66 at
///   two workers returns exactly 2× the correct total for the affected columns.
/// - an outer join whose preserved side does not reach the sharded table. TPC-DS Q97 `FULL OUTER
///   JOIN`s two independently-aggregated fact tables; the side without the sharded table is
///   replicated, so its unmatched rows (and under `FULL`, all of its rows) survive once per worker
///   rather than once overall.
///
/// A subtree that never scans the sharded table is uniformly replicated and harmless on its own —
/// it only becomes a duplication bug where a parent combines it additively with sharded data, and
/// that parent is the node this catches. So skip such subtrees rather than flagging them, and stop
/// at a nested `Aggregate`: below one, the replicated subtree's result is identical and complete on
/// every worker, which is what lets TPC-DS Q54's `UNION ALL` of two non-sharded facts feed a
/// `DISTINCT` customer filter safely.
fn reject_unsafe_broadcast_shapes(lp: &LogicalPlan, sharded_name: &str) -> Result<()> {
    if count_table_scans(lp, sharded_name) == 0 {
        return Ok(());
    }
    match lp {
        LogicalPlan::Aggregate(_) => Ok(()),
        LogicalPlan::Union(u) => {
            for arm in &u.inputs {
                if count_table_scans(arm, sharded_name) == 0 {
                    return Err(Error::Unsupported(format!(
                        "auto-distribute: UNION ALL arm does not scan sharded table \
                         `{sharded_name}` — broadcasting it would repeat that arm's rows on \
                         every worker"
                    )));
                }
                reject_unsafe_broadcast_shapes(arm, sharded_name)?;
            }
            Ok(())
        }
        LogicalPlan::Join(j) => {
            match j.join_type {
                JoinType::Full => {
                    return Err(Error::Unsupported(
                        "auto-distribute: FULL OUTER JOIN is not broadcast-safe with a single \
                         sharded table"
                            .into(),
                    ));
                }
                JoinType::Left | JoinType::LeftSemi | JoinType::LeftAnti | JoinType::LeftMark
                    if count_table_scans(&j.left, sharded_name) == 0 =>
                {
                    return Err(Error::Unsupported(format!(
                        "auto-distribute: LEFT join's preserved side does not scan sharded table \
                         `{sharded_name}` — its unmatched rows would repeat on every worker"
                    )));
                }
                JoinType::Right | JoinType::RightSemi | JoinType::RightAnti
                    if count_table_scans(&j.right, sharded_name) == 0 =>
                {
                    return Err(Error::Unsupported(format!(
                        "auto-distribute: RIGHT join's preserved side does not scan sharded table \
                         `{sharded_name}` — its unmatched rows would repeat on every worker"
                    )));
                }
                _ => {}
            }
            reject_unsafe_broadcast_shapes(&j.left, sharded_name)?;
            reject_unsafe_broadcast_shapes(&j.right, sharded_name)
        }
        other => {
            for c in other.inputs() {
                reject_unsafe_broadcast_shapes(c, sharded_name)?;
            }
            Ok(())
        }
    }
}

/// Reject stage SQL that references the Unparser's `left` / `right` join-side alias from outside
/// the lexical scope that defined it.
///
/// DataFusion's Unparser names a decorrelated subquery's join sides `"left"` / `"right"`. When
/// that side is *also* wrapped in another alias one level out — TPC-DS Q8/Q38/Q87's chained
/// `EXISTS`, which unparses to `(SELECT … FROM (…) AS "left" WHERE EXISTS (…)) AS hot_cust WHERE
/// EXISTS (… `left`.c_last_name …)` — the trailing reference sits in a sibling scope where
/// `left` was never bound, and the row it means is only reachable through the outer alias the
/// Unparser failed to substitute. The SQL parses fine; it fails at name resolution on the worker
/// (`No field named left.c_last_name`). Renaming the alias uniformly would not help, since the
/// reference is dangling rather than merely awkwardly quoted, so reject and fall back.
///
/// Scope is tracked as a stack of paren depths: `AS "left"` binds `left` in the frame that is open
/// where the alias appears, and stays visible to every nested subquery (which is what makes the
/// legitimate correlation work) until that frame's paren closes.
pub(crate) fn reject_out_of_scope_join_alias_refs(sql: &str) -> Result<()> {
    const DEFS: [(&str, &str); 4] = [
        ("\"left\"", "left"),
        ("\"right\"", "right"),
        ("`left`", "left"),
        ("`right`", "right"),
    ];
    const USES: [(&str, &str); 4] = [
        ("\"left\".", "left"),
        ("\"right\".", "right"),
        ("`left`.", "left"),
        ("`right`.", "right"),
    ];
    let bytes = sql.as_bytes();
    let mut stack: Vec<Vec<&str>> = vec![Vec::new()];
    let mut i = 0usize;
    while i < bytes.len() {
        match bytes[i] {
            b'(' => {
                stack.push(Vec::new());
                i += 1;
            }
            b')' => {
                if stack.len() > 1 {
                    stack.pop();
                }
                i += 1;
            }
            _ => {
                if let Some((pat, name)) = USES.iter().find(|(pat, _)| sql[i..].starts_with(*pat)) {
                    if !stack.iter().any(|frame| frame.contains(name)) {
                        return Err(Error::Unsupported(format!(
                            "auto-distribute: generated SQL references join-side alias `{name}` \
                             outside the scope that defines it (Unparser aliasing)"
                        )));
                    }
                    i += pat.len();
                    continue;
                }
                if let Some((pat, name)) = DEFS.iter().find(|(pat, _)| sql[i..].starts_with(*pat)) {
                    stack.last_mut().expect("stack never empty").push(name);
                    i += pat.len();
                    continue;
                }
                i += 1;
            }
        }
    }
    Ok(())
}

/// Reject `ROLLUP` / `CUBE` / `GROUPING SETS`.
///
/// The partial/final split treats `group_expr` positionally: each entry becomes one `g{j}` column
/// that the final stage re-groups on. A grouping set is a *single* `group_expr` entry that expands
/// to several grouping levels, so `g0` would stand for the whole `ROLLUP(...)` construct — the
/// remap can't name `channel`/`id` individually, and the emitted stage SQL passes `ROLLUP (...)`
/// through to workers, which re-parse it under the Databricks dialect as a call to a function
/// named `rollup` (TPC-DS Q5/Q18/Q22/Q77/Q80). Decline and let the query run single-node.
fn reject_grouping_sets(group_expr: &[Expr]) -> Result<()> {
    for g in group_expr {
        if matches!(g, Expr::GroupingSet(_)) {
            return Err(Error::Unsupported(
                "auto-distribute: ROLLUP / CUBE / GROUPING SETS are not supported".into(),
            ));
        }
    }
    Ok(())
}

/// Fix SQL fragments from DataFusion's Unparser that the Databricks-dialect re-parser rejects.
///
/// Two common failure modes when generated stage SQL is sent to workers:
/// - `alias."col"` — dot access with a double-quoted column name;
/// - `"table".col` — dot access on a double-quoted table name (e.g. reserved `part`).
pub(crate) fn sanitize_generated_sql(sql: &str) -> String {
    fix_interval_pg_style(&fix_quoted_column_after_dot(&fix_quoted_table_dot_access(
        sql,
    )))
}

/// DataFusion's Unparser emits Postgres-style combined interval literals
/// (`INTERVAL '12 MONS'`, `INTERVAL '90 DAYS'`). Workers re-parse under the Databricks dialect,
/// which requires a unit *after* the quoted value (`INTERVAL '12' MONTH`). Rewrite the combined
/// form so stage SQL round-trips. Case-insensitive on the keyword and unit abbreviation.
fn fix_interval_pg_style(sql: &str) -> String {
    let b = sql.as_bytes();
    let n = b.len();
    let mut out = String::with_capacity(n);
    let mut i = 0;
    while i < n {
        // Skip quoted strings so we never rewrite interval-looking content inside literals.
        if b[i] == b'\'' || b[i] == b'"' {
            let quote = b[i];
            let start = i;
            i += 1;
            while i < n {
                if b[i] == quote {
                    if i + 1 < n && b[i + 1] == quote {
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                i += 1;
            }
            out.push_str(&sql[start..i]);
            continue;
        }

        if interval_kw_at(b, i) {
            let after_kw = i + 8;
            let mut j = after_kw;
            while j < n && b[j].is_ascii_whitespace() {
                j += 1;
            }
            if j < n && b[j] == b'\'' {
                let lit_open = j;
                j += 1;
                let lit_body = j;
                while j < n && b[j] != b'\'' {
                    j += 1;
                }
                if j < n {
                    let body = &sql[lit_body..j];
                    if let Some((num, unit)) = split_pg_interval_body(body) {
                        out.push_str(&sql[i..lit_open]);
                        out.push('\'');
                        out.push_str(num);
                        out.push('\'');
                        out.push(' ');
                        out.push_str(unit);
                        i = j + 1; // past closing quote
                        continue;
                    }
                }
            }
        }

        out.push(b[i] as char);
        i += 1;
    }
    out
}

fn interval_kw_at(b: &[u8], i: usize) -> bool {
    const KW: &[u8] = b"interval";
    if i + KW.len() > b.len() {
        return false;
    }
    if i > 0 {
        let prev = b[i - 1];
        if prev.is_ascii_alphanumeric() || prev == b'_' {
            return false;
        }
    }
    if !b[i..i + KW.len()].eq_ignore_ascii_case(KW) {
        return false;
    }
    let after = i + KW.len();
    if after < b.len() {
        let next = b[after];
        if next.is_ascii_alphanumeric() || next == b'_' {
            return false;
        }
    }
    true
}

/// Split `"12 MONS"` / `"-90 DAYS"` into (`"-90"`, `"DAY"`). Returns `None` when the body is already
/// a bare numeric literal (no unit inside the quotes).
fn split_pg_interval_body(body: &str) -> Option<(&str, &'static str)> {
    let body = body.trim();
    let mut parts = body.split_whitespace();
    let num = parts.next()?;
    let unit_raw = parts.next()?;
    if parts.next().is_some() {
        return None; // multi-unit combined forms — leave alone
    }
    // Number may be signed / decimal; require it to look numeric.
    if !num
        .bytes()
        .enumerate()
        .all(|(i, c)| c.is_ascii_digit() || ((c == b'+' || c == b'-') && i == 0) || c == b'.')
    {
        return None;
    }
    let unit = match unit_raw.to_ascii_uppercase().as_str() {
        "YEAR" | "YEARS" | "YR" | "YRS" => "YEAR",
        "MONTH" | "MONTHS" | "MON" | "MONS" => "MONTH",
        "DAY" | "DAYS" | "D" => "DAY",
        "HOUR" | "HOURS" | "HR" | "HRS" => "HOUR",
        "MINUTE" | "MINUTES" | "MIN" | "MINS" => "MINUTE",
        "SECOND" | "SECONDS" | "SEC" | "SECS" => "SECOND",
        _ => return None,
    };
    Some((num, unit))
}

/// `"table".col` → `` `table`.col `` so dot access parses under the Databricks dialect.
fn fix_quoted_table_dot_access(sql: &str) -> String {
    let mut out = String::with_capacity(sql.len());
    let mut i = 0;
    let bytes = sql.as_bytes();
    while i < bytes.len() {
        if bytes[i] == b'"' {
            let start = i;
            i += 1;
            while i < bytes.len() && bytes[i] != b'"' {
                i += 1;
            }
            if i < bytes.len() {
                let ident = &sql[start + 1..i];
                i += 1; // closing quote
                if i < bytes.len() && bytes[i] == b'.' && is_simple_ident(ident) {
                    out.push('`');
                    out.push_str(ident);
                    out.push('`');
                    out.push('.');
                    i += 1;
                    continue;
                }
                out.push_str(&sql[start..i]);
                continue;
            }
        }
        out.push(bytes[i] as char);
        i += 1;
    }
    out
}

/// `alias."col"` → `alias.col` when `col` is a plain identifier.
fn fix_quoted_column_after_dot(sql: &str) -> String {
    let mut out = String::with_capacity(sql.len());
    let mut i = 0;
    let bytes = sql.as_bytes();
    while i < bytes.len() {
        let start = i;
        if bytes[i].is_ascii_alphabetic() || bytes[i] == b'_' {
            while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                i += 1;
            }
            if i < bytes.len() && bytes[i] == b'.' && i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                let qstart = i + 2;
                let mut j = qstart;
                while j < bytes.len() && bytes[j] != b'"' {
                    j += 1;
                }
                if j < bytes.len() {
                    let ident = &sql[qstart..j];
                    if is_simple_ident(ident) {
                        out.push_str(&sql[start..=i]);
                        out.push_str(ident);
                        i = j + 1;
                        continue;
                    }
                }
            }
            out.push_str(&sql[start..i]);
            continue;
        }
        out.push(bytes[i] as char);
        i += 1;
    }
    out
}

fn is_simple_ident(s: &str) -> bool {
    let mut chars = s.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Extract a non-negative integer `LIMIT` value from a literal scalar.
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

/// Count scans of table `name` anywhere in `lp` — across plan inputs **and** subquery plans nested
/// in expressions (EXISTS / IN / scalar subqueries), so a correlated subquery over the table counts.
pub(crate) fn count_table_scans(lp: &LogicalPlan, name: &str) -> usize {
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
    let mut n = match lp {
        LogicalPlan::TableScan(s) if s.table_name.table() == name => 1,
        _ => 0,
    };
    for c in lp.inputs() {
        n += count_table_scans(c, name);
    }
    for e in lp.expressions() {
        let _ = e.apply(|node| {
            let sub = match node {
                Expr::Exists(ex) => Some(&ex.subquery.subquery),
                Expr::InSubquery(iq) => Some(&iq.subquery.subquery),
                Expr::ScalarSubquery(sq) => Some(&sq.subquery),
                _ => None,
            };
            if let Some(plan) = sub {
                n += count_table_scans(plan, name);
            }
            Ok(TreeNodeRecursion::Continue)
        });
    }
    n
}

/// Collect the base (scanned) table names referenced anywhere in `lp`.
pub(crate) fn base_tables(lp: &LogicalPlan) -> Vec<String> {
    let mut out = Vec::new();
    collect_tables(lp, &mut out);
    out
}

fn collect_tables(lp: &LogicalPlan, out: &mut Vec<String>) {
    if let LogicalPlan::TableScan(s) = lp {
        out.push(s.table_name.table().to_string());
    }
    for c in lp.inputs() {
        collect_tables(c, out);
    }
}

#[cfg(test)]
mod guard_tests {
    use super::reject_out_of_scope_join_alias_refs;

    #[test]
    fn plain_sql_without_join_side_aliases_is_accepted() {
        assert!(reject_out_of_scope_join_alias_refs(
            "SELECT a, sum(b) FROM t WHERE c > 1 GROUP BY a"
        )
        .is_ok());
    }

    #[test]
    fn correlated_reference_into_a_nested_subquery_is_accepted() {
        // `left` is bound one level out and used inside the EXISTS — the legitimate shape.
        let sql = r#"SELECT count(1) FROM (SELECT * FROM t) AS "left" WHERE EXISTS (SELECT 1 FROM u WHERE (`left`.k = u.k))"#;
        assert!(reject_out_of_scope_join_alias_refs(sql).is_ok());
    }

    #[test]
    fn reference_after_the_defining_scope_closed_is_rejected() {
        // TPC-DS Q38/Q87 shape: `left` is bound inside the parens that `AS hot_cust` closes, so
        // the second EXISTS refers to a name that no longer exists.
        let sql = r#"SELECT count(1) FROM (SELECT * FROM (SELECT * FROM t) AS "left" WHERE EXISTS (SELECT 1 FROM u WHERE (`left`.k = u.k))) AS hot_cust WHERE EXISTS (SELECT 1 FROM v WHERE (`left`.k = v.k))"#;
        let err = reject_out_of_scope_join_alias_refs(sql).expect_err("dangling `left`");
        assert!(err.to_string().contains("outside the scope"), "{err}");
    }

    #[test]
    fn reference_with_no_definition_at_all_is_rejected() {
        assert!(reject_out_of_scope_join_alias_refs(r#"SELECT "left".a FROM t"#).is_err());
    }

    #[test]
    fn a_sibling_scopes_definition_does_not_leak() {
        let sql = r#"SELECT * FROM (SELECT 1 FROM x AS "left" WHERE `left`.a = 1), (SELECT `left`.b FROM y)"#;
        assert!(reject_out_of_scope_join_alias_refs(sql).is_err());
    }
}

#[cfg(test)]
mod sanitize_tests {
    use super::{fix_quoted_column_after_dot, fix_quoted_table_dot_access, sanitize_generated_sql};

    #[test]
    fn quoted_column_after_dot_becomes_unquoted() {
        let sql = r#"sum(shipping."volume")"#;
        assert_eq!(fix_quoted_column_after_dot(sql), "sum(shipping.volume)");
    }

    #[test]
    fn quoted_table_dot_access_uses_backticks() {
        let sql = r#""part".p_partkey = lineitem.l_partkey"#;
        assert_eq!(
            fix_quoted_table_dot_access(sql),
            "`part`.p_partkey = lineitem.l_partkey"
        );
    }

    #[test]
    fn sanitize_composes_both_fixes() {
        let sql = r#"SELECT sum(shipping."volume") FROM "part" WHERE "part".p_partkey = 1"#;
        let got = sanitize_generated_sql(sql);
        assert!(got.contains("shipping.volume"));
        assert!(got.contains("`part`.p_partkey"));
        assert!(!got.contains(r#""volume""#));
    }

    #[test]
    fn sanitize_rewrites_pg_style_interval_literals() {
        // Unparser form that broke TPC-H Q6 distributed stage SQL under Databricks dialect.
        assert_eq!(
            sanitize_generated_sql(
                "SELECT * FROM t WHERE d < (CAST('1994-01-01' AS DATE) + INTERVAL '12 MONS')"
            ),
            "SELECT * FROM t WHERE d < (CAST('1994-01-01' AS DATE) + INTERVAL '12' MONTH)"
        );
        assert_eq!(
            sanitize_generated_sql("x + INTERVAL '90 DAYS'"),
            "x + INTERVAL '90' DAY"
        );
        // Already-legal form is left alone.
        assert_eq!(
            sanitize_generated_sql("x + INTERVAL '1' YEAR"),
            "x + INTERVAL '1' YEAR"
        );
        // Content inside string literals is not rewritten.
        assert_eq!(
            sanitize_generated_sql("SELECT 'INTERVAL ''12 MONS''' AS s"),
            "SELECT 'INTERVAL ''12 MONS''' AS s"
        );
    }

    #[test]
    fn sanitize_rewrites_signed_and_abbreviated_interval_units() {
        assert_eq!(
            sanitize_generated_sql("x - INTERVAL '-90 DAYS'"),
            "x - INTERVAL '-90' DAY"
        );
        assert_eq!(
            sanitize_generated_sql("x + Interval '2 YR'"),
            "x + Interval '2' YEAR"
        );
        assert_eq!(
            sanitize_generated_sql("x + INTERVAL '3 MON'"),
            "x + INTERVAL '3' MONTH"
        );
        assert_eq!(
            sanitize_generated_sql("x + INTERVAL '4 HRS'"),
            "x + INTERVAL '4' HOUR"
        );
        assert_eq!(
            sanitize_generated_sql("x + INTERVAL '5 MINS'"),
            "x + INTERVAL '5' MINUTE"
        );
        assert_eq!(
            sanitize_generated_sql("x + INTERVAL '6 SECS'"),
            "x + INTERVAL '6' SECOND"
        );
    }

    #[test]
    fn sanitize_leaves_multi_unit_pg_interval_bodies_alone() {
        // Multi-unit combined forms are not safely rewritable — leave the Unparser output as-is.
        assert_eq!(
            sanitize_generated_sql("x + INTERVAL '1 YEAR 2 MONS'"),
            "x + INTERVAL '1 YEAR 2 MONS'"
        );
    }
}

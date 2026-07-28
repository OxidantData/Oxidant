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
use std::sync::Arc;

use datafusion::common::TableReference;
use datafusion::logical_expr::{Aggregate, Expr, GroupingSet, JoinType, LogicalPlan, Union};
use datafusion::sql::unparser::Unparser;
use weft_common::{Error, Result};
use weft_loom::Engine;

use super::shape_extensions::{
    ensure_subquery_tables_replicated, reject_explicit_unsupported, try_materialize_complex_fact,
    try_materialize_subquery_fact, try_non_aggregate, try_union_all, try_window,
};
use crate::driver::{ExchangeMode, StageDef};

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
    let primary: Result<DistributedQuery> = (|| {
        if let Some(dq) = try_materialize_subquery_fact(lp, replicated)? {
            return Ok(dq);
        }
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
        let mut dq = match peel(lp) {
            Ok(peeled) => aggregation_stages_for(&peeled, replicated),
            Err(linear_error) => match super::dag_splitter::try_branch_dag(lp, replicated)? {
                Some(dq) => Ok(dq),
                None => Err(linear_error),
            },
        }?;
        validate_stage_sql(&mut dq)?;
        Ok(dq)
    })();

    match primary {
        Ok(dq) => Ok(dq),
        Err(primary_error) => {
            let reason = primary_error.to_string();
            let materializable_rejection = reason.contains("scanned multiple times")
                || reason.contains("scanned 2×")
                || reason.contains("scanned 3×")
                || reason.contains("scanned 4×")
                || reason.contains("scanned 5×")
                || reason.contains("scanned 6×")
                || reason.contains("scanned 7×")
                || reason.contains("FULL OUTER JOIN is not broadcast-safe")
                || reason.contains("shuffle join needs an equijoin key")
                || reason.contains("arm 0 is not a distributable aggregation")
                || reason.contains("unsupported top-level plan node")
                || reason.contains("Cross Join")
                || reason.contains("window over an aggregation")
                || reason.contains("window function")
                || reason.contains("global aggregation over DISTINCT")
                || reason.contains("COUNT(DISTINCT)")
                || reason.contains("UNION ALL arm does not scan sharded table")
                || reason.contains("branch-aware CrossJoin")
                || reason.contains("expected left-deep equijoin chain");
            if !materializable_rejection {
                return Err(primary_error);
            }
            match try_materialize_complex_fact(lp, replicated) {
                Ok(Some(mut dq)) => {
                    validate_stage_sql(&mut dq)?;
                    Ok(dq)
                }
                Ok(None) => Err(primary_error),
                Err(gather_err) => Err(gather_err),
            }
        }
    }
}

/// Last-line check on the SQL every stage will hand to a worker.
///
/// Individual shape handlers each splice Unparser output into their own stage SQL, so a
/// generated-SQL defect has to be caught in each of them or in one place after the fact. This is
/// that one place — it runs on whatever the chosen path produced.
///
/// Before rejecting, tries [`rewrite_out_of_scope_join_alias_refs`] on each stage's SQL / the
/// finalize SQL in place: most dangling `left`/`right` join-side references are just the
/// Unparser failing to substitute an outer alias it already emitted, so they can be patched up
/// rather than falling back to single-node execution. The reject call after the rewrite is a
/// safety net for shapes the rewrite can't fix (no definition at all, sibling-scope leaks).
fn validate_stage_sql(dq: &mut DistributedQuery) -> Result<()> {
    for s in dq.stages.iter_mut() {
        s.sql = rewrite_out_of_scope_join_alias_refs(&s.sql)?;
        reject_out_of_scope_join_alias_refs(&s.sql)?;
    }
    if let Some(f) = dq.finalize_sql.take() {
        let rewritten = rewrite_out_of_scope_join_alias_refs(&f)?;
        reject_out_of_scope_join_alias_refs(&rewritten)?;
        dq.finalize_sql = Some(rewritten);
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
    if let Some(dq) = try_split_broadcast_union(p, sharded_name)? {
        return Ok(dq);
    }
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
    // A DataFusion grouping set occupies one `group_expr` slot but represents several output
    // columns. Partial aggregation must use the union of those columns as its finest grouping
    // level; the final stage reconstructs the requested ROLLUP/CUBE/GROUPING SETS levels.
    let group_sql: Vec<String> = flattened_group_exprs(&agg.group_expr)
        .into_iter()
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

    // Coarser grouping-set levels span multiple finest-level keys. Hashing by all `g{j}` columns
    // would therefore split (for example) a ROLLUP grand total across every worker. Gather the
    // already-compressed finest-level partials to one partition for the final grouping set.
    let hash_key_cols: Vec<u32> = if is_grouping_set(&agg.group_expr) {
        vec![]
    } else {
        (0..group_sql.len() as u32).collect()
    };
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
    // Per-shard DISTINCT + partial COUNT/SUM, then combine, double-counts keys that land on
    // more than one worker (TPC-DS Q87: 496 vs 494). Needs a shuffle-by-distinct-key stage
    // before the global aggregate; until then decline.
    if plan_contains_distinct(&p.agg.input) {
        return Err(Error::Unsupported(
            "auto-distribute: global aggregation over DISTINCT of a sharded table is not \
             supported (per-shard DISTINCT would double-count cross-shard keys)"
                .into(),
        ));
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
        let (sel, comb) = partial_combine_sql(&a.func, i, &a.arg_sql)?;
        psel.extend(sel);
        combine.push(comb);
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
    let (left_key_expr, right_key_expr, residual_filter) = match join.on.as_slice() {
        [(l, r)] => (l.clone(), r.clone(), join.filter.clone()),
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
        Some(f) => format!("SELECT * FROM {} WHERE {f}", left_scan.table_sql),
        None => format!("SELECT * FROM {}", left_scan.table_sql),
    };
    let right_sql = match &right_scan.filter_sql {
        Some(f) => format!("SELECT * FROM {} WHERE {f}", right_scan.table_sql),
        None => format!("SELECT * FROM {}", right_scan.table_sql),
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
    let mut join_tail = format!(
        "FROM shuffle_input_0 AS {left_alias} JOIN shuffle_input_1 AS {right_alias} ON {on_sql}"
    );
    if let Some(residual) = residual_filter.as_ref() {
        join_tail.push_str(&format!(" WHERE {}", expr_sql(&up, residual)?));
    }

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
    /// Bare table name (used for replicate/shard policy matching).
    pub(crate) table: &'a str,
    /// Catalog-qualified SQL relation text for stage `FROM` clauses (KAN-4).
    ///
    /// Workers resolve unqualified names to `spark_catalog.default.*`; Glue SF100 tables live
    /// under `glue.<db>.<table>`, so leaf stage SQL must preserve the logical plan's
    /// [`TableReference`] qualification.
    pub(crate) table_sql: String,
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
            table_sql: table_ref_sql(&s.table_name),
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

pub(crate) fn equijoin_from_filter(filter: Option<&Expr>) -> Result<(Expr, Expr, Option<Expr>)> {
    let Some(filter) = filter else {
        return Err(Error::Unsupported(
            "auto-distribute: shuffle join needs an equijoin key (on or filter)".into(),
        ));
    };
    use datafusion::logical_expr::Operator;

    fn flatten_and(expr: &Expr, out: &mut Vec<Expr>) {
        match expr {
            Expr::BinaryExpr(b) if b.op == Operator::And => {
                flatten_and(&b.left, out);
                flatten_and(&b.right, out);
            }
            _ => out.push(expr.clone()),
        }
    }

    let mut conjuncts = Vec::new();
    flatten_and(filter, &mut conjuncts);
    let Some(key_idx) = conjuncts
        .iter()
        .position(|expr| matches!(expr, Expr::BinaryExpr(b) if b.op == Operator::Eq))
    else {
        return Err(Error::Unsupported(
            "auto-distribute: shuffle join filter must contain an equality".into(),
        ));
    };

    let Expr::BinaryExpr(key) = conjuncts.remove(key_idx) else {
        unreachable!("key index selected only binary equality expressions");
    };
    let residual = conjuncts.into_iter().reduce(Expr::and);
    Ok((*key.left, *key.right, residual))
}

#[cfg(test)]
mod equijoin_filter_tests {
    use super::equijoin_from_filter;
    use datafusion::prelude::{col, lit};

    #[test]
    fn extracts_first_equality_and_preserves_non_equality_residual() {
        let filter = col("a").eq(col("b")).and(col("c").gt(lit(1_i64)));

        let (left, right, residual) =
            equijoin_from_filter(Some(&filter)).expect("equality conjunct should be accepted");

        assert_eq!(left, col("a"));
        assert_eq!(right, col("b"));
        assert_eq!(residual, Some(col("c").gt(lit(1_i64))));
    }
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

/// Partial-stage `SELECT` fragment(s) and final-stage combine expression for one aggregate at
/// output position `i`, given its (DataFusion-canonical, lowercased) function name and argument
/// SQL. Shared by `global_aggregation_stages` and `recombine_stage_sql`, which differ only in
/// group-by handling around this per-aggregate decomposition.
fn partial_combine_sql(func: &str, i: usize, arg_sql: &str) -> Result<(Vec<String>, String)> {
    match func {
        "sum" => Ok((
            vec![format!("sum({arg_sql}) AS a{i}")],
            format!("sum(a{i}) AS r{i}"),
        )),
        "count" => Ok((
            vec![format!("count({arg_sql}) AS a{i}")],
            format!("sum(a{i}) AS r{i}"), // counts recombine by summing
        )),
        "min" => Ok((
            vec![format!("min({arg_sql}) AS a{i}")],
            format!("min(a{i}) AS r{i}"),
        )),
        "max" => Ok((
            vec![format!("max({arg_sql}) AS a{i}")],
            format!("max(a{i}) AS r{i}"),
        )),
        "avg" => Ok((
            vec![format!(
                "sum({arg_sql}) AS a{i}s, count({arg_sql}) AS a{i}c"
            )],
            // No cast: SUM/COUNT keep DataFusion's own AVG result type (a DECIMAL average
            // stays DECIMAL at the same scale). Forcing DOUBLE here made TPC-DS Q7/Q26 return
            // numerically-right values at the wrong scale (`120.65` vs `120.650000`).
            format!("(sum(a{i}s) / NULLIF(sum(a{i}c), 0)) AS r{i}"),
        )),
        // stddev/stddev_samp/var/var_samp (Spark's `stddev`, `stddev_samp`, `variance`, `var_samp`,
        // `var_sample` all resolve to these DataFusion-canonical names) recombine from the partial
        // sum, sum-of-squares, and count via the parallel variance identity
        // `Var = (Σx² - (Σx)²/n) / (n-1)` (sample) or `/ n` (population); stddev is `sqrt(Var)`.
        "stddev" | "var" => {
            let sel = vec![format!(
                "sum({arg_sql}) AS a{i}s, sum(({arg_sql})*({arg_sql})) AS a{i}q, count({arg_sql}) AS a{i}c"
            )];
            let combine = format!(
                "(sum(a{i}q) - (sum(a{i}s)*sum(a{i}s))/NULLIF(sum(a{i}c),0)) / NULLIF(sum(a{i}c)-1, 0)"
            );
            let combine = if func == "stddev" {
                format!("sqrt({combine}) AS r{i}")
            } else {
                format!("{combine} AS r{i}")
            };
            Ok((sel, combine))
        }
        "stddev_pop" | "var_pop" => {
            let sel = vec![format!(
                "sum({arg_sql}) AS a{i}s, sum(({arg_sql})*({arg_sql})) AS a{i}q, count({arg_sql}) AS a{i}c"
            )];
            let combine = format!(
                "(sum(a{i}q) - (sum(a{i}s)*sum(a{i}s))/NULLIF(sum(a{i}c),0)) / NULLIF(sum(a{i}c), 0)"
            );
            let combine = if func == "stddev_pop" {
                format!("sqrt({combine}) AS r{i}")
            } else {
                format!("{combine} AS r{i}")
            };
            Ok((sel, combine))
        }
        other => Err(Error::Unsupported(format!(
            "auto-distribute: aggregate `{other}` not supported"
        ))),
    }
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
    // Partial SELECT list: group cols as g{j}, then per-aggregate partial state. Final combine
    // SELECT list (over `shuffle_input`): g{j} group cols + recombined aggregates.
    let (psel, combine) = partial_and_combine_lists(group_sql, aggs)?;

    let group_by = group_sql.join(", ");
    let partial_sql = sanitize_generated_sql(&format!(
        "SELECT {} {tail} GROUP BY {group_by}",
        psel.join(", ")
    ));
    let final_group_by = final_group_by_sql(&p.agg.group_expr, group_sql.len())?;
    let reject_empty_partition = if is_grouping_set(&p.agg.group_expr) {
        // Empty shuffle buckets would otherwise each emit the empty grouping-set row (the ROLLUP
        // grand total). Only partition 0 receives rows on the grouping-set gather.
        " HAVING COUNT(*) > 0"
    } else {
        ""
    };
    let inner = format!(
        "SELECT {} FROM shuffle_input GROUP BY {final_group_by}{reject_empty_partition}",
        combine.join(", "),
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
    let final_group_by = final_group_by_sql(&p.agg.group_expr, group_sql.len())?;
    let reject_empty_partition = if is_grouping_set(&p.agg.group_expr) {
        " HAVING COUNT(*) > 0"
    } else {
        ""
    };
    let inner = format!(
        "SELECT {} FROM shuffle_input GROUP BY {final_group_by}{reject_empty_partition}",
        combine.join(", "),
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
    let flattened_groups = flattened_group_exprs(&agg.group_expr);
    for (j, g) in flattened_groups.iter().enumerate() {
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
    let n_group = flattened_groups.len();
    for (j, field) in agg.schema.fields().iter().take(n_group).enumerate() {
        remap.insert(field.name().clone(), format!("g{j}"));
    }
    // DataFusion inserts a hidden `__grouping_id` field between the flattened group fields and the
    // aggregate fields. It is not part of `aggr_expr` and must not consume an `r{i}` position.
    let agg_field_offset = n_group + usize::from(is_grouping_set(&agg.group_expr));
    for (i, field) in agg
        .schema
        .fields()
        .iter()
        .skip(agg_field_offset)
        .take(agg.aggr_expr.len())
        .enumerate()
    {
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

/// If `agg.input` contains a `UNION` where at least one arm scans `sharded_name` and at least one
/// other arm scans it zero times, plan the two arm groups with different **placements** instead
/// of flatly rejecting the query (see [`reject_unsafe_broadcast_shapes`] for why a uniform
/// broadcast is unsafe here — TPC-DS Q33/Q56/Q60/Q66/Q71/Q76's per-channel `UNION ALL`, one arm
/// per fact table):
///
/// - the sharded arm(s) become an ordinary partial-aggregate producer stage (one per worker,
///   hash-shuffled by the outer `GROUP BY` key) — unchanged from the single-arm path.
/// - the replicated-only arm(s) become a **second** producer stage using the very same partial
///   SQL shape and the same hash key, but run on exactly one worker
///   ([`ExchangeMode::Forward`] — see the driver's producer loop): every worker holds identical
///   data for these arms, so computing the (already-exact) partial there once and shuffling it
///   by the shared group key merges correctly with the sharded arms' genuine per-worker partials,
///   instead of being replicated once per worker and multiplying the total.
/// - the final combine stage reads *both* producer stages (`shuffle_input_0`/`shuffle_input_1`,
///   the same multi-upstream shape [`crate::plan::join_chain`] uses for shuffle joins) and
///   recombines exactly as the single-arm path would.
///
/// `Ok(None)` — falling through to the flat [`reject_unsafe_broadcast_shapes`] guard — when: no
/// `Union` is found under `agg.input`; every arm (or no arm) scans `sharded_name`, so there is
/// nothing to place differently; the aggregate has a `DISTINCT` aggregate (not yet composed with
/// this split); or the `Union` sits under a plan node this function does not know how to rebuild
/// with a narrowed child (only single-child nodes and `Join` are supported — see
/// [`split_union_by_sharding`]). `ROLLUP`/`CUBE`/`GROUPING SETS` (TPC-DS Q77/Q80) *are* supported,
/// mirroring the single-arm path's empty-hash-key gather + `HAVING COUNT(*) > 0` convention.
fn try_split_broadcast_union(
    p: &Peeled<'_>,
    sharded_name: &str,
) -> Result<Option<DistributedQuery>> {
    let agg = p.agg;
    let aggs = agg
        .aggr_expr
        .iter()
        .map(AggSpec::classify)
        .collect::<Result<Vec<_>>>()?;
    if aggs.iter().any(|a| a.distinct) {
        return Ok(None);
    }
    let Some((sharded_input, replicated_input)) =
        split_union_by_sharding(&agg.input, sharded_name)?
    else {
        return Ok(None);
    };

    // Combining per-arm Forward placement with ROLLUP/CUBE/GROUPING SETS currently returns
    // wrong answers (TPC-DS Q5/Q77/Q80: distributed ≠ single-node). Decline so
    // `reject_unsafe_broadcast_shapes` keeps these as honest single-node fallbacks until the
    // gather + multi-level recombine composition is proven correct.
    if is_grouping_set(&agg.group_expr) {
        return Ok(None);
    }

    // The sharded side keeps every safety check the single-arm path would have run on the whole
    // input — nested Unions, self-joins, outer joins whose preserved side misses the sharded
    // table, etc. — just scoped to the narrower subtree. The replicated side is, by construction,
    // untouched by any of those (it scans `sharded_name` zero times, so every check below is a
    // vacuous pass), so it needs no further validation here.
    reject_unsafe_broadcast_shapes(&sharded_input, sharded_name)?;
    let scans = count_table_scans(&sharded_input, sharded_name);
    if scans > 1 {
        return Err(Error::Unsupported(format!(
            "auto-distribute: sharded table `{sharded_name}` scanned {scans}× \
             (self-join / subquery) — not broadcast-safe"
        )));
    }

    let up = Unparser::default();
    let group_sql: Vec<String> = flattened_group_exprs(&agg.group_expr)
        .into_iter()
        .map(|g| expr_sql(&up, g))
        .collect::<Result<_>>()?;
    let remap = build_remap(p);

    let sharded_tail = union_split_tail(&sharded_input)?;
    let replicated_tail = union_split_tail(&replicated_input)?;
    let (psel, combine) = partial_and_combine_lists(&group_sql, &aggs)?;
    let group_by = group_sql.join(", ");

    let sharded_partial = sanitize_generated_sql(&format!(
        "SELECT {} {sharded_tail} GROUP BY {group_by}",
        psel.join(", ")
    ));
    let replicated_partial = sanitize_generated_sql(&format!(
        "SELECT {} {replicated_tail} GROUP BY {group_by}",
        psel.join(", ")
    ));

    // Grouping sets (ROLLUP/CUBE/GROUPING SETS — TPC-DS Q77/Q80) gather everything to partition 0
    // instead of hashing by key, same as the single-arm path (see `aggregation_stages_for`): a
    // grand-total level spans multiple finest-level keys, which a per-key hash can't co-locate.
    let is_grouping_set = is_grouping_set(&agg.group_expr);
    let hash_key_cols: Vec<u32> = if is_grouping_set {
        vec![]
    } else {
        (0..group_sql.len() as u32).collect()
    };
    let sharded_stage = StageDef::new(0, sharded_partial, vec![], hash_key_cols.clone());
    let mut replicated_stage = StageDef::new(1, replicated_partial, vec![], hash_key_cols);
    replicated_stage.exchange = ExchangeMode::Forward;

    let final_group_by = final_group_by_sql(&agg.group_expr, group_sql.len())?;
    // Matches `recombine_stage_sql`: with an empty hash key, every rendezvous partition but 0
    // gets zero gathered rows yet would still emit the ROLLUP grand-total row for that emptiness.
    let reject_empty_partition = if is_grouping_set {
        " HAVING COUNT(*) > 0"
    } else {
        ""
    };
    let inner = format!(
        "SELECT {} FROM (SELECT * FROM shuffle_input_0 UNION ALL SELECT * FROM shuffle_input_1) \
         AS merged_arms GROUP BY {final_group_by}{reject_empty_partition}",
        combine.join(", ")
    );
    let final_sql = wrap_output(p, &inner, &remap)?;
    let combine_stage = StageDef::new(2, final_sql, vec![0, 1], vec![]);

    Ok(Some(DistributedQuery {
        stages: vec![sharded_stage, replicated_stage, combine_stage],
        finalize_sql: build_finalize(p)?,
    }))
}

/// Unparse `lp` and extract its `FROM …` tail — the same unparse-then-slice `agg.input` handling
/// as the single-arm path, factored out so [`try_split_broadcast_union`] can apply it to each of
/// the two narrowed sub-plans [`split_union_by_sharding`] produces.
fn union_split_tail(lp: &LogicalPlan) -> Result<String> {
    let sql = Unparser::default()
        .plan_to_sql(lp)
        .map_err(|e| Error::Unsupported(format!("auto-distribute: unparse union-split arm: {e}")))?
        .to_string();
    let tail = extract_from_tail(&sql)?;
    Ok(sanitize_generated_sql(&tail))
}

/// The partial-stage `SELECT` list (`g{j}` group columns + each aggregate's partial state) and
/// the corresponding final-stage combine expressions, shared by every partial/combine caller in
/// this module ([`recombine_stage_sql`], [`global_aggregation_stages`], and
/// [`try_split_broadcast_union`]).
fn partial_and_combine_lists(
    group_sql: &[String],
    aggs: &[AggSpec],
) -> Result<(Vec<String>, Vec<String>)> {
    let mut psel: Vec<String> = group_sql
        .iter()
        .enumerate()
        .map(|(j, g)| format!("{g} AS g{j}"))
        .collect();
    let mut combine: Vec<String> = (0..group_sql.len()).map(|j| format!("g{j}")).collect();
    for (i, a) in aggs.iter().enumerate() {
        let (sel, comb) = partial_combine_sql(&a.func, i, &a.arg_sql)?;
        psel.extend(sel);
        combine.push(comb);
    }
    Ok((psel, combine))
}

/// Split a `Union` reachable from `lp` into two rebuilt plans — one keeping only the arms that
/// scan `sharded_name` at least once, one keeping only the arms that scan it zero times — or
/// `Ok(None)` when there is nothing useful to split (no `Union`, or every/no arm scans it).
///
/// Descends through any node with exactly one child (`Projection`/`Filter`/`SubqueryAlias`/…) by
/// recursing and rebuilding via [`LogicalPlan::with_new_exprs`] with the same expressions and a
/// narrowed child — the standard "rewrite one subtree, keep the rest" pattern. A `Join` (TPC-DS
/// Q71 wraps its per-channel `UNION ALL` in `item`/`time_dim` broadcast joins before the outer
/// aggregate) is descended into on whichever single side contains the `Union`; the other side
/// (small replicated dimensions) is cloned unchanged into both rebuilt plans. Any other multi-
/// child node, or a `Union` reachable from more than one child, returns `Ok(None)` rather than
/// guessing.
fn split_union_by_sharding(
    lp: &LogicalPlan,
    sharded_name: &str,
) -> Result<Option<(LogicalPlan, LogicalPlan)>> {
    if let LogicalPlan::Union(u) = lp {
        let mut sharded_arms = Vec::new();
        let mut replicated_arms = Vec::new();
        for arm in &u.inputs {
            if count_table_scans(arm, sharded_name) > 0 {
                sharded_arms.push(Arc::clone(arm));
            } else {
                replicated_arms.push(Arc::clone(arm));
            }
        }
        if sharded_arms.is_empty() || replicated_arms.is_empty() {
            return Ok(None);
        }
        return Ok(Some((
            union_of_arms(sharded_arms)?,
            union_of_arms(replicated_arms)?,
        )));
    }

    let children = lp.inputs();
    if children.is_empty() {
        return Ok(None);
    }
    if children.len() == 1 {
        return match split_union_by_sharding(children[0], sharded_name)? {
            Some((s, r)) => Ok(Some((with_new_child(lp, s)?, with_new_child(lp, r)?))),
            None => Ok(None),
        };
    }
    // Multi-child node (Join, …): the Union must live under exactly one child; the rest are
    // cloned unchanged into both rebuilt plans.
    let mut found: Option<(usize, LogicalPlan, LogicalPlan)> = None;
    for (idx, child) in children.iter().enumerate() {
        if let Some((s, r)) = split_union_by_sharding(child, sharded_name)? {
            if found.is_some() {
                return Ok(None); // ambiguous: more than one child has a splittable Union
            }
            found = Some((idx, s, r));
        }
    }
    let Some((idx, s_child, r_child)) = found else {
        return Ok(None);
    };
    let mut s_children: Vec<LogicalPlan> = children.iter().map(|c| (*c).clone()).collect();
    let mut r_children = s_children.clone();
    s_children[idx] = s_child;
    r_children[idx] = r_child;
    Ok(Some((
        lp.with_new_exprs(lp.expressions(), s_children)
            .map_err(|e| {
                Error::Unsupported(format!("auto-distribute: rebuild union-split join: {e}"))
            })?,
        lp.with_new_exprs(lp.expressions(), r_children)
            .map_err(|e| {
                Error::Unsupported(format!("auto-distribute: rebuild union-split join: {e}"))
            })?,
    )))
}

/// Rebuild `lp` with its single existing child replaced by `child`, keeping `lp`'s own
/// expressions unchanged — the "rewrite one subtree" step [`split_union_by_sharding`] applies at
/// every single-child node on the way down to (or up from) a splittable `Union`.
fn with_new_child(lp: &LogicalPlan, child: LogicalPlan) -> Result<LogicalPlan> {
    lp.with_new_exprs(lp.expressions(), vec![child])
        .map_err(|e| Error::Unsupported(format!("auto-distribute: rebuild union-split node: {e}")))
}

/// Rebuild a `Union` from a (possibly single-element) arm subset, collapsing to the bare plan
/// when only one arm remains — matches how a single-arm `agg.input` unparses today (no `UNION`
/// wrapper for one input).
fn union_of_arms(mut arms: Vec<Arc<LogicalPlan>>) -> Result<LogicalPlan> {
    if arms.len() == 1 {
        return Ok((*arms.remove(0)).clone());
    }
    Union::try_new(arms)
        .map(LogicalPlan::Union)
        .map_err(|e| Error::Unsupported(format!("auto-distribute: rebuild split union: {e}")))
}

/// Reject plan shapes where broadcasting the replicated tables to every worker duplicates output
/// rows instead of partitioning them.
///
/// The single-sharded-table broadcast model is correct when every output row is produced by
/// matching against the (partitioned) sharded table, which a plain inner-join chain guarantees.
/// Two shapes break that invariant, and both go wrong silently — the query returns a number that
/// is a multiple of the right one:
///
/// - a `UNION ALL` arm with no path to the sharded table — only when [`try_split_broadcast_union`]
///   above could not place it separately (e.g. the `Union` sits under a shape it can't rebuild).
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

/// Rewrite dangling `left`/`right` join-side aliases (see
/// [`reject_out_of_scope_join_alias_refs`]) to the outer alias that actually owns them, when the
/// SQL shape makes that alias recoverable.
///
/// The Unparser's `(… AS "left" …) AS hot_cust WHERE EXISTS (… `left`.col …)` shape closes the
/// frame that bound `left` and immediately re-aliases it as `hot_cust` one level out — the
/// dangling reference is really `` `hot_cust`.col ``, the Unparser just didn't substitute it.
/// This walks the same paren-scope stack as the rejector, but:
///
/// - when a frame that bound `left`/`right` closes, remembers those names as *pending*;
/// - if the very next significant token is `AS <alias>` (quoted or bare), maps each pending name
///   to that alias — this is the only case that resolves pending names, so sibling scopes
///   (`), (SELECT …)` with no `AS` in between) never absorb into an unrelated alias;
///   anything else clears pending without recording a mapping;
/// - an out-of-scope use rewrites to the mapped outer alias (matching the use's own quote style)
///   if a mapping exists, otherwise it's still dangling and returns the same
///   [`Error::Unsupported`] the rejector would.
///
/// In-scope uses are copied through unchanged.
pub(crate) fn rewrite_out_of_scope_join_alias_refs(sql: &str) -> Result<String> {
    const DEFS: [(&str, &str); 4] = [
        ("\"left\"", "left"),
        ("\"right\"", "right"),
        ("`left`", "left"),
        ("`right`", "right"),
    ];
    const USES: [(&str, &str, u8); 4] = [
        ("\"left\".", "left", b'"'),
        ("\"right\".", "right", b'"'),
        ("`left`.", "left", b'`'),
        ("`right`.", "right", b'`'),
    ];
    let bytes = sql.as_bytes();
    let mut stack: Vec<Vec<&str>> = vec![Vec::new()];
    let mut absorbed: HashMap<&str, String> = HashMap::new();
    let mut out = String::with_capacity(sql.len());
    let mut i = 0usize;
    while i < bytes.len() {
        match bytes[i] {
            b'(' => {
                stack.push(Vec::new());
                out.push('(');
                i += 1;
            }
            b')' => {
                let closed = if stack.len() > 1 { stack.pop() } else { None };
                out.push(')');
                i += 1;
                if let Some(closed) = closed {
                    if !closed.is_empty() {
                        if let Some(alias) = peek_as_alias(sql, i) {
                            for name in closed {
                                absorbed.insert(name, alias.clone());
                            }
                        }
                        // Anything other than `AS <alias>` right after the close: pending is
                        // dropped without recording a mapping (sibling scopes must not leak).
                    }
                }
            }
            _ => {
                if let Some(&(pat, name, quote)) =
                    USES.iter().find(|(pat, _, _)| sql[i..].starts_with(*pat))
                {
                    if stack.iter().any(|frame| frame.contains(&name)) {
                        out.push_str(pat);
                    } else if let Some(alias) = absorbed.get(name) {
                        out.push(quote as char);
                        out.push_str(alias);
                        out.push(quote as char);
                        out.push('.');
                    } else {
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
                    out.push_str(pat);
                    i += pat.len();
                    continue;
                }
                out.push(bytes[i] as char);
                i += 1;
            }
        }
    }
    Ok(out)
}

/// Look just past a just-closed `)` at byte offset `pos` for `AS <alias>` and, if found, return
/// the alias's identifier text (quotes stripped). Returns `None` for anything else — a sibling
/// separator (`,`), a keyword (`WHERE`, …), another `)`, or end of input.
fn peek_as_alias(sql: &str, pos: usize) -> Option<String> {
    let bytes = sql.as_bytes();
    let mut j = pos;
    while j < bytes.len() && bytes[j].is_ascii_whitespace() {
        j += 1;
    }
    if j + 2 > bytes.len() || !bytes[j..j + 2].eq_ignore_ascii_case(b"AS") {
        return None;
    }
    let after_as = j + 2;
    if after_as < bytes.len()
        && (bytes[after_as].is_ascii_alphanumeric() || bytes[after_as] == b'_')
    {
        return None; // e.g. `ASC`, or an identifier starting with "as"
    }
    let mut k = after_as;
    while k < bytes.len() && bytes[k].is_ascii_whitespace() {
        k += 1;
    }
    match bytes.get(k)? {
        b'"' | b'`' => {
            let quote = bytes[k];
            let start = k + 1;
            let mut end = start;
            while end < bytes.len() && bytes[end] != quote {
                end += 1;
            }
            if end < bytes.len() {
                Some(sql[start..end].to_string())
            } else {
                None
            }
        }
        c if c.is_ascii_alphabetic() || *c == b'_' => {
            let start = k;
            let mut end = k;
            while end < bytes.len() && (bytes[end].is_ascii_alphanumeric() || bytes[end] == b'_') {
                end += 1;
            }
            Some(sql[start..end].to_string())
        }
        _ => None,
    }
}

/// Whether DataFusion's aggregate uses its single-expression grouping-set representation.
fn is_grouping_set(group_expr: &[Expr]) -> bool {
    matches!(group_expr, [Expr::GroupingSet(_)])
}

/// Expand DataFusion's positional `group_expr` representation into the actual output columns.
///
/// `ROLLUP(a, b)` and `CUBE(a, b)` become `[a, b]`. Explicit grouping sets become the stable,
/// de-duplicated union returned by DataFusion's own [`GroupingSet::distinct_expr`].
fn flattened_group_exprs(group_expr: &[Expr]) -> Vec<&Expr> {
    match group_expr {
        [Expr::GroupingSet(grouping_set)] => grouping_set.distinct_expr(),
        ordinary => ordinary.iter().collect(),
    }
}

/// Render the final stage's group-by over safe `g{j}` columns.
///
/// The grouping construct appears only in `GROUP BY`; unlike the old positional lowering, it is
/// never emitted into the partial SELECT list where Databricks would resolve `ROLLUP` as a scalar
/// function. Keeping the explicit space (`ROLLUP (...)`) also matches the syntax accepted by the
/// worker parser and by the original TPC-DS queries.
fn final_group_by_sql(group_expr: &[Expr], flattened_len: usize) -> Result<String> {
    let flattened = flattened_group_exprs(group_expr);
    if flattened.len() != flattened_len {
        return Err(Error::Unsupported(format!(
            "auto-distribute: grouping set has {} flattened columns but stage SQL has {flattened_len}",
            flattened.len()
        )));
    }

    let safe_name = |expr: &Expr| -> Result<String> {
        flattened
            .iter()
            .position(|candidate| *candidate == expr)
            .map(|j| format!("g{j}"))
            .ok_or_else(|| {
                Error::Unsupported(format!(
                    "auto-distribute: grouping-set expression `{expr}` is not in its flattened columns"
                ))
            })
    };

    match group_expr {
        [Expr::GroupingSet(GroupingSet::Rollup(exprs))] => Ok(format!(
            "ROLLUP ({})",
            exprs
                .iter()
                .map(safe_name)
                .collect::<Result<Vec<_>>>()?
                .join(", ")
        )),
        [Expr::GroupingSet(GroupingSet::Cube(exprs))] => Ok(format!(
            "CUBE ({})",
            exprs
                .iter()
                .map(safe_name)
                .collect::<Result<Vec<_>>>()?
                .join(", ")
        )),
        [Expr::GroupingSet(GroupingSet::GroupingSets(levels))] => {
            let levels = levels
                .iter()
                .map(|level| {
                    let names = level.iter().map(safe_name).collect::<Result<Vec<_>>>()?;
                    Ok(format!("({})", names.join(", ")))
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(format!("GROUPING SETS ({})", levels.join(", ")))
        }
        ordinary => {
            if ordinary.len() != flattened_len {
                return Err(Error::Unsupported(format!(
                    "auto-distribute: expected {} ordinary group columns, got {flattened_len}",
                    ordinary.len()
                )));
            }
            Ok((0..flattened_len)
                .map(|j| format!("g{j}"))
                .collect::<Vec<_>>()
                .join(", "))
        }
    }
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

/// True when `lp` (or any nested subquery plan) contains a `Distinct` / `DistinctOn` node.
fn plan_contains_distinct(lp: &LogicalPlan) -> bool {
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
    matches!(lp, LogicalPlan::Distinct(_))
        || lp.inputs().iter().any(|c| plan_contains_distinct(c))
        || {
            let mut found = false;
            for e in lp.expressions() {
                let _ = e.apply(|node| {
                    let sub = match node {
                        Expr::Exists(ex) => Some(ex.subquery.subquery.as_ref()),
                        Expr::InSubquery(iq) => Some(iq.subquery.subquery.as_ref()),
                        Expr::ScalarSubquery(sq) => Some(sq.subquery.as_ref()),
                        _ => None,
                    };
                    if let Some(plan) = sub {
                        if plan_contains_distinct(plan) {
                            found = true;
                            return Ok(TreeNodeRecursion::Stop);
                        }
                    }
                    Ok(TreeNodeRecursion::Continue)
                });
                if found {
                    break;
                }
            }
            found
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

/// SQL relation text preserving catalog/schema qualification from a logical [`TableReference`].
pub(crate) fn table_ref_sql(reference: &TableReference) -> String {
    reference.to_string()
}

/// Look up the catalog-qualified SQL text for a bare table name in `lp` (and expression
/// subqueries). Falls back to the bare name when no matching scan exists (local MemTables).
pub(crate) fn qualified_table_sql(lp: &LogicalPlan, bare: &str) -> String {
    find_qualified_table_sql(lp, bare).unwrap_or_else(|| bare.to_string())
}

fn find_qualified_table_sql(lp: &LogicalPlan, bare: &str) -> Option<String> {
    if let LogicalPlan::TableScan(s) = lp {
        if s.table_name.table() == bare {
            return Some(table_ref_sql(&s.table_name));
        }
    }
    for e in lp.expressions() {
        use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
        let mut found = None;
        let _ = e.apply(|node| {
            let sub = match node {
                Expr::Exists(ex) => Some(ex.subquery.subquery.as_ref()),
                Expr::InSubquery(iq) => Some(iq.subquery.subquery.as_ref()),
                Expr::ScalarSubquery(sq) => Some(sq.subquery.as_ref()),
                _ => None,
            };
            if let Some(plan) = sub {
                if let Some(sql) = find_qualified_table_sql(plan, bare) {
                    found = Some(sql);
                    return Ok(TreeNodeRecursion::Stop);
                }
            }
            Ok(TreeNodeRecursion::Continue)
        });
        if found.is_some() {
            return found;
        }
    }
    for c in lp.inputs() {
        if let Some(sql) = find_qualified_table_sql(c, bare) {
            return Some(sql);
        }
    }
    None
}

#[cfg(test)]
mod guard_tests {
    use super::{
        find_qualified_table_sql, qualified_table_sql, reject_out_of_scope_join_alias_refs,
        rewrite_out_of_scope_join_alias_refs, table_ref_sql,
    };
    use datafusion::common::TableReference;
    use datafusion::logical_expr::LogicalPlanBuilder;
    use datafusion::prelude::lit;
    use std::sync::Arc;
    use weft_loom::arrow::datatypes::{DataType, Field, Schema};

    #[test]
    fn table_ref_sql_preserves_qualification() {
        assert_eq!(table_ref_sql(&TableReference::bare("lineitem")), "lineitem");
        assert_eq!(
            table_ref_sql(&TableReference::partial("tpch_sf100", "lineitem")),
            "tpch_sf100.lineitem"
        );
        assert_eq!(
            table_ref_sql(&TableReference::full("glue", "tpch_sf100", "lineitem")),
            "glue.tpch_sf100.lineitem"
        );
    }

    #[test]
    fn qualified_table_sql_reads_full_table_reference_from_scan() {
        use datafusion::logical_expr::logical_plan::builder::LogicalTableSource;

        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let source = Arc::new(LogicalTableSource::new(schema));
        let lp = LogicalPlanBuilder::scan(
            TableReference::full("glue", "tpch_sf100", "lineitem"),
            source,
            None,
        )
        .unwrap()
        .filter(lit(true))
        .unwrap()
        .build()
        .unwrap();
        assert_eq!(
            qualified_table_sql(&lp, "lineitem"),
            "glue.tpch_sf100.lineitem"
        );
        assert_eq!(
            find_qualified_table_sql(&lp, "orders"),
            None,
            "missing bare name must not invent a qualification"
        );
    }

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
        // The rewrite is a no-op on already-in-scope SQL.
        assert_eq!(rewrite_out_of_scope_join_alias_refs(sql).unwrap(), sql);
    }

    #[test]
    fn dangling_reference_to_the_enclosing_alias_is_rewritten() {
        // TPC-DS Q38/Q87 shape: `left` is bound inside the parens that `AS hot_cust` closes, so
        // the second EXISTS's `left` is dangling — but the Unparser did emit the outer alias
        // that owns it (`hot_cust`), so the reference can be rewritten rather than rejected.
        let sql = r#"SELECT count(1) FROM (SELECT * FROM (SELECT * FROM t) AS "left" WHERE EXISTS (SELECT 1 FROM u WHERE (`left`.k = u.k))) AS hot_cust WHERE EXISTS (SELECT 1 FROM v WHERE (`left`.k = v.k))"#;

        // Unrewritten, this is still the unsupported dangling shape.
        let err = reject_out_of_scope_join_alias_refs(sql).expect_err("dangling `left`");
        assert!(err.to_string().contains("outside the scope"), "{err}");

        let rewritten = rewrite_out_of_scope_join_alias_refs(sql).expect("recoverable");
        let expected = r#"SELECT count(1) FROM (SELECT * FROM (SELECT * FROM t) AS "left" WHERE EXISTS (SELECT 1 FROM u WHERE (`left`.k = u.k))) AS hot_cust WHERE EXISTS (SELECT 1 FROM v WHERE (`hot_cust`.k = v.k))"#;
        assert_eq!(rewritten, expected);

        // The in-scope correlation inside the first EXISTS is untouched; only the dangling
        // second reference was rewritten. The rejector, run as a safety net, now passes.
        assert!(reject_out_of_scope_join_alias_refs(&rewritten).is_ok());
    }

    #[test]
    fn rewrite_absorbs_a_double_quoted_outer_alias_matching_the_uses_quote_style() {
        // Same shape, but the outer alias is `AS "hot_cust"` and the dangling use is
        // double-quoted — the rewrite must preserve that quote style rather than always
        // emitting backticks.
        let sql = r#"SELECT * FROM (SELECT * FROM t AS "left" WHERE EXISTS (SELECT 1 FROM u WHERE ("left".k = u.k))) AS "hot_cust" WHERE EXISTS (SELECT 1 FROM v WHERE ("left".k = v.k))"#;
        let rewritten = rewrite_out_of_scope_join_alias_refs(sql).expect("recoverable");
        let expected = r#"SELECT * FROM (SELECT * FROM t AS "left" WHERE EXISTS (SELECT 1 FROM u WHERE ("left".k = u.k))) AS "hot_cust" WHERE EXISTS (SELECT 1 FROM v WHERE ("hot_cust".k = v.k))"#;
        assert_eq!(rewritten, expected);
        assert!(reject_out_of_scope_join_alias_refs(&rewritten).is_ok());
    }

    #[test]
    fn reference_with_no_definition_at_all_is_rejected() {
        assert!(reject_out_of_scope_join_alias_refs(r#"SELECT "left".a FROM t"#).is_err());
        // No definition ever appears, so there's nothing to absorb into — still unfixable.
        assert!(rewrite_out_of_scope_join_alias_refs(r#"SELECT "left".a FROM t"#).is_err());
    }

    #[test]
    fn a_sibling_scopes_definition_does_not_leak() {
        let sql = r#"SELECT * FROM (SELECT 1 FROM x AS "left" WHERE `left`.a = 1), (SELECT `left`.b FROM y)"#;
        assert!(reject_out_of_scope_join_alias_refs(sql).is_err());
        // The first scope closes into a sibling `,`, not an `AS <alias>`, so pending is cleared
        // rather than absorbed — the second, unrelated `left` use in the sibling stays unfixable.
        assert!(rewrite_out_of_scope_join_alias_refs(sql).is_err());
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

#[cfg(test)]
mod agg_combine_tests {
    use super::partial_combine_sql;

    #[test]
    fn stddev_samp_combine_uses_nminus1_and_sqrt() {
        // `stddev`/`stddev_samp` resolve to DataFusion's canonical `stddev` name.
        let (sel, combine) = partial_combine_sql("stddev", 0, "t.v").expect("supported");
        assert_eq!(
            sel,
            vec!["sum(t.v) AS a0s, sum((t.v)*(t.v)) AS a0q, count(t.v) AS a0c"]
        );
        assert_eq!(
            combine,
            "sqrt((sum(a0q) - (sum(a0s)*sum(a0s))/NULLIF(sum(a0c),0)) / NULLIF(sum(a0c)-1, 0)) AS r0"
        );
    }

    #[test]
    fn stddev_pop_combine_divides_by_n() {
        let (_, combine) = partial_combine_sql("stddev_pop", 2, "x").expect("supported");
        assert_eq!(
            combine,
            "sqrt((sum(a2q) - (sum(a2s)*sum(a2s))/NULLIF(sum(a2c),0)) / NULLIF(sum(a2c), 0)) AS r2"
        );
    }

    #[test]
    fn var_samp_combine_matches_stddev_without_sqrt() {
        // `var`/`var_samp`/`var_sample`/`variance` all resolve to DataFusion's canonical `var` name.
        let (_, combine) = partial_combine_sql("var", 0, "x").expect("supported");
        assert_eq!(
            combine,
            "(sum(a0q) - (sum(a0s)*sum(a0s))/NULLIF(sum(a0c),0)) / NULLIF(sum(a0c)-1, 0) AS r0"
        );
    }

    #[test]
    fn var_pop_combine_matches_stddev_pop_without_sqrt() {
        let (_, combine) = partial_combine_sql("var_pop", 0, "x").expect("supported");
        assert_eq!(
            combine,
            "(sum(a0q) - (sum(a0s)*sum(a0s))/NULLIF(sum(a0c),0)) / NULLIF(sum(a0c), 0) AS r0"
        );
    }

    #[test]
    fn existing_aggregates_are_unchanged() {
        assert_eq!(
            partial_combine_sql("sum", 0, "x").unwrap(),
            (
                vec!["sum(x) AS a0".to_string()],
                "sum(a0) AS r0".to_string()
            )
        );
        assert_eq!(
            partial_combine_sql("avg", 1, "x").unwrap(),
            (
                vec!["sum(x) AS a1s, count(x) AS a1c".to_string()],
                "(sum(a1s) / NULLIF(sum(a1c), 0)) AS r1".to_string()
            )
        );
    }

    #[test]
    fn unsupported_aggregate_is_an_honest_error_not_a_wrong_answer() {
        assert!(partial_combine_sql("median", 0, "x").is_err());
    }
}

#[cfg(test)]
mod grouping_set_tests {
    use std::sync::Arc;

    use datafusion::logical_expr::{Expr, GroupingSet};
    use datafusion::prelude::col;
    use weft_loom::arrow::array::{Int64Array, RecordBatch};
    use weft_loom::arrow::datatypes::{DataType, Field, Schema};
    use weft_loom::arrow::util::pretty::pretty_format_batches;
    use weft_loom::Engine;

    use super::{final_group_by_sql, flattened_group_exprs, plan_distributed_logical};

    fn table() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k1", DataType::Int64, false),
            Field::new("k2", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 1, 2])),
                Arc::new(Int64Array::from(vec![10, 20, 10])),
                Arc::new(Int64Array::from(vec![5, 7, 11])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn renders_rollup_cube_and_explicit_grouping_sets_over_safe_columns() {
        let rollup = vec![Expr::GroupingSet(GroupingSet::Rollup(vec![
            col("a"),
            col("b"),
        ]))];
        assert_eq!(flattened_group_exprs(&rollup).len(), 2);
        assert_eq!(final_group_by_sql(&rollup, 2).unwrap(), "ROLLUP (g0, g1)");

        let cube = vec![Expr::GroupingSet(GroupingSet::Cube(vec![
            col("a"),
            col("b"),
        ]))];
        assert_eq!(final_group_by_sql(&cube, 2).unwrap(), "CUBE (g0, g1)");

        let grouping_sets = vec![Expr::GroupingSet(GroupingSet::GroupingSets(vec![
            vec![col("a"), col("b")],
            vec![col("b")],
            vec![],
        ]))];
        assert_eq!(flattened_group_exprs(&grouping_sets).len(), 2);
        assert_eq!(
            final_group_by_sql(&grouping_sets, 2).unwrap(),
            "GROUPING SETS ((g0, g1), (g1), ())"
        );
    }

    #[tokio::test]
    async fn rollup_uses_finest_partial_group_and_matches_single_node() {
        let engine = Engine::new();
        engine.register_batches("t", vec![table()]).unwrap();
        let sql = "SELECT k1, k2, SUM(v) AS total FROM t \
                   GROUP BY ROLLUP (k1, k2) \
                   ORDER BY k1 NULLS FIRST, k2 NULLS FIRST";
        let logical = engine.logical_plan(sql).await.unwrap();
        let dq = plan_distributed_logical(&logical, &[]).expect("ROLLUP should distribute");

        assert_eq!(dq.stages.len(), 2);
        assert!(
            dq.stages[0].hash_key_cols.is_empty(),
            "coarser ROLLUP levels require a gather"
        );
        assert!(!dq.stages[0].sql.contains("ROLLUP"), "{}", dq.stages[0].sql);
        assert!(dq.stages[0].sql.contains("AS g0"), "{}", dq.stages[0].sql);
        assert!(dq.stages[0].sql.contains("AS g1"), "{}", dq.stages[0].sql);
        assert!(
            dq.stages[1]
                .sql
                .contains("GROUP BY ROLLUP (g0, g1) HAVING COUNT(*) > 0"),
            "{}",
            dq.stages[1].sql
        );

        let partial = engine.sql(&dq.stages[0].sql).await.unwrap();
        let partial_schema = partial[0].schema();
        let final_engine = Engine::new();
        final_engine
            .register_batches("shuffle_input", partial)
            .unwrap();
        let combined = final_engine.sql(&dq.stages[1].sql).await.unwrap();
        final_engine.register_batches("result", combined).unwrap();
        let actual = final_engine
            .sql(dq.finalize_sql.as_deref().expect("ORDER BY finalize"))
            .await
            .unwrap();
        let expected = engine.sql(sql).await.unwrap();
        assert_eq!(
            pretty_format_batches(&actual).unwrap().to_string(),
            pretty_format_batches(&expected).unwrap().to_string()
        );

        // Every non-zero consumer partition receives a typed empty shuffle bucket. It must not
        // manufacture another ROLLUP grand-total row.
        let empty_engine = Engine::new();
        empty_engine
            .register_batches(
                "shuffle_input",
                vec![RecordBatch::new_empty(partial_schema)],
            )
            .unwrap();
        let empty = empty_engine.sql(&dq.stages[1].sql).await.unwrap();
        assert_eq!(empty.iter().map(RecordBatch::num_rows).sum::<usize>(), 0);
    }
}

/// Regression locks for PR #52 planner fixes: Q21-shaped HAVING above an aliasing projection,
/// fail-loud unmapped HAVING columns, and AVG recombine without a forced DOUBLE cast.
#[cfg(test)]
mod peel_remap_tests {
    use std::sync::Arc;

    use datafusion::prelude::col;
    use weft_loom::arrow::array::{Int64Array, RecordBatch};
    use weft_loom::arrow::datatypes::{DataType, Field, Schema};
    use weft_loom::Engine;

    use super::{build_remap, ensure_all_columns_remapped, peel, plan_distributed_logical};

    fn tiny_table() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![0i64, 1, 0])),
                Arc::new(Int64Array::from(vec![10i64, 20, 30])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn ensure_all_columns_remapped_accepts_stage_names_only() {
        ensure_all_columns_remapped(&col("r0")).expect("r0 is a remapped aggregate output");
        ensure_all_columns_remapped(&col("g0")).expect("g0 is a remapped group key");
        let err = ensure_all_columns_remapped(&col("inv_before"))
            .expect_err("original alias must not slip through");
        assert!(err.to_string().contains("HAVING references"), "got: {err}");
    }

    #[tokio::test]
    async fn q21_shaped_having_above_alias_projection_remaps() {
        // Filter → SubqueryAlias → Projection → Aggregate. An earlier peel required the Filter to
        // sit directly on Aggregate and silently dropped the predicate (unfiltered Q21 rows).
        let engine = Engine::new();
        engine.register_batches("t", vec![tiny_table()]).unwrap();
        let lp = engine
            .logical_plan(
                "SELECT * FROM (\
                     SELECT k, SUM(v) AS inv_before FROM t GROUP BY k\
                 ) x WHERE inv_before > 10",
            )
            .await
            .unwrap();

        let peeled = peel(&lp).expect("Q21-shaped plan must peel");
        assert!(
            !peeled.having.is_empty(),
            "intervening Filter must be collected as HAVING"
        );
        assert!(
            !peeled.alias_projections.is_empty(),
            "inner SUM alias projection must be retained for remap"
        );
        let remap = build_remap(&peeled);
        assert_eq!(
            remap.get("inv_before").map(String::as_str),
            Some("r0"),
            "alias inv_before must map to aggregate slot r0; got {remap:?}"
        );

        let dq = plan_distributed_logical(&lp, &[]).expect("must distribute");
        let final_sql = &dq.stages.last().expect("stages").sql;
        // Predicate must use remapped `r0`; the output projection may still alias it back
        // to `"inv_before"` for schema fidelity.
        assert!(
            final_sql.contains("WHERE") && final_sql.contains("(r0 > 10)"),
            "final stage must filter on remapped r0; got: {final_sql}"
        );
        assert!(
            !final_sql.contains("inv_before >"),
            "HAVING must not filter on the pre-remap alias name; got: {final_sql}"
        );
    }

    #[tokio::test]
    async fn avg_recombine_does_not_force_double_cast() {
        // Forcing CAST(... AS DOUBLE) made TPC-DS Q7/Q26 return the right number at the wrong scale.
        let engine = Engine::new();
        engine.register_batches("t", vec![tiny_table()]).unwrap();
        let lp = engine
            .logical_plan("SELECT k, AVG(v) AS av FROM t GROUP BY k")
            .await
            .unwrap();
        let dq = plan_distributed_logical(&lp, &[]).expect("avg must distribute");
        let sql = dq
            .stages
            .iter()
            .map(|s| s.sql.as_str())
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            sql.contains("(sum(a0s) / NULLIF(sum(a0c), 0))"),
            "expected SUM/COUNT recombine; got:\n{sql}"
        );
        let upper = sql.to_uppercase();
        assert!(
            !upper.contains("AS DOUBLE") && !upper.contains("AS FLOAT64"),
            "AVG recombine must not force DOUBLE; got:\n{sql}"
        );
    }
}

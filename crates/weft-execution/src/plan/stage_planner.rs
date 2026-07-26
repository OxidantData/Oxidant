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
//! scalar / IN / EXISTS subqueries **over replicated tables only**, `UNION ALL` of
//! distributable aggregations, and **narrow window** support: re-combinable aggregate
//! windows (`SUM`/`COUNT`/`MIN`/`MAX`/`AVG`) with a non-empty `PARTITION BY` over one
//! sharded table (hash-shuffle by the partition key, then compute the window locally).
//! Ranking windows, global windows (no `PARTITION BY`), and `UNION` (distinct) return an
//! explicit [`Error::Unsupported`] so the caller falls back to single-node execution.
//! Correlated subqueries over sharded tables are rejected (not broadcast-safe).

use std::collections::HashMap;

use datafusion::logical_expr::{Aggregate, Expr, LogicalPlan};
use datafusion::sql::unparser::Unparser;
use weft_common::{Error, Result};
use weft_loom::Engine;

use super::shape_extensions::{
    ensure_subquery_tables_replicated, reject_explicit_unsupported, try_union_all, try_window,
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
pub async fn plan_distributed(
    engine: &Engine,
    sql: &str,
    replicated: &[&str],
) -> Result<DistributedQuery> {
    let lp = engine.logical_plan(sql).await?;
    if let Some(dq) = try_union_all(&lp, replicated)? {
        return Ok(dq);
    }
    if let Some(dq) = try_window(&lp, replicated)? {
        return Ok(dq);
    }
    reject_explicit_unsupported(&lp)?;
    let peeled = peel(&lp)?;
    aggregation_stages_for(&peeled, replicated)
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
    /// `HAVING` predicate over the aggregate output, if any.
    pub(crate) having: Option<&'a Expr>,
    /// The aggregate node itself.
    pub(crate) agg: &'a Aggregate,
}

/// Strip an optional `Limit` / `Sort` / `Projection` off the top and require an `Aggregate` under
/// them. Rejects anything else (the caller falls back to single-node).
pub(crate) fn peel(lp: &LogicalPlan) -> Result<Peeled<'_>> {
    let mut limit = None;
    let mut sort = None;
    let mut projection = None;
    let mut having = None;
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
                projection = Some(p.expr.as_slice());
                node = &p.input;
            }
            LogicalPlan::Filter(f) => {
                // Filter directly above Aggregate is HAVING.
                having = Some(f.predicate.as_ref());
                node = &f.input;
            }
            LogicalPlan::Aggregate(agg) => {
                return Ok(Peeled {
                    projection,
                    sort,
                    limit,
                    having,
                    agg,
                })
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
    let sharded_name = sharded[0];
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

    // remap: original output column name -> safe name (`g{j}` group, `r{i}` aggregate result).
    let mut remap: HashMap<String, String> = HashMap::new();
    for (j, g) in agg.group_expr.iter().enumerate() {
        remap.insert(g.schema_name().to_string(), format!("g{j}"));
    }
    for (i, a) in agg.aggr_expr.iter().enumerate() {
        remap.insert(a.schema_name().to_string(), format!("r{i}"));
    }

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

    let mut remap: HashMap<String, String> = HashMap::new();
    for (i, a) in p.agg.aggr_expr.iter().enumerate() {
        remap.insert(a.schema_name().to_string(), format!("r{i}"));
    }

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
                combine.push(format!(
                    "(CAST(sum(a{i}s) AS DOUBLE) / NULLIF(sum(a{i}c), 0)) AS r{i}"
                ));
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

    let mut remap: HashMap<String, String> = HashMap::new();
    for (j, g) in p.agg.group_expr.iter().enumerate() {
        remap.insert(g.schema_name().to_string(), format!("g{j}"));
    }
    for (i, a) in p.agg.aggr_expr.iter().enumerate() {
        remap.insert(a.schema_name().to_string(), format!("r{i}"));
    }

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
                combine.push(format!(
                    "(CAST(sum(a{i}s) AS DOUBLE) / NULLIF(sum(a{i}c), 0)) AS r{i}"
                ));
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

/// Wrap the combined inner query so the final stage's output matches the original query's columns:
/// re-apply the output projection with aggregate/group columns remapped to `r{i}`/`g{j}`, each
/// item explicitly aliased back to its original output name (so a bare `t.k` stays column `k`, and
/// downstream `ORDER BY` over those names resolves). `ORDER BY` / `LIMIT` are *not* applied here —
/// they're global and run in [`build_finalize`].
fn wrap_output(p: &Peeled<'_>, inner: &str, remap: &HashMap<String, String>) -> Result<String> {
    let up = Unparser::default();
    // Apply HAVING against remapped `g{j}`/`r{i}` columns *before* the output projection aliases
    // them back to original names (otherwise `WHERE r0 > …` fails against `having_in.sv`).
    let from_sql = if let Some(pred) = p.having {
        let having_sql = expr_sql(&up, &remap_columns(pred, remap))?;
        format!("(SELECT * FROM ({inner}) AS combined WHERE {having_sql}) AS having_in")
    } else {
        format!("({inner}) AS combined")
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
                if let Some(safe) = remap.get(&c.flat_name()) {
                    return Ok(Transformed::yes(datafusion::prelude::col(safe)));
                }
            }
            Ok(Transformed::no(node))
        })
        .map(|t| t.data)
        .unwrap_or(e.clone())
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
}

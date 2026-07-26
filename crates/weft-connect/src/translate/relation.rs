//! Lower a Spark Connect `Relation` to a DataFusion [`LogicalPlan`].

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::datasource::{provider_as_source, MemTable};
use datafusion::logical_expr::{col, lit, Expr, LogicalPlan, LogicalPlanBuilder, SortExpr};
use datafusion::prelude::SessionContext;
use datafusion::sql::unparser::Unparser;
use tonic::Status;
use weft_proto::spark::connect as sc;

use super::expr::{is_wildcard, to_expr};
use super::inval;

type PlanFuture<'a> = Pin<Box<dyn Future<Output = Result<LogicalPlan, Status>> + Send + 'a>>;

/// Monotonic counter giving each inline `LocalRelation` scan a unique table name.
static LOCAL_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Translate a relation to a logical plan. Boxed so the (async) recursion compiles.
pub fn to_plan<'a>(ctx: &'a SessionContext, rel: &'a sc::Relation) -> PlanFuture<'a> {
    Box::pin(async move { translate(ctx, rel).await })
}

async fn child<'a>(
    ctx: &'a SessionContext,
    rel: &'a Option<Box<sc::Relation>>,
) -> Result<LogicalPlan, Status> {
    let rel = rel
        .as_deref()
        .ok_or_else(|| inval("missing input relation"))?;
    to_plan(ctx, rel).await
}

async fn translate(ctx: &SessionContext, rel: &sc::Relation) -> Result<LogicalPlan, Status> {
    use sc::relation::RelType;
    let rt = rel
        .rel_type
        .as_ref()
        .ok_or_else(|| inval("empty relation"))?;
    match rt {
        RelType::Read(r) => read(ctx, r).await,
        RelType::Sql(s) => ctx
            .sql(&s.query)
            .await
            .map_err(|e| inval(format!("sql: {e}")))?
            .into_unoptimized_plan()
            .pipe(Ok),
        RelType::LocalRelation(lr) => local_relation(lr),
        RelType::Range(r) => range(ctx, r).await,
        RelType::Project(p) => {
            let input = child(ctx, &p.input).await?;
            let exprs = project_exprs(ctx, &input, &p.expressions)?;
            project_with_windows(input, exprs)
        }
        RelType::Filter(f) => {
            let input = child(ctx, &f.input).await?;
            let cond = to_expr(
                ctx,
                f.condition
                    .as_ref()
                    .ok_or_else(|| inval("filter.condition"))?,
                None,
            )?;
            build(LogicalPlanBuilder::from(input).filter(cond))
        }
        RelType::Aggregate(a) => aggregate(ctx, a).await,
        RelType::Sort(s) => {
            let input = child(ctx, &s.input).await?;
            let order = sort_exprs(ctx, &s.order)?;
            build(LogicalPlanBuilder::from(input).sort(order))
        }
        RelType::Limit(l) => {
            let input = child(ctx, &l.input).await?;
            build(LogicalPlanBuilder::from(input).limit(0, Some(l.limit as usize)))
        }
        RelType::Offset(o) => {
            let input = child(ctx, &o.input).await?;
            build(LogicalPlanBuilder::from(input).limit(o.offset as usize, None))
        }
        RelType::Tail(t) => {
            // No native tail; approximate as a limit (last-N semantics need full materialization).
            let input = child(ctx, &t.input).await?;
            build(LogicalPlanBuilder::from(input).limit(0, Some(t.limit as usize)))
        }
        RelType::Join(j) => join(ctx, j).await,
        RelType::SetOp(s) => set_op(ctx, s).await,
        RelType::Deduplicate(d) => deduplicate(ctx, d).await,
        RelType::SubqueryAlias(s) => {
            let input = child(ctx, &s.input).await?;
            build(LogicalPlanBuilder::from(input).alias(s.alias.clone()))
        }
        RelType::FillNa(f) => na_fill(ctx, f).await,
        RelType::DropNa(d) => na_drop(ctx, d).await,
        RelType::Replace(r) => na_replace(ctx, r).await,
        RelType::WithColumns(w) => with_columns(ctx, w).await,
        RelType::WithColumnsRenamed(w) => with_columns_renamed(ctx, w).await,
        RelType::Drop(d) => drop_columns(ctx, d).await,
        RelType::ToDf(t) => to_df(ctx, t).await,
        RelType::Unpivot(u) => unpivot(ctx, u).await,
        // Repartition hints set shuffle partition count for distributed routing.
        RelType::Repartition(r) => {
            if r.num_partitions > 0 {
                std::env::set_var("WEFT_SHUFFLE_PARTITIONS", r.num_partitions.to_string());
            }
            child(ctx, &r.input).await
        }
        RelType::RepartitionByExpression(r) => {
            if let Some(n) = r.num_partitions.filter(|&n| n > 0) {
                std::env::set_var("WEFT_SHUFFLE_PARTITIONS", n.to_string());
            }
            child(ctx, &r.input).await
        }
        RelType::Hint(h) => child(ctx, &h.input).await,
        RelType::Describe(d) => stat_describe(ctx, d).await,
        RelType::Summary(s) => stat_summary(ctx, s).await,
        RelType::Crosstab(c) => stat_crosstab(ctx, c).await,
        other => Err(Status::unimplemented(format!(
            "relation not supported yet: {}",
            rel_name(other)
        ))),
    }
}

fn rel_name(rt: &sc::relation::RelType) -> &'static str {
    // A short label for the unimplemented message (Debug is huge for nested relations).
    macro_rules! n {
        ($($v:ident),*) => { match rt { $(sc::relation::RelType::$v(_) => stringify!($v),)* _ => "Unknown" } };
    }
    n!(
        Read,
        Project,
        Filter,
        Join,
        SetOp,
        Sort,
        Limit,
        Aggregate,
        Sql,
        LocalRelation,
        Sample,
        Offset,
        Deduplicate,
        Range,
        SubqueryAlias,
        Repartition,
        ToDf,
        WithColumnsRenamed,
        ShowString,
        Drop,
        Tail,
        WithColumns,
        Hint,
        Unpivot,
        ToSchema,
        RepartitionByExpression,
        Unknown
    )
}

async fn read(ctx: &SessionContext, r: &sc::Read) -> Result<LogicalPlan, Status> {
    let _is_streaming = r.is_streaming;
    match r.read_type.as_ref().ok_or_else(|| inval("empty read"))? {
        sc::read::ReadType::NamedTable(t) => ctx
            .table(&t.unparsed_identifier)
            .await
            .map_err(|e| inval(format!("table `{}`: {e}", t.unparsed_identifier)))?
            .into_unoptimized_plan()
            .pipe(Ok),
        sc::read::ReadType::DataSource(d) => {
            // Path-based read: register the path's format and scan it. Covers parquet/csv/json.
            let path = d
                .paths
                .first()
                .ok_or_else(|| inval("data source: no path"))?;
            let fmt = d.format.as_deref().unwrap_or("parquet");
            let df = match fmt {
                "parquet" => ctx.read_parquet(path, Default::default()).await,
                "csv" => ctx.read_csv(path, Default::default()).await,
                "json" => ctx.read_json(path, Default::default()).await,
                other => {
                    return Err(Status::unimplemented(format!(
                        "data source format `{other}`"
                    )))
                }
            };
            df.map_err(|e| inval(format!("read {fmt} `{path}`: {e}")))?
                .into_unoptimized_plan()
                .pipe(Ok)
        }
    }
}

fn local_relation(lr: &sc::LocalRelation) -> Result<LogicalPlan, Status> {
    let data = lr.data.as_deref().unwrap_or_default();
    let reader = StreamReader::try_new(std::io::Cursor::new(data.to_vec()), None)
        .map_err(|e| inval(format!("local relation decode: {e}")))?;
    let schema = reader.schema();
    let batches = reader
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| inval(format!("local relation decode: {e}")))?;
    let schema = batches.first().map(|b| b.schema()).unwrap_or(schema);
    let mem = MemTable::try_new(schema, vec![batches])
        .map_err(|e| inval(format!("local relation memtable: {e}")))?;
    // Each inline relation gets a unique scan name so two of them (e.g. both sides of a join)
    // don't collide on unqualified column names.
    let n = LOCAL_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    build(LogicalPlanBuilder::scan(
        format!("spark_local_{n}"),
        provider_as_source(Arc::new(mem)),
        None,
    ))
}

async fn range(ctx: &SessionContext, r: &sc::Range) -> Result<LogicalPlan, Status> {
    let start = r.start.unwrap_or(0);
    let step = if r.step == 0 { 1 } else { r.step };
    // DataFusion's `range` table function yields a `value` column; Spark's range column is `id`.
    let sql = format!(
        "SELECT value AS id FROM range({start}, {end}, {step})",
        end = r.end
    );
    ctx.sql(&sql)
        .await
        .map_err(|e| inval(format!("range: {e}")))?
        .into_unoptimized_plan()
        .pipe(Ok)
}

/// Build a projection's expression list, expanding any `*` wildcard against the input schema.
fn project_exprs(
    ctx: &SessionContext,
    input: &LogicalPlan,
    exprs: &[sc::Expression],
) -> Result<Vec<Expr>, Status> {
    let mut out = Vec::with_capacity(exprs.len());
    for e in exprs {
        if is_wildcard(e) {
            out.extend(input.schema().columns().into_iter().map(Expr::Column));
        } else {
            out.push(to_expr(ctx, e, None)?);
        }
    }
    Ok(out)
}

async fn aggregate(ctx: &SessionContext, a: &sc::Aggregate) -> Result<LogicalPlan, Status> {
    let input = child(ctx, &a.input).await?;
    let group = a
        .grouping_expressions
        .iter()
        .map(|e| to_expr(ctx, e, None))
        .collect::<Result<Vec<_>, _>>()?;
    if a.group_type == sc::aggregate::GroupType::Pivot as i32 {
        let pivot = a.pivot.as_ref().ok_or_else(|| inval("pivot: no spec"))?;
        return pivot_aggregate(ctx, input, group, a, pivot).await;
    }
    // Spark's aggregate_expressions may repeat the grouping columns; DataFusion's aggregate adds
    // the group columns itself, so drop plain group-column refs from the aggregate list.
    let group_cols: Vec<String> = group.iter().map(|e| e.schema_name().to_string()).collect();
    let mut aggs = Vec::new();
    for e in &a.aggregate_expressions {
        let ex = to_expr(ctx, e, None)?;
        if group_cols.contains(&ex.schema_name().to_string()) {
            continue;
        }
        aggs.push(ex);
    }
    build(LogicalPlanBuilder::from(input).aggregate(group, aggs))
}

/// `df.groupBy(...).pivot(col, [values]).agg(...)`: when values are omitted, discovers distinct
/// pivot values from the input relation.
async fn pivot_aggregate(
    ctx: &SessionContext,
    input: LogicalPlan,
    group: Vec<Expr>,
    a: &sc::Aggregate,
    pivot: &sc::aggregate::Pivot,
) -> Result<LogicalPlan, Status> {
    let pivot_col = to_expr(
        ctx,
        pivot.col.as_ref().ok_or_else(|| inval("pivot.col"))?,
        None,
    )?;
    let mut values = pivot.values.clone();
    if values.is_empty() {
        let sub = Unparser::default()
            .plan_to_sql(&input)
            .map_err(|e| inval(format!("pivot unparse: {e}")))?;
        let col_name = pivot
            .col
            .as_ref()
            .and_then(|e| e.expr_type.as_ref())
            .and_then(|t| match t {
                sc::expression::ExprType::UnresolvedAttribute(u) => {
                    Some(u.unparsed_identifier.clone())
                }
                _ => None,
            })
            .unwrap_or_else(|| "pivot_col".into());
        let sql =
            format!("SELECT DISTINCT `{col_name}` AS v FROM ({sub}) AS _pivot_src ORDER BY v");
        let batches = ctx
            .sql(&sql)
            .await
            .map_err(|e| inval(format!("pivot distinct: {e}")))?
            .collect()
            .await
            .map_err(|e| inval(format!("pivot collect: {e}")))?;
        for b in batches {
            use datafusion::arrow::array::Array;
            use datafusion::scalar::ScalarValue;
            let arr = b.column(0);
            for i in 0..arr.len() {
                if !arr.is_null(i) {
                    let sv = ScalarValue::try_from_array(arr, i)
                        .map_err(|e| inval(format!("pivot value: {e}")))?;
                    values.push(scalar_to_spark_literal(&sv)?);
                }
            }
        }
        if values.is_empty() {
            return Err(inval("pivot: no distinct values found"));
        }
    }
    let aggs = a
        .aggregate_expressions
        .iter()
        .map(|e| to_expr(ctx, e, None))
        .collect::<Result<Vec<_>, _>>()?;

    let single = aggs.len() == 1;
    let labels: Vec<String> = aggs.iter().map(agg_label).collect();
    let mut out = Vec::new();
    for v in &values {
        let name = pivot_value_name(v);
        let filter = pivot_col.clone().eq(super::expr::literal(v)?);
        for (agg, label) in aggs.iter().zip(&labels) {
            // Strip any alias, set the per-value filter on the aggregate, then name the column.
            let filtered = with_filter(agg.clone().unalias(), filter.clone());
            let col_name = if single {
                name.clone()
            } else {
                format!("{name}_{label}")
            };
            out.push(filtered.alias(col_name));
        }
    }
    build(LogicalPlanBuilder::from(input).aggregate(group, out))
}

/// Set an aggregate's FILTER (so the pivot keeps only rows matching the value).
fn with_filter(agg: Expr, filter: Expr) -> Expr {
    if let Expr::AggregateFunction(mut af) = agg {
        af.params.filter = Some(Box::new(filter));
        Expr::AggregateFunction(af)
    } else {
        agg
    }
}

/// The name Spark uses for an aggregate in a multi-aggregate pivot column (its alias or display).
fn agg_label(e: &Expr) -> String {
    match e {
        Expr::Alias(a) => a.name.clone(),
        other => other.schema_name().to_string(),
    }
}

/// Build a Spark Connect literal from a DataFusion scalar (for pivot value discovery).
fn scalar_to_spark_literal(
    sv: &datafusion::scalar::ScalarValue,
) -> Result<sc::expression::Literal, Status> {
    use datafusion::scalar::ScalarValue;
    use sc::expression::literal::LiteralType as L;
    let literal_type = match sv {
        ScalarValue::Null => return Err(inval("pivot literal: null")),
        ScalarValue::Boolean(Some(b)) => Some(L::Boolean(*b)),
        ScalarValue::Int8(Some(v)) => Some(L::Byte(*v as i32)),
        ScalarValue::Int16(Some(v)) => Some(L::Short(*v as i32)),
        ScalarValue::Int32(Some(v)) => Some(L::Integer(*v)),
        ScalarValue::Int64(Some(v)) => Some(L::Long(*v)),
        ScalarValue::Float32(Some(v)) => Some(L::Float(*v)),
        ScalarValue::Float64(Some(v)) => Some(L::Double(*v)),
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => Some(L::String(s.clone())),
        ScalarValue::Date32(Some(d)) => Some(L::Date(*d)),
        ScalarValue::TimestampMicrosecond(Some(t), _)
        | ScalarValue::TimestampNanosecond(Some(t), _) => Some(L::Timestamp(*t)),
        other => return Err(inval(format!("pivot literal: unsupported {other:?}"))),
    };
    Ok(sc::expression::Literal {
        literal_type,
        ..Default::default()
    })
}

/// The output column name Spark gives a pivot value (its literal rendered as a string).
fn pivot_value_name(l: &sc::expression::Literal) -> String {
    use sc::expression::literal::LiteralType as L;
    match l.literal_type.as_ref() {
        Some(L::String(s)) => s.clone(),
        Some(L::Boolean(b)) => b.to_string(),
        Some(L::Byte(v)) => v.to_string(),
        Some(L::Short(v)) => v.to_string(),
        Some(L::Integer(v)) => v.to_string(),
        Some(L::Long(v)) => v.to_string(),
        Some(L::Float(v)) => v.to_string(),
        Some(L::Double(v)) => v.to_string(),
        _ => "null".to_string(),
    }
}

fn sort_exprs(
    ctx: &SessionContext,
    order: &[sc::expression::SortOrder],
) -> Result<Vec<SortExpr>, Status> {
    use sc::expression::sort_order::{NullOrdering, SortDirection};
    order
        .iter()
        .map(|o| {
            let e = to_expr(
                ctx,
                o.child.as_ref().ok_or_else(|| inval("sort.child"))?,
                None,
            )?;
            let asc = o.direction != SortDirection::Descending as i32;
            let nulls_first = match NullOrdering::try_from(o.null_ordering) {
                Ok(NullOrdering::SortNullsFirst) => true,
                Ok(NullOrdering::SortNullsLast) => false,
                _ => asc, // Spark default: nulls first for ASC, last for DESC
            };
            Ok(SortExpr::new(e, asc, nulls_first))
        })
        .collect()
}

async fn join(ctx: &SessionContext, j: &sc::Join) -> Result<LogicalPlan, Status> {
    use datafusion::logical_expr::JoinType;
    use sc::join::JoinType as SJ;
    let left = to_plan(ctx, j.left.as_deref().ok_or_else(|| inval("join.left"))?).await?;
    let right = to_plan(ctx, j.right.as_deref().ok_or_else(|| inval("join.right"))?).await?;
    let jt = match SJ::try_from(j.join_type).unwrap_or(SJ::Unspecified) {
        SJ::Inner | SJ::Unspecified => JoinType::Inner,
        SJ::LeftOuter => JoinType::Left,
        SJ::RightOuter => JoinType::Right,
        SJ::FullOuter => JoinType::Full,
        SJ::LeftAnti => JoinType::LeftAnti,
        SJ::LeftSemi => JoinType::LeftSemi,
        SJ::Cross => {
            return build(LogicalPlanBuilder::from(left).cross_join(right));
        }
    };
    if !j.using_columns.is_empty() {
        return join_using(left, right, jt, &j.using_columns);
    }
    let Some(cond) = j.join_condition.as_ref() else {
        return build(LogicalPlanBuilder::from(left).cross_join(right));
    };
    // When the condition resolves columns by `plan_id` (`df.a == df2.b`), alias both sides and map
    // each input's plan id to its alias so the columns bind unambiguously. When it uses explicit
    // qualified names instead (`col("a.x")` over user `.alias("a")`), leave the inputs as-is.
    if super::expr::uses_plan_id(cond) {
        let n = LOCAL_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let (ql, qr) = (format!("jl{n}"), format!("jr{n}"));
        let mut ids = super::expr::Ids::new();
        if let Some(id) = plan_id_of(&j.left) {
            ids.insert(id, ql.clone());
        }
        if let Some(id) = plan_id_of(&j.right) {
            ids.insert(id, qr.clone());
        }
        let left = aliased(left, ql)?;
        let right = aliased(right, qr)?;
        let on = to_expr(ctx, cond, Some(&ids))?;
        build(LogicalPlanBuilder::from(left).join_on(right, jt, [on]))
    } else {
        let on = to_expr(ctx, cond, None)?;
        build(LogicalPlanBuilder::from(left).join_on(right, jt, [on]))
    }
}

/// The Spark `plan_id` carried in a relation's common metadata, if any.
fn plan_id_of(rel: &Option<Box<sc::Relation>>) -> Option<i64> {
    rel.as_ref()?.common.as_ref().and_then(|c| c.plan_id)
}

/// Wrap a plan in a subquery alias so its columns carry `name` as their qualifier.
fn aliased(plan: LogicalPlan, name: String) -> Result<LogicalPlan, Status> {
    LogicalPlanBuilder::from(plan)
        .alias(name)
        .and_then(|b| b.build())
        .map_err(plan_err)
}

/// A join on shared column names (`df.join(other, "id")`). DataFusion's `join_using` keeps both
/// key columns; Spark coalesces them, so we equi-join on the qualified keys and project the key
/// once (left side) plus the rest of both inputs.
fn join_using(
    left: LogicalPlan,
    right: LogicalPlan,
    jt: datafusion::logical_expr::JoinType,
    keys: &[String],
) -> Result<LogicalPlan, Status> {
    // Give each side a distinct alias so the key columns and projection are unambiguous even when
    // both inputs carry the same (possibly unqualified) column names.
    let n = LOCAL_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let left = LogicalPlanBuilder::from(left)
        .alias(format!("jl{n}"))
        .and_then(|b| b.build())
        .map_err(plan_err)?;
    let right = LogicalPlanBuilder::from(right)
        .alias(format!("jr{n}"))
        .and_then(|b| b.build())
        .map_err(plan_err)?;
    let find = |plan: &LogicalPlan, name: &str| {
        plan.schema()
            .columns()
            .into_iter()
            .find(|c| c.name() == name)
            .ok_or_else(|| inval(format!("join key `{name}` not found")))
    };
    let mut on = Vec::new();
    for k in keys {
        let l = find(&left, k)?;
        let r = find(&right, k)?;
        on.push(Expr::Column(l).eq(Expr::Column(r)));
    }
    // Output: the key once (from the left), then left's other columns, then right's non-key columns.
    let mut proj: Vec<Expr> = Vec::new();
    for c in left.schema().columns() {
        proj.push(Expr::Column(c));
    }
    for c in right.schema().columns() {
        if !keys.iter().any(|k| k == c.name()) {
            proj.push(Expr::Column(c));
        }
    }
    LogicalPlanBuilder::from(left)
        .join_on(right, jt, on)
        .and_then(|b| b.project(proj))
        .and_then(|b| b.build())
        .map_err(plan_err)
}

async fn set_op(ctx: &SessionContext, s: &sc::SetOperation) -> Result<LogicalPlan, Status> {
    use sc::set_operation::SetOpType;
    let left = to_plan(
        ctx,
        s.left_input.as_deref().ok_or_else(|| inval("setop.left"))?,
    )
    .await?;
    let right = to_plan(
        ctx,
        s.right_input
            .as_deref()
            .ok_or_else(|| inval("setop.right"))?,
    )
    .await?;
    let all = s.is_all.unwrap_or(false);
    match SetOpType::try_from(s.set_op_type).unwrap_or(SetOpType::Unspecified) {
        SetOpType::Union if all => build(LogicalPlanBuilder::from(left).union(right)),
        SetOpType::Union => build(LogicalPlanBuilder::from(left).union_distinct(right)),
        SetOpType::Intersect => LogicalPlanBuilder::intersect(left, right, all).map_err(plan_err),
        SetOpType::Except => LogicalPlanBuilder::except(left, right, all).map_err(plan_err),
        SetOpType::Unspecified => Err(inval("unspecified set operation")),
    }
}

async fn deduplicate(ctx: &SessionContext, d: &sc::Deduplicate) -> Result<LogicalPlan, Status> {
    let input = child(ctx, &d.input).await?;
    if d.all_columns_as_keys.unwrap_or(false) || d.column_names.is_empty() {
        build(LogicalPlanBuilder::from(input).distinct())
    } else {
        let on: Vec<Expr> = d.column_names.iter().map(col).collect();
        let select = input
            .schema()
            .columns()
            .into_iter()
            .map(Expr::Column)
            .collect::<Vec<_>>();
        build(LogicalPlanBuilder::from(input).distinct_on(on, select, None))
    }
}

async fn with_columns(ctx: &SessionContext, w: &sc::WithColumns) -> Result<LogicalPlan, Status> {
    let input = child(ctx, &w.input).await?;
    let new: Vec<(String, Expr)> = w
        .aliases
        .iter()
        .map(|a| {
            let name = a
                .name
                .first()
                .cloned()
                .ok_or_else(|| inval("withColumn name"))?;
            let e = to_expr(
                ctx,
                a.expr.as_deref().ok_or_else(|| inval("withColumn expr"))?,
                None,
            )?;
            Ok((name.clone(), e.alias(name)))
        })
        .collect::<Result<_, Status>>()?;
    let replaced: Vec<&String> = new.iter().map(|(n, _)| n).collect();
    let mut proj: Vec<Expr> = input
        .schema()
        .columns()
        .into_iter()
        .filter(|c| !replaced.contains(&&c.name))
        .map(Expr::Column)
        .collect();
    proj.extend(new.into_iter().map(|(_, e)| e));
    project_with_windows(input, proj)
}

/// Project `exprs`, first lifting any window functions into a `Window` plan node (DataFusion's
/// physical planner requires window functions there, not inside a bare projection), then rewriting
/// each window sub-expression to reference its output column.
fn project_with_windows(input: LogicalPlan, exprs: Vec<Expr>) -> Result<LogicalPlan, Status> {
    use datafusion::logical_expr::utils::find_window_exprs;
    let window_exprs = find_window_exprs(&exprs);
    if window_exprs.is_empty() {
        return LogicalPlanBuilder::from(input)
            .project(exprs)
            .and_then(|b| b.build())
            .map_err(plan_err);
    }
    // The Window node appends one column per window expr after the input columns, in order — map
    // each window expr to that column by position (its display name resolves qualifiers, so a
    // name-based lookup would miss).
    let input_len = input.schema().fields().len();
    let plan = LogicalPlanBuilder::from(input)
        .window(window_exprs.clone())
        .and_then(|b| b.build())
        .map_err(plan_err)?;
    let out_cols = plan.schema().columns();
    let win_map: std::collections::HashMap<Expr, Expr> = window_exprs
        .into_iter()
        .enumerate()
        .map(|(i, we)| (we, Expr::Column(out_cols[input_len + i].clone())))
        .collect();
    let projected = exprs
        .into_iter()
        .map(|e| replace_windows(e, &win_map))
        .collect::<Result<Vec<_>, _>>()?;
    LogicalPlanBuilder::from(plan)
        .project(projected)
        .and_then(|b| b.build())
        .map_err(plan_err)
}

/// Replace each window-function sub-expression with its `Window`-node output column.
fn replace_windows(e: Expr, map: &std::collections::HashMap<Expr, Expr>) -> Result<Expr, Status> {
    use datafusion::common::tree_node::{Transformed, TreeNode};
    e.transform_up(|node| match map.get(&node) {
        Some(col) => Ok(Transformed::yes(col.clone())),
        None => Ok(Transformed::no(node)),
    })
    .map(|t| t.data)
    .map_err(plan_err)
}

async fn with_columns_renamed(
    ctx: &SessionContext,
    w: &sc::WithColumnsRenamed,
) -> Result<LogicalPlan, Status> {
    let input = child(ctx, &w.input).await?;
    // Newer clients use `renames`; older ones a `rename_columns_map`.
    let lookup = |name: &str| -> Option<String> {
        if let Some(r) = w.renames.iter().find(|r| r.col_name == name) {
            return Some(r.new_col_name.clone());
        }
        w.rename_columns_map.get(name).cloned()
    };
    let proj = input
        .schema()
        .columns()
        .into_iter()
        .map(|c| match lookup(&c.name) {
            Some(new) => Expr::Column(c).alias(new),
            None => Expr::Column(c),
        })
        .collect::<Vec<_>>();
    build(LogicalPlanBuilder::from(input).project(proj))
}

async fn drop_columns(ctx: &SessionContext, d: &sc::Drop) -> Result<LogicalPlan, Status> {
    let input = child(ctx, &d.input).await?;
    let mut drop: Vec<String> = d.column_names.clone();
    for e in &d.columns {
        if let Some(sc::expression::ExprType::UnresolvedAttribute(a)) = e.expr_type.as_ref() {
            drop.push(a.unparsed_identifier.clone());
        }
    }
    let proj = input
        .schema()
        .columns()
        .into_iter()
        .filter(|c| !drop.iter().any(|d| d == &c.name))
        .map(Expr::Column)
        .collect::<Vec<_>>();
    build(LogicalPlanBuilder::from(input).project(proj))
}

async fn to_df(ctx: &SessionContext, t: &sc::ToDf) -> Result<LogicalPlan, Status> {
    let input = child(ctx, &t.input).await?;
    let cols = input.schema().columns();
    if cols.len() != t.column_names.len() {
        return Err(inval(format!(
            "toDF: {} names for {} columns",
            t.column_names.len(),
            cols.len()
        )));
    }
    let proj = cols
        .into_iter()
        .zip(&t.column_names)
        .map(|(c, n)| Expr::Column(c).alias(n))
        .collect::<Vec<_>>();
    build(LogicalPlanBuilder::from(input).project(proj))
}

/// `df.unpivot(ids, values, var, val)` (melt): for each value column, project the ids plus a
/// `(var = "<col name>", val = <col>)` pair, then union them. Value columns default to all
/// non-id columns; they must share a common type (DataFusion's union coerces/errors like Spark).
async fn unpivot(ctx: &SessionContext, u: &sc::Unpivot) -> Result<LogicalPlan, Status> {
    let input = child(ctx, &u.input).await?;
    let ids = u
        .ids
        .iter()
        .map(|e| to_expr(ctx, e, None))
        .collect::<Result<Vec<_>, _>>()?;

    // Value columns: explicit list, or every column not used as an id.
    let value_exprs: Vec<Expr> = match u.values.as_ref() {
        Some(v) if !v.values.is_empty() => v
            .values
            .iter()
            .map(|e| to_expr(ctx, e, None))
            .collect::<Result<_, _>>()?,
        _ => {
            let id_names: Vec<String> = ids.iter().map(|e| e.schema_name().to_string()).collect();
            input
                .schema()
                .columns()
                .into_iter()
                .filter(|c| !id_names.iter().any(|n| n == c.name()))
                .map(Expr::Column)
                .collect()
        }
    };
    if value_exprs.is_empty() {
        return Err(inval("unpivot: no value columns"));
    }

    let mut acc: Option<LogicalPlan> = None;
    for ve in value_exprs {
        let vname = ve.schema_name().to_string();
        let mut proj = ids.clone();
        proj.push(lit(vname).alias(&u.variable_column_name));
        proj.push(ve.alias(&u.value_column_name));
        let part = LogicalPlanBuilder::from(input.clone())
            .project(proj)
            .and_then(|b| b.build())
            .map_err(plan_err)?;
        acc = Some(match acc {
            None => part,
            Some(prev) => LogicalPlanBuilder::from(prev)
                .union(part)
                .and_then(|b| b.build())
                .map_err(plan_err)?,
        });
    }
    acc.ok_or_else(|| inval("unpivot: empty"))
}

/// `df.na.fill(...)`: `coalesce(col, value)` for each targeted column whose type matches the
/// fill value's category (numeric value → numeric columns, etc.), per Spark semantics.
async fn na_fill(ctx: &SessionContext, f: &sc::NaFill) -> Result<LogicalPlan, Status> {
    use datafusion::execution::FunctionRegistry;
    let input = child(ctx, &f.input).await?;
    let targets = &f.cols;
    let coalesce = ctx.state().udf("coalesce").map_err(plan_err)?;
    let mut proj = Vec::new();
    for (field, c) in input.schema().fields().iter().zip(input.schema().columns()) {
        let name = field.name();
        let want = targets.is_empty() || targets.iter().any(|t| t == name);
        let lit = if f.values.len() == 1 {
            f.values.first()
        } else {
            targets
                .iter()
                .position(|t| t == name)
                .and_then(|i| f.values.get(i))
        };
        if let (true, Some(lit)) = (want, lit) {
            if let Some(val) = na_fill_value(field.data_type(), lit)? {
                proj.push(coalesce.call(vec![Expr::Column(c), val]).alias(name));
                continue;
            }
        }
        proj.push(Expr::Column(c));
    }
    build(LogicalPlanBuilder::from(input).project(proj))
}

/// The fill value cast to the column type, but only when the value's category matches the column
/// (so `fill(0)` touches numeric columns and `fill("x")` touches string columns, like Spark).
fn na_fill_value(
    dt: &datafusion::arrow::datatypes::DataType,
    l: &sc::expression::Literal,
) -> Result<Option<Expr>, Status> {
    use datafusion::arrow::datatypes::DataType;
    use sc::expression::literal::LiteralType as L;
    let lt = l
        .literal_type
        .as_ref()
        .ok_or_else(|| inval("na fill: empty literal"))?;
    let num = matches!(
        lt,
        L::Byte(_) | L::Short(_) | L::Integer(_) | L::Long(_) | L::Float(_) | L::Double(_)
    );
    let matches = (num && dt.is_numeric())
        || (matches!(lt, L::String(_))
            && matches!(
                dt,
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
            ))
        || (matches!(lt, L::Boolean(_)) && matches!(dt, DataType::Boolean));
    if !matches {
        return Ok(None);
    }
    let val = super::expr::literal(l)?;
    Ok(Some(Expr::Cast(datafusion::logical_expr::Cast::new(
        Box::new(val),
        dt.clone(),
    ))))
}

/// `df.na.drop(...)`: keep rows whose count of non-null values among the subset is at least
/// `min_non_nulls` (default = all of the subset, i.e. `how="any"`).
async fn na_drop(ctx: &SessionContext, d: &sc::NaDrop) -> Result<LogicalPlan, Status> {
    use datafusion::arrow::datatypes::DataType;
    let input = child(ctx, &d.input).await?;
    let cols: Vec<Expr> = if d.cols.is_empty() {
        input
            .schema()
            .columns()
            .into_iter()
            .map(Expr::Column)
            .collect()
    } else {
        d.cols.iter().map(col).collect()
    };
    let min = d.min_non_nulls.unwrap_or(cols.len() as i32) as i64;
    let non_null_count = cols
        .into_iter()
        .map(|c| {
            Expr::Cast(datafusion::logical_expr::Cast::new(
                Box::new(c.is_not_null()),
                DataType::Int64,
            ))
        })
        .reduce(|a, b| a + b)
        .ok_or_else(|| inval("na drop: no columns"))?;
    build(LogicalPlanBuilder::from(input).filter(non_null_count.gt_eq(lit(min))))
}

/// `df.na.replace(...)`: `CASE col WHEN old THEN new … ELSE col END` for each targeted column.
async fn na_replace(ctx: &SessionContext, r: &sc::NaReplace) -> Result<LogicalPlan, Status> {
    let input = child(ctx, &r.input).await?;
    let mut proj = Vec::new();
    for (field, c) in input.schema().fields().iter().zip(input.schema().columns()) {
        let name = field.name();
        let want = r.cols.is_empty() || r.cols.iter().any(|t| t == name);
        if want && !r.replacements.is_empty() {
            let when_then = r
                .replacements
                .iter()
                .map(|rep| {
                    let old = super::expr::literal(
                        rep.old_value.as_ref().ok_or_else(|| inval("replace.old"))?,
                    )?;
                    let new = super::expr::literal(
                        rep.new_value.as_ref().ok_or_else(|| inval("replace.new"))?,
                    )?;
                    Ok((Box::new(old), Box::new(new)))
                })
                .collect::<Result<Vec<_>, Status>>()?;
            let case = Expr::Case(datafusion::logical_expr::Case::new(
                Some(Box::new(Expr::Column(c.clone()))),
                when_then,
                Some(Box::new(Expr::Column(c))),
            ));
            proj.push(case.alias(name));
        } else {
            proj.push(Expr::Column(c));
        }
    }
    build(LogicalPlanBuilder::from(input).project(proj))
}

fn build(b: datafusion::error::Result<LogicalPlanBuilder>) -> Result<LogicalPlan, Status> {
    b.map_err(plan_err)?.build().map_err(plan_err)
}

/// `df.describe()` — per-column count/min/max/mean/stddev for numeric columns.
async fn stat_describe(ctx: &SessionContext, d: &sc::StatDescribe) -> Result<LogicalPlan, Status> {
    let input = child(ctx, &d.input).await?;
    let schema = input.schema();
    let cols: Vec<String> = if d.cols.is_empty() {
        schema.fields().iter().map(|f| f.name().clone()).collect()
    } else {
        d.cols.clone()
    };
    if cols.is_empty() {
        return Err(inval("StatDescribe: no columns"));
    }
    let sub = Unparser::default()
        .plan_to_sql(&input)
        .map_err(|e| inval(format!("describe unparse: {e}")))?
        .to_string();
    let mut parts = Vec::new();
    for summary in ["count", "mean", "stddev", "min", "max"] {
        let mut sel = vec![format!("'{summary}' AS summary")];
        for c in &cols {
            let field = schema
                .fields()
                .iter()
                .find(|f| f.name() == c)
                .ok_or_else(|| inval(format!("describe: unknown column `{c}`")))?;
            let expr = match (summary, field.data_type()) {
                ("count", _) => format!("count(`{c}`)"),
                ("mean", dt) if is_numeric(dt) => format!("avg(CAST(`{c}` AS DOUBLE))"),
                ("stddev", dt) if is_numeric(dt) => {
                    format!("stddev(CAST(`{c}` AS DOUBLE))")
                }
                ("min", _) => format!("min(`{c}`)"),
                ("max", _) => format!("max(`{c}`)"),
                _ => "NULL".to_string(),
            };
            sel.push(format!("{expr} AS `{c}`"));
        }
        parts.push(format!("SELECT {} FROM ({sub}) AS _t", sel.join(", ")));
    }
    let sql = parts.join(" UNION ALL ");
    ctx.sql(&sql)
        .await
        .map_err(plan_err)?
        .into_unoptimized_plan()
        .pipe(Ok)
}

/// `df.summary()` — extended statistics (subset of Spark's summary).
async fn stat_summary(ctx: &SessionContext, s: &sc::StatSummary) -> Result<LogicalPlan, Status> {
    let d = sc::StatDescribe {
        input: s.input.clone(),
        cols: s.statistics.clone(),
    };
    stat_describe(ctx, &d).await
}

/// `df.stat.crosstab(col1, col2)` — pivot count of col2 values per col1.
async fn stat_crosstab(ctx: &SessionContext, c: &sc::StatCrosstab) -> Result<LogicalPlan, Status> {
    let input = child(ctx, &c.input).await?;
    let sub = Unparser::default()
        .plan_to_sql(&input)
        .map_err(|e| inval(format!("crosstab unparse: {e}")))?
        .to_string();
    let sql = format!(
        "SELECT `{c1}`, `{c2}`, count(*) AS n FROM ({sub}) AS _t GROUP BY `{c1}`, `{c2}`",
        c1 = c.col1,
        c2 = c.col2
    );
    ctx.sql(&sql)
        .await
        .map_err(plan_err)?
        .into_unoptimized_plan()
        .pipe(Ok)
}

fn is_numeric(dt: &datafusion::arrow::datatypes::DataType) -> bool {
    use datafusion::arrow::datatypes::DataType;
    matches!(
        dt,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float16
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(_, _)
    )
}

fn plan_err(e: datafusion::error::DataFusionError) -> Status {
    Status::invalid_argument(format!("plan: {e}"))
}

/// Returns true when the relation tree contains a streaming read.
pub fn relation_is_streaming(rel: &sc::Relation) -> bool {
    use sc::relation::RelType;
    let Some(rt) = rel.rel_type.as_ref() else {
        return false;
    };
    match rt {
        RelType::Read(r) => r.is_streaming,
        RelType::Project(p) => p.input.as_ref().is_some_and(|i| relation_is_streaming(i)),
        RelType::Filter(f) => f.input.as_ref().is_some_and(|i| relation_is_streaming(i)),
        RelType::Aggregate(a) => a.input.as_ref().is_some_and(|i| relation_is_streaming(i)),
        RelType::Join(j) => {
            j.left.as_ref().is_some_and(|l| relation_is_streaming(l))
                || j.right.as_ref().is_some_and(|r| relation_is_streaming(r))
        }
        RelType::Sort(s) => s.input.as_ref().is_some_and(|i| relation_is_streaming(i)),
        RelType::Limit(l) => l.input.as_ref().is_some_and(|i| relation_is_streaming(i)),
        RelType::SetOp(u) => {
            u.left_input
                .as_ref()
                .is_some_and(|l| relation_is_streaming(l))
                || u.right_input
                    .as_ref()
                    .is_some_and(|r| relation_is_streaming(r))
        }
        RelType::SubqueryAlias(s) => s.input.as_ref().is_some_and(|i| relation_is_streaming(i)),
        _ => false,
    }
}

/// Tiny `.pipe()` so the `ctx.sql(...).into_unoptimized_plan().pipe(Ok)` reads top-to-bottom.
trait Pipe: Sized {
    fn pipe<R>(self, f: impl FnOnce(Self) -> R) -> R {
        f(self)
    }
}
impl<T> Pipe for T {}

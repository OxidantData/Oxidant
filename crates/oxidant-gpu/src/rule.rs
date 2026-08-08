//! [`GpuOffloadRule`]: the conservative physical optimizer rule that swaps a
//! scan + conjunctive-filter + group-by-aggregate subtree for [`GpuScanAggExec`].
//!
//! Matches ONLY this shape (TPC-H Q1/Q6) and nothing else:
//!
//! ```text
//! AggregateExec mode=Final|FinalPartitioned|Single
//!   └ (AggregateExec mode=Partial — required unless Single)
//!   └ FilterExec                       — zero or more, every conjunct supported
//!   └ ProjectionExec                   — bare columns keeping their names, plus
//!                                        CSE-shared arithmetic trees (alias
//!                                        `__common_expr_N`), substituted back
//!                                        into the aggregation inputs that
//!                                        reference them
//!   └ CoalescePartitionsExec / RepartitionExec — pass-through
//!   └ DataSourceExec over FileScanConfig with a ParquetSource
//!        — local file:// URL, one or more part files (KAN-75), no pushed
//!          limit, no partition values, no file ranges; a pushed-down scan
//!          predicate must also consist only of supported conjuncts
//! ```
//!
//! plus: every group key is a bare column, every aggregate is
//! sum/avg/count/min/max over a bare column OR an arithmetic (+,-,*,/) tree over
//! columns and supported literals (KAN-76 — emitted as `derived_columns`;
//! `count(*)`/`count(1)` allowed), nothing DISTINCT, no in-aggregate ORDER BY,
//! no grouping sets, and every referenced column has a shim-supported dtype.
//!
//! On match the whole subtree is replaced by one [`GpuScanAggExec`] carrying the
//! final aggregate's schema (the GPU computes FINAL results; anything above —
//! projection, ORDER BY, LIMIT — stays as CPU nodes, and the rule is registered
//! before `EnforceDistribution` so distribution/sort enforcement still applies
//! above the replacement). On ANY deviation the plan is returned unchanged.

use std::any::Any;
use std::collections::HashSet;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{Result, ScalarValue};
use datafusion::datasource::physical_plan::{FileScanConfig, ParquetSource};
use datafusion::datasource::source::DataSourceExec;
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::expressions::{BinaryExpr, CastExpr, Column, Literal};
use datafusion::physical_expr::utils::split_conjunction;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::ExecutionPlan;

use crate::exec::GpuScanAggExec;
use crate::spec::{
    AggFunc, AggSpec, ArithOp, CmpOp, ColumnSpec, DerivedColumn, FilterSpec, GpuExpr, GpuOpSpec,
    LiteralSpec, LiteralType,
};

/// `&Arc<dyn ExecutionPlan>` → `&dyn Any` via trait upcasting (DataFusion 54
/// dropped the old `as_any` methods; `ExecutionPlan: Any` makes this a coercion).
fn upcast_plan(plan: &Arc<dyn ExecutionPlan>) -> &dyn Any {
    plan.as_ref()
}

fn downcast_plan<T: 'static>(plan: &Arc<dyn ExecutionPlan>) -> Option<&T> {
    upcast_plan(plan).downcast_ref()
}

/// Same upcast for physical expressions (`PhysicalExpr: Any`).
fn downcast_expr<T: 'static>(expr: &Arc<dyn PhysicalExpr>) -> Option<&T> {
    let any: &dyn Any = expr.as_ref();
    any.downcast_ref()
}

/// The offload rule. Cheap to construct; stateless.
#[derive(Debug, Default)]
pub struct GpuOffloadRule;

impl PhysicalOptimizerRule for GpuOffloadRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|p| match try_gpu_plan(&p) {
            Some(exec) => Ok(Transformed::yes(Arc::new(exec) as Arc<dyn ExecutionPlan>)),
            None => Ok(Transformed::no(p)),
        })
        .data()
    }

    fn name(&self) -> &str {
        "gpu_offload"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Extract a [`GpuOpSpec`] from `plan` iff it matches the offloadable shape exactly;
/// `None` on any deviation (the rule then leaves the plan untouched).
fn try_gpu_plan(plan: &Arc<dyn ExecutionPlan>) -> Option<GpuScanAggExec> {
    let agg = downcast_plan::<AggregateExec>(plan)?;
    if !matches!(
        agg.mode(),
        AggregateMode::Final | AggregateMode::FinalPartitioned | AggregateMode::Single
    ) {
        return None;
    }

    // Walk from the final aggregate to the scan through the whitelisted node types,
    // collecting FilterExec predicates on the way.
    let mut filters: Vec<Arc<dyn PhysicalExpr>> = Vec::new();
    // Computed entries of pass-through projections (DataFusion's common-subexpr
    // elimination materializes shared aggregate inputs as `__common_expr_N`):
    // alias → arithmetic tree, leaves already resolved to base columns.
    let mut common_exprs: Vec<(String, GpuExpr)> = Vec::new();
    let mut partial_agg: Option<&AggregateExec> = None;
    let mut node: &Arc<dyn ExecutionPlan> = agg.input();
    let scan: &DataSourceExec = loop {
        let any = upcast_plan(node);
        if let Some(a) = any.downcast_ref::<AggregateExec>() {
            if *a.mode() != AggregateMode::Partial || partial_agg.is_some() {
                return None;
            }
            partial_agg = Some(a);
            node = a.input();
        } else if let Some(f) = any.downcast_ref::<FilterExec>() {
            filters.push(f.predicate().clone());
            node = f.input();
        } else if let Some(p) = any.downcast_ref::<ProjectionExec>() {
            for pe in p.expr() {
                if let Some(c) = downcast_expr::<Column>(&pe.expr) {
                    // Bare column keeping its own name — ProjectionPushdown
                    // leaves one behind until its later passes: everything
                    // here is extracted by NAME from the file schema, so a
                    // subset/reorder is a no-op for the spec. A rename is not
                    // offloadable.
                    if c.name() != pe.alias {
                        return None;
                    }
                } else {
                    // A computed entry: only offloadable as an arithmetic
                    // tree (KAN-76); aggregate inputs referencing the alias
                    // get it substituted back.
                    let expr = gpu_expr(&pe.expr, &common_exprs)?;
                    common_exprs.push((pe.alias.clone(), expr));
                }
            }
            node = p.input();
        } else if any.is::<CoalescePartitionsExec>() || any.is::<RepartitionExec>() {
            let children = node.children();
            if children.len() != 1 {
                return None;
            }
            node = children[0];
        } else if let Some(s) = any.downcast_ref::<DataSourceExec>() {
            break s;
        } else {
            return None;
        }
    };

    // The scan must be local parquet part files with nothing else going on: any
    // number of files across any number of file groups (KAN-75), all local with
    // no partition values, no file ranges, and no pushed limit.
    let scan_any: &dyn Any = scan.data_source().as_ref();
    let cfg: &FileScanConfig = scan_any.downcast_ref()?;
    let source_any: &dyn Any = cfg.file_source.as_ref();
    source_any.downcast_ref::<ParquetSource>()?;
    if !cfg.object_store_url.as_str().starts_with("file://") {
        return None;
    }
    if cfg.limit.is_some() {
        return None;
    }
    let mut files: Vec<String> = Vec::new();
    for group in &cfg.file_groups {
        for file in group.iter() {
            if !file.partition_values.is_empty() {
                return None;
            }
            if file.range.is_some() {
                // A byte-range read would make the shim (which reads whole files)
                // scan different rows than the CPU plan.
                return None;
            }
            // object_store::path::Path strips the leading '/', but the shim opens
            // paths with plain filesystem calls — restore the absolute form (our
            // local tables are always registered with absolute LOCATIONs).
            files.push(format!("/{}", file.object_meta.location));
        }
    }
    if files.is_empty() {
        return None;
    }
    // Kept for display/back-compat; the shim scans `files`.
    let table_path = files[0].clone();
    // A predicate pushed into the scan by FilterPushdown must be extracted too —
    // silently dropping it would aggregate rows the query filters out.
    if let Some(pushed) = cfg.file_source.filter() {
        filters.push(pushed);
    }

    // Group keys come from the partial aggregate (they reference source columns);
    // for Single mode the one aggregate node plays both roles.
    let src_agg: &AggregateExec = partial_agg.unwrap_or(agg);
    if src_agg.aggr_expr().is_empty() {
        return None;
    }
    if !src_agg.group_expr().null_expr().is_empty() {
        return None; // grouping sets / rollup
    }
    let mut group_by: Vec<String> = Vec::new();
    for (expr, alias) in src_agg.group_expr().expr() {
        let col: &Column = downcast_expr(expr)?;
        // The shim names its output group columns per `group_by`, which must equal
        // the declared output schema's names — only true for un-aliased keys.
        if col.name() != alias {
            return None;
        }
        group_by.push(col.name().to_string());
    }

    // Aggregations: function + source column from the partial aggregate, output
    // alias from the final one (paired by position — they are built 1:1). An
    // input that is not a bare column must be an arithmetic tree over columns
    // and literals (KAN-76), interned as a `_gpu_derived_N` column.
    if agg.aggr_expr().len() != src_agg.aggr_expr().len() {
        return None;
    }
    // (serialized expr, definition, base columns) — identical expressions share
    // one derived column, so the shim never evaluates the same tree twice.
    let mut derived: Vec<(String, DerivedColumn, Vec<String>)> = Vec::new();
    let mut aggregations: Vec<AggSpec> = Vec::new();
    for (final_expr, src_expr) in agg.aggr_expr().iter().zip(src_agg.aggr_expr().iter()) {
        let func = match src_expr.fun().name() {
            "sum" => AggFunc::Sum,
            "avg" => AggFunc::Avg,
            "count" => AggFunc::Count,
            "min" => AggFunc::Min,
            "max" => AggFunc::Max,
            _ => return None, // approx_distinct, stddev, array_agg, ...
        };
        if src_expr.fun().name() != final_expr.fun().name()
            || src_expr.is_distinct()
            || !src_expr.order_bys().is_empty()
        {
            return None;
        }
        let args = src_expr.expressions();
        let col = match args.as_slice() {
            [] if func == AggFunc::Count => None,
            [arg] => {
                // Numeric casts are transparent (e.g. avg over an Int64 column
                // plans as `avg(CAST(l_quantity AS Float64))` — still a bare
                // column input, no derived column needed).
                let arg = unwrap_numeric_casts(arg)?;
                if let Some(c) = downcast_expr::<Column>(arg) {
                    match common_exprs.iter().find(|(alias, _)| alias == c.name()) {
                        // A CSE'd shared input: aggregate the substituted tree as
                        // a derived column like any other expression input.
                        Some((_, sub)) => Some(intern_derived(&mut derived, sub.clone())?),
                        None => Some(c.name().to_string()),
                    }
                } else if func == AggFunc::Count && downcast_expr::<Literal>(arg).is_some() {
                    None // count(*) plans as count(1)
                } else {
                    let expr = gpu_expr(arg, &common_exprs)?;
                    Some(intern_derived(&mut derived, expr)?)
                }
            }
            _ => return None,
        };
        aggregations.push(AggSpec {
            func,
            col,
            alias: final_expr.name().to_string(),
        });
    }

    // Filters: every conjunct of every predicate must be col <op> literal.
    let mut filter_specs: Vec<FilterSpec> = Vec::new();
    for predicate in &filters {
        for conjunct in split_conjunction(predicate) {
            filter_specs.push(cmp_filter(conjunct)?);
        }
    }

    // Column read set: filter columns + group keys + aggregation inputs, deduped
    // in first-use order, each with a shim-supported dtype from the file schema.
    // An aggregation pointing at a derived column contributes that expression's
    // BASE columns instead (the shim computes the derived column itself).
    let file_schema = cfg.file_source.table_schema().file_schema();
    let mut read_names: Vec<&str> = filter_specs
        .iter()
        .map(|f| f.col.as_str())
        .chain(group_by.iter().map(|g| g.as_str()))
        .collect();
    for a in &aggregations {
        let Some(col) = a.col.as_deref() else {
            continue;
        };
        match derived.iter().find(|(_, d, _)| d.name == col) {
            Some((_, _, base_cols)) => read_names.extend(base_cols.iter().map(|c| c.as_str())),
            None => read_names.push(col),
        }
    }
    let mut seen: HashSet<&str> = HashSet::new();
    let mut columns: Vec<ColumnSpec> = Vec::new();
    for name in read_names {
        if !seen.insert(name) {
            continue;
        }
        let field = file_schema.field_with_name(name).ok()?;
        columns.push(ColumnSpec {
            name: name.to_string(),
            dtype: dtype_str(field.data_type())?,
        });
    }

    let spec = GpuOpSpec {
        table_path,
        files,
        columns,
        derived_columns: derived.into_iter().map(|(_, d, _)| d).collect(),
        filters: filter_specs,
        group_by,
        aggregations,
    };
    Some(GpuScanAggExec::new(spec, plan.schema()))
}

/// One conjunct as `col <op> literal` (literal may sit on the right, or on the
/// left with the operator flipped). Anything else is not offloadable.
fn cmp_filter(expr: &Arc<dyn PhysicalExpr>) -> Option<FilterSpec> {
    let binary: &BinaryExpr = downcast_expr(expr)?;
    let op = match binary.op() {
        Operator::Lt => CmpOp::Lt,
        Operator::LtEq => CmpOp::LtEq,
        Operator::Gt => CmpOp::Gt,
        Operator::GtEq => CmpOp::GtEq,
        Operator::Eq => CmpOp::Eq,
        Operator::NotEq => CmpOp::NotEq,
        _ => return None,
    };
    if let (Some(col), Some(lit)) = (
        downcast_expr::<Column>(binary.left()),
        literal_value(binary.right()),
    ) {
        return Some(FilterSpec {
            col: col.name().to_string(),
            op,
            literal: literal_spec(lit)?,
        });
    }
    if let (Some(lit), Some(col)) = (
        literal_value(binary.left()),
        downcast_expr::<Column>(binary.right()),
    ) {
        return Some(FilterSpec {
            col: col.name().to_string(),
            op: flip(op),
            literal: literal_spec(lit)?,
        });
    }
    None
}

/// The literal side of a comparison, unwrapping any casts the planner wrapped
/// around it (type coercion folds them into the literal's own type, but a
/// pushed-down scan predicate may carry an explicit `CAST`).
fn literal_value(expr: &Arc<dyn PhysicalExpr>) -> Option<&ScalarValue> {
    let mut current = expr;
    loop {
        if let Some(lit) = downcast_expr::<Literal>(current) {
            return Some(lit.value());
        }
        if let Some(cast) = downcast_expr::<CastExpr>(current) {
            current = cast.expr();
            continue;
        }
        return None;
    }
}

/// `literal <op> col` ↔ `col <flipped op> literal`.
fn flip(op: CmpOp) -> CmpOp {
    match op {
        CmpOp::Lt => CmpOp::Gt,
        CmpOp::LtEq => CmpOp::GtEq,
        CmpOp::Gt => CmpOp::Lt,
        CmpOp::GtEq => CmpOp::LtEq,
        CmpOp::Eq => CmpOp::Eq,
        CmpOp::NotEq => CmpOp::NotEq,
    }
}

/// A cast between shim-supported numeric types is transparent: the shim computes
/// in float64 anyway (see `exec::coerce_shim_batch`), so `CAST(x AS Float64)` and
/// decimal-alignment casts change nothing row-wise. Casts to any other type are
/// NOT transparent (string/date semantics would change) and refuse.
fn unwrap_numeric_casts(mut expr: &Arc<dyn PhysicalExpr>) -> Option<&Arc<dyn PhysicalExpr>> {
    while let Some(cast) = downcast_expr::<CastExpr>(expr) {
        if !is_numeric_dtype(cast.cast_type()) {
            return None;
        }
        expr = cast.expr();
    }
    Some(expr)
}

/// Shim-supported numeric dtypes — the transparent-cast vocabulary.
fn is_numeric_dtype(dt: &DataType) -> bool {
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
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
    )
}

/// KAN-76: an aggregate input that is not a bare column must be an arithmetic
/// (+,-,*,/) tree whose leaves are columns and/or supported literals, modulo
/// transparent numeric casts ([`unwrap_numeric_casts`]). Anything else —
/// function calls, non-numeric casts, subqueries — is not offloadable.
///
/// `common` maps CSE projection aliases (`__common_expr_N`) to their already
/// extracted trees: a column leaf naming one is substituted inline.
fn gpu_expr(expr: &Arc<dyn PhysicalExpr>, common: &[(String, GpuExpr)]) -> Option<GpuExpr> {
    let expr = unwrap_numeric_casts(expr)?;
    if let Some(c) = downcast_expr::<Column>(expr) {
        if let Some((_, sub)) = common.iter().find(|(alias, _)| alias == c.name()) {
            return Some(sub.clone());
        }
        return Some(GpuExpr::Col {
            col: c.name().to_string(),
        });
    }
    if let Some(v) = literal_value(expr) {
        return Some(GpuExpr::Lit {
            lit: literal_spec(v)?,
        });
    }
    let binary: &BinaryExpr = downcast_expr(expr)?;
    let op = match binary.op() {
        Operator::Plus => ArithOp::Add,
        Operator::Minus => ArithOp::Sub,
        Operator::Multiply => ArithOp::Mul,
        Operator::Divide => ArithOp::Div,
        _ => return None,
    };
    Some(GpuExpr::Arith {
        op,
        lhs: Box::new(gpu_expr(binary.left(), common)?),
        rhs: Box::new(gpu_expr(binary.right(), common)?),
    })
}

/// Intern `expr` as a `_gpu_derived_N` column: identical expressions (same
/// serialized form) reuse the same name; each entry also records its base
/// columns so the read set can include them. `None` only on a serialization
/// failure (never expected — the tree is plain data).
fn intern_derived(
    derived: &mut Vec<(String, DerivedColumn, Vec<String>)>,
    expr: GpuExpr,
) -> Option<String> {
    let key = serde_json::to_string(&expr).ok()?;
    if let Some((_, d, _)) = derived.iter().find(|(k, _, _)| *k == key) {
        return Some(d.name.clone());
    }
    let name = format!("_gpu_derived_{}", derived.len());
    let mut base_cols = Vec::new();
    collect_cols(&expr, &mut base_cols);
    derived.push((
        key,
        DerivedColumn {
            name: name.clone(),
            expr,
        },
        base_cols,
    ));
    Some(name)
}

/// Every column leaf of a derived expression, left to right (dedup happens at
/// the read-set level).
fn collect_cols(expr: &GpuExpr, out: &mut Vec<String>) {
    match expr {
        GpuExpr::Col { col } => out.push(col.clone()),
        GpuExpr::Lit { .. } => {}
        GpuExpr::Arith { lhs, rhs, .. } => {
            collect_cols(lhs, out);
            collect_cols(rhs, out);
        }
    }
}

/// A scalar literal → the shim's typed-value vocabulary. Nulls are not offloadable.
fn literal_spec(value: &ScalarValue) -> Option<LiteralSpec> {
    if value.is_null() {
        return None;
    }
    let (ty, text) = match value {
        // arrow's Display for decimal scalars prints the raw parts
        // ("Some(5),15,2"), not the number — format it ourselves so the shim
        // can parse the value with std::stod.
        ScalarValue::Decimal128(Some(v), _p, s) => (LiteralType::Decimal, decimal_string(*v, *s)),
        ScalarValue::Int8(_)
        | ScalarValue::Int16(_)
        | ScalarValue::Int32(_)
        | ScalarValue::Int64(_)
        | ScalarValue::UInt8(_)
        | ScalarValue::UInt16(_)
        | ScalarValue::UInt32(_)
        | ScalarValue::UInt64(_) => (LiteralType::Int, value.to_string()),
        ScalarValue::Float32(_) | ScalarValue::Float64(_) => {
            (LiteralType::Float, value.to_string())
        }
        ScalarValue::Utf8(_) | ScalarValue::LargeUtf8(_) | ScalarValue::Utf8View(_) => {
            (LiteralType::String, value.to_string())
        }
        ScalarValue::Date32(_) | ScalarValue::Date64(_) => (LiteralType::Date, value.to_string()),
        ScalarValue::TimestampSecond(..)
        | ScalarValue::TimestampMillisecond(..)
        | ScalarValue::TimestampMicrosecond(..)
        | ScalarValue::TimestampNanosecond(..) => (LiteralType::Timestamp, value.to_string()),
        _ => return None,
    };
    Some(LiteralSpec { ty, value: text })
}

/// Render a scaled decimal integer as a plain decimal string
/// (e.g. 5 with scale 2 → "0.05", -2400 with scale 2 → "-24.00").
fn decimal_string(v: i128, scale: i8) -> String {
    if scale <= 0 {
        return format!("{}", v * 10i128.pow((-scale) as u32));
    }
    let neg = v < 0;
    let digits = format!("{:0>width$}", v.abs(), width = scale as usize + 1);
    let (int_part, frac_part) = digits.split_at(digits.len() - scale as usize);
    format!("{}{}.{}", if neg { "-" } else { "" }, int_part, frac_part)
}

/// The shim's column dtype vocabulary; `None` → the query stays on CPU.
fn dtype_str(dt: &DataType) -> Option<String> {
    Some(match dt {
        DataType::Int8 => "int8".to_string(),
        DataType::Int16 => "int16".to_string(),
        DataType::Int32 => "int32".to_string(),
        DataType::Int64 => "int64".to_string(),
        DataType::UInt8 => "uint8".to_string(),
        DataType::UInt16 => "uint16".to_string(),
        DataType::UInt32 => "uint32".to_string(),
        DataType::UInt64 => "uint64".to_string(),
        DataType::Float32 => "float32".to_string(),
        DataType::Float64 => "float64".to_string(),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "string".to_string(),
        DataType::Boolean => "bool".to_string(),
        DataType::Date32 => "date32".to_string(),
        DataType::Date64 => "date64".to_string(),
        // cudf timestamps are timezone-less.
        DataType::Timestamp(unit, None) => match unit {
            TimeUnit::Second => "timestamp(s)".to_string(),
            TimeUnit::Millisecond => "timestamp(ms)".to_string(),
            TimeUnit::Microsecond => "timestamp(us)".to_string(),
            TimeUnit::Nanosecond => "timestamp(ns)".to_string(),
        },
        DataType::Decimal128(p, s) => format!("decimal128({p},{s})"),
        DataType::Decimal256(p, s) => format!("decimal256({p},{s})"),
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decimal_literals_render_as_numbers() {
        assert_eq!(decimal_string(5, 2), "0.05");
        assert_eq!(decimal_string(7, 2), "0.07");
        assert_eq!(decimal_string(2400, 2), "24.00");
        assert_eq!(decimal_string(-2400, 2), "-24.00");
        assert_eq!(decimal_string(123, 0), "123");
        assert_eq!(decimal_string(12, 5), "0.00012");
        let lit = literal_spec(&ScalarValue::Decimal128(Some(5), 15, 2)).unwrap();
        assert!(matches!(lit.ty, LiteralType::Decimal));
        assert_eq!(lit.value, "0.05");
    }
}

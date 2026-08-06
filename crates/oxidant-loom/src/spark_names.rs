//! Spark-compatible output column naming for the top result projection.
//!
//! A drop-in Spark replacement must return the same *result column names* as Spark — BI tools,
//! `df.columns`, and `CREATE TABLE AS` all depend on them. DataFusion's auto-generated names for
//! anonymous (un-aliased) projection expressions diverge from Spark's `Expression.sql` /
//! `toPrettySQL` output in cosmetic-but-load-bearing ways:
//!
//! | divergence                         | DataFusion           | Spark            |
//! |------------------------------------|----------------------|------------------|
//! | string / numeric / bool literals   | `Utf8("x")`,`Int64(1)`| `x`, `1`, `true`|
//! | qualified column refs              | `t.a`                | `a`              |
//! | the array constructor              | `make_array(…)`      | `array(…)`       |
//! | binary operations                  | `a = 1`              | `(a = 1)`        |
//! | function argument separator        | `f(a,b)`             | `f(a, b)`        |
//! | binary / date literals             | `Binary("…")`        | `X'…'`, `DATE '…'`|
//!
//! [`project_spark_names`] reads the **top** projection's expressions (descending through the
//! result-shaping wrappers `Sort`/`Limit`/`Distinct` that preserve their input's columns) and, when
//! any column's Spark name differs from DataFusion's, wraps the whole plan in one **outer**
//! renaming projection: `SELECT col0 AS spark0, col1 AS spark1, … FROM (original plan)`.
//!
//! Wrapping rather than mutating the inner projection is the load-bearing correctness choice: a
//! `Sort`/`Filter`/CTE above the projection references its output columns *by name*, so renaming
//! those columns in place breaks resolution (`ORDER BY 1`, `GROUP BY ALL`, window plans all fail).
//! The outer projection leaves the inner plan byte-identical and only renames the final output, so
//! nothing internal can break. Explicit user aliases (`SELECT a AS foo`) are passed through
//! unchanged. Only names change; types and row order are untouched (projection is 1:1, order-
//! preserving). If the rename would produce duplicate output names (`SELECT 1, 1` → two `1`
//! columns, which DataFusion projections forbid) we leave the plan as-is — those rows simply keep
//! DataFusion's names rather than regress.
//!
//! This runs on the production `Engine::sql` / `Engine::schema` path (not a comparison-time hack),
//! so the Spark-parity gain is a real drop-in-compatibility improvement, not a measurement artifact.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::common::{Column, ScalarValue};
use datafusion::logical_expr::expr::{AggregateFunction, ScalarFunction};
use datafusion::logical_expr::{Aggregate, Distinct, Expr, LogicalPlan, Projection};

/// Map from an `Aggregate` node's output field name (DataFusion's `schema_name`, e.g. `count(*)`,
/// `sum(t.x)`) to the aggregate expression that produced it. Built for the aggregate that feeds the
/// top result projection so a bare `Column` referencing an aggregate output can be re-rendered with
/// Spark's name (`count(1)`, `sum(x)`) instead of DataFusion's. Empty when the projection isn't fed
/// by an aggregate (the common non-grouped case), which makes every lookup a safe miss.
type AggMap<'a> = HashMap<String, &'a Expr>;

/// Wrap `plan` in an outer projection that renames anonymous output columns to their Spark names.
/// Returns the plan unchanged when there's nothing to rename, when the root isn't a recognized
/// projection-bearing shape, or when the rename would collide on output names (all safe no-ops).
pub fn project_spark_names(plan: LogicalPlan) -> LogicalPlan {
    let Some(proj) = top_projection(&plan) else {
        return plan;
    };
    let schema = plan.schema();
    // The wrappers above the projection preserve its schema, so the root's output fields line up
    // 1:1 with the projection's expressions. If they somehow don't, bail rather than mis-map.
    if schema.fields().len() != proj.expr.len() {
        return plan;
    }

    // When the projection is fed by an aggregate (`SELECT k, count(*) … GROUP BY k` plans as
    // `Projection → … → Aggregate`), the projection references each aggregate as a *bare* `Column`
    // named `count(*)`/`sum(t.x)`; the real `AggregateFunction` lives in the child `Aggregate` node.
    // Map those output names to their exprs so `render` can produce Spark's `count(1)`/`sum(x)`.
    let agg = aggregate_output_map(&proj.input);

    let mut outer: Vec<Expr> = Vec::with_capacity(proj.expr.len());
    let mut seen: HashSet<String> = HashSet::with_capacity(proj.expr.len());
    let mut changed = false;
    for (i, pe) in proj.expr.iter().enumerate() {
        let (qualifier, field) = schema.qualified_field(i);
        let col = Expr::Column(Column::new(qualifier.cloned(), field.name()));
        // Reference the existing output column; rename it only if it's anonymous and its Spark
        // name differs. A genuine *user* alias (`SELECT a AS foo`, `AS \`Simple WHEN\``,
        // `AS \`NULLIF(a.i,b.i)\``) keeps its chosen name; DataFusion also wraps some anonymous
        // projection items (notably scalar-function calls) in an *internal* alias to their own
        // auto-name, which we must still rename. The robust discriminator (per the regression
        // swarm): an internal auto-alias's name equals DataFusion's own auto-name of the inner expr
        // — `a.name == a.expr.schema_name()`. The lone exception is an auto-alias whose name embeds
        // a *nested* alias artifact (` AS `, e.g. our `array`→`make_array` rename inside
        // `array_contains(c, array(1,2,3))`), which recomputed `schema_name` spells differently; a
        // user alias never contains ` AS `, so that artifact is also a safe auto-alias signal.
        let render_target = match pe {
            Expr::Alias(a) => {
                let is_auto = a.name == a.expr.schema_name().to_string() || a.name.contains(" AS ");
                is_auto.then(|| a.expr.as_ref())
            }
            other => Some(other),
        };
        let out_name = match render_target {
            None => field.name().to_string(),
            Some(target) => {
                let spark = render(target, &agg);
                if spark != *field.name() {
                    changed = true;
                }
                spark
            }
        };
        // Duplicate output names would make `Projection::try_new` reject the plan — Spark permits
        // them but DataFusion doesn't, so bail and keep DataFusion's (distinct) names instead.
        if !seen.insert(out_name.clone()) {
            return plan;
        }
        outer.push(if out_name == *field.name() {
            col
        } else {
            col.alias(out_name)
        });
    }

    if !changed {
        return plan;
    }
    let input = Arc::new(plan);
    match Projection::try_new(outer, Arc::clone(&input)) {
        Ok(p) => LogicalPlan::Projection(p),
        // Unreachable in practice (columns come from the input schema and names are unique), but
        // never panic the engine: fall back to the original plan.
        Err(_) => (*input).clone(),
    }
}

/// The first `Projection` reached from the plan root by descending only through result-shaping
/// wrappers that pass their input's columns through unchanged. `None` if the root isn't such a
/// shape (e.g. a bare `Aggregate`, `Union`, `SubqueryAlias`, or `Distinct::On`).
fn top_projection(plan: &LogicalPlan) -> Option<&Projection> {
    match plan {
        LogicalPlan::Projection(p) => Some(p),
        LogicalPlan::Sort(s) => top_projection(&s.input),
        LogicalPlan::Limit(l) => top_projection(&l.input),
        LogicalPlan::Distinct(Distinct::All(input)) => top_projection(input),
        _ => None,
    }
}

/// Build the [`AggMap`] for the `Aggregate` that feeds `plan` (the top projection's input), if any.
/// Each aggregate expression is keyed by the field name it produces in the aggregate's output schema
/// (DataFusion's `schema_name`), which is exactly the name the top projection references it by.
fn aggregate_output_map(plan: &LogicalPlan) -> AggMap<'_> {
    let mut map = AggMap::new();
    if let Some(aggr) = find_aggregate(plan) {
        for e in &aggr.aggr_expr {
            map.insert(e.schema_name().to_string(), e);
        }
    }
    map
}

/// The `Aggregate` node directly feeding the top projection, reached by descending only through
/// single-input column-preserving wrappers (`Filter` = HAVING, plus `Sort`/`Limit`/`Distinct`/
/// `SubqueryAlias`). Stops at anything else (another `Projection`, a join, a union, a bare scan) so
/// we never resolve a projection column against an aggregate it doesn't actually read from.
fn find_aggregate(plan: &LogicalPlan) -> Option<&Aggregate> {
    match plan {
        LogicalPlan::Aggregate(a) => Some(a),
        LogicalPlan::Filter(f) => find_aggregate(&f.input),
        LogicalPlan::Sort(s) => find_aggregate(&s.input),
        LogicalPlan::Limit(l) => find_aggregate(&l.input),
        LogicalPlan::Distinct(Distinct::All(input)) => find_aggregate(input),
        LogicalPlan::SubqueryAlias(s) => find_aggregate(&s.input),
        _ => None,
    }
}

/// Spark's auto-generated output name for a top-projection expression, with no aggregate context.
/// A test-only convenience over [`render`]; production naming goes through [`render`] directly with
/// the real aggregate map (see [`project_spark_names`]).
#[cfg(test)]
fn spark_expr_name(e: &Expr) -> String {
    render(e, &AggMap::new())
}

/// Recursive Spark-`prettyName` renderer. Variants we model explicitly are rendered Spark-style;
/// anything else falls back to DataFusion's own schema name (safe — that column simply keeps its
/// current name and stays in the `schema-only` bucket rather than regressing).
fn render(e: &Expr, agg: &AggMap) -> String {
    match e {
        // An `Alias` reached *inside* an expression is an internal artifact (e.g. the
        // `array`→`make_array` alias wraps its call), never a user alias — render the inner expr.
        Expr::Alias(a) => render(&a.expr, agg),
        // A bare column that names an aggregate output (`count(*)`, `sum(t.x)`) is re-rendered as the
        // aggregate expression itself so it gets Spark's name (`count(1)`, `sum(x)`); otherwise it's
        // an ordinary (group-by / scan) column, printed unqualified as Spark does.
        Expr::Column(c) => agg
            .get(c.name.as_str())
            .and_then(|ae| render_aggregate_ref(ae, agg))
            .unwrap_or_else(|| c.name.clone()),
        Expr::Literal(v, _) => render_literal(v),
        // Spark fully parenthesizes binary operations, including nested ones.
        Expr::BinaryExpr(b) => {
            format!(
                "({} {} {})",
                render(&b.left, agg),
                b.op,
                render(&b.right, agg)
            )
        }
        // Spark (like Postgres) omits coercion casts from the column name, except for typed NULL
        // literals where Spark prints `CAST(NULL AS <type>)`.
        Expr::Cast(c) => {
            if matches!(&*c.expr, Expr::Literal(ScalarValue::Null, _)) {
                format!("CAST(NULL AS {})", spark_type_name(c.field.data_type()))
            } else {
                render(&c.expr, agg)
            }
        }
        Expr::TryCast(c) => {
            if matches!(&*c.expr, Expr::Literal(ScalarValue::Null, _)) {
                format!("CAST(NULL AS {})", spark_type_name(c.field.data_type()))
            } else {
                render(&c.expr, agg)
            }
        }
        Expr::Negative(x) => format!("(- {})", render(x, agg)),
        Expr::Not(x) => format!("NOT {}", render(x, agg)),
        Expr::IsNull(x) => format!("{} IS NULL", render(x, agg)),
        Expr::IsNotNull(x) => format!("{} IS NOT NULL", render(x, agg)),
        // Spark names a `CASE` expression `CASE [<expr>] WHEN <c> THEN <v> … [ELSE <v>] END`, with
        // each branch rendered Spark-style (literal wrappers stripped, binary ops parenthesized) —
        // e.g. `CASE WHEN (1 < 2) THEN 1 ELSE (1 / 0) END`. DataFusion's own name keeps the
        // `Int64(1)`-style wrappers, so without this the column stays in `schema-only`.
        Expr::Case(c) => {
            let mut s = String::from("CASE");
            if let Some(operand) = &c.expr {
                s.push_str(&format!(" {}", render(operand, agg)));
            }
            for (when, then) in &c.when_then_expr {
                s.push_str(&format!(
                    " WHEN {} THEN {}",
                    render(when, agg),
                    render(then, agg)
                ));
            }
            if let Some(els) = &c.else_expr {
                s.push_str(&format!(" ELSE {}", render(els, agg)));
            }
            s.push_str(" END");
            s
        }
        Expr::ScalarFunction(sf) => render_scalar_fn(sf, agg),
        // An aggregate reached directly (rare — projections normally reference it by column) renders
        // the same way; fall back to the DataFusion name if it's an unsupported aggregate shape.
        Expr::AggregateFunction(af) => {
            render_aggregate(af, agg).unwrap_or_else(|| e.schema_name().to_string())
        }
        _ => e.schema_name().to_string(),
    }
}

/// Render an aggregate expression (possibly wrapped in the synthetic alias DataFusion attaches to
/// `count(*)`) Spark-style. `None` for any non-aggregate or unsupported shape, so the caller falls
/// back to the bare column / DataFusion name and the column just stays in `schema-only`.
fn render_aggregate_ref(ae: &Expr, agg: &AggMap) -> Option<String> {
    match ae {
        Expr::AggregateFunction(af) => render_aggregate(af, agg),
        Expr::Alias(a) => render_aggregate_ref(&a.expr, agg),
        _ => None,
    }
}

/// Render an [`AggregateFunction`] as Spark names it: `prettyName([DISTINCT ]arg, …)` with the
/// arguments rendered Spark-style (so `count(*)`'s `Int64(1)` arg prints as `1` → `count(1)`,
/// `count(t.a)` → `count(a)`), then an optional `FILTER (WHERE …)`. Bails (`None`) on ordered-set
/// (`WITHIN GROUP`) and null-treatment (`IGNORE NULLS`) shapes whose Spark spelling we don't model,
/// so those keep DataFusion's name rather than risk a wrong one.
fn render_aggregate(af: &AggregateFunction, agg: &AggMap) -> Option<String> {
    let p = &af.params;
    if !p.order_by.is_empty() || p.null_treatment.is_some() {
        return None;
    }
    let distinct = if p.distinct { "DISTINCT " } else { "" };
    let args = p
        .args
        .iter()
        .map(|a| render(a, agg))
        .collect::<Vec<_>>()
        .join(", ");
    let mut s = format!("{}({distinct}{args})", af.func.name());
    if let Some(filter) = &p.filter {
        s.push_str(&format!(" FILTER (WHERE {})", render(filter, agg)));
    }
    Some(s)
}

/// Render a scalar function call Spark-style: `prettyName(arg, arg, …)` with comma-**space**
/// separators (DataFusion uses no space) and the `make_array`→`array` constructor rename.
fn render_scalar_fn(sf: &ScalarFunction, agg: &AggMap) -> String {
    // Spark's `If` expression prints as `(IF(predicate, trueValue, falseValue))` — uppercased and
    // wrapped in an outer pair of parens (unlike an ordinary function). oxidant lowers `if` to a
    // `CASE` for execution, but the un-optimized projection still carries the `if` ScalarFunction
    // at naming time, so this is where the Spark column name is produced.
    if sf.func.name() == "if" && sf.args.len() == 3 {
        let args = sf
            .args
            .iter()
            .map(|a| render(a, agg))
            .collect::<Vec<_>>()
            .join(", ");
        return format!("(IF({args}))");
    }
    // oxidant wraps the divisor of a decimal/float `/` or `%` in the internal identity guard
    // `spark_nonzero_divisor` so a zero divisor raises DIVIDE_BY_ZERO like Spark ANSI. The guard is a
    // pure pass-through, so it is invisible in the Spark column name — render it as just its argument,
    // making the enclosing `(left / right)` / `(left % right)` name byte-identical to the unwrapped op.
    if sf.func.name() == "spark_nonzero_divisor" && sf.args.len() == 1 {
        return render(&sf.args[0], agg);
    }
    // oxidant lowers a Spark literal-zero integral `/` (e.g. `1/0`) to the internal `spark_divide`
    // UDF so the column is statically DOUBLE while still raising DIVIDE_BY_ZERO on an actual zero
    // divisor. Spark names it as the ordinary `(left / right)` division it was written as, so
    // render it that way — the operand coercion casts are stripped by `render`, exactly as for a
    // plain `BinaryExpr` divide.
    if sf.func.name() == "spark_divide" && sf.args.len() == 2 {
        return format!(
            "({} / {})",
            render(&sf.args[0], agg),
            render(&sf.args[1], agg)
        );
    }
    // oxidant lowers an integral `*` with a `bigint` result to the internal `spark_checked_mul` UDF so
    // an Int64 overflow raises ARITHMETIC_OVERFLOW like Spark ANSI. Spark names it as the ordinary
    // `(left * right)` multiply it was written as, so render it that way — the operand coercion casts
    // are stripped by `render`, exactly as for a plain `BinaryExpr` multiply.
    if sf.func.name() == "spark_checked_mul" && sf.args.len() == 2 {
        return format!(
            "({} * {})",
            render(&sf.args[0], agg),
            render(&sf.args[1], agg)
        );
    }
    // Spark names `from_json`'s output column with *only* the JSON argument — the schema string and
    // the optional options map are dropped (`JsonToStructs.prettyName`): `from_json({"a":1})`.
    if sf.func.name() == "from_json" && !sf.args.is_empty() {
        return format!("from_json({})", render(&sf.args[0], agg));
    }
    // Spark prints `regexp_like`/`regexp`/`rlike` (the `RLike` expression) with the UPPERCASE
    // `REGEXP_LIKE` prettyName: `REGEXP_LIKE(1a 2b 14m, \d+b)`.
    if matches!(sf.func.name(), "regexp_like" | "regexp" | "rlike") && sf.args.len() == 2 {
        return format!(
            "REGEXP_LIKE({}, {})",
            render(&sf.args[0], agg),
            render(&sf.args[1], agg)
        );
    }
    // Spark's `RegExpInStr.prettyName` carries the optional `idx` argument (default `0`) in the
    // name even when the call omits it: `regexp_instr(abc, b)` → column `regexp_instr(abc, b, 0)`.
    if sf.func.name() == "regexp_instr" && sf.args.len() == 2 {
        return format!(
            "regexp_instr({}, {}, 0)",
            render(&sf.args[0], agg),
            render(&sf.args[1], agg)
        );
    }
    // Spark's cast-alias constructors (`int(x)`, `double(x)`, `decimal(x)`, …) name their column
    // after the *child*, exactly like an explicit `CAST(x AS T)` (Spark omits the cast from the
    // name): `SELECT int(1)` → column `1`. oxidant lowers these to a `Cast` for execution, but the
    // un-optimized projection still carries the constructor `ScalarFunction` at naming time, so the
    // Spark column name is produced here.
    if sf.args.len() == 1
        && crate::spark_functions::spark_cast_constructors::CAST_ALIAS_NAMES
            .contains(&sf.func.name())
    {
        return render(&sf.args[0], agg);
    }
    // Spark `positive(x)` (`UnaryPositive`) prints as `(+ x)`.
    if sf.func.name() == "positive" && sf.args.len() == 1 {
        return format!("(+ {})", render(&sf.args[0], agg));
    }
    let name = match sf.func.name() {
        "make_array" => "array",
        other => other,
    };
    let args = sf
        .args
        .iter()
        .map(|a| render(a, agg))
        .collect::<Vec<_>>()
        .join(", ");
    format!("{name}({args})")
}

/// Spark SQL type spelling for CAST display names.
fn spark_type_name(dt: &datafusion::arrow::datatypes::DataType) -> String {
    use datafusion::arrow::datatypes::DataType;
    match dt {
        DataType::Int32 => "int".into(),
        DataType::Int64 => "bigint".into(),
        DataType::Float32 => "float".into(),
        DataType::Float64 => "double".into(),
        DataType::Boolean => "boolean".into(),
        DataType::Utf8 | DataType::LargeUtf8 => "string".into(),
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => "binary".into(),
        DataType::Date32 | DataType::Date64 => "date".into(),
        DataType::Timestamp(_, tz) => {
            if tz.is_some() {
                "timestamp".into()
            } else {
                "timestamp_ntz".into()
            }
        }
        DataType::Decimal128(p, s) => format!("decimal({p},{s})"),
        other => other.to_string(),
    }
}

/// Render a literal the way Spark names it: bare value text, full-length `X'…'` for binary,
/// `DATE '…'` for dates. (Typed-null `CAST(NULL AS T)` spelling is deferred to a later increment;
/// a bare `NULL` here can only leave such columns in `schema-only`, never regress a passing one.)
fn render_literal(v: &ScalarValue) -> String {
    if v.is_null() {
        return "NULL".to_string();
    }
    match v {
        ScalarValue::Binary(Some(b))
        | ScalarValue::LargeBinary(Some(b))
        | ScalarValue::BinaryView(Some(b))
        | ScalarValue::FixedSizeBinary(_, Some(b)) => spark_hex(b),
        ScalarValue::Date32(Some(_)) | ScalarValue::Date64(Some(_)) => {
            // `ScalarValue`'s `Display` already renders the ISO date; Spark wraps it in `DATE '…'`.
            format!("DATE '{v}'")
        }
        // For the common scalars (`Utf8`, `Int*`, `Float*`, `Boolean`, …) `ScalarValue`'s `Display`
        // is already the bare Spark form (`x`, `11`, `2.1`, `true`); the `Utf8("x")` wrapper oxidant
        // emits today comes from `Expr`'s debug-style display, which we bypass here.
        _ => v.to_string(),
    }
}

/// Uppercase `X'…'` hex spelling of a binary literal (Spark's `Literal.sql` for `BinaryType`).
/// Unlike `ScalarValue`'s `Display`, this renders the full byte string (Display truncates to 10).
fn spark_hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2 + 3);
    s.push_str("X'");
    for b in bytes {
        s.push_str(&format!("{b:02X}"));
    }
    s.push('\'');
    s
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::DataType;
    use datafusion::logical_expr::{col, lit, Cast, Operator};

    fn binary(l: Expr, op: Operator, r: Expr) -> Expr {
        Expr::BinaryExpr(datafusion::logical_expr::BinaryExpr::new(
            Box::new(l),
            op,
            Box::new(r),
        ))
    }

    #[test]
    fn renders_typed_null_cast() {
        assert_eq!(
            spark_expr_name(&Expr::Cast(Cast::new(
                Box::new(Expr::Literal(ScalarValue::Null, None)),
                DataType::Int32,
            ))),
            "CAST(NULL AS int)"
        );
    }

    #[test]
    fn strips_literal_type_wrappers() {
        assert_eq!(spark_expr_name(&lit("x")), "x");
        assert_eq!(spark_expr_name(&lit(11i64)), "11");
        assert_eq!(spark_expr_name(&lit(true)), "true");
    }

    #[test]
    fn unqualifies_columns() {
        assert_eq!(spark_expr_name(&col("testdata.a")), "a");
        assert_eq!(spark_expr_name(&col("a")), "a");
    }

    #[test]
    fn parenthesizes_binary_ops() {
        assert_eq!(
            spark_expr_name(&binary(col("a"), Operator::Eq, lit(1i64))),
            "(a = 1)"
        );
        // nested: (a + (b / 2))
        let inner = binary(col("b"), Operator::Divide, lit(2i64));
        let outer = binary(col("a"), Operator::Plus, inner);
        assert_eq!(spark_expr_name(&outer), "(a + (b / 2))");
    }

    #[test]
    fn renders_binary_literal_full_hex() {
        let v = ScalarValue::Binary(Some(vec![0x45, 0x61, 0x00, 0xFF]));
        assert_eq!(render_literal(&v), "X'456100FF'");
    }

    use datafusion::functions_aggregate::count::count_udaf;
    use datafusion::functions_aggregate::sum::sum_udaf;
    use datafusion::logical_expr::expr::AggregateFunctionParams;

    fn agg(name: &str, args: Vec<Expr>, distinct: bool, filter: Option<Expr>) -> Expr {
        let func = match name {
            "count" => count_udaf(),
            "sum" => sum_udaf(),
            _ => unreachable!(),
        };
        Expr::AggregateFunction(AggregateFunction {
            func,
            params: AggregateFunctionParams {
                args,
                distinct,
                filter: filter.map(Box::new),
                order_by: vec![],
                null_treatment: None,
            },
        })
    }

    #[test]
    fn renders_aggregate_columns_spark_style() {
        // `count(*)` is built by DataFusion as `count(Int64(1))` aliased to "count(*)"; Spark names
        // both `count(*)` and `count(1)` as `count(1)`.
        let count_star = agg("count", vec![lit(1i64)], false, None).alias("count(*)");
        // `count(testdata.b)` → Spark `count(b)` (unqualified).
        let count_col = agg("count", vec![col("testdata.b")], false, None);
        // `count(DISTINCT 1)` keeps the keyword.
        let count_distinct = agg("count", vec![lit(1i64)], true, None);
        // `sum(t.power)` → `sum(power)`.
        let sum_col = agg("sum", vec![col("t.power")], false, None);

        let map: AggMap = [
            ("count(*)".to_string(), &count_star),
            (count_col.schema_name().to_string(), &count_col),
            (count_distinct.schema_name().to_string(), &count_distinct),
            (sum_col.schema_name().to_string(), &sum_col),
        ]
        .into_iter()
        .collect();

        let render_ref = |name: &str| render(&col(name), &map);
        assert_eq!(render_ref("count(*)"), "count(1)");
        assert_eq!(render_ref(&count_col.schema_name().to_string()), "count(b)");
        assert_eq!(
            render_ref(&count_distinct.schema_name().to_string()),
            "count(DISTINCT 1)"
        );
        assert_eq!(render_ref(&sum_col.schema_name().to_string()), "sum(power)");

        // A column not in the aggregate map keeps its (already-unqualified) bare name.
        assert_eq!(render(&col("k"), &map), "k");
        // An aggregate nested inside a binary op resolves through the map too: `(count(b) * 3)`.
        let count_name = count_col.schema_name().to_string();
        let expr = binary(col(count_name), Operator::Multiply, lit(3i64));
        assert_eq!(render(&expr, &map), "(count(b) * 3)");
    }
}

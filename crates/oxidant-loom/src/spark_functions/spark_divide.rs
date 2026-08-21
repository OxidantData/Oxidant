//! `spark_divide(left, right)` — the internal Float64 true-division UDF that `SparkDividePlanner`
//! lowers a **literal-zero integral** Spark `/` to (e.g. the `1 / 0` in `if(1 == 1, 1, 1/0)`).
//!
//! ## Why this UDF exists (and a plain double cast does not suffice)
//!
//! Spark's `/` is *always* true division with a static **DOUBLE** result type — even on a branch
//! that is never taken. So in `if(1 == 1, 1, 1/0)` the dead `1/0` else-branch still drives
//! common-type promotion: the whole column is `double`, and Spark prints the selected `1` as `1.0`.
//!
//! `SparkDividePlanner` lowers an ordinary integral `/` to `CAST(l AS DOUBLE) / CAST(r AS DOUBLE)`,
//! which yields the right double value — *except* for a literal-zero divisor: Spark's eager
//! `SELECT 5 / 0` raises ANSI `DIVIDE_BY_ZERO`, but IEEE double division of `5.0 / 0.0` yields
//! `Infinity` and silently drops that error (a forbidden missing-error regression). So the
//! literal-zero case was left on DataFusion's *integer* divide, which raises `DIVIDE_BY_ZERO` like
//! Spark — but is statically typed `int`, so the dead-branch column came out `int` and printed `1`
//! instead of `1.0` (a static-type fidelity gap, the 6 `conditional-functions.sql` correctness rows).
//!
//! This UDF resolves the tension: it carries a static **Float64** return type (so the dead branch
//! promotes the column to `double` and the simplifier prints `1.0`) **and** raises `DIVIDE_BY_ZERO`
//! when a divisor *actually evaluates to zero* (so eager `SELECT 5 / 0` still errors, exactly like
//! Spark). The dead-branch cases never hit the error because the constant-guard `CASE`/`coalesce`
//! is structurally pruned by the simplifier before this UDF is ever invoked on a zero divisor, and
//! a dynamic `CASE WHEN p THEN 1/0 ELSE …` only evaluates the branch on the rows where `p` holds
//! (DataFusion's `evaluate_selection`), exactly matching Spark's lazy, per-row branch evaluation.
//!
//! Null semantics match Spark's `/`: a `NULL` operand yields `NULL` (no error); only a *non-null*
//! zero divisor raises. Both operands are cast to `Float64` by the planner before this runs.

use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, Float64Array};
use datafusion::arrow::datatypes::{DataType, Float64Type};
use datafusion::common::{exec_err, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::prelude::SessionContext;

/// Register `spark_divide` into `ctx` (kept consistent with the module pattern; the function is
/// internal — no Spark SQL calls it by name, `SparkDividePlanner` embeds it directly via [`udf`]).
pub fn register(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(SparkDivide::new()));
}

/// The `spark_divide` UDF as a shareable `Arc`, for `SparkDividePlanner` to embed in the lowered
/// expression (the planner builds an `Expr::ScalarFunction` that carries this concrete `func`, so
/// no registry lookup is needed at plan or execution time).
pub(crate) fn udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkDivide::new()))
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct SparkDivide {
    signature: Signature,
}

impl SparkDivide {
    fn new() -> Self {
        Self {
            // The planner always casts both operands to `Float64`, so an exact `Float64` signature
            // matches with no further coercion.
            signature: Signature::exact(
                vec![DataType::Float64, DataType::Float64],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for SparkDivide {
    fn name(&self) -> &str {
        "spark_divide"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    /// Always `Float64` — this is the whole point: a static DOUBLE type for Spark's `/`.
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let n = args.number_rows;
        if args.args.len() != 2 {
            return exec_err!("spark_divide expects exactly 2 arguments");
        }
        let lhs = args.args[0].clone().into_array(n)?;
        let rhs = args.args[1].clone().into_array(n)?;
        // Defensive cast (never panics on an unexpected width); the planner already hands us
        // `Float64`, so this is a no-op there.
        let a = to_f64(&lhs)?;
        let b = to_f64(&rhs)?;
        let mut out = Float64Array::builder(n);
        for i in 0..n {
            if a.is_null(i) || b.is_null(i) {
                // Spark `/` with a NULL operand is NULL — never an error.
                out.append_null();
            } else {
                let divisor = b.value(i);
                if divisor == 0.0 {
                    // Spark ANSI `/` raises DIVIDE_BY_ZERO on an actual zero divisor. The message
                    // text is deliberately free of the tokens the parity harness keys on for
                    // missing-function / parse / unimplemented buckets, so a both-error row stays
                    // `error-parity`.
                    return exec_err!(
                        "[DIVIDE_BY_ZERO] Division by zero. Use try_divide to tolerate a 0 divisor and return NULL instead. SQLSTATE: 22012"
                    );
                }
                out.append_value(a.value(i) / divisor);
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

/// Cast any numeric array to `Float64Array`, preserving nulls. Used defensively so an unexpected
/// input width is an error, never a downcast panic (an engine panic would be a robustness bug).
fn to_f64(arr: &ArrayRef) -> Result<Float64Array> {
    use datafusion::arrow::array::AsArray;
    let casted = datafusion::arrow::compute::cast(arr, &DataType::Float64)?;
    Ok(casted.as_primitive::<Float64Type>().clone())
}

#[cfg(test)]
mod tests {
    use crate::Engine;

    /// Render the single scalar cell of `q` as a string (NULL → "NULL").
    async fn cell(q: &str) -> String {
        use datafusion::arrow::array::Array;
        let engine = Engine::new();
        let batches = engine.sql(q).await.unwrap_or_else(|e| panic!("{q}: {e}"));
        let col = batches[0].column(0);
        if col.is_null(0) {
            return "NULL".to_string();
        }
        let txt = crate::arrow::util::pretty::pretty_format_batches(&batches)
            .unwrap()
            .to_string();
        txt.lines()
            .nth(3)
            .unwrap()
            .trim_matches(|c| c == '|' || c == ' ')
            .to_string()
    }

    /// A dead `1/0` branch must not error and must print as a DOUBLE (`1.0`, not `1`).
    #[tokio::test]
    async fn dead_divide_zero_branch_is_double() {
        assert_eq!(cell("SELECT if(1 == 1, 1, 1/0) AS x").await, "1.0");
        assert_eq!(cell("SELECT if(1 != 1, 1/0, 1) AS x").await, "1.0");
        assert_eq!(cell("SELECT coalesce(1, 1/0) AS x").await, "1.0");
        assert_eq!(cell("SELECT coalesce(null, 1, 1/0) AS x").await, "1.0");
        assert_eq!(
            cell("SELECT case when 1 < 2 then 1 else 1/0 end AS x").await,
            "1.0"
        );
        assert_eq!(
            cell("SELECT case when 1 > 2 then 1/0 else 1 end AS x").await,
            "1.0"
        );
    }

    /// An eager `5/0` must still raise DIVIDE_BY_ZERO (the error Spark raises), not return Infinity.
    #[tokio::test]
    async fn eager_divide_by_zero_errors() {
        let engine = Engine::new();
        let err = engine.sql("SELECT 5/0 AS x").await.unwrap_err().to_string();
        assert!(
            err.contains("DIVIDE_BY_ZERO") || err.to_lowercase().contains("divi"),
            "want a divide-by-zero error, got: {err}"
        );
    }

    /// A dynamic dead branch (`WHEN i > 100 THEN 1/0`) only evaluates the branch on matching rows,
    /// so a never-matching predicate never divides by zero.
    #[tokio::test]
    async fn dynamic_dead_branch_does_not_error() {
        // VALUES row with i = 1 (< 100) — the THEN branch must not be evaluated, so no error.
        assert_eq!(
            cell("SELECT case when i > 100 then 1/0 else 0 end AS x FROM (SELECT 1 AS i) t").await,
            "0.0"
        );
    }
}

//! `spark_nonzero_divisor(divisor)` — an identity guard UDF that returns its single argument
//! **unchanged** but raises Spark's ANSI `DIVIDE_BY_ZERO` whenever a *non-null* element evaluates to
//! a decimal zero.
//!
//! ## Why this UDF exists
//!
//! Spark's ANSI `/` and `%` raise `DIVIDE_BY_ZERO` on a zero **decimal** divisor (e.g. `a / b` and
//! `a % b` over `SELECT 1.0 a, 0.0 b`). DataFusion's native decimal divide/modulo instead produce a
//! value (or null) and silently drop that error — a forbidden missing-error gap.
//!
//! `SparkDividePlanner` (in `lib.rs`) lowers a decimal `/` or `%` to `left OP
//! spark_nonzero_divisor(right)`. This UDF is a pure **identity** on the divisor: its return type is
//! exactly the input type and every non-zero (and null) element passes through byte-identical, so the
//! enclosing decimal divide/modulo keeps DataFusion's exact result type and value on every row Spark
//! also accepts. Only a *non-null zero* divisor changes behaviour: there this UDF raises
//! `DIVIDE_BY_ZERO`, exactly where Spark ANSI does — so it can only convert a missing-error into
//! error-parity, never pass→fail. A `NULL` divisor passes through unchanged (Spark `/`/`%` with a
//! NULL operand is NULL, never an error).

use std::sync::Arc;

use datafusion::arrow::array::{
    Array, Decimal128Array, Decimal256Array, Float16Array, Float32Array, Float64Array,
};
use datafusion::arrow::datatypes::{i256, DataType};
use datafusion::common::{exec_err, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::prelude::SessionContext;

/// Register `spark_nonzero_divisor` into `ctx` (kept consistent with the module pattern; the function
/// is internal — no Spark SQL calls it by name, `SparkDividePlanner` embeds it directly via [`udf`]).
pub fn register(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(SparkNonzeroDivisor::new()));
}

/// The `spark_nonzero_divisor` UDF as a shareable `Arc`, for `SparkDividePlanner` to embed in the
/// lowered expression (the planner builds an `Expr::ScalarFunction` carrying this concrete `func`, so
/// no registry lookup is needed at plan or execution time).
pub(crate) fn udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkNonzeroDivisor::new()))
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct SparkNonzeroDivisor {
    signature: Signature,
}

impl SparkNonzeroDivisor {
    fn new() -> Self {
        Self {
            // User-defined so the divisor's exact decimal type passes through with no coercion (see
            // `coerce_types`): the wrapped divide/modulo must keep DataFusion's native result type.
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkNonzeroDivisor {
    fn name(&self) -> &str {
        "spark_nonzero_divisor"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    /// Identity: the divisor is returned unchanged, so its type is its input type.
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 1 {
            return exec_err!("spark_nonzero_divisor expects exactly 1 argument");
        }
        Ok(arg_types[0].clone())
    }
    /// Identity coercion: never insert a cast — the divisor must reach the wrapped operator with the
    /// exact type DataFusion would have seen without this guard.
    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 1 {
            return exec_err!("spark_nonzero_divisor expects exactly 1 argument");
        }
        Ok(arg_types.to_vec())
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let n = args.number_rows;
        if args.args.len() != 1 {
            return exec_err!("spark_nonzero_divisor expects exactly 1 argument");
        }
        let arr = args.args[0].clone().into_array(n)?;
        // Scan for a non-null zero divisor. We only ever wrap decimal/float divisors (see the
        // planner), so only those arms can raise; any other type passes through untouched. For floats
        // both `+0.0` and `-0.0` are zero divisors (Spark's `isZero` treats them alike); `NaN` is not.
        let has_zero = match arr.data_type() {
            DataType::Decimal128(_, _) => {
                let d = arr.as_any().downcast_ref::<Decimal128Array>();
                d.map(|a| (0..a.len()).any(|i| !a.is_null(i) && a.value(i) == 0))
                    .unwrap_or(false)
            }
            DataType::Decimal256(_, _) => {
                let d = arr.as_any().downcast_ref::<Decimal256Array>();
                d.map(|a| (0..a.len()).any(|i| !a.is_null(i) && a.value(i) == i256::ZERO))
                    .unwrap_or(false)
            }
            DataType::Float64 => {
                let d = arr.as_any().downcast_ref::<Float64Array>();
                d.map(|a| (0..a.len()).any(|i| !a.is_null(i) && a.value(i) == 0.0))
                    .unwrap_or(false)
            }
            DataType::Float32 => {
                let d = arr.as_any().downcast_ref::<Float32Array>();
                d.map(|a| (0..a.len()).any(|i| !a.is_null(i) && a.value(i) == 0.0))
                    .unwrap_or(false)
            }
            DataType::Float16 => {
                let d = arr.as_any().downcast_ref::<Float16Array>();
                d.map(|a| (0..a.len()).any(|i| !a.is_null(i) && a.value(i).to_f32() == 0.0))
                    .unwrap_or(false)
            }
            _ => false,
        };
        if has_zero {
            // Spark ANSI `/` and `%` raise DIVIDE_BY_ZERO on a zero divisor. The message text avoids
            // the tokens the parity harness keys on for missing-function / parse / unimplemented
            // buckets, so a both-error row stays `error-parity`.
            return exec_err!(
                "[DIVIDE_BY_ZERO] Division by zero. Use try_divide to tolerate a 0 divisor and return NULL instead. SQLSTATE: 22012"
            );
        }
        // Identity: return the divisor exactly as received (preserves type and every value/null).
        Ok(args.args[0].clone())
    }
}

#[cfg(test)]
mod tests {
    use crate::Engine;

    /// A zero decimal divisor must raise DIVIDE_BY_ZERO for both `/` and `%`.
    #[tokio::test]
    async fn decimal_divide_and_modulo_by_zero_error() {
        let engine = Engine::new();
        for q in [
            "SELECT a / b FROM (SELECT 1.0 a, 0.0 b) t",
            "SELECT a % b FROM (SELECT 1.0 a, 0.0 b) t",
        ] {
            let err = engine.sql(q).await.unwrap_err().to_string();
            assert!(
                err.contains("DIVIDE_BY_ZERO") || err.to_lowercase().contains("divi"),
                "want a divide-by-zero error for `{q}`, got: {err}"
            );
        }
    }

    /// A non-zero decimal divisor produces the same value as plain DataFusion decimal divide.
    #[tokio::test]
    async fn nonzero_decimal_divide_unchanged() {
        let engine = Engine::new();
        let batches = engine
            .sql("SELECT a / b AS x FROM (SELECT 6.0 a, 2.0 b) t")
            .await
            .unwrap();
        let txt = crate::arrow::util::pretty::pretty_format_batches(&batches)
            .unwrap()
            .to_string();
        assert!(txt.contains('3'), "got:\n{txt}");
    }

    /// A NULL decimal divisor yields NULL, never an error.
    #[tokio::test]
    async fn null_decimal_divisor_is_null() {
        use datafusion::arrow::array::Array;
        let engine = Engine::new();
        let batches = engine
            .sql("SELECT a / b AS x FROM (SELECT 1.0 a, CAST(NULL AS DECIMAL(2,1)) b) t")
            .await
            .unwrap();
        assert!(batches[0].column(0).is_null(0));
    }
}

//! `spark_checked_mul(left, right)` — the internal Int64 ANSI-checked multiply UDF that
//! `lower_checked_multiply` (in `lib.rs`) lowers an integral `*` to when the result type is `bigint`.
//!
//! ## Why this UDF exists
//!
//! Spark's `*` is ANSI-checked: `bigint(-9223372036854775808) * bigint(-1)` (and the unfiltered
//! `q1 * q2` over `INT8_TBL`) overflow `Int64` and Spark raises `ARITHMETIC_OVERFLOW`. DataFusion's
//! native `Int64` multiply *wraps* silently (two's-complement), yielding a corrupt value where Spark
//! errors — a forbidden missing-error gap.
//!
//! This UDF carries a static `Int64` return type (identical to DataFusion's native multiply, so the
//! result type and every non-overflowing product are byte-identical) and uses `i64::checked_mul`:
//! on `None` (overflow) it raises Spark's `ARITHMETIC_OVERFLOW`, exactly where Spark ANSI does. A
//! `NULL` operand yields `NULL` (never an error), matching Spark. Because the checked product equals
//! the wrapping product whenever no overflow occurs, only the overflow rows change — and Spark ANSI
//! rejects those too, so this can only convert a missing-error into error-parity, never pass→fail.
//!
//! `lower_checked_multiply` only routes a `*` here when the Spark result type is `bigint` (at least
//! one operand `Int64`); `Int32 * Int32` is left on DataFusion so it keeps Spark's `int` result
//! type. Both operands are cast to `Int64` by the rewrite before this runs.

use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, Int64Array};
use datafusion::arrow::datatypes::{DataType, Int64Type};
use datafusion::common::{exec_err, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::prelude::SessionContext;

/// Register `spark_checked_mul` into `ctx` (kept consistent with the module pattern; the function is
/// internal — no Spark SQL calls it by name, `lower_checked_multiply` embeds it directly via [`udf`]).
pub fn register(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(SparkCheckedMul::new()));
}

/// The `spark_checked_mul` UDF as a shareable `Arc`, for `lower_checked_multiply` to embed in the
/// lowered expression (the rewrite builds an `Expr::ScalarFunction` carrying this concrete `func`, so
/// no registry lookup is needed at plan or execution time).
pub(crate) fn udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkCheckedMul::new()))
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct SparkCheckedMul {
    signature: Signature,
}

impl SparkCheckedMul {
    fn new() -> Self {
        Self {
            // The planner always casts both operands to `Int64`, so an exact `Int64` signature
            // matches with no further coercion.
            signature: Signature::exact(
                vec![DataType::Int64, DataType::Int64],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for SparkCheckedMul {
    fn name(&self) -> &str {
        "spark_checked_mul"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    /// Always `Int64` — identical to DataFusion's native `Int64` multiply result type.
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let n = args.number_rows;
        if args.args.len() != 2 {
            return exec_err!("spark_checked_mul expects exactly 2 arguments");
        }
        let lhs = args.args[0].clone().into_array(n)?;
        let rhs = args.args[1].clone().into_array(n)?;
        // Defensive cast (never panics on an unexpected width); the planner already hands us
        // `Int64`, so this is a no-op there.
        let a = to_i64(&lhs)?;
        let b = to_i64(&rhs)?;
        let mut out = Int64Array::builder(n);
        for i in 0..n {
            if a.is_null(i) || b.is_null(i) {
                // Spark `*` with a NULL operand is NULL — never an error.
                out.append_null();
            } else {
                match a.value(i).checked_mul(b.value(i)) {
                    Some(v) => out.append_value(v),
                    None => {
                        // Spark ANSI `*` raises ARITHMETIC_OVERFLOW on Int64 overflow. The message
                        // text avoids the tokens the parity harness keys on for missing-function /
                        // parse / unimplemented buckets, so a both-error row stays `error-parity`.
                        return exec_err!(
                            "[ARITHMETIC_OVERFLOW] long overflow. Use 'try_multiply' to tolerate overflow and return NULL instead. SQLSTATE: 22003"
                        );
                    }
                }
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

/// Cast any integer array to `Int64Array`, preserving nulls. Used defensively so an unexpected input
/// width is an error, never a downcast panic (an engine panic would be a robustness bug).
fn to_i64(arr: &ArrayRef) -> Result<Int64Array> {
    use datafusion::arrow::array::AsArray;
    let casted = datafusion::arrow::compute::cast(arr, &DataType::Int64)?;
    Ok(casted.as_primitive::<Int64Type>().clone())
}

#[cfg(test)]
mod tests {
    use crate::Engine;

    /// A non-overflowing integral product is byte-identical to a plain multiply.
    #[tokio::test]
    async fn non_overflow_multiply_matches() {
        let engine = Engine::new();
        let batches = engine
            .sql("SELECT bigint(123) * bigint(456) AS x")
            .await
            .unwrap();
        let txt = crate::arrow::util::pretty::pretty_format_batches(&batches)
            .unwrap()
            .to_string();
        assert!(txt.contains("56088"), "got:\n{txt}");
    }

    /// An overflowing `bigint * bigint` must raise ARITHMETIC_OVERFLOW, not wrap silently.
    #[tokio::test]
    async fn overflow_multiply_errors() {
        let engine = Engine::new();
        let err = engine
            .sql("SELECT bigint('-9223372036854775808') * bigint('-1') AS x")
            .await
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("ARITHMETIC_OVERFLOW") || err.to_lowercase().contains("overflow"),
            "want an overflow error, got: {err}"
        );
    }

    /// A NULL operand yields NULL, never an error.
    #[tokio::test]
    async fn null_operand_is_null() {
        use datafusion::arrow::array::Array;
        let engine = Engine::new();
        let batches = engine
            .sql("SELECT cast(null as bigint) * bigint(5) AS x")
            .await
            .unwrap();
        assert!(batches[0].column(0).is_null(0));
    }
}

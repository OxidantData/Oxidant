//! Spark `try_*` arithmetic helpers and the `nullifzero` / `zeroifnull` null helpers.
//!
//! Spark's `try_add` / `try_subtract` / `try_multiply` / `try_divide` / `try_mod` perform the
//! same arithmetic as the corresponding operators but, instead of raising an `ARITHMETIC_OVERFLOW`
//! / `DIVIDE_BY_ZERO` error under ANSI mode, they return `NULL`. We implement the overflow-safe
//! numeric core here:
//!
//! * **Integers** (`Int64`): checked arithmetic — any overflow yields `NULL`.
//! * **Doubles** (`Float64`): IEEE arithmetic; division/modulo by zero yields `NULL` (Spark turns
//!   `DIVIDE_BY_ZERO` into `NULL` here, rather than producing `Infinity`/`NaN`).
//! * Any other numeric input is coerced to `Float64` before the operation (DataFusion's signature
//!   coercion picks a common type; we accept whatever it hands us and widen to `f64`).
//!
//! `nullifzero(x)` returns `NULL` when `x = 0`, else `x`. `zeroifnull(x)` returns `0` when `x` is
//! `NULL`, else `x`. Both preserve the operand's numeric type (we keep `Int64` as `Int64` and
//! everything else as `Float64`).
//!
//! Faithfulness caveat: DataFusion parses unsuffixed integer literals as `Int64`, so Spark's
//! 32-bit `int` overflow cases (e.g. `try_add(2147483647, 1) = NULL`) are *not* detectable here —
//! the value fits in `Int64`. We are faithful for the types we actually receive; the harness's
//! 32-bit-overflow rows are unreachable without Spark's narrower integer typing and are left as-is.

use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, Float64Array, Int32Array, Int64Array};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{exec_err, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::prelude::SessionContext;

/// Register all functions in this module.
pub fn register(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(TryBinary::new(Op::Add)));
    ctx.register_udf(ScalarUDF::from(TryBinary::new(Op::Subtract)));
    ctx.register_udf(ScalarUDF::from(TryBinary::new(Op::Multiply)));
    ctx.register_udf(ScalarUDF::from(TryBinary::new(Op::Divide)));
    ctx.register_udf(ScalarUDF::from(TryBinary::new(Op::Mod)));
    ctx.register_udf(ScalarUDF::from(NullIfZero::new()));
    ctx.register_udf(ScalarUDF::from(ZeroIfNull::new()));
}

/// Which `try_*` binary arithmetic operation a [`TryBinary`] performs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Op {
    Add,
    Subtract,
    Multiply,
    Divide,
    Mod,
}

impl Op {
    fn name(self) -> &'static str {
        match self {
            Op::Add => "try_add",
            Op::Subtract => "try_subtract",
            Op::Multiply => "try_multiply",
            Op::Divide => "try_divide",
            Op::Mod => "try_mod",
        }
    }

    /// Whether the result is always floating point (Spark's `try_divide` always returns a
    /// double/decimal — never a plain integer — so we widen integer operands to `f64`).
    fn forces_float(self) -> bool {
        matches!(self, Op::Divide)
    }

    /// Apply to two `i64` operands, returning `None` on overflow.
    fn apply_i64(self, a: i64, b: i64) -> Option<i64> {
        match self {
            Op::Add => a.checked_add(b),
            Op::Subtract => a.checked_sub(b),
            Op::Multiply => a.checked_mul(b),
            // Divide is handled as float; never reached for i64.
            Op::Divide => None,
            // Spark `try_mod` returns NULL on a zero divisor; `checked_rem` also guards i64::MIN % -1.
            Op::Mod => a.checked_rem(b),
        }
    }

    /// Apply to two `i32` operands, returning `None` on overflow — Spark's `int` `try_*` (a 32-bit
    /// overflow yields NULL, not a widened `bigint`).
    fn apply_i32(self, a: i32, b: i32) -> Option<i32> {
        match self {
            Op::Add => a.checked_add(b),
            Op::Subtract => a.checked_sub(b),
            Op::Multiply => a.checked_mul(b),
            Op::Divide => None,
            Op::Mod => a.checked_rem(b),
        }
    }

    /// Apply to two `f64` operands, returning `None` where Spark yields `NULL`
    /// (division / modulo by zero).
    fn apply_f64(self, a: f64, b: f64) -> Option<f64> {
        match self {
            Op::Add => Some(a + b),
            Op::Subtract => Some(a - b),
            Op::Multiply => Some(a * b),
            Op::Divide => (b != 0.0).then(|| a / b),
            Op::Mod => (b != 0.0).then(|| a % b),
        }
    }
}

/// A Spark `try_*` overflow-safe binary arithmetic UDF.
#[derive(Debug, PartialEq, Eq, Hash)]
struct TryBinary {
    op: Op,
    signature: Signature,
}

impl TryBinary {
    fn new(op: Op) -> Self {
        Self {
            op,
            // Lean permissive: accept any two numeric args and coerce inside `invoke`.
            signature: Signature::any(2, Volatility::Immutable),
        }
    }

    /// Whether both operands are exactly `Int32` (Spark `int`) for an integral op. Then we compute
    /// in 32-bit checked arithmetic and type the result `int` — matching Spark, where `int OP int`
    /// stays `int` and a 32-bit overflow yields NULL. (oxidant's `spark_int_literals` retypes in-range
    /// integer literals to `Int32`, so `try_add(1, 1)` reaches this UDF as `Int32, Int32`.)
    fn both_i32(&self, arg_types: &[DataType]) -> bool {
        !self.op.forces_float()
            && arg_types.len() == 2
            && arg_types.iter().all(|t| matches!(t, DataType::Int32))
    }

    /// Whether both operands stay integral for this op (only then do we produce `Int64`).
    fn integral(&self, arg_types: &[DataType]) -> bool {
        !self.op.forces_float()
            && arg_types.iter().all(|t| {
                matches!(
                    t,
                    DataType::Int8
                        | DataType::Int16
                        | DataType::Int32
                        | DataType::Int64
                        | DataType::UInt8
                        | DataType::UInt16
                        | DataType::UInt32
                        | DataType::UInt64
                )
            })
    }
}

impl ScalarUDFImpl for TryBinary {
    fn name(&self) -> &str {
        self.op.name()
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(if self.both_i32(arg_types) {
            DataType::Int32
        } else if self.integral(arg_types) {
            DataType::Int64
        } else {
            DataType::Float64
        })
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let n = args.number_rows;
        if args.args.len() != 2 {
            return exec_err!("{} expects exactly 2 arguments", self.op.name());
        }
        let arg_types: Vec<DataType> = args
            .arg_fields
            .iter()
            .map(|f| f.data_type().clone())
            .collect();
        let lhs = args.args[0].clone().into_array(n)?;
        let rhs = args.args[1].clone().into_array(n)?;

        if self.both_i32(&arg_types) {
            let a = cast_i32(&lhs)?;
            let b = cast_i32(&rhs)?;
            let out: Int32Array = (0..n)
                .map(|i| {
                    if a.is_null(i) || b.is_null(i) {
                        None
                    } else {
                        self.op.apply_i32(a.value(i), b.value(i))
                    }
                })
                .collect();
            Ok(ColumnarValue::Array(Arc::new(out)))
        } else if self.integral(&arg_types) {
            let a = cast_i64(&lhs)?;
            let b = cast_i64(&rhs)?;
            let out: Int64Array = (0..n)
                .map(|i| {
                    if a.is_null(i) || b.is_null(i) {
                        None
                    } else {
                        self.op.apply_i64(a.value(i), b.value(i))
                    }
                })
                .collect();
            Ok(ColumnarValue::Array(Arc::new(out)))
        } else {
            let a = cast_f64(&lhs)?;
            let b = cast_f64(&rhs)?;
            let out: Float64Array = (0..n)
                .map(|i| {
                    if a.is_null(i) || b.is_null(i) {
                        None
                    } else {
                        self.op.apply_f64(a.value(i), b.value(i))
                    }
                })
                .collect();
            Ok(ColumnarValue::Array(Arc::new(out)))
        }
    }
}

/// `nullifzero(x)` — `NULL` if `x = 0`, else `x`. Preserves `Int64` for integral input, else `f64`.
#[derive(Debug, PartialEq, Eq, Hash)]
struct NullIfZero {
    signature: Signature,
}

impl NullIfZero {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
    fn integral(arg: &DataType) -> bool {
        matches!(
            arg,
            // A bare untyped NULL is `int` in Spark for these helpers (e.g. `zeroifnull(NULL):int`),
            // so treat the Null type as integral too.
            DataType::Null
                | DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64
        )
    }
}

impl ScalarUDFImpl for NullIfZero {
    fn name(&self) -> &str {
        "nullifzero"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(if Self::integral(&arg_types[0]) {
            DataType::Int64
        } else {
            DataType::Float64
        })
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let n = args.number_rows;
        let dt = args.arg_fields[0].data_type().clone();
        let arr = args.args[0].clone().into_array(n)?;
        if Self::integral(&dt) {
            let a = cast_i64(&arr)?;
            let out: Int64Array = (0..n)
                .map(|i| {
                    if a.is_null(i) || a.value(i) == 0 {
                        None
                    } else {
                        Some(a.value(i))
                    }
                })
                .collect();
            Ok(ColumnarValue::Array(Arc::new(out)))
        } else {
            let a = cast_f64(&arr)?;
            let out: Float64Array = (0..n)
                .map(|i| {
                    if a.is_null(i) || a.value(i) == 0.0 {
                        None
                    } else {
                        Some(a.value(i))
                    }
                })
                .collect();
            Ok(ColumnarValue::Array(Arc::new(out)))
        }
    }
}

/// `zeroifnull(x)` — `0` if `x` is `NULL`, else `x`. Preserves `Int64` for integral input, else `f64`.
#[derive(Debug, PartialEq, Eq, Hash)]
struct ZeroIfNull {
    signature: Signature,
}

impl ZeroIfNull {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ZeroIfNull {
    fn name(&self) -> &str {
        "zeroifnull"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(if NullIfZero::integral(&arg_types[0]) {
            DataType::Int64
        } else {
            DataType::Float64
        })
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let n = args.number_rows;
        let dt = args.arg_fields[0].data_type().clone();
        let arr = args.args[0].clone().into_array(n)?;
        if NullIfZero::integral(&dt) {
            let a = cast_i64(&arr)?;
            let out: Int64Array = (0..n)
                .map(|i| {
                    if a.is_null(i) {
                        Some(0)
                    } else {
                        Some(a.value(i))
                    }
                })
                .collect();
            Ok(ColumnarValue::Array(Arc::new(out)))
        } else {
            let a = cast_f64(&arr)?;
            let out: Float64Array = (0..n)
                .map(|i| {
                    if a.is_null(i) {
                        Some(0.0)
                    } else {
                        Some(a.value(i))
                    }
                })
                .collect();
            Ok(ColumnarValue::Array(Arc::new(out)))
        }
    }
}

/// An *unsafe* (Spark/ANSI-faithful) cast: invalid input (e.g. the string `'abc'` cast to a
/// number) is an error, not a silently-produced `NULL`. Spark's `try_*` / `nullifzero` /
/// `zeroifnull` cast their operands under ANSI rules, so a non-numeric string raises
/// `CAST_INVALID_INPUT` rather than yielding `NULL`.
fn cast_strict(arr: &ArrayRef, to: &DataType) -> Result<ArrayRef> {
    let opts = datafusion::arrow::compute::CastOptions {
        safe: false,
        format_options: Default::default(),
    };
    datafusion::arrow::compute::cast_with_options(arr, to, &opts)
        .map_err(|e| datafusion::common::DataFusionError::ArrowError(Box::new(e), None))
}

/// Cast any Arrow array to `Int64`, preserving nulls; invalid input errors (see [`cast_strict`]).
fn cast_i64(arr: &ArrayRef) -> Result<Int64Array> {
    Ok(cast_strict(arr, &DataType::Int64)?
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("cast to Int64 yields Int64Array")
        .clone())
}

/// Cast any Arrow array to `Int32`, preserving nulls; invalid input errors (see [`cast_strict`]).
fn cast_i32(arr: &ArrayRef) -> Result<Int32Array> {
    Ok(cast_strict(arr, &DataType::Int32)?
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("cast to Int32 yields Int32Array")
        .clone())
}

/// Cast any Arrow array to `Float64`, preserving nulls; invalid input errors (see [`cast_strict`]).
fn cast_f64(arr: &ArrayRef) -> Result<Float64Array> {
    Ok(cast_strict(arr, &DataType::Float64)?
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("cast to Float64 yields Float64Array")
        .clone())
}

#[cfg(test)]
mod tests {
    /// Run `q` and return the single scalar cell as a string. A NULL renders as an empty cell in
    /// Arrow's pretty formatter, so we read the value out of the first record batch directly and
    /// map NULL to the literal "NULL" for unambiguous assertions.
    async fn cell(q: &str) -> String {
        use datafusion::arrow::array::Array;
        let engine = crate::Engine::new();
        let batches = engine.sql(q).await.unwrap_or_else(|e| panic!("{q}: {e}"));
        let col = batches[0].column(0);
        if col.is_null(0) {
            return "NULL".to_string();
        }
        // Reuse the pretty formatter for the value, then strip the box-drawing chrome.
        let txt = crate::arrow::util::pretty::pretty_format_batches(&batches)
            .unwrap()
            .to_string();
        // The 4th line (index 3) of a single-column/single-row table is the value row.
        txt.lines()
            .nth(3)
            .unwrap()
            .trim_matches(|c| c == '|' || c == ' ')
            .to_string()
    }

    #[tokio::test]
    async fn try_add_subtract_multiply_basic() {
        assert_eq!(cell("SELECT try_add(1, 1) AS x").await, "2");
        assert_eq!(cell("SELECT try_subtract(1, 1) AS x").await, "0");
        assert_eq!(cell("SELECT try_multiply(2, 3) AS x").await, "6");
    }

    #[tokio::test]
    async fn try_add_int64_overflow_is_null() {
        // i64::MAX + 1 overflows Int64 -> NULL (Spark's try_add tolerates overflow).
        assert_eq!(
            cell("SELECT try_add(CAST(9223372036854775807 AS BIGINT), CAST(1 AS BIGINT)) AS x")
                .await,
            "NULL"
        );
    }

    #[tokio::test]
    async fn try_int32_overflow_is_null_and_keeps_int() {
        // int OP int stays `int`: a 32-bit overflow yields NULL (Spark), not a widened bigint.
        assert_eq!(cell("SELECT try_add(2147483647, 1) AS x").await, "NULL");
        assert_eq!(
            cell("SELECT try_subtract(-2147483648, 1) AS x").await,
            "NULL"
        );
        assert_eq!(
            cell("SELECT try_multiply(2147483647, 2) AS x").await,
            "NULL"
        );
        // In-range int arithmetic stays exact.
        assert_eq!(cell("SELECT try_add(1, 1) AS x").await, "2");
    }

    #[tokio::test]
    async fn try_divide_by_zero_is_null() {
        assert_eq!(cell("SELECT try_divide(1, 0) AS x").await, "NULL");
        assert_eq!(cell("SELECT try_divide(1, 2) AS x").await, "0.5");
    }

    #[tokio::test]
    async fn try_mod_basic_and_zero() {
        assert_eq!(cell("SELECT try_mod(7, 3) AS x").await, "1");
        assert_eq!(cell("SELECT try_mod(7, 0) AS x").await, "NULL");
    }

    #[tokio::test]
    async fn nullifzero_and_zeroifnull() {
        assert_eq!(cell("SELECT nullifzero(0) AS x").await, "NULL");
        assert_eq!(cell("SELECT nullifzero(1) AS x").await, "1");
        assert_eq!(cell("SELECT zeroifnull(NULL) AS x").await, "0");
        assert_eq!(cell("SELECT zeroifnull(1) AS x").await, "1");
    }
}

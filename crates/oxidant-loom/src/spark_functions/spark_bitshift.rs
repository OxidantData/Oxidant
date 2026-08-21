//! Spark integer bit-shift functions: `shiftleft`, `shiftright`, `shiftrightunsigned`.
//!
//! Spark's `ShiftLeft`/`ShiftRight`/`ShiftRightUnsigned` operate on `IntegerType` or `LongType`,
//! return the **same** integer type, and follow Java's `<<`/`>>`/`>>>` — in particular the shift
//! amount is **masked** to the operand width (`numBits & 31` for 32-bit, `& 63` for 64-bit), so
//! `shiftleft(int(-1), 31)` is `int` `-2147483648`, not a panic or a widened value. DataFusion has
//! no equivalent builtin (the calls fail as `Invalid function 'shiftleft'`), so these are additive
//! `ScalarUDF`s. Smaller integer inputs (`tinyint`/`smallint`) are coerced up to `int` exactly as
//! Spark does; the second argument (bit count) is read as a 32-bit integer.

use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, Int32Array, Int64Array};
use datafusion::arrow::datatypes::{DataType, Int32Type, Int64Type};
use datafusion::common::{exec_err, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use datafusion::prelude::SessionContext;

/// Register `shiftleft`, `shiftright`, and `shiftrightunsigned`.
pub fn register(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(SparkShift::new(ShiftOp::Left)));
    ctx.register_udf(ScalarUDF::from(SparkShift::new(ShiftOp::Right)));
    ctx.register_udf(ScalarUDF::from(SparkShift::new(ShiftOp::RightUnsigned)));
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum ShiftOp {
    Left,
    Right,
    RightUnsigned,
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct SparkShift {
    signature: Signature,
    op: ShiftOp,
}

impl SparkShift {
    fn new(op: ShiftOp) -> Self {
        // (Int32|Int64 value, Int32|Int64 numBits). The bit count is commonly an `Int64` literal
        // (DataFusion's default before the Int32 retype), so accept it either width; `invoke` reads
        // it as i32. The value's width is preserved as the return type. tinyint/smallint args coerce
        // up to one of these candidates, matching Spark's promotion of the operand.
        let sig = Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::Int32, DataType::Int32]),
                TypeSignature::Exact(vec![DataType::Int32, DataType::Int64]),
                TypeSignature::Exact(vec![DataType::Int64, DataType::Int32]),
                TypeSignature::Exact(vec![DataType::Int64, DataType::Int64]),
            ],
            Volatility::Immutable,
        );
        Self { signature: sig, op }
    }

    fn fn_name(&self) -> &'static str {
        match self.op {
            ShiftOp::Left => "shiftleft",
            ShiftOp::Right => "shiftright",
            ShiftOp::RightUnsigned => "shiftrightunsigned",
        }
    }
}

/// Apply the op to a 32-bit value with Java masking (`bits & 31`).
fn shift32(op: ShiftOp, v: i32, bits: i32) -> i32 {
    let s = (bits & 31) as u32;
    match op {
        ShiftOp::Left => v.wrapping_shl(s),
        ShiftOp::Right => v.wrapping_shr(s),
        ShiftOp::RightUnsigned => ((v as u32) >> s) as i32,
    }
}

/// Apply the op to a 64-bit value with Java masking (`bits & 63`).
fn shift64(op: ShiftOp, v: i64, bits: i32) -> i64 {
    let s = (bits & 63) as u32;
    match op {
        ShiftOp::Left => v.wrapping_shl(s),
        ShiftOp::Right => v.wrapping_shr(s),
        ShiftOp::RightUnsigned => ((v as u64) >> s) as i64,
    }
}

impl ScalarUDFImpl for SparkShift {
    fn name(&self) -> &str {
        self.fn_name()
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        // Same integer type as the (value) first argument.
        Ok(arg_types.first().cloned().unwrap_or(DataType::Int32))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let n = args.number_rows;
        let value = args.args[0].clone().into_array(n)?;
        let bits_arr = args.args[1].clone().into_array(n)?;
        // The bit count may arrive as Int32 or Int64; read each row as i32 (Java masks it anyway).
        let bits_at: Box<dyn Fn(usize) -> Option<i32>> = match bits_arr.data_type() {
            DataType::Int32 => {
                let b = bits_arr.as_primitive::<Int32Type>().clone();
                Box::new(move |i| (!b.is_null(i)).then(|| b.value(i)))
            }
            DataType::Int64 => {
                let b = bits_arr.as_primitive::<Int64Type>().clone();
                Box::new(move |i| (!b.is_null(i)).then(|| b.value(i) as i32))
            }
            other => return exec_err!("{}: bit count must be int, got {other}", self.fn_name()),
        };

        let out: ArrayRef = match value.data_type() {
            DataType::Int32 => {
                let v = value.as_primitive::<Int32Type>();
                Arc::new(
                    (0..n)
                        .map(|i| match (v.is_null(i), bits_at(i)) {
                            (false, Some(b)) => Some(shift32(self.op, v.value(i), b)),
                            _ => None,
                        })
                        .collect::<Int32Array>(),
                )
            }
            DataType::Int64 => {
                let v = value.as_primitive::<Int64Type>();
                Arc::new(
                    (0..n)
                        .map(|i| match (v.is_null(i), bits_at(i)) {
                            (false, Some(b)) => Some(shift64(self.op, v.value(i), b)),
                            _ => None,
                        })
                        .collect::<Int64Array>(),
                )
            }
            other => return exec_err!("{}: unsupported value type {other}", self.fn_name()),
        };
        Ok(ColumnarValue::Array(out))
    }
}

#[cfg(test)]
mod tests {
    async fn run(q: &str) -> String {
        let engine = crate::Engine::new();
        let batches = engine.sql(q).await.unwrap_or_else(|e| panic!("{q}: {e}"));
        crate::arrow::util::pretty::pretty_format_batches(&batches)
            .unwrap()
            .to_string()
    }

    #[tokio::test]
    async fn shift_left_masks_and_preserves_int() {
        // -1 << 31 = i32::MIN (-2147483648); shift amount masked to 31 bits.
        assert!(run("SELECT shiftleft(int(-1), 31) AS x")
            .await
            .contains("-2147483648"));
        // 1 << 3 = 8.
        assert!(run("SELECT shiftleft(int(1), 3) AS x").await.contains('8'));
    }

    #[tokio::test]
    async fn shift_right_signed_vs_unsigned() {
        // -8 >> 1 = -4 (arithmetic).
        assert!(run("SELECT shiftright(int(-8), 1) AS x")
            .await
            .contains("-4"));
        // -1 >>> 28 = 15 (logical, on 32-bit).
        assert!(run("SELECT shiftrightunsigned(int(-1), 28) AS x")
            .await
            .contains("15"));
    }

    #[tokio::test]
    async fn shift_left_bigint() {
        // 1L << 40 = 1099511627776 (stays bigint, 64-bit mask).
        assert!(run("SELECT shiftleft(CAST(1 AS BIGINT), 40) AS x")
            .await
            .contains("1099511627776"));
    }
}

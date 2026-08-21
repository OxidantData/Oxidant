//! Spark-faithful `round` / `bround` for **integral** inputs.
//!
//! DataFusion's builtin `round` coerces integral inputs (`tinyint`/`smallint`/`int`/`bigint`) to
//! `Float64` and returns a `Float64`, so `round(25y, -1)` yields the double `30.0`. Spark instead
//! *preserves the integral type*, rounds in integer space (`HALF_UP` for `round`, `HALF_EVEN` for
//! `bround`), and — under ANSI mode — raises `ARITHMETIC_OVERFLOW` when the rounded value no longer
//! fits the input type (e.g. `round(127y, -1) = 130` overflows `tinyint`).
//!
//! [`SparkRound`] registers under the name `round` (overriding the builtin) and `bround` (new).
//! It intercepts **only** integral first arguments; every `Decimal`/`Float32`/`Float64` input is
//! delegated **unchanged** to the wrapped builtin `round` (same coercion, same return field, same
//! invoke), so the decimal/float rounding behavior — and its golden parity — cannot regress.
//!
//! `bround` on a non-integral input is *not* delegated: the builtin rounds `HALF_UP`, but Spark's
//! `bround` is `HALF_EVEN`, which differs on exact ties. Rather than ship a wrong tie result we
//! raise an "unsupported" error for that path (it is exercised by no golden, and an error is
//! faithful-by-omission — strictly better than a wrong value).

use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, Int16Array, Int32Array, Int64Array, Int8Array,
};
use datafusion::arrow::datatypes::{
    DataType, Field, FieldRef, Int16Type, Int32Type, Int64Type, Int8Type,
};
use datafusion::common::{exec_err, internal_err, DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::type_coercion::functions::fields_with_udf;
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion::prelude::SessionContext;

/// Register Spark-faithful `round` (overriding the builtin) and `bround` (new).
pub fn register(ctx: &SessionContext) {
    let builtin = datafusion::functions::math::round();
    ctx.register_udf(ScalarUDF::from(SparkRound::new(builtin.clone(), false)));
    ctx.register_udf(ScalarUDF::from(SparkRound::new(builtin, true)));
}

/// Spark `round` / `bround`. `half_even = false` ⇒ `round` (HALF_UP); `true` ⇒ `bround`
/// (HALF_EVEN). Wraps the builtin `round` for all non-integral inputs.
#[derive(Debug, PartialEq, Eq, Hash)]
struct SparkRound {
    signature: Signature,
    /// The DataFusion builtin `round`, used verbatim for Decimal / Float inputs.
    builtin: Arc<ScalarUDF>,
    /// `true` for `bround` (HALF_EVEN), `false` for `round` (HALF_UP).
    half_even: bool,
}

impl SparkRound {
    fn new(builtin: Arc<ScalarUDF>, half_even: bool) -> Self {
        Self {
            // We do our own per-argument coercion in `coerce_types`: integral first args are kept
            // integral (Spark semantics); everything else defers to the builtin's coercion.
            signature: Signature::user_defined(Volatility::Immutable),
            builtin,
            half_even,
        }
    }
}

impl ScalarUDFImpl for SparkRound {
    fn name(&self) -> &str {
        if self.half_even {
            "bround"
        } else {
            "round"
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.is_empty() {
            return exec_err!("{} requires at least one argument", self.name());
        }
        if int_bounds(&arg_types[0]).is_some() {
            // Spark keeps the integral type of the value; the scale is an Int32.
            let mut coerced = vec![arg_types[0].clone()];
            if arg_types.len() >= 2 {
                coerced.push(DataType::Int32);
            }
            Ok(coerced)
        } else {
            // Decimal / Float / everything else: coerce exactly as the builtin `round` would, by
            // running the builtin's own signature coercion over the (uncoerced) argument types.
            let fields: Vec<FieldRef> = arg_types
                .iter()
                .map(|dt| Arc::new(Field::new("f", dt.clone(), true)))
                .collect();
            let coerced = fields_with_udf(&fields, self.builtin.as_ref())?;
            Ok(coerced.iter().map(|f| f.data_type().clone()).collect())
        }
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        // The actual return field is computed in `return_field_from_args` (mirrors the builtin,
        // which needs the scalar `decimal_places` literal to pick a decimal output scale).
        internal_err!("use return_field_from_args")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let value_type = args.arg_fields[0].data_type();
        if int_bounds(value_type).is_some() {
            // Spark returns the SAME integral type as the value argument.
            let nullable = args.arg_fields.iter().any(|f| f.is_nullable());
            Ok(Arc::new(Field::new(
                self.name(),
                value_type.clone(),
                nullable,
            )))
        } else {
            self.builtin.return_field_from_args(args)
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let value_type = args.arg_fields[0].data_type().clone();
        if int_bounds(&value_type).is_some() {
            round_integral(&args, self.half_even)
        } else if self.half_even {
            // See module docs: HALF_UP delegation would be wrong on ties for Spark's HALF_EVEN
            // `bround`; refuse rather than return a wrong value (no golden exercises this path).
            exec_err!("bround on non-integral input ({value_type}) is not supported")
        } else {
            // `round` on Decimal / Float: byte-for-byte the builtin's behavior.
            self.builtin.invoke_with_args(args)
        }
    }
}

/// Inclusive `[min, max]` range of an integral Arrow type, or `None` if `dt` is not one of the four
/// signed integer types Spark rounds in integer space.
fn int_bounds(dt: &DataType) -> Option<(i128, i128)> {
    match dt {
        DataType::Int8 => Some((i8::MIN as i128, i8::MAX as i128)),
        DataType::Int16 => Some((i16::MIN as i128, i16::MAX as i128)),
        DataType::Int32 => Some((i32::MIN as i128, i32::MAX as i128)),
        DataType::Int64 => Some((i64::MIN as i128, i64::MAX as i128)),
        _ => None,
    }
}

/// Round an integer `v` to `dp` decimal places in `i128` space.
///
/// `dp >= 0` is a no-op for integers (no fractional digits to drop). For `dp < 0` we round to the
/// nearest `10^(-dp)`: `HALF_EVEN` when `half_even`, otherwise `HALF_UP` (round half away from zero).
/// The result is exact in `i128` for any `i64`-range input (overflow vs the *input type* is checked
/// by the caller).
fn round_int(v: i128, dp: i32, half_even: bool) -> i128 {
    if dp >= 0 {
        return v;
    }
    let n = dp.unsigned_abs();
    if n >= 39 {
        // 10^39 exceeds i128::MAX; an i64-range value at this scale always rounds to 0.
        return 0;
    }
    let factor = 10i128.pow(n);
    let r = v % factor; // remainder, same sign as v, |r| < factor
    let q = v - r; // multiple of factor nearest zero
    let abs_r = r.abs();
    let half = factor / 2; // exact: 10^n (n >= 1) is even
    let round_away = if abs_r > half {
        true
    } else if abs_r < half {
        false
    } else if half_even {
        // Tie: keep the even multiple of `factor`.
        (q / factor) % 2 != 0
    } else {
        // Tie: HALF_UP rounds away from zero.
        true
    };
    if round_away {
        if v >= 0 {
            q + factor
        } else {
            q - factor
        }
    } else {
        q
    }
}

/// Evaluate `round`/`bround` for an integral value array, preserving the input integral type and
/// raising `ARITHMETIC_OVERFLOW` (ANSI) when a rounded value escapes that type's range.
fn round_integral(args: &ScalarFunctionArgs, half_even: bool) -> Result<ColumnarValue> {
    let n = args.number_rows;
    let value = args.args[0].clone().into_array(n)?;
    let value_type = value.data_type().clone();
    let (lo, hi) = match int_bounds(&value_type) {
        Some(b) => b,
        None => return internal_err!("round_integral called on non-integral type {value_type}"),
    };

    // Decimal places: default 0; coerced to Int32 (already Int32 after `coerce_types`, but we cast
    // defensively to tolerate any caller).
    let dp: Int32Array = if args.args.len() >= 2 {
        let s = args.args[1].clone().into_array(n)?;
        let s = datafusion::arrow::compute::cast(&s, &DataType::Int32)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
        s.as_primitive::<Int32Type>().clone()
    } else {
        Int32Array::from(vec![0i32; n])
    };

    let both_scalar = matches!(&args.args[0], ColumnarValue::Scalar(_))
        && (args.args.len() < 2 || matches!(&args.args[1], ColumnarValue::Scalar(_)));

    macro_rules! build {
        ($ArrowTy:ty, $Native:ty, $ArrayTy:ty) => {{
            let a = value.as_primitive::<$ArrowTy>();
            let mut out: Vec<Option<$Native>> = Vec::with_capacity(n);
            for i in 0..n {
                if a.is_null(i) || dp.is_null(i) {
                    out.push(None);
                    continue;
                }
                let rounded = round_int(a.value(i) as i128, dp.value(i), half_even);
                if rounded < lo || rounded > hi {
                    return exec_err!("[ARITHMETIC_OVERFLOW] Overflow");
                }
                out.push(Some(rounded as $Native));
            }
            Arc::new(<$ArrayTy>::from(out)) as ArrayRef
        }};
    }

    let arr: ArrayRef = match value_type {
        DataType::Int8 => build!(Int8Type, i8, Int8Array),
        DataType::Int16 => build!(Int16Type, i16, Int16Array),
        DataType::Int32 => build!(Int32Type, i32, Int32Array),
        DataType::Int64 => build!(Int64Type, i64, Int64Array),
        other => return internal_err!("unreachable integral type {other}"),
    };

    if both_scalar {
        Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(&arr, 0)?))
    } else {
        Ok(ColumnarValue::Array(arr))
    }
}

#[cfg(test)]
mod tests {
    use super::round_int;

    #[test]
    fn half_up_matches_spark_golden() {
        // round(25y, *) family (HALF_UP).
        assert_eq!(round_int(25, 1, false), 25);
        assert_eq!(round_int(25, 0, false), 25);
        assert_eq!(round_int(25, -1, false), 30);
        assert_eq!(round_int(25, -2, false), 0);
        assert_eq!(round_int(25, -3, false), 0);
        // round(525, *) family.
        assert_eq!(round_int(525, -1, false), 530);
        assert_eq!(round_int(525, -2, false), 500);
        assert_eq!(round_int(525, -3, false), 1000);
        // Overflowing rounds compute the out-of-range value (caller range-checks it).
        assert_eq!(round_int(127, -1, false), 130);
        assert_eq!(round_int(-128, -1, false), -130);
        assert_eq!(round_int(i64::MAX as i128, -1, false), 9223372036854775810);
    }

    #[test]
    fn half_even_matches_spark_golden() {
        // bround(25y, *) family (HALF_EVEN).
        assert_eq!(round_int(25, 1, true), 25);
        assert_eq!(round_int(25, -1, true), 20); // tie → even (20)
        assert_eq!(round_int(25, -2, true), 0);
        // bround(525, *) family.
        assert_eq!(round_int(525, -1, true), 520); // tie → even (520)
        assert_eq!(round_int(525, -2, true), 500);
        assert_eq!(round_int(525, -3, true), 1000);
    }

    #[tokio::test]
    async fn round_preserves_integral_type_and_value() {
        let engine = crate::Engine::new();
        for (q, want) in [
            ("SELECT round(25y, -1) AS x", "30"),
            ("SELECT round(525s, -1) AS x", "530"),
            ("SELECT bround(25y, -1) AS x", "20"),
            ("SELECT bround(525L, -1) AS x", "520"),
            ("SELECT round(525, 0) AS x", "525"),
        ] {
            let batches = engine.sql(q).await.unwrap_or_else(|e| panic!("{q}: {e}"));
            let got = crate::arrow::util::pretty::pretty_format_batches(&batches)
                .unwrap()
                .to_string();
            assert!(got.contains(want), "{q} -> want {want}, got:\n{got}");
        }
    }

    #[tokio::test]
    async fn round_overflow_is_an_error() {
        let engine = crate::Engine::new();
        // round(127y, -1) = 130 overflows tinyint under ANSI.
        assert!(engine.sql("SELECT round(127y, -1)").await.is_err());
        assert!(engine.sql("SELECT round(-128y, -1)").await.is_err());
        assert!(engine.sql("SELECT round(32767s, -1)").await.is_err());
    }

    #[tokio::test]
    async fn round_on_double_is_unchanged() {
        // Non-integral inputs delegate to the builtin (HALF_UP double rounding).
        let engine = crate::Engine::new();
        let batches = engine
            .sql("SELECT round(2.5D, 0) AS x")
            .await
            .expect("round(double) should work");
        let got = crate::arrow::util::pretty::pretty_format_batches(&batches)
            .unwrap()
            .to_string();
        assert!(got.contains("3"), "round(2.5D,0) -> want 3.0, got:\n{got}");
    }
}

//! Spark math functions with no DataFusion 54 built-in.
//!
//! [`super::spark_math`] overrides `round`/`bround` for Spark's integral semantics; this file adds
//! the names DataFusion simply does not register. They fall into three shapes:
//!
//! **Double-in, double-out** — `e()`, `expm1`, `log1p`, `sec`, `csc`, `rint`, `hypot`. Spark
//! evaluates all of these in IEEE double, so the argument is widened to `DOUBLE` and the result is
//! `DOUBLE`. `rint` rounds half **to even** (`rint(2.5)` = 2.0), unlike `round`'s half-up — the
//! same distinction `bround` draws.
//!
//! **Type-preserving integer work** — `negative` (Spark's `UnaryMinus`, the counterpart of the
//! existing `positive`) and `bit_reverse`, which reverses the bits *within the argument's own
//! width*, so `bit_reverse(CAST(1 AS TINYINT))` is `-128`, not a 64-bit value.
//!
//! **Lowered to native expressions** — `mod` and `pmod`. Both are registered as UDFs whose
//! [`ScalarUDFImpl::simplify`] rewrites them into arithmetic the optimizer understands, so they
//! pick up DataFusion's own numeric coercion and constant folding instead of an opaque call.
//! `mod(a, b)` is `a % b` (remainder, taking the sign of the dividend). `pmod(a, b)` is the
//! *positive* modulo `((a % b) + abs(b)) % b`, which is Spark's rule in both signs:
//! `pmod(-7, 3)` = 2 and `pmod(-7, -3)` = 2.
//!
//! **`width_bucket(value, min, max, numBuckets)`** returns the 1-based bucket of an equi-width
//! histogram over `[min, max)`, as `bigint`, with Spark's out-of-range conventions: `0` below the
//! range, `numBuckets + 1` at or above it, and a reversed (`min > max`) range counting downward.
//! `numBuckets <= 0`, or a NaN/infinite bound, is an error in Spark rather than a NULL.

use std::sync::Arc;

use datafusion::arrow::array::{
    Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array,
};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{exec_err, plan_err, DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion::logical_expr::type_coercion::binary::binary_numeric_coercion;
use datafusion::logical_expr::{
    BinaryExpr, ColumnarValue, Expr, Operator, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl,
    Signature, Volatility,
};
use datafusion::prelude::SessionContext;

/// Register the Spark math functions DataFusion 54 does not provide.
pub fn register(ctx: &SessionContext) {
    for f in [
        DoubleFn::E,
        DoubleFn::Expm1,
        DoubleFn::Log1p,
        DoubleFn::Sec,
        DoubleFn::Csc,
        DoubleFn::Rint,
        DoubleFn::Hypot,
    ] {
        ctx.register_udf(ScalarUDF::from(DoubleMath::new(f)));
    }
    ctx.register_udf(ScalarUDF::from(Negative::new()));
    ctx.register_udf(ScalarUDF::from(BitReverse::new()));
    ctx.register_udf(ScalarUDF::from(Modulo::new("mod", false)));
    ctx.register_udf(ScalarUDF::from(Modulo::new("pmod", true)));
    ctx.register_udf(ScalarUDF::from(WidthBucket::new()));
}

fn arrow_err(e: datafusion::arrow::error::ArrowError) -> DataFusionError {
    DataFusionError::ArrowError(Box::new(e), None)
}

fn to_f64(v: &ColumnarValue, n: usize) -> Result<Float64Array> {
    let arr = v.clone().into_array(n)?;
    Ok(datafusion::arrow::compute::cast(&arr, &DataType::Float64)
        .map_err(arrow_err)?
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("cast to Float64 yields Float64Array")
        .clone())
}

// ---------------------------------------------------------------------------
// double-in / double-out
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum DoubleFn {
    /// `e()` — Euler's number. Nullary.
    E,
    Expm1,
    Log1p,
    Sec,
    Csc,
    Rint,
    /// Binary.
    Hypot,
}

impl DoubleFn {
    fn name(self) -> &'static str {
        match self {
            DoubleFn::E => "e",
            DoubleFn::Expm1 => "expm1",
            DoubleFn::Log1p => "log1p",
            DoubleFn::Sec => "sec",
            DoubleFn::Csc => "csc",
            DoubleFn::Rint => "rint",
            DoubleFn::Hypot => "hypot",
        }
    }
    fn arity(self) -> usize {
        match self {
            DoubleFn::E => 0,
            DoubleFn::Hypot => 2,
            _ => 1,
        }
    }
}

/// The double-precision math functions.
#[derive(Debug, PartialEq, Eq, Hash)]
struct DoubleMath {
    f: DoubleFn,
    signature: Signature,
}

impl DoubleMath {
    fn new(f: DoubleFn) -> Self {
        let signature = match f.arity() {
            0 => Signature::nullary(Volatility::Immutable),
            k => Signature::any(k, Volatility::Immutable),
        };
        Self { f, signature }
    }
}

impl ScalarUDFImpl for DoubleMath {
    fn name(&self) -> &str {
        self.f.name()
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if self.f == DoubleFn::E {
            return Ok(ColumnarValue::Scalar(ScalarValue::Float64(Some(
                std::f64::consts::E,
            ))));
        }
        let n = args.number_rows;
        let a = to_f64(&args.args[0], n)?;
        let b = if self.f == DoubleFn::Hypot {
            Some(to_f64(&args.args[1], n)?)
        } else {
            None
        };
        let mut out = Float64Array::builder(n);
        for i in 0..n {
            if a.is_null(i) || b.as_ref().is_some_and(|b| b.is_null(i)) {
                out.append_null();
                continue;
            }
            let x = a.value(i);
            out.append_value(match self.f {
                DoubleFn::E => unreachable!("handled above"),
                DoubleFn::Expm1 => x.exp_m1(),
                DoubleFn::Log1p => x.ln_1p(),
                DoubleFn::Sec => 1.0 / x.cos(),
                DoubleFn::Csc => 1.0 / x.sin(),
                // Half-to-even, matching Java's `Math.rint` (which is what Spark calls).
                DoubleFn::Rint => rint(x),
                DoubleFn::Hypot => x.hypot(b.as_ref().expect("hypot has 2 args").value(i)),
            });
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

/// Round half to even, like `Math.rint`. Rust's `f64::round` is half-away-from-zero, and
/// `round_ties_even` is newer than this crate's MSRV, so do it explicitly.
fn rint(x: f64) -> f64 {
    let r = x.round();
    if (x - x.trunc()).abs() == 0.5 && r % 2.0 != 0.0 {
        // Exactly on a tie and `round` went to the odd neighbour: step back one.
        r - x.signum()
    } else {
        r
    }
}

// ---------------------------------------------------------------------------
// negative
// ---------------------------------------------------------------------------

/// `negative(x)` — Spark's `UnaryMinus`. Type-preserving, the mirror of the existing `positive`.
#[derive(Debug, PartialEq, Eq, Hash)]
struct Negative {
    signature: Signature,
}

impl Negative {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for Negative {
    fn name(&self) -> &str {
        "negative"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }
    fn simplify(&self, args: Vec<Expr>, _info: &SimplifyContext) -> Result<ExprSimplifyResult> {
        let mut it = args.into_iter();
        let (Some(arg), None) = (it.next(), it.next()) else {
            return plan_err!("negative expects exactly 1 argument");
        };
        // `Expr::Negative` keeps the argument's type and picks up the same overflow behaviour a
        // literal `-x` has, which is exactly Spark's `UnaryMinus`.
        Ok(ExprSimplifyResult::Simplified(Expr::Negative(Box::new(
            arg,
        ))))
    }
    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        exec_err!("negative should have been simplified to unary minus before execution")
    }
}

// ---------------------------------------------------------------------------
// bit_reverse
// ---------------------------------------------------------------------------

/// `bit_reverse(x)` — reverse the bits of an integer *within its own width*.
#[derive(Debug, PartialEq, Eq, Hash)]
struct BitReverse {
    signature: Signature,
}

impl BitReverse {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for BitReverse {
    fn name(&self) -> &str {
        "bit_reverse"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match &arg_types[0] {
            t @ (DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64) => {
                Ok(t.clone())
            }
            // Spark accepts only the integral types here; anything else is a type error rather
            // than a silent widening that would change the bit width and therefore the answer.
            other => plan_err!(
                "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] bit_reverse requires an integral type, \
                 got {other}"
            ),
        }
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let n = args.number_rows;
        let arr = args.args[0].clone().into_array(n)?;
        // The width is the whole point, so dispatch on the concrete array type instead of
        // widening to i64 first.
        macro_rules! reverse {
            ($arrow_ty:ty, $prim:ty) => {{
                let a = arr
                    .as_any()
                    .downcast_ref::<$arrow_ty>()
                    .expect("width checked");
                let mut out = <$arrow_ty>::builder(n);
                for i in 0..n {
                    if a.is_null(i) {
                        out.append_null();
                    } else {
                        out.append_value((a.value(i) as $prim).reverse_bits() as _);
                    }
                }
                Ok(ColumnarValue::Array(Arc::new(out.finish())))
            }};
        }
        match arr.data_type() {
            DataType::Int8 => reverse!(Int8Array, u8),
            DataType::Int16 => reverse!(Int16Array, u16),
            DataType::Int32 => reverse!(Int32Array, u32),
            DataType::Int64 => reverse!(Int64Array, u64),
            other => exec_err!("bit_reverse requires an integral type, got {other}"),
        }
    }
}

// ---------------------------------------------------------------------------
// mod / pmod
// ---------------------------------------------------------------------------

/// `mod(a, b)` (`positive = false`) and `pmod(a, b)` (`positive = true`).
#[derive(Debug, PartialEq, Eq, Hash)]
struct Modulo {
    name: &'static str,
    positive: bool,
    signature: Signature,
}

impl Modulo {
    fn new(name: &'static str, positive: bool) -> Self {
        Self {
            name,
            positive,
            // `user_defined` so `coerce_types` widens both operands the way `%` would; the
            // rewritten `BinaryExpr` is planned after coercion and needs matching types.
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for Modulo {
    fn name(&self) -> &str {
        self.name
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let [a, b] = arg_types else {
            return plan_err!("{} expects exactly 2 arguments", self.name);
        };
        match binary_numeric_coercion(a, b) {
            Some(common) => Ok(vec![common.clone(), common]),
            None => plan_err!("{}: non-numeric operand types ({a} and {b})", self.name),
        }
    }
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }
    fn simplify(&self, args: Vec<Expr>, _info: &SimplifyContext) -> Result<ExprSimplifyResult> {
        let mut it = args.into_iter();
        let (Some(a), Some(b), None) = (it.next(), it.next(), it.next()) else {
            return plan_err!("{} expects exactly 2 arguments", self.name);
        };
        let rem = |l: Expr, r: Expr| {
            Expr::BinaryExpr(BinaryExpr::new(Box::new(l), Operator::Modulo, Box::new(r)))
        };
        if !self.positive {
            return Ok(ExprSimplifyResult::Simplified(rem(a, b)));
        }
        // `((a % b) + abs(b)) % b` is Spark's positive modulo in all four sign combinations:
        // pmod(7, 3) = 1, pmod(-7, 3) = 2, pmod(7, -3) = 1, pmod(-7, -3) = 2.
        let abs_b = Expr::ScalarFunction(datafusion::logical_expr::expr::ScalarFunction::new_udf(
            datafusion::functions::math::abs(),
            vec![b.clone()],
        ));
        let shifted = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(rem(a, b.clone())),
            Operator::Plus,
            Box::new(abs_b),
        ));
        Ok(ExprSimplifyResult::Simplified(rem(shifted, b)))
    }
    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        exec_err!(
            "{} should have been simplified to arithmetic before execution",
            self.name
        )
    }
}

// ---------------------------------------------------------------------------
// width_bucket
// ---------------------------------------------------------------------------

/// `width_bucket(value, min, max, numBuckets)` — equi-width histogram bucket, as `bigint`.
#[derive(Debug, PartialEq, Eq, Hash)]
struct WidthBucket {
    signature: Signature,
}

impl WidthBucket {
    fn new() -> Self {
        Self {
            signature: Signature::any(4, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for WidthBucket {
    fn name(&self) -> &str {
        "width_bucket"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let n = args.number_rows;
        let value = to_f64(&args.args[0], n)?;
        let min = to_f64(&args.args[1], n)?;
        let max = to_f64(&args.args[2], n)?;
        let buckets = to_f64(&args.args[3], n)?;

        let mut out = Int64Array::builder(n);
        for i in 0..n {
            if value.is_null(i) || min.is_null(i) || max.is_null(i) || buckets.is_null(i) {
                out.append_null();
                continue;
            }
            let (v, lo, hi, nb) = (value.value(i), min.value(i), max.value(i), buckets.value(i));
            if nb <= 0.0 || nb.is_nan() || nb.is_infinite() {
                return exec_err!(
                    "[INVALID_PARAMETER_VALUE] width_bucket: numBuckets must be a positive \
                     finite value, got {nb}"
                );
            }
            if lo.is_nan() || hi.is_nan() || lo.is_infinite() || hi.is_infinite() {
                return exec_err!(
                    "[INVALID_PARAMETER_VALUE] width_bucket: the range bounds must be finite, \
                     got [{lo}, {hi}]"
                );
            }
            if lo == hi {
                return exec_err!(
                    "[INVALID_PARAMETER_VALUE] width_bucket: the range bounds must differ, \
                     got [{lo}, {hi}]"
                );
            }
            let nb = nb.trunc();
            // A reversed range (min > max) counts downward — the same formula with the comparisons
            // flipped, which is what Spark's `WidthBucket` does.
            let below = if lo < hi { v < lo } else { v > lo };
            let above = if lo < hi { v >= hi } else { v <= hi };
            let bucket = if v.is_nan() {
                // Spark puts NaN above the range.
                nb as i64 + 1
            } else if below {
                0
            } else if above {
                nb as i64 + 1
            } else {
                ((v - lo) / (hi - lo) * nb).floor() as i64 + 1
            };
            out.append_value(bucket);
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn row(q: &str) -> String {
        let engine = crate::Engine::new();
        let batches = engine.sql(q).await.unwrap_or_else(|e| panic!("{q}: {e}"));
        crate::arrow::util::pretty::pretty_format_batches(&batches)
            .unwrap()
            .to_string()
    }

    #[tokio::test]
    async fn double_math_matches_ieee() {
        for (q, want) in [
            ("SELECT e() AS x", "2.718281828459045"),
            ("SELECT expm1(CAST(0 AS DOUBLE)) AS x", "0.0"),
            ("SELECT log1p(CAST(0 AS DOUBLE)) AS x", "0.0"),
            ("SELECT sec(CAST(0 AS DOUBLE)) AS x", "1.0"),
            (
                "SELECT hypot(CAST(3 AS DOUBLE), CAST(4 AS DOUBLE)) AS x",
                "5.0",
            ),
            // `rint` is half-to-even, so 2.5 goes down and 3.5 goes up.
            ("SELECT rint(CAST(2.5 AS DOUBLE)) AS x", "2.0"),
            ("SELECT rint(CAST(3.5 AS DOUBLE)) AS x", "4.0"),
            ("SELECT rint(CAST(-2.5 AS DOUBLE)) AS x", "-2.0"),
            ("SELECT rint(CAST(2.4 AS DOUBLE)) AS x", "2.0"),
        ] {
            let got = row(q).await;
            assert!(got.contains(want), "{q} -> want {want}, got:\n{got}");
        }
    }

    #[test]
    fn rint_is_half_to_even() {
        assert_eq!(rint(2.5), 2.0);
        assert_eq!(rint(3.5), 4.0);
        assert_eq!(rint(-2.5), -2.0);
        assert_eq!(rint(-3.5), -4.0);
        assert_eq!(rint(0.5), 0.0);
        assert_eq!(rint(1.5), 2.0);
        assert_eq!(rint(2.4), 2.0);
        assert_eq!(rint(2.6), 3.0);
    }

    #[tokio::test]
    async fn negative_preserves_the_argument_type() {
        for (q, want) in [
            ("SELECT negative(1) AS x", "-1"),
            ("SELECT negative(-1) AS x", "1"),
            (
                "SELECT typeof(negative(CAST(1 AS TINYINT))) AS x",
                "tinyint",
            ),
            ("SELECT typeof(negative(CAST(1 AS BIGINT))) AS x", "bigint"),
        ] {
            let got = row(q).await;
            assert!(got.contains(want), "{q} -> want {want}, got:\n{got}");
        }
    }

    /// The width matters: reversing a `tinyint` 1 must give -128, not a 64-bit number.
    #[tokio::test]
    async fn bit_reverse_respects_the_integer_width() {
        for (q, want) in [
            ("SELECT bit_reverse(CAST(1 AS TINYINT)) AS x", "-128"),
            ("SELECT bit_reverse(CAST(1 AS SMALLINT)) AS x", "-32768"),
            ("SELECT bit_reverse(CAST(1 AS INT)) AS x", "-2147483648"),
            (
                "SELECT bit_reverse(CAST(1 AS BIGINT)) AS x",
                "-9223372036854775808",
            ),
            ("SELECT bit_reverse(CAST(0 AS INT)) AS x", "0"),
            ("SELECT bit_reverse(CAST(-1 AS INT)) AS x", "-1"),
        ] {
            let got = row(q).await;
            assert!(got.contains(want), "{q} -> want {want}, got:\n{got}");
        }
    }

    /// `mod` takes the sign of the dividend; `pmod` never returns a negative.
    #[tokio::test]
    async fn mod_and_pmod_signs_match_spark() {
        for (q, want) in [
            ("SELECT mod(7, 3) AS x", "1"),
            ("SELECT mod(-7, 3) AS x", "-1"),
            ("SELECT mod(7, -3) AS x", "1"),
            ("SELECT pmod(7, 3) AS x", "1"),
            ("SELECT pmod(-7, 3) AS x", "2"),
            ("SELECT pmod(7, -3) AS x", "1"),
            ("SELECT pmod(-7, -3) AS x", "2"),
        ] {
            let got = row(q).await;
            assert!(got.contains(want), "{q} -> want {want}, got:\n{got}");
        }
    }

    #[tokio::test]
    async fn width_bucket_ranges_and_edges() {
        for (q, want) in [
            // Five buckets over [0, 10): 5.0 lands in bucket 3.
            ("SELECT width_bucket(5.0, 0.0, 10.0, 5) AS x", "3"),
            ("SELECT width_bucket(0.0, 0.0, 10.0, 5) AS x", "1"),
            // Below the range is 0; at or above the top is numBuckets + 1.
            ("SELECT width_bucket(-1.0, 0.0, 10.0, 5) AS x", "0"),
            ("SELECT width_bucket(10.0, 0.0, 10.0, 5) AS x", "6"),
            ("SELECT width_bucket(11.0, 0.0, 10.0, 5) AS x", "6"),
            // A reversed range counts downward.
            ("SELECT width_bucket(5.0, 10.0, 0.0, 5) AS x", "3"),
            ("SELECT width_bucket(11.0, 10.0, 0.0, 5) AS x", "0"),
        ] {
            let got = row(q).await;
            assert!(got.contains(want), "{q} -> want {want}, got:\n{got}");
        }
    }

    #[tokio::test]
    async fn width_bucket_rejects_a_degenerate_range() {
        let engine = crate::Engine::new();
        for q in [
            "SELECT width_bucket(5.0, 0.0, 10.0, 0) AS x",
            "SELECT width_bucket(5.0, 3.0, 3.0, 5) AS x",
        ] {
            let err = engine.sql(q).await.expect_err("must reject");
            assert!(
                format!("{err}").contains("INVALID_PARAMETER_VALUE"),
                "{q}: {err}"
            );
        }
    }
}

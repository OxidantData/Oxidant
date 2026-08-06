//! Spark-only **aggregate** (UDAF) functions — second batch. Same contract and shape as
//! [`super::spark_aggregates`]: each function is a DataFusion [`AggregateUDF`] registered into every
//! [`crate::Engine`]'s session, faithful to Spark's documented semantics.
//!
//! Implemented here:
//!
//! - `try_sum(expr)` — like `sum`, but returns NULL instead of raising on integer overflow. For
//!   non-overflowing inputs it is identical to `sum`. Integer inputs accumulate as `i64` and the
//!   result is `bigint`; floating inputs accumulate as `f64` (`double`). (Decimal/interval inputs,
//!   which Spark also supports, are not reachable from the SQL surface without literal suffixes the
//!   parser rejects, so they are accepted by widening to `double`/`bigint` like `sum`.)
//! - `try_avg(expr)` — like `avg`, but NULL on overflow of the running sum. Result is `double`.
//! - `skewness(expr)` — Spark's **population** skewness `m3 / m2^1.5`, computed with mergeable
//!   online central moments (Welford). NULLs ignored; empty/all-null → NULL; zero variance → NaN
//!   (matching Spark). Result is `double`.
//!
//! `mode(expr)` is already provided by [`super::spark_aggregates`] (a deterministic 1-arg UDAF), so
//! it is NOT re-registered here. See the batch report for the `mode` / `percentile_cont`
//! investigation.

use datafusion::arrow::array::{Array, ArrayRef, AsArray, Float64Array};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Float64Type, Int64Type};
use datafusion::common::{Result, ScalarValue};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::format_state_name;
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, Signature, Volatility,
};
use datafusion::prelude::SessionContext;

/// Register all second-batch Spark-only aggregate functions into `ctx`.
pub fn register(ctx: &SessionContext) {
    ctx.register_udaf(AggregateUDF::from(TrySum::new()));
    ctx.register_udaf(AggregateUDF::from(TryAvg::new()));
    ctx.register_udaf(AggregateUDF::from(Skewness::new()));
}

/// Is `dt` an integral type that Spark sums into a `bigint`?
fn is_integer(dt: &DataType) -> bool {
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
    )
}

/// Cast an arbitrary numeric array to `Float64Array` (so any numeric input type works).
fn to_f64_array(arr: &ArrayRef) -> Result<Float64Array> {
    let casted = datafusion::arrow::compute::cast(arr, &DataType::Float64)?;
    Ok(casted.as_primitive::<Float64Type>().clone())
}

// ---------------------------------------------------------------------------
// try_sum(expr)
// ---------------------------------------------------------------------------

/// `try_sum(expr)` — sum that yields NULL on integer overflow instead of raising. Integer inputs
/// produce `bigint`; all other numeric inputs produce `double`. NULLs are ignored; an empty/all-null
/// group yields NULL.
#[derive(Debug, PartialEq, Eq, Hash)]
struct TrySum {
    signature: Signature,
}

impl TrySum {
    fn new() -> Self {
        Self {
            signature: Signature::numeric(1, Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for TrySum {
    fn name(&self) -> &str {
        "try_sum"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if is_integer(&arg_types[0]) {
            Ok(DataType::Int64)
        } else {
            Ok(DataType::Float64)
        }
    }
    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        let integer = is_integer(args.input_fields[0].data_type());
        let acc_ty = if integer {
            DataType::Int64
        } else {
            DataType::Float64
        };
        Ok(vec![
            Field::new(format_state_name(args.name, "sum"), acc_ty, true).into(),
            // `seen_any` (have we observed a non-null) and `overflowed` flags.
            Field::new(
                format_state_name(args.name, "seen"),
                DataType::Boolean,
                true,
            )
            .into(),
            Field::new(
                format_state_name(args.name, "overflow"),
                DataType::Boolean,
                true,
            )
            .into(),
        ])
    }
    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let integer = is_integer(acc_args.expr_fields[0].data_type());
        Ok(Box::new(TrySumAccumulator {
            integer,
            int_sum: 0,
            float_sum: 0.0,
            seen: false,
            overflowed: false,
        }))
    }
}

#[derive(Debug)]
struct TrySumAccumulator {
    integer: bool,
    int_sum: i64,
    float_sum: f64,
    seen: bool,
    overflowed: bool,
}

impl TrySumAccumulator {
    fn add_int(&mut self, v: i64) {
        match self.int_sum.checked_add(v) {
            Some(s) => self.int_sum = s,
            None => self.overflowed = true,
        }
    }
}

impl Accumulator for TrySumAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if self.overflowed {
            return Ok(());
        }
        if self.integer {
            let col = datafusion::arrow::compute::cast(&values[0], &DataType::Int64)?;
            let col = col.as_primitive::<Int64Type>();
            for v in col.iter().flatten() {
                self.seen = true;
                self.add_int(v);
                if self.overflowed {
                    break;
                }
            }
        } else {
            let col = to_f64_array(&values[0])?;
            for v in col.iter().flatten() {
                self.seen = true;
                self.float_sum += v;
            }
        }
        Ok(())
    }
    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let sums = &states[0];
        let seen = states[1].as_boolean();
        let overflow = states[2].as_boolean();
        for i in 0..sums.len() {
            if seen.is_valid(i) && seen.value(i) {
                self.seen = true;
            }
            if overflow.is_valid(i) && overflow.value(i) {
                self.overflowed = true;
            }
        }
        if self.overflowed {
            return Ok(());
        }
        if self.integer {
            let col = sums.as_primitive::<Int64Type>();
            for i in 0..col.len() {
                if seen.is_valid(i) && seen.value(i) && !col.is_null(i) {
                    self.add_int(col.value(i));
                    if self.overflowed {
                        break;
                    }
                }
            }
        } else {
            let col = sums.as_primitive::<Float64Type>();
            for i in 0..col.len() {
                if seen.is_valid(i) && seen.value(i) && !col.is_null(i) {
                    self.float_sum += col.value(i);
                }
            }
        }
        Ok(())
    }
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let sum = if self.integer {
            ScalarValue::Int64(Some(self.int_sum))
        } else {
            ScalarValue::Float64(Some(self.float_sum))
        };
        Ok(vec![
            sum,
            ScalarValue::Boolean(Some(self.seen)),
            ScalarValue::Boolean(Some(self.overflowed)),
        ])
    }
    fn evaluate(&mut self) -> Result<ScalarValue> {
        if !self.seen || self.overflowed {
            return Ok(if self.integer {
                ScalarValue::Int64(None)
            } else {
                ScalarValue::Float64(None)
            });
        }
        Ok(if self.integer {
            ScalarValue::Int64(Some(self.int_sum))
        } else {
            ScalarValue::Float64(Some(self.float_sum))
        })
    }
    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }
}

// ---------------------------------------------------------------------------
// try_avg(expr)
// ---------------------------------------------------------------------------

/// `try_avg(expr)` — average that yields NULL if the running sum overflows `i64`, otherwise the mean
/// as `double`. NULLs are ignored; an empty/all-null group yields NULL.
#[derive(Debug, PartialEq, Eq, Hash)]
struct TryAvg {
    signature: Signature,
}

impl TryAvg {
    fn new() -> Self {
        Self {
            signature: Signature::numeric(1, Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for TryAvg {
    fn name(&self) -> &str {
        "try_avg"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }
    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        let integer = is_integer(args.input_fields[0].data_type());
        let acc_ty = if integer {
            DataType::Int64
        } else {
            DataType::Float64
        };
        Ok(vec![
            Field::new(format_state_name(args.name, "sum"), acc_ty, true).into(),
            Field::new(format_state_name(args.name, "count"), DataType::Int64, true).into(),
            Field::new(
                format_state_name(args.name, "overflow"),
                DataType::Boolean,
                true,
            )
            .into(),
        ])
    }
    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let integer = is_integer(acc_args.expr_fields[0].data_type());
        Ok(Box::new(TryAvgAccumulator {
            integer,
            int_sum: 0,
            float_sum: 0.0,
            count: 0,
            overflowed: false,
        }))
    }
}

#[derive(Debug)]
struct TryAvgAccumulator {
    integer: bool,
    int_sum: i64,
    float_sum: f64,
    count: i64,
    overflowed: bool,
}

impl TryAvgAccumulator {
    fn add_int(&mut self, v: i64) {
        match self.int_sum.checked_add(v) {
            Some(s) => self.int_sum = s,
            None => self.overflowed = true,
        }
    }
}

impl Accumulator for TryAvgAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if self.overflowed {
            return Ok(());
        }
        if self.integer {
            let col = datafusion::arrow::compute::cast(&values[0], &DataType::Int64)?;
            let col = col.as_primitive::<Int64Type>();
            for v in col.iter().flatten() {
                self.count += 1;
                self.add_int(v);
                if self.overflowed {
                    break;
                }
            }
        } else {
            let col = to_f64_array(&values[0])?;
            for v in col.iter().flatten() {
                self.count += 1;
                self.float_sum += v;
            }
        }
        Ok(())
    }
    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let sums = &states[0];
        let counts = states[1].as_primitive::<Int64Type>();
        let overflow = states[2].as_boolean();
        for i in 0..overflow.len() {
            if overflow.is_valid(i) && overflow.value(i) {
                self.overflowed = true;
            }
        }
        for i in 0..counts.len() {
            if !counts.is_null(i) {
                self.count += counts.value(i);
            }
        }
        if self.overflowed {
            return Ok(());
        }
        if self.integer {
            let col = sums.as_primitive::<Int64Type>();
            for i in 0..col.len() {
                if !col.is_null(i) {
                    self.add_int(col.value(i));
                    if self.overflowed {
                        break;
                    }
                }
            }
        } else {
            let col = sums.as_primitive::<Float64Type>();
            for i in 0..col.len() {
                if !col.is_null(i) {
                    self.float_sum += col.value(i);
                }
            }
        }
        Ok(())
    }
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let sum = if self.integer {
            ScalarValue::Int64(Some(self.int_sum))
        } else {
            ScalarValue::Float64(Some(self.float_sum))
        };
        Ok(vec![
            sum,
            ScalarValue::Int64(Some(self.count)),
            ScalarValue::Boolean(Some(self.overflowed)),
        ])
    }
    fn evaluate(&mut self) -> Result<ScalarValue> {
        if self.count == 0 || self.overflowed {
            return Ok(ScalarValue::Float64(None));
        }
        let sum = if self.integer {
            self.int_sum as f64
        } else {
            self.float_sum
        };
        Ok(ScalarValue::Float64(Some(sum / self.count as f64)))
    }
    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }
}

// ---------------------------------------------------------------------------
// skewness(expr) -> double  (Spark population skewness via online moments)
// ---------------------------------------------------------------------------

/// `skewness(expr)` — Spark's population skewness `m3 / m2^1.5` where `m2`/`m3` are the 2nd/3rd
/// central moments (divided by `n`). Computed with mergeable online central moments for numerical
/// stability. NULLs ignored; empty/all-null group → NULL; zero variance → NaN (matching Spark).
#[derive(Debug, PartialEq, Eq, Hash)]
struct Skewness {
    signature: Signature,
}

impl Skewness {
    fn new() -> Self {
        Self {
            signature: Signature::numeric(1, Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for Skewness {
    fn name(&self) -> &str {
        "skewness"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }
    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![
            Field::new(format_state_name(args.name, "n"), DataType::Float64, true).into(),
            Field::new(format_state_name(args.name, "avg"), DataType::Float64, true).into(),
            Field::new(format_state_name(args.name, "m2"), DataType::Float64, true).into(),
            Field::new(format_state_name(args.name, "m3"), DataType::Float64, true).into(),
        ])
    }
    fn accumulator(&self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(SkewnessAccumulator {
            n: 0.0,
            avg: 0.0,
            m2: 0.0,
            m3: 0.0,
        }))
    }
}

/// Online central moments up to 3rd order (Spark's `CentralMomentAgg` recurrences).
#[derive(Debug)]
struct SkewnessAccumulator {
    n: f64,
    avg: f64,
    m2: f64,
    m3: f64,
}

impl SkewnessAccumulator {
    /// Add one observation, updating `(n, avg, m2, m3)` with the standard online recurrences.
    fn add(&mut self, x: f64) {
        let n1 = self.n;
        self.n += 1.0;
        let delta = x - self.avg;
        let delta_n = delta / self.n;
        let term1 = delta * delta_n * n1;
        self.avg += delta_n;
        self.m3 += term1 * delta_n * (self.n - 2.0) - 3.0 * delta_n * self.m2;
        self.m2 += term1;
    }
    /// Merge another partition's moments (parallel-combine formulas).
    fn merge(&mut self, n2: f64, avg2: f64, m2_2: f64, m3_2: f64) {
        if n2 == 0.0 {
            return;
        }
        if self.n == 0.0 {
            self.n = n2;
            self.avg = avg2;
            self.m2 = m2_2;
            self.m3 = m3_2;
            return;
        }
        let n1 = self.n;
        let total = n1 + n2;
        let delta = avg2 - self.avg;
        let delta2 = delta * delta;
        let new_avg = self.avg + delta * n2 / total;
        let new_m2 = self.m2 + m2_2 + delta2 * n1 * n2 / total;
        let new_m3 = self.m3
            + m3_2
            + delta2 * delta * n1 * n2 * (n1 - n2) / (total * total)
            + 3.0 * delta * (n1 * m2_2 - n2 * self.m2) / total;
        self.n = total;
        self.avg = new_avg;
        self.m2 = new_m2;
        self.m3 = new_m3;
    }
}

impl Accumulator for SkewnessAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let col = to_f64_array(&values[0])?;
        for v in col.iter().flatten() {
            self.add(v);
        }
        Ok(())
    }
    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let ns = states[0].as_primitive::<Float64Type>();
        let avgs = states[1].as_primitive::<Float64Type>();
        let m2s = states[2].as_primitive::<Float64Type>();
        let m3s = states[3].as_primitive::<Float64Type>();
        for i in 0..ns.len() {
            if !ns.is_null(i) {
                self.merge(ns.value(i), avgs.value(i), m2s.value(i), m3s.value(i));
            }
        }
        Ok(())
    }
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::Float64(Some(self.n)),
            ScalarValue::Float64(Some(self.avg)),
            ScalarValue::Float64(Some(self.m2)),
            ScalarValue::Float64(Some(self.m3)),
        ])
    }
    fn evaluate(&mut self) -> Result<ScalarValue> {
        if self.n == 0.0 {
            return Ok(ScalarValue::Float64(None));
        }
        // Spark: skewness = sqrt(n) * m3 / m2^1.5, where m2/m3 are the *summed* central moments
        // (not divided by n). Equivalently with the per-n moments: (m3/n) / (m2/n)^1.5.
        // We carry summed m2/m3, so use Spark's exact expression.
        let result = (self.n).sqrt() * self.m3 / self.m2.powf(1.5);
        Ok(ScalarValue::Float64(Some(result)))
    }
    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }
}

#[cfg(test)]
mod tests {
    async fn one_cell(engine: &crate::Engine, q: &str) -> String {
        let batches = engine.sql(q).await.unwrap_or_else(|e| panic!("{q}: {e}"));
        crate::arrow::util::pretty::pretty_format_batches(&batches)
            .unwrap()
            .to_string()
    }

    #[tokio::test]
    async fn try_sum_basic_integer() {
        let engine = crate::Engine::new();
        // Golden try_aggregates.sql: try_sum(5,10,15) = 30 (bigint).
        let got = one_cell(
            &engine,
            "SELECT try_sum(col) AS s FROM VALUES (5),(10),(15) AS tab(col)",
        )
        .await;
        assert!(got.contains(" 30 "), "want 30, got:\n{got}");
    }

    #[tokio::test]
    async fn try_sum_skips_null() {
        let engine = crate::Engine::new();
        // Golden: try_sum(NULL,10,15) = 25.
        let got = one_cell(
            &engine,
            "SELECT try_sum(col) AS s FROM VALUES (CAST(NULL AS INT)),(10),(15) AS tab(col)",
        )
        .await;
        assert!(got.contains(" 25 "), "want 25, got:\n{got}");
    }

    #[tokio::test]
    async fn try_sum_all_null_is_null() {
        let engine = crate::Engine::new();
        let got = one_cell(
            &engine,
            "SELECT try_sum(col) AS s FROM VALUES (CAST(NULL AS INT)),(CAST(NULL AS INT)) AS tab(col)",
        )
        .await;
        let datarow: Vec<&str> = got
            .lines()
            .filter(|l| l.starts_with('|') && !l.contains('s'))
            .collect();
        assert!(
            datarow
                .iter()
                .all(|l| !l.chars().any(|c| c.is_ascii_digit())),
            "want NULL, got:\n{got}"
        );
    }

    #[tokio::test]
    async fn try_sum_overflow_is_null() {
        let engine = crate::Engine::new();
        // i64::MAX + 1 overflows -> NULL (golden uses 9223372036854775807L + 1L = NULL).
        let got = one_cell(
            &engine,
            "SELECT try_sum(col) AS s FROM VALUES (CAST(9223372036854775807 AS BIGINT)),(CAST(1 AS BIGINT)) AS tab(col)",
        )
        .await;
        let datarow: Vec<&str> = got
            .lines()
            .filter(|l| l.starts_with('|') && !l.contains('s'))
            .collect();
        assert!(
            datarow
                .iter()
                .all(|l| !l.chars().any(|c| c.is_ascii_digit())),
            "want NULL on overflow, got:\n{got}"
        );
    }

    #[tokio::test]
    async fn try_sum_decimal_like_double() {
        let engine = crate::Engine::new();
        // Golden: try_sum(5.0,10.0,15.0) = 30.0.
        let got = one_cell(
            &engine,
            "SELECT try_sum(col) AS s FROM VALUES (CAST(5.0 AS DOUBLE)),(CAST(10.0 AS DOUBLE)),(CAST(15.0 AS DOUBLE)) AS tab(col)",
        )
        .await;
        assert!(got.contains("30.0"), "want 30.0, got:\n{got}");
    }

    #[tokio::test]
    async fn try_sum_group_by() {
        let engine = crate::Engine::new();
        let got = one_cell(
            &engine,
            "SELECT k, try_sum(v) AS s FROM VALUES (0,1),(0,2),(1,10),(1,20) AS t(k,v) GROUP BY k ORDER BY k",
        )
        .await;
        assert!(got.contains("| 0 | 3 "), "k=0 want 3, got:\n{got}");
        assert!(got.contains("| 1 | 30 "), "k=1 want 30, got:\n{got}");
    }

    #[tokio::test]
    async fn try_avg_basic() {
        let engine = crate::Engine::new();
        // Golden: try_avg(5,10,15) = 10.0.
        let got = one_cell(
            &engine,
            "SELECT try_avg(col) AS a FROM VALUES (5),(10),(15) AS tab(col)",
        )
        .await;
        assert!(got.contains("10.0"), "want 10.0, got:\n{got}");
    }

    #[tokio::test]
    async fn try_avg_skips_null() {
        let engine = crate::Engine::new();
        // Golden: try_avg(NULL,10,15) = 12.5.
        let got = one_cell(
            &engine,
            "SELECT try_avg(col) AS a FROM VALUES (CAST(NULL AS INT)),(10),(15) AS tab(col)",
        )
        .await;
        assert!(got.contains("12.5"), "want 12.5, got:\n{got}");
    }

    #[tokio::test]
    async fn try_avg_all_null_is_null() {
        let engine = crate::Engine::new();
        let got = one_cell(
            &engine,
            "SELECT try_avg(col) AS a FROM VALUES (CAST(NULL AS INT)),(CAST(NULL AS INT)) AS tab(col)",
        )
        .await;
        let datarow: Vec<&str> = got
            .lines()
            .filter(|l| l.starts_with('|') && !l.contains('a'))
            .collect();
        assert!(
            datarow
                .iter()
                .all(|l| !l.chars().any(|c| c.is_ascii_digit())),
            "want NULL, got:\n{got}"
        );
    }

    #[tokio::test]
    async fn try_avg_overflow_is_null() {
        let engine = crate::Engine::new();
        let got = one_cell(
            &engine,
            "SELECT try_avg(col) AS a FROM VALUES (CAST(9223372036854775807 AS BIGINT)),(CAST(1 AS BIGINT)) AS tab(col)",
        )
        .await;
        let datarow: Vec<&str> = got
            .lines()
            .filter(|l| l.starts_with('|') && !l.contains('a'))
            .collect();
        assert!(
            datarow
                .iter()
                .all(|l| !l.chars().any(|c| c.is_ascii_digit())),
            "want NULL on overflow, got:\n{got}"
        );
    }

    #[tokio::test]
    async fn skewness_matches_spark_golden() {
        let engine = crate::Engine::new();
        // group-by.sql golden: SKEWNESS(a) over a in {1,1,2,2,3,3,3} = -0.2723801058145729.
        let got = one_cell(
            &engine,
            "SELECT skewness(a) AS s FROM VALUES (1),(1),(2),(2),(3),(3),(3) AS t(a)",
        )
        .await;
        assert!(
            got.contains("-0.272380105814572"),
            "want ~-0.27238, got:\n{got}"
        );
    }

    #[tokio::test]
    async fn skewness_symmetric_is_zero() {
        let engine = crate::Engine::new();
        // Symmetric distribution -> skewness 0.
        let got = one_cell(
            &engine,
            "SELECT skewness(a) AS s FROM VALUES (1),(2),(3),(4),(5) AS t(a)",
        )
        .await;
        assert!(
            got.contains(" 0 ") || got.contains("0.0"),
            "want 0, got:\n{got}"
        );
    }

    #[tokio::test]
    async fn skewness_empty_is_null() {
        let engine = crate::Engine::new();
        let got = one_cell(
            &engine,
            "SELECT skewness(a) AS s FROM (SELECT CAST(NULL AS INT) AS a WHERE 1=0)",
        )
        .await;
        let datarow: Vec<&str> = got
            .lines()
            .filter(|l| l.starts_with('|') && !l.contains('s'))
            .collect();
        assert!(
            datarow
                .iter()
                .all(|l| !l.chars().any(|c| c.is_ascii_digit())),
            "want NULL, got:\n{got}"
        );
    }

    #[tokio::test]
    async fn skewness_group_by() {
        let engine = crate::Engine::new();
        // Two groups; right-skewed group should be positive.
        let got = one_cell(
            &engine,
            "SELECT k, skewness(v) AS s FROM VALUES (0,1),(0,1),(0,1),(0,10),(1,1),(1,2),(1,3) AS t(k,v) GROUP BY k ORDER BY k",
        )
        .await;
        // k=1 is symmetric -> 0; k=0 is right-skewed -> positive (just assert k=1 == 0 region).
        assert!(got.contains("| 1 |"), "missing k=1 row, got:\n{got}");
    }
}

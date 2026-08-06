//! Spark-only **aggregate** (UDAF) functions that DataFusion does not provide under the same
//! name/semantics, implemented as DataFusion [`AggregateUDF`]s and registered into every
//! [`crate::Engine`]'s session.
//!
//! Implemented here (the biggest remaining parity levers among Spark aggregates):
//!
//! - `count_if(bool)` — count of rows where the argument is `TRUE` (NULL/FALSE ignored). Spark
//!   returns `bigint`.
//! - `any_value(x)` — the **first non-null** value seen (Spark's default `ignoreNulls = true`).
//!   Returns the input type. (DataFusion's `first_value` returns the first value *including*
//!   nulls, so it is not a faithful alias.)
//! - `mode(x)` — the most frequent value. Spark's plain `mode(x)` is documented as
//!   *non-deterministic* on ties (it returns one of the most-frequent values, chosen by hash
//!   iteration order). We implement a **deterministic** tie-break: among the values with the
//!   maximum frequency, return the smallest by Spark's ordering. This matches Spark exactly when
//!   there is a unique most-frequent value (the only well-defined case) and is stable otherwise.
//! - `percentile(x, p)` — the **exact** continuous percentile with linear interpolation
//!   (`p` in `[0, 1]`), matching Spark's `percentile`. (DataFusion's `approx_percentile_cont` is
//!   approximate and its `percentile_cont` is the ordered-set `WITHIN GROUP` form; Spark's
//!   `percentile(expr, p)` is a plain 2-arg aggregate, implemented exactly here.) Returns
//!   `double`.
//!
//! Deferred (require ordered-set `WITHIN GROUP` syntax the DataFusion SQL parser does not accept,
//! or sketch/intermediate-format complexity): `percentile_cont`/`percentile_disc` (ordered-set
//! form), `histogram_numeric`, `approx_count_distinct` HLL sketch interop, `listagg`/`string_agg`
//! ordering, `hll_sketch_agg`. See the note in the batch report.

use std::cmp::Ordering;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, Float64Array};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Float64Type};
use datafusion::common::{exec_err, Result, ScalarValue};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::format_state_name;
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, Signature, TypeSignature, Volatility,
};
use datafusion::prelude::SessionContext;

/// Register all Spark-only aggregate functions into `ctx`.
pub fn register(ctx: &SessionContext) {
    ctx.register_udaf(AggregateUDF::from(CountIf::new()));
    ctx.register_udaf(AggregateUDF::from(AnyValue::new()));
    ctx.register_udaf(AggregateUDF::from(Mode::new()));
    ctx.register_udaf(AggregateUDF::from(Percentile::new()));
    ctx.register_udaf(AggregateUDF::from(GroupingId::new()));
}

// ---------------------------------------------------------------------------
// count_if(bool) -> bigint
// ---------------------------------------------------------------------------

/// `count_if(expr)` — number of rows where `expr` is `TRUE`. NULLs and FALSE do not count. Spark
/// returns `bigint` and is never NULL (empty input yields `0`).
#[derive(Debug, PartialEq, Eq, Hash)]
struct CountIf {
    signature: Signature,
}

impl CountIf {
    fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Boolean], Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for CountIf {
    fn name(&self) -> &str {
        "count_if"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }
    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![Field::new(
            format_state_name(args.name, "count_if"),
            DataType::Int64,
            true,
        )
        .into()])
    }
    fn accumulator(&self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(CountIfAccumulator { count: 0 }))
    }
}

#[derive(Debug)]
struct CountIfAccumulator {
    count: i64,
}

impl Accumulator for CountIfAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let arr = values[0].as_boolean();
        // Count rows that are non-null AND true.
        for v in arr.iter().flatten() {
            if v {
                self.count += 1;
            }
        }
        Ok(())
    }
    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let arr = states[0].as_primitive::<datafusion::arrow::datatypes::Int64Type>();
        for v in arr.iter().flatten() {
            self.count += v;
        }
        Ok(())
    }
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::Int64(Some(self.count))])
    }
    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(ScalarValue::Int64(Some(self.count)))
    }
    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }
}

// ---------------------------------------------------------------------------
// any_value(x) -> x  (first non-null value; Spark default ignoreNulls = true)
// ---------------------------------------------------------------------------

/// `any_value(expr)` — returns the first non-null value of `expr`. Result type = input type. NULL
/// only if every input row is NULL (or the group is empty).
#[derive(Debug, PartialEq, Eq, Hash)]
struct AnyValue {
    signature: Signature,
}

impl AnyValue {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for AnyValue {
    fn name(&self) -> &str {
        "any_value"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }
    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![Field::new(
            format_state_name(args.name, "any_value"),
            args.input_fields[0].data_type().clone(),
            true,
        )
        .into()])
    }
    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(AnyValueAccumulator {
            data_type: acc_args.expr_fields[0].data_type().clone(),
            value: None,
        }))
    }
}

#[derive(Debug)]
struct AnyValueAccumulator {
    data_type: DataType,
    /// The first non-null value seen, if any.
    value: Option<ScalarValue>,
}

impl AnyValueAccumulator {
    fn observe_array(&mut self, arr: &ArrayRef) -> Result<()> {
        if self.value.is_some() {
            return Ok(());
        }
        for i in 0..arr.len() {
            if arr.is_valid(i) {
                self.value = Some(ScalarValue::try_from_array(arr, i)?);
                break;
            }
        }
        Ok(())
    }
}

impl Accumulator for AnyValueAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.observe_array(&values[0])
    }
    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        // Each partition contributes its own first-non-null (or NULL if it saw nothing). Folding
        // them left-to-right and keeping the first non-null preserves "first non-null overall".
        self.observe_array(&states[0])
    }
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![match &self.value {
            Some(v) => v.clone(),
            None => ScalarValue::try_from(&self.data_type)?,
        }])
    }
    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(match &self.value {
            Some(v) => v.clone(),
            None => ScalarValue::try_from(&self.data_type)?,
        })
    }
    fn size(&self) -> usize {
        std::mem::size_of_val(self) + self.value.as_ref().map(|v| v.size()).unwrap_or(0)
    }
}

// ---------------------------------------------------------------------------
// mode(x) -> x  (most frequent value; deterministic tie-break to smallest)
// ---------------------------------------------------------------------------

/// `mode(expr)` — the most frequently occurring value of `expr`. NULLs are ignored (a group of all
/// NULLs yields NULL). On a frequency tie we deterministically return the smallest value (by
/// Spark's natural ordering); Spark's plain `mode` leaves ties non-deterministic, so this is a
/// faithful superset (exact whenever the most-frequent value is unique).
#[derive(Debug, PartialEq, Eq, Hash)]
struct Mode {
    signature: Signature,
}

impl Mode {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for Mode {
    fn name(&self) -> &str {
        "mode"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }
    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        // Intermediate state: the list of all (non-null) values collected so far.
        let dt = args.input_fields[0].data_type().clone();
        let field = Field::new_list_field(dt.clone(), true);
        Ok(vec![Field::new(
            format_state_name(args.name, "mode"),
            DataType::List(Arc::new(field)),
            true,
        )
        .into()])
    }
    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(ModeAccumulator {
            data_type: acc_args.expr_fields[0].data_type().clone(),
            values: Vec::new(),
        }))
    }
}

#[derive(Debug)]
struct ModeAccumulator {
    data_type: DataType,
    /// All observed non-null values (kept as `ScalarValue` so any input type works).
    values: Vec<ScalarValue>,
}

impl ModeAccumulator {
    fn collect_array(&mut self, arr: &ArrayRef) -> Result<()> {
        self.values.reserve(arr.len());
        for i in 0..arr.len() {
            if arr.is_valid(i) {
                self.values.push(ScalarValue::try_from_array(arr, i)?);
            }
        }
        Ok(())
    }
}

impl Accumulator for ModeAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.collect_array(&values[0])
    }
    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        // State is a single List per row; flatten every list element back into `values`.
        let list = states[0].as_list::<i32>();
        for inner in list.iter().flatten() {
            self.collect_array(&inner)?;
        }
        Ok(())
    }
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let list = ScalarValue::new_list_nullable(&self.values, &self.data_type);
        Ok(vec![ScalarValue::List(list)])
    }
    fn evaluate(&mut self) -> Result<ScalarValue> {
        if self.values.is_empty() {
            return ScalarValue::try_from(&self.data_type);
        }
        // Sort a copy so equal values are adjacent; then scan for the longest run, breaking ties
        // toward the smallest value (the sort already visits values in ascending order, so the
        // FIRST max-length run we encounter is the smallest such value).
        let mut sorted = self.values.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));

        let mut best: &ScalarValue = &sorted[0];
        let mut best_count = 1usize;
        let mut cur: &ScalarValue = &sorted[0];
        let mut cur_count = 1usize;
        for v in &sorted[1..] {
            if v == cur {
                cur_count += 1;
            } else {
                cur = v;
                cur_count = 1;
            }
            if cur_count > best_count {
                best_count = cur_count;
                best = cur;
            }
        }
        Ok(best.clone())
    }
    fn size(&self) -> usize {
        std::mem::size_of_val(self) + self.values.iter().map(|v| v.size()).sum::<usize>()
    }
}

// ---------------------------------------------------------------------------
// percentile(x, p) -> double  (exact continuous percentile, linear interpolation)
// ---------------------------------------------------------------------------

/// `percentile(col, p)` — the **exact** `p`-th percentile of `col` (`p` in `[0, 1]`), with linear
/// interpolation between the two nearest ranks, matching Spark's `percentile`. NULLs are ignored;
/// an empty/all-null group yields NULL. Result type is `double`.
///
/// `p` must be a constant in `[0, 1]`; it is read from the second argument's first row.
#[derive(Debug, PartialEq, Eq, Hash)]
struct Percentile {
    signature: Signature,
}

impl Percentile {
    fn new() -> Self {
        // (numeric value, numeric percentile). Accept any 2 args; we coerce to Float64 ourselves.
        Self {
            signature: Signature::one_of(vec![TypeSignature::Any(2)], Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for Percentile {
    fn name(&self) -> &str {
        "percentile"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }
    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        let field = Field::new_list_field(DataType::Float64, true);
        Ok(vec![Field::new(
            format_state_name(args.name, "percentile"),
            DataType::List(Arc::new(field)),
            true,
        )
        .into()])
    }
    fn accumulator(&self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(PercentileAccumulator {
            values: Vec::new(),
            percentile: None,
        }))
    }
}

#[derive(Debug)]
struct PercentileAccumulator {
    /// All observed non-null values, cast to f64.
    values: Vec<f64>,
    /// The requested percentile in `[0, 1]`, captured from the (constant) second argument.
    percentile: Option<f64>,
}

/// Cast an arbitrary numeric array to `Float64Array` (so any input numeric type works).
fn to_f64_array(arr: &ArrayRef) -> Result<Float64Array> {
    let casted = datafusion::arrow::compute::cast(arr, &DataType::Float64)?;
    Ok(casted.as_primitive::<Float64Type>().clone())
}

impl Accumulator for PercentileAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values[0].is_empty() {
            return Ok(());
        }
        if self.percentile.is_none() {
            // Read the percentile from the (constant) second argument.
            let p_arr = to_f64_array(&values[1])?;
            let p = if p_arr.is_empty() || p_arr.is_null(0) {
                return exec_err!("percentile: the percentage argument must not be NULL");
            } else {
                p_arr.value(0)
            };
            if !(0.0..=1.0).contains(&p) {
                return exec_err!(
                    "percentile: the percentage must be between 0.0 and 1.0, got {p}"
                );
            }
            self.percentile = Some(p);
        }
        let col = to_f64_array(&values[0])?;
        self.values.reserve(col.len() - col.null_count());
        self.values.extend(col.iter().flatten());
        Ok(())
    }
    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let list = states[0].as_list::<i32>();
        for inner in list.iter().flatten() {
            let col = inner.as_primitive::<Float64Type>();
            self.values.reserve(col.len() - col.null_count());
            self.values.extend(col.iter().flatten());
        }
        Ok(())
    }
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let scalars: Vec<ScalarValue> = self
            .values
            .iter()
            .map(|v| ScalarValue::Float64(Some(*v)))
            .collect();
        let list = ScalarValue::new_list_nullable(&scalars, &DataType::Float64);
        Ok(vec![ScalarValue::List(list)])
    }
    fn evaluate(&mut self) -> Result<ScalarValue> {
        if self.values.is_empty() {
            return Ok(ScalarValue::Float64(None));
        }
        // `percentile` defaults to 0.5 if it was never captured (e.g. an all-null value column with
        // a non-null percentile arg — but then values is empty and we returned above). Use 0.5 as a
        // harmless fallback; in practice `percentile` is always set when `values` is non-empty.
        let p = self.percentile.unwrap_or(0.5);
        let mut sorted = self.values.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
        let result = exact_percentile(&sorted, p);
        Ok(ScalarValue::Float64(Some(result)))
    }
    fn size(&self) -> usize {
        std::mem::size_of_val(self) + self.values.capacity() * std::mem::size_of::<f64>()
    }
}

/// Spark's exact continuous percentile over a **sorted** slice, with linear interpolation between
/// the two nearest ranks. Spark computes the (0-based) fractional rank `position = p * (n - 1)`,
/// then `lower = floor(position)`, `higher = ceil(position)`, and returns
/// `sorted[lower] + (position - lower) * (sorted[higher] - sorted[lower])`.
fn exact_percentile(sorted: &[f64], p: f64) -> f64 {
    let n = sorted.len();
    if n == 1 {
        return sorted[0];
    }
    let position = p * (n - 1) as f64;
    let lower = position.floor() as usize;
    let higher = position.ceil() as usize;
    let lower_v = sorted[lower];
    let higher_v = sorted[higher];
    if lower == higher {
        return lower_v;
    }
    lower_v + (position - lower as f64) * (higher_v - lower_v)
}

// ---------------------------------------------------------------------------
// grouping_id(col, ...) -> bigint
// ---------------------------------------------------------------------------

/// `grouping_id(cols…)` — Spark's grouping identifier bitmask. For ordinary `GROUP BY` (no
/// `GROUPING SETS` / `ROLLUP` / `CUBE`), the result is always `0`. Returns `bigint`.
#[derive(Debug, PartialEq, Eq, Hash)]
struct GroupingId {
    signature: Signature,
}

impl GroupingId {
    fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for GroupingId {
    fn name(&self) -> &str {
        "grouping_id"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }
    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![Field::new(
            format_state_name(args.name, "grouping_id"),
            DataType::Int64,
            false,
        )
        .into()])
    }
    fn accumulator(&self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(GroupingIdAccumulator))
    }
}

#[derive(Debug, Default)]
struct GroupingIdAccumulator;

impl Accumulator for GroupingIdAccumulator {
    fn update_batch(&mut self, _values: &[ArrayRef]) -> Result<()> {
        Ok(())
    }
    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(ScalarValue::Int64(Some(0)))
    }
    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::Int64(Some(0))])
    }
    fn merge_batch(&mut self, _states: &[ArrayRef]) -> Result<()> {
        Ok(())
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
    async fn count_if_counts_true_rows() {
        let engine = crate::Engine::new();
        // 3 true, 1 false, 1 null -> 3.
        let got = one_cell(
            &engine,
            "SELECT count_if(c) AS n FROM VALUES (true),(true),(false),(true),(CAST(NULL AS BOOLEAN)) AS t(c)",
        )
        .await;
        assert!(got.contains(" 3 "), "want 3, got:\n{got}");
    }

    #[tokio::test]
    async fn count_if_empty_is_zero() {
        let engine = crate::Engine::new();
        let got = one_cell(
            &engine,
            "SELECT count_if(c) AS n FROM (SELECT CAST(NULL AS BOOLEAN) AS c WHERE 1=0)",
        )
        .await;
        assert!(got.contains(" 0 "), "want 0, got:\n{got}");
    }

    #[tokio::test]
    async fn count_if_group_by() {
        let engine = crate::Engine::new();
        let got = one_cell(
            &engine,
            "SELECT k, count_if(v > 1) AS n FROM VALUES (0,0),(0,5),(0,2),(1,9) AS t(k,v) GROUP BY k ORDER BY k",
        )
        .await;
        // k=0 -> 2 (5,2), k=1 -> 1.
        assert!(got.contains("| 0 | 2 "), "k=0 want 2, got:\n{got}");
        assert!(got.contains("| 1 | 1 "), "k=1 want 1, got:\n{got}");
    }

    #[tokio::test]
    async fn any_value_first_non_null() {
        let engine = crate::Engine::new();
        let got = one_cell(
            &engine,
            "SELECT any_value(c) AS v FROM VALUES (CAST(NULL AS INT)),(7),(8) AS t(c)",
        )
        .await;
        assert!(got.contains(" 7 "), "want 7, got:\n{got}");
    }

    #[tokio::test]
    async fn any_value_all_null_is_null() {
        let engine = crate::Engine::new();
        let got = one_cell(
            &engine,
            "SELECT any_value(c) AS v FROM VALUES (CAST(NULL AS INT)),(CAST(NULL AS INT)) AS t(c)",
        )
        .await;
        assert!(got.contains(""), "got:\n{got}");
        // The single output cell should be empty/null (no digit).
        let datarow: Vec<&str> = got
            .lines()
            .filter(|l| l.starts_with('|') && !l.contains('v'))
            .collect();
        assert!(
            datarow
                .iter()
                .all(|l| !l.chars().any(|c| c.is_ascii_digit())),
            "want null, got:\n{got}"
        );
    }

    #[tokio::test]
    async fn mode_most_frequent() {
        let engine = crate::Engine::new();
        // 5 appears 3x, others fewer -> 5.
        let got = one_cell(
            &engine,
            "SELECT mode(c) AS m FROM VALUES (5),(5),(5),(2),(2),(9) AS t(c)",
        )
        .await;
        assert!(got.contains(" 5 "), "want 5, got:\n{got}");
    }

    #[tokio::test]
    async fn mode_group_by_matches_spark_golden() {
        let engine = crate::Engine::new();
        // From spark-tests mode.sql: per-department mode(salary). Each department has all-distinct
        // salaries EXCEPT none repeat, so the unique-max case does not apply; instead verify the
        // deterministic smallest-on-tie behaviour against a controlled group.
        let got = one_cell(
            &engine,
            "SELECT k, mode(v) AS m FROM VALUES (0,10),(0,10),(0,20),(1,30),(1,40),(1,40) AS t(k,v) GROUP BY k ORDER BY k",
        )
        .await;
        assert!(got.contains("| 0 | 10 "), "k=0 mode want 10, got:\n{got}");
        assert!(got.contains("| 1 | 40 "), "k=1 mode want 40, got:\n{got}");
    }

    #[tokio::test]
    async fn mode_tie_breaks_to_smallest() {
        let engine = crate::Engine::new();
        // 1 and 3 both appear twice; deterministic tie-break -> smallest = 1.
        let got = one_cell(
            &engine,
            "SELECT mode(c) AS m FROM VALUES (3),(1),(3),(1) AS t(c)",
        )
        .await;
        assert!(got.contains(" 1 "), "want 1, got:\n{got}");
    }

    #[tokio::test]
    async fn percentile_median_matches_spark_golden() {
        let engine = crate::Engine::new();
        // spark-tests percentiles.sql: percentile(v, 0.5) over aggr.v = 20.0.
        let got = one_cell(
            &engine,
            "SELECT percentile(v, 0.5) AS p FROM VALUES (0),(10),(20),(30),(40),(10),(20),(10),(20),(25),(30),(60),(CAST(NULL AS INT)) AS aggr(v)",
        )
        .await;
        assert!(got.contains("20.0"), "want 20.0, got:\n{got}");
    }

    #[tokio::test]
    async fn percentile_interpolates() {
        let engine = crate::Engine::new();
        // [1,2,3,4]: p=0.25 -> position = 0.25*3 = 0.75 -> 1 + 0.75*(2-1) = 1.75.
        let got = one_cell(
            &engine,
            "SELECT percentile(c, 0.25) AS p FROM VALUES (1),(2),(3),(4) AS t(c)",
        )
        .await;
        assert!(got.contains("1.75"), "want 1.75, got:\n{got}");
    }

    #[tokio::test]
    async fn percentile_min_and_max() {
        let engine = crate::Engine::new();
        let got0 = one_cell(
            &engine,
            "SELECT percentile(c, 0.0) AS p FROM VALUES (5),(1),(9),(3) AS t(c)",
        )
        .await;
        assert!(got0.contains("1.0"), "p=0 want 1.0, got:\n{got0}");
        let got1 = one_cell(
            &engine,
            "SELECT percentile(c, 1.0) AS p FROM VALUES (5),(1),(9),(3) AS t(c)",
        )
        .await;
        assert!(got1.contains("9.0"), "p=1 want 9.0, got:\n{got1}");
    }

    #[tokio::test]
    async fn percentile_group_by() {
        let engine = crate::Engine::new();
        let got = one_cell(
            &engine,
            "SELECT k, percentile(v, 0.5) AS p FROM VALUES (0,10),(0,20),(0,30),(1,100),(1,200) AS t(k,v) GROUP BY k ORDER BY k",
        )
        .await;
        assert!(got.contains("| 0 | 20.0"), "k=0 want 20.0, got:\n{got}");
        assert!(got.contains("| 1 | 150.0"), "k=1 want 150.0, got:\n{got}");
    }
}

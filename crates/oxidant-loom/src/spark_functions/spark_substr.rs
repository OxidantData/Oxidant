//! Spark `substr` / `substring` — registered under the same names as DataFusion's built-in,
//! which it **replaces** because the built-in diverges from Spark on three documented points
//! (verified against Spark `UTF8String.substringSQL` and the v4.0.0 goldens in
//! `oxidant-spark-compat/spark-tests/results/string-functions.sql.out`):
//!
//! * Spark **implicitly casts** the first argument to string — `substr(int_col, 1, 5)` is
//!   legal (TPC-DS Q8/Q15/Q19/Q45 take `SUBSTRING(ca_zip, 1, 5)` over numeric zips). The
//!   built-in signature is exact-`String` and fails planning with `requires String, but
//!   received Int64`.
//! * A negative `start_pos` counts back **from the end of the string**:
//!   `substr('Spark SQL', -3)` → `'SQL'`. The built-in clamps to the string start.
//! * A negative `length` yields the **empty string**; the built-in raises
//!   `negative count not allowed`.
//!
//! Position semantics otherwise match the built-in (1-based; `0` behaves like `1`; bounds
//! intersect with the string), so previously-plannable calls are unaffected.
//!
//! Two registration points are required (both live here):
//!
//! * [`udf`] goes into the function registry so non-SQL resolution (Spark Connect
//!   `UnresolvedFunction("substr")`) finds it. Registering here alone is NOT enough for SQL:
//!   sqlparser parses every `substr(`/`substring(` call (comma *and* `FROM`/`FOR` forms) into
//!   the `Expr::Substring` AST node, which DataFusion plans through `ExprPlanner::
//!   plan_substring` — and its built-in planner constructs the call from the built-in UDF
//!   directly, bypassing the registry.
//! * [`SparkSubstrPlanner`] is that `ExprPlanner` hook: it plans `Expr::Substring` to the
//!   Spark UDF. The engine must register it via `SessionStateBuilder::with_expr_planners`
//!   *before* `with_default_features` — `register_expr_planner` appends, so a planner
//!   registered later never runs (the default planner always claims `plan_substring` first).

use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, Int64Array, StringArray, StringBuilder};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{exec_err, Result};
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::planner::{ExprPlanner, PlannerResult};
use datafusion::logical_expr::{
    ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

/// Spark's `substr` (alias `substring`), shadowing the DataFusion built-in in the registry.
pub fn udf() -> ScalarUDF {
    ScalarUDF::from(SparkSubstr::new()).with_aliases(["substring"])
}

/// `ExprPlanner` routing every SQL substring form to the Spark [`udf`]; see module docs for
/// why the registry alone is insufficient and why this planner must precede the defaults.
#[derive(Debug)]
pub struct SparkSubstrPlanner;

impl ExprPlanner for SparkSubstrPlanner {
    fn plan_substring(&self, args: Vec<Expr>) -> Result<PlannerResult<Vec<Expr>>> {
        Ok(PlannerResult::Planned(Expr::ScalarFunction(
            ScalarFunction::new_udf(Arc::new(udf()), args),
        )))
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct SparkSubstr {
    signature: Signature,
}

impl SparkSubstr {
    fn new() -> Self {
        // Any first argument (Spark implicitly casts to string); arity checked in
        // `return_type`/`invoke_with_args` (2 or 3).
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkSubstr {
    fn name(&self) -> &str {
        "substr"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if !(2..=3).contains(&arg_types.len()) {
            return exec_err!("substr expects 2 or 3 arguments, got {}", arg_types.len());
        }
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let n = args.number_rows;
        if !(2..=3).contains(&args.args.len()) {
            return exec_err!("substr expects 2 or 3 arguments, got {}", args.args.len());
        }
        let arrays: Vec<ArrayRef> = args
            .args
            .iter()
            .map(|a| a.clone().into_array(n))
            .collect::<Result<Vec<_>>>()?;

        // Spark's implicit cast: any atomic first argument renders as its string form.
        let strs = cast(&arrays[0], &DataType::Utf8)?;
        let strs = strs.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
            datafusion::common::DataFusionError::Internal(
                "substr: first argument did not cast to Utf8".into(),
            )
        })?;
        let starts = cast(&arrays[1], &DataType::Int64)?;
        let starts = starts
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                datafusion::common::DataFusionError::Internal(
                    "substr: start position did not cast to Int64".into(),
                )
            })?;
        let lens = match arrays.get(2) {
            Some(a) => Some(cast(a, &DataType::Int64)?),
            None => None,
        };
        let lens = lens
            .as_ref()
            .map(|l| {
                l.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                    datafusion::common::DataFusionError::Internal(
                        "substr: length did not cast to Int64".into(),
                    )
                })
            })
            .transpose()?;

        let mut out = StringBuilder::new();
        for row in 0..n {
            if strs.is_null(row) || starts.is_null(row) || lens.is_some_and(|l| l.is_null(row)) {
                out.append_null();
                continue;
            }
            // Spark's 2-arg form binds `len` to `Int32.MAX_VALUE` (rest of the string).
            let len = lens.map_or(i64::from(i32::MAX), |l| l.value(row));
            out.append_value(spark_substr(strs.value(row), starts.value(row), len));
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

/// Spark `UTF8String.substringSQL` (character-based): `pos` is 1-based, `0` behaves like `1`,
/// negative counts back from the end; the result is `[start, start + len)` intersected with the
/// string bounds (empty when the intersection is empty, e.g. negative `len`).
fn spark_substr(s: &str, pos: i64, len: i64) -> String {
    let num_chars = s.chars().count() as i64;
    let start = if pos > 0 {
        pos - 1
    } else if pos < 0 {
        num_chars.saturating_add(pos)
    } else {
        0
    };
    // Spark computes `end` in its 32-bit length domain (clamp, not overflow).
    let end = start
        .saturating_add(len)
        .clamp(i64::from(i32::MIN), i64::from(i32::MAX));
    if end <= start || start >= num_chars || end <= 0 {
        return String::new();
    }
    let from = start.max(0);
    s.chars()
        .skip(from as usize)
        .take((end - from).max(0) as usize)
        .collect()
}

#[cfg(test)]
mod tests {
    use crate::Engine;

    async fn run(q: &str) -> String {
        let engine = Engine::new();
        let batches = engine.sql(q).await.unwrap_or_else(|e| panic!("{q}: {e}"));
        crate::arrow::util::pretty::pretty_format_batches(&batches)
            .unwrap()
            .to_string()
    }

    #[tokio::test]
    async fn substr_spark_golden_basics() {
        // Spark goldens (string-functions.sql.out): 'k SQL', 'SQL', 'k'.
        assert!(run("SELECT substr('Spark SQL', 5) = 'k SQL' AS x")
            .await
            .contains("true"));
        // Negative start counts from the end — the built-in would return the whole string.
        assert!(run("SELECT substr('Spark SQL', -3) = 'SQL' AS x")
            .await
            .contains("true"));
        assert!(run("SELECT substr('Spark SQL', 5, 1) = 'k' AS x")
            .await
            .contains("true"));
        // `substring` alias and the FROM/FOR surface syntax resolve identically.
        assert!(run("SELECT substring('Spark SQL', -3) = 'SQL' AS x")
            .await
            .contains("true"));
        assert!(run("SELECT substr('Spark SQL' from 5 for 1) = 'k' AS x")
            .await
            .contains("true"));
        assert!(run("SELECT substring('Spark SQL' from -3) = 'SQL' AS x")
            .await
            .contains("true"));
    }

    #[tokio::test]
    async fn substr_implicit_cast_first_arg() {
        // TPC-DS Q8/Q15/Q19/Q45 shape: SUBSTRING over an integer zip code.
        assert!(
            run("SELECT substr(zip, 1, 5) = '24128' AS x FROM (VALUES (24128)) AS t(zip)")
                .await
                .contains("true")
        );
        assert!(
            run("SELECT substring(zip, 1, 2) = '24' AS x FROM (VALUES (24128)) AS t(zip)")
                .await
                .contains("true")
        );
    }

    #[tokio::test]
    async fn substr_position_and_length_edges() {
        // pos 0 behaves exactly like pos 1 (Spark `substringSQL`: start 0, full length).
        assert!(run("SELECT substr('alphabet', 0, 5) = 'alpha' AS x")
            .await
            .contains("true"));
        // Negative pos counts back from the end.
        assert!(run("SELECT substr('alphabet', -2, 1) = 'e' AS x")
            .await
            .contains("true"));
        // Negative len → empty string (not an error).
        assert!(run("SELECT substr('alphabet', 1, -1) = '' AS x")
            .await
            .contains("true"));
        // NULL propagation.
        assert!(
            run("SELECT substr(CAST(NULL AS STRING), 1, 1) IS NULL AS x")
                .await
                .contains("true")
        );
        assert!(run("SELECT substr('alphabet', NULL, 1) IS NULL AS x")
            .await
            .contains("true"));
        assert!(run("SELECT substr('alphabet', 1, NULL) IS NULL AS x")
            .await
            .contains("true"));
    }
}

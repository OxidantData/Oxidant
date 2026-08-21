//! Spark "cast alias" type-constructor functions: `int(x)`, `double(x)`, `decimal(x)`, … plus
//! `positive(x)` (Spark `UnaryPositive`).
//!
//! Spark registers a family of one-argument functions (SPARK-16730, "cast alias functions for Hive
//! compatibility") that are pure synonyms for `CAST(x AS T)`:
//!
//! | function       | desugars to                  |
//! |----------------|------------------------------|
//! | `boolean(x)`   | `CAST(x AS BOOLEAN)`         |
//! | `tinyint(x)`   | `CAST(x AS TINYINT)`         |
//! | `smallint(x)`  | `CAST(x AS SMALLINT)`        |
//! | `int(x)`       | `CAST(x AS INT)`            |
//! | `bigint(x)`    | `CAST(x AS BIGINT)`         |
//! | `float(x)`     | `CAST(x AS FLOAT)`         |
//! | `double(x)`    | `CAST(x AS DOUBLE)`         |
//! | `string(x)`    | `CAST(x AS STRING)`         |
//! | `binary(x)`    | `CAST(x AS BINARY)`         |
//! | `date(x)`      | `CAST(x AS DATE)`         |
//! | `timestamp(x)` | `CAST(x AS TIMESTAMP)`         |
//! | `decimal(x)`   | `CAST(x AS DECIMAL(10,0))`    |
//!
//! They *parse* as ordinary function calls, so DataFusion rejects them at planning with
//! `Invalid function 'float'`. Each is **exactly** `Cast(child, T)` in Spark — identical value and
//! type semantics to the SQL `CAST`, including ANSI overflow/parse errors — so we lower each to a
//! DataFusion `Expr::Cast` via [`ScalarUDFImpl::simplify`]. This is a faithful, equivalent-plan
//! lowering (explicitly allowed by the parity contract: "lowering Spark syntax to an EQUIVALENT
//! DataFusion plan"), never a lossy rewrite: `float(x)` evaluates byte-for-byte like
//! `CAST(x AS FLOAT)` would in this same engine, errors and all. The output column name is the
//! child's (Spark, like Postgres, omits the cast from a column's name) — handled in
//! [`crate::spark_names`], which runs on the raw plan before this lowering.
//!
//! `positive(x)` (Spark `UnaryPositive`, also a `RuntimeReplaceable`/identity expression) returns
//! its argument unchanged for a numeric/interval input and Spark's implicit-numeric-cast of a
//! string input to `DOUBLE` — matching Spark's `inputTypes = NumericAndInterval` coercion. Its
//! column name prints as `(+ x)` (also handled in [`crate::spark_names`]).
//!
//! `simplify` is the primary path (it always runs during logical optimization, before execution);
//! `invoke_with_args` is a defensive fallback that performs the same overflow-erroring cast
//! (`safe = false`, matching DataFusion's `CastExpr`) in the unlikely event the call reaches
//! execution un-simplified.

use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, BooleanBuilder, StringArray};
use datafusion::arrow::compute::kernels::cast::cast_with_options;
use datafusion::arrow::compute::CastOptions;
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::arrow::util::display::FormatOptions;
use datafusion::common::{exec_err, DataFusionError, Result};
use datafusion::logical_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion::logical_expr::{
    cast, ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::prelude::SessionContext;

/// The Spark cast-alias constructor names (used by [`crate::spark_names`] to render their column
/// names as the child's, like an explicit `CAST`). Kept in sync with [`register`].
pub(crate) const CAST_ALIAS_NAMES: &[&str] = &[
    "boolean",
    "tinyint",
    "smallint",
    "int",
    "bigint",
    "float",
    "double",
    "string",
    "binary",
    "date",
    "timestamp",
    "decimal",
];

/// Register the cast-alias constructors and `positive` into `ctx`.
pub fn register(ctx: &SessionContext) {
    use DataType::*;
    let casts: &[(&str, DataType)] = &[
        ("tinyint", Int8),
        ("smallint", Int16),
        ("int", Int32),
        ("bigint", Int64),
        ("float", Float32),
        ("double", Float64),
        ("string", Utf8),
        ("binary", Binary),
        ("date", Date32),
        // `CAST(x AS TIMESTAMP)` in DataFusion is the tz-naive `Timestamp(Nanosecond, None)`; we
        // mirror it exactly so `timestamp(x)` is identical to the SQL cast.
        ("timestamp", Timestamp(TimeUnit::Nanosecond, None)),
        // Spark's `decimal(x)` is `CAST(x AS DECIMAL(10,0))` (the default precision/scale).
        ("decimal", Decimal128(10, 0)),
    ];
    for (name, target) in casts {
        ctx.register_udf(ScalarUDF::from(CastAlias::new(name, target.clone())));
    }
    // `boolean` needs Spark's stricter string semantics, so it gets a dedicated impl.
    ctx.register_udf(ScalarUDF::from(BooleanCast::new()));
    ctx.register_udf(ScalarUDF::from(Positive::new()));
}

fn arrow_err(e: datafusion::arrow::error::ArrowError) -> DataFusionError {
    DataFusionError::ArrowError(Box::new(e), None)
}

/// Cast with DataFusion's `CastExpr` semantics (`safe = false`: overflow / parse failure is an
/// error, matching Spark ANSI `CAST`), used by the `invoke_with_args` fallbacks.
fn ansi_cast(arr: &ArrayRef, target: &DataType) -> Result<ArrayRef> {
    let opts = CastOptions {
        safe: false,
        format_options: FormatOptions::default(),
    };
    cast_with_options(arr, target, &opts).map_err(arrow_err)
}

// ---------------------------------------------------------------------------
// cast-alias constructors
// ---------------------------------------------------------------------------

/// One Spark cast-alias constructor (`int`, `double`, `decimal`, …): a 1-arg function that lowers
/// to `CAST(arg AS <target>)`.
#[derive(Debug, PartialEq, Eq, Hash)]
struct CastAlias {
    name: &'static str,
    target: DataType,
    signature: Signature,
}

impl CastAlias {
    fn new(name: &'static str, target: DataType) -> Self {
        Self {
            name,
            target,
            // Exactly one argument of any type. A 2-arg call (`string(1, 2)`) is rejected at
            // planning with a wrong-number-of-arguments error — exactly as Spark rejects it.
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for CastAlias {
    fn name(&self) -> &str {
        self.name
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(self.target.clone())
    }
    fn simplify(&self, args: Vec<Expr>, _info: &SimplifyContext) -> Result<ExprSimplifyResult> {
        // Arity is guaranteed by the signature; be defensive rather than panic the engine.
        let mut args = args;
        let Some(arg) = args.pop() else {
            return Ok(ExprSimplifyResult::Original(args));
        };
        Ok(ExprSimplifyResult::Simplified(cast(
            arg,
            self.target.clone(),
        )))
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        // Fallback only (the `simplify` lowering is the real path); evaluate the cast with the same
        // overflow-erroring semantics as DataFusion's `CastExpr`.
        let n = args.number_rows;
        let arr = args.args[0].clone().into_array(n)?;
        Ok(ColumnarValue::Array(ansi_cast(&arr, &self.target)?))
    }
}

// ---------------------------------------------------------------------------
// boolean (string-aware)
// ---------------------------------------------------------------------------

/// Whether `t` is a string type.
fn is_string(t: &DataType) -> bool {
    matches!(t, DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View)
}

/// Spark's `Cast` of a `STRING` to `BOOLEAN`: the trimmed, case-insensitive token must be one of
/// the accepted true/false spellings, else the cast is invalid (`None`). Matches
/// `org.apache.spark.sql.catalyst.util.StringUtils.isTrueString`/`isFalseString`.
fn parse_spark_bool(s: &str) -> Option<bool> {
    match s.trim().to_ascii_lowercase().as_str() {
        "t" | "true" | "y" | "yes" | "1" => Some(true),
        "f" | "false" | "n" | "no" | "0" => Some(false),
        _ => None,
    }
}

/// `boolean(x)` — Spark's `CAST(x AS BOOLEAN)`. A non-string argument lowers to a plain `Cast`
/// (identical to the SQL cast: `0`→false, non-zero→true, …). A *string* argument uses Spark's
/// stricter rule (`parse_spark_bool`): DataFusion's own string→boolean cast is laxer (it accepts
/// `on`/`off` and silently yields `NULL` on junk) whereas Spark accepts only `t/true/y/yes/1` and
/// `f/false/n/no/0` and raises `CAST_INVALID_INPUT` (ANSI) on anything else (`on`, `off`, `11`,
/// `000`, ``, …). Implementing the Spark set makes a string `boolean(...)` match Spark exactly
/// (right answer / right rejection).
#[derive(Debug, PartialEq, Eq, Hash)]
struct BooleanCast {
    signature: Signature,
}

impl BooleanCast {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for BooleanCast {
    fn name(&self) -> &str {
        "boolean"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }
    fn simplify(&self, args: Vec<Expr>, info: &SimplifyContext) -> Result<ExprSimplifyResult> {
        let mut args = args;
        let Some(arg) = args.pop() else {
            return Ok(ExprSimplifyResult::Original(args));
        };
        match info.get_data_type(&arg) {
            // String input keeps the UDF so `invoke_with_args` applies Spark's strict parse.
            Ok(t) if is_string(&t) => Ok(ExprSimplifyResult::Original(vec![arg])),
            // Everything else is a plain `CAST(arg AS BOOLEAN)`.
            Ok(_) => Ok(ExprSimplifyResult::Simplified(cast(arg, DataType::Boolean))),
            Err(_) => Ok(ExprSimplifyResult::Original(vec![arg])),
        }
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let n = args.number_rows;
        let in_type = args.arg_fields[0].data_type().clone();
        let arr = args.args[0].clone().into_array(n)?;
        if !is_string(&in_type) {
            return Ok(ColumnarValue::Array(ansi_cast(&arr, &DataType::Boolean)?));
        }
        // Normalize to Utf8 then apply Spark's strict string→boolean rule per row.
        let utf8 = cast_with_options(
            &arr,
            &DataType::Utf8,
            &CastOptions {
                safe: false,
                format_options: FormatOptions::default(),
            },
        )
        .map_err(arrow_err)?;
        let strs = utf8.as_any().downcast_ref::<StringArray>().unwrap();
        let mut out = BooleanBuilder::with_capacity(n);
        for i in 0..n {
            if strs.is_null(i) {
                out.append_null();
                continue;
            }
            match parse_spark_bool(strs.value(i)) {
                Some(b) => out.append_value(b),
                None => {
                    return exec_err!(
                        "[CAST_INVALID_INPUT] The value '{}' of the type \"STRING\" cannot be cast to \"BOOLEAN\" because it is malformed",
                        strs.value(i)
                    )
                }
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

// ---------------------------------------------------------------------------
// positive
// ---------------------------------------------------------------------------

/// `positive(x)` — Spark `UnaryPositive`: the identity on a numeric/interval argument, and the
/// implicit numeric cast (to `DOUBLE`) of a string argument, exactly like Spark's coercion.
#[derive(Debug, PartialEq, Eq, Hash)]
struct Positive {
    signature: Signature,
}

impl Positive {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for Positive {
    fn name(&self) -> &str {
        "positive"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        // String coerces to double (Spark); every other type passes through unchanged.
        Ok(if is_string(&arg_types[0]) {
            DataType::Float64
        } else {
            arg_types[0].clone()
        })
    }
    fn simplify(&self, args: Vec<Expr>, info: &SimplifyContext) -> Result<ExprSimplifyResult> {
        let mut args = args;
        let Some(arg) = args.pop() else {
            return Ok(ExprSimplifyResult::Original(args));
        };
        // Match `return_type`: a string argument is cast to double, anything else is identity.
        match info.get_data_type(&arg) {
            Ok(ty) if is_string(&ty) => {
                Ok(ExprSimplifyResult::Simplified(cast(arg, DataType::Float64)))
            }
            Ok(_) => Ok(ExprSimplifyResult::Simplified(arg)),
            // Type not yet resolvable — leave the call for the fallback path.
            Err(_) => Ok(ExprSimplifyResult::Original(vec![arg])),
        }
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let n = args.number_rows;
        let in_type = args.arg_fields[0].data_type();
        if is_string(in_type) {
            let arr = args.args[0].clone().into_array(n)?;
            Ok(ColumnarValue::Array(ansi_cast(&arr, &DataType::Float64)?))
        } else {
            // Identity: hand the argument straight back.
            Ok(args.args[0].clone())
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::Engine;

    /// Run `q` and return its single rendered cell (NULL → "NULL").
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

    /// Run `SELECT typeof(<expr>)` and return Spark's type name.
    async fn typ(expr: &str) -> String {
        cell(&format!("SELECT typeof({expr}) AS x")).await
    }

    #[tokio::test]
    async fn cast_alias_types_and_values() {
        for (expr, ty, val) in [
            ("boolean(1)", "boolean", "true"),
            ("tinyint(1)", "tinyint", "1"),
            ("smallint(1)", "smallint", "1"),
            ("int(1)", "int", "1"),
            ("bigint(1)", "bigint", "1"),
            ("float(1)", "float", "1.0"),
            ("double(1)", "double", "1.0"),
            ("string(1)", "string", "1"),
            ("decimal(1)", "decimal(10,0)", "1"),
        ] {
            assert_eq!(typ(expr).await, ty, "type of {expr}");
            assert_eq!(
                cell(&format!("SELECT {expr} AS x")).await,
                val,
                "value of {expr}"
            );
        }
    }

    #[tokio::test]
    async fn cast_alias_string_to_number() {
        // String → numeric uses the same cast as `CAST('123' AS INT)`.
        assert_eq!(typ("int('123')").await, "int");
        assert_eq!(cell("SELECT int('123') AS x").await, "123");
        assert_eq!(typ("double('1.5')").await, "double");
        assert_eq!(cell("SELECT double('1.5') AS x").await, "1.5");
    }

    #[tokio::test]
    async fn date_and_timestamp_constructors() {
        assert_eq!(typ("date('2014-04-04')").await, "date");
        assert_eq!(cell("SELECT date('2014-04-04') AS x").await, "2014-04-04");
        // Arrow's pretty-printer renders the timestamp with a `T`; the parity harness swaps it for
        // a space. The value/type are what matter here.
        assert_eq!(typ("timestamp(date('2014-04-04'))").await, "timestamp_ntz");
        assert_eq!(
            cell("SELECT timestamp(date('2014-04-04')) AS x").await,
            "2014-04-04T00:00:00"
        );
    }

    #[tokio::test]
    async fn positive_is_identity_with_numeric_coercion() {
        // Numeric input passes through unchanged (type + value). NB: `-1.11` is a `Float64` literal
        // in DataFusion (not Spark's `decimal(3,2)`), so identity yields a double — faithful to
        // oxidant's literal typing, which is a separate (pre-existing) concern.
        assert_eq!(typ("positive(1)").await, "int");
        assert_eq!(cell("SELECT positive(1) AS x").await, "1");
        assert_eq!(
            typ("positive(cast(-1.11 as decimal(3,2)))").await,
            "decimal(3,2)"
        );
        assert_eq!(cell("SELECT positive(-1.11) AS x").await, "-1.11");
        // String input coerces to double (Spark).
        assert_eq!(typ("positive('-1.11')").await, "double");
        assert_eq!(cell("SELECT positive('-1.11') AS x").await, "-1.11");
    }

    #[tokio::test]
    async fn wrong_arg_count_is_rejected() {
        let engine = Engine::new();
        assert!(engine.sql("SELECT string(1, 2)").await.is_err());
    }

    #[tokio::test]
    async fn boolean_string_matches_spark() {
        // Accepted true/false spellings (trimmed, case-insensitive).
        for s in ["t", "true", "y", "yes", "1", "TRUE", "  Yes  "] {
            assert_eq!(
                cell(&format!("SELECT boolean('{s}') AS x")).await,
                "true",
                "{s}"
            );
        }
        for s in ["f", "false", "n", "no", "0", "False", "   f   "] {
            assert_eq!(
                cell(&format!("SELECT boolean('{s}') AS x")).await,
                "false",
                "{s}"
            );
        }
        // Spark rejects these (DataFusion's cast would wrongly accept on/off or return NULL).
        let engine = Engine::new();
        for s in ["on", "off", "of", "o", "11", "000", "", "test", "yeah"] {
            assert!(
                engine.sql(&format!("SELECT boolean('{s}')")).await.is_err(),
                "boolean('{s}') should be rejected"
            );
        }
        // Non-string input still casts (0 → false, non-zero → true).
        assert_eq!(cell("SELECT boolean(0) AS x").await, "false");
        assert_eq!(cell("SELECT boolean(5) AS x").await, "true");
    }
}

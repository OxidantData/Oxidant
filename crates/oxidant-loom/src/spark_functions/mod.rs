//! Spark-only scalar functions that DataFusion does not provide, implemented as DataFusion
//! `ScalarUDF`s and registered into every [`crate::Engine`]'s session.
//!
//! This module is **additive and conflict-free by construction**: each Spark function lives in its
//! own submodule exposing `pub fn udf() -> ScalarUDF`, and [`register`] registers them all. Adding
//! a function is one new submodule + one line in [`register`] — nothing else changes. (Functions
//! that DataFusion already implements under a different name are handled as *aliases* in
//! `crate::register_spark_function_aliases`, not here.)
//!
//! Every function must be faithful to Spark's documented semantics; correctness is gated by the
//! `oxidant-spark-compat` golden-test ratchet.

use datafusion::arrow::datatypes::DataType;
use datafusion::common::{Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::prelude::SessionContext;

mod spark_aggregates;
mod spark_aggregates2;
mod spark_array;
mod spark_bitshift;
// `pub(crate)` (like `spark_divide`): `lower_checked_multiply` in `lib.rs` embeds the
// `spark_checked_mul` UDF directly via `spark_checked_mul::udf()` when lowering an integral `*`
// whose Spark result type is `bigint`.
pub(crate) mod spark_checked_mul;
// `pub(crate)` so `crate::spark_names` can reuse the cast-alias name list for column naming.
pub(crate) mod spark_cast_constructors;
mod spark_convert;
mod spark_csv;
mod spark_datetime;
// `pub(crate)` (unlike the other internal submodules): `SparkDividePlanner` in `lib.rs` embeds the
// `spark_divide` UDF directly via `spark_divide::udf()` when lowering a literal-zero integral `/`.
pub(crate) mod spark_divide;
// `pub(crate)` (like `spark_divide`): `SparkDividePlanner` in `lib.rs` embeds the
// `spark_nonzero_divisor` guard UDF directly via `spark_nonzero_divisor::udf()` when lowering a
// decimal `/` or `%` so a zero decimal divisor raises Spark's ANSI DIVIDE_BY_ZERO.
mod spark_datetime2;
mod spark_datetime3;
mod spark_encoding;
mod spark_from_json;
mod spark_if;
mod spark_json;
mod spark_math;
mod spark_misc;
pub(crate) mod spark_nonzero_divisor;
mod spark_regex_misc;
mod spark_strings;
// `pub(crate)` (like `spark_divide`): `Engine::new_inner` in `lib.rs` embeds the
// `SparkSubstrPlanner` directly — SQL `substr`/`substring` is planned through
// `ExprPlanner::plan_substring`, never the function registry (see the submodule docs).
pub(crate) mod spark_substr;
mod try_arithmetic;

/// Register all Spark-only scalar functions into `ctx`.
pub fn register(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(SparkTypeof::new()));
    spark_cast_constructors::register(ctx);
    try_arithmetic::register(ctx);
    spark_strings::register(ctx);
    spark_encoding::register(ctx);
    spark_datetime::register(ctx);
    spark_convert::register(ctx);
    spark_csv::register(ctx);
    spark_regex_misc::register(ctx);
    spark_datetime2::register(ctx);
    spark_datetime3::register(ctx);
    spark_json::register(ctx);
    spark_from_json::register(ctx);
    spark_if::register(ctx);
    spark_divide::register(ctx);
    spark_nonzero_divisor::register(ctx);
    spark_checked_mul::register(ctx);
    spark_math::register(ctx);
    spark_misc::register(ctx);
    spark_array::register(ctx);
    spark_aggregates::register(ctx);
    spark_aggregates2::register(ctx);
    spark_bitshift::register(ctx);
    ctx.register_udf(spark_substr::udf());
}

/// `typeof(expr)` — Spark returns the *type name* of the argument (e.g. `int`, `string`,
/// `array<int>`, `decimal(10,2)`). The value is constant for a column (it depends only on the
/// argument's data type), so we emit a scalar.
///
/// This is also the **reference template** for Spark scalar UDFs: note the four required
/// `ScalarUDFImpl` methods (`name`, `signature`, `return_type`, `invoke_with_args` — DF54 does NOT
/// want an `as_any`), the `#[derive(Debug, PartialEq, Eq, Hash)]` the trait requires, and that
/// `invoke_with_args` reads the input type from `args.arg_fields[i].data_type()`.
#[derive(Debug, PartialEq, Eq, Hash)]
struct SparkTypeof {
    signature: Signature,
}

impl SparkTypeof {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkTypeof {
    fn name(&self) -> &str {
        "typeof"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let dt = args.arg_fields[0].data_type();
        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            spark_type_name(dt),
        ))))
    }
}

/// Spark's spelling of a type name, for `typeof` (and reusable by future functions).
pub(crate) fn spark_type_name(dt: &DataType) -> String {
    match dt {
        DataType::Null => "void".into(),
        DataType::Boolean => "boolean".into(),
        DataType::Int8 => "tinyint".into(),
        DataType::Int16 => "smallint".into(),
        DataType::Int32 => "int".into(),
        DataType::Int64 => "bigint".into(),
        DataType::Float16 | DataType::Float32 => "float".into(),
        DataType::Float64 => "double".into(),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "string".into(),
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => "binary".into(),
        DataType::Date32 | DataType::Date64 => "date".into(),
        DataType::Timestamp(_, Some(_)) => "timestamp".into(),
        DataType::Timestamp(_, None) => "timestamp_ntz".into(),
        DataType::Decimal128(p, s) | DataType::Decimal256(p, s) => format!("decimal({p},{s})"),
        DataType::List(f) | DataType::LargeList(f) | DataType::FixedSizeList(f, _) => {
            format!("array<{}>", spark_type_name(f.data_type()))
        }
        DataType::Struct(fields) => format!(
            "struct<{}>",
            fields
                .iter()
                .map(|f| format!("{}:{}", f.name(), spark_type_name(f.data_type())))
                .collect::<Vec<_>>()
                .join(",")
        ),
        other => format!("{other:?}").to_lowercase(),
    }
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn typeof_reports_spark_type_names() {
        let engine = crate::Engine::new();
        for (q, want) in [
            ("SELECT typeof(1) AS x", "int"),
            ("SELECT typeof(CAST(1 AS BIGINT)) AS x", "bigint"),
            ("SELECT typeof('a') AS x", "string"),
            ("SELECT typeof(CAST(1 AS DOUBLE)) AS x", "double"),
            ("SELECT typeof(true) AS x", "boolean"),
        ] {
            let batches = engine.sql(q).await.unwrap_or_else(|e| panic!("{q}: {e}"));
            let got = crate::arrow::util::pretty::pretty_format_batches(&batches)
                .unwrap()
                .to_string();
            assert!(got.contains(want), "{q} -> want {want}, got:\n{got}");
        }
    }
}

//! Spark `from_csv` / `to_csv` scalar functions.
//!
//! `from_csv(csvStr, schema)` parses a one-row CSV string into a struct per the Spark DDL schema.
//! `to_csv(expr)` serializes a struct to Spark's CSV row format (comma-separated values, no header).

use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, StringArray, StructArray};
use datafusion::arrow::buffer::NullBuffer;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{DataFusionError, Result, ScalarValue, exec_err, plan_err};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion::prelude::SessionContext;

use super::spark_from_json::parse_spark_schema;

/// Register `from_csv` and `to_csv` into `ctx`.
pub fn register(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(FromCsv::new()));
    ctx.register_udf(ScalarUDF::from(ToCsv::new()));
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct FromCsv {
    signature: Signature,
}

impl FromCsv {
    fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

fn arrow_err(e: datafusion::arrow::error::ArrowError) -> DataFusionError {
    DataFusionError::ArrowError(Box::new(e), None)
}

impl ScalarUDFImpl for FromCsv {
    fn name(&self) -> &str {
        "from_csv"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() < 2 || arg_types.len() > 3 {
            return plan_err!(
                "from_csv() requires 2 or 3 arguments, got {}",
                arg_types.len()
            );
        }
        Ok(arg_types.to_vec())
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        plan_err!("from_csv: use return_field_from_args")
    }
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        if args.arg_fields.len() < 2 || args.arg_fields.len() > 3 {
            return plan_err!(
                "from_csv() requires 2 or 3 arguments, got {}",
                args.arg_fields.len()
            );
        }
        if args.arg_fields.len() == 3 {
            return plan_err!("from_csv options argument is not supported");
        }
        let schema = match args.scalar_arguments.get(1).copied().flatten() {
            Some(ScalarValue::Utf8(Some(s)))
            | Some(ScalarValue::LargeUtf8(Some(s)))
            | Some(ScalarValue::Utf8View(Some(s))) => s.clone(),
            _ => return plan_err!("from_csv(): non-constant schema argument is not supported"),
        };
        let dt = parse_spark_schema(&schema).map_err(|e| {
            DataFusionError::Plan(format!("from_csv: invalid schema `{schema}`: {e}"))
        })?;
        let DataType::Struct(_) = &dt else {
            return plan_err!("from_csv schema must be a struct, got {dt}");
        };
        Ok(Arc::new(Field::new("from_csv", dt, true)))
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 2 {
            return exec_err!("from_csv expects 2 arguments");
        }
        let n = args.number_rows;
        let csv_arr = args.args[0].to_array(n)?;
        let csv_arr =
            datafusion::arrow::compute::cast(&csv_arr, &DataType::Utf8).map_err(arrow_err)?;
        let sa = csv_arr
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| DataFusionError::Execution("from_csv: csv is not Utf8".into()))?;
        let schema_arr = args.args[1].clone().into_array(n)?;
        let schema_arr =
            datafusion::arrow::compute::cast(&schema_arr, &DataType::Utf8).map_err(arrow_err)?;
        let schema_arr = schema_arr
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| DataFusionError::Execution("from_csv: schema is not Utf8".into()))?;
        let schema_str = (0..schema_arr.len())
            .find(|&i| !schema_arr.is_null(i))
            .map(|i| schema_arr.value(i).to_string())
            .ok_or_else(|| DataFusionError::Execution("from_csv: null schema".into()))?;
        let dt = parse_spark_schema(&schema_str)
            .map_err(|e| DataFusionError::Execution(format!("from_csv: invalid schema: {e}")))?;
        let DataType::Struct(fields) = dt else {
            return exec_err!("from_csv schema must be a struct");
        };
        let mut columns: Vec<Vec<ScalarValue>> = fields
            .iter()
            .map(|_| Vec::with_capacity(sa.len()))
            .collect();
        let mut validity = Vec::with_capacity(sa.len());
        for i in 0..sa.len() {
            if sa.is_null(i) {
                validity.push(false);
                for (j, field) in fields.iter().enumerate() {
                    columns[j].push(null_of(field.data_type())?);
                }
                continue;
            }
            validity.push(true);
            let cells = split_csv_row(sa.value(i));
            for (j, field) in fields.iter().enumerate() {
                let cell = cells.get(j).map(|s| s.as_str()).unwrap_or("");
                columns[j].push(parse_csv_cell(cell, field.data_type())?);
            }
        }
        let children: Vec<ArrayRef> = columns
            .into_iter()
            .map(ScalarValue::iter_to_array)
            .collect::<Result<Vec<_>>>()?;
        let nulls = NullBuffer::from(validity);
        let st = StructArray::try_new(fields, children, Some(nulls)).map_err(arrow_err)?;
        Ok(ColumnarValue::Array(Arc::new(st)))
    }
}

fn null_of(dt: &DataType) -> Result<ScalarValue> {
    ScalarValue::try_from(dt).map_err(|e| DataFusionError::Execution(e.to_string()))
}

fn parse_csv_cell(cell: &str, dt: &DataType) -> Result<ScalarValue> {
    if cell.is_empty() {
        return null_of(dt);
    }
    match dt {
        DataType::Int32 => match cell.trim().parse::<i32>() {
            Ok(n) => Ok(ScalarValue::Int32(Some(n))),
            Err(_) => null_of(dt),
        },
        DataType::Int64 => match cell.trim().parse::<i64>() {
            Ok(n) => Ok(ScalarValue::Int64(Some(n))),
            Err(_) => null_of(dt),
        },
        DataType::Utf8 => Ok(ScalarValue::Utf8(Some(cell.to_string()))),
        DataType::Boolean => match cell.trim().to_ascii_lowercase().as_str() {
            "true" | "t" | "1" => Ok(ScalarValue::Boolean(Some(true))),
            "false" | "f" | "0" => Ok(ScalarValue::Boolean(Some(false))),
            _ => null_of(dt),
        },
        DataType::Float64 => match cell.trim().parse::<f64>() {
            Ok(n) => Ok(ScalarValue::Float64(Some(n))),
            Err(_) => null_of(dt),
        },
        other => exec_err!("from_csv does not support field type {other}"),
    }
}

/// Split one CSV record, honoring RFC 4180 quotes (`"a,b"` and `""` escapes).
fn split_csv_row(row: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut in_quotes = false;
    let mut chars = row.chars().peekable();
    while let Some(c) = chars.next() {
        if in_quotes {
            if c == '"' {
                if chars.peek() == Some(&'"') {
                    chars.next();
                    cur.push('"');
                } else {
                    in_quotes = false;
                }
            } else {
                cur.push(c);
            }
        } else if c == '"' {
            in_quotes = true;
        } else if c == ',' {
            out.push(std::mem::take(&mut cur));
        } else {
            cur.push(c);
        }
    }
    out.push(cur);
    out
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct ToCsv {
    signature: Signature,
}

impl ToCsv {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ToCsv {
    fn name(&self) -> &str {
        "to_csv"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 1 {
            return exec_err!("to_csv expects 1 argument");
        }
        let arr = args.args[0].to_array(args.number_rows)?;
        let st = arr.as_struct();
        let ncols = st.num_columns();
        let mut out: Vec<Option<String>> = Vec::with_capacity(st.len());
        for row in 0..st.len() {
            if st.is_null(row) {
                out.push(None);
                continue;
            }
            let mut cells = Vec::with_capacity(ncols);
            for c in 0..ncols {
                cells.push(cell_to_csv(st.column(c), row));
            }
            out.push(Some(cells.join(",")));
        }
        Ok(ColumnarValue::Array(
            Arc::new(StringArray::from(out)) as ArrayRef
        ))
    }
}

fn cell_to_csv(arr: &ArrayRef, row: usize) -> String {
    if arr.is_null(row) {
        return String::new();
    }
    match arr.data_type() {
        DataType::Utf8 => arr.as_string::<i32>().value(row).to_string(),
        DataType::Int64 => arr
            .as_primitive::<datafusion::arrow::datatypes::Int64Type>()
            .value(row)
            .to_string(),
        DataType::Binary => {
            let b = arr.as_binary::<i32>().value(row);
            b.iter().map(|x| format!("{x:02X}")).collect()
        }
        _ => String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Engine;
    use datafusion::arrow::array::Int32Array;

    async fn cell(sql: &str) -> String {
        let e = Engine::new();
        let batches = e.sql(sql).await.unwrap();
        let b = &batches[0];
        let col = b.column(0).as_string::<i32>();
        col.value(0).to_string()
    }

    #[tokio::test]
    async fn to_csv_struct_binary() {
        let got = cell("SELECT to_csv(named_struct('n', 1, 'info', X'4142')) AS x").await;
        assert!(got.contains('1'), "expected row with 1, got: {got}");
    }

    #[tokio::test]
    async fn from_csv_schema_and_values_match_spark() {
        let engine = crate::Engine::new();
        let batches = engine
            .sql("SELECT from_csv('1,hello', 'a INT, b STRING') AS parsed")
            .await
            .unwrap_or_else(|e| panic!("collect: {e}"));
        let col = batches[0].column(0);
        let DataType::Struct(fields) = col.data_type() else {
            panic!("expected struct, got {:?}", col.data_type());
        };
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].name(), "a");
        assert_eq!(fields[0].data_type(), &DataType::Int32);
        assert_eq!(fields[1].name(), "b");
        assert_eq!(fields[1].data_type(), &DataType::Utf8);
        let st = col.as_struct();
        assert_eq!(st.len(), 1);
        assert_eq!(st.null_count(), 0);
        let a = st
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("a INT");
        let b = st.column(1).as_string::<i32>();
        assert_eq!(a.value(0), 1);
        assert_eq!(b.value(0), "hello");
    }

    #[tokio::test]
    async fn from_csv_field_projection() {
        let engine = crate::Engine::new();
        let batches = engine
            .sql(
                "SELECT parsed.a AS a FROM (SELECT from_csv('1,hello', 'a INT, b STRING') AS parsed)",
            )
            .await
            .unwrap_or_else(|e| panic!("projection: {e}"));
        let a = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("Int32");
        assert_eq!(a.value(0), 1);
    }

    #[tokio::test]
    async fn from_csv_quoted_delimiter_and_null_input() {
        let engine = crate::Engine::new();
        let batches = engine
            .sql("SELECT from_csv('1,\"hello,world\"', 'a INT, b STRING') AS parsed")
            .await
            .unwrap_or_else(|e| panic!("quoted: {e}"));
        let st = batches[0].column(0).as_struct();
        let b = st.column(1).as_string::<i32>();
        assert_eq!(b.value(0), "hello,world");

        let batches = engine
            .sql("SELECT from_csv(CAST(NULL AS STRING), 'a INT, b STRING') AS parsed")
            .await
            .unwrap_or_else(|e| panic!("null: {e}"));
        assert_eq!(batches[0].column(0).null_count(), 1);
    }
}

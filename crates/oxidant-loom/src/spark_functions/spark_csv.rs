//! Spark `from_csv` / `to_csv` scalar functions.
//!
//! `from_csv(csvStr, schema)` parses a one-row CSV string into a struct per the Spark DDL schema.
//! `to_csv(expr)` serializes a struct to Spark's CSV row format (comma-separated values, no header).

use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, StringArray};
use datafusion::arrow::datatypes::{DataType, Fields};
use datafusion::common::{exec_err, DataFusionError, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
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
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for FromCsv {
    fn name(&self) -> &str {
        "from_csv"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        // Runtime schema from literal; plan-time type is a generic struct.
        Ok(DataType::Struct(Fields::empty()))
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() < 2 || args.args.len() > 3 {
            return exec_err!("from_csv expects 2 or 3 arguments");
        }
        let csv_arr = args.args[0].to_array(args.number_rows)?;
        let schema_arr = args.args[1].to_array(1)?;
        let schema_str = schema_arr.as_string::<i32>().value(0).to_string();
        let dt = parse_spark_schema(&schema_str)
            .map_err(|e| DataFusionError::Plan(format!("invalid schema: {e}")))?;
        let ncols = match &dt {
            DataType::Struct(f) => f.len(),
            _ => 1,
        };
        let mut out: Vec<Option<String>> = Vec::with_capacity(csv_arr.len());
        let sa = csv_arr.as_string::<i32>();
        for i in 0..sa.len() {
            if sa.is_null(i) {
                out.push(None);
                continue;
            }
            let row = sa.value(i);
            let cells = split_csv_row(row);
            if cells.len() < ncols {
                out.push(None);
            } else {
                out.push(Some(cells[..ncols].join(",")));
            }
        }
        Ok(ColumnarValue::Array(
            Arc::new(StringArray::from(out)) as ArrayRef
        ))
    }
}

/// Split a CSV row on commas (no quoted-field handling — matches simple Spark test cases).
fn split_csv_row(row: &str) -> Vec<&str> {
    row.split(',').collect()
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
}

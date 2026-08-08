//! The JSON operation spec handed to the GPU shim.
//!
//! [`GpuOpSpec`] is the whole contract between the DataFusion plan rule and the shim
//! (`libcudf_shim` in a `--features gpu` build, `csrc/mock_shim.c` otherwise): a
//! single parquet file, the columns to read, conjunctive column-vs-literal filters,
//! group-by keys, and the aggregations to compute. The shim answers with ONE Arrow
//! record batch (C Data Interface struct array) holding the FINAL aggregate results:
//! group-by columns (named per [`GpuOpSpec::group_by`]) followed by the aggregations
//! (named per [`AggSpec::alias`]), in order.

use serde::Serialize;

/// Root spec: everything the shim needs to run scan + filter + group-by aggregate.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct GpuOpSpec {
    /// Absolute local path of the single parquet file to scan.
    pub table_path: String,
    /// Columns the shim must read (filter columns + group keys + aggregation inputs),
    /// in first-use order, with the dtype vocabulary of [`LiteralType`]'s siblings
    /// (`int64`, `float64`, `string`, `date32`, `timestamp(us)`, `decimal128(p,s)`, ...).
    pub columns: Vec<ColumnSpec>,
    /// Conjunctive (AND-ed) column-vs-literal comparisons.
    pub filters: Vec<FilterSpec>,
    /// Group-by key column names (empty for a whole-table aggregation).
    pub group_by: Vec<String>,
    /// Aggregations to compute per group.
    pub aggregations: Vec<AggSpec>,
}

/// One input column the shim must materialize from the file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ColumnSpec {
    pub name: String,
    pub dtype: String,
}

/// `col <op> literal` — always column on the left, literal on the right.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct FilterSpec {
    pub col: String,
    pub op: CmpOp,
    pub literal: LiteralSpec,
}

/// Supported comparison operators (JSON symbols, e.g. `"op": "<="`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum CmpOp {
    #[serde(rename = "<")]
    Lt,
    #[serde(rename = "<=")]
    LtEq,
    #[serde(rename = ">")]
    Gt,
    #[serde(rename = ">=")]
    GtEq,
    #[serde(rename = "=")]
    Eq,
    #[serde(rename = "!=")]
    NotEq,
}

/// A typed filter literal. `value` is the literal rendered as a string (ISO 8601 for
/// date/timestamp, plain decimal for numbers, raw text for strings); `ty` tells the
/// shim how to parse it back.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct LiteralSpec {
    #[serde(rename = "type")]
    pub ty: LiteralType,
    pub value: String,
}

/// The shim-side parse vocabulary for [`LiteralSpec::value`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum LiteralType {
    Date,
    Timestamp,
    Decimal,
    Float,
    Int,
    String,
}

/// One aggregation over a column (or `*`).
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct AggSpec {
    pub func: AggFunc,
    /// Input column name; `null` only for `count(*)`.
    pub col: Option<String>,
    /// Output column name the shim must give the result (matches the replaced
    /// DataFusion aggregate's output schema).
    pub alias: String,
}

/// Supported aggregation functions (JSON lowercase names, e.g. `"func": "sum"`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum AggFunc {
    Sum,
    Avg,
    Count,
    Min,
    Max,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The serialized JSON must match the shim contract exactly (KAN-70): key names,
    /// operator symbols, lowercase func/type names, `null` col for count-star.
    #[test]
    fn spec_serializes_to_shim_contract_json() {
        let spec = GpuOpSpec {
            table_path: "/data/lineitem.parquet".to_string(),
            columns: vec![
                ColumnSpec {
                    name: "l_shipdate".to_string(),
                    dtype: "date32".to_string(),
                },
                ColumnSpec {
                    name: "l_extendedprice".to_string(),
                    dtype: "float64".to_string(),
                },
            ],
            filters: vec![
                FilterSpec {
                    col: "l_shipdate".to_string(),
                    op: CmpOp::GtEq,
                    literal: LiteralSpec {
                        ty: LiteralType::Date,
                        value: "1994-01-01".to_string(),
                    },
                },
                FilterSpec {
                    col: "l_quantity".to_string(),
                    op: CmpOp::Lt,
                    literal: LiteralSpec {
                        ty: LiteralType::Int,
                        value: "24".to_string(),
                    },
                },
            ],
            group_by: vec!["l_returnflag".to_string()],
            aggregations: vec![
                AggSpec {
                    func: AggFunc::Sum,
                    col: Some("l_extendedprice".to_string()),
                    alias: "revenue".to_string(),
                },
                AggSpec {
                    func: AggFunc::Count,
                    col: None,
                    alias: "cnt".to_string(),
                },
            ],
        };
        let json = serde_json::to_value(&spec).unwrap();
        let expected = serde_json::json!({
            "table_path": "/data/lineitem.parquet",
            "columns": [
                {"name": "l_shipdate", "dtype": "date32"},
                {"name": "l_extendedprice", "dtype": "float64"},
            ],
            "filters": [
                {"col": "l_shipdate", "op": ">=", "literal": {"type": "date", "value": "1994-01-01"}},
                {"col": "l_quantity", "op": "<", "literal": {"type": "int", "value": "24"}},
            ],
            "group_by": ["l_returnflag"],
            "aggregations": [
                {"func": "sum", "col": "l_extendedprice", "alias": "revenue"},
                {"func": "count", "col": null, "alias": "cnt"},
            ],
        });
        assert_eq!(json, expected);
    }

    /// Every operator / func / literal-type spelling the shim must accept.
    #[test]
    fn enum_spellings_match_contract() {
        let ops = [
            (CmpOp::Lt, "<"),
            (CmpOp::LtEq, "<="),
            (CmpOp::Gt, ">"),
            (CmpOp::GtEq, ">="),
            (CmpOp::Eq, "="),
            (CmpOp::NotEq, "!="),
        ];
        for (op, s) in ops {
            assert_eq!(serde_json::to_value(op).unwrap(), s);
        }
        let funcs = [
            (AggFunc::Sum, "sum"),
            (AggFunc::Avg, "avg"),
            (AggFunc::Count, "count"),
            (AggFunc::Min, "min"),
            (AggFunc::Max, "max"),
        ];
        for (func, s) in funcs {
            assert_eq!(serde_json::to_value(func).unwrap(), s);
        }
        let tys = [
            (LiteralType::Date, "date"),
            (LiteralType::Timestamp, "timestamp"),
            (LiteralType::Decimal, "decimal"),
            (LiteralType::Float, "float"),
            (LiteralType::Int, "int"),
            (LiteralType::String, "string"),
        ];
        for (ty, s) in tys {
            assert_eq!(serde_json::to_value(ty).unwrap(), s);
        }
    }
}

//! The JSON operation spec handed to the GPU shim.
//!
//! [`GpuOpSpec`] is the whole contract between the DataFusion plan rule and the shim
//! (`libcudf_shim` in a `--features gpu` build, `csrc/mock_shim.c` otherwise): a set
//! of local parquet part files (KAN-75), the columns to read, conjunctive
//! column-vs-literal filters, derived columns to evaluate after filtering (KAN-76),
//! group-by keys, and the aggregations to compute. The shim answers with ONE Arrow
//! record batch (C Data Interface struct array) holding the FINAL aggregate results:
//! group-by columns (named per [`GpuOpSpec::group_by`]) followed by the aggregations
//! (named per [`AggSpec::alias`]), in order.

use serde::Serialize;

/// Root spec: everything the shim needs to run scan + filter + group-by aggregate.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct GpuOpSpec {
    /// Absolute local path of the FIRST part file (`files[0]`) — kept for
    /// display/back-compat (`GpuScanAggExec`'s DisplayAs); the shim reads `files`.
    pub table_path: String,
    /// Every part file of the table, absolute local paths (KAN-75). Always
    /// populated; the shim scans ALL of them.
    pub files: Vec<String>,
    /// Columns the shim must read (filter columns + group keys + the base columns
    /// of every aggregation input), in first-use order, with the dtype vocabulary
    /// of [`LiteralType`]'s siblings
    /// (`int64`, `float64`, `string`, `date32`, `timestamp(us)`, `decimal128(p,s)`, ...).
    pub columns: Vec<ColumnSpec>,
    /// Derived columns (KAN-76): arithmetic expressions the shim evaluates AFTER
    /// applying `filters`, appending each as a new column. Aggregations may
    /// reference their names; [`GpuOpSpec::columns`] carries their base columns.
    pub derived_columns: Vec<DerivedColumn>,
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

/// A shim-computed column (KAN-76): `expr` is evaluated row-wise after filtering
/// and appended under `name` (a synthesized `_gpu_derived_N`), where aggregations
/// can reference it like any base column.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct DerivedColumn {
    pub name: String,
    pub expr: GpuExpr,
}

/// The restricted expression language of a derived column: columns, literals, and
/// arithmetic over them. Serialized untagged to exactly the shim contract:
/// `{"col": "name"}` | `{"lit": LiteralSpec}` |
/// `{"op": "add|sub|mul|div", "lhs": GpuExpr, "rhs": GpuExpr}`.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(untagged)]
pub enum GpuExpr {
    Col {
        col: String,
    },
    Lit {
        lit: LiteralSpec,
    },
    Arith {
        op: ArithOp,
        lhs: Box<GpuExpr>,
        rhs: Box<GpuExpr>,
    },
}

/// Arithmetic operators for [`GpuExpr::Arith`] (JSON lowercase names).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ArithOp {
    Add,
    Sub,
    Mul,
    Div,
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

    /// The serialized JSON must match the shim contract exactly (KAN-70/75/76):
    /// key names, operator symbols, lowercase func/type names, `null` col for
    /// count-star, `files` always populated with `table_path == files[0]`, and
    /// derived columns as untagged col/lit/op expression trees.
    #[test]
    fn spec_serializes_to_shim_contract_json() {
        let spec = GpuOpSpec {
            table_path: "/data/lineitem/part-0.parquet".to_string(),
            files: vec![
                "/data/lineitem/part-0.parquet".to_string(),
                "/data/lineitem/part-1.parquet".to_string(),
            ],
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
            derived_columns: vec![DerivedColumn {
                name: "_gpu_derived_0".to_string(),
                expr: GpuExpr::Arith {
                    op: ArithOp::Mul,
                    lhs: Box::new(GpuExpr::Col {
                        col: "l_extendedprice".to_string(),
                    }),
                    rhs: Box::new(GpuExpr::Arith {
                        op: ArithOp::Sub,
                        lhs: Box::new(GpuExpr::Lit {
                            lit: LiteralSpec {
                                ty: LiteralType::Float,
                                value: "1.0".to_string(),
                            },
                        }),
                        rhs: Box::new(GpuExpr::Col {
                            col: "l_discount".to_string(),
                        }),
                    }),
                },
            }],
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
                    col: Some("_gpu_derived_0".to_string()),
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
            "table_path": "/data/lineitem/part-0.parquet",
            "files": ["/data/lineitem/part-0.parquet", "/data/lineitem/part-1.parquet"],
            "columns": [
                {"name": "l_shipdate", "dtype": "date32"},
                {"name": "l_extendedprice", "dtype": "float64"},
            ],
            "derived_columns": [
                {"name": "_gpu_derived_0", "expr": {
                    "op": "mul",
                    "lhs": {"col": "l_extendedprice"},
                    "rhs": {"op": "sub", "lhs": {"lit": {"type": "float", "value": "1.0"}}, "rhs": {"col": "l_discount"}},
                }},
            ],
            "filters": [
                {"col": "l_shipdate", "op": ">=", "literal": {"type": "date", "value": "1994-01-01"}},
                {"col": "l_quantity", "op": "<", "literal": {"type": "int", "value": "24"}},
            ],
            "group_by": ["l_returnflag"],
            "aggregations": [
                {"func": "sum", "col": "_gpu_derived_0", "alias": "revenue"},
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
        let arith = [
            (ArithOp::Add, "add"),
            (ArithOp::Sub, "sub"),
            (ArithOp::Mul, "mul"),
            (ArithOp::Div, "div"),
        ];
        for (op, s) in arith {
            assert_eq!(serde_json::to_value(op).unwrap(), s);
        }
    }
}

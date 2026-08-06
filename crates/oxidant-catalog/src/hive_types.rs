//! Hive/Glue type-string → Arrow type mapping, shared by the Hive Metastore and AWS Glue catalog
//! providers (both speak the same Hive type-string vocabulary).
//!
//! The point of attaching a catalog-declared schema to a [`TableMetadata`](crate::TableMetadata) is
//! to let the engine read files *against* the authoritative schema: files whose physical types
//! differ (e.g. one monthly Parquet dump stores a column as `int`, another as `bigint`) are cast to
//! the declared types at scan time, instead of failing DataFusion's strict schema-inference
//! "merge" check.
//!
//! Mapping policy is deliberately **all-or-nothing** ([`columns_to_schema`]): if any column's type
//! string is one we don't faithfully model (complex `array<…>` / `struct<…>` / `map<…>`, or
//! anything unrecognized), we return `None` so the caller falls back to inferring the *whole*
//! table from the data files. A partial schema would shift column positions and silently corrupt
//! reads, so inferring everything is the safe choice.

use crate::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use crate::{Error, Result, TableFormat};

/// Build an Arrow [`Schema`] from an ordered list of `(column_name, hive_type_string)` pairs
/// (data columns first, then partition columns).
///
/// Returns `None` — meaning "fall back to data-file inference" — when the list is empty or any
/// column's type string is unmappable (see the module docs for why this is all-or-nothing).
pub fn columns_to_schema(columns: impl IntoIterator<Item = (String, String)>) -> Option<Schema> {
    let mut fields: Vec<Field> = Vec::new();
    for (name, ty) in columns {
        let dt = hive_type_to_arrow(&ty)?;
        // Catalog columns are nullable: Hive/Glue don't carry per-column NOT NULL, external data
        // lakes routinely contain nulls, and this matches what Parquet inference would produce.
        fields.push(Field::new(name, dt, true));
    }
    if fields.is_empty() {
        return None;
    }
    Some(Schema::new(fields))
}

/// Map a Hive/Glue type string to an Arrow [`DataType`].
///
/// Covers the common scalar types and the parameterized `decimal(p,s)` / `varchar(n)` / `char(n)`.
/// Returns `None` for complex types (`array<…>`, `struct<…>`, `map<…>`, `uniontype<…>`) and any
/// unrecognized string, so the caller can fall back to inferring the whole table's schema from the
/// data files rather than risk a wrong mapping. Whitespace- and case-insensitive.
pub fn hive_type_to_arrow(ty: &str) -> Option<DataType> {
    let t = ty.trim().to_ascii_lowercase();
    Some(match t.as_str() {
        "bigint" | "long" => DataType::Int64,
        "int" | "integer" => DataType::Int32,
        "smallint" | "short" => DataType::Int16,
        "tinyint" | "byte" => DataType::Int8,
        "double" => DataType::Float64,
        "float" | "real" => DataType::Float32,
        // Hive/Spark `string` and unparameterized `varchar`/`char` map to Arrow Utf8.
        "string" | "varchar" | "char" | "text" => DataType::Utf8,
        "boolean" | "bool" => DataType::Boolean,
        "date" => DataType::Date32,
        // Hive `timestamp` is microsecond, no timezone — the standard Hive/Spark→Arrow mapping and
        // what Spark-written Parquet stores (logical TIMESTAMP_MICROS / INT96 reads as micros).
        "timestamp" => DataType::Timestamp(TimeUnit::Microsecond, None),
        "binary" => DataType::Binary,
        // Parameterized: varchar(n) / char(n) → Utf8 (length is a constraint, not an Arrow type);
        // decimal(p,s) → Decimal128(p,s). Anything else falls through to `None`.
        _ => {
            if t.starts_with("varchar(") || t.starts_with("char(") {
                DataType::Utf8
            } else if let Some(args) = t
                .strip_prefix("decimal(")
                .or_else(|| t.strip_prefix("numeric("))
                .and_then(|r| r.strip_suffix(')'))
            {
                let (p, s) = parse_decimal_args(args)?;
                DataType::Decimal128(p, s)
            } else {
                // Bare `decimal`/`numeric` default to Hive's (10, 0).
                match t.as_str() {
                    "decimal" | "numeric" => DataType::Decimal128(10, 0),
                    _ => return None,
                }
            }
        }
    })
}

/// Parse the `p,s` (or just `p`) inside `decimal(...)`. Scale defaults to 0. Returns `None` on a
/// malformed spec so the column falls back to inference.
fn parse_decimal_args(args: &str) -> Option<(u8, i8)> {
    let mut it = args.split(',');
    let p: u8 = it.next()?.trim().parse().ok()?;
    let s: i8 = match it.next() {
        Some(s) => s.trim().parse().ok()?,
        None => 0,
    };
    if it.next().is_some() || p == 0 {
        return None;
    }
    Some((p, s))
}

/// The reverse of [`hive_type_to_arrow`]: map an Arrow [`DataType`] to the Hive/Glue type string a
/// `CREATE TABLE` (Glue `create-table` / Hive Metastore `create_table`) declares it as. Used when
/// writing a CTAS result's schema back to an external catalog. Returns `None` for a type this
/// mapping can't faithfully declare (e.g. `List`/`Struct`/`Map`) — the caller should surface this
/// as `Error::Unsupported` rather than silently guessing a lossy Hive type.
pub fn arrow_type_to_hive(dt: &DataType) -> Option<String> {
    Some(match dt {
        DataType::Int64 => "bigint".to_string(),
        DataType::Int32 => "int".to_string(),
        DataType::Int16 => "smallint".to_string(),
        DataType::Int8 => "tinyint".to_string(),
        DataType::Float64 => "double".to_string(),
        DataType::Float32 => "float".to_string(),
        DataType::Utf8 | DataType::LargeUtf8 => "string".to_string(),
        DataType::Boolean => "boolean".to_string(),
        DataType::Date32 | DataType::Date64 => "date".to_string(),
        // Hive `timestamp` has no unit/timezone distinction — every Arrow timestamp variant
        // declares the same Hive type string (the engine's writer controls the physical encoding).
        DataType::Timestamp(_, _) => "timestamp".to_string(),
        DataType::Binary | DataType::LargeBinary => "binary".to_string(),
        DataType::Decimal128(p, s) => format!("decimal({p},{s})"),
        _ => return None,
    })
}

/// The reverse of [`columns_to_schema`]: split an Arrow [`Schema`] into `(data_columns,
/// partition_columns)` — each an ordered list of `(name, hive_type_string)` pairs — for building a
/// Glue `TableInput`/Hive `Table` write request. `partition_columns` names the fields (already
/// present in `schema`) that are partition keys rather than data columns.
///
/// Returns `Error::Unsupported` naming the first column whose type [`arrow_type_to_hive`] can't
/// map, rather than silently dropping or misdeclaring it.
pub type HiveColumns = Vec<(String, String)>;

pub fn schema_to_columns(
    schema: &Schema,
    partition_columns: &[String],
) -> Result<(HiveColumns, HiveColumns)> {
    let mut data_cols = Vec::new();
    let mut part_cols = Vec::new();
    for field in schema.fields() {
        let ty = arrow_type_to_hive(field.data_type()).ok_or_else(|| {
            Error::Unsupported(format!(
                "column `{}` has type {:?}, which cannot be declared to an external catalog",
                field.name(),
                field.data_type()
            ))
        })?;
        if partition_columns.iter().any(|p| p == field.name()) {
            part_cols.push((field.name().clone(), ty));
        } else {
            data_cols.push((field.name().clone(), ty));
        }
    }
    Ok((data_cols, part_cols))
}

/// The Hive SerDe/InputFormat/OutputFormat triple (plus SerDe parameters) a `CREATE TABLE`
/// declares for a given physical format — identical whether the table is fronted by AWS Glue or a
/// native Hive Metastore, since both speak the same Hive storage-descriptor vocabulary.
#[derive(Debug, Clone, Copy)]
pub struct HiveSerde {
    pub input_format: &'static str,
    pub output_format: &'static str,
    pub serde_lib: &'static str,
    pub serde_params: &'static [(&'static str, &'static str)],
}

/// Look up the [`HiveSerde`] for a CTAS write target format. Only `Parquet`/`Csv`/`Json` are
/// supported write targets today — `Delta`/`Iceberg` need a real commit protocol (transaction log
/// / manifest+snapshot) rather than a plain SerDe declaration, so they return `Unsupported`.
pub fn format_serde(format: TableFormat) -> Result<HiveSerde> {
    match format {
        TableFormat::Parquet => Ok(HiveSerde {
            input_format: "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
            output_format: "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
            serde_lib: "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
            serde_params: &[],
        }),
        TableFormat::Csv => Ok(HiveSerde {
            input_format: "org.apache.hadoop.mapred.TextInputFormat",
            output_format: "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
            serde_lib: "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
            serde_params: &[("field.delim", ",")],
        }),
        TableFormat::Json => Ok(HiveSerde {
            input_format: "org.apache.hadoop.mapred.TextInputFormat",
            output_format: "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
            serde_lib: "org.apache.hive.hcatalog.data.JsonSerDe",
            serde_params: &[],
        }),
        TableFormat::Delta | TableFormat::Iceberg => Err(Error::Unsupported(format!(
            "{format:?} is not a supported CTAS write target yet (needs a real commit protocol, \
             not a plain SerDe declaration)"
        ))),
    }
}

/// Validate that `value` is safe to use as a bare `db`/`table` path segment when building a
/// warehouse-derived default location (`{warehouse}/{db}/{table}/`) — i.e. it's a plain identifier
/// (ASCII alphanumeric/underscore, non-empty), not `.`/`..`/a path separator/anything else that
/// could escape the intended directory. Glue and Hive database/table names are conventionally
/// restricted to this shape anyway, so this rejects malformed input rather than silently mangling
/// it (unlike the unrelated local-warehouse directory-naming `sanitize()`, which only ever feeds a
/// throwaway directory name, not a catalog-visible identifier).
pub fn validate_identifier(kind: &str, value: &str) -> Result<()> {
    let ok = !value.is_empty() && value.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
    if ok {
        Ok(())
    } else {
        Err(Error::Plan(format!(
            "{kind} `{value}` is not a valid identifier (expected letters, digits, underscore only)"
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scalar_type_mapping() {
        let cases = [
            ("bigint", DataType::Int64),
            ("INT", DataType::Int32),
            ("integer", DataType::Int32),
            ("smallint", DataType::Int16),
            ("tinyint", DataType::Int8),
            ("double", DataType::Float64),
            ("float", DataType::Float32),
            ("string", DataType::Utf8),
            (" Boolean ", DataType::Boolean),
            ("date", DataType::Date32),
            ("binary", DataType::Binary),
            (
                "timestamp",
                DataType::Timestamp(TimeUnit::Microsecond, None),
            ),
        ];
        for (hive, arrow) in cases {
            assert_eq!(hive_type_to_arrow(hive), Some(arrow), "type `{hive}`");
        }
    }

    #[test]
    fn parameterized_type_mapping() {
        assert_eq!(hive_type_to_arrow("varchar(50)"), Some(DataType::Utf8));
        assert_eq!(hive_type_to_arrow("char(10)"), Some(DataType::Utf8));
        assert_eq!(
            hive_type_to_arrow("decimal(15,2)"),
            Some(DataType::Decimal128(15, 2))
        );
        // Spaces inside the parens are tolerated.
        assert_eq!(
            hive_type_to_arrow("decimal( 38 , 10 )"),
            Some(DataType::Decimal128(38, 10))
        );
        // Precision-only decimal → scale 0; bare `decimal` → Hive default (10, 0).
        assert_eq!(
            hive_type_to_arrow("decimal(9)"),
            Some(DataType::Decimal128(9, 0))
        );
        assert_eq!(
            hive_type_to_arrow("decimal"),
            Some(DataType::Decimal128(10, 0))
        );
    }

    #[test]
    fn unknown_and_complex_types_fall_back() {
        // Complex types we don't model → None (whole-table inference fallback).
        assert_eq!(hive_type_to_arrow("array<int>"), None);
        assert_eq!(hive_type_to_arrow("struct<a:int,b:string>"), None);
        assert_eq!(hive_type_to_arrow("map<string,int>"), None);
        assert_eq!(hive_type_to_arrow("uniontype<int,string>"), None);
        // Unrecognized / malformed.
        assert_eq!(hive_type_to_arrow("frobnicate"), None);
        assert_eq!(hive_type_to_arrow("decimal(x,2)"), None);
        assert_eq!(hive_type_to_arrow("decimal(1,2,3)"), None);
    }

    #[test]
    fn schema_from_columns_includes_partition_keys() {
        let cols = [
            ("vendor_id".to_string(), "bigint".to_string()),
            ("fare".to_string(), "decimal(10,2)".to_string()),
            // Partition column appended after data columns by the caller.
            ("month".to_string(), "string".to_string()),
        ];
        let schema = columns_to_schema(cols).expect("schema built");
        assert_eq!(schema.fields().len(), 3);
        assert_eq!(schema.field(0).name(), "vendor_id");
        assert_eq!(schema.field(0).data_type(), &DataType::Int64);
        assert_eq!(schema.field(1).data_type(), &DataType::Decimal128(10, 2));
        assert_eq!(schema.field(2).name(), "month");
        assert_eq!(schema.field(2).data_type(), &DataType::Utf8);
        // Catalog columns are nullable.
        assert!(schema.field(0).is_nullable());
    }

    #[test]
    fn empty_columns_fall_back_to_inference() {
        let empty: Vec<(String, String)> = Vec::new();
        assert_eq!(columns_to_schema(empty), None);
    }

    #[test]
    fn any_unmappable_column_falls_back_to_inference() {
        // One complex column poisons the whole schema → infer rather than shift positions.
        let cols = [
            ("id".to_string(), "bigint".to_string()),
            ("tags".to_string(), "array<string>".to_string()),
        ];
        assert_eq!(columns_to_schema(cols), None);
    }

    #[test]
    fn arrow_type_to_hive_round_trips_hive_type_to_arrow() {
        // Every scalar hive_type_to_arrow maps FROM should round-trip back to an equivalent
        // (not necessarily byte-identical, e.g. "long"→Int64→"bigint") Hive type string.
        let cases = [
            (DataType::Int64, "bigint"),
            (DataType::Int32, "int"),
            (DataType::Int16, "smallint"),
            (DataType::Int8, "tinyint"),
            (DataType::Float64, "double"),
            (DataType::Float32, "float"),
            (DataType::Utf8, "string"),
            (DataType::Boolean, "boolean"),
            (DataType::Date32, "date"),
            (
                DataType::Timestamp(TimeUnit::Microsecond, None),
                "timestamp",
            ),
            (DataType::Binary, "binary"),
            (DataType::Decimal128(15, 2), "decimal(15,2)"),
        ];
        for (arrow, hive) in cases {
            assert_eq!(
                arrow_type_to_hive(&arrow).as_deref(),
                Some(hive),
                "{arrow:?}"
            );
            // And the forward mapping accepts what we just produced.
            assert_eq!(hive_type_to_arrow(hive), Some(arrow.clone()), "{hive}");
        }
    }

    #[test]
    fn arrow_type_to_hive_rejects_complex_types() {
        assert_eq!(
            arrow_type_to_hive(&DataType::List(std::sync::Arc::new(Field::new(
                "item",
                DataType::Int32,
                true
            )))),
            None
        );
    }

    #[test]
    fn schema_to_columns_splits_data_and_partition_columns() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("dt", DataType::Utf8, true),
        ]);
        let (data, parts) = schema_to_columns(&schema, &["dt".to_string()]).expect("mapped");
        assert_eq!(
            data,
            vec![
                ("id".to_string(), "bigint".to_string()),
                ("name".to_string(), "string".to_string()),
            ]
        );
        assert_eq!(parts, vec![("dt".to_string(), "string".to_string())]);
    }

    #[test]
    fn schema_to_columns_rejects_unmappable_type() {
        let schema = Schema::new(vec![Field::new(
            "tags",
            DataType::List(std::sync::Arc::new(Field::new(
                "item",
                DataType::Utf8,
                true,
            ))),
            true,
        )]);
        let err = schema_to_columns(&schema, &[]).unwrap_err();
        assert!(matches!(err, Error::Unsupported(_)));
    }

    #[test]
    fn format_serde_covers_write_targets_and_rejects_lakehouse_formats() {
        for format in [TableFormat::Parquet, TableFormat::Csv, TableFormat::Json] {
            assert!(format_serde(format).is_ok(), "{format:?}");
        }
        for format in [TableFormat::Delta, TableFormat::Iceberg] {
            assert!(
                matches!(format_serde(format), Err(Error::Unsupported(_))),
                "{format:?}"
            );
        }
    }

    #[test]
    fn validate_identifier_accepts_plain_names() {
        for ok in ["orders", "Orders_2024", "_tmp", "a1"] {
            assert!(validate_identifier("table", ok).is_ok(), "{ok}");
        }
    }

    #[test]
    fn validate_identifier_rejects_path_traversal_and_separators() {
        for bad in ["..", "../../etc/evil", "a/b", "a\\b", "", "a.b", "a b"] {
            let err = validate_identifier("table", bad).unwrap_err();
            assert!(matches!(err, Error::Plan(_)), "{bad}");
        }
    }
}

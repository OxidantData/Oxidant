//! Arrow → Spark Connect `DataType` conversion, for `AnalyzePlan(Schema)` and for stamping
//! result-relation schemas. Covers the scalar types Oxidant produces today (all of ClickBench) plus
//! best-effort nested types; anything unmapped falls back to `string` rather than failing.

use oxidant_loom::arrow::datatypes::{DataType, Fields, Schema};
use oxidant_proto::spark::connect as sc;

use sc::data_type as d;
use sc::data_type::Kind;

fn wrap(kind: Kind) -> sc::DataType {
    sc::DataType { kind: Some(kind) }
}

/// Convert one Arrow [`DataType`] to a Spark Connect [`sc::DataType`].
pub fn arrow_to_spark(t: &DataType) -> sc::DataType {
    let kind = match t {
        DataType::Null => Kind::Null(d::Null::default()),
        DataType::Boolean => Kind::Boolean(d::Boolean::default()),
        DataType::Int8 | DataType::UInt8 => Kind::Byte(d::Byte::default()),
        DataType::Int16 | DataType::UInt16 => Kind::Short(d::Short::default()),
        DataType::Int32 | DataType::UInt32 => Kind::Integer(d::Integer::default()),
        DataType::Int64 | DataType::UInt64 => Kind::Long(d::Long::default()),
        DataType::Float16 | DataType::Float32 => Kind::Float(d::Float::default()),
        DataType::Float64 => Kind::Double(d::Double::default()),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            Kind::String(d::String::default())
        }
        DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => Kind::Binary(d::Binary::default()),
        DataType::Date32 | DataType::Date64 => Kind::Date(d::Date::default()),
        DataType::Timestamp(_, Some(_)) => Kind::Timestamp(d::Timestamp::default()),
        DataType::Timestamp(_, None) => Kind::TimestampNtz(d::TimestampNtz::default()),
        DataType::Decimal128(p, s) | DataType::Decimal256(p, s) => Kind::Decimal(d::Decimal {
            precision: Some(*p as i32),
            scale: Some(*s as i32),
            type_variation_reference: 0,
        }),
        DataType::List(f)
        | DataType::LargeList(f)
        | DataType::ListView(f)
        | DataType::LargeListView(f)
        | DataType::FixedSizeList(f, _) => Kind::Array(Box::new(d::Array {
            element_type: Some(Box::new(arrow_to_spark(f.data_type()))),
            contains_null: f.is_nullable(),
            type_variation_reference: 0,
        })),
        DataType::Struct(fields) => Kind::Struct(struct_of(fields)),
        DataType::Map(entry, _) => {
            // The Map field's child is a Struct{key, value}.
            if let DataType::Struct(kv) = entry.data_type() {
                let key = kv.first().map(|f| arrow_to_spark(f.data_type()));
                let val = kv.get(1);
                Kind::Map(Box::new(d::Map {
                    key_type: key.map(Box::new),
                    value_type: val.map(|f| Box::new(arrow_to_spark(f.data_type()))),
                    value_contains_null: val.map(|f| f.is_nullable()).unwrap_or(true),
                    type_variation_reference: 0,
                }))
            } else {
                Kind::String(d::String::default())
            }
        }
        // Intervals, time, dictionary, union, etc. — not produced by Oxidant yet; be lenient.
        _ => Kind::String(d::String::default()),
    };
    wrap(kind)
}

/// Build a Spark Connect `Struct` `DataType` from Arrow [`Fields`].
fn struct_of(fields: &Fields) -> d::Struct {
    d::Struct {
        fields: fields
            .iter()
            .map(|f| d::StructField {
                name: f.name().clone(),
                data_type: Some(arrow_to_spark(f.data_type())),
                nullable: f.is_nullable(),
                metadata: None,
            })
            .collect(),
        type_variation_reference: 0,
    }
}

/// Convert an Arrow [`Schema`] to a Spark Connect struct `DataType` (a row schema).
pub fn schema_to_spark(schema: &Schema) -> sc::DataType {
    wrap(Kind::Struct(struct_of(schema.fields())))
}

/// The Spark type name printed by `df.printSchema()` for an Arrow [`DataType`] (mirrors the
/// `arrow_to_spark` mapping: unsigned ints widen to Spark's signed names, unmapped → `string`).
fn spark_type_name(t: &DataType) -> String {
    match t {
        DataType::Null => "void".to_string(),
        DataType::Boolean => "boolean".to_string(),
        DataType::Int8 | DataType::UInt8 => "byte".to_string(),
        DataType::Int16 | DataType::UInt16 => "short".to_string(),
        DataType::Int32 | DataType::UInt32 => "integer".to_string(),
        DataType::Int64 | DataType::UInt64 => "long".to_string(),
        DataType::Float16 | DataType::Float32 => "float".to_string(),
        DataType::Float64 => "double".to_string(),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "string".to_string(),
        DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => "binary".to_string(),
        DataType::Date32 | DataType::Date64 => "date".to_string(),
        DataType::Timestamp(_, Some(_)) => "timestamp".to_string(),
        DataType::Timestamp(_, None) => "timestamp_ntz".to_string(),
        DataType::Decimal128(p, s) | DataType::Decimal256(p, s) => format!("decimal({p},{s})"),
        DataType::List(_)
        | DataType::LargeList(_)
        | DataType::ListView(_)
        | DataType::LargeListView(_)
        | DataType::FixedSizeList(_, _) => "array".to_string(),
        DataType::Struct(_) => "struct".to_string(),
        DataType::Map(_, _) => "map".to_string(),
        _ => "string".to_string(),
    }
}

/// Format an Arrow [`Schema`] as Spark's `printSchema()` tree (the `TreeString` analyze response):
///
/// ```text
/// root
///  |-- a: long (nullable = false)
///  |-- b: string (nullable = true)
/// ```
///
/// Struct/array element children are recursed into with Spark's `|    |--` indentation so nested
/// schemas render the way `df.printSchema()` does.
pub fn schema_tree_string(schema: &Schema) -> String {
    let mut out = String::from("root\n");
    for f in schema.fields() {
        tree_field(&mut out, " |", f.name(), f.data_type(), f.is_nullable());
    }
    out
}

/// Append one field line (and any nested children) to a printSchema tree under `prefix`.
fn tree_field(out: &mut String, prefix: &str, name: &str, dt: &DataType, nullable: bool) {
    out.push_str(&format!(
        "{prefix}-- {name}: {} (nullable = {nullable})\n",
        spark_type_name(dt)
    ));
    let child_prefix = format!("{prefix}    |");
    match dt {
        DataType::Struct(fields) => {
            for f in fields {
                tree_field(out, &child_prefix, f.name(), f.data_type(), f.is_nullable());
            }
        }
        DataType::List(f)
        | DataType::LargeList(f)
        | DataType::ListView(f)
        | DataType::LargeListView(f)
        | DataType::FixedSizeList(f, _) => {
            tree_field(
                out,
                &child_prefix,
                "element",
                f.data_type(),
                f.is_nullable(),
            );
        }
        _ => {}
    }
}

/// Convert a Spark Connect [`sc::DataType`] to an Arrow [`DataType`] (the reverse of
/// [`arrow_to_spark`]) — used to lower `cast` targets. Unmapped kinds error.
pub fn spark_to_arrow(t: &sc::DataType) -> Result<DataType, tonic::Status> {
    use datafusion::arrow::datatypes::{Field, TimeUnit};
    use std::sync::Arc;
    let kind = t
        .kind
        .as_ref()
        .ok_or_else(|| tonic::Status::invalid_argument("empty DataType"))?;
    Ok(match kind {
        Kind::Null(_) => DataType::Null,
        Kind::Boolean(_) => DataType::Boolean,
        Kind::Byte(_) => DataType::Int8,
        Kind::Short(_) => DataType::Int16,
        Kind::Integer(_) => DataType::Int32,
        Kind::Long(_) => DataType::Int64,
        Kind::Float(_) => DataType::Float32,
        Kind::Double(_) => DataType::Float64,
        Kind::String(_) => DataType::Utf8,
        Kind::Binary(_) => DataType::Binary,
        Kind::Date(_) => DataType::Date32,
        Kind::Timestamp(_) => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        Kind::TimestampNtz(_) => DataType::Timestamp(TimeUnit::Microsecond, None),
        Kind::Decimal(d) => {
            DataType::Decimal128(d.precision.unwrap_or(38) as u8, d.scale.unwrap_or(0) as i8)
        }
        Kind::Array(a) => {
            let inner = a
                .element_type
                .as_deref()
                .ok_or_else(|| tonic::Status::invalid_argument("array.element_type"))?;
            DataType::List(Arc::new(Field::new(
                "item",
                spark_to_arrow(inner)?,
                a.contains_null,
            )))
        }
        other => {
            return Err(tonic::Status::unimplemented(format!(
                "spark→arrow type: {other:?}"
            )))
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use oxidant_loom::arrow::datatypes::{Field, Schema};
    use std::sync::Arc;

    #[test]
    fn maps_clickbench_scalars() {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Utf8, true),
            Field::new("c", DataType::Float64, false),
            Field::new("d", DataType::Date32, false),
            Field::new("e", DataType::Int16, false),
        ]);
        let st = schema_to_spark(&schema);
        let Some(Kind::Struct(s)) = st.kind else {
            panic!("expected struct")
        };
        assert_eq!(s.fields.len(), 5);
        assert!(matches!(
            s.fields[0].data_type.as_ref().unwrap().kind,
            Some(Kind::Long(_))
        ));
        assert!(matches!(
            s.fields[1].data_type.as_ref().unwrap().kind,
            Some(Kind::String(_))
        ));
        assert!(matches!(
            s.fields[2].data_type.as_ref().unwrap().kind,
            Some(Kind::Double(_))
        ));
        assert!(matches!(
            s.fields[3].data_type.as_ref().unwrap().kind,
            Some(Kind::Date(_))
        ));
        assert!(matches!(
            s.fields[4].data_type.as_ref().unwrap().kind,
            Some(Kind::Short(_))
        ));
        assert!(s.fields[1].nullable);
        assert!(!s.fields[0].nullable);
    }

    #[test]
    fn nested_list_and_struct() {
        let inner = DataType::List(Arc::new(Field::new("item", DataType::Int32, true)));
        let st = arrow_to_spark(&inner);
        assert!(matches!(st.kind, Some(Kind::Array(_))));
    }
}

//! Output schema enforcement, table-property validation, and multi-flow SQL composition.

use std::collections::BTreeMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use oxidant_common::{Error, Result};
use oxidant_config::AppendFlow;
use oxidant_loom::schema_conform::conform_batch_to_schema;
use oxidant_loom::spark_functions::parse_spark_schema;
use oxidant_loom::Engine;

/// A single flow query and its column-matching mode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlowQuery {
    pub sql: String,
    pub by_name: bool,
}

/// Kafka / streaming source options allowed in SDP `TBLPROPERTIES`.
const SOURCE_TABLE_PROPERTIES: &[&str] = &[
    "subscribe",
    "topic",
    "oxidant.spool.dir",
    "startingOffsets",
    "maxOffsetsPerTrigger",
    "kafka.bootstrap.servers",
    "bootstrap.servers",
];

/// Sink-side table properties the pipeline runner understands today.
const SINK_TABLE_PROPERTIES: &[&str] = &["icebergCompat"];

/// Parse a declared output schema (`(id INT, name STRING)` or `id INT, name STRING`) to Arrow.
pub fn parse_output_schema(ddl: &str) -> Result<SchemaRef> {
    let trimmed = ddl.trim();
    let inner = trimmed
        .strip_prefix('(')
        .and_then(|s| s.strip_suffix(')'))
        .unwrap_or(trimmed)
        .trim();
    if inner.is_empty() {
        return Err(Error::Plan("output schema must not be empty".into()));
    }
    let dt = parse_spark_schema(inner)
        .map_err(|e| Error::Plan(format!("invalid output schema `{ddl}`: {e}")))?;
    match dt {
        DataType::Struct(fields) => Ok(Arc::new(Schema::new(fields))),
        other => Err(Error::Plan(format!(
            "output schema must be a struct of columns, got {other:?}"
        ))),
    }
}

/// Split SDP `TBLPROPERTIES` into source options and sink options, refusing unknown keys.
pub fn split_table_properties(
    properties: &BTreeMap<String, String>,
) -> Result<(BTreeMap<String, String>, BTreeMap<String, String>)> {
    let mut source = BTreeMap::new();
    let mut sink = BTreeMap::new();
    for (key, value) in properties {
        if is_source_table_property(key) {
            source.insert(key.clone(), value.clone());
        } else if is_sink_table_property(key) {
            sink.insert(key.clone(), value.clone());
        } else {
            return Err(Error::Unsupported(format!(
                "TBLPROPERTIES key `{key}` is not supported on pipeline table outputs — remove \
                 it rather than silently dropping it"
            )));
        }
    }
    Ok((source, sink))
}

fn is_source_table_property(key: &str) -> bool {
    SOURCE_TABLE_PROPERTIES.contains(&key) || key.starts_with("kafka.") || key.starts_with("spark.")
}

fn is_sink_table_property(key: &str) -> bool {
    SINK_TABLE_PROPERTIES.contains(&key)
}

/// Validate an SDP external sink format.
///
/// A sink writes through the same `LakeSink` a `writeStream` drives, which implements exactly
/// two formats: `delta` (one atomic transaction per micro-batch) and bare `parquet` (one file
/// per batch, no commit protocol). `csv` / `json` are writable *table* formats but have no
/// streaming writer, so they are refused here rather than accepted and failed mid-run. Kafka is
/// refused outright — the Kafka integration is source-only.
pub fn validate_external_sink_format(format: &str, label: &str) -> Result<()> {
    let normalized = format.trim().to_ascii_lowercase();
    if normalized == "kafka" {
        return Err(Error::Unsupported(format!(
            "{label}: Kafka sink is not supported — the Kafka integration is source-only \
             (see docs/TODOS.md)"
        )));
    }
    validate_output_format(format, label)?;
    if !matches!(normalized.as_str(), "delta" | "parquet") {
        return Err(Error::Unsupported(format!(
            "{label}: sink format `{format}` has no streaming writer — use `delta` (atomic \
             per-batch commits) or `parquet` (one file per batch, no commit protocol; \
             see docs/TODOS.md)"
        )));
    }
    Ok(())
}

/// Validate a table output format; mirrors [`oxidant_config::validate`] sink rules.
pub fn validate_output_format(format: &str, label: &str) -> Result<()> {
    let normalized = format.trim().to_ascii_lowercase();
    if matches!(normalized.as_str(), "delta" | "parquet" | "csv" | "json") {
        return Ok(());
    }
    if normalized == "iceberg" {
        return Err(Error::Unsupported(format!(
            "{label}: `iceberg` is not a write target. Use `delta` with `icebergCompat` enabled — \
             Iceberg metadata is published over the same Parquet files."
        )));
    }
    Err(Error::Unsupported(format!(
        "{label}: unwritable format `{format}` (expected delta, parquet, csv, or json)"
    )))
}

/// Collect the primary and append flows for a table.
pub fn flow_queries(
    sql: Option<&str>,
    sql_by_name: bool,
    append_flows: &[AppendFlow],
) -> Vec<FlowQuery> {
    let mut out = Vec::new();
    if let Some(sql) = sql.map(str::trim).filter(|s| !s.is_empty()) {
        out.push(FlowQuery {
            sql: sql.to_string(),
            by_name: sql_by_name,
        });
    }
    for flow in append_flows {
        let sql = flow.sql.trim();
        if sql.is_empty() {
            continue;
        }
        out.push(FlowQuery {
            sql: sql.to_string(),
            by_name: flow.by_name,
        });
    }
    out
}

fn validate_by_name_requires_schema(flows: &[FlowQuery], has_declared_schema: bool) -> Result<()> {
    if has_declared_schema {
        return Ok(());
    }
    for flow in flows {
        if flow.by_name {
            return Err(Error::Plan(
                "BY NAME requires a declared output schema on the target table".into(),
            ));
        }
    }
    Ok(())
}

/// Union flow queries, optionally enforcing a declared output schema on each branch.
pub async fn union_flow_sql(
    engine: &Engine,
    flows: &[FlowQuery],
    output_schema: Option<&str>,
) -> Result<String> {
    if flows.is_empty() {
        return Err(Error::Plan("table has no flow SQL".into()));
    }
    let target = output_schema.map(parse_output_schema).transpose()?;
    validate_by_name_requires_schema(flows, target.is_some())?;
    let mut branches = Vec::with_capacity(flows.len());
    for flow in flows {
        let branch = if let Some(schema) = &target {
            enforce_schema_sql(engine, &flow.sql, schema, flow.by_name).await?
        } else {
            flow.sql.clone()
        };
        branches.push(branch);
    }
    if branches.len() == 1 {
        return Ok(branches.into_iter().next().expect("one branch"));
    }
    Ok(branches
        .into_iter()
        .map(|sql| format!("({sql})"))
        .collect::<Vec<_>>()
        .join(" UNION ALL "))
}

/// Wrap `query` so its output matches `target`, casting compatible types and erroring otherwise.
pub async fn enforce_schema_sql(
    engine: &Engine,
    query: &str,
    target: &SchemaRef,
    by_name: bool,
) -> Result<String> {
    let source = engine.schema(query).await?;
    let projections = if by_name {
        by_name_projections(&source, target)?
    } else {
        positional_projections(&source, target)?
    };
    Ok(format!(
        "SELECT {} FROM ({}) AS _flow",
        projections.join(", "),
        query.trim()
    ))
}

fn positional_projections(source: &SchemaRef, target: &SchemaRef) -> Result<Vec<String>> {
    if source.fields().len() != target.fields().len() {
        return Err(Error::Plan(format!(
            "flow produces {} column(s) but the declared schema has {}",
            source.fields().len(),
            target.fields().len()
        )));
    }
    Ok(source
        .fields()
        .iter()
        .zip(target.fields())
        .map(|(src, dst)| column_projection(src.name(), dst))
        .collect())
}

fn by_name_projections(source: &SchemaRef, target: &SchemaRef) -> Result<Vec<String>> {
    let mut out = Vec::with_capacity(target.fields().len());
    for dst in target.fields() {
        let src = source
            .fields()
            .iter()
            .find(|f| f.name().eq_ignore_ascii_case(dst.name()))
            .ok_or_else(|| {
                Error::Plan(format!(
                    "flow is missing column `{}` required by the declared schema",
                    dst.name()
                ))
            })?;
        out.push(column_projection(src.name(), dst));
    }
    Ok(out)
}

fn column_projection(source_name: &str, target: &Field) -> String {
    format!(
        "{} AS {}",
        quote_ident(source_name),
        quote_ident(target.name())
    )
}

/// Cast query batches to a declared output schema with column-named errors.
pub fn conform_batches_to_schema(
    batches: Vec<datafusion::arrow::record_batch::RecordBatch>,
    target: &SchemaRef,
) -> Result<Vec<datafusion::arrow::record_batch::RecordBatch>> {
    batches
        .into_iter()
        .map(|batch| conform_batch_to_schema(batch, target))
        .collect()
}

fn quote_ident(name: &str) -> String {
    if name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
        && !name.is_empty()
        && !name.chars().next().unwrap().is_ascii_digit()
    {
        name.to_string()
    } else {
        format!("`{name}`")
    }
}

#[cfg_attr(not(test), allow(dead_code))]
fn spark_ddl_type(dt: &DataType) -> String {
    match dt {
        DataType::Boolean => "BOOLEAN".to_string(),
        DataType::Int8 | DataType::UInt8 => "TINYINT".to_string(),
        DataType::Int16 | DataType::UInt16 => "SMALLINT".to_string(),
        DataType::Int32 | DataType::UInt32 => "INT".to_string(),
        DataType::Int64 | DataType::UInt64 => "BIGINT".to_string(),
        DataType::Float16 | DataType::Float32 => "FLOAT".to_string(),
        DataType::Float64 => "DOUBLE".to_string(),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "STRING".to_string(),
        DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => "BINARY".to_string(),
        DataType::Date32 | DataType::Date64 => "DATE".to_string(),
        DataType::Timestamp(_, Some(_)) | DataType::Timestamp(_, None) => "TIMESTAMP".to_string(),
        DataType::Decimal128(p, s) | DataType::Decimal256(p, s) => format!("DECIMAL({p},{s})"),
        DataType::List(f)
        | DataType::LargeList(f)
        | DataType::ListView(f)
        | DataType::LargeListView(f)
        | DataType::FixedSizeList(f, _) => format!("ARRAY<{}>", spark_ddl_type(f.data_type())),
        DataType::Struct(fields) => {
            let inner: Vec<String> = fields
                .iter()
                .map(|f| {
                    format!(
                        "{}:{}",
                        quote_ident(f.name()),
                        spark_ddl_type(f.data_type())
                    )
                })
                .collect();
            format!("STRUCT<{}>", inner.join(","))
        }
        DataType::Map(entry, _) => match entry.data_type() {
            DataType::Struct(kv) if kv.len() == 2 => format!(
                "MAP<{},{}>",
                spark_ddl_type(kv[0].data_type()),
                spark_ddl_type(kv[1].data_type())
            ),
            _ => "MAP<STRING,STRING>".to_string(),
        },
        other => format!("{other:?}").to_uppercase(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::StringArray;
    use datafusion::arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    #[test]
    fn conform_rejects_invalid_string_to_bigint() {
        let source = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, true)]));
        let target = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(
            source,
            vec![Arc::new(StringArray::from(vec!["1", "not-a-number"]))],
        )
        .expect("batch");
        let err = conform_batch_to_schema(batch, &target).expect_err("invalid cast");
        assert!(err.to_string().contains("`id`"), "{err}");
    }

    #[test]
    fn parse_output_schema_accepts_parenthesized_ddl() {
        let schema = parse_output_schema("(id INT, name STRING)").expect("parses");
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).name(), "name");
    }

    #[test]
    fn refuses_unsupported_tblproperty_by_name() {
        let mut props = BTreeMap::new();
        props.insert(
            "delta.autoOptimize.optimizeWrite".to_string(),
            "true".to_string(),
        );
        let err = split_table_properties(&props).expect_err("unsupported property");
        assert!(
            err.to_string().contains("delta.autoOptimize.optimizeWrite"),
            "{err}"
        );
    }

    #[test]
    fn routes_kafka_and_sink_properties() {
        let props = BTreeMap::from([
            ("subscribe".to_string(), "orders".to_string()),
            ("icebergCompat".to_string(), "true".to_string()),
        ]);
        let (source, sink) = split_table_properties(&props).expect("allowed");
        assert_eq!(source.get("subscribe").map(String::as_str), Some("orders"));
        assert_eq!(sink.get("icebergCompat").map(String::as_str), Some("true"));
    }

    #[test]
    fn iceberg_format_points_at_delta_compat() {
        let err = validate_output_format("iceberg", "table `t`").expect_err("iceberg refused");
        assert!(err.to_string().contains("icebergCompat"), "{err}");
    }

    #[test]
    fn kafka_sink_format_is_refused() {
        let err = validate_external_sink_format("kafka", "sink `out`").expect_err("kafka refused");
        assert!(
            err.to_string().contains("Kafka sink is not supported"),
            "{err}"
        );
        assert!(err.to_string().contains("TODOS"), "{err}");
    }

    #[test]
    fn sink_formats_are_the_two_the_writer_implements() {
        validate_external_sink_format("delta", "sink `out`").expect("delta sink");
        validate_external_sink_format("PARQUET", "sink `out`")
            .expect("parquet sink, case-insensitive");
        // `csv` / `json` pass `validate_output_format` but LakeSink cannot write them, so a sink
        // must refuse them up front instead of failing on the first micro-batch.
        for format in ["csv", "json"] {
            validate_output_format(format, "table `out`").expect("writable table format");
            let err = validate_external_sink_format(format, "sink `out`")
                .expect_err("no streaming writer");
            assert!(err.to_string().contains("has no streaming writer"), "{err}");
        }
        let err = validate_external_sink_format("orc", "sink `out`").expect_err("unknown format");
        assert!(err.to_string().contains("unwritable format"), "{err}");
    }

    #[test]
    fn flow_queries_collects_primary_and_append_flows() {
        let flows = flow_queries(
            Some("SELECT 1"),
            false,
            &[AppendFlow {
                sql: "SELECT 2".into(),
                by_name: true,
            }],
        );
        assert_eq!(flows.len(), 2);
        assert!(!flows[0].by_name);
        assert!(flows[1].by_name);
    }

    #[test]
    fn positional_projection_mismatch_errors() {
        let source = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
        let target = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Utf8, true),
        ]));
        let err = positional_projections(&source, &target).expect_err("count mismatch");
        assert!(err.to_string().contains("1 column"), "{err}");
    }

    #[test]
    fn by_name_projection_maps_columns() {
        let source = Arc::new(Schema::new(vec![
            Field::new("b", DataType::Utf8, true),
            Field::new("a", DataType::Int32, true),
        ]));
        let target = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Utf8, true),
        ]));
        let projections = by_name_projections(&source, &target).expect("maps");
        assert!(projections[0].contains("a AS a"));
        assert!(projections[1].contains("b AS b"));
    }

    #[test]
    fn kafka_aliases_are_allowed_in_tblproperties() {
        let props = BTreeMap::from([
            ("topic".to_string(), "orders".to_string()),
            ("bootstrap.servers".to_string(), "b1:9092".to_string()),
        ]);
        let (source, sink) = split_table_properties(&props).expect("allowed");
        assert_eq!(source.get("topic").map(String::as_str), Some("orders"));
        assert_eq!(
            source.get("bootstrap.servers").map(String::as_str),
            Some("b1:9092")
        );
        assert!(sink.is_empty());
    }

    #[test]
    fn by_name_without_declared_schema_is_refused() {
        let flows = vec![FlowQuery {
            sql: "SELECT 1".into(),
            by_name: true,
        }];
        let err =
            validate_by_name_requires_schema(&flows, false).expect_err("by name needs schema");
        assert!(
            err.to_string()
                .contains("BY NAME requires a declared output schema"),
            "{err}"
        );
    }

    #[test]
    fn spark_ddl_type_renders_timestamp_for_planner() {
        assert_eq!(
            spark_ddl_type(&DataType::Timestamp(
                datafusion::arrow::datatypes::TimeUnit::Microsecond,
                None
            )),
            "TIMESTAMP"
        );
    }

    #[test]
    fn spark_ddl_type_quotes_struct_field_names() {
        assert_eq!(
            spark_ddl_type(&DataType::Struct(
                datafusion::arrow::datatypes::Fields::from(vec![Field::new(
                    "a b",
                    DataType::Int32,
                    true
                )])
            )),
            "STRUCT<`a b`:INT>"
        );
    }
}

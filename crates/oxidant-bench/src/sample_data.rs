//! `oxidant-bench sample-data` — regenerate the committed `sample-data/` tree: TPC-H SF 0.01
//! in four physical formats (CSV, Parquet, Delta, Iceberg), same rows in every format.
//!
//! ```text
//! sample-data/
//!   csv/tpch_<t>.csv          all 8 tables, headered
//!   parquet/tpch_<t>.parquet  all 8 tables, snappy (the primary tables)
//!   delta/tpch_<t>/           the 4 headline tables (_delta_log + one parquet part file)
//!   iceberg/tpch_<t>/         the 4 headline tables (metadata/ + data/)
//! ```
//!
//! Everything is written by this one binary — no Python/JVM toolchain. The lakehouse metadata
//! is RELOCATABLE by construction: Delta `add` actions and every Iceberg path (data files,
//! manifest, manifest list) are table-root-relative, so the committed tree reads identically
//! from a fresh clone, a CI checkout, or the Docker image. (pyiceberg/deltalake-spark bake
//! absolute `file://` URIs into Iceberg manifests, which is why this generator does not use
//! them.) Output is deterministic: fixed file names, UUIDs and timestamps, so regeneration
//! produces an empty git diff.

use std::path::Path;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::basic::Compression;
use datafusion::parquet::file::properties::WriterProperties;
use datafusion::parquet::file::reader::{FileReader, SerializedFileReader};
use datafusion::prelude::{CsvReadOptions, SessionContext};
use iceberg::io::FileIOBuilder;
use iceberg::spec::{
    DataContentType, DataFileBuilder, DataFileFormat, ManifestFile, ManifestListWriter,
    ManifestWriterBuilder, NestedField, PartitionSpec, PrimitiveType, Struct, Type,
};

use crate::tpch_data;

/// TPC-H scale factor for the bundled samples (~60k lineitem rows; the whole tree stays
/// well under 25 MB).
const SF: f64 = 0.01;
/// The four headline tables that also get Delta + Iceberg variants.
const HEADLINE: [&str; 4] = ["nation", "customer", "orders", "lineitem"];
/// Fixed timestamp stamped into lakehouse metadata so regeneration is byte-stable.
const FIXED_TS_MS: i64 = 1_700_000_000_000;
/// Single data file name inside each lakehouse table.
const PART_FILE: &str = "part-00000.snappy.parquet";

/// Regenerate the full tree under `dir` (usually the repo's `sample-data/`). Each phase is
/// idempotent — a phase whose final marker file exists is skipped, so reruns are cheap.
pub async fn run(dir: &Path) {
    std::fs::create_dir_all(dir).expect("create sample-data dir");
    let dir = dir
        .canonicalize()
        .unwrap_or_else(|e| panic!("sample-data dir {} not found: {e}", dir.display()));
    csv_phase(&dir);
    parquet_phase(&dir).await;
    delta_phase(&dir);
    iceberg_phase(&dir).await;
    eprintln!("[sample-data] done: {}", dir.display());
}

// ---- CSV ----------------------------------------------------------------------------------

fn csv_phase(dir: &Path) {
    let out = dir.join("csv");
    let fresh = !out.join("tpch_lineitem.csv").exists();
    tpch_data::generate_prefixed(SF, &out, "tpch_").expect("generate tpch csv");
    eprintln!(
        "[sample-data] csv/: {}",
        if fresh {
            "generated"
        } else {
            "already present, skipped"
        }
    );
}

// ---- Parquet ------------------------------------------------------------------------------

async fn parquet_phase(dir: &Path) {
    let out_dir = dir.join("parquet");
    if out_dir.join("tpch_lineitem.parquet").exists() {
        eprintln!("[sample-data] parquet/: already present, skipped");
        return;
    }
    std::fs::create_dir_all(&out_dir).expect("mkdir parquet");
    let ctx = SessionContext::new();
    for t in tpch_data::TABLES {
        let csv = dir.join("csv").join(format!("tpch_{t}.csv"));
        let schema = tpch_data::schema(t);
        let df = ctx
            .read_csv(
                csv.to_str().unwrap(),
                CsvReadOptions::new()
                    .has_header(true)
                    .schema(schema.as_ref()),
            )
            .await
            .unwrap_or_else(|e| panic!("read {}: {e}", csv.display()));
        let batches = df.collect().await.expect("collect csv batches");
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        let out = out_dir.join(format!("tpch_{t}.parquet"));
        write_snappy_parquet(&out, &schema, &batches);
        eprintln!("[sample-data] parquet/tpch_{t}.parquet: {rows} rows");
    }
}

fn write_snappy_parquet(
    path: &Path,
    schema: &SchemaRef,
    batches: &[datafusion::arrow::record_batch::RecordBatch],
) {
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let file = std::fs::File::create(path).expect("create parquet");
    let mut writer =
        ArrowWriter::try_new(file, schema.clone(), Some(props)).expect("parquet writer");
    for batch in batches {
        writer.write(batch).expect("write batch");
    }
    writer.close().expect("close parquet");
}

/// Row count from a parquet file's footer (no data pages read).
fn parquet_num_rows(path: &Path) -> u64 {
    let file = std::fs::File::open(path).expect("open parquet");
    let reader = SerializedFileReader::new(file).expect("parquet footer");
    reader.metadata().file_metadata().num_rows() as u64
}

// ---- Delta --------------------------------------------------------------------------------

fn delta_phase(dir: &Path) {
    let root = dir.join("delta");
    if root
        .join("tpch_lineitem/_delta_log/00000000000000000000.json")
        .exists()
    {
        eprintln!("[sample-data] delta/: already present, skipped");
        return;
    }
    for (i, t) in HEADLINE.iter().enumerate() {
        let table_dir = root.join(format!("tpch_{t}"));
        let log_dir = table_dir.join("_delta_log");
        std::fs::create_dir_all(&log_dir).expect("mkdir delta log");
        // The data file is byte-identical to the primary parquet table.
        std::fs::copy(
            dir.join("parquet").join(format!("tpch_{t}.parquet")),
            table_dir.join(PART_FILE),
        )
        .expect("copy delta part");
        let size = std::fs::metadata(table_dir.join(PART_FILE)).unwrap().len();
        let rows = parquet_num_rows(&table_dir.join(PART_FILE));
        let uuid = format!("00000000-0000-0000-0000-{:012}", i + 1);
        let schema_string = delta_schema_string(&tpch_data::schema(t));
        let commit = [
            serde_json::json!({
                "protocol": {"minReaderVersion": 1, "minWriterVersion": 2}
            })
            .to_string(),
            serde_json::json!({
                "metaData": {
                    "id": uuid,
                    "name": null,
                    "description": null,
                    "format": {"provider": "parquet", "options": {}},
                    "schemaString": schema_string,
                    "partitionColumns": [],
                    "configuration": {},
                    "createdTime": FIXED_TS_MS,
                }
            })
            .to_string(),
            serde_json::json!({
                "add": {
                    // Table-root-relative path: what makes the committed tree relocatable.
                    "path": PART_FILE,
                    "partitionValues": {},
                    "size": size,
                    "modificationTime": FIXED_TS_MS,
                    "dataChange": true,
                    "stats": serde_json::json!({"numRecords": rows}).to_string(),
                }
            })
            .to_string(),
        ]
        .join("\n");
        std::fs::write(
            log_dir.join("00000000000000000000.json"),
            format!("{commit}\n"),
        )
        .expect("write delta commit");
        eprintln!("[sample-data] delta/tpch_{t}: {rows} rows");
    }
}

/// Spark JSON schema (Delta's `schemaString`) for the table's Arrow schema.
fn delta_schema_string(schema: &SchemaRef) -> String {
    fn spark_type(dt: &DataType) -> serde_json::Value {
        match dt {
            DataType::Int64 => serde_json::json!("long"),
            DataType::Int32 => serde_json::json!("integer"),
            DataType::Utf8 => serde_json::json!("string"),
            DataType::Decimal128(p, s) => serde_json::json!(format!("decimal({p},{s})")),
            DataType::Date32 => serde_json::json!("date"),
            other => panic!("sample-data: no Spark type mapping for {other}"),
        }
    }
    let fields: Vec<serde_json::Value> = schema
        .fields()
        .iter()
        .map(|f| {
            serde_json::json!({
                "name": f.name(),
                "type": spark_type(f.data_type()),
                "nullable": f.is_nullable(),
                "metadata": {},
            })
        })
        .collect();
    serde_json::json!({"type": "struct", "fields": fields}).to_string()
}

// ---- Iceberg ------------------------------------------------------------------------------

/// Iceberg v2 schema for the table's Arrow schema, with field ids assigned 1..=N in column
/// order (what `iceberg::arrow::arrow_schema_to_schema_auto_assign_ids` would produce for a
/// flat schema, without crossing the Arrow-version boundary).
fn iceberg_schema(schema: &SchemaRef) -> iceberg::spec::Schema {
    fn iceberg_type(dt: &DataType) -> Type {
        let primitive = match dt {
            DataType::Int64 => PrimitiveType::Long,
            DataType::Int32 => PrimitiveType::Int,
            DataType::Utf8 => PrimitiveType::String,
            DataType::Decimal128(p, s) => PrimitiveType::Decimal {
                precision: *p as u32,
                scale: *s as u32,
            },
            DataType::Date32 => PrimitiveType::Date,
            other => panic!("sample-data: no Iceberg type mapping for {other}"),
        };
        Type::Primitive(primitive)
    }
    let fields: Vec<Arc<NestedField>> = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, f)| {
            let id = (i + 1) as i32;
            let ty = iceberg_type(f.data_type());
            Arc::new(if f.is_nullable() {
                NestedField::optional(id, f.name(), ty)
            } else {
                NestedField::required(id, f.name(), ty)
            })
        })
        .collect();
    iceberg::spec::Schema::builder()
        .with_schema_id(0)
        .with_fields(fields)
        .build()
        .expect("iceberg schema")
}

async fn iceberg_phase(dir: &Path) {
    let root = dir.join("iceberg");
    if root
        .join("tpch_lineitem/metadata/v1.metadata.json")
        .exists()
    {
        eprintln!("[sample-data] iceberg/: already present, skipped");
        return;
    }
    let file_io = FileIOBuilder::new_fs_io().build().expect("iceberg fs io");
    for (i, t) in HEADLINE.iter().enumerate() {
        let table_dir = root.join(format!("tpch_{t}"));
        let metadata_dir = table_dir.join("metadata");
        std::fs::create_dir_all(table_dir.join("data")).expect("mkdir data");
        std::fs::create_dir_all(&metadata_dir).expect("mkdir metadata");
        std::fs::copy(
            dir.join("parquet").join(format!("tpch_{t}.parquet")),
            table_dir.join("data").join(PART_FILE),
        )
        .expect("copy iceberg part");
        let data_file_abs = table_dir.join("data").join(PART_FILE);
        let size = std::fs::metadata(&data_file_abs).unwrap().len();
        let rows = parquet_num_rows(&data_file_abs);

        // Iceberg schema from the Arrow schema (field ids assigned 1..=N). Hand-mapped because
        // iceberg-rust 0.8 pins a different Arrow version than DataFusion — its `arrow::arrow_
        // schema_to_schema` helper takes a foreign `Schema` type (same coupling tpch_data.rs
        // avoids by going through CSV).
        let schema = Arc::new(iceberg_schema(&tpch_data::schema(t)));
        let spec = PartitionSpec::builder(schema.clone())
            .with_spec_id(0)
            .build()
            .expect("unpartitioned spec");

        // Every path recorded in metadata is table-root-relative (relocatable tree).
        let data_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(format!("data/{PART_FILE}"))
            .file_format(DataFileFormat::Parquet)
            .partition(Struct::empty())
            .partition_spec_id(0)
            .record_count(rows)
            .file_size_in_bytes(size)
            .build()
            .expect("data file");

        let manifest_rel = "metadata/data-manifest-0.avro";
        let mut manifest_writer = ManifestWriterBuilder::new(
            file_io
                .new_output(table_dir.join(manifest_rel).to_str().unwrap())
                .expect("manifest output"),
            Some(1),
            None,
            schema.clone(),
            spec.clone(),
        )
        .build_v2_data();
        manifest_writer.add_file(data_file, 0).expect("add file");
        let written = manifest_writer
            .write_manifest_file()
            .await
            .expect("write manifest");
        // FileIO needs an absolute path to write to, but the path RECORDED in the manifest
        // list must stay table-root-relative.
        let manifest = ManifestFile {
            manifest_path: manifest_rel.to_string(),
            ..written
        };

        let manifest_list_rel = "metadata/snap-1.avro";
        let mut manifest_list = ManifestListWriter::v2(
            file_io
                .new_output(table_dir.join(manifest_list_rel).to_str().unwrap())
                .expect("manifest list output"),
            1,
            None,
            0,
        );
        manifest_list
            .add_manifests(vec![manifest].into_iter())
            .expect("add manifest");
        manifest_list.close().await.expect("close manifest list");

        let uuid = format!("00000000-0000-0000-0000-{:012}", i + 1);
        let metadata = serde_json::json!({
            "format-version": 2,
            "table-uuid": uuid,
            "location": ".",
            "last-sequence-number": 0,
            "last-updated-ms": FIXED_TS_MS,
            "last-column-id": schema.highest_field_id(),
            "current-schema-id": 0,
            "schemas": [serde_json::to_value(schema.as_ref()).expect("schema json")],
            "default-spec-id": 0,
            "partition-specs": [{"spec-id": 0, "fields": []}],
            "last-partition-id": 999,
            "properties": {},
            "current-snapshot-id": 1,
            "snapshots": [{
                "snapshot-id": 1,
                "sequence-number": 0,
                "timestamp-ms": FIXED_TS_MS,
                "summary": {"operation": "append"},
                "manifest-list": manifest_list_rel,
                "schema-id": 0,
            }],
            "snapshot-log": [{"snapshot-id": 1, "timestamp-ms": FIXED_TS_MS}],
            "metadata-log": [],
            "sort-orders": [{"order-id": 0, "fields": []}],
            "default-sort-order-id": 0,
            "refs": {"main": {"snapshot-id": 1, "type": "branch"}},
        });
        std::fs::write(
            metadata_dir.join("v1.metadata.json"),
            serde_json::to_string_pretty(&metadata).unwrap(),
        )
        .expect("write metadata.json");
        std::fs::write(metadata_dir.join("version-hint.text"), "1").expect("write version hint");
        eprintln!("[sample-data] iceberg/tpch_{t}: {rows} rows");
    }
}

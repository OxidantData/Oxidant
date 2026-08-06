use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{Int32Array, Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::arrow::ArrowWriter;
use delta_kernel::Snapshot;
use delta_kernel_default_engine::{
    executor::tokio::TokioMultiThreadExecutor, DefaultEngineBuilder,
};
use iceberg::io::FileIOBuilder;
use iceberg::spec::{
    DataContentType, DataFileBuilder, DataFileFormat, ManifestListWriter, ManifestWriterBuilder,
    NestedField, PartitionSpec, PrimitiveType, Schema as IcebergSchema, Struct, Type,
};
use object_store::aws::AmazonS3Builder;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt};
use oxidant_catalog::{CatalogProvider, Error, Result, TableFormat, TableMetadata};
use oxidant_loom::Engine;
use tempfile::TempDir;

const BUCKET: &str = "oxidant-test";

#[derive(Debug)]
struct FixtureCatalog {
    tables: HashMap<String, TableMetadata>,
}

#[async_trait]
impl CatalogProvider for FixtureCatalog {
    fn name(&self) -> &str {
        "minio"
    }

    async fn list_namespaces(&self, _parent: &[String]) -> Result<Vec<Vec<String>>> {
        Ok(vec![vec!["db".into()]])
    }

    async fn list_tables(&self, namespace: &[String]) -> Result<Vec<String>> {
        if namespace == ["db"] {
            Ok(self.tables.keys().cloned().collect())
        } else {
            Ok(Vec::new())
        }
    }

    async fn load_table(&self, namespace: &[String], table: &str) -> Result<TableMetadata> {
        if namespace != ["db"] {
            return Err(Error::Plan(format!("unknown namespace {namespace:?}")));
        }
        self.tables
            .get(table)
            .cloned()
            .ok_or_else(|| Error::Plan(format!("unknown fixture table `{table}`")))
    }
}

fn minio_enabled() -> bool {
    std::env::var("OXIDANT_MINIO_TEST")
        .ok()
        .is_some_and(|value| value == "1" || value.eq_ignore_ascii_case("true"))
}

fn endpoint() -> String {
    std::env::var("OXIDANT_MINIO_ENDPOINT").unwrap_or_else(|_| "http://127.0.0.1:9000".into())
}

fn storage_options() -> HashMap<String, String> {
    HashMap::from([
        ("s3.endpoint".into(), endpoint()),
        ("s3.access-key-id".into(), "minioadmin".into()),
        ("s3.secret-access-key".into(), "minioadmin123".into()),
        ("s3.region".into(), "us-east-1".into()),
        ("s3.allow-http".into(), "true".into()),
    ])
}

fn minio_store() -> Arc<dyn ObjectStore> {
    Arc::new(
        AmazonS3Builder::new()
            .with_bucket_name(BUCKET)
            .with_region("us-east-1")
            .with_endpoint(endpoint())
            .with_access_key_id("minioadmin")
            .with_secret_access_key("minioadmin123")
            .with_allow_http(true)
            .build()
            .expect("build MinIO object store"),
    )
}

fn write_parquet(path: &Path, batch: &RecordBatch) -> u64 {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    let file = std::fs::File::create(path).unwrap();
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None).unwrap();
    writer.write(batch).unwrap();
    writer.close().unwrap();
    std::fs::metadata(path).unwrap().len()
}

async fn upload_tree(store: &Arc<dyn ObjectStore>, root: &Path, prefix: &str) {
    let mut pending = vec![root.to_path_buf()];
    while let Some(directory) = pending.pop() {
        for entry in std::fs::read_dir(&directory).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            if path.is_dir() {
                pending.push(path);
                continue;
            }
            let relative = path.strip_prefix(root).unwrap().to_string_lossy();
            let object = ObjectPath::from(format!(
                "{}/{}",
                prefix.trim_matches('/'),
                relative.replace('\\', "/")
            ));
            store
                .put(&object, std::fs::read(&path).unwrap().into())
                .await
                .unwrap();
        }
    }
}

fn delta_metadata_lines() -> String {
    let schema = serde_json::json!({
        "type": "struct",
        "fields": [{
            "name": "id",
            "type": "long",
            "nullable": false,
            "metadata": {}
        }]
    });
    [
        serde_json::json!({
            "protocol": {
                "minReaderVersion": 3,
                "minWriterVersion": 7,
                "readerFeatures": ["deletionVectors"],
                "writerFeatures": ["deletionVectors"]
            }
        })
        .to_string(),
        serde_json::json!({
            "metaData": {
                "id": "00000000-0000-0000-0000-000000000001",
                "name": null,
                "description": null,
                "format": {"provider": "parquet", "options": {}},
                "schemaString": schema.to_string(),
                "partitionColumns": [],
                "configuration": {},
                "createdTime": 1
            }
        })
        .to_string(),
    ]
    .join("\n")
}

async fn write_delta_fixture(root: &Path) {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from((0_i64..30).collect::<Vec<_>>()))],
    )
    .unwrap();
    let file_size = write_parquet(&root.join("data.parquet"), &batch);
    let deletion_vector = serde_json::json!({
        "storageType": "i",
        "pathOrInlineDv": "^Bg9^0rr910000000000iXQKl0rr91000f55c8Xg0@@D72lkbi5=-{L",
        "sizeInBytes": 44,
        "cardinality": 6
    });
    let add = serde_json::json!({
        "add": {
            "path": "data.parquet",
            "partitionValues": {},
            "size": file_size,
            "modificationTime": 1,
            "dataChange": true,
            "stats": "{\"numRecords\":30}",
            "deletionVector": deletion_vector
        }
    })
    .to_string();
    let log = root.join("_delta_log");
    std::fs::create_dir_all(&log).unwrap();
    std::fs::write(
        log.join("00000000000000000000.json"),
        format!("{}\n{add}\n", delta_metadata_lines()),
    )
    .unwrap();

    let table_url = format!("file://{}", root.display());
    let executor = Arc::new(TokioMultiThreadExecutor::new(
        tokio::runtime::Handle::current(),
    ));
    tokio::task::spawn_blocking(move || {
        let engine = DefaultEngineBuilder::new(Arc::new(LocalFileSystem::new()))
            .with_task_executor(executor)
            .build();
        let snapshot = Snapshot::builder_for(&table_url).build(&engine).unwrap();
        snapshot.checkpoint(&engine, None).unwrap();
    })
    .await
    .unwrap();
    std::fs::remove_file(log.join("00000000000000000000.json")).unwrap();
}

async fn write_iceberg_fixture(root: &Path, s3_root: &str) -> PathBuf {
    let metadata_dir = root.join("metadata");
    std::fs::create_dir_all(&metadata_dir).unwrap();
    let data_path = root.join("data.parquet");
    let delete_path = root.join("position-deletes.parquet");
    let data_uri = format!("{s3_root}/data.parquet");
    let delete_uri = format!("{s3_root}/position-deletes.parquet");

    let data_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Utf8, true),
    ]));
    let data_batch = RecordBatch::try_new(
        data_schema,
        vec![
            Arc::new(Int64Array::from(vec![10, 20, 30, 40])),
            Arc::new(StringArray::from(vec!["a", "b", "c", "d"])),
        ],
    )
    .unwrap();
    let data_size = write_parquet(&data_path, &data_batch);

    let delete_schema = Arc::new(Schema::new(vec![
        Field::new("file_path", DataType::Utf8, false),
        Field::new("pos", DataType::Int64, false),
    ]));
    let delete_batch = RecordBatch::try_new(
        delete_schema,
        vec![
            Arc::new(StringArray::from(vec![
                data_uri.as_str(),
                data_uri.as_str(),
            ])),
            Arc::new(Int64Array::from(vec![1, 3])),
        ],
    )
    .unwrap();
    let delete_size = write_parquet(&delete_path, &delete_batch);

    let file_io = FileIOBuilder::new_fs_io().build().unwrap();
    let schema = Arc::new(
        IcebergSchema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                Arc::new(NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Long),
                )),
                Arc::new(NestedField::optional(
                    2,
                    "value",
                    Type::Primitive(PrimitiveType::String),
                )),
            ])
            .build()
            .unwrap(),
    );
    let spec = PartitionSpec::builder(schema.clone())
        .with_spec_id(0)
        .build()
        .unwrap();
    let data_file = DataFileBuilder::default()
        .content(DataContentType::Data)
        .file_path(data_uri.clone())
        .file_format(DataFileFormat::Parquet)
        .partition(Struct::empty())
        .partition_spec_id(0)
        .record_count(4)
        .file_size_in_bytes(data_size)
        .build()
        .unwrap();
    let position_delete = DataFileBuilder::default()
        .content(DataContentType::PositionDeletes)
        .file_path(delete_uri)
        .file_format(DataFileFormat::Parquet)
        .partition(Struct::empty())
        .partition_spec_id(0)
        .record_count(2)
        .file_size_in_bytes(delete_size)
        .referenced_data_file(Some(data_uri))
        .build()
        .unwrap();

    let data_manifest_path = metadata_dir.join("data-manifest.avro");
    let mut data_writer = ManifestWriterBuilder::new(
        file_io
            .new_output(data_manifest_path.to_str().unwrap())
            .unwrap(),
        Some(42),
        None,
        schema.clone(),
        spec.clone(),
    )
    .build_v2_data();
    data_writer.add_file(data_file, 1).unwrap();
    let mut data_manifest = data_writer.write_manifest_file().await.unwrap();
    data_manifest.manifest_path = format!("{s3_root}/metadata/data-manifest.avro");

    let delete_manifest_path = metadata_dir.join("delete-manifest.avro");
    let mut delete_writer = ManifestWriterBuilder::new(
        file_io
            .new_output(delete_manifest_path.to_str().unwrap())
            .unwrap(),
        Some(42),
        None,
        schema,
        spec,
    )
    .build_v2_deletes();
    delete_writer.add_file(position_delete, 2).unwrap();
    let mut delete_manifest = delete_writer.write_manifest_file().await.unwrap();
    delete_manifest.manifest_path = format!("{s3_root}/metadata/delete-manifest.avro");

    let manifest_list_path = metadata_dir.join("snap-42.avro");
    let mut manifest_list = ManifestListWriter::v2(
        file_io
            .new_output(manifest_list_path.to_str().unwrap())
            .unwrap(),
        42,
        None,
        2,
    );
    manifest_list
        .add_manifests(vec![data_manifest, delete_manifest].into_iter())
        .unwrap();
    manifest_list.close().await.unwrap();

    let metadata_path = metadata_dir.join("00001-fixture.metadata.json");
    let metadata = serde_json::json!({
        "format-version": 2,
        "table-uuid": "00000000-0000-0000-0000-000000000042",
        "location": s3_root,
        "last-sequence-number": 2,
        "last-updated-ms": 2,
        "last-column-id": 2,
        "current-schema-id": 0,
        "schemas": [{
            "type": "struct",
            "schema-id": 0,
            "fields": [
                {"id": 1, "name": "id", "required": true, "type": "long"},
                {"id": 2, "name": "value", "required": false, "type": "string"}
            ]
        }],
        "default-spec-id": 0,
        "partition-specs": [{"spec-id": 0, "fields": []}],
        "last-partition-id": 999,
        "properties": {},
        "current-snapshot-id": 42,
        "snapshots": [{
            "snapshot-id": 42,
            "sequence-number": 2,
            "timestamp-ms": 2,
            "summary": {"operation": "delete"},
            "manifest-list": format!("{s3_root}/metadata/snap-42.avro"),
            "schema-id": 0
        }],
        "snapshot-log": [{"snapshot-id": 42, "timestamp-ms": 2}],
        "metadata-log": [],
        "sort-orders": [{"order-id": 0, "fields": []}],
        "default-sort-order-id": 0,
        "refs": {"main": {"snapshot-id": 42, "type": "branch"}}
    });
    std::fs::write(&metadata_path, serde_json::to_vec(&metadata).unwrap()).unwrap();
    metadata_path
}

fn int64_values(batches: &[RecordBatch], column: usize) -> Vec<i64> {
    batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(column)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .iter()
                .copied()
                .collect::<Vec<_>>()
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn minio_parquet_iceberg_and_delta_are_correct() {
    if !minio_enabled() {
        eprintln!("skipping MinIO lakehouse integration test; set OXIDANT_MINIO_TEST=1");
        return;
    }

    let fixture = TempDir::new().unwrap();
    let run = format!(
        "lakehouse-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let store = minio_store();

    let plain = fixture.path().join("plain");
    let plain_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let rows_2024 = RecordBatch::try_new(
        plain_schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1, 2]))],
    )
    .unwrap();
    let rows_2025 =
        RecordBatch::try_new(plain_schema, vec![Arc::new(Int64Array::from(vec![3]))]).unwrap();
    write_parquet(&plain.join("year=2024/part.parquet"), &rows_2024);
    write_parquet(&plain.join("year=2025/part.parquet"), &rows_2025);
    upload_tree(&store, &plain, &format!("{run}/plain")).await;

    let delta = fixture.path().join("delta");
    std::fs::create_dir_all(&delta).unwrap();
    write_delta_fixture(&delta).await;
    upload_tree(&store, &delta, &format!("{run}/delta")).await;

    let iceberg = fixture.path().join("iceberg");
    std::fs::create_dir_all(&iceberg).unwrap();
    let iceberg_root = format!("s3://{BUCKET}/{run}/iceberg");
    let iceberg_metadata = write_iceberg_fixture(&iceberg, &iceberg_root).await;
    upload_tree(&store, &iceberg, &format!("{run}/iceberg")).await;

    let opts = storage_options();
    let plain_table_schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("year", DataType::Int32, false),
    ]));
    let iceberg_schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Utf8, true),
    ]));
    let delta_schema: SchemaRef =
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let tables = HashMap::from([
        (
            "plain".into(),
            TableMetadata::new(
                "minio.db.plain",
                format!("s3://{BUCKET}/{run}/plain"),
                TableFormat::Parquet,
            )
            .with_schema(plain_table_schema)
            .with_partition_columns(vec!["year".into()])
            .with_storage_options(opts.clone()),
        ),
        (
            "iceberg".into(),
            TableMetadata::new("minio.db.iceberg", iceberg_root, TableFormat::Iceberg)
                .with_schema(iceberg_schema)
                .with_properties(HashMap::from([(
                    "metadata_location".into(),
                    format!(
                        "s3://{BUCKET}/{run}/iceberg/{}",
                        iceberg_metadata
                            .strip_prefix(&iceberg)
                            .unwrap()
                            .to_string_lossy()
                    ),
                )]))
                .with_storage_options(opts.clone()),
        ),
        (
            "delta".into(),
            TableMetadata::new(
                "minio.db.delta",
                format!("s3://{BUCKET}/{run}/delta"),
                TableFormat::Delta,
            )
            .with_schema(delta_schema)
            .with_storage_options(opts),
        ),
    ]);

    let engine = Engine::new();
    engine.register_catalog("minio", Arc::new(FixtureCatalog { tables }));

    let parquet = engine
        .sql("SELECT id, year FROM minio.db.plain WHERE year = 2024 ORDER BY id")
        .await
        .unwrap();
    assert_eq!(int64_values(&parquet, 0), vec![1, 2]);
    let years = parquet
        .iter()
        .flat_map(|batch| {
            batch
                .column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .iter()
                .copied()
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert_eq!(years, vec![2024, 2024]);

    let iceberg_rows = engine
        .sql("SELECT id FROM minio.db.iceberg ORDER BY id")
        .await
        .unwrap();
    assert_eq!(int64_values(&iceberg_rows, 0), vec![10, 30]);

    let delta_rows = engine
        .sql("SELECT id FROM minio.db.delta ORDER BY id")
        .await
        .unwrap();
    let expected = (0_i64..30)
        .filter(|value| ![3, 4, 7, 11, 18, 29].contains(value))
        .collect::<Vec<_>>();
    assert_eq!(int64_values(&delta_rows, 0), expected);
}

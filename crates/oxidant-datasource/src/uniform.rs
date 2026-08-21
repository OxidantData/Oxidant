//! Universal format: publish Iceberg metadata over a Delta table's own data files.
//!
//! A live table is written once and read by whatever a team already runs — Athena, Trino, Spark,
//! Snowflake, DuckDB. Those engines do not all speak the same table format, and the usual answer
//! is a second copy of the data, which doubles storage and guarantees the two copies disagree
//! about what "now" means.
//!
//! They do not have to disagree. Delta and Iceberg are both *metadata over Parquet*: the data
//! files are ordinary Parquet either way, and only the description of which files are live
//! differs. So this module writes a second metadata tree — Iceberg manifests, a manifest list, and
//! a `metadata.json` — pointing at the exact same Parquet objects the Delta log already lists.
//! One copy of the bytes, two catalogs' worth of readers. This is the same trick as Delta
//! UniForm, and the Delta table advertises it through `delta.universalFormat.enabledFormats`.
//!
//! ```text
//!                     part-00000-….parquet   part-00001-….parquet     ← written once
//!                            ▲                        ▲
//!         _delta_log/*.json ─┘                        └─ metadata/*.avro + v_N.metadata.json
//!         (Delta readers)                                (Iceberg readers)
//! ```
//!
//! **Field IDs.** Iceberg normally resolves columns by field id stamped into the Parquet footer.
//! Files written for Delta carry no such ids, so the published metadata sets
//! `schema.name-mapping.default`, which is exactly how Iceberg is specified to fall back to
//! matching by column name. Without it an Iceberg reader opens the table and returns all nulls —
//! the failure mode this module exists to avoid.
//!
//! **Freshness.** Publishing is deliberately not per-commit: the Iceberg tree describes the table
//! as of the Delta version it was generated from, so Iceberg readers trail Delta readers by at
//! most one publish interval. Databricks' UniForm behaves the same way, for the same reason —
//! rewriting a manifest per micro-batch would cost more than the data write.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt};
use oxidant_common::{Error, Result};

use crate::delta_write::DeltaAddFile;

/// Delta table property that tells other engines Iceberg metadata is published alongside the log.
pub const UNIVERSAL_FORMAT_KEY: &str = "delta.universalFormat.enabledFormats";
pub const UNIVERSAL_FORMAT_ICEBERG: &str = "iceberg";

/// Hive's sentinel for a null partition value, which Delta writes and Iceberg reads as null.
const NULL_PARTITION: &str = "__HIVE_DEFAULT_PARTITION__";

/// An Iceberg view of a Delta table: schema, partitioning, and identity, resolved once.
pub struct UniformTable {
    /// Absolute table location URI (`s3://bucket/db/table`, `file:///data/table`).
    location: String,
    schema: Arc<iceberg::spec::Schema>,
    spec: iceberg::spec::PartitionSpec,
    partition_columns: Vec<String>,
    table_uuid: String,
}

impl UniformTable {
    /// Resolve the Iceberg view of a Delta table, or explain why the schema cannot have one.
    ///
    /// Called at query start so an unmappable column fails where the user is looking, rather than
    /// silently leaving the Iceberg side of the table stale forever.
    pub fn new(
        location: &str,
        schema: &SchemaRef,
        partition_columns: &[String],
        table_uuid: &str,
    ) -> Result<Self> {
        let iceberg_schema = Arc::new(iceberg_schema(schema)?);
        let mut builder =
            iceberg::spec::PartitionSpec::builder(iceberg_schema.clone()).with_spec_id(0);
        for column in partition_columns {
            builder = builder
                .add_partition_field(column, column, iceberg::spec::Transform::Identity)
                .map_err(|e| {
                    Error::Unsupported(format!(
                        "uniform: `{column}` cannot be an Iceberg partition field: {e}"
                    ))
                })?;
        }
        let spec = builder
            .build()
            .map_err(|e| Error::Unsupported(format!("uniform: partition spec: {e}")))?;

        Ok(Self {
            location: absolute_location(location),
            schema: iceberg_schema,
            spec,
            partition_columns: partition_columns.to_vec(),
            table_uuid: table_uuid.to_string(),
        })
    }

    /// Publish an Iceberg snapshot describing `files`, and return its `metadata.json` location.
    ///
    /// `delta_version` is the Delta commit this snapshot mirrors; it becomes the Iceberg snapshot
    /// id and the metadata file's version, so the two trees stay legibly in step and republishing
    /// the same Delta version is idempotent.
    pub async fn publish(
        &self,
        store: &dyn ObjectStore,
        root: &ObjectPath,
        files: &[DeltaAddFile],
        delta_version: u64,
    ) -> Result<String> {
        use iceberg::io::FileIOBuilder;
        use iceberg::spec::{
            DataContentType, DataFileBuilder, DataFileFormat, ManifestFile, ManifestListWriter,
            ManifestWriterBuilder,
        };

        // iceberg-rust writes Avro through its own FileIO, which this build only has a local
        // filesystem backend for. Rather than give it a second set of cloud credentials — a
        // different auth path from every other write the engine makes — the manifests are built
        // in a scratch directory and uploaded through the same ObjectStore as the data.
        let scratch = tempfile::TempDir::new()
            .map_err(|e| Error::Io(format!("uniform: scratch dir: {e}")))?;
        let file_io = FileIOBuilder::new_fs_io()
            .build()
            .map_err(|e| Error::Io(format!("uniform: file io: {e}")))?;

        let snapshot_id = (delta_version + 1) as i64;
        let sequence_number = snapshot_id;

        let mut data_files = Vec::with_capacity(files.len());
        for file in files {
            data_files.push(
                DataFileBuilder::default()
                    .content(DataContentType::Data)
                    // An absolute URI, not a table-relative path: an Iceberg reader resolves data
                    // files without knowing where the table root is.
                    .file_path(format!("{}/{}", self.location, file.path))
                    .file_format(DataFileFormat::Parquet)
                    .partition(self.partition_struct(&file.partition_values)?)
                    .partition_spec_id(0)
                    .record_count(file.num_records)
                    .file_size_in_bytes(file.size)
                    .build()
                    .map_err(|e| Error::Execution(format!("uniform: data file: {e}")))?,
            );
        }

        let manifest_name = format!("{snapshot_id:020}-m0.avro");
        let manifest_local = scratch.path().join(&manifest_name);
        let mut manifest_writer = ManifestWriterBuilder::new(
            file_io
                .new_output(manifest_local.to_string_lossy().as_ref())
                .map_err(|e| Error::Io(format!("uniform: manifest output: {e}")))?,
            Some(snapshot_id),
            None,
            self.schema.clone(),
            self.spec.clone(),
        )
        .build_v2_data();
        for data_file in data_files {
            manifest_writer
                .add_file(data_file, sequence_number)
                .map_err(|e| Error::Execution(format!("uniform: add manifest entry: {e}")))?;
        }
        let written = manifest_writer
            .write_manifest_file()
            .await
            .map_err(|e| Error::Io(format!("uniform: write manifest: {e}")))?;

        let manifest_uri = format!("{}/metadata/{manifest_name}", self.location);
        self.upload(
            store,
            root,
            &manifest_local,
            &format!("metadata/{manifest_name}"),
        )
        .await?;

        // The manifest list records where the manifest *ended up*, not where it was staged.
        let manifest = ManifestFile {
            manifest_path: manifest_uri,
            ..written
        };

        let list_name = format!("snap-{snapshot_id}-1-{}.avro", self.table_uuid);
        let list_local = scratch.path().join(&list_name);
        let mut manifest_list = ManifestListWriter::v2(
            file_io
                .new_output(list_local.to_string_lossy().as_ref())
                .map_err(|e| Error::Io(format!("uniform: manifest list output: {e}")))?,
            snapshot_id,
            None,
            sequence_number,
        );
        manifest_list
            .add_manifests(std::iter::once(manifest))
            .map_err(|e| Error::Execution(format!("uniform: add manifest: {e}")))?;
        manifest_list
            .close()
            .await
            .map_err(|e| Error::Io(format!("uniform: close manifest list: {e}")))?;
        self.upload(store, root, &list_local, &format!("metadata/{list_name}"))
            .await?;

        let metadata = self.table_metadata(snapshot_id, sequence_number, &list_name, files)?;
        let metadata_name = format!("v{}.metadata.json", delta_version + 1);
        let body = serde_json::to_string_pretty(&metadata)
            .map_err(|e| Error::Execution(format!("uniform: metadata json: {e}")))?;
        put(
            store,
            root,
            &format!("metadata/{metadata_name}"),
            body.into_bytes(),
        )
        .await?;

        // `version-hint.text` is how a catalog-less Iceberg reader (Trino's hadoop catalog,
        // DuckDB's iceberg extension) finds the current metadata file.
        put(
            store,
            root,
            "metadata/version-hint.text",
            (delta_version + 1).to_string().into_bytes(),
        )
        .await?;

        Ok(format!("{}/metadata/{metadata_name}", self.location))
    }

    /// Iceberg v2 table metadata for one snapshot.
    fn table_metadata(
        &self,
        snapshot_id: i64,
        sequence_number: i64,
        manifest_list: &str,
        files: &[DeltaAddFile],
    ) -> Result<serde_json::Value> {
        let now_ms = chrono::Utc::now().timestamp_millis();
        let rows: u64 = files.iter().map(|f| f.num_records).sum();
        let bytes: u64 = files.iter().map(|f| f.size).sum();

        Ok(serde_json::json!({
            "format-version": 2,
            "table-uuid": self.table_uuid,
            "location": self.location,
            "last-sequence-number": sequence_number,
            "last-updated-ms": now_ms,
            "last-column-id": self.schema.highest_field_id(),
            "current-schema-id": 0,
            "schemas": [serde_json::to_value(self.schema.as_ref())
                .map_err(|e| Error::Execution(format!("uniform: schema json: {e}")))?],
            "default-spec-id": 0,
            "partition-specs": [serde_json::to_value(&self.spec)
                .map_err(|e| Error::Execution(format!("uniform: spec json: {e}")))?],
            "last-partition-id": 1000 + self.partition_columns.len().max(1) as i32 - 1,
            "properties": {
                // The whole reason an Iceberg reader can make sense of Delta's data files: they
                // carry no Iceberg field ids, so columns are resolved by name instead.
                "schema.name-mapping.default": self.name_mapping()?,
                "write.format.default": "parquet",
                "created-by": "oxidant-uniform",
            },
            "current-snapshot-id": snapshot_id,
            "snapshots": [{
                "snapshot-id": snapshot_id,
                "sequence-number": sequence_number,
                "timestamp-ms": now_ms,
                "summary": {
                    "operation": "append",
                    "total-records": rows.to_string(),
                    "total-files-size": bytes.to_string(),
                    "total-data-files": files.len().to_string(),
                },
                "manifest-list": format!("{}/metadata/{manifest_list}", self.location),
                "schema-id": 0,
            }],
            "snapshot-log": [{"snapshot-id": snapshot_id, "timestamp-ms": now_ms}],
            "metadata-log": [],
            "sort-orders": [{"order-id": 0, "fields": []}],
            "default-sort-order-id": 0,
            "refs": {"main": {"snapshot-id": snapshot_id, "type": "branch"}},
        }))
    }

    /// Iceberg's `schema.name-mapping.default`: field id → the column name(s) it may be read
    /// under. This is what makes field-id-less Parquet readable as Iceberg.
    fn name_mapping(&self) -> Result<String> {
        let fields: Vec<serde_json::Value> = self
            .schema
            .as_struct()
            .fields()
            .iter()
            .map(|f| serde_json::json!({"field-id": f.id, "names": [f.name]}))
            .collect();
        serde_json::to_string(&fields)
            .map_err(|e| Error::Execution(format!("uniform: name mapping: {e}")))
    }

    /// Delta's string partition values, converted to the Iceberg literals the manifest expects.
    fn partition_struct(&self, values: &BTreeMap<String, String>) -> Result<iceberg::spec::Struct> {
        use iceberg::spec::{Literal, PrimitiveType, Struct, Type};

        if self.partition_columns.is_empty() {
            return Ok(Struct::empty());
        }
        let mut literals = Vec::with_capacity(self.partition_columns.len());
        for column in &self.partition_columns {
            let field = self
                .schema
                .field_by_name(column)
                .ok_or_else(|| Error::Plan(format!("uniform: no partition column `{column}`")))?;
            // Delta's Hive sentinel for a null partition value, and a genuinely absent value,
            // both mean "null" to Iceberg.
            let literal = match values.get(column) {
                None => None,
                Some(v) if v == NULL_PARTITION => None,
                Some(v) => Some(match field.field_type.as_ref() {
                    Type::Primitive(PrimitiveType::String) => Literal::string(v),
                    Type::Primitive(PrimitiveType::Int) => Literal::int(
                        v.parse::<i32>()
                            .map_err(|_| partition_parse_error(column, v))?,
                    ),
                    Type::Primitive(PrimitiveType::Long) => Literal::long(
                        v.parse::<i64>()
                            .map_err(|_| partition_parse_error(column, v))?,
                    ),
                    Type::Primitive(PrimitiveType::Boolean) => Literal::bool(
                        v.parse::<bool>()
                            .map_err(|_| partition_parse_error(column, v))?,
                    ),
                    Type::Primitive(PrimitiveType::Date) => {
                        let date = chrono::NaiveDate::parse_from_str(v, "%Y-%m-%d")
                            .map_err(|_| partition_parse_error(column, v))?;
                        Literal::date(
                            (date - chrono::NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch"))
                                .num_days() as i32,
                        )
                    }
                    other => {
                        return Err(Error::Unsupported(format!(
                            "uniform: partition column `{column}` is {other}, which has no \
                             Iceberg partition literal — partition on a string, integer, boolean, \
                             or date column"
                        )))
                    }
                }),
            };
            literals.push(literal);
        }
        Ok(Struct::from_iter(literals))
    }

    async fn upload(
        &self,
        store: &dyn ObjectStore,
        root: &ObjectPath,
        local: &std::path::Path,
        relative: &str,
    ) -> Result<()> {
        let bytes = std::fs::read(local)
            .map_err(|e| Error::Io(format!("uniform: read {}: {e}", local.display())))?;
        put(store, root, relative, bytes).await
    }
}

fn partition_parse_error(column: &str, value: &str) -> Error {
    Error::Execution(format!(
        "uniform: partition value `{value}` for `{column}` does not parse as its column type"
    ))
}

async fn put(
    store: &dyn ObjectStore,
    root: &ObjectPath,
    relative: &str,
    bytes: Vec<u8>,
) -> Result<()> {
    let mut path = root.clone();
    for part in relative.split('/').filter(|s| !s.is_empty()) {
        path = path.join(part);
    }
    store
        .put(&path, bytes.into())
        .await
        .map_err(|e| Error::Io(format!("uniform: write `{path}`: {e}")))?;
    Ok(())
}

/// Iceberg records absolute URIs. A bare filesystem path is not one, so it is made into a
/// `file://` URI rather than written as-is, which no other engine would resolve.
fn absolute_location(location: &str) -> String {
    let trimmed = location.trim_end_matches('/');
    if trimmed.contains("://") {
        trimmed.to_string()
    } else {
        format!("file://{trimmed}")
    }
}

/// Arrow schema → Iceberg v2 schema, field ids assigned 1..=N in column order.
///
/// The ids must line up with the name mapping this module publishes, which is why they are
/// assigned here rather than taken from anywhere else.
fn iceberg_schema(schema: &SchemaRef) -> Result<iceberg::spec::Schema> {
    use iceberg::spec::{NestedField, PrimitiveType, Type};

    let mut fields = Vec::with_capacity(schema.fields().len());
    for (index, field) in schema.fields().iter().enumerate() {
        let primitive = match field.data_type() {
            DataType::Boolean => PrimitiveType::Boolean,
            DataType::Int8 | DataType::Int16 | DataType::Int32 => PrimitiveType::Int,
            DataType::Int64 => PrimitiveType::Long,
            DataType::Float32 => PrimitiveType::Float,
            DataType::Float64 => PrimitiveType::Double,
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => PrimitiveType::String,
            DataType::Binary | DataType::LargeBinary | DataType::BinaryView => {
                PrimitiveType::Binary
            }
            DataType::Date32 => PrimitiveType::Date,
            DataType::Timestamp(_, Some(_)) => PrimitiveType::Timestamptz,
            DataType::Timestamp(TimeUnit::Microsecond | TimeUnit::Millisecond, None) => {
                PrimitiveType::Timestamp
            }
            DataType::Decimal128(p, s) => PrimitiveType::Decimal {
                precision: *p as u32,
                scale: (*s).max(0) as u32,
            },
            other => {
                return Err(Error::Unsupported(format!(
                    "uniform: column `{}` is {other}, which has no Iceberg type — cast it, or \
                     turn off Iceberg publishing for this table",
                    field.name()
                )))
            }
        };
        let id = (index + 1) as i32;
        let ty = Type::Primitive(primitive);
        fields.push(Arc::new(if field.is_nullable() {
            NestedField::optional(id, field.name(), ty)
        } else {
            NestedField::required(id, field.name(), ty)
        }));
    }
    iceberg::spec::Schema::builder()
        .with_schema_id(0)
        .with_fields(fields)
        .build()
        .map_err(|e| Error::Unsupported(format!("uniform: iceberg schema: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{Field, Schema};
    use object_store::local::LocalFileSystem;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]))
    }

    fn files() -> Vec<DeltaAddFile> {
        vec![DeltaAddFile {
            path: "part-00000-a-0-c000.snappy.parquet".into(),
            size: 512,
            num_records: 3,
            partition_values: BTreeMap::new(),
            stats: Some("{\"numRecords\":3}".into()),
        }]
    }

    #[test]
    fn arrow_types_map_onto_iceberg_primitives() {
        let s: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("b", DataType::Boolean, true),
            Field::new("i", DataType::Int32, true),
            Field::new("l", DataType::Int64, true),
            Field::new("s", DataType::Utf8, true),
            Field::new("d", DataType::Date32, true),
            Field::new("ts", DataType::Timestamp(TimeUnit::Millisecond, None), true),
        ]));
        let ice = iceberg_schema(&s).unwrap();
        assert_eq!(ice.as_struct().fields().len(), 6);
        // Ids are positional and 1-based, which is what the published name mapping assumes.
        assert_eq!(ice.as_struct().fields()[0].id, 1);
        assert_eq!(ice.as_struct().fields()[5].id, 6);
    }

    #[test]
    fn an_unmappable_column_is_refused_at_resolve_time() {
        let s: SchemaRef = Arc::new(Schema::new(vec![Field::new(
            "d",
            DataType::Duration(TimeUnit::Second),
            true,
        )]));
        let Err(err) = UniformTable::new("s3://b/t", &s, &[], "uuid") else {
            panic!("a Duration column has no Iceberg type");
        };
        assert!(matches!(err, Error::Unsupported(_)), "{err:?}");
    }

    #[test]
    fn a_bare_path_becomes_a_file_uri_because_iceberg_records_absolute_locations() {
        assert_eq!(absolute_location("/data/t"), "file:///data/t");
        assert_eq!(absolute_location("s3://b/t/"), "s3://b/t");
        assert_eq!(absolute_location("file:///data/t"), "file:///data/t");
    }

    #[test]
    fn the_name_mapping_pairs_every_field_id_with_its_column_name() {
        // Without this an Iceberg reader finds no field ids in the Parquet footer and returns a
        // table full of nulls.
        let t = UniformTable::new("s3://b/t", &schema(), &[], "uuid").unwrap();
        let mapping: serde_json::Value = serde_json::from_str(&t.name_mapping().unwrap()).unwrap();
        assert_eq!(mapping[0]["field-id"], 1);
        assert_eq!(mapping[0]["names"][0], "id");
        assert_eq!(mapping[1]["field-id"], 2);
        assert_eq!(mapping[1]["names"][0], "name");
    }

    #[tokio::test]
    async fn publishing_writes_a_metadata_tree_pointing_at_the_delta_data_files() {
        let dir = tempfile::TempDir::new().unwrap();
        let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
        let root = ObjectPath::from("tbl");
        let location = format!("{}/tbl", dir.path().display());

        let t = UniformTable::new(&location, &schema(), &[], "tbl-uuid").unwrap();
        let metadata_location = t.publish(&store, &root, &files(), 4).await.unwrap();

        assert!(metadata_location.ends_with("metadata/v5.metadata.json"));
        let body =
            std::fs::read_to_string(dir.path().join("tbl/metadata/v5.metadata.json")).unwrap();
        let json: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(json["format-version"], 2);
        assert_eq!(json["current-snapshot-id"], 5);
        assert_eq!(json["snapshots"][0]["summary"]["total-records"], "3");
        assert!(
            json["properties"]["schema.name-mapping.default"]
                .as_str()
                .unwrap()
                .contains("\"names\":[\"id\"]"),
            "the name mapping is what makes field-id-less Parquet readable"
        );

        // The manifest and manifest list landed through the object store, not only the scratch
        // directory iceberg-rust wrote them to.
        assert!(dir.path().join("tbl/metadata/version-hint.text").exists());
        let metadata_dir: Vec<String> = std::fs::read_dir(dir.path().join("tbl/metadata"))
            .unwrap()
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        assert!(
            metadata_dir.iter().any(|f| f.ends_with("-m0.avro")),
            "{metadata_dir:?}"
        );
        assert!(
            metadata_dir.iter().any(|f| f.starts_with("snap-5-")),
            "{metadata_dir:?}"
        );
    }

    #[tokio::test]
    async fn a_partitioned_table_publishes_identity_partition_literals() {
        let dir = tempfile::TempDir::new().unwrap();
        let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
        let root = ObjectPath::from("tbl");
        let location = format!("{}/tbl", dir.path().display());

        let t = UniformTable::new(
            &location,
            &schema(),
            std::slice::from_ref(&"name".to_string()),
            "tbl-uuid",
        )
        .unwrap();

        let mut partitioned = files();
        partitioned[0].partition_values = [("name".to_string(), "alpha".to_string())]
            .into_iter()
            .collect();
        partitioned[0].path = "name=alpha/part-00000-a-0-c000.snappy.parquet".into();

        t.publish(&store, &root, &partitioned, 0).await.unwrap();
        let body =
            std::fs::read_to_string(dir.path().join("tbl/metadata/v1.metadata.json")).unwrap();
        let json: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(json["partition-specs"][0]["fields"][0]["name"], "name");
        assert_eq!(
            json["partition-specs"][0]["fields"][0]["transform"],
            "identity"
        );
    }

    #[test]
    fn a_null_partition_value_becomes_an_iceberg_null_not_the_hive_sentinel() {
        let t = UniformTable::new(
            "s3://b/t",
            &schema(),
            std::slice::from_ref(&"name".to_string()),
            "uuid",
        )
        .unwrap();
        let values: BTreeMap<String, String> = [("name".to_string(), NULL_PARTITION.to_string())]
            .into_iter()
            .collect();
        let s = t.partition_struct(&values).unwrap();
        assert!(s.iter().next().unwrap().is_none(), "sentinel means null");
    }
}

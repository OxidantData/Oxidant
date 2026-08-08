//! An AWS Glue Data Catalog [`CatalogProvider`].
//!
//! Implements the catalog SPI with the official AWS SDK for Rust (`aws-sdk-glue`) in-process —
//! the old implementation shelled out to the `aws glue` CLI per metadata op (~0.5–2s per
//! subprocess spawn, stderr string-matching for error classification, and a hard dependency on
//! the `aws` binary being installed; see KAN-82). Credentials resolve through the standard AWS
//! chain (`aws-config`: environment variables, shared config/credentials files, container
//! credentials, EC2 instance role / IRSA). `list_namespaces` → `GetDatabases` (paginated),
//! `list_tables` → `GetTables` (paginated), `load_table` → `GetTable` resolved to the table's
//! storage location + format. Once registered via `Engine::register_catalog`, Glue tables
//! resolve and query lazily through the DataFusion bridge — a genuine external catalog.
//!
//! Shared by the control-plane gateway (`POST /api/connections` with `kind=glue`) and the
//! cluster-side Spark Connect server (`spark.sql.catalog.<name>.type=glue`), so an attached Glue
//! catalog resolves identically whether a query runs on the gateway engine or on a cluster.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use aws_sdk_glue::error::ProvideErrorMetadata;
use aws_sdk_glue::types::{Column, SerDeInfo, StorageDescriptor, Table, TableInput};
use oxidant_catalog::arrow::datatypes::SchemaRef;
use oxidant_catalog::hive_types::{
    columns_to_schema, format_serde, schema_to_columns, validate_identifier,
};
use oxidant_catalog::{CatalogProvider, Error, Result, TableFormat, TableMetadata};

/// A Glue catalog connection, addressed by its registered `name`; metadata ops run through the
/// in-process AWS SDK client.
pub struct GlueCatalog {
    name: String,
    client: aws_sdk_glue::Client,
    /// `s3://bucket/prefix` root new tables are written under (`{warehouse}/{db}/{table}/`) when a
    /// `CREATE TABLE ... AS SELECT` doesn't specify an explicit `LOCATION`. `None` means CTAS
    /// against this catalog must supply an explicit location (see `create_table`).
    warehouse: Option<String>,
}

impl GlueCatalog {
    /// Build a Glue catalog provider for `region`, loading the surrounding AWS config
    /// (credentials chain, retry/behavior defaults) via `aws-config`. Async because loading the
    /// SDK config is; no network I/O happens here — credentials resolve lazily on first call.
    pub async fn new(
        name: impl Into<String>,
        region: impl Into<String>,
        warehouse: Option<String>,
    ) -> Self {
        let sdk_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(aws_sdk_glue::config::Region::new(region.into()))
            .load()
            .await;
        Self::from_client(name, warehouse, aws_sdk_glue::Client::new(&sdk_config))
    }

    /// Build from a flat options map (`region`, `warehouse`) — the shape used by both the gateway
    /// connection request and the `spark.sql.catalog.<name>.*` startup config. `region` resolves
    /// as option → `AWS_REGION` → `AWS_DEFAULT_REGION` → `us-west-2`; `warehouse` (e.g.
    /// `s3://bucket/prefix`, the Spark/Iceberg connection-option convention) is optional — CTAS
    /// against this catalog needs it (or an explicit `LOCATION`).
    pub async fn from_config(name: &str, options: &HashMap<String, String>) -> Self {
        let region = resolve_region(
            options,
            std::env::var("AWS_REGION").ok().as_deref(),
            std::env::var("AWS_DEFAULT_REGION").ok().as_deref(),
        );
        let warehouse = options.get("warehouse").cloned();
        Self::new(name, region, warehouse).await
    }

    /// Build from a preconfigured SDK client — tests inject a client pointed at a stub endpoint.
    pub fn from_client(
        name: impl Into<String>,
        warehouse: Option<String>,
        client: aws_sdk_glue::Client,
    ) -> Self {
        Self {
            name: name.into(),
            client,
            warehouse,
        }
    }
}

/// Classify a failed Glue API call from its error code + message.
///
/// Glue reports a missing database/table as `EntityNotFoundException` — an expected
/// "doesn't exist" signal (e.g. probed by CTAS to decide whether to create vs. fail), not a
/// genuine failure. That case maps to [`Error::Plan`], which `oxidant-loom`'s catalog bridge (and
/// `CatalogProvider::table_exists`'s default impl) already treats as "not found" rather than a
/// hard error. Every other failure (auth, network, throttling, ...) maps to [`Error::Io`] so it
/// still surfaces as a real error instead of being silently swallowed as "table missing".
///
/// Pure (code + message in, [`Error`] out) so the mapping is unit-testable without constructing
/// an SDK error.
fn classify_glue_failure(action: &str, code: Option<&str>, message: &str) -> Error {
    // Keep the service error code in the message (the old CLI path embedded it via stderr) —
    // operators grep for `AccessDeniedException` & friends.
    let detail = match code {
        Some(code) => format!("{code}: {message}"),
        None => message.to_string(),
    };
    if code == Some("EntityNotFoundException") {
        Error::Plan(format!("aws glue {action}: {detail}"))
    } else {
        Error::Io(format!("aws glue {action}: {detail}"))
    }
}

/// Map a failed SDK call to [`classify_glue_failure`] via the error's service code + message.
/// Non-service failures (timeouts, connector/transport errors, ...) carry no code, so they land
/// in the [`Error::Io`] bucket — a real error, never "not found".
fn sdk_failure<E>(action: &str, err: &aws_sdk_glue::error::SdkError<E>) -> Error
where
    E: ProvideErrorMetadata + std::fmt::Debug,
{
    let detail = err
        .message()
        .map(str::to_string)
        .filter(|m| !m.is_empty())
        .unwrap_or_else(|| format!("{err:?}"));
    classify_glue_failure(action, err.code(), &detail)
}

#[async_trait]
impl CatalogProvider for GlueCatalog {
    fn name(&self) -> &str {
        &self.name
    }

    async fn list_namespaces(&self, parent: &[String]) -> Result<Vec<Vec<String>>> {
        // Glue databases are flat — no nesting below a database.
        if !parent.is_empty() {
            return Ok(vec![]);
        }
        // `GetDatabases` paginates via NextToken; the SDK does not auto-paginate, so loop.
        let mut names = Vec::new();
        let mut next_token: Option<String> = None;
        loop {
            let resp = self
                .client
                .get_databases()
                .set_next_token(next_token)
                .send()
                .await
                .map_err(|e| sdk_failure("GetDatabases", &e))?;
            names.extend(resp.database_list.into_iter().map(|db| db.name));
            match resp.next_token {
                Some(token) => next_token = Some(token),
                None => break,
            }
        }
        Ok(names.into_iter().map(|d| vec![d]).collect())
    }

    async fn list_tables(&self, namespace: &[String]) -> Result<Vec<String>> {
        let db = single_db(namespace)?;
        let mut names = Vec::new();
        let mut next_token: Option<String> = None;
        loop {
            let resp = self
                .client
                .get_tables()
                .database_name(db)
                .set_next_token(next_token)
                .send()
                .await
                .map_err(|e| sdk_failure("GetTables", &e))?;
            names.extend(
                resp.table_list
                    .unwrap_or_default()
                    .into_iter()
                    .map(|t| t.name),
            );
            match resp.next_token {
                Some(token) => next_token = Some(token),
                None => break,
            }
        }
        Ok(names)
    }

    async fn load_table(&self, namespace: &[String], table: &str) -> Result<TableMetadata> {
        let db = single_db(namespace)?;
        let resp = self
            .client
            .get_table()
            .database_name(db)
            .name(table)
            .send()
            .await
            .map_err(|e| sdk_failure("GetTable", &e))?;
        // A 200 without a `Table` is a protocol anomaly; treat it like the old CLI path treated
        // a table object with no location — a "not usable" `Plan` error, never silently empty.
        let t = resp
            .table
            .ok_or_else(|| Error::Plan(format!("glue table `{db}.{table}` has no location")))?;
        parse_glue_table(&self.name, db, table, &t)
    }

    async fn create_table(
        &self,
        namespace: &[String],
        table: &str,
        schema: SchemaRef,
        format: TableFormat,
        location: Option<String>,
        partition_columns: &[String],
    ) -> Result<TableMetadata> {
        let db = single_db(namespace)?;
        let location = self.resolve_create_location(db, table, location)?;
        let table_input = build_table_input(table, &location, &schema, format, partition_columns)?;

        self.client
            .create_table()
            .database_name(db)
            .table_input(table_input)
            .send()
            .await
            .map_err(|e| sdk_failure("CreateTable", &e))?;

        let md = TableMetadata::new(format!("{}.{db}.{table}", self.name), location, format)
            .with_schema(schema)
            .with_partition_columns(partition_columns.to_vec());
        Ok(md)
    }
}

impl GlueCatalog {
    /// Resolve the storage location for a table being created: the explicit `location` if given
    /// (normalized to end in `/`, required for `ListingTable`/`is_collection()` on read-back),
    /// else `{warehouse}/{db}/{table}/`, else an error naming what's missing.
    ///
    /// `db`/`table` are validated as plain identifiers first (`validate_identifier`) — they come
    /// straight from the SQL statement's table reference, and are interpolated into the
    /// warehouse-derived path, so a name like `../../etc/evil` must not escape the intended
    /// directory (a real path-traversal bug for `file://` warehouses).
    fn resolve_create_location(
        &self,
        db: &str,
        table: &str,
        location: Option<String>,
    ) -> Result<String> {
        if let Some(l) = location {
            return Ok(if l.ends_with('/') { l } else { format!("{l}/") });
        }
        validate_identifier("database", db)?;
        validate_identifier("table", table)?;
        let warehouse = self.warehouse.as_deref().ok_or_else(|| {
            Error::Plan(format!(
                "catalog `{}` has no `warehouse` configured and no explicit LOCATION given",
                self.name
            ))
        })?;
        Ok(format!("{}/{db}/{table}/", warehouse.trim_end_matches('/')))
    }
}

/// Resolve the AWS region for a Glue catalog: catalog option → `AWS_REGION` →
/// `AWS_DEFAULT_REGION` → `us-west-2`. Env values are injected so unit tests can cover the
/// full precedence chain without mutating process environment.
fn resolve_region(
    options: &HashMap<String, String>,
    aws_region: Option<&str>,
    aws_default_region: Option<&str>,
) -> String {
    if let Some(r) = options.get("region").filter(|s| !s.is_empty()) {
        return r.clone();
    }
    if let Some(r) = aws_region.filter(|s| !s.is_empty()) {
        return r.to_string();
    }
    if let Some(r) = aws_default_region.filter(|s| !s.is_empty()) {
        return r.to_string();
    }
    "us-west-2".to_string()
}

/// Infer the readable file format from a Glue table's `Parameters` map.
///
/// Order of signals (most authoritative first): Iceberg `table_type`, Spark
/// `spark.sql.sources.provider` / `provider`, then the Glue/Athena `classification`
/// parameter. Falls back to Parquet when no signal is conclusive.
fn detect_format(parameters: &HashMap<String, String>) -> TableFormat {
    let param = |key: &str| parameters.get(key).map(String::as_str);
    if param("table_type").is_some_and(|v| v.eq_ignore_ascii_case("ICEBERG")) {
        return TableFormat::Iceberg;
    }
    for key in ["spark.sql.sources.provider", "provider"] {
        if let Some(f) = param(key).and_then(TableFormat::from_provider) {
            return f;
        }
    }
    param("classification")
        .and_then(TableFormat::from_provider)
        .unwrap_or(TableFormat::Parquet)
}

/// Map a Glue [`Table`] to [`TableMetadata`] (no I/O). `Parameters.metadata_location` rides
/// along in `properties` so the Iceberg reader can use the authoritative current-metadata
/// pointer written by Athena/Spark/Glue.
fn parse_glue_table(catalog_name: &str, db: &str, table: &str, t: &Table) -> Result<TableMetadata> {
    let sd = t.storage_descriptor.as_ref();
    let location = sd
        .and_then(|sd| sd.location.as_deref())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| Error::Plan(format!("glue table `{db}.{table}` has no location")))?;
    let parameters = t.parameters.clone().unwrap_or_default();
    let format = detect_format(&parameters);

    // The Glue-declared schema is the *authoritative* table schema: data columns
    // (`StorageDescriptor.Columns`) followed by partition columns (`PartitionKeys`). When it is
    // present and fully mappable we attach it so the engine reads files *against* it — files
    // whose physical types differ (a common case across monthly Parquet dumps) are cast to the
    // declared types by DataFusion's scan-time expression adapter, rather than failing schema
    // inference's strict "merge" check. If the columns are absent/empty, or *any* column has a
    // type we can't faithfully map, we leave `schema = None` and fall back to Parquet inference
    // (preserving today's behavior — never guessing a type that could silently corrupt a read).
    let data_cols: &[Column] = sd.and_then(|sd| sd.columns.as_deref()).unwrap_or(&[]);
    let part_cols: &[Column] = t.partition_keys.as_deref().unwrap_or(&[]);
    let schema = columns_to_schema(glue_column_pairs(data_cols, part_cols));
    // Partition-column NAMES (Hive layout: values live in the object path, e.g.
    // `.../year=2015/month=01/`, not inside the data files). The engine must know these so it
    // reads them from the path instead of expecting them in the Parquet — otherwise a
    // partitioned table (typical of the monthly taxi dumps) scans as NULLs or fails. The types
    // come along in `schema` (Glue appends partition columns to the declared schema).
    let partition_columns: Vec<String> = part_cols.iter().map(|c| c.name.clone()).collect();

    let comment = t
        .description
        .as_deref()
        .filter(|s| !s.is_empty())
        .map(str::to_string);
    // Every Glue `Parameters` entry is already a string pair; `metadata_location` (the Iceberg
    // current-metadata pointer, Athena/Spark/Glue convention) is just one of them.
    let properties = parameters;

    let md = TableMetadata::new(
        format!("{catalog_name}.{db}.{table}"),
        location.to_string(),
        format,
    )
    .with_comment(comment)
    .with_properties(properties)
    .with_partition_columns(partition_columns);
    Ok(match schema {
        Some(s) => md.with_schema(Arc::new(s)),
        None => md,
    })
}

/// Build the Glue `CreateTable` [`TableInput`] for a new table at `location` with
/// `schema`/`format`/`partition_columns`. A pure function (no I/O) so it's independently
/// unit-testable without an AWS endpoint.
fn build_table_input(
    table: &str,
    location: &str,
    schema: &oxidant_catalog::arrow::datatypes::Schema,
    format: TableFormat,
    partition_columns: &[String],
) -> Result<TableInput> {
    let serde = format_serde(format)?;
    let (data_cols, part_cols) = schema_to_columns(schema, partition_columns)?;
    let to_columns = |cols: &[(String, String)]| {
        cols.iter()
            .map(|(name, ty)| {
                Column::builder()
                    .name(name)
                    .r#type(ty)
                    .build()
                    .map_err(|e| Error::Io(format!("build Glue column: {e}")))
            })
            .collect::<Result<Vec<_>>>()
    };
    let serde_info = SerDeInfo::builder()
        .serialization_library(serde.serde_lib)
        .set_parameters(Some(
            serde
                .serde_params
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        ))
        .build();
    let storage = StorageDescriptor::builder()
        .location(location)
        .set_columns(Some(to_columns(&data_cols)?))
        .input_format(serde.input_format)
        .output_format(serde.output_format)
        .serde_info(serde_info)
        .build();
    TableInput::builder()
        .name(table)
        .storage_descriptor(storage)
        .set_partition_keys(Some(to_columns(&part_cols)?))
        .parameters("classification", classification_for(format))
        .build()
        .map_err(|e| Error::Io(format!("build Glue TableInput: {e}")))
}

/// The Glue/Athena `classification` table parameter for a physical format (the same convention
/// `load_table` reads back via `Parameters.classification`).
fn classification_for(format: TableFormat) -> &'static str {
    match format {
        TableFormat::Parquet => "parquet",
        TableFormat::Csv => "csv",
        TableFormat::Json => "json",
        TableFormat::Delta => "delta",
        TableFormat::Iceberg => "iceberg",
    }
}

/// Flatten a Glue table's `StorageDescriptor.Columns` (data columns) and `PartitionKeys`
/// (partition columns) into ordered `(name, type)` pairs, data columns first. Feeds
/// [`columns_to_schema`], which decides schema-vs-inference.
///
/// A column with no `Type` yields an empty type string, which is unmappable — so
/// `columns_to_schema` returns `None` (whole-table inference). This is the conservative,
/// all-or-nothing behavior: never build a partial schema that could shift column positions.
fn glue_column_pairs(data_cols: &[Column], part_cols: &[Column]) -> Vec<(String, String)> {
    data_cols
        .iter()
        .chain(part_cols.iter())
        .map(|col| (col.name.clone(), col.r#type.clone().unwrap_or_default()))
        .collect()
}

fn single_db(namespace: &[String]) -> Result<&str> {
    match namespace {
        [db] => Ok(db.as_str()),
        [] => Err(Error::Plan(
            "a Glue table reference needs a database, e.g. `catalog.database.table`".into(),
        )),
        _ => Err(Error::Plan(format!(
            "Glue namespaces are a single database; got `{}`",
            namespace.join(".")
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use oxidant_catalog::arrow::datatypes::DataType;

    fn col(name: &str, ty: &str) -> Column {
        Column::builder()
            .name(name)
            .r#type(ty)
            .build()
            .expect("column")
    }

    // The pure Hive-type→Arrow mapping is unit-tested in `oxidant_catalog::hive_types`; these tests
    // cover Glue's `{Name,Type}` column → `(name, type)` flattening and its integration with
    // `columns_to_schema` (data columns then partition keys, with the all-or-nothing fallback).

    #[test]
    fn schema_from_columns_includes_partition_keys() {
        let data = [col("vendor_id", "bigint"), col("fare", "decimal(10,2)")];
        let parts = [col("month", "string")];
        let schema = columns_to_schema(glue_column_pairs(&data, &parts)).expect("schema");
        assert_eq!(schema.fields().len(), 3);
        assert_eq!(schema.field(0).name(), "vendor_id");
        assert_eq!(schema.field(0).data_type(), &DataType::Int64);
        assert_eq!(schema.field(1).data_type(), &DataType::Decimal128(10, 2));
        // Partition column appended after data columns.
        assert_eq!(schema.field(2).name(), "month");
        assert_eq!(schema.field(2).data_type(), &DataType::Utf8);
        assert!(schema.field(0).is_nullable());
    }

    #[test]
    fn empty_or_absent_columns_fall_back_to_inference() {
        // Empty Columns (the existing-table case) → None, preserving the inference behavior.
        let empty: [Column; 0] = [];
        assert_eq!(columns_to_schema(glue_column_pairs(&empty, &empty)), None);
    }

    #[test]
    fn any_unmappable_column_falls_back_to_inference() {
        // One complex column poisons the whole schema → infer rather than shift positions.
        let data = [col("id", "bigint"), col("tags", "array<string>")];
        let empty: [Column; 0] = [];
        assert_eq!(columns_to_schema(glue_column_pairs(&data, &empty)), None);
    }

    #[test]
    fn column_missing_type_falls_back() {
        // A Glue column with no `Type` yields an empty type string → unmappable → None.
        let untyped = Column::builder().name("id").build().expect("column");
        let empty: [Column; 0] = [];
        assert_eq!(
            columns_to_schema(glue_column_pairs(&[untyped], &empty)),
            None
        );
    }

    // `classify_glue_failure` is what lets a CTAS's "does the target table already exist?" probe
    // (`GetTable`) tell "doesn't exist yet, go ahead and create it" (EntityNotFoundException)
    // apart from a genuine failure that must still surface as an error.

    #[test]
    fn entity_not_found_classifies_as_not_found() {
        match classify_glue_failure(
            "GetTable",
            Some("EntityNotFoundException"),
            "Entity Not Found",
        ) {
            Error::Plan(msg) => assert!(msg.contains("Entity Not Found")),
            other => panic!("expected Error::Plan, got {other:?}"),
        }
    }

    #[test]
    fn access_denied_classifies_as_io_error() {
        match classify_glue_failure(
            "GetTable",
            Some("AccessDeniedException"),
            "User is not authorized",
        ) {
            Error::Io(msg) => assert!(msg.contains("AccessDeniedException")),
            other => panic!("expected Error::Io, got {other:?}"),
        }
    }

    #[test]
    fn generic_failure_classifies_as_io_error() {
        // Transport/connector failures carry no service error code.
        match classify_glue_failure("GetTable", None, "connection closed") {
            Error::Io(msg) => assert!(msg.contains("connection closed")),
            other => panic!("expected Error::Io, got {other:?}"),
        }
    }

    // `build_table_input` / `resolve_create_location` back `GlueCatalog::create_table` (CTAS write
    // support) — tested as pure functions so no AWS endpoint is needed.

    fn sample_schema() -> oxidant_catalog::arrow::datatypes::Schema {
        use oxidant_catalog::arrow::datatypes::{DataType, Field};
        oxidant_catalog::arrow::datatypes::Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("dt", DataType::Utf8, true),
        ])
    }

    #[test]
    fn build_table_input_shapes_parquet_table_correctly() {
        let schema = sample_schema();
        let input = build_table_input(
            "orders",
            "s3://bucket/db/orders/",
            &schema,
            TableFormat::Parquet,
            &["dt".to_string()],
        )
        .expect("built");
        assert_eq!(input.name(), "orders");
        let sd = input.storage_descriptor().expect("storage descriptor");
        assert_eq!(sd.location(), Some("s3://bucket/db/orders/"));
        let col_pairs: Vec<(&str, Option<&str>)> = sd
            .columns()
            .iter()
            .map(|c| (c.name(), c.r#type()))
            .collect();
        assert_eq!(
            col_pairs,
            vec![("id", Some("bigint")), ("name", Some("string"))]
        );
        let part_pairs: Vec<(&str, Option<&str>)> = input
            .partition_keys()
            .iter()
            .map(|c| (c.name(), c.r#type()))
            .collect();
        assert_eq!(part_pairs, vec![("dt", Some("string"))]);
        assert_eq!(
            sd.serde_info().and_then(|s| s.serialization_library()),
            Some("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe")
        );
        assert_eq!(
            input
                .parameters()
                .and_then(|p| p.get("classification"))
                .map(String::as_str),
            Some("parquet")
        );
    }

    #[test]
    fn build_table_input_rejects_lakehouse_write_formats() {
        let schema = sample_schema();
        for format in [TableFormat::Delta, TableFormat::Iceberg] {
            let err = build_table_input("t", "s3://bucket/t/", &schema, format, &[]).unwrap_err();
            assert!(matches!(err, Error::Unsupported(_)), "{format:?}");
        }
    }

    /// A `GlueCatalog` for pure location-resolution tests: the SDK client is never called, so a
    /// dummy config (static credentials, no network) is enough.
    fn test_catalog(warehouse: Option<String>) -> GlueCatalog {
        let conf = aws_sdk_glue::Config::builder()
            .region(aws_sdk_glue::config::Region::new("us-west-2"))
            .credentials_provider(aws_sdk_glue::config::SharedCredentialsProvider::new(
                aws_sdk_glue::config::Credentials::new("akid", "secret", None, None, "test"),
            ))
            .behavior_version(aws_sdk_glue::config::BehaviorVersion::latest())
            .build();
        GlueCatalog::from_client("glue", warehouse, aws_sdk_glue::Client::from_conf(conf))
    }

    #[test]
    fn resolve_create_location_prefers_explicit_location() {
        let cat = test_catalog(Some("s3://wh".to_string()));
        assert_eq!(
            cat.resolve_create_location("db", "t", Some("s3://explicit/t/".to_string()))
                .unwrap(),
            "s3://explicit/t/"
        );
    }

    #[test]
    fn resolve_create_location_falls_back_to_warehouse() {
        let cat = test_catalog(Some("s3://wh/".to_string()));
        assert_eq!(
            cat.resolve_create_location("db", "t", None).unwrap(),
            "s3://wh/db/t/"
        );
    }

    #[test]
    fn resolve_create_location_errors_without_warehouse_or_location() {
        let cat = test_catalog(None);
        let err = cat.resolve_create_location("db", "t", None).unwrap_err();
        assert!(matches!(err, Error::Plan(_)));
    }

    #[test]
    fn resolve_create_location_rejects_path_traversal() {
        let cat = test_catalog(Some("s3://wh".to_string()));
        for (db, table) in [("db", "../../etc/evil"), ("../escape", "t"), ("db", "a/b")] {
            let err = cat.resolve_create_location(db, table, None).unwrap_err();
            assert!(matches!(err, Error::Plan(_)), "{db}.{table}");
        }
    }

    #[test]
    fn resolve_create_location_normalizes_missing_trailing_slash() {
        let cat = test_catalog(None);
        assert_eq!(
            cat.resolve_create_location("db", "t", Some("s3://explicit/t".to_string()))
                .unwrap(),
            "s3://explicit/t/"
        );
    }

    // Format detection / region resolution — pure helpers over SDK-type fixtures (no endpoint).

    fn glue_table_fixture(parameters: HashMap<String, String>) -> Table {
        Table::builder()
            .name("t")
            .storage_descriptor(
                StorageDescriptor::builder()
                    .location("s3://bucket/db/t/")
                    .columns(col("id", "bigint"))
                    .build(),
            )
            .set_partition_keys(Some(vec![]))
            .set_parameters(Some(parameters))
            .build()
            .expect("table")
    }

    fn params(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn iceberg_table_type_detects_iceberg_and_surfaces_metadata_location() {
        let t = glue_table_fixture(params(&[
            ("table_type", "ICEBERG"),
            (
                "metadata_location",
                "s3://bucket/db/t/metadata/00010-abc.metadata.json",
            ),
            ("classification", "parquet"),
        ]));
        let md = parse_glue_table("glue", "db", "t", &t).expect("parsed");
        assert_eq!(md.format, TableFormat::Iceberg);
        assert_eq!(
            md.properties.get("metadata_location").map(String::as_str),
            Some("s3://bucket/db/t/metadata/00010-abc.metadata.json")
        );
    }

    #[test]
    fn iceberg_table_type_wins_over_conflicting_delta_provider() {
        // Authoritative Iceberg `table_type` must beat Spark provider / classification noise —
        // otherwise workers open the wrong reader and can silently mis-apply deletes.
        let t = glue_table_fixture(params(&[
            ("table_type", "ICEBERG"),
            ("spark.sql.sources.provider", "delta"),
            ("provider", "delta"),
            ("classification", "parquet"),
            (
                "metadata_location",
                "s3://bucket/db/t/metadata/snap.metadata.json",
            ),
        ]));
        let md = parse_glue_table("glue", "db", "t", &t).expect("parsed");
        assert_eq!(md.format, TableFormat::Iceberg);
        assert_eq!(
            md.properties.get("metadata_location").map(String::as_str),
            Some("s3://bucket/db/t/metadata/snap.metadata.json")
        );
    }

    #[test]
    fn spark_provider_delta_detects_as_delta() {
        let t = glue_table_fixture(params(&[
            ("spark.sql.sources.provider", "delta"),
            ("classification", "parquet"),
        ]));
        let md = parse_glue_table("glue", "db", "t", &t).expect("parsed");
        assert_eq!(md.format, TableFormat::Delta);
    }

    #[test]
    fn bare_provider_delta_detects_as_delta() {
        // Some Glue writers set only `provider`, not `spark.sql.sources.provider`.
        let t = glue_table_fixture(params(&[
            ("provider", "delta"),
            ("classification", "parquet"),
        ]));
        let md = parse_glue_table("glue", "db", "t", &t).expect("parsed");
        assert_eq!(md.format, TableFormat::Delta);
    }

    #[test]
    fn classification_delta_detects_as_delta() {
        let t = glue_table_fixture(params(&[("classification", "delta")]));
        let md = parse_glue_table("glue", "db", "t", &t).expect("parsed");
        assert_eq!(md.format, TableFormat::Delta);
    }

    #[test]
    fn plain_parquet_table_unchanged() {
        let t = glue_table_fixture(params(&[("classification", "parquet")]));
        let md = parse_glue_table("glue", "db", "t", &t).expect("parsed");
        assert_eq!(md.format, TableFormat::Parquet);
        assert_eq!(md.location, "s3://bucket/db/t/");
        assert!(!md.properties.contains_key("metadata_location"));
    }

    #[test]
    fn table_without_location_is_plan_error() {
        let t = Table::builder()
            .name("t")
            .set_parameters(Some(params(&[("classification", "parquet")])))
            .build()
            .expect("table");
        let err = parse_glue_table("glue", "db", "t", &t).unwrap_err();
        assert!(matches!(err, Error::Plan(_)));
    }

    #[test]
    fn region_precedence_option_env_default() {
        let mut opts = HashMap::new();
        opts.insert("region".to_string(), "eu-west-1".to_string());
        // Option wins over both env vars.
        assert_eq!(
            resolve_region(&opts, Some("us-east-1"), Some("ap-south-1")),
            "eu-west-1"
        );
        // AWS_REGION wins over AWS_DEFAULT_REGION when option absent.
        assert_eq!(
            resolve_region(&HashMap::new(), Some("us-east-1"), Some("ap-south-1")),
            "us-east-1"
        );
        // AWS_DEFAULT_REGION when AWS_REGION absent.
        assert_eq!(
            resolve_region(&HashMap::new(), None, Some("ap-south-1")),
            "ap-south-1"
        );
        // Hardcoded fallback when nothing is set.
        assert_eq!(resolve_region(&HashMap::new(), None, None), "us-west-2");
        // Empty strings are ignored (treated as unset).
        opts.insert("region".to_string(), "".to_string());
        assert_eq!(
            resolve_region(&opts, Some(""), Some("ap-south-1")),
            "ap-south-1"
        );
    }

    // ---------------------------------------------------------------------
    // Stub-endpoint integration test: a raw mini HTTP server speaks just enough of the AWS JSON
    // 1.1 protocol (POST + `x-amz-target` dispatch) for the real SDK client to run
    // `list_namespaces` / `list_tables` / `load_table` end-to-end — no AWS in CI.
    // ---------------------------------------------------------------------

    use aws_sdk_glue::config::{BehaviorVersion, Credentials, Region, SharedCredentialsProvider};

    const DATABASES_JSON: &str = r#"{"DatabaseList":[{"Name":"db1"}]}"#;
    const TABLES_JSON: &str = r#"{"TableList":[{"Name":"orders"}]}"#;
    const ORDERS_JSON: &str = r#"{"Table":{"Name":"orders","StorageDescriptor":{"Location":"s3://bucket/db1/orders/","Columns":[{"Name":"id","Type":"bigint"}]},"PartitionKeys":[{"Name":"dt","Type":"string"}],"Parameters":{"classification":"parquet"}}}"#;
    const ICEBERG_JSON: &str = r#"{"Table":{"Name":"iceberg_t","StorageDescriptor":{"Location":"s3://bucket/db1/iceberg_t/","Columns":[{"Name":"id","Type":"bigint"}]},"PartitionKeys":[],"Parameters":{"table_type":"ICEBERG","metadata_location":"s3://bucket/db1/iceberg_t/metadata/00010-abc.metadata.json"}}}"#;
    const NOT_FOUND_JSON: &str =
        r#"{"__type":"EntityNotFoundException","message":"Table nope not found"}"#;

    /// Bind a stub Glue endpoint on a loopback ephemeral port; each accepted connection reads
    /// one request and answers from the `x-amz-target` / body. Returns the port; the server task
    /// runs until aborted.
    async fn spawn_glue_stub() -> u16 {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind stub");
        let port = listener.local_addr().expect("local addr").port();
        tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else {
                    return;
                };
                tokio::spawn(async move {
                    let mut buf = Vec::new();
                    let mut chunk = [0_u8; 8192];
                    // Read the full request: headers first, then exactly Content-Length bytes.
                    let header_end = loop {
                        let n = sock.read(&mut chunk).await.expect("read");
                        if n == 0 {
                            return;
                        }
                        buf.extend_from_slice(&chunk[..n]);
                        if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                            break pos + 4;
                        }
                    };
                    let head = String::from_utf8_lossy(&buf[..header_end]).to_string();
                    let content_length = head
                        .lines()
                        .find_map(|line| {
                            let (k, v) = line.split_once(':')?;
                            k.trim()
                                .eq_ignore_ascii_case("content-length")
                                .then(|| v.trim().parse::<usize>().ok())?
                        })
                        .unwrap_or(0);
                    while buf.len() < header_end + content_length {
                        let n = sock.read(&mut chunk).await.expect("read body");
                        if n == 0 {
                            break;
                        }
                        buf.extend_from_slice(&chunk[..n]);
                    }
                    let request = String::from_utf8_lossy(&buf).to_string();
                    // NB: match `GetTables` before `GetTable` — the latter is a substring.
                    let (status, body) = if request.contains("GetDatabases") {
                        ("200 OK", DATABASES_JSON)
                    } else if request.contains("GetTables") {
                        ("200 OK", TABLES_JSON)
                    } else if request.contains("GetTable") {
                        if request.contains("nope") {
                            ("400 Bad Request", NOT_FOUND_JSON)
                        } else if request.contains("iceberg_t") {
                            ("200 OK", ICEBERG_JSON)
                        } else {
                            ("200 OK", ORDERS_JSON)
                        }
                    } else {
                        (
                            "400 Bad Request",
                            r#"{"__type":"ValidationException","message":"?"}"#,
                        )
                    };
                    let response = format!(
                        "HTTP/1.1 {status}\r\ncontent-type: application/x-amz-json-1.1\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = sock.write_all(response.as_bytes()).await;
                });
            }
        });
        port
    }

    fn stub_catalog(port: u16) -> GlueCatalog {
        let conf = aws_sdk_glue::Config::builder()
            .endpoint_url(format!("http://127.0.0.1:{port}"))
            .region(Region::new("us-west-2"))
            .credentials_provider(SharedCredentialsProvider::new(Credentials::new(
                "akid", "secret", None, None, "test",
            )))
            .behavior_version(BehaviorVersion::latest())
            .build();
        GlueCatalog::from_client("glue", None, aws_sdk_glue::Client::from_conf(conf))
    }

    #[tokio::test]
    async fn sdk_client_round_trips_against_stub_endpoint() {
        let port = spawn_glue_stub().await;
        let cat = stub_catalog(port);

        // GetDatabases → namespaces; a non-empty parent short-circuits without a call.
        let namespaces = cat.list_namespaces(&[]).await.expect("databases");
        assert_eq!(namespaces, vec![vec!["db1".to_string()]]);
        assert!(cat
            .list_namespaces(&["db1".to_string()])
            .await
            .expect("flat")
            .is_empty());

        // GetTables → table names in the database.
        let tables = cat.list_tables(&["db1".to_string()]).await.expect("tables");
        assert_eq!(tables, vec!["orders".to_string()]);

        // GetTable → parsed metadata: parquet format, declared schema, partition columns.
        let md = cat
            .load_table(&["db1".to_string()], "orders")
            .await
            .expect("load orders");
        assert_eq!(md.format, TableFormat::Parquet);
        assert_eq!(md.location, "s3://bucket/db1/orders/");
        assert_eq!(md.name, "glue.db1.orders");
        assert_eq!(md.partition_columns, vec!["dt".to_string()]);
        let schema = md.schema.expect("declared schema attached");
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).data_type(), &DataType::Int64);
        assert_eq!(schema.field(1).name(), "dt");

        // Iceberg parameters drive format detection and the metadata_location property.
        let md = cat
            .load_table(&["db1".to_string()], "iceberg_t")
            .await
            .expect("load iceberg_t");
        assert_eq!(md.format, TableFormat::Iceberg);
        assert_eq!(
            md.properties.get("metadata_location").map(String::as_str),
            Some("s3://bucket/db1/iceberg_t/metadata/00010-abc.metadata.json")
        );

        // A 400 EntityNotFoundException maps to `Error::Plan` ("not found"), not a hard error.
        let err = cat
            .load_table(&["db1".to_string()], "nope")
            .await
            .expect_err("missing table");
        assert!(matches!(err, Error::Plan(_)), "{err:?}");
    }
}

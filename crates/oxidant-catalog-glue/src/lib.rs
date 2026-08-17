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
//! DDL coverage (KAN-100): `create_database` → `CreateDatabase`, `drop_database` →
//! `DeleteDatabase` (CASCADE emulated by deleting the database's tables first — Glue has no
//! cascade flag), `drop_table` → `DeleteTable`, `alter_table` → `GetTable` + `UpdateTable`
//! (properties / comment / location / added columns — `RENAME COLUMN` / `CHANGE COLUMN` deferred
//! until Loom wires those ALTER forms into the SPI), `list_partitions` → `GetPartitions`
//! (paginated), and `repair_table` (`MSCK REPAIR TABLE`): scan the table's storage location for
//! Hive-style `key=value/` partition directories via `object_store` and `BatchCreatePartition`
//! the ones Glue doesn't know yet.
//!
//! Shared by the control-plane gateway (`POST /api/connections` with `kind=glue`) and the
//! cluster-side Spark Connect server (`spark.sql.catalog.<name>.type=glue`), so an attached Glue
//! catalog resolves identically whether a query runs on the gateway engine or on a cluster.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use aws_sdk_glue::error::ProvideErrorMetadata;
use aws_sdk_glue::types::{
    Column, DatabaseInput, PartitionInput, SerDeInfo, StorageDescriptor, Table, TableInput,
};
use oxidant_catalog::arrow::datatypes::SchemaRef;
use oxidant_catalog::hive_types::{
    arrow_type_to_hive, columns_to_schema, format_serde, schema_to_columns, validate_identifier,
    validate_partition_value,
};
use oxidant_catalog::{CatalogProvider, Error, Result, TableChange, TableFormat, TableMetadata};

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

    async fn create_database(
        &self,
        database: &str,
        if_not_exists: bool,
        comment: Option<String>,
        location: Option<String>,
    ) -> Result<()> {
        // No explicit LOCATION → the Hive/Spark default database root under the catalog's
        // warehouse (`{warehouse}/{db}.db/`). The name is interpolated into that path, so it
        // must be a plain identifier (same path-traversal guard as `resolve_create_location`).
        let location = match location {
            Some(l) => Some(l),
            None => match self.warehouse.as_deref() {
                Some(w) => {
                    validate_identifier("database", database)?;
                    Some(format!("{}/{database}.db/", w.trim_end_matches('/')))
                }
                None => None,
            },
        };
        let mut input = DatabaseInput::builder().name(database);
        if let Some(comment) = comment.filter(|c| !c.is_empty()) {
            input = input.description(comment);
        }
        if let Some(location) = location {
            input = input.location_uri(location);
        }
        let input = input
            .build()
            .map_err(|e| Error::Io(format!("build Glue DatabaseInput: {e}")))?;
        match self
            .client
            .create_database()
            .database_input(input)
            .send()
            .await
        {
            Ok(_) => Ok(()),
            // IF NOT EXISTS: a duplicate database is a no-op, not an error.
            Err(e) if if_not_exists && e.code() == Some("AlreadyExistsException") => Ok(()),
            Err(e) => Err(sdk_failure("CreateDatabase", &e)),
        }
    }

    async fn drop_database(&self, database: &str, if_exists: bool, cascade: bool) -> Result<()> {
        if cascade {
            // Glue's DeleteDatabase refuses a non-empty database and has no cascade flag —
            // emulate Spark/Hive `DROP DATABASE ... CASCADE` by deleting every table first.
            match self.list_tables(&[database.to_string()]).await {
                Ok(tables) => {
                    for table in tables {
                        self.drop_table(&[database.to_string()], &table, false)
                            .await?;
                    }
                }
                // Missing database + IF EXISTS → no-op (same as DeleteDatabase would be).
                Err(Error::Plan(_)) if if_exists => return Ok(()),
                Err(e) => return Err(e),
            }
        }
        match self.client.delete_database().name(database).send().await {
            Ok(_) => Ok(()),
            // IF EXISTS: a missing database is a no-op, not an error.
            Err(e) if if_exists && e.code() == Some("EntityNotFoundException") => Ok(()),
            Err(e) => Err(sdk_failure("DeleteDatabase", &e)),
        }
    }

    async fn drop_table(&self, namespace: &[String], table: &str, if_exists: bool) -> Result<()> {
        let db = single_db(namespace)?;
        match self
            .client
            .delete_table()
            .database_name(db)
            .name(table)
            .send()
            .await
        {
            Ok(_) => Ok(()),
            // IF EXISTS: a missing table is a no-op, not an error.
            Err(e) if if_exists && e.code() == Some("EntityNotFoundException") => Ok(()),
            Err(e) => Err(sdk_failure("DeleteTable", &e)),
        }
    }

    async fn alter_table(
        &self,
        namespace: &[String],
        table: &str,
        changes: Vec<TableChange>,
    ) -> Result<TableMetadata> {
        let db = single_db(namespace)?;
        // Glue's UpdateTable replaces the whole definition, so fetch the current one, apply the
        // changes to a TableInput derived from it, and write that back. `apply_table_changes`
        // validates every change BEFORE the call, so a rejected change leaves the table
        // untouched (the trait contract).
        let resp = self
            .client
            .get_table()
            .database_name(db)
            .name(table)
            .send()
            .await
            .map_err(|e| sdk_failure("GetTable", &e))?;
        let t = resp
            .table
            .ok_or_else(|| Error::Plan(format!("GetTable for `{db}.{table}` returned no table")))?;
        let mut input = table_to_input(&t)?;
        apply_table_changes(&mut input, &changes)?;

        self.client
            .update_table()
            .database_name(db)
            .table_input(input)
            .send()
            .await
            .map_err(|e| sdk_failure("UpdateTable", &e))?;

        // Return the post-alter definition (a fresh GetTable) — what Glue actually stored.
        self.load_table(namespace, table).await
    }

    async fn list_partitions(&self, namespace: &[String], table: &str) -> Result<Vec<Vec<String>>> {
        let db = single_db(namespace)?;
        // `GetPartitions` paginates via NextToken (like GetTables); loop manually.
        let mut partitions = Vec::new();
        let mut next_token: Option<String> = None;
        loop {
            let resp = self
                .client
                .get_partitions()
                .database_name(db)
                .table_name(table)
                .set_next_token(next_token)
                .send()
                .await
                .map_err(|e| sdk_failure("GetPartitions", &e))?;
            partitions.extend(
                resp.partitions
                    .unwrap_or_default()
                    .into_iter()
                    .map(|p| p.values.unwrap_or_default()),
            );
            match resp.next_token {
                Some(token) => next_token = Some(token),
                None => break,
            }
        }
        Ok(partitions)
    }

    async fn repair_table(&self, namespace: &[String], table: &str) -> Result<usize> {
        let db = single_db(namespace)?;
        let resp = self
            .client
            .get_table()
            .database_name(db)
            .name(table)
            .send()
            .await
            .map_err(|e| sdk_failure("GetTable", &e))?;
        let t = resp
            .table
            .ok_or_else(|| Error::Plan(format!("GetTable for `{db}.{table}` returned no table")))?;
        let part_keys: Vec<String> = t
            .partition_keys
            .as_deref()
            .unwrap_or(&[])
            .iter()
            .map(|c| c.name.clone())
            .collect();
        // MSCK REPAIR on an unpartitioned table has nothing to discover (Spark no-ops too).
        if part_keys.is_empty() {
            return Ok(0);
        }
        let sd = t
            .storage_descriptor
            .clone()
            .filter(|sd| sd.location.as_deref().is_some_and(|l| !l.is_empty()))
            .ok_or_else(|| Error::Plan(format!("glue table `{db}.{table}` has no location")))?;
        let location = sd.location.clone().expect("checked above");

        let discovered = discover_partitions(&location, &part_keys).await?;
        let existing: std::collections::HashSet<Vec<String>> = self
            .list_partitions(namespace, table)
            .await?
            .into_iter()
            .collect();
        let missing: Vec<Vec<String>> = discovered
            .into_iter()
            .filter(|values| !existing.contains(values))
            .collect();
        if missing.is_empty() {
            return Ok(0);
        }
        // Glue caps BatchCreatePartition at 100 partition inputs per call.
        for chunk in missing.chunks(100) {
            let mut req = self
                .client
                .batch_create_partition()
                .database_name(db)
                .table_name(table);
            for values in chunk {
                for (key, value) in part_keys.iter().zip(values.iter()) {
                    validate_partition_value(key, value)?;
                }
                let mut part_sd = sd.clone();
                part_sd.location = Some(partition_location(&location, &part_keys, values)?);
                req = req.partition_input_list(
                    PartitionInput::builder()
                        .set_values(Some(values.clone()))
                        .storage_descriptor(part_sd)
                        .build(),
                );
            }
            match req.send().await {
                Ok(_) => {}
                // Idempotent repair: a retry after partial success may re-send partitions Glue
                // already registered.
                Err(e) if e.code() == Some("AlreadyExistsException") => {}
                Err(e) => return Err(sdk_failure("BatchCreatePartition", &e)),
            }
        }
        Ok(missing.len())
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
    let mut input = TableInput::builder()
        .name(table)
        .storage_descriptor(storage)
        .set_partition_keys(Some(to_columns(&part_cols)?))
        .parameters("classification", classification_for(format));
    if format == TableFormat::Delta {
        // A Delta table's SerDe is indistinguishable from a plain Parquet table's, so this
        // parameter is what tells Spark, EMR, and Athena to read `_delta_log/` instead of
        // listing the directory. `detect_format` reads it back on the way in.
        input = input.parameters("spark.sql.sources.provider", "delta");
    }
    if format == TableFormat::Iceberg {
        // The authoritative Iceberg signal, and the one Athena, Trino, and Glue all key off.
        // `metadata_location` is written separately, by whatever commits a snapshot — for a
        // streaming table that is the Iceberg metadata published alongside the Delta log.
        input = input.parameters("table_type", "ICEBERG");
    }
    input
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

/// Copy a Glue [`Table`] (as returned by `GetTable`) into the [`TableInput`] `UpdateTable`
/// expects — the update replaces the whole definition, so everything Glue tracks must ride
/// along or it would be silently dropped. Read-only fields (`DatabaseName`, `CreateTime`, ...)
/// have no `TableInput` counterpart and are simply not set.
fn table_to_input(t: &Table) -> Result<TableInput> {
    TableInput::builder()
        .name(&t.name)
        .set_description(t.description.clone())
        .set_owner(t.owner.clone())
        .retention(t.retention)
        .set_storage_descriptor(t.storage_descriptor.clone())
        .set_partition_keys(t.partition_keys.clone())
        .set_view_original_text(t.view_original_text.clone())
        .set_view_expanded_text(t.view_expanded_text.clone())
        .set_table_type(t.table_type.clone())
        .set_parameters(t.parameters.clone())
        .build()
        .map_err(|e| Error::Io(format!("build Glue TableInput: {e}")))
}

/// Apply `ALTER TABLE` changes to a [`TableInput`] in place (mutating the SDK structs' public
/// fields). Pure, so each change kind is unit-testable without an AWS endpoint. Any change this
/// can't honor (e.g. `ADD COLUMNS` with an Arrow type that has no faithful Hive type string)
/// fails with [`Error::Unsupported`] before the caller issues `UpdateTable`, leaving the
/// catalog table untouched.
fn apply_table_changes(input: &mut TableInput, changes: &[TableChange]) -> Result<()> {
    for change in changes {
        match change {
            TableChange::SetProperties(props) => {
                input
                    .parameters
                    .get_or_insert_with(HashMap::new)
                    .extend(props.iter().map(|(k, v)| (k.clone(), v.clone())));
            }
            TableChange::UnsetProperties(keys) => {
                if let Some(params) = input.parameters.as_mut() {
                    for key in keys {
                        params.remove(key);
                    }
                }
            }
            TableChange::SetComment(comment) => {
                input.description = comment.clone().filter(|c| !c.is_empty());
            }
            TableChange::SetLocation(location) => {
                let loc = if location.ends_with('/') {
                    location.clone()
                } else {
                    format!("{location}/")
                };
                let sd = input.storage_descriptor.as_mut().ok_or_else(|| {
                    Error::Plan(format!(
                        "ALTER TABLE SET LOCATION on `{}`: table has no storage descriptor",
                        input.name
                    ))
                })?;
                sd.location = Some(loc);
            }
            TableChange::AddColumns(fields) => {
                let sd = input.storage_descriptor.as_mut().ok_or_else(|| {
                    Error::Plan(format!(
                        "ALTER TABLE ADD COLUMNS on `{}`: table has no storage descriptor",
                        input.name
                    ))
                })?;
                let mut columns = sd.columns.take().unwrap_or_default();
                for field in fields {
                    let ty = arrow_type_to_hive(field.data_type()).ok_or_else(|| {
                        Error::Unsupported(format!(
                            "column `{}` has type {:?}, which cannot be declared to an external catalog",
                            field.name(),
                            field.data_type()
                        ))
                    })?;
                    columns.push(
                        Column::builder()
                            .name(field.name())
                            .r#type(ty)
                            .build()
                            .map_err(|e| Error::Io(format!("build Glue column: {e}")))?,
                    );
                }
                sd.columns = Some(columns);
            }
        }
    }
    Ok(())
}

/// The storage location of one partition under a table root: `{location}/{k1=v1}/{k2=v2}/`
/// (the Hive layout `MSCK REPAIR TABLE` discovers and `load_table` reads back).
fn partition_location(
    table_location: &str,
    part_keys: &[String],
    values: &[String],
) -> Result<String> {
    for (key, value) in part_keys.iter().zip(values.iter()) {
        validate_partition_value(key, value)?;
    }
    let dirs = part_keys
        .iter()
        .zip(values)
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("/");
    Ok(format!("{}/{dirs}/", table_location.trim_end_matches('/')))
}

/// Parse a directory path (relative to the table root, e.g. `["dt=2024-01", "hr=05"]`) into
/// partition values when every component is `{key}={value}` for the table's partition keys, in
/// order — the Hive layout MSCK REPAIR discovers. Anything else (wrong depth, non-`key=value`
/// component, wrong key) is not a partition directory and yields `None`.
fn partition_values_from_dirs(dirs: &[&str], part_keys: &[String]) -> Option<Vec<String>> {
    if dirs.len() != part_keys.len() {
        return None;
    }
    dirs.iter()
        .zip(part_keys)
        .map(|(dir, key)| {
            let (k, v) = dir.split_once('=')?;
            if k != key {
                return None;
            }
            validate_partition_value(key, v).ok()?;
            Some(v.to_string())
        })
        .collect()
}

/// Scan a table's storage `location` for Hive-style partition directories and return the
/// discovered partition-value tuples (deduplicated, sorted). Uses `object_store` — `s3://`
/// resolves credentials through the standard AWS chain, `file://` works locally — the same
/// library the engine's readers list with, so no second storage client is needed.
async fn discover_partitions(location: &str, part_keys: &[String]) -> Result<Vec<Vec<String>>> {
    use futures::TryStreamExt;
    use object_store::ObjectStore;

    let url = url::Url::parse(location)
        .map_err(|e| Error::Plan(format!("invalid table location `{location}`: {e}")))?;
    let (store, prefix) = object_store::parse_url(&url)
        .map_err(|e| Error::Io(format!("open object store for `{location}`: {e}")))?;
    let objects: Vec<object_store::ObjectMeta> = store
        .list(Some(&prefix))
        .try_collect()
        .await
        .map_err(|e| Error::Io(format!("list `{location}`: {e}")))?;

    let mut found = std::collections::BTreeSet::new();
    for meta in objects {
        let Some(rel) = meta.location.prefix_match(&prefix) else {
            continue;
        };
        // The partition directory is the path holding the data file — every component but the
        // file name.
        let rel_parts: Vec<object_store::path::PathPart<'_>> = rel.collect();
        let mut dirs: Vec<&str> = rel_parts.iter().map(|p| p.as_ref()).collect();
        dirs.pop();
        if let Some(values) = partition_values_from_dirs(&dirs, part_keys) {
            found.insert(values);
        }
    }
    Ok(found.into_iter().collect())
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
    use oxidant_catalog::arrow::datatypes::{DataType, Field};

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
    fn build_table_input_labels_iceberg_with_the_table_type_athena_reads() {
        // An Iceberg entry is Iceberg because of `table_type`, not its SerDe. `metadata_location`
        // is added separately by whatever commits a snapshot — for a streaming table, the Iceberg
        // metadata published alongside the Delta log.
        let schema = sample_schema();
        let input =
            build_table_input("t", "s3://bucket/t/", &schema, TableFormat::Iceberg, &[]).unwrap();
        let params = input.parameters().expect("parameters");
        assert_eq!(
            params.get("table_type").map(String::as_str),
            Some("ICEBERG")
        );
        assert_eq!(detect_format(params), TableFormat::Iceberg);
    }

    #[test]
    fn build_table_input_labels_delta_so_spark_and_athena_read_the_transaction_log() {
        let schema = sample_schema();
        let input =
            build_table_input("t", "s3://bucket/t/", &schema, TableFormat::Delta, &[]).unwrap();
        let params = input.parameters().expect("parameters");
        // A Delta table's SerDe is identical to a plain Parquet table's — this parameter is the
        // only thing that tells a reader to consult `_delta_log/` instead of listing the
        // directory. `detect_format` reads it back on the way in.
        assert_eq!(
            params.get("spark.sql.sources.provider").map(String::as_str),
            Some("delta")
        );
        assert_eq!(
            params.get("classification").map(String::as_str),
            Some("delta")
        );
        assert_eq!(
            input
                .storage_descriptor()
                .and_then(|sd| sd.serde_info())
                .and_then(|s| s.serialization_library()),
            Some("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe")
        );
    }

    #[test]
    fn a_delta_table_written_by_build_table_input_round_trips_through_detect_format() {
        let schema = sample_schema();
        let input =
            build_table_input("t", "s3://bucket/t/", &schema, TableFormat::Delta, &[]).unwrap();
        let params: HashMap<String, String> = input
            .parameters()
            .expect("parameters")
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        assert_eq!(detect_format(&params), TableFormat::Delta);
    }

    // `apply_table_changes` backs `alter_table`'s in-memory mutation of the fetched
    // `TableInput` before `UpdateTable` is sent — pure and unit-testable without an endpoint.
    // The stub-endpoint `alter_table_*` tests below cover the same logic end to end through the
    // real SDK request/response cycle; these cover every change kind and its error paths in
    // isolation.

    fn bare_table_input(name: &str) -> TableInput {
        TableInput::builder().name(name).build().expect("input")
    }

    #[test]
    fn apply_table_changes_sets_unsets_properties_comment_location_and_columns() {
        let mut input = TableInput::builder()
            .name("t")
            .storage_descriptor(
                StorageDescriptor::builder()
                    .location("s3://x/old/")
                    .columns(col("id", "bigint"))
                    .build(),
            )
            .parameters("keep", "1")
            .parameters("drop_me", "2")
            .build()
            .expect("input");

        apply_table_changes(
            &mut input,
            &[
                TableChange::SetProperties(HashMap::from([(
                    "added".to_string(),
                    "yes".to_string(),
                )])),
                TableChange::UnsetProperties(vec!["drop_me".to_string(), "absent".to_string()]),
                TableChange::SetComment(Some("hi".to_string())),
                TableChange::SetLocation("s3://x/new/".to_string()),
                TableChange::AddColumns(vec![Field::new("region", DataType::Utf8, true)]),
            ],
        )
        .expect("apply");

        let params = input.parameters().expect("params");
        assert_eq!(params.get("keep").map(String::as_str), Some("1"));
        assert_eq!(params.get("added").map(String::as_str), Some("yes"));
        assert!(
            !params.contains_key("drop_me"),
            "unset property must be removed"
        );
        assert_eq!(input.description(), Some("hi"));
        let sd = input.storage_descriptor().expect("storage descriptor");
        assert_eq!(sd.location(), Some("s3://x/new/"));
        let names: Vec<&str> = sd.columns().iter().map(|c| c.name()).collect();
        assert_eq!(
            names,
            vec!["id", "region"],
            "added column appended, not replacing existing ones"
        );
    }

    #[test]
    fn apply_table_changes_clears_comment_with_empty_string_and_none() {
        let mut input = bare_table_input("t");
        apply_table_changes(&mut input, &[TableChange::SetComment(Some(String::new()))])
            .expect("apply");
        assert_eq!(
            input.description(),
            None,
            "empty comment clears, doesn't set \"\""
        );
        apply_table_changes(&mut input, &[TableChange::SetComment(None)]).expect("apply");
        assert_eq!(input.description(), None);
    }

    #[test]
    fn apply_table_changes_unset_of_absent_key_is_a_noop() {
        let mut input = TableInput::builder()
            .name("t")
            .parameters("keep", "1")
            .build()
            .expect("input");
        apply_table_changes(
            &mut input,
            &[TableChange::UnsetProperties(vec!["never_set".to_string()])],
        )
        .expect("apply");
        assert_eq!(
            input
                .parameters()
                .and_then(|p| p.get("keep"))
                .map(String::as_str),
            Some("1")
        );
    }

    #[test]
    fn apply_table_changes_set_location_without_storage_descriptor_errors() {
        let mut input = bare_table_input("t");
        let err = apply_table_changes(&mut input, &[TableChange::SetLocation("s3://x/".into())])
            .unwrap_err();
        assert!(matches!(err, Error::Plan(_)), "{err:?}");
    }

    #[test]
    fn apply_table_changes_set_location_normalizes_trailing_slash() {
        let mut input = TableInput::builder()
            .name("t")
            .storage_descriptor(StorageDescriptor::builder().location("s3://x/old/").build())
            .build()
            .expect("input");
        apply_table_changes(
            &mut input,
            &[TableChange::SetLocation("s3://x/new".to_string())],
        )
        .expect("apply");
        assert_eq!(
            input
                .storage_descriptor()
                .and_then(|sd| sd.location())
                .map(str::to_string),
            Some("s3://x/new/".to_string())
        );
    }

    #[test]
    fn apply_table_changes_add_columns_without_storage_descriptor_errors() {
        let mut input = bare_table_input("t");
        let err = apply_table_changes(
            &mut input,
            &[TableChange::AddColumns(vec![Field::new(
                "c",
                DataType::Int64,
                true,
            )])],
        )
        .unwrap_err();
        assert!(matches!(err, Error::Plan(_)), "{err:?}");
    }

    #[test]
    fn apply_table_changes_add_columns_unsupported_type_errors_and_leaves_input_untouched() {
        let mut input = TableInput::builder()
            .name("t")
            .storage_descriptor(StorageDescriptor::builder().location("s3://x/").build())
            .build()
            .expect("input");
        let unrepresentable = DataType::List(Arc::new(Field::new("item", DataType::Int32, true)));
        let err = apply_table_changes(
            &mut input,
            &[TableChange::AddColumns(vec![Field::new(
                "tags",
                unrepresentable,
                true,
            )])],
        )
        .unwrap_err();
        assert!(matches!(err, Error::Unsupported(_)), "{err:?}");
        // The rejected change must leave the table definition untouched (the trait contract).
        assert!(input
            .storage_descriptor()
            .expect("storage descriptor")
            .columns()
            .is_empty());
    }

    // `table_to_input` feeds `alter_table`'s `UpdateTable` — everything Glue tracks on the
    // fetched `Table` must ride along or `UpdateTable` (which replaces the whole definition)
    // would silently drop it.

    #[test]
    fn table_to_input_copies_every_field_glue_tracks() {
        let t = Table::builder()
            .name("t")
            .description("a table")
            .owner("me")
            .retention(30)
            .storage_descriptor(StorageDescriptor::builder().location("s3://x/").build())
            .set_partition_keys(Some(vec![col("dt", "string")]))
            .table_type("EXTERNAL_TABLE")
            .set_parameters(Some(params(&[("k", "v")])))
            .build()
            .expect("table");
        let input = table_to_input(&t).expect("input");
        assert_eq!(input.name(), "t");
        assert_eq!(input.description(), Some("a table"));
        assert_eq!(input.owner(), Some("me"));
        assert_eq!(input.retention(), 30);
        assert_eq!(input.table_type(), Some("EXTERNAL_TABLE"));
        assert_eq!(
            input
                .parameters()
                .and_then(|p| p.get("k"))
                .map(String::as_str),
            Some("v")
        );
        assert_eq!(input.partition_keys().len(), 1);
        assert_eq!(
            input.storage_descriptor().and_then(|sd| sd.location()),
            Some("s3://x/")
        );
    }

    // `partition_values_from_dirs` / `partition_location` back `repair_table`'s Hive-layout
    // partition discovery — pure helpers, unit-testable without an object store.

    #[test]
    fn partition_values_from_dirs_matches_hive_layout() {
        let keys = vec!["year".to_string(), "month".to_string()];
        assert_eq!(
            partition_values_from_dirs(&["year=2024", "month=01"], &keys),
            Some(vec!["2024".to_string(), "01".to_string()])
        );
    }

    #[test]
    fn partition_values_from_dirs_rejects_wrong_key_order_or_depth() {
        let keys = vec!["year".to_string(), "month".to_string()];
        // Right depth, wrong key name at that position.
        assert_eq!(
            partition_values_from_dirs(&["month=01", "year=2024"], &keys),
            None
        );
        // Missing a segment.
        assert_eq!(partition_values_from_dirs(&["year=2024"], &keys), None);
        // Extra segment.
        assert_eq!(
            partition_values_from_dirs(&["year=2024", "month=01", "day=05"], &keys),
            None
        );
        // Not a `key=value` component at all.
        assert_eq!(
            partition_values_from_dirs(&["year=2024", "01"], &keys),
            None
        );
        // Path traversal in a value must not be treated as a partition.
        assert_eq!(
            partition_values_from_dirs(&["dt=../../outside"], &["dt".to_string()]),
            None
        );
    }

    #[test]
    fn partition_location_builds_hive_style_path() {
        let keys = vec!["year".to_string(), "month".to_string()];
        let values = vec!["2024".to_string(), "01".to_string()];
        assert_eq!(
            partition_location("s3://bucket/db/t", &keys, &values).expect("location"),
            "s3://bucket/db/t/year=2024/month=01/"
        );
        // A trailing slash on the table location is trimmed, not doubled.
        assert_eq!(
            partition_location("s3://bucket/db/t/", &keys, &values).expect("location"),
            "s3://bucket/db/t/year=2024/month=01/"
        );
    }

    #[test]
    fn partition_location_rejects_path_traversal_in_values() {
        let keys = vec!["dt".to_string()];
        let err = partition_location("s3://bucket/db/t", &keys, &["../../outside".to_string()])
            .unwrap_err();
        assert!(matches!(err, Error::Plan(_)), "{err:?}");
    }

    // `discover_partitions` scans a real storage location for Hive-style `key=value/`
    // directories via `object_store` — exercised here against a local `file://` tempdir so the
    // test needs no network, mirroring how `repair_table` will resolve a table's actual location
    // in production (S3 there, local disk here).

    #[tokio::test]
    async fn discover_partitions_finds_hive_style_directories() {
        let dir = tempfile::tempdir().expect("tempdir");
        for part in ["dt=2024-01-01", "dt=2024-01-02"] {
            let p = dir.path().join(part);
            std::fs::create_dir_all(&p).expect("mkdir");
            std::fs::write(p.join("part-0.parquet"), b"x").expect("write");
        }
        // A stray file directly under the table root (zero-depth, not inside a `key=value/`
        // dir) must be ignored rather than misread as a partition.
        std::fs::write(dir.path().join("_SUCCESS"), b"").expect("write");

        let location = url::Url::from_directory_path(dir.path())
            .expect("file url")
            .to_string();
        let found = discover_partitions(&location, &["dt".to_string()])
            .await
            .expect("discover");
        assert_eq!(
            found,
            vec![
                vec!["2024-01-01".to_string()],
                vec!["2024-01-02".to_string()]
            ]
        );
    }

    #[tokio::test]
    async fn discover_partitions_ignores_non_hive_directories() {
        let dir = tempfile::tempdir().expect("tempdir");
        // Not a `key=value` component at all.
        let not_partition = dir.path().join("not_a_partition");
        std::fs::create_dir_all(&not_partition).expect("mkdir");
        std::fs::write(not_partition.join("f.parquet"), b"x").expect("write");
        // Right shape, wrong key name for this table's partition columns.
        let wrong_key = dir.path().join("year=2024");
        std::fs::create_dir_all(&wrong_key).expect("mkdir");
        std::fs::write(wrong_key.join("f.parquet"), b"x").expect("write");
        // Path traversal in a partition value must not be registered.
        let traversal = dir.path().join("dt=../../outside");
        std::fs::create_dir_all(&traversal).expect("mkdir");
        std::fs::write(traversal.join("f.parquet"), b"x").expect("write");

        let location = url::Url::from_directory_path(dir.path())
            .expect("file url")
            .to_string();
        let found = discover_partitions(&location, &["dt".to_string()])
            .await
            .expect("discover");
        assert!(found.is_empty(), "{found:?}");
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

    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    use aws_sdk_glue::config::{BehaviorVersion, Credentials, Region, SharedCredentialsProvider};

    // Page 1 carries a `NextToken`; the page-2 responses only come back when the follow-up
    // request threads the token through, so the two-page assertions double as proof the
    // manual pagination loop works (Glue silently truncates at its 100-item page size
    // otherwise).
    const DATABASES_PAGE1_JSON: &str = r#"{"DatabaseList":[{"Name":"db1"}],"NextToken":"p2"}"#;
    const DATABASES_PAGE2_JSON: &str = r#"{"DatabaseList":[{"Name":"db2"}]}"#;
    const TABLES_PAGE1_JSON: &str = r#"{"TableList":[{"Name":"orders"}],"NextToken":"p2"}"#;
    const TABLES_PAGE2_JSON: &str = r#"{"TableList":[{"Name":"customers"}]}"#;
    const ORDERS_JSON: &str = r#"{"Table":{"Name":"orders","StorageDescriptor":{"Location":"s3://bucket/db1/orders/","Columns":[{"Name":"id","Type":"bigint"}]},"PartitionKeys":[{"Name":"dt","Type":"string"}],"Parameters":{"classification":"parquet"}}}"#;
    const ICEBERG_JSON: &str = r#"{"Table":{"Name":"iceberg_t","StorageDescriptor":{"Location":"s3://bucket/db1/iceberg_t/","Columns":[{"Name":"id","Type":"bigint"}]},"PartitionKeys":[],"Parameters":{"table_type":"ICEBERG","metadata_location":"s3://bucket/db1/iceberg_t/metadata/00010-abc.metadata.json"}}}"#;
    const NOT_FOUND_JSON: &str =
        r#"{"__type":"EntityNotFoundException","message":"Table nope not found"}"#;

    /// Read one AWS JSON 1.1 request (headers, then exactly `Content-Length` body bytes) off an
    /// accepted stub connection. Shared by every stub-endpoint test below. An empty string means
    /// the peer closed before sending anything (the connection should just be dropped).
    async fn read_stub_request(sock: &mut tokio::net::TcpStream) -> String {
        use tokio::io::AsyncReadExt;
        let mut buf = Vec::new();
        let mut chunk = [0_u8; 8192];
        let header_end = loop {
            let n = sock.read(&mut chunk).await.expect("read");
            if n == 0 {
                return String::new();
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
        String::from_utf8_lossy(&buf).to_string()
    }

    /// Write an AWS JSON 1.1 response (`status` like `"200 OK"` / `"400 Bad Request"`) back to a
    /// stub connection.
    async fn write_stub_response(sock: &mut tokio::net::TcpStream, status: &str, body: &str) {
        use tokio::io::AsyncWriteExt;
        let response = format!(
            "HTTP/1.1 {status}\r\ncontent-type: application/x-amz-json-1.1\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
            body.len()
        );
        let _ = sock.write_all(response.as_bytes()).await;
    }

    /// Bind a stub Glue endpoint on a loopback ephemeral port; each accepted connection reads
    /// one request and answers from the `x-amz-target` / body. Returns the port; the server task
    /// runs until aborted.
    async fn spawn_glue_stub() -> u16 {
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
                    let request = read_stub_request(&mut sock).await;
                    if request.is_empty() {
                        return;
                    }
                    let page2 = request.contains(r#""NextToken":"p2""#);
                    // NB: match `GetTables` before `GetTable` — the latter is a substring.
                    let (status, body) = if request.contains("GetDatabases") {
                        if page2 {
                            ("200 OK", DATABASES_PAGE2_JSON)
                        } else {
                            ("200 OK", DATABASES_PAGE1_JSON)
                        }
                    } else if request.contains("GetTables") {
                        if page2 {
                            ("200 OK", TABLES_PAGE2_JSON)
                        } else {
                            ("200 OK", TABLES_PAGE1_JSON)
                        }
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
                    write_stub_response(&mut sock, status, body).await;
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

        // GetDatabases → namespaces across BOTH pages (page 1 carries NextToken=p2; page 2 only
        // answers when the follow-up request threads the token through). A non-empty parent
        // short-circuits without a call.
        let namespaces = cat.list_namespaces(&[]).await.expect("databases");
        assert_eq!(
            namespaces,
            vec![vec!["db1".to_string()], vec!["db2".to_string()]],
            "both paginated pages"
        );
        assert!(cat
            .list_namespaces(&["db1".to_string()])
            .await
            .expect("flat")
            .is_empty());

        // GetTables → table names in the database, again across both pages.
        let tables = cat.list_tables(&["db1".to_string()]).await.expect("tables");
        assert_eq!(
            tables,
            vec!["orders".to_string(), "customers".to_string()],
            "both paginated pages"
        );

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

    // ---------------------------------------------------------------------
    // KAN-100 DDL: CREATE/DROP DATABASE, DROP TABLE (IF EXISTS / CASCADE), ALTER TABLE,
    // SHOW PARTITIONS, REPAIR TABLE — each against the same stub-endpoint pattern above, plus
    // the `IF EXISTS`/`IF NOT EXISTS`/`CASCADE` branches and the error-propagation paths (KAN-83
    // principle: a backend failure must never masquerade as "doesn't exist").
    // ---------------------------------------------------------------------

    const PARTITIONS_PAGE1_JSON: &str =
        r#"{"Partitions":[{"Values":["2024-01-01"]}],"NextToken":"p2"}"#;
    const PARTITIONS_PAGE2_JSON: &str = r#"{"Partitions":[{"Values":["2024-01-02"]}]}"#;
    const PARTITIONS_SINGLE_EXISTING_JSON: &str = r#"{"Partitions":[{"Values":["2024-01-01"]}]}"#;

    const ALTER_BEFORE_JSON: &str = r#"{"Table":{"Name":"orders_alter","StorageDescriptor":{"Location":"s3://bucket/db1/orders_alter/","Columns":[{"Name":"id","Type":"bigint"}]},"PartitionKeys":[],"Parameters":{"keep_me":"1","drop_me":"2"}}}"#;
    const ALTER_AFTER_JSON: &str = r#"{"Table":{"Name":"orders_alter","StorageDescriptor":{"Location":"s3://bucket/db1/orders_alter_new/","Columns":[{"Name":"id","Type":"bigint"},{"Name":"region","Type":"string"}]},"PartitionKeys":[],"Parameters":{"keep_me":"1","new_prop":"v1"},"Description":"updated comment"}}"#;

    /// Stub for `CREATE DATABASE`, `DROP DATABASE` (`IF EXISTS`/`CASCADE`), and `DROP TABLE`
    /// (`IF EXISTS`): dispatches on the database/table name embedded in the request body (the
    /// same "match on a name/marker substring" idiom `spawn_glue_stub` uses for `nope`/
    /// `iceberg_t`). `delete_table_calls` counts `DeleteTable` requests that hit the default
    /// (success) branch — the `DROP DATABASE ... CASCADE` test uses it to prove the member
    /// tables were actually dropped before the database itself.
    async fn spawn_ddl_stub(delete_table_calls: Arc<AtomicUsize>) -> u16 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind stub");
        let port = listener.local_addr().expect("local addr").port();
        tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else {
                    return;
                };
                let delete_table_calls = delete_table_calls.clone();
                tokio::spawn(async move {
                    let request = read_stub_request(&mut sock).await;
                    if request.is_empty() {
                        return;
                    }
                    let page2 = request.contains(r#""NextToken":"p2""#);
                    // NB: match `GetTables` before any bare `Table`/`Database` check below.
                    let (status, body) = if request.contains("GetTables") {
                        if request.contains(r#""DatabaseName":"missing""#) {
                            (
                                "400 Bad Request",
                                r#"{"__type":"EntityNotFoundException","message":"Database not found"}"#,
                            )
                        } else if page2 {
                            ("200 OK", TABLES_PAGE2_JSON)
                        } else {
                            ("200 OK", TABLES_PAGE1_JSON)
                        }
                    } else if request.contains("CreateDatabase") {
                        if request.contains(r#""Name":"dup""#) {
                            (
                                "400 Bad Request",
                                r#"{"__type":"AlreadyExistsException","message":"Database already exists"}"#,
                            )
                        } else if request.contains(r#""Name":"boom""#) {
                            (
                                "400 Bad Request",
                                r#"{"__type":"AccessDeniedException","message":"User is not authorized"}"#,
                            )
                        } else {
                            ("200 OK", "{}")
                        }
                    } else if request.contains("DeleteDatabase") {
                        if request.contains(r#""Name":"missing""#) {
                            (
                                "400 Bad Request",
                                r#"{"__type":"EntityNotFoundException","message":"Database not found"}"#,
                            )
                        } else if request.contains(r#""Name":"boom""#) {
                            (
                                "400 Bad Request",
                                r#"{"__type":"AccessDeniedException","message":"User is not authorized"}"#,
                            )
                        } else {
                            ("200 OK", "{}")
                        }
                    } else if request.contains("DeleteTable") {
                        if request.contains(r#""Name":"missingtable""#) {
                            (
                                "400 Bad Request",
                                r#"{"__type":"EntityNotFoundException","message":"Table not found"}"#,
                            )
                        } else if request.contains(r#""Name":"boomtable""#) {
                            (
                                "400 Bad Request",
                                r#"{"__type":"AccessDeniedException","message":"User is not authorized"}"#,
                            )
                        } else {
                            delete_table_calls.fetch_add(1, Ordering::SeqCst);
                            ("200 OK", "{}")
                        }
                    } else {
                        (
                            "400 Bad Request",
                            r#"{"__type":"ValidationException","message":"?"}"#,
                        )
                    };
                    write_stub_response(&mut sock, status, body).await;
                });
            }
        });
        port
    }

    #[tokio::test]
    async fn create_database_succeeds_and_if_not_exists_swallows_a_duplicate() {
        let port = spawn_ddl_stub(Arc::new(AtomicUsize::new(0))).await;
        let cat = stub_catalog(port);

        cat.create_database("newdb", false, None, None)
            .await
            .expect("plain create");

        // IF NOT EXISTS turns a duplicate into a no-op...
        cat.create_database("dup", true, None, None)
            .await
            .expect("if not exists is a no-op on a duplicate");
        // ...but without it, the same duplicate is a real error.
        let err = cat
            .create_database("dup", false, None, None)
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Io(_)), "{err:?}");
    }

    #[tokio::test]
    async fn create_database_generic_error_propagates_regardless_of_if_not_exists() {
        let port = spawn_ddl_stub(Arc::new(AtomicUsize::new(0))).await;
        let cat = stub_catalog(port);
        // AccessDenied is not "already exists" — IF NOT EXISTS must not swallow it either.
        for if_not_exists in [false, true] {
            let err = cat
                .create_database("boom", if_not_exists, None, None)
                .await
                .unwrap_err();
            assert!(matches!(err, Error::Io(_)), "{err:?}");
        }
    }

    #[tokio::test]
    async fn drop_database_succeeds_and_if_exists_swallows_a_missing_database() {
        let port = spawn_ddl_stub(Arc::new(AtomicUsize::new(0))).await;
        let cat = stub_catalog(port);

        cat.drop_database("existingdb", false, false)
            .await
            .expect("plain drop");

        cat.drop_database("missing", true, false)
            .await
            .expect("if exists is a no-op on a missing database");
        let err = cat
            .drop_database("missing", false, false)
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Plan(_)), "{err:?}");
    }

    #[tokio::test]
    async fn drop_database_generic_error_is_never_swallowed_by_if_exists() {
        let port = spawn_ddl_stub(Arc::new(AtomicUsize::new(0))).await;
        let cat = stub_catalog(port);
        // KAN-83 principle: an access/backend error must not masquerade as "doesn't exist",
        // even under IF EXISTS.
        let err = cat.drop_database("boom", true, false).await.unwrap_err();
        assert!(matches!(err, Error::Io(_)), "{err:?}");
    }

    #[tokio::test]
    async fn drop_database_cascade_deletes_member_tables_before_the_database() {
        let delete_table_calls = Arc::new(AtomicUsize::new(0));
        let port = spawn_ddl_stub(delete_table_calls.clone()).await;
        let cat = stub_catalog(port);

        cat.drop_database("cascadedb", false, true)
            .await
            .expect("cascade drop");

        // `GetTables` (paginated) returned "orders" and "customers"; CASCADE must have dropped
        // both via `DeleteTable` before `DeleteDatabase` ran (Glue itself refuses to delete a
        // non-empty database and has no cascade flag).
        assert_eq!(delete_table_calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn drop_database_if_exists_cascade_on_missing_database_is_noop() {
        let port = spawn_ddl_stub(Arc::new(AtomicUsize::new(0))).await;
        let cat = stub_catalog(port);

        cat.drop_database("missing", true, true)
            .await
            .expect("if exists cascade on a missing database is a no-op");
    }

    #[tokio::test]
    async fn drop_table_succeeds_and_if_exists_swallows_a_missing_table() {
        let port = spawn_ddl_stub(Arc::new(AtomicUsize::new(0))).await;
        let cat = stub_catalog(port);

        cat.drop_table(&["db1".to_string()], "orders", false)
            .await
            .expect("plain drop");

        cat.drop_table(&["db1".to_string()], "missingtable", true)
            .await
            .expect("if exists is a no-op on a missing table");
        let err = cat
            .drop_table(&["db1".to_string()], "missingtable", false)
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Plan(_)), "{err:?}");
    }

    #[tokio::test]
    async fn drop_table_generic_error_is_never_swallowed_by_if_exists() {
        let port = spawn_ddl_stub(Arc::new(AtomicUsize::new(0))).await;
        let cat = stub_catalog(port);
        for if_exists in [false, true] {
            let err = cat
                .drop_table(&["db1".to_string()], "boomtable", if_exists)
                .await
                .unwrap_err();
            assert!(matches!(err, Error::Io(_)), "{err:?}");
        }
    }

    /// Stub for `alter_table`: the first `GetTable` returns the pre-alter definition, `UpdateTable`
    /// captures its raw request body (the request-construction contract, asserted directly), and
    /// the second `GetTable` — the post-alter `load_table` re-fetch `alter_table` does to return
    /// what Glue actually stored — answers with the post-alter definition, proving the trait
    /// contract isn't just an echo of the request.
    async fn spawn_alter_stub(update_calls: Arc<Mutex<Vec<String>>>) -> u16 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind stub");
        let port = listener.local_addr().expect("local addr").port();
        let get_table_calls = Arc::new(AtomicUsize::new(0));
        tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else {
                    return;
                };
                let update_calls = update_calls.clone();
                let get_table_calls = get_table_calls.clone();
                tokio::spawn(async move {
                    let request = read_stub_request(&mut sock).await;
                    if request.is_empty() {
                        return;
                    }
                    // NB: match `UpdateTable` before `GetTable` — not a substring here, but kept
                    // first for symmetry with the other stubs' ordering comments.
                    if request.contains("UpdateTable") {
                        update_calls.lock().expect("lock").push(request.clone());
                        write_stub_response(&mut sock, "200 OK", "{}").await;
                    } else if request.contains("GetTable") {
                        let body = if get_table_calls.fetch_add(1, Ordering::SeqCst) == 0 {
                            ALTER_BEFORE_JSON
                        } else {
                            ALTER_AFTER_JSON
                        };
                        write_stub_response(&mut sock, "200 OK", body).await;
                    } else {
                        write_stub_response(
                            &mut sock,
                            "400 Bad Request",
                            r#"{"__type":"ValidationException","message":"?"}"#,
                        )
                        .await;
                    }
                });
            }
        });
        port
    }

    #[tokio::test]
    async fn alter_table_applies_changes_and_returns_what_glue_actually_stored() {
        let update_calls = Arc::new(Mutex::new(Vec::new()));
        let port = spawn_alter_stub(update_calls.clone()).await;
        let cat = stub_catalog(port);

        let changes = vec![
            TableChange::SetProperties(HashMap::from([("new_prop".to_string(), "v1".to_string())])),
            TableChange::UnsetProperties(vec!["drop_me".to_string()]),
            TableChange::SetComment(Some("updated comment".to_string())),
            TableChange::SetLocation("s3://bucket/db1/orders_alter_new/".to_string()),
            TableChange::AddColumns(vec![Field::new("region", DataType::Utf8, true)]),
        ];
        let md = cat
            .alter_table(&["db1".to_string()], "orders_alter", changes)
            .await
            .expect("alter");

        // The returned metadata is the post-alter `GetTable` (ALTER_AFTER_JSON) — what Glue
        // actually stored — not an echo of the request.
        assert_eq!(md.location, "s3://bucket/db1/orders_alter_new/");
        assert_eq!(md.comment.as_deref(), Some("updated comment"));
        assert_eq!(
            md.properties.get("new_prop").map(String::as_str),
            Some("v1")
        );
        assert!(!md.properties.contains_key("drop_me"));
        let schema = md.schema.expect("schema");
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(1).name(), "region");

        // Exactly one `UpdateTable` call, and its request body independently carries the
        // merged/removed properties, new comment, new location, and appended column.
        let calls = update_calls.lock().expect("lock");
        assert_eq!(calls.len(), 1);
        let body = &calls[0];
        assert!(body.contains(r#""new_prop":"v1""#), "{body}");
        assert!(!body.contains("drop_me"), "{body}");
        assert!(body.contains(r#""keep_me":"1""#), "{body}");
        assert!(
            body.contains(r#""Description":"updated comment""#),
            "{body}"
        );
        assert!(
            body.contains(r#""Location":"s3://bucket/db1/orders_alter_new/""#),
            "{body}"
        );
        assert!(body.contains(r#""Name":"region""#), "{body}");
    }

    #[tokio::test]
    async fn alter_table_rejects_unsupported_column_before_update_table_is_sent() {
        let update_calls = Arc::new(Mutex::new(Vec::new()));
        let port = spawn_alter_stub(update_calls.clone()).await;
        let cat = stub_catalog(port);

        let unrepresentable = DataType::List(Arc::new(Field::new("item", DataType::Int32, true)));
        let err = cat
            .alter_table(
                &["db1".to_string()],
                "orders_alter",
                vec![TableChange::AddColumns(vec![Field::new(
                    "tags",
                    unrepresentable,
                    true,
                )])],
            )
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Unsupported(_)), "{err:?}");

        // The rejected change must be caught before any `UpdateTable` call — the table stays
        // untouched (the trait contract).
        assert!(update_calls.lock().expect("lock").is_empty());
    }

    async fn spawn_partitions_stub() -> u16 {
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
                    let request = read_stub_request(&mut sock).await;
                    if request.is_empty() {
                        return;
                    }
                    let page2 = request.contains(r#""NextToken":"p2""#);
                    let body = if page2 {
                        PARTITIONS_PAGE2_JSON
                    } else {
                        PARTITIONS_PAGE1_JSON
                    };
                    write_stub_response(&mut sock, "200 OK", body).await;
                });
            }
        });
        port
    }

    #[tokio::test]
    async fn list_partitions_paginates() {
        let port = spawn_partitions_stub().await;
        let cat = stub_catalog(port);
        let partitions = cat
            .list_partitions(&["db1".to_string()], "orders")
            .await
            .expect("partitions");
        assert_eq!(
            partitions,
            vec![
                vec!["2024-01-01".to_string()],
                vec!["2024-01-02".to_string()]
            ],
            "both paginated pages"
        );
    }

    /// Stub for `repair_table`: `GetTable` answers with `get_table_body` (built per-test so it
    /// can point `Location` at a real local tempdir), `GetPartitions` answers with
    /// `get_partitions_body` (the partitions Glue already knows about), and `BatchCreatePartition`
    /// captures its request body into `batch_create_calls` instead of asserting anything itself —
    /// letting each test check exactly which partition values were (or weren't) sent.
    async fn spawn_repair_stub(
        get_table_body: String,
        get_partitions_body: &'static str,
        batch_create_calls: Arc<Mutex<Vec<String>>>,
        batch_create_error: Option<&'static str>,
    ) -> u16 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind stub");
        let port = listener.local_addr().expect("local addr").port();
        tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else {
                    return;
                };
                let get_table_body = get_table_body.clone();
                let batch_create_calls = batch_create_calls.clone();
                let batch_create_error = batch_create_error;
                tokio::spawn(async move {
                    let request = read_stub_request(&mut sock).await;
                    if request.is_empty() {
                        return;
                    }
                    // NB: match `BatchCreatePartition` and `GetPartitions` before `GetTable` —
                    // neither is a substring of it, but the ordering mirrors the other stubs.
                    if request.contains("BatchCreatePartition") {
                        batch_create_calls
                            .lock()
                            .expect("lock")
                            .push(request.clone());
                        if let Some(body) = batch_create_error {
                            write_stub_response(&mut sock, "400 Bad Request", body).await;
                        } else {
                            write_stub_response(&mut sock, "200 OK", "{}").await;
                        }
                    } else if request.contains("GetPartitions") {
                        write_stub_response(&mut sock, "200 OK", get_partitions_body).await;
                    } else if request.contains("GetTable") {
                        write_stub_response(&mut sock, "200 OK", &get_table_body).await;
                    } else {
                        write_stub_response(
                            &mut sock,
                            "400 Bad Request",
                            r#"{"__type":"ValidationException","message":"?"}"#,
                        )
                        .await;
                    }
                });
            }
        });
        port
    }

    #[tokio::test]
    async fn repair_table_discovers_and_creates_only_the_missing_partitions() {
        let dir = tempfile::tempdir().expect("tempdir");
        for part in ["dt=2024-01-01", "dt=2024-01-02", "dt=2024-01-03"] {
            let p = dir.path().join(part);
            std::fs::create_dir_all(&p).expect("mkdir");
            std::fs::write(p.join("part-0.parquet"), b"x").expect("write");
        }
        let location = url::Url::from_directory_path(dir.path())
            .expect("file url")
            .to_string();
        let get_table_body = format!(
            r#"{{"Table":{{"Name":"repair_t","StorageDescriptor":{{"Location":"{location}","Columns":[{{"Name":"id","Type":"bigint"}}]}},"PartitionKeys":[{{"Name":"dt","Type":"string"}}],"Parameters":{{"classification":"parquet"}}}}}}"#
        );

        let batch_create_calls = Arc::new(Mutex::new(Vec::new()));
        let port = spawn_repair_stub(
            get_table_body,
            PARTITIONS_SINGLE_EXISTING_JSON,
            batch_create_calls.clone(),
            None,
        )
        .await;
        let cat = stub_catalog(port);

        let added = cat
            .repair_table(&["db1".to_string()], "repair_t")
            .await
            .expect("repair");
        // `dt=2024-01-01` is already registered (GetPartitions); only 01-02 and 01-03 are new.
        assert_eq!(added, 2);

        let calls = batch_create_calls.lock().expect("lock");
        assert_eq!(
            calls.len(),
            1,
            "missing partitions batch into a single call"
        );
        assert!(calls[0].contains(r#""2024-01-02""#), "{}", calls[0]);
        assert!(calls[0].contains(r#""2024-01-03""#), "{}", calls[0]);
        assert!(
            !calls[0].contains(r#""2024-01-01""#),
            "already-registered partition must not be re-created: {}",
            calls[0]
        );
    }

    #[tokio::test]
    async fn repair_table_is_a_noop_on_an_unpartitioned_table() {
        const UNPARTITIONED_JSON: &str = r#"{"Table":{"Name":"flat","StorageDescriptor":{"Location":"s3://bucket/db1/flat/","Columns":[{"Name":"id","Type":"bigint"}]},"PartitionKeys":[],"Parameters":{"classification":"parquet"}}}"#;
        let batch_create_calls = Arc::new(Mutex::new(Vec::new()));
        // `GetPartitions` deliberately answers with a body that isn't valid partition-list JSON:
        // if `repair_table` didn't short-circuit before calling it, the SDK's response parsing
        // would fail and turn the `.expect("repair")` below into a panic — a trip-wire, not a
        // real fixture.
        let port = spawn_repair_stub(
            UNPARTITIONED_JSON.to_string(),
            "not valid partitions JSON",
            batch_create_calls.clone(),
            None,
        )
        .await;
        let cat = stub_catalog(port);

        let added = cat
            .repair_table(&["db1".to_string()], "flat")
            .await
            .expect("repair");
        assert_eq!(added, 0);
        assert!(batch_create_calls.lock().expect("lock").is_empty());
    }

    #[tokio::test]
    async fn repair_table_treats_batch_create_already_exists_as_idempotent_success() {
        let dir = tempfile::tempdir().expect("tempdir");
        let part = dir.path().join("dt=2024-01-02");
        std::fs::create_dir_all(&part).expect("mkdir");
        std::fs::write(part.join("part-0.parquet"), b"x").expect("write");
        let location = url::Url::from_directory_path(dir.path())
            .expect("file url")
            .to_string();
        let get_table_body = format!(
            r#"{{"Table":{{"Name":"repair_t","StorageDescriptor":{{"Location":"{location}","Columns":[{{"Name":"id","Type":"bigint"}}]}},"PartitionKeys":[{{"Name":"dt","Type":"string"}}],"Parameters":{{"classification":"parquet"}}}}}}"#
        );

        let batch_create_calls = Arc::new(Mutex::new(Vec::new()));
        let port = spawn_repair_stub(
            get_table_body,
            r#"{"Partitions":[]}"#,
            batch_create_calls.clone(),
            Some(r#"{"__type":"AlreadyExistsException","message":"Partition already exists"}"#),
        )
        .await;
        let cat = stub_catalog(port);

        let added = cat
            .repair_table(&["db1".to_string()], "repair_t")
            .await
            .expect("repair");
        assert_eq!(added, 1);
        assert_eq!(batch_create_calls.lock().expect("lock").len(), 1);
    }
}

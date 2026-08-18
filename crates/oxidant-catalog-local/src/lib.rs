//! A filesystem / object-store [`CatalogProvider`] for Oxidant.
//!
//! This is the catalog you declare in `oxidant.yaml` when you want to point the engine at
//! directories of data files without standing up a metastore:
//!
//! ```yaml
//! catalogs:
//!   local:
//!     type: local
//!     warehouse: ./warehouse
//!     tables:
//!       raw.events: { format: parquet, location: ./data/events/ }
//!       raw.orders: { format: delta,   location: ./data/orders/ }
//!     discover:
//!       - { namespace: bronze, path: ./data/bronze }
//! ```
//!
//! It matters for more than convenience. `LakeSink::open` — the streaming write path — needs
//! `create_database`, `create_table`, and `alter_table`, and before this crate only the Glue
//! provider implemented them (Hive has no `create_database`; the REST provider has no write DDL
//! at all). So a live table could only ever be materialized into AWS. Implementing the write
//! side here is what lets the whole Kafka → Delta → query loop run on a laptop.
//!
//! Two sources of tables, merged:
//!
//! - **declared** — `tables:` entries from config, which are read-only definitions pointing at
//!   data someone else wrote, and are never modified by DDL;
//! - **managed** — everything in the manifest (`{warehouse}/_oxidant_catalog.json`), created by
//!   `create_table` / `create_database` and mutated by `alter_table` / `drop_*`.
//!
//! A declared table shadows a managed one of the same name, and `create_table` refuses to
//! overwrite a declared entry — config is the operator's statement of intent, and a pipeline
//! must not quietly redefine it.
//!
//! ## What is implemented but unreachable
//!
//! `drop_table`, `drop_database`, and `list_partitions` are implemented here for symmetry with
//! the Glue provider, but **no SQL path routes to them** — `DROP TABLE` and `SHOW PARTITIONS`
//! do not reach the catalog SPI in the engine today. They work when called directly; they are
//! not a working `DROP TABLE`. See `docs/TODOS.md`.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;
use oxidant_catalog::arrow::datatypes::SchemaRef;
use oxidant_catalog::hive_types::{columns_to_schema, schema_to_columns, validate_identifier};
use oxidant_catalog::{CatalogProvider, Error, Result, TableChange, TableFormat, TableMetadata};

mod discover;
mod manifest;
mod store;

use discover::{extension_of, file_stem, sniff_file_format, sniff_format, DirEntry};
use manifest::{table_key, DatabaseEntry, ManifestStore, TableEntry};
pub use store::{store_for_location, table_root_prefix};

/// A table declared in config: a pointer to data the catalog does not own.
#[derive(Debug, Clone)]
pub struct DeclaredTable {
    /// Namespace (database) the table lives in.
    pub namespace: String,
    /// Table name.
    pub table: String,
    /// Physical format.
    pub format: TableFormat,
    /// Table root — a path or a URI.
    pub location: String,
    /// Reader/storage options: CSV `header`/`delimiter`, `s3.*` credentials, and friends.
    pub storage_options: BTreeMap<String, String>,
    /// Partition column names, when the layout should not be inferred.
    pub partition_columns: Vec<String>,
}

/// A directory tree to scan at startup.
#[derive(Debug, Clone)]
pub struct DiscoverRoot {
    /// Namespace the discovered tables land in.
    pub namespace: String,
    /// Directory to scan; each immediate subdirectory becomes one table.
    pub path: String,
    /// Storage credentials / endpoint options for `path`, and inherited by every table found
    /// under it. Without these an `s3://` root falls back to the ambient AWS chain alone, which
    /// on a host with no ambient credentials fails the whole catalog.
    #[allow(clippy::struct_field_names)]
    pub storage_options: BTreeMap<String, String>,
}

impl DiscoverRoot {
    fn storage_options_map(&self) -> HashMap<String, String> {
        self.storage_options.clone().into_iter().collect()
    }
}

/// A catalog over directories, backed by a JSON manifest under its warehouse.
pub struct LocalCatalog {
    name: String,
    warehouse: String,
    /// Config-declared tables, keyed by `(namespace, table)`.
    declared: BTreeMap<(String, String), DeclaredTable>,
    manifest: ManifestStore,
}

impl LocalCatalog {
    /// Build a catalog rooted at `warehouse`, with `declared` tables pre-loaded and each entry
    /// in `discover` scanned for more.
    ///
    /// Discovery happens here, once at construction, rather than per `list_tables` call: a
    /// catalog whose contents changed between two statements of the same query would be a
    /// nasty class of bug, and re-scanning an object store on every metadata call is slow.
    pub async fn new(
        name: impl Into<String>,
        warehouse: impl Into<String>,
        warehouse_options: HashMap<String, String>,
        declared: Vec<DeclaredTable>,
        discover: Vec<DiscoverRoot>,
    ) -> Result<Self> {
        let name = name.into();
        let warehouse = warehouse.into();
        // The warehouse's own credentials, not an empty map: an `s3://` warehouse needs them to
        // read and write the manifest, and falling back to the ambient AWS chain makes a catalog
        // that works on one host and fails to build on another.
        let (store, root) = store_for_location(&warehouse, &warehouse_options)?;
        let mut tables: BTreeMap<(String, String), DeclaredTable> = BTreeMap::new();
        for table in declared {
            tables.insert((table.namespace.clone(), table.table.clone()), table);
        }
        for root in &discover {
            for found in discover_tables(root).await? {
                // A `tables:` entry is an explicit statement and outranks a scan: an operator
                // who pinned a format must not have it overridden by a sniffed one.
                tables
                    .entry((found.namespace.clone(), found.table.clone()))
                    .or_insert(found);
            }
        }
        Ok(Self {
            name,
            warehouse,
            declared: tables,
            manifest: ManifestStore::new(store, root),
        })
    }

    /// The warehouse root new tables are created under.
    pub fn warehouse(&self) -> &str {
        &self.warehouse
    }

    /// Resolve a namespace slice to the single database name this catalog supports.
    ///
    /// Nested namespaces are rejected rather than flattened: `a.b.c` silently becoming `a_b_c`
    /// (or `a`) would put a table somewhere the user did not ask for.
    fn single_db(&self, namespace: &[String]) -> Result<String> {
        match namespace {
            [db] if !db.is_empty() => Ok(db.clone()),
            [] => Err(Error::Plan(format!(
                "catalog `{}` needs a database: use `{}.<database>.<table>`",
                self.name, self.name
            ))),
            _ => Err(Error::Plan(format!(
                "catalog `{}` has single-level databases, but `{}` has {} levels",
                self.name,
                namespace.join("."),
                namespace.len()
            ))),
        }
    }

    /// Default location for a new table: `{warehouse}/{database}.db/{table}/`.
    ///
    /// The Hive/Spark convention, and the same one the Glue provider uses, so a table created
    /// here lands where a reader familiar with either would look for it.
    fn resolve_create_location(
        &self,
        db: &str,
        table: &str,
        location: Option<String>,
    ) -> Result<String> {
        if let Some(location) = location {
            // Trailing slash is load-bearing: a table root is a collection, and DataFusion's
            // `ListingTable` distinguishes a directory from a single file by exactly this.
            return Ok(if location.ends_with('/') {
                location
            } else {
                format!("{location}/")
            });
        }
        // These come from a SQL table reference and are interpolated into a path, so a name
        // like `../../etc` must not escape the warehouse — a real traversal bug for a
        // filesystem warehouse.
        validate_identifier("database", db)?;
        validate_identifier("table", table)?;
        Ok(format!(
            "{}/{db}.db/{table}/",
            self.warehouse.trim_end_matches('/')
        ))
    }

    /// Build read metadata for a declared table.
    fn declared_metadata(&self, entry: &DeclaredTable) -> TableMetadata {
        TableMetadata::new(
            format!("{}.{}.{}", self.name, entry.namespace, entry.table),
            entry.location.clone(),
            entry.format,
        )
        .with_storage_options(entry.storage_options.clone().into_iter().collect())
        .with_partition_columns(entry.partition_columns.clone())
    }

    /// Build read metadata for a managed table.
    fn managed_metadata(&self, db: &str, table: &str, entry: &TableEntry) -> TableMetadata {
        let format = TableFormat::from_provider(&entry.format).unwrap_or(TableFormat::Parquet);
        let mut md = TableMetadata::new(
            format!("{}.{db}.{table}", self.name),
            entry.location.clone(),
            format,
        )
        .with_storage_options(entry.storage_options.clone().into_iter().collect())
        .with_partition_columns(
            entry
                .partition_columns
                .iter()
                .map(|(name, _)| name.clone())
                .collect(),
        );
        // Reattach the schema when every column maps back. All-or-nothing, exactly as the Glue
        // provider does it: a partially-recovered schema would be read positionally and return
        // the wrong column, whereas `None` just means "infer from the data files".
        let all_columns: Vec<(String, String)> = entry
            .columns
            .iter()
            .chain(entry.partition_columns.iter())
            .cloned()
            .collect();
        if let Some(schema) = columns_to_schema(all_columns) {
            md = md.with_schema(Arc::new(schema));
        }
        md.comment = entry.comment.clone();
        md.properties = entry.properties.clone().into_iter().collect();
        md
    }
}

/// Scan one `discover:` root, returning a declared table per recognizable subdirectory.
async fn discover_tables(root: &DiscoverRoot) -> Result<Vec<DeclaredTable>> {
    let (store, prefix) = store_for_location(&root.path, &root.storage_options_map())?;
    // One flat listing of the whole subtree, then group by the first path segment. Listing
    // per candidate directory would be one round trip per table against an object store.
    let mut listing = store.list(Some(&prefix));
    let mut children: BTreeMap<String, Vec<DirEntry>> = BTreeMap::new();
    // Hive partition keys seen under each table, by depth: `dt=2024-01-01/hour=03/f.parquet`
    // gives `{0: "dt", 1: "hour"}`. Kept per depth rather than as a set so the order matches the
    // directory nesting, which is the order the reader has to peel them off in.
    let mut partitions: BTreeMap<String, BTreeMap<usize, String>> = BTreeMap::new();
    let mut bare_files: Vec<String> = Vec::new();
    while let Some(item) = listing.next().await {
        let item = item.map_err(|e| Error::Io(format!("scan `{}`: {e}", root.path)))?;
        let Some(relative) = strip_prefix(&item.location, &prefix) else {
            continue;
        };
        let mut segments = relative.split('/').filter(|s| !s.is_empty());
        let Some(head) = segments.next().map(str::to_string) else {
            continue;
        };
        let rest: Vec<&str> = segments.collect();
        match rest.as_slice() {
            // A file at the scanned root: the table-per-file layout, where the file itself is
            // the table (`parquet/tpch_nation.parquet`).
            [] => bare_files.push(head),
            // A file directly inside a table directory.
            [file] => push_unique(children.entry(head).or_default(), DirEntry::file(file)),
            // Anything deeper means the first segment below the table is a directory — which is
            // how `_delta_log/` and `metadata/` announce themselves.
            [dir, .., file] => {
                let entries = children.entry(head.clone()).or_default();
                push_unique(entries, DirEntry::dir(dir));
                // The leaf's extension too, as a single synthetic entry per extension. Without
                // it a Hive-partitioned directory (`orders/dt=…/part.parquet`) shows only its
                // partition directory, sniffs as nothing, and is silently skipped — which is
                // every partitioned dataset, including the ones Oxidant's own writer produces.
                if let Some(extension) = extension_of(file) {
                    push_unique(entries, DirEntry::file(&format!("nested.{extension}")));
                }
                // Hive partition keys, so the discovered table knows its own partitioning
                // instead of reading the columns back as missing.
                let keys = partitions.entry(head).or_default();
                for (depth, segment) in rest[..rest.len() - 1].iter().enumerate() {
                    if let Some((key, _)) = segment.split_once('=') {
                        keys.entry(depth).or_insert_with(|| key.to_string());
                    }
                }
            }
        }
    }

    let mut out = Vec::new();
    for (table, entries) in children {
        let Some(format) = sniff_format(&entries) else {
            // Not an error: a stray `README.md` or a directory Oxidant cannot read should be
            // skipped, not turned into a table that fails at query time.
            continue;
        };
        // Delta and Iceberg record their own partitioning in their metadata; only the flat
        // formats need it inferred from the paths.
        let partition_columns = match format {
            TableFormat::Delta | TableFormat::Iceberg => Vec::new(),
            _ => partitions
                .get(&table)
                .map(|keys| keys.values().cloned().collect())
                .unwrap_or_default(),
        };
        out.push(DeclaredTable {
            namespace: root.namespace.clone(),
            table: table.clone(),
            format,
            location: join_location(&root.path, &table),
            storage_options: BTreeMap::new(),
            partition_columns,
        });
    }
    for file in bare_files {
        let Some(format) = sniff_file_format(&file) else {
            continue;
        };
        out.push(DeclaredTable {
            namespace: root.namespace.clone(),
            table: file_stem(&file).to_string(),
            format,
            // A single-file table points at the file, not at a directory — so no trailing
            // slash here, unlike every directory-backed table.
            location: format!("{}/{file}", root.path.trim_end_matches('/')),
            storage_options: BTreeMap::new(),
            partition_columns: Vec::new(),
        });
    }
    Ok(out)
}

/// Append `entry` unless it is already present.
fn push_unique(entries: &mut Vec<DirEntry>, entry: DirEntry) {
    if !entries.contains(&entry) {
        entries.push(entry);
    }
}

/// Strip `prefix` from an object path, returning the remainder.
fn strip_prefix(location: &ObjectPath, prefix: &ObjectPath) -> Option<String> {
    let location = location.as_ref();
    let prefix = prefix.as_ref();
    if prefix.is_empty() {
        return Some(location.to_string());
    }
    location
        .strip_prefix(prefix)
        .map(|rest| rest.trim_start_matches('/').to_string())
}

/// Join a table name onto a root, keeping the trailing slash a table location needs.
fn join_location(root: &str, table: &str) -> String {
    format!("{}/{table}/", root.trim_end_matches('/'))
}

#[async_trait]
impl CatalogProvider for LocalCatalog {
    fn name(&self) -> &str {
        &self.name
    }

    async fn list_namespaces(&self, parent: &[String]) -> Result<Vec<Vec<String>>> {
        // Single-level databases, so anything below the top level has no children.
        if !parent.is_empty() {
            return Ok(Vec::new());
        }
        let manifest = self.manifest.load().await?;
        let mut names: std::collections::BTreeSet<String> = self
            .declared
            .keys()
            .map(|(namespace, _)| namespace.clone())
            .collect();
        names.extend(manifest.databases.keys().cloned());
        // A managed table implies its database even if the database entry is missing, so a
        // hand-edited manifest cannot hide tables.
        for key in manifest.tables.keys() {
            if let Some((db, _)) = key.split_once('.') {
                names.insert(db.to_string());
            }
        }
        Ok(names.into_iter().map(|name| vec![name]).collect())
    }

    async fn list_tables(&self, namespace: &[String]) -> Result<Vec<String>> {
        let db = self.single_db(namespace)?;
        let manifest = self.manifest.load().await?;
        let mut names: std::collections::BTreeSet<String> = self
            .declared
            .keys()
            .filter(|(namespace, _)| *namespace == db)
            .map(|(_, table)| table.clone())
            .collect();
        let prefix = format!("{db}.");
        for key in manifest.tables.keys() {
            if let Some(table) = key.strip_prefix(&prefix) {
                names.insert(table.to_string());
            }
        }
        Ok(names.into_iter().collect())
    }

    async fn load_table(&self, namespace: &[String], table: &str) -> Result<TableMetadata> {
        let db = self.single_db(namespace)?;
        if let Some(entry) = self.declared.get(&(db.clone(), table.to_string())) {
            return Ok(self.declared_metadata(entry));
        }
        let manifest = self.manifest.load().await?;
        match manifest.tables.get(&table_key(&db, table)) {
            Some(entry) => Ok(self.managed_metadata(&db, table, entry)),
            // `Error::Plan` is the SPI's "does not exist" signal — `table_exists` reads it as
            // `false`, while an `Error::Io` would propagate as a real failure.
            None => Err(Error::Plan(format!(
                "table `{}.{db}.{table}` not found",
                self.name
            ))),
        }
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
        let db = self.single_db(namespace)?;
        if self.declared.contains_key(&(db.clone(), table.to_string())) {
            return Err(Error::Plan(format!(
                "`{}.{db}.{table}` is declared in configuration and points at data this catalog \
                 does not own; remove it from `tables:` to let the catalog manage it",
                self.name
            )));
        }
        let location = self.resolve_create_location(&db, table, location)?;
        let (columns, partitions) = schema_to_columns(&schema, partition_columns)?;
        let key = table_key(&db, table);
        let entry = TableEntry {
            location: location.clone(),
            format: format_name(format).to_string(),
            columns,
            partition_columns: partitions,
            comment: None,
            properties: BTreeMap::new(),
            storage_options: BTreeMap::new(),
        };
        self.manifest
            .update(|manifest| {
                manifest
                    .databases
                    .entry(db.clone())
                    .or_insert_with(DatabaseEntry::default);
                manifest.tables.insert(key.clone(), entry.clone());
                Ok(())
            })
            .await?;

        Ok(
            TableMetadata::new(format!("{}.{db}.{table}", self.name), location, format)
                .with_schema(schema)
                .with_partition_columns(partition_columns.to_vec()),
        )
    }

    async fn create_database(
        &self,
        database: &str,
        if_not_exists: bool,
        comment: Option<String>,
        location: Option<String>,
    ) -> Result<()> {
        let location = match location {
            Some(location) => Some(location),
            None => {
                validate_identifier("database", database)?;
                Some(format!(
                    "{}/{database}.db/",
                    self.warehouse.trim_end_matches('/')
                ))
            }
        };
        let database = database.to_string();
        self.manifest
            .update(move |manifest| {
                if manifest.databases.contains_key(&database) && !if_not_exists {
                    return Err(Error::Plan(format!("database `{database}` already exists")));
                }
                manifest.databases.insert(
                    database.clone(),
                    DatabaseEntry {
                        comment: comment.clone(),
                        location: location.clone(),
                    },
                );
                Ok(())
            })
            .await
    }

    async fn drop_database(&self, database: &str, if_exists: bool, cascade: bool) -> Result<()> {
        let database = database.to_string();
        self.manifest
            .update(move |manifest| {
                let known = manifest.databases.contains_key(&database);
                let prefix = format!("{database}.");
                let tables: Vec<String> = manifest
                    .tables
                    .keys()
                    .filter(|key| key.starts_with(&prefix))
                    .cloned()
                    .collect();
                if !known && tables.is_empty() {
                    if if_exists {
                        return Ok(());
                    }
                    return Err(Error::Plan(format!("database `{database}` does not exist")));
                }
                if !tables.is_empty() && !cascade {
                    return Err(Error::Plan(format!(
                        "database `{database}` is not empty ({} table(s)); use CASCADE",
                        tables.len()
                    )));
                }
                for key in tables {
                    manifest.tables.remove(&key);
                }
                manifest.databases.remove(&database);
                Ok(())
            })
            .await
    }

    async fn drop_table(&self, namespace: &[String], table: &str, if_exists: bool) -> Result<()> {
        let db = self.single_db(namespace)?;
        if self.declared.contains_key(&(db.clone(), table.to_string())) {
            return Err(Error::Plan(format!(
                "`{}.{db}.{table}` is declared in configuration; remove it from `tables:` rather \
                 than dropping it",
                self.name
            )));
        }
        let key = table_key(&db, table);
        let name = self.name.clone();
        self.manifest
            .update(move |manifest| {
                if manifest.tables.remove(&key).is_none() && !if_exists {
                    return Err(Error::Plan(format!("table `{name}.{key}` does not exist")));
                }
                Ok(())
            })
            .await
        // The data files are deliberately left in place: the catalog records where a table
        // lives, it does not own the bytes, and a DROP that silently deleted an S3 prefix is
        // not something to do without an explicit PURGE.
    }

    async fn alter_table(
        &self,
        namespace: &[String],
        table: &str,
        changes: Vec<TableChange>,
    ) -> Result<TableMetadata> {
        let db = self.single_db(namespace)?;
        if self.declared.contains_key(&(db.clone(), table.to_string())) {
            return Err(Error::Plan(format!(
                "`{}.{db}.{table}` is declared in configuration and cannot be altered; edit the \
                 config file instead",
                self.name
            )));
        }
        let key = table_key(&db, table);
        let name = self.name.clone();
        let updated =
            self.manifest
                .update(move |manifest| {
                    let entry = manifest.tables.get_mut(&key).ok_or_else(|| {
                        Error::Plan(format!("table `{name}.{key}` does not exist"))
                    })?;
                    // Validate every change before applying any: a provider that rejects a change
                    // must leave the table untouched, per the SPI contract.
                    for change in &changes {
                        if let TableChange::AddColumns(fields) = change {
                            for field in fields {
                                oxidant_catalog::hive_types::arrow_type_to_hive(field.data_type())
                                    .ok_or_else(|| {
                                        Error::Unsupported(format!(
                                            "column `{}` has type {:?}, which this catalog cannot \
                                         represent",
                                            field.name(),
                                            field.data_type()
                                        ))
                                    })?;
                            }
                        }
                    }
                    for change in &changes {
                        match change {
                            TableChange::SetProperties(props) => {
                                for (k, v) in props {
                                    entry.properties.insert(k.clone(), v.clone());
                                }
                            }
                            TableChange::UnsetProperties(keys) => {
                                for k in keys {
                                    entry.properties.remove(k);
                                }
                            }
                            TableChange::SetComment(comment) => {
                                entry.comment = comment.clone();
                            }
                            TableChange::SetLocation(location) => {
                                entry.location = location.clone();
                            }
                            TableChange::AddColumns(fields) => {
                                for field in fields {
                                    let ty = oxidant_catalog::hive_types::arrow_type_to_hive(
                                        field.data_type(),
                                    )
                                    .expect("validated above");
                                    entry.columns.push((field.name().clone(), ty));
                                }
                            }
                        }
                    }
                    Ok(entry.clone())
                })
                .await?;
        Ok(self.managed_metadata(&db, table, &updated))
    }
}

/// The provider string a [`TableFormat`] is persisted as.
fn format_name(format: TableFormat) -> &'static str {
    match format {
        TableFormat::Parquet => "parquet",
        TableFormat::Delta => "delta",
        TableFormat::Iceberg => "iceberg",
        TableFormat::Csv => "csv",
        TableFormat::Json => "json",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use oxidant_catalog::arrow::datatypes::{DataType, Field, Schema};

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("event_date", DataType::Utf8, true),
        ]))
    }

    async fn catalog(dir: &std::path::Path) -> LocalCatalog {
        LocalCatalog::new(
            "local",
            dir.to_string_lossy(),
            HashMap::new(),
            vec![],
            vec![],
        )
        .await
        .expect("catalog")
    }

    #[tokio::test]
    async fn create_then_load_round_trips_including_the_schema() {
        let dir = tempfile::tempdir().expect("tempdir");
        let catalog = catalog(dir.path()).await;
        catalog
            .create_database("live", true, None, None)
            .await
            .expect("create database");
        let created = catalog
            .create_table(
                &["live".into()],
                "orders",
                schema(),
                TableFormat::Delta,
                None,
                &["event_date".to_string()],
            )
            .await
            .expect("create table");
        assert!(
            created.location.ends_with("live.db/orders/"),
            "default location should follow the warehouse convention, got {}",
            created.location
        );

        let loaded = catalog
            .load_table(&["live".into()], "orders")
            .await
            .expect("load");
        assert_eq!(loaded.format, TableFormat::Delta);
        assert_eq!(loaded.location, created.location);
        assert_eq!(loaded.partition_columns, vec!["event_date".to_string()]);
        // The schema must survive the round trip — the streaming sink's drift check reads it,
        // and a `None` here would silently disable that guard.
        let recovered = loaded.schema.expect("schema round-trips");
        assert_eq!(recovered.fields().len(), 3);
        assert_eq!(recovered.field(0).name(), "id");
        assert_eq!(recovered.field(0).data_type(), &DataType::Int64);
    }

    #[tokio::test]
    async fn a_missing_table_reports_not_found_not_an_io_failure() {
        // `Error::Plan` is what `table_exists` reads as `false`; an `Error::Io` would surface
        // as a hard failure and make CTAS's existence probe error out.
        let dir = tempfile::tempdir().expect("tempdir");
        let catalog = catalog(dir.path()).await;
        let err = catalog
            .load_table(&["live".into()], "nope")
            .await
            .expect_err("missing table");
        assert!(matches!(err, Error::Plan(_)), "got: {err:?}");
        assert!(!catalog
            .table_exists(&["live".into()], "nope")
            .await
            .expect("probe"));
    }

    #[tokio::test]
    async fn create_database_if_not_exists_is_idempotent_but_bare_create_is_not() {
        let dir = tempfile::tempdir().expect("tempdir");
        let catalog = catalog(dir.path()).await;
        catalog
            .create_database("live", true, None, None)
            .await
            .expect("first");
        catalog
            .create_database("live", true, None, None)
            .await
            .expect("second is a no-op");
        let err = catalog
            .create_database("live", false, None, None)
            .await
            .expect_err("without if_not_exists this must fail");
        assert!(err.to_string().contains("already exists"), "got: {err}");
    }

    #[tokio::test]
    async fn listing_surfaces_both_declared_and_managed_tables() {
        let dir = tempfile::tempdir().expect("tempdir");
        let declared = DeclaredTable {
            namespace: "raw".into(),
            table: "events".into(),
            format: TableFormat::Parquet,
            location: "/data/events/".into(),
            storage_options: BTreeMap::new(),
            partition_columns: vec![],
        };
        let catalog = LocalCatalog::new(
            "local",
            dir.path().to_string_lossy(),
            HashMap::new(),
            vec![declared],
            vec![],
        )
        .await
        .expect("catalog");
        catalog
            .create_table(
                &["live".into()],
                "orders",
                schema(),
                TableFormat::Delta,
                None,
                &[],
            )
            .await
            .expect("create");

        let namespaces = catalog.list_namespaces(&[]).await.expect("namespaces");
        assert!(namespaces.contains(&vec!["raw".to_string()]));
        assert!(namespaces.contains(&vec!["live".to_string()]));
        assert_eq!(
            catalog.list_tables(&["raw".into()]).await.expect("raw"),
            vec!["events".to_string()]
        );
        assert_eq!(
            catalog.list_tables(&["live".into()]).await.expect("live"),
            vec!["orders".to_string()]
        );
    }

    #[tokio::test]
    async fn discovery_finds_a_hive_partitioned_directory() {
        // Before this, a partitioned dataset showed only its `dt=…` directory to the sniffer,
        // matched no format, and was silently skipped — which is every partitioned Parquet table,
        // including the ones Oxidant's own writer produces when `partition_by` is set.
        let root = tempfile::tempdir().expect("tempdir");
        let table = root.path().join("orders");
        std::fs::create_dir_all(table.join("dt=2026-01-01/hour=03")).expect("mkdir");
        std::fs::write(
            table.join("dt=2026-01-01/hour=03/part-0.parquet"),
            b"PAR1not-real",
        )
        .expect("write");

        let found = discover_tables(&DiscoverRoot {
            namespace: "bronze".into(),
            path: root.path().to_string_lossy().into_owned(),
            storage_options: BTreeMap::new(),
        })
        .await
        .expect("discover");

        assert_eq!(found.len(), 1, "expected exactly the one table: {found:?}");
        assert_eq!(found[0].table, "orders");
        assert_eq!(found[0].format, TableFormat::Parquet);
        // Partition keys in nesting order, so the reader peels them off the path correctly
        // instead of reading both columns back as missing.
        assert_eq!(found[0].partition_columns, ["dt", "hour"]);
    }

    #[tokio::test]
    async fn discovery_still_skips_a_directory_it_cannot_identify() {
        // Looking deeper for data files must not turn every stray directory into a table.
        let root = tempfile::tempdir().expect("tempdir");
        std::fs::create_dir_all(root.path().join("notes/sub")).expect("mkdir");
        std::fs::write(root.path().join("notes/sub/README.md"), b"hi").expect("write");

        let found = discover_tables(&DiscoverRoot {
            namespace: "bronze".into(),
            path: root.path().to_string_lossy().into_owned(),
            storage_options: BTreeMap::new(),
        })
        .await
        .expect("discover");
        assert!(found.is_empty(), "should have found nothing: {found:?}");
    }

    #[tokio::test]
    async fn a_declared_table_is_protected_from_ddl() {
        // Config is the operator's statement of intent. A pipeline silently redefining or
        // dropping a table someone declared would be a nasty surprise.
        let dir = tempfile::tempdir().expect("tempdir");
        let declared = DeclaredTable {
            namespace: "raw".into(),
            table: "events".into(),
            format: TableFormat::Parquet,
            location: "/data/events/".into(),
            storage_options: BTreeMap::new(),
            partition_columns: vec![],
        };
        let catalog = LocalCatalog::new(
            "local",
            dir.path().to_string_lossy(),
            HashMap::new(),
            vec![declared],
            vec![],
        )
        .await
        .expect("catalog");
        for err in [
            catalog
                .create_table(
                    &["raw".into()],
                    "events",
                    schema(),
                    TableFormat::Delta,
                    None,
                    &[],
                )
                .await
                .err(),
            catalog
                .drop_table(&["raw".into()], "events", false)
                .await
                .err(),
            catalog
                .alter_table(
                    &["raw".into()],
                    "events",
                    vec![TableChange::SetComment(None)],
                )
                .await
                .err(),
        ] {
            let err = err.expect("DDL on a declared table must fail");
            assert!(
                err.to_string().contains("declared in configuration"),
                "got: {err}"
            );
        }
    }

    #[tokio::test]
    async fn alter_table_applies_properties_comment_and_columns() {
        let dir = tempfile::tempdir().expect("tempdir");
        let catalog = catalog(dir.path()).await;
        catalog
            .create_table(
                &["live".into()],
                "t",
                schema(),
                TableFormat::Delta,
                None,
                &[],
            )
            .await
            .expect("create");
        let altered = catalog
            .alter_table(
                &["live".into()],
                "t",
                vec![
                    TableChange::SetProperties(
                        [("metadata_location".to_string(), "s3://b/m.json".to_string())]
                            .into_iter()
                            .collect(),
                    ),
                    TableChange::SetComment(Some("live orders".into())),
                    TableChange::AddColumns(vec![Field::new("amount", DataType::Int64, true)]),
                ],
            )
            .await
            .expect("alter");
        assert_eq!(
            altered
                .properties
                .get("metadata_location")
                .map(String::as_str),
            Some("s3://b/m.json")
        );
        assert_eq!(altered.comment.as_deref(), Some("live orders"));
        let schema = altered.schema.expect("schema");
        assert_eq!(schema.fields().len(), 4);
        assert_eq!(schema.field(3).name(), "amount");
    }

    #[tokio::test]
    async fn alter_table_rejecting_a_change_leaves_the_table_untouched() {
        // The SPI contract: a provider that cannot honor a change must not half-apply the rest.
        let dir = tempfile::tempdir().expect("tempdir");
        let catalog = catalog(dir.path()).await;
        catalog
            .create_table(
                &["live".into()],
                "t",
                schema(),
                TableFormat::Delta,
                None,
                &[],
            )
            .await
            .expect("create");
        let unmappable = Field::new(
            "weird",
            DataType::Duration(oxidant_catalog::arrow::datatypes::TimeUnit::Nanosecond),
            true,
        );
        let err = catalog
            .alter_table(
                &["live".into()],
                "t",
                vec![
                    TableChange::SetComment(Some("should not stick".into())),
                    TableChange::AddColumns(vec![unmappable]),
                ],
            )
            .await
            .expect_err("unmappable type must be rejected");
        assert!(matches!(err, Error::Unsupported(_)), "got: {err:?}");
        let loaded = catalog
            .load_table(&["live".into()], "t")
            .await
            .expect("load");
        assert_eq!(
            loaded.comment, None,
            "the comment must not have been applied"
        );
        assert_eq!(loaded.schema.expect("schema").fields().len(), 3);
    }

    #[tokio::test]
    async fn dropping_a_non_empty_database_requires_cascade() {
        let dir = tempfile::tempdir().expect("tempdir");
        let catalog = catalog(dir.path()).await;
        catalog
            .create_table(
                &["live".into()],
                "t",
                schema(),
                TableFormat::Delta,
                None,
                &[],
            )
            .await
            .expect("create");
        let err = catalog
            .drop_database("live", false, false)
            .await
            .expect_err("non-empty without cascade");
        assert!(err.to_string().contains("CASCADE"), "got: {err}");
        catalog
            .drop_database("live", false, true)
            .await
            .expect("cascade drops it");
        assert!(catalog
            .list_tables(&["live".into()])
            .await
            .expect("list")
            .is_empty());
    }

    #[tokio::test]
    async fn a_nested_namespace_is_rejected_rather_than_flattened() {
        let dir = tempfile::tempdir().expect("tempdir");
        let catalog = catalog(dir.path()).await;
        let err = catalog
            .load_table(&["a".into(), "b".into()], "t")
            .await
            .expect_err("nested namespace");
        assert!(err.to_string().contains("single-level"), "got: {err}");
    }

    #[tokio::test]
    async fn a_traversing_table_name_cannot_escape_the_warehouse() {
        let dir = tempfile::tempdir().expect("tempdir");
        let catalog = catalog(dir.path()).await;
        let err = catalog
            .create_table(
                &["live".into()],
                "../../etc/evil",
                schema(),
                TableFormat::Delta,
                None,
                &[],
            )
            .await
            .expect_err("path traversal must be rejected");
        assert!(
            err.to_string().contains("not a valid identifier"),
            "got: {err}"
        );
    }

    #[tokio::test]
    async fn an_explicit_location_gains_a_trailing_slash() {
        // `ListingTable` distinguishes a collection from a single file by the trailing slash;
        // without it a table root is read as one file.
        let dir = tempfile::tempdir().expect("tempdir");
        let catalog = catalog(dir.path()).await;
        let created = catalog
            .create_table(
                &["live".into()],
                "t",
                schema(),
                TableFormat::Delta,
                Some("/data/custom".into()),
                &[],
            )
            .await
            .expect("create");
        assert_eq!(created.location, "/data/custom/");
    }

    #[test]
    fn locations_join_with_a_trailing_slash() {
        assert_eq!(
            join_location("/data/bronze", "orders"),
            "/data/bronze/orders/"
        );
        assert_eq!(
            join_location("/data/bronze/", "orders"),
            "/data/bronze/orders/"
        );
    }
}

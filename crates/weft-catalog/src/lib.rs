//! `weft-catalog` — the pluggable catalog SPI Weft resolves table names through.
//!
//! Weft embeds DataFusion's `SessionContext`, which already does multi-part name resolution
//! (`catalog.namespace.table`) and **lazy, async** table loading. This crate defines the
//! provider-facing seam so an external metastore can plug into that resolution path without
//! eagerly registering every table:
//!
//! - [`CatalogProvider`] — the trait an external catalog implements (Hive Metastore, Unity
//!   Catalog / Iceberg REST, AWS Glue, or a user's own). It lists namespaces/tables and, on
//!   demand, resolves one table to a [`TableMetadata`] (location + format + optional schema).
//! - [`CatalogRegistry`] — the per-session set of named catalogs plus the current catalog /
//!   namespace pointers (`USE`, `setCurrentCatalog`, `setCurrentDatabase`).
//!
//! The bridge that turns a [`CatalogProvider`] into a DataFusion `CatalogProvider` /
//! `SchemaProvider` (so `SELECT … FROM cat.ns.tbl` resolves lazily) lives in `weft-loom`
//! (`catalog_bridge`), reusing the engine's Parquet/Delta/Iceberg readers to build the
//! `TableProvider` from a [`TableMetadata`].
//!
//! Concrete providers live in their own crates (e.g. `weft-catalog-hive`); the type→provider
//! factory lives in `weft-connect`, which can see them all, so this crate stays provider-agnostic.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;

/// Shared Hive/Glue type-string → Arrow schema mapping (used by the Hive and Glue providers).
pub mod hive_types;
// Re-exported so external `CatalogProvider` implementors (e.g. `weft-catalog-glue`) can build the
// `TableMetadata.schema` from arrow types using the *same* arrow version the engine embeds, without
// taking a direct `arrow` dependency (which could drift to a mismatched version).
pub use datafusion::arrow;
// Re-exported so external `CatalogProvider` implementors can name the trait's `Result`/`Error`
// without taking a direct `weft-common` dependency.
pub use weft_common::{Error, Result};

/// Physical table format Weft can read. The bridge maps each to a concrete reader.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableFormat {
    /// Parquet directory / single file.
    Parquet,
    /// Delta Lake (resolved via `_delta_log`).
    Delta,
    /// Apache Iceberg (resolved via `metadata.json` + manifests).
    Iceberg,
    /// CSV file / directory.
    Csv,
    /// Newline-delimited JSON file / directory.
    Json,
}

impl TableFormat {
    /// Parse a Spark/Hive `provider`/format string (case-insensitive). Returns `None` for a
    /// format Weft cannot read yet (e.g. `orc`, `avro`) so callers can surface a clear error.
    pub fn from_provider(s: &str) -> Option<TableFormat> {
        match s.trim().to_ascii_lowercase().as_str() {
            "parquet" => Some(Self::Parquet),
            "delta" => Some(Self::Delta),
            "iceberg" => Some(Self::Iceberg),
            "csv" => Some(Self::Csv),
            "json" => Some(Self::Json),
            _ => None,
        }
    }
}

/// What an external catalog returns for one table: enough for the engine to read it.
#[derive(Debug, Clone)]
pub struct TableMetadata {
    /// Fully-qualified display name, e.g. `prod.sales.orders`.
    pub name: String,
    /// Storage URI of the table root (e.g. `s3://bucket/path`, `file:///data/t`, `hdfs://…`).
    pub location: String,
    /// Physical format the engine reads it as.
    pub format: TableFormat,
    /// The table schema when the catalog already knows it (lets the bridge skip DataFusion's
    /// schema inference). `None` → infer from the data files.
    pub schema: Option<SchemaRef>,
    /// Storage credentials / endpoint options (e.g. `s3.access-key-id`, `s3.endpoint`), used to
    /// register an `object_store` for the location's scheme. Empty for local/anonymous reads.
    pub storage_options: HashMap<String, String>,
    /// Partition column names (informational for v1; Parquet hive-partitioning is inferred).
    pub partition_columns: Vec<String>,
    /// Table-level comment/description, when the catalog has one (e.g. Hive's table-level
    /// comment field, Glue's `Description`). `None` when the source doesn't have one set.
    pub comment: Option<String>,
    /// Table properties / parameters (e.g. Hive's `parameters` map, Glue's `Parameters` map).
    /// Empty when the source doesn't surface these.
    pub properties: HashMap<String, String>,
}

impl TableMetadata {
    /// Construct minimal metadata (no schema/credentials/partitions).
    pub fn new(name: impl Into<String>, location: impl Into<String>, format: TableFormat) -> Self {
        Self {
            name: name.into(),
            location: location.into(),
            format,
            schema: None,
            storage_options: HashMap::new(),
            partition_columns: Vec::new(),
            comment: None,
            properties: HashMap::new(),
        }
    }

    /// Builder: attach a known schema.
    pub fn with_schema(mut self, schema: SchemaRef) -> Self {
        self.schema = Some(schema);
        self
    }

    /// Builder: attach storage options.
    pub fn with_storage_options(mut self, options: HashMap<String, String>) -> Self {
        self.storage_options = options;
        self
    }

    /// Builder: attach partition columns.
    pub fn with_partition_columns(mut self, cols: Vec<String>) -> Self {
        self.partition_columns = cols;
        self
    }

    /// Builder: attach a table-level comment/description.
    pub fn with_comment(mut self, comment: Option<String>) -> Self {
        self.comment = comment;
        self
    }

    /// Builder: attach table properties/parameters.
    pub fn with_properties(mut self, properties: HashMap<String, String>) -> Self {
        self.properties = properties;
        self
    }
}

/// A pluggable catalog. **Implement this to bring your own metastore.**
///
/// Namespaces are multi-part (`["sales"]`, or `["a", "b"]` for nested namespaces) so the trait
/// covers both flat (Hive: database) and hierarchical (Unity: catalog.schema) metastores.
/// Methods are async because real catalogs are network services.
#[async_trait]
pub trait CatalogProvider: Send + Sync {
    /// The catalog's registered name (the `<name>` in `spark.sql.catalog.<name>`).
    fn name(&self) -> &str;

    /// List the child namespaces under `parent` (empty `parent` = top level).
    async fn list_namespaces(&self, parent: &[String]) -> Result<Vec<Vec<String>>>;

    /// List the table names directly in `namespace`.
    async fn list_tables(&self, namespace: &[String]) -> Result<Vec<String>>;

    /// Resolve one table to its read metadata. The hot path: called lazily by the DataFusion
    /// bridge the first time a query references `namespace.table`.
    async fn load_table(&self, namespace: &[String], table: &str) -> Result<TableMetadata>;

    /// Whether `namespace.table` exists. Default: probe [`load_table`](Self::load_table) and treat
    /// a not-found (`Plan`/`Io`) error as `false`; providers with a cheaper existence check should
    /// override.
    async fn table_exists(&self, namespace: &[String], table: &str) -> Result<bool> {
        match self.load_table(namespace, table).await {
            Ok(_) => Ok(true),
            Err(Error::Plan(_)) | Err(Error::Io(_)) => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// Whether `namespace` exists. Default: check the parent's namespace listing.
    async fn namespace_exists(&self, namespace: &[String]) -> Result<bool> {
        if namespace.is_empty() {
            return Ok(true);
        }
        let parent = &namespace[..namespace.len() - 1];
        let last = &namespace[namespace.len() - 1];
        Ok(self
            .list_namespaces(parent)
            .await?
            .iter()
            .any(|ns| ns.last() == Some(last)))
    }

    /// Create a new table in `namespace` backed by `schema`/`format`, physically stored at
    /// `location` (or a catalog-chosen default location when `None`), with `partition_columns`
    /// appended after the data columns. Called by the DataFusion bridge's `register_table` when a
    /// `CREATE TABLE ... AS SELECT` targets this catalog — the caller writes the actual data files
    /// separately and only needs the returned [`TableMetadata`] (in particular its `location`) to
    /// know where.
    ///
    /// Default: `Unsupported`, so a read-only provider (or any future third-party one) keeps
    /// compiling without implementing writes.
    async fn create_table(
        &self,
        namespace: &[String],
        table: &str,
        schema: SchemaRef,
        format: TableFormat,
        location: Option<String>,
        partition_columns: &[String],
    ) -> Result<TableMetadata> {
        let _ = (
            namespace,
            table,
            schema,
            format,
            location,
            partition_columns,
        );
        Err(Error::Unsupported(format!(
            "catalog `{}` does not support creating tables",
            self.name()
        )))
    }
}

/// The per-session set of named catalogs plus the current catalog / namespace pointers.
///
/// This is the source of truth for the Spark `Catalog` RPC (`listCatalogs`, `currentCatalog`,
/// `setCurrentDatabase`, …). Query *resolution* goes through the DataFusion bridge that
/// `weft-loom` registers from the same providers, so the two stay in lockstep: registering a
/// catalog here is paired with `Engine::register_catalog`.
pub struct CatalogRegistry {
    inner: Mutex<RegistryState>,
}

struct RegistryState {
    catalogs: HashMap<String, Arc<dyn CatalogProvider>>,
    current_catalog: String,
    /// Current namespace within the current catalog (Spark's "current database").
    current_namespace: Vec<String>,
}

/// The name DataFusion uses for its built-in in-process catalog, and Weft's default current
/// catalog when no external catalog is selected.
pub const DEFAULT_CATALOG: &str = "spark_catalog";
/// Spark's default current database name.
pub const DEFAULT_NAMESPACE: &str = "default";

impl Default for CatalogRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl CatalogRegistry {
    /// A registry seeded with just the built-in catalog selected and the `default` database.
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(RegistryState {
                catalogs: HashMap::new(),
                current_catalog: DEFAULT_CATALOG.to_string(),
                current_namespace: vec![DEFAULT_NAMESPACE.to_string()],
            }),
        }
    }

    /// Register (or replace) an external catalog under `name`.
    pub fn register(&self, name: &str, provider: Arc<dyn CatalogProvider>) {
        self.lock().catalogs.insert(name.to_string(), provider);
    }

    /// Whether a catalog named `name` is registered (the built-in catalog is always present).
    pub fn contains(&self, name: &str) -> bool {
        name == DEFAULT_CATALOG || self.lock().catalogs.contains_key(name)
    }

    /// Fetch a registered external provider by name (`None` for the built-in catalog or unknown).
    pub fn provider(&self, name: &str) -> Option<Arc<dyn CatalogProvider>> {
        self.lock().catalogs.get(name).cloned()
    }

    /// All catalog names, built-in first, then external in a stable (sorted) order.
    pub fn catalog_names(&self) -> Vec<String> {
        let state = self.lock();
        let mut names: Vec<String> = state.catalogs.keys().cloned().collect();
        names.sort();
        let mut out = vec![DEFAULT_CATALOG.to_string()];
        out.extend(names.into_iter().filter(|n| n != DEFAULT_CATALOG));
        out
    }

    /// The current catalog name.
    pub fn current_catalog(&self) -> String {
        self.lock().current_catalog.clone()
    }

    /// Set the current catalog. Errors if it is not registered.
    pub fn set_current_catalog(&self, name: &str) -> Result<()> {
        if !self.contains(name) {
            return Err(Error::Plan(format!("catalog `{name}` is not registered")));
        }
        self.lock().current_catalog = name.to_string();
        Ok(())
    }

    /// The current namespace ("current database"), e.g. `["sales"]`.
    pub fn current_namespace(&self) -> Vec<String> {
        self.lock().current_namespace.clone()
    }

    /// Set the current namespace (Spark `setCurrentDatabase`). A dotted name splits on `.`.
    pub fn set_current_namespace(&self, namespace: &str) {
        self.lock().current_namespace = split_ident(namespace);
    }
}

impl CatalogRegistry {
    fn lock(&self) -> std::sync::MutexGuard<'_, RegistryState> {
        self.inner.lock().expect("catalog registry poisoned")
    }
}

/// Split a (possibly dotted, possibly back-tick-quoted) identifier into parts. Quoting lets a
/// part contain a literal dot, e.g. `` `a.b`.c `` → `["a.b", "c"]`.
pub fn split_ident(ident: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut cur = String::new();
    let mut in_quote = false;
    for ch in ident.chars() {
        match ch {
            '`' => in_quote = !in_quote,
            '.' if !in_quote => {
                parts.push(std::mem::take(&mut cur));
            }
            c => cur.push(c),
        }
    }
    parts.push(cur);
    parts.retain(|p| !p.is_empty());
    parts
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A trivial in-memory provider for testing the SPI surface.
    struct FakeCatalog {
        name: String,
        tables: HashMap<String, String>, // "ns.table" -> location
    }

    #[async_trait]
    impl CatalogProvider for FakeCatalog {
        fn name(&self) -> &str {
            &self.name
        }
        async fn list_namespaces(&self, parent: &[String]) -> Result<Vec<Vec<String>>> {
            if parent.is_empty() {
                Ok(vec![vec!["ns".to_string()]])
            } else {
                Ok(vec![])
            }
        }
        async fn list_tables(&self, namespace: &[String]) -> Result<Vec<String>> {
            let prefix = format!("{}.", namespace.join("."));
            Ok(self
                .tables
                .keys()
                .filter_map(|k| k.strip_prefix(&prefix).map(|t| t.to_string()))
                .collect())
        }
        async fn load_table(&self, namespace: &[String], table: &str) -> Result<TableMetadata> {
            let key = format!("{}.{table}", namespace.join("."));
            let loc = self
                .tables
                .get(&key)
                .ok_or_else(|| Error::Plan(format!("no such table: {key}")))?;
            Ok(TableMetadata::new(key, loc.clone(), TableFormat::Parquet))
        }
    }

    fn fake() -> FakeCatalog {
        let mut tables = HashMap::new();
        tables.insert("ns.orders".to_string(), "file:///data/orders".to_string());
        FakeCatalog {
            name: "prod".to_string(),
            tables,
        }
    }

    #[tokio::test]
    async fn load_and_exists() {
        let c = fake();
        let md = c.load_table(&["ns".to_string()], "orders").await.unwrap();
        assert_eq!(md.format, TableFormat::Parquet);
        assert_eq!(md.location, "file:///data/orders");
        assert!(c.table_exists(&["ns".to_string()], "orders").await.unwrap());
        assert!(!c
            .table_exists(&["ns".to_string()], "missing")
            .await
            .unwrap());
        assert!(c.namespace_exists(&["ns".to_string()]).await.unwrap());
        assert!(!c.namespace_exists(&["nope".to_string()]).await.unwrap());
    }

    #[test]
    fn registry_current_pointers() {
        let reg = CatalogRegistry::new();
        assert_eq!(reg.current_catalog(), DEFAULT_CATALOG);
        assert_eq!(reg.current_namespace(), vec![DEFAULT_NAMESPACE.to_string()]);
        reg.register("prod", Arc::new(fake()));
        assert!(reg.contains("prod"));
        assert_eq!(reg.catalog_names(), vec!["spark_catalog", "prod"]);
        reg.set_current_catalog("prod").unwrap();
        assert_eq!(reg.current_catalog(), "prod");
        assert!(reg.set_current_catalog("nope").is_err());
        reg.set_current_namespace("sales");
        assert_eq!(reg.current_namespace(), vec!["sales".to_string()]);
    }

    #[test]
    fn split_identifiers() {
        assert_eq!(split_ident("a.b.c"), vec!["a", "b", "c"]);
        assert_eq!(split_ident("`a.b`.c"), vec!["a.b", "c"]);
        assert_eq!(split_ident("solo"), vec!["solo"]);
    }

    #[test]
    fn format_parsing() {
        assert_eq!(
            TableFormat::from_provider("PARQUET"),
            Some(TableFormat::Parquet)
        );
        assert_eq!(
            TableFormat::from_provider("delta"),
            Some(TableFormat::Delta)
        );
        assert_eq!(TableFormat::from_provider("orc"), None);
    }
}

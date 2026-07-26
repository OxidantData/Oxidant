//! `weft-catalog-hive` — a [`CatalogProvider`] backed by a Hive Metastore.
//!
//! Resolves Spark three-part names `<catalog>.<database>.<table>` against a Hive Metastore over
//! Thrift: the catalog is the registered name, the database is the (single-level) namespace, and
//! `load_table` returns the table's storage location + inferred format. Read-only in v1 (no DDL).
//!
//! Configure it the Spark way — `spark.sql.catalog.<name>.type=hive` plus
//! `spark.sql.catalog.<name>.uri=thrift://host:9083` — which `weft-connect`'s catalog factory turns
//! into [`HiveCatalog::from_config`].

mod thrift;

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use weft_catalog::arrow::datatypes::SchemaRef;
use weft_catalog::hive_types::{
    columns_to_schema, format_serde, schema_to_columns, validate_identifier,
};
use weft_catalog::{CatalogProvider, Error, Result, TableFormat, TableMetadata};

use thrift::{HiveTable, MetastoreClient, NewHiveTable};

/// The default Hive Metastore Thrift port.
const DEFAULT_PORT: u16 = 9083;

/// A Hive Metastore catalog.
pub struct HiveCatalog {
    name: String,
    host: String,
    port: u16,
    /// Root new tables are written under (`{warehouse}/{db}/{table}/`) when a `CREATE TABLE ... AS
    /// SELECT` doesn't specify an explicit `LOCATION`. `None` means CTAS against this catalog must
    /// supply an explicit location (see `create_table`). Same convention as `GlueCatalog`.
    warehouse: Option<String>,
}

impl HiveCatalog {
    /// Build from a `thrift://host:port` URI (port defaults to 9083). No `warehouse` — CTAS
    /// against a catalog built this way needs an explicit `LOCATION`; use [`Self::from_config`] to
    /// set one via the `warehouse` option.
    pub fn from_uri(name: &str, uri: &str) -> Result<Self> {
        let (host, port) = parse_thrift_uri(uri)?;
        Ok(Self {
            name: name.to_string(),
            host,
            port,
            warehouse: None,
        })
    }

    /// Build from `spark.sql.catalog.<name>.*` options (the `spark.sql.catalog.<name>.` prefix
    /// already stripped, so keys are `type`, `uri`, `warehouse`, …). Requires a `uri` (or
    /// `host`[+`port`]).
    pub fn from_config(name: &str, options: &HashMap<String, String>) -> Result<Self> {
        let warehouse = options.get("warehouse").cloned();
        if let Some(uri) = options.get("uri").or_else(|| options.get("thrift.uri")) {
            let (host, port) = parse_thrift_uri(uri)?;
            return Ok(Self {
                name: name.to_string(),
                host,
                port,
                warehouse,
            });
        }
        if let Some(host) = options.get("host") {
            let port = options
                .get("port")
                .and_then(|p| p.parse().ok())
                .unwrap_or(DEFAULT_PORT);
            return Ok(Self {
                name: name.to_string(),
                host: host.clone(),
                port,
                warehouse,
            });
        }
        Err(Error::Plan(format!(
            "hive catalog `{name}` requires `spark.sql.catalog.{name}.uri` (e.g. thrift://host:9083)"
        )))
    }

    async fn connect(&self) -> Result<MetastoreClient> {
        MetastoreClient::connect(&self.host, self.port).await
    }

    /// Resolve the storage location for a table being created: the explicit `location` if given
    /// (normalized to end in `/`, required for `ListingTable`/`is_collection()` on read-back),
    /// else `{warehouse}/{db}/{table}/`, else an error naming what's missing.
    ///
    /// `db`/`table` are validated as plain identifiers first (`validate_identifier`) — they come
    /// straight from the SQL statement's table reference, and were previously interpolated
    /// unsanitized into the warehouse-derived path, letting a name like `../../etc/evil` escape
    /// the intended directory (a real path-traversal bug for `file://` warehouses).
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

#[async_trait]
impl CatalogProvider for HiveCatalog {
    fn name(&self) -> &str {
        &self.name
    }

    async fn list_namespaces(&self, parent: &[String]) -> Result<Vec<Vec<String>>> {
        // Hive databases are flat: there are no nested namespaces under a database.
        if !parent.is_empty() {
            return Ok(vec![]);
        }
        let dbs = self.connect().await?.get_all_databases().await?;
        Ok(dbs.into_iter().map(|d| vec![d]).collect())
    }

    async fn list_tables(&self, namespace: &[String]) -> Result<Vec<String>> {
        let db = single_database(namespace)?;
        self.connect().await?.get_all_tables(db).await
    }

    async fn load_table(&self, namespace: &[String], table: &str) -> Result<TableMetadata> {
        let db = single_database(namespace)?;
        let t = self.connect().await?.get_table(db, table).await?;
        let location = t.location.clone().ok_or_else(|| {
            Error::Plan(format!("hive table `{db}.{table}` has no storage location"))
        })?;
        let format = detect_format(&t)?;

        // The Hive-declared schema (data columns then partition columns) is authoritative when
        // fully mappable: the engine then reads files *against* it, casting physically-mismatched
        // column types at scan time instead of failing schema inference's strict "merge" check.
        // If any column has a type we can't faithfully map (or there are no columns), we leave
        // `schema = None` and fall back to data-file inference — never a partial, position-shifting
        // schema.
        let schema_cols = t.columns.iter().chain(t.partition_typed.iter()).cloned();
        let schema = columns_to_schema(schema_cols);

        let properties: HashMap<String, String> = t.parameters.iter().cloned().collect();
        let md = TableMetadata::new(format!("{}.{db}.{table}", self.name), location, format)
            .with_partition_columns(t.partition_columns.clone())
            .with_comment(t.comment().map(str::to_string))
            .with_properties(properties);
        Ok(match schema {
            Some(s) => md.with_schema(Arc::new(s)),
            None => md,
        })
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
        let db = single_database(namespace)?;
        let location = self.resolve_create_location(db, table, location)?;
        let serde = format_serde(format)?;
        let (columns, part_cols) = schema_to_columns(&schema, partition_columns)?;

        let new_table = NewHiveTable {
            db_name: db.to_string(),
            table_name: table.to_string(),
            location: location.clone(),
            input_format: serde.input_format,
            output_format: serde.output_format,
            serde_lib: serde.serde_lib,
            serde_params: serde
                .serde_params
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            columns,
            partition_columns: part_cols,
        };
        self.connect().await?.create_table(&new_table).await?;

        let md = TableMetadata::new(format!("{}.{db}.{table}", self.name), location, format)
            .with_schema(schema)
            .with_partition_columns(partition_columns.to_vec());
        Ok(md)
    }
}

/// Hive namespaces are a single database name. Reject empty / nested namespaces with a clear error.
fn single_database(namespace: &[String]) -> Result<&str> {
    match namespace {
        [db] => Ok(db.as_str()),
        [] => Err(Error::Plan(
            "a Hive table reference needs a database, e.g. `catalog.database.table`".to_string(),
        )),
        _ => Err(Error::Plan(format!(
            "Hive namespaces are a single database; got `{}`",
            namespace.join(".")
        ))),
    }
}

/// Infer the readable file format from a Hive table's properties and storage descriptor.
///
/// Order of signals (most authoritative first): Iceberg `table_type` property, an explicit Spark
/// `provider` property, then the input format / serde class name. Falls back to Parquet — the
/// common lakehouse default — when no signal is conclusive.
fn detect_format(t: &HiveTable) -> Result<TableFormat> {
    if t.param("table_type")
        .is_some_and(|v| v.eq_ignore_ascii_case("iceberg"))
    {
        return Ok(TableFormat::Iceberg);
    }
    for key in ["spark.sql.sources.provider", "provider"] {
        if let Some(f) = t.param(key).and_then(TableFormat::from_provider) {
            return Ok(f);
        }
    }
    let probe = format!(
        "{} {}",
        t.input_format.as_deref().unwrap_or(""),
        t.serde_lib.as_deref().unwrap_or("")
    )
    .to_lowercase();
    if probe.contains("parquet") {
        Ok(TableFormat::Parquet)
    } else if probe.contains("orc") {
        Err(Error::Unsupported(
            "ORC tables are not supported yet".to_string(),
        ))
    } else if probe.contains("avro") {
        Err(Error::Unsupported(
            "Avro tables are not supported yet".to_string(),
        ))
    } else if probe.contains("json") {
        Ok(TableFormat::Json)
    } else if probe.contains("csv") || probe.contains("opencsv") || probe.contains("lazysimple") {
        Ok(TableFormat::Csv)
    } else {
        // No conclusive signal — assume Parquet (the dominant lakehouse format).
        Ok(TableFormat::Parquet)
    }
}

/// Parse `thrift://host:port` (scheme optional, port defaults to 9083).
fn parse_thrift_uri(uri: &str) -> Result<(String, u16)> {
    let rest = uri
        .strip_prefix("thrift://")
        .unwrap_or_else(|| uri.strip_prefix("//").unwrap_or(uri));
    let rest = rest.trim_end_matches('/');
    if rest.is_empty() {
        return Err(Error::Plan(format!("empty Hive Metastore URI: `{uri}`")));
    }
    match rest.rsplit_once(':') {
        Some((host, port)) => {
            let port = port
                .parse()
                .map_err(|_| Error::Plan(format!("bad port in Hive URI `{uri}`")))?;
            Ok((host.to_string(), port))
        }
        None => Ok((rest.to_string(), DEFAULT_PORT)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uri_parsing() {
        assert_eq!(
            parse_thrift_uri("thrift://hms.internal:9083").unwrap(),
            ("hms.internal".to_string(), 9083)
        );
        assert_eq!(
            parse_thrift_uri("localhost").unwrap(),
            ("localhost".to_string(), 9083)
        );
        assert_eq!(
            parse_thrift_uri("thrift://10.0.0.5:9999").unwrap(),
            ("10.0.0.5".to_string(), 9999)
        );
        assert!(parse_thrift_uri("thrift://").is_err());
    }

    #[test]
    fn format_detection() {
        let mut t = HiveTable {
            input_format: Some(
                "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat".to_string(),
            ),
            ..Default::default()
        };
        assert_eq!(detect_format(&t).unwrap(), TableFormat::Parquet);

        t.parameters = vec![("table_type".to_string(), "ICEBERG".to_string())];
        assert_eq!(detect_format(&t).unwrap(), TableFormat::Iceberg);

        let delta = HiveTable {
            parameters: vec![(
                "spark.sql.sources.provider".to_string(),
                "delta".to_string(),
            )],
            ..Default::default()
        };
        assert_eq!(detect_format(&delta).unwrap(), TableFormat::Delta);

        let orc = HiveTable {
            input_format: Some("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat".to_string()),
            ..Default::default()
        };
        assert!(matches!(detect_format(&orc), Err(Error::Unsupported(_))));
    }

    #[test]
    fn config_requires_uri() {
        let opts = HashMap::new();
        assert!(HiveCatalog::from_config("prod", &opts).is_err());
        let mut opts = HashMap::new();
        opts.insert("uri".to_string(), "thrift://h:9083".to_string());
        let c = HiveCatalog::from_config("prod", &opts).unwrap();
        assert_eq!(c.name(), "prod");
    }

    fn catalog_with_warehouse(warehouse: Option<&str>) -> HiveCatalog {
        let mut opts = HashMap::new();
        opts.insert("uri".to_string(), "thrift://h:9083".to_string());
        if let Some(w) = warehouse {
            opts.insert("warehouse".to_string(), w.to_string());
        }
        HiveCatalog::from_config("hive", &opts).unwrap()
    }

    #[test]
    fn resolve_create_location_rejects_path_traversal() {
        let cat = catalog_with_warehouse(Some("s3://wh"));
        for (db, table) in [("db", "../../etc/evil"), ("../escape", "t"), ("db", "a/b")] {
            let err = cat.resolve_create_location(db, table, None).unwrap_err();
            assert!(matches!(err, Error::Plan(_)), "{db}.{table}");
        }
    }

    #[test]
    fn resolve_create_location_normalizes_missing_trailing_slash() {
        let cat = catalog_with_warehouse(None);
        assert_eq!(
            cat.resolve_create_location("db", "t", Some("s3://explicit/t".to_string()))
                .unwrap(),
            "s3://explicit/t/"
        );
    }

    #[test]
    fn resolve_create_location_falls_back_to_warehouse() {
        let cat = catalog_with_warehouse(Some("s3://wh/"));
        assert_eq!(
            cat.resolve_create_location("db", "t", None).unwrap(),
            "s3://wh/db/t/"
        );
    }
}

//! Catalog wiring for the Spark Connect front-end:
//! - parse `spark.sql.catalog.<name>.*` config into provider instances (the Spark-compatible,
//!   zero-code way to bring an external catalog), via [`build_provider`];
//! - serve the Spark `Catalog` RPC (`listCatalogs`/`listDatabases`/`listTables`/`tableExists`/
//!   current-catalog/db) from the [`CatalogRegistry`] + providers, in [`handle_catalog`].
//!
//! Query *resolution* is handled separately by the DataFusion bridge `weft-loom` registers; this
//! module is the metadata/listing surface and the config seam.

use std::collections::HashMap;
use std::sync::Arc;

use tonic::Status;
use weft_catalog::{split_ident, CatalogProvider, CatalogRegistry};
use weft_loom::arrow::array::{ArrayRef, BooleanArray, ListBuilder, StringBuilder};
use weft_loom::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use weft_loom::arrow::record_batch::RecordBatch;
use weft_loom::Engine;
use weft_proto::spark::connect as sc;

use super::err_to_status;

/// Config prefix Spark uses to declare a catalog plugin: `spark.sql.catalog.<name>[.<key>]`.
const PREFIX: &str = "spark.sql.catalog.";

/// Group flat `spark.sql.catalog.<name>.<key>` config entries by catalog name.
///
/// The bare `spark.sql.catalog.<name>` entry (Spark's implementation-class slot) is captured as the
/// `type` option, so both `spark.sql.catalog.prod=hive` and `spark.sql.catalog.prod.type=hive` work.
pub fn group_catalog_options(
    config: &HashMap<String, String>,
) -> HashMap<String, HashMap<String, String>> {
    let mut out: HashMap<String, HashMap<String, String>> = HashMap::new();
    for (k, v) in config {
        let Some(rest) = k.strip_prefix(PREFIX) else {
            continue;
        };
        match rest.split_once('.') {
            Some((name, key)) => {
                out.entry(name.to_string())
                    .or_default()
                    .insert(key.to_string(), v.clone());
            }
            None => {
                // `spark.sql.catalog.<name>` = <type/impl>
                out.entry(rest.to_string())
                    .or_default()
                    .entry("type".to_string())
                    .or_insert_with(|| v.clone());
            }
        }
    }
    out
}

/// Build a catalog provider from its grouped options. Dispatches on `type` (the
/// `spark.sql.catalog.<name>.type` value). New built-in provider types are added here.
pub fn build_provider(
    name: &str,
    options: &HashMap<String, String>,
) -> Result<Arc<dyn CatalogProvider>, Status> {
    let kind = options
        .get("type")
        .map(|s| s.trim().to_ascii_lowercase())
        .ok_or_else(|| {
            Status::invalid_argument(format!(
                "catalog `{name}` needs `spark.sql.catalog.{name}.type` (e.g. `hive`)"
            ))
        })?;
    match kind.as_str() {
        "hive" => {
            let cat = weft_catalog_hive::HiveCatalog::from_config(name, options)
                .map_err(err_to_status)?;
            Ok(Arc::new(cat))
        }
        "glue" => {
            // Credentials come from the instance role (IMDS); `region` (default us-west-2) and an
            // optional `aws_bin` arrive as `spark.sql.catalog.<name>.{region,aws_bin}`.
            let cat = weft_catalog_glue::GlueCatalog::from_config(name, options);
            Ok(Arc::new(cat))
        }
        "rest" | "unity" | "iceberg" => {
            let cat = weft_catalog_rest::RestCatalog::from_config(name, options)
                .map_err(err_to_status)?;
            Ok(Arc::new(cat))
        }
        other => Err(Status::unimplemented(format!(
            "catalog type `{other}` is not supported yet (have: hive, glue, rest/unity/iceberg)"
        ))),
    }
}

/// Serve a Spark `Catalog` relation, returning the result rows as Arrow batches.
pub async fn handle_catalog(
    engine: &Engine,
    registry: &CatalogRegistry,
    cat: &sc::Catalog,
) -> Result<Vec<RecordBatch>, Status> {
    use sc::catalog::CatType;
    let ct = cat
        .cat_type
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("empty Catalog request"))?;
    match ct {
        CatType::ListCatalogs(_) => list_catalogs(registry),
        CatType::CurrentCatalog(_) => Ok(scalar_string("name", &registry.current_catalog())),
        CatType::SetCurrentCatalog(s) => {
            registry
                .set_current_catalog(&s.catalog_name)
                .map_err(err_to_status)?;
            Ok(empty_result())
        }
        CatType::CurrentDatabase(_) => Ok(scalar_string(
            "name",
            &registry.current_namespace().join("."),
        )),
        CatType::SetCurrentDatabase(s) => {
            registry.set_current_namespace(&s.db_name);
            Ok(empty_result())
        }
        CatType::ListDatabases(l) => list_databases(engine, registry, l.pattern.as_deref()).await,
        CatType::ListTables(l) => {
            list_tables(engine, registry, l.db_name.as_deref(), l.pattern.as_deref()).await
        }
        CatType::TableExists(t) => {
            let exists =
                table_exists(engine, registry, &t.table_name, t.db_name.as_deref()).await?;
            Ok(scalar_bool(exists))
        }
        CatType::DatabaseExists(d) => {
            let exists = database_exists(engine, registry, &d.db_name).await?;
            Ok(scalar_bool(exists))
        }
        other => Err(Status::unimplemented(format!(
            "catalog operation not supported yet: {}",
            cat_op_name(other)
        ))),
    }
}

/// The static result schema for a catalog op — used by `AnalyzePlan(Schema)` so a client that
/// probes the schema before executing doesn't trigger the op (no side effects).
pub fn result_schema(cat: &sc::Catalog) -> Option<SchemaRef> {
    use sc::catalog::CatType;
    let ct = cat.cat_type.as_ref()?;
    Some(match ct {
        CatType::ListCatalogs(_) => catalogs_schema(),
        CatType::ListDatabases(_) => databases_schema(),
        CatType::ListTables(_) => tables_schema(),
        CatType::CurrentCatalog(_) | CatType::CurrentDatabase(_) => {
            Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)]))
        }
        CatType::TableExists(_) | CatType::DatabaseExists(_) => {
            Arc::new(Schema::new(vec![Field::new(
                "exists",
                DataType::Boolean,
                false,
            )]))
        }
        _ => return None,
    })
}

// ---- list operations -------------------------------------------------------

fn list_catalogs(registry: &CatalogRegistry) -> Result<Vec<RecordBatch>, Status> {
    let mut names = StringBuilder::new();
    let mut descs = StringBuilder::new();
    for name in registry.catalog_names() {
        names.append_value(&name);
        descs.append_null();
    }
    let batch = RecordBatch::try_new(
        catalogs_schema(),
        vec![Arc::new(names.finish()), Arc::new(descs.finish())],
    )
    .map_err(internal)?;
    Ok(vec![batch])
}

async fn list_databases(
    engine: &Engine,
    registry: &CatalogRegistry,
    pattern: Option<&str>,
) -> Result<Vec<RecordBatch>, Status> {
    let catalog = registry.current_catalog();
    let namespaces = namespaces_of(engine, registry, &catalog).await?;

    let mut names = StringBuilder::new();
    let mut catalogs = StringBuilder::new();
    let mut descs = StringBuilder::new();
    let mut locations = StringBuilder::new();
    for ns in namespaces {
        let db = ns.join(".");
        if !matches_pattern(&db, pattern) {
            continue;
        }
        names.append_value(&db);
        catalogs.append_value(&catalog);
        descs.append_null();
        locations.append_value("");
    }
    let batch = RecordBatch::try_new(
        databases_schema(),
        vec![
            Arc::new(names.finish()),
            Arc::new(catalogs.finish()),
            Arc::new(descs.finish()),
            Arc::new(locations.finish()),
        ],
    )
    .map_err(internal)?;
    Ok(vec![batch])
}

async fn list_tables(
    engine: &Engine,
    registry: &CatalogRegistry,
    db_name: Option<&str>,
    pattern: Option<&str>,
) -> Result<Vec<RecordBatch>, Status> {
    // Resolve which (catalog, namespace) to list: an explicit db_name may be catalog-qualified;
    // otherwise use the current catalog + current/just-given database.
    let (catalog, namespace) = match db_name {
        Some(db) => resolve_namespace(registry, db),
        None => (registry.current_catalog(), registry.current_namespace()),
    };

    let table_names = tables_of(engine, registry, &catalog, &namespace).await?;

    let mut names = StringBuilder::new();
    let mut catalogs = StringBuilder::new();
    let mut namespaces = ListBuilder::new(StringBuilder::new());
    let mut descs = StringBuilder::new();
    let mut types = StringBuilder::new();
    let mut temporary = Vec::new();
    for t in table_names {
        if !matches_pattern(&t, pattern) {
            continue;
        }
        names.append_value(&t);
        catalogs.append_value(&catalog);
        for part in &namespace {
            namespaces.values().append_value(part);
        }
        namespaces.append(true);
        descs.append_null();
        types.append_value("EXTERNAL");
        temporary.push(false);
    }
    let batch = RecordBatch::try_new(
        tables_schema(),
        vec![
            Arc::new(names.finish()) as ArrayRef,
            Arc::new(catalogs.finish()) as ArrayRef,
            Arc::new(namespaces.finish()) as ArrayRef,
            Arc::new(descs.finish()) as ArrayRef,
            Arc::new(types.finish()) as ArrayRef,
            Arc::new(BooleanArray::from(temporary)) as ArrayRef,
        ],
    )
    .map_err(internal)?;
    Ok(vec![batch])
}

async fn table_exists(
    engine: &Engine,
    registry: &CatalogRegistry,
    table_name: &str,
    db_name: Option<&str>,
) -> Result<bool, Status> {
    // Combine db_name (if given) with table_name; table_name itself may be qualified.
    let combined = match db_name {
        Some(db) if !db.is_empty() => format!("{db}.{table_name}"),
        _ => table_name.to_string(),
    };
    let parts = split_ident(&combined);
    let (table, ns_parts) = match parts.split_last() {
        Some((t, rest)) => (t.clone(), rest.to_vec()),
        None => return Ok(false),
    };
    let (catalog, namespace) = if ns_parts.is_empty() {
        (registry.current_catalog(), registry.current_namespace())
    } else {
        resolve_namespace(registry, &ns_parts.join("."))
    };

    if let Some(provider) = registry.provider(&catalog) {
        return provider
            .table_exists(&namespace, &table)
            .await
            .map_err(err_to_status);
    }
    // Built-in catalog: check DataFusion's registered tables in the namespace.
    let schema = namespace.last().cloned().unwrap_or_default();
    Ok(engine.builtin_table_names(&schema).contains(&table))
}

async fn database_exists(
    engine: &Engine,
    registry: &CatalogRegistry,
    db_name: &str,
) -> Result<bool, Status> {
    let (catalog, namespace) = resolve_namespace(registry, db_name);
    if let Some(provider) = registry.provider(&catalog) {
        return provider
            .namespace_exists(&namespace)
            .await
            .map_err(err_to_status);
    }
    let target = namespace.join(".");
    Ok(engine.builtin_namespaces().contains(&target))
}

// ---- resolution helpers ----------------------------------------------------

/// List the namespaces of `catalog`: from its provider if external, else DataFusion's built-in.
async fn namespaces_of(
    engine: &Engine,
    registry: &CatalogRegistry,
    catalog: &str,
) -> Result<Vec<Vec<String>>, Status> {
    if let Some(provider) = registry.provider(catalog) {
        provider.list_namespaces(&[]).await.map_err(err_to_status)
    } else {
        Ok(engine
            .builtin_namespaces()
            .into_iter()
            .map(|s| vec![s])
            .collect())
    }
}

/// List the table names of `catalog`.`namespace`.
async fn tables_of(
    engine: &Engine,
    registry: &CatalogRegistry,
    catalog: &str,
    namespace: &[String],
) -> Result<Vec<String>, Status> {
    if let Some(provider) = registry.provider(catalog) {
        provider.list_tables(namespace).await.map_err(err_to_status)
    } else {
        let schema = namespace.last().cloned().unwrap_or_default();
        Ok(engine.builtin_table_names(&schema))
    }
}

/// Split a (possibly catalog-qualified) database identifier into `(catalog, namespace)`.
/// If the first part names a registered catalog, it's the catalog and the rest is the namespace;
/// otherwise the whole thing is a namespace in the current catalog.
fn resolve_namespace(registry: &CatalogRegistry, db: &str) -> (String, Vec<String>) {
    let parts = split_ident(db);
    if let Some((first, rest)) = parts.split_first() {
        if !rest.is_empty() && registry.contains(first) {
            return (first.clone(), rest.to_vec());
        }
    }
    (registry.current_catalog(), parts)
}

fn matches_pattern(name: &str, pattern: Option<&str>) -> bool {
    match pattern {
        None => true,
        Some(p) if p.is_empty() || p == "*" => true,
        // Spark uses a SQL `LIKE`-ish glob; support the common `*` wildcard, else substring.
        Some(p) => {
            if let Some(stripped) = p.strip_suffix('*') {
                name.starts_with(stripped.trim_end_matches('*'))
            } else {
                name == p
            }
        }
    }
}

// ---- result builders + schemas --------------------------------------------

fn scalar_string(col: &str, value: &str) -> Vec<RecordBatch> {
    use weft_loom::arrow::array::StringArray;
    let schema = Arc::new(Schema::new(vec![Field::new(col, DataType::Utf8, false)]));
    vec![
        RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec![value]))])
            .expect("scalar"),
    ]
}

fn scalar_bool(value: bool) -> Vec<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "exists",
        DataType::Boolean,
        false,
    )]));
    vec![
        RecordBatch::try_new(schema, vec![Arc::new(BooleanArray::from(vec![value]))])
            .expect("bool"),
    ]
}

/// An empty (zero-row, zero-column) result for the set-current ops.
fn empty_result() -> Vec<RecordBatch> {
    vec![RecordBatch::new_empty(Arc::new(Schema::empty()))]
}

fn catalogs_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("description", DataType::Utf8, true),
    ]))
}

fn databases_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("catalog", DataType::Utf8, true),
        Field::new("description", DataType::Utf8, true),
        Field::new("locationUri", DataType::Utf8, false),
    ]))
}

fn tables_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("catalog", DataType::Utf8, true),
        Field::new(
            "namespace",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        ),
        Field::new("description", DataType::Utf8, true),
        Field::new("tableType", DataType::Utf8, false),
        Field::new("isTemporary", DataType::Boolean, false),
    ]))
}

fn cat_op_name(ct: &sc::catalog::CatType) -> &'static str {
    use sc::catalog::CatType::*;
    match ct {
        CreateTable(_) | CreateExternalTable(_) => "createTable",
        DropTable(_) => "dropTable",
        CreateDatabase(_) => "createDatabase",
        DropDatabase(_) => "dropDatabase",
        ListColumns(_) => "listColumns",
        ListFunctions(_) => "listFunctions",
        _ => "this catalog operation",
    }
}

fn internal(e: impl std::fmt::Display) -> Status {
    Status::internal(format!("catalog result: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use weft_catalog::{
        CatalogProvider as WeftCat, Error as CatErr, Result as CatRes, TableFormat, TableMetadata,
    };
    use weft_loom::arrow::array::{Int64Array, StringArray};
    use weft_loom::arrow::datatypes::{DataType as Dt, Field as F, Schema as Sch};
    use weft_loom::arrow::record_batch::RecordBatch;

    struct FakeCat {
        location: String,
    }

    #[async_trait]
    impl WeftCat for FakeCat {
        fn name(&self) -> &str {
            "prod"
        }
        async fn list_namespaces(&self, parent: &[String]) -> CatRes<Vec<Vec<String>>> {
            if parent.is_empty() {
                Ok(vec![vec!["sales".to_string()]])
            } else {
                Ok(vec![])
            }
        }
        async fn list_tables(&self, ns: &[String]) -> CatRes<Vec<String>> {
            if ns == ["sales"] {
                Ok(vec!["orders".to_string()])
            } else {
                Ok(vec![])
            }
        }
        async fn load_table(&self, ns: &[String], t: &str) -> CatRes<TableMetadata> {
            if ns == ["sales"] && t == "orders" {
                Ok(TableMetadata::new(
                    "prod.sales.orders",
                    self.location.clone(),
                    TableFormat::Parquet,
                ))
            } else {
                Err(CatErr::Plan(format!("no such table {t}")))
            }
        }
    }

    fn parquet_dir() -> std::path::PathBuf {
        use weft_loom::arrow::array::Int64Array;
        let dir = std::env::temp_dir().join(format!("weft-conn-cat-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let schema = Arc::new(Sch::new(vec![F::new("x", Dt::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let f = std::fs::File::create(dir.join("part-0.parquet")).unwrap();
        let mut w = datafusion::parquet::arrow::ArrowWriter::try_new(f, schema, None).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
        dir
    }

    fn col_strings(b: &RecordBatch, col: usize) -> Vec<String> {
        b.column(col)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .map(|s| s.unwrap_or_default().to_string())
            .collect()
    }

    fn op(ct: sc::catalog::CatType) -> sc::Catalog {
        sc::Catalog { cat_type: Some(ct) }
    }

    #[tokio::test]
    async fn external_catalog_listing_and_lazy_query() {
        use sc::catalog::CatType;
        let dir = parquet_dir();
        let location = format!("file://{}", dir.to_string_lossy());

        let engine = Engine::new();
        let registry = CatalogRegistry::new();
        let provider: Arc<dyn WeftCat> = Arc::new(FakeCat { location });
        engine.register_catalog("prod", provider.clone());
        registry.register("prod", provider);
        registry.set_current_catalog("prod").unwrap();
        registry.set_current_namespace("sales");

        // listCatalogs includes the external catalog.
        let b = handle_catalog(
            &engine,
            &registry,
            &op(CatType::ListCatalogs(sc::ListCatalogs { pattern: None })),
        )
        .await
        .unwrap();
        assert!(col_strings(&b[0], 0).contains(&"prod".to_string()));

        // listDatabases on the current (external) catalog.
        let b = handle_catalog(
            &engine,
            &registry,
            &op(CatType::ListDatabases(sc::ListDatabases { pattern: None })),
        )
        .await
        .unwrap();
        assert_eq!(col_strings(&b[0], 0), vec!["sales".to_string()]);

        // listTables → orders.
        let b = handle_catalog(
            &engine,
            &registry,
            &op(CatType::ListTables(sc::ListTables {
                db_name: None,
                pattern: None,
            })),
        )
        .await
        .unwrap();
        assert_eq!(col_strings(&b[0], 0), vec!["orders".to_string()]);

        // tableExists for a real and a missing table.
        let b = handle_catalog(
            &engine,
            &registry,
            &op(CatType::TableExists(sc::TableExists {
                table_name: "orders".to_string(),
                db_name: Some("sales".to_string()),
            })),
        )
        .await
        .unwrap();
        assert!(bool_at(&b[0]));
        let b = handle_catalog(
            &engine,
            &registry,
            &op(CatType::TableExists(sc::TableExists {
                table_name: "ghost".to_string(),
                db_name: Some("sales".to_string()),
            })),
        )
        .await
        .unwrap();
        assert!(!bool_at(&b[0]));

        // The table was never pre-registered — this SQL resolves it lazily through the bridge.
        let batches = engine
            .sql("SELECT COUNT(*) AS c FROM prod.sales.orders")
            .await
            .unwrap();
        let c = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(c, 3);
        let _ = std::fs::remove_dir_all(&dir);
    }

    fn bool_at(b: &RecordBatch) -> bool {
        use weft_loom::arrow::array::BooleanArray;
        b.column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .value(0)
    }

    #[tokio::test]
    async fn current_catalog_defaults_and_set_errors() {
        use sc::catalog::CatType;
        let engine = Engine::new();
        let registry = CatalogRegistry::new();
        let b = handle_catalog(
            &engine,
            &registry,
            &op(CatType::CurrentCatalog(sc::CurrentCatalog {})),
        )
        .await
        .unwrap();
        assert_eq!(col_strings(&b[0], 0), vec!["spark_catalog".to_string()]);

        // Setting an unregistered catalog is an error.
        let err = handle_catalog(
            &engine,
            &registry,
            &op(CatType::SetCurrentCatalog(sc::SetCurrentCatalog {
                catalog_name: "nope".to_string(),
            })),
        )
        .await
        .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn config_grouping_and_provider_build() {
        let mut config = HashMap::new();
        config.insert(
            "spark.sql.catalog.prod.type".to_string(),
            "hive".to_string(),
        );
        config.insert(
            "spark.sql.catalog.prod.uri".to_string(),
            "thrift://hms:9083".to_string(),
        );
        config.insert("spark.sql.shuffle.partitions".to_string(), "8".to_string());
        let groups = group_catalog_options(&config);
        assert_eq!(groups.len(), 1);
        let prod = &groups["prod"];
        assert_eq!(prod["type"], "hive");
        // Builds without connecting (connection is lazy).
        assert!(build_provider("prod", prod).is_ok());
        // Unknown type is a clean unimplemented error.
        let mut bad = HashMap::new();
        bad.insert("type".to_string(), "mystery".to_string());
        // `.err().unwrap()` (not `.unwrap_err()`) — the Ok type `Arc<dyn CatalogProvider>` is not
        // `Debug`, which `unwrap_err`'s panic message would require.
        assert_eq!(
            build_provider("x", &bad).err().unwrap().code(),
            tonic::Code::Unimplemented
        );
    }
}

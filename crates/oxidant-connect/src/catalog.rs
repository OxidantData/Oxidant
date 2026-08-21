//! Catalog wiring for the Spark Connect front-end:
//! - parse `spark.sql.catalog.<name>.*` config into provider instances (the Spark-compatible,
//!   zero-code way to bring an external catalog), via [`build_provider`];
//! - serve the Spark `Catalog` RPC (`listCatalogs`/`listDatabases`/`listTables`/`tableExists`/
//!   current-catalog/db) from the [`CatalogRegistry`] + providers, in [`handle_catalog`].
//!
//! Query *resolution* is handled separately by the DataFusion bridge `oxidant-loom` registers; this
//! module is the metadata/listing surface and the config seam.

use std::collections::HashMap;
use std::sync::Arc;

use oxidant_catalog::{split_ident, CatalogProvider, CatalogRegistry};
use oxidant_loom::arrow::array::{ArrayRef, BooleanArray, ListBuilder, StringBuilder};
use oxidant_loom::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use oxidant_loom::arrow::record_batch::RecordBatch;
use oxidant_loom::Engine;
use oxidant_proto::spark::connect as sc;
use tonic::Status;

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
///
/// Async because the Glue provider loads its AWS SDK config (`aws-config::load`) at build time —
/// still cheap and non-networked: credentials resolve lazily on the first actual Glue call.
pub async fn build_provider(
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
            let cat = oxidant_catalog_hive::HiveCatalog::from_config(name, options)
                .map_err(err_to_status)?;
            Ok(Arc::new(cat))
        }
        "glue" => {
            // Credentials come from the standard AWS chain (env, shared config, instance role /
            // IRSA); `region` (default us-west-2) and an optional `warehouse` arrive as
            // `spark.sql.catalog.<name>.{region,warehouse}`.
            let cat = oxidant_catalog_glue::GlueCatalog::from_config(name, options).await;
            match build_lakeformation_authorizer(name, options).await? {
                Some(authorizer) => Ok(Arc::new(cat.with_authorizer(authorizer))),
                None => Ok(Arc::new(cat)),
            }
        }
        "rest" | "unity" | "iceberg" => {
            let cat = oxidant_catalog_rest::RestCatalog::from_config(name, options)
                .map_err(err_to_status)?;
            Ok(Arc::new(cat))
        }
        "local" => {
            let cat = build_local_catalog(name, options).await?;
            Ok(Arc::new(cat))
        }
        other => Err(Status::unimplemented(format!(
            "catalog type `{other}` is not supported yet \
             (have: local, hive, glue, rest/unity/iceberg)"
        ))),
    }
}

/// Build a filesystem/object-store catalog from its options.
///
/// `tables` and `discover` arrive as JSON strings under a single option each. The rest of the
/// catalog config is flat `key=value`, and a nested structure has nowhere to live in that shape —
/// carrying them as JSON keeps a config-file catalog and a `--catalog-conf` one on one code path
/// instead of two. `oxidant-config` writes exactly this encoding.
async fn build_local_catalog(
    name: &str,
    options: &HashMap<String, String>,
) -> Result<oxidant_catalog_local::LocalCatalog, Status> {
    let warehouse = options
        .get("warehouse")
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            Status::invalid_argument(format!(
                "local catalog `{name}` needs `spark.sql.catalog.{name}.warehouse` — it is where \
                 tables created by a pipeline or by CREATE TABLE are written"
            ))
        })?;

    let declared = match options.get("tables") {
        Some(json) => parse_declared_tables(name, json)?,
        None => Vec::new(),
    };
    // Storage credentials for the warehouse itself, and the default for every `discover:` root
    // that does not carry its own. Without this an `s3://` warehouse can only ever use the
    // ambient AWS chain, and the pinned keys in the config are silently ignored.
    let storage = storage_options(options);
    let discover = match options.get("discover") {
        Some(json) => parse_discover_roots(name, json, &storage)?,
        None => Vec::new(),
    };

    oxidant_catalog_local::LocalCatalog::new(name, warehouse, storage, declared, discover)
        .await
        .map_err(err_to_status)
}

/// The storage-credential subset of a catalog's flat options.
///
/// An allowlist by prefix rather than the whole map: the catalog's options also carry `warehouse`,
/// `type`, and the JSON blobs, and handing those to `AmazonS3Builder` would be meaningless at best.
fn storage_options(options: &HashMap<String, String>) -> HashMap<String, String> {
    const STORAGE_PREFIXES: &[&str] = &["s3.", "fs.s3a.", "aws."];
    options
        .iter()
        .filter(|(key, _)| STORAGE_PREFIXES.iter().any(|p| key.starts_with(p)))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}

/// Decode the `tables` option: a JSON object of `"namespace.table" -> {format, location, ...}`.
fn parse_declared_tables(
    name: &str,
    json: &str,
) -> Result<Vec<oxidant_catalog_local::DeclaredTable>, Status> {
    #[derive(serde::Deserialize)]
    struct Entry {
        format: String,
        location: String,
        #[serde(default)]
        options: std::collections::BTreeMap<String, String>,
        #[serde(default)]
        partition_columns: Vec<String>,
    }

    let parsed: std::collections::BTreeMap<String, Entry> =
        serde_json::from_str(json).map_err(|e| {
            Status::invalid_argument(format!("catalog `{name}`: `tables` is not valid JSON: {e}"))
        })?;
    let mut out = Vec::with_capacity(parsed.len());
    for (key, entry) in parsed {
        let (namespace, table) = key.split_once('.').ok_or_else(|| {
            Status::invalid_argument(format!(
                "catalog `{name}`: table key `{key}` must be `namespace.table`"
            ))
        })?;
        let format =
            oxidant_catalog::TableFormat::from_provider(&entry.format).ok_or_else(|| {
                // Named rather than defaulted: silently reading an ORC table as Parquet would fail
                // deep in a scan with an error that points nowhere near the config.
                Status::invalid_argument(format!(
                    "catalog `{name}`: table `{key}` has unreadable format `{}`",
                    entry.format
                ))
            })?;
        out.push(oxidant_catalog_local::DeclaredTable {
            namespace: namespace.to_string(),
            table: table.to_string(),
            format,
            location: entry.location,
            storage_options: entry.options,
            partition_columns: entry.partition_columns,
        });
    }
    Ok(out)
}

/// Decode the `discover` option: a JSON array of `{namespace, path}`.
fn parse_discover_roots(
    name: &str,
    json: &str,
    inherited: &HashMap<String, String>,
) -> Result<Vec<oxidant_catalog_local::DiscoverRoot>, Status> {
    #[derive(serde::Deserialize)]
    struct Entry {
        namespace: String,
        path: String,
        /// Per-root storage credentials. Absent, the catalog's own are inherited — a root in the
        /// same bucket as the warehouse should not have to repeat them.
        #[serde(default)]
        options: Option<std::collections::BTreeMap<String, String>>,
    }

    let parsed: Vec<Entry> = serde_json::from_str(json).map_err(|e| {
        Status::invalid_argument(format!(
            "catalog `{name}`: `discover` is not valid JSON: {e}"
        ))
    })?;
    Ok(parsed
        .into_iter()
        .map(|entry| oxidant_catalog_local::DiscoverRoot {
            namespace: entry.namespace,
            path: entry.path,
            storage_options: entry.options.unwrap_or_else(|| {
                inherited
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect()
            }),
        })
        .collect())
}

/// Parse a boolean catalog option (`true`/`1`/`yes`/`on`, case-insensitive). Anything else that is
/// non-empty is an error rather than a silent `false` — a typo in a security switch must not
/// quietly leave enforcement off.
fn parse_bool_option(catalog: &str, key: &str, raw: &str) -> Result<bool, Status> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "" => Ok(false),
        "true" | "1" | "yes" | "on" => Ok(true),
        "false" | "0" | "no" | "off" => Ok(false),
        other => Err(Status::invalid_argument(format!(
            "spark.sql.catalog.{catalog}.{key} must be a boolean (got `{other}`)"
        ))),
    }
}

/// Build the Lake Formation authorizer for a `glue` catalog, when the operator enabled it.
///
/// Options, all under `spark.sql.catalog.<name>.`:
///
/// | Key | Meaning |
/// |---|---|
/// | `lakeformation` | `true` turns fine-grained access control on |
/// | `lakeformation.identity` | `hybrid` (default), `user`, or `machine` |
/// | `lakeformation.runtime_role_arn` | IAM role representing the querying user, assumed via STS |
/// | `lakeformation.authorized_caller` | Value of the `LakeFormationAuthorizedCaller` session tag |
/// | `lakeformation.catalog_id` | Glue catalog (account) ID; defaults to the caller's account |
/// | `lakeformation.vend_credentials` | `true` (default) reads data with Lake Formation-vended credentials |
///
/// Construction resolves the principal eagerly — including assuming the runtime role — so a
/// misconfiguration fails loudly here rather than surfacing later as a confusing per-table denial.
/// Notably it does **not** degrade to an unenforced catalog on error: silently continuing without
/// the authorization layer the operator asked for is the one outcome a security switch must never
/// produce.
async fn build_lakeformation_authorizer(
    name: &str,
    options: &HashMap<String, String>,
) -> Result<Option<Arc<dyn oxidant_catalog::TableAuthorizer>>, Status> {
    use oxidant_catalog_lakeformation::enforcement::{
        AuthorizerConfig, IdentityMode, LakeFormationAuthorizer,
    };

    let enabled = options
        .get("lakeformation")
        .map(|v| parse_bool_option(name, "lakeformation", v))
        .transpose()?
        .unwrap_or(false);
    if !enabled {
        return Ok(None);
    }

    let identity = match options.get("lakeformation.identity") {
        Some(raw) => IdentityMode::parse(raw).map_err(err_to_status)?,
        None => IdentityMode::default(),
    };
    // Defaults ON, matching Athena and EMR: governed data is read with credentials Lake Formation
    // scoped to the table, not with the engine's own identity. Turning it off leaves the column/row
    // policy applied but makes enforcement advisory — anyone with direct S3 access to the prefix
    // bypasses it — so it is an escape hatch, not a tuning knob.
    let vend_credentials = options
        .get("lakeformation.vend_credentials")
        .map(|v| parse_bool_option(name, "lakeformation.vend_credentials", v))
        .transpose()?
        .unwrap_or(true);

    let config = AuthorizerConfig {
        region: crate::catalog::resolve_catalog_region(options),
        catalog_id: options.get("lakeformation.catalog_id").cloned(),
        identity,
        runtime_role_arn: options
            .get("lakeformation.runtime_role_arn")
            .cloned()
            .filter(|s| !s.trim().is_empty()),
        authorized_caller: options
            .get("lakeformation.authorized_caller")
            .cloned()
            .filter(|s| !s.trim().is_empty()),
        vend_credentials,
    };

    let authorizer = LakeFormationAuthorizer::new(config)
        .await
        .map_err(err_to_status)?;
    Ok(Some(Arc::new(authorizer)))
}

/// Region for a catalog's AWS clients: option → `AWS_REGION` → `AWS_DEFAULT_REGION` → `us-west-2`.
/// Mirrors the Glue and Lake Formation providers' own precedence so one catalog's clients never
/// disagree about which region they are talking to.
fn resolve_catalog_region(options: &HashMap<String, String>) -> String {
    options
        .get("region")
        .cloned()
        .filter(|s| !s.trim().is_empty())
        .or_else(|| std::env::var("AWS_REGION").ok().filter(|s| !s.is_empty()))
        .or_else(|| {
            std::env::var("AWS_DEFAULT_REGION")
                .ok()
                .filter(|s| !s.is_empty())
        })
        .unwrap_or_else(|| "us-west-2".to_string())
}

/// Serve a Spark `Catalog` relation, returning the result rows as Arrow batches.
///
/// KAN-85: the current catalog/namespace pointers live on the (per-session) engine — the SAME
/// state SQL `USE` drives — so `spark.catalog.setCurrentCatalog("glue")` and a later SQL
/// `SHOW TABLES` see one consistent session state. `registry` is still the provider map
/// (`catalog_names`/`contains`/`provider`).
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
        CatType::CurrentCatalog(_) => Ok(scalar_string(
            "name",
            &engine.current_catalog_and_namespace().0,
        )),
        CatType::SetCurrentCatalog(s) => {
            // Same semantics as SQL `USE CATALOG` (KAN-84 namespace reset, KAN-87 matching).
            engine
                .set_current_catalog(&s.catalog_name)
                .await
                .map_err(err_to_status)?;
            Ok(empty_result())
        }
        CatType::CurrentDatabase(_) => Ok(scalar_string(
            "name",
            &engine.current_catalog_and_namespace().1.join("."),
        )),
        CatType::SetCurrentDatabase(s) => {
            // Same semantics as SQL `USE <db>` (KAN-86 existence validation included).
            engine
                .set_current_namespace(&s.db_name)
                .await
                .map_err(err_to_status)?;
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
        CatType::RefreshTable(r) => {
            // Evict the bridge's cached provider for the table + bump the catalog version so
            // cached stage plans rebuild. Driver-side only: worker processes converge via the
            // catalog cache TTL (`OXIDANT_CATALOG_CACHE_TTL_MS`) — see `Engine::refresh_table`.
            engine
                .refresh_table(&r.table_name)
                .await
                .map_err(err_to_status)?;
            Ok(empty_result())
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
    let catalog = engine.current_catalog_and_namespace().0;
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
    // otherwise use the session's current catalog + current/just-given database (engine state,
    // KAN-85 — the same pointers SQL `USE` drives).
    let (catalog, namespace) = match db_name {
        Some(db) => resolve_namespace(engine, registry, db),
        None => engine.current_catalog_and_namespace(),
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
        engine.current_catalog_and_namespace()
    } else {
        resolve_namespace(engine, registry, &ns_parts.join("."))
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
    let (catalog, namespace) = resolve_namespace(engine, registry, db_name);
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
/// otherwise the whole thing is a namespace in the session's current catalog (engine state,
/// KAN-85).
fn resolve_namespace(
    engine: &Engine,
    registry: &CatalogRegistry,
    db: &str,
) -> (String, Vec<String>) {
    let parts = split_ident(db);
    if let Some((first, rest)) = parts.split_first() {
        if !rest.is_empty() && registry.contains(first) {
            return (first.clone(), rest.to_vec());
        }
    }
    let (catalog, _) = engine.current_catalog_and_namespace();
    (catalog, parts)
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
    use oxidant_loom::arrow::array::StringArray;
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
        RefreshTable(_) => "refreshTable",
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
    use oxidant_catalog::{
        CatalogProvider as OxidantCat, Error as CatErr, Result as CatRes, TableFormat,
        TableMetadata,
    };
    use oxidant_loom::arrow::array::{Int64Array, StringArray};
    use oxidant_loom::arrow::datatypes::{DataType as Dt, Field as F, Schema as Sch};
    use oxidant_loom::arrow::record_batch::RecordBatch;

    struct FakeCat {
        location: String,
    }

    #[async_trait]
    impl OxidantCat for FakeCat {
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
        parquet_dir_with_rows(3)
    }

    /// Write a one-column (`x: Int64`) parquet file with `n` rows into a fresh temp dir.
    fn parquet_dir_with_rows(n: usize) -> std::path::PathBuf {
        use oxidant_loom::arrow::array::Int64Array;
        // Tests run as threads in ONE process (same pid) and start in parallel — pid+nanos
        // alone collided in practice (one test's cleanup removed another's dir), so a
        // process-unique counter disambiguates.
        static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "oxidant-conn-cat-{}-{}-{}",
            std::process::id(),
            SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let schema = Arc::new(Sch::new(vec![F::new("x", Dt::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from_iter(0..n as i64))],
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
        let provider: Arc<dyn OxidantCat> = Arc::new(FakeCat { location });
        engine.register_catalog("prod", provider.clone());
        registry.register("prod", provider);
        // KAN-85: the current catalog/namespace pointers live on the engine (the same state
        // SQL `USE` drives), not on the registry's provider map.
        engine.set_current_catalog("prod").await.unwrap();
        engine.set_current_namespace("sales").await.unwrap();

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

    /// A fake catalog whose `sales.orders` location can be swapped between resolutions — the
    /// `refreshTable` lever (changed metastore metadata must become visible after a refresh).
    struct MutableFakeCat {
        location: std::sync::Mutex<String>,
    }

    #[async_trait]
    impl OxidantCat for MutableFakeCat {
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
                    self.location.lock().unwrap().clone(),
                    TableFormat::Parquet,
                ))
            } else {
                Err(CatErr::Plan(format!("no such table {t}")))
            }
        }
    }

    async fn count_orders(engine: &Engine) -> i64 {
        let batches = engine
            .sql("SELECT COUNT(*) AS c FROM prod.sales.orders")
            .await
            .unwrap();
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0)
    }

    /// `spark.catalog.refreshTable`: evicts the bridge's cached provider so the next query
    /// re-resolves the table from the metastore — a changed location/schema is picked up
    /// without an engine restart. Both the bare-name (session current catalog+namespace) and
    /// the fully-qualified forms are exercised.
    #[tokio::test]
    async fn refresh_table_picks_up_changed_metadata() {
        use sc::catalog::CatType;
        let dir = parquet_dir_with_rows(3);
        let dir2 = parquet_dir_with_rows(5);
        let mutable = Arc::new(MutableFakeCat {
            location: std::sync::Mutex::new(format!("file://{}", dir.to_string_lossy())),
        });
        let provider: Arc<dyn OxidantCat> = mutable.clone();
        let engine = Engine::new();
        let registry = CatalogRegistry::new();
        engine.register_catalog("prod", provider.clone());
        registry.register("prod", provider);
        engine.set_current_catalog("prod").await.unwrap();
        engine.set_current_namespace("sales").await.unwrap();

        // First query resolves + caches the provider (3-row location).
        assert_eq!(count_orders(&engine).await, 3);

        // The metastore moves underneath (now 5 rows); the cache still serves the old provider.
        *mutable.location.lock().unwrap() = format!("file://{}", dir2.to_string_lossy());
        assert_eq!(
            count_orders(&engine).await,
            3,
            "within the cache TTL the stale provider is served"
        );

        // refreshTable (bare table name → session's current catalog + namespace) evicts it.
        let b = handle_catalog(
            &engine,
            &registry,
            &op(CatType::RefreshTable(sc::RefreshTable {
                table_name: "orders".to_string(),
            })),
        )
        .await
        .unwrap();
        assert_eq!(b[0].num_rows(), 0, "refreshTable is a side-effect-only op");
        assert_eq!(
            count_orders(&engine).await,
            5,
            "post-refresh sees the new location"
        );

        // The fully-qualified form works too (swap back to the 3-row location).
        *mutable.location.lock().unwrap() = format!("file://{}", dir.to_string_lossy());
        handle_catalog(
            &engine,
            &registry,
            &op(CatType::RefreshTable(sc::RefreshTable {
                table_name: "prod.sales.orders".to_string(),
            })),
        )
        .await
        .unwrap();
        assert_eq!(count_orders(&engine).await, 3);

        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&dir2);
    }

    /// refreshTable edge case (KAN-84): external-catalog sessions seed an EMPTY namespace, so a
    /// bare-name refresh has no `db` segment and no session namespace to key the bridge's schema
    /// providers by. A naive `namespace.last()` guard would silently no-op; instead the eviction
    /// falls back to a bare-name sweep of the current external catalog's schema providers.
    #[tokio::test]
    async fn refresh_table_bare_name_with_empty_session_namespace() {
        use sc::catalog::CatType;
        let dir = parquet_dir_with_rows(3);
        let dir2 = parquet_dir_with_rows(5);
        let mutable = Arc::new(MutableFakeCat {
            location: std::sync::Mutex::new(format!("file://{}", dir.to_string_lossy())),
        });
        let provider: Arc<dyn OxidantCat> = mutable.clone();
        let engine = Engine::new();
        let registry = CatalogRegistry::new();
        engine.register_catalog("prod", provider.clone());
        registry.register("prod", provider);
        // USE CATALOG prod — and nothing else: the session namespace stays EMPTY (KAN-84).
        engine.set_current_catalog("prod").await.unwrap();
        assert!(
            engine.current_catalog_and_namespace().1.is_empty(),
            "external-catalog session seeds an empty namespace"
        );

        // Resolve + cache the provider via a fully-qualified query (3-row location).
        assert_eq!(count_orders(&engine).await, 3);

        // The metastore moves; the bare-name refresh must still evict despite the empty
        // session namespace.
        *mutable.location.lock().unwrap() = format!("file://{}", dir2.to_string_lossy());
        handle_catalog(
            &engine,
            &registry,
            &op(CatType::RefreshTable(sc::RefreshTable {
                table_name: "orders".to_string(),
            })),
        )
        .await
        .unwrap();
        assert_eq!(
            count_orders(&engine).await,
            5,
            "bare-name refresh evicts even with an empty session namespace"
        );

        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&dir2);
    }

    fn bool_at(b: &RecordBatch) -> bool {
        use oxidant_loom::arrow::array::BooleanArray;
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

    /// KAN-85 unification: the Catalog RPC and SQL `USE` operate on the SAME per-session
    /// state — `spark.catalog.setCurrentCatalog("prod")` must steer a later SQL `SHOW TABLES`
    /// in that session, and must not leak into another session.
    #[tokio::test]
    async fn rpc_set_current_then_sql_read_is_consistent_per_session() {
        use sc::catalog::CatType;
        let dir = parquet_dir();
        let location = format!("file://{}", dir.to_string_lossy());

        let engine = Engine::new();
        let registry = CatalogRegistry::new();
        let provider: Arc<dyn OxidantCat> = Arc::new(FakeCat { location });
        engine.register_catalog("prod", provider.clone());
        registry.register("prod", provider);

        let s1 = engine.for_session("s1");
        let s2 = engine.for_session("s2");

        // The RPC sets s1's current catalog/namespace (same state SQL USE drives).
        handle_catalog(
            &s1,
            &registry,
            &op(CatType::SetCurrentCatalog(sc::SetCurrentCatalog {
                catalog_name: "prod".to_string(),
            })),
        )
        .await
        .unwrap();
        handle_catalog(
            &s1,
            &registry,
            &op(CatType::SetCurrentDatabase(sc::SetCurrentDatabase {
                db_name: "sales".to_string(),
            })),
        )
        .await
        .unwrap();

        // SQL SHOW TABLES in s1 lists prod.sales' tables.
        let batches = s1.sql("SHOW TABLES").await.unwrap();
        let names: Vec<String> = batches.iter().flat_map(|b| col_strings(b, 1)).collect();
        assert_eq!(names, vec!["orders".to_string()], "{names:?}");

        // The RPC reads the same state back for s1 …
        let b = handle_catalog(
            &s1,
            &registry,
            &op(CatType::CurrentCatalog(sc::CurrentCatalog {})),
        )
        .await
        .unwrap();
        assert_eq!(col_strings(&b[0], 0), vec!["prod".to_string()]);
        // … while s2 stays on the builtin default.
        let b = handle_catalog(
            &s2,
            &registry,
            &op(CatType::CurrentCatalog(sc::CurrentCatalog {})),
        )
        .await
        .unwrap();
        assert_eq!(col_strings(&b[0], 0), vec!["spark_catalog".to_string()]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn config_grouping_and_provider_build() {
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
        assert!(build_provider("prod", prod).await.is_ok());
        // Unknown type is a clean unimplemented error.
        let mut bad = HashMap::new();
        bad.insert("type".to_string(), "mystery".to_string());
        // `.err().unwrap()` (not `.unwrap_err()`) — the Ok type `Arc<dyn CatalogProvider>` is not
        // `Debug`, which `unwrap_err`'s panic message would require.
        assert_eq!(
            build_provider("x", &bad).await.err().unwrap().code(),
            tonic::Code::Unimplemented
        );
    }

    // ---- Lake Formation config parsing -------------------------------------
    //
    // These cover the option parsing only. Actually constructing the authorizer calls
    // `sts:GetCallerIdentity`, so it belongs in the AWS-backed suite, not in unit tests.

    #[test]
    fn lakeformation_enable_flag_accepts_the_usual_boolean_spellings() {
        for on in ["true", "TRUE", "1", "yes", "on", " true "] {
            assert!(
                parse_bool_option("glue", "lakeformation", on).expect("valid"),
                "{on}"
            );
        }
        for off in ["false", "0", "no", "off", ""] {
            assert!(
                !parse_bool_option("glue", "lakeformation", off).expect("valid"),
                "{off}"
            );
        }
    }

    /// A typo in a security switch must be an error, never a silent `false` that leaves
    /// enforcement off while the operator believes it is on.
    #[test]
    fn misspelled_lakeformation_flag_is_an_error_not_a_silent_false() {
        let err = parse_bool_option("glue", "lakeformation", "ture").expect_err("typo");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(err.message().contains("lakeformation"), "{err}");
    }

    #[test]
    fn identity_mode_parsing_rejects_unknown_modes() {
        use oxidant_catalog_lakeformation::enforcement::IdentityMode;
        assert_eq!(IdentityMode::parse("hybrid").unwrap(), IdentityMode::Hybrid);
        assert_eq!(IdentityMode::parse("user").unwrap(), IdentityMode::User);
        assert_eq!(
            IdentityMode::parse("machine").unwrap(),
            IdentityMode::Machine
        );
        assert!(IdentityMode::parse("root").is_err());
    }

    /// A catalog without the flag must not build an authorizer — the whole feature is opt-in, and
    /// this is the assertion that keeps it that way.
    #[tokio::test]
    async fn lakeformation_is_off_unless_explicitly_enabled() {
        let mut opts = HashMap::new();
        opts.insert("region".to_string(), "us-west-2".to_string());
        assert!(build_lakeformation_authorizer("glue", &opts)
            .await
            .expect("no LF options")
            .is_none());

        opts.insert("lakeformation".to_string(), "false".to_string());
        assert!(build_lakeformation_authorizer("glue", &opts)
            .await
            .expect("explicitly disabled")
            .is_none());
    }

    #[test]
    fn catalog_region_precedence_prefers_the_explicit_option() {
        let mut opts = HashMap::new();
        opts.insert("region".to_string(), "eu-west-1".to_string());
        assert_eq!(resolve_catalog_region(&opts), "eu-west-1");
        // Blank is treated as unset rather than as a region named "".
        opts.insert("region".to_string(), "  ".to_string());
        assert!(!resolve_catalog_region(&opts).trim().is_empty());
    }
}

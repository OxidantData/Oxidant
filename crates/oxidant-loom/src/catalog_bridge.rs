//! Bridge a [`oxidant_catalog::CatalogProvider`] into DataFusion's catalog API.
//!
//! DataFusion already resolves three-part names (`catalog.schema.table`) and loads tables
//! **lazily and asynchronously** through [`SchemaProvider::table`]. This module adapts a oxidant
//! catalog onto that model so an external metastore plugs straight into query resolution: the
//! catalog is hit only when a query first references one of its tables, and the resolved
//! [`TableMetadata`] is turned into a `TableProvider` via the engine's shared listing-table
//! builder (so Parquet/Delta/Iceberg all read through the same version-safe path).
//!
//! Mapping to DataFusion's fixed three-level model: a oxidant *namespace* is the middle level
//! (DataFusion's "schema"), so it is single-part here — covering Hive (`database`) and Unity /
//! Iceberg-REST (`schema`). The sync `schema_names`/`table_names`/`table_exist` methods are
//! best-effort (a cached snapshot); authoritative listing for the `spark.catalog.*` RPC goes
//! straight to the oxidant provider in `oxidant-connect`, not through these.

use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::{CatalogProvider, SchemaProvider};
use datafusion::common::{DataFusionError, Result as DfResult};
use datafusion::datasource::{MemTable, TableProvider};
use datafusion::execution::context::SessionState;
use datafusion::prelude::SessionContext;
use oxidant_catalog::{CatalogProvider as OxidantCatalog, TableFormat, TableMetadata};
use oxidant_common::Error;
use oxidant_datasource::SnapshotIdentity;

#[derive(Default)]
struct LakehouseSnapshotContext {
    requested: HashMap<String, SnapshotIdentity>,
    observed: Mutex<HashMap<String, SnapshotIdentity>>,
}

tokio::task_local! {
    static LAKEHOUSE_SNAPSHOT_CONTEXT: Arc<LakehouseSnapshotContext>;
    /// Set while executing a stage the driver marked as touching Lake Formation-governed data.
    ///
    /// Enforcement is per-process: a worker resolves tables through its own catalog, so a worker
    /// started without the Lake Formation config has no authorizer and would happily read its
    /// shard unfiltered. The worker cannot detect that on its own — without an authorizer it has
    /// no way to know the table is governed. So the *driver*, which did resolve the policy, stamps
    /// the requirement on the stage ticket and the worker refuses any table whose catalog cannot
    /// enforce. Same shape as `require_lakehouse_snapshot_pins`.
    static LAKEFORMATION_REQUIRED: bool;
    /// The principal the driver resolved this query's policy as; empty when unknown.
    static LAKEFORMATION_PRINCIPAL: String;
}

/// Whether the driver observed Lake Formation-governed tables while resolving this query.
///
/// `Arc<AtomicBool>` rather than a plain flag because resolution happens across the task tree
/// under [`capture_lakeformation_enforcement`].
#[derive(Default)]
struct LakeFormationObservation {
    required: std::sync::atomic::AtomicBool,
    /// The principal the governed tables were resolved as, stamped on the stage tickets so a
    /// worker enforcing as a *different* principal is caught rather than silently applying its
    /// own, possibly broader, policy.
    principal: Mutex<String>,
}

tokio::task_local! {
    static LAKEFORMATION_OBSERVED: Arc<LakeFormationObservation>;
}

/// Run `future` recording whether any table it resolved was Lake Formation-governed. The driver
/// stamps the result on every stage ticket of the query.
pub async fn capture_lakeformation_enforcement<F, T>(
    future: F,
) -> oxidant_common::Result<(T, bool, String)>
where
    F: Future<Output = oxidant_common::Result<T>>,
{
    let observation = Arc::new(LakeFormationObservation::default());
    let value = LAKEFORMATION_OBSERVED
        .scope(observation.clone(), future)
        .await?;
    let principal = observation
        .principal
        .lock()
        .expect("lakeformation observation poisoned")
        .clone();
    Ok((
        value,
        observation.required.load(Ordering::Relaxed),
        principal,
    ))
}

/// Execute `future` under the driver's enforcement requirement (worker side).
pub async fn with_lakeformation_required<F, T>(required: bool, principal: String, future: F) -> T
where
    F: Future<Output = T>,
{
    LAKEFORMATION_PRINCIPAL
        .scope(principal, LAKEFORMATION_REQUIRED.scope(required, future))
        .await
}

fn note_lakeformation_enforced(principal: &str) {
    let _ = LAKEFORMATION_OBSERVED.try_with(|o| {
        o.required.store(true, Ordering::Relaxed);
        let mut held = o
            .principal
            .lock()
            .expect("lakeformation observation poisoned");
        if held.is_empty() {
            *held = principal.to_string();
        }
    });
}

/// The principal the driver stamped on this stage's ticket, when it stamped one.
fn required_lakeformation_principal() -> Option<String> {
    LAKEFORMATION_PRINCIPAL
        .try_with(|p| p.clone())
        .ok()
        .filter(|p| !p.is_empty())
}

fn lakeformation_required() -> bool {
    LAKEFORMATION_REQUIRED.try_with(|r| *r).unwrap_or(false)
}

pub(crate) async fn capture_lakehouse_snapshots<F, T>(
    future: F,
) -> oxidant_common::Result<(T, String)>
where
    F: Future<Output = oxidant_common::Result<T>>,
{
    let context = Arc::new(LakehouseSnapshotContext::default());
    let value = LAKEHOUSE_SNAPSHOT_CONTEXT
        .scope(context.clone(), future)
        .await?;
    let observed = context
        .observed
        .lock()
        .expect("lakehouse snapshot observations poisoned")
        .clone();
    let json = serde_json::to_string(&observed)
        .map_err(|e| Error::Execution(format!("serialize lakehouse snapshot pins: {e}")))?;
    Ok((value, json))
}

pub(crate) async fn with_lakehouse_snapshots<F, T>(
    pins_json: &str,
    future: F,
) -> oxidant_common::Result<T>
where
    F: Future<Output = oxidant_common::Result<T>>,
{
    let requested = if pins_json.trim().is_empty() {
        HashMap::new()
    } else {
        serde_json::from_str(pins_json)
            .map_err(|e| Error::Plan(format!("invalid lakehouse snapshot pins: {e}")))?
    };
    LAKEHOUSE_SNAPSHOT_CONTEXT
        .scope(
            Arc::new(LakehouseSnapshotContext {
                requested,
                observed: Mutex::new(HashMap::new()),
            }),
            future,
        )
        .await
}

fn requested_lakehouse_snapshot(table_name: &str) -> Option<SnapshotIdentity> {
    LAKEHOUSE_SNAPSHOT_CONTEXT
        .try_with(|context| context.requested.get(table_name).cloned())
        .ok()
        .flatten()
}

fn record_lakehouse_snapshot(table_name: &str, snapshot: &SnapshotIdentity) {
    let _ = LAKEHOUSE_SNAPSHOT_CONTEXT.try_with(|context| {
        context
            .observed
            .lock()
            .expect("lakehouse snapshot observations poisoned")
            .insert(table_name.to_string(), snapshot.clone());
    });
}

fn lakehouse_snapshot_context_is_set() -> bool {
    LAKEHOUSE_SNAPSHOT_CONTEXT.try_with(|_| ()).is_ok()
}

/// DataFusion `CatalogProvider` backed by a oxidant [`OxidantCatalog`].
pub struct OxidantCatalogProvider {
    catalog: Arc<dyn OxidantCatalog>,
    ctx: Arc<SessionContext>,
    require_lakehouse_snapshot_pins: Arc<AtomicBool>,
    /// Shared catalog-version counter (the engine's stage-plan-cache staleness guard), bumped
    /// when a TTL revalidation or an eviction detects/removes a changed table — see
    /// [`OxidantSchemaProvider::table`].
    catalog_version: Arc<AtomicU64>,
    /// Lazily-created per-namespace schema providers (cheap wrappers; cached so repeated
    /// references to the same namespace share a table cache).
    schemas: Mutex<HashMap<String, Arc<OxidantSchemaProvider>>>,
    /// Storage attributes staged for a CTAS in flight. See [`CtasAttributes`].
    pending_ctas: PendingCtas,
}

impl OxidantCatalogProvider {
    /// Wrap a oxidant catalog. `ctx` supplies the session state used to infer schemas / read files.
    pub fn new(
        catalog: Arc<dyn OxidantCatalog>,
        ctx: Arc<SessionContext>,
        require_lakehouse_snapshot_pins: Arc<AtomicBool>,
        catalog_version: Arc<AtomicU64>,
    ) -> Self {
        Self {
            catalog,
            ctx,
            require_lakehouse_snapshot_pins,
            catalog_version,
            schemas: Mutex::new(HashMap::new()),
            pending_ctas: PendingCtas::default(),
        }
    }

    /// Stage the storage attributes for a `CREATE TABLE … USING <fmt> … AS SELECT …` about to run
    /// against this catalog. See [`CtasAttributes`].
    pub(crate) fn set_pending_ctas_attributes(
        &self,
        namespace: &[String],
        table: &str,
        attributes: CtasAttributes,
    ) {
        self.pending_ctas
            .lock()
            .expect("pending CTAS attributes poisoned")
            .insert(ctas_attributes_key(namespace, table), attributes);
    }

    /// Drop a staged entry whose statement never reached `register_table` (a planning or execution
    /// failure). Without this, a failed `USING delta` CTAS would leave its format behind for the
    /// next plain CTAS of the same name to pick up.
    pub(crate) fn clear_pending_ctas_attributes(&self, namespace: &[String], table: &str) {
        self.pending_ctas
            .lock()
            .expect("pending CTAS attributes poisoned")
            .remove(&ctas_attributes_key(namespace, table));
    }

    /// Evict every cached variant of `table` in `namespace` (`spark.catalog.refreshTable`), so
    /// the next `table()` re-resolves it from the metastore. Returns whether anything was
    /// cached. The caller bumps the shared catalog version (the eviction itself does not, so a
    /// no-op eviction of a never-resolved table doesn't invalidate plan caches for nothing).
    pub fn evict_table(&self, namespace: &str, table: &str) -> bool {
        self.schemas
            .lock()
            .expect("schemas poisoned")
            .get(namespace)
            .is_some_and(|schema| schema.evict_table(table))
    }

    /// Evict every cached variant of `table` across ALL materialized schema providers, by bare
    /// name. Used by `spark.catalog.refreshTable` when the session has no current namespace to
    /// key on (external-catalog sessions seed an empty one — KAN-84): a bare-name refresh must
    /// still evict wherever the table was resolved. Returns whether anything was evicted.
    pub fn evict_table_anywhere(&self, table: &str) -> bool {
        self.schemas
            .lock()
            .expect("schemas poisoned")
            .values()
            .fold(false, |evicted, schema| {
                schema.evict_table(table) || evicted
            })
    }
}

impl fmt::Debug for OxidantCatalogProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OxidantCatalogProvider")
            .field("catalog", &self.catalog.name())
            .finish()
    }
}

impl CatalogProvider for OxidantCatalogProvider {
    fn schema_names(&self) -> Vec<String> {
        // Best-effort: the namespaces we've already materialized a provider for. Authoritative
        // listing is the `spark.catalog.listDatabases` RPC, which queries the oxidant provider.
        self.schemas
            .lock()
            .expect("schemas poisoned")
            .keys()
            .cloned()
            .collect()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        // Always hand back a provider (without a sync existence check); a non-existent table
        // surfaces as `Ok(None)` from the async `table()` below — DataFusion's normal "table not
        // found" path.
        let mut schemas = self.schemas.lock().expect("schemas poisoned");
        let provider = schemas.entry(name.to_string()).or_insert_with(|| {
            Arc::new(OxidantSchemaProvider::new(
                self.catalog.clone(),
                vec![name.to_string()],
                self.ctx.clone(),
                self.require_lakehouse_snapshot_pins.clone(),
                self.catalog_version.clone(),
                self.pending_ctas.clone(),
            ))
        });
        Some(provider.clone() as Arc<dyn SchemaProvider>)
    }
}

/// DataFusion `SchemaProvider` for one namespace of a oxidant catalog.
struct OxidantSchemaProvider {
    catalog: Arc<dyn OxidantCatalog>,
    namespace: Vec<String>,
    ctx: Arc<SessionContext>,
    require_lakehouse_snapshot_pins: Arc<AtomicBool>,
    /// Shared catalog-version counter (see [`OxidantCatalogProvider::catalog_version`]).
    catalog_version: Arc<AtomicU64>,
    /// Storage attributes staged for a CTAS in flight, shared with the owning catalog provider.
    pending_ctas: PendingCtas,
    /// Resolved tables, cached so a table referenced repeatedly in a query is loaded once.
    ///
    /// Keyed by `(name, replicated)` where `replicated` is the shard/replicate decision the
    /// provider was built under ([`crate::shard::is_replicated_table`]): a provider *embeds*
    /// that decision (a replicated table lists all files; a sharded one only this worker's
    /// shard), and the driver's per-query auto-broadcast classification flips a table's role
    /// between queries via the stage ticket's task-local overlay. Keying by name alone served
    /// the stale variant — a table first resolved as replicated was later scanned in full on
    /// every worker where the plan assumed shards (rows × worker count), or first resolved as
    /// sharded and later served as only a shard where the plan assumed a full copy (rows
    /// dropped) — KAN-35. Both variants fit in the cache, so a role flip costs one re-list.
    ///
    /// Non-lakehouse entries are revalidated against the metastore once their TTL
    /// ([`catalog_cache_ttl`]) expires — a cached provider is NOT immortal: a Glue table whose
    /// schema/location changed out-of-band must be picked up without an engine restart.
    ///
    /// The third key component is the **authorizing principal** (empty when the catalog has no
    /// authorizer). A provider embeds its access decision the same way it embeds the shard
    /// decision, so two principals must never share one entry — that would serve one user's
    /// authorized column set and row filter to another.
    tables: Mutex<HashMap<TableCacheKey, CachedTable>>,
}

/// `(table name, replicated, principal)` — see [`OxidantSchemaProvider::tables`].
type TableCacheKey = (String, bool, String);

struct CachedTable {
    provider: Arc<dyn TableProvider>,
    snapshot_key: Option<String>,
    snapshot: Option<SnapshotIdentity>,
    /// When this entry was resolved (TTL bookkeeping; non-lakehouse entries only).
    resolved_at: std::time::Instant,
    /// [`metadata_fingerprint`] of the metadata the provider was built from. `None` for entries
    /// that never went through `load_table` (CTAS `register_table`) — a TTL revalidation then
    /// always re-resolves, which is the correct conservative direction.
    fingerprint: Option<String>,
    /// Whether the authorizer reported this table as Lake Formation-governed when the provider was
    /// built.
    ///
    /// Recorded because a cache *hit* returns without re-authorizing, and the driver has to stamp
    /// the enforcement requirement on its stage tickets from what it observed while planning. Miss
    /// it and the second query on a table — anything inside the catalog TTL — would ship tickets
    /// saying "not governed", and every worker would stop enforcing. Fail-open in the common case.
    lakeformation_enforced: bool,
}

impl CachedTable {
    /// Re-record that this table is Lake Formation-governed when it is served from cache.
    /// See [`CachedTable::lakeformation_enforced`].
    fn note_enforcement(&self, principal: &str) {
        if self.lakeformation_enforced {
            note_lakeformation_enforced(principal);
        }
    }
}

impl OxidantSchemaProvider {
    fn new(
        catalog: Arc<dyn OxidantCatalog>,
        namespace: Vec<String>,
        ctx: Arc<SessionContext>,
        require_lakehouse_snapshot_pins: Arc<AtomicBool>,
        catalog_version: Arc<AtomicU64>,
        pending_ctas: PendingCtas,
    ) -> Self {
        Self {
            catalog,
            namespace,
            ctx,
            require_lakehouse_snapshot_pins,
            catalog_version,
            pending_ctas,
            tables: Mutex::new(HashMap::new()),
        }
    }

    /// Drop all cached variants (replicated/sharded × principal) of `name` so the next `table()`
    /// re-resolves from the metastore. Returns whether anything was evicted.
    fn evict_table(&self, name: &str) -> bool {
        let mut tables = self.tables.lock().expect("tables poisoned");
        let before = tables.len();
        tables.retain(|(n, _, _), _| n != name);
        tables.len() != before
    }

    /// The principal this catalog's authorizer resolves decisions for, or `""` when the catalog
    /// has no authorizer configured (the default — no access control layer).
    fn principal(&self) -> String {
        self.catalog
            .authorizer()
            .map(|a| a.principal().to_string())
            .unwrap_or_default()
    }

    /// Resolve the catalog authorizer's access decision for `name`, before any provider is built.
    ///
    /// `None` when the catalog has no authorizer — the default, no access-control layer. Any
    /// failure to *resolve* a decision propagates: an authorizer that cannot answer must fail the
    /// query rather than let it read unfiltered data.
    ///
    /// Resolving before the provider exists is what lets vended credentials be installed for the
    /// table's storage prefix *first*, so the very first byte read already goes through Lake
    /// Formation's credentials rather than the engine's own.
    async fn resolve_access(&self, name: &str) -> DfResult<Option<oxidant_catalog::TableAccess>> {
        let Some(authorizer) = self.catalog.authorizer() else {
            return Ok(None);
        };
        let access = authorizer
            .authorize_scan(&self.namespace, name)
            .await
            .map_err(oxidant_to_df)?;
        if access.enforced {
            note_lakeformation_enforced(authorizer.principal());
        }
        Ok(Some(access))
    }

    /// Point this table's storage prefix at its Lake Formation-vended credentials.
    ///
    /// No-op unless the decision carried credentials (vending is opt-in) and the table lives on S3.
    /// Registers a [`RoutingObjectStore`] for the bucket the first time a governed table appears in
    /// it, so governed and ungoverned tables can share a bucket and two governed tables can hold
    /// different credentials — neither of which DataFusion's per-bucket store registry can express
    /// on its own.
    fn install_vended_credentials(
        &self,
        state: &SessionState,
        md: &TableMetadata,
        name: &str,
        access: &oxidant_catalog::TableAccess,
    ) -> DfResult<()> {
        use datafusion::datasource::listing::ListingTableUrl;

        let Some(creds) = access.credentials.as_ref() else {
            return Ok(());
        };
        let Some(authorizer) = self.catalog.authorizer() else {
            return Ok(());
        };
        let loc = crate::shard::ensure_collection_url(&md.location);
        let url = ListingTableUrl::parse(&loc).map_err(loc_err(md))?;
        if url.scheme() != "s3" {
            // Credential vending is an S3 mechanism; a governed table elsewhere still gets its
            // column/row policy applied, just not scoped credentials.
            return Ok(());
        }
        // Make sure the ambient store exists first — it becomes the routing store's fallback, and
        // is what every ungoverned path in this bucket keeps using.
        ensure_remote_store(state, &url, Some(&md.storage_options))?;
        let os_url = url.object_store();
        let base = state.runtime_env().object_store(&os_url)?;
        // Key by runtime env AS WELL as bucket. The map is process-global but DataFusion's
        // object-store registry is per-`RuntimeEnv`, and several engines share a process (every
        // in-process multi-worker harness, and the test suite). Keyed by bucket alone, the second
        // engine would be told the routing store already existed, skip registering it in its own
        // registry, and quietly read governed data through its plain store.
        let env_key = format!("{:p}|{}", Arc::as_ptr(state.runtime_env()), os_url.as_str());
        let (routing, created) = crate::lakeformation_store::routing_store_or_insert(
            &env_key,
            state.runtime_env(),
            &base,
        );
        if created {
            state
                .runtime_env()
                .register_object_store(os_url.as_ref(), routing.clone());
        }

        let bucket = os_url
            .as_str()
            .strip_prefix("s3://")
            .and_then(|r| r.split('/').next())
            .unwrap_or_default()
            .to_string();
        let region = md
            .storage_options
            .get("s3.region")
            .or_else(|| md.storage_options.get("fs.s3a.endpoint.region"))
            .cloned()
            .unwrap_or_else(|| {
                // The same ambient chain the ambient store uses, so a governed table cannot end up
                // in a different region than its ungoverned neighbours in the same bucket.
                oxidant_catalog::aws_region::ambient_region().unwrap_or_else(|| {
                    oxidant_catalog::aws_region::LEGACY_FALLBACK_REGION.to_string()
                })
            });
        let credential_provider = Arc::new(
            crate::lakeformation_store::LakeFormationCredentialProvider::new(
                authorizer,
                self.namespace.clone(),
                name.to_string(),
                creds,
            ),
        );
        // Apply the SAME storage-option normalization the ambient store gets. Without it the
        // credentialed store ignores a configured endpoint or addressing style and talks to real
        // AWS — leaving a governed table as the only broken thing in a bucket whose other tables
        // read fine, with an opaque error.
        let table_store = apply_s3_storage_options(
            object_store::aws::AmazonS3Builder::from_env()
                .with_bucket_name(&bucket)
                .with_region(region),
            &md.storage_options,
        )
        .with_credentials(credential_provider)
        .build()
        .map_err(|e| {
            DataFusionError::Execution(format!(
                "build lake formation credentialed store for `{name}`: {e}"
            ))
        })?;
        routing.add_route(url.prefix().clone(), Arc::new(table_store));
        Ok(())
    }

    /// Wrap `provider` so its scans honor `access`. Returns it untouched when the decision
    /// restricts nothing (an ungoverned table, or a principal granted everything).
    fn enforce(
        &self,
        state: &SessionState,
        name: &str,
        provider: Arc<dyn TableProvider>,
        access: Option<&oxidant_catalog::TableAccess>,
    ) -> DfResult<Arc<dyn TableProvider>> {
        let Some(access) = access else {
            return Ok(provider);
        };
        if crate::lakeformation_provider::LakeFormationTableProvider::is_noop(access) {
            return Ok(provider);
        }
        Ok(Arc::new(
            crate::lakeformation_provider::LakeFormationTableProvider::try_new(
                state, provider, access, name,
            )?,
        ))
    }
}

/// The non-lakehouse table-cache TTL: `OXIDANT_CATALOG_CACHE_TTL_MS` (default 60000 ms; `0`
/// revalidates against the metastore on every `table()` call; unparseable falls back to the
/// default). Read per call — like `OXIDANT_STAGE_PLAN_CACHE_ENTRIES` — so tests and operators
/// can toggle it without a restart.
fn catalog_cache_ttl() -> std::time::Duration {
    const DEFAULT_TTL_MS: u64 = 60_000;
    match std::env::var("OXIDANT_CATALOG_CACHE_TTL_MS") {
        Ok(raw) => raw
            .trim()
            .parse::<u64>()
            .map(std::time::Duration::from_millis)
            .unwrap_or(std::time::Duration::from_millis(DEFAULT_TTL_MS)),
        Err(_) => std::time::Duration::from_millis(DEFAULT_TTL_MS),
    }
}

/// Apply a table's `storage_options` to an S3 builder, mapping the Iceberg/Hadoop-AWS key spellings
/// onto `object_store`'s config keys.
///
/// Shared by the ambient store and the Lake Formation-credentialed per-table store so the two
/// cannot disagree about endpoint, region, or addressing style.
fn apply_s3_storage_options(
    mut builder: object_store::aws::AmazonS3Builder,
    options: &HashMap<String, String>,
) -> object_store::aws::AmazonS3Builder {
    for (key, value) in options {
        let normalized = normalize_s3_config_key(key);
        if let Ok(config_key) = normalized.parse::<object_store::aws::AmazonS3ConfigKey>() {
            builder = builder.with_config(config_key, value);
        }
    }
    builder
}

/// A cheap fingerprint of the metadata a provider was built from — location, format, declared
/// schema fields (name + type + nullability), and partition columns — so a TTL revalidation can tell
/// "metastore unchanged" apart from "re-typed / moved" without diffing providers.
///
/// `access` folds in the authorizer's decision when the catalog has one. Metadata alone is not
/// enough: revoking a column grant or tightening a row filter leaves the Glue table byte-identical,
/// so without this a revoked permission would keep being served from cache until the process
/// restarted. With it, the change is picked up on the next TTL revalidation.
fn metadata_fingerprint(
    md: &TableMetadata,
    access: Option<&oxidant_catalog::TableAccess>,
) -> String {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    md.location.hash(&mut h);
    format!("{:?}", md.format).hash(&mut h);
    match &md.schema {
        Some(schema) => {
            for field in schema.fields() {
                field.name().hash(&mut h);
                format!("{:?}", field.data_type()).hash(&mut h);
                field.is_nullable().hash(&mut h);
            }
        }
        None => "inferred".hash(&mut h),
    }
    md.partition_columns.hash(&mut h);
    if let Some(access) = access {
        access.enforced.hash(&mut h);
        access.authorized_columns.hash(&mut h);
        access.row_filter.hash(&mut h);
    }
    format!("{:016x}", h.finish())
}

impl fmt::Debug for OxidantSchemaProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OxidantSchemaProvider")
            .field("catalog", &self.catalog.name())
            .field("namespace", &self.namespace)
            .finish()
    }
}

#[async_trait]
impl SchemaProvider for OxidantSchemaProvider {
    fn table_names(&self) -> Vec<String> {
        // Best-effort: already-resolved tables. `spark.catalog.listTables` uses the oxidant provider.
        // The cache may hold both shard-context variants of a table; report each name once.
        self.tables
            .lock()
            .expect("tables poisoned")
            .keys()
            .map(|(name, _, _)| name.clone())
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect()
    }

    async fn table(&self, name: &str) -> DfResult<Option<Arc<dyn TableProvider>>> {
        // The shard/replicate decision is baked into a cached provider at resolution time;
        // resolve (or reuse) the variant matching this query's classification (KAN-35).
        // Checked BEFORE the cache, not on the resolution path: a table this process already
        // resolved for some earlier, ungoverned query is sitting in the cache unwrapped, and a
        // miss-only check would hand that cached provider straight to a query the driver marked
        // governed. This is the distributed fail-open hole — a worker started without the Lake
        // Formation config cannot detect it on its own, because with no authorizer it has no way
        // to know the table is governed, so the driver stamps the requirement on the ticket.
        if lakeformation_required() {
            match self.catalog.authorizer() {
                None => {
                    return Err(DataFusionError::Plan(format!(
                        "this query reads Lake Formation-governed data, but catalog `{}` in this \
                         process has no Lake Formation authorizer configured — refusing to read \
                         `{}.{name}` unfiltered. Pass the same \
                         `spark.sql.catalog.*.lakeformation*` config to every worker as to the \
                         driver.",
                        self.catalog.name(),
                        self.namespace.join("."),
                    )));
                }
                // Configured, but as somebody else. Driver and workers are given their catalog
                // config independently, so a worker set to a different `identity` /
                // `runtime_role_arn` would enforce its OWN policy on its shard — silently
                // returning more rows and columns than the user is entitled to, with no error
                // anywhere. Refuse instead of quietly widening the result.
                Some(authorizer) => {
                    if let Some(expected) = required_lakeformation_principal() {
                        if authorizer.principal() != expected {
                            return Err(DataFusionError::Plan(format!(
                                "Lake Formation principal mismatch reading `{}.{name}`: the \
                                 driver resolved this query's policy as `{expected}`, but catalog \
                                 `{}` in this process enforces as `{}`. Every worker must be \
                                 configured with the same `lakeformation.identity` and \
                                 `lakeformation.runtime_role_arn` as the driver.",
                                self.namespace.join("."),
                                self.catalog.name(),
                                authorizer.principal(),
                            )));
                        }
                    }
                }
            }
        }
        let replicated = crate::shard::is_replicated_table(name);
        let key = (name.to_string(), replicated, self.principal());
        // Set to `(fingerprint, provider)` when a non-lakehouse cache entry is past its TTL and
        // must be revalidated against the metastore. The revalidation happens outside the lock —
        // `load_table` is async and must not hold it.
        let mut revalidate = None;
        {
            let tables = self.tables.lock().expect("tables poisoned");
            if let Some(cached) = tables.get(&key) {
                if let (Some(snapshot_key), Some(snapshot)) =
                    (&cached.snapshot_key, &cached.snapshot)
                {
                    if self.require_lakehouse_snapshot_pins.load(Ordering::Relaxed)
                        && !lakehouse_snapshot_context_is_set()
                    {
                        return Err(DataFusionError::Plan(format!(
                            "distributed lakehouse table `{snapshot_key}` was resolved outside \
                             its pinned snapshot scope"
                        )));
                    }
                    match requested_lakehouse_snapshot(snapshot_key) {
                        Some(requested) if requested != *snapshot => {}
                        Some(_) => {
                            record_lakehouse_snapshot(snapshot_key, snapshot);
                            cached.note_enforcement(&self.principal());
                            return Ok(Some(cached.provider.clone()));
                        }
                        None if self.require_lakehouse_snapshot_pins.load(Ordering::Relaxed) => {
                            return Err(DataFusionError::Plan(format!(
                                "distributed stage omitted the snapshot pin for lakehouse table \
                                 `{snapshot_key}`"
                            )));
                        }
                        None => {
                            record_lakehouse_snapshot(snapshot_key, snapshot);
                            cached.note_enforcement(&self.principal());
                            return Ok(Some(cached.provider.clone()));
                        }
                    }
                } else {
                    // Non-lakehouse entry: serve the cached provider within the TTL; past it,
                    // revalidate below. A cached provider is NOT immortal — an out-of-band
                    // metastore change (re-typed Glue schema, new location) must be picked up
                    // without an engine restart.
                    let ttl = catalog_cache_ttl();
                    if !ttl.is_zero() && cached.resolved_at.elapsed() < ttl {
                        cached.note_enforcement(&self.principal());
                        return Ok(Some(cached.provider.clone()));
                    }
                    revalidate = Some((cached.fingerprint.clone(), cached.provider.clone()));
                }
            }
        }
        let self_principal = self.principal();
        if let Some((cached_fingerprint, cached_provider)) = revalidate {
            match self.catalog.load_table(&self.namespace, name).await {
                Ok(metadata) => {
                    // Re-resolve the access decision alongside the metadata: a grant revoked
                    // out-of-band changes the fingerprint even when the table itself did not.
                    let revalidated_access = self.resolve_access(name).await?;
                    let fingerprint = metadata_fingerprint(&metadata, revalidated_access.as_ref());
                    if Some(&fingerprint) == cached_fingerprint.as_ref() {
                        // Unchanged: keep the provider, just restart the TTL window.
                        if let Some(entry) =
                            self.tables.lock().expect("tables poisoned").get_mut(&key)
                        {
                            entry.resolved_at = std::time::Instant::now();
                        }
                        return Ok(Some(cached_provider));
                    }
                    // The metastore metadata moved (re-typed schema, new location, …): rebuild
                    // the provider and bump the shared catalog version so cached distributed
                    // stage plans (keyed on it) miss and rebuild too.
                    let state = self.ctx.state();
                    if let Some(access) = revalidated_access.as_ref() {
                        self.install_vended_credentials(&state, &metadata, name, access)?;
                    }
                    let resolved = metadata_to_provider(
                        &state,
                        &metadata,
                        name,
                        self.require_lakehouse_snapshot_pins.load(Ordering::Relaxed),
                    )
                    .await?;
                    let provider =
                        self.enforce(&state, name, resolved.provider, revalidated_access.as_ref())?;
                    self.tables.lock().expect("tables poisoned").insert(
                        key,
                        CachedTable {
                            provider: provider.clone(),
                            snapshot_key: resolved.snapshot_key,
                            snapshot: resolved.snapshot,
                            resolved_at: std::time::Instant::now(),
                            fingerprint: Some(fingerprint),
                            lakeformation_enforced: revalidated_access
                                .as_ref()
                                .is_some_and(|a| a.enforced),
                        },
                    );
                    self.catalog_version.fetch_add(1, Ordering::Relaxed);
                    return Ok(Some(provider));
                }
                // The table vanished from the metastore: drop the stale entry and take
                // DataFusion's standard not-found path.
                Err(Error::Plan(_)) => {
                    self.tables.lock().expect("tables poisoned").remove(&key);
                    self.catalog_version.fetch_add(1, Ordering::Relaxed);
                    return Ok(None);
                }
                // A revalidation failure (Glue throttling, transient network, …) must not fail
                // the query: serve the cached provider and try again after the next TTL window
                // (restart the window here — otherwise every `table()` call under sustained
                // throttling re-hits the metastore).
                // A revalidation failure must not clear the enforcement requirement: we are serving
                // a cached provider built under a policy we could not re-check.
                Err(_) => {
                    if let Some(entry) = self.tables.lock().expect("tables poisoned").get_mut(&key)
                    {
                        entry.resolved_at = std::time::Instant::now();
                        entry.note_enforcement(&self_principal);
                    }
                    return Ok(Some(cached_provider));
                }
            }
        }
        let metadata = match self.catalog.load_table(&self.namespace, name).await {
            Ok(md) => md,
            // A "no such table" (analysis) error → DataFusion's standard not-found path.
            Err(Error::Plan(_)) => return Ok(None),
            // A storage / connection / unsupported failure is a real error — surface it.
            Err(e) => return Err(oxidant_to_df(e)),
        };
        let state = self.ctx.state();
        // Authorize BEFORE the provider is built: the decision may carry Lake Formation-vended
        // credentials, which have to be installed for this table's prefix before anything reads a
        // byte. Caching the enforcing provider (rather than wrapping later) also means no later
        // reader can obtain the unrestricted one.
        let access = self.resolve_access(name).await?;
        if let Some(access) = access.as_ref() {
            self.install_vended_credentials(&state, &metadata, name, access)?;
        }
        let resolved = metadata_to_provider(
            &state,
            &metadata,
            name,
            self.require_lakehouse_snapshot_pins.load(Ordering::Relaxed),
        )
        .await?;
        let provider = self.enforce(&state, name, resolved.provider, access.as_ref())?;
        let fingerprint = metadata_fingerprint(&metadata, access.as_ref());
        self.tables.lock().expect("tables poisoned").insert(
            key,
            CachedTable {
                provider: provider.clone(),
                snapshot_key: resolved.snapshot_key,
                snapshot: resolved.snapshot,
                resolved_at: std::time::Instant::now(),
                fingerprint: Some(fingerprint),
                lakeformation_enforced: access.as_ref().is_some_and(|a| a.enforced),
            },
        );
        Ok(Some(provider))
    }

    fn table_exist(&self, name: &str) -> bool {
        self.tables
            .lock()
            .expect("tables poisoned")
            .keys()
            .any(|(n, _, _)| n == name)
    }

    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> DfResult<Option<Arc<dyn TableProvider>>> {
        let catalog = self.catalog.clone();
        let namespace = self.namespace.clone();
        let ctx = self.ctx.clone();
        let name_for_worker = name.clone();
        // Read here, on the caller's thread, rather than inside the write worker: the staged entry
        // belongs to this provider and this statement, and taking it now means a write that fails
        // cannot leave it behind for the next CTAS of the same name.
        let attributes = self
            .pending_ctas
            .lock()
            .expect("pending CTAS attributes poisoned")
            .remove(&ctas_attributes_key(&self.namespace, &name))
            .unwrap_or_default();

        // `register_table` is a sync fn (DataFusion's trait), but the write path is all async
        // (Glue CLI / Hive Thrift / object-store puts). `Handle::current().block_on(...)` would
        // panic under a single-thread runtime (e.g. plain `#[tokio::test]`, used throughout this
        // file's own tests) — so this dispatches to a single persistent background worker thread
        // (see `ctas_writer`) instead of spawning a fresh OS thread + runtime per call, which is
        // safe under any caller runtime flavor but also bounds CTAS write concurrency to one at a
        // time process-wide (a deliberately rare, non-hot-path DDL operation).
        let provider = ctas_writer().run(move |rt| {
            rt.block_on(register_table_async(
                catalog,
                ctx,
                namespace,
                name_for_worker,
                table,
                attributes,
            ))
        })??;

        self.tables.lock().expect("tables poisoned").insert(
            (
                name.clone(),
                crate::shard::is_replicated_table(&name),
                self.principal(),
            ),
            CachedTable {
                provider: provider.clone(),
                snapshot_key: None,
                snapshot: None,
                resolved_at: std::time::Instant::now(),
                // CTAS never went through `load_table`, so there is no metadata fingerprint to
                // compare — a TTL revalidation conservatively re-resolves this entry.
                fingerprint: None,
                // A freshly written table has not been authorized; the next resolution will.
                lakeformation_enforced: false,
            },
        );
        Ok(Some(provider))
    }
}

/// The storage attributes of a `CREATE TABLE … USING <fmt> [LOCATION …] [PARTITIONED BY (…)]
/// AS SELECT …` that DataFusion's own CTAS planning cannot carry.
///
/// DataFusion plans `CREATE TABLE … AS SELECT` into a `MemTable` and then calls
/// `SchemaProvider::register_table` with nothing but a name and that table — there is no seam for
/// a storage format, an explicit `LOCATION`, or partition columns, and sqlparser does not accept
/// Spark's `USING` clause in a `CREATE TABLE` at all. `Engine::sql` therefore parses that tail off
/// itself ([`crate::spark_create_table::lower_external_ctas`]), stages the attributes on the
/// target catalog's provider, and runs the stripped statement; [`register_table_async`] picks them
/// up on its way to `CatalogProvider::create_table`. Absent an entry the behavior is exactly what
/// it was before this existed — Parquet, at the catalog's default location, unpartitioned.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct CtasAttributes {
    pub format: Option<TableFormat>,
    pub location: Option<String>,
    pub partition_columns: Vec<String>,
}

/// Attributes staged for CTASes in flight, keyed by `namespace.table` (lowercased).
///
/// Held per catalog provider — which is per session — rather than process-wide, so two sessions,
/// or two catalogs, running a CTAS for the same `namespace.table` cannot take each other's
/// attributes. Within one session the same name is the same table, and the entry is removed as
/// soon as it is read.
type PendingCtas = Arc<Mutex<HashMap<String, CtasAttributes>>>;

fn ctas_attributes_key(namespace: &[String], table: &str) -> String {
    let mut key = namespace.join(".");
    if !key.is_empty() {
        key.push('.');
    }
    key.push_str(table);
    key.to_ascii_lowercase()
}

/// A single persistent background thread (created lazily, once, for the process lifetime) with
/// its own `current_thread` Tokio runtime, used to run CTAS write futures from `register_table`'s
/// sync entry point without spawning a new OS thread + runtime on every call.
type CtasJob = Box<dyn FnOnce(&tokio::runtime::Runtime) + Send>;

struct CtasWriter {
    jobs: std::sync::mpsc::Sender<CtasJob>,
}

impl CtasWriter {
    fn new() -> Self {
        let (tx, rx) = std::sync::mpsc::channel::<CtasJob>();
        std::thread::Builder::new()
            .name("oxidant-ctas-writer".to_string())
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("build CTAS writer runtime");
                for job in rx {
                    // A panicking job must not take this thread down with it — every other
                    // catalog/session shares this single process-wide writer, so one bad CTAS
                    // (e.g. an internal panic in a dependency) would otherwise permanently break
                    // CTAS writes for everyone until the process restarts. `run`'s caller already
                    // gets a clean "CTAS writer thread died" error for THIS call (the boxed job's
                    // `result_tx` is dropped mid-unwind, closing its channel), so the only extra
                    // work needed here is keeping the loop itself alive for the NEXT job.
                    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| job(&rt)));
                }
            })
            .expect("spawn CTAS writer thread");
        Self { jobs: tx }
    }

    /// Run `f` (which calls `rt.block_on(...)` itself) on the writer thread and block the caller
    /// until it completes, returning its result.
    fn run<T: Send + 'static>(
        &self,
        f: impl FnOnce(&tokio::runtime::Runtime) -> T + Send + 'static,
    ) -> DfResult<T> {
        let (result_tx, result_rx) = std::sync::mpsc::channel();
        self.jobs
            .send(Box::new(move |rt| {
                let _ = result_tx.send(f(rt));
            }))
            .map_err(|_| {
                DataFusionError::Execution("CTAS writer thread unavailable".to_string())
            })?;
        result_rx
            .recv()
            .map_err(|_| DataFusionError::Execution("CTAS writer thread died".to_string()))
    }
}

fn ctas_writer() -> &'static CtasWriter {
    static WRITER: std::sync::OnceLock<CtasWriter> = std::sync::OnceLock::new();
    WRITER.get_or_init(CtasWriter::new)
}

/// The async body of `OxidantSchemaProvider::register_table`: extract the CTAS result's schema and
/// data from `table` (always a `MemTable` — what DataFusion's native `CREATE TABLE ... AS SELECT`
/// produces), ask the catalog to declare the table (`CatalogProvider::create_table`), physically
/// write the data to the resolved location, then build a REAL `TableProvider` over those durable
/// files (not the transient `MemTable`) so a subsequent `SELECT` — same session or a new one —
/// reads genuine external-catalog data.
async fn register_table_async(
    catalog: Arc<dyn OxidantCatalog>,
    ctx: Arc<SessionContext>,
    namespace: Vec<String>,
    name: String,
    table: Arc<dyn TableProvider>,
    // `USING <fmt>` / `LOCATION` / `PARTITIONED BY` from the original Spark DDL, which DataFusion's
    // CTAS planning cannot carry (see `CtasAttributes`). Default → the historical behavior.
    attributes: CtasAttributes,
) -> DfResult<Arc<dyn TableProvider>> {
    let (schema, batches) = extract_mem_table_data(&table).await?;
    let format = attributes.format.unwrap_or(TableFormat::Parquet);

    // Whether the table was already in the catalog, so the cleanup below only removes what this
    // statement created. `create_table` overwrites an existing entry rather than refusing one, so
    // an unconditional drop on failure would delete a table that was here before.
    let existed = catalog
        .table_exists(&namespace, &name)
        .await
        .unwrap_or(false);

    let metadata = catalog
        .create_table(
            &namespace,
            &name,
            schema.clone(),
            format,
            attributes.location,
            &attributes.partition_columns,
        )
        .await
        .map_err(oxidant_to_df)?;

    let state = ctx.state();
    if let Err(error) = write_batches_to_location(
        &state,
        &metadata.location,
        metadata.format,
        &schema,
        batches,
        &metadata.storage_options,
        &metadata.partition_columns,
    )
    .await
    {
        // The catalog entry is registered but its data is not there. Leaving it would make the
        // statement un-retryable in a way the user cannot clear: `CREATE TABLE` then reports the
        // table already exists, and no SQL path reaches `DROP TABLE` to remove it — so the only
        // recovery would be editing the catalog manifest by hand.
        //
        // Best-effort, and the write's error is what propagates: a failure to clean up must not
        // replace the message explaining why the write failed.
        if !existed {
            if let Err(cleanup) = catalog.drop_table(&namespace, &name, true).await {
                eprintln!(
                    "[oxidant] `{}` was registered but its data could not be written, and the \
                     registration could not be undone ({cleanup}); drop it before retrying",
                    metadata.name
                );
            }
        }
        return Err(error);
    }

    Ok(metadata_to_provider(&state, &metadata, &name, false)
        .await?
        .provider)
}

/// Extract `(schema, batches)` from a `TableProvider` that's always a `MemTable` on this path
/// (DataFusion's `CreateMemoryTable` DDL handling always wraps the CTAS `SELECT`'s output that
/// way before calling `register_table`). Falls back to a full `scan` + `collect` if that ever
/// changes, so this doesn't silently break on a DataFusion upgrade.
async fn extract_mem_table_data(
    table: &Arc<dyn TableProvider>,
) -> DfResult<(SchemaRef, Vec<RecordBatch>)> {
    // `TableProvider: Any` (a supertrait), so a `&dyn TableProvider` upcasts to `&dyn Any` for
    // downcasting — this DataFusion version doesn't expose a dedicated `as_any()` method.
    let any: &dyn std::any::Any = table.as_ref();
    if let Some(mem) = any.downcast_ref::<MemTable>() {
        let schema = mem.schema();
        let mut batches = Vec::new();
        for partition in &mem.batches {
            batches.extend(partition.read().await.iter().cloned());
        }
        return Ok((schema, batches));
    }
    // Defensive fallback: scan the provider directly.
    let ctx = SessionContext::new();
    let state = ctx.state();
    let plan = table.scan(&state, None, &[], None).await?;
    let batches = datafusion::physical_plan::collect(plan, ctx.task_ctx()).await?;
    Ok((table.schema(), batches))
}

/// Turn resolved table metadata into a readable DataFusion `TableProvider`.
pub(crate) struct ProviderResolution {
    pub(crate) provider: Arc<dyn TableProvider>,
    snapshot_key: Option<String>,
    snapshot: Option<SnapshotIdentity>,
}

pub(crate) async fn metadata_to_provider(
    state: &SessionState,
    md: &TableMetadata,
    table_name: &str,
    require_lakehouse_snapshot_pin: bool,
) -> DfResult<ProviderResolution> {
    use datafusion::datasource::file_format::csv::CsvFormat;
    use datafusion::datasource::file_format::json::JsonFormat;
    use datafusion::datasource::listing::{ListingOptions, ListingTableUrl};

    let provider = match md.format {
        TableFormat::Parquet => {
            let loc = crate::shard::ensure_collection_url(&md.location);
            let url = ListingTableUrl::parse(&loc).map_err(loc_err(md))?;
            ensure_remote_store(state, &url, Some(&md.storage_options))?;
            parquet_metadata_provider(state, md, table_name, vec![url]).await?
        }
        TableFormat::Csv => {
            let loc = crate::shard::ensure_collection_url(&md.location);
            let url = ListingTableUrl::parse(&loc).map_err(loc_err(md))?;
            ensure_remote_store(state, &url, Some(&md.storage_options))?;
            let opts =
                ListingOptions::new(Arc::new(CsvFormat::default())).with_file_extension(".csv");
            let (opts, file_schema) = apply_partition_columns(opts, md);
            sharded_listing_table(
                state,
                vec![url],
                opts,
                file_schema,
                table_name,
                &md.name,
                ".csv",
            )
            .await?
        }
        TableFormat::Json => {
            let loc = crate::shard::ensure_collection_url(&md.location);
            let url = ListingTableUrl::parse(&loc).map_err(loc_err(md))?;
            ensure_remote_store(state, &url, Some(&md.storage_options))?;
            let opts =
                ListingOptions::new(Arc::new(JsonFormat::default())).with_file_extension(".json");
            let (opts, file_schema) = apply_partition_columns(opts, md);
            sharded_listing_table(
                state,
                vec![url],
                opts,
                file_schema,
                table_name,
                &md.name,
                ".json",
            )
            .await?
        }
        // Lakehouse formats are handled below because they also return a pinned snapshot identity.
        TableFormat::Delta => {
            return resolve_lakehouse_provider(
                state,
                md,
                table_name,
                require_lakehouse_snapshot_pin,
            )
            .await;
        }
        TableFormat::Iceberg => {
            return resolve_lakehouse_provider(
                state,
                md,
                table_name,
                require_lakehouse_snapshot_pin,
            )
            .await;
        }
    };
    Ok(ProviderResolution {
        provider,
        snapshot_key: None,
        snapshot: None,
    })
}

/// Row-count table statistic from catalog properties, when the metastore carries one:
/// Hive/Spark `ANALYZE TABLE` writes `numRows`, and Spark's own statistics use
/// `spark.sql.statistics.numRows`. `None` when absent or unparseable — callers treat that as
/// "unknown" and keep byte-only replicate/shard classification for the table. Reading it is
/// free: the properties ride along on the `load_table` the sizing walk already performs, so
/// no extra I/O lands on the per-query classification path.
pub fn row_count_from_properties(properties: &HashMap<String, String>) -> Option<u64> {
    ["numRows", "spark.sql.statistics.numRows"]
        .iter()
        .find_map(|key| properties.get(*key).and_then(|v| v.trim().parse().ok()))
}

/// Byte size + catalog row-count statistic for a catalog table. The bytes come from the same
/// listing walk as [`estimate_bytes_for_metadata`]; the row count is read from the metadata's
/// properties ([`row_count_from_properties`]) — `None` for formats/metastores without one.
pub async fn estimate_stats_for_metadata(
    state: &SessionState,
    md: &TableMetadata,
) -> (Option<u64>, Option<u64>) {
    (
        estimate_bytes_for_metadata(state, md).await,
        row_count_from_properties(&md.properties),
    )
}

/// Sum on-disk / object-store bytes for a catalog table (no shard filter). Returns `None` when
/// the format cannot be sized (or listing fails).
pub async fn estimate_bytes_for_metadata(state: &SessionState, md: &TableMetadata) -> Option<u64> {
    use datafusion::datasource::listing::ListingTableUrl;

    match md.format {
        TableFormat::Parquet | TableFormat::Csv | TableFormat::Json => {
            let loc = crate::shard::ensure_collection_url(&md.location);
            let url = ListingTableUrl::parse(&loc).ok()?;
            ensure_remote_store(state, &url, Some(&md.storage_options)).ok()?;
            let ext = match md.format {
                TableFormat::Parquet => ".parquet",
                TableFormat::Csv => ".csv",
                TableFormat::Json => ".json",
                _ => unreachable!(),
            };
            crate::shard::sum_listing_bytes(state, vec![url], ext)
                .await
                .ok()
        }
        TableFormat::Delta | TableFormat::Iceberg => {
            let root =
                ListingTableUrl::parse(crate::shard::ensure_collection_url(&md.location)).ok()?;
            ensure_remote_store(state, &root, Some(&md.storage_options)).ok()?;
            let store = state.runtime_env().object_store(&root).ok()?;
            let metadata_location = md.properties.get("metadata_location").map(String::as_str);
            let resolved = oxidant_datasource::active_files_for_scan(
                store,
                &md.location,
                match md.format {
                    TableFormat::Delta => "delta",
                    TableFormat::Iceberg => "iceberg",
                    _ => unreachable!(),
                },
                metadata_location,
                None,
                &oxidant_datasource::ScanRequest::default(),
            )
            .await
            .ok()?;
            Some(resolved.files.iter().map(|f| f.size).sum())
        }
    }
}

/// List+shard files then build a [`ListingTable`], or an empty MemTable when this shard is vacant.
/// Replicated tables (full file set on every worker) are served through the process-global
/// [`crate::dim_cache`]: the listing's object metadata fingerprints the data version.
async fn sharded_listing_table(
    state: &SessionState,
    urls: Vec<datafusion::datasource::listing::ListingTableUrl>,
    opts: datafusion::datasource::listing::ListingOptions,
    schema: Option<datafusion::arrow::datatypes::SchemaRef>,
    table_name: &str,
    qualified_name: &str,
    file_extension: &str,
) -> DfResult<Arc<dyn TableProvider>> {
    let sharded =
        crate::shard::list_visible_file_shard(state, urls, file_extension, Some(table_name))
            .await
            .map_err(oxidant_to_df)?;
    if sharded.is_empty() {
        let schema = schema.ok_or_else(|| {
            DataFusionError::Plan(format!(
                "sharded table `{table_name}` has no files on this worker and no declared schema"
            ))
        })?;
        return crate::shard::empty_table(schema).map_err(oxidant_to_df);
    }
    let dim_cache_fingerprint = crate::shard::is_replicated_table(table_name).then(|| {
        (
            crate::dim_cache::fingerprint_object_metas(&sharded),
            sharded.iter().map(|(_, meta)| meta.size).sum::<u64>(),
        )
    });
    let urls = sharded.into_iter().map(|(url, _)| url).collect();
    let provider = crate::build_listing_table(state, urls, opts, schema)
        .await
        .map_err(oxidant_to_df)?;
    match dim_cache_fingerprint {
        Some((fingerprint, source_bytes)) => {
            crate::dim_cache::memoize_provider(
                state,
                qualified_name,
                fingerprint,
                source_bytes,
                provider,
            )
            .await
        }
        None => Ok(provider),
    }
}

/// Configure Hive-style partition columns on a listing table. Glue (and other Hive metastores)
/// append partition columns to the declared schema, but their values live in the object *path*
/// (e.g. `.../year=2015/month=01/part.parquet`), not inside the data files. So we (1) declare them
/// as table partition columns on the `ListingOptions` — DataFusion derives their values from the
/// path — and (2) hand `build_listing_table` the *file* schema with those columns removed, so the
/// reader doesn't look for them in the files. Without a declared schema (Parquet inference) or with
/// no partition columns, this is a no-op passing the metadata schema through unchanged.
fn apply_partition_columns(
    opts: datafusion::datasource::listing::ListingOptions,
    md: &TableMetadata,
) -> (
    datafusion::datasource::listing::ListingOptions,
    Option<SchemaRef>,
) {
    match &md.schema {
        Some(schema) if !md.partition_columns.is_empty() => {
            let (file_schema, part_cols) = split_partition_schema(schema, &md.partition_columns);
            (opts.with_table_partition_cols(part_cols), Some(file_schema))
        }
        _ => (opts, md.schema.clone()),
    }
}

/// Split a Hive-partitioned table's declared schema into `(file_schema, partition_cols)`: the file
/// schema is every field that is *not* a partition column, and `partition_cols` is the
/// `(name, type)` pairs for the partition columns, emitted in the declared partition order. Types
/// come from the declared schema (Glue records them on `PartitionKeys`).
fn split_partition_schema(
    schema: &SchemaRef,
    partition_columns: &[String],
) -> (
    SchemaRef,
    Vec<(String, datafusion::arrow::datatypes::DataType)>,
) {
    use datafusion::arrow::datatypes::Schema;
    let part_set: std::collections::HashSet<&str> =
        partition_columns.iter().map(String::as_str).collect();
    let mut file_fields = Vec::new();
    let mut part_types = HashMap::new();
    for f in schema.fields() {
        if part_set.contains(f.name().as_str()) {
            part_types.insert(f.name().clone(), f.data_type().clone());
        } else {
            file_fields.push(f.clone());
        }
    }
    let part_cols = partition_columns
        .iter()
        .filter_map(|n| part_types.get(n).map(|dt| (n.clone(), dt.clone())))
        .collect();
    (Arc::new(Schema::new(file_fields)), part_cols)
}

/// Tracks which assumed-role identity (if any) each S3 bucket was registered with, for the
/// lifetime of this process. DataFusion's object-store registry (`RuntimeEnv::register_object_store`)
/// is keyed purely by `scheme://authority` (i.e. just the bucket) — it has no concept of two
/// different credential identities coexisting for the same bucket within one session. If table A
/// and table B live in the same bucket but declare different `fs.s3a.assumed.role.arn` values (or
/// one declares one and the other doesn't), whichever is resolved first silently decides the
/// identity for BOTH for the rest of the session unless something checks for the mismatch — this
/// map is that check (see `ensure_remote_store`). One oxidant engine process backs one cluster
/// (`oxidant-cl-<id>`), so process-wide scope here matches the session it's actually protecting.
static REGISTERED_BUCKET_ROLES: std::sync::Mutex<Option<HashMap<String, Option<String>>>> =
    std::sync::Mutex::new(None);

/// Ensure an object store is registered on the session's runtime for a remote table location so
/// DataFusion can read it. Currently handles `s3://` — credentials come from the environment or the
/// EC2 instance role (IMDS) via object_store's default provider; no static keys, UNLESS
/// `storage_options` names `fs.s3a.assumed.role.arn` (Hadoop-AWS's assume-role config, resolved to
/// a temporary session via `crate::assume_role_credentials::AssumeRoleCredentialProvider` — see
/// its module docs). Registering on the shared runtime is idempotent and persists for the session,
/// so query-time resolution finds it. `file://` and bare paths need nothing and are skipped.
///
/// Errors (rather than silently proceeding) if `bucket` was already registered under a DIFFERENT
/// assumed-role identity than this call requests — seeing `Ok(())` from this function is a
/// guarantee that the session's registered store for this bucket matches `storage_options`, not
/// just "some store exists for this bucket." See `REGISTERED_BUCKET_ROLES`'s doc comment for why
/// that guarantee needs an explicit check instead of being automatic.
/// Map a table's storage-option key to the `object_store` config key it means.
///
/// The accepted spellings are the real Iceberg (`s3.*`) and Hadoop-AWS (`fs.s3a.*`) names, not
/// Oxidant-invented equivalents, so a config ported from Spark or Trino works unchanged.
///
/// Iceberg spells its properties with hyphens and `object_store` parses only underscores, so the
/// fallback converts them. Without that, any `s3.*` key outside the explicit list above parses as
/// nothing and is **silently dropped** — the setting looks applied and simply is not.
fn normalize_s3_config_key(key: &str) -> std::borrow::Cow<'_, str> {
    use std::borrow::Cow;
    match key {
        "s3.access-key-id" | "fs.s3a.access.key" => Cow::Borrowed("access_key_id"),
        "s3.secret-access-key" | "fs.s3a.secret.key" => Cow::Borrowed("secret_access_key"),
        "s3.session-token" | "fs.s3a.session.token" => Cow::Borrowed("session_token"),
        "s3.endpoint" | "fs.s3a.endpoint" => Cow::Borrowed("endpoint"),
        "s3.region" | "fs.s3a.endpoint.region" => Cow::Borrowed("region"),
        "s3.allow-http" => Cow::Borrowed("allow_http"),
        "s3.virtual-hosted-style-request" => Cow::Borrowed("virtual_hosted_style_request"),
        other => match other.strip_prefix("s3.") {
            Some(rest) if rest.contains('-') => Cow::Owned(rest.replace('-', "_")),
            Some(rest) => Cow::Borrowed(rest),
            None => Cow::Borrowed(other),
        },
    }
}

/// Does this (already normalized) config key state *who the table is* to S3?
///
/// Such a key has to outrank the ambient default credential chain: a table carrying its own keys
/// means them, one naming a web-identity token or container endpoint means that identity, and
/// `skip_signature` means the bucket is public and must be read unsigned — which attaching any
/// credential provider at all would break.
///
/// `object_store` accepts every one of these both bare and `aws_`-prefixed, so match on the bare
/// form after stripping the prefix. Missing an alias here is not a cosmetic slip: it would let the
/// ambient chain silently override the identity the table explicitly asked for.
fn pins_s3_identity(normalized_key: &str) -> bool {
    matches!(
        normalized_key
            .strip_prefix("aws_")
            .unwrap_or(normalized_key),
        "access_key_id"
            | "secret_access_key"
            | "session_token"
            | "token"
            | "skip_signature"
            | "web_identity_token_file"
            | "role_arn"
            | "role_session_name"
            | "container_credentials_relative_uri"
            | "container_credentials_full_uri"
            | "container_authorization_token_file"
    )
}

/// Should this store resolve credentials through the AWS default chain
/// ([`crate::default_credentials::DefaultChainCredentialProvider`])?
///
/// Only when the table has not said who it is. Attaching the chain on top of an explicit identity
/// would override it — silently, and with the *ambient* identity, which is the worst way to get
/// this wrong. `fs.s3a.assumed.role.arn` is handled before this is consulted and takes its own
/// branch.
fn should_use_default_chain(storage_options: Option<&HashMap<String, String>>) -> bool {
    match storage_options {
        None => true,
        Some(options) => !options
            .keys()
            .any(|key| pins_s3_identity(&normalize_s3_config_key(key))),
    }
}

pub(crate) fn ensure_remote_store(
    state: &SessionState,
    url: &datafusion::datasource::listing::ListingTableUrl,
    storage_options: Option<&HashMap<String, String>>,
) -> DfResult<()> {
    if url.scheme() != "s3" {
        return Ok(());
    }
    let os_url = url.object_store(); // canonical `s3://bucket` key
                                     // `os_url` is the canonical `s3://bucket/` — pull the bucket from the authority.
    let bucket = os_url
        .as_str()
        .strip_prefix("s3://")
        .and_then(|r| r.split('/').next())
        .unwrap_or("")
        .to_string();
    if bucket.is_empty() {
        return Ok(());
    }
    let requested_role = storage_options
        .and_then(|opts| opts.get(crate::assume_role_credentials::ASSUMED_ROLE_ARN_KEY))
        .cloned();

    if state.runtime_env().object_store(&os_url).is_ok() {
        // Already registered for this bucket — confirm it was registered with the SAME identity
        // this call is asking for. A mismatch means two tables in this bucket disagree on which
        // role to assume, which DataFusion's registry has no way to honor simultaneously — that's
        // a real misconfiguration to surface, not something to paper over by silently keeping
        // whichever table happened to resolve first.
        let mut registry = REGISTERED_BUCKET_ROLES
            .lock()
            .expect("bucket-role registry poisoned");
        let map = registry.get_or_insert_with(HashMap::new);
        return match map.get(&bucket) {
            Some(registered) if *registered == requested_role => Ok(()),
            // Registered before this tracking map existed in this process's lifetime (shouldn't
            // happen in practice — both paths go through this same function — but fails open
            // rather than blocking reads that were working fine before this check existed).
            None => Ok(()),
            Some(registered) => Err(DataFusionError::Plan(format!(
                "bucket `{bucket}` is already registered in this session using a different S3 \
                 identity (assumed role {registered:?}) than this table requests \
                 ({requested_role:?}) — DataFusion can only have one active identity per bucket \
                 per session; two tables in the same bucket must agree on `fs.s3a.assumed.role.arn`"
            ))),
        };
    }

    // `AWS_REGION`, then `AWS_DEFAULT_REGION`, then the shared profile, then IMDS — the order the
    // AWS CLI uses. This used to read `AWS_REGION` alone and hardcode `us-west-2` otherwise, which
    // also meant it *overwrote* whatever `from_env` had resolved above. A table's own `s3.region`
    // still wins: it is applied to the builder after this.
    let region = oxidant_catalog::aws_region::ambient_region()
        .unwrap_or_else(|| oxidant_catalog::aws_region::LEGACY_FALLBACK_REGION.to_string());
    let mut builder = object_store::aws::AmazonS3Builder::from_env()
        .with_bucket_name(&bucket)
        .with_region(region.clone());
    if let Some(options) = storage_options {
        builder = apply_s3_storage_options(builder, options);
    }
    if let Some(role_arn) = &requested_role {
        let session_name = storage_options
            .and_then(|opts| {
                opts.get(crate::assume_role_credentials::ASSUMED_ROLE_SESSION_NAME_KEY)
            })
            .cloned();
        let provider = crate::assume_role_credentials::AssumeRoleCredentialProvider::new(
            role_arn.clone(),
            session_name,
            region,
        );
        builder = builder.with_credentials(std::sync::Arc::new(provider));
    } else if should_use_default_chain(storage_options) {
        // Nothing pinned this table's identity, so resolve it the way the AWS CLI does — through
        // the full default chain, which is a superset of what `from_env` alone covers and adds the
        // shared profile (`~/.aws/credentials`, SSO, `credential_process`). Env vars, IRSA, ECS,
        // and IMDS keep their existing precedence because the chain checks them in that same
        // order; the only behaviour that changes is that a profile is now found instead of the
        // request falling through to the instance-metadata endpoint and failing there.
        let provider = crate::default_credentials::DefaultChainCredentialProvider::new(region);
        builder = builder.with_credentials(std::sync::Arc::new(provider));
    }
    match builder.build() {
        Ok(store) => {
            // KAN-153: concurrent ranged GETs + per-task s3_wait/decode counters, then
            // KAN-2 whole-object disk cache (oversized facts still bypass to ranged GETs).
            let store = crate::s3_io::wrap_remote_store(Arc::new(store));
            state
                .runtime_env()
                .register_object_store(os_url.as_ref(), store);
            REGISTERED_BUCKET_ROLES
                .lock()
                .expect("bucket-role registry poisoned")
                .get_or_insert_with(HashMap::new)
                .insert(bucket, requested_role);
            Ok(())
        }
        Err(e) => {
            eprintln!("warn: could not register S3 object store for `{bucket}`: {e}");
            Ok(())
        }
    }
}

/// Write `batches` to the table at `location`.
///
/// Parquet/CSV/JSON serialize in memory and `put` a single file; Delta goes through
/// [`write_delta_batches`], which writes data files **and** commits them to the transaction log so
/// the result is a real Delta table rather than a directory of Parquet. Everything goes through the
/// session's `object_store` for `location`'s scheme, so `s3://` (registered via
/// [`ensure_remote_store`]) works exactly like `file://`/bare local paths (DataFusion's default
/// registry resolves those to `LocalFileSystem` with no explicit registration) — unlike the
/// local-only `ArrowWriter`-to-`std::fs::File` writer used by the (unrelated) local-warehouse
/// `CREATE TABLE ... USING <fmt>` path.
async fn write_batches_to_location(
    state: &SessionState,
    location: &str,
    format: TableFormat,
    schema: &SchemaRef,
    batches: Vec<RecordBatch>,
    storage_options: &HashMap<String, String>,
    partition_columns: &[String],
) -> DfResult<()> {
    use datafusion::datasource::listing::ListingTableUrl;
    use object_store::ObjectStoreExt;

    let url = ListingTableUrl::parse(location)
        .map_err(|e| DataFusionError::Plan(format!("bad table location `{location}`: {e}")))?;
    ensure_remote_store(state, &url, Some(storage_options))?;
    let store = state.runtime_env().object_store(&url)?;

    if format == TableFormat::Delta {
        write_delta_batches(
            store,
            url.prefix().clone(),
            location,
            schema,
            &batches,
            partition_columns,
            WriteMode::Append,
        )
        .await?;
        return Ok(());
    }

    // Only Delta partitions on write here. For the flat formats the catalog would record
    // partition columns the data layout does not have — a table unreadable through its own
    // metadata, because the reader expects those columns in the directory path and they are
    // instead sitting inside a single file at the root.
    if !partition_columns.is_empty() {
        return Err(DataFusionError::NotImplemented(format!(
            "`PARTITIONED BY` is not supported for a {format:?} table: this writer emits one \
             flat file, so the partition columns ({}) would never reach the directory path the \
             reader looks in. Use `USING delta`, which partitions on write.",
            partition_columns.join(", ")
        )));
    }

    let ext = match format {
        TableFormat::Parquet => "parquet",
        TableFormat::Csv => "csv",
        TableFormat::Json => "json",
        TableFormat::Delta => unreachable!("handled above"),
        TableFormat::Iceberg => return Err(iceberg_not_a_write_target()),
    };
    let bytes = encode_batches(format, schema, &batches)?;
    let path = url.prefix().clone().join(format!("part-00000.{ext}"));
    store
        .put(&path, bytes.into())
        .await
        .map_err(|e| DataFusionError::Execution(format!("write `{location}`: {e}")))?;
    Ok(())
}

/// Whether a write adds to the table or replaces its whole contents.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WriteMode {
    /// `CREATE TABLE ... AS SELECT` and `INSERT INTO`.
    Append,
    /// `INSERT OVERWRITE`: the prior files are retired in the same commit that adds the new ones,
    /// so a concurrent reader sees the whole old table or the whole new one, never a mixture.
    Replace,
}

/// Write `batches` as Delta data files under `root` and commit them, returning the rows committed.
///
/// Opening the writer reads the existing `_delta_log` once, so this both creates a brand-new table
/// (CTAS) and adds to an established one (`INSERT`) without the caller having to know which.
async fn write_delta_batches(
    store: Arc<dyn object_store::ObjectStore>,
    root: object_store::path::Path,
    location: &str,
    schema: &SchemaRef,
    batches: &[RecordBatch],
    partition_columns: &[String],
    mode: WriteMode,
) -> DfResult<u64> {
    use oxidant_datasource::delta_write::{
        new_file_prefix, stable_table_id, DeltaTableWriter, DeltaWriterConfig,
    };

    let mut writer = DeltaTableWriter::open(
        store,
        root,
        schema.clone(),
        DeltaWriterConfig {
            table_id: stable_table_id(location),
            partition_columns: partition_columns.to_vec(),
            // No `appId`. `txn` actions deduplicate a *replayed* batch of a streaming query, which
            // has a stable batch id; a SQL statement has none, and two `INSERT INTO`s of identical
            // rows are two inserts, not a replay to be swallowed.
            app_id: None,
            ..Default::default()
        },
    )
    .await
    .map_err(oxidant_to_df)?;

    let prefix = new_file_prefix();
    let commit = match mode {
        WriteMode::Append => writer.append(batches, &prefix, None).await,
        WriteMode::Replace => writer.replace(batches, &prefix, None).await,
    }
    .map_err(oxidant_to_df)?;
    Ok(commit.rows)
}

/// Cast `batch` to `schema` when it does not already match it.
///
/// The rows an `INSERT` produces do not always arrive in the table's own types — a Spark integer
/// literal is retyped to `Int32` after planning, so `INSERT INTO t SELECT 9` feeds an `Int32`
/// column to a `BIGINT` table. Writing that straight through would put a file in the table whose
/// Parquet schema disagrees with the table's, which every reader would then have to guess at.
/// Casting here is what Spark does for compatible types; an incompatible one is an error naming
/// the column rather than a silently wrong value.
fn conform_to_schema(batch: RecordBatch, schema: &SchemaRef) -> DfResult<RecordBatch> {
    if batch.schema() == *schema {
        return Ok(batch);
    }
    if batch.num_columns() != schema.fields().len() {
        return Err(DataFusionError::Plan(format!(
            "INSERT provides {} column(s) but the table has {}",
            batch.num_columns(),
            schema.fields().len()
        )));
    }
    let columns = batch
        .columns()
        .iter()
        .zip(schema.fields())
        .map(|(column, field)| {
            if column.data_type() == field.data_type() {
                return Ok(column.clone());
            }
            datafusion::arrow::compute::cast(column, field.data_type()).map_err(|e| {
                DataFusionError::Plan(format!(
                    "INSERT column `{}` is {:?}, which cannot be written to a {:?} column: {e}",
                    field.name(),
                    column.data_type(),
                    field.data_type()
                ))
            })
        })
        .collect::<DfResult<Vec<_>>>()?;
    RecordBatch::try_new(schema.clone(), columns)
        .map_err(|e| DataFusionError::Execution(format!("conform INSERT batch to table: {e}")))
}

/// The one shared refusal for a write that names Iceberg as its storage format.
///
/// Iceberg is deliberately read-only here: a Delta table published with `icebergCompat`
/// (`oxidant_datasource::uniform`) is readable by an Iceberg reader over the *same* Parquet files,
/// so one copy of the data serves both. Writing a second, independent Iceberg table would be a
/// second copy with its own divergent history.
fn iceberg_not_a_write_target() -> DataFusionError {
    DataFusionError::NotImplemented(
        "Iceberg is not a write target. Write the table as Delta with `icebergCompat` and it is \
         readable as Iceberg over the same files."
            .to_string(),
    )
}

/// Serialize `batches` into an in-memory buffer in `format` (Parquet/Csv/Json).
fn encode_batches(
    format: TableFormat,
    schema: &SchemaRef,
    batches: &[RecordBatch],
) -> DfResult<Vec<u8>> {
    let mut buf = Vec::new();
    match format {
        TableFormat::Parquet => {
            let mut writer =
                datafusion::parquet::arrow::ArrowWriter::try_new(&mut buf, schema.clone(), None)
                    .map_err(|e| {
                        DataFusionError::Execution(format!("build parquet writer: {e}"))
                    })?;
            for b in batches {
                writer
                    .write(b)
                    .map_err(|e| DataFusionError::Execution(format!("write parquet batch: {e}")))?;
            }
            writer
                .close()
                .map_err(|e| DataFusionError::Execution(format!("close parquet writer: {e}")))?;
        }
        TableFormat::Csv => {
            let mut writer = datafusion::arrow::csv::Writer::new(&mut buf);
            for b in batches {
                writer
                    .write(b)
                    .map_err(|e| DataFusionError::Execution(format!("write csv batch: {e}")))?;
            }
        }
        TableFormat::Json => {
            let mut writer = datafusion::arrow::json::LineDelimitedWriter::new(&mut buf);
            for b in batches {
                writer
                    .write(b)
                    .map_err(|e| DataFusionError::Execution(format!("write json batch: {e}")))?;
            }
            writer
                .finish()
                .map_err(|e| DataFusionError::Execution(format!("finish json writer: {e}")))?;
        }
        TableFormat::Iceberg => return Err(iceberg_not_a_write_target()),
        // Delta is committed through `write_delta_batches`, never serialized as one flat file.
        TableFormat::Delta => {
            return Err(DataFusionError::Internal(
                "Delta batches must be written through `write_delta_batches`".to_string(),
            ));
        }
    }
    Ok(buf)
}

async fn parquet_metadata_provider(
    state: &SessionState,
    md: &TableMetadata,
    table_name: &str,
    roots: Vec<datafusion::datasource::listing::ListingTableUrl>,
) -> DfResult<Arc<dyn TableProvider>> {
    parquet_metadata_provider_with_assignment(
        state,
        md,
        table_name,
        roots,
        crate::shard::ShardAssignment::from_env(),
    )
    .await
}

async fn parquet_metadata_provider_with_assignment(
    state: &SessionState,
    md: &TableMetadata,
    table_name: &str,
    roots: Vec<datafusion::datasource::listing::ListingTableUrl>,
    assignment: Option<crate::shard::ShardAssignment>,
) -> DfResult<Arc<dyn TableProvider>> {
    let listed = crate::shard::list_visible_file_shard_with(
        state,
        roots.clone(),
        ".parquet",
        Some(table_name),
        assignment,
    )
    .await
    .map_err(oxidant_to_df)?;
    if listed.is_empty() {
        // This worker's shard may be vacant while peers still hold files (KAN-5). Prefer the
        // catalog schema; otherwise infer from the unsharded listing so we can return an empty
        // typed table instead of failing the whole stage.
        if let Some(schema) = &md.schema {
            return crate::shard::empty_table(schema.clone()).map_err(oxidant_to_df);
        }
        let unsharded = crate::shard::list_visible_file_shard_with(
            state,
            roots,
            ".parquet",
            Some(table_name),
            None,
        )
        .await
        .map_err(oxidant_to_df)?;
        if unsharded.is_empty() {
            return Err(DataFusionError::Plan(format!(
                "Parquet table `{}` has no visible data files and no declared schema",
                md.location
            )));
        }
        let schema = infer_listed_parquet_schema(state, &unsharded).await?;
        return crate::shard::empty_table(schema).map_err(oxidant_to_df);
    }

    let table_schema = match &md.schema {
        Some(schema) => schema.clone(),
        None => infer_listed_parquet_schema(state, &listed).await?,
    };
    // Best effort: a table whose object store will not resolve is simply read-only, which is what
    // it already was before `insert_into` existed.
    let write_target = write_target_for(state, md);
    let (file_schema, partition_columns) =
        split_partition_schema(&table_schema, &md.partition_columns);
    let partition_fields = md
        .partition_columns
        .iter()
        .filter_map(|name| table_schema.field_with_name(name).ok().cloned())
        .map(Arc::new)
        .collect::<Vec<_>>();
    let empty_partition_values = std::collections::BTreeMap::new();
    let mut groups: Vec<(
        datafusion::execution::object_store::ObjectStoreUrl,
        Vec<datafusion::datasource::listing::PartitionedFile>,
    )> = Vec::new();
    // A replicated table scans this full resolved file set on every worker; fingerprint it for
    // the process-global dim cache before `listed` is consumed into file groups below.
    let dim_cache_fingerprint = crate::shard::is_replicated_table(table_name).then(|| {
        (
            crate::dim_cache::fingerprint_object_metas(&listed),
            listed.iter().map(|(_, meta)| meta.size).sum::<u64>(),
        )
    });
    for (url, meta) in listed {
        let mut file = datafusion::datasource::listing::PartitionedFile::new_from_meta(meta);
        file.partition_values = partition_values(
            url.as_str(),
            &empty_partition_values,
            &md.partition_columns,
            &partition_columns,
        )?;
        let store_url = url.object_store();
        match groups
            .iter_mut()
            .find(|(existing, _)| existing == &store_url)
        {
            Some((_, files)) => files.push(file),
            None => groups.push((store_url, vec![file])),
        }
    }
    let logical_statistics = lakehouse_logical_statistics(
        state,
        &datafusion::datasource::table_schema::TableSchema::new(
            file_schema.clone(),
            partition_fields.clone(),
        ),
        &groups,
    )
    .await;
    let provider: Arc<dyn TableProvider> = Arc::new(LakehouseTableProvider {
        schema: table_schema,
        file_schema,
        partition_fields,
        groups,
        case_insensitive_schema_adapter: md.schema.is_some(),
        statistics: logical_statistics,
        write_target,
    });
    match dim_cache_fingerprint {
        Some((fingerprint, source_bytes)) => {
            crate::dim_cache::memoize_provider(state, &md.name, fingerprint, source_bytes, provider)
                .await
        }
        None => Ok(provider),
    }
}

async fn resolve_lakehouse_provider(
    state: &SessionState,
    md: &TableMetadata,
    table_name: &str,
    require_snapshot_pin: bool,
) -> DfResult<ProviderResolution> {
    use datafusion::datasource::listing::ListingTableUrl;

    let root = ListingTableUrl::parse(crate::shard::ensure_collection_url(&md.location))
        .map_err(loc_err(md))?;
    ensure_remote_store(state, &root, Some(&md.storage_options))?;
    let store = state.runtime_env().object_store(&root)?;
    // Captured now, while the object store for this table's scheme is known to be registered, so
    // an `INSERT` later does not have to re-derive credentials from a `TaskContext`.
    let write_target = Some(LakehouseWriteTarget {
        location: md.location.clone(),
        format: md.format,
        partition_columns: md.partition_columns.clone(),
        store: store.clone(),
        root: root.prefix().clone(),
    });
    let pinned_snapshot = requested_lakehouse_snapshot(&md.name);
    if require_snapshot_pin {
        if !lakehouse_snapshot_context_is_set() {
            return Err(DataFusionError::Plan(format!(
                "distributed lakehouse table `{}` was resolved outside its pinned snapshot scope",
                md.name
            )));
        }
        if pinned_snapshot.is_none() {
            return Err(DataFusionError::Plan(format!(
                "distributed stage omitted the snapshot pin for lakehouse table `{}`",
                md.name
            )));
        }
    }
    let metadata_location = md.properties.get("metadata_location").map(String::as_str);
    let resolved = oxidant_datasource::active_files_for_scan(
        store,
        &md.location,
        match md.format {
            TableFormat::Delta => "delta",
            TableFormat::Iceberg => "iceberg",
            _ => unreachable!("lakehouse resolver called for non-lakehouse format"),
        },
        metadata_location,
        pinned_snapshot.as_ref(),
        &oxidant_datasource::ScanRequest::default(),
    )
    .await
    .map_err(oxidant_to_df)?;
    record_lakehouse_snapshot(&md.name, &resolved.snapshot);
    if let Some(mapping) = resolved
        .column_mappings
        .iter()
        .find(|mapping| mapping.logical_path != mapping.physical_path)
    {
        return Err(DataFusionError::NotImplemented(format!(
            "lakehouse column mapping is not yet supported for `{}` (logical `{}` is stored as \
             `{}`); refusing to return null or misnamed columns",
            md.name, mapping.logical_path, mapping.physical_path
        )));
    }

    if resolved.files.is_empty() {
        // A table with no live files is **empty**, not broken. `CREATE TABLE ... AS SELECT` over
        // an empty result, an `INSERT OVERWRITE ... WHERE false`, and a freshly declared streaming
        // table before its first batch all land here — an error would make each of them produce a
        // table that cannot be selected from at all.
        //
        // This needs a schema, and only the catalog has one: there is no data file to infer from.
        // Without a declared schema the old error stands, because guessing a column list would
        // make a downstream query fail somewhere much less obvious.
        return match &md.schema {
            Some(schema) => Ok(ProviderResolution {
                provider: crate::shard::empty_table(schema.clone()).map_err(oxidant_to_df)?,
                snapshot_key: Some(md.name.clone()),
                snapshot: Some(resolved.snapshot),
            }),
            None => Err(DataFusionError::Plan(format!(
                "table `{}` has no active data files and the catalog does not declare a schema, \
                 so its columns cannot be determined",
                md.location
            ))),
        };
    }

    let table_schema = match &md.schema {
        Some(schema) => schema.clone(),
        None => {
            infer_parquet_schema(state, &resolved.files[0].location, resolved.files[0].size).await?
        }
    };
    let (file_schema, partition_columns) =
        split_partition_schema(&table_schema, &md.partition_columns);
    let partition_fields = md
        .partition_columns
        .iter()
        .filter_map(|name| table_schema.field_with_name(name).ok().cloned())
        .map(Arc::new)
        .collect::<Vec<_>>();

    let mut files_by_location = resolved
        .files
        .into_iter()
        .map(|file| {
            let url = ListingTableUrl::parse(&file.location).map_err(|e| {
                DataFusionError::Plan(format!("bad lakehouse file `{}`: {e}", file.location))
            })?;
            Ok((url, file))
        })
        .collect::<DfResult<Vec<_>>>()?;
    let selected = crate::shard::apply_known_file_shard(
        files_by_location
            .iter()
            .map(|(url, file)| (url.clone(), file.size))
            .collect(),
        Some(table_name),
    );
    let selected_locations = selected
        .into_iter()
        .map(|(url, _)| url.as_str().to_string())
        .collect::<std::collections::HashSet<_>>();
    files_by_location.retain(|(url, _)| selected_locations.contains(url.as_str()));

    if files_by_location.is_empty() {
        return Ok(ProviderResolution {
            provider: crate::shard::empty_table(table_schema).map_err(oxidant_to_df)?,
            snapshot_key: Some(md.name.clone()),
            snapshot: Some(resolved.snapshot),
        });
    }

    let mut position_delete_cache = HashMap::new();
    // A replicated lakehouse table scans this full pinned file set on every worker; its decoded
    // batches are cached process-globally under the snapshot identity (dim cache).
    let dim_source_bytes = crate::shard::is_replicated_table(table_name).then(|| {
        files_by_location
            .iter()
            .map(|(_, file)| file.size)
            .sum::<u64>()
    });
    let mut groups: Vec<(
        datafusion::execution::object_store::ObjectStoreUrl,
        Vec<datafusion::datasource::listing::PartitionedFile>,
    )> = Vec::new();
    for (url, file) in files_by_location {
        let mut deleted_rows = Vec::new();
        for deletion in &file.deletions {
            match deletion {
                oxidant_datasource::RowDeletion::DeltaDeletionVector {
                    deleted_row_indexes,
                } => deleted_rows.extend(deleted_row_indexes.iter().copied()),
                oxidant_datasource::RowDeletion::IcebergPositionDelete { delete_file } => {
                    let positions =
                        if let Some(positions) = position_delete_cache.get(&delete_file.location) {
                            positions
                        } else {
                            let positions =
                                read_iceberg_position_deletes(state, md, delete_file).await?;
                            position_delete_cache.insert(delete_file.location.clone(), positions);
                            position_delete_cache
                                .get(&delete_file.location)
                                .expect("position delete cache entry was just inserted")
                        };
                    let mut matched_target = false;
                    for (recorded_path, indexes) in positions {
                        if iceberg_paths_equal(recorded_path, &file.location) {
                            matched_target = true;
                            deleted_rows.extend(indexes.iter().copied());
                        }
                    }
                    if !matched_target
                        && delete_file.record_count > 0
                        && delete_file.referenced_data_file.is_some()
                    {
                        return Err(DataFusionError::Execution(format!(
                            "Iceberg position delete `{}` was associated with data file `{}` but \
                             contained no matching `file_path`; refusing to skip the delete",
                            delete_file.location, file.location
                        )));
                    }
                }
                oxidant_datasource::RowDeletion::IcebergEqualityDelete { delete_file } => {
                    return Err(DataFusionError::NotImplemented(format!(
                        "Iceberg equality deletes are not yet supported; refusing to read `{}` \
                         with equality delete file `{}`",
                        md.name, delete_file.location
                    )));
                }
            }
        }
        deleted_rows.sort_unstable();
        deleted_rows.dedup();

        let mut partitioned = datafusion::datasource::listing::PartitionedFile::new(
            url.prefix().to_string(),
            file.size,
        );
        partitioned.partition_values = partition_values(
            &file.location,
            &file.partition_values,
            &md.partition_columns,
            &partition_columns,
        )?;
        if !deleted_rows.is_empty() {
            let access_plan = deletion_access_plan(state, &url, file.size, &deleted_rows).await?;
            partitioned = partitioned.with_extension(access_plan);
        }

        let store_url = url.object_store();
        match groups
            .iter_mut()
            .find(|(existing, _)| existing == &store_url)
        {
            Some((_, files)) => files.push(partitioned),
            None => groups.push((store_url, vec![partitioned])),
        }
    }

    let logical_statistics = lakehouse_logical_statistics(
        state,
        &datafusion::datasource::table_schema::TableSchema::new(
            file_schema.clone(),
            partition_fields.clone(),
        ),
        &groups,
    )
    .await;
    let provider: Arc<dyn TableProvider> = Arc::new(LakehouseTableProvider {
        schema: table_schema,
        file_schema,
        partition_fields,
        groups,
        case_insensitive_schema_adapter: md.schema.is_some(),
        statistics: logical_statistics,
        write_target,
    });
    let provider = match dim_source_bytes {
        Some(source_bytes) => {
            crate::dim_cache::memoize_provider(
                state,
                &md.name,
                crate::dim_cache::fingerprint_snapshot(&resolved.snapshot),
                source_bytes,
                provider,
            )
            .await?
        }
        None => provider,
    };
    Ok(ProviderResolution {
        provider,
        snapshot_key: Some(md.name.clone()),
        snapshot: Some(resolved.snapshot),
    })
}

#[derive(Debug)]
struct LakehouseTableProvider {
    schema: SchemaRef,
    file_schema: SchemaRef,
    partition_fields: Vec<datafusion::arrow::datatypes::FieldRef>,
    groups: Vec<(
        datafusion::execution::object_store::ObjectStoreUrl,
        Vec<datafusion::datasource::listing::PartitionedFile>,
    )>,
    case_insensitive_schema_adapter: bool,
    /// Table-wide parquet-footer statistics computed ONCE at provider construction (KAN-160,
    /// via [`lakehouse_logical_statistics`]) — the same KAN-143 aggregate the physical scan
    /// attaches per file group — so LOGICAL-plan consumers (join ordering, the
    /// oxidant-execution semi-join filter admission gate) see the table's real row count
    /// instead of DataFusion's `Statistics::new_unknown` default. `None` keeps the unknown
    /// shape (statistics disabled or no footer readable).
    statistics: Option<datafusion::common::Statistics>,
    /// Where an `INSERT` into this table writes, when one is possible. `None` makes the table
    /// read-only — the object store for its location could not be resolved.
    write_target: Option<LakehouseWriteTarget>,
}

/// Everything an `INSERT` into a lakehouse table needs, captured when the provider is built.
///
/// The object store is held rather than re-resolved because `DataSink::write_all` only gets a
/// `TaskContext`, and re-deriving an `s3://` table's credentials from there would repeat the
/// assumed-role/vended-credential resolution that [`ensure_remote_store`] already did.
#[derive(Debug, Clone)]
struct LakehouseWriteTarget {
    location: String,
    format: TableFormat,
    partition_columns: Vec<String>,
    store: Arc<dyn object_store::ObjectStore>,
    root: object_store::path::Path,
}

#[async_trait]
impl TableProvider for LakehouseTableProvider {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// `INSERT INTO` / `INSERT OVERWRITE` into a catalog table.
    ///
    /// Delta takes both: `INSERT INTO` adds files and `INSERT OVERWRITE` retires every
    /// currently-live file in the same commit that adds the new ones, so a concurrent reader sees
    /// the whole old table or the whole new one and never a mixture. A plain Parquet table takes
    /// only `INSERT INTO` (a new file in the directory); it has no commit log, so an overwrite
    /// could not be made atomic and is refused rather than faked. Iceberg is read-only — see
    /// [`iceberg_not_a_write_target`].
    async fn insert_into(
        &self,
        _state: &dyn datafusion::catalog::Session,
        input: Arc<dyn datafusion::physical_plan::ExecutionPlan>,
        insert_op: datafusion::logical_expr::dml::InsertOp,
    ) -> DfResult<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        use datafusion::datasource::sink::DataSinkExec;
        use datafusion::logical_expr::dml::InsertOp;

        let Some(target) = self.write_target.clone() else {
            return Err(DataFusionError::NotImplemented(
                "this table's storage location cannot be written to".to_string(),
            ));
        };
        let mode = match insert_op {
            InsertOp::Append => WriteMode::Append,
            InsertOp::Overwrite => WriteMode::Replace,
            // `REPLACE INTO` needs row-level matching on a key — Delta expresses that as MERGE,
            // which this writer does not implement. Refusing beats silently appending duplicates.
            InsertOp::Replace => {
                return Err(DataFusionError::NotImplemented(
                    "`REPLACE INTO` is not supported here; use `INSERT OVERWRITE` to replace the \
                     whole table"
                        .to_string(),
                ));
            }
        };
        // Rejected at planning time rather than mid-write, so a statement that cannot work never
        // starts producing rows.
        match (target.format, mode) {
            (TableFormat::Delta, _) => {}
            // A Hive-partitioned Parquet table keeps its partition columns in the *directory
            // path*, not in the file — which is what the reader
            // (`parquet_metadata_provider_with_assignment` → `split_partition_schema`) expects.
            // The flat single-file write below would put a file with those columns still in it at
            // the table root, and every row would read back with the wrong partition values.
            (TableFormat::Parquet, WriteMode::Append) if !target.partition_columns.is_empty() => {
                return Err(DataFusionError::NotImplemented(format!(
                    "`INSERT` into the partitioned Parquet table at `{}` is not supported: its \
                     partition columns ({}) live in the directory path, and this writer emits one \
                     flat file. Use a Delta table, which partitions on write.",
                    target.location,
                    target.partition_columns.join(", ")
                )));
            }
            (TableFormat::Parquet, WriteMode::Append) => {}
            (TableFormat::Parquet, WriteMode::Replace) => {
                return Err(DataFusionError::NotImplemented(
                    "`INSERT OVERWRITE` is not supported for a plain Parquet table: it has no \
                     transaction log, so replacing its files could not be made atomic and a \
                     concurrent reader would see a half-empty table. Use a Delta table."
                        .to_string(),
                ));
            }
            (TableFormat::Iceberg, _) => return Err(iceberg_not_a_write_target()),
            (format, _) => {
                return Err(DataFusionError::NotImplemented(format!(
                    "`INSERT` into a {format:?} catalog table is not supported"
                )))
            }
        }
        let sink = Arc::new(LakehouseDataSink {
            target,
            schema: self.schema.clone(),
            mode,
        });
        Ok(Arc::new(DataSinkExec::new(input, sink, None)))
    }

    fn table_type(&self) -> datafusion::logical_expr::TableType {
        datafusion::logical_expr::TableType::Base
    }

    fn statistics(&self) -> Option<datafusion::common::Statistics> {
        self.statistics.clone()
    }

    async fn scan(
        &self,
        state: &dyn datafusion::catalog::Session,
        projection: Option<&Vec<usize>>,
        _filters: &[datafusion::logical_expr::Expr],
        limit: Option<usize>,
    ) -> DfResult<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        use datafusion::datasource::physical_plan::{FileScanConfigBuilder, ParquetSource};
        use datafusion::datasource::source::DataSourceExec;
        use datafusion::datasource::table_schema::TableSchema;

        use datafusion::datasource::physical_plan::parquet::CachedParquetFileReaderFactory;

        let table_schema =
            TableSchema::new(self.file_schema.clone(), self.partition_fields.clone());
        let mut plans: Vec<Arc<dyn datafusion::physical_plan::ExecutionPlan>> =
            Vec::with_capacity(self.groups.len());
        for (store_url, files) in &self.groups {
            let parquet_options = datafusion::common::config::TableParquetOptions {
                global: state.config_options().execution.parquet.clone(),
                ..Default::default()
            };
            let metadata_size_hint = parquet_options.global.metadata_size_hint;
            let mut source = ParquetSource::new(table_schema.clone())
                .with_table_parquet_options(parquet_options);
            // Attach the runtime's shared file-metadata cache exactly the way stock
            // `ParquetFormat::create_physical_plan` does (datafusion-datasource-parquet
            // 54, file_format.rs). A hand-built `DataSourceExec` otherwise falls back to
            // `DefaultParquetFileReaderFactory` — no cache — so every task of every stage
            // re-GETs every file's footer. With the cached factory, repeated scans of a
            // file (across tasks, stages, and queries in this session) cost at most one
            // footer fetch per (path, size, mtime) — and the footer
            // `parquet_footer_file_groups` already read for scan statistics is reused
            // instead of fetched twice, because both paths key into the same
            // `DefaultFilesMetadataCache` entry.
            let metadata_cache = state.runtime_env().cache_manager.get_file_metadata_cache();
            let store = state.runtime_env().object_store(store_url)?;
            source = source.with_parquet_file_reader_factory(Arc::new(
                CachedParquetFileReaderFactory::new(store, metadata_cache),
            ));
            if let Some(metadata_size_hint) = metadata_size_hint {
                source = source.with_metadata_size_hint(metadata_size_hint);
            }
            let source = Arc::new(source);
            // Attach parquet-footer statistics (exact row counts) so plan-time consumers see
            // this table's real size instead of `Statistics::new_unknown`: the KAN-25
            // build-side budget guard stops rerouting every join to sort-merge on sight, and
            // DataFusion's own join selection can size/swap build sides and broadcast small
            // ones. With row deletions (Delta DVs / Iceberg position deletes) the footer
            // count overstates — the conservative direction for both consumers.
            let (file_groups, statistics) =
                parquet_footer_file_groups(state, store_url, &table_schema, files).await;
            let expr_adapter = self.case_insensitive_schema_adapter.then(|| {
                Arc::new(crate::schema_adapt::CaseInsensitiveExprAdapterFactory)
                    as Arc<dyn datafusion::physical_expr_adapter::PhysicalExprAdapterFactory>
            });
            let builder = FileScanConfigBuilder::new(store_url.clone(), source)
                .with_file_groups(file_groups)
                .with_limit(limit)
                .with_expr_adapter(expr_adapter)
                .with_projection_indices(projection.cloned())?;
            let builder = match statistics {
                Some(stats) => builder.with_statistics(stats),
                None => builder,
            };
            plans.push(DataSourceExec::from_data_source(builder.build()));
        }
        if plans.is_empty() {
            Err(DataFusionError::Execution(
                "lakehouse scan has no object-store file groups".into(),
            ))
        } else {
            datafusion::physical_plan::union::UnionExec::try_new(plans)
        }
    }
}

/// Whether [`LakehouseTableProvider`] scans attach parquet-footer statistics (env
/// `OXIDANT_PARQUET_SCAN_STATS`, default **true**). `0`/`false`/`off`/`no` restores the
/// unknown-statistics scans — the escape hatch if footer reads misbehave against some
/// object store. `OXIDANT_PREFER_HASH_JOIN` remains the per-session join-strategy override.
fn parquet_scan_stats_enabled() -> bool {
    parquet_stats_env_flag("OXIDANT_PARQUET_SCAN_STATS")
}

/// Whether the footer statistics also carry COLUMN statistics (min/max, null counts,
/// distinct counts) in addition to row/byte counts (env `OXIDANT_PARQUET_COLUMN_STATS`,
/// default **true**). Column stats feed DataFusion's join cardinality estimation — an FK
/// star join's output is otherwise estimated as `Inexact(min(left, right))`, orders of
/// magnitude under the real fact-sized result (see `provable_row_bound` in lib.rs). Setting
/// `0`/`false`/`off`/`no` restores the row-counts-only shape (the pre-KAN-143 behavior)
/// while keeping the footer prefetch itself. `OXIDANT_PARQUET_SCAN_STATS=0` still disables
/// footer reads entirely.
fn parquet_column_stats_enabled() -> bool {
    parquet_stats_env_flag("OXIDANT_PARQUET_COLUMN_STATS")
}

/// Shared `0`/`false`/`off`/`no` parsing for the scan-statistics env gates; unset means ON.
fn parquet_stats_env_flag(var: &str) -> bool {
    std::env::var(var)
        .ok()
        .as_deref()
        .map(|v| {
            !(v == "0"
                || v.eq_ignore_ascii_case("false")
                || v.eq_ignore_ascii_case("off")
                || v.eq_ignore_ascii_case("no"))
        })
        .unwrap_or(true)
}

/// Build one single-file [`FileGroup`] per file of a scan's object-store group, reading each
/// file's parquet footer statistics (row count, byte size) through the runtime's
/// metadata-cached reader so repeat scans of a table cost no extra I/O. Returns the groups
/// plus the table-wide aggregate for the [`FileScanConfigBuilder`]: `None` when statistics
/// are disabled or NO footer could be read (the pre-fix unknown-statistics shape — callers
/// then keep DataFusion's `Statistics::new_unknown` default).
///
/// Statistics are advisory: a file whose footer read fails is kept, stat-less, and the
/// aggregate degrades to an `Inexact` partial sum — an under-estimate, the direction the
/// KAN-45/KAN-53 runtime pool-exhaustion retry already backstops. Footer reads NEVER fail
/// the scan itself.
///
/// Column-level min/max/null counts are attached only when [`column_stats_trusted`] proves
/// they describe the file's ACTUAL physical columns: they are computed against the declared
/// file schema, whose column names, case and types can differ from the physical file columns
/// (the gap [`crate::schema_adapt::CaseInsensitiveExprAdapterFactory`] bridges at read
/// time), and the parquet opener consumes per-file column statistics as constant-column
/// (`min == max` / all-null) proofs that rewrite projections to literals before the
/// expression adapter runs. Row counts are schema-independent, so they stay exact.
async fn parquet_footer_file_groups(
    state: &dyn datafusion::catalog::Session,
    store_url: &datafusion::execution::object_store::ObjectStoreUrl,
    table_schema: &datafusion::datasource::table_schema::TableSchema,
    files: &[datafusion::datasource::listing::PartitionedFile],
) -> (
    Vec<datafusion::datasource::physical_plan::FileGroup>,
    Option<datafusion::common::Statistics>,
) {
    use datafusion::common::stats::Precision;
    use datafusion::common::{ColumnStatistics, Statistics};
    use datafusion::datasource::physical_plan::FileGroup;
    use futures::StreamExt;

    let plain_groups = || {
        files
            .iter()
            .cloned()
            .map(|file| FileGroup::new(vec![file]))
            .collect::<Vec<_>>()
    };
    if !parquet_scan_stats_enabled() {
        return (plain_groups(), None);
    }
    let store = match state.runtime_env().object_store(store_url) {
        Ok(store) => store,
        Err(e) => {
            eprintln!(
                "warn: could not resolve object store `{store_url}` for parquet footer \
                 statistics: {e}"
            );
            return (plain_groups(), None);
        }
    };
    let attach_columns = parquet_column_stats_enabled();
    let file_schema = table_schema.file_schema().clone();
    let mut per_file = futures::stream::iter(files.iter().cloned().enumerate().map(|(i, file)| {
        let store = Arc::clone(&store);
        let schema = file_schema.clone();
        async move {
            let stats =
                parquet_file_footer_statistics(state, &store, &schema, &file.object_meta).await;
            (i, file, stats)
        }
    }))
    .buffer_unordered(8)
    .collect::<Vec<_>>()
    .await;
    // Footer reads complete out of order; file groups keep the original file order so the
    // scan's partition layout is identical to the no-statistics shape.
    per_file.sort_by_key(|(i, _, _)| *i);

    let mut groups = Vec::with_capacity(per_file.len());
    let mut num_rows = Precision::Exact(0_usize);
    let mut total_byte_size = Precision::Exact(0_usize);
    let mut read = 0_usize;
    let mut missed = 0_usize;
    // Table-wide column aggregates, built only while EVERY successfully read file
    // contributes trusted column stats; a missed file or one untrusted file poisons the
    // whole aggregate (a partial min/max/null-count across files would be silently wrong).
    // `None` until the first trusted file seeds it — folding into `Absent` would never
    // produce a value (`Precision::min`/`max` propagate `Absent`).
    let mut column_aggs: Option<Vec<ColumnStatistics>> = None;
    let mut column_aggs_poisoned = !attach_columns;
    let mut ndv_summed_files = 0_usize;
    for (_, file, stats) in per_file {
        match stats {
            Ok(footer) => {
                read += 1;
                num_rows = num_rows.add(&footer.stats.num_rows);
                total_byte_size = total_byte_size.add(&footer.stats.total_byte_size);
                let attach_file_columns = attach_columns && footer.column_stats_trusted;
                // Keep ONLY row/byte counts when column stats are disabled or untrusted for
                // this file: DataFusion computes them against the DECLARED file schema, whose
                // column names, case and types can differ from the physical file columns
                // (exactly the gap `CaseInsensitiveExprAdapterFactory` bridges at read time):
                // a case-mismatched column comes back `null_count == num_rows`, and the
                // parquet opener treats that (or `min == max`) as proof of a constant
                // column — literal-replacing or file-pruning real data away.
                let file_stats = if attach_file_columns {
                    footer.stats.clone()
                } else {
                    Statistics::new_unknown(table_schema.file_schema())
                        .with_num_rows(footer.stats.num_rows)
                        .with_total_byte_size(footer.stats.total_byte_size)
                };
                // Group-level statistics (what `partition_statistics(Some(p))` reads) cover
                // the full table schema — file columns + partition columns — per
                // `FileGroup::file_statistics`'s contract.
                let mut group_stats = Statistics::new_unknown(table_schema.table_schema())
                    .with_num_rows(footer.stats.num_rows)
                    .with_total_byte_size(footer.stats.total_byte_size);
                if attach_file_columns {
                    group_stats.column_statistics = table_wide_column_stats(
                        footer.stats.column_statistics.clone(),
                        table_schema,
                    );
                }
                // A 0-row file contributes nothing to the column aggregates — folding its
                // `Absent` min/max in would poison the whole table's statistics.
                if footer.stats.num_rows == Precision::Exact(0) {
                    groups.push(
                        FileGroup::new(vec![file.with_statistics(Arc::new(file_stats))])
                            .with_statistics(Arc::new(group_stats)),
                    );
                    continue;
                }
                match (
                    column_aggs_poisoned,
                    attach_file_columns,
                    column_aggs.as_mut(),
                ) {
                    (false, true, Some(aggs)) => {
                        ndv_summed_files += 1;
                        for (agg, file_column) in
                            aggs.iter_mut().zip(footer.stats.column_statistics.iter())
                        {
                            agg.null_count = agg.null_count.add(&file_column.null_count);
                            agg.min_value = agg.min_value.min(&file_column.min_value);
                            agg.max_value = agg.max_value.max(&file_column.max_value);
                            agg.distinct_count =
                                agg.distinct_count.add(&file_column.distinct_count);
                            agg.byte_size = agg.byte_size.add(&file_column.byte_size);
                        }
                    }
                    (false, true, None) => {
                        ndv_summed_files = 1;
                        column_aggs = Some(footer.stats.column_statistics.clone());
                    }
                    // This file's column stats are unusable — no honest aggregate remains.
                    (false, false, _) => column_aggs_poisoned = true,
                    _ => {}
                }
                groups.push(
                    FileGroup::new(vec![file.with_statistics(Arc::new(file_stats))])
                        .with_statistics(Arc::new(group_stats)),
                );
            }
            Err(e) => {
                missed += 1;
                column_aggs_poisoned = true;
                eprintln!(
                    "warn: could not read parquet footer statistics for `{}`: {e}",
                    file.object_meta.location
                );
                groups.push(FileGroup::new(vec![file]));
            }
        }
    }
    if read == 0 {
        return (groups, None);
    }
    if missed > 0 {
        num_rows = num_rows.to_inexact();
        total_byte_size = total_byte_size.to_inexact();
    }
    let mut statistics = Statistics::new_unknown(table_schema.table_schema())
        .with_num_rows(num_rows)
        .with_total_byte_size(total_byte_size);
    if let Some(mut aggs) = column_aggs.filter(|_| !column_aggs_poisoned) {
        // NDVs do not compose across files: the per-file exact counts SUM to an upper
        // bound (the same value Spark's catalog statistics use), never an exact count.
        if ndv_summed_files > 1 {
            for agg in &mut aggs {
                agg.distinct_count = agg.distinct_count.to_inexact();
            }
        }
        statistics.column_statistics = table_wide_column_stats(aggs, table_schema);
    }
    (groups, Some(statistics))
}

/// Table-wide statistics for [`LakehouseTableProvider::statistics`] (KAN-160): merge the
/// per-object-store-group aggregates of [`parquet_footer_file_groups`] — the very same
/// KAN-143 footer statistics the physical scan attaches — so the logical plan sees the
/// table's real row count. Computing here (provider construction, an async context) is what
/// lets the sync `TableProvider::statistics()` stay cheap: the footer reads go through the
/// runtime's shared metadata cache, so the later physical scan reuses them instead of
/// fetching twice. The per-file trust gate is inherited unchanged (a case/type-mismatched
/// file contributes row/byte counts only; a missed file degrades the group to `Inexact`).
///
/// `None` when statistics are disabled or NO group produced an aggregate (the pre-KAN-160
/// unknown-statistics shape — consumers then fail open). A group whose footers are all
/// unreadable is SKIPPED rather than collapsing the table to `Absent`: the remaining groups
/// still merge, with the merged row/byte counts degraded to `Inexact` so consumers treat the
/// partial total conservatively. Multiple object-store groups for
/// one table are rare; column min/max do not compose honestly across independent
/// aggregates, so a multi-group table keeps merged row/byte counts only (single-group
/// tables keep the group's full column statistics).
async fn lakehouse_logical_statistics(
    state: &dyn datafusion::catalog::Session,
    table_schema: &datafusion::datasource::table_schema::TableSchema,
    groups: &[(
        datafusion::execution::object_store::ObjectStoreUrl,
        Vec<datafusion::datasource::listing::PartitionedFile>,
    )],
) -> Option<datafusion::common::Statistics> {
    use datafusion::common::Statistics;

    let mut merged: Option<Statistics> = None;
    let mut skipped_group = false;
    for (store_url, files) in groups {
        let (_, group_stats) =
            parquet_footer_file_groups(state, store_url, table_schema, files).await;
        // A group with no readable footer contributes nothing — skip it rather than collapse
        // the WHOLE table to `Absent` (which would fail the KAN-160 semi-join gate open for a
        // partially-readable table). The merged counts are then a partial total, so they are
        // marked `Inexact` below: conservative, and the gate stays usable on the exact path.
        let Some(group_stats) = group_stats else {
            skipped_group = true;
            continue;
        };
        merged = Some(match merged {
            None => group_stats,
            Some(prev) => Statistics::new_unknown(table_schema.table_schema())
                .with_num_rows(prev.num_rows.add(&group_stats.num_rows))
                .with_total_byte_size(prev.total_byte_size.add(&group_stats.total_byte_size)),
        });
    }
    if skipped_group {
        if let Some(stats) = &mut merged {
            stats.num_rows = stats.num_rows.to_inexact();
            stats.total_byte_size = stats.total_byte_size.to_inexact();
        }
    }
    merged
}

/// A file's footer statistics plus whether the COLUMN-level part is safe to use.
struct FileFooterStatistics {
    /// Full statistics against the declared file schema, exactly as DataFusion computes
    /// them (`DFParquetMetadata::statistics_from_parquet_metadata`): exact row count,
    /// per-column min/max, null counts, distinct counts and byte sizes where the footer
    /// carries them.
    stats: datafusion::common::Statistics,
    /// `false` when the declared file schema does not line up with the file's physical
    /// columns (see [`column_stats_trusted`]) — consumers must then keep only the row/byte
    /// counts, which are schema-independent and stay exact.
    column_stats_trusted: bool,
}

/// Read one file's footer (through the runtime's shared metadata cache, same entry the
/// scan's `CachedParquetFileReaderFactory` later reuses) and compute its statistics. This
/// is exactly `ParquetFormat::infer_stats` — `DFParquetMetadata::fetch_metadata` +
/// `statistics_from_parquet_metadata`, same cache, same size hint, same page-index policy —
/// except the metadata stays around for the [`column_stats_trusted`] check instead of being
/// dropped inside `fetch_statistics`.
async fn parquet_file_footer_statistics(
    state: &dyn datafusion::catalog::Session,
    store: &Arc<dyn object_store::ObjectStore>,
    file_schema: &SchemaRef,
    object_meta: &object_store::ObjectMeta,
) -> DfResult<FileFooterStatistics> {
    use datafusion::datasource::file_format::parquet::ParquetFormat;
    use datafusion_datasource_parquet::metadata::DFParquetMetadata;

    let metadata_cache = state.runtime_env().cache_manager.get_file_metadata_cache();
    let metadata = DFParquetMetadata::new(store.as_ref(), object_meta)
        .with_metadata_size_hint(ParquetFormat::default().metadata_size_hint())
        .with_file_metadata_cache(Some(metadata_cache))
        .fetch_metadata()
        .await?;
    let stats = DFParquetMetadata::statistics_from_parquet_metadata(&metadata, file_schema)?;
    Ok(FileFooterStatistics {
        column_stats_trusted: column_stats_trusted(&metadata, file_schema),
        stats,
    })
}

/// Whether the column statistics DataFusion derives for `file_schema` describe the file's
/// ACTUAL physical columns — and are therefore safe to hand to the parquet opener, which
/// turns `min == max` / all-null column stats into constant-column literal rewrites of the
/// projection and predicate (`constant_columns_from_stats`, datafusion-datasource-parquet
/// 54 opener).
///
/// DataFusion looks each declared column up BY NAME in the physical schema
/// (`StatisticsConverter::try_new`); when the lookup fails it stamps
/// `null_count = Exact(num_rows)` — stats that "prove" an all-null column. That is correct
/// for a genuinely absent column (schema evolution: the reader yields nulls too), but wrong
/// when the column exists under a different name case: the
/// `CaseInsensitiveExprAdapterFactory` then decodes real data while the stats claim all
/// null, and the opener literal-replaces the column away. Attach column stats only when
/// every declared column that exists in the file case-INSENSITIVELY also resolves by its
/// exact name.
fn column_stats_trusted(
    metadata: &datafusion::parquet::file::metadata::ParquetMetaData,
    file_schema: &SchemaRef,
) -> bool {
    use datafusion::parquet::arrow::arrow_reader::statistics::StatisticsConverter;
    use datafusion::parquet::arrow::parquet_to_arrow_schema;

    let file_metadata = metadata.file_metadata();
    // Mirror `statistics_from_parquet_metadata`'s own physical-schema derivation —
    // including its binary/string type coercions — so the check passes exactly when its
    // converter lookups succeeded.
    let mut physical_schema = match parquet_to_arrow_schema(
        file_metadata.schema_descr(),
        file_metadata.key_value_metadata(),
    ) {
        Ok(schema) => schema,
        Err(_) => return false,
    };
    if let Some(merged) = datafusion_datasource_parquet::apply_file_schema_type_coercions(
        file_schema,
        &physical_schema,
    ) {
        physical_schema = merged;
    }
    file_schema.fields().iter().all(|field| {
        StatisticsConverter::try_new(field.name(), &physical_schema, file_metadata.schema_descr())
            .is_ok()
            || !physical_schema
                .fields()
                .iter()
                .any(|physical| physical.name().eq_ignore_ascii_case(field.name()))
    })
}

/// Widen file-schema-shaped column statistics to the full table schema by appending unknown
/// entries for the partition columns (which footers never describe).
fn table_wide_column_stats(
    mut file_columns: Vec<datafusion::common::ColumnStatistics>,
    table_schema: &datafusion::datasource::table_schema::TableSchema,
) -> Vec<datafusion::common::ColumnStatistics> {
    let partition_columns = table_schema
        .table_schema()
        .fields()
        .len()
        .saturating_sub(file_columns.len());
    file_columns.extend(
        std::iter::repeat_with(datafusion::common::ColumnStatistics::new_unknown)
            .take(partition_columns),
    );
    file_columns
}

async fn infer_listed_parquet_schema(
    state: &SessionState,
    files: &[(
        datafusion::datasource::listing::ListingTableUrl,
        object_store::ObjectMeta,
    )],
) -> DfResult<SchemaRef> {
    use datafusion::datasource::file_format::parquet::ParquetFormat;
    use datafusion::datasource::file_format::FileFormat;

    let store_url = files[0].0.object_store();
    if files.iter().any(|(url, _)| url.object_store() != store_url) {
        return Err(DataFusionError::NotImplemented(
            "schema inference across multiple object stores is not supported; provide a catalog schema"
                .into(),
        ));
    }
    let store = state.runtime_env().object_store(&store_url)?;
    let objects = files
        .iter()
        .map(|(_, metadata)| metadata.clone())
        .collect::<Vec<_>>();
    ParquetFormat::default()
        .infer_schema(state as &dyn datafusion::catalog::Session, &store, &objects)
        .await
}

async fn infer_parquet_schema(
    state: &SessionState,
    location: &str,
    size: u64,
) -> DfResult<SchemaRef> {
    use datafusion::parquet::arrow::async_reader::ParquetObjectReader;
    use datafusion::parquet::arrow::ParquetRecordBatchStreamBuilder;

    let url = datafusion::datasource::listing::ListingTableUrl::parse(location)
        .map_err(|e| DataFusionError::Plan(format!("bad Parquet file `{location}`: {e}")))?;
    let store = state.runtime_env().object_store(&url)?;
    let reader = ParquetObjectReader::new(store, url.prefix().clone()).with_file_size(size);
    let builder = ParquetRecordBatchStreamBuilder::new(reader)
        .await
        .map_err(|e| {
            DataFusionError::Execution(format!("read Parquet schema `{location}`: {e}"))
        })?;
    Ok(builder.schema().clone())
}

async fn deletion_access_plan(
    state: &SessionState,
    url: &datafusion::datasource::listing::ListingTableUrl,
    size: u64,
    deleted_rows: &[u64],
) -> DfResult<datafusion::datasource::physical_plan::parquet::ParquetAccessPlan> {
    use datafusion::datasource::physical_plan::parquet::ParquetAccessPlan;
    use datafusion::parquet::arrow::arrow_reader::{RowSelection, RowSelector};
    use datafusion::parquet::arrow::async_reader::{AsyncFileReader, ParquetObjectReader};

    let store = state.runtime_env().object_store(url)?;
    let mut reader = ParquetObjectReader::new(store, url.prefix().clone()).with_file_size(size);
    let metadata = reader.get_metadata(None).await.map_err(|e| {
        DataFusionError::Execution(format!("read Parquet metadata `{}`: {e}", url.as_str()))
    })?;
    let mut plan = ParquetAccessPlan::new_all(metadata.num_row_groups());
    let mut file_offset = 0u64;
    let mut delete_offset = 0usize;
    for (row_group_index, row_group) in metadata.row_groups().iter().enumerate() {
        let row_count = u64::try_from(row_group.num_rows()).map_err(|_| {
            DataFusionError::Execution(format!("negative Parquet row count in `{}`", url.as_str()))
        })?;
        let row_group_end = file_offset + row_count;
        let start = delete_offset;
        while delete_offset < deleted_rows.len() && deleted_rows[delete_offset] < row_group_end {
            delete_offset += 1;
        }
        if start != delete_offset {
            let mut selectors = Vec::new();
            let mut cursor = 0usize;
            for deleted in &deleted_rows[start..delete_offset] {
                if *deleted < file_offset {
                    return Err(DataFusionError::Execution(format!(
                        "deletion index {deleted} is out of order for `{}`",
                        url.as_str()
                    )));
                }
                let local = usize::try_from(*deleted - file_offset).map_err(|_| {
                    DataFusionError::Execution(format!(
                        "deletion index {deleted} does not fit this platform"
                    ))
                })?;
                if local > cursor {
                    selectors.push(RowSelector::select(local - cursor));
                }
                selectors.push(RowSelector::skip(1));
                cursor = local + 1;
            }
            let row_count = usize::try_from(row_count)
                .map_err(|_| DataFusionError::Execution("Parquet row group is too large".into()))?;
            if cursor < row_count {
                selectors.push(RowSelector::select(row_count - cursor));
            }
            plan.scan_selection(row_group_index, RowSelection::from(selectors));
        }
        file_offset = row_group_end;
    }
    if delete_offset != deleted_rows.len() {
        return Err(DataFusionError::Execution(format!(
            "deletion index {} exceeds the {} rows in `{}`",
            deleted_rows[delete_offset],
            file_offset,
            url.as_str()
        )));
    }
    Ok(plan)
}

async fn read_iceberg_position_deletes(
    state: &SessionState,
    md: &TableMetadata,
    delete_file: &oxidant_datasource::IcebergDeleteFile,
) -> DfResult<HashMap<String, Vec<u64>>> {
    use datafusion::arrow::array::Array;
    use datafusion::common::ScalarValue;
    use datafusion::parquet::arrow::async_reader::ParquetObjectReader;
    use datafusion::parquet::arrow::ParquetRecordBatchStreamBuilder;
    use futures::TryStreamExt;

    if delete_file.file_format != oxidant_datasource::IcebergFileFormat::Parquet {
        return Err(DataFusionError::NotImplemented(format!(
            "Iceberg position delete format {:?} is not supported for `{}`",
            delete_file.file_format, delete_file.location
        )));
    }
    let url = datafusion::datasource::listing::ListingTableUrl::parse(&delete_file.location)
        .map_err(|e| {
            DataFusionError::Plan(format!(
                "bad Iceberg position delete path `{}`: {e}",
                delete_file.location
            ))
        })?;
    ensure_remote_store(state, &url, Some(&md.storage_options))?;
    let store = state.runtime_env().object_store(&url)?;
    let reader =
        ParquetObjectReader::new(store, url.prefix().clone()).with_file_size(delete_file.size);
    let batches = ParquetRecordBatchStreamBuilder::new(reader)
        .await
        .map_err(|e| {
            DataFusionError::Execution(format!(
                "read Iceberg position delete metadata `{}`: {e}",
                delete_file.location
            ))
        })?
        .build()
        .map_err(|e| {
            DataFusionError::Execution(format!(
                "open Iceberg position delete `{}`: {e}",
                delete_file.location
            ))
        })?
        .try_collect::<Vec<_>>()
        .await
        .map_err(|e| {
            DataFusionError::Execution(format!(
                "read Iceberg position delete `{}`: {e}",
                delete_file.location
            ))
        })?;

    let mut positions = HashMap::<String, Vec<u64>>::new();
    for batch in batches {
        let file_column = batch.schema().index_of("file_path").map_err(|_| {
            DataFusionError::Execution(format!(
                "Iceberg position delete `{}` has no `file_path` column",
                delete_file.location
            ))
        })?;
        let pos_column = batch.schema().index_of("pos").map_err(|_| {
            DataFusionError::Execution(format!(
                "Iceberg position delete `{}` has no `pos` column",
                delete_file.location
            ))
        })?;
        for row in 0..batch.num_rows() {
            if batch.column(file_column).is_null(row) || batch.column(pos_column).is_null(row) {
                return Err(DataFusionError::Execution(format!(
                    "Iceberg position delete `{}` contains a null file path or position",
                    delete_file.location
                )));
            }
            let file_path = match ScalarValue::try_from_array(batch.column(file_column), row)? {
                ScalarValue::Utf8(Some(value))
                | ScalarValue::Utf8View(Some(value))
                | ScalarValue::LargeUtf8(Some(value)) => value,
                other => {
                    return Err(DataFusionError::Execution(format!(
                        "Iceberg position delete `{}` has non-string `file_path` value {other:?}",
                        delete_file.location
                    )));
                }
            };
            let position = match ScalarValue::try_from_array(batch.column(pos_column), row)? {
                ScalarValue::Int64(Some(value)) if value >= 0 => value as u64,
                other => {
                    return Err(DataFusionError::Execution(format!(
                        "Iceberg position delete `{}` has invalid `pos` value {other:?}",
                        delete_file.location
                    )));
                }
            };
            positions.entry(file_path).or_default().push(position);
        }
    }
    for indexes in positions.values_mut() {
        indexes.sort_unstable();
        indexes.dedup();
    }
    Ok(positions)
}

fn iceberg_paths_equal(recorded: &str, resolved: &str) -> bool {
    let normalize = |value: &str| {
        value
            .strip_prefix("s3a://")
            .map(|rest| format!("s3://{rest}"))
            .unwrap_or_else(|| value.to_string())
    };
    let recorded = normalize(recorded);
    let resolved = normalize(resolved);
    recorded == resolved
        || (!recorded.contains("://")
            && resolved.ends_with(&format!("/{}", recorded.trim_start_matches('/'))))
}

fn partition_values(
    location: &str,
    values: &std::collections::BTreeMap<String, String>,
    partition_names: &[String],
    partition_columns: &[(String, datafusion::arrow::datatypes::DataType)],
) -> DfResult<Vec<datafusion::common::ScalarValue>> {
    partition_names
        .iter()
        .filter_map(|name| {
            partition_columns
                .iter()
                .find(|(column, _)| column == name)
                .map(|(_, data_type)| (name, data_type))
        })
        .map(|(name, data_type)| {
            let value = values
                .get(name)
                .cloned()
                .or_else(|| hive_partition_value(location, name));
            match value {
                Some(value) if value != "__HIVE_DEFAULT_PARTITION__" => {
                    datafusion::common::ScalarValue::try_from_string(value, data_type)
                }
                _ => datafusion::common::ScalarValue::try_from(data_type),
            }
        })
        .collect()
}

fn hive_partition_value(location: &str, key: &str) -> Option<String> {
    let needle = format!("/{key}=");
    let start = location.find(&needle)? + needle.len();
    let rest = &location[start..];
    let end = rest.find('/').unwrap_or(rest.len());
    Some(rest[..end].to_string())
}

fn loc_err(md: &TableMetadata) -> impl Fn(DataFusionError) -> DataFusionError + '_ {
    move |e| DataFusionError::Plan(format!("bad table location `{}`: {e}", md.location))
}

/// Map a oxidant error onto DataFusion's error type, preserving the failure class.
/// Resolve where an `INSERT` into `md`'s table would write, or `None` when it cannot (an object
/// store that will not resolve, or a location that does not parse) — the table is then read-only,
/// exactly as it was before `insert_into` existed.
fn write_target_for(state: &SessionState, md: &TableMetadata) -> Option<LakehouseWriteTarget> {
    use datafusion::datasource::listing::ListingTableUrl;

    let url = ListingTableUrl::parse(crate::shard::ensure_collection_url(&md.location)).ok()?;
    let store = state.runtime_env().object_store(&url).ok()?;
    Some(LakehouseWriteTarget {
        location: md.location.clone(),
        format: md.format,
        partition_columns: md.partition_columns.clone(),
        store,
        root: url.prefix().clone(),
    })
}

/// Commits an `INSERT`'s rows into a catalog table.
#[derive(Debug)]
struct LakehouseDataSink {
    target: LakehouseWriteTarget,
    schema: SchemaRef,
    mode: WriteMode,
}

impl datafusion::physical_plan::DisplayAs for LakehouseDataSink {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        let op = match self.mode {
            WriteMode::Append => "append",
            WriteMode::Replace => "overwrite",
        };
        write!(
            f,
            "LakehouseDataSink(format={:?}, op={op}, location={})",
            self.target.format, self.target.location
        )
    }
}

#[async_trait]
impl datafusion::datasource::sink::DataSink for LakehouseDataSink {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    async fn write_all(
        &self,
        mut data: datafusion::execution::SendableRecordBatchStream,
        _context: &Arc<datafusion::execution::TaskContext>,
    ) -> DfResult<u64> {
        use futures::StreamExt;
        use object_store::ObjectStoreExt;

        // Collected rather than streamed: one Delta commit is atomic over the file set it carries,
        // so the whole insert has to be written before any of it is visible. See `docs/TODOS.md` —
        // staging a large insert's files before the commit is the follow-up, and is the same work
        // as streaming a derived table's recompute.
        let mut batches = Vec::new();
        while let Some(batch) = data.next().await {
            batches.push(conform_to_schema(batch?, &self.schema)?);
        }

        if self.target.format == TableFormat::Delta {
            return write_delta_batches(
                self.target.store.clone(),
                self.target.root.clone(),
                &self.target.location,
                &self.schema,
                &batches,
                &self.target.partition_columns,
                self.mode,
            )
            .await;
        }

        // Plain Parquet: one new file per insert. The name has to be unique across every writer of
        // the directory, so it carries the same random prefix a Delta data file does — a fixed
        // `part-00000.parquet` would have each insert silently overwrite the last.
        let rows: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
        if rows == 0 {
            return Ok(0);
        }
        let bytes = encode_batches(TableFormat::Parquet, &self.schema, &batches)?;
        let name = format!(
            "part-{}-c000.snappy.parquet",
            oxidant_datasource::delta_write::new_file_prefix()
        );
        let path = self.target.root.clone().join(name);
        self.target
            .store
            .put(&path, bytes.into())
            .await
            .map_err(|e| DataFusionError::Execution(format!("write `{path}`: {e}")))?;
        Ok(rows)
    }
}

fn oxidant_to_df(e: Error) -> DataFusionError {
    match e {
        Error::Plan(m) => DataFusionError::Plan(m),
        Error::Unsupported(m) => DataFusionError::NotImplemented(m),
        Error::Execution(m) | Error::Io(m) => DataFusionError::Execution(m),
    }
}

#[cfg(test)]
mod tests {
    /// Credential resolution order. The default chain is only reached when the table has *not*
    /// stated who it is — otherwise a table carrying its own keys, or a public bucket read
    /// unsigned, would silently get the ambient identity instead.
    #[test]
    fn a_table_that_states_its_own_s3_identity_outranks_the_default_chain() {
        for key in [
            "s3.access-key-id",
            "fs.s3a.access.key",
            "s3.secret-access-key",
            "fs.s3a.secret.key",
            "s3.session-token",
            "fs.s3a.session.token",
            "s3.skip-signature",
            // `object_store` accepts an `aws_`-prefixed alias for every key. Missing one here
            // would let the ambient chain override the identity the table asked for.
            "aws_access_key_id",
            "aws_secret_access_key",
            "aws_session_token",
            "aws_skip_signature",
            // IRSA and the ECS container endpoint name an identity just as much as a key pair.
            "aws_web_identity_token_file",
            "web_identity_token_file",
            "aws_role_arn",
            "container_credentials_full_uri",
        ] {
            assert!(
                pins_s3_identity(&normalize_s3_config_key(key)),
                "`{key}` states an S3 identity and must outrank the default chain"
            );
        }
        // Everything else describes *where* the bucket is, not who we are, so it must leave the
        // credential chain in play.
        for key in [
            "s3.endpoint",
            "fs.s3a.endpoint",
            "s3.region",
            "fs.s3a.endpoint.region",
            "s3.allow-http",
            "s3.virtual-hosted-style-request",
        ] {
            assert!(
                !pins_s3_identity(&normalize_s3_config_key(key)),
                "`{key}` is not an identity and must not suppress the default chain"
            );
        }
    }

    /// The wiring decision itself: which tables actually reach the default credential chain.
    /// `pins_s3_identity` being right is only half of it — this is the call site's behaviour.
    #[test]
    fn the_default_chain_is_used_exactly_when_the_table_names_no_identity() {
        let opts = |pairs: &[(&str, &str)]| -> HashMap<String, String> {
            pairs
                .iter()
                .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                .collect()
        };

        // A table with no storage options at all — the overwhelmingly common case, and the one
        // that used to fall through to the instance-metadata endpoint on a developer machine.
        assert!(should_use_default_chain(None));
        assert!(should_use_default_chain(Some(&opts(&[]))));

        // Options that describe *where* the bucket is leave the chain in play.
        assert!(should_use_default_chain(Some(&opts(&[
            ("s3.endpoint", "https://s3.example"),
            ("s3.region", "eu-west-1"),
            ("s3.allow-http", "true"),
        ]))));

        // Anything naming an identity takes over completely.
        for pinned in [
            vec![("s3.access-key-id", "AKIA"), ("s3.secret-access-key", "s")],
            vec![("fs.s3a.access.key", "AKIA")],
            vec![("aws_session_token", "t")],
            vec![("s3.skip-signature", "true")],
            vec![("aws_web_identity_token_file", "/var/run/token")],
            // Mixed with location options, the identity still wins.
            vec![
                ("s3.endpoint", "https://s3.example"),
                ("s3.access-key-id", "AKIA"),
            ],
        ] {
            assert!(
                !should_use_default_chain(Some(&opts(&pinned))),
                "`{pinned:?}` names an identity, so the ambient chain must not override it"
            );
        }
    }

    #[test]
    fn storage_option_keys_normalize_to_object_store_config_keys() {
        for (key, expected) in [
            ("s3.access-key-id", "access_key_id"),
            ("fs.s3a.access.key", "access_key_id"),
            ("s3.secret-access-key", "secret_access_key"),
            ("fs.s3a.secret.key", "secret_access_key"),
            ("s3.endpoint", "endpoint"),
            ("s3.region", "region"),
            // An unprefixed key passes through unchanged.
            ("access_key_id", "access_key_id"),
            // Iceberg spells properties with hyphens; object_store parses only underscores. A key
            // outside the explicit list above still has to survive the trip, or it is silently
            // dropped and the setting never takes effect.
            ("s3.skip-signature", "skip_signature"),
            ("s3.checksum-algorithm", "checksum_algorithm"),
            ("s3.request-payer", "request_payer"),
        ] {
            assert_eq!(normalize_s3_config_key(key), expected, "key `{key}`");
        }
    }

    use super::*;
    use datafusion::arrow::array::{Int32Array, Int64Array};
    use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::parquet::arrow::ArrowWriter;
    use oxidant_catalog::{Result as CatResult, TableMetadata};

    /// A fake catalog whose single namespace `ns` has one table `orders` at a fixed location.
    struct FakeCatalog {
        location: String,
    }

    #[async_trait]
    impl OxidantCatalog for FakeCatalog {
        fn name(&self) -> &str {
            "fake"
        }
        async fn list_namespaces(&self, _parent: &[String]) -> CatResult<Vec<Vec<String>>> {
            Ok(vec![vec!["ns".to_string()]])
        }
        async fn list_tables(&self, _ns: &[String]) -> CatResult<Vec<String>> {
            Ok(vec!["orders".to_string()])
        }
        async fn load_table(&self, ns: &[String], table: &str) -> CatResult<TableMetadata> {
            if ns == ["ns"] && table == "orders" {
                Ok(TableMetadata::new(
                    "fake.ns.orders",
                    self.location.clone(),
                    TableFormat::Parquet,
                ))
            } else {
                Err(Error::Plan(format!(
                    "no such table: {}.{table}",
                    ns.join(".")
                )))
            }
        }
    }

    fn write_parquet_dir() -> std::path::PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        // pid + nanos is NOT unique enough: several tests in this process call this
        // helper, and two near-simultaneous callers land on the same (coarse) clock
        // tick — they then share one dir, and the first finisher's `remove_dir_all`
        // deletes the other's files mid-scan (ENOENT in
        // `catalog_parquet_scan_caches_footer_across_queries`, E-LOOM-FLAKE). The
        // process-unique sequence makes every dir distinct (same pattern as
        // `shard::tests::write_parts_with_rows`).
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "oxidant-cat-{}-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3, 4]))],
        )
        .unwrap();
        let f = std::fs::File::create(dir.join("part-0.parquet")).unwrap();
        let mut w = ArrowWriter::try_new(f, schema, None).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
        dir
    }

    #[tokio::test]
    async fn lazy_resolution_through_registered_catalog() {
        let dir = write_parquet_dir();
        let location = format!("file://{}", dir.to_string_lossy());

        let engine = crate::Engine::new();
        engine.register_catalog("fake", Arc::new(FakeCatalog { location }));

        // Never pre-registered the table — it resolves lazily via the bridge's async `table()`.
        let batches = engine
            .sql("SELECT COUNT(*) AS c, SUM(x) AS s FROM fake.ns.orders")
            .await
            .unwrap();
        let c = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        let s = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!((c, s), (4, 10));
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Dim cache end-to-end (plain-Parquet path): a replicated table is read+decoded once per
    /// worker per data version. A second engine (new session → fresh provider cache) over the
    /// same files must be served from the process-global cache (hit, no insert), and restating
    /// the table must change the fingerprint: miss → fresh rows, never stale cached ones.
    #[tokio::test]
    async fn replicated_scan_is_cached_across_engines_and_invalidated_by_restate() {
        let dir = write_parquet_dir(); // part-0.parquet: x in [1, 2, 3, 4]
        let location = format!("file://{}/", dir.to_string_lossy());
        let sql = "SELECT SUM(x) AS s FROM fake.ns.orders";

        fn sum(batches: &[RecordBatch]) -> i64 {
            batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0)
        }

        // Process-global counters move only via deltas: other tests in this binary share the
        // cache, and this test's fingerprints are unique to its temp dir.
        let before = crate::dim_cache::global().stats();

        let engine1 = crate::Engine::new();
        engine1.register_catalog(
            "fake",
            Arc::new(FakeCatalog {
                location: location.clone(),
            }),
        );
        let first = crate::shard::with_replicated_tables("orders", engine1.sql(sql)).await;
        assert_eq!(sum(&first.unwrap()), 10);
        let after_first = crate::dim_cache::global().stats();
        assert_eq!(
            (
                after_first.misses - before.misses,
                after_first.inserts - before.inserts
            ),
            (1, 1),
            "first scan populates the cache"
        );
        assert_eq!(after_first.hits - before.hits, 0);

        // New engine (fresh provider cache), same files → same fingerprint → served from the
        // dim cache without touching object storage again.
        let engine2 = crate::Engine::new();
        engine2.register_catalog(
            "fake",
            Arc::new(FakeCatalog {
                location: location.clone(),
            }),
        );
        let second = crate::shard::with_replicated_tables("orders", engine2.sql(sql)).await;
        assert_eq!(sum(&second.unwrap()), 10);
        let after_second = crate::dim_cache::global().stats();
        assert_eq!(
            after_second.hits - after_first.hits,
            1,
            "second engine hits the cache"
        );
        assert_eq!(after_second.misses - after_first.misses, 0);
        assert_eq!(after_second.inserts - after_first.inserts, 0);

        // Restate the table: rewrite the data file (new size + mtime → new fingerprint).
        let part = dir.join("part-0.parquet");
        std::fs::remove_file(&part).unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![10_i64, 20]))],
        )
        .unwrap();
        let f = std::fs::File::create(&part).unwrap();
        let mut w = ArrowWriter::try_new(f, schema, None).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();

        let engine3 = crate::Engine::new();
        engine3.register_catalog(
            "fake",
            Arc::new(FakeCatalog {
                location: location.clone(),
            }),
        );
        let third = crate::shard::with_replicated_tables("orders", engine3.sql(sql)).await;
        assert_eq!(
            sum(&third.unwrap()),
            30,
            "a restated table must serve fresh rows, never the stale cached version"
        );
        let after_third = crate::dim_cache::global().stats();
        assert_eq!(after_third.hits - after_second.hits, 0);
        assert_eq!(
            (
                after_third.misses - after_second.misses,
                after_third.inserts - after_second.inserts
            ),
            (1, 1),
            "the restated version misses and is cached under its own fingerprint"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn vacant_parquet_shard_infers_schema_from_peer_files() {
        use datafusion::datasource::listing::ListingTableUrl;

        let dir = write_parquet_dir();
        let location =
            crate::shard::ensure_collection_url(&format!("file://{}", dir.to_string_lossy()));
        let root = ListingTableUrl::parse(&location).unwrap();
        let ctx = SessionContext::new();
        let state = ctx.state();
        let md = TableMetadata::new("fake.ns.orders", location, TableFormat::Parquet);

        // One file is assigned to worker 0; worker 1 must still register an empty, typed table.
        let provider = parquet_metadata_provider_with_assignment(
            &state,
            &md,
            "orders",
            vec![root],
            Some(crate::shard::ShardAssignment { index: 1, count: 2 }),
        )
        .await
        .expect("vacant shard should infer schema from the unsharded listing");

        assert_eq!(provider.schema().fields().len(), 1);
        assert_eq!(provider.schema().field(0).name(), "x");
        assert_eq!(provider.schema().field(0).data_type(), &DataType::Int64);
        let plan = provider.scan(&state, None, &[], None).await.unwrap();
        let batches = datafusion::physical_plan::collect(plan, ctx.task_ctx())
            .await
            .unwrap();
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 0);

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Write a Hive-partitioned parquet layout: `<dir>/region=<r>/part-0.parquet`, each file
    /// holding only the DATA column `x` (the partition column `region` lives in the path).
    fn write_partitioned_parquet_dir() -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("oxidant-cat-part-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let file_schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        for (region, vals) in [("west", vec![1_i64, 2]), ("east", vec![10, 20, 30])] {
            let pdir = dir.join(format!("region={region}"));
            std::fs::create_dir_all(&pdir).unwrap();
            let batch =
                RecordBatch::try_new(file_schema.clone(), vec![Arc::new(Int64Array::from(vals))])
                    .unwrap();
            let f = std::fs::File::create(pdir.join("part-0.parquet")).unwrap();
            let mut w = ArrowWriter::try_new(f, file_schema.clone(), None).unwrap();
            w.write(&batch).unwrap();
            w.close().unwrap();
        }
        dir
    }

    /// A fake catalog exposing one Hive-partitioned table whose declared schema is `x` + the
    /// partition column `region` (Glue's convention: partition columns appended to the schema).
    struct PartitionedFakeCatalog {
        location: String,
    }

    #[async_trait]
    impl OxidantCatalog for PartitionedFakeCatalog {
        fn name(&self) -> &str {
            "fakepart"
        }
        async fn list_namespaces(&self, _parent: &[String]) -> CatResult<Vec<Vec<String>>> {
            Ok(vec![vec!["ns".to_string()]])
        }
        async fn list_tables(&self, _ns: &[String]) -> CatResult<Vec<String>> {
            Ok(vec!["events".to_string()])
        }
        async fn load_table(&self, ns: &[String], table: &str) -> CatResult<TableMetadata> {
            if ns == ["ns"] && table == "events" {
                let schema = Arc::new(Schema::new(vec![
                    Field::new("x", DataType::Int64, false),
                    Field::new("region", DataType::Utf8, false),
                ]));
                Ok(TableMetadata::new(
                    "fakepart.ns.events",
                    self.location.clone(),
                    TableFormat::Parquet,
                )
                .with_schema(schema)
                .with_partition_columns(vec!["region".to_string()]))
            } else {
                Err(Error::Plan(format!(
                    "no such table: {}.{table}",
                    ns.join(".")
                )))
            }
        }
    }

    #[tokio::test]
    async fn hive_partitioned_read_derives_partition_column_from_path() {
        // The partition column `region` is in the *path*, not the data files. Before A4 it was in
        // the declared schema but never registered as a table partition column, so it scanned as
        // NULL (or failed). Now a filter on it must prune to the matching partition and sum only
        // that partition's rows.
        let dir = write_partitioned_parquet_dir();
        let location = format!("file://{}", dir.to_string_lossy());
        let engine = crate::Engine::new();
        engine.register_catalog("fakepart", Arc::new(PartitionedFakeCatalog { location }));

        let west = engine
            .sql("SELECT SUM(x) AS s FROM fakepart.ns.events WHERE region = 'west'")
            .await
            .unwrap();
        let s = west[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        // If `region` scanned as NULL (the pre-A4 bug), the filter matches nothing and SUM is NULL
        // (value 0); a correct partition-from-path read sums only the `region=west` rows.
        assert_eq!(
            s.value(0),
            3,
            "west partition sums 1 + 2 (region derived from the path)"
        );

        let total = engine
            .sql("SELECT SUM(x) AS s FROM fakepart.ns.events")
            .await
            .unwrap();
        let t = total[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(t, 63, "all partitions: 1+2 + 10+20+30");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A fake EXTERNAL catalog whose `create_table` writes to a local temp dir (no real Glue/Hive
    /// — exercises the `register_table` write path end to end: downcast the `MemTable` → declare
    /// the table → write real Parquet files → build a durable `ListingTable` provider).
    struct WritableFakeCatalog {
        dir: std::path::PathBuf,
    }

    #[async_trait]
    impl OxidantCatalog for WritableFakeCatalog {
        fn name(&self) -> &str {
            "fakewrite"
        }
        async fn list_namespaces(&self, _parent: &[String]) -> CatResult<Vec<Vec<String>>> {
            Ok(vec![vec!["ns".to_string()]])
        }
        async fn list_tables(&self, _ns: &[String]) -> CatResult<Vec<String>> {
            Ok(vec![])
        }
        async fn load_table(&self, ns: &[String], table: &str) -> CatResult<TableMetadata> {
            // Real Glue/Hive would already know about a table `create_table` just declared; this
            // fake mimics that by checking whether `create_table`'s write path actually landed
            // files under the same location convention it used.
            let db = ns.first().cloned().unwrap_or_default();
            let dir = self.dir.join(&db).join(table);
            if dir.is_dir() {
                Ok(TableMetadata::new(
                    format!("fakewrite.{db}.{table}"),
                    format!("file://{}/", dir.to_string_lossy()),
                    TableFormat::Parquet,
                ))
            } else {
                Err(Error::Plan(format!(
                    "no such table: {}.{table}",
                    ns.join(".")
                )))
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
        ) -> CatResult<TableMetadata> {
            let db = namespace.first().cloned().unwrap_or_default();
            let location = location
                .unwrap_or_else(|| format!("file://{}/{db}/{table}/", self.dir.to_string_lossy()));
            Ok(
                TableMetadata::new(format!("fakewrite.{db}.{table}"), location, format)
                    .with_schema(schema)
                    .with_partition_columns(partition_columns.to_vec()),
            )
        }
    }

    #[tokio::test]
    async fn ctas_against_external_catalog_writes_durable_data() {
        let base = std::env::temp_dir().join(format!("oxidant-cat-write-{}", std::process::id()));

        {
            let engine = crate::Engine::new();
            engine.register_catalog(
                "fakewrite",
                Arc::new(WritableFakeCatalog { dir: base.clone() }),
            );
            // No `USING <fmt>` clause — falls straight through to DataFusion's native
            // `CreateMemoryTable` DDL handling, which is exactly the path that used to fail with
            // "schema provider does not support registering tables" for an external catalog.
            engine
                .sql("CREATE TABLE fakewrite.ns.newtable AS SELECT 1 AS x UNION ALL SELECT 2 AS x")
                .await
                .unwrap();
        } // `engine` (and its in-memory MemTable) dropped here.

        // A brand-new Engine/session proves the data is durable on disk, not just cached in the
        // first Engine's transient MemTable.
        let engine2 = crate::Engine::new();
        engine2.register_catalog(
            "fakewrite",
            Arc::new(WritableFakeCatalog { dir: base.clone() }),
        );
        let batches = engine2
            .sql("SELECT SUM(x) AS s FROM fakewrite.ns.newtable")
            .await
            .unwrap();
        let s = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(s, 3);
        let _ = std::fs::remove_dir_all(&base);
    }

    #[tokio::test]
    async fn missing_table_is_a_clean_not_found() {
        let engine = crate::Engine::new();
        engine.register_catalog(
            "fake",
            Arc::new(FakeCatalog {
                location: "file:///nonexistent".to_string(),
            }),
        );
        let err = engine
            .sql("SELECT * FROM fake.ns.missing")
            .await
            .unwrap_err();
        // DataFusion's table-not-found analysis error, not a panic / internal error.
        assert!(format!("{err}").to_lowercase().contains("not"));
    }

    #[tokio::test]
    async fn show_databases_in_catalog_lists_namespaces() {
        use datafusion::arrow::array::{Array, StringArray};
        let engine = crate::Engine::new();
        engine.register_catalog(
            "fake",
            Arc::new(FakeCatalog {
                location: "file:///nonexistent".to_string(),
            }),
        );
        let batches = engine.sql("SHOW DATABASES IN fake").await.unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].schema().field(0).name(), "namespace");
        let ns = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let got: Vec<&str> = (0..ns.len()).map(|i| ns.value(i)).collect();
        assert_eq!(got, vec!["ns"]);
    }

    #[tokio::test]
    async fn show_tables_in_namespace_lists_tables() {
        use datafusion::arrow::array::{Array, StringArray};
        use datafusion::arrow::datatypes::DataType;
        let engine = crate::Engine::new();
        engine.register_catalog(
            "fake",
            Arc::new(FakeCatalog {
                location: "file:///nonexistent".to_string(),
            }),
        );
        let batches = engine.sql("SHOW TABLES IN fake.ns").await.unwrap();
        assert_eq!(batches.len(), 1);
        // Exact 3-column Spark schema, names + types.
        let schema = batches[0].schema();
        assert_eq!(schema.field(0).name(), "namespace");
        assert_eq!(schema.field(1).name(), "tableName");
        assert_eq!(schema.field(2).name(), "isTemporary");
        assert_eq!(schema.field(2).data_type(), &DataType::Boolean);
        let names = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let got: Vec<&str> = (0..names.len()).map(|i| names.value(i)).collect();
        assert_eq!(got, vec!["orders"]);
    }

    #[tokio::test]
    async fn show_databases_includes_registered_catalog() {
        use datafusion::arrow::array::{Array, StringArray};
        let engine = crate::Engine::new();
        engine.register_catalog(
            "fake",
            Arc::new(FakeCatalog {
                location: "file:///nonexistent".to_string(),
            }),
        );
        let batches = engine.sql("SHOW DATABASES").await.unwrap();
        assert_eq!(batches[0].schema().field(0).name(), "namespace");
        let ns = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let got: Vec<&str> = (0..ns.len()).map(|i| ns.value(i)).collect();
        assert!(got.contains(&"ns"), "expected `ns` in {got:?}");
    }

    /// A fake catalog whose single table `mixed` lives at a fixed location with an *optionally*
    /// declared schema — the lever the coercion test flips.
    struct SchemaCatalog {
        location: String,
        schema: Option<SchemaRef>,
    }

    #[async_trait]
    impl OxidantCatalog for SchemaCatalog {
        fn name(&self) -> &str {
            "fake"
        }
        async fn list_namespaces(&self, _parent: &[String]) -> CatResult<Vec<Vec<String>>> {
            Ok(vec![vec!["ns".to_string()]])
        }
        async fn list_tables(&self, _ns: &[String]) -> CatResult<Vec<String>> {
            Ok(vec!["mixed".to_string()])
        }
        async fn load_table(&self, ns: &[String], table: &str) -> CatResult<TableMetadata> {
            if ns == ["ns"] && table == "mixed" {
                let md = TableMetadata::new(
                    "fake.ns.mixed",
                    self.location.clone(),
                    TableFormat::Parquet,
                );
                Ok(match &self.schema {
                    Some(s) => md.with_schema(s.clone()),
                    None => md,
                })
            } else {
                Err(Error::Plan(format!(
                    "no such table: {}.{table}",
                    ns.join(".")
                )))
            }
        }
    }

    /// Write two Parquet files into a fresh dir where column `v` is Int32 in one file and Int64 in
    /// the other — the cross-file type mismatch that breaks schema inference. Returns the dir.
    fn write_mixed_int_parquet_dir() -> std::path::PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        // The two callers of this helper run concurrently in this test binary, and
        // pid + nanos collides whenever they land on the same clock tick: they share
        // one dir and the first finisher's `remove_dir_all` deletes the other's files
        // mid-scan (ENOENT). The process-unique sequence makes every dir distinct
        // (same pattern as `shard::tests::write_parts_with_rows`).
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "oxidant-mixed-{}-{}-{:?}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();

        // File A: v as Int32 (values 1,2,3).
        let schema32 = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, true)]));
        let batch32 = RecordBatch::try_new(
            schema32.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let f = std::fs::File::create(dir.join("part-a.parquet")).unwrap();
        let mut w = ArrowWriter::try_new(f, schema32, None).unwrap();
        w.write(&batch32).unwrap();
        w.close().unwrap();

        // File B: v as Int64 (values 10,20).
        let schema64 = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)]));
        let batch64 = RecordBatch::try_new(
            schema64.clone(),
            vec![Arc::new(Int64Array::from(vec![10, 20]))],
        )
        .unwrap();
        let f = std::fs::File::create(dir.join("part-b.parquet")).unwrap();
        let mut w = ArrowWriter::try_new(f, schema64, None).unwrap();
        w.write(&batch64).unwrap();
        w.close().unwrap();

        dir
    }

    /// With a catalog-declared schema (`v: Int64`), the mixed-int-type Parquet files read fine: the
    /// Int32 file is *cast* to Int64 at scan time by DataFusion's default expression adapter, so the
    /// query succeeds. This is the catalog-schema-honoring behavior the change adds.
    #[tokio::test]
    async fn declared_schema_coerces_mixed_file_types() {
        let dir = write_mixed_int_parquet_dir();
        let location = format!("file://{}", dir.to_string_lossy());
        let declared = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)]));

        let engine = crate::Engine::new();
        engine.register_catalog(
            "fake",
            Arc::new(SchemaCatalog {
                location,
                schema: Some(declared),
            }),
        );

        let batches = engine
            .sql("SELECT COUNT(*) AS c, SUM(v) AS s FROM fake.ns.mixed")
            .await
            .expect("query with declared schema should succeed");
        let c = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        let s = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!((c, s), (5, 36)); // 1+2+3+10+20
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Control: *without* the declared schema, the same mixed-int-type files reproduce DataFusion's
    /// schema-merge failure — proving the declared schema is what makes the read work.
    #[tokio::test]
    async fn without_declared_schema_merge_fails() {
        let dir = write_mixed_int_parquet_dir();
        let location = format!("file://{}", dir.to_string_lossy());

        let engine = crate::Engine::new();
        engine.register_catalog(
            "fake",
            Arc::new(SchemaCatalog {
                location,
                schema: None,
            }),
        );

        let err = engine
            .sql("SELECT SUM(v) AS s FROM fake.ns.mixed")
            .await
            .expect_err("inference should fail to merge Int32 vs Int64");
        let msg = format!("{err}").to_lowercase();
        assert!(
            msg.contains("merge") || msg.contains("does not equal") || msg.contains("data type"),
            "expected a schema-merge error, got: {err}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Write two Parquet files whose column is named `VendorID` (mixed case) — Int32 in one, Int64
    /// in the other — mimicking real NYC-taxi monthly dumps. Glue would declare this column as the
    /// lowercase `vendorid`, so the file→table name match must be case-insensitive.
    fn write_mixedcase_int_parquet_dir() -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "oxidant-mixedcase-{}-{:?}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();

        let schema32 = Arc::new(Schema::new(vec![Field::new(
            "VendorID",
            DataType::Int32,
            true,
        )]));
        let batch32 = RecordBatch::try_new(
            schema32.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let f = std::fs::File::create(dir.join("part-a.parquet")).unwrap();
        let mut w = ArrowWriter::try_new(f, schema32, None).unwrap();
        w.write(&batch32).unwrap();
        w.close().unwrap();

        let schema64 = Arc::new(Schema::new(vec![Field::new(
            "VendorID",
            DataType::Int64,
            true,
        )]));
        let batch64 = RecordBatch::try_new(
            schema64.clone(),
            vec![Arc::new(Int64Array::from(vec![10, 20]))],
        )
        .unwrap();
        let f = std::fs::File::create(dir.join("part-b.parquet")).unwrap();
        let mut w = ArrowWriter::try_new(f, schema64, None).unwrap();
        w.write(&batch64).unwrap();
        w.close().unwrap();

        dir
    }

    /// Databricks/Athena parity: a lowercase catalog column (`vendorid`) binds to the mixed-case
    /// file column (`VendorID`) case-insensitively, *and* the Int32 file is cast to the declared
    /// Int64 — so `SUM(vendorid)` returns the correct non-null total instead of NULL.
    #[tokio::test]
    async fn declared_schema_matches_columns_case_insensitively() {
        let dir = write_mixedcase_int_parquet_dir();
        let location = format!("file://{}", dir.to_string_lossy());
        // Glue-style lowercase declared name, widened to Int64.
        let declared = Arc::new(Schema::new(vec![Field::new(
            "vendorid",
            DataType::Int64,
            true,
        )]));

        let engine = crate::Engine::new();
        engine.register_catalog(
            "fake",
            Arc::new(SchemaCatalog {
                location,
                schema: Some(declared),
            }),
        );

        let batches = engine
            .sql("SELECT COUNT(vendorid) AS c, SUM(vendorid) AS s FROM fake.ns.mixed")
            .await
            .expect("case-insensitive declared-schema query should succeed");
        let c = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        let s = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        // All 5 rows resolved (not NULL) and summed across both physical types.
        assert_eq!((c, s), (5, 36)); // 1+2+3+10+20
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Regression test for the bug a code review caught: DataFusion's object-store registry is
    /// keyed purely by bucket, so registering a second, differently-configured (or unconfigured)
    /// identity for a bucket that already has one registered must be rejected, not silently
    /// accepted with the FIRST table's identity — otherwise two tables in one bucket with
    /// different `fs.s3a.assumed.role.arn` values would silently share whichever one resolved
    /// first, a real cross-permission-boundary bug.
    #[tokio::test]
    async fn ensure_remote_store_rejects_mismatched_role_for_already_registered_bucket() {
        use datafusion::datasource::listing::ListingTableUrl;

        let ctx = SessionContext::new();
        let state = ctx.state();
        // Bucket name unique to this test — REGISTERED_BUCKET_ROLES is a process-wide static
        // shared across every test in this binary, so reusing a name any other test touches would
        // make this test's outcome depend on test execution order.
        let url =
            ListingTableUrl::parse("s3://oxidant-loom-test-mismatch-bucket/table-a/").unwrap();

        let mut opts_role_a = HashMap::new();
        opts_role_a.insert(
            crate::assume_role_credentials::ASSUMED_ROLE_ARN_KEY.to_string(),
            "arn:aws:iam::123456789012:role/oxidant-poolctl/role-a".to_string(),
        );
        ensure_remote_store(&state, &url, Some(&opts_role_a))
            .expect("first registration for a fresh bucket must succeed");

        // Same bucket, different assumed role — DataFusion has no way to honor both
        // simultaneously, so this must be a loud error, not a silent reuse of role-a's identity.
        let mut opts_role_b = HashMap::new();
        opts_role_b.insert(
            crate::assume_role_credentials::ASSUMED_ROLE_ARN_KEY.to_string(),
            "arn:aws:iam::123456789012:role/oxidant-poolctl/role-b".to_string(),
        );
        let err = ensure_remote_store(&state, &url, Some(&opts_role_b))
            .expect_err("a second, conflicting role for the same bucket must be rejected");
        let msg = format!("{err}");
        assert!(
            msg.contains("role-a"),
            "error should name the already-registered role: {msg}"
        );
        assert!(
            msg.contains("role-b"),
            "error should name the conflicting requested role: {msg}"
        );

        // Same bucket, same role again — must succeed (idempotent, not just "first wins").
        ensure_remote_store(&state, &url, Some(&opts_role_a))
            .expect("re-requesting the SAME role for an already-registered bucket must succeed");

        // Same bucket, no role requested this time (e.g. a second table with no assume-role
        // config) — also a mismatch against the registered role-a identity, must be rejected.
        let err = ensure_remote_store(&state, &url, None).expect_err(
            "no-role-requested must be rejected when the bucket is already role-scoped",
        );
        assert!(
            format!("{err}").contains("role-a"),
            "error should name the already-registered role"
        );
    }

    #[test]
    fn iceberg_path_matching_normalizes_s3a_and_relative_paths() {
        assert!(iceberg_paths_equal(
            "s3a://bucket/table/data/part.parquet",
            "s3://bucket/table/data/part.parquet"
        ));
        assert!(iceberg_paths_equal(
            "data/part.parquet",
            "s3://bucket/table/data/part.parquet"
        ));
        assert!(!iceberg_paths_equal(
            "data/other.parquet",
            "s3://bucket/table/data/part.parquet"
        ));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn lost_task_local_pin_fails_instead_of_resolving_latest() {
        let state = SessionContext::new().state();
        let metadata = TableMetadata::new(
            "fake.ns.orders",
            "file:///does/not/matter",
            TableFormat::Delta,
        );
        let pins = serde_json::json!({
            "fake.ns.orders": SnapshotIdentity::Delta { version: 7 }
        })
        .to_string();

        let result = with_lakehouse_snapshots(&pins, async move {
            let result = tokio::spawn(async move {
                metadata_to_provider(&state, &metadata, "orders", true).await
            })
            .await
            .expect("spawned resolver task");
            Ok(result)
        })
        .await
        .unwrap();
        let error = match result {
            Ok(_) => panic!("a spawned distributed resolution must not fall back to latest"),
            Err(error) => error,
        };
        assert!(
            error
                .to_string()
                .contains("outside its pinned snapshot scope"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn split_partition_schema_preserves_partition_order_and_drops_file_fields() {
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("region", DataType::Utf8, false),
            Field::new("amount", DataType::Int32, true),
            Field::new("year", DataType::Int32, false),
        ]));
        let (file_schema, part_cols) =
            split_partition_schema(&schema, &["year".into(), "region".into()]);

        assert_eq!(
            file_schema
                .fields()
                .iter()
                .map(|f| f.name().as_str())
                .collect::<Vec<_>>(),
            vec!["id", "amount"]
        );
        // Partition columns must follow the *declared partition order*, not schema field order.
        assert_eq!(
            part_cols
                .iter()
                .map(|(n, dt)| (n.as_str(), dt.clone()))
                .collect::<Vec<_>>(),
            vec![("year", DataType::Int32), ("region", DataType::Utf8),]
        );
    }

    #[test]
    fn split_partition_schema_skips_partition_names_missing_from_schema() {
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("region", DataType::Utf8, false),
        ]));
        let (file_schema, part_cols) =
            split_partition_schema(&schema, &["region".into(), "missing".into()]);

        assert_eq!(file_schema.fields().len(), 1);
        assert_eq!(file_schema.field(0).name(), "id");
        assert_eq!(part_cols.len(), 1);
        assert_eq!(part_cols[0].0, "region");
    }

    // ---- Parquet footer caching on the scan data path (KAN-2) -------------------

    /// Shared log of every `get_ranges(location, ranges)` call a [`CountingStore`] serves —
    /// the one object-store call parquet footer/metadata fetches AND data-page reads both
    /// funnel through (DF's metadata fetcher and the arrow row-group readers alike), so a
    /// test can count footer GETs vs data GETs separately.
    type GetRangeCalls = Arc<Mutex<Vec<(object_store::path::Path, Vec<std::ops::Range<u64>>)>>>;

    /// See [`GetRangeCalls`]. Delegates everything to the local filesystem.
    #[derive(Debug)]
    struct CountingStore {
        inner: object_store::local::LocalFileSystem,
        calls: GetRangeCalls,
    }

    impl CountingStore {
        fn new() -> (Self, GetRangeCalls) {
            let calls: GetRangeCalls = Arc::new(Mutex::new(Vec::new()));
            (
                Self {
                    inner: object_store::local::LocalFileSystem::new(),
                    calls: Arc::clone(&calls),
                },
                calls,
            )
        }
    }

    impl fmt::Display for CountingStore {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "CountingStore({})", self.inner)
        }
    }

    #[async_trait]
    impl object_store::ObjectStore for CountingStore {
        async fn put_opts(
            &self,
            location: &object_store::path::Path,
            payload: object_store::PutPayload,
            options: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            self.inner.put_opts(location, payload, options).await
        }

        async fn put_multipart_opts(
            &self,
            location: &object_store::path::Path,
            options: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(location, options).await
        }

        async fn get_opts(
            &self,
            location: &object_store::path::Path,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            self.inner.get_opts(location, options).await
        }

        async fn get_ranges(
            &self,
            location: &object_store::path::Path,
            ranges: &[std::ops::Range<u64>],
        ) -> object_store::Result<Vec<bytes::Bytes>> {
            self.calls
                .lock()
                .expect("counting store poisoned")
                .push((location.clone(), ranges.to_vec()));
            self.inner.get_ranges(location, ranges).await
        }

        fn delete_stream(
            &self,
            locations: futures::stream::BoxStream<
                'static,
                object_store::Result<object_store::path::Path>,
            >,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::path::Path>>
        {
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&object_store::path::Path>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&object_store::path::Path>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &object_store::path::Path,
            to: &object_store::path::Path,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    /// Drain the recorded `get_ranges` calls.
    fn take_get_range_calls(
        calls: &GetRangeCalls,
    ) -> Vec<(object_store::path::Path, Vec<std::ops::Range<u64>>)> {
        std::mem::take(&mut *calls.lock().expect("counting store poisoned"))
    }

    /// Split recorded calls into `(metadata, data)` counts against a file's layout: every
    /// metadata range (page index, footer, 8-byte tail, and whole-file/tail prefetches) ends
    /// past `boundary` — the start of the page-index region, or of the footer when the file
    /// has no index — while data-page ranges end at or before it.
    fn count_metadata_vs_data(
        calls: &[(object_store::path::Path, Vec<std::ops::Range<u64>>)],
        boundary: u64,
    ) -> (usize, usize) {
        let mut metadata = 0;
        let mut data = 0;
        for (_, ranges) in calls {
            if ranges.iter().all(|r| r.end > boundary) {
                metadata += 1;
            } else {
                data += 1;
            }
        }
        (metadata, data)
    }

    /// A parquet file's `(file_size, footer_start, page_index_start)`: `footer_start` is the
    /// offset of the thrift footer (from the 8-byte tail), `page_index_start` the lowest
    /// column/offset index offset when the writer emitted a page index.
    fn parquet_file_layout(path: &std::path::Path) -> (u64, u64, Option<u64>) {
        use datafusion::parquet::file::metadata::ParquetMetaDataReader;

        let bytes = std::fs::read(path).unwrap();
        let file_size = u64::try_from(bytes.len()).unwrap();
        let tail: [u8; 4] = bytes[bytes.len() - 8..bytes.len() - 4].try_into().unwrap();
        let footer_start = file_size - 8 - u64::from(u32::from_le_bytes(tail));
        let file = std::fs::File::open(path).unwrap();
        let metadata = ParquetMetaDataReader::new()
            .parse_and_finish(&file)
            .unwrap();
        let page_index_start = metadata
            .row_groups()
            .iter()
            .flat_map(|row_group| row_group.columns())
            .flat_map(|column| {
                [column.column_index_offset(), column.offset_index_offset()]
                    .into_iter()
                    .flatten()
            })
            .map(|offset| u64::try_from(offset).unwrap())
            .min();
        (file_size, footer_start, page_index_start)
    }

    /// The catalog scan's data path reads footers through the runtime's shared file-metadata
    /// cache: the first scan of a file pays exactly one cold footer fetch — SHARED with the
    /// `OXIDANT_PARQUET_SCAN_STATS` prefetch (two cache namespaces would double it) — and a
    /// second query over the same table pays none at all, while data pages are still re-read
    /// (only metadata is cached). Before the cached reader factory was attached, EVERY scan
    /// re-fetched every footer.
    #[tokio::test]
    async fn catalog_parquet_scan_caches_footer_across_queries() {
        // Pin the scan-stats gate ON for the whole test (serialized against the KAN-8 tests
        // flipping it): then the stats prefetch pays the cold fetch at `Optional` page-index
        // policy, and the data path must hit the same cache entry.
        let _env = crate::tests::JOIN_GUARD_ENV_LOCK.lock().await;
        std::env::set_var("OXIDANT_PARQUET_SCAN_STATS", "1");

        let dir = write_parquet_dir(); // part-0.parquet: x in [1, 2, 3, 4]
        let (_, footer_start, page_index_start) = parquet_file_layout(&dir.join("part-0.parquet"));
        let boundary = page_index_start.unwrap_or(footer_start);
        // The cold fetch goes through the stats prefetch, whose `ParquetFormat::default()`
        // carries DataFusion's own default `metadata_size_hint` of 512 KiB: for this ~500-byte
        // file the prefetch range is the whole file, so the tail probe, footer and page index
        // arrive in ONE ranged GET and land in the metadata cache. A data path on a SEPARATE
        // cache would pay its own fetch on top of this; every scan before the cached reader
        // factory was attached paid one of its own.
        let cold_gets = 1;

        let engine = crate::Engine::new();
        let (store, calls) = CountingStore::new();
        let os_url = datafusion::execution::object_store::ObjectStoreUrl::parse("file://").unwrap();
        engine
            .ctx
            .runtime_env()
            .register_object_store(os_url.as_ref(), Arc::new(store));
        engine.register_catalog(
            "fake",
            Arc::new(SchemaCatalog {
                location: format!("file://{}", dir.to_string_lossy()),
                // Declared schema: resolution must not infer (inference shares the same cache
                // and would pay the cold fetch before the scan even starts).
                schema: Some(Arc::new(Schema::new(vec![Field::new(
                    "x",
                    DataType::Int64,
                    false,
                )]))),
            }),
        );

        fn sum(batches: &[RecordBatch]) -> i64 {
            batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0)
        }
        let sql = "SELECT SUM(x) AS s FROM fake.ns.mixed";

        let _ = take_get_range_calls(&calls);
        let first = engine.sql(sql).await.unwrap();
        assert_eq!(sum(&first), 10);
        let (first_meta, first_data) =
            count_metadata_vs_data(&take_get_range_calls(&calls), boundary);
        assert_eq!(
            first_meta, cold_gets,
            "first scan must pay exactly one cold footer fetch for the file"
        );
        assert!(first_data > 0, "data pages are read on the first scan");

        let second = engine.sql(sql).await.unwrap();
        assert_eq!(sum(&second), 10);
        let (second_meta, second_data) =
            count_metadata_vs_data(&take_get_range_calls(&calls), boundary);
        assert_eq!(
            second_meta, 0,
            "second query must serve the footer from the metadata cache"
        );
        assert!(
            second_data > 0,
            "data pages are NOT cached — only the footer is"
        );

        std::env::remove_var("OXIDANT_PARQUET_SCAN_STATS");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// `metadata_size_hint` reaches the cached reader factory the way stock DF wires it: with
    /// the hint covering the footer, the cold fetch is ONE ranged GET instead of two (the
    /// 8-byte tail probe plus the footer) — the control with the hint explicitly disabled pays
    /// both — and a re-scan stays at zero either way. (DataFusion's own session default for
    /// the hint is 512 KiB; `None` here only isolates the decoder's two-fetch behavior.)
    #[tokio::test]
    async fn metadata_size_hint_collapses_cold_footer_fetch() {
        // Stats prefetch OFF so the DATA path pays the cold fetch — the only configuration
        // where the hint's single-GET prefetch is observable (with stats on, the prefetch
        // warms the cache before the data path reads).
        let _env = crate::tests::JOIN_GUARD_ENV_LOCK.lock().await;
        std::env::set_var("OXIDANT_PARQUET_SCAN_STATS", "0");

        let dir = write_parquet_dir();
        let part = dir.join("part-0.parquet");
        let (file_size, footer_start, page_index_start) = parquet_file_layout(&part);
        let boundary = page_index_start.unwrap_or(footer_start);
        let location =
            crate::shard::ensure_collection_url(&format!("file://{}", dir.to_string_lossy()));
        let md = TableMetadata::new("fake.ns.mixed", location.clone(), TableFormat::Parquet)
            .with_schema(Arc::new(Schema::new(vec![Field::new(
                "x",
                DataType::Int64,
                false,
            )])));

        async fn scan_twice(
            hint: Option<usize>,
            location: &str,
            md: &TableMetadata,
            boundary: u64,
        ) -> ((usize, usize), (usize, usize)) {
            let mut config = datafusion::prelude::SessionConfig::new();
            config.options_mut().execution.parquet.metadata_size_hint = hint;
            let ctx = SessionContext::new_with_config(config);
            let (store, calls) = CountingStore::new();
            let os_url =
                datafusion::execution::object_store::ObjectStoreUrl::parse("file://").unwrap();
            ctx.runtime_env()
                .register_object_store(os_url.as_ref(), Arc::new(store));
            let state = ctx.state();
            let root = datafusion::datasource::listing::ListingTableUrl::parse(location).unwrap();
            let provider =
                parquet_metadata_provider_with_assignment(&state, md, "mixed", vec![root], None)
                    .await
                    .unwrap();

            let mut outcomes = Vec::new();
            for _ in 0..2 {
                let plan = provider.scan(&state, None, &[], None).await.unwrap();
                let batches = datafusion::physical_plan::collect(plan, ctx.task_ctx())
                    .await
                    .unwrap();
                assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 4);
                outcomes.push(count_metadata_vs_data(
                    &take_get_range_calls(&calls),
                    boundary,
                ));
            }
            (outcomes[0], outcomes[1])
        }

        // Control, hint explicitly disabled: the cold data-path fetch is the 8-byte tail
        // probe plus the footer (page-index policy `Skip` — the page index is never fetched
        // on this path).
        let (control_first, control_second) = scan_twice(None, &location, &md, boundary).await;
        assert_eq!(control_first.0, 2, "no hint: tail probe + footer");
        assert!(control_first.1 > 0, "data pages are read");
        assert_eq!(control_second.0, 0, "warm cache: no footer fetch");
        assert!(control_second.1 > 0, "data pages are re-read");

        // Hint sized to exactly cover the footer: the prefetch range IS the footer, so the
        // tail probe is folded in — one ranged GET, not two.
        let hint = usize::try_from(file_size - footer_start).unwrap();
        let (hinted_first, hinted_second) = scan_twice(Some(hint), &location, &md, boundary).await;
        assert_eq!(
            hinted_first.0, 1,
            "a hint covering the footer collapses the cold fetch to one GET"
        );
        assert!(hinted_first.1 > 0);
        assert_eq!(hinted_second.0, 0, "warm cache: no footer fetch");
        assert!(hinted_second.1 > 0);

        std::env::remove_var("OXIDANT_PARQUET_SCAN_STATS");
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ---- catalog cache TTL revalidation + eviction --------------------------

    /// Serializes the TTL tests: `OXIDANT_CATALOG_CACHE_TTL_MS` is process-global and each
    /// test's assertions depend on the value it set. An async mutex so the guard can be held
    /// across the tests' `.await` points (clippy `await_holding_lock`).
    static TTL_ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    /// A fake catalog whose single table `ns.orders` can be re-pointed at a new location (or
    /// re-declared with a new schema) between resolutions, counting `load_table` calls — the
    /// levers for the TTL-revalidation tests.
    struct MutableCatalog {
        state: Mutex<MutableCatalogState>,
    }

    struct MutableCatalogState {
        location: String,
        schema: Option<SchemaRef>,
        loads: usize,
        /// When set, the next `load_table` fails with a transient (non-`Plan`) error — the
        /// Glue-throttling lever for the fail-open revalidation test.
        fail_next: bool,
        /// When set, the table has vanished from the metastore (`load_table` answers
        /// `Error::Plan`) — the lever for the table-vanished revalidation test.
        dropped: bool,
    }

    impl MutableCatalog {
        fn new(location: String) -> Self {
            Self {
                state: Mutex::new(MutableCatalogState {
                    location,
                    schema: None,
                    loads: 0,
                    fail_next: false,
                    dropped: false,
                }),
            }
        }
        fn set_location(&self, location: String) {
            self.state.lock().unwrap().location = location;
        }
        fn set_fail_next(&self) {
            self.state.lock().unwrap().fail_next = true;
        }
        fn set_dropped(&self, dropped: bool) {
            self.state.lock().unwrap().dropped = dropped;
        }
        fn loads(&self) -> usize {
            self.state.lock().unwrap().loads
        }
    }

    #[async_trait]
    impl OxidantCatalog for MutableCatalog {
        fn name(&self) -> &str {
            "mutable"
        }
        async fn list_namespaces(&self, _parent: &[String]) -> CatResult<Vec<Vec<String>>> {
            Ok(vec![vec!["ns".to_string()]])
        }
        async fn list_tables(&self, _ns: &[String]) -> CatResult<Vec<String>> {
            Ok(vec!["orders".to_string()])
        }
        async fn load_table(&self, ns: &[String], table: &str) -> CatResult<TableMetadata> {
            if ns == ["ns"] && table == "orders" {
                let mut state = self.state.lock().unwrap();
                state.loads += 1;
                if state.dropped {
                    return Err(Error::Plan("no such table: ns.orders".to_string()));
                }
                if state.fail_next {
                    state.fail_next = false;
                    return Err(Error::Io("simulated Glue throttling".to_string()));
                }
                let md = TableMetadata::new(
                    "mutable.ns.orders",
                    state.location.clone(),
                    TableFormat::Parquet,
                );
                Ok(match &state.schema {
                    Some(s) => md.with_schema(s.clone()),
                    None => md,
                })
            } else {
                Err(Error::Plan(format!(
                    "no such table: {}.{table}",
                    ns.join(".")
                )))
            }
        }
    }

    /// Build a schema provider over `catalog` with a private catalog-version counter.
    fn ttl_test_provider(catalog: Arc<MutableCatalog>) -> (OxidantSchemaProvider, Arc<AtomicU64>) {
        let version = Arc::new(AtomicU64::new(0));
        let provider = OxidantSchemaProvider::new(
            catalog,
            vec!["ns".to_string()],
            Arc::new(SessionContext::new()),
            Arc::new(AtomicBool::new(false)),
            version.clone(),
            PendingCtas::default(),
        );
        (provider, version)
    }

    /// (a) Within the TTL a cache hit serves the cached provider — the metastore is not
    /// re-read even if the table moved underneath.
    #[tokio::test]
    async fn within_ttl_serves_cached_provider_without_reload() {
        let _guard = TTL_ENV_LOCK.lock().await;
        std::env::set_var("OXIDANT_CATALOG_CACHE_TTL_MS", "3600000");
        let dir = write_parquet_dir();
        let catalog = Arc::new(MutableCatalog::new(format!(
            "file://{}",
            dir.to_string_lossy()
        )));
        let (provider, _version) = ttl_test_provider(catalog.clone());

        let first = provider.table("orders").await.unwrap().unwrap();
        assert_eq!(catalog.loads(), 1);

        let dir2 = write_parquet_dir();
        catalog.set_location(format!("file://{}", dir2.to_string_lossy()));
        let second = provider.table("orders").await.unwrap().unwrap();
        assert_eq!(
            catalog.loads(),
            1,
            "within the TTL the metastore is not re-read"
        );
        assert!(
            Arc::ptr_eq(&first, &second),
            "cached provider is served as-is"
        );

        std::env::remove_var("OXIDANT_CATALOG_CACHE_TTL_MS");
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&dir2);
    }

    /// (b) Past the TTL with unchanged metadata, the cached provider is kept (no rebuild) and
    /// `resolved_at` restarts the TTL window — proven by an immediate third call NOT
    /// revalidating (a stale `resolved_at` would revalidate again).
    #[tokio::test]
    async fn past_ttl_unchanged_metadata_keeps_provider_and_refreshes_window() {
        let _guard = TTL_ENV_LOCK.lock().await;
        std::env::set_var("OXIDANT_CATALOG_CACHE_TTL_MS", "1000");
        let dir = write_parquet_dir();
        let catalog = Arc::new(MutableCatalog::new(format!(
            "file://{}",
            dir.to_string_lossy()
        )));
        let (provider, version) = ttl_test_provider(catalog.clone());

        let first = provider.table("orders").await.unwrap().unwrap();
        assert_eq!(catalog.loads(), 1);

        // Expire the TTL window, then resolve again: revalidation happens, the fingerprint is
        // unchanged, so the SAME provider is returned and no version bump occurs.
        tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
        let second = provider.table("orders").await.unwrap().unwrap();
        assert_eq!(catalog.loads(), 2, "past the TTL the metastore is re-read");
        assert!(
            Arc::ptr_eq(&first, &second),
            "unchanged metadata keeps the cached provider"
        );
        assert_eq!(
            version.load(Ordering::Relaxed),
            0,
            "no change, no version bump"
        );

        // Immediately after: the refreshed window means no second revalidation.
        let third = provider.table("orders").await.unwrap().unwrap();
        assert_eq!(
            catalog.loads(),
            2,
            "resolved_at refreshed — an immediate re-resolve stays cached"
        );
        assert!(Arc::ptr_eq(&first, &third));

        std::env::remove_var("OXIDANT_CATALOG_CACHE_TTL_MS");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// (c) Past the TTL with CHANGED metadata (new location), the provider is rebuilt from the
    /// fresh metadata and the shared catalog version is bumped (so cached stage plans miss).
    #[tokio::test]
    async fn past_ttl_changed_metadata_re_resolves_and_bumps_version() {
        let _guard = TTL_ENV_LOCK.lock().await;
        std::env::set_var("OXIDANT_CATALOG_CACHE_TTL_MS", "0");
        let dir = write_parquet_dir();
        let dir2 = write_parquet_dir();
        let catalog = Arc::new(MutableCatalog::new(format!(
            "file://{}",
            dir.to_string_lossy()
        )));
        let (provider, version) = ttl_test_provider(catalog.clone());

        let first = provider.table("orders").await.unwrap().unwrap();
        assert_eq!(catalog.loads(), 1);
        assert_eq!(version.load(Ordering::Relaxed), 0);

        catalog.set_location(format!("file://{}", dir2.to_string_lossy()));
        let second = provider.table("orders").await.unwrap().unwrap();
        assert_eq!(
            catalog.loads(),
            2,
            "TTL=0 revalidates on every table() call"
        );
        assert!(
            !Arc::ptr_eq(&first, &second),
            "changed metadata rebuilds the provider"
        );
        assert_eq!(
            version.load(Ordering::Relaxed),
            1,
            "a detected change bumps the catalog version"
        );

        // Steady state again: the new fingerprint is cached, so the next revalidation keeps it.
        let third = provider.table("orders").await.unwrap().unwrap();
        assert_eq!(catalog.loads(), 3);
        assert!(Arc::ptr_eq(&second, &third));
        assert_eq!(version.load(Ordering::Relaxed), 1);

        std::env::remove_var("OXIDANT_CATALOG_CACHE_TTL_MS");
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&dir2);
    }

    /// (d) `evict_table` drops the cached entry outright (even inside the TTL window), so the
    /// next `table()` re-resolves from the metastore.
    #[tokio::test]
    async fn evict_table_forces_re_resolution() {
        let _guard = TTL_ENV_LOCK.lock().await;
        // A huge TTL proves eviction, not TTL expiry, drives the re-resolution.
        std::env::set_var("OXIDANT_CATALOG_CACHE_TTL_MS", "3600000");
        let dir = write_parquet_dir();
        let dir2 = write_parquet_dir();
        let catalog = Arc::new(MutableCatalog::new(format!(
            "file://{}",
            dir.to_string_lossy()
        )));
        let (provider, _version) = ttl_test_provider(catalog.clone());

        let first = provider.table("orders").await.unwrap().unwrap();
        assert_eq!(catalog.loads(), 1);

        assert!(provider.evict_table("orders"), "cached entry evicted");
        assert!(!provider.evict_table("orders"), "nothing left to evict");

        catalog.set_location(format!("file://{}", dir2.to_string_lossy()));
        let second = provider.table("orders").await.unwrap().unwrap();
        assert_eq!(catalog.loads(), 2, "eviction forces a fresh metastore read");
        assert!(!Arc::ptr_eq(&first, &second));

        std::env::remove_var("OXIDANT_CATALOG_CACHE_TTL_MS");
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&dir2);
    }

    /// (e) Fail-open: past the TTL, a TRANSIENT revalidation failure (Glue throttling — a
    /// non-`Plan` error) serves the cached provider instead of failing the query, does NOT bump
    /// the version, and restarts the TTL window (an immediate follow-up call must not re-hit
    /// the metastore — that would amplify throttling into a per-query retry storm).
    #[tokio::test]
    async fn past_ttl_revalidation_error_serves_cached_and_defers_retry() {
        let _guard = TTL_ENV_LOCK.lock().await;
        std::env::set_var("OXIDANT_CATALOG_CACHE_TTL_MS", "1000");
        let dir = write_parquet_dir();
        let catalog = Arc::new(MutableCatalog::new(format!(
            "file://{}",
            dir.to_string_lossy()
        )));
        let (provider, version) = ttl_test_provider(catalog.clone());

        let first = provider.table("orders").await.unwrap().unwrap();
        assert_eq!(catalog.loads(), 1);

        // Expire the TTL window, then make the revalidation fail transiently: the cached
        // provider is served and no version bump occurs.
        tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
        catalog.set_fail_next();
        let second = provider.table("orders").await.unwrap().unwrap();
        assert_eq!(catalog.loads(), 2, "past the TTL the metastore is re-read");
        assert!(
            Arc::ptr_eq(&first, &second),
            "a transient revalidation failure serves the cached provider"
        );
        assert_eq!(
            version.load(Ordering::Relaxed),
            0,
            "a failed revalidation is not a detected change"
        );

        // The fail-open arm restarts the TTL window: an immediate follow-up stays cached
        // instead of re-hitting the (throttling) metastore.
        let third = provider.table("orders").await.unwrap().unwrap();
        assert_eq!(
            catalog.loads(),
            2,
            "fail-open refreshes resolved_at — no immediate retry amplification"
        );
        assert!(Arc::ptr_eq(&first, &third));

        std::env::remove_var("OXIDANT_CATALOG_CACHE_TTL_MS");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// (f) Table vanished: past the TTL, a `Plan` ("no such table") revalidation result drops
    /// the stale entry, bumps the catalog version (cached stage plans must not reference a
    /// dropped table), and resolves to DataFusion's standard not-found path (`Ok(None)`). A
    /// later re-creation resolves fresh.
    #[tokio::test]
    async fn past_ttl_vanished_table_evicts_and_bumps_version() {
        let _guard = TTL_ENV_LOCK.lock().await;
        std::env::set_var("OXIDANT_CATALOG_CACHE_TTL_MS", "0");
        let dir = write_parquet_dir();
        let catalog = Arc::new(MutableCatalog::new(format!(
            "file://{}",
            dir.to_string_lossy()
        )));
        let (provider, version) = ttl_test_provider(catalog.clone());

        let first = provider.table("orders").await.unwrap().unwrap();
        assert_eq!(catalog.loads(), 1);
        assert_eq!(version.load(Ordering::Relaxed), 0);

        catalog.set_dropped(true);
        let gone = provider.table("orders").await.unwrap();
        assert_eq!(
            catalog.loads(),
            2,
            "TTL=0 revalidates on every table() call"
        );
        assert!(gone.is_none(), "a vanished table resolves to not-found");
        assert_eq!(
            version.load(Ordering::Relaxed),
            1,
            "dropping the stale entry bumps the catalog version"
        );

        // Re-created in the metastore: the next call re-resolves from scratch (fresh load —
        // no version bump on the initial-load path).
        catalog.set_dropped(false);
        let recreated = provider.table("orders").await.unwrap().unwrap();
        assert_eq!(catalog.loads(), 3);
        assert!(!Arc::ptr_eq(&first, &recreated));
        assert_eq!(version.load(Ordering::Relaxed), 1);

        std::env::remove_var("OXIDANT_CATALOG_CACHE_TTL_MS");
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ---- KAN-143: parquet footer COLUMN statistics --------------------------
    //
    // `parquet_footer_file_groups` attaches per-column min/max, null counts and distinct
    // counts (where the footer carries them) on top of row/byte counts, gated by
    // `OXIDANT_PARQUET_COLUMN_STATS` (default ON) and per-file by `column_stats_trusted`.
    // The aggregate feeds DataFusion's join cardinality estimation; the per-file copy feeds
    // the parquet opener's constant-column proofs, which is exactly why mismatched-schema
    // files must be excluded.

    use datafusion::common::stats::Precision;
    use datafusion::common::ScalarValue;

    /// Serializes these tests against EVERY test in this binary that flips
    /// `OXIDANT_PARQUET_SCAN_STATS` / `OXIDANT_PARQUET_COLUMN_STATS` (the kill-switch test
    /// below, the KAN-8 cache tests here, and the lib.rs join-guard tests) — they all hold
    /// `JOIN_GUARD_ENV_LOCK` while the flag is non-default, so a stats assertion only races
    /// if it runs lock-FREE. Every test whose assertions depend on the flags' default state
    /// must therefore hold the lock for its whole body, not just the mutators.
    async fn stats_env_lock() -> tokio::sync::MutexGuard<'static, ()> {
        crate::tests::JOIN_GUARD_ENV_LOCK.lock().await
    }

    /// Write `batch` as a single parquet file in a fresh, process-unique temp dir; returns
    /// (dir, file path). Distinct dir per call (same pid+sequence pattern as
    /// `write_parquet_dir` — see its comment).
    fn write_single_parquet(
        name: &str,
        batch: &RecordBatch,
    ) -> (std::path::PathBuf, std::path::PathBuf) {
        use std::sync::atomic::{AtomicU64, Ordering};
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "oxidant-colstats-{}-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(name);
        let f = std::fs::File::create(&path).unwrap();
        let mut w = ArrowWriter::try_new(f, batch.schema(), None).unwrap();
        w.write(batch).unwrap();
        w.close().unwrap();
        (dir, path)
    }

    /// Run `parquet_footer_file_groups` over `paths` with a catalog-declared `file_schema`
    /// (no partition columns), against a plain local-filesystem session.
    async fn footer_stats_for(
        paths: &[&std::path::Path],
        file_schema: SchemaRef,
    ) -> (
        Vec<datafusion::datasource::physical_plan::FileGroup>,
        Option<datafusion::common::Statistics>,
    ) {
        let ctx = SessionContext::new();
        let state = ctx.state();
        let store_url =
            datafusion::execution::object_store::ObjectStoreUrl::parse("file://").unwrap();
        let table_schema =
            datafusion::datasource::table_schema::TableSchema::new(file_schema, vec![]);
        let files = paths
            .iter()
            .map(|path| {
                let meta = std::fs::metadata(path).unwrap();
                datafusion::datasource::listing::PartitionedFile::new_from_meta(
                    object_store::ObjectMeta {
                        location: object_store::path::Path::from_filesystem_path(path).unwrap(),
                        last_modified: meta.modified().map(chrono::DateTime::from).unwrap(),
                        size: meta.len(),
                        e_tag: None,
                        version: None,
                    },
                )
            })
            .collect::<Vec<_>>();
        parquet_footer_file_groups(&state, &store_url, &table_schema, &files).await
    }

    fn int64_batch(schema: &SchemaRef, a: Vec<i64>, b: Vec<Option<i64>>) -> RecordBatch {
        RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(a)), Arc::new(Int64Array::from(b))],
        )
        .unwrap()
    }

    /// Matching schemas: full column stats are attached at all three levels (per-file,
    /// per-group, table aggregate) and parquet-rs's unwritten NDVs stay `Absent` (the join
    /// estimator's NDV then falls back to the min/max range — the KAN-143 win).
    #[tokio::test]
    async fn footer_column_stats_attached_when_schema_matches() {
        let _env = stats_env_lock().await;
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, true),
        ]));
        let batch = int64_batch(
            &schema,
            vec![1, 2, 3, 8],
            vec![Some(10), None, Some(30), Some(20)],
        );
        let (dir, path) = write_single_parquet("part-0.parquet", &batch);

        let (groups, statistics) = footer_stats_for(&[&path], schema).await;
        let statistics = statistics.expect("statistics attached");
        assert_eq!(statistics.num_rows, Precision::Exact(4));
        assert_eq!(statistics.column_statistics.len(), 2);
        let a = &statistics.column_statistics[0];
        assert_eq!(a.min_value, Precision::Exact(ScalarValue::Int64(Some(1))));
        assert_eq!(a.max_value, Precision::Exact(ScalarValue::Int64(Some(8))));
        assert_eq!(a.null_count, Precision::Exact(0));
        // parquet-rs never writes distinct counts; they must stay Absent, never invented.
        assert_eq!(a.distinct_count, Precision::Absent);
        let b = &statistics.column_statistics[1];
        assert_eq!(b.null_count, Precision::Exact(1));
        assert_eq!(b.min_value, Precision::Exact(ScalarValue::Int64(Some(10))));
        assert_eq!(b.max_value, Precision::Exact(ScalarValue::Int64(Some(30))));

        // Per-file statistics (what the parquet opener's constant-column proofs read) carry
        // the same column stats in the file-schema shape.
        let file_stats = groups[0]
            .file_statistics(Some(0))
            .expect("per-file statistics");
        assert_eq!(file_stats.num_rows, Precision::Exact(4));
        assert_eq!(
            file_stats.column_statistics[0].max_value,
            Precision::Exact(ScalarValue::Int64(Some(8)))
        );
        // Group-level statistics (what `partition_statistics(Some(p))` reads) too.
        let group_stats = groups[0].file_statistics(None).expect("group statistics");
        assert_eq!(group_stats.column_statistics.len(), 2);
        assert_eq!(
            group_stats.column_statistics[1].null_count,
            Precision::Exact(1)
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Two files: the table aggregate MIN/MAX spans both files, null counts SUM exactly, and
    // NDVs stay `Absent` (parquet-rs writes none) rather than a wrong sum.
    #[tokio::test]
    async fn footer_column_stats_aggregate_across_files() {
        let _env = stats_env_lock().await;
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, true),
        ]));
        let batch1 = int64_batch(
            &schema,
            vec![1, 2, 3, 8],
            vec![Some(10), None, None, Some(20)],
        );
        let batch2 = int64_batch(
            &schema,
            vec![5, 6, 7, 40],
            vec![None, Some(5), Some(15), Some(25)],
        );
        let (dir, path1) = write_single_parquet("part-0.parquet", &batch1);
        let path2 = dir.join("part-1.parquet");
        let f = std::fs::File::create(&path2).unwrap();
        let mut w = ArrowWriter::try_new(f, schema.clone(), None).unwrap();
        w.write(&batch2).unwrap();
        w.close().unwrap();

        let (_, statistics) = footer_stats_for(&[&path1, &path2], schema).await;
        let statistics = statistics.expect("statistics attached");
        assert_eq!(statistics.num_rows, Precision::Exact(8));
        let a = &statistics.column_statistics[0];
        assert_eq!(a.min_value, Precision::Exact(ScalarValue::Int64(Some(1))));
        assert_eq!(a.max_value, Precision::Exact(ScalarValue::Int64(Some(40))));
        assert_eq!(a.distinct_count, Precision::Absent);
        let b = &statistics.column_statistics[1];
        assert_eq!(b.null_count, Precision::Exact(3));
        assert_eq!(b.min_value, Precision::Exact(ScalarValue::Int64(Some(5))));
        assert_eq!(b.max_value, Precision::Exact(ScalarValue::Int64(Some(25))));
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The KAN-143 guard: the declared schema's column case differs from the file's — the
    /// gap `CaseInsensitiveExprAdapterFactory` bridges at READ time. DataFusion's stats
    /// converter resolves columns by exact name and stamps `null_count == num_rows` for the
    /// "missing" column; attaching that would let the opener literal-replace real data with
    /// NULL. Column stats must be dropped (row counts kept) at every level.
    #[tokio::test]
    async fn footer_column_stats_dropped_on_declared_case_mismatch() {
        let _env = stats_env_lock().await;
        let physical = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            physical.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3, 4]))],
        )
        .unwrap();
        let (dir, path) = write_single_parquet("part-0.parquet", &batch);
        let declared = Arc::new(Schema::new(vec![Field::new("X", DataType::Int64, false)]));

        let (groups, statistics) = footer_stats_for(&[&path], declared).await;
        let statistics = statistics.expect("row counts still attached");
        assert_eq!(statistics.num_rows, Precision::Exact(4));
        assert_eq!(statistics.column_statistics.len(), 1);
        assert_eq!(
            statistics.column_statistics[0].null_count,
            Precision::Absent
        );
        assert_eq!(statistics.column_statistics[0].min_value, Precision::Absent);
        let file_stats = groups[0]
            .file_statistics(Some(0))
            .expect("per-file statistics");
        assert_eq!(file_stats.num_rows, Precision::Exact(4));
        assert_eq!(file_stats.column_statistics[0].max_value, Precision::Absent);
        assert_eq!(
            groups[0].file_statistics(None).unwrap().column_statistics[0].min_value,
            Precision::Absent
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// End-to-end proof the guard protects the read path: with column stats enabled, the
    /// mixed-case declared-schema query still decodes REAL values — no all-null literal
    /// rewrite. (The pre-KAN-143 unconditional drop made this trivially true; the guard
    /// keeps it true now that stats are attached.)
    #[tokio::test]
    async fn footer_column_stats_case_mismatch_still_reads_real_data() {
        let dir = write_mixedcase_int_parquet_dir(); // files: `VendorID`, 1+2+3 and 10+20
        let location = format!("file://{}", dir.to_string_lossy());
        let declared = Arc::new(Schema::new(vec![Field::new(
            "vendorid",
            DataType::Int64,
            true,
        )]));

        let engine = crate::Engine::new();
        engine.register_catalog(
            "fake",
            Arc::new(SchemaCatalog {
                location,
                schema: Some(declared),
            }),
        );
        let batches = engine
            .sql("SELECT COUNT(vendorid) AS c, SUM(vendorid) AS s FROM fake.ns.mixed")
            .await
            .expect("case-insensitive declared-schema query should succeed");
        let c = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        let s = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(
            (c, s),
            (5, 36),
            "real data must survive statistics attachment"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A declared column genuinely ABSENT from the file (schema evolution) is NOT a
    /// mismatch: the reader fills it with NULLs, so the footer-derived `null_count ==
    /// num_rows` is the truth and the stats stay attached.
    #[tokio::test]
    async fn footer_column_stats_genuinely_absent_column_kept() {
        let _env = stats_env_lock().await;
        let physical = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            physical.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3, 4]))],
        )
        .unwrap();
        let (dir, path) = write_single_parquet("part-0.parquet", &batch);
        let declared = Arc::new(Schema::new(vec![
            Field::new("x", DataType::Int64, true),
            Field::new("y", DataType::Int64, true),
        ]));

        let (_, statistics) = footer_stats_for(&[&path], declared).await;
        let statistics = statistics.expect("statistics attached");
        let x = &statistics.column_statistics[0];
        assert_eq!(x.min_value, Precision::Exact(ScalarValue::Int64(Some(1))));
        assert_eq!(x.max_value, Precision::Exact(ScalarValue::Int64(Some(4))));
        let y = &statistics.column_statistics[1];
        assert_eq!(y.null_count, Precision::Exact(4));
        // DataFusion surfaces the missing column's min/max as Exact NULL scalars (its
        // "all-null" representation), never a concrete value.
        assert!(matches!(
            y.min_value.get_value(),
            Some(value) if value.is_null()
        ));
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A 0-row file has no row groups and no column statistics in its footer. It must not
    /// poison the table aggregate: min/max still come from the non-empty file, row counts
    /// stay exact, and nothing panics.
    #[tokio::test]
    async fn footer_column_stats_row_group_less_file_does_not_poison_aggregate() {
        let _env = stats_env_lock().await;
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, true),
        ]));
        let (dir, empty_path) =
            write_single_parquet("part-0.parquet", &int64_batch(&schema, vec![], vec![]));
        let full_batch = int64_batch(
            &schema,
            vec![1, 2, 3, 8],
            vec![Some(10), None, Some(30), Some(20)],
        );
        let full_path = dir.join("part-1.parquet");
        let f = std::fs::File::create(&full_path).unwrap();
        let mut w = ArrowWriter::try_new(f, schema.clone(), None).unwrap();
        w.write(&full_batch).unwrap();
        w.close().unwrap();

        let (_, statistics) = footer_stats_for(&[&empty_path, &full_path], schema).await;
        let statistics = statistics.expect("statistics attached");
        assert_eq!(statistics.num_rows, Precision::Exact(4));
        let a = &statistics.column_statistics[0];
        assert_eq!(a.min_value, Precision::Exact(ScalarValue::Int64(Some(1))));
        assert_eq!(a.max_value, Precision::Exact(ScalarValue::Int64(Some(8))));
        assert_eq!(
            statistics.column_statistics[1].null_count,
            Precision::Exact(1)
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The scan-level statistics DataFusion's join estimation actually consumes are the
    /// PROJECTED ones: projecting a subset of columns must project the column statistics
    /// along (right values at the right positions), and projected-away columns must vanish.
    #[tokio::test]
    async fn footer_column_stats_follow_projection() {
        let _env = stats_env_lock().await;
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
            Field::new("c", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(Int64Array::from(vec![100, 200])),
                Arc::new(Int64Array::from(vec![7, 9])),
            ],
        )
        .unwrap();
        let (dir, _path) = write_single_parquet("part-0.parquet", &batch);
        let location =
            crate::shard::ensure_collection_url(&format!("file://{}", dir.to_string_lossy()));
        let md = TableMetadata::new("fake.ns.mixed", location.clone(), TableFormat::Parquet)
            .with_schema(schema);

        let ctx = SessionContext::new();
        let state = ctx.state();
        let root = datafusion::datasource::listing::ListingTableUrl::parse(&location).unwrap();
        let provider =
            parquet_metadata_provider_with_assignment(&state, &md, "mixed", vec![root], None)
                .await
                .unwrap();

        // Project ONLY `c` (table index 2): the projected scan statistics must carry c's
        // min/max at position 0 and nothing else.
        let plan = provider
            .scan(&state, Some(&vec![2]), &[], None)
            .await
            .unwrap();
        fn find_scan(
            plan: &Arc<dyn datafusion::physical_plan::ExecutionPlan>,
        ) -> Option<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
            if plan.name() == "DataSourceExec" {
                return Some(Arc::clone(plan));
            }
            for child in plan.children() {
                if let Some(found) = find_scan(child) {
                    return Some(found);
                }
            }
            None
        }
        let scan = find_scan(&plan).expect("a DataSourceExec in the scan plan");
        let stats = scan.partition_statistics(None).unwrap();
        assert_eq!(stats.num_rows, Precision::Exact(2));
        assert_eq!(stats.column_statistics.len(), 1);
        assert_eq!(
            stats.column_statistics[0].min_value,
            Precision::Exact(ScalarValue::Int64(Some(7)))
        );
        assert_eq!(
            stats.column_statistics[0].max_value,
            Precision::Exact(ScalarValue::Int64(Some(9)))
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Kill switches: `OXIDANT_PARQUET_COLUMN_STATS=0` restores the row-counts-only shape
    /// (the pre-KAN-143 behavior) while keeping footer counts; `OXIDANT_PARQUET_SCAN_STATS=0`
    /// still disables footer reads entirely.
    #[tokio::test]
    async fn footer_column_stats_env_kill_switches() {
        let _env = stats_env_lock().await;
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3, 4]))],
        )
        .unwrap();
        let (dir, path) = write_single_parquet("part-0.parquet", &batch);

        std::env::set_var("OXIDANT_PARQUET_COLUMN_STATS", "0");
        let (groups, statistics) = footer_stats_for(&[&path], schema.clone()).await;
        let statistics = statistics.expect("row counts still attached");
        assert_eq!(statistics.num_rows, Precision::Exact(4));
        assert_eq!(statistics.column_statistics[0].min_value, Precision::Absent);
        assert_eq!(
            groups[0]
                .file_statistics(Some(0))
                .unwrap()
                .column_statistics[0]
                .max_value,
            Precision::Absent
        );

        std::env::set_var("OXIDANT_PARQUET_SCAN_STATS", "0");
        let (groups, statistics) = footer_stats_for(&[&path], schema).await;
        assert!(
            statistics.is_none(),
            "scan-stats off: no aggregate statistics"
        );
        assert!(groups[0].file_statistics(Some(0)).is_none());

        std::env::remove_var("OXIDANT_PARQUET_COLUMN_STATS");
        std::env::remove_var("OXIDANT_PARQUET_SCAN_STATS");
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ---------------------------------------------------------------------
    // Fine-grained access control end to end: a catalog carrying a
    // `TableAuthorizer` must have its decision applied to real SQL, through
    // the bridge, against a real Parquet table.
    //
    // The fixture lives in its own namespace (`oxidant_lf_secure`) so no other
    // test in this file resolves through the authorizer path.
    // ---------------------------------------------------------------------

    /// Fixture table `(id, region, amount, ssn)`. `ssn` is the column-security subject; `region`
    /// the row-filter subject. 4 rows, 2 of them `region = 'us'`.
    fn write_lf_fixture() -> std::path::PathBuf {
        use datafusion::arrow::array::StringArray;
        use std::sync::atomic::{AtomicU64, Ordering};
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "oxidant-lf-{}-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("region", DataType::Utf8, false),
            Field::new("amount", DataType::Int64, false),
            Field::new("ssn", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
                Arc::new(StringArray::from(vec!["us", "eu", "us", "eu"])),
                Arc::new(Int64Array::from(vec![10, 20, 30, 40])),
                Arc::new(StringArray::from(vec!["aaa", "bbb", "ccc", "ddd"])),
            ],
        )
        .unwrap();
        let f = std::fs::File::create(dir.join("part-0.parquet")).unwrap();
        let mut w = ArrowWriter::try_new(f, schema, None).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
        dir
    }

    /// A catalog serving the fixture under `oxidant_lf_secure.lf_protected_customers`, with an
    /// authorizer whose decision the test controls.
    struct SecuredCatalog {
        location: String,
        authorizer: Arc<dyn oxidant_catalog::TableAuthorizer>,
    }

    #[async_trait]
    impl OxidantCatalog for SecuredCatalog {
        fn name(&self) -> &str {
            "lf"
        }
        fn authorizer(&self) -> Option<Arc<dyn oxidant_catalog::TableAuthorizer>> {
            Some(self.authorizer.clone())
        }
        async fn list_namespaces(&self, _parent: &[String]) -> CatResult<Vec<Vec<String>>> {
            Ok(vec![vec!["oxidant_lf_secure".to_string()]])
        }
        async fn list_tables(&self, _ns: &[String]) -> CatResult<Vec<String>> {
            Ok(vec!["lf_protected_customers".to_string()])
        }
        async fn load_table(&self, ns: &[String], table: &str) -> CatResult<TableMetadata> {
            if ns == ["oxidant_lf_secure"] && table == "lf_protected_customers" {
                Ok(TableMetadata::new(
                    "lf.oxidant_lf_secure.lf_protected_customers",
                    self.location.clone(),
                    TableFormat::Parquet,
                ))
            } else {
                Err(Error::Plan(format!(
                    "no such table: {}.{table}",
                    ns.join(".")
                )))
            }
        }
    }

    /// An authorizer returning a fixed decision, or a fixed failure.
    struct StaticAuthorizer {
        access: Option<oxidant_catalog::TableAccess>,
        principal: String,
    }

    #[async_trait]
    impl oxidant_catalog::TableAuthorizer for StaticAuthorizer {
        async fn authorize_scan(
            &self,
            _namespace: &[String],
            table: &str,
        ) -> CatResult<oxidant_catalog::TableAccess> {
            self.access.clone().ok_or_else(|| {
                Error::Io(format!(
                    "lake formation unreachable while authorizing {table}"
                ))
            })
        }
        fn principal(&self) -> &str {
            &self.principal
        }
    }

    fn secured_engine(
        access: Option<oxidant_catalog::TableAccess>,
    ) -> (crate::Engine, std::path::PathBuf) {
        let dir = write_lf_fixture();
        let engine = crate::Engine::new();
        engine.register_catalog(
            "lf",
            Arc::new(SecuredCatalog {
                location: format!("file://{}", dir.to_string_lossy()),
                authorizer: Arc::new(StaticAuthorizer {
                    access,
                    principal: "arn:aws:iam::123456789012:role/analyst".to_string(),
                }),
            }),
        );
        (engine, dir)
    }

    fn granted(columns: Option<&[&str]>, row_filter: Option<&str>) -> oxidant_catalog::TableAccess {
        oxidant_catalog::TableAccess {
            authorized_columns: columns.map(|c| c.iter().map(|s| s.to_string()).collect()),
            row_filter: row_filter.map(|s| s.to_string()),
            credentials: None,
            enforced: true,
        }
    }

    const LF_TABLE: &str = "lf.oxidant_lf_secure.lf_protected_customers";

    #[tokio::test]
    async fn secured_table_hides_denied_column_and_filters_rows_through_real_sql() {
        let (engine, dir) = secured_engine(Some(granted(
            Some(&["id", "region", "amount"]),
            Some("region = 'us'"),
        )));

        let batches = engine
            .sql(&format!("SELECT * FROM {LF_TABLE} ORDER BY id"))
            .await
            .expect("select star");
        let schema = batches[0].schema();
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, vec!["id", "region", "amount"], "ssn must be absent");
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 2, "only region='us' of the 4 fixture rows");

        // The denied column is not merely unselectable — it is not in the schema.
        let err = engine
            .sql(&format!("SELECT ssn FROM {LF_TABLE}"))
            .await
            .expect_err("ssn is denied");
        assert!(format!("{err}").contains("ssn"), "{err}");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn secured_table_count_reflects_the_row_filter() {
        let (engine, dir) = secured_engine(Some(granted(None, Some("region = 'us'"))));
        let batches = engine
            .sql(&format!("SELECT COUNT(*) AS c FROM {LF_TABLE}"))
            .await
            .expect("count");
        let c = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64")
            .value(0);
        assert_eq!(c, 2, "the raw Parquet table has 4 rows");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// An unreachable authorizer must fail the query. Falling back to an unfiltered scan would
    /// silently disable the control at exactly the moment it cannot be verified.
    #[tokio::test]
    async fn authorizer_failure_fails_the_query_instead_of_scanning_unfiltered() {
        let (engine, dir) = secured_engine(None);
        let err = engine
            .sql(&format!("SELECT * FROM {LF_TABLE}"))
            .await
            .expect_err("authorizer failure must not fall open");
        assert!(format!("{err}").contains("lake formation"), "{err}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A table Lake Formation does not govern reads exactly as it would with no authorizer — this
    /// is what lets enforcement be switched on for a catalog without changing behavior for the
    /// tables that are not registered with it.
    #[tokio::test]
    async fn ungoverned_table_reads_unrestricted() {
        let (engine, dir) = secured_engine(Some(oxidant_catalog::TableAccess::unenforced()));
        let batches = engine
            .sql(&format!("SELECT * FROM {LF_TABLE}"))
            .await
            .expect("unenforced");
        let schema = batches[0].schema();
        assert_eq!(schema.fields().len(), 4, "all columns including ssn");
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 4, "all rows");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// `DESCRIBE` must not name a column the principal cannot read — EMR's rule that DDL/EXPLAIN
    /// paths do not expose restricted information.
    #[tokio::test]
    async fn describe_does_not_expose_a_denied_column() {
        let (engine, dir) = secured_engine(Some(granted(Some(&["id", "region", "amount"]), None)));
        let batches = engine
            .sql(&format!("DESCRIBE {LF_TABLE}"))
            .await
            .expect("describe");
        let rendered = format!("{batches:?}");
        assert!(!rendered.contains("ssn"), "DESCRIBE leaked a denied column");
        assert!(rendered.contains("amount"), "{rendered}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The same for `EXPLAIN`: the plan text must not name a denied column.
    #[tokio::test]
    async fn explain_does_not_expose_a_denied_column() {
        let (engine, dir) = secured_engine(Some(granted(Some(&["id", "region", "amount"]), None)));
        let batches = engine
            .sql(&format!("EXPLAIN SELECT * FROM {LF_TABLE}"))
            .await
            .expect("explain");
        let rendered = format!("{batches:?}");
        assert!(!rendered.contains("ssn"), "EXPLAIN leaked a denied column");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Two principals must never share a cached provider — the cache key carries the principal
    /// precisely so one user's authorized column set is not served to another.
    #[tokio::test]
    async fn different_principals_do_not_share_a_cached_provider() {
        let dir = write_lf_fixture();
        let location = format!("file://{}", dir.to_string_lossy());

        let narrow = crate::Engine::new();
        narrow.register_catalog(
            "lf",
            Arc::new(SecuredCatalog {
                location: location.clone(),
                authorizer: Arc::new(StaticAuthorizer {
                    access: Some(granted(Some(&["id"]), None)),
                    principal: "arn:aws:iam::123456789012:role/narrow".to_string(),
                }),
            }),
        );
        let wide = crate::Engine::new();
        wide.register_catalog(
            "lf",
            Arc::new(SecuredCatalog {
                location,
                authorizer: Arc::new(StaticAuthorizer {
                    access: Some(granted(Some(&["id", "region", "amount", "ssn"]), None)),
                    principal: "arn:aws:iam::123456789012:role/wide".to_string(),
                }),
            }),
        );

        let n = narrow
            .sql(&format!("SELECT * FROM {LF_TABLE}"))
            .await
            .expect("narrow");
        assert_eq!(n[0].schema().fields().len(), 1);
        let w = wide
            .sql(&format!("SELECT * FROM {LF_TABLE}"))
            .await
            .expect("wide");
        assert_eq!(w[0].schema().fields().len(), 4);

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The distributed fail-open hole, closed: a worker whose catalog has no Lake Formation
    /// authorizer must refuse a governed table rather than read its shard unfiltered and hand the
    /// rows back to the driver. The worker cannot detect this itself — with no authorizer it has
    /// no way to know the table is governed — so the driver stamps the requirement on the ticket.
    #[tokio::test]
    async fn worker_without_an_authorizer_refuses_a_query_the_driver_marked_governed() {
        let dir = write_lf_fixture();
        // A catalog with NO authorizer: exactly what a worker started without the Lake Formation
        // config looks like.
        let engine = crate::Engine::new();
        engine.register_catalog(
            "lf",
            Arc::new(FakeCatalog {
                location: format!("file://{}", dir.to_string_lossy()),
            }),
        );

        // Without the driver's requirement this reads happily — today's behavior for an
        // ungoverned catalog, which must not change.
        let ok = engine.sql("SELECT * FROM lf.ns.orders").await;
        assert!(ok.is_ok(), "ungoverned read must still work: {ok:?}");

        // Under the requirement, the same read is refused.
        let err = with_lakeformation_required(
            true,
            String::new(),
            engine.sql("SELECT * FROM lf.ns.orders"),
        )
        .await
        .expect_err("a worker that cannot enforce must refuse, not read unfiltered");
        let msg = format!("{err}");
        assert!(msg.contains("no Lake Formation authorizer"), "{msg}");
        assert!(msg.contains("every worker"), "{msg}");

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The driver-side half: resolving a governed table must be *observed*, so the flag it stamps
    /// on the stage tickets is set. Without this the guard above would never engage.
    #[tokio::test]
    async fn resolving_a_governed_table_is_observed_for_the_stage_ticket() {
        let (engine, dir) = secured_engine(Some(granted(Some(&["id", "region"]), None)));
        let (_, required, _) = capture_lakeformation_enforcement(async {
            engine
                .sql(&format!("SELECT id FROM {LF_TABLE}"))
                .await
                .map(|_| ())
        })
        .await
        .expect("query");
        assert!(
            required,
            "a governed table must mark the query as requiring enforcement"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The routing store is unit-tested on its own; this covers the *wiring* — that resolving a
    /// governed S3 table actually installs its vended credentials against that table's prefix, and
    /// that the bucket's registered store is the router rather than the plain one. Without this,
    /// every read would silently fall through to the engine's ambient identity.
    #[tokio::test]
    async fn vended_credentials_are_installed_against_the_tables_prefix() {
        use datafusion::datasource::listing::ListingTableUrl;

        let access = oxidant_catalog::TableAccess {
            authorized_columns: Some(vec!["id".to_string()]),
            row_filter: None,
            credentials: Some(oxidant_catalog::VendedCredentials {
                access_key_id: "ASIAEXAMPLE".to_string(),
                secret_access_key: "secret".to_string(),
                session_token: Some("token".to_string()),
                expires_at: Some(
                    std::time::SystemTime::now() + std::time::Duration::from_secs(3600),
                ),
            }),
            enforced: true,
        };

        let engine = crate::Engine::new();
        let provider = OxidantSchemaProvider::new(
            Arc::new(SecuredCatalog {
                location: "s3://lf-test-bucket/secure/customers/".to_string(),
                authorizer: Arc::new(StaticAuthorizer {
                    access: Some(access.clone()),
                    principal: "arn:aws:iam::123456789012:role/analyst".to_string(),
                }),
            }),
            vec!["oxidant_lf_secure".to_string()],
            Arc::new(engine.ctx().clone()),
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicU64::new(0)),
            PendingCtas::default(),
        );

        let md = TableMetadata::new(
            "lf.oxidant_lf_secure.lf_protected_customers",
            "s3://lf-test-bucket/secure/customers/",
            TableFormat::Parquet,
        );
        let state = engine.ctx().state();
        provider
            .install_vended_credentials(&state, &md, "lf_protected_customers", &access)
            .expect("install vended credentials");

        // The bucket's registered store must now be the router, carrying exactly this table's
        // prefix. `Display` reports the governed-prefix count.
        let url = ListingTableUrl::parse("s3://lf-test-bucket/secure/customers/").expect("url");
        let registered = state
            .runtime_env()
            .object_store(url.object_store())
            .expect("store registered for the bucket");
        let rendered = format!("{registered}");
        assert!(
            rendered.contains("LakeFormationRoutingObjectStore"),
            "the bucket must resolve to the router, not the plain store: {rendered}"
        );
        assert!(
            rendered.contains("1 governed prefixes"),
            "the table's prefix must be routed: {rendered}"
        );
    }

    /// Vending is opt-in: a decision without credentials must leave the bucket on its ambient
    /// store, so enabling Lake Formation does not disturb how ungoverned data is read.
    #[tokio::test]
    async fn no_credentials_means_no_routing_store_is_installed() {
        use datafusion::datasource::listing::ListingTableUrl;

        let access = oxidant_catalog::TableAccess {
            authorized_columns: Some(vec!["id".to_string()]),
            row_filter: None,
            credentials: None,
            enforced: true,
        };
        let engine = crate::Engine::new();
        let provider = OxidantSchemaProvider::new(
            Arc::new(SecuredCatalog {
                location: "s3://lf-plain-bucket/secure/customers/".to_string(),
                authorizer: Arc::new(StaticAuthorizer {
                    access: Some(access.clone()),
                    principal: "arn:aws:iam::123456789012:role/analyst".to_string(),
                }),
            }),
            vec!["oxidant_lf_secure".to_string()],
            Arc::new(engine.ctx().clone()),
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicU64::new(0)),
            PendingCtas::default(),
        );
        let md = TableMetadata::new(
            "t",
            "s3://lf-plain-bucket/secure/customers/",
            TableFormat::Parquet,
        );
        let state = engine.ctx().state();
        provider
            .install_vended_credentials(&state, &md, "t", &access)
            .expect("no-op without credentials");

        let url = ListingTableUrl::parse("s3://lf-plain-bucket/secure/customers/").expect("url");
        if let Ok(registered) = state.runtime_env().object_store(url.object_store()) {
            assert!(
                !format!("{registered}").contains("LakeFormationRoutingObjectStore"),
                "no credentials must mean no router"
            );
        }
    }

    /// The fail-open this guards: a cache *hit* returns without re-authorizing, so unless the
    /// cached entry re-records that the table is governed, the SECOND query on it — anything
    /// inside the catalog TTL, i.e. the common case — would ship stage tickets saying "not
    /// governed" and every worker would stop enforcing.
    #[tokio::test]
    async fn a_cached_governed_table_still_demands_worker_enforcement() {
        let (engine, dir) = secured_engine(Some(granted(Some(&["id", "region"]), None)));

        // First query resolves and caches.
        let (_, first, _) = capture_lakeformation_enforcement(async {
            engine
                .sql(&format!("SELECT id FROM {LF_TABLE}"))
                .await
                .map(|_| ())
        })
        .await
        .expect("first query");
        assert!(first);

        // Second query is served from the cache — and must still demand enforcement.
        let (_, second, _) = capture_lakeformation_enforcement(async {
            engine
                .sql(&format!("SELECT id FROM {LF_TABLE}"))
                .await
                .map(|_| ())
        })
        .await
        .expect("second query");
        assert!(
            second,
            "a cache hit must not silently downgrade the query to `not governed`"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// …and an ungoverned table must NOT set it, or every query in a mixed catalog would start
    /// demanding enforcement from workers that legitimately have none.
    #[tokio::test]
    async fn resolving_an_ungoverned_table_does_not_demand_enforcement() {
        let (engine, dir) = secured_engine(Some(oxidant_catalog::TableAccess::unenforced()));
        let (_, required, _) = capture_lakeformation_enforcement(async {
            engine
                .sql(&format!("SELECT * FROM {LF_TABLE}"))
                .await
                .map(|_| ())
        })
        .await
        .expect("query");
        assert!(
            !required,
            "an ungoverned table must not demand worker enforcement"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A decision is part of the cache fingerprint, so revoking a grant takes effect on the next
    /// TTL revalidation rather than surviving until the process restarts.
    #[tokio::test]
    async fn revoked_grant_is_picked_up_within_the_catalog_ttl() {
        struct MutableAuthorizer {
            access: std::sync::Mutex<oxidant_catalog::TableAccess>,
        }
        #[async_trait]
        impl oxidant_catalog::TableAuthorizer for MutableAuthorizer {
            async fn authorize_scan(
                &self,
                _ns: &[String],
                _table: &str,
            ) -> CatResult<oxidant_catalog::TableAccess> {
                Ok(self.access.lock().expect("access poisoned").clone())
            }
            fn principal(&self) -> &str {
                "arn:aws:iam::123456789012:role/analyst"
            }
        }

        let dir = write_lf_fixture();
        let authorizer = Arc::new(MutableAuthorizer {
            access: std::sync::Mutex::new(granted(Some(&["id", "region", "amount", "ssn"]), None)),
        });
        let engine = crate::Engine::new();
        engine.register_catalog(
            "lf",
            Arc::new(SecuredCatalog {
                location: format!("file://{}", dir.to_string_lossy()),
                authorizer: authorizer.clone(),
            }),
        );

        // TTL 0 revalidates on every resolution, which is the behavior under test without a sleep.
        // `OXIDANT_CATALOG_CACHE_TTL_MS` is process-global, so take the same lock the other TTL
        // tests use — otherwise this test's window leaks into theirs and they miscount reloads.
        let _guard = TTL_ENV_LOCK.lock().await;
        std::env::set_var("OXIDANT_CATALOG_CACHE_TTL_MS", "0");

        let before = engine
            .sql(&format!("SELECT * FROM {LF_TABLE}"))
            .await
            .expect("before revoke");
        assert_eq!(before[0].schema().fields().len(), 4);

        // Revoke `ssn` out of band, as a Lake Formation admin would.
        *authorizer.access.lock().expect("access poisoned") =
            granted(Some(&["id", "region", "amount"]), None);

        let after = engine
            .sql(&format!("SELECT * FROM {LF_TABLE}"))
            .await
            .expect("after revoke");
        let names: Vec<String> = after[0]
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        assert_eq!(
            names,
            vec!["id", "region", "amount"],
            "the revoked column must stop being served"
        );

        std::env::remove_var("OXIDANT_CATALOG_CACHE_TTL_MS");
        let _ = std::fs::remove_dir_all(&dir);
    }
}

//! Reads Lake Formation-governed tables with Lake Formation's own vended credentials.
//!
//! This is the step that turns enforcement from advisory into real. Without it Oxidant reads S3
//! with its own (broad) identity and merely *chooses* to apply the column/row policy; with it the
//! bytes are fetched under short-lived credentials Lake Formation scoped to that one table, so a
//! table the principal was not granted is unreadable even if the engine's own role could reach the
//! prefix. Athena and EMR both work this way — AWS calls it credential vending.
//!
//! Two problems have to be solved to fit that model onto DataFusion.
//!
//! **1. Credentials expire.** They are short-lived by design (typically an hour), so a long query
//! must not die halfway through holding a stale token. [`LakeFormationCredentialProvider`] is an
//! `object_store::CredentialProvider` that re-vends before expiry, the same shape
//! [`crate::assume_role_credentials`] already uses for `sts:AssumeRole`. Re-vending goes back
//! through the authorizer, so a permission revoked mid-query is picked up at the next refresh
//! rather than being papered over until the process restarts.
//!
//! **2. Credentials are per *table*, but DataFusion registers object stores per *bucket*.**
//! `DefaultObjectStoreRegistry` keys on `scheme://host`, so two governed tables sharing a bucket —
//! the normal case for a data lake — cannot each have their own store. [`RoutingObjectStore`] is
//! registered once per bucket and dispatches every operation by path: a request under a governed
//! table's prefix goes to that table's credentials, and anything else falls back to the ambient
//! store. Longest-prefix wins, so a governed table nested under another governed prefix still gets
//! its own credentials.

use std::collections::HashMap;
use std::ops::Range;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use futures::StreamExt;
use object_store::aws::AwsCredential;
use object_store::path::Path;
use object_store::{
    CopyOptions, CredentialProvider, Error as StoreError, GetOptions, GetResult, ListResult,
    MultipartUpload, ObjectMeta, ObjectStore, ObjectStoreExt, PutMultipartOptions, PutOptions,
    PutPayload, PutResult, RenameOptions, Result as StoreResult,
};
use oxidant_catalog::{TableAuthorizer, VendedCredentials};

/// Re-vend once the cached credential is within this margin of expiring, so an in-flight request
/// never races a credential that expires mid-request. Same margin as the assume-role provider.
const REFRESH_MARGIN: Duration = Duration::from_secs(5 * 60);

/// Assumed lifetime for a vended credential that carries no expiry. Comfortably longer than
/// [`REFRESH_MARGIN`] so it is usable, short enough that the policy is re-read regularly.
const UNKNOWN_EXPIRY_LIFETIME: Duration = Duration::from_secs(15 * 60);

/// Lake Formation credentials for one table, refreshed transparently before they expire.
pub struct LakeFormationCredentialProvider {
    authorizer: Arc<dyn TableAuthorizer>,
    namespace: Vec<String>,
    table: String,
    /// `(credential, expiry)`. `None` until the first vend.
    cached: RwLock<Option<(Arc<AwsCredential>, SystemTime)>>,
    /// Serializes the actual vending call so N concurrent cache-miss readers produce ONE Lake
    /// Formation request rather than N. Held across an `.await`, hence the async mutex.
    refresh_lock: tokio::sync::Mutex<()>,
}

// Hand-written (the SPI trait object is not `Debug`) and deliberately redacting: `object_store`
// requires `Debug` on credential providers, and a derived one would print vended secrets into any
// store-level error or trace line.
impl std::fmt::Debug for LakeFormationCredentialProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LakeFormationCredentialProvider")
            .field(
                "table",
                &format!("{}.{}", self.namespace.join("."), self.table),
            )
            .field("principal", &self.authorizer.principal())
            .field("cached", &"<redacted>")
            .finish()
    }
}

impl LakeFormationCredentialProvider {
    /// Seed with the credentials the initial authorization already vended, so the first read does
    /// not pay for a second round trip.
    pub fn new(
        authorizer: Arc<dyn TableAuthorizer>,
        namespace: Vec<String>,
        table: String,
        initial: &VendedCredentials,
    ) -> Self {
        let provider = Self {
            authorizer,
            namespace,
            table,
            cached: RwLock::new(None),
            refresh_lock: tokio::sync::Mutex::new(()),
        };
        provider.store(initial);
        provider
    }

    fn store(&self, creds: &VendedCredentials) {
        // A credential with no stated expiry gets a bounded default lifetime rather than being
        // treated as already-stale. `expiration` is optional on the Lake Formation response (and
        // the conversion can fail), and marking it stale would make `fresh_cached` miss on EVERY
        // S3 request: each one would serialize on the refresh lock and issue a fresh
        // GetUnfilteredTableMetadata + GetTemporaryGlueTableCredentials pair, which never
        // converges because the re-vended credential has no expiry either. That turns one scan
        // into a throttled crawl.
        let expiry = creds
            .expires_at
            .unwrap_or_else(|| SystemTime::now() + UNKNOWN_EXPIRY_LIFETIME);
        *self
            .cached
            .write()
            .expect("lake formation credential cache poisoned") = Some((
            Arc::new(AwsCredential {
                key_id: creds.access_key_id.clone(),
                secret_key: creds.secret_access_key.clone(),
                token: creds.session_token.clone(),
            }),
            expiry,
        ));
    }

    /// A cached credential, if one exists and is not within [`REFRESH_MARGIN`] of expiring. Never
    /// holds the lock across an `.await`.
    fn fresh_cached(&self) -> Option<Arc<AwsCredential>> {
        let guard = self
            .cached
            .read()
            .expect("lake formation credential cache poisoned");
        let (cred, expiry) = guard.as_ref()?;
        (SystemTime::now() + REFRESH_MARGIN < *expiry).then(|| cred.clone())
    }

    async fn refresh(
        &self,
    ) -> Result<Arc<AwsCredential>, Box<dyn std::error::Error + Send + Sync>> {
        let _guard = self.refresh_lock.lock().await;
        if let Some(cred) = self.fresh_cached() {
            return Ok(cred);
        }
        // Re-authorizing rather than only re-vending is deliberate: it re-reads the policy, so a
        // grant revoked since the query started stops working at the next refresh.
        let access = self
            .authorizer
            .authorize_scan(&self.namespace, &self.table)
            .await?;
        let creds = access.credentials.ok_or_else(|| {
            format!(
                "lake formation returned no credentials when refreshing access to `{}`",
                self.table
            )
        })?;
        self.store(&creds);
        self.fresh_cached()
            .or_else(|| {
                // Freshly vended but already inside the refresh margin (a very short-lived
                // credential): use it rather than spinning.
                self.cached
                    .read()
                    .expect("lake formation credential cache poisoned")
                    .as_ref()
                    .map(|(c, _)| c.clone())
            })
            .ok_or_else(|| "lake formation credential cache was empty after refresh".into())
    }
}

#[async_trait]
impl CredentialProvider for LakeFormationCredentialProvider {
    type Credential = AwsCredential;

    async fn get_credential(&self) -> StoreResult<Arc<AwsCredential>> {
        if let Some(cred) = self.fresh_cached() {
            return Ok(cred);
        }
        self.refresh().await.map_err(|source| StoreError::Generic {
            store: "S3",
            source,
        })
    }
}

/// One bucket's object store, dispatching by path prefix to per-table stores.
///
/// Registered under the bucket URL in place of the plain store. Reads of a governed table's prefix
/// use that table's Lake Formation credentials; everything else uses `fallback`, so ungoverned
/// tables in the same bucket keep working exactly as before.
#[derive(Debug)]
pub struct RoutingObjectStore {
    fallback: Arc<dyn ObjectStore>,
    /// `(prefix, store)`, consulted longest-prefix-first.
    routes: RwLock<Vec<(Path, Arc<dyn ObjectStore>)>>,
}

impl std::fmt::Display for RoutingObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "LakeFormationRoutingObjectStore({} governed prefixes)",
            self.routes.read().map(|r| r.len()).unwrap_or_default()
        )
    }
}

impl RoutingObjectStore {
    pub fn new(fallback: Arc<dyn ObjectStore>) -> Self {
        Self {
            fallback,
            routes: RwLock::new(Vec::new()),
        }
    }

    /// Route everything under `prefix` to `store`. Replaces an existing route for the same prefix,
    /// which is what lets a re-resolved table swap in a new authorizer without leaking the old one.
    pub fn add_route(&self, prefix: Path, store: Arc<dyn ObjectStore>) {
        let mut routes = self.routes.write().expect("routing table poisoned");
        routes.retain(|(p, _)| p != &prefix);
        routes.push((prefix, store));
        // Longest prefix first, so a governed table nested inside another governed prefix wins.
        routes.sort_by(|(a, _), (b, _)| b.as_ref().len().cmp(&a.as_ref().len()));
    }

    /// Whether a route already covers exactly `prefix`.
    pub fn has_route(&self, prefix: &Path) -> bool {
        self.routes
            .read()
            .expect("routing table poisoned")
            .iter()
            .any(|(p, _)| p == prefix)
    }

    /// Whether `location` lies at or under `prefix`.
    ///
    /// The empty prefix is the bucket root — a governed table stored directly at `s3://bucket/`.
    /// It covers everything; testing it with the usual `"{prefix}/"` form would compare against
    /// `"/"`, match nothing, and silently route every object of that table to the ambient fallback
    /// store, bypassing vended credentials entirely.
    fn covers(prefix: &Path, location: &Path) -> bool {
        let prefix = prefix.as_ref();
        if prefix.is_empty() {
            return true;
        }
        location.as_ref() == prefix || location.as_ref().starts_with(&format!("{prefix}/"))
    }

    /// The store governing `location`, or the fallback.
    fn route(&self, location: &Path) -> Arc<dyn ObjectStore> {
        let routes = self.routes.read().expect("routing table poisoned");
        for (prefix, store) in routes.iter() {
            if Self::covers(prefix, location) {
                return store.clone();
            }
        }
        self.fallback.clone()
    }

    /// The store for a listing of `prefix`. A listing with no prefix (or one *above* every governed
    /// table) cannot be attributed to a single table's credentials, so it uses the fallback — the
    /// governed data itself is still protected, because reading any object under it routes to that
    /// table's credentials.
    fn route_prefix(&self, prefix: Option<&Path>) -> Arc<dyn ObjectStore> {
        match prefix {
            Some(p) => self.route(p),
            None => self.fallback.clone(),
        }
    }
}

#[async_trait]
impl ObjectStore for RoutingObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> StoreResult<PutResult> {
        self.route(location).put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> StoreResult<Box<dyn MultipartUpload>> {
        self.route(location)
            .put_multipart_opts(location, opts)
            .await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> StoreResult<GetResult> {
        self.route(location).get_opts(location, options).await
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> StoreResult<Vec<Bytes>> {
        self.route(location).get_ranges(location, ranges).await
    }

    /// `ObjectStoreExt`'s `get`, `get_range`, `head` and `delete` are all built on `get_opts` and
    /// `delete_stream`, so overriding the dyn-compatible core here is what makes every read path
    /// route — there is no extension method that could slip past to the fallback store.
    fn delete_stream(
        &self,
        locations: BoxStream<'static, StoreResult<Path>>,
    ) -> BoxStream<'static, StoreResult<Path>> {
        // Route per path: a batch may span governed and ungoverned prefixes, and each object must
        // be deleted under the identity that governs it.
        let routes = self.routes.read().expect("routing table poisoned").clone();
        let fallback = self.fallback.clone();
        locations
            .then(move |location| {
                let routes = routes.clone();
                let fallback = fallback.clone();
                async move {
                    let location = location?;
                    let store = routes
                        .iter()
                        .find(|(prefix, _)| Self::covers(prefix, &location))
                        .map(|(_, store)| store.clone())
                        .unwrap_or(fallback);
                    store.delete(&location).await?;
                    Ok(location)
                }
            })
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, StoreResult<ObjectMeta>> {
        self.route_prefix(prefix).list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, StoreResult<ObjectMeta>> {
        self.route_prefix(prefix).list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> StoreResult<ListResult> {
        self.route_prefix(prefix).list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> StoreResult<()> {
        self.route(from).copy_opts(from, to, options).await
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> StoreResult<()> {
        self.route(from).rename_opts(from, to, options).await
    }
}

/// The per-bucket routing stores this process installed, keyed by `scheme://bucket`.
///
/// Kept so a second governed table in the same bucket adds a route to the *existing* store rather
/// than replacing it — replacing would silently drop the first table's credentials.
#[allow(clippy::type_complexity)]
static ROUTING_STORES: RwLock<
    Option<
        HashMap<
            String,
            (
                std::sync::Weak<datafusion::execution::runtime_env::RuntimeEnv>,
                Arc<RoutingObjectStore>,
            ),
        >,
    >,
> = RwLock::new(None);

/// The routing store for `bucket_key`, creating one over `fallback` if this is the first governed
/// table in that bucket. Returns `(store, created)`; `created` tells the caller it still has to
/// register the store with DataFusion's object-store registry.
///
/// Get-or-insert under one write lock on purpose. Two tables in the same bucket resolving
/// concurrently would otherwise both see "no routing store", each build one, and the second
/// registration would drop the first table's route — leaving that table reading with the ambient
/// identity instead of its vended credentials, silently.
pub fn routing_store_or_insert(
    bucket_key: &str,
    owner: &Arc<datafusion::execution::runtime_env::RuntimeEnv>,
    fallback: &Arc<dyn ObjectStore>,
) -> (Arc<RoutingObjectStore>, bool) {
    let mut guard = ROUTING_STORES
        .write()
        .expect("routing store registry poisoned");
    let map = guard.get_or_insert_with(HashMap::new);
    if let Some((existing_owner, existing)) = map.get(bucket_key) {
        // The key embeds a `RuntimeEnv` address, and addresses get reused: when an `Engine` is
        // dropped a later one can be allocated at the same place (in-process multi-worker
        // harnesses, `--mode local-cluster`, long-lived test processes). Holding a `Weak` lets us
        // tell "same live engine" from "same address, different engine" — without this the new
        // engine is told the store already exists, skips registering it in its own registry, and
        // reads governed prefixes through its plain ambient store: the exact bypass this map is
        // meant to prevent. A `Weak` rather than an `Arc` so a stale entry cannot keep a dead
        // engine's runtime alive.
        if existing_owner
            .upgrade()
            .is_some_and(|o| Arc::ptr_eq(&o, owner))
        {
            return (existing.clone(), false);
        }
    }
    let store = Arc::new(RoutingObjectStore::new(fallback.clone()));
    map.insert(
        bucket_key.to_string(),
        (Arc::downgrade(owner), store.clone()),
    );
    (store, true)
}

/// Drop all remembered routing stores — test-only, so one test's routes cannot leak into another.
#[cfg(test)]
pub fn reset_routing_stores() {
    ROUTING_STORES
        .write()
        .expect("routing store registry poisoned")
        .take();
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    fn store_with(marker: &str) -> Arc<dyn ObjectStore> {
        let s = InMemory::new();
        let path = Path::from(format!("{marker}.txt"));
        futures::executor::block_on(s.put(&path, PutPayload::from(marker.as_bytes().to_vec())))
            .expect("seed");
        Arc::new(s)
    }

    /// Which store a path lands on is the whole security property: a governed table's objects must
    /// never be fetched with the ambient identity.
    #[tokio::test]
    async fn objects_under_a_governed_prefix_route_to_that_tables_store() {
        let routing = RoutingObjectStore::new(store_with("fallback"));
        let governed = store_with("governed");
        routing.add_route(Path::from("secure/customers"), governed.clone());

        // Under the prefix -> governed store.
        let picked = routing.route(&Path::from("secure/customers/part-0.parquet"));
        assert!(Arc::ptr_eq(&picked, &governed));
        // The prefix itself -> governed store.
        let picked = routing.route(&Path::from("secure/customers"));
        assert!(Arc::ptr_eq(&picked, &governed));
    }

    /// A sibling whose name merely *starts with* the governed prefix is a different table and must
    /// not borrow its credentials — `secure/customers_public` is not under `secure/customers`.
    #[tokio::test]
    async fn sibling_prefix_sharing_a_name_stem_does_not_match() {
        let fallback = store_with("fallback");
        let routing = RoutingObjectStore::new(fallback.clone());
        routing.add_route(Path::from("secure/customers"), store_with("governed"));

        let picked = routing.route(&Path::from("secure/customers_public/part-0.parquet"));
        assert!(
            Arc::ptr_eq(&picked, &fallback),
            "must not treat customers_public as part of customers"
        );
    }

    /// Ungoverned tables in the same bucket keep reading with the ambient identity — this is what
    /// lets one bucket hold both governed and ungoverned data.
    #[tokio::test]
    async fn ungoverned_paths_use_the_fallback_store() {
        let fallback = store_with("fallback");
        let routing = RoutingObjectStore::new(fallback.clone());
        routing.add_route(Path::from("secure/customers"), store_with("governed"));

        let picked = routing.route(&Path::from("public/events/part-0.parquet"));
        assert!(Arc::ptr_eq(&picked, &fallback));
    }

    /// The case DataFusion's per-bucket registry cannot express on its own: two governed tables in
    /// one bucket, each with its own credentials.
    #[tokio::test]
    async fn two_governed_tables_in_one_bucket_keep_separate_credentials() {
        let routing = RoutingObjectStore::new(store_with("fallback"));
        let customers = store_with("customers");
        let orders = store_with("orders");
        routing.add_route(Path::from("secure/customers"), customers.clone());
        routing.add_route(Path::from("secure/orders"), orders.clone());

        assert!(Arc::ptr_eq(
            &routing.route(&Path::from("secure/customers/a.parquet")),
            &customers
        ));
        assert!(Arc::ptr_eq(
            &routing.route(&Path::from("secure/orders/a.parquet")),
            &orders
        ));
    }

    /// Longest prefix wins, so a governed table nested inside another governed prefix is read with
    /// its own credentials rather than the outer table's.
    #[tokio::test]
    async fn nested_governed_prefix_wins_over_the_outer_one() {
        let routing = RoutingObjectStore::new(store_with("fallback"));
        let outer = store_with("outer");
        let inner = store_with("inner");
        routing.add_route(Path::from("secure"), outer);
        routing.add_route(Path::from("secure/customers"), inner.clone());

        assert!(Arc::ptr_eq(
            &routing.route(&Path::from("secure/customers/a.parquet")),
            &inner
        ));
    }

    /// Re-resolving a table replaces its route instead of stacking a second one, so a rotated
    /// authorizer cannot leave the previous credentials reachable.
    #[tokio::test]
    async fn re_adding_a_prefix_replaces_rather_than_duplicates() {
        let routing = RoutingObjectStore::new(store_with("fallback"));
        let first = store_with("first");
        let second = store_with("second");
        routing.add_route(Path::from("secure/customers"), first);
        routing.add_route(Path::from("secure/customers"), second.clone());

        assert_eq!(routing.routes.read().unwrap().len(), 1);
        assert!(Arc::ptr_eq(
            &routing.route(&Path::from("secure/customers/a.parquet")),
            &second
        ));
        assert!(routing.has_route(&Path::from("secure/customers")));
    }

    /// A governed table stored at the BUCKET ROOT has an empty prefix. Matching it with the usual
    /// `"{prefix}/"` form compares against `"/"`, matches nothing, and routes every one of its
    /// objects to the ambient fallback — a silent credential-vending bypass.
    #[tokio::test]
    async fn a_table_at_the_bucket_root_still_routes_to_its_credentials() {
        let fallback = store_with("fallback");
        let routing = RoutingObjectStore::new(fallback.clone());
        let governed = store_with("governed");
        routing.add_route(Path::from(""), governed.clone());

        for p in ["part-0.parquet", "nested/part-1.parquet"] {
            let picked = routing.route(&Path::from(p));
            assert!(
                Arc::ptr_eq(&picked, &governed),
                "{p} must use the table's vended credentials, not the ambient store"
            );
        }
    }

    /// A credential with no stated expiry must not be cached indefinitely.
    /// A credential with no stated expiry gets a bounded lifetime rather than being treated as
    /// already-stale. Stale-on-arrival would make `fresh_cached` miss on EVERY S3 request, and
    /// each miss issues a serialized GetUnfilteredTableMetadata + GetTemporaryGlueTableCredentials
    /// pair that never converges — one scan becomes a throttled crawl.
    #[test]
    fn credential_without_expiry_gets_a_bounded_lifetime_not_permanent_staleness() {
        struct NoopAuthorizer;
        #[async_trait]
        impl TableAuthorizer for NoopAuthorizer {
            async fn authorize_scan(
                &self,
                _ns: &[String],
                _t: &str,
            ) -> oxidant_catalog::Result<oxidant_catalog::TableAccess> {
                Ok(oxidant_catalog::TableAccess::unenforced())
            }
            fn principal(&self) -> &str {
                "test"
            }
        }
        let provider = LakeFormationCredentialProvider::new(
            Arc::new(NoopAuthorizer),
            vec!["db".to_string()],
            "t".to_string(),
            &VendedCredentials {
                access_key_id: "AKID".to_string(),
                secret_access_key: "secret".to_string(),
                session_token: None,
                expires_at: None,
            },
        );
        assert!(
            provider.fresh_cached().is_some(),
            "must be usable, or every S3 request re-vends"
        );
    }

    /// A credential expiring inside the refresh margin is already stale, so a long query re-vends
    /// rather than carrying a token that dies mid-request.
    #[test]
    fn credential_inside_the_refresh_margin_is_stale() {
        struct NoopAuthorizer;
        #[async_trait]
        impl TableAuthorizer for NoopAuthorizer {
            async fn authorize_scan(
                &self,
                _ns: &[String],
                _t: &str,
            ) -> oxidant_catalog::Result<oxidant_catalog::TableAccess> {
                Ok(oxidant_catalog::TableAccess::unenforced())
            }
            fn principal(&self) -> &str {
                "test"
            }
        }
        let creds = |d: Duration| VendedCredentials {
            access_key_id: "AKID".to_string(),
            secret_access_key: "secret".to_string(),
            session_token: None,
            expires_at: Some(SystemTime::now() + d),
        };
        let provider = LakeFormationCredentialProvider::new(
            Arc::new(NoopAuthorizer),
            vec!["db".to_string()],
            "t".to_string(),
            &creds(REFRESH_MARGIN / 2),
        );
        assert!(provider.fresh_cached().is_none(), "inside the margin");

        provider.store(&creds(REFRESH_MARGIN * 4));
        assert!(provider.fresh_cached().is_some(), "comfortably ahead");
    }
}

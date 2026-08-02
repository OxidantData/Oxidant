//! Process-global cache of decoded replicated (broadcast / dimension) table scans.
//!
//! In distributed mode every worker scans a replicated table's FULL file set on every stage that
//! references it, and nothing carries that decoded data across stages or queries — the same
//! parquet is re-read from object storage and re-decoded on every stage of a query and on every
//! query of a benchmark suite. This module memoizes the decoded Arrow [`RecordBatch`]es behind a
//! process-global, byte-capped LRU so a replicated table is read + decoded **once per worker per
//! data version**, then served from memory as a [`MemTable`].
//!
//! # Correctness contract
//!
//! The cache key is `(lowercase catalog-qualified table name, data-version fingerprint)`. The
//! fingerprint is the *only* freshness signal; callers compute it from the exact state the scan
//! is about to read:
//!
//! - Delta/Iceberg: the pinned [`weft_datasource::SnapshotIdentity`] the driver resolved
//!   ([`fingerprint_snapshot`]).
//! - Plain Parquet/CSV/JSON: the resolved file set — sorted `(location, size, last_modified,
//!   e_tag, version)` from object-store metadata ([`fingerprint_object_metas`]).
//!
//! A refreshed/restated table therefore resolves to a *different* key: the new scan misses, reads
//! fresh files, and inserts a new entry; the stale entry ages out under the LRU. The cache never
//! validates one key's entry against newer storage state — if a reliable version signal is not
//! available for a table, that table must not be cached (callers only invoke [`memoize_provider`]
//! where one of the two fingerprint sources above exists).
//!
//! The hook runs at provider *resolution* time (`catalog_bridge`), exactly where the engine
//! itself resolves the file set / snapshot for the scan, so the cache is never fed a file set
//! the engine would not have scanned. Only tables classified replicated for the current
//! stage/query ([`crate::shard::is_replicated_table`]) are eligible — a sharded table's
//! per-worker file subset is never cached as if it were the full table.
//!
//! # Eviction and observability
//!
//! Total decoded bytes (deep Arrow size) are capped by `WEFT_DIM_CACHE_BYTES` (default 2 GiB;
//! `0` disables caching entirely and providers pass through untouched). Hits, misses, inserts,
//! evictions, and cached bytes are counted on the global cache ([`DimCache::stats`]) and logged
//! through `tracing` on insert/evict.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, OnceLock};

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::datasource::{MemTable, TableProvider};
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::execution::SessionState;

/// Default cache budget: 2 GiB of decoded Arrow data per worker process.
const DEFAULT_DIM_CACHE_BYTES: u64 = 2 << 30;

/// Cache key: one decoded copy per (table, data version). `table` is the lowercase
/// catalog-qualified name (`prod.db.date_dim`), so two same-named tables in different
/// namespaces never share an entry; `fingerprint` is the caller-computed version signal.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DimCacheKey {
    table: String,
    fingerprint: String,
}

impl DimCacheKey {
    pub fn new(table: &str, fingerprint: String) -> Self {
        Self {
            table: table.to_ascii_lowercase(),
            fingerprint,
        }
    }
}

/// One cached scan: the decoded batches plus the schema they were decoded against.
struct DimCacheEntry {
    schema: SchemaRef,
    batches: Arc<Vec<RecordBatch>>,
    /// Deep Arrow memory size of `batches` — the unit the byte cap accounts in.
    bytes: usize,
}

/// Counters + current footprint of a [`DimCache`], for observability and tests.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DimCacheStats {
    pub hits: u64,
    pub misses: u64,
    pub inserts: u64,
    pub evictions: u64,
    pub cached_bytes: u64,
    pub entries: u64,
}

struct DimCacheInner {
    entries: HashMap<DimCacheKey, DimCacheEntry>,
    /// Front = least recently used. Touched on every hit and insert.
    lru: VecDeque<DimCacheKey>,
    total_bytes: usize,
    hits: u64,
    misses: u64,
    inserts: u64,
    evictions: u64,
}

/// A byte-capped LRU of decoded replicated-table scans. Thread-safe: one `Mutex` guards the
/// map, LRU order, footprint, and counters — cache operations are cheap pointer moves, so the
/// lock is never held across I/O (population scans happen *before* [`DimCache::insert`]).
pub struct DimCache {
    inner: Mutex<DimCacheInner>,
    cap_bytes: u64,
}

impl DimCache {
    fn with_cap(cap_bytes: u64) -> Self {
        Self {
            inner: Mutex::new(DimCacheInner {
                entries: HashMap::new(),
                lru: VecDeque::new(),
                total_bytes: 0,
                hits: 0,
                misses: 0,
                inserts: 0,
                evictions: 0,
            }),
            cap_bytes,
        }
    }

    /// Whether this cache stores anything (`WEFT_DIM_CACHE_BYTES=0` disables caching).
    pub fn enabled(&self) -> bool {
        self.cap_bytes > 0
    }

    /// Look up `key`, refreshing its LRU position. Counts a hit or a miss.
    pub fn get(&self, key: &DimCacheKey) -> Option<(SchemaRef, Arc<Vec<RecordBatch>>)> {
        let mut inner = self.inner.lock().expect("dim cache poisoned");
        let Some(entry) = inner.entries.get(key) else {
            inner.misses += 1;
            return None;
        };
        let hit = (entry.schema.clone(), entry.batches.clone());
        if let Some(pos) = inner.lru.iter().position(|k| k == key) {
            inner.lru.remove(pos);
        }
        inner.lru.push_back(key.clone());
        inner.hits += 1;
        Some(hit)
    }

    /// Store `batches` under `key`, evicting least-recently-used entries until the cap holds.
    /// An entry larger than the whole cap is not inserted (it could only ever be evicted).
    pub fn insert(&self, key: DimCacheKey, schema: SchemaRef, batches: Arc<Vec<RecordBatch>>) {
        if !self.enabled() {
            return;
        }
        let bytes: usize = batches.iter().map(RecordBatch::get_array_memory_size).sum();
        if bytes as u64 > self.cap_bytes {
            return;
        }
        let mut inner = self.inner.lock().expect("dim cache poisoned");
        // Replacing a live entry (same table re-cached at the same version after an eviction
        // race) must not double-count its bytes.
        if let Some(old) = inner.entries.remove(&key) {
            inner.total_bytes -= old.bytes;
            if let Some(pos) = inner.lru.iter().position(|k| k == &key) {
                inner.lru.remove(pos);
            }
        }
        while inner.total_bytes + bytes > self.cap_bytes as usize {
            let Some(victim) = inner.lru.pop_front() else {
                break;
            };
            if let Some(entry) = inner.entries.remove(&victim) {
                inner.total_bytes -= entry.bytes;
                inner.evictions += 1;
                tracing::info!(
                    table = %victim.table,
                    bytes = entry.bytes,
                    total_bytes = inner.total_bytes,
                    "dim cache eviction"
                );
            }
        }
        inner.total_bytes += bytes;
        inner.lru.push_back(key.clone());
        inner.entries.insert(
            key,
            DimCacheEntry {
                schema,
                batches,
                bytes,
            },
        );
        inner.inserts += 1;
    }

    /// Snapshot of counters and current footprint.
    pub fn stats(&self) -> DimCacheStats {
        let inner = self.inner.lock().expect("dim cache poisoned");
        DimCacheStats {
            hits: inner.hits,
            misses: inner.misses,
            inserts: inner.inserts,
            evictions: inner.evictions,
            cached_bytes: inner.total_bytes as u64,
            entries: inner.entries.len() as u64,
        }
    }
}

/// The process-global cache, sized once from `WEFT_DIM_CACHE_BYTES` (default
/// [`DEFAULT_DIM_CACHE_BYTES`]; `0` disables). One worker process backs one cluster node, and
/// engines/sessions within it come and go — process scope is exactly the sharing boundary.
pub fn global() -> &'static DimCache {
    DIM_CACHE.get_or_init(|| DimCache::with_cap(cap_bytes_from_env()))
}

static DIM_CACHE: OnceLock<DimCache> = OnceLock::new();

/// Parse the `WEFT_DIM_CACHE_BYTES` budget (absent/unparseable → default).
fn cap_bytes_from_env() -> u64 {
    parse_cap_bytes(std::env::var("WEFT_DIM_CACHE_BYTES").ok().as_deref())
}

fn parse_cap_bytes(raw: Option<&str>) -> u64 {
    match raw {
        Some(s) => s.trim().parse().unwrap_or(DEFAULT_DIM_CACHE_BYTES),
        None => DEFAULT_DIM_CACHE_BYTES,
    }
}

/// Data-version fingerprint for a plain Parquet/CSV/JSON table: the resolved file set's
/// sorted `(location, size, last_modified, e_tag, version)` tuples, hashed. Any file added,
/// removed, or rewritten (new mtime/etag at the same path) changes the fingerprint, so a
/// restated table never reuses the previous version's decoded rows.
pub fn fingerprint_object_metas(files: &[(ListingTableUrl, object_store::ObjectMeta)]) -> String {
    use std::hash::{Hash, Hasher};

    let mut parts: Vec<String> = files
        .iter()
        .map(|(_, meta)| {
            format!(
                "{}\u{0}{}\u{0}{}\u{0}{:?}\u{0}{:?}",
                meta.location.as_ref(),
                meta.size,
                meta.last_modified.timestamp_micros(),
                meta.e_tag,
                meta.version
            )
        })
        .collect();
    parts.sort_unstable();
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    parts.hash(&mut hasher);
    format!("files:{:016x}", hasher.finish())
}

/// Data-version fingerprint for a Delta/Iceberg table: the pinned snapshot identity itself
/// (`{"format":"delta","version":7}`), serialized. Two pins are equal iff they name the same
/// table state, which is exactly the cache's reuse boundary.
pub fn fingerprint_snapshot(snapshot: &weft_datasource::SnapshotIdentity) -> String {
    match serde_json::to_string(snapshot) {
        Ok(json) => format!("snapshot:{json}"),
        // SnapshotIdentity is a plain data enum; serialization cannot fail. If it ever did,
        // fall back to the Debug form rather than skip caching.
        Err(_) => format!("snapshot:{snapshot:?}"),
    }
}

/// Serve a replicated table from the process-global cache, populating it on a miss.
///
/// `provider` must be the scan provider built over the exact file set / snapshot that
/// `fingerprint` describes (both come out of the same resolution in `catalog_bridge`), and
/// `source_bytes` its on-disk byte total — a cheap preflight: a table whose compressed bytes
/// already exceed the cap cannot fit decoded, so it is served uncached without a wasted scan.
///
/// - **hit**: returns a [`MemTable`] over the cached batches; `provider` is dropped unread.
/// - **miss**: fully scans `provider` (all columns, no pushdown — a replicated table is read in
///   full by every worker anyway), caches the decoded batches, and returns a [`MemTable`] over
///   the *same* buffers, so even the populating query reads the files exactly once.
/// - **disabled / too large**: returns `provider` unchanged.
///
/// A failed population scan propagates the error (the query would have failed on the same
/// files at execution time) and caches nothing.
pub async fn memoize_provider(
    state: &SessionState,
    table: &str,
    fingerprint: String,
    source_bytes: u64,
    provider: Arc<dyn TableProvider>,
) -> DfResult<Arc<dyn TableProvider>> {
    memoize_with(global(), state, table, fingerprint, source_bytes, provider).await
}

/// [`memoize_provider`] against an explicit cache — the unit-test seam that keeps tests off
/// the process-global instance.
async fn memoize_with(
    cache: &DimCache,
    state: &SessionState,
    table: &str,
    fingerprint: String,
    source_bytes: u64,
    provider: Arc<dyn TableProvider>,
) -> DfResult<Arc<dyn TableProvider>> {
    if !cache.enabled() {
        return Ok(provider);
    }
    let key = DimCacheKey::new(table, fingerprint);
    if let Some((schema, batches)) = cache.get(&key) {
        return mem_table(&key, schema, batches);
    }
    if source_bytes > cache.cap_bytes {
        return Ok(provider);
    }
    let plan = provider.scan(state, None, &[], None).await?;
    let batches = datafusion::physical_plan::collect(plan, state.task_ctx()).await?;
    let schema = batches
        .first()
        .map(RecordBatch::schema)
        .unwrap_or_else(|| provider.schema());
    let batches = Arc::new(batches);
    cache.insert(key.clone(), schema.clone(), batches.clone());
    let stats = cache.stats();
    tracing::info!(
        table = %key.table,
        source_bytes,
        hits = stats.hits,
        misses = stats.misses,
        inserts = stats.inserts,
        evictions = stats.evictions,
        cached_bytes = stats.cached_bytes,
        "dim cache populated"
    );
    mem_table(&key, schema, batches)
}

fn mem_table(
    key: &DimCacheKey,
    schema: SchemaRef,
    batches: Arc<Vec<RecordBatch>>,
) -> DfResult<Arc<dyn TableProvider>> {
    let table = MemTable::try_new(schema, vec![(*batches).clone()]).map_err(|e| {
        DataFusionError::Execution(format!("dim cache mem table `{}`: {e}", key.table))
    })?;
    Ok(Arc::new(table))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use object_store::path::Path;
    use object_store::ObjectMeta;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]))
    }

    fn batch(values: Vec<i64>) -> RecordBatch {
        RecordBatch::try_new(schema(), vec![Arc::new(Int64Array::from(values))]).unwrap()
    }

    fn key(table: &str, fp: &str) -> DimCacheKey {
        DimCacheKey::new(table, fp.to_string())
    }

    fn meta(
        path: &str,
        size: u64,
        micros: i64,
        e_tag: Option<&str>,
    ) -> (ListingTableUrl, ObjectMeta) {
        (
            ListingTableUrl::parse(format!("file:///tmp/{path}")).unwrap(),
            ObjectMeta {
                location: Path::from(path),
                last_modified: chrono::DateTime::from_timestamp_micros(micros).unwrap(),
                size,
                e_tag: e_tag.map(str::to_string),
                version: None,
            },
        )
    }

    #[test]
    fn fingerprint_object_metas_ignores_order_but_tracks_content() {
        let a = meta("a.parquet", 10, 100, Some("etag-a"));
        let b = meta("b.parquet", 20, 200, Some("etag-b"));
        assert_eq!(
            fingerprint_object_metas(&[a.clone(), b.clone()]),
            fingerprint_object_metas(&[b.clone(), a.clone()]),
            "file order must not matter (listings are not ordered across stores)"
        );
        let baseline = fingerprint_object_metas(&[a.clone(), b.clone()]);
        // A rewritten file: same path and size, new mtime + etag.
        let rewritten = meta("a.parquet", 10, 101, Some("etag-a2"));
        assert_ne!(fingerprint_object_metas(&[rewritten, b.clone()]), baseline);
        // A same-content re-list (identical metas) must reuse the entry.
        assert_eq!(fingerprint_object_metas(&[a.clone(), b]), baseline);
        // A resized file.
        let grown = meta("a.parquet", 11, 100, Some("etag-a"));
        assert_ne!(
            fingerprint_object_metas(&[grown]),
            fingerprint_object_metas(&[a])
        );
    }

    #[test]
    fn fingerprint_snapshot_distinguishes_versions_and_formats() {
        let delta7 = weft_datasource::SnapshotIdentity::Delta { version: 7 };
        let delta8 = weft_datasource::SnapshotIdentity::Delta { version: 8 };
        assert_eq!(fingerprint_snapshot(&delta7), fingerprint_snapshot(&delta7));
        assert_ne!(fingerprint_snapshot(&delta7), fingerprint_snapshot(&delta8));
        let iceberg = weft_datasource::SnapshotIdentity::Iceberg {
            snapshot_id: 7,
            sequence_number: 0,
            metadata_location: "s3://b/t/metadata/v1.json".to_string(),
        };
        assert_ne!(
            fingerprint_snapshot(&delta7),
            fingerprint_snapshot(&iceberg)
        );
    }

    #[test]
    fn cache_key_lowercases_table_name() {
        assert_eq!(key("Prod.DB.Orders", "f"), key("prod.db.orders", "f"));
        assert_ne!(key("prod.db.orders", "f1"), key("prod.db.orders", "f2"));
    }

    #[test]
    fn hit_serves_cached_batches_and_counts() {
        let cache = DimCache::with_cap(1 << 20);
        let batches = Arc::new(vec![batch(vec![1, 2, 3])]);
        cache.insert(key("db.t", "v1"), schema(), batches.clone());
        assert!(cache.get(&key("db.t", "v1")).is_some());
        assert!(cache.get(&key("db.t", "v2")).is_none());
        let stats = cache.stats();
        assert_eq!((stats.hits, stats.misses, stats.inserts), (1, 1, 1));
        assert_eq!(stats.entries, 1);
        assert!(stats.cached_bytes > 0);
    }

    #[test]
    fn version_change_misses_and_replaces_nothing() {
        let cache = DimCache::with_cap(1 << 20);
        cache.insert(key("db.t", "v1"), schema(), Arc::new(vec![batch(vec![1])]));
        // A restated table has a new fingerprint: the lookup misses and the old entry stays
        // (other in-flight stages may still hold it; the LRU reclaims it eventually).
        assert!(cache.get(&key("db.t", "v2")).is_none());
        cache.insert(key("db.t", "v2"), schema(), Arc::new(vec![batch(vec![2])]));
        assert_eq!(cache.stats().entries, 2);
        assert!(cache.get(&key("db.t", "v1")).is_some());
        assert!(cache.get(&key("db.t", "v2")).is_some());
    }

    #[test]
    fn lru_evicts_least_recently_used_first() {
        let one = batch(vec![1]);
        let entry_bytes = one.get_array_memory_size() as u64;
        let cache = DimCache::with_cap(2 * entry_bytes + 64);
        cache.insert(key("db.a", "v"), schema(), Arc::new(vec![one.clone()]));
        cache.insert(key("db.b", "v"), schema(), Arc::new(vec![one.clone()]));
        // Touch `a` so `b` is now the LRU entry.
        assert!(cache.get(&key("db.a", "v")).is_some());
        cache.insert(key("db.c", "v"), schema(), Arc::new(vec![one]));
        assert!(
            cache.get(&key("db.b", "v")).is_none(),
            "LRU victim should be b"
        );
        assert!(cache.get(&key("db.a", "v")).is_some());
        assert!(cache.get(&key("db.c", "v")).is_some());
        let stats = cache.stats();
        assert_eq!(stats.evictions, 1);
        assert_eq!(stats.entries, 2);
    }

    #[test]
    fn byte_cap_is_enforced() {
        let one = batch(vec![1]);
        let entry_bytes = one.get_array_memory_size() as u64;
        // Cap fits exactly two entries; the third insert evicts the oldest.
        let cache = DimCache::with_cap(2 * entry_bytes);
        for name in ["a", "b", "c"] {
            cache.insert(
                key(&format!("db.{name}"), "v"),
                schema(),
                Arc::new(vec![one.clone()]),
            );
        }
        let stats = cache.stats();
        assert!(stats.cached_bytes <= 2 * entry_bytes);
        assert_eq!(stats.entries, 2);
        assert_eq!(stats.evictions, 1);

        // An entry larger than the whole cap is never inserted.
        let huge = DimCache::with_cap(entry_bytes / 2);
        huge.insert(key("db.big", "v"), schema(), Arc::new(vec![one]));
        assert_eq!(huge.stats().entries, 0);
        assert_eq!(huge.stats().cached_bytes, 0);
    }

    #[test]
    fn disabled_cache_stores_nothing() {
        let cache = DimCache::with_cap(0);
        assert!(!cache.enabled());
        cache.insert(key("db.t", "v"), schema(), Arc::new(vec![batch(vec![1])]));
        assert!(cache.get(&key("db.t", "v")).is_none());
        assert_eq!(cache.stats().entries, 0);
    }

    #[test]
    fn parse_cap_bytes_defaults_and_zero() {
        assert_eq!(parse_cap_bytes(None), DEFAULT_DIM_CACHE_BYTES);
        assert_eq!(parse_cap_bytes(Some("0")), 0);
        assert_eq!(parse_cap_bytes(Some(" 1048576 ")), 1 << 20);
        assert_eq!(parse_cap_bytes(Some("junk")), DEFAULT_DIM_CACHE_BYTES);
    }

    #[test]
    fn concurrent_gets_and_inserts_stay_consistent() {
        let cache = Arc::new(DimCache::with_cap(1 << 20));
        let mut handles = Vec::new();
        for thread in 0..8 {
            let cache = cache.clone();
            handles.push(std::thread::spawn(move || {
                for i in 0..100 {
                    let k = key(&format!("db.t{}", (thread + i) % 4), "v");
                    cache.insert(k.clone(), schema(), Arc::new(vec![batch(vec![1])]));
                    let _ = cache.get(&k);
                }
            }));
        }
        for handle in handles {
            handle.join().unwrap();
        }
        let stats = cache.stats();
        assert!(stats.cached_bytes <= 1 << 20);
        assert_eq!(stats.entries, 4);
        assert!(stats.hits > 0 && stats.inserts >= 4);
    }

    #[tokio::test]
    async fn memoize_disabled_passes_provider_through() {
        let cache = DimCache::with_cap(0);
        let ctx = datafusion::prelude::SessionContext::new();
        let state = ctx.state();
        let provider: Arc<dyn TableProvider> =
            Arc::new(MemTable::try_new(schema(), vec![vec![batch(vec![1])]]).unwrap());
        let out = memoize_with(
            &cache,
            &state,
            "db.t",
            "v1".to_string(),
            1,
            provider.clone(),
        )
        .await
        .unwrap();
        assert!(
            Arc::ptr_eq(&out, &provider),
            "a disabled cache must return the original provider untouched"
        );
        assert_eq!(
            cache.stats().misses,
            0,
            "disabled mode must not even look up"
        );
    }

    #[tokio::test]
    async fn memoize_oversized_source_passes_provider_through() {
        let cache = DimCache::with_cap(1024);
        let ctx = datafusion::prelude::SessionContext::new();
        let state = ctx.state();
        let provider: Arc<dyn TableProvider> =
            Arc::new(MemTable::try_new(schema(), vec![vec![batch(vec![1])]]).unwrap());
        // source_bytes beyond the cap: skip the population scan entirely.
        let out = memoize_with(
            &cache,
            &state,
            "db.t",
            "v1".to_string(),
            2048,
            provider.clone(),
        )
        .await
        .unwrap();
        assert!(Arc::ptr_eq(&out, &provider));
        assert_eq!(cache.stats().inserts, 0);
    }

    #[tokio::test]
    async fn memoize_populates_then_serves_from_cache() {
        let cache = DimCache::with_cap(1 << 20);
        let ctx = datafusion::prelude::SessionContext::new();
        let state = ctx.state();
        let provider: Arc<dyn TableProvider> = Arc::new(
            MemTable::try_new(schema(), vec![vec![batch(vec![1, 2]), batch(vec![3])]]).unwrap(),
        );
        let first = memoize_with(&cache, &state, "db.t", "v1".to_string(), 1, provider)
            .await
            .unwrap();
        let stats = cache.stats();
        assert_eq!((stats.misses, stats.inserts, stats.hits), (1, 1, 0));

        // Second resolution at the same version: served from the cache, no provider scan.
        let second_provider: Arc<dyn TableProvider> =
            Arc::new(MemTable::try_new(schema(), vec![vec![]]).unwrap());
        let second = memoize_with(&cache, &state, "db.t", "v1".to_string(), 1, second_provider)
            .await
            .unwrap();
        assert_eq!(cache.stats().hits, 1);
        for provider in [first, second] {
            let plan = provider.scan(&state, None, &[], None).await.unwrap();
            let batches = datafusion::physical_plan::collect(plan, state.task_ctx())
                .await
                .unwrap();
            assert_eq!(
                batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
                3,
                "both the populating and the cached provider must serve all rows"
            );
        }
    }
}

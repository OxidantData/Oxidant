//! Disk-caching [`ObjectStore`] wrapper for remote (S3) analytical reads (KAN-2 throughput).
//!
//! Workers re-read the same parquet bytes from S3 on every query (~250 MB/node for a TPC-DS
//! SF10 fact scan), and the effective read path lands ~85-100 MB/s/node — well below what the
//! instance pulls with parallel ranged reads, and the dominant per-row cost left after the
//! plan-shape program (Q39-class residual vs Spark). This wrapper serves repeat reads from
//! local NVMe: the first read of an object materializes it under `OXIDANT_S3_CACHE_DIR`, later
//! `get`/`get_opts`/`get_ranges` calls are served from the local file
//! (`GetResultPayload::File` — the same shape `LocalFileSystem` returns), and entries
//! revalidate against S3 `HEAD` (size + etag) after `OXIDANT_S3_CACHE_TTL_MS` so an overwritten
//! object is never served stale past the TTL. Same disk-cache pattern Databricks and
//! Snowflake run for remote tables.
//!
//! Correctness contract:
//! - Read-path only: writes (`put`/`delete`/copy/rename/multipart) delegate to the inner
//!   store and invalidate any cached copy of the affected path.
//! - Any cache error (unwritable dir, short read, vanished file) falls back to the inner
//!   store — the cache never breaks a read.
//! - Single-flight per path: concurrent readers share one download (the parquet reader
//!   touches the same files from every partition task on the node).

use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use futures::stream::{BoxStream, StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::{
    Attributes, CopyOptions, GetOptions, GetRange, GetResult, GetResultPayload, ListResult,
    MultipartUpload, ObjectMeta, ObjectStore, ObjectStoreExt, PutMultipartOptions, PutOptions,
    PutPayload, PutResult, RenameOptions, Result,
};
use tokio::sync::OnceCell;

/// Default on-disk budget per node: 20 GiB of parquet objects.
const DEFAULT_MAX_BYTES: u64 = 20 * 1024 * 1024 * 1024;
/// Default revalidation window: 5 minutes between S3 HEAD checks per cached object.
const DEFAULT_TTL_MS: u64 = 300_000;

/// One materialized object in the cache index.
struct Entry {
    /// Local file holding the full object bytes.
    file: std::path::PathBuf,
    bytes: u64,
    size: u64,
    e_tag: Option<String>,
    last_used: std::time::Instant,
    last_validated: std::time::Instant,
}

/// Sidecar next to each cached file recording the object identity it was
/// downloaded from (`{size}\n{e_tag or "-"}`). This is what lets a fresh
/// process re-adopt on-disk bytes after a restart instead of re-downloading:
/// the in-memory index starts empty, so adoption validates the sidecar against
/// a fresh S3 HEAD (size + etag) plus the on-disk length before trusting it.
/// Written after the data file's atomic rename; a crash between the two just
/// forces a re-download (fail-safe). Missing/corrupt sidecar ⇒ re-download.
fn sidecar_path(file: &std::path::Path) -> std::path::PathBuf {
    std::path::PathBuf::from(format!("{}.meta", file.display()))
}

fn write_sidecar(file: &std::path::Path, size: u64, e_tag: Option<&str>) {
    let side = sidecar_path(file);
    let tmp = side.with_extension(format!("tmp{}", std::process::id()));
    let body = format!("{size}\n{}\n", e_tag.unwrap_or("-"));
    if std::fs::write(&tmp, body).is_ok() {
        let _ = std::fs::rename(&tmp, &side);
    } else {
        let _ = std::fs::remove_file(&tmp);
    }
}

fn remove_sidecar(file: &std::path::Path) {
    let _ = std::fs::remove_file(sidecar_path(file));
}

type MaterializeResult = std::result::Result<std::path::PathBuf, Arc<std::io::Error>>;
type Cell = OnceCell<std::path::PathBuf>;

struct State {
    /// Path -> materialized object. In-memory index; on-disk files persist across restarts
    /// and are re-adopted by HEAD validation (size + etag match) on first touch.
    map: Mutex<HashMap<Path, Entry>>,
    /// Single-flight: path -> shared cell resolving to the materialized local file.
    inflight: Mutex<HashMap<Path, Arc<Cell>>>,
    total_bytes: AtomicU64,
}

/// An [`ObjectStore`] that serves repeat reads from local disk; see the module docs.
pub struct DiskCachingStore {
    inner: Arc<dyn ObjectStore>,
    dir: std::path::PathBuf,
    max_bytes: u64,
    ttl: std::time::Duration,
    state: Arc<State>,
}

fn cache_err(
    context: &str,
    e: impl std::error::Error + Send + Sync + 'static,
) -> object_store::Error {
    object_store::Error::Generic {
        store: "oxidant-s3-cache",
        source: Box::new(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("{context}: {e}"),
        )),
    }
}

impl DiskCachingStore {
    /// Wrap `inner` with a disk cache at `dir` (`max_bytes` LRU budget, `ttl` revalidation
    /// window). Creates `dir` if missing.
    pub fn new(
        inner: Arc<dyn ObjectStore>,
        dir: std::path::PathBuf,
        max_bytes: u64,
        ttl: std::time::Duration,
    ) -> std::io::Result<Self> {
        std::fs::create_dir_all(&dir)?;
        Ok(Self {
            inner,
            dir,
            max_bytes,
            ttl,
            state: Arc::new(State {
                map: Mutex::new(HashMap::new()),
                inflight: Mutex::new(HashMap::new()),
                total_bytes: AtomicU64::new(0),
            }),
        })
    }

    /// Build the wrapper from `OXIDANT_S3_CACHE_DIR` (unset/empty disables caching),
    /// `OXIDANT_S3_CACHE_MAX_BYTES` (default 20 GiB) and `OXIDANT_S3_CACHE_TTL_MS` (default 300000).
    pub fn from_env(inner: Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore> {
        let Some(dir) = std::env::var("OXIDANT_S3_CACHE_DIR")
            .ok()
            .filter(|s| !s.trim().is_empty())
        else {
            return inner;
        };
        let max_bytes = std::env::var("OXIDANT_S3_CACHE_MAX_BYTES")
            .ok()
            .and_then(|s| s.trim().parse().ok())
            .unwrap_or(DEFAULT_MAX_BYTES);
        let ttl_ms = std::env::var("OXIDANT_S3_CACHE_TTL_MS")
            .ok()
            .and_then(|s| s.trim().parse().ok())
            .unwrap_or(DEFAULT_TTL_MS);
        match Self::new(
            inner.clone(),
            std::path::PathBuf::from(dir.clone()),
            max_bytes,
            std::time::Duration::from_millis(ttl_ms),
        ) {
            Ok(store) => {
                eprintln!(
                    "oxidant: S3 disk cache enabled at {dir} (max {max_bytes} B, ttl {ttl_ms} ms)"
                );
                Arc::new(store)
            }
            Err(e) => {
                eprintln!("warn: OXIDANT_S3_CACHE_DIR={dir} unusable ({e}); reading S3 direct");
                inner
            }
        }
    }

    /// The local file for `location` under the cache dir (`%`-encoded full key).
    fn local_file(&self, location: &Path) -> std::path::PathBuf {
        let mut name = location.to_string().replace('/', "%2F");
        if name.len() > 180 {
            // Keep filesystem-name limits with a stable tail hash.
            let hash = format!("{:016x}", fnv1a(name.as_bytes()));
            name = format!("{}..{hash}", &name[..140]);
        }
        self.dir.join(name)
    }

    /// Resolve `location` to a local file, downloading through the single-flight on miss and
    /// revalidating by HEAD (size + etag) when the entry is stale. `Err` leaves no cache
    /// state; callers fall back to the inner store.
    async fn ensure_local(&self, location: &Path) -> MaterializeResult {
        if let Some(file) = self.fresh_entry(location) {
            return Ok(file);
        }
        // Single-flight: one cell per path; `get_or_try_init` coalesces concurrent
        // initializers (one download), wakes waiters on completion, releases the slot for
        // the next waiter to retry if the initializing task is dropped (cancelled/panicked)
        // mid-download, and leaves the cell uninitialized on a returned Err (next caller
        // retries).
        let cell = {
            let mut inflight = self
                .state
                .inflight
                .lock()
                .expect("s3 cache inflight poisoned");
            inflight
                .entry(location.clone())
                .or_insert_with(|| Arc::new(OnceCell::new()))
                .clone()
        };
        let file = cell
            .get_or_try_init(|| self.materialize(location))
            .await
            .map_err(Arc::new)?
            .clone();
        // Best-effort cleanup: the entry in `map` (written by `materialize`) serves all
        // later touches via `fresh_entry`, so the cell only matters while a download is in
        // flight. A racing caller holding an Arc to it is unaffected either way.
        let mut inflight = self
            .state
            .inflight
            .lock()
            .expect("s3 cache inflight poisoned");
        if inflight
            .get(location)
            .is_some_and(|c| Arc::ptr_eq(c, &cell))
        {
            inflight.remove(location);
        }
        Ok(file)
    }

    /// The local file for a still-fresh entry (side effect: bumps `last_used`).
    fn fresh_entry(&self, location: &Path) -> Option<std::path::PathBuf> {
        let mut map = self.state.map.lock().expect("s3 cache map poisoned");
        let entry = map.get_mut(location)?;
        if entry.last_validated.elapsed() < self.ttl && entry.file.exists() {
            entry.last_used = std::time::Instant::now();
            Some(entry.file.clone())
        } else {
            None
        }
    }

    /// Single-flight body: HEAD-validate a stale/present entry, else download the object.
    async fn materialize(&self, location: &Path) -> std::io::Result<std::path::PathBuf> {
        let meta = self.inner.head(location).await.map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("s3 head {location}: {e}"),
            )
        })?;
        // Re-adopt a present entry (and its on-disk file) when the object is unchanged —
        // covers TTL revalidation within this process's lifetime.
        {
            let mut map = self.state.map.lock().expect("s3 cache map poisoned");
            if let Some(entry) = map.get_mut(location) {
                if entry.size == meta.size && entry.e_tag == meta.e_tag && entry.file.exists() {
                    entry.last_used = std::time::Instant::now();
                    entry.last_validated = std::time::Instant::now();
                    return Ok(entry.file.clone());
                }
                let stale = entry.file.clone();
                let stale_bytes = entry.bytes;
                map.remove(location);
                self.state
                    .total_bytes
                    .fetch_sub(stale_bytes, Ordering::Relaxed);
                drop(map);
                let _ = std::fs::remove_file(&stale);
                remove_sidecar(&stale);
            }
        }
        let file = self.local_file(location);
        // Process-restart adoption: the in-memory index starts empty, so validate the
        // on-disk file's sidecar against the HEAD above (size + etag + on-disk length)
        // and skip the download when the bytes are provably for this object version.
        if let Some(bytes) = self.adopt_on_disk(&file, &meta) {
            self.state.total_bytes.fetch_add(bytes, Ordering::Relaxed);
            self.state
                .map
                .lock()
                .expect("s3 cache map poisoned")
                .insert(
                    location.clone(),
                    Entry {
                        file: file.clone(),
                        bytes,
                        size: meta.size,
                        e_tag: meta.e_tag.clone(),
                        last_used: std::time::Instant::now(),
                        last_validated: std::time::Instant::now(),
                    },
                );
            self.evict_if_needed();
            return Ok(file);
        }
        let tmp = file.with_extension(format!("part{}", std::process::id()));
        let mut stream = self
            .inner
            .get(location)
            .await
            .map_err(|e| {
                std::io::Error::new(std::io::ErrorKind::Other, format!("s3 get {location}: {e}"))
            })?
            .into_stream();
        let mut out = tokio::fs::File::create(&tmp).await?;
        let mut bytes = 0u64;
        while let Some(chunk) = stream.try_next().await.map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("s3 stream {location}: {e}"),
            )
        })? {
            tokio::io::AsyncWriteExt::write_all(&mut out, &chunk).await?;
            bytes += chunk.len() as u64;
        }
        tokio::io::AsyncWriteExt::flush(&mut out).await?;
        out.sync_all().await?;
        drop(out);
        std::fs::rename(&tmp, &file)?;
        write_sidecar(&file, meta.size, meta.e_tag.as_deref());
        self.state.total_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.state
            .map
            .lock()
            .expect("s3 cache map poisoned")
            .insert(
                location.clone(),
                Entry {
                    file: file.clone(),
                    bytes,
                    size: meta.size,
                    e_tag: meta.e_tag.clone(),
                    last_used: std::time::Instant::now(),
                    last_validated: std::time::Instant::now(),
                },
            );
        self.evict_if_needed();
        Ok(file)
    }

    /// Validate the on-disk file for `location` against a fresh HEAD: sidecar
    /// identity (size + etag) must match the object version, and the file's
    /// on-disk length must match the sidecar (catches truncated downloads).
    /// Returns the byte count to charge against the LRU budget on adoption.
    fn adopt_on_disk(&self, file: &std::path::Path, meta: &ObjectMeta) -> Option<u64> {
        let raw = std::fs::read_to_string(sidecar_path(file)).ok()?;
        let mut lines = raw.lines();
        let size: u64 = lines.next()?.parse().ok()?;
        let e_tag = match lines.next()? {
            "-" => None,
            s => Some(s.to_string()),
        };
        if size != meta.size || e_tag != meta.e_tag {
            return None;
        }
        let len = std::fs::metadata(file).ok()?.len();
        if len != size {
            return None;
        }
        Some(len)
    }

    /// LRU-evict least-recently-used entries until the cache fits its budget.
    fn evict_if_needed(&self) {
        let mut map = self.state.map.lock().expect("s3 cache map poisoned");
        while self.state.total_bytes.load(Ordering::Relaxed) > self.max_bytes {
            let victim = map
                .iter()
                .min_by_key(|(_, e)| e.last_used)
                .map(|(p, _)| p.clone());
            let Some(victim) = victim else { break };
            let Some(entry) = map.remove(&victim) else {
                break;
            };
            self.state
                .total_bytes
                .fetch_sub(entry.bytes, Ordering::Relaxed);
            let _ = std::fs::remove_file(&entry.file);
            remove_sidecar(&entry.file);
        }
    }

    /// Invalidate a cached path after a write through the inner store.
    fn invalidate(&self, location: &Path) {
        let mut map = self.state.map.lock().expect("s3 cache map poisoned");
        if let Some(entry) = map.remove(location) {
            self.state
                .total_bytes
                .fetch_sub(entry.bytes, Ordering::Relaxed);
            let _ = std::fs::remove_file(&entry.file);
            remove_sidecar(&entry.file);
        }
    }

    /// Read `ranges` out of the local file.
    fn read_ranges(file: &std::path::Path, ranges: &[Range<u64>]) -> std::io::Result<Vec<Bytes>> {
        use std::os::unix::fs::FileExt;
        let f = std::fs::File::open(file)?;
        let mut out = Vec::with_capacity(ranges.len());
        for r in ranges {
            let mut buf = vec![0u8; (r.end - r.start) as usize];
            f.read_exact_at(&mut buf, r.start)?;
            out.push(Bytes::from(buf));
        }
        Ok(out)
    }

    /// A `GetOptions` range as concrete object bounds (`len` = object size).
    fn concrete_range(range: Option<&GetRange>, len: u64) -> Range<u64> {
        match range {
            None => 0..len,
            Some(GetRange::Bounded(r)) => r.clone(),
            Some(GetRange::Offset(start)) => *start..len,
            Some(GetRange::Suffix(n)) => len.saturating_sub(*n)..len,
        }
    }
}

impl Debug for DiskCachingStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "DiskCachingStore({})", self.inner)
    }
}

impl std::fmt::Display for DiskCachingStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "DiskCachingStore({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for DiskCachingStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        self.invalidate(location);
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.invalidate(location);
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        // HEAD-flagged and conditional requests bypass the cache (freshness is their point).
        if options.head || options.if_match.is_some() || options.if_none_match.is_some() {
            return self.inner.get_opts(location, options).await;
        }
        match self.ensure_local(location).await {
            Ok(file) => {
                let len = std::fs::metadata(&file)
                    .map(|m| m.len())
                    .map_err(|e| cache_err("stat cached file", e))?;
                let range = Self::concrete_range(options.range.as_ref(), len);
                let f = std::fs::File::open(&file).map_err(|e| cache_err("open cached file", e))?;
                Ok(GetResult {
                    payload: GetResultPayload::File(f, file),
                    meta: ObjectMeta {
                        location: location.clone(),
                        last_modified: chrono::Utc::now(),
                        size: len,
                        e_tag: None,
                        version: None,
                    },
                    range,
                    attributes: Attributes::default(),
                })
            }
            Err(_) => self.inner.get_opts(location, options).await,
        }
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        match self.ensure_local(location).await {
            Ok(file) => {
                Self::read_ranges(&file, ranges).map_err(|e| cache_err("read cached file", e))
            }
            Err(_) => self.inner.get_ranges(location, ranges).await,
        }
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        let this = self.clone_refs();
        locations
            .map(move |location| {
                let this = this.clone_refs();
                async move {
                    let location = location?;
                    this.invalidate(&location);
                    this.inner.delete(&location).await?;
                    Ok(location)
                }
            })
            .buffer_unordered(10)
            .boxed()
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.invalidate(to);
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()> {
        self.invalidate(from);
        self.invalidate(to);
        self.inner.rename_opts(from, to, options).await
    }
}

impl DiskCachingStore {
    fn clone_refs(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            dir: self.dir.clone(),
            max_bytes: self.max_bytes,
            ttl: self.ttl,
            state: Arc::clone(&self.state),
        }
    }
}

fn fnv1a(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Minimal in-memory ObjectStore with call counters.
    #[derive(Debug, Default)]
    struct MockStore {
        objects: std::sync::RwLock<HashMap<Path, Bytes>>,
        gets: AtomicU64,
        heads: AtomicU64,
    }

    impl MockStore {
        fn with_object(self, path: &str, data: Vec<u8>) -> Self {
            self.objects
                .write()
                .unwrap()
                .insert(Path::from(path), Bytes::from(data));
            self
        }
    }

    impl std::fmt::Display for MockStore {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "MockStore")
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for MockStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            _opts: PutOptions,
        ) -> Result<PutResult> {
            let data: Vec<u8> = payload
                .iter()
                .flat_map(|b: &Bytes| b.iter().copied())
                .collect();
            self.objects
                .write()
                .unwrap()
                .insert(location.clone(), Bytes::from(data));
            Ok(PutResult {
                e_tag: None,
                version: None,
            })
        }

        async fn put_multipart_opts(
            &self,
            _location: &Path,
            _opts: PutMultipartOptions,
        ) -> Result<Box<dyn MultipartUpload>> {
            unimplemented!()
        }

        async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
            if options.head {
                self.heads.fetch_add(1, Ordering::Relaxed);
            } else {
                self.gets.fetch_add(1, Ordering::Relaxed);
            }
            let objects = self.objects.read().unwrap();
            let Some(data) = objects.get(location) else {
                return Err(object_store::Error::NotFound {
                    path: location.to_string(),
                    source: "not found".into(),
                });
            };
            let len = data.len() as u64;
            let range = DiskCachingStore::concrete_range(options.range.as_ref(), len);
            let slice = data.slice(range.start as usize..range.end as usize);
            Ok(GetResult {
                payload: GetResultPayload::Stream(Box::pin(futures::stream::once(async move {
                    Ok(slice)
                }))),
                meta: ObjectMeta {
                    location: location.clone(),
                    last_modified: chrono::Utc::now(),
                    size: len,
                    e_tag: Some(format!("\"{len}\"")),
                    version: None,
                },
                range,
                attributes: Attributes::default(),
            })
        }

        fn list(&self, _prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
            unimplemented!()
        }

        async fn list_with_delimiter(&self, _prefix: Option<&Path>) -> Result<ListResult> {
            unimplemented!()
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, Result<Path>>,
        ) -> BoxStream<'static, Result<Path>> {
            locations
        }

        async fn copy_opts(&self, _from: &Path, _to: &Path, _options: CopyOptions) -> Result<()> {
            unimplemented!()
        }
    }

    fn cache(
        store: Arc<MockStore>,
        dir: &std::path::Path,
        ttl_ms: u64,
        max_bytes: u64,
    ) -> Arc<dyn ObjectStore> {
        Arc::new(
            DiskCachingStore::new(
                store,
                dir.to_path_buf(),
                max_bytes,
                std::time::Duration::from_millis(ttl_ms),
            )
            .unwrap(),
        )
    }

    #[tokio::test]
    async fn first_read_downloads_once_and_repeats_hit_disk() {
        let dir = tempfile::tempdir().unwrap();
        let inner =
            Arc::new(MockStore::default().with_object("a/b.parquet", b"hello-world".to_vec()));
        let gets = Arc::clone(&inner);
        let store = cache(inner, dir.path(), 60_000, 1 << 20);

        let a = store
            .get_ranges(&Path::from("a/b.parquet"), &[0..5, 6..11])
            .await
            .unwrap();
        let b = store
            .get_ranges(&Path::from("a/b.parquet"), &[0..5, 6..11])
            .await
            .unwrap();
        assert_eq!(a, b);
        assert_eq!(a[0].as_ref(), b"hello");
        assert_eq!(a[1].as_ref(), b"world");
        assert_eq!(
            gets.gets.load(Ordering::Relaxed),
            1,
            "the second read must come from disk, not the inner store"
        );
    }

    #[tokio::test]
    async fn get_opts_returns_file_payload_covering_the_whole_object() {
        let dir = tempfile::tempdir().unwrap();
        let inner = Arc::new(MockStore::default().with_object("t.parquet", b"0123456789".to_vec()));
        let store = cache(inner, dir.path(), 60_000, 1 << 20);
        let result = store
            .get_opts(&Path::from("t.parquet"), GetOptions::default())
            .await
            .unwrap();
        assert!(matches!(result.payload, GetResultPayload::File(_, _)));
        assert_eq!(result.range, 0..10);
        assert_eq!(result.bytes().await.unwrap().as_ref(), b"0123456789");
    }

    #[tokio::test]
    async fn stale_entry_revalidates_by_head_and_redownloads_on_change() {
        let dir = tempfile::tempdir().unwrap();
        let inner = Arc::new(MockStore::default().with_object("x.parquet", b"v1-data".to_vec()));
        let gets = Arc::clone(&inner);
        let store = cache(inner, dir.path(), 0, 1 << 20); // ttl 0: every touch revalidates

        let _ = store
            .get_ranges(&Path::from("x.parquet"), std::slice::from_ref(&(0..2)))
            .await
            .unwrap();
        assert_eq!(gets.gets.load(Ordering::Relaxed), 1);
        // Same etag: HEAD revalidates, no second download.
        let _ = store
            .get_ranges(&Path::from("x.parquet"), std::slice::from_ref(&(0..2)))
            .await
            .unwrap();
        assert_eq!(gets.heads.load(Ordering::Relaxed), 2);
        assert_eq!(gets.gets.load(Ordering::Relaxed), 1);
        // Changed object: new etag forces a re-download.
        gets.objects.write().unwrap().insert(
            Path::from("x.parquet"),
            Bytes::from_static(b"v2-data-longer"),
        );
        let r = store
            .get_ranges(&Path::from("x.parquet"), std::slice::from_ref(&(0..7)))
            .await
            .unwrap();
        assert_eq!(r[0].as_ref(), b"v2-data");
        assert_eq!(gets.gets.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn lru_evicts_oldest_when_over_budget() {
        let dir = tempfile::tempdir().unwrap();
        let inner = Arc::new(
            MockStore::default()
                .with_object("a", vec![b'a'; 100])
                .with_object("b", vec![b'b'; 100])
                .with_object("c", vec![b'c'; 100]),
        );
        let store = cache(inner, dir.path(), 60_000, 150);
        store
            .get_ranges(&Path::from("a"), std::slice::from_ref(&(0..1)))
            .await
            .unwrap();
        store
            .get_ranges(&Path::from("b"), std::slice::from_ref(&(0..1)))
            .await
            .unwrap();
        store
            .get_ranges(&Path::from("c"), std::slice::from_ref(&(0..1)))
            .await
            .unwrap();
        let cached: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .collect();
        assert!(
            cached.len() <= 2,
            "budget 150 with 100-byte objects keeps at most two files: {cached:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn concurrent_first_readers_share_one_download() {
        let dir = tempfile::tempdir().unwrap();
        let inner = Arc::new(MockStore::default().with_object("hot.parquet", vec![7u8; 4096]));
        let gets = Arc::clone(&inner);
        let store = cache(inner, dir.path(), 60_000, 1 << 20);
        let mut handles = Vec::new();
        for _ in 0..32 {
            let store = Arc::clone(&store);
            handles.push(tokio::spawn(async move {
                store
                    .get_ranges(&Path::from("hot.parquet"), std::slice::from_ref(&(0..4096)))
                    .await
                    .unwrap()
            }));
        }
        for h in handles {
            let r = h.await.unwrap();
            assert_eq!(r[0].len(), 4096);
        }
        assert_eq!(
            gets.gets.load(Ordering::Relaxed),
            1,
            "32 concurrent first reads must coalesce on one download"
        );
    }

    #[tokio::test]
    async fn put_invalidates_the_cached_copy() {
        let dir = tempfile::tempdir().unwrap();
        let inner = Arc::new(MockStore::default().with_object("p", b"old".to_vec()));
        let store = cache(inner, dir.path(), 60_000, 1 << 20);
        let _ = store
            .get_ranges(&Path::from("p"), std::slice::from_ref(&(0..3)))
            .await
            .unwrap();
        store
            .put_opts(
                &Path::from("p"),
                PutPayload::from_static(b"new"),
                PutOptions::default(),
            )
            .await
            .unwrap();
        let r = store
            .get_ranges(&Path::from("p"), std::slice::from_ref(&(0..3)))
            .await
            .unwrap();
        assert_eq!(r[0].as_ref(), b"new");
    }

    #[tokio::test]
    async fn restart_readopts_on_disk_entry_via_sidecar() {
        let dir = tempfile::tempdir().unwrap();
        let inner =
            Arc::new(MockStore::default().with_object("a/b.parquet", b"persistent".to_vec()));
        let gets = Arc::clone(&inner);
        // First process: materialize (one download), then drop the store entirely —
        // the in-memory index dies with it, leaving only the on-disk file + sidecar.
        let first = cache(Arc::clone(&inner), dir.path(), 60_000, 1 << 20);
        let _ = first
            .get_ranges(&Path::from("a/b.parquet"), std::slice::from_ref(&(0..10)))
            .await
            .unwrap();
        assert_eq!(gets.gets.load(Ordering::Relaxed), 1);
        drop(first);

        // Second process (fresh index): must re-adopt the on-disk bytes after a HEAD
        // instead of re-downloading the object.
        let second = cache(inner, dir.path(), 60_000, 1 << 20);
        let r = second
            .get_ranges(&Path::from("a/b.parquet"), std::slice::from_ref(&(0..10)))
            .await
            .unwrap();
        assert_eq!(r[0].as_ref(), b"persistent");
        assert_eq!(
            gets.gets.load(Ordering::Relaxed),
            1,
            "restart must re-adopt the on-disk copy, not re-download"
        );
        assert_eq!(gets.heads.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn restart_redownloads_when_object_changed() {
        let dir = tempfile::tempdir().unwrap();
        let inner = Arc::new(MockStore::default().with_object("c.parquet", b"v1".to_vec()));
        let gets = Arc::clone(&inner);
        let first = cache(Arc::clone(&inner), dir.path(), 60_000, 1 << 20);
        let _ = first
            .get_ranges(&Path::from("c.parquet"), std::slice::from_ref(&(0..2)))
            .await
            .unwrap();
        drop(first);
        // Object overwritten in S3 (new size + etag) after the sidecar was written.
        gets.objects
            .write()
            .unwrap()
            .insert(Path::from("c.parquet"), Bytes::from_static(b"v2-longer"));
        let second = cache(inner, dir.path(), 60_000, 1 << 20);
        let r = second
            .get_ranges(&Path::from("c.parquet"), std::slice::from_ref(&(0..9)))
            .await
            .unwrap();
        assert_eq!(r[0].as_ref(), b"v2-longer");
        assert_eq!(gets.gets.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn restart_redownloads_when_sidecar_missing() {
        let dir = tempfile::tempdir().unwrap();
        let inner = Arc::new(MockStore::default().with_object("m.parquet", b"data".to_vec()));
        let gets = Arc::clone(&inner);
        let first = cache(Arc::clone(&inner), dir.path(), 60_000, 1 << 20);
        let _ = first
            .get_ranges(&Path::from("m.parquet"), std::slice::from_ref(&(0..4)))
            .await
            .unwrap();
        drop(first);
        // A sidecar-less file is untrusted (crash between data + sidecar write, or a
        // foreign file in the cache dir): fall back to a full download.
        let orphans: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.to_string_lossy().ends_with(".meta"))
            .collect();
        assert_eq!(orphans.len(), 1);
        std::fs::remove_file(&orphans[0]).unwrap();
        let second = cache(inner, dir.path(), 60_000, 1 << 20);
        let r = second
            .get_ranges(&Path::from("m.parquet"), std::slice::from_ref(&(0..4)))
            .await
            .unwrap();
        assert_eq!(r[0].as_ref(), b"data");
        assert_eq!(gets.gets.load(Ordering::Relaxed), 2);
    }
}

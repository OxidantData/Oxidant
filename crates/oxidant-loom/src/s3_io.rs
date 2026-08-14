//! S3 / object-store scan I/O accounting and range-read concurrency (KAN-153).
//!
//! Cold TPC-DS SF100 fact scans are I/O-bound on ranged GETs (facts are single parquet
//! objects larger than [`crate::s3_cache`]'s per-object materialization cap, so they bypass
//! the whole-object cache). This module:
//!
//! 1. **Instruments** `get` / `get_opts` / `get_ranges` wait time vs the rest of the stage
//!    (`s3_wait_ms` / `decode_ms` on the worker stage-summary line).
//! 2. **Raises range-read concurrency** above object_store's hard-coded 10-way
//!    [`object_store::coalesce_ranges`] default via [`ConcurrentRangesStore`] and
//!    `OXIDANT_S3_RANGE_CONCURRENCY`.
//!
//! Follow-up (not in this pass): a ranged/block-level disk cache for objects that exceed
//! `OXIDANT_S3_CACHE_MAX_OBJECT_BYTES`, so repeated fact scans stop re-fetching from S3.

use std::fmt::{Debug, Formatter};
use std::future::Future;
use std::ops::Range;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use futures::stream::{BoxStream, StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions, Result,
    OBJECT_STORE_COALESCE_DEFAULT,
};

/// Default max concurrent coalesced range GETs (object_store hard-codes 10).
pub const DEFAULT_RANGE_CONCURRENCY: usize = 32;

/// Per-task (or scoped) counters for object-store wait vs non-wait work (KAN-153).
///
/// `s3_wait_ns` is **exclusive** wall time: the interval while at least one instrumented
/// object-store call is in flight on this counter set. Concurrent range GETs inside one
/// `get_ranges` therefore count once toward wait, not once per range — so
/// `decode_ms ≈ duration_ms − s3_wait_ms` is a meaningful residual for CPU/decode.
#[derive(Debug, Default)]
pub struct ScanIoStats {
    /// Exclusive object-store wait nanoseconds (see type docs).
    s3_wait_ns: AtomicU64,
    /// Sum of individual request latencies (can exceed exclusive wait under concurrency).
    s3_request_ns: AtomicU64,
    /// Bytes returned by instrumented gets / get_ranges.
    s3_bytes: AtomicU64,
    /// Number of `get` / `get_opts` (non-HEAD) calls.
    s3_gets: AtomicU64,
    /// Number of `get_ranges` calls (each may fan into many HTTP ranges).
    s3_range_calls: AtomicU64,
    /// Number of coalesced HTTP/range fetches issued by [`ConcurrentRangesStore`].
    s3_range_fetches: AtomicU64,
    /// Disk-cache hits served from local NVMe (`OXIDANT_S3_CACHE_DIR`).
    cache_hits: AtomicU64,
    /// Disk-cache misses that triggered materialization.
    cache_misses: AtomicU64,
    /// Objects skipped because they exceed `OXIDANT_S3_CACHE_MAX_OBJECT_BYTES`.
    cache_bypass_too_large: AtomicU64,
    /// In-flight instrumented calls (for exclusive-wait accounting).
    in_flight: AtomicU32,
    /// Instant (as ns since an arbitrary epoch) when `in_flight` went 0→1; 0 = idle.
    wait_started_ns: AtomicU64,
}

/// Snapshot of [`ScanIoStats`] for logging / tests.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ScanIoSnapshot {
    pub s3_wait_ms: u64,
    pub s3_request_ms: u64,
    pub s3_bytes: u64,
    pub s3_gets: u64,
    pub s3_range_calls: u64,
    pub s3_range_fetches: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub cache_bypass_too_large: u64,
}

impl ScanIoStats {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn snapshot(&self) -> ScanIoSnapshot {
        ScanIoSnapshot {
            s3_wait_ms: self.s3_wait_ns.load(Ordering::Relaxed) / 1_000_000,
            s3_request_ms: self.s3_request_ns.load(Ordering::Relaxed) / 1_000_000,
            s3_bytes: self.s3_bytes.load(Ordering::Relaxed),
            s3_gets: self.s3_gets.load(Ordering::Relaxed),
            s3_range_calls: self.s3_range_calls.load(Ordering::Relaxed),
            s3_range_fetches: self.s3_range_fetches.load(Ordering::Relaxed),
            cache_hits: self.cache_hits.load(Ordering::Relaxed),
            cache_misses: self.cache_misses.load(Ordering::Relaxed),
            cache_bypass_too_large: self.cache_bypass_too_large.load(Ordering::Relaxed),
        }
    }

    /// Residual non-wait ms for a stage of `duration`, floored at 0.
    pub fn decode_ms(&self, duration: std::time::Duration) -> u64 {
        let wait_ms = self.s3_wait_ns.load(Ordering::Relaxed) / 1_000_000;
        duration.as_millis().saturating_sub(wait_ms as u128) as u64
    }

    fn begin_wait(&self) {
        if self.in_flight.fetch_add(1, Ordering::AcqRel) == 0 {
            self.wait_started_ns
                .store(monotonic_ns(), Ordering::Release);
        }
    }

    fn end_wait(&self, request_elapsed: std::time::Duration, bytes: u64) {
        self.s3_request_ns
            .fetch_add(request_elapsed.as_nanos() as u64, Ordering::Relaxed);
        self.s3_bytes.fetch_add(bytes, Ordering::Relaxed);
        if self.in_flight.fetch_sub(1, Ordering::AcqRel) == 1 {
            let started = self.wait_started_ns.swap(0, Ordering::AcqRel);
            if started > 0 {
                let now = monotonic_ns();
                if now > started {
                    self.s3_wait_ns.fetch_add(now - started, Ordering::Relaxed);
                }
            }
        }
    }

    pub fn note_range_fetches(&self, fetches: u64) {
        if fetches > 0 {
            self.s3_range_fetches.fetch_add(fetches, Ordering::Relaxed);
        }
    }

    pub fn note_cache_hit(&self) {
        self.cache_hits.fetch_add(1, Ordering::Relaxed);
    }

    pub fn note_cache_miss(&self) {
        self.cache_misses.fetch_add(1, Ordering::Relaxed);
    }

    pub fn note_cache_bypass_too_large(&self) {
        self.cache_bypass_too_large.fetch_add(1, Ordering::Relaxed);
    }
}

fn monotonic_ns() -> u64 {
    // Instant is opaque; convert via a process-start epoch for exclusive-wait math.
    static START: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
    let start = START.get_or_init(Instant::now);
    // `+ 1` keeps this strictly positive. `wait_started_ns` uses 0 as its "no wait in
    // flight" sentinel, and the very first call initializes START and reads it back
    // immediately — on a coarse timebase (Apple Silicon ticks ~41.67ns) those two reads
    // land in the same tick and `elapsed()` is genuinely 0, which `end_wait` would then
    // read as "never started" and silently drop the first S3 wait of the process.
    start.elapsed().as_nanos() as u64 + 1
}

tokio::task_local! {
    static TASK_IO: Arc<ScanIoStats>;
}

/// Install `stats` for the duration of `fut` so instrumented object-store calls attribute
/// to this stage task. Nested scopes replace the outer counter for the inner duration.
pub async fn with_task_io_stats<F, T>(stats: Arc<ScanIoStats>, fut: F) -> T
where
    F: Future<Output = T>,
{
    TASK_IO.scope(stats, fut).await
}

/// Current task's [`ScanIoStats`], if a stage installed one via [`with_task_io_stats`].
pub fn current_task_io_stats() -> Option<Arc<ScanIoStats>> {
    TASK_IO.try_with(Arc::clone).ok()
}

fn record_to_task(f: impl FnOnce(&ScanIoStats)) {
    let _ = TASK_IO.try_with(|stats| f(stats));
}

/// Record a cache hit/miss/bypass against the current task counters (best-effort).
pub fn note_cache_hit() {
    record_to_task(|s| s.note_cache_hit());
}
pub fn note_cache_miss() {
    record_to_task(|s| s.note_cache_miss());
}
pub fn note_cache_bypass_too_large() {
    record_to_task(|s| s.note_cache_bypass_too_large());
}

/// `OXIDANT_S3_RANGE_CONCURRENCY` (default [`DEFAULT_RANGE_CONCURRENCY`]). Values `<1`
/// fall back to the default. Set to `10` to match stock object_store parallelism.
pub fn range_concurrency_from_env() -> usize {
    parse_range_concurrency(
        std::env::var("OXIDANT_S3_RANGE_CONCURRENCY")
            .ok()
            .as_deref(),
    )
}

fn parse_range_concurrency(raw: Option<&str>) -> usize {
    match raw.and_then(|s| s.trim().parse::<usize>().ok()) {
        Some(n) if n >= 1 => n,
        _ => DEFAULT_RANGE_CONCURRENCY,
    }
}

/// `OXIDANT_S3_RANGE_COALESCE_BYTES` (default [`OBJECT_STORE_COALESCE_DEFAULT`] = 1 MiB).
pub fn range_coalesce_from_env() -> u64 {
    parse_range_coalesce(
        std::env::var("OXIDANT_S3_RANGE_COALESCE_BYTES")
            .ok()
            .as_deref(),
    )
}

fn parse_range_coalesce(raw: Option<&str>) -> u64 {
    match raw.and_then(|s| s.trim().parse::<u64>().ok()) {
        Some(n) => n,
        None => OBJECT_STORE_COALESCE_DEFAULT,
    }
}

/// Merge overlapping / near ranges the same way object_store's `coalesce_ranges` does.
fn merge_ranges(ranges: &[Range<u64>], coalesce: u64) -> Vec<Range<u64>> {
    if ranges.is_empty() {
        return Vec::new();
    }
    let mut ranges = ranges.to_vec();
    ranges.sort_unstable_by_key(|r| r.start);
    let mut out = Vec::with_capacity(ranges.len());
    let mut start_idx = 0;
    let mut end_idx = 1;
    while start_idx != ranges.len() {
        let mut range_end = ranges[start_idx].end;
        while end_idx != ranges.len()
            && ranges[end_idx]
                .start
                .checked_sub(range_end)
                .map(|delta| delta <= coalesce)
                .unwrap_or(true)
        {
            range_end = range_end.max(ranges[end_idx].end);
            end_idx += 1;
        }
        out.push(ranges[start_idx].start..range_end);
        start_idx = end_idx;
        end_idx += 1;
    }
    out
}

/// Fetch `ranges` with configurable coalesce distance and concurrency (KAN-153).
///
/// Same coalesce semantics as [`object_store::coalesce_ranges`], but parallelism is not
/// hard-capped at 10.
pub async fn get_ranges_concurrent<F, Fut>(
    ranges: &[Range<u64>],
    fetch: F,
    coalesce: u64,
    concurrency: usize,
) -> Result<Vec<Bytes>>
where
    F: FnMut(Range<u64>) -> Fut + Send,
    Fut: Future<Output = Result<Bytes>> + Send,
{
    let concurrency = concurrency.max(1);
    let fetch_ranges = merge_ranges(ranges, coalesce);
    // Fan-out count only — [`InstrumentedStore`] owns `s3_range_calls` (one per API call).
    record_to_task(|s| s.note_range_fetches(fetch_ranges.len() as u64));

    let fetched: Vec<Bytes> = futures::stream::iter(fetch_ranges.iter().cloned())
        .map(fetch)
        .buffered(concurrency)
        .try_collect()
        .await?;

    Ok(ranges
        .iter()
        .map(|range| {
            let idx = fetch_ranges.partition_point(|v| v.start <= range.start) - 1;
            let fetch_range = &fetch_ranges[idx];
            let fetch_bytes = &fetched[idx];
            let start = (range.start - fetch_range.start) as usize;
            let end = ((range.end - fetch_range.start) as usize).min(fetch_bytes.len());
            fetch_bytes.slice(start..end)
        })
        .collect())
}

/// Object-store wrapper that times reads into the current task's [`ScanIoStats`].
pub struct InstrumentedStore {
    inner: Arc<dyn ObjectStore>,
}

impl InstrumentedStore {
    pub fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self { inner }
    }

    pub fn wrap(inner: Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore> {
        Arc::new(Self::new(inner))
    }
}

impl Debug for InstrumentedStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "InstrumentedStore({})", self.inner)
    }
}

impl std::fmt::Display for InstrumentedStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "InstrumentedStore({})", self.inner)
    }
}

async fn timed_call<T>(
    bytes_of: impl FnOnce(&T) -> u64,
    fut: impl Future<Output = Result<T>>,
) -> Result<T> {
    let stats = current_task_io_stats();
    if let Some(ref s) = stats {
        s.begin_wait();
    }
    let start = Instant::now();
    let result = fut.await;
    let elapsed = start.elapsed();
    if let Some(s) = stats {
        match &result {
            Ok(v) => s.end_wait(elapsed, bytes_of(v)),
            Err(_) => s.end_wait(elapsed, 0),
        }
    }
    result
}

#[async_trait::async_trait]
impl ObjectStore for InstrumentedStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        // HEAD / conditional freshness checks are not scan payload I/O.
        if options.head || options.if_match.is_some() || options.if_none_match.is_some() {
            return self.inner.get_opts(location, options).await;
        }
        let result = timed_call(
            |r: &GetResult| r.meta.size.min(r.range.end.saturating_sub(r.range.start)),
            self.inner.get_opts(location, options),
        )
        .await?;
        record_to_task(|s| {
            s.s3_gets.fetch_add(1, Ordering::Relaxed);
        });
        Ok(result)
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        let total: u64 = ranges.iter().map(|r| r.end.saturating_sub(r.start)).sum();
        let result = timed_call(|_| total, self.inner.get_ranges(location, ranges)).await?;
        record_to_task(|s| {
            s.s3_range_calls.fetch_add(1, Ordering::Relaxed);
        });
        Ok(result)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()> {
        self.inner.rename_opts(from, to, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        self.inner.delete_stream(locations)
    }
}

/// Object-store wrapper that overrides [`ObjectStore::get_ranges`] with configurable
/// coalesce + concurrency (KAN-153). Other methods delegate unchanged.
pub struct ConcurrentRangesStore {
    inner: Arc<dyn ObjectStore>,
    concurrency: usize,
    coalesce: u64,
}

impl ConcurrentRangesStore {
    pub fn new(inner: Arc<dyn ObjectStore>, concurrency: usize, coalesce: u64) -> Self {
        Self {
            inner,
            concurrency: concurrency.max(1),
            coalesce,
        }
    }

    /// Wrap with [`range_concurrency_from_env`] / [`range_coalesce_from_env`].
    pub fn from_env(inner: Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore> {
        Arc::new(Self::new(
            inner,
            range_concurrency_from_env(),
            range_coalesce_from_env(),
        ))
    }

    pub fn concurrency(&self) -> usize {
        self.concurrency
    }

    pub fn coalesce(&self) -> u64 {
        self.coalesce
    }
}

impl Debug for ConcurrentRangesStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ConcurrentRangesStore(concurrency={}, coalesce={}, inner={})",
            self.concurrency, self.coalesce, self.inner
        )
    }
}

impl std::fmt::Display for ConcurrentRangesStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ConcurrentRangesStore(concurrency={}, coalesce={})",
            self.concurrency, self.coalesce
        )
    }
}

#[async_trait::async_trait]
impl ObjectStore for ConcurrentRangesStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        let inner = Arc::clone(&self.inner);
        let location = location.clone();
        get_ranges_concurrent(
            ranges,
            move |range| {
                let inner = Arc::clone(&inner);
                let location = location.clone();
                async move { ObjectStoreExt::get_range(inner.as_ref(), &location, range).await }
            },
            self.coalesce,
            self.concurrency,
        )
        .await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()> {
        self.inner.rename_opts(from, to, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        self.inner.delete_stream(locations)
    }
}

/// Compose the production S3 read stack (innermost → outermost):
/// `AmazonS3` → concurrent ranges → instrumented → optional disk cache.
///
/// Disk cache sits outside instrumentation so cache hits do not inflate `s3_wait_ms`;
/// oversized-object bypass still times the ranged GETs on the way through.
pub fn wrap_remote_store(inner: Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore> {
    let store = ConcurrentRangesStore::from_env(inner);
    let store = InstrumentedStore::wrap(store);
    crate::s3_cache::DiskCachingStore::from_env(store)
}

/// Format the KAN-153 fields appended to `Oxidant stage summary:` lines.
pub fn format_stage_io_fields(stats: &ScanIoStats, duration: std::time::Duration) -> String {
    let snap = stats.snapshot();
    let decode_ms = stats.decode_ms(duration);
    format!(
        "s3_wait_ms={} decode_ms={} s3_bytes={} s3_gets={} s3_range_calls={} \
         s3_range_fetches={} cache_hit={} cache_miss={} cache_bypass={}",
        snap.s3_wait_ms,
        decode_ms,
        snap.s3_bytes,
        snap.s3_gets,
        snap.s3_range_calls,
        snap.s3_range_fetches,
        snap.cache_hits,
        snap.cache_misses,
        snap.cache_bypass_too_large,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use object_store::PutPayload;
    use std::time::Duration;

    #[test]
    fn parse_concurrency_defaults_and_clamps() {
        assert_eq!(parse_range_concurrency(None), DEFAULT_RANGE_CONCURRENCY);
        assert_eq!(parse_range_concurrency(Some("")), DEFAULT_RANGE_CONCURRENCY);
        assert_eq!(
            parse_range_concurrency(Some("0")),
            DEFAULT_RANGE_CONCURRENCY
        );
        assert_eq!(parse_range_concurrency(Some("10")), 10);
        assert_eq!(parse_range_concurrency(Some("64")), 64);
        assert_eq!(
            parse_range_concurrency(Some("nope")),
            DEFAULT_RANGE_CONCURRENCY
        );
    }

    #[test]
    fn parse_coalesce_defaults() {
        assert_eq!(parse_range_coalesce(None), OBJECT_STORE_COALESCE_DEFAULT);
        assert_eq!(parse_range_coalesce(Some("4096")), 4096);
        assert_eq!(
            parse_range_coalesce(Some("x")),
            OBJECT_STORE_COALESCE_DEFAULT
        );
    }

    #[test]
    fn merge_ranges_coalesces_near_gaps() {
        let merged = merge_ranges(&[0..100, 150..200, 10_000..10_100], 100);
        assert_eq!(merged, vec![0..200, 10_000..10_100]);
    }

    #[test]
    fn decode_ms_floors_at_zero() {
        let stats = ScanIoStats::new();
        stats.s3_wait_ns.store(5_000_000_000, Ordering::Relaxed); // 5s
        assert_eq!(stats.decode_ms(Duration::from_secs(3)), 0);
        assert_eq!(stats.decode_ms(Duration::from_secs(8)), 3000);
    }

    #[test]
    fn format_stage_io_fields_includes_kan153_keys() {
        let stats = ScanIoStats::new();
        stats.s3_bytes.store(42, Ordering::Relaxed);
        stats.cache_bypass_too_large.store(1, Ordering::Relaxed);
        let line = format_stage_io_fields(&stats, Duration::from_millis(100));
        for needle in ["s3_wait_ms=", "decode_ms=", "s3_bytes=42", "cache_bypass=1"] {
            assert!(line.contains(needle), "missing {needle} in {line}");
        }
    }

    #[tokio::test]
    async fn instrumented_store_attributes_wait_to_task_stats() {
        let mem = Arc::new(InMemory::new());
        let path = Path::from("t.parquet");
        mem.put(&path, PutPayload::from(vec![0u8; 64]))
            .await
            .unwrap();
        let store = InstrumentedStore::wrap(mem);

        let stats = Arc::new(ScanIoStats::new());
        with_task_io_stats(stats.clone(), async {
            let ranges = store
                .get_ranges(&path, &[0..16, 16..32, 32..48])
                .await
                .unwrap();
            assert_eq!(ranges.len(), 3);
            assert_eq!(ranges[0].len(), 16);
        })
        .await;

        let snap = stats.snapshot();
        assert!(snap.s3_range_calls >= 1, "{snap:?}");
        assert_eq!(snap.s3_bytes, 48);
        // NOTE: deliberately no `s3_wait_ns > 0` assertion here. This store is `InMemory`,
        // so the "fetch" is a memcpy that can complete inside one timebase tick — an
        // exclusive wait of 0 is CORRECT, not a bug. Asserting otherwise made this test
        // fail intermittently under full-workspace load. Wait accounting is pinned
        // deterministically by `exclusive_wait_records_a_real_await` below.
    }

    /// `s3_wait_ns` must accumulate across a genuine await. Driven directly through
    /// begin/end so the elapsed interval is a real 5ms, not whatever an in-memory
    /// object store happens to take.
    #[tokio::test]
    async fn exclusive_wait_records_a_real_await() {
        let stats = ScanIoStats::new();
        stats.begin_wait();
        tokio::time::sleep(Duration::from_millis(5)).await;
        stats.end_wait(Duration::from_millis(5), 128);

        assert!(
            stats.s3_wait_ns.load(Ordering::Relaxed) > 0,
            "exclusive wait must be recorded across a real await"
        );
        assert_eq!(stats.snapshot().s3_bytes, 128);
    }

    /// Regression: `wait_started_ns` uses 0 as its "nothing in flight" sentinel, so a
    /// timestamp of 0 would make `end_wait` discard the interval. The first call in a
    /// process initializes the epoch and reads it back within the same timebase tick,
    /// which is exactly when a raw `elapsed()` returns 0.
    #[test]
    fn monotonic_ns_is_never_zero() {
        assert!(monotonic_ns() > 0);
    }

    #[tokio::test]
    async fn concurrent_ranges_schedules_with_configured_parallelism() {
        use std::sync::Mutex;

        /// Store that records how many `get_range` calls overlap.
        #[derive(Debug)]
        struct TrackingStore {
            inner: InMemory,
            in_flight: AtomicU32,
            max_in_flight: Mutex<u32>,
            delay: Duration,
        }

        impl std::fmt::Display for TrackingStore {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                write!(f, "TrackingStore")
            }
        }

        #[async_trait::async_trait]
        impl ObjectStore for TrackingStore {
            async fn put_opts(
                &self,
                location: &Path,
                payload: PutPayload,
                opts: PutOptions,
            ) -> Result<PutResult> {
                self.inner.put_opts(location, payload, opts).await
            }

            async fn put_multipart_opts(
                &self,
                location: &Path,
                opts: PutMultipartOptions,
            ) -> Result<Box<dyn MultipartUpload>> {
                self.inner.put_multipart_opts(location, opts).await
            }

            async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
                let cur = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                {
                    let mut max = self.max_in_flight.lock().unwrap();
                    *max = (*max).max(cur);
                }
                tokio::time::sleep(self.delay).await;
                let result = self.inner.get_opts(location, options).await;
                self.in_flight.fetch_sub(1, Ordering::SeqCst);
                result
            }

            fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
                self.inner.list(prefix)
            }

            async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
                self.inner.list_with_delimiter(prefix).await
            }

            async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
                self.inner.copy_opts(from, to, options).await
            }

            async fn rename_opts(
                &self,
                from: &Path,
                to: &Path,
                options: RenameOptions,
            ) -> Result<()> {
                self.inner.rename_opts(from, to, options).await
            }

            fn delete_stream(
                &self,
                locations: BoxStream<'static, Result<Path>>,
            ) -> BoxStream<'static, Result<Path>> {
                self.inner.delete_stream(locations)
            }
        }

        let tracking = Arc::new(TrackingStore {
            inner: InMemory::new(),
            in_flight: AtomicU32::new(0),
            max_in_flight: Mutex::new(0),
            delay: Duration::from_millis(20),
        });
        let path = Path::from("big.bin");
        // 8 disjoint 1-byte ranges far apart so coalesce does not merge them.
        let mut payload = vec![0u8; 80_000];
        for (i, b) in payload.iter_mut().enumerate() {
            *b = (i % 251) as u8;
        }
        tracking
            .put(&path, PutPayload::from(payload))
            .await
            .unwrap();

        let store = ConcurrentRangesStore::new(tracking.clone(), 4, 0);
        let ranges: Vec<Range<u64>> = (0..8)
            .map(|i| {
                let start = i * 10_000;
                start..start + 1
            })
            .collect();

        let out = store.get_ranges(&path, &ranges).await.unwrap();
        assert_eq!(out.len(), 8);

        let max = *tracking.max_in_flight.lock().unwrap();
        assert!(
            max >= 4,
            "expected at least 4 overlapping range fetches, saw {max}"
        );
        assert!(
            max <= 4,
            "concurrency cap 4 must not be exceeded, saw {max}"
        );
    }

    #[tokio::test]
    async fn higher_concurrency_reduces_wall_time_under_latency() {
        use std::sync::atomic::AtomicU64;

        #[derive(Debug)]
        struct SlowStore {
            inner: InMemory,
            delay_ms: u64,
            fetches: AtomicU64,
        }

        impl std::fmt::Display for SlowStore {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                write!(f, "SlowStore")
            }
        }

        #[async_trait::async_trait]
        impl ObjectStore for SlowStore {
            async fn put_opts(
                &self,
                location: &Path,
                payload: PutPayload,
                opts: PutOptions,
            ) -> Result<PutResult> {
                self.inner.put_opts(location, payload, opts).await
            }

            async fn put_multipart_opts(
                &self,
                location: &Path,
                opts: PutMultipartOptions,
            ) -> Result<Box<dyn MultipartUpload>> {
                self.inner.put_multipart_opts(location, opts).await
            }

            async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
                self.fetches.fetch_add(1, Ordering::Relaxed);
                tokio::time::sleep(Duration::from_millis(self.delay_ms)).await;
                self.inner.get_opts(location, options).await
            }

            fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
                self.inner.list(prefix)
            }

            async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
                self.inner.list_with_delimiter(prefix).await
            }

            async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
                self.inner.copy_opts(from, to, options).await
            }

            async fn rename_opts(
                &self,
                from: &Path,
                to: &Path,
                options: RenameOptions,
            ) -> Result<()> {
                self.inner.rename_opts(from, to, options).await
            }

            fn delete_stream(
                &self,
                locations: BoxStream<'static, Result<Path>>,
            ) -> BoxStream<'static, Result<Path>> {
                self.inner.delete_stream(locations)
            }
        }

        async fn run(concurrency: usize) -> Duration {
            let store = Arc::new(SlowStore {
                inner: InMemory::new(),
                delay_ms: 30,
                fetches: AtomicU64::new(0),
            });
            let path = Path::from("x.bin");
            store
                .put(&path, PutPayload::from(vec![7u8; 64_000]))
                .await
                .unwrap();
            let wrapped = ConcurrentRangesStore::new(store, concurrency, 0);
            // 8 far-apart 1-byte ranges → 8 fetches.
            let ranges: Vec<Range<u64>> = (0..8)
                .map(|i| {
                    let s = i * 8_000;
                    s..s + 1
                })
                .collect();
            let start = Instant::now();
            let _ = wrapped.get_ranges(&path, &ranges).await.unwrap();
            start.elapsed()
        }

        let slow = run(1).await;
        let stock = run(10).await;
        let fast = run(32).await;
        eprintln!(
            "KAN-153 microbench (8 ranges × 30ms artificial latency): \
             concurrency=1 → {slow:?}; concurrency=10 (stock) → {stock:?}; \
             concurrency=32 (default) → {fast:?}"
        );
        // 8 serial 30ms ≈ 240ms; 8-wide ≈ 30ms. Allow generous jitter.
        assert!(
            fast * 2 < slow,
            "concurrency=32 ({fast:?}) should be much faster than concurrency=1 ({slow:?})"
        );
        assert!(
            stock * 2 < slow,
            "concurrency=10 ({stock:?}) should be much faster than concurrency=1 ({slow:?})"
        );
    }

    #[tokio::test]
    async fn with_task_io_stats_isolates_concurrent_tasks() {
        let a = Arc::new(ScanIoStats::new());
        let b = Arc::new(ScanIoStats::new());
        let mem = Arc::new(InMemory::new());
        let path = Path::from("p");
        mem.put(&path, PutPayload::from(vec![1u8; 8]))
            .await
            .unwrap();
        let store = InstrumentedStore::wrap(mem);

        let ((), ()) = tokio::join!(
            with_task_io_stats(a.clone(), async {
                let _ = store.get(&path).await.unwrap();
            }),
            with_task_io_stats(b.clone(), async {
                let _ = store
                    .get_ranges(&path, std::slice::from_ref(&(0..4)))
                    .await
                    .unwrap();
            }),
        );

        let sa = a.snapshot();
        let sb = b.snapshot();
        assert!(sa.s3_gets >= 1, "task A should see get: {sa:?}");
        assert!(sb.s3_range_calls >= 1, "task B should see ranges: {sb:?}");
        assert_eq!(sa.s3_range_calls, 0, "task A must not see B's ranges");
        assert_eq!(sb.s3_gets, 0, "task B must not see A's get");
    }
}

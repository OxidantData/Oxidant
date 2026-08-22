//! Per-process (i.e. per-executor) compiled-model cache.
//!
//! SPIKE (issue #118). A `ScalarUDF` sees the model URI as a literal argument on **every**
//! RecordBatch, so without a cache a 1M-row scan would fetch and compile the model thousands of
//! times. The cache is keyed by `uri` + a version token (S3 ETag / local mtime+size) so
//! republishing a model to the same URI is picked up without a restart.
//!
//! Re-probing that version costs an S3 HEAD, which is still far too expensive per batch, so a
//! successful probe is trusted for `OXIDANT_ML_MODEL_TTL_MS` (default 60s). Same trade the
//! engine already makes for table-size estimates: bounded staleness changes *which* model
//! version scores a query, never whether the query is correct — and a query that spans the TTL
//! boundary can mix versions, which is called out in the report as a reason to prefer a
//! DDL-registered model with a pinned version.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use oxidant_common::Result;

use crate::model::OnnxModel;
use crate::store;

/// What the cache did, for the spike's lifecycle numbers.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct CacheStats {
    pub hits: u64,
    pub misses: u64,
    /// Version probes that actually hit the object store (i.e. were not TTL-suppressed).
    pub stats: u64,
    pub fetch_ms: u64,
    pub compile_ms: u64,
    /// Serialized bytes of every model currently resident.
    pub resident_bytes: u64,
    pub resident_models: u64,
}

#[derive(Default)]
struct Counters {
    hits: AtomicU64,
    misses: AtomicU64,
    stats: AtomicU64,
    fetch_ms: AtomicU64,
    compile_ms: AtomicU64,
}

struct Entry {
    model: Arc<OnnxModel>,
    version: String,
    /// When `version` was last confirmed against the object store.
    checked: Instant,
}

static CACHE: OnceLock<Mutex<HashMap<String, Entry>>> = OnceLock::new();
static COUNTERS: OnceLock<Counters> = OnceLock::new();

fn cache() -> &'static Mutex<HashMap<String, Entry>> {
    CACHE.get_or_init(Default::default)
}

fn counters() -> &'static Counters {
    COUNTERS.get_or_init(Default::default)
}

fn ttl() -> Duration {
    let ms = std::env::var("OXIDANT_ML_MODEL_TTL_MS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(60_000);
    Duration::from_millis(ms)
}

/// Fetch, compile, and cache the model at `uri`; subsequent calls return the same `Arc`.
pub fn get(uri: &str) -> Result<Arc<OnnxModel>> {
    let ttl = ttl();
    // Fast path: a recently-confirmed entry needs no object-store round trip at all.
    {
        let guard = cache().lock().expect("ml model cache poisoned");
        if let Some(entry) = guard.get(uri) {
            if entry.checked.elapsed() < ttl {
                counters().hits.fetch_add(1, Ordering::Relaxed);
                return Ok(entry.model.clone());
            }
        }
    }

    let started = Instant::now();
    let version = store::stat(uri)?.cache_token();
    counters().stats.fetch_add(1, Ordering::Relaxed);

    // Re-check under the lock: the version we just probed may match what is already resident,
    // in which case this is a hit that merely refreshes the TTL.
    {
        let mut guard = cache().lock().expect("ml model cache poisoned");
        if let Some(entry) = guard.get_mut(uri) {
            if entry.version == version {
                entry.checked = Instant::now();
                counters().hits.fetch_add(1, Ordering::Relaxed);
                return Ok(entry.model.clone());
            }
        }
    }

    // Miss. Fetch and compile *outside* the lock — a cold S3 GET of a large model would
    // otherwise stall every other model's lookups on this executor. Two threads racing the
    // same cold URI both do the work and the last writer wins; that wastes one compile, which
    // is cheaper than serializing all loads behind one mutex.
    let bytes = store::fetch(uri)?;
    let fetch_ms = started.elapsed().as_millis() as u64;
    let compile_started = Instant::now();
    let model = Arc::new(OnnxModel::load(&bytes)?);
    counters().fetch_ms.fetch_add(fetch_ms, Ordering::Relaxed);
    counters().compile_ms.fetch_add(
        compile_started.elapsed().as_millis() as u64,
        Ordering::Relaxed,
    );
    counters().misses.fetch_add(1, Ordering::Relaxed);

    let mut guard = cache().lock().expect("ml model cache poisoned");
    guard.insert(
        uri.to_string(),
        Entry {
            model: model.clone(),
            version,
            checked: Instant::now(),
        },
    );
    Ok(model)
}

/// Snapshot of cache activity since process start.
pub fn stats() -> CacheStats {
    let c = counters();
    let guard = cache().lock().expect("ml model cache poisoned");
    CacheStats {
        hits: c.hits.load(Ordering::Relaxed),
        misses: c.misses.load(Ordering::Relaxed),
        stats: c.stats.load(Ordering::Relaxed),
        fetch_ms: c.fetch_ms.load(Ordering::Relaxed),
        compile_ms: c.compile_ms.load(Ordering::Relaxed),
        resident_bytes: guard.values().map(|e| e.model.model_bytes as u64).sum(),
        resident_models: guard.len() as u64,
    }
}

/// Drop every cached model. Spike/test helper — there is no eviction policy yet, which is a
/// gap the report calls out.
pub fn clear() {
    cache().lock().expect("ml model cache poisoned").clear();
}

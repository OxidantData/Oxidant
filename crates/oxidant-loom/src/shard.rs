//! File-list sharding for distributed Glue/Parquet scans.
//!
//! When `OXIDANT_SHARD_INDEX` (or `OXIDANT_POD_NAME`) and `OXIDANT_WORKER_COUNT` are set,
//! each worker opens only its share of listed files. Assignment is **size-weighted**
//! (greedy LPT: largest files first, each to the worker with the least bytes so far;
//! ties broken by lowest worker index). Files are ordered deterministically by
//! `(size desc, path asc)` before assignment. Replicated tables (dimension tables)
//! skip sharding via `OXIDANT_REPLICATED_TABLES` and/or the driver's auto-broadcast
//! classification (task-local overlay from the stage ticket).

use std::collections::HashSet;
use std::future::Future;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::catalog::TableProvider;
use datafusion::datasource::empty::EmptyTable;
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::execution::context::SessionState;
use futures::TryStreamExt;
use object_store::ObjectMeta;
use oxidant_common::{Error, Result};

/// Default auto-broadcast byte cap (32 GiB). Tables at/above this stay sharded even when
/// smaller than the query's largest table. NOTE: KAN-161 briefly lowered this to 4 GiB
/// (v0.1.11) to shard the SF100 mid facts, but the planner did not yet support the
/// resulting multi-sharded shapes — 20/99 TPC-DS queries regressed to STRICT refusals
/// (union-of-sharded-facts, subquery-over-sharded, shuffle-projection key retention,
/// both-sharded window joins). Reverted in v0.1.12; the threshold re-flips only when the
/// multi-sharded shape support (KAN-162) lands with the 99-query classification guard.
pub const DEFAULT_AUTO_BROADCAST_THRESHOLD_BYTES: u64 = 32 * 1024 * 1024 * 1024;

tokio::task_local! {
    /// Driver-classified replicate set for the current stage (lowercase names).
    static REPLICATED_TABLES_CONTEXT: Arc<HashSet<String>>;
    /// Explicit shard assignment for the current task. In-process workers (tests, local
    /// multi-worker harnesses) cannot use the process-global env — `OXIDANT_SHARD_INDEX` holds
    /// one value per process — so a worker built with an explicit assignment installs it
    /// here around stage execution. Live workers never set this; their env is authoritative.
    static SHARD_ASSIGNMENT_CONTEXT: ShardAssignment;
}

/// Shard assignment for this process, if configured for a multi-worker cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShardAssignment {
    pub index: usize,
    pub count: usize,
}

/// Test-only gate serializing process-global shard-env mutation against readers.
/// `from_env` reads `OXIDANT_WORKER_COUNT`/`OXIDANT_SHARD_INDEX` on EVERY catalog listing, so
/// a test that sets them (`explicit_assignment_task_local_wins_over_env`) holds the
/// write side for its whole env window; every other test thread's `from_env` then waits
/// the window out instead of listing a shard it should not (the
/// `without_declared_schema_merge_fails` flake: a two-file fixture listing lost the
/// file the leaked {0/2} assignment gave worker 1, so the Int32/Int64 merge conflict
/// the test expects never materialized). Compiled out of non-test builds — production
/// workers set the env once at boot and never mutate it.
#[cfg(test)]
static SHARD_ENV_GATE: std::sync::RwLock<()> = std::sync::RwLock::new(());

#[cfg(test)]
thread_local! {
    /// Set while this thread holds the write side of [`SHARD_ENV_GATE`]: the holder's
    /// own `from_env` calls must bypass the read side — a `RwLock` read under a held
    /// write deadlocks.
    static SHARD_ENV_GATE_HELD: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Read side of [`SHARD_ENV_GATE`] for [`ShardAssignment::from_env`]. Poison-tolerant:
/// a panicking env-mutating test must not fail every later `from_env` caller.
#[cfg(test)]
fn shard_env_read_guard() -> Option<std::sync::RwLockReadGuard<'static, ()>> {
    if SHARD_ENV_GATE_HELD.with(|held| held.get()) {
        None
    } else {
        Some(
            SHARD_ENV_GATE
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
        )
    }
}

/// The write side of [`SHARD_ENV_GATE`]: tests hold one for the whole span they have
/// `OXIDANT_WORKER_COUNT`/`OXIDANT_SHARD_INDEX` set.
#[cfg(test)]
pub(crate) struct ShardEnvWriteGuard {
    _guard: std::sync::RwLockWriteGuard<'static, ()>,
}

#[cfg(test)]
impl ShardEnvWriteGuard {
    pub(crate) fn take() -> Self {
        let guard = SHARD_ENV_GATE
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        SHARD_ENV_GATE_HELD.with(|held| held.set(true));
        Self { _guard: guard }
    }
}

#[cfg(test)]
impl Drop for ShardEnvWriteGuard {
    fn drop(&mut self) {
        SHARD_ENV_GATE_HELD.with(|held| held.set(false));
    }
}

impl ShardAssignment {
    pub fn from_env() -> Option<Self> {
        // Test builds: wait out any in-flight shard-env mutation (`SHARD_ENV_GATE`).
        #[cfg(test)]
        let _gate = shard_env_read_guard();

        // A task-scoped explicit assignment (in-process workers / tests) wins over the
        // process-global env, which can only name one shard per process.
        if let Ok(assignment) = SHARD_ASSIGNMENT_CONTEXT.try_with(|a| *a) {
            return Some(assignment);
        }
        let count: usize = std::env::var("OXIDANT_WORKER_COUNT")
            .ok()
            .and_then(|s| s.parse().ok())
            .filter(|&n| n > 1)?;

        let index = if let Ok(s) = std::env::var("OXIDANT_SHARD_INDEX") {
            s.parse().ok()?
        } else if let Ok(name) = std::env::var("OXIDANT_POD_NAME") {
            // StatefulSet: oxidant-<cluster>-worker-0
            name.rsplit('-').next()?.parse().ok()?
        } else {
            return None;
        };

        if index >= count {
            eprintln!(
                "oxidant-loom: OXIDANT_SHARD_INDEX {index} >= OXIDANT_WORKER_COUNT {count}; ignoring shard config"
            );
            return None;
        }
        Some(Self { index, count })
    }
}

/// Greedy LPT shard assignment: largest file first (path tie-break), each file goes to
/// the worker with the least total bytes assigned so far (lowest index on tie).
///
/// Returns one worker index per input file, in the same order as `files`.
/// Documented balance bound: `max(worker_bytes) - min(worker_bytes) <= largest_file_size`.
fn assign_files_by_size(
    files: &[(ListingTableUrl, ObjectMeta)],
    worker_count: usize,
) -> Vec<usize> {
    let known_sizes = files
        .iter()
        .map(|(url, meta)| (url.clone(), meta.size))
        .collect::<Vec<_>>();
    assign_known_files_by_size(&known_sizes, worker_count)
}

fn assign_known_files_by_size(files: &[(ListingTableUrl, u64)], worker_count: usize) -> Vec<usize> {
    if files.is_empty() {
        return Vec::new();
    }
    debug_assert!(worker_count > 0);

    let mut order: Vec<usize> = (0..files.len()).collect();
    order.sort_by(|&a, &b| {
        files[b]
            .1
            .cmp(&files[a].1)
            .then_with(|| files[a].0.as_str().cmp(files[b].0.as_str()))
    });

    let mut worker_bytes = vec![0u64; worker_count];
    let mut assignments = vec![0usize; files.len()];

    for file_idx in order {
        let size = files[file_idx].1;
        let worker = worker_bytes
            .iter()
            .enumerate()
            .min_by_key(|(i, &bytes)| (bytes, *i))
            .map(|(i, _)| i)
            .unwrap_or(0);
        assignments[file_idx] = worker;
        worker_bytes[worker] = worker_bytes[worker].saturating_add(size);
    }

    assignments
}

/// Parse a comma-separated table-name list into lowercase names (empty tokens dropped).
pub fn parse_replicated_tables_csv(csv: &str) -> Vec<String> {
    csv.split(',')
        .map(str::trim)
        .filter(|t| !t.is_empty())
        .map(|t| t.to_ascii_lowercase())
        .collect()
}

/// Optional operator force-include list (`OXIDANT_REPLICATED_TABLES`). Auto-broadcast is the
/// primary path; this env only adds names on top.
pub fn replicated_tables_override_from_env() -> Vec<String> {
    std::env::var("OXIDANT_REPLICATED_TABLES")
        .ok()
        .map(|s| parse_replicated_tables_csv(&s))
        .unwrap_or_default()
}

/// Tables that every worker should scan fully (broadcast / dimension tables).
///
/// Union of the optional env override and the stage ticket's task-local set (when installed).
pub fn replicated_table_names() -> Vec<String> {
    let mut names = replicated_tables_override_from_env();
    if let Ok(extra) =
        REPLICATED_TABLES_CONTEXT.try_with(|set| set.iter().cloned().collect::<Vec<_>>())
    {
        for name in extra {
            if !names.iter().any(|n| n == &name) {
                names.push(name);
            }
        }
    }
    names
}

pub fn is_replicated_table(table_name: &str) -> bool {
    let needle = table_name.to_ascii_lowercase();
    if replicated_tables_override_from_env()
        .iter()
        .any(|t| t == &needle)
    {
        return true;
    }
    REPLICATED_TABLES_CONTEXT
        .try_with(|set| set.contains(&needle))
        .unwrap_or(false)
}

/// Run `future` with a stage-scoped replicate set (comma-separated names). Empty csv installs
/// an empty overlay so env override remains the only force-include source.
pub async fn with_replicated_tables<F, T>(csv: &str, future: F) -> T
where
    F: Future<Output = T>,
{
    let set: HashSet<String> = parse_replicated_tables_csv(csv).into_iter().collect();
    REPLICATED_TABLES_CONTEXT.scope(Arc::new(set), future).await
}

/// Run `future` with an explicit shard assignment (see [`SHARD_ASSIGNMENT_CONTEXT`]).
/// In-process workers install this around stage execution so each worker resolves its own
/// file shard despite sharing one process env; production workers leave it unset and read
/// `OXIDANT_WORKER_COUNT` / `OXIDANT_SHARD_INDEX` via [`ShardAssignment::from_env`].
pub async fn with_shard_assignment<F, T>(assignment: ShardAssignment, future: F) -> T
where
    F: Future<Output = T>,
{
    SHARD_ASSIGNMENT_CONTEXT.scope(assignment, future).await
}

/// Byte cap for auto-broadcast (`OXIDANT_AUTO_BROADCAST_THRESHOLD_BYTES`). Default 32 GiB.
/// `0` disables size-based auto-replication (env override only).
pub fn auto_broadcast_threshold_bytes() -> u64 {
    match std::env::var("OXIDANT_AUTO_BROADCAST_THRESHOLD_BYTES") {
        Ok(s) => s
            .trim()
            .parse()
            .unwrap_or(DEFAULT_AUTO_BROADCAST_THRESHOLD_BYTES),
        Err(_) => DEFAULT_AUTO_BROADCAST_THRESHOLD_BYTES,
    }
}

/// Classify which scanned tables should be fully replicated on every worker.
///
/// - Always include `override_names` (operator force-include).
/// - Among tables with known sizes: let `max` be the maximum known size; auto-replicate `t`
///   when `size(t) < max` and `size(t) <= threshold`.
/// - Unknown sizes and tables at `max` stay sharded (unless overridden).
/// - `threshold == 0` disables auto (override only).
pub fn classify_replicated_tables(
    sized: &[(String, Option<u64>)],
    override_names: &[&str],
    threshold: u64,
) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let mut push = |name: &str| {
        let lower = name.to_ascii_lowercase();
        if !out.iter().any(|n| n == &lower) {
            out.push(lower);
        }
    };
    for name in override_names {
        if !name.trim().is_empty() {
            push(name.trim());
        }
    }
    if threshold == 0 {
        return out;
    }
    let max = sized.iter().filter_map(|(_, s)| *s).max();
    let Some(max) = max else {
        return out;
    };
    for (name, size) in sized {
        let Some(sz) = size else {
            continue;
        };
        if *sz < max && *sz <= threshold {
            push(name);
        }
    }
    out
}

/// Row-count multiple for row-aware auto-broadcast exclusion
/// (`OXIDANT_REPLICATE_MAX_ROW_MULTIPLE`). **On by default at `4.0`**: a byte-eligible replicate
/// candidate whose row count exceeds multiple × the largest (by bytes) table's row count stays
/// sharded — see [`classify_replicated_tables_with_rows`]. Set the env to a different positive
/// value to tune, or to `0` / a negative / unparseable value to disable the rule and restore
/// byte-only classification.
///
/// Defaulting ON is safe because the shuffle-join-chain planner re-roots a dim-leftmost inner
/// chain at a sharded leaf (`join_order::reroot_inner_chain_at_sharded`): TPC-DS Q37/Q82's
/// `item`-first comma chains used to reject the resulting 2-sharded classification and fall
/// back to single-node execution, which is why the rule originally shipped gated off.
pub fn replicate_max_row_multiple() -> Option<f64> {
    const DEFAULT_MULTIPLE: f64 = 4.0;
    match std::env::var("OXIDANT_REPLICATE_MAX_ROW_MULTIPLE") {
        Ok(s) => s.trim().parse::<f64>().ok().filter(|m| *m > 0.0),
        Err(_) => Some(DEFAULT_MULTIPLE),
    }
}

/// Row-aware variant of [`classify_replicated_tables`].
///
/// `rows` is parallel to `sized` (entry `i` is the row-count estimate for `sized[i]`;
/// `None` = unknown). When `max_row_multiple` is `Some(m)` and the largest-by-bytes table's
/// row count is known, an auto-replicated candidate whose own row count exceeds `m ×` the
/// largest table's is EXCLUDED from the replicate set (kept sharded) even though its bytes
/// are under `threshold`. Bytes are the wrong axis for scan cost here: a replicated table is
/// re-scanned in full on every worker, so its row count — not its compressed size — bounds
/// the per-worker scan (TPC-DS SF10 `inventory`: 117M rows in ~0.5 GB parquet vs the
/// 14.4M-row byte-largest `catalog_sales`).
///
/// Byte-for-byte identical to [`classify_replicated_tables`] when `max_row_multiple` is
/// `None` (or non-positive), when the largest table's row count is unknown, or per candidate
/// when that candidate's row count is unknown. `override_names` always win. A candidate at
/// exactly `m ×` the largest table's rows still replicates (the rule fires on strictly
/// greater).
pub fn classify_replicated_tables_with_rows(
    sized: &[(String, Option<u64>)],
    rows: &[Option<u64>],
    override_names: &[&str],
    threshold: u64,
    max_row_multiple: Option<f64>,
) -> Vec<String> {
    let out = classify_replicated_tables(sized, override_names, threshold);
    let Some(multiple) = max_row_multiple.filter(|m| *m > 0.0) else {
        return out;
    };
    // The anchor is the largest-by-bytes table — the one the byte rule always keeps sharded.
    let Some((anchor_idx, _)) = sized
        .iter()
        .enumerate()
        .filter_map(|(i, (_, s))| s.map(|sz| (i, sz)))
        .max_by_key(|(_, sz)| *sz)
    else {
        return out;
    };
    let Some(anchor_rows) = rows.get(anchor_idx).copied().flatten() else {
        return out;
    };
    let overrides: Vec<String> = override_names
        .iter()
        .map(|n| n.trim().to_ascii_lowercase())
        .filter(|n| !n.is_empty())
        .collect();
    out.into_iter()
        .filter(|name| {
            if overrides.iter().any(|o| o == name) {
                return true; // force-include wins
            }
            let Some(idx) = sized.iter().position(|(n, _)| n.eq_ignore_ascii_case(name)) else {
                return true;
            };
            match rows.get(idx).copied().flatten() {
                // Unknown candidate rows → keep the byte-rule decision for this table.
                None => true,
                Some(candidate_rows) => (candidate_rows as f64) <= multiple * (anchor_rows as f64),
            }
        })
        .collect()
}

/// Sum object sizes under `urls` (no shard filter). Used for auto-broadcast sizing.
pub async fn sum_listing_bytes(
    state: &SessionState,
    urls: Vec<ListingTableUrl>,
    file_extension: &str,
) -> Result<u64> {
    let files = list_visible_file_shard_with(state, urls, file_extension, None, None).await?;
    Ok(files.iter().map(|(_, meta)| meta.size).sum())
}

/// Ensure a directory/prefix location ends with `/` so DataFusion treats it as a collection.
pub fn ensure_collection_url(location: &str) -> String {
    let trimmed = location.trim();
    if trimmed.is_empty() || trimmed.ends_with('/') || looks_like_single_file(trimmed) {
        return trimmed.to_string();
    }
    format!("{trimmed}/")
}

fn looks_like_single_file(location: &str) -> bool {
    let base = location.rsplit('/').next().unwrap_or(location);
    base.contains('.') && !base.starts_with('.')
}

/// List files under `urls` and return only this worker's shard (or all URLs when unsharded /
/// replicated). An empty return means this shard owns no files — callers should use
/// [`empty_table`].
pub async fn apply_file_shard(
    state: &SessionState,
    urls: Vec<ListingTableUrl>,
    file_extension: &str,
    table_name: Option<&str>,
) -> Result<Vec<ListingTableUrl>> {
    apply_file_shard_with(
        state,
        urls,
        file_extension,
        table_name,
        ShardAssignment::from_env(),
    )
    .await
}

/// Shard an already-resolved file list using metadata-provided sizes.
///
/// Unlike [`apply_file_shard`], this never lists or heads object-store files. Delta and Iceberg
/// resolvers already carry authoritative sizes in their transaction/manifest metadata, so using
/// those values avoids one remote metadata request per file on every worker.
pub fn apply_known_file_shard(
    files: Vec<(ListingTableUrl, u64)>,
    table_name: Option<&str>,
) -> Vec<(ListingTableUrl, u64)> {
    apply_known_file_shard_with(files, table_name, ShardAssignment::from_env())
}

/// Same as [`apply_known_file_shard`] with an explicit assignment for tests.
pub fn apply_known_file_shard_with(
    files: Vec<(ListingTableUrl, u64)>,
    table_name: Option<&str>,
    assignment: Option<ShardAssignment>,
) -> Vec<(ListingTableUrl, u64)> {
    let Some(assignment) = assignment else {
        return files;
    };
    if table_name.is_some_and(is_replicated_table) {
        return files;
    }

    let file_shards = assign_known_files_by_size(&files, assignment.count);
    files
        .into_iter()
        .enumerate()
        .filter(|(index, _)| file_shards[*index] == assignment.index)
        .map(|(_, file)| file)
        .collect()
}

/// List files once, exclude Spark/Hive metadata paths, and return this worker's size-weighted
/// shard together with the already-fetched object metadata.
pub async fn list_visible_file_shard(
    state: &SessionState,
    urls: Vec<ListingTableUrl>,
    file_extension: &str,
    table_name: Option<&str>,
) -> Result<Vec<(ListingTableUrl, ObjectMeta)>> {
    list_visible_file_shard_with(
        state,
        urls,
        file_extension,
        table_name,
        ShardAssignment::from_env(),
    )
    .await
}

/// Same as [`list_visible_file_shard`] with an explicit assignment for tests.
pub async fn list_visible_file_shard_with(
    state: &SessionState,
    urls: Vec<ListingTableUrl>,
    file_extension: &str,
    table_name: Option<&str>,
    assignment: Option<ShardAssignment>,
) -> Result<Vec<(ListingTableUrl, ObjectMeta)>> {
    let mut files = Vec::new();
    for url in &urls {
        let store_url = url.object_store();
        let store = state
            .runtime_env()
            .object_store(&store_url)
            .map_err(|e| Error::Io(format!("object store for {}: {e}", store_url.as_str())))?;
        let stream = url
            .list_all_files(state as &dyn Session, store.as_ref(), file_extension)
            .await
            .map_err(|e| Error::Io(format!("list files for {}: {e}", url.as_str())))?;
        let metas: Vec<ObjectMeta> = stream
            .try_collect()
            .await
            .map_err(|e| Error::Io(format!("list files stream: {e}")))?;
        for meta in metas {
            if visible_data_path(url, &meta) {
                files.push((object_meta_to_url(url, &meta)?, meta));
            }
        }
    }
    files.sort_by(|a, b| a.1.location.as_ref().cmp(b.1.location.as_ref()));

    let Some(assignment) = assignment else {
        return Ok(files);
    };
    if table_name.is_some_and(is_replicated_table) {
        return Ok(files);
    }
    let assignments = assign_files_by_size(&files, assignment.count);
    Ok(files
        .into_iter()
        .enumerate()
        .filter(|(index, _)| assignments[*index] == assignment.index)
        .map(|(_, file)| file)
        .collect())
}

fn visible_data_path(base: &ListingTableUrl, meta: &ObjectMeta) -> bool {
    let location = meta.location.as_ref();
    let relative = if base.is_collection() {
        let prefix = base.prefix().as_ref().trim_end_matches('/');
        location
            .strip_prefix(prefix)
            .unwrap_or(location)
            .trim_start_matches('/')
    } else {
        location.rsplit('/').next().unwrap_or(location)
    };
    !relative.split('/').any(|segment| {
        segment.starts_with('_')
            || segment.starts_with('.')
            || segment.eq_ignore_ascii_case("metadata")
    })
}

/// Same as [`apply_file_shard`] with an explicit assignment (tests / custom membership).
pub async fn apply_file_shard_with(
    state: &SessionState,
    urls: Vec<ListingTableUrl>,
    file_extension: &str,
    table_name: Option<&str>,
    assignment: Option<ShardAssignment>,
) -> Result<Vec<ListingTableUrl>> {
    let Some(assignment) = assignment else {
        return Ok(urls);
    };
    if let Some(name) = table_name {
        if is_replicated_table(name) {
            return Ok(urls);
        }
    }

    let mut all_files: Vec<(ListingTableUrl, ObjectMeta)> = Vec::new();
    for url in &urls {
        let store_url = url.object_store();
        let store = state
            .runtime_env()
            .object_store(&store_url)
            .map_err(|e| Error::Io(format!("object store for {}: {e}", store_url.as_str())))?;
        let stream = url
            .list_all_files(state as &dyn Session, store.as_ref(), file_extension)
            .await
            .map_err(|e| Error::Io(format!("list files for {}: {e}", url.as_str())))?;
        let metas: Vec<ObjectMeta> = stream
            .try_collect()
            .await
            .map_err(|e| Error::Io(format!("list files stream: {e}")))?;
        for meta in metas {
            let file_url = object_meta_to_url(url, &meta)?;
            all_files.push((file_url, meta));
        }
    }

    if all_files.is_empty() {
        return Ok(urls);
    }

    all_files.sort_by(|a, b| a.1.location.as_ref().cmp(b.1.location.as_ref()));

    let file_shards = assign_files_by_size(&all_files, assignment.count);

    let shard_urls: Vec<ListingTableUrl> = all_files
        .into_iter()
        .enumerate()
        .filter(|(i, _)| file_shards[*i] == assignment.index)
        .map(|(_, (u, _))| u)
        .collect();

    Ok(shard_urls)
}

fn object_meta_to_url(base: &ListingTableUrl, meta: &ObjectMeta) -> Result<ListingTableUrl> {
    let location = meta.location.as_ref();
    if location.starts_with("s3://")
        || location.starts_with("s3a://")
        || location.starts_with("file://")
    {
        return ListingTableUrl::parse(location)
            .map_err(|e| Error::Plan(format!("bad sharded file url `{location}`: {e}")));
    }

    let store = base.object_store();
    let store_str = store.as_str().trim_end_matches('/');
    let loc = location.trim_start_matches('/');
    let full = format!("{store_str}/{loc}");
    ListingTableUrl::parse(&full)
        .map_err(|e| Error::Plan(format!("bad sharded file url `{full}`: {e}")))
}

/// Zero-row provider with a known schema — used when this worker's shard has no files.
pub fn empty_table(schema: SchemaRef) -> Result<Arc<dyn TableProvider>> {
    Ok(Arc::new(EmptyTable::new(schema)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::parquet::arrow::ArrowWriter;
    use datafusion::prelude::SessionContext;
    use object_store::path::Path;
    use object_store::ObjectMeta;

    fn meta(path: &str, size: u64) -> ObjectMeta {
        ObjectMeta {
            location: Path::from(path),
            last_modified: chrono::Utc::now(),
            size,
            e_tag: None,
            version: None,
        }
    }

    fn dummy_url(path: &str) -> ListingTableUrl {
        ListingTableUrl::parse(format!("file:///tmp/{path}")).unwrap()
    }

    fn worker_byte_totals(
        files: &[(ListingTableUrl, ObjectMeta)],
        assignments: &[usize],
        worker_count: usize,
    ) -> Vec<u64> {
        let mut totals = vec![0u64; worker_count];
        for (file, &worker) in files.iter().zip(assignments) {
            totals[worker] = totals[worker].saturating_add(file.1.size);
        }
        totals
    }

    #[test]
    fn size_weighted_assignment_is_deterministic() {
        let files = vec![
            (dummy_url("a"), meta("a", 100)),
            (dummy_url("b"), meta("b", 50)),
            (dummy_url("c"), meta("c", 50)),
        ];
        let a = assign_files_by_size(&files, 2);
        let b = assign_files_by_size(&files, 2);
        assert_eq!(a, b);
        assert_eq!(a, vec![0, 1, 1]);
    }

    #[test]
    fn size_weighted_assignment_balances_skewed_files() {
        // One huge file + many tiny — round-robin would put the giant on one worker alone.
        let mut files: Vec<(ListingTableUrl, ObjectMeta)> = (0..11)
            .map(|i| {
                let size = if i == 0 { 10_000 } else { 1 };
                (
                    dummy_url(&format!("part-{i}")),
                    meta(&format!("part-{i}"), size),
                )
            })
            .collect();
        files.sort_by(|a, b| a.1.location.as_ref().cmp(b.1.location.as_ref()));

        let assignments = assign_files_by_size(&files, 3);
        let totals = worker_byte_totals(&files, &assignments, 3);
        let largest = files.iter().map(|(_, m)| m.size).max().unwrap();
        let spread = totals.iter().max().unwrap() - totals.iter().min().unwrap();
        assert!(
            spread <= largest,
            "max-min byte spread {spread} should be <= largest file {largest}; totals={totals:?}"
        );

        // Every file assigned exactly once.
        assert_eq!(assignments.len(), files.len());
        for worker in 0..3 {
            assert!(assignments.contains(&worker));
        }
    }

    #[test]
    fn known_size_sharding_uses_metadata_without_object_store_calls() {
        let files = vec![
            (dummy_url("large.parquet"), 100),
            (dummy_url("medium.parquet"), 60),
            (dummy_url("small.parquet"), 40),
        ];
        let worker_zero = apply_known_file_shard_with(
            files.clone(),
            Some("orders"),
            Some(ShardAssignment { index: 0, count: 2 }),
        );
        let worker_one = apply_known_file_shard_with(
            files,
            Some("orders"),
            Some(ShardAssignment { index: 1, count: 2 }),
        );

        assert_eq!(worker_zero.iter().map(|(_, size)| size).sum::<u64>(), 100);
        assert_eq!(worker_one.iter().map(|(_, size)| size).sum::<u64>(), 100);
    }

    #[test]
    fn replicated_parses_csv() {
        std::env::remove_var("OXIDANT_REPLICATED_TABLES");
        assert!(!is_replicated_table("orders"));
        std::env::set_var("OXIDANT_REPLICATED_TABLES", "nation,region,customer");
        assert!(is_replicated_table("Nation"));
        assert!(!is_replicated_table("orders"));
        std::env::remove_var("OXIDANT_REPLICATED_TABLES");
    }

    #[test]
    fn classify_largest_sharded_smaller_under_threshold_replicated() {
        let sized = vec![
            ("lineitem".into(), Some(75_000_000_000)),
            ("orders".into(), Some(17_000_000_000)),
            ("nation".into(), Some(2_000)),
        ];
        let got = classify_replicated_tables(&sized, &[], 32 * 1024 * 1024 * 1024);
        assert!(got.contains(&"orders".to_string()));
        assert!(got.contains(&"nation".to_string()));
        assert!(!got.iter().any(|t| t == "lineitem"));
    }

    #[test]
    fn classify_second_large_table_over_threshold_stays_sharded() {
        let sized = vec![
            ("fact_a".into(), Some(80_000_000_000)),
            ("fact_b".into(), Some(50_000_000_000)),
            ("dim".into(), Some(1_000_000)),
        ];
        let got = classify_replicated_tables(&sized, &[], 32 * 1024 * 1024 * 1024);
        assert!(got.contains(&"dim".to_string()));
        assert!(!got.iter().any(|t| t == "fact_a"));
        assert!(!got.iter().any(|t| t == "fact_b"));
    }

    #[test]
    fn classify_override_wins_and_threshold_zero_disables_auto() {
        let sized = vec![("lineitem".into(), Some(100)), ("orders".into(), Some(10))];
        let with_override = classify_replicated_tables(&sized, &["lineitem"], 32 << 30);
        assert!(with_override.contains(&"lineitem".to_string()));
        assert!(with_override.contains(&"orders".to_string()));

        let disabled = classify_replicated_tables(&sized, &["nation"], 0);
        assert_eq!(disabled, vec!["nation".to_string()]);
    }

    /// KAN-161 revert: the default auto-broadcast cap is back at 32 GiB. The 4 GiB default
    /// (v0.1.11) classified the SF100 mid facts sharded, but the planner lacked the
    /// multi-sharded shape support — 20/99 TPC-DS queries regressed to STRICT refusals.
    /// The 4 GiB re-flip lands with KAN-162 (multi-sharded union split + classification
    /// guard); until then the mid facts replicate and Q14/Q23 keep their v0.1.10 plans.
    #[test]
    fn default_threshold_reverts_to_32_gib_pending_kan162() {
        assert_eq!(
            DEFAULT_AUTO_BROADCAST_THRESHOLD_BYTES,
            32 * 1024 * 1024 * 1024
        );

        let g = 1024 * 1024 * 1024;
        let sized = vec![
            ("catalog_sales".into(), Some(15 * g)), // largest → byte anchor, always sharded
            ("web_sales".into(), Some(8 * g)),
            ("store_sales".into(), Some(6 * g)),
            ("catalog_returns".into(), Some(2 * g)),
            ("web_returns".into(), Some(g)),
            ("store_returns".into(), Some(g / 2)),
            ("item".into(), Some(50_000_000)),
        ];
        let got = classify_replicated_tables(&sized, &[], DEFAULT_AUTO_BROADCAST_THRESHOLD_BYTES);
        for sharded in ["catalog_sales"] {
            assert!(
                !got.iter().any(|t| t == sharded),
                "{sharded} must stay sharded (byte anchor)"
            );
        }
        for replicated in [
            "web_sales",
            "store_sales",
            "catalog_returns",
            "web_returns",
            "store_returns",
            "item",
        ] {
            assert!(
                got.contains(&replicated.to_string()),
                "{replicated} must replicate at the reverted 32 GiB default"
            );
        }
    }

    /// TPC-DS SF10 shape: `catalog_sales` is the byte anchor (~1+ GB, 14.4M rows) while
    /// `inventory` is byte-small (~0.5 GB) but 8× the rows (117M). With a 4.0 row multiple
    /// the byte rule would replicate inventory, the row rule keeps it sharded instead.
    #[test]
    fn classify_row_multiple_excludes_row_heavy_candidate() {
        let sized = vec![
            ("catalog_sales".into(), Some(1_500_000_000)),
            ("inventory".into(), Some(500_000_000)),
            ("item".into(), Some(50_000_000)),
        ];
        let rows = vec![Some(14_400_000), Some(117_000_000), Some(300_000)];
        let got = classify_replicated_tables_with_rows(
            &sized,
            &rows,
            &[],
            32 * 1024 * 1024 * 1024,
            Some(4.0),
        );
        assert!(got.contains(&"item".to_string()));
        assert!(
            !got.iter().any(|t| t == "inventory"),
            "8× the anchor's rows must stay sharded: {got:?}"
        );
        assert!(!got.iter().any(|t| t == "catalog_sales"));
    }

    /// A candidate at exactly `multiple ×` the anchor's rows still replicates — the rule
    /// fires only on strictly greater.
    #[test]
    fn classify_row_multiple_boundary_keeps_replicate() {
        let sized = vec![
            ("fact".into(), Some(1_000_000_000)),
            ("candidate".into(), Some(100_000_000)),
        ];
        let rows = vec![Some(1_000_000), Some(4_000_000)];
        let got = classify_replicated_tables_with_rows(
            &sized,
            &rows,
            &[],
            32 * 1024 * 1024 * 1024,
            Some(4.0),
        );
        assert!(
            got.contains(&"candidate".to_string()),
            "exactly 4× the anchor's rows must still replicate: {got:?}"
        );
        // One row past the boundary excludes it.
        let rows = vec![Some(1_000_000), Some(4_000_001)];
        let got = classify_replicated_tables_with_rows(
            &sized,
            &rows,
            &[],
            32 * 1024 * 1024 * 1024,
            Some(4.0),
        );
        assert!(!got.iter().any(|t| t == "candidate"));
    }

    /// The `OXIDANT_REPLICATED_TABLES` force-include wins over the row-multiple exclusion.
    #[test]
    fn classify_row_multiple_override_wins() {
        let sized = vec![
            ("catalog_sales".into(), Some(1_500_000_000)),
            ("inventory".into(), Some(500_000_000)),
        ];
        let rows = vec![Some(14_400_000), Some(117_000_000)];
        let got = classify_replicated_tables_with_rows(
            &sized,
            &rows,
            &["inventory"],
            32 * 1024 * 1024 * 1024,
            Some(4.0),
        );
        assert!(got.contains(&"inventory".to_string()));
    }

    /// Counts unavailable ⇒ byte-for-byte legacy behavior: no row counts at all, unknown
    /// anchor rows with a known candidate, unknown candidate rows, and a disabled rule all
    /// reproduce `classify_replicated_tables` exactly.
    #[test]
    fn classify_row_multiple_counts_unavailable_is_legacy() {
        let sized = vec![
            ("catalog_sales".into(), Some(1_500_000_000)),
            ("inventory".into(), Some(500_000_000)),
            ("item".into(), Some(50_000_000)),
        ];
        let threshold = 32 * 1024 * 1024 * 1024;
        let legacy = classify_replicated_tables(&sized, &[], threshold);

        let no_rows = vec![None, None, None];
        assert_eq!(
            classify_replicated_tables_with_rows(&sized, &no_rows, &[], threshold, Some(4.0)),
            legacy
        );

        // Anchor rows unknown, candidate rows known → no exclusion.
        let anchor_unknown = vec![None, Some(117_000_000), Some(300_000)];
        assert_eq!(
            classify_replicated_tables_with_rows(
                &sized,
                &anchor_unknown,
                &[],
                threshold,
                Some(4.0)
            ),
            legacy
        );

        // Candidate rows unknown → that candidate keeps the byte decision.
        let candidate_unknown = vec![Some(14_400_000), None, Some(300_000)];
        assert_eq!(
            classify_replicated_tables_with_rows(
                &sized,
                &candidate_unknown,
                &[],
                threshold,
                Some(4.0)
            ),
            legacy
        );

        // Rule disabled → identical even with full counts.
        let rows = vec![Some(14_400_000), Some(117_000_000), Some(300_000)];
        assert_eq!(
            classify_replicated_tables_with_rows(&sized, &rows, &[], threshold, None),
            legacy
        );
        assert_eq!(
            classify_replicated_tables_with_rows(&sized, &rows, &[], threshold, Some(0.0)),
            legacy
        );
    }

    /// `OXIDANT_REPLICATE_MAX_ROW_MULTIPLE`: unset defaults ON (4.0); `0` / negative /
    /// unparseable disable the rule; another positive value overrides the default.
    #[test]
    fn replicate_max_row_multiple_env_parsing() {
        std::env::remove_var("OXIDANT_REPLICATE_MAX_ROW_MULTIPLE");
        assert_eq!(replicate_max_row_multiple(), Some(4.0));

        std::env::set_var("OXIDANT_REPLICATE_MAX_ROW_MULTIPLE", "8.0");
        assert_eq!(replicate_max_row_multiple(), Some(8.0));

        std::env::set_var("OXIDANT_REPLICATE_MAX_ROW_MULTIPLE", "0");
        assert_eq!(replicate_max_row_multiple(), None);

        std::env::set_var("OXIDANT_REPLICATE_MAX_ROW_MULTIPLE", "-1.5");
        assert_eq!(replicate_max_row_multiple(), None);

        std::env::set_var("OXIDANT_REPLICATE_MAX_ROW_MULTIPLE", "not-a-number");
        assert_eq!(replicate_max_row_multiple(), None);

        std::env::remove_var("OXIDANT_REPLICATE_MAX_ROW_MULTIPLE");
    }

    #[tokio::test]
    async fn task_local_replicated_skips_known_file_shard_without_env() {
        std::env::remove_var("OXIDANT_REPLICATED_TABLES");
        let files = vec![(dummy_url("a.parquet"), 100), (dummy_url("b.parquet"), 60)];
        let assignment = Some(ShardAssignment { index: 0, count: 2 });
        let sharded = apply_known_file_shard_with(files.clone(), Some("dim"), assignment);
        assert_eq!(sharded.len(), 1, "without overlay, dim is sharded");

        let full = with_replicated_tables("dim", async {
            apply_known_file_shard_with(files, Some("dim"), assignment)
        })
        .await;
        assert_eq!(
            full.len(),
            2,
            "task-local overlay replicates full file list"
        );
    }

    /// Replicated-slice producers (the union-split replicated arms) shard a table the
    /// classification marked replicated, on every worker, exactly once per slice. The walk is
    /// the same [`assign_known_files_by_size`] sharded tables use: W=1 must hand the whole
    /// list to the single worker, W=2 must split into disjoint halves covering every file,
    /// and W > files must leave the extra workers empty without dropping or duplicating a
    /// file.
    #[test]
    fn slice_assignment_covers_w1_disjoint_w2_and_degenerate_w_gt_files() {
        let files = vec![
            (dummy_url("large.parquet"), 100),
            (dummy_url("medium.parquet"), 60),
            (dummy_url("small.parquet"), 40),
        ];
        let paths = |v: &[(ListingTableUrl, u64)]| {
            v.iter()
                .map(|(u, _)| u.as_str().to_string())
                .collect::<Vec<_>>()
        };

        // W=1: one worker owns every file — byte-identical to an unsharded scan.
        let only = apply_known_file_shard_with(
            files.clone(),
            Some("store_sales"),
            Some(ShardAssignment { index: 0, count: 1 }),
        );
        assert_eq!(only.len(), files.len(), "W=1 keeps the full file list");

        // W=2: disjoint slices whose union is the full list, assigned deterministically.
        let w0 = apply_known_file_shard_with(
            files.clone(),
            Some("store_sales"),
            Some(ShardAssignment { index: 0, count: 2 }),
        );
        let w1 = apply_known_file_shard_with(
            files.clone(),
            Some("store_sales"),
            Some(ShardAssignment { index: 1, count: 2 }),
        );
        let mut all: Vec<String> = paths(&w0).into_iter().chain(paths(&w1)).collect();
        all.sort();
        all.dedup();
        assert_eq!(
            all.len(),
            files.len(),
            "W=2 slices are disjoint and complete"
        );
        assert!(
            !w0.is_empty() && !w1.is_empty(),
            "3 files over 2 workers give both a non-empty slice"
        );
        // Determinism: the same input list yields the same assignment on every worker.
        let w0_again = apply_known_file_shard_with(
            files.clone(),
            Some("store_sales"),
            Some(ShardAssignment { index: 0, count: 2 }),
        );
        assert_eq!(paths(&w0), paths(&w0_again), "slice assignment is stable");

        // W=4 > 3 files: some workers own no files; coverage stays disjoint and complete.
        let mut union: Vec<String> = Vec::new();
        let mut empty = 0;
        for index in 0..4 {
            let slice = apply_known_file_shard_with(
                files.clone(),
                Some("store_sales"),
                Some(ShardAssignment { index, count: 4 }),
            );
            empty += usize::from(slice.is_empty());
            union.extend(paths(&slice));
        }
        union.sort();
        union.dedup();
        assert_eq!(union.len(), files.len(), "degenerate W keeps full coverage");
        assert!(empty >= 1, "more workers than files leaves an empty slice");
    }

    /// The task-local assignment an in-process worker installs beats the process-global env,
    /// so two workers in one test process resolve disjoint shards of the same table.
    #[tokio::test]
    async fn explicit_assignment_task_local_wins_over_env() {
        // Hold the shard-env gate for the whole window the process env names a shard, so
        // concurrent tests' catalog listings wait it out instead of observing it.
        let _env = ShardEnvWriteGuard::take();
        std::env::set_var("OXIDANT_WORKER_COUNT", "2");
        std::env::set_var("OXIDANT_SHARD_INDEX", "0");
        let files = vec![(dummy_url("a.parquet"), 100), (dummy_url("b.parquet"), 60)];
        let env_shard = apply_known_file_shard(files.clone(), Some("orders"));
        assert_eq!(env_shard.len(), 1, "env shard 0 owns the larger file");

        let other = with_shard_assignment(ShardAssignment { index: 1, count: 2 }, async {
            apply_known_file_shard(files.clone(), Some("orders"))
        })
        .await;
        assert_eq!(
            other.len(),
            1,
            "task-local assignment {{1/2}} overrides env {{0/2}}"
        );
        assert_ne!(
            env_shard[0].0.as_str(),
            other[0].0.as_str(),
            "the two in-process workers hold disjoint files"
        );
        // Outside the scope the env answer is unchanged.
        assert_eq!(
            apply_known_file_shard(files, Some("orders"))[0].0.as_str(),
            env_shard[0].0.as_str()
        );
        std::env::remove_var("OXIDANT_WORKER_COUNT");
        std::env::remove_var("OXIDANT_SHARD_INDEX");
    }

    #[test]
    fn collection_url_gets_trailing_slash() {
        assert_eq!(
            ensure_collection_url("s3://bucket/tpch/lineitem"),
            "s3://bucket/tpch/lineitem/"
        );
        assert_eq!(
            ensure_collection_url("s3://bucket/tpch/lineitem/"),
            "s3://bucket/tpch/lineitem/"
        );
        assert_eq!(
            ensure_collection_url("s3://bucket/tpch/lineitem/part-0.parquet"),
            "s3://bucket/tpch/lineitem/part-0.parquet"
        );
    }

    fn write_parts(n: usize) -> std::path::PathBuf {
        write_parts_with_rows(n, 1)
    }

    fn write_parts_with_rows(n: usize, rows_per_file: usize) -> std::path::PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "oxidant-shard-{}-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        for i in 0..n {
            let values: Vec<i64> = (0..rows_per_file)
                .map(|j| (i * rows_per_file + j) as i64)
                .collect();
            let batch =
                RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(values))])
                    .unwrap();
            let f = std::fs::File::create(dir.join(format!("part-{i}.parquet"))).unwrap();
            let mut w = ArrowWriter::try_new(f, schema.clone(), None).unwrap();
            w.write(&batch).unwrap();
            w.close().unwrap();
        }
        dir
    }

    #[tokio::test]
    async fn file_list_shard_is_disjoint_and_complete() {
        let dir = write_parts(4);
        let location = ensure_collection_url(&format!("file://{}", dir.to_string_lossy()));
        let url = ListingTableUrl::parse(&location).unwrap();
        let ctx = SessionContext::new();

        let a = apply_file_shard_with(
            &ctx.state(),
            vec![url.clone()],
            ".parquet",
            Some("orders"),
            Some(ShardAssignment { index: 0, count: 2 }),
        )
        .await
        .unwrap();
        let b = apply_file_shard_with(
            &ctx.state(),
            vec![url],
            ".parquet",
            Some("orders"),
            Some(ShardAssignment { index: 1, count: 2 }),
        )
        .await
        .unwrap();

        assert_eq!(a.len() + b.len(), 4);
        let mut all: Vec<String> = a
            .iter()
            .chain(b.iter())
            .map(|u| u.as_str().to_string())
            .collect();
        all.sort();
        all.dedup();
        assert_eq!(all.len(), 4, "shards must be disjoint");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn parquet_listing_excludes_delta_checkpoints_and_hidden_metadata() {
        let dir = write_parts(1);
        let delta_log = dir.join("_delta_log");
        let iceberg_metadata = dir.join("metadata");
        std::fs::create_dir_all(&delta_log).unwrap();
        std::fs::create_dir_all(&iceberg_metadata).unwrap();
        std::fs::copy(
            dir.join("part-0.parquet"),
            delta_log.join("00000000000000000010.checkpoint.parquet"),
        )
        .unwrap();
        std::fs::copy(
            dir.join("part-0.parquet"),
            iceberg_metadata.join("metadata-table.parquet"),
        )
        .unwrap();
        let location = ensure_collection_url(&format!("file://{}", dir.to_string_lossy()));
        let url = ListingTableUrl::parse(&location).unwrap();
        let ctx = SessionContext::new();

        let files =
            list_visible_file_shard_with(&ctx.state(), vec![url], ".parquet", Some("orders"), None)
                .await
                .unwrap();

        assert_eq!(files.len(), 1);
        assert!(files[0].0.as_str().ends_with("/part-0.parquet"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn empty_shard_returns_no_urls() {
        let dir = write_parts(1);
        let location = ensure_collection_url(&format!("file://{}", dir.to_string_lossy()));
        let url = ListingTableUrl::parse(&location).unwrap();
        let ctx = SessionContext::new();

        let shard = apply_file_shard_with(
            &ctx.state(),
            vec![url],
            ".parquet",
            Some("orders"),
            Some(ShardAssignment { index: 2, count: 3 }),
        )
        .await
        .unwrap();

        assert!(shard.is_empty());
        let empty = empty_table(Arc::new(Schema::new(vec![Field::new(
            "x",
            DataType::Int64,
            false,
        )])))
        .unwrap();
        assert_eq!(empty.schema().fields().len(), 1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn skewed_file_list_shard_balances_by_size() {
        // One large part + several tiny parts on disk — integration check via object-store sizes.
        let dir = write_parts_with_rows(6, 1);
        {
            let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
            let values: Vec<i64> = (0..50_000).collect();
            let batch =
                RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(values))])
                    .unwrap();
            let f = std::fs::File::create(dir.join("part-huge.parquet")).unwrap();
            let mut w = ArrowWriter::try_new(f, schema, None).unwrap();
            w.write(&batch).unwrap();
            w.close().unwrap();
        }

        let location = ensure_collection_url(&format!("file://{}", dir.to_string_lossy()));
        let url = ListingTableUrl::parse(&location).unwrap();
        let ctx = SessionContext::new();
        let worker_count = 3;

        let mut per_worker: Vec<Vec<ListingTableUrl>> = Vec::new();
        for index in 0..worker_count {
            per_worker.push(
                apply_file_shard_with(
                    &ctx.state(),
                    vec![url.clone()],
                    ".parquet",
                    Some("orders"),
                    Some(ShardAssignment {
                        index,
                        count: worker_count,
                    }),
                )
                .await
                .unwrap(),
            );
        }

        let mut totals = vec![0u64; worker_count];
        let mut all_paths: Vec<String> = Vec::new();
        for (worker, urls) in per_worker.iter().enumerate() {
            for u in urls {
                let path = u.as_str().strip_prefix("file://").unwrap_or(u.as_str());
                let size = std::fs::metadata(path).unwrap().len();
                totals[worker] += size;
                all_paths.push(path.to_string());
            }
        }

        all_paths.sort();
        all_paths.dedup();
        assert_eq!(all_paths.len(), 7, "shards must partition all files");

        let largest = std::fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.metadata().unwrap().len())
            .max()
            .unwrap();
        let spread = totals.iter().max().unwrap() - totals.iter().min().unwrap();
        assert!(
            spread <= largest,
            "max-min byte spread {spread} should be <= largest file {largest}; totals={totals:?}"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }
}

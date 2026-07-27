//! File-list sharding for distributed Glue/Parquet scans.
//!
//! When `WEFT_SHARD_INDEX` (or `WEFT_POD_NAME`) and `WEFT_WORKER_COUNT` are set,
//! each worker opens only its share of listed files. Assignment is **size-weighted**
//! (greedy LPT: largest files first, each to the worker with the least bytes so far;
//! ties broken by lowest worker index). Files are ordered deterministically by
//! `(size desc, path asc)` before assignment. Replicated tables (dimension tables)
//! skip sharding via `WEFT_REPLICATED_TABLES`.

use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::catalog::TableProvider;
use datafusion::datasource::empty::EmptyTable;
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::execution::context::SessionState;
use futures::TryStreamExt;
use object_store::ObjectMeta;
use weft_common::{Error, Result};

/// Shard assignment for this process, if configured for a multi-worker cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShardAssignment {
    pub index: usize,
    pub count: usize,
}

impl ShardAssignment {
    pub fn from_env() -> Option<Self> {
        let count: usize = std::env::var("WEFT_WORKER_COUNT")
            .ok()
            .and_then(|s| s.parse().ok())
            .filter(|&n| n > 1)?;

        let index = if let Ok(s) = std::env::var("WEFT_SHARD_INDEX") {
            s.parse().ok()?
        } else if let Ok(name) = std::env::var("WEFT_POD_NAME") {
            // StatefulSet: weft-<cluster>-worker-0
            name.rsplit('-').next()?.parse().ok()?
        } else {
            return None;
        };

        if index >= count {
            eprintln!(
                "weft-loom: WEFT_SHARD_INDEX {index} >= WEFT_WORKER_COUNT {count}; ignoring shard config"
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

/// Tables that every worker should scan fully (broadcast / dimension tables).
pub fn replicated_table_names() -> Vec<String> {
    std::env::var("WEFT_REPLICATED_TABLES")
        .ok()
        .map(|s| {
            s.split(',')
                .map(str::trim)
                .filter(|t| !t.is_empty())
                .map(|t| t.to_ascii_lowercase())
                .collect()
        })
        .unwrap_or_default()
}

pub fn is_replicated_table(table_name: &str) -> bool {
    let needle = table_name.to_ascii_lowercase();
    replicated_table_names().iter().any(|t| t == &needle)
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
        std::env::remove_var("WEFT_REPLICATED_TABLES");
        assert!(!is_replicated_table("orders"));
        std::env::set_var("WEFT_REPLICATED_TABLES", "nation,region,customer");
        assert!(is_replicated_table("Nation"));
        assert!(!is_replicated_table("orders"));
        std::env::remove_var("WEFT_REPLICATED_TABLES");
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
        let dir = std::env::temp_dir().join(format!(
            "weft-shard-{}-{}",
            std::process::id(),
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

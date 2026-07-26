//! File-list sharding for distributed Glue/Parquet scans.
//!
//! When `WEFT_SHARD_INDEX` (or `WEFT_POD_NAME`) and `WEFT_WORKER_COUNT` are set,
//! each worker opens only `files[i] where i % N == shard`. Replicated tables
//! (dimension tables) skip sharding via `WEFT_REPLICATED_TABLES`.

use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::TableProvider;
use datafusion::datasource::empty::EmptyTable;
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::execution::context::SessionState;
use datafusion::catalog::Session;
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

    pub fn owns(&self, file_index: usize) -> bool {
        file_index % self.count == self.index
    }
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
    apply_file_shard_with(state, urls, file_extension, table_name, ShardAssignment::from_env()).await
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
        let store = state.runtime_env().object_store(&store_url).map_err(|e| {
            Error::Io(format!("object store for {}: {e}", store_url.as_str()))
        })?;
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

    let shard_urls: Vec<ListingTableUrl> = all_files
        .into_iter()
        .enumerate()
        .filter(|(i, _)| assignment.owns(*i))
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

    #[test]
    fn assignment_owns_round_robin() {
        let a = ShardAssignment {
            index: 1,
            count: 3,
        };
        assert!(!a.owns(0));
        assert!(a.owns(1));
        assert!(!a.owns(2));
        assert!(a.owns(4));
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
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int64Array::from(vec![i as i64]))],
            )
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
}

//! The catalog's persistent state: one JSON document under the warehouse root.
//!
//! Written through `object_store` rather than `std::fs`, so a local catalog whose warehouse is
//! `s3://…` behaves identically to one on disk — the same choice `oxidant-streaming`'s Delta
//! sink and checkpoint store already make.
//!
//! The manifest is a **versioned log**, not one mutable document: each update writes the next
//! numbered version with a create-if-absent PUT, and the current state is the highest version
//! present. This is the same shape — and for the same reason — as the Delta commit log in
//! `oxidant-datasource`.
//!
//! It would be simpler to keep one file and overwrite it conditionally, and that is what this
//! did first. It does not work: `object_store`'s `LocalFileSystem` returns `NotSupported` for
//! `PutMode::Update`, so a conditional overwrite fails outright on exactly the local warehouse
//! this catalog exists to serve. An *unconditional* overwrite would work everywhere and be
//! wrong everywhere — two pipelines creating tables in one warehouse would each read the same
//! manifest, add their own table, and the second write would erase the first's. Create-only
//! writes give a real conflict signal on both a filesystem and an object store, and turn that
//! race into a retry.

use std::collections::BTreeMap;

use futures::StreamExt;
use object_store::path::Path as ObjectPath;
use object_store::{Error as ObjectStoreError, ObjectStore, ObjectStoreExt, PutMode, PutOptions};
use oxidant_catalog::hive_types::HiveColumns;
use oxidant_catalog::{Error, Result};
use serde::{Deserialize, Serialize};

/// Directory holding the manifest's versions, under the warehouse root.
pub(crate) const MANIFEST_DIR: &str = "_oxidant_catalog";

/// How many times a losing writer retries at the next version before giving up.
///
/// Matches the Delta commit path's tolerance for a racing writer: a handful of retries absorbs
/// realistic contention, and more would just delay surfacing a genuinely stuck catalog.
const WRITE_ATTEMPTS: usize = 8;
/// How many times a read re-lists after finding the version it resolved already pruned.
const READ_ATTEMPTS: usize = 4;

/// Versions further than this below the current one are pruned after a successful write.
///
/// Safe because a reader always resolves to the *highest* version, so it can never be reading
/// one that is eligible for pruning. Without this the version list — and therefore the cost of
/// every metadata read — grows without bound over a warehouse's lifetime.
const VERSIONS_RETAINED: u64 = 10;

/// Zero-padded so a lexicographic object-store listing is also numeric order.
fn version_file(version: u64) -> String {
    format!("{version:020}.json")
}

/// Parse a version number back out of a manifest filename.
fn parse_version(name: &str) -> Option<u64> {
    name.strip_suffix(".json")?.parse().ok()
}

/// The whole catalog document.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub(crate) struct Manifest {
    /// Format version, so a future layout change can be detected rather than misparsed.
    #[serde(default = "current_version")]
    pub version: u32,
    /// Databases, keyed by name.
    #[serde(default)]
    pub databases: BTreeMap<String, DatabaseEntry>,
    /// Tables, keyed by `database.table`.
    #[serde(default)]
    pub tables: BTreeMap<String, TableEntry>,
}

fn current_version() -> u32 {
    1
}

/// A database (schema) in the manifest.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub(crate) struct DatabaseEntry {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub location: Option<String>,
}

/// A table in the manifest.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub(crate) struct TableEntry {
    /// Table root URI or path.
    pub location: String,
    /// `parquet` | `delta` | `iceberg` | `csv` | `json`.
    pub format: String,
    /// Data columns as `(name, hive_type)`.
    ///
    /// Stored in Hive's type vocabulary rather than as a serialized Arrow schema because
    /// `arrow`'s `serde` support is not enabled in this build, and because it is exactly what
    /// Glue and Hive persist — so a table created here and one created in Glue round-trip with
    /// the same fidelity, including the same all-or-nothing fallback for unmappable types.
    #[serde(default)]
    pub columns: HiveColumns,
    /// Partition columns as `(name, hive_type)`, in partition-key order.
    #[serde(default)]
    pub partition_columns: HiveColumns,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
    /// Per-column comments, keyed by column name. A column absent here has no comment set.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub column_comments: BTreeMap<String, String>,
    /// Table properties. Carries `metadata_location` for Iceberg entries, matching Glue.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub properties: BTreeMap<String, String>,
    /// Storage options (`s3.*` / `fs.s3a.*` credentials, CSV `header`/`delimiter`).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub storage_options: BTreeMap<String, String>,
}

/// Compose the manifest key for a table.
pub(crate) fn table_key(database: &str, table: &str) -> String {
    format!("{database}.{table}")
}

/// Reads and conflict-detecting writes of the versioned manifest.
pub(crate) struct ManifestStore {
    store: std::sync::Arc<dyn ObjectStore>,
    dir: ObjectPath,
}

impl ManifestStore {
    pub fn new(store: std::sync::Arc<dyn ObjectStore>, root: ObjectPath) -> Self {
        Self {
            dir: root.clone().join(MANIFEST_DIR),
            store,
        }
    }

    /// Load the current manifest, or an empty one when the catalog has never been written.
    ///
    /// A missing manifest is the normal first-run state, not an error: pointing the catalog at
    /// a fresh warehouse directory must work without a provisioning step.
    pub async fn load(&self) -> Result<Manifest> {
        Ok(self.load_versioned().await?.0)
    }

    /// Load the current manifest along with the version it was read at.
    ///
    /// A version can be pruned between the listing and the read — another writer advancing past
    /// [`VERSIONS_RETAINED`] inside that window is all it takes. Re-listing is the fix, and it
    /// has to be a real retry: returning an empty manifest instead would tell every caller that
    /// every table in the catalog had vanished, and would let `update` commit a from-scratch
    /// manifest at a version no reader will ever resolve.
    async fn load_versioned(&self) -> Result<(Manifest, Option<u64>)> {
        for attempt in 0..READ_ATTEMPTS {
            let versions = self.list_versions().await?;
            let Some(&current) = versions.last() else {
                // No versions at all is the normal first-run state, not a lost race.
                return Ok((Manifest::default(), None));
            };
            let path = self.dir.clone().join(version_file(current));
            let bytes = match self.store.get(&path).await {
                Ok(result) => result
                    .bytes()
                    .await
                    .map_err(|e| Error::Io(format!("read catalog manifest `{path}`: {e}")))?,
                Err(ObjectStoreError::NotFound { .. }) if attempt + 1 < READ_ATTEMPTS => continue,
                Err(ObjectStoreError::NotFound { .. }) => {
                    return Err(Error::Io(format!(
                        "catalog manifest version {current} disappeared while being read, \
                         {READ_ATTEMPTS} times running — another writer is pruning faster than \
                         this reader can keep up"
                    )));
                }
                Err(e) => return Err(Error::Io(format!("read catalog manifest `{path}`: {e}"))),
            };
            let manifest: Manifest = serde_json::from_slice(&bytes).map_err(|e| {
                // A corrupt manifest must be loud. Silently starting from an empty one would
                // make every existing table vanish from the catalog.
                Error::Io(format!("catalog manifest `{path}` is not valid JSON: {e}"))
            })?;
            return Ok((manifest, Some(current)));
        }
        unreachable!("the loop either returns or exhausts READ_ATTEMPTS with an error")
    }

    /// Every manifest version present, ascending.
    async fn list_versions(&self) -> Result<Vec<u64>> {
        let mut listing = self.store.list(Some(&self.dir));
        let mut versions = Vec::new();
        while let Some(item) = listing.next().await {
            let item =
                item.map_err(|e| Error::Io(format!("list catalog manifest `{}`: {e}", self.dir)))?;
            if let Some(version) = item.location.filename().and_then(parse_version) {
                versions.push(version);
            }
        }
        versions.sort_unstable();
        Ok(versions)
    }

    /// Read the manifest, apply `mutate`, and commit the result as the next version.
    ///
    /// `mutate` returns the caller's own value alongside its changes, and may be run more than
    /// once — so it must be a pure function of the manifest it is handed, holding no state
    /// across attempts.
    pub async fn update<T, F>(&self, mut mutate: F) -> Result<T>
    where
        F: FnMut(&mut Manifest) -> Result<T>,
    {
        for _ in 0..WRITE_ATTEMPTS {
            let (mut manifest, current) = self.load_versioned().await?;
            let outcome = mutate(&mut manifest)?;
            let next = current.map_or(0, |v| v + 1);
            let body = serde_json::to_vec_pretty(&manifest)
                .map_err(|e| Error::Io(format!("serialize catalog manifest: {e}")))?;
            let path = self.dir.clone().join(version_file(next));
            let options = PutOptions {
                // Create-only: whoever writes this version first wins, and the loser sees a
                // real conflict rather than overwriting the winner's changes.
                mode: PutMode::Create,
                ..Default::default()
            };
            match self.store.put_opts(&path, body.into(), options).await {
                Ok(_) => {
                    self.prune(next).await;
                    return Ok(outcome);
                }
                // Another writer took this version. Re-read and re-apply on top of theirs.
                Err(ObjectStoreError::AlreadyExists { .. }) => continue,
                Err(e) => return Err(Error::Io(format!("write catalog manifest `{path}`: {e}"))),
            }
        }
        Err(Error::Io(format!(
            "write catalog manifest under `{}`: gave up after {WRITE_ATTEMPTS} attempts \
             (another writer kept winning the race)",
            self.dir
        )))
    }

    /// Delete versions far enough below `current` that no reader can resolve to them.
    ///
    /// Best-effort: a failed delete leaves a file behind, which costs a little listing time and
    /// nothing else, so it must never fail the write that just succeeded.
    async fn prune(&self, current: u64) {
        let Some(cutoff) = current.checked_sub(VERSIONS_RETAINED) else {
            return;
        };
        let Ok(versions) = self.list_versions().await else {
            return;
        };
        for version in versions.into_iter().filter(|v| *v < cutoff) {
            let _ = self
                .store
                .delete(&self.dir.clone().join(version_file(version)))
                .await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::local::LocalFileSystem;
    use std::sync::Arc;

    fn store_on(dir: &std::path::Path) -> ManifestStore {
        let fs = LocalFileSystem::new_with_prefix(dir).expect("local store");
        ManifestStore::new(Arc::new(fs), ObjectPath::from("/"))
    }

    #[tokio::test]
    async fn a_missing_manifest_loads_as_empty_rather_than_erroring() {
        let dir = tempfile::tempdir().expect("tempdir");
        let manifest = store_on(dir.path()).load().await.expect("loads");
        assert!(manifest.databases.is_empty());
        assert!(manifest.tables.is_empty());
    }

    #[tokio::test]
    async fn updates_round_trip_through_the_store() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = store_on(dir.path());
        store
            .update(|m| {
                m.databases.insert("live".into(), DatabaseEntry::default());
                Ok(())
            })
            .await
            .expect("first write");
        store
            .update(|m| {
                m.tables.insert(
                    table_key("live", "orders"),
                    TableEntry {
                        location: "/data/orders/".into(),
                        format: "delta".into(),
                        ..Default::default()
                    },
                );
                Ok(())
            })
            .await
            .expect("second write");

        let manifest = store.load().await.expect("loads");
        assert!(manifest.databases.contains_key("live"));
        assert_eq!(manifest.tables["live.orders"].format, "delta");
        // The second write must not have erased the first.
        assert_eq!(manifest.databases.len(), 1);
    }

    #[tokio::test]
    async fn a_corrupt_manifest_is_loud_rather_than_silently_empty() {
        // Starting from an empty manifest here would make every existing table vanish.
        let dir = tempfile::tempdir().expect("tempdir");
        let versions = dir.path().join(MANIFEST_DIR);
        std::fs::create_dir_all(&versions).expect("mkdir");
        std::fs::write(versions.join(version_file(0)), b"{not json").expect("write");
        let err = store_on(dir.path())
            .load()
            .await
            .expect_err("corrupt manifest must error");
        assert!(err.to_string().contains("not valid JSON"), "got: {err}");
    }

    #[tokio::test]
    async fn concurrent_writers_do_not_lose_each_others_tables() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().to_path_buf();
        let writers: Vec<_> = (0..8)
            .map(|i| {
                let path = path.clone();
                tokio::spawn(async move {
                    store_on(&path)
                        .update(move |m| {
                            m.tables.insert(
                                table_key("live", &format!("t{i}")),
                                TableEntry {
                                    location: format!("/data/t{i}/"),
                                    format: "delta".into(),
                                    ..Default::default()
                                },
                            );
                            Ok(())
                        })
                        .await
                })
            })
            .collect();
        for writer in writers {
            writer.await.expect("task").expect("write succeeds");
        }
        let manifest = store_on(&path).load().await.expect("loads");
        assert_eq!(
            manifest.tables.len(),
            8,
            "every writer's table must survive; got {:?}",
            manifest.tables.keys().collect::<Vec<_>>()
        );
    }
}

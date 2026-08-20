//! `oxidant-config` — the declarative configuration file for the `oxidant` binary.
//!
//! Before this crate the binary was configured three incompatible ways: ~120 `OXIDANT_*`
//! environment variables read via `std::env::var` scattered across the workspace, hand-rolled
//! CLI flags, and `--catalog-conf key=value` pairs in Spark's flat
//! `spark.sql.catalog.<name>.*` namespace. A user running a Kafka→lakehouse pipeline had to
//! know all three.
//!
//! This crate is a typed, validated front-end over those surfaces — **not** a replacement for
//! them. Deliberately:
//!
//! - [`OxidantConfig::catalog_conf`] lowers `catalogs:` into the *existing* flat
//!   `spark.sql.catalog.*` map, so it feeds the existing catalog bootstrap unchanged and a
//!   config-declared catalog behaves identically to one declared with `--catalog-conf`.
//! - [`OxidantConfig::engine_env`] lowers `engine:` into the *existing* `OXIDANT_*` env
//!   contract. Those variables are read deep inside the engine at construction time, so the
//!   values only take effect in a process the CLI starts — this cannot retune a running
//!   server.
//!
//! The one genuinely new surface is [`PipelineConfig`] + [`TableConfig`]: a declarative
//! bronze→silver→gold DAG the binary can run headless, with no PySpark client.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use oxidant_common::{Error, Result};
use serde::{Deserialize, Serialize};

mod auto_cdc;
mod interpolate;
mod pipeline;
mod validate;

use interpolate::interpolate_config;

pub use auto_cdc::{simple_column as auto_cdc_simple_column, validate as validate_auto_cdc};
pub use pipeline::{
    AppendFlow, AutoCdcConfig, ExpectAction, Expectation, PipelineConfig, SourceConfig,
    TableConfig, TableKind, Trigger,
};

/// Environment variable naming a config file, consulted when `--config` is absent.
pub const CONFIG_ENV: &str = "OXIDANT_CONFIG";

/// File looked for in the working directory when neither `--config` nor [`CONFIG_ENV`] is set.
pub const DEFAULT_CONFIG_FILE: &str = "oxidant.yaml";

/// The whole config file.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
pub struct OxidantConfig {
    /// Engine tuning, lowered to `OXIDANT_*` environment variables.
    #[serde(default)]
    pub engine: EngineConfig,

    /// Values substituted into `${NAME}` references anywhere else in the file. See
    /// [`interpolate`].
    #[serde(default)]
    pub vars: BTreeMap<String, String>,

    /// Named catalogs, lowered to flat `spark.sql.catalog.<name>.*` config keys.
    #[serde(default)]
    pub catalogs: BTreeMap<String, CatalogConfig>,

    /// The catalog an unqualified table name resolves against (`spark.sql.defaultCatalog`).
    #[serde(default)]
    pub default_catalog: Option<String>,

    /// Pipeline-wide settings. Required if (and only if) `tables:` is non-empty.
    #[serde(default)]
    pub pipeline: Option<PipelineConfig>,

    /// The declarative table DAG.
    #[serde(default)]
    pub tables: Vec<TableConfig>,

    /// Where this config was loaded from. Relative paths inside the file resolve against the
    /// file's own directory, not the process working directory — so `oxidant -c ../x/oxidant.yaml`
    /// means the same thing from any cwd.
    #[serde(skip)]
    pub source_path: Option<PathBuf>,
}

impl OxidantConfig {
    /// Parse and validate a config file.
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let text = std::fs::read_to_string(path)
            .map_err(|e| Error::Io(format!("read config `{}`: {e}", path.display())))?;
        let mut config = Self::parse_from(&text, Some(path))
            .map_err(|e| Error::Io(format!("config `{}`: {e}", path.display())))?;
        config.source_path = Some(path.to_path_buf());
        Ok(config)
    }

    /// Parse and validate config text. Split from [`load`](Self::load) so tests need no file.
    ///
    /// `${CONFIG_DIR}` is unavailable on this path — there is no file to take a directory from.
    pub fn parse(text: &str) -> Result<Self> {
        Self::parse_from(text, None)
    }

    /// The shared body of [`load`](Self::load) and [`parse`](Self::parse).
    ///
    /// Order matters and is load-bearing: interpolate, *then* validate, *then* check paths. A
    /// `${VAR}` that has not been substituted yet is not a path anyone can judge, so checking
    /// first would reject every portable config.
    fn parse_from(text: &str, source: Option<&Path>) -> Result<Self> {
        let mut config: Self = interpolate_config(text, source)?;
        config.validate()?;
        config.resolve_paths()?;
        Ok(config)
    }

    /// Resolve a config file by the documented precedence: an explicit `--config` path, then
    /// [`CONFIG_ENV`], then [`DEFAULT_CONFIG_FILE`] in the working directory.
    ///
    /// An explicit path that does not exist is an error — a typo in `--config` must not
    /// silently fall through to a default and run with the wrong catalogs. The implicit
    /// sources are skipped when absent, so the binary keeps working with no config at all.
    pub fn resolve(explicit: Option<&str>) -> Result<Option<Self>> {
        if let Some(path) = explicit {
            return Self::load(path).map(Some);
        }
        if let Ok(path) = std::env::var(CONFIG_ENV) {
            if !path.trim().is_empty() {
                return Self::load(path).map(Some);
            }
        }
        let default = Path::new(DEFAULT_CONFIG_FILE);
        if default.is_file() {
            return Self::load(default).map(Some);
        }
        Ok(None)
    }

    /// Lower `catalogs:` + `default_catalog:` to the flat `spark.sql.catalog.*` map the
    /// engine's catalog bootstrap already consumes.
    ///
    /// Structured local-catalog fields (`tables:`, `discover:`) are carried as JSON strings
    /// under a single key each. That keeps the whole config expressible in the flat map, so a
    /// config-declared catalog and a `--catalog-conf`-declared one take the identical code
    /// path — there is no second bootstrap to keep in sync.
    pub fn catalog_conf(&self) -> BTreeMap<String, String> {
        let mut out = BTreeMap::new();
        for (name, catalog) in &self.catalogs {
            let prefix = format!("spark.sql.catalog.{name}");
            for (key, value) in catalog.flatten() {
                out.insert(format!("{prefix}.{key}"), value);
            }
        }
        if let Some(default) = &self.default_catalog {
            out.insert("spark.sql.defaultCatalog".to_string(), default.clone());
        }
        out
    }

    /// Lower `engine:` to `(OXIDANT_*, value)` pairs.
    ///
    /// Returned rather than applied so the caller decides precedence. The CLI applies these
    /// *without* overwriting a variable already set in the environment, so an operator's
    /// `OXIDANT_MEMORY_LIMIT_BYTES=… oxidant …` still wins over the file — the same direction
    /// as a CLI flag beating a config value.
    pub fn engine_env(&self) -> Vec<(String, String)> {
        self.engine.to_env()
    }

    /// Apply [`engine_env`](Self::engine_env), leaving already-set variables alone.
    ///
    /// # Call this before the process becomes multi-threaded
    /// This mutates process-global state with `std::env::set_var`, which races any concurrent
    /// `getenv` from another thread — the reason Rust 2024 made it `unsafe`. The CLI calls it in
    /// `main`, before the Tokio runtime and its workers exist. That is also before any `Engine`
    /// is constructed, which is required anyway: the engine reads these once, at construction,
    /// so a value applied afterwards is silently ignored.
    pub fn apply_engine_env(&self) {
        for (key, value) in self.engine_env() {
            if std::env::var_os(&key).is_none() {
                std::env::set_var(&key, &value);
            }
        }
    }

    /// Check every filesystem path in the config, rewriting each to its clean absolute form.
    ///
    /// **A relative local path is an error, not something resolved on your behalf.** "Relative to
    /// what" has no answer a reader can be sure of: the process working directory and the config
    /// file's own directory are both defensible, they disagree, and picking one silently sends
    /// data somewhere the operator did not mean. `oxidant pipeline show` printing the same table
    /// at two different locations depending on where you ran it is not a usable contract.
    ///
    /// Paths carrying a URI scheme (`s3://`, `file://`, `hdfs://`) are already absolute by
    /// construction and pass through untouched.
    ///
    /// Absolute paths are still cleaned lexically: `/repo/examples/../data` is perfectly valid to
    /// every shell and filesystem call, but `object_store`'s `Path` refuses a `..` segment, so
    /// leaving one in makes the catalog fail to build with no useful error anywhere. Lexical
    /// rather than `canonicalize()` on purpose — the target need not exist yet (a fresh warehouse
    /// is the normal first-run state), and symlinks stay as the operator wrote them.
    fn resolve_paths(&mut self) -> Result<()> {
        for (name, catalog) in &mut self.catalogs {
            if let Some(warehouse) = catalog.warehouse.as_mut() {
                absolutize(warehouse, &format!("catalogs.{name}.warehouse"))?;
            }
            for (table, config) in &mut catalog.tables {
                absolutize(
                    &mut config.location,
                    &format!("catalogs.{name}.tables.{table}.location"),
                )?;
            }
            for (index, entry) in catalog.discover.iter_mut().enumerate() {
                absolutize(
                    &mut entry.path,
                    &format!("catalogs.{name}.discover[{index}].path"),
                )?;
            }
        }
        if let Some(pipeline) = self.pipeline.as_mut() {
            if let Some(storage) = pipeline.storage.as_mut() {
                absolutize(storage, "pipeline.storage")?;
            }
            absolutize(&mut pipeline.checkpoints, "pipeline.checkpoints")?;
        }
        for table in &mut self.tables {
            let Some(source) = table.source.as_mut() else {
                continue;
            };
            // Only the options that are *defined* to be paths. Source options are otherwise an
            // opaque passthrough — broker lists, topic names, offset specs — and rejecting one
            // that merely looks path-shaped would break a perfectly good config.
            for key in PATH_SOURCE_OPTIONS {
                if let Some(value) = source.options.get_mut(*key) {
                    absolutize(
                        value,
                        &format!("tables.{}.source.options.{key}", table.name),
                    )?;
                }
            }
        }
        Ok(())
    }
}

/// Require `location` to be absolute, and clean it in place. `key` names it in the error.
fn absolutize(location: &mut String, key: &str) -> Result<()> {
    if has_uri_scheme(location) {
        return Ok(());
    }
    if !Path::new(location.as_str()).is_absolute() {
        return Err(Error::Io(format!(
            "`{key}` must be an absolute path (got `{location}`). Relative paths are rejected \
             rather than guessed at: resolving them against the working directory and against \
             this config file's directory give different answers, and silently picking one \
             writes your data somewhere you did not ask for."
        )));
    }
    // Preserve a trailing separator: table locations are directories, and `ListingTable`
    // distinguishes a collection from a single file by exactly that.
    let trailing = location.ends_with('/');
    let mut cleaned = normalize(Path::new(location.as_str()))
        .to_string_lossy()
        .into_owned();
    if trailing && !cleaned.ends_with('/') {
        cleaned.push('/');
    }
    *location = cleaned;
    Ok(())
}

/// Source options whose value is a filesystem path, and so resolves against the config file.
///
/// `oxidant.spool.dir` is the offline Kafka spool; `path` is what the file-based streaming
/// sources (`parquet`, `json`, `csv`) read from.
const PATH_SOURCE_OPTIONS: &[&str] = &["oxidant.spool.dir", "path"];

/// Resolve `.` and `..` segments lexically, without touching the filesystem.
///
/// Required, not cosmetic. Rebasing `../sample-data/x` against `examples/` yields
/// `examples/../sample-data/x`, which every shell and every filesystem call resolves happily —
/// but `object_store`'s `Path` treats `..` as an invalid segment and refuses it, so the table
/// silently never registers. Resolving here means the rest of the system only ever sees clean
/// paths.
///
/// Lexical rather than `canonicalize()` on purpose: the target need not exist yet (a fresh
/// warehouse is the normal first-run state), and canonicalizing would also resolve symlinks,
/// which changes what the operator wrote.
pub(crate) fn normalize(path: &Path) -> PathBuf {
    use std::path::Component;

    let mut out = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                // Only pop a segment that can actually be popped. A leading `..` (or one just
                // after the root) has nothing above it and must be preserved, or the path would
                // silently point somewhere else entirely.
                let can_pop = out
                    .components()
                    .next_back()
                    .is_some_and(|c| matches!(c, Component::Normal(_)));
                if can_pop {
                    out.pop();
                } else {
                    out.push(component.as_os_str());
                }
            }
            other => out.push(other.as_os_str()),
        }
    }
    out
}

/// Whether a location carries a URI scheme (`s3://`, `file://`, …) rather than being a path.
///
/// Deliberately requires `://` and an alphabetic first character: a Windows-style `C:\` is not
/// a scheme, and neither is a relative path that happens to contain a colon.
fn has_uri_scheme(location: &str) -> bool {
    match location.split_once("://") {
        Some((scheme, _)) => {
            !scheme.is_empty()
                && scheme.starts_with(|c: char| c.is_ascii_alphabetic())
                && scheme
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '-' | '.'))
        }
        None => false,
    }
}

/// Engine tuning knobs, lowered to the `OXIDANT_*` environment contract.
///
/// This is a curated subset — the knobs an operator running a pipeline actually reaches for.
/// Everything else stays reachable through [`EngineConfig::env`] rather than growing a field
/// per variable; the full list lives in `docs/` and `AGENTS.md`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
pub struct EngineConfig {
    /// `OXIDANT_MEMORY_LIMIT_BYTES`. Leave unset for auto-sizing from cgroup/host RAM.
    ///
    /// Note this variable sizes both the DataFusion pool *and* the shuffle cache, so a
    /// worker's real ceiling is above the number set here. Never set it to `0`.
    #[serde(default)]
    pub memory_limit_bytes: Option<u64>,
    /// `OXIDANT_MEMORY_POOL_FRACTION`.
    #[serde(default)]
    pub memory_pool_fraction: Option<f64>,
    /// `OXIDANT_SHUFFLE_PARTITIONS`. Leave unset for the engine default (>= 200).
    #[serde(default)]
    pub shuffle_partitions: Option<usize>,
    /// `OXIDANT_TARGET_PARTITIONS`.
    #[serde(default)]
    pub target_partitions: Option<usize>,
    /// `OXIDANT_BATCH_SIZE`.
    #[serde(default)]
    pub batch_size: Option<usize>,
    /// `OXIDANT_SHUFFLE_SPILL_BYTES`.
    #[serde(default)]
    pub shuffle_spill_bytes: Option<u64>,
    /// `OXIDANT_SHUFFLE_SPILL_DIR`.
    #[serde(default)]
    pub shuffle_spill_dir: Option<String>,
    /// `OXIDANT_BROADCAST_JOIN_THRESHOLD_BYTES`.
    #[serde(default)]
    pub broadcast_join_threshold_bytes: Option<u64>,
    /// `OXIDANT_S3_CACHE_DIR`.
    #[serde(default)]
    pub s3_cache_dir: Option<String>,
    /// `OXIDANT_S3_CACHE_MAX_BYTES`.
    #[serde(default)]
    pub s3_cache_max_bytes: Option<u64>,
    /// `OXIDANT_WORKERS` — remote Flight worker endpoints (`host:port`).
    #[serde(default)]
    pub workers: Vec<String>,
    /// Escape hatch for any `OXIDANT_*` variable without a typed field above. Keys are used
    /// verbatim, so they must be spelled exactly as the engine reads them.
    #[serde(default)]
    pub env: BTreeMap<String, String>,
}

impl EngineConfig {
    /// Lower to `(variable, value)` pairs. Typed fields first, then [`env`](Self::env), so an
    /// explicit escape-hatch entry wins over the typed field for the same variable.
    fn to_env(&self) -> Vec<(String, String)> {
        let mut out: Vec<(String, String)> = Vec::new();
        let mut push = |key: &str, value: Option<String>| {
            if let Some(value) = value {
                out.push((key.to_string(), value));
            }
        };
        push(
            "OXIDANT_MEMORY_LIMIT_BYTES",
            self.memory_limit_bytes.map(|v| v.to_string()),
        );
        push(
            "OXIDANT_MEMORY_POOL_FRACTION",
            self.memory_pool_fraction.map(|v| v.to_string()),
        );
        push(
            "OXIDANT_SHUFFLE_PARTITIONS",
            self.shuffle_partitions.map(|v| v.to_string()),
        );
        push(
            "OXIDANT_TARGET_PARTITIONS",
            self.target_partitions.map(|v| v.to_string()),
        );
        push("OXIDANT_BATCH_SIZE", self.batch_size.map(|v| v.to_string()));
        push(
            "OXIDANT_SHUFFLE_SPILL_BYTES",
            self.shuffle_spill_bytes.map(|v| v.to_string()),
        );
        push("OXIDANT_SHUFFLE_SPILL_DIR", self.shuffle_spill_dir.clone());
        push(
            "OXIDANT_BROADCAST_JOIN_THRESHOLD_BYTES",
            self.broadcast_join_threshold_bytes.map(|v| v.to_string()),
        );
        push("OXIDANT_S3_CACHE_DIR", self.s3_cache_dir.clone());
        push(
            "OXIDANT_S3_CACHE_MAX_BYTES",
            self.s3_cache_max_bytes.map(|v| v.to_string()),
        );
        if !self.workers.is_empty() {
            out.push(("OXIDANT_WORKERS".to_string(), self.workers.join(",")));
        }
        for (key, value) in &self.env {
            out.retain(|(k, _)| k != key);
            out.push((key.clone(), value.clone()));
        }
        out
    }
}

/// One named catalog.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
pub struct CatalogConfig {
    /// `local` | `glue` | `hive` | `rest` | `unity` | `iceberg`.
    #[serde(rename = "type")]
    pub catalog_type: String,
    /// Root under which new tables are created (`{warehouse}/{database}.db/{table}/`).
    #[serde(default)]
    pub warehouse: Option<String>,
    /// AWS region (Glue). Resolved from the ambient AWS chain when unset.
    #[serde(default)]
    pub region: Option<String>,
    /// Metastore / REST endpoint (Hive `thrift://…`, REST `https://…`).
    #[serde(default)]
    pub uri: Option<String>,
    /// Bearer token for a REST catalog.
    #[serde(default)]
    pub token: Option<String>,
    /// Enable Lake Formation enforcement on a Glue catalog.
    #[serde(default)]
    pub lakeformation: Option<bool>,
    /// Pre-loaded tables, keyed by `namespace.table`. Local catalogs only.
    #[serde(default)]
    pub tables: BTreeMap<String, LocalTableConfig>,
    /// Directory trees to scan and register at startup. Local catalogs only.
    #[serde(default)]
    pub discover: Vec<DiscoverConfig>,
    /// Escape hatch for any `spark.sql.catalog.<name>.<key>` without a typed field above
    /// (e.g. `lakeformation.identity`, `warehouse` variants a backend adds later).
    #[serde(default)]
    pub options: BTreeMap<String, String>,
}

impl CatalogConfig {
    /// Lower to the `<key> -> <value>` pairs that sit under `spark.sql.catalog.<name>.`.
    fn flatten(&self) -> BTreeMap<String, String> {
        let mut out = BTreeMap::new();
        out.insert("type".to_string(), self.catalog_type.clone());
        let mut push = |key: &str, value: Option<String>| {
            if let Some(value) = value {
                out.insert(key.to_string(), value);
            }
        };
        push("warehouse", self.warehouse.clone());
        push("region", self.region.clone());
        push("uri", self.uri.clone());
        push("token", self.token.clone());
        push("lakeformation", self.lakeformation.map(|v| v.to_string()));
        // Structured local-catalog config rides as JSON under one key each, so the whole
        // catalog stays expressible in the flat map the bootstrap already consumes.
        if !self.tables.is_empty() {
            if let Ok(json) = serde_json::to_string(&self.tables) {
                out.insert("tables".to_string(), json);
            }
        }
        if !self.discover.is_empty() {
            if let Ok(json) = serde_json::to_string(&self.discover) {
                out.insert("discover".to_string(), json);
            }
        }
        for (key, value) in &self.options {
            out.insert(key.clone(), value.clone());
        }
        out
    }
}

/// One pre-declared table in a local catalog.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
pub struct LocalTableConfig {
    /// `parquet` | `delta` | `iceberg` | `csv` | `json`.
    pub format: String,
    /// Table root — a local path or a URI (`s3://bucket/prefix`).
    pub location: String,
    /// Reader/storage options: CSV `header`/`delimiter`, or `s3.*` / `fs.s3a.*` credentials
    /// that pin this table's identity ahead of the ambient AWS chain.
    #[serde(default)]
    pub options: BTreeMap<String, String>,
    /// Partition column names, when the layout is Hive-style and should not be inferred.
    #[serde(default)]
    pub partition_columns: Vec<String>,
}

/// A directory tree to scan for tables at startup.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
pub struct DiscoverConfig {
    /// Namespace the discovered tables are registered under.
    pub namespace: String,
    /// Directory to scan. Each immediate subdirectory becomes one table, named after it.
    pub path: String,
    /// Storage credentials / endpoint options for this root, inherited by every table found
    /// under it. Omitted, the catalog's own options apply — a root in the same bucket as the
    /// warehouse should not have to repeat them.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub options: BTreeMap<String, String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uri_schemes_are_recognized_but_bare_paths_are_not() {
        assert!(has_uri_scheme("s3://bucket/prefix"));
        assert!(has_uri_scheme("file:///data/t"));
        assert!(has_uri_scheme("hdfs://nn:8020/t"));
        assert!(!has_uri_scheme("./data/events"));
        assert!(!has_uri_scheme("/abs/data"));
        assert!(!has_uri_scheme("data/events"));
    }

    #[test]
    fn a_relative_local_path_is_rejected_with_the_key_that_holds_it() {
        for written in ["./warehouse", "warehouse", "../sample-data/delta/t"] {
            let key = "catalogs.local.warehouse";
            let mut location = written.to_string();
            let Err(err) = absolutize(&mut location, key) else {
                panic!("`{written}` should be rejected");
            };
            let message = err.to_string();
            assert!(message.contains(key), "error must name the key: {message}");
            assert!(
                message.contains(written),
                "error must quote what was written: {message}"
            );
        }
    }

    #[test]
    fn uris_pass_through_and_absolute_paths_are_cleaned() {
        // A URI is absolute by construction and is not a path to be cleaned — rewriting one
        // would corrupt it.
        let mut uri = "s3://bucket/x".to_string();
        absolutize(&mut uri, "k").expect("a URI is accepted");
        assert_eq!(uri, "s3://bucket/x");

        // `..` and `.` are resolved out: every shell accepts them, but `object_store`'s `Path`
        // refuses a `..` segment, so one left in makes the catalog fail to build with no useful
        // error anywhere.
        let mut messy = "/repo/examples/../sample-data/./delta/t".to_string();
        absolutize(&mut messy, "k").expect("absolute is accepted");
        assert_eq!(messy, "/repo/sample-data/delta/t");

        // A trailing slash is load-bearing — `ListingTable` reads it as "directory, not file" —
        // and must survive the cleaning.
        let mut dir = "/data/events/".to_string();
        absolutize(&mut dir, "k").expect("absolute is accepted");
        assert_eq!(dir, "/data/events/");
    }

    #[test]
    fn a_relative_path_valued_source_option_is_rejected_by_name() {
        // The offline spool is the one that bit hardest before this: resolved against whatever
        // directory the user happened to run from, it silently read nothing and the pipeline
        // reported zero rows with no error at all.
        let err = OxidantConfig::parse(SPOOL_CONFIG.replace("SPOOL", "./spool/orders").as_str())
            .expect_err("a relative spool is rejected");
        assert!(
            err.to_string()
                .contains("tables.bronze.source.options.oxidant.spool.dir"),
            "error must name the option: {err}"
        );

        // A non-path option is an opaque passthrough and must not be inspected at all.
        let config =
            OxidantConfig::parse(SPOOL_CONFIG.replace("SPOOL", "/srv/spool/orders").as_str())
                .expect("absolute paths are accepted");
        let options = &config.tables[0].source.as_ref().expect("source").options;
        assert_eq!(options.get("subscribe").map(String::as_str), Some("orders"));
        assert_eq!(
            options.get("oxidant.spool.dir").map(String::as_str),
            Some("/srv/spool/orders")
        );
    }

    /// A minimal pipeline config with one path-valued source option, spelled `SPOOL`.
    const SPOOL_CONFIG: &str = r#"
catalogs:
  local: { type: local, warehouse: /srv/w }
pipeline:
  name: p
  catalog: local
  schema: live
  checkpoints: /srv/ckpt
tables:
  - name: bronze
    source:
      format: kafka
      options:
        subscribe: orders
        oxidant.spool.dir: SPOOL
"#;

    #[test]
    fn a_leading_parent_segment_that_cannot_be_popped_is_preserved() {
        // Dropping it would silently point the location somewhere entirely different.
        assert_eq!(normalize(Path::new("../a/b")), PathBuf::from("../a/b"));
        assert_eq!(normalize(Path::new("a/./b/../c")), PathBuf::from("a/c"));
    }

    #[test]
    fn catalog_conf_lowers_to_the_existing_flat_spark_keys() {
        let config = OxidantConfig::parse(
            r#"
catalogs:
  glue:
    type: glue
    region: us-east-1
    warehouse: s3://bucket/warehouse
default_catalog: glue
"#,
        )
        .expect("parses");
        let conf = config.catalog_conf();
        assert_eq!(
            conf.get("spark.sql.catalog.glue.type").map(String::as_str),
            Some("glue")
        );
        assert_eq!(
            conf.get("spark.sql.catalog.glue.region")
                .map(String::as_str),
            Some("us-east-1")
        );
        assert_eq!(
            conf.get("spark.sql.defaultCatalog").map(String::as_str),
            Some("glue")
        );
    }

    #[test]
    fn local_catalog_tables_ride_as_json_under_one_key() {
        let config = OxidantConfig::parse(
            r#"
catalogs:
  local:
    type: local
    warehouse: /srv/warehouse
    tables:
      raw.events: { format: parquet, location: /srv/data/events/ }
"#,
        )
        .expect("parses");
        let conf = config.catalog_conf();
        let json = conf
            .get("spark.sql.catalog.local.tables")
            .expect("tables key");
        let parsed: BTreeMap<String, LocalTableConfig> =
            serde_json::from_str(json).expect("round-trips as JSON");
        assert_eq!(parsed["raw.events"].format, "parquet");
        assert_eq!(parsed["raw.events"].location, "/srv/data/events/");
    }

    #[test]
    fn unknown_keys_are_rejected_rather_than_ignored() {
        // A silently-ignored typo is the whole reason `deny_unknown_fields` is on: a
        // misspelled `warehouse` would otherwise run against the wrong location.
        let err = OxidantConfig::parse(
            r#"
catalogs:
  glue:
    type: glue
    warehosue: s3://bucket/warehouse
"#,
        )
        .expect_err("unknown key must fail");
        assert!(
            err.to_string().contains("warehosue"),
            "error should name the offending key, got: {err}"
        );
    }

    #[test]
    fn engine_config_lowers_to_the_env_contract() {
        let config = OxidantConfig::parse(
            r#"
engine:
  memory_limit_bytes: 28895544320
  shuffle_partitions: 200
  workers: ["h1:50561", "h2:50561"]
  env:
    OXIDANT_AQE: "true"
"#,
        )
        .expect("parses");
        let env: BTreeMap<String, String> = config.engine_env().into_iter().collect();
        assert_eq!(
            env.get("OXIDANT_MEMORY_LIMIT_BYTES").map(String::as_str),
            Some("28895544320")
        );
        assert_eq!(
            env.get("OXIDANT_SHUFFLE_PARTITIONS").map(String::as_str),
            Some("200")
        );
        assert_eq!(
            env.get("OXIDANT_WORKERS").map(String::as_str),
            Some("h1:50561,h2:50561")
        );
        assert_eq!(env.get("OXIDANT_AQE").map(String::as_str), Some("true"));
    }

    #[test]
    fn the_env_escape_hatch_outranks_the_typed_field_for_the_same_variable() {
        let config = OxidantConfig::parse(
            r#"
engine:
  shuffle_partitions: 200
  env:
    OXIDANT_SHUFFLE_PARTITIONS: "400"
"#,
        )
        .expect("parses");
        let env = config.engine_env();
        let hits: Vec<&String> = env
            .iter()
            .filter(|(k, _)| k == "OXIDANT_SHUFFLE_PARTITIONS")
            .map(|(_, v)| v)
            .collect();
        assert_eq!(hits, vec!["400"], "the escape hatch must win exactly once");
    }

    #[test]
    fn an_empty_config_is_valid() {
        // The binary must keep working with no config at all — `oxidant sql -e 'SELECT 1'`
        // should not require a file.
        let config = OxidantConfig::parse("{}").expect("empty config parses");
        assert!(config.catalogs.is_empty());
        assert!(config.tables.is_empty());
        assert!(config.catalog_conf().is_empty());
    }
}

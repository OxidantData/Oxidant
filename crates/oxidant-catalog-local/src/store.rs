//! Resolving a location — a bare path or a URI — to an [`ObjectStore`] plus a prefix inside it.
//!
//! The catalog needs its own store resolution because it runs *before* and *beside* the engine:
//! the manifest is read at `list_tables` time and written by DDL, neither of which goes through
//! a DataFusion `SessionState`. The engine's own `Engine::object_store_for` is the right thing
//! for reading table *data*; this is for the catalog's own metadata.

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use object_store::aws::{AmazonS3Builder, AmazonS3ConfigKey};
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;
use oxidant_catalog::{Error, Result};

/// Build an object store for `location`, plus the prefix within it that `location` names.
///
/// Local paths (bare, or `file://`) get a `LocalFileSystem` rooted at `/` with the path as the
/// prefix, and **must be absolute** — a relative one is rejected rather than resolved against the
/// working directory. This is the backstop for locations that never went through
/// `oxidant-config`'s own check: `--catalog-conf`, `OXIDANT_CATALOG_CONF`, and a PySpark client's
/// `Config` RPC all reach here directly.
///
/// `s3://` goes through `AmazonS3Builder::from_env()` plus any `s3.*` / `fs.s3a.*` options, which
/// is the same key vocabulary the engine's own S3 registration accepts — so a table's storage
/// options mean the same thing to the catalog and to the scan.
pub fn store_for_location(
    location: &str,
    options: &HashMap<String, String>,
) -> Result<(Arc<dyn ObjectStore>, ObjectPath)> {
    let trimmed = location.trim();
    if trimmed.is_empty() {
        return Err(Error::Plan("empty location".into()));
    }
    match scheme_of(trimmed) {
        None | Some("file") => {
            let path = local_path(trimmed)?;
            // Rooted at `/` rather than at the directory itself: the directory may not exist
            // yet (a fresh warehouse), and `new_with_prefix` requires it to.
            let store = LocalFileSystem::new();
            let prefix = ObjectPath::from_absolute_path(&path).map_err(|e| {
                Error::Plan(format!("`{}` is not a usable path: {e}", path.display()))
            })?;
            Ok((Arc::new(store), prefix))
        }
        Some("s3") => {
            let (bucket, prefix) = split_bucket(trimmed, "s3://")?;
            let mut builder = AmazonS3Builder::from_env().with_bucket_name(&bucket);
            for (key, value) in options {
                // Unknown keys are skipped rather than rejected: `storage_options` is a mixed
                // bag (a CSV table carries `header`/`delimiter` there too), and erroring on
                // anything the S3 builder does not recognize would make those unusable.
                if let Some(config_key) =
                    normalize_s3_key(key).and_then(|k| AmazonS3ConfigKey::from_str(&k).ok())
                {
                    builder = builder.with_config(config_key, value);
                }
            }
            let store = builder
                .build()
                .map_err(|e| Error::Io(format!("build S3 store for `{trimmed}`: {e}")))?;
            Ok((Arc::new(store), ObjectPath::from(prefix)))
        }
        Some(other) => Err(Error::Unsupported(format!(
            "catalog location `{trimmed}` uses scheme `{other}`; this catalog supports local \
             paths, `file://`, and `s3://`"
        ))),
    }
}

/// The table root prefix for a location, for callers that already have a store.
pub fn table_root_prefix(location: &str) -> Result<ObjectPath> {
    Ok(store_for_location(location, &HashMap::new())?.1)
}

/// The URI scheme of a location, or `None` when it is a plain path.
fn scheme_of(location: &str) -> Option<&str> {
    let (scheme, _) = location.split_once("://")?;
    if !scheme.is_empty()
        && scheme.starts_with(|c: char| c.is_ascii_alphabetic())
        && scheme
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '-' | '.'))
    {
        Some(scheme)
    } else {
        None
    }
}

/// Resolve a bare or `file://` location to an absolute filesystem path.
fn local_path(location: &str) -> Result<std::path::PathBuf> {
    let raw = location.strip_prefix("file://").unwrap_or(location);
    let path = std::path::Path::new(raw);
    if !path.is_absolute() {
        return Err(Error::Plan(format!(
            "catalog location `{location}` must be an absolute path. Relative paths are \
             rejected rather than resolved against the working directory, which would make \
             the same catalog point at different data depending on where the process was \
             started. In `oxidant.yaml` you can build one from `${{CONFIG_DIR}}` or your \
             own `vars:`."
        )));
    }
    Ok(path.to_path_buf())
}

/// Split `s3://bucket/prefix` into its bucket and prefix.
fn split_bucket(location: &str, scheme: &str) -> Result<(String, String)> {
    let rest = location
        .strip_prefix(scheme)
        .ok_or_else(|| Error::Plan(format!("`{location}` is not a `{scheme}` URI")))?;
    let (bucket, prefix) = match rest.split_once('/') {
        Some((bucket, prefix)) => (bucket, prefix),
        None => (rest, ""),
    };
    if bucket.is_empty() {
        return Err(Error::Plan(format!("`{location}` has no bucket")));
    }
    Ok((bucket.to_string(), prefix.trim_matches('/').to_string()))
}

/// Map an Iceberg `s3.*` or Hadoop `fs.s3a.*` option name to the `object_store` config key.
///
/// The same vocabulary the engine's own S3 registration accepts, so a table's `storage_options`
/// mean one thing across the catalog and the scan rather than two.
fn normalize_s3_key(key: &str) -> Option<String> {
    let key = key.trim().to_ascii_lowercase();
    let mapped = match key.as_str() {
        "s3.access-key-id" | "fs.s3a.access.key" => "access_key_id",
        "s3.secret-access-key" | "fs.s3a.secret.key" => "secret_access_key",
        "s3.session-token" | "fs.s3a.session.token" => "token",
        "s3.endpoint" | "fs.s3a.endpoint" => "endpoint",
        "s3.region" | "fs.s3a.endpoint.region" => "region",
        "s3.allow-http" => "allow_http",
        "s3.skip-signature" => "skip_signature",
        "s3.virtual-hosted-style-request" => "virtual_hosted_style_request",
        other => {
            // Generic `s3.<foo-bar>` → `foo_bar` fallback, matching the engine's own mapping.
            let rest = other.strip_prefix("s3.")?;
            return Some(rest.replace('-', "_"));
        }
    };
    Some(mapped.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schemes_are_detected_without_mistaking_paths_for_uris() {
        assert_eq!(scheme_of("s3://bucket/x"), Some("s3"));
        assert_eq!(scheme_of("file:///data"), Some("file"));
        assert_eq!(scheme_of("./data/events"), None);
        assert_eq!(scheme_of("/abs/path"), None);
    }

    #[test]
    fn s3_locations_split_into_bucket_and_prefix() {
        assert_eq!(
            split_bucket("s3://bucket/a/b", "s3://").unwrap(),
            ("bucket".to_string(), "a/b".to_string())
        );
        // A bare bucket with no prefix is the warehouse root.
        assert_eq!(
            split_bucket("s3://bucket", "s3://").unwrap(),
            ("bucket".to_string(), String::new())
        );
        assert!(split_bucket("s3://", "s3://").is_err());
    }

    #[test]
    fn both_option_vocabularies_map_to_the_same_keys() {
        assert_eq!(
            normalize_s3_key("s3.access-key-id").as_deref(),
            Some("access_key_id")
        );
        assert_eq!(
            normalize_s3_key("fs.s3a.access.key").as_deref(),
            Some("access_key_id")
        );
        assert_eq!(normalize_s3_key("s3.endpoint").as_deref(), Some("endpoint"));
        // Non-S3 options (a CSV reader's `header`) must not be handed to the S3 builder.
        assert_eq!(normalize_s3_key("header"), None);
        assert_eq!(normalize_s3_key("delimiter"), None);
    }

    #[test]
    fn a_relative_location_is_rejected_rather_than_resolved_against_the_cwd() {
        // This is the backstop for the paths `oxidant-config` never sees: `--catalog-conf`,
        // `OXIDANT_CATALOG_CONF`, and a PySpark client's `Config` RPC land here directly. Silently
        // resolving against the cwd would make one catalog config name different data depending on
        // where the process happened to be started.
        for relative in ["./data/events", "data/events", "../data/events"] {
            match local_path(relative) {
                Ok(resolved) => panic!("`{relative}` was accepted as `{}`", resolved.display()),
                Err(e) => assert!(
                    e.to_string().contains("absolute"),
                    "the error should say what is wrong: {e}"
                ),
            }
        }
        // `file://` is a spelling of an absolute path and stays accepted.
        assert!(local_path("file:///data/events").is_ok());
    }

    #[test]
    fn a_warehouse_directory_that_does_not_exist_yet_still_resolves() {
        // A fresh warehouse is the normal first-run state; requiring the directory up front
        // would make every new deployment a two-step.
        let dir = tempfile::tempdir().expect("tempdir");
        let missing = dir.path().join("not-created-yet");
        let (_store, prefix) =
            store_for_location(&missing.to_string_lossy(), &HashMap::new()).expect("resolves");
        assert!(prefix.as_ref().ends_with("not-created-yet"));
    }

    #[test]
    fn an_unsupported_scheme_is_named_in_the_error() {
        let err = store_for_location("gs://bucket/x", &HashMap::new()).expect_err("unsupported");
        assert!(err.to_string().contains("gs"), "got: {err}");
    }
}

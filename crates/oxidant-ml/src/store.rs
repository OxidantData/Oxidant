//! Where model bytes come from.
//!
//! SPIKE (issue #118). `oxidant-ml` knows how to read a **local** model on its own so the crate
//! is testable standalone; anything remote is delegated to a [`BlobSource`] that the engine
//! installs at startup ([`install_blob_source`]). That is deliberate: an `s3://` model URI must
//! resolve through the *same* region resolution, assumed-role credentials, and `s3_io` wrapping
//! that a table scan of the same bucket would use, and all of that lives in `oxidant-loom`.

use std::sync::{Arc, OnceLock};

use oxidant_common::{Error, Result};

/// A cheap identity probe for a model object: what changes when the model is republished.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BlobVersion {
    pub size: u64,
    /// ETag (S3) or mtime-nanos (local). `None` when the backend offers neither.
    pub tag: Option<String>,
}

impl BlobVersion {
    /// The string that goes into the model cache key alongside the URI.
    pub fn cache_token(&self) -> String {
        match &self.tag {
            Some(tag) => format!("{}:{tag}", self.size),
            None => self.size.to_string(),
        }
    }
}

/// Resolves model URIs to bytes. Implemented by `oxidant-loom` over the engine's object store.
pub trait BlobSource: Send + Sync + std::fmt::Debug {
    /// True if this source handles `uri`. Sources are tried before the local fallback.
    fn handles(&self, uri: &str) -> bool;
    fn stat(&self, uri: &str) -> Result<BlobVersion>;
    fn fetch(&self, uri: &str) -> Result<Vec<u8>>;
}

static REMOTE: OnceLock<Arc<dyn BlobSource>> = OnceLock::new();

/// Install the engine's object-store-backed source. First call wins; later calls are ignored so
/// a second `Engine` in the same process cannot swap the source out from under a cached model.
pub fn install_blob_source(source: Arc<dyn BlobSource>) {
    let _ = REMOTE.set(source);
}

/// Whether a remote source has been installed — reported by the spike bins so a "works on
/// local paths only" run is never mistaken for a working `s3://` path.
pub fn remote_source() -> Option<&'static Arc<dyn BlobSource>> {
    REMOTE.get()
}

fn source_for(uri: &str) -> Option<&'static Arc<dyn BlobSource>> {
    REMOTE.get().filter(|s| s.handles(uri))
}

/// Identity-probe `uri` without downloading it (an S3 HEAD, or a local `stat`).
pub fn stat(uri: &str) -> Result<BlobVersion> {
    if let Some(source) = source_for(uri) {
        return source.stat(uri);
    }
    let path = local_path(uri)?;
    let meta = std::fs::metadata(&path)
        .map_err(|e| Error::Io(format!("ml_predict: cannot stat model `{uri}`: {e}")))?;
    let tag = meta
        .modified()
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_nanos().to_string());
    Ok(BlobVersion {
        size: meta.len(),
        tag,
    })
}

/// Download `uri` in full.
pub fn fetch(uri: &str) -> Result<Vec<u8>> {
    if let Some(source) = source_for(uri) {
        return source.fetch(uri);
    }
    let path = local_path(uri)?;
    std::fs::read(&path)
        .map_err(|e| Error::Io(format!("ml_predict: cannot read model `{uri}`: {e}")))
}

/// Strip a `file://` prefix; reject any other scheme that no installed source claimed, so a
/// typo'd `s3://` URI fails with "no source" instead of "file not found".
fn local_path(uri: &str) -> Result<String> {
    if let Some(rest) = uri.strip_prefix("file://") {
        return Ok(rest.to_string());
    }
    if let Some((scheme, _)) = uri.split_once("://") {
        return Err(Error::Plan(format!(
            "ml_predict: no object store is registered for `{scheme}://` model URIs \
             (the engine installs one at startup; a standalone `oxidant-ml` reads local paths only)"
        )));
    }
    Ok(uri.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cache_token_includes_the_tag_when_the_backend_offers_one() {
        let versioned = BlobVersion {
            size: 53428,
            tag: Some("\"abc123\"".into()),
        };
        assert_eq!(versioned.cache_token(), "53428:\"abc123\"");
        assert_eq!(
            BlobVersion {
                size: 53428,
                tag: None
            }
            .cache_token(),
            "53428"
        );
    }

    #[test]
    fn republishing_a_model_changes_its_cache_token() {
        let v1 = BlobVersion {
            size: 100,
            tag: Some("etag-1".into()),
        };
        // Same byte count, different content: the ETag is what saves us here, which is why the
        // token is not just the size.
        let v2 = BlobVersion {
            size: 100,
            tag: Some("etag-2".into()),
        };
        assert_ne!(v1.cache_token(), v2.cache_token());
    }

    #[test]
    fn file_urls_and_bare_paths_both_resolve_locally() {
        assert_eq!(local_path("/tmp/m.onnx").unwrap(), "/tmp/m.onnx");
        assert_eq!(local_path("file:///tmp/m.onnx").unwrap(), "/tmp/m.onnx");
    }

    #[test]
    fn an_unclaimed_scheme_fails_loudly_instead_of_as_a_missing_file() {
        // Without this, a typo'd or unconfigured `s3://` URI would surface as
        // "No such file or directory: s3://bucket/m.onnx", which sends you looking in the
        // wrong place entirely.
        let err = local_path("s3://bucket/m.onnx").unwrap_err().to_string();
        assert!(err.contains("no object store is registered"), "{err}");
        assert!(err.contains("s3://"), "{err}");
    }
}

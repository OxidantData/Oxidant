//! Resolves `s3://` ONNX model URIs through the engine's own object-store wiring.
//!
//! SPIKE (issue #118). `oxidant-ml` reads local models by itself; everything remote comes
//! through this [`oxidant_ml::BlobSource`], which routes to [`catalog_bridge::ensure_remote_store`]
//! — the same function the read and write paths use for table data. A model in a bucket
//! therefore resolves with the same region resolution, the same default/assumed-role credential
//! chain, and the same `s3_io` instrumentation + concurrent-range wrapper that a `SELECT` from
//! that bucket would get, rather than a second, divergent S3 client.
//!
//! It keeps its own bare `SessionContext` purely as a place to hang the object-store registry:
//! a model URI has no catalog table behind it and so no per-table `storage_options`, and holding
//! a real `Engine`'s context in a process-global would keep that engine alive forever.

use std::sync::{Arc, OnceLock};

use datafusion::datasource::listing::ListingTableUrl;
use datafusion::prelude::SessionContext;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use oxidant_common::{Error, Result};
use oxidant_ml::{BlobSource, BlobVersion};

use crate::catalog_bridge;

/// Install the engine's model blob source. Idempotent; safe to call from every `Engine::new`.
pub fn install() {
    oxidant_ml::install_blob_source(Arc::new(EngineBlobSource));
}

#[derive(Debug)]
struct EngineBlobSource;

impl BlobSource for EngineBlobSource {
    fn handles(&self, uri: &str) -> bool {
        uri.starts_with("s3://")
    }

    fn stat(&self, uri: &str) -> Result<BlobVersion> {
        let (store, path) = resolve(uri)?;
        let meta = block_on(async move { store.head(&path).await })
            .map_err(|e| Error::Io(format!("ml_predict: HEAD `{uri}`: {e}")))?;
        Ok(BlobVersion {
            size: meta.size,
            tag: meta.e_tag,
        })
    }

    fn fetch(&self, uri: &str) -> Result<Vec<u8>> {
        let (store, path) = resolve(uri)?;
        let bytes = block_on(async move { store.get(&path).await?.bytes().await })
            .map_err(|e| Error::Io(format!("ml_predict: GET `{uri}`: {e}")))?;
        Ok(bytes.to_vec())
    }
}

/// The registry the model object stores are registered into. Separate from any `Engine`'s
/// session (see the module docs) but populated by the identical code path.
fn registry() -> &'static SessionContext {
    static REGISTRY: OnceLock<SessionContext> = OnceLock::new();
    REGISTRY.get_or_init(SessionContext::new)
}

fn resolve(uri: &str) -> Result<(Arc<dyn ObjectStore>, Path)> {
    let url = ListingTableUrl::parse(uri)
        .map_err(|e| Error::Plan(format!("ml_predict: bad model URI `{uri}`: {e}")))?;
    let state = registry().state();
    catalog_bridge::ensure_remote_store(&state, &url, None)
        .map_err(|e| Error::Io(format!("ml_predict: object store for `{uri}`: {e}")))?;
    let store = state
        .runtime_env()
        .object_store(&url)
        .map_err(|e| Error::Io(format!("ml_predict: object store for `{uri}`: {e}")))?;
    // `ListingTableUrl::prefix` is the in-bucket path; a model is one object, not a listing root.
    Ok((store, url.prefix().clone()))
}

/// Run one object-store future to completion from a synchronous UDF.
///
/// `ScalarUDFImpl::invoke_with_args` is called on a tokio worker thread, where both creating a
/// runtime and `Handle::block_on` panic. So the future is handed to a dedicated runtime and
/// driven from a *fresh* thread, which is allowed to block. Model loads only happen on a cache
/// miss, so the thread spawn is amortized across an entire query at worst.
fn block_on<F, T, E>(fut: F) -> std::result::Result<T, String>
where
    F: std::future::Future<Output = std::result::Result<T, E>> + Send + 'static,
    T: Send + 'static,
    E: std::fmt::Display + Send + 'static,
{
    static RT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    let rt = RT
        .get_or_init(|| {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .expect("ml model fetch runtime")
        })
        .handle()
        .clone();
    std::thread::scope(|scope| match scope.spawn(move || rt.block_on(fut)).join() {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(e)) => Err(e.to_string()),
        Err(_) => Err("model fetch thread panicked".to_string()),
    })
}

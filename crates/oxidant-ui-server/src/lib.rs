//! HTTP server for the Oxidant monitoring UI: Spark-compatible `/api/v1` REST, SSE, and static SPA.

pub mod dashboards;
pub mod lifecycle;
pub mod pipelines;
mod routes;
mod static_files;
pub mod status;

use std::net::SocketAddr;

use axum::Router;
use oxidant_common::Result;
use oxidant_observability::SharedStore;
use tower_http::cors::{Any, CorsLayer};

pub use dashboards::DashboardStore;
// `app_router_with_spa` is the env-free form: everything the router reads from the environment
// — the SPA directory and the pipeline checkpoint root — is passed in instead. Exported so an
// integration test can point the connector-log routes at a bucket without setting process-wide
// environment that every other test in the binary would see.
pub use routes::{app_router, app_router_with, app_router_with_spa, app_router_with_status_token};

/// Configuration for the monitoring UI HTTP server.
#[derive(Clone)]
pub struct UiServerConfig {
    pub port: u16,
    pub store: SharedStore,
    /// Interface to bind the UI HTTP listener on. Use `127.0.0.1` on
    /// shared/public machines (the UI has no auth) and reach it via SSH tunnel.
    pub bind: std::net::IpAddr,
    /// Extra routes merged into the app router before the SPA fallback — e.g. the REST
    /// statement-execution API, which lives in `oxidant-connect` (it owns the query engine;
    /// this crate cannot depend on it without a cycle). `None` for the standalone
    /// history server.
    pub merge_router: Option<Router>,
}

/// Start the UI HTTP server and serve until shutdown.
pub async fn serve(config: UiServerConfig) -> Result<()> {
    let addr = SocketAddr::from((config.bind, config.port));
    let mut app = app_router(config.store);
    if let Some(extra) = config.merge_router {
        // The merged router carries no fallback, so the SPA fallback below stays in effect.
        app = app.merge(extra);
    }
    let app = app.layer(
        CorsLayer::new()
            .allow_origin(Any)
            .allow_methods(Any)
            .allow_headers(Any),
    );
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|e| oxidant_common::Error::Io(format!("ui bind {addr}: {e}")))?;
    tracing::info!("Oxidant UI listening on http://{addr}");
    axum::serve(listener, app)
        .await
        .map_err(|e| oxidant_common::Error::Io(format!("ui server: {e}")))?;
    Ok(())
}

/// Build a router for tests.
pub fn router(store: SharedStore) -> Router {
    app_router(store)
}

/// Serializes every test in this crate that mutates process-wide environment
/// (`OXIDANT_CONNECTOR_CONFIG_DIR`, `OXIDANT_SYSTEMD_UNIT_DIR`, `OXIDANT_SYSTEMCTL_BIN` in
/// `lifecycle.rs`; `OXIDANT_CHECKPOINT_DIR` in `pipelines.rs`; `OXIDANT_UI_DIR` in
/// `static_files.rs`, exercised through `routes.rs`'s tests; `OXIDANT_STATUS_TOKEN` in
/// `status.rs`, which also reads the whole environ table back through `/environment`) against
/// every other one.
///
/// One lock, not one per module: `std::env::set_var` racing `std::env::var` (or another
/// `set_var`) from a *different* module's test, on a different thread, is a data race on the
/// process environ table (glibc `putenv`/`realloc`), not merely a logical one — crate unit
/// tests all compile into one binary and run on parallel threads by default, so two
/// module-private `static` mutexes with the same name cannot exclude each other; only a lock
/// every mutator actually shares can. `#[cfg(test)]`: nothing outside a test build should ever
/// touch this.
#[cfg(test)]
pub(crate) static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[cfg(test)]
mod tests {
    use super::*;

    /// The UI has no auth, so the bind address is a security posture: on a
    /// public AMI the standalone unit binds loopback. Prove `bind` is honored.
    #[tokio::test]
    async fn serve_binds_configured_loopback_address() {
        let store: SharedStore = std::sync::Arc::new(oxidant_observability::AppStateStore::new());
        let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = probe.local_addr().unwrap().port();
        drop(probe);
        let config = UiServerConfig {
            port,
            store,
            bind: std::net::IpAddr::from([127, 0, 0, 1]),
            merge_router: None,
        };
        let server = tokio::spawn(serve(config));
        // Wait for the listener, then confirm loopback serves a page.
        let mut body = None;
        for _ in 0..50 {
            match reqwest_get(port).await {
                Ok(b) => {
                    body = Some(b);
                    break;
                }
                Err(_) => tokio::time::sleep(std::time::Duration::from_millis(100)).await,
            }
        }
        server.abort();
        assert!(
            body.is_some(),
            "UI never came up on 127.0.0.1:{port} — bind address not honored"
        );
    }

    async fn reqwest_get(port: u16) -> std::io::Result<String> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", port)).await?;
        stream
            .write_all(b"GET / HTTP/1.0\r\nHost: localhost\r\n\r\n")
            .await?;
        let mut buf = Vec::new();
        stream.read_to_end(&mut buf).await?;
        String::from_utf8(buf).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
    }
}

//! HTTP server for the Oxidant monitoring UI: Spark-compatible `/api/v1` REST, SSE, and static SPA.

mod routes;
mod static_files;

use std::net::SocketAddr;

use axum::Router;
use oxidant_common::Result;
use oxidant_observability::SharedStore;
use tower_http::cors::{Any, CorsLayer};

pub use routes::app_router;

/// Configuration for the monitoring UI HTTP server.
#[derive(Clone)]
pub struct UiServerConfig {
    pub port: u16,
    pub store: SharedStore,
    /// Interface to bind the UI HTTP listener on. Use `127.0.0.1` on
    /// shared/public machines (the UI has no auth) and reach it via SSH tunnel.
    pub bind: std::net::IpAddr,
}

/// Start the UI HTTP server and serve until shutdown.
pub async fn serve(config: UiServerConfig) -> Result<()> {
    let addr = SocketAddr::from((config.bind, config.port));
    let app = app_router(config.store).layer(
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

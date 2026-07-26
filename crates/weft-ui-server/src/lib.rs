//! HTTP server for the Weft monitoring UI: Spark-compatible `/api/v1` REST, SSE, and static SPA.

mod routes;
mod static_files;

use std::net::SocketAddr;

use axum::Router;
use tower_http::cors::{Any, CorsLayer};
use weft_common::Result;
use weft_observability::SharedStore;

pub use routes::app_router;

/// Configuration for the monitoring UI HTTP server.
#[derive(Clone)]
pub struct UiServerConfig {
    pub port: u16,
    pub store: SharedStore,
}

/// Start the UI HTTP server and serve until shutdown.
pub async fn serve(config: UiServerConfig) -> Result<()> {
    let addr = SocketAddr::from(([0, 0, 0, 0], config.port));
    let app = app_router(config.store).layer(
        CorsLayer::new()
            .allow_origin(Any)
            .allow_methods(Any)
            .allow_headers(Any),
    );
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|e| weft_common::Error::Io(format!("ui bind {addr}: {e}")))?;
    tracing::info!("Weft UI listening on http://0.0.0.0:{}", config.port);
    axum::serve(listener, app)
        .await
        .map_err(|e| weft_common::Error::Io(format!("ui server: {e}")))?;
    Ok(())
}

/// Build a router for tests.
pub fn router(store: SharedStore) -> Router {
    app_router(store)
}

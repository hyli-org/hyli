//! Public API for interacting with the node.

use crate::{
    bus::{metrics::BusMetrics, BusClientSender, SharedMessageBus},
    log_error, module_bus_client, module_handle_messages,
    modules::{signal::ShutdownModule, Module},
};
use anyhow::{anyhow, Context, Result};
pub use axum::Router;
use axum::{
    body::Bytes,
    extract::{DefaultBodyLimit, Path, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use sdk::*;
use std::path::PathBuf;
use tokio_util::sync::CancellationToken;
use tower_http::catch_panic::CatchPanicLayer;
use tracing::info;

pub use client_sdk::contract_indexer::AppError;
pub use client_sdk::rest_client as client;

use super::{
    rest::request_logger,
    signal::{self, PersistModule},
};

module_bus_client! {
    struct AdminBusClient {
        sender(signal::PersistModule),
    }
}

pub struct AdminApiRunContext {
    pub port: u16,
    pub router: Router,
    pub max_body_size: usize,
    pub data_directory: PathBuf,
}

impl AdminApiRunContext {
    pub fn new(
        port: u16,
        router: Router,
        max_body_size: usize,
        data_directory: PathBuf,
    ) -> AdminApiRunContext {
        Self {
            port,
            router,
            max_body_size,
            data_directory,
        }
    }
}

pub struct AdminApi {
    port: u16,
    app: Option<Router>,
    bus: AdminBusClient,
}

impl Module for AdminApi {
    type Context = AdminApiRunContext;

    async fn build(bus: SharedMessageBus, ctx: Self::Context) -> Result<Self> {
        let app = ctx.router.merge(
            Router::new()
                .route("/v1/admin/persist", post(persist))
                .route("/v1/admin/download/{file}", get(download))
                .with_state(RouterState {
                    bus: AdminBusClient::new_from_bus(bus.new_handle()).await,
                    data_directory: ctx.data_directory,
                }),
        );
        let app = app
            .layer(CatchPanicLayer::custom(handle_panic))
            .layer(DefaultBodyLimit::max(ctx.max_body_size)) // 10 MB
            .layer(tower_http::cors::CorsLayer::permissive())
            .layer(axum::middleware::from_fn(request_logger));
        Ok(AdminApi {
            port: ctx.port,
            app: Some(app),
            bus: AdminBusClient::new_from_bus(bus.new_handle()).await,
        })
    }

    async fn run(&mut self) -> Result<()> {
        self.serve().await
    }
}

pub async fn persist(State(mut state): State<RouterState>) -> Result<impl IntoResponse, AppError> {
    tracing::info!("Persisting modules state");
    state
        .bus
        .send(signal::PersistModule {})
        .context("Sending persist signal")?;
    Ok(())
}

pub async fn download(
    Path(file_path): Path<String>,
    State(state): State<RouterState>,
) -> Result<impl IntoResponse, AppError> {
    // Construct the full path by joining the data directory with the requested file path
    let full_path = state.data_directory.join(&file_path);

    // Security check: ensure the file is within the data directory
    if !full_path.starts_with(&state.data_directory) {
        return Err(AppError(
            StatusCode::BAD_REQUEST,
            anyhow!("Invalid file path"),
        ));
    }

    // Check if the file exists
    if !full_path.exists() {
        return Err(AppError(StatusCode::NOT_FOUND, anyhow!("File not found")));
    }

    // Check if it's actually a file (not a directory)
    if !full_path.is_file() {
        return Err(AppError(
            StatusCode::BAD_REQUEST,
            anyhow!("Path is not a file"),
        ));
    }

    // Read the file content
    match tokio::fs::read(&full_path).await {
        Ok(content) => {
            // Try to determine content type based on file extension
            let content_type = if let Some(ext) = full_path.extension() {
                match ext.to_str().unwrap_or("").to_lowercase().as_str() {
                    "json" => "application/json",
                    "txt" => "text/plain",
                    "bin" => "application/octet-stream",
                    _ => "application/octet-stream",
                }
            } else {
                "application/octet-stream"
            };

            Ok((
                StatusCode::OK,
                [(header::CONTENT_TYPE, content_type)],
                Bytes::from(content),
            ))
        }
        Err(_) => Err(AppError(
            StatusCode::INTERNAL_SERVER_ERROR,
            anyhow!("Failed to read file"),
        )),
    }
}

pub struct RouterState {
    bus: AdminBusClient,
    data_directory: PathBuf,
}

impl AdminApi {
    pub async fn serve(&mut self) -> Result<()> {
        info!(
            "📡  Starting {} module, listening on port {}",
            std::any::type_name::<Self>(),
            self.port
        );

        let listener = hyle_net::net::bind_tcp_listener(self.port)
            .await
            .context("Starting rest server")?;

        #[allow(
            clippy::expect_used,
            reason = "app is guaranteed to be set during initialization"
        )]
        let app = self.app.take().expect("app is not set");

        // On module shutdown, we want to shutdown the axum server and wait for its shutdown to complete.
        let axum_cancel_token = CancellationToken::new();
        let axum_server = tokio::spawn({
            let token = axum_cancel_token.clone();
            async move {
                log_error!(
                    axum::serve(listener, app)
                        .with_graceful_shutdown(async move {
                            token.cancelled().await;
                        })
                        .await,
                    "serving Axum"
                )?;
                Ok::<(), anyhow::Error>(())
            }
        });
        module_handle_messages! {
            on_self self,
            delay_shutdown_until {
                // When the module tries to shutdown it'll cancel the token
                // and then we actually exit the loop when axum is done.
                axum_cancel_token.cancel();
                axum_server.is_finished()
            },
        };

        Ok(())
    }
}

impl Clone for RouterState {
    fn clone(&self) -> Self {
        Self {
            bus: self.bus.clone(),
            data_directory: self.data_directory.clone(),
        }
    }
}

impl Clone for AdminBusClient {
    fn clone(&self) -> AdminBusClient {
        use crate::utils::static_type_map::Pick;

        AdminBusClient::new(
            Pick::<BusMetrics>::get(self).clone(),
            Pick::<tokio::sync::broadcast::Sender<PersistModule>>::get(self).clone(),
            Pick::<tokio::sync::broadcast::Receiver<ShutdownModule>>::get(self).resubscribe(),
            Pick::<tokio::sync::broadcast::Receiver<PersistModule>>::get(self).resubscribe(),
        )
    }
}

fn handle_panic(err: Box<dyn std::any::Any + Send + 'static>) -> Response<String> {
    let details = if let Some(s) = err.downcast_ref::<String>() {
        s.clone()
    } else if let Some(s) = err.downcast_ref::<&str>() {
        s.to_string()
    } else {
        "Unknown panic message".to_string()
    };

    tracing::error!("Panic occurred in Axum route: {}", details);

    let body = serde_json::json!({
        "error": {
            "kind": "panic",
            "details": details,
        }
    });
    let body = serde_json::to_string(&body).unwrap();

    Response::builder()
        .status(StatusCode::INTERNAL_SERVER_ERROR)
        .header(header::CONTENT_TYPE, "application/json")
        .body(body)
        .unwrap()
}

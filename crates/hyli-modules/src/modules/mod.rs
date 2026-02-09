//! Hyli modules - domain-specific modules built on top of the core module system

// Re-export core module system from hyli-bus
pub use hyli_bus::module_bus_client;
pub use hyli_bus::modules::{Module, ModulesHandler, ShutdownClient, files, signal};

use axum::Router;

// Domain-specific modules
pub mod admin;
pub mod block_processor;
pub mod bus_ws_connector;
#[cfg(feature = "db")]
pub mod contract_listener;
pub mod contract_state_indexer;
pub mod da_listener;
pub mod da_listener_metrics;
pub mod data_availability;
pub mod gcs_uploader;
pub mod gcs_uploader_metrics;
#[cfg(feature = "db")]
pub mod indexer;
pub mod prover;
pub mod prover_metrics;
pub mod rest;
pub mod websocket;

/// API context for building REST APIs across modules
#[derive(Default)]
pub struct BuildApiContextInner {
    pub router: std::sync::Mutex<Option<Router>>,
    pub openapi: std::sync::Mutex<utoipa::openapi::OpenApi>,
}
pub type SharedBuildApiCtx = std::sync::Arc<BuildApiContextInner>;

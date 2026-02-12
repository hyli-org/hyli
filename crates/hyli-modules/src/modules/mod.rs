//! Hyli modules - domain-specific modules built on top of the core module system

// Re-export core module system from hyli-bus
pub use hyli_bus::module_bus_client;
pub use hyli_bus::modules::{files, signal, Module, ModulesHandler, ShutdownClient};

use axum::Router;

// Domain-specific modules
pub mod admin;
pub mod block_processor;
pub mod bus_ws_connector;
#[cfg(feature = "db")]
pub mod contract_listener;
#[cfg(feature = "indexer")]
pub mod contract_state_indexer;
pub mod da_listener;
pub mod da_listener_metrics;
pub mod data_availability;
#[cfg(feature = "gcs")]
pub mod gcs_uploader;
#[cfg(feature = "gcs")]
pub mod gcs_uploader_metrics;
#[cfg(feature = "db")]
pub mod indexer;
#[cfg(feature = "rest")]
pub mod prover;
#[cfg(feature = "rest")]
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

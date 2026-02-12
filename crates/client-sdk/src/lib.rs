#[cfg(feature = "axum")]
use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};
#[cfg(not(feature = "axum"))]
use hyli_net::http::StatusCode;

/// Error type for REST API handlers, wrapping `anyhow::Error` with a status code.
pub struct AppError(pub StatusCode, pub anyhow::Error);

#[cfg(feature = "axum")]
impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        (self.0, format!("{}", self.1)).into_response()
    }
}

impl<E> From<E> for AppError
where
    E: Into<anyhow::Error>,
{
    fn from(err: E) -> Self {
        Self(StatusCode::INTERNAL_SERVER_ERROR, err.into())
    }
}

#[cfg(feature = "indexer")]
pub mod contract_indexer;
pub mod helpers;
pub mod light_executor;
#[cfg(feature = "rest")]
pub mod rest_client;
pub mod tcp_client;
/// Helper modules for testing contracts
pub mod tests;
pub mod transaction_builder;

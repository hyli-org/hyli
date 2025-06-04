use anyhow::{Context, Result};
use std::{collections::BTreeMap, str, sync::Arc};
use tokio::sync::RwLock;
use tracing::debug;

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Router,
};
use borsh::{BorshDeserialize, BorshSerialize};
use sdk::*;
use utoipa::openapi::OpenApi;

pub use axum;
pub use utoipa;
pub use utoipa_axum;

use crate::transaction_builder::TxExecutorHandler;

#[derive(BorshSerialize, BorshDeserialize)]
pub struct ContractStateStore<State> {
    pub state: Option<State>,
    pub contract_name: ContractName,
    pub unsettled_blobs: BTreeMap<TxId, (BlobTransaction, TxContext)>,
}

pub type ContractHandlerStore<T> = Arc<RwLock<ContractStateStore<T>>>;

impl<State> Default for ContractStateStore<State> {
    fn default() -> Self {
        ContractStateStore {
            state: None,
            contract_name: Default::default(),
            unsettled_blobs: BTreeMap::new(),
        }
    }
}

pub trait ContractHandler<Event = ()>
where
    Self: Sized + TxExecutorHandler + 'static,
{
    fn api(
        store: ContractHandlerStore<Self>,
    ) -> impl std::future::Future<Output = (Router<()>, OpenApi)> + std::marker::Send;

    fn handle_transaction_success(
        &mut self,
        tx: &BlobTransaction,
        index: BlobIndex,
        tx_context: TxContext,
    ) -> Result<Option<Event>> {
        let Blob {
            contract_name,
            data: _,
        } = tx.blobs.get(index.0).context("Failed to get blob")?;

        let calldata = Calldata {
            identity: tx.identity.clone(),
            index,
            blobs: tx.blobs.clone().into(),
            tx_blob_count: tx.blobs.len(),
            tx_hash: tx.hashed(),
            tx_ctx: Some(tx_context),
            private_input: vec![],
        };

        let hyle_output = self.handle(&calldata)?;
        let program_outputs = str::from_utf8(&hyle_output.program_outputs).unwrap_or("no output");

        info!("🚀 Executed {contract_name}: {}", program_outputs);
        debug!(
            handler = %contract_name,
            "hyle_output: {:?}", hyle_output
        );
        Ok(None)
    }

    fn handle_transaction_failed(
        &mut self,
        _tx: &BlobTransaction,
        _index: BlobIndex,
        _tx_context: TxContext,
    ) -> Result<Option<Event>> {
        Ok(None)
    }

    fn handle_transaction_timeout(
        &mut self,
        _tx: &BlobTransaction,
        _index: BlobIndex,
        _tx_context: TxContext,
    ) -> Result<Option<Event>> {
        Ok(None)
    }

    fn handle_transaction_sequenced(
        &mut self,
        _tx: &BlobTransaction,
        _index: BlobIndex,
        _tx_context: TxContext,
    ) -> Result<Option<Event>> {
        Ok(None)
    }
}

// Make our own error that wraps `anyhow::Error`.
pub struct AppError(pub StatusCode, pub anyhow::Error);

// Tell axum how to convert `AppError` into a response.
impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        (self.0, format!("{}", self.1)).into_response()
    }
}

// This enables using `?` on functions that return `Result<_, anyhow::Error>` to turn them into
// `Result<_, AppError>`. That way you don't need to do that manually.
impl<E> From<E> for AppError
where
    E: Into<anyhow::Error>,
{
    fn from(err: E) -> Self {
        Self(StatusCode::INTERNAL_SERVER_ERROR, err.into())
    }
}

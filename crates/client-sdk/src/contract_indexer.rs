use anyhow::{Context, Result};
use std::{ops::Deref, str, sync::Arc};
use tokio::sync::RwLock;
use tracing::{debug, error};

use axum::Router;
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
}

pub type ContractHandlerStore<T> = Arc<RwLock<ContractStateStore<T>>>;

impl<State> Default for ContractStateStore<State> {
    fn default() -> Self {
        ContractStateStore {
            state: None,
            contract_name: Default::default(),
        }
    }
}

/// Implement this trait on your contract state to plug into [`ContractStateIndexer`].
///
/// The `Event` type parameter is what you emit to notify the rest of the app of state changes.
/// If you don't need events, omit it (defaults to `()`).
///
/// ## Method summary
///
/// | Method | When called | Default behaviour |
/// |---|---|---|
/// | `on_transaction_success` | After state is updated on success | No-op, returns `None` — **override this to emit events** |
/// | `handle_transaction_success` | Tx settled successfully | Builds calldata, calls `TxExecutorHandler::handle`, then calls `on_transaction_success` |
/// | `handle_transaction_failed` | Tx was rejected | No-op, returns `None` |
/// | `handle_transaction_timeout` | Tx timed out | No-op, returns `None` |
/// | `handle_transaction_sequenced` | Tx entered the mempool | No-op, returns `None` |
///
/// Override only the methods you care about. Any `Some(event)` you return is automatically
/// broadcast as a `CSIBusEvent<Event>` on the message bus by `ContractStateIndexer`
/// (see `crates/hyli-modules/src/modules/contract_state_indexer.rs`).
///
/// ## Emitting events without duplicating logic
///
/// Override `on_transaction_success` instead of `handle_transaction_success`.
/// By the time it is called, `self` already reflects the new state.
///
/// ```rust,ignore
/// impl ContractHandler<AmmEvent> for AmmState {
///     fn on_transaction_success(
///         &mut self, tx: &BlobTransaction, index: BlobIndex, _ctx: Arc<TxContext>,
///         _output: &HyliOutput,
///     ) -> Result<Option<AmmEvent>> {
///         // self is already updated here
///         Ok(Some(AmmEvent::LiquidityChanged { total: self.total_liquidity }))
///     }
///
///     async fn api(store: ContractHandlerStore<Self>) -> (Router<()>, OpenApi) {
///         // expose REST endpoints backed by `store`
///     }
/// }
/// ```
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
        tx_context: Arc<TxContext>,
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
            tx_ctx: Some(tx_context.deref().clone()),
            private_input: vec![],
        };

        let hyli_output = match self.handle(&calldata) {
            Ok(ho) => ho,
            Err(e) => {
                error!(
                    "Failed to handle blob {index} for contract {contract_name}: {}",
                    e
                );
                return Ok(None);
            }
        };

        let program_outputs = str::from_utf8(&hyli_output.program_outputs).unwrap_or("no output");

        info!("🚀 Executed {contract_name}: {}", program_outputs);
        debug!(
            handler = %contract_name,
            "hyli_output: {:?}", hyli_output
        );
        self.on_transaction_success(tx, index, tx_context, &hyli_output)
    }

    /// Called by the default `handle_transaction_success` after `TxExecutorHandler::handle`
    /// has already mutated `self`. Override this to emit events without duplicating any
    /// calldata-building or state-application logic.
    fn on_transaction_success(
        &mut self,
        _tx: &BlobTransaction,
        _index: BlobIndex,
        _tx_context: Arc<TxContext>,
        _output: &HyliOutput,
    ) -> Result<Option<Event>> {
        Ok(None)
    }

    fn on_transaction_failed(
        &mut self,
        _tx: &BlobTransaction,
        _index: BlobIndex,
        _tx_context: Arc<TxContext>,
    ) -> Result<Option<Event>> {
        Ok(None)
    }

    fn on_transaction_timeout(
        &mut self,
        _tx: &BlobTransaction,
        _index: BlobIndex,
        _tx_context: Arc<TxContext>,
    ) -> Result<Option<Event>> {
        Ok(None)
    }

    fn on_transaction_sequenced(
        &mut self,
        _tx: &BlobTransaction,
        _index: BlobIndex,
        _tx_context: Arc<TxContext>,
    ) -> Result<Option<Event>> {
        Ok(None)
    }
}

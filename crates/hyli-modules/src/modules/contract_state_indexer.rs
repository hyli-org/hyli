use crate::{
    bus::{BusClientSender, BusMessage, SharedMessageBus},
    log_debug, log_error, module_bus_client, module_handle_messages,
    modules::{contract_listener::ContractListenerEvent, Module},
};
use anyhow::{anyhow, bail, Context, Error, Result};
use borsh::{BorshDeserialize, BorshSerialize};
use client_sdk::contract_indexer::{ContractHandler, ContractStateStore};
use hyli_bus::modules::ModulePersistOutput;
use hyli_model::api::ContractChangeType;
use sdk::api::TransactionStatusDb;
use sdk::*;
use std::{any::TypeId, ops::Deref, path::PathBuf, sync::Arc};
use tokio::sync::RwLock;
use tracing::warn;

use super::contract_listener::ContractTx;

use super::SharedBuildApiCtx;

#[derive(Debug, Clone)]
pub struct CSIBusEvent<E> {
    #[allow(unused)]
    pub event: E,
}

impl<E: BusMessage> BusMessage for CSIBusEvent<E> {
    const CAPACITY: usize = E::CAPACITY;
}

module_bus_client! {
#[derive(Debug)]
struct CSIBusClient<E: Clone + Send + Sync + BusMessage + 'static> {
    sender(CSIBusEvent<E>),
    receiver(ContractListenerEvent),
}
}

pub struct ContractStateIndexer<State, Event: Clone + Send + Sync + BusMessage + 'static = ()> {
    bus: CSIBusClient<Event>,
    store: Arc<RwLock<ContractStateStore<State>>>,
    contract_name: ContractName,
    data_dir: PathBuf,
    file: PathBuf,
}

pub struct ContractStateIndexerCtx {
    pub data_directory: PathBuf,
    pub contract_name: ContractName,
    pub api: SharedBuildApiCtx,
}

impl<State, Event> Module for ContractStateIndexer<State, Event>
where
    State: Clone
        + Sync
        + Send
        + std::fmt::Debug
        + ContractHandler<Event>
        + BorshSerialize
        + BorshDeserialize
        + 'static,
    Event: std::fmt::Debug + Clone + Send + Sync + BusMessage + 'static,
{
    type Context = ContractStateIndexerCtx;

    async fn build(bus: SharedMessageBus, ctx: Self::Context) -> Result<Self> {
        let bus = CSIBusClient::new_from_bus(bus.new_handle()).await;
        let file = PathBuf::from(format!("state_indexer_{}.bin", ctx.contract_name));

        let mut store =
            match Self::load_from_disk::<ContractStateStore<State>>(&ctx.data_directory, &file)? {
                Some(s) => s,
                None => {
                    warn!("Starting {} from default.", ctx.contract_name);
                    ContractStateStore::default()
                }
            };
        store.contract_name = ctx.contract_name.clone();
        let store = Arc::new(RwLock::new(store));

        let (nested, mut api) = State::api(Arc::clone(&store)).await;
        if let Ok(mut o) = ctx.api.openapi.lock() {
            // Deduplicate operation ids
            for p in api.paths.paths.iter_mut() {
                p.1.get = p.1.get.take().map(|mut g| {
                    g.operation_id = g.operation_id.map(|o| format!("{}_{o}", ctx.contract_name));
                    g
                });
                p.1.post = p.1.post.take().map(|mut g| {
                    g.operation_id = g.operation_id.map(|o| format!("{}_{o}", ctx.contract_name));
                    g
                });
            }
            *o = o
                .clone()
                .nest(format!("/v1/indexer/contract/{}", ctx.contract_name), api);
        }

        if let Ok(mut guard) = ctx.api.router.lock() {
            if let Some(router) = guard.take() {
                guard.replace(router.nest(
                    format!("/v1/indexer/contract/{}", ctx.contract_name).as_str(),
                    nested,
                ));
            }
        }

        Ok(ContractStateIndexer {
            bus,
            file,
            data_dir: ctx.data_directory,
            store,
            contract_name: ctx.contract_name,
        })
    }

    async fn run(&mut self) -> Result<()> {
        module_handle_messages! {
            on_self self,
            listen<ContractListenerEvent> event => {
                _ = log_error!(
                    self.handle_contract_listener_event(event).await,
                    "Handling contract listener event"
                )
            }
        };

        Ok(())
    }

    async fn persist(&mut self) -> Result<ModulePersistOutput> {
        let checksum = Self::save_on_disk::<ContractStateStore<State>>(
            &self.data_dir,
            &self.file,
            self.store.read().await.deref(),
        )?;
        Ok(vec![(self.data_dir.join(&self.file), checksum)])
    }
}

impl<State, Event> ContractStateIndexer<State, Event>
where
    State: Clone
        + Sync
        + Send
        + std::fmt::Debug
        + ContractHandler<Event>
        + BorshSerialize
        + BorshDeserialize
        + 'static,
    Event: std::fmt::Debug + Clone + Send + Sync + BusMessage + 'static,
{
    async fn handle_contract_listener_event(
        &mut self,
        event: ContractListenerEvent,
    ) -> Result<(), Error> {
        if let ContractListenerEvent::SettledTx(tx_data) = event {
            self.handle_settled_tx(tx_data).await?;
        }
        Ok(())
    }

    async fn handle_settled_tx(&mut self, tx_data: ContractTx) -> Result<(), Error> {
        let ContractTx {
            tx,
            tx_ctx,
            status,
            contract_changes,
            ..
        } = tx_data;

        if let Some(contract_change) = contract_changes.get(&self.contract_name) {
            if contract_change
                .change_types
                .contains(&ContractChangeType::Registered)
            {
                let timeout_window = match (
                    contract_change.hard_timeout.map(|t| BlockHeight(t as u64)),
                    contract_change.soft_timeout.map(|t| BlockHeight(t as u64)),
                ) {
                    (Some(hard_timeout), Some(soft_timeout)) => {
                        TimeoutWindow::timeout(hard_timeout, soft_timeout)
                    }
                    (Some(timeout), None) | (None, Some(timeout)) => {
                        TimeoutWindow::timeout(timeout, timeout)
                    }
                    (None, None) => TimeoutWindow::NoTimeout,
                };
                let contract = Contract {
                    name: self.contract_name.clone(),
                    program_id: ProgramId(contract_change.program_id.clone()),
                    state: StateCommitment(contract_change.state_commitment.clone()),
                    verifier: contract_change.verifier.clone().into(),
                    timeout_window,
                };
                self.handle_register_contract(&contract, &contract_change.metadata)
                    .await
                    .context("handling contract registration from settled tx")?;
            }
        }
        let has_contract_blob = tx
            .blobs
            .iter()
            .any(|blob| blob.contract_name == self.contract_name);
        if !has_contract_blob {
            return Ok(());
        }

        match status {
            TransactionStatusDb::Success => {
                self.handle_tx(&tx, tx_ctx, |state, tx, index, ctx| {
                    state.handle_transaction_success(tx, index, ctx)
                })
                .await
                .context("handling settled tx success")?;
            }
            TransactionStatusDb::Failure => {
                self.handle_tx(&tx, tx_ctx, |state, tx, index, ctx| {
                    state.handle_transaction_failed(tx, index, ctx)
                })
                .await
                .context("handling settled tx failure")?;
            }
            TransactionStatusDb::TimedOut => {
                self.handle_tx(&tx, tx_ctx, |state, tx, index, ctx| {
                    state.handle_transaction_timeout(tx, index, ctx)
                })
                .await
                .context("handling settled tx timeout")?;
            }
            _ => {}
        }
        Ok(())
    }

    async fn handle_tx<F>(
        &mut self,
        tx: &BlobTransaction,
        tx_ctx: Arc<TxContext>,
        handler: F,
    ) -> Result<()>
    where
        F: Fn(&mut State, &BlobTransaction, BlobIndex, Arc<TxContext>) -> Result<Option<Event>>,
    {
        let mut store = self.store.write().await;
        let state = store
            .state
            .as_mut()
            .ok_or(anyhow!("No state found for {}", self.contract_name))?;

        for (index, Blob { contract_name, .. }) in tx.blobs.iter().enumerate() {
            if self.contract_name != *contract_name {
                continue;
            }

            let event = handler(state, tx, BlobIndex(index), tx_ctx.clone())?;
            if TypeId::of::<Event>() != TypeId::of::<()>() {
                if let Some(event) = event {
                    let _ = log_debug!(
                        self.bus.send(CSIBusEvent { event }),
                        "Sending CSI bus event"
                    );
                }
            }
        }
        Ok(())
    }

    async fn handle_register_contract(
        &self,
        contract: &Contract,
        metadata: &Option<Vec<u8>>,
    ) -> Result<()> {
        let mut store = self.store.write().await;
        if let Some(state) = store.state.as_ref() {
            tracing::warn!(cn = %self.contract_name, "⚠️  Got re-register contract '{}'", self.contract_name);
            if state.get_state_commitment() == contract.state {
                tracing::info!(cn = %self.contract_name, "📝 Re-register contract '{}' with same state commitment", contract.name);
            } else {
                let previous_state = contract.state.clone();
                let state = State::construct_state(&self.contract_name, contract, metadata)?;
                if previous_state != state.get_state_commitment() {
                    bail!("Rebuilt contract '{}' state commitment does not match the one in the register effect", self.contract_name);
                }
                tracing::warn!(cn = %self.contract_name, "📝 Contract '{}' re-built initial state", self.contract_name);
                store.state = Some(state);
            }
        } else {
            let state = State::construct_state(&self.contract_name, contract, metadata)?;
            tracing::info!(cn = %self.contract_name, "📝 Registered supported contract '{}'", self.contract_name);
            store.state = Some(state);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use client_sdk::transaction_builder::TxExecutorHandler;
    use sdk::*;
    use utoipa::openapi::OpenApi;

    use super::*;
    use crate::bus::SharedMessageBus;
    use std::sync::Arc;

    #[derive(Clone, Debug, Default, BorshSerialize, BorshDeserialize)]
    struct MockState(Vec<u8>);

    impl TryFrom<StateCommitment> for MockState {
        type Error = Error;

        fn try_from(value: StateCommitment) -> Result<Self> {
            Ok(MockState(value.0))
        }
    }

    impl ZkContract for MockState {
        fn execute(&mut self, _calldata: &Calldata) -> RunResult {
            Err("not implemented".into())
        }

        fn commit(&self) -> StateCommitment {
            StateCommitment(vec![])
        }
    }

    impl FullStateRevert for MockState {}

    impl TxExecutorHandler for MockState {
        type Contract = MockState;

        fn handle(&mut self, _: &Calldata) -> Result<HyliOutput> {
            anyhow::bail!("not implemented");
        }

        fn build_commitment_metadata(&self, _: &Blob) -> Result<Vec<u8>> {
            anyhow::bail!("not implemented");
        }

        fn construct_state(
            _: &sdk::ContractName,
            _: &sdk::Contract,
            _: &Option<Vec<u8>>,
        ) -> Result<Self> {
            Ok(Self::default())
        }
        fn get_state_commitment(&self) -> StateCommitment {
            self.commit()
        }
    }

    impl ContractHandler for MockState {
        fn handle_transaction_success(
            &mut self,
            tx: &BlobTransaction,
            index: BlobIndex,
            _tx_context: Arc<TxContext>,
        ) -> Result<Option<()>> {
            self.0 = tx.blobs.get(index.0).unwrap().data.0.clone();
            Ok(None)
        }

        async fn api(_store: Arc<RwLock<ContractStateStore<Self>>>) -> (axum::Router<()>, OpenApi) {
            (axum::Router::new(), OpenApi::default())
        }
    }

    async fn build_indexer(contract_name: ContractName) -> ContractStateIndexer<MockState> {
        let ctx = ContractStateIndexerCtx {
            contract_name,
            data_directory: PathBuf::from("test_data"),
            api: Default::default(),
        };

        ContractStateIndexer::<MockState>::build(SharedMessageBus::new(), ctx)
            .await
            .unwrap()
    }

    async fn register_contract(indexer: &mut ContractStateIndexer<MockState>) {
        let state_commitment = StateCommitment::default();
        let contract = Contract {
            state: state_commitment,
            verifier: "test".into(),
            program_id: ProgramId(vec![]),
            ..Default::default()
        };
        indexer
            .handle_register_contract(&contract, &None)
            .await
            .unwrap();
    }

    #[test_log::test(tokio::test)]
    async fn test_handle_register_contract() {
        let contract_name = ContractName::from("test_contract");
        let mut indexer = build_indexer(contract_name.clone()).await;

        register_contract(&mut indexer).await;

        let store = indexer.store.read().await;
        assert!(store.state.is_some());
    }

    #[test_log::test(tokio::test)]
    async fn test_settle_tx() {
        let contract_name = ContractName::from("test_contract");
        let blob = Blob {
            contract_name: contract_name.clone(),
            data: BlobData(vec![1, 2, 3]),
        };
        let tx = BlobTransaction::new("test", vec![blob]);
        let tx_context = Arc::new(TxContext::default());

        let mut indexer = build_indexer(contract_name.clone()).await;
        register_contract(&mut indexer).await;

        indexer
            .handle_tx(&tx, tx_context, |state, tx, index, ctx| {
                state.handle_transaction_success(tx, index, ctx)
            })
            .await
            .unwrap();

        let store = indexer.store.read().await;
        assert_eq!(store.state.clone().unwrap().0, vec![1, 2, 3]);
    }

    #[test_log::test(tokio::test)]
    async fn test_handle_contract_listener_event_settled_tx() {
        let contract_name = ContractName::from("test_contract");
        let mut indexer = build_indexer(contract_name.clone()).await;
        register_contract(&mut indexer).await;

        let blob = Blob {
            contract_name: contract_name.clone(),
            data: BlobData(vec![1, 2, 3]),
        };
        let tx = BlobTransaction::new("test", vec![blob]);
        let tx_id = TxId(DataProposalHash::default(), tx.hashed());
        let tx_context = Arc::new(TxContext::default());

        let event = ContractListenerEvent::SettledTx(ContractTx {
            tx_id,
            tx,
            tx_ctx: tx_context,
            status: TransactionStatusDb::Success,
            contract_changes: std::collections::HashMap::new(),
        });

        indexer.handle_contract_listener_event(event).await.unwrap();
        let store = indexer.store.read().await;
        assert_eq!(store.state.clone().unwrap().0, vec![1, 2, 3]);
    }

    #[test_log::test(tokio::test)]
    async fn test_handle_contract_listener_event_settled_tx_ignores_other_contract_blob() {
        let contract_name = ContractName::from("test_contract");
        let mut indexer = build_indexer(contract_name.clone()).await;
        register_contract(&mut indexer).await;

        let blob = Blob {
            contract_name: ContractName::from("other_contract"),
            data: BlobData(vec![9, 9, 9]),
        };
        let tx = BlobTransaction::new("test", vec![blob]);
        let tx_id = TxId(DataProposalHash::default(), tx.hashed());
        let tx_context = Arc::new(TxContext::default());

        let event = ContractListenerEvent::SettledTx(ContractTx {
            tx_id,
            tx,
            tx_ctx: tx_context,
            status: TransactionStatusDb::Success,
            contract_changes: std::collections::HashMap::new(),
        });

        indexer.handle_contract_listener_event(event).await.unwrap();
        let store = indexer.store.read().await;
        assert_eq!(store.state.clone().unwrap().0, Vec::<u8>::new());
    }

    #[test_log::test(tokio::test)]
    async fn test_handle_contract_listener_event_settled_tx_failure_with_empty_contract_changes() {
        let contract_name = ContractName::from("test_contract");
        let mut indexer = build_indexer(contract_name.clone()).await;
        register_contract(&mut indexer).await;

        let blob = Blob {
            contract_name: contract_name.clone(),
            data: BlobData(vec![4, 5, 6]),
        };
        let tx = BlobTransaction::new("test", vec![blob]);
        let tx_id = TxId(DataProposalHash::default(), tx.hashed());
        let tx_context = Arc::new(TxContext::default());

        let event = ContractListenerEvent::SettledTx(ContractTx {
            tx_id,
            tx,
            tx_ctx: tx_context,
            status: TransactionStatusDb::Failure,
            contract_changes: std::collections::HashMap::new(),
        });

        indexer.handle_contract_listener_event(event).await.unwrap();
        let store = indexer.store.read().await;
        assert_eq!(store.state.clone().unwrap().0, Vec::<u8>::new());
    }

    #[test_log::test(tokio::test)]
    async fn test_handle_contract_listener_event_settled_tx_timeout_with_empty_contract_changes() {
        let contract_name = ContractName::from("test_contract");
        let mut indexer = build_indexer(contract_name.clone()).await;
        register_contract(&mut indexer).await;

        let blob = Blob {
            contract_name: contract_name.clone(),
            data: BlobData(vec![7, 8, 9]),
        };
        let tx = BlobTransaction::new("test", vec![blob]);
        let tx_id = TxId(DataProposalHash::default(), tx.hashed());
        let tx_context = Arc::new(TxContext::default());

        let event = ContractListenerEvent::SettledTx(ContractTx {
            tx_id,
            tx,
            tx_ctx: tx_context,
            status: TransactionStatusDb::TimedOut,
            contract_changes: std::collections::HashMap::new(),
        });

        indexer.handle_contract_listener_event(event).await.unwrap();
        let store = indexer.store.read().await;
        assert_eq!(store.state.clone().unwrap().0, Vec::<u8>::new());
    }
}

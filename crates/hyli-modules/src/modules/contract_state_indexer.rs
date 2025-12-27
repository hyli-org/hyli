use crate::{
    bus::{BusClientSender, BusMessage, SharedMessageBus},
    log_debug, log_error, module_bus_client, module_handle_messages,
    modules::Module,
};
use anyhow::{anyhow, bail, Context, Error, Result};
use borsh::{BorshDeserialize, BorshSerialize};
use client_sdk::contract_indexer::{ContractHandler, ContractStateStore};
use sdk::*;
use std::{any::TypeId, ops::Deref, path::PathBuf, sync::Arc};
use tokio::sync::RwLock;
use tracing::debug;

use crate::node_state::module::NodeStateEvent;

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
    receiver(NodeStateEvent),
}
}

pub struct ContractStateIndexer<State, Event: Clone + Send + Sync + BusMessage + 'static = ()> {
    bus: CSIBusClient<Event>,
    store: Arc<RwLock<ContractStateStore<State>>>,
    contract_name: ContractName,
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
        let file = ctx
            .data_directory
            .join(format!("state_indexer_{}.bin", ctx.contract_name).as_str());

        let mut store =
            Self::load_from_disk_or_default::<ContractStateStore<State>>(file.as_path());
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
            store,
            contract_name: ctx.contract_name,
        })
    }

    async fn run(&mut self) -> Result<()> {
        module_handle_messages! {
            on_self self,
            listen<NodeStateEvent> event => {
                _ = log_error!(
                    self.handle_node_state_event(event).await,
                    "Handling node state event"
                )
            }
        };

        Ok(())
    }

    async fn persist(&mut self) -> Result<()> {
        if let Err(e) = Self::save_on_disk::<ContractStateStore<State>>(
            self.file.as_path(),
            self.store.read().await.deref(),
        ) {
            tracing::warn!(cn = %self.contract_name, "Failed to save contract state indexer on disk: {}", e);
        }

        Ok(())
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
    /// Note: Each copy of the contract state indexer does the same handle_block on each data event
    /// coming from node state.
    async fn handle_node_state_event(&mut self, event: NodeStateEvent) -> Result<(), Error> {
        let NodeStateEvent::NewBlock(block) = event;
        self.handle_processed_block(block.signed_block, block.stateful_events)
            .await?;

        Ok(())
    }

    async fn handle_tx<F>(&mut self, tx: &UnsettledBlobTransaction, handler: F) -> Result<()>
    where
        F: Fn(&mut State, &BlobTransaction, BlobIndex, Arc<TxContext>) -> Result<Option<Event>>,
    {
        let mut store = self.store.write().await;
        let state = store
            .state
            .as_mut()
            .ok_or(anyhow!("No state found for {}", self.contract_name))?;

        let tx_ctx = tx.tx_context.clone();
        for (index, Blob { contract_name, .. }) in tx.tx.blobs.iter().enumerate() {
            if self.contract_name != *contract_name {
                continue;
            }

            let event = handler(state, &tx.tx, BlobIndex(index), tx_ctx.clone())?;
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

    async fn handle_processed_block(
        &mut self,
        block: Arc<SignedBlock>,
        stateful_events: Arc<StatefulEvents>,
    ) -> Result<()> {
        if !stateful_events.events.is_empty() {
            debug!(handler = %self.contract_name, "🔨 Processing block: {} with {} events", block.consensus_proposal.slot, stateful_events.events.len());
        }

        for (_, event) in &stateful_events.events {
            match event {
                StatefulEvent::ContractRegistration(name, contract, metadata) => {
                    if self.contract_name == *name {
                        self.handle_register_contract(contract, metadata).await?;
                    }
                }
                StatefulEvent::ContractDelete(..) | StatefulEvent::ContractUpdate(..) => {
                    // TODO: Not supported yet
                }
                StatefulEvent::SequencedTx(..) => {
                    // Nothing to do so far
                }
                StatefulEvent::WaitingSequencingTx(..) => {
                    // Transaction is waiting sequencing, nothing to do yet
                }
                StatefulEvent::SettledTx(tx) => {
                    self.handle_tx(tx, |state, tx, index, ctx| {
                        state.handle_transaction_success(tx, index, ctx)
                    })
                    .await
                    .context("handling settled tx")?;
                }
                StatefulEvent::FailedTx(tx) => {
                    self.handle_tx(tx, |state, tx, index, ctx| {
                        state.handle_transaction_failed(tx, index, ctx)
                    })
                    .await
                    .context("handling failed tx")?;
                }
                StatefulEvent::TimedOutTx(tx) => {
                    self.handle_tx(tx, |state, tx, index, ctx| {
                        state.handle_transaction_timeout(tx, index, ctx)
                    })
                    .await
                    .context("handling timed out tx")?;
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
    use crate::bus::metrics::BusMetrics;
    use crate::bus::SharedMessageBus;
    use crate::node_state::NodeState;
    use std::collections::BTreeMap;
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

        ContractStateIndexer::<MockState>::build(
            SharedMessageBus::new(BusMetrics::global("global".to_string())),
            ctx,
        )
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
        let tx_id = TxId(DataProposalHash::default(), tx.hashed());
        let tx_context = Arc::new(TxContext::default());

        let mut indexer = build_indexer(contract_name.clone()).await;
        register_contract(&mut indexer).await;

        indexer
            .handle_tx(
                &UnsettledBlobTransaction {
                    tx_id,
                    tx: tx.clone(),
                    blobs_hash: tx.blobs_hash(),
                    possible_proofs: BTreeMap::new(),
                    tx_context,
                    settleable_contracts: std::collections::HashSet::new(),
                },
                |state, tx, index, ctx| state.handle_transaction_success(tx, index, ctx),
            )
            .await
            .unwrap();

        let store = indexer.store.read().await;
        assert_eq!(store.state.clone().unwrap().0, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn test_handle_node_state_event() {
        let contract_name = ContractName::from("test_contract");
        let mut indexer = build_indexer(contract_name.clone()).await;
        register_contract(&mut indexer).await;

        let mut node_state = NodeState::create("test".to_string(), "test");
        let block = node_state
            .handle_signed_block(SignedBlock::default())
            .unwrap();

        let event = NodeStateEvent::NewBlock(block);

        indexer.handle_node_state_event(event).await.unwrap();
        // Add assertions based on the expected state changes
    }
}

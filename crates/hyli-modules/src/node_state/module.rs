//! State required for participation in consensus by the node.

use super::metrics::NodeStateMetrics;
use super::{NodeState, NodeStateStore};
use crate::bus::SharedMessageBus;
use crate::bus::{command_response::Query, BusClientSender};
use crate::module_handle_messages;
use crate::modules::admin::{QueryNodeStateStore, QueryNodeStateStoreResponse};
use crate::modules::files::NODE_STATE_BIN;
use crate::modules::{module_bus_client, Module, SharedBuildApiCtx};
use crate::{log_error, log_warn};
use anyhow::Result;
use hyli_bus::modules::ModulePersistOutput;
use sdk::*;
use std::path::PathBuf;
use tracing::{info, warn};

/// NodeStateModule maintains a NodeState,
/// listens to DA, and sends events when it has processed blocks.
/// Node state module is separate from DataAvailabiliity
/// mostly to run asynchronously.
pub struct NodeStateModule {
    bus: NodeStateBusClient,
    inner: NodeState,
    data_directory: PathBuf,
}

pub use sdk::NodeStateEvent;

#[derive(Clone)]
pub struct QueryBlockHeight {}

#[derive(Clone)]
pub struct QuerySettledHeight(pub ContractName);

#[derive(Clone)]
pub struct QueryUnsettledTxCount(pub Option<ContractName>);

#[derive(Clone)]
pub struct QueryUnsettledTx(pub TxHash);

module_bus_client! {
#[derive(Debug)]
pub struct NodeStateBusClient {
    sender(NodeStateEvent),
    receiver(DataEvent),
    receiver(Query<ContractName, (BlockHeight, Contract)>),
    receiver(Query<QuerySettledHeight, BlockHeight>),
    receiver(Query<QueryUnsettledTxCount, u64>),
    receiver(Query<QueryBlockHeight , BlockHeight>),
    receiver(Query<QueryUnsettledTx, UnsettledBlobTransaction>),
    receiver(Query<QueryNodeStateStore, QueryNodeStateStoreResponse>),
}
}

pub struct NodeStateCtx {
    pub node_id: String,
    pub data_directory: PathBuf,
    pub api: SharedBuildApiCtx,
}

impl Module for NodeStateModule {
    type Context = NodeStateCtx;

    async fn build(bus: SharedMessageBus, ctx: Self::Context) -> Result<Self> {
        let api = super::api::api(bus.new_handle(), &ctx).await;
        if let Ok(mut guard) = ctx.api.router.lock() {
            if let Some(router) = guard.take() {
                guard.replace(router.nest("/v1/", api));
            }
        }
        let metrics = NodeStateMetrics::global("node_state");

        let store = match Self::load_from_disk::<NodeStateStore>(
            &ctx.data_directory,
            NODE_STATE_BIN.as_ref(),
        )? {
            Some(s) => s,
            None => {
                warn!("Starting NodeModule's NodeStateStore from default.");
                NodeStateStore::default()
            }
        };

        for name in store.contracts.keys() {
            info!("📝 Loaded contract state for {}", name);
        }

        let node_state = NodeState { store, metrics };
        let bus = NodeStateBusClient::new_from_bus(bus.new_handle()).await;

        Ok(Self {
            bus,
            inner: node_state,
            data_directory: ctx.data_directory,
        })
    }

    async fn run(&mut self) -> Result<()> {
        module_handle_messages! {
            on_self self,
            command_response<QueryBlockHeight, BlockHeight> _ => {
                Ok(self.inner.current_height)
            }
            command_response<ContractName, (BlockHeight, Contract)> cmd => {
                let block_height = self.inner.current_height;
                match self.inner.contracts.get(cmd).cloned() {
                    Some(contract) => Ok((block_height, contract)),
                    None => Err(anyhow::anyhow!("Contract {} not found", cmd)),
                }
            }
            command_response<QuerySettledHeight, BlockHeight> cmd => {
                if !self.inner.contracts.contains_key(&cmd.0) {
                    Err(anyhow::anyhow!("Contract {} not found", cmd.0))
                } else {
                    let height = self.inner.unsettled_transactions.get_earliest_unsettled_height(&cmd.0).unwrap_or(self.inner.current_height);
                    Ok(BlockHeight(height.0 - 1))
                }
            }
            command_response<QueryUnsettledTxCount, u64> cmd => {
                let count = if let Some(contract_name) = &cmd.0 {
                    self.inner.unsettled_transactions.get_tx_order(contract_name).map(|txs| txs.len() as u64).unwrap_or(0)
                } else {
                    self.inner.unsettled_transactions.len() as u64
                };
                Ok(count)
            }
            command_response<QueryUnsettledTx, UnsettledBlobTransaction> tx_hash => {
                match self.inner.unsettled_transactions.get(&tx_hash.0) {
                    Some(tx) => Ok(tx.clone()),
                    None => Err(anyhow::anyhow!("Transaction not found")),
                }
            }
            command_response<QueryNodeStateStore, QueryNodeStateStoreResponse> _ => {
                serialize_node_state_store(&self.inner.store)
            }
            listen<DataEvent> block => {
                match block {
                    DataEvent::OrderedSignedBlock(signed_block) => {
                        if let Ok(block) = log_warn!(self.inner.handle_signed_block(signed_block), "handling signed block in NodeStateModule") {
                            _ = log_error!(
                                self.bus.send(NodeStateEvent::NewBlock(block)),
                                "Sending DataEvent while processing SignedBlock"
                            );
                        }
                    }
                }
            }
        };

        Ok(())
    }

    async fn persist(&mut self) -> Result<ModulePersistOutput> {
        let file = PathBuf::from(NODE_STATE_BIN);
        let checksum =
            Self::save_on_disk::<NodeStateStore>(&self.data_directory, &file, &self.inner)?;
        Ok(vec![(self.data_directory.join(file), checksum)])
    }
}

fn serialize_node_state_store(
    store: &NodeStateStore,
) -> anyhow::Result<QueryNodeStateStoreResponse> {
    let bytes = borsh::to_vec(store)?;
    Ok(QueryNodeStateStoreResponse(bytes))
}

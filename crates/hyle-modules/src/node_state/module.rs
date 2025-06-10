//! State required for participation in consensus by the node.

use super::metrics::NodeStateMetrics;
use super::{NodeState, NodeStateStore};
use crate::bus::SharedMessageBus;
use crate::bus::{command_response::Query, BusClientSender};
use crate::log_error;
use crate::module_handle_messages;
use crate::modules::{module_bus_client, Module, SharedBuildApiCtx};
use anyhow::{Context, Result};
use sdk::*;
use std::path::PathBuf;
use tracing::info;

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
pub struct QueryUnsettledTx(pub TxHash);

module_bus_client! {
#[derive(Debug)]
pub struct NodeStateBusClient {
    sender(NodeStateEvent),
    sender(NodeStateIndexerEvent),
    receiver(DataEvent),
    receiver(Query<ContractName, Contract>),
    receiver(Query<QuerySettledHeight, BlockHeight>),
    receiver(Query<QueryBlockHeight , BlockHeight>),
    receiver(Query<QueryUnsettledTx, UnsettledBlobTransaction>),
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
        let metrics = NodeStateMetrics::global(ctx.node_id.clone(), "node_state");

        let store = Self::load_from_disk_or_default::<NodeStateStore>(
            ctx.data_directory.join("node_state.bin").as_path(),
        );

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
            on_bus self.bus,
            command_response<QueryBlockHeight, BlockHeight> _ => {
                Ok(self.inner.current_height)
            }
            command_response<ContractName, Contract> cmd => {
                self.inner.contracts.get(cmd).cloned().context("Contract not found")
            }
            command_response<QuerySettledHeight, BlockHeight> cmd => {
                if !self.inner.contracts.contains_key(&cmd.0) {
                    return Err(anyhow::anyhow!("Contract not found"));
                }
                let height = self.inner.unsettled_transactions.get_earliest_unsettled_height(&cmd.0).unwrap_or(self.inner.current_height);
                Ok(BlockHeight(height.0 - 1))
        }
            command_response<QueryUnsettledTx, UnsettledBlobTransaction> tx_hash => {
                match self.inner.unsettled_transactions.get(&tx_hash.0) {
                    Some(tx) => Ok(tx.clone()),
                    None => Err(anyhow::anyhow!("Transaction not found")),
                }
            }
            listen<DataEvent> block => {
                match block {
                    DataEvent::OrderedSignedBlock(block) => {
                        // Extract data proposal metadata before processing
                        let mut data_proposals_metadata = Vec::new();

                        for (lane_id, data_proposals) in &block.data_proposals {
                            for dp in data_proposals {
                                let dp_hash = dp.hashed();
                                let tx_hashes: Vec<TxHash> = dp.txs.iter().map(|tx| tx.hashed()).collect();

                                let parent_hash: Option<DataProposalHash> = if block.height().0 == 0{
                                    Some(DataProposalHash(hex::encode(&lane_id.0.0)))
                                } else {
                                    dp.parent_data_proposal_hash.clone()
                                };

                                data_proposals_metadata.push(DataProposalMetadata {
                                    hash: dp_hash,
                                    parent_hash,
                                    lane_id: lane_id.clone(),
                                    tx_count: dp.txs.len(),
                                    estimated_size: dp.estimate_size(),
                                    tx_hashes,
                                });
                            }
                        }

                        // TODO: If we are in a broken state, this will likely kill the node every time.
                        let node_state_block = self.inner.handle_signed_block(&block)?;

                        // Send data proposal metadata event if we have any
                        if !data_proposals_metadata.is_empty() {
                            _ = log_error!(
                                self.bus.send(NodeStateIndexerEvent::DataProposalsFromBlock {
                                    block_hash: node_state_block.hash.clone(),
                                    block_height: node_state_block.block_height,
                                    block_timestamp: node_state_block.block_timestamp.clone(),
                                    data_proposals: data_proposals_metadata,
                                }),
                                "Sending DataProposalsFromBlock event"
                            );
                        }

                        _ = log_error!(self
                            .bus
                            .send(NodeStateEvent::NewBlock(Box::new(node_state_block))), "Sending DataEvent while processing SignedBlock");

                    }
                }
            }
        };

        let _ = log_error!(
            Self::save_on_disk::<NodeStateStore>(
                self.data_directory.join("node_state.bin").as_path(),
                &self.inner,
            ),
            "Saving node state"
        );

        Ok(())
    }
}

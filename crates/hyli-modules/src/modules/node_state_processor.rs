use std::path::PathBuf;

use anyhow::Result;
use hyli_bus::modules::ModulePersistOutput;
use sdk::{DataEvent, Hashed, MempoolStatusEvent, SignedBlock};
use tracing::{debug, info};

use crate::{
    bus::{BusClientSender, SharedMessageBus},
    modules::{module_bus_client, Module},
    node_state::{metrics::NodeStateMetrics, module::NodeStateEvent, NodeState, NodeStateStore},
};
use crate::{log_error, module_handle_messages};

module_bus_client! {
#[derive(Debug)]
struct NodeStateProcessorBusClient {
    sender(NodeStateEvent),
    sender(MempoolStatusEvent),
    receiver(DataEvent),
}
}

pub struct NodeStateProcessorCtx {
    pub data_directory: PathBuf,
}

/// Module that processes ordered signed blocks through NodeState.
/// Listens to DataEvent::OrderedSignedBlock from SignedDAListener and processes them.
pub struct NodeStateProcessor {
    config: NodeStateProcessorCtx,
    bus: NodeStateProcessorBusClient,
    node_state: NodeState,
}

impl Module for NodeStateProcessor {
    type Context = NodeStateProcessorCtx;

    async fn build(bus: SharedMessageBus, ctx: Self::Context) -> Result<Self> {
        let node_state_store = Self::load_from_disk_or_default::<NodeStateStore>(
            &ctx.data_directory,
            "da_listener_node_state.bin".as_ref(),
        )?;

        let node_state = NodeState {
            store: node_state_store,
            metrics: NodeStateMetrics::global(
                "node_state_processor".to_string(),
                "node_state_processor",
            ),
        };

        let bus = NodeStateProcessorBusClient::new_from_bus(bus.new_handle()).await;

        for name in node_state.contracts.keys() {
            info!("📝 Loaded contract state for {}", name);
        }

        Ok(NodeStateProcessor {
            config: ctx,
            bus,
            node_state,
        })
    }

    async fn run(&mut self) -> Result<()> {
        module_handle_messages! {
            on_self self,
            listen<DataEvent> event => {
                match event {
                    DataEvent::OrderedSignedBlock(block) => {
                        let _ = log_error!(self.process_block(block).await, "Processing block");
                    }
                }
            }
        };
        Ok(())
    }

    async fn persist(&mut self) -> Result<ModulePersistOutput> {
        let file = "da_listener_node_state.bin";
        let checksum = Self::save_on_disk::<NodeStateStore>(
            &self.config.data_directory,
            file.as_ref(),
            &self.node_state,
        )?;
        Ok(vec![(self.config.data_directory.join(file), checksum)])
    }
}

impl NodeStateProcessor {
    async fn process_block(&mut self, block: SignedBlock) -> Result<()> {
        let block_height = block.height();

        if block_height.0.is_multiple_of(1000) {
            info!(
                "📦 Processing block: {} {}",
                block.consensus_proposal.slot,
                block.consensus_proposal.hashed()
            );
        } else {
            debug!(
                "📦 Processing block: {} {}",
                block.consensus_proposal.slot,
                block.consensus_proposal.hashed()
            );
        }

        let processed_block = self.node_state.handle_signed_block(block)?;
        self.bus
            .send_waiting_if_full(NodeStateEvent::NewBlock(processed_block))
            .await?;

        Ok(())
    }
}

use std::path::PathBuf;

use anyhow::Result;
use futures::FutureExt;
use hyli_bus::modules::ModulePersistOutput;
use sdk::{BlockHeight, DataEvent, MempoolStatusEvent, SignedBlock};
use tracing::{info, warn};

use crate::{
    bus::{BusClientSender, SharedMessageBus},
    modules::{module_bus_client, Module},
    node_state::{
        metrics::NodeStateMetrics, module::NodeStateEvent, module::NodeStateModule, NodeState,
        NodeStateStore,
    },
};

/// Trait for processing blocks in the DA listener
pub trait BlockProcessor: Send + Sync {
    /// Configuration needed to build this processor
    type Config: Send + Sync;

    /// Build the processor with bus access
    fn build(
        bus: SharedMessageBus,
        config: &Self::Config,
        data_directory: PathBuf,
    ) -> impl std::future::Future<Output = Result<Self>> + Send
    where
        Self: Sized;

    /// Process a single block (handle NodeState, send events, etc.)
    fn process_block(
        &mut self,
        block: SignedBlock,
    ) -> impl std::future::Future<Output = Result<()>> + Send;

    /// Process a mempool status event
    fn process_mempool_status(
        &mut self,
        status: MempoolStatusEvent,
    ) -> impl std::future::Future<Output = Result<()>> + Send;

    /// Persist processor state along with current block height atomically
    fn persist(
        &mut self,
        current_block: BlockHeight,
    ) -> impl std::future::Future<Output = Result<ModulePersistOutput>> + Send;
}

module_bus_client! {
#[derive(Debug)]
struct BusOnlyProcessorBusClient {
    sender(DataEvent),
    sender(MempoolStatusEvent),
}
}

/// Processor that only sends blocks to the bus without additional processing
pub struct BusOnlyProcessor {
    bus: BusOnlyProcessorBusClient,
    data_directory: PathBuf,
}

impl BlockProcessor for BusOnlyProcessor {
    type Config = ();

    async fn build(bus: SharedMessageBus, _config: &(), data_directory: PathBuf) -> Result<Self> {
        let bus = BusOnlyProcessorBusClient::new_from_bus(bus.new_handle()).await;
        Ok(Self {
            bus,
            data_directory,
        })
    }

    fn process_block(
        &mut self,
        block: SignedBlock,
    ) -> impl std::future::Future<Output = Result<()>> + Send {
        self.bus
            .send_waiting_if_full(DataEvent::OrderedSignedBlock(block))
    }

    fn process_mempool_status(
        &mut self,
        status: MempoolStatusEvent,
    ) -> impl std::future::Future<Output = Result<()>> + Send {
        self.bus.send_waiting_if_full(status)
    }

    fn persist(
        &mut self,
        current_block: BlockHeight,
    ) -> impl std::future::Future<Output = Result<ModulePersistOutput>> + Send {
        let data_directory = self.data_directory.clone();
        async move {
            let file = PathBuf::from("da_start_height.bin");
            let checksum = NodeStateModule::save_on_disk(&data_directory, &file, &current_block)?;
            Ok(vec![(data_directory.join(file), checksum)])
        }
    }
}

module_bus_client! {
#[derive(Debug)]
struct NodeStateBlockProcessorBusClient {
    sender(NodeStateEvent),
    sender(MempoolStatusEvent),
}
}

/// Processor that processes blocks through NodeState before sending events
/// This ensures atomic persistence of both NodeState and block height
pub struct NodeStateBlockProcessor {
    node_state: NodeState,
    bus: NodeStateBlockProcessorBusClient,
    data_directory: PathBuf,
}

impl BlockProcessor for NodeStateBlockProcessor {
    type Config = ();

    async fn build(bus: SharedMessageBus, _config: &(), data_directory: PathBuf) -> Result<Self> {
        let node_state_store = match NodeStateModule::load_from_disk::<NodeStateStore>(
            &data_directory,
            "da_listener_node_state.bin".as_ref(),
        )? {
            Some(store) => store,
            None => {
                warn!("Starting BlockProcessor's NodeStateStore from default.");
                NodeStateStore::default()
            }
        };

        let node_state = NodeState {
            store: node_state_store,
            metrics: NodeStateMetrics::global("node_state_processor"),
        };

        let bus = NodeStateBlockProcessorBusClient::new_from_bus(bus.new_handle()).await;

        for name in node_state.contracts.keys() {
            info!("📝 Loaded contract state for {}", name);
        }

        Ok(Self {
            node_state,
            bus,
            data_directory,
        })
    }

    fn process_block(
        &mut self,
        block: SignedBlock,
    ) -> impl std::future::Future<Output = Result<()>> + Send {
        let processed_block = match self.node_state.handle_signed_block(block) {
            Ok(pb) => pb,
            Err(e) => return async move { Err(e) }.boxed(),
        };
        Box::pin(
            self.bus
                .send_waiting_if_full(NodeStateEvent::NewBlock(processed_block)),
        )
    }

    fn process_mempool_status(
        &mut self,
        status: MempoolStatusEvent,
    ) -> impl std::future::Future<Output = Result<()>> + Send {
        self.bus.send_waiting_if_full(status)
    }

    fn persist(
        &mut self,
        current_block: BlockHeight,
    ) -> impl std::future::Future<Output = Result<ModulePersistOutput>> + Send {
        let data_directory = self.data_directory.clone();
        let node_state_store = self.node_state.store.clone();
        async move {
            // Atomically persist BOTH node_state and current_block
            // This guarantees coherence!
            let node_state_file = PathBuf::from("da_listener_node_state.bin");
            let node_state_checksum = NodeStateModule::save_on_disk(
                &data_directory,
                &node_state_file,
                &node_state_store,
            )?;
            let start_height_file = PathBuf::from("da_start_height.bin");
            let start_height_checksum =
                NodeStateModule::save_on_disk(&data_directory, &start_height_file, &current_block)?;
            Ok(vec![
                (data_directory.join(node_state_file), node_state_checksum),
                (
                    data_directory.join(start_height_file),
                    start_height_checksum,
                ),
            ])
        }
    }
}

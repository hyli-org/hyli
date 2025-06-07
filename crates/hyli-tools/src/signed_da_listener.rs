use std::path::PathBuf;

use anyhow::Result;
use hyle_contract_sdk::BlockHeight;

use hyle_modules::{
    bus::{BusClientSender, SharedMessageBus},
    modules::{Module, module_bus_client},
    utils::da_codec::{DataAvailabilityClient, DataAvailabilityEvent, DataAvailabilityRequest},
};
use hyle_modules::{log_error, module_handle_messages};

module_bus_client! {
#[derive(Debug)]
struct DAListenerBusClient {
    sender(DataAvailabilityEvent),
}
}

/// Module that listens to the data availability stream and sends the blocks to the bus
pub struct DAListener {
    config: DAListenerConf,
    bus: DAListenerBusClient,
}

pub struct DAListenerConf {
    pub data_directory: PathBuf,
    pub da_read_from: String,
    pub start_block: Option<BlockHeight>,
}

impl Module for DAListener {
    type Context = DAListenerConf;

    async fn build(bus: SharedMessageBus, ctx: Self::Context) -> Result<Self> {
        let bus = DAListenerBusClient::new_from_bus(bus.new_handle()).await;

        Ok(DAListener { config: ctx, bus })
    }

    fn run(&mut self) -> impl Future<Output = Result<()>> + Send {
        self.start()
    }
}

impl DAListener {
    async fn start_client(&self, block_height: BlockHeight) -> Result<DataAvailabilityClient> {
        let mut client = DataAvailabilityClient::connect_with_opts(
            "raw_da_listener".to_string(),
            Some(1024 * 1024 * 1024),
            self.config.da_read_from.clone(),
        )
        .await?;

        client.send(DataAvailabilityRequest(block_height)).await?;

        Ok(client)
    }

    pub async fn start(&mut self) -> Result<()> {
        let mut client = self
            .start_client(self.config.start_block.unwrap_or(BlockHeight(0)))
            .await?;

        module_handle_messages! {
            on_bus self.bus,
            frame = client.recv() => {
                if let Some(streamed_signed_block) = frame {
                    let _ = log_error!(self.processing_next_frame(streamed_signed_block).await, "Consuming da stream");
                    client.ping().await?;
                } else {
                    client = self.start_client(self.config.start_block.unwrap_or(BlockHeight(0))).await?;
                }
            }
        };
        Ok(())
    }

    async fn processing_next_frame(&mut self, event: DataAvailabilityEvent) -> Result<()> {
        if let DataAvailabilityEvent::SignedBlock(block) = event {
            tracing::debug!("Received signed block: {:?}", block);
            self.bus
                .send(DataAvailabilityEvent::SignedBlock(block.clone()))?;
        }

        Ok(())
    }
}

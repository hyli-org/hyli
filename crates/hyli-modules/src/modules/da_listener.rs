use std::{collections::BTreeMap, path::PathBuf};

use anyhow::Result;
use hyli_bus::modules::ModulePersistOutput;
use hyli_bus::{module_bus_client, module_handle_messages};
use sdk::{BlockHeight, DataAvailabilityEvent, DataAvailabilityRequest, Hashed, SignedBlock};
use tokio::task::yield_now;
use tracing::{debug, info, warn};

use crate::log_error;
use crate::{
    bus::SharedMessageBus,
    modules::{
        block_processor::BlockProcessor, da_listener_metrics::DaTcpClientMetrics,
        data_availability::blocks_fjall::Blocks, Module,
    },
    node_state::module::NodeStateModule,
    utils::da_codec::DataAvailabilityClient,
};

/// Configuration for DA listeners
pub struct DAListenerConf<P: BlockProcessor> {
    pub data_directory: PathBuf,
    pub da_read_from: String,
    /// Used to specify a starting block height
    pub start_block: Option<BlockHeight>,
    pub timeout_client_secs: u64,
    pub processor_config: P::Config,
}

module_bus_client! {
#[derive(Debug)]
struct SignedDAListenerBusClient {
}
}

/// Module that listens to the raw data availability stream and processes blocks
pub struct SignedDAListener<P: BlockProcessor> {
    config: DAListenerConf<P>,
    bus: SignedDAListenerBusClient,
    processor: P,
    current_block: BlockHeight,
    block_buffer: BTreeMap<BlockHeight, SignedBlock>,
    tcp_client_metrics: DaTcpClientMetrics,
}

impl<P: BlockProcessor + 'static> Module for SignedDAListener<P> {
    type Context = DAListenerConf<P>;

    async fn build(bus: SharedMessageBus, ctx: Self::Context) -> Result<Self> {
        let start_block_in_file = match NodeStateModule::load_from_disk::<BlockHeight>(
            &ctx.data_directory,
            "da_start_height.bin".as_ref(),
        )? {
            Some(b) => b,
            None => {
                warn!("Starting SignedDAListener's NodeStateStore from default.");
                BlockHeight(0)
            }
        };

        debug!(
            "Building SignedDAListener with start block from file: {:?}",
            start_block_in_file
        );

        let current_block = ctx
            .start_block
            .or(Some(start_block_in_file))
            .unwrap_or_default();

        info!(
            "SignedDAListener current block height set to: {}",
            current_block
        );

        let processor = P::build(
            bus.new_handle(),
            &ctx.processor_config,
            ctx.data_directory.clone(),
        )
        .await?;
        let bus = SignedDAListenerBusClient::new_from_bus(bus.new_handle()).await;

        Ok(SignedDAListener {
            config: ctx,
            bus,
            current_block,
            processor,
            block_buffer: BTreeMap::new(),
            tcp_client_metrics: DaTcpClientMetrics::global(
                "signed_da_listener".to_string(),
                "signed_da_listener",
            ),
        })
    }

    fn run(&mut self) -> impl futures::Future<Output = Result<()>> + Send {
        self.start()
    }

    async fn persist(&mut self) -> Result<ModulePersistOutput> {
        self.processor.persist(self.current_block).await
    }
}

impl<P: BlockProcessor + 'static> SignedDAListener<P> {
    async fn start_client(&self, block_height: BlockHeight) -> Result<DataAvailabilityClient> {
        let mut client = DataAvailabilityClient::connect_with_opts(
            "signed_da_listener".to_string(),
            Some(1024 * 1024 * 1024),
            self.config.da_read_from.clone(),
        )
        .await?;

        client.send(DataAvailabilityRequest(block_height)).await?;
        self.tcp_client_metrics.start(block_height.0);

        Ok(client)
    }

    async fn process_block(&mut self, block: SignedBlock) -> Result<()> {
        let block_height = block.height();

        // If this is the next block we expect, process it immediately, otherwise buffer it
        match block_height.cmp(&self.current_block) {
            std::cmp::Ordering::Less => {
                // Block is from the past, log and ignore
                warn!(
                    "📦 Ignoring past block: {} {}",
                    block.consensus_proposal.slot,
                    block.consensus_proposal.hashed()
                );
            }
            std::cmp::Ordering::Equal => {
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
                self.processor.process_block(block).await?;

                self.current_block = block_height + 1;

                // Process any buffered blocks that are now in sequence
                self.process_buffered_blocks().await?;
            }
            std::cmp::Ordering::Greater => {
                // Block is from the future, buffer it
                debug!(
                    "📦 Buffering future block: {} {}",
                    block.consensus_proposal.slot,
                    block.consensus_proposal.hashed()
                );
                self.block_buffer.insert(block_height, block);
            }
        }

        Ok(())
    }

    async fn process_buffered_blocks(&mut self) -> Result<()> {
        if let Some((height, _)) = self.block_buffer.first_key_value() {
            if *height > self.current_block {
                return Ok(());
            }
        }

        while let Some((height, block)) = self.block_buffer.pop_first() {
            if height == self.current_block {
                debug!(
                    "📦 Processing buffered block: {} {}",
                    block.consensus_proposal.slot,
                    block.consensus_proposal.hashed()
                );
                self.processor.process_block(block).await?;
                self.current_block = height + 1;
            } else {
                // In general, DA isn't guaranteed to send blocks in order.
                info!(
                    "📦 Wanted block {}, next buffered is {}, waiting.",
                    self.current_block,
                    block.height(),
                );
                // Put the block back
                _ = self.block_buffer.insert(height, block);
                break;
            }
        }

        Ok(())
    }

    pub async fn start(&mut self) -> Result<()> {
        if let Some(folder) = self.config.da_read_from.strip_prefix("folder:") {
            info!("Reading blocks from folder {folder}");
            let mut blocks = vec![];
            let mut entries = std::fs::read_dir(folder)
                .unwrap_or_else(|_| std::fs::read_dir(".").unwrap())
                .filter_map(|e| e.ok())
                .collect::<Vec<_>>();
            entries.sort_by_key(|e| e.file_name());
            for entry in entries {
                let path = entry.path();
                if path.extension().map(|e| e == "bin").unwrap_or(false) {
                    if let Ok(bytes) = std::fs::read(&path) {
                        if let Ok((block, tx_count)) =
                            borsh::from_slice::<(SignedBlock, usize)>(&bytes)
                        {
                            blocks.push((block, tx_count));
                        }
                    }
                }
                yield_now().await; // Yield to allow other tasks to run
            }
            // Sort blocks by block_height (numeric order)
            blocks.sort_by_key(|b| b.0.consensus_proposal.slot);

            info!("Got {} blocks from folder. Processing...", blocks.len());
            for (block, _) in blocks {
                self.process_block(block).await?;
            }
            module_handle_messages! {
                on_self self,
            };
        } else if let Some(folder) = self.config.da_read_from.strip_prefix("da:") {
            info!("Reading blocks from DA {folder}");
            let mut blocks = Blocks::new(&PathBuf::from(folder))?;
            let block_hashes = blocks
                .range(BlockHeight(0), BlockHeight(u64::MAX))
                .collect::<Result<Vec<_>>>()?;
            for block_hash in block_hashes {
                let block = blocks.get(&block_hash)?.unwrap();
                self.process_block(block).await?;
            }
            module_handle_messages! {
                on_self self,
            };
        } else {
            let mut client = self.start_client(self.current_block).await?;

            info!(
                "Starting DA client for signed blocks at block {}",
                self.current_block
            );

            module_handle_messages! {
                on_self self,
                _ = tokio::time::sleep(tokio::time::Duration::from_secs(self.config.timeout_client_secs)) => {
                    warn!("No blocks received in the last {} seconds, restarting client", self.config.timeout_client_secs);
                    self.tcp_client_metrics.reconnect("timeout");
                    client = self.start_client(self.current_block).await?;
                }
                frame = client.recv() => {
                    if let Some(streamed_signed_block) = frame {
                        let _ = log_error!(self.processing_next_frame(streamed_signed_block).await, "Consuming da stream");
                        if let Err(e) = client.ping().await {
                            warn!("Ping failed: {}. Restarting client...", e);
                            self.tcp_client_metrics.reconnect("ping_error");
                            client = self.start_client(self.current_block).await?;
                        }
                    } else {
                        warn!("DA stream connection lost. Reconnecting...");
                        self.tcp_client_metrics.reconnect("stream_closed");
                        client = self.start_client(self.current_block).await?;
                    }
                }
            };
        }
        Ok(())
    }

    async fn processing_next_frame(&mut self, event: DataAvailabilityEvent) -> Result<()> {
        match event {
            DataAvailabilityEvent::SignedBlock(block) => {
                self.process_block(block).await?;
            }
            DataAvailabilityEvent::MempoolStatusEvent(status) => {
                self.processor.process_mempool_status(status).await?;
            }
        }

        Ok(())
    }
}

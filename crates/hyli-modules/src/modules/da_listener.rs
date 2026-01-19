use std::{
    collections::{BTreeMap, HashMap},
    path::PathBuf,
    time::{Duration, Instant},
};

use anyhow::Result;
use sdk::{
    BlockHeight, DataAvailabilityEvent, DataAvailabilityRequest, DataEvent, Hashed,
    MempoolStatusEvent, SignedBlock,
};
use tokio::task::yield_now;
use tracing::{debug, error, info, warn};

use crate::{
    bus::{BusClientSender, SharedMessageBus},
    modules::{
        da_listener_metrics::DaTcpClientMetrics, data_availability::blocks_fjall::Blocks,
        module_bus_client, Module,
    },
    node_state::module::NodeStateModule,
    utils::da_codec::DataAvailabilityClient,
};
use crate::{log_error, module_handle_messages};

/// Configuration for DA listeners (SignedDAListener and NodeStateProcessor)
pub struct DAListenerConf {
    pub data_directory: PathBuf,
    pub da_read_from: String,
    /// Used to specify a starting block height
    pub start_block: Option<BlockHeight>,
    pub timeout_client_secs: u64,
    /// Fallback DA server addresses for block requests
    pub da_fallback_addresses: Vec<String>,
}

module_bus_client! {
#[derive(Debug)]
struct SignedDAListenerBusClient {
    sender(DataEvent),
    sender(MempoolStatusEvent),
}
}

/// Track state of pending block requests with retry logic
#[derive(Debug, Clone)]
struct BlockRequestState {
    request_time: Instant,
    retry_count: usize,
    current_da_index: usize,
}

/// Module that listens to the raw data availability stream and sends the signed blocks to the bus
pub struct SignedDAListener {
    config: DAListenerConf,
    bus: SignedDAListenerBusClient,
    current_block: BlockHeight,
    block_buffer: BTreeMap<BlockHeight, SignedBlock>,
    tcp_client_metrics: DaTcpClientMetrics,
    // New fields for block request handling
    pending_block_requests: HashMap<BlockHeight, BlockRequestState>,
    current_da_index: usize,
}

impl Module for SignedDAListener {
    type Context = DAListenerConf;

    async fn build(bus: SharedMessageBus, ctx: Self::Context) -> Result<Self> {
        let start_block_in_file = NodeStateModule::load_from_disk::<BlockHeight>(
            ctx.data_directory.join("da_start_height.bin").as_path(),
        );

        debug!(
            "Building SignedDAListener with start block from file: {:?}",
            start_block_in_file
        );

        let current_block = ctx.start_block.or(start_block_in_file).unwrap_or_default();

        info!(
            "SignedDAListener current block height set to: {}",
            current_block
        );

        let bus = SignedDAListenerBusClient::new_from_bus(bus.new_handle()).await;

        Ok(SignedDAListener {
            config: ctx,
            current_block,
            bus,
            block_buffer: BTreeMap::new(),
            tcp_client_metrics: DaTcpClientMetrics::global(
                "signed_da_listener".to_string(),
                "signed_da_listener",
            ),
            pending_block_requests: HashMap::new(),
            current_da_index: 0,
        })
    }

    fn run(&mut self) -> impl futures::Future<Output = Result<()>> + Send {
        self.start()
    }
}

impl SignedDAListener {
    async fn start_client(&self, block_height: BlockHeight) -> Result<DataAvailabilityClient> {
        let mut client = DataAvailabilityClient::connect_with_opts(
            "signed_da_listener".to_string(),
            Some(1024 * 1024 * 1024),
            self.config.da_read_from.clone(),
        )
        .await?;

        client
            .send(DataAvailabilityRequest::StreamFromHeight(block_height))
            .await?;
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
                        "📦 Sending block: {} {}",
                        block.consensus_proposal.slot,
                        block.consensus_proposal.hashed()
                    );
                } else {
                    debug!(
                        "📦 Sending block: {} {}",
                        block.consensus_proposal.slot,
                        block.consensus_proposal.hashed()
                    );
                }

                // Remove from pending requests if it was requested
                self.pending_block_requests.remove(&block_height);

                self.bus
                    .send_waiting_if_full(DataEvent::OrderedSignedBlock(block))
                    .await?;

                self.current_block = block_height + 1;

                // Process any buffered blocks that are now in sequence
                self.process_buffered_blocks().await?;
            }
            std::cmp::Ordering::Greater => {
                // Gap detected! Request missing blocks
                if block_height > self.current_block {
                    warn!(
                        "📦 Gap detected! Expected {}, got {}. Requesting missing blocks.",
                        self.current_block, block_height
                    );
                    for missing_height in self.current_block.0..block_height.0 {
                        self.request_specific_block(BlockHeight(missing_height));
                    }
                }
                // Buffer the future block
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
                self.bus
                    .send_waiting_if_full(DataEvent::OrderedSignedBlock(block))
                    .await?;
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

    fn request_specific_block(&mut self, height: BlockHeight) {
        if self.pending_block_requests.contains_key(&height) {
            return; // Already requested
        }

        debug!("📦 Requesting specific block at height {}", height);

        let state = BlockRequestState {
            request_time: Instant::now(),
            retry_count: 0,
            current_da_index: self.current_da_index,
        };

        self.pending_block_requests.insert(height, state);
    }

    async fn switch_to_next_da_server(
        &mut self,
        client: &mut DataAvailabilityClient,
    ) -> Result<()> {
        self.current_da_index =
            (self.current_da_index + 1) % (self.config.da_fallback_addresses.len() + 1);

        let da_address = if self.current_da_index == 0 {
            &self.config.da_read_from
        } else {
            &self.config.da_fallback_addresses[self.current_da_index - 1]
        };

        warn!("📦 Switching to DA server: {}", da_address);

        // Reconnect to the server
        *client = DataAvailabilityClient::connect_with_opts(
            "signed_da_listener".to_string(),
            Some(1024 * 1024 * 1024),
            da_address.clone(),
        )
        .await?;

        // Start streaming from current block
        client
            .send(DataAvailabilityRequest::StreamFromHeight(
                self.current_block,
            ))
            .await?;

        Ok(())
    }

    async fn check_block_request_timeouts(
        &mut self,
        client: &mut DataAvailabilityClient,
    ) -> Result<()> {
        let now = Instant::now();
        let timeout_base_secs = if self.config.da_fallback_addresses.is_empty() {
            5 // Default timeout when no fallback addresses
        } else {
            self.config.da_fallback_addresses.len() as u64 * 5 // Longer timeout per fallback
        };

        let mut timed_out = Vec::new();
        let mut failed_blocks = Vec::new();

        for (&height, state) in &self.pending_block_requests {
            // Exponential backoff: 5s, 10s, 20s, 40s, ...
            let backoff_duration = Duration::from_secs(
                timeout_base_secs * 2_u64.pow(state.retry_count as u32).min(60),
            );

            if now.duration_since(state.request_time) > backoff_duration {
                timed_out.push(height);
            }
        }

        for height in timed_out {
            if let Some(mut state) = self.pending_block_requests.remove(&height) {
                state.retry_count += 1;

                // Check if we should try a fallback DA server
                if !self.config.da_fallback_addresses.is_empty()
                    && state.current_da_index == self.current_da_index
                {
                    // Try next DA server
                    state.current_da_index = (state.current_da_index + 1)
                        % (self.config.da_fallback_addresses.len() + 1);

                    if state.current_da_index == 0 {
                        // Back to main DA after trying all fallbacks
                        // Check if we've exceeded max retries
                        if state.retry_count > (self.config.da_fallback_addresses.len() + 1) * 3 {
                            error!(
                                "📦 Block {} unretrievable after {} retries across all DA servers. STOPPING.",
                                height, state.retry_count
                            );
                            failed_blocks.push(height);
                            continue;
                        }
                    }

                    warn!(
                        "📦 Block request for height {} timed out (attempt {}). Switching DA server.",
                        height, state.retry_count
                    );

                    self.switch_to_next_da_server(client).await?;
                } else {
                    warn!(
                        "📦 Block request for height {} timed out (attempt {}). Retrying.",
                        height, state.retry_count
                    );
                }

                // Send the block request
                state.request_time = now;
                client
                    .send(DataAvailabilityRequest::BlockRequest(height))
                    .await?;

                self.pending_block_requests.insert(height, state);
            }
        }

        // If any blocks failed after all retries, return error to stop the module
        if !failed_blocks.is_empty() {
            return Err(anyhow::anyhow!(
                "Blocks {:?} are unretrievable from all DA servers",
                failed_blocks
            ));
        }

        Ok(())
    }

    async fn handle_block_not_found(
        &mut self,
        height: BlockHeight,
        client: &mut DataAvailabilityClient,
    ) -> Result<()> {
        error!("📦 Block {} not found at DA server", height);

        self.pending_block_requests.remove(&height);

        // Try fallback DA servers
        if !self.config.da_fallback_addresses.is_empty() {
            self.switch_to_next_da_server(client).await?;

            // Re-request the block
            self.request_specific_block(height);
            client
                .send(DataAvailabilityRequest::BlockRequest(height))
                .await?;
        } else {
            // No fallback servers available - fatal error
            error!(
                "📦 Block {} is unretrievable and no fallback DA servers configured. STOPPING.",
                height
            );
            return Err(anyhow::anyhow!(
                "Block {} unretrievable from DA server and no fallbacks available",
                height
            ));
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

            let mut timeout_check_interval = tokio::time::interval(Duration::from_secs(1));

            module_handle_messages! {
                on_self self,
                _ = tokio::time::sleep(tokio::time::Duration::from_secs(self.config.timeout_client_secs)) => {
                    warn!("No blocks received in the last {} seconds, restarting client", self.config.timeout_client_secs);
                    self.tcp_client_metrics.reconnect("timeout");
                    client = self.start_client(self.current_block).await?;
                }
                _ = timeout_check_interval.tick() => {
                    // Check for timed-out block requests and send new requests
                    log_error!(self.check_block_request_timeouts(&mut client).await, "Checking block request timeouts")?;

                    // Send pending block requests that haven't been sent yet
                    let pending: Vec<BlockHeight> = self.pending_block_requests.keys().copied().collect();
                    for height in pending {
                        if let Err(e) = client.send(DataAvailabilityRequest::BlockRequest(height)).await {
                            error!("Failed to send block request for height {}: {}", height, e);
                        }
                    }
                }
                frame = client.recv() => {
                    if let Some(event) = frame {
                        match &event {
                            DataAvailabilityEvent::BlockNotFound(height) => {
                                error!("📦 Block {} not found at DA server", height);
                                log_error!(self.handle_block_not_found(*height, &mut client).await, "Handling BlockNotFound")?;
                            },
                            _ => { /* Handled below */}
                        }

                        let _ = log_error!(self.processing_next_frame(event).await, "Consuming da stream");
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
                self.pending_block_requests.remove(&block.height());
                self.process_block(block).await?;
            }
            DataAvailabilityEvent::MempoolStatusEvent(status) => {
                self.bus.send_waiting_if_full(status).await?;
            }
            DataAvailabilityEvent::BlockNotFound(_) => {
                // Already handled in the event loop
            }
        }

        Ok(())
    }
}

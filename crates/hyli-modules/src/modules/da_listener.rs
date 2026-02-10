use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    path::PathBuf,
    time::{Duration, Instant},
};

use anyhow::Result;
use hyli_bus::modules::ModulePersistOutput;
use hyli_bus::{module_bus_client, module_handle_messages};
use sdk::{BlockHeight, DataAvailabilityEvent, DataAvailabilityRequest, Hashed, SignedBlock};
use tokio::task::yield_now;
use tracing::{debug, error, info, warn};

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
    /// Fallback DA server addresses for block requests
    pub da_fallback_addresses: Vec<String>,
    pub processor_config: P::Config,
}

module_bus_client! {
#[derive(Debug)]
struct SignedDAListenerBusClient {
}
}

/// Track state of pending block requests with retry logic
#[derive(Debug, Clone)]
struct BlockRequestState {
    request_time: Instant,
    retry_count: usize,
}

pub enum DaStreamPoll {
    Event(DataAvailabilityEvent),
    Timeout,
    StreamClosed,
}

#[derive(Debug, Clone, Copy)]
pub enum BlockNotFoundPolicy {
    HandleInStream,
    SurfaceEvent,
}

pub struct SignedDaStream {
    client_id: String,
    max_frame_length: Option<usize>,
    addresses: Vec<String>,
    client: Option<DataAvailabilityClient>,
    tcp_client_metrics: DaTcpClientMetrics,
    block_not_found_policy: BlockNotFoundPolicy,
    timeout: Duration,
    deadline: Instant,
    block_request_check_deadline: Instant,
    current_block: BlockHeight,
    block_buffer: BTreeMap<BlockHeight, SignedBlock>,
    pending_block_requests: HashMap<BlockHeight, BlockRequestState>,
}

impl SignedDaStream {
    const MAX_CONNECT_ROUNDS: usize = 3;

    pub fn new(
        module_name: &'static str,
        client_id: impl Into<String>,
        max_frame_length: Option<usize>,
        addresses: Vec<String>,
        start_height: BlockHeight,
        timeout: Duration,
    ) -> Self {
        SignedDaStream {
            client_id: client_id.into(),
            max_frame_length,
            addresses,
            client: None,
            tcp_client_metrics: DaTcpClientMetrics::global(module_name),
            block_not_found_policy: BlockNotFoundPolicy::HandleInStream,
            timeout,
            deadline: Instant::now() + timeout,
            block_request_check_deadline: Instant::now() + Duration::from_secs(1),
            current_block: start_height,
            block_buffer: BTreeMap::new(),
            pending_block_requests: HashMap::new(),
        }
    }

    pub fn current_block(&self) -> BlockHeight {
        self.current_block
    }

    pub fn set_block_not_found_policy(&mut self, policy: BlockNotFoundPolicy) {
        self.block_not_found_policy = policy;
    }

    fn init_working_addresses(&self) -> VecDeque<String> {
        self.addresses.iter().cloned().collect()
    }

    async fn try_connect_from_working_set(
        &mut self,
        working_addresses: &mut VecDeque<String>,
    ) -> Result<Option<DataAvailabilityClient>> {
        while let Some(da_address) = working_addresses.pop_front() {
            match DataAvailabilityClient::connect_with_opts(
                self.client_id.clone(),
                self.max_frame_length,
                da_address.clone(),
            )
            .await
            {
                Ok(mut client) => {
                    info!("📦 Connected to DA server {}", da_address);
                    client
                        .send(DataAvailabilityRequest::StreamFromHeight(
                            self.current_block,
                        ))
                        .await?;
                    self.deadline = Instant::now() + self.timeout;
                    return Ok(Some(client));
                }
                Err(e) => {
                    warn!(
                        "📦 Failed to connect to DA server {}: {}. Trying next...",
                        da_address, e
                    );
                }
            }
        }

        Ok(None)
    }

    async fn send_request(&mut self, request: DataAvailabilityRequest) -> Result<()> {
        let client = self
            .client
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("DA client not initialized"))?;
        client.send(request).await?;
        Ok(())
    }

    /// Connect to a DA server and subscribe from `current_block`.
    ///
    /// For each round, initializes a working queue from all configured peers.
    /// Failed peers are popped from the queue for that round. When the queue is
    /// empty, a new round starts, up to `MAX_CONNECT_ROUNDS`.
    pub async fn start_client(&mut self) -> Result<()> {
        let total_servers = self.addresses.len();
        if total_servers == 0 {
            return Err(anyhow::anyhow!("No DA servers configured"));
        }

        let mut connect_round = 1;
        let mut working_addresses = self.init_working_addresses();

        loop {
            if let Some(client) = self
                .try_connect_from_working_set(&mut working_addresses)
                .await?
            {
                self.client = Some(client);
                return Ok(());
            }

            if connect_round >= Self::MAX_CONNECT_ROUNDS {
                break;
            }

            warn!(
                "📦 Completed round {}/{} through all DA servers, retrying...",
                connect_round,
                Self::MAX_CONNECT_ROUNDS
            );
            connect_round += 1;
            working_addresses = self.init_working_addresses();
        }

        Err(anyhow::anyhow!(
            "Failed to connect to any DA server after {} rounds through all {} servers",
            Self::MAX_CONNECT_ROUNDS,
            total_servers
        ))
    }

    pub async fn start_client_with_metrics(&mut self) -> Result<()> {
        self.start_client().await?;
        self.tcp_client_metrics.start(self.current_block.0);
        Ok(())
    }

    pub async fn reconnect(&mut self, reason: &'static str) -> Result<()> {
        self.tcp_client_metrics.reconnect(reason);
        self.start_client().await
    }

    pub async fn listen_next(&mut self) -> Result<DaStreamPoll> {
        loop {
            let now = Instant::now();
            if now >= self.block_request_check_deadline {
                self.block_request_check_deadline = now + Duration::from_secs(1);
                if let Err(e) = self.check_block_request_timeouts().await {
                    error!("Block request housekeeping failed: {}", e);
                    return Err(e);
                }
            }

            let now = Instant::now();
            let next_wakeup = self.deadline.min(self.block_request_check_deadline);
            let wait_duration = next_wakeup.saturating_duration_since(now);

            let received = {
                match self.client.as_mut() {
                    Some(client) => tokio::time::timeout(wait_duration, client.recv()).await,
                    None => return Ok(DaStreamPoll::StreamClosed),
                }
            };

            let poll = match received {
                Ok(Some(event)) => {
                    self.deadline = Instant::now() + self.timeout;
                    DaStreamPoll::Event(event)
                }
                Ok(None) => DaStreamPoll::StreamClosed,
                Err(_) => {
                    if Instant::now() >= self.deadline {
                        DaStreamPoll::Timeout
                    } else {
                        continue;
                    }
                }
            };

            if let DaStreamPoll::Event(DataAvailabilityEvent::BlockNotFound(height)) = poll {
                if matches!(
                    self.block_not_found_policy,
                    BlockNotFoundPolicy::HandleInStream
                ) {
                    if let Err(e) = self.handle_block_not_found(height).await {
                        error!("Failed to handle BlockNotFound {}: {}", height, e);
                        return Err(e);
                    }
                    continue;
                }
            }

            return Ok(poll);
        }
    }

    pub fn request_specific_block(&mut self, height: BlockHeight) {
        if self.pending_block_requests.contains_key(&height) {
            return; // Already requested
        }

        debug!("📦 Requesting specific block at height {}", height);

        let state = BlockRequestState {
            request_time: Instant::now(),
            retry_count: 0,
        };

        self.pending_block_requests.insert(height, state);
    }

    async fn check_block_request_timeouts(&mut self) -> Result<()> {
        let now = Instant::now();
        let timeout_base_secs = 5;

        let mut timed_out = Vec::new();
        let mut failed_blocks = Vec::new();

        for (&height, state) in &self.pending_block_requests {
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

                let max_retries = self.addresses.len().max(1) * Self::MAX_CONNECT_ROUNDS;
                if state.retry_count > max_retries {
                    error!(
                        "📦 Block {} unretrievable after {} retries across all DA servers. STOPPING.",
                        height, state.retry_count
                    );
                    failed_blocks.push(height);
                    continue;
                }

                warn!(
                    "📦 Block request for height {} timed out (attempt {}). Retrying.",
                    height, state.retry_count
                );

                state.request_time = now;
                self.send_request(DataAvailabilityRequest::BlockRequest(height))
                    .await?;

                self.pending_block_requests.insert(height, state);
            }
        }

        let pending = self
            .pending_block_requests
            .keys()
            .copied()
            .collect::<Vec<_>>();
        for height in pending {
            self.send_request(DataAvailabilityRequest::BlockRequest(height))
                .await?;
        }

        if !failed_blocks.is_empty() {
            return Err(anyhow::anyhow!(
                "Blocks {:?} are unretrievable from all DA servers",
                failed_blocks
            ));
        }

        Ok(())
    }

    pub async fn handle_block_not_found(&mut self, height: BlockHeight) -> Result<()> {
        error!("📦 Block {} not found at DA server", height);

        self.pending_block_requests.remove(&height);

        let has_fallbacks = self.addresses.len() > 1;
        if has_fallbacks {
            self.start_client().await?;

            self.request_specific_block(height);
            self.send_request(DataAvailabilityRequest::BlockRequest(height))
                .await?;
        } else {
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

    pub async fn on_signed_block(&mut self, block: SignedBlock) -> Result<Vec<SignedBlock>> {
        let block_height = block.height();
        let mut output = Vec::new();
        self.pending_block_requests.remove(&block_height);

        match block_height.cmp(&self.current_block) {
            std::cmp::Ordering::Less => {
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

                output.push(block);
                self.current_block = block_height + 1;

                let mut buffered = self.process_buffered_blocks().await?;
                output.append(&mut buffered);
            }
            std::cmp::Ordering::Greater => {
                if block_height > self.current_block {
                    warn!(
                        "📦 Gap detected! Expected {}, got {}. Requesting missing blocks.",
                        self.current_block, block_height
                    );
                    for missing_height in self.current_block.0..block_height.0 {
                        self.request_specific_block(BlockHeight(missing_height));
                    }
                }
                debug!(
                    "📦 Buffering future block: {} {}",
                    block.consensus_proposal.slot,
                    block.consensus_proposal.hashed()
                );
                self.block_buffer.insert(block_height, block);
            }
        }

        Ok(output)
    }

    async fn process_buffered_blocks(&mut self) -> Result<Vec<SignedBlock>> {
        let mut output = Vec::new();

        if let Some((height, _)) = self.block_buffer.first_key_value() {
            if *height > self.current_block {
                return Ok(output);
            }
        }

        while let Some((height, block)) = self.block_buffer.pop_first() {
            if height == self.current_block {
                debug!(
                    "📦 Processing buffered block: {} {}",
                    block.consensus_proposal.slot,
                    block.consensus_proposal.hashed()
                );
                output.push(block);
                self.current_block = height + 1;
            } else {
                info!(
                    "📦 Wanted block {}, next buffered is {}, waiting.",
                    self.current_block,
                    block.height(),
                );
                _ = self.block_buffer.insert(height, block);
                break;
            }
        }

        Ok(output)
    }

    async fn ping(&mut self) -> Result<()> {
        let client = self
            .client
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("DA client not initialized"))?;
        client.ping().await?;
        Ok(())
    }
}

/// Module that listens to the raw data availability stream and processes blocks
pub struct SignedDAListener<P: BlockProcessor> {
    config: DAListenerConf<P>,
    bus: SignedDAListenerBusClient,
    processor: P,
    stream: SignedDaStream,
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
        let mut addresses = vec![ctx.da_read_from.clone()];
        addresses.extend(ctx.da_fallback_addresses.clone());
        let stream = SignedDaStream::new(
            "signed_da_listener",
            "signed_da_listener",
            Some(1024 * 1024 * 1024),
            addresses,
            current_block,
            Duration::from_secs(ctx.timeout_client_secs),
        );

        Ok(SignedDAListener {
            config: ctx,
            bus,
            processor,
            stream,
        })
    }

    fn run(&mut self) -> impl futures::Future<Output = Result<()>> + Send {
        self.start()
    }

    async fn persist(&mut self) -> Result<ModulePersistOutput> {
        self.processor.persist(self.stream.current_block()).await
    }
}

impl<P: BlockProcessor + 'static> SignedDAListener<P> {
    async fn start_client(&mut self) -> Result<()> {
        self.stream.start_client_with_metrics().await
    }

    async fn handle_signed_block(&mut self, block: SignedBlock) -> Result<()> {
        let blocks = self.stream.on_signed_block(block).await?;
        for block in blocks {
            self.processor.process_block(block).await?;
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
            blocks.sort_by_key(|b| b.0.consensus_proposal.slot);

            info!("Got {} blocks from folder. Processing...", blocks.len());
            for (block, _) in blocks {
                self.handle_signed_block(block).await?;
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
                self.handle_signed_block(block).await?;
            }
            module_handle_messages! {
                on_self self,
            };
        } else {
            self.start_client().await?;

            info!(
                "Starting DA client for signed blocks at block {}",
                self.stream.current_block()
            );

            module_handle_messages! {
                on_self self,
                poll = self.stream.listen_next() => {
                    let poll = poll?;
                    match poll {
                        DaStreamPoll::Timeout => {
                            warn!("No blocks received in the last {} seconds, restarting client", self.config.timeout_client_secs);
                            self.stream.reconnect("timeout").await?;
                        }
                        DaStreamPoll::StreamClosed => {
                            warn!("DA stream connection lost. Reconnecting...");
                            self.stream.reconnect("stream_closed").await?;
                        }
                        DaStreamPoll::Event(event) => {
                            let _ = log_error!(self.processing_next_frame(event).await, "Consuming da stream");
                            if let Err(e) = self.stream.ping().await {
                                warn!("Ping failed: {}. Restarting client...", e);
                                self.stream.reconnect("ping_error").await?;
                            }
                        }
                    }
                }
            };
        }
        Ok(())
    }

    async fn processing_next_frame(&mut self, event: DataAvailabilityEvent) -> Result<()> {
        match event {
            DataAvailabilityEvent::SignedBlock(block) => {
                self.handle_signed_block(block).await?;
            }
            DataAvailabilityEvent::MempoolStatusEvent(status) => {
                self.processor.process_mempool_status(status).await?;
            }
            DataAvailabilityEvent::BlockNotFound(_) => {}
        }

        Ok(())
    }
}

#[cfg(test)]
mod da_listener_tests;

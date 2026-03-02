//! Minimal block storage layer for data availability.

// Pick one of the two implementations
use hyli_modules::modules::data_availability::blocks_fjall::Blocks;
use hyli_modules::utils::da_codec::DataAvailabilityServer;
//use hyli_modules::modules::data_availability::blocks_memory::Blocks;
use hyli_modules::modules::da_listener::{DaStreamPoll, SignedDaStream};
use hyli_modules::{bus::SharedMessageBus, modules::Module};
use hyli_modules::{log_error, module_bus_client, module_handle_messages};
use hyli_net::tcp::TcpEvent;
use tokio::task::JoinHandle;

use crate::{
    bus::BusClientSender,
    consensus::ConsensusCommand,
    genesis::GenesisEvent,
    model::*,
    p2p::network::{OutboundMessage, PeerEvent},
    utils::conf::SharedConf,
};
use anyhow::{Context, Result};
use core::str;
use std::{
    collections::{BTreeSet, HashMap, VecDeque},
    time::Duration,
};
use strum_macros::AsRefStr;
use tokio::task::JoinSet;
use tracing::{debug, error, info, trace, warn};

use crate::model::SharedRunContext;

impl Module for DataAvailability {
    type Context = SharedRunContext;

    async fn build(bus: SharedMessageBus, ctx: Self::Context) -> anyhow::Result<Self> {
        let bus = DABusClient::new_from_bus(bus.new_handle()).await;

        let mut blocks = Blocks::new(&ctx.config.data_directory.join("data_availability.db"))?;
        blocks.set_metrics_context(ctx.config.id.clone());
        let highest_block = {
            let blocks_handle = blocks.new_handle();
            tokio::task::spawn_blocking(move || blocks_handle.highest()).await?
        };

        // When fast catchup is enabled, we load the node state from disk to load blocks

        let catchup_policy = if ctx.config.consensus.solo {
            None
        } else {
            let floor = if ctx.config.run_fast_catchup {
                ctx.start_height.and_then(|start_height| {
                    // Avoid fast catchup reexecution
                    if highest_block < start_height {
                        Some(start_height + 1)
                    } else {
                        None
                    }
                })
            } else {
                None
            };
            Some(DaCatchupPolicy::Regular {
                floor,
                ceiling: None,
                backfill_enabled: ctx.config.run_fast_catchup
                    && ctx.config.fast_catchup_backfill
                    && floor.is_some(),
                backfill_start: None,
            })
        };

        info!(
            "📦  DataAvailability module built with policy {:?}",
            catchup_policy
        );
        Ok(DataAvailability {
            config: ctx.config.clone(),
            bus,
            blocks,
            fjall_async_policy: FjallAsyncPolicy::from_env(),
            buffered_signed_blocks: BTreeSet::new(),
            catchupper: DaCatchupper::new(catchup_policy, ctx.config.da_max_frame_length),
            allow_peer_catchup: false,
            peer_send_queues: HashMap::new(),
        })
    }

    async fn run(&mut self) -> anyhow::Result<()> {
        self.start().await
    }
}

module_bus_client! {
#[derive(Debug)]
struct DABusClient {
    sender(OutboundMessage),
    sender(DataEvent),
    sender(ConsensusCommand),
    receiver(MempoolBlockEvent),
    receiver(MempoolStatusEvent),
    receiver(GenesisEvent),
    receiver(PeerEvent),
}
}

#[derive(Debug)]
pub struct DataAvailability {
    config: SharedConf,
    bus: DABusClient,
    pub blocks: Blocks,
    fjall_async_policy: FjallAsyncPolicy,

    buffered_signed_blocks: BTreeSet<SignedBlock>,

    catchupper: DaCatchupper,
    // Gate peer-triggered catchup until genesis outcome is known.
    allow_peer_catchup: bool,

    // Track blocks to send to each streaming peer (ensures ordering)
    peer_send_queues: HashMap<String, VecDeque<ConsensusProposalHash>>,
}

#[derive(Debug, Clone, Copy)]
struct FjallAsyncPolicy {
    timeout: Duration,
    max_retries: usize,
    retry_backoff: Duration,
}

impl FjallAsyncPolicy {
    fn from_env() -> Self {
        let timeout = std::env::var("HYLI_FJALL_ASYNC_TIMEOUT_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .map(Duration::from_millis)
            .unwrap_or_else(|| Duration::from_secs(5));
        let max_retries = std::env::var("HYLI_FJALL_ASYNC_MAX_RETRIES")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(2);
        let retry_backoff = std::env::var("HYLI_FJALL_ASYNC_RETRY_BACKOFF_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .map(Duration::from_millis)
            .unwrap_or_else(|| Duration::from_millis(50));
        Self {
            timeout,
            max_retries,
            retry_backoff,
        }
    }
}

#[derive(Debug, Clone, AsRefStr)]
#[strum(serialize_all = "kebab-case")]
enum DaCatchupPolicy {
    Regular {
        floor: Option<BlockHeight>,
        ceiling: Option<BlockHeight>,
        backfill_enabled: bool,
        backfill_start: Option<BlockHeight>,
    },
    BackfillPending {
        ceiling: BlockHeight,
    },
    Backfill {
        start: BlockHeight,
        ceiling: BlockHeight,
    },
}

#[derive(Debug)]
struct DaCatchupper {
    policy: Option<DaCatchupPolicy>,
    task: Option<tokio::task::JoinHandle<anyhow::Result<()>>>,
    last_height: Option<BlockHeight>,
    sender: tokio::sync::mpsc::Sender<SignedBlock>,
    receiver: Option<tokio::sync::mpsc::Receiver<SignedBlock>>,
    pub peers: Vec<String>,
    da_max_frame_length: usize,
    restart_attempts: usize,
}

impl DaCatchupper {
    pub fn new(policy: Option<DaCatchupPolicy>, da_max_frame_length: usize) -> Self {
        let (sender, receiver) = tokio::sync::mpsc::channel::<SignedBlock>(100);
        DaCatchupper {
            policy,
            task: None,
            last_height: None,
            sender,
            receiver: Some(receiver),
            peers: vec![],
            da_max_frame_length,
            restart_attempts: 0,
        }
    }

    pub fn take_receiver(&mut self) -> Option<tokio::sync::mpsc::Receiver<SignedBlock>> {
        self.receiver.take()
    }

    pub fn is_fast_catchup_initial_block(&self, height: &BlockHeight) -> bool {
        matches!(
            self.policy,
            Some(DaCatchupPolicy::Regular {
                floor: Some(floor),
                ..
            }) if height == &floor
        )
    }

    #[cfg(test)]
    pub fn stop_task(&mut self) {
        if let Some(task) = &mut self.task {
            task.abort();
            self.task = None;
        }
    }

    pub fn ensure_started(&mut self, from_height: BlockHeight) -> anyhow::Result<()> {
        self.ensure_task_running(Some(from_height))
    }

    fn max_restart_attempts() -> usize {
        std::env::var("HYLI_DA_CATCHUP_MAX_RESTARTS")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(12)
    }

    pub fn add_peer_and_maybe_restart(&mut self, peer: String) -> anyhow::Result<bool> {
        if self.peers.contains(&peer) {
            return Ok(false);
        }
        self.peers.push(peer);

        if self.task.is_some() {
            let restart_height = self.last_height.unwrap_or(BlockHeight(0));
            info!(
                "Catchup peer set changed, restarting task from height {}",
                restart_height
            );
            if let Some(task) = &mut self.task {
                task.abort();
            }
            self.task = None;
            self.restart_attempts = 0;
            self.ensure_task_running(Some(restart_height))?;
        }

        Ok(true)
    }

    fn mode_start_height(
        policy: &DaCatchupPolicy,
        requested_start: Option<BlockHeight>,
    ) -> Option<BlockHeight> {
        match policy {
            DaCatchupPolicy::Regular {
                floor: Some(floor), ..
            } => Some(*floor),
            DaCatchupPolicy::Regular { floor: None, .. } => requested_start,
            DaCatchupPolicy::BackfillPending { .. } => None,
            DaCatchupPolicy::Backfill { start, .. } => Some(*start),
        }
    }

    fn mode_ceiling(policy: &DaCatchupPolicy) -> Option<BlockHeight> {
        match policy {
            DaCatchupPolicy::Regular { ceiling, .. } => *ceiling,
            DaCatchupPolicy::BackfillPending { .. } => None,
            DaCatchupPolicy::Backfill { ceiling, .. } => Some(*ceiling),
        }
    }

    fn ensure_task_running(&mut self, requested_start: Option<BlockHeight>) -> anyhow::Result<()> {
        if self.task.is_some() {
            trace!("Catchup task already running, skipping spawn");
            return Ok(());
        }
        let sender = self.sender.clone();

        let Some(policy) = self.policy.as_ref() else {
            debug!("No catchup policy configured, skipping catchup start");
            return Ok(());
        };
        let Some(from_height) = Self::mode_start_height(policy, requested_start) else {
            debug!(
                "Catchup mode {} has no start height yet, waiting",
                policy.as_ref()
            );
            return Ok(());
        };
        self.spawn_for_mode(from_height, sender)
    }

    fn spawn_for_mode(
        &mut self,
        from_height: BlockHeight,
        sender: tokio::sync::mpsc::Sender<SignedBlock>,
    ) -> anyhow::Result<()> {
        let Some(policy) = self.policy.as_ref() else {
            return Ok(());
        };
        let target_height = Self::mode_ceiling(policy);
        let mode_name = policy.as_ref();
        if let Some(height) = target_height {
            if height <= from_height {
                debug!(
                    "Skipping {} catchup spawn: empty range (from={}, to={})",
                    mode_name, from_height, height
                );
                return self.complete_mode_if_reached(height);
            }
        }

        let peers = self.peers.clone();
        if peers.is_empty() {
            info!("No peers available for catchup");
            return Ok(());
        }

        info!(
            "Starting {} catchup from height {} to {:?}",
            mode_name, from_height, target_height
        );

        self.task = Some(Self::spawn_stream_task(
            peers,
            self.da_max_frame_length,
            from_height,
            sender,
        ));
        self.last_height = Some(from_height);

        Ok(())
    }

    fn transition_after_regular_done(&mut self) {
        let Some(DaCatchupPolicy::Regular {
            floor,
            backfill_enabled,
            backfill_start,
            ..
        }) = self.policy.clone()
        else {
            self.policy = None;
            return;
        };

        debug!(
            "Regular catchup completion: floor={:?}, backfill_enabled={}, backfill_start={:?}",
            floor, backfill_enabled, backfill_start
        );
        self.policy = None;
        if backfill_enabled {
            if let Some(floor) = floor {
                if let Some(start) = backfill_start {
                    if start < floor {
                        info!(
                            "Transitioning to backfill mode: start={}, ceiling={}",
                            start, floor
                        );
                        self.policy = Some(DaCatchupPolicy::Backfill {
                            start,
                            ceiling: floor,
                        });
                    } else {
                        info!(
                            "Skipping backfill: discovered start {} is not below floor {}",
                            start, floor
                        );
                    }
                } else {
                    info!(
                        "Transitioning to backfill-pending mode at ceiling {}",
                        floor
                    );
                    self.policy = Some(DaCatchupPolicy::BackfillPending { ceiling: floor });
                }
            } else {
                debug!("Backfill is enabled but regular floor is unknown, no transition");
            }
        } else {
            debug!("Backfill disabled, clearing catchup policy");
        }
    }

    fn complete_mode_if_reached(&mut self, processed_height: BlockHeight) -> anyhow::Result<()> {
        let Some(policy) = self.policy.as_ref() else {
            return Ok(());
        };
        let Some(ceiling) = Self::mode_ceiling(policy) else {
            return Ok(());
        };
        if processed_height < ceiling {
            return Ok(());
        }

        if let Some(task) = &mut self.task {
            task.abort();
        }
        self.task = None;
        match policy {
            DaCatchupPolicy::Regular { .. } => {
                info!(
                    "Regular catchup done at height {}, evaluating backfill",
                    processed_height
                );
                self.transition_after_regular_done();
            }
            DaCatchupPolicy::BackfillPending { .. } => {
                debug!("Backfill is pending first-hole discovery");
            }
            DaCatchupPolicy::Backfill { .. } => {
                info!("Backfill catchup done at height {}", processed_height);
                self.policy = None;
            }
        }
        self.ensure_task_running(None)
    }

    pub fn on_first_hole_discovered(&mut self, hole: Option<BlockHeight>) {
        match &mut self.policy {
            Some(DaCatchupPolicy::Regular { backfill_start, .. }) => {
                debug!(
                    "First-hole discovery during regular mode: hole={:?}, existing_backfill_start={:?}",
                    hole, backfill_start
                );
                *backfill_start = backfill_start.or(hole);
            }
            Some(DaCatchupPolicy::BackfillPending { ceiling }) => {
                debug!(
                    "First-hole discovery during backfill-pending: hole={:?}, ceiling={}",
                    hole, ceiling
                );
                if let Some(start) = hole {
                    if start < *ceiling {
                        info!(
                            "Resolved backfill-pending -> backfill (start={}, ceiling={})",
                            start, ceiling
                        );
                        self.policy = Some(DaCatchupPolicy::Backfill {
                            start,
                            ceiling: *ceiling,
                        });
                    } else {
                        info!(
                            "Dropping catchup policy: first hole {} is not below pending ceiling {}",
                            start, ceiling
                        );
                        self.policy = None;
                    }
                } else {
                    info!("Dropping catchup policy: no first hole found for pending backfill");
                    self.policy = None;
                }
            }
            _ => {
                debug!(
                    "Ignoring first-hole discovery for non-catchup state: hole={:?}",
                    hole
                );
            }
        }
    }

    pub fn on_catchup_progress(&mut self, processed_height: BlockHeight) -> anyhow::Result<()> {
        if self.policy.is_none() {
            return Ok(());
        }

        if let Some(last_height) = &mut self.last_height {
            *last_height = processed_height.max(*last_height);
        }
        self.restart_attempts = 0;
        self.complete_mode_if_reached(processed_height)
    }

    pub fn on_tick(&mut self) -> anyhow::Result<()> {
        if self.policy.is_none() {
            return Ok(());
        }
        self.ensure_task_running(None)?;
        let Some(task) = &self.task else {
            return Ok(());
        };
        if !task.is_finished() {
            return Ok(());
        }
        let restart_height = self.last_height.unwrap_or(BlockHeight(0));
        self.restart_attempts += 1;
        let max_restart_attempts = Self::max_restart_attempts();
        if self.restart_attempts > max_restart_attempts {
            self.task = None;
            return Err(anyhow::anyhow!(
                "Catchup failed after {} restarts (last height {}). Aborting catchup.",
                self.restart_attempts,
                restart_height
            ));
        }

        info!(
            "Catchup task finished before reaching target, restarting from height {} (attempt {}/{})",
            restart_height,
            self.restart_attempts,
            max_restart_attempts
        );
        self.task = None;
        self.spawn_for_mode(restart_height, self.sender.clone())
    }

    pub fn on_mempool_started_building(&mut self, height: BlockHeight) -> anyhow::Result<()> {
        if let Some(DaCatchupPolicy::Regular {
            floor,
            ceiling,
            backfill_enabled,
            backfill_start: _,
        }) = &mut self.policy
        {
            if ceiling.is_none() {
                *ceiling = Some(height);
                info!(
                    "Bounded regular catchup at ceiling {} (floor={:?}, backfill_enabled={})",
                    height, floor, backfill_enabled
                );
            } else {
                debug!(
                    "Ignoring started-building event at {}: regular ceiling already set to {:?}",
                    height, ceiling
                );
            }
        } else {
            debug!(
                "Ignoring started-building event at {}: catchup is not in regular mode",
                height
            );
        }
        if let Some(progress) = self.last_height {
            self.complete_mode_if_reached(progress)?;
        }
        Ok(())
    }

    fn spawn_stream_task(
        peers: Vec<String>,
        da_max_frame_length: usize,
        start_height: BlockHeight,
        sender: tokio::sync::mpsc::Sender<SignedBlock>,
    ) -> JoinHandle<anyhow::Result<()>> {
        info!("Starting catchup from height {}", start_height);

        tokio::spawn(async move {
            let timeout_duration = std::env::var("HYLI_DA_SLEEP_TIMEOUT")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .map(Duration::from_secs)
                .unwrap_or_else(|| Duration::from_secs(10));

            let mut stream = SignedDaStream::new(
                "catchupper",
                "catchupper",
                Some(da_max_frame_length),
                peers,
                start_height,
                timeout_duration,
            );
            log_error!(
                stream.start_client_with_metrics().await,
                "Error occurred setting up the DA listener"
            )?;

            loop {
                match stream.listen_next().await? {
                    DaStreamPoll::Timeout => {
                        warn!("Timeout expired while waiting for block.");
                        stream.reconnect("timeout").await?;
                    }
                    DaStreamPoll::StreamClosed => {
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        stream.reconnect("stream_closed").await?;
                    }
                    DaStreamPoll::Event(event) => match event {
                        DataAvailabilityEvent::SignedBlock(block) => {
                            let blocks = stream.on_signed_block(block).await?;
                            for block in blocks {
                                debug!(
                                    "📦 Received block (height {}) from stream",
                                    block.consensus_proposal.slot
                                );

                                if let Err(e) = sender.send(block).await {
                                    tracing::error!(
                                        "Error while sending block over channel: {:#}",
                                        e
                                    );
                                    return Ok(());
                                }
                            }
                        }
                        _ => {
                            tracing::trace!("Dropped received message in catchup task");
                        }
                    },
                }
            }
        })
    }
}

impl Default for DaCatchupper {
    fn default() -> Self {
        Self::new(None, 0)
    }
}

impl DataAvailability {
    async fn run_fjall_blocking<T, F, Op>(
        &self,
        op: &'static str,
        keyspace: &'static str,
        mut build: F,
    ) -> Result<T>
    where
        T: Send + 'static,
        F: FnMut() -> Op,
        Op: FnOnce() -> Result<T> + Send + 'static,
    {
        let mut attempts = 0usize;
        loop {
            let handle = tokio::task::spawn_blocking(build());
            let timed = tokio::time::timeout(self.fjall_async_policy.timeout, handle).await;
            match timed {
                Ok(joined) => match joined {
                    Ok(Ok(value)) => return Ok(value),
                    Ok(Err(err)) => {
                        if attempts >= self.fjall_async_policy.max_retries {
                            return Err(err);
                        }
                        attempts += 1;
                        self.blocks.record_retry(op, keyspace);
                        warn!(
                            op = op,
                            keyspace = keyspace,
                            attempt = attempts,
                            max_retries = self.fjall_async_policy.max_retries,
                            "retrying failed fjall call"
                        );
                    }
                    Err(join_err) => {
                        if attempts >= self.fjall_async_policy.max_retries {
                            return Err(anyhow::anyhow!(
                                "fjall blocking task join error: {}",
                                join_err
                            ));
                        }
                        attempts += 1;
                        self.blocks.record_retry(op, keyspace);
                        warn!(
                            op = op,
                            keyspace = keyspace,
                            attempt = attempts,
                            max_retries = self.fjall_async_policy.max_retries,
                            error = %join_err,
                            "retrying fjall call after join error"
                        );
                    }
                },
                Err(_) => {
                    self.blocks.record_timeout(op, keyspace);
                    if attempts >= self.fjall_async_policy.max_retries {
                        return Err(anyhow::anyhow!(
                            "fjall call timed out after {} ms (op={}, keyspace={})",
                            self.fjall_async_policy.timeout.as_millis(),
                            op,
                            keyspace
                        ));
                    }
                    attempts += 1;
                    self.blocks.record_retry(op, keyspace);
                    warn!(
                        op = op,
                        keyspace = keyspace,
                        attempt = attempts,
                        max_retries = self.fjall_async_policy.max_retries,
                        timeout_ms = self.fjall_async_policy.timeout.as_millis(),
                        "fjall call timed out; retrying"
                    );
                }
            }
            tokio::time::sleep(self.fjall_async_policy.retry_backoff).await;
        }
    }

    async fn blocks_get(&self, hash: ConsensusProposalHash) -> Result<Option<SignedBlock>> {
        let blocks = self.blocks.new_handle();
        self.run_fjall_blocking("get", "by_hash", move || {
            let blocks = blocks.new_handle();
            let hash = hash.clone();
            move || blocks.get(&hash)
        })
        .await
    }

    async fn blocks_get_by_height(&self, height: BlockHeight) -> Result<Option<SignedBlock>> {
        let blocks = self.blocks.new_handle();
        self.run_fjall_blocking("get_by_height", "by_height", move || {
            let blocks = blocks.new_handle();
            move || blocks.get_by_height(height)
        })
        .await
    }

    async fn blocks_contains(&self, hash: ConsensusProposalHash) -> Result<bool> {
        let blocks = self.blocks.new_handle();
        self.run_fjall_blocking("contains", "by_hash", move || {
            let blocks = blocks.new_handle();
            let hash = hash.clone();
            move || Ok(blocks.contains(&hash))
        })
        .await
    }

    async fn blocks_highest(&self) -> Result<BlockHeight> {
        let blocks = self.blocks.new_handle();
        self.run_fjall_blocking("highest", "by_height", move || {
            let blocks = blocks.new_handle();
            move || Ok(blocks.highest())
        })
        .await
    }

    async fn blocks_put(&self, block: SignedBlock) -> Result<()> {
        let blocks = self.blocks.new_handle();
        self.run_fjall_blocking("put", "by_hash", move || {
            let mut blocks = blocks.new_handle();
            let block = block.clone();
            move || blocks.put(block)
        })
        .await
    }

    async fn blocks_persist(&self) -> Result<()> {
        let blocks = self.blocks.new_handle();
        self.run_fjall_blocking("persist", "db", move || {
            let blocks = blocks.new_handle();
            move || blocks.persist()
        })
        .await
    }

    pub fn start_scanning_for_first_hole(
        &self,
    ) -> tokio::sync::mpsc::Receiver<Option<BlockHeight>> {
        let blocks_handle = self.blocks.new_handle();

        let (first_hole_sender, first_hole_receiver) =
            tokio::sync::mpsc::channel::<Option<BlockHeight>>(10);

        if let Some(DaCatchupPolicy::Regular {
            backfill_enabled: true,
            ..
        }) = self.catchupper.policy
        {
            // Start scanning local storage for first hole, if any
            _ = tokio::task::spawn(async move {
                loop {
                    match blocks_handle.first_hole_by_height() {
                        Err(e) => {
                            debug!("Catchup not started yet, no data in partition: {}", e);
                            tokio::time::sleep(Duration::from_millis(500)).await;
                        }
                        Ok(el) => {
                            _ = first_hole_sender.send(el).await;
                            break;
                        }
                    }
                }
            });
        }

        first_hole_receiver
    }

    pub async fn start(&mut self) -> Result<()> {
        info!(
            "📡  Starting DataAvailability module, listening for stream requests on port {}",
            self.config.da_server_port
        );

        let mut server = DataAvailabilityServer::start_with_opts(
            self.config.da_server_port,
            Some(self.config.da_max_frame_length),
            "Da",
        )
        .await?;

        let mut catchup_block_receiver = self
            .catchupper
            .take_receiver()
            .ok_or_else(|| anyhow::anyhow!("Catchup receiver already taken"))?;

        let mut first_hole_receiver = self.start_scanning_for_first_hole();

        // Used to send blocks to clients (indexers/peers)
        // This is a JoinSet of tuples containing:
        // - The peer IP address to send the blocks to
        // - The number of retries for sending the blocks
        let mut catchup_joinset: JoinSet<(String, usize)> = tokio::task::JoinSet::new();
        let mut catchup_task_checker_ticker =
            tokio::time::interval(std::time::Duration::from_secs(5));
        let mut storage_metrics_ticker = tokio::time::interval(std::time::Duration::from_secs(30));

        module_handle_messages! {
            on_self self,
            listen<MempoolBlockEvent> evt => {
                _ = log_error!(self.handle_mempool_event(evt, &mut server, &mut catchup_joinset).await, "Handling Mempool Event");
            }

            listen<MempoolStatusEvent> evt => {
                self.handle_mempool_status_event(evt, &mut server).await;
            }

            listen<GenesisEvent> cmd => {
                match cmd {
                    GenesisEvent::GenesisBlock(signed_block) => {
                        debug!("🌱  Genesis block received with validators {:?}", signed_block.consensus_proposal.staking_actions.clone());
                        _ = log_error!(self.handle_signed_block(signed_block, &mut server, &mut catchup_joinset).await.context("Handling Genesis block"),  "Handling GenesisBlock Event");
                    }
                    GenesisEvent::NoGenesis => {
                        self.allow_peer_catchup = true;
                        let highest = self.blocks_highest().await?;
                        _ = log_error!(
                            self.catchupper.ensure_started(highest),
                            "Init catchup after NoGenesis"
                        );
                    }
                }
            }

            listen<PeerEvent> PeerEvent::NewPeer { da_address, .. } => {
                let added = self.catchupper.add_peer_and_maybe_restart(da_address.clone())?;
                if added {
                    info!("New peer {}", da_address);
                } else {
                    debug!("Known peer announced again: {}", da_address);
                }
                if self.allow_peer_catchup {
                    let highest = self.blocks_highest().await?;
                    self.catchupper.ensure_started(highest)?;
                } else {
                    debug!("Skipping catchup init on new peer while genesis path is unresolved");
                }
            }

            _ = catchup_task_checker_ticker.tick(), if self.catchupper.policy.is_some() => {
                self.catchupper.on_tick()?;
            }

            Some(streamed_block) = catchup_block_receiver.recv() => {
                if let Some(height) = self.handle_signed_block(streamed_block, &mut server, &mut catchup_joinset).await {
                    _ = log_error!(self.catchupper.on_catchup_progress(height), "Catchup transition after streamed block");
                }
            }

            Some(tcp_event) = server.listen_next() => {
                match tcp_event {
                    TcpEvent::Message { socket_addr, data, .. } => {
                        match data {
                            DataAvailabilityRequest::StreamFromHeight(start_height) => {
                                _ = log_error!(
                                    self.start_streaming_to_peer(
                                        start_height,
                                        &mut catchup_joinset,
                                        &socket_addr,
                                        &mut server,
                                    ).await,
                                    "Starting streaming to peer"
                                );
                            }
                            DataAvailabilityRequest::BlockRequest(block_height) => {
                                _ = log_error!(
                                    self.handle_block_request(block_height, &socket_addr, &mut server).await,
                                    "Handling block request"
                                );
                            }
                        }
                    }
                    TcpEvent::Closed { socket_addr } => {
                        server.drop_peer_stream(socket_addr.clone());
                        self.peer_send_queues.remove(&socket_addr);
                    }
                    TcpEvent::Error { socket_addr, error } => {
                        warn!("TCP error from {}: {}. Dropping socket.", socket_addr, error);
                        server.drop_peer_stream(socket_addr.clone());
                        self.peer_send_queues.remove(&socket_addr);
                    }
                }
            }

            // Send one block to a peer as part of "catchup",
            // once we have sent all blocks the peer is presumably synchronised.
            Some(Ok((peer_ip, retries))) = catchup_joinset.join_next() => {

                #[cfg(test)]
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;

                _ = log_error!(
                    self.handle_send_next_block_to_peer(
                        peer_ip.clone(),
                        retries,
                        &mut catchup_joinset,
                        &mut server
                    ).await,
                    "Send next block to peer"
                );
            }

            Some(hole) = first_hole_receiver.recv() => {
                info!("Setting backfill start height as {:?}", &hole);
                self.catchupper.on_first_hole_discovered(hole);
                self.catchupper.on_tick()?;
            }

            _ = storage_metrics_ticker.tick() => {
                self.blocks.record_metrics();
            }
        };

        Ok(())
    }

    async fn handle_send_next_block_to_peer(
        &mut self,
        peer_ip: String,
        retries: usize,
        catchup_joinset: &mut JoinSet<(String, usize)>,
        server: &mut DataAvailabilityServer,
    ) -> Result<()> {
        if !server.connected(&peer_ip) {
            debug!("Peer {} disconnected, removing from send queues", peer_ip);
            self.peer_send_queues.remove(&peer_ip);
            return Ok(());
        }

        if retries > 10 {
            warn!(
                "Failed to send block, too many retries for peer {}",
                &peer_ip
            );
            server.drop_peer_stream(peer_ip.clone());
            self.peer_send_queues.remove(&peer_ip);
            return Ok(());
        }

        // Get next block from this peer's queue
        let hash = match self.peer_send_queues.get_mut(&peer_ip) {
            Some(queue) => match queue.pop_front() {
                Some(h) => h,
                None => {
                    // Queue is empty - peer is caught up and waiting for new blocks
                    // Keep them in the map but don't spawn a new task yet
                    debug!("Peer {} caught up, waiting for new blocks", peer_ip);
                    return Ok(());
                }
            },
            None => {
                debug!("Peer {} not in send queues", peer_ip);
                return Ok(());
            }
        };

        debug!("📡  Sending block {} to peer {}", &hash, &peer_ip);
        if let Ok(Some(signed_block)) = self.blocks_get(hash.clone()).await {
            // Errors will be handled when sending new blocks, ignore here.
            match server.send(
                peer_ip.clone(),
                DataAvailabilityEvent::SignedBlock(signed_block),
                vec![],
            ) {
                Ok(()) => {
                    // Successfully sent, continue with next block
                    catchup_joinset.spawn(async move { (peer_ip, 0) });
                }
                Err(_) => {
                    // Retry sending the same block (put it back at front of queue)
                    if let Some(queue) = self.peer_send_queues.get_mut(&peer_ip) {
                        queue.push_front(hash);
                    }
                    catchup_joinset.spawn(async move {
                        tokio::time::sleep(Duration::from_millis(100 * (retries as u64))).await;
                        (peer_ip, retries + 1)
                    });
                }
            }
        } else {
            error!(
                "Block {} not found in storage while sending to peer {}. Should not happen",
                &hash, &peer_ip
            );
            // Continue anyway with next block
            catchup_joinset.spawn(async move { (peer_ip, 0) });
        }
        Ok(())
    }

    async fn handle_block_request(
        &mut self,
        block_height: BlockHeight,
        socket_addr: &str,
        server: &mut DataAvailabilityServer,
    ) -> Result<()> {
        debug!(
            "📦 Received block request for height {} from {}",
            block_height, socket_addr
        );

        // Check if block exists in storage
        match self.blocks_get_by_height(block_height).await {
            Ok(Some(block)) => {
                debug!(
                    "📦 Found block at height {}, sending to {}",
                    block_height, socket_addr
                );
                // Send immediately - this is inserted next in the send queue
                if let Err(e) = server.send(
                    socket_addr.to_string(),
                    DataAvailabilityEvent::SignedBlock(block),
                    vec![],
                ) {
                    warn!(
                        "📦 Error while responding to block request at height {} for {}: {:#}. Dropping socket.",
                        block_height, socket_addr, e
                    );
                    server.drop_peer_stream(socket_addr.to_string());
                    return Ok(());
                }
            }
            Ok(None) => {
                // Block not in storage - this is a gap
                error!(
                    "📦 Block at height {} not found in storage, sending BlockNotFound to {}",
                    block_height, socket_addr
                );
                if let Err(e) = server.send(
                    socket_addr.to_string(),
                    DataAvailabilityEvent::BlockNotFound(block_height),
                    vec![],
                ) {
                    warn!(
                        "📦 Error while responding BlockNotFound at height {} for {}: {:#}. Dropping socket.",
                        block_height, socket_addr, e
                    );
                    server.drop_peer_stream(socket_addr.to_string());
                    return Ok(());
                }
            }
            Err(e) => {
                error!(
                    "📦 Error retrieving block at height {}: {:#}",
                    block_height, e
                );
                if let Err(e) = server.send(
                    socket_addr.to_string(),
                    DataAvailabilityEvent::BlockNotFound(block_height),
                    vec![],
                ) {
                    warn!(
                        "📦 Error while responding BlockNotFound at height {} for {}: {:#}. Dropping socket.",
                        block_height, socket_addr, e
                    );
                    server.drop_peer_stream(socket_addr.to_string());
                    return Ok(());
                }
            }
        }

        Ok(())
    }

    async fn handle_mempool_event(
        &mut self,
        evt: MempoolBlockEvent,
        tcp_server: &mut DataAvailabilityServer,
        catchup_joinset: &mut JoinSet<(String, usize)>,
    ) -> Result<()> {
        match evt {
            MempoolBlockEvent::BuiltSignedBlock(signed_block) => {
                debug!(
                    "📦  Received built block (height {}) from Mempool",
                    signed_block.height()
                );
                // Mempool-produced blocks are local tip updates, not catchup-stream progress.
                // Feeding them into catchup progress can prematurely complete backfill.
                _ = self
                    .handle_signed_block(signed_block, tcp_server, catchup_joinset)
                    .await;
            }
            MempoolBlockEvent::StartedBuildingBlocks(height) => {
                debug!(
                    "Received started building block (at height {}) from Mempool",
                    height
                );
                self.catchupper.on_mempool_started_building(height)?;
            }
        }

        Ok(())
    }

    async fn handle_mempool_status_event(
        &mut self,
        evt: MempoolStatusEvent,
        tcp_server: &mut DataAvailabilityServer,
    ) {
        let errors = tcp_server.broadcast(DataAvailabilityEvent::MempoolStatusEvent(evt));

        for (peer, error) in errors {
            warn!("Error while broadcasting mempool status event {:#}", error);
            tcp_server.drop_peer_stream(peer.clone());
        }
    }

    /// if handled, returns the highest height of the processed blocks
    async fn handle_signed_block(
        &mut self,
        block: SignedBlock,
        tcp_server: &mut DataAvailabilityServer,
        catchup_joinset: &mut JoinSet<(String, usize)>,
    ) -> Option<BlockHeight> {
        let hash = block.hashed();
        // if new block is already handled, ignore it
        if self.blocks_contains(hash.clone()).await.unwrap_or(false) {
            warn!(
                "Block {} {} already exists !",
                block.height(),
                block.hashed()
            );
            return None;
        }

        if block.height() == BlockHeight(0) {
            info!("Received genesis block {}", block.hashed());
        } else if self
            .catchupper
            .is_fast_catchup_initial_block(&block.height())
        {
            info!(
                "Received block with height {} which is the catchup floor height",
                block.height()
            );
        }
        // if new block is not the next block in the chain, buffer
        else if !self
            .blocks_contains(block.parent_hash().clone())
            .await
            .unwrap_or(false)
        {
            debug!(
                "Parent block '{}' not found for block hash='{}' height {}",
                block.parent_hash(),
                block.hashed(),
                block.height()
            );
            debug!("Buffering block {}", block.hashed());
            self.buffered_signed_blocks.insert(block);
            return None;
        }

        let highest = self.blocks_highest().await.unwrap_or(BlockHeight(0));
        if block.height() < highest {
            // If we are in fast catchup, we need to backfill the block
            _ = log_error!(self.store_block(&block).await, "Backfilling block");
        } else {
            // store block
            _ = log_error!(
                self.add_processed_block(block.clone(), tcp_server, catchup_joinset)
                    .await,
                "Adding processed block"
            );
        }

        let highest_processed_height = self.pop_buffer(hash, tcp_server, catchup_joinset).await;
        _ = log_error!(self.blocks_persist().await, "Persisting blocks");

        let height = block.height();

        Some(highest_processed_height.unwrap_or(height))
    }

    /// Returns the highest height of the processed blocks
    async fn pop_buffer(
        &mut self,
        mut last_block_hash: ConsensusProposalHash,
        tcp_server: &mut DataAvailabilityServer,
        catchup_joinset: &mut JoinSet<(String, usize)>,
    ) -> Option<BlockHeight> {
        let mut res = None;

        // Iterative loop to avoid stack overflows
        while let Some(first_buffered) = self.buffered_signed_blocks.first() {
            if first_buffered.parent_hash() != &last_block_hash {
                debug!(
                    "Stopping processing buffered blocks - hole in the buffer after {} (found parent hash {})",
                    last_block_hash,
                    first_buffered.parent_hash(),
                );
                break;
            }
            #[allow(
                clippy::unwrap_used,
                reason = "Must exist as checked in the while above"
            )]
            let first_buffered = self.buffered_signed_blocks.pop_first().unwrap();
            last_block_hash = first_buffered.hashed();

            let height = first_buffered.height();

            if self
                .add_processed_block(first_buffered.clone(), tcp_server, catchup_joinset)
                .await
                .is_ok()
            {
                res = res.map_or(Some(height), |r: BlockHeight| Some(r.max(height)))
            }
        }

        res
    }

    async fn store_block(&mut self, block: &SignedBlock) -> Result<()> {
        self.blocks_put(block.clone())
            .await
            .context(format!("Storing block {}", block.height()))?;

        trace!("Block {} {}: {:#?}", block.height(), block.hashed(), block);

        let height = block.height().0;
        let info_log_interval = if height < 1_000 { 10 } else { 1_000 };
        if height.is_multiple_of(info_log_interval) {
            info!(
                "new block #{} 0x{} with {} txs",
                block.height(),
                block.hashed(),
                block.count_txs(),
            );
        }
        debug!(
            "new block #{} 0x{} with {} transactions: {}",
            block.height(),
            block.hashed(),
            block.count_txs(),
            block
                .iter_txs_with_id()
                .map(|(_, tx_id, tx)| {
                    let variant: &'static str = (&tx.transaction_data).into();
                    format!("\n - 0x{} {}", tx_id.1, variant)
                })
                .collect::<Vec<_>>()
                .join("")
        );

        Ok(())
    }

    async fn add_processed_block(
        &mut self,
        block: SignedBlock,
        _tcp_server: &mut DataAvailabilityServer,
        catchup_joinset: &mut JoinSet<(String, usize)>,
    ) -> anyhow::Result<()> {
        self.store_block(&block).await?;

        let block_hash = block.hashed();

        // Add new block to all streaming peer queues to ensure ordering
        // (instead of broadcasting which can cause out-of-order delivery)
        for (peer, queue) in self.peer_send_queues.iter_mut() {
            let was_empty = queue.is_empty();
            queue.push_back(block_hash.clone());

            // If queue was empty (peer was caught up), restart their send task
            if was_empty {
                debug!(
                    "Restarting send task for caught-up peer {} with new block {}",
                    peer, block_hash
                );
                let peer_clone = peer.clone();
                catchup_joinset.spawn(async move { (peer_clone, 0) });
            } else {
                debug!(
                    "Appending block {} to queue for peer {} (queue size: {})",
                    block_hash,
                    peer,
                    queue.len()
                );
            }
        }

        // Send the block to NodeState for processing
        _ = log_error!(
            self.bus
                .send_waiting_if_full(DataEvent::OrderedSignedBlock(block))
                .await,
            "Sending OrderedSignedBlock"
        );

        Ok(())
    }

    async fn start_streaming_to_peer(
        &mut self,
        start_height: BlockHeight,
        catchup_joinset: &mut JoinSet<(String, usize)>,
        peer_ip: &str,
        server: &mut DataAvailabilityServer,
    ) -> Result<()> {
        let range_start = std::time::Instant::now();
        let highest = self
            .blocks
            .last()
            .map_or(start_height, |block| block.height());

        // Collect all blocks from start_height to current highest
        let processed_block_hashes: VecDeque<_> = self
            .blocks
            .range(start_height, highest + 1)
            .filter_map(|item| item.ok())
            .collect();
        self.blocks
            .record_op("range_collect", "by_height", range_start.elapsed());

        let expected = highest.0.saturating_sub(start_height.0).saturating_add(1);
        // If requester starts beyond our current tip, they are already caught up:
        // the valid stream response is an empty queue (wait for future blocks), not BlockNotFound.
        let expected = if start_height > highest { 0 } else { expected };
        if processed_block_hashes.len() as u64 != expected {
            let first_missing = (start_height.0..=highest.0)
                .find(|height| {
                    self.blocks
                        .has_by_height(BlockHeight(*height))
                        .map(|present| !present)
                        .unwrap_or(true)
                })
                .map(BlockHeight)
                .unwrap_or(start_height);

            info!(
                "Rejecting stream for peer {}: local gap detected at height {} while serving [{}..={}]",
                peer_ip, first_missing, start_height, highest
            );

            if let Err(e) = server.send(
                peer_ip.to_string(),
                DataAvailabilityEvent::BlockNotFound(first_missing),
                vec![],
            ) {
                warn!(
                    "Error sending BlockNotFound at height {} to {}: {:#}",
                    first_missing, peer_ip, e
                );
            }

            server.drop_peer_stream(peer_ip.to_string());
            self.peer_send_queues.remove(peer_ip);
            return Ok(());
        }

        info!(
            "Starting stream to peer {} from height {} ({} blocks queued)",
            peer_ip,
            start_height,
            processed_block_hashes.len()
        );

        // Store queue for this peer - new blocks will be appended here
        let peer_ip_string = peer_ip.to_string();
        self.peer_send_queues
            .insert(peer_ip_string.clone(), processed_block_hashes);

        // Start the send task for this peer
        catchup_joinset.spawn(async move { (peer_ip_string, 0) });

        Ok(())
    }
}

#[cfg(test)]
pub mod tests {
    #![allow(clippy::indexing_slicing)]
    use std::{collections::HashMap, time::Duration};

    use super::module_bus_client;
    use super::Blocks;
    use crate::data_availability::DaCatchupPolicy;
    use crate::{
        bus::BusClientSender,
        consensus::CommittedConsensusProposal,
        model::*,
        utils::{conf::Conf, integration_test::find_available_port},
    };
    use anyhow::Result;
    use hyli_modules::log_error;
    use hyli_modules::node_state::module::NodeStateBusClient;
    use hyli_modules::node_state::NodeState;
    use hyli_modules::utils::da_codec::DataAvailabilityClient;
    use hyli_modules::utils::da_codec::DataAvailabilityServer;
    use hyli_net::tcp::TcpEvent;
    use staking::state::Staking;
    use tokio::task::JoinSet;

    struct DataAvailabilityTestCtx {
        pub node_state_bus: NodeStateBusClient,
        pub da: super::DataAvailability,
        pub node_state: NodeState,
    }

    impl DataAvailabilityTestCtx {
        pub async fn new(shared_bus: crate::bus::SharedMessageBus) -> Self {
            let path = tempfile::tempdir().unwrap().keep();
            let tmpdir = path;
            let blocks = Blocks::new(&tmpdir).unwrap();

            let bus = super::DABusClient::new_from_bus(shared_bus.new_handle()).await;
            let node_state_bus = NodeStateBusClient::new_from_bus(shared_bus).await;

            let mut config: Conf = Conf::new(vec![], None, None).unwrap();

            let node_state = NodeState::create("data_availability");

            config.da_server_port = find_available_port().await;
            config.da_public_address = format!("127.0.0.1:{}", config.da_server_port);
            let da = super::DataAvailability {
                config: config.into(),
                bus,
                blocks,
                buffered_signed_blocks: Default::default(),
                catchupper: Default::default(),
                allow_peer_catchup: false,
                peer_send_queues: HashMap::new(),
            };

            DataAvailabilityTestCtx {
                node_state_bus,
                da,
                node_state,
            }
        }

        pub async fn handle_signed_block(
            &mut self,
            block: SignedBlock,
            tcp_server: &mut DataAvailabilityServer,
        ) {
            let mut catchup_joinset: JoinSet<(String, usize)> = JoinSet::new();
            _ = self
                .da
                .handle_signed_block(block.clone(), tcp_server, &mut catchup_joinset)
                .await;
            let block_hash = block.hashed();
            let Ok(full_block) = self.node_state.handle_signed_block(block) else {
                tracing::warn!("Error while handling signed block {}", block_hash);
                return;
            };
            _ = log_error!(
                self.node_state_bus
                    .send_waiting_if_full(NodeStateEvent::NewBlock(full_block))
                    .await,
                "Sending NodeState event"
            );
        }
    }

    #[test_log::test]
    fn test_blocks() -> Result<()> {
        let tmpdir = tempfile::tempdir().unwrap().keep();
        let mut blocks = Blocks::new(&tmpdir).unwrap();
        let block = SignedBlock::default();
        blocks.put(block.clone())?;
        assert!(blocks.last().unwrap().height() == block.height());
        let last = blocks.get(&block.hashed())?;
        assert!(last.is_some());
        assert!(last.unwrap().height() == BlockHeight(0));
        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn test_pop_buffer_large() {
        let tmpdir = tempfile::tempdir().unwrap().keep();
        let blocks = Blocks::new(&tmpdir).unwrap();

        let port = find_available_port().await;
        let mut server = DataAvailabilityServer::start(port, "DaServer")
            .await
            .unwrap();

        let bus = super::DABusClient::new_from_bus(crate::bus::SharedMessageBus::new()).await;
        let mut da = super::DataAvailability {
            config: Default::default(),
            bus,
            blocks,
            buffered_signed_blocks: Default::default(),
            catchupper: Default::default(),
            allow_peer_catchup: false,
            peer_send_queues: HashMap::new(),
        };
        let mut block = SignedBlock::default();
        let mut blocks = vec![];
        for i in 1..10000 {
            blocks.push(block.clone());
            block.consensus_proposal.parent_hash = block.hashed();
            block.consensus_proposal.slot = i;
        }
        blocks.reverse();
        let mut catchup_joinset: JoinSet<(String, usize)> = JoinSet::new();
        for block in blocks {
            if block.height().0 == 0 {
                assert_eq!(
                    da.handle_signed_block(block, &mut server, &mut catchup_joinset)
                        .await,
                    Some(BlockHeight(9998))
                );
            } else {
                assert_eq!(
                    da.handle_signed_block(block, &mut server, &mut catchup_joinset)
                        .await,
                    None
                );
            }
        }
    }

    module_bus_client! {
    #[derive(Debug)]
    struct TestBusClient {
        sender(MempoolBlockEvent),
    }
    }

    #[test_log::test(tokio::test)]
    async fn test_da_streaming() {
        let tmpdir = tempfile::tempdir().unwrap().keep();
        let blocks = Blocks::new(&tmpdir).unwrap();

        let global_bus = crate::bus::SharedMessageBus::new();
        let bus = super::DABusClient::new_from_bus(global_bus.new_handle()).await;
        let mut block_sender = TestBusClient::new_from_bus(global_bus).await;

        let mut config: Conf = Conf::new(vec![], None, None).unwrap();
        config.da_server_port = find_available_port().await;
        config.da_public_address = format!("127.0.0.1:{}", config.da_server_port);
        let mut da = super::DataAvailability {
            config: config.clone().into(),
            bus,
            blocks,
            buffered_signed_blocks: Default::default(),
            catchupper: Default::default(),
            allow_peer_catchup: false,
            peer_send_queues: HashMap::new(),
        };

        let mut block = SignedBlock::default();
        let mut blocks = vec![];
        for i in 1..15 {
            blocks.push(block.clone());
            block.consensus_proposal.parent_hash = block.hashed();
            block.consensus_proposal.slot = i;
        }
        blocks.reverse();

        // Start Da and its client
        tokio::spawn(async move {
            da.start().await.unwrap();
        });

        let mut client =
            DataAvailabilityClient::connect("client_id", config.da_public_address.clone())
                .await
                .unwrap();

        client
            .send(DataAvailabilityRequest::StreamFromHeight(BlockHeight(0)))
            .await
            .unwrap();

        // Feed Da with blocks, should stream them to the client
        for block in blocks {
            block_sender
                .send(MempoolBlockEvent::BuiltSignedBlock(block))
                .unwrap();
        }

        // wait until it's up
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let mut heights_received = vec![];
        while let Some(event) = client.recv().await {
            if let DataAvailabilityEvent::SignedBlock(block) = event {
                heights_received.push(block.height().0);
            }
            if heights_received.len() == 14 {
                break;
            }
        }
        assert_eq!(heights_received, (0..14).collect::<Vec<u64>>());

        client.close().await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let mut ccp = CommittedConsensusProposal {
            staking: Staking::default(),
            consensus_proposal: ConsensusProposal::default(),
            certificate: AggregateSignature {
                signature: crate::model::Signature("signature".into()),
                validators: vec![],
            },
        };

        for i in 14..18 {
            ccp.consensus_proposal.parent_hash = ccp.consensus_proposal.hashed();
            ccp.consensus_proposal.slot = i;
            block_sender
                .send(MempoolBlockEvent::BuiltSignedBlock(SignedBlock {
                    data_proposals: vec![(LaneId::default(), vec![])],
                    certificate: ccp.certificate.clone(),
                    consensus_proposal: ccp.consensus_proposal.clone(),
                }))
                .unwrap();
        }

        // End of the first stream

        let mut client =
            DataAvailabilityClient::connect("client_id", config.da_public_address.clone())
                .await
                .unwrap();

        client
            .send(DataAvailabilityRequest::StreamFromHeight(BlockHeight(0)))
            .await
            .unwrap();

        let mut heights_received = vec![];
        while let Some(event) = client.recv().await {
            if let DataAvailabilityEvent::SignedBlock(block) = event {
                heights_received.push(block.height().0);
            }
            if heights_received.len() == 18 {
                break;
            }
        }

        for i in 0..18 {
            assert!(heights_received.contains(&i));
        }
    }

    #[test_log::test(tokio::test)]
    async fn test_da_many_clients_only_last_connected() {
        let port = find_available_port().await;
        let mut server = DataAvailabilityServer::start(port, "DaServer")
            .await
            .unwrap();

        let client_count = 5usize;
        let mut clients = Vec::with_capacity(client_count);
        let mut addr_by_idx = HashMap::new();

        for i in 0..client_count {
            let mut client =
                DataAvailabilityClient::connect(format!("client-{i}"), format!("0.0.0.0:{port}"))
                    .await
                    .unwrap();
            client
                .send(DataAvailabilityRequest::StreamFromHeight(BlockHeight(
                    i as u64,
                )))
                .await
                .unwrap();

            let event = tokio::time::timeout(Duration::from_secs(1), server.listen_next())
                .await
                .unwrap()
                .unwrap();

            match event {
                TcpEvent::Message {
                    socket_addr, data, ..
                } => {
                    assert_eq!(
                        data,
                        DataAvailabilityRequest::StreamFromHeight(BlockHeight(i as u64))
                    );
                    assert!(
                        server.connected(&socket_addr),
                        "Server should track connected client {}",
                        socket_addr
                    );
                    addr_by_idx.insert(i, socket_addr);
                }
                other => panic!("Expected Message event, got {other:?}"),
            }

            clients.push(client);
        }

        let last_idx = client_count - 1;
        let last_addr = addr_by_idx.get(&last_idx).unwrap().clone();

        for client in clients.drain(..last_idx) {
            let dropped_addr = client.socket_addr.to_string();
            drop(client);
            let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
            loop {
                if !server.connected(&dropped_addr) {
                    break;
                }
                if tokio::time::Instant::now() >= deadline {
                    panic!("Expected client {} to be dropped", dropped_addr);
                }
                if let Ok(Some(
                    TcpEvent::Closed { socket_addr } | TcpEvent::Error { socket_addr, .. },
                )) = tokio::time::timeout(Duration::from_millis(200), server.listen_next()).await
                {
                    if socket_addr == dropped_addr {
                        server.drop_peer_stream(socket_addr);
                    }
                }
            }
        }

        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        loop {
            if server.connected_clients().len() == 1 && server.connected(&last_addr) {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!(
                    "Expected only last client connected, got {:?}",
                    server.connected_clients()
                );
            }
            if let Ok(Some(
                TcpEvent::Closed { socket_addr } | TcpEvent::Error { socket_addr, .. },
            )) = tokio::time::timeout(Duration::from_millis(200), server.listen_next()).await
            {
                if socket_addr != last_addr {
                    server.drop_peer_stream(socket_addr);
                }
            }
        }
    }

    #[test_log::test(tokio::test)]
    async fn test_da_catchup() {
        let sender_global_bus = crate::bus::SharedMessageBus::new();
        let mut block_sender = TestBusClient::new_from_bus(sender_global_bus.new_handle()).await;
        let mut da_sender = DataAvailabilityTestCtx::new(sender_global_bus).await;
        let port = find_available_port().await;
        let mut server = DataAvailabilityServer::start(port, "DaServer")
            .await
            .unwrap();

        let receiver_global_bus = crate::bus::SharedMessageBus::new();
        let mut da_receiver = DataAvailabilityTestCtx::new(receiver_global_bus).await;
        da_receiver.da.catchupper.policy = Some(DaCatchupPolicy::Regular {
            floor: None,
            ceiling: None,
            backfill_enabled: false,
            backfill_start: None,
        });
        da_receiver.da.catchupper.da_max_frame_length = da_sender.da.config.da_max_frame_length;

        // Push some blocks to the sender
        let mut block = SignedBlock::default();
        let mut blocks = vec![];
        for i in 1..11 {
            blocks.push(block.clone());
            block.consensus_proposal.parent_hash = block.hashed();
            block.consensus_proposal.slot = i;
        }

        let block_ten = block.clone();

        blocks.reverse();

        for block in blocks {
            da_sender.handle_signed_block(block, &mut server).await;
        }

        let da_sender_address = da_sender.da.config.da_public_address.clone();

        tokio::spawn(async move {
            da_sender.da.start().await.unwrap();
        });

        // wait until it's up
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Setup done
        let mut rx = da_receiver
            .da
            .catchupper
            .take_receiver()
            .expect("catchup receiver should be available");
        da_receiver
            .da
            .catchupper
            .peers
            .push(da_sender_address.clone());

        _ = da_receiver.da.catchupper.ensure_started(BlockHeight(0));

        // Waiting a bit to push the block ten in the middle of all other 1..9 blocks
        tokio::time::sleep(Duration::from_millis(200)).await;
        _ = block_sender.send(MempoolBlockEvent::BuiltSignedBlock(block_ten.clone()));

        let mut received_blocks = vec![];
        while let Some(streamed_block) = rx.recv().await {
            da_receiver
                .handle_signed_block(streamed_block.clone(), &mut server)
                .await;
            let _ = da_receiver
                .da
                .catchupper
                .on_catchup_progress(streamed_block.height());
            received_blocks.push(streamed_block);
            if received_blocks.len() == 11 {
                break;
            }
        }
        assert_eq!(received_blocks.len(), 11);

        for i in 0..11 {
            assert!(received_blocks.iter().any(|b| b.height().0 == i));
        }

        // Add a few blocks (via bus to avoid mutex)
        let mut ccp = CommittedConsensusProposal {
            staking: Staking::default(),
            consensus_proposal: ConsensusProposal::default(),
            certificate: AggregateSignature::default(),
        };

        for i in 11..15 {
            ccp.consensus_proposal.parent_hash = ccp.consensus_proposal.hashed();
            ccp.consensus_proposal.slot = i;
            block_sender
                .send(MempoolBlockEvent::BuiltSignedBlock(SignedBlock {
                    data_proposals: vec![(LaneId::default(), vec![])],
                    certificate: ccp.certificate.clone(),
                    consensus_proposal: ccp.consensus_proposal.clone(),
                }))
                .unwrap();
        }

        // We should still be subscribed
        while let Some(streamed_block) = rx.recv().await {
            da_receiver
                .handle_signed_block(streamed_block.clone(), &mut server)
                .await;
            let _ = da_receiver
                .da
                .catchupper
                .on_catchup_progress(streamed_block.height());
            received_blocks.push(streamed_block);
            if received_blocks.len() == 15 {
                break;
            }
        }
        assert_eq!(received_blocks.len(), 15);
        assert_eq!(received_blocks[14].height(), BlockHeight(14));

        // Unsub
        // TODO: ideally via processing the correct message
        da_receiver.da.catchupper.stop_task();

        // Add a few blocks (via bus to avoid mutex)
        let mut ccp = CommittedConsensusProposal {
            staking: Staking::default(),
            consensus_proposal: ConsensusProposal::default(),
            certificate: AggregateSignature::default(),
        };

        for i in 15..20 {
            ccp.consensus_proposal.parent_hash = ccp.consensus_proposal.hashed();
            ccp.consensus_proposal.slot = i;
            block_sender
                .send(MempoolBlockEvent::BuiltSignedBlock(SignedBlock {
                    data_proposals: vec![(LaneId::default(), vec![])],
                    certificate: ccp.certificate.clone(),
                    consensus_proposal: ccp.consensus_proposal.clone(),
                }))
                .unwrap();
        }

        // Resubscribe - we should only receive the new ones.
        da_receiver
            .da
            .catchupper
            .ensure_started(BlockHeight(15))
            .expect("Error while asking for catchup blocks");

        let mut received_blocks = vec![];
        while let Some(block) = rx.recv().await {
            received_blocks.push(block);
            if received_blocks.len() == 5 {
                break;
            }
        }
        assert_eq!(received_blocks.len(), 5);
        assert_eq!(received_blocks[0].height(), BlockHeight(15));
        assert_eq!(received_blocks[4].height(), BlockHeight(19));
    }

    #[test_log::test(tokio::test)]
    async fn test_da_fast_catchup() {
        let sender_global_bus = crate::bus::SharedMessageBus::new();
        let mut block_sender = TestBusClient::new_from_bus(sender_global_bus.new_handle()).await;
        let mut da_sender = DataAvailabilityTestCtx::new(sender_global_bus).await;
        let port = find_available_port().await;
        let mut server = DataAvailabilityServer::start(port, "DaServer")
            .await
            .unwrap();

        let receiver_global_bus = crate::bus::SharedMessageBus::new();
        let mut da_receiver = DataAvailabilityTestCtx::new(receiver_global_bus).await;
        da_receiver.da.catchupper.policy = Some(DaCatchupPolicy::Regular {
            floor: Some(BlockHeight(8)),
            ceiling: None,
            backfill_enabled: true,
            backfill_start: None,
        });
        da_receiver.da.catchupper.da_max_frame_length = da_sender.da.config.da_max_frame_length;

        // Push some blocks to the sender
        let mut block = SignedBlock::default();
        let mut blocks = vec![];
        for i in 1..11 {
            blocks.push(block.clone());
            block.consensus_proposal.parent_hash = block.hashed();
            block.consensus_proposal.slot = i;
        }

        let block_ten = block.clone();

        blocks.reverse();

        for block in blocks {
            da_sender.handle_signed_block(block, &mut server).await;
        }

        let da_sender_address = da_sender.da.config.da_public_address.clone();

        tokio::spawn(async move {
            da_sender.da.start().await.unwrap();
        });

        // wait until it's up
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Setup done
        let mut rx = da_receiver
            .da
            .catchupper
            .take_receiver()
            .expect("catchup receiver should be available");
        da_receiver
            .da
            .catchupper
            .peers
            .push(da_sender_address.clone());

        // Initial fast catchup starts at floor and remains unbounded until mempool starts building.
        _ = da_receiver.da.catchupper.ensure_started(BlockHeight(0));
        _ = block_sender.send(MempoolBlockEvent::BuiltSignedBlock(block_ten.clone()));

        let mut received_blocks = vec![];
        while let Some(streamed_block) = rx.recv().await {
            da_receiver
                .handle_signed_block(streamed_block.clone(), &mut server)
                .await;
            let _ = da_receiver
                .da
                .catchupper
                .on_catchup_progress(streamed_block.height());
            received_blocks.push(streamed_block);
            if received_blocks.len() == 3 {
                break;
            }
        }

        assert_eq!(received_blocks.len(), 3);

        for i in 8..11 {
            assert!(received_blocks.iter().any(|b| b.height().0 == i));
        }

        // Mempool starts producing blocks; this sets regular mode ceiling and transitions to backfill.
        da_receiver
            .da
            .catchupper
            .on_first_hole_discovered(Some(BlockHeight(5)));
        _ = da_receiver
            .da
            .catchupper
            .on_mempool_started_building(BlockHeight(10));

        let mut received_blocks = vec![];
        while let Some(streamed_block) = rx.recv().await {
            da_receiver
                .handle_signed_block(streamed_block.clone(), &mut server)
                .await;
            let _ = da_receiver
                .da
                .catchupper
                .on_catchup_progress(streamed_block.height());
            received_blocks.push(streamed_block);
            if received_blocks.len() == 3 {
                break;
            }
        }

        assert_eq!(received_blocks.len(), 3);

        for i in 5..8 {
            assert!(received_blocks.iter().any(|b| b.height().0 == i));
        }
    }

    #[test_log::test(tokio::test)]
    async fn test_block_request_while_streaming() {
        // Create DA server with blocks 0-9 already stored
        let tmpdir = tempfile::tempdir().unwrap().keep();
        let mut blocks_storage = Blocks::new(&tmpdir).unwrap();

        let global_bus = crate::bus::SharedMessageBus::new();
        let bus = super::DABusClient::new_from_bus(global_bus.new_handle()).await;

        let mut config: Conf = Conf::new(vec![], None, None).unwrap();
        config.da_server_port = find_available_port().await;
        config.da_public_address = format!("127.0.0.1:{}", config.da_server_port);

        // Create and store blocks 0-9
        let mut block = SignedBlock::default();
        blocks_storage.put(block.clone()).unwrap();
        for i in 1..10 {
            block.consensus_proposal.parent_hash = block.hashed();
            block.consensus_proposal.slot = i;
            blocks_storage.put(block.clone()).unwrap();
        }

        let mut da = super::DataAvailability {
            config: config.clone().into(),
            bus,
            blocks: blocks_storage,
            buffered_signed_blocks: Default::default(),
            catchupper: Default::default(),
            allow_peer_catchup: false,
            peer_send_queues: HashMap::new(),
        };

        // Start DA server
        tokio::spawn(async move {
            da.start().await.unwrap();
        });

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let mut client =
            DataAvailabilityClient::connect("client_id", config.da_public_address.clone())
                .await
                .unwrap();

        // Start streaming from block 0
        client
            .send(DataAvailabilityRequest::StreamFromHeight(BlockHeight(0)))
            .await
            .unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Request specific block 7 while streaming
        client
            .send(DataAvailabilityRequest::BlockRequest(BlockHeight(7)))
            .await
            .unwrap();

        // Request a non-existent block to test BlockNotFound
        client
            .send(DataAvailabilityRequest::BlockRequest(BlockHeight(100)))
            .await
            .unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Collect responses (use a set to track unique blocks received)
        let mut received_block_heights = std::collections::HashSet::new();
        let mut received_block_7_from_request = false;
        let mut received_block_not_found = false;
        let mut event_count = 0;
        let start_time = tokio::time::Instant::now();

        while let Some(event) = client.recv().await {
            event_count += 1;
            match event {
                DataAvailabilityEvent::SignedBlock(block) => {
                    let height = block.height().0;
                    tracing::info!("Received block {} (event #{})", height, event_count);

                    // Track if block 7 arrives early (from request, not just stream)
                    if height == 7 && received_block_heights.len() < 5 {
                        received_block_7_from_request = true;
                    }

                    received_block_heights.insert(height);
                }
                DataAvailabilityEvent::BlockNotFound(height) => {
                    tracing::info!("Received BlockNotFound for height {}", height);
                    assert_eq!(height.0, 100, "Should be BlockNotFound for block 100");
                    received_block_not_found = true;
                }
                DataAvailabilityEvent::MempoolStatusEvent(_) => {}
            }

            // Stop after receiving enough events (at least 8 blocks and BlockNotFound)
            if received_block_heights.len() >= 8 && received_block_not_found {
                break;
            }

            // Safety timeout (2 seconds)
            if start_time.elapsed() > tokio::time::Duration::from_secs(2) {
                tracing::warn!("Test timeout after 2 seconds");
                break;
            }
        }

        // Verify results
        assert!(
            received_block_7_from_request,
            "Block 7 should have arrived early (from BlockRequest, not just stream)"
        );
        assert!(
            received_block_not_found,
            "Should have received BlockNotFound for block 100"
        );
        assert!(
            received_block_heights.len() >= 8,
            "Should have received at least 8 different blocks, got {}",
            received_block_heights.len()
        );

        // Verify we got essential blocks including block 7
        assert!(
            received_block_heights.contains(&0),
            "Should have received block 0"
        );
        assert!(
            received_block_heights.contains(&7),
            "Should have received block 7 (from request)"
        );

        tracing::info!("✅ Test passed: BlockRequest works while streaming");
        tracing::info!(
            "   - Received {} unique blocks",
            received_block_heights.len()
        );
        tracing::info!("   - Block 7 arrived early via BlockRequest (not just stream)");
        tracing::info!("   - Got BlockNotFound for non-existent block 100");
        tracing::info!("   - Blocks received: {:?}", {
            let mut v: Vec<_> = received_block_heights.iter().collect();
            v.sort();
            v
        });

        client.close().await.unwrap();
    }

    #[test_log::test(tokio::test)]
    async fn test_stream_rejected_when_start_height_missing() {
        let tmpdir = tempfile::tempdir().unwrap().keep();
        let mut blocks_storage = Blocks::new(&tmpdir).unwrap();

        let global_bus = crate::bus::SharedMessageBus::new();
        let bus = super::DABusClient::new_from_bus(global_bus.new_handle()).await;

        let mut config: Conf = Conf::new(vec![], None, None).unwrap();
        config.da_server_port = find_available_port().await;
        config.da_public_address = format!("127.0.0.1:{}", config.da_server_port);

        // Only store high blocks (10k+), leaving low heights unavailable.
        let mut block = SignedBlock::default();
        block.consensus_proposal.slot = 10_000;
        blocks_storage.put(block.clone()).unwrap();
        for i in 10_001..10_006 {
            block.consensus_proposal.parent_hash = block.hashed();
            block.consensus_proposal.slot = i;
            blocks_storage.put(block.clone()).unwrap();
        }

        let mut da = super::DataAvailability {
            config: config.clone().into(),
            bus,
            blocks: blocks_storage,
            buffered_signed_blocks: Default::default(),
            catchupper: Default::default(),
            allow_peer_catchup: false,
            peer_send_queues: HashMap::new(),
        };

        tokio::spawn(async move {
            da.start().await.unwrap();
        });

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let mut client =
            DataAvailabilityClient::connect("client_id", config.da_public_address.clone())
                .await
                .unwrap();

        client
            .send(DataAvailabilityRequest::StreamFromHeight(BlockHeight(10)))
            .await
            .unwrap();

        let first_event = tokio::time::timeout(Duration::from_secs(1), client.recv())
            .await
            .expect("Timed out waiting for stream rejection");
        if let Some(event) = first_event {
            assert_eq!(
                event,
                DataAvailabilityEvent::BlockNotFound(BlockHeight(10)),
                "Only BlockNotFound should be emitted before stream closes"
            );
            let second_event = tokio::time::timeout(Duration::from_secs(1), client.recv())
                .await
                .expect("Timed out waiting for stream closure");
            assert!(
                second_event.is_none(),
                "Stream should close after rejecting invalid start height"
            );
        }
    }

    #[test_log::test(tokio::test)]
    async fn test_stream_rejected_when_requested_range_has_gap() {
        let tmpdir = tempfile::tempdir().unwrap().keep();
        let mut blocks_storage = Blocks::new(&tmpdir).unwrap();

        let global_bus = crate::bus::SharedMessageBus::new();
        let bus = super::DABusClient::new_from_bus(global_bus.new_handle()).await;

        let mut config: Conf = Conf::new(vec![], None, None).unwrap();
        config.da_server_port = find_available_port().await;
        config.da_public_address = format!("127.0.0.1:{}", config.da_server_port);

        // Build sparse heights 0,1,3,4 so start height exists but range is not contiguous.
        let mut block = SignedBlock::default();
        block.consensus_proposal.slot = 0;
        blocks_storage.put(block.clone()).unwrap();
        block.consensus_proposal.parent_hash = block.hashed();
        block.consensus_proposal.slot = 1;
        blocks_storage.put(block.clone()).unwrap();
        block.consensus_proposal.parent_hash = block.hashed();
        block.consensus_proposal.slot = 3;
        blocks_storage.put(block.clone()).unwrap();
        block.consensus_proposal.parent_hash = block.hashed();
        block.consensus_proposal.slot = 4;
        blocks_storage.put(block.clone()).unwrap();

        let mut da = super::DataAvailability {
            config: config.clone().into(),
            bus,
            blocks: blocks_storage,
            buffered_signed_blocks: Default::default(),
            catchupper: Default::default(),
            allow_peer_catchup: false,
            peer_send_queues: HashMap::new(),
        };

        tokio::spawn(async move {
            da.start().await.unwrap();
        });

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let mut client =
            DataAvailabilityClient::connect("client_id", config.da_public_address.clone())
                .await
                .unwrap();

        client
            .send(DataAvailabilityRequest::StreamFromHeight(BlockHeight(0)))
            .await
            .unwrap();

        let first_event = tokio::time::timeout(Duration::from_secs(1), client.recv())
            .await
            .expect("Timed out waiting for stream rejection");
        if let Some(event) = first_event {
            assert_eq!(
                event,
                DataAvailabilityEvent::BlockNotFound(BlockHeight(2)),
                "Only BlockNotFound should be emitted before stream closes"
            );
            let second_event = tokio::time::timeout(Duration::from_secs(1), client.recv())
                .await
                .expect("Timed out waiting for stream closure");
            assert!(
                second_event.is_none(),
                "Stream should close after rejecting sparse range"
            );
        }
    }

    #[test_log::test(tokio::test)]
    async fn test_regular_mode_stops_only_when_reaching_ceiling() {
        let (tx, _rx) = tokio::sync::mpsc::channel::<SignedBlock>(1);
        let mut catchupper = super::DaCatchupper {
            policy: Some(DaCatchupPolicy::Regular {
                floor: Some(BlockHeight(180)),
                ceiling: Some(BlockHeight(75_001)),
                backfill_enabled: false,
                backfill_start: Some(BlockHeight(0)),
            }),
            task: Some(tokio::spawn(async {
                futures::future::pending::<anyhow::Result<()>>().await
            })),
            last_height: Some(BlockHeight(180)),
            sender: tx.clone(),
            receiver: None,
            peers: vec!["127.0.0.1:12345".to_string()],
            da_max_frame_length: 1024,
            restart_attempts: 0,
        };

        // Simulate stale external progress lower than floor.
        catchupper
            .on_catchup_progress(BlockHeight(180))
            .expect("on_catchup_progress should succeed");
        assert!(
            catchupper.task.is_some(),
            "catchup should keep running until it reaches ceiling"
        );

        // Explicit catchup progress to the ceiling should complete it.
        catchupper
            .on_catchup_progress(BlockHeight(75_001))
            .expect("on_catchup_progress should succeed");
        assert!(
            catchupper.task.is_none(),
            "catchup should stop once progress reaches ceiling"
        );
        assert!(
            catchupper.policy.is_none(),
            "regular mode should clear policy when done and no backfill is configured"
        );
    }

    #[test_log::test(tokio::test)]
    async fn test_started_building_only_sets_regular_ceiling() {
        let (tx, _rx) = tokio::sync::mpsc::channel::<SignedBlock>(1);
        let mut catchupper = super::DaCatchupper {
            policy: Some(DaCatchupPolicy::Regular {
                floor: Some(BlockHeight(180)),
                ceiling: None,
                backfill_enabled: true,
                backfill_start: None,
            }),
            task: Some(tokio::spawn(async {
                futures::future::pending::<anyhow::Result<()>>().await
            })),
            last_height: Some(BlockHeight(180)),
            sender: tx.clone(),
            receiver: None,
            peers: vec!["127.0.0.1:12345".to_string()],
            da_max_frame_length: 1024,
            restart_attempts: 0,
        };

        catchupper
            .on_mempool_started_building(BlockHeight(75_001))
            .expect("on_mempool_started_building should succeed");

        assert!(
            catchupper.task.is_some(),
            "started-building event should not complete regular catchup"
        );
        assert!(
            matches!(
                catchupper.policy,
                Some(DaCatchupPolicy::Regular {
                    ceiling: Some(BlockHeight(75_001)),
                    backfill_start: None,
                    ..
                })
            ),
            "started-building event should only bound regular mode"
        );
    }

    #[test_log::test(tokio::test)]
    async fn test_started_building_transitions_when_regular_range_is_empty_without_progress() {
        let (tx, _rx) = tokio::sync::mpsc::channel::<SignedBlock>(1);
        let mut catchupper = super::DaCatchupper {
            policy: Some(DaCatchupPolicy::Regular {
                floor: Some(BlockHeight(8)),
                ceiling: None,
                backfill_enabled: true,
                backfill_start: Some(BlockHeight(5)),
            }),
            task: None,
            last_height: None,
            sender: tx.clone(),
            receiver: None,
            peers: vec![],
            da_max_frame_length: 1024,
            restart_attempts: 0,
        };

        catchupper
            .on_mempool_started_building(BlockHeight(8))
            .expect("on_mempool_started_building should succeed");
        catchupper.on_tick().expect("on_tick should succeed");

        assert!(
            matches!(
                catchupper.policy,
                Some(DaCatchupPolicy::Backfill {
                    start: BlockHeight(5),
                    ceiling: BlockHeight(8)
                })
            ),
            "empty regular range should immediately transition to backfill mode"
        );
        assert!(
            catchupper.task.is_none(),
            "backfill task should not start without peers"
        );
    }

    #[test_log::test(tokio::test)]
    async fn test_started_building_completes_regular_when_progress_already_reached_ceiling() {
        let (tx, _rx) = tokio::sync::mpsc::channel::<SignedBlock>(1);
        let mut catchupper = super::DaCatchupper {
            policy: Some(DaCatchupPolicy::Regular {
                floor: Some(BlockHeight(8)),
                ceiling: None,
                backfill_enabled: true,
                backfill_start: Some(BlockHeight(5)),
            }),
            task: Some(tokio::spawn(async {
                futures::future::pending::<anyhow::Result<()>>().await
            })),
            last_height: Some(BlockHeight(10)),
            sender: tx.clone(),
            receiver: None,
            peers: vec![],
            da_max_frame_length: 1024,
            restart_attempts: 0,
        };

        catchupper
            .on_mempool_started_building(BlockHeight(10))
            .expect("on_mempool_started_building should succeed");

        assert!(
            catchupper.task.is_none(),
            "regular catchup should complete when known progress already reached new ceiling"
        );
        assert!(
            matches!(
                catchupper.policy,
                Some(DaCatchupPolicy::Backfill {
                    start: BlockHeight(5),
                    ceiling: BlockHeight(8)
                })
            ),
            "regular mode should transition to backfill after completion"
        );
    }

    #[test_log::test(tokio::test)]
    async fn test_late_first_hole_transitions_pending_to_backfill() {
        let (tx, _rx) = tokio::sync::mpsc::channel::<SignedBlock>(1);
        let mut catchupper = super::DaCatchupper {
            policy: Some(DaCatchupPolicy::Regular {
                floor: Some(BlockHeight(10)),
                ceiling: Some(BlockHeight(10)),
                backfill_enabled: true,
                backfill_start: None,
            }),
            task: Some(tokio::spawn(async {
                futures::future::pending::<anyhow::Result<()>>().await
            })),
            last_height: Some(BlockHeight(10)),
            sender: tx.clone(),
            receiver: None,
            peers: vec![],
            da_max_frame_length: 1024,
            restart_attempts: 0,
        };

        catchupper
            .on_catchup_progress(BlockHeight(10))
            .expect("regular mode completion should succeed");
        assert!(
            matches!(
                catchupper.policy,
                Some(DaCatchupPolicy::BackfillPending {
                    ceiling: BlockHeight(10)
                })
            ),
            "regular completion without known hole should wait for hole discovery"
        );

        catchupper.on_first_hole_discovered(Some(BlockHeight(5)));
        assert!(
            matches!(
                catchupper.policy,
                Some(DaCatchupPolicy::Backfill {
                    start: BlockHeight(5),
                    ceiling: BlockHeight(10)
                })
            ),
            "late hole discovery should transition pending state into backfill mode"
        );
    }

    #[test_log::test(tokio::test)]
    async fn test_on_tick_restarts_finished_task() {
        let (tx, _rx) = tokio::sync::mpsc::channel::<SignedBlock>(1);
        let mut catchupper = super::DaCatchupper {
            policy: Some(DaCatchupPolicy::Regular {
                floor: Some(BlockHeight(180)),
                ceiling: Some(BlockHeight(75_001)),
                backfill_enabled: false,
                backfill_start: Some(BlockHeight(0)),
            }),
            task: Some(tokio::spawn(async { Ok(()) })),
            last_height: Some(BlockHeight(180)),
            sender: tx.clone(),
            receiver: None,
            peers: vec!["127.0.0.1:12345".to_string()],
            da_max_frame_length: 1024,
            restart_attempts: 0,
        };

        tokio::time::sleep(Duration::from_millis(5)).await;
        catchupper.on_tick().expect("on_tick should succeed");
        let restart_height = catchupper
            .last_height
            .expect("catchup should have restarted");
        assert_eq!(
            restart_height,
            BlockHeight(180),
            "catchup should restart from the last known catchup height"
        );
    }

    #[test_log::test(tokio::test)]
    async fn test_on_tick_keeps_backfill_mode_when_task_not_started() {
        let (tx, _rx) = tokio::sync::mpsc::channel::<SignedBlock>(1);
        let mut catchupper = super::DaCatchupper {
            policy: Some(DaCatchupPolicy::Backfill {
                start: BlockHeight(180),
                ceiling: BlockHeight(75_001),
            }),
            task: None,
            last_height: None,
            sender: tx.clone(),
            receiver: None,
            peers: vec![],
            da_max_frame_length: 1024,
            restart_attempts: 0,
        };

        catchupper.on_tick().expect("on_tick should succeed");
        assert!(
            matches!(catchupper.policy, Some(DaCatchupPolicy::Backfill { .. })),
            "backfill mode should remain active until a task can start"
        );
        assert!(
            catchupper.task.is_none(),
            "no task should start when no peers are available"
        );
    }

    #[test_log::test(tokio::test)]
    async fn test_stream_accepts_when_peer_is_already_up_to_date() {
        let tmpdir = tempfile::tempdir().unwrap().keep();
        let mut blocks_storage = Blocks::new(&tmpdir).unwrap();

        let global_bus = crate::bus::SharedMessageBus::new();
        let bus = super::DABusClient::new_from_bus(global_bus.new_handle()).await;

        let mut config: Conf = Conf::new(vec![], None, None).unwrap();
        config.da_server_port = find_available_port().await;
        config.da_public_address = format!("127.0.0.1:{}", config.da_server_port);

        let mut block = SignedBlock::default();
        blocks_storage.put(block.clone()).unwrap();
        for i in 1..5 {
            block.consensus_proposal.parent_hash = block.hashed();
            block.consensus_proposal.slot = i;
            blocks_storage.put(block.clone()).unwrap();
        }

        let mut da = super::DataAvailability {
            config: config.clone().into(),
            bus,
            blocks: blocks_storage,
            buffered_signed_blocks: Default::default(),
            catchupper: Default::default(),
            allow_peer_catchup: false,
            peer_send_queues: HashMap::new(),
        };

        tokio::spawn(async move {
            da.start().await.unwrap();
        });

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let mut client =
            DataAvailabilityClient::connect("client_id", config.da_public_address.clone())
                .await
                .unwrap();

        // Request starts at the next block after current highest (peer is already up to date).
        client
            .send(DataAvailabilityRequest::StreamFromHeight(BlockHeight(5)))
            .await
            .unwrap();

        // Should not be rejected with BlockNotFound.
        let next = tokio::time::timeout(Duration::from_millis(300), client.recv()).await;
        assert!(
            next.is_err(),
            "Did not expect an immediate stream event/rejection for up-to-date peer"
        );
    }
}

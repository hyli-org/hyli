//! Minimal block storage layer for data availability.

// Pick one of the two implementations
use hyle_modules::modules::data_availability::blocks_fjall::Blocks;
//use hyle_modules::modules::data_availability::blocks_memory::Blocks;

use hyle_modules::node_state::module::NodeStateModule;
use hyle_modules::node_state::NodeStateStore;
use hyle_modules::{bus::SharedMessageBus, modules::Module};
use hyle_modules::{
    log_error, module_bus_client, module_handle_messages,
    utils::da_codec::{
        DataAvailabilityClient, DataAvailabilityEvent, DataAvailabilityRequest,
        DataAvailabilityServer,
    },
};
use hyle_net::tcp::TcpEvent;
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
use rand::seq::SliceRandom;
use std::{collections::BTreeSet, time::Duration};
use tokio::{
    task::JoinSet,
    time::{sleep_until, Instant},
};
use tracing::{debug, info, trace, warn};

use crate::model::SharedRunContext;

impl Module for DataAvailability {
    type Context = SharedRunContext;

    async fn build(bus: SharedMessageBus, ctx: Self::Context) -> anyhow::Result<Self> {
        let bus = DABusClient::new_from_bus(bus.new_handle()).await;

        // When fast catchup is enabled, we load the node state from disk to load blocks

        let catchup_policy = if ctx.config.consensus.solo {
            DaCatchupPolicy::NoCatchup
        } else if ctx.config.run_fast_catchup {
            let floor_height = NodeStateModule::load_from_disk::<NodeStateStore>(
                &ctx.config.data_directory.join("node_state.bin"),
            )
            .map(|ns| ns.current_height + 1);

            DaCatchupPolicy::Fast {
                floor: floor_height.unwrap_or(BlockHeight(0)),
                backfill: ctx.config.fast_catchup_backfill,
            }
        } else {
            DaCatchupPolicy::Regular
        };

        let blocks = Blocks::new(&ctx.config.data_directory.join("data_availability.db"))?;

        let highest_height = blocks.highest();

        info!(
            "📦  DataAvailability module started with policy {:?} and highest height {}",
            catchup_policy, highest_height
        );

        Ok(DataAvailability {
            config: ctx.config.clone(),
            bus,
            blocks,
            last_processed_height: highest_height,
            buffered_signed_blocks: BTreeSet::new(),
            catchupper: DaCatchupper::new(catchup_policy, ctx.config.da_max_frame_length),
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

type DaTcpServer =
    hyle_net::tcp::tcp_server::TcpServer<DataAvailabilityRequest, DataAvailabilityEvent>;

#[derive(Debug)]
pub struct DataAvailability {
    config: SharedConf,
    bus: DABusClient,
    pub blocks: Blocks,
    pub last_processed_height: BlockHeight,

    buffered_signed_blocks: BTreeSet<SignedBlock>,

    catchupper: DaCatchupper,
}

/// Catchup configuration for the Data Availability module.
#[derive(Default, Debug)]
enum DaCatchupPolicy {
    NoCatchup,
    #[default]
    Regular,
    Fast {
        floor: BlockHeight,
        backfill: bool,
    },
}

/// Represents the state of the catchup task.
/// A fast catchup from `from` height is used to quickly catch up to the latest block
#[derive(Debug, Default)]
struct DaCatchupper {
    policy: DaCatchupPolicy,
    status: DaCatchupStatus,
    pub peers: Vec<String>,
    pub backfills: Vec<[BlockHeight; 2]>,
    pub stop_height: Option<BlockHeight>,
    da_max_frame_length: usize,
}

impl DaCatchupper {
    pub fn new(policy: DaCatchupPolicy, da_max_frame_length: usize) -> Self {
        DaCatchupper {
            policy,
            status: DaCatchupStatus::Idle,
            peers: vec![],
            backfills: vec![],
            da_max_frame_length,
            stop_height: None,
        }
    }

    pub fn is_fast_catchup_initial_block(&self, height: &BlockHeight) -> bool {
        matches!(
            self.policy,
            DaCatchupPolicy::Fast { floor, .. } if height == &floor
        )
    }

    pub fn need_to_tick(&self) -> bool {
        matches!(
            self.status,
            DaCatchupStatus::Catchup { .. } | DaCatchupStatus::Backfill { .. }
        ) || !self.backfills.is_empty()
    }

    pub fn stop_task(&mut self) {
        if let DaCatchupStatus::Catchup { task, .. } | DaCatchupStatus::Backfill { task, .. } =
            &mut self.status
        {
            task.abort();
            self.status = DaCatchupStatus::Idle;
        }
    }

    /// Check if the catchup is done based on the highest processed height compared to the stop height.
    pub fn catchup_done(&self, highest_processed_height: BlockHeight) -> bool {
        if let Some(stop_height) = self.stop_height {
            return highest_processed_height >= stop_height;
        }
        false
    }

    pub fn choose_random_peer(&self) -> Option<String> {
        self.peers.choose(&mut rand::thread_rng()).cloned()
    }

    /// Start a new catchup task based on the current policy, if the catchup is idle.
    pub async fn init_catchup(
        &mut self,
        last_processed_height: BlockHeight,
        sender: &tokio::sync::mpsc::Sender<SignedBlock>,
    ) -> anyhow::Result<()> {
        if !matches!(self.status, DaCatchupStatus::Idle) {
            debug!("Catchup is already done, no need to start a new task");
            return Ok(());
        }

        if self.catchup_done(last_processed_height) {
            info!("Catchup is already done, no need to start a new task");
            return Ok(());
        }

        if self.peers.is_empty() {
            warn!("No peers available for catchup, cannot proceed");
            return Ok(());
        }

        let mut last_processed_height = last_processed_height;

        if let DaCatchupPolicy::Fast { floor, backfill } = &self.policy {
            self.backfills.push([last_processed_height, *floor]);
            last_processed_height = *floor;
        }

        self.status = DaCatchupStatus::Catchup {
            task: self
                .start_task(last_processed_height, None, sender.clone())
                .await
                .context("Starting catchup task")?,
            last_processed_height,
        };

        Ok(())
    }

    /// Check if task is finished and catchup is still needed,
    /// or start a new one if the catchup is idle.
    async fn catchup_task_needs_restart(
        &mut self,
        last_processed_height: BlockHeight,
        task: &JoinHandle<()>,
    ) -> Option<bool> {
        if self.catchup_done(last_processed_height) {
            info!(
                "Catchup task finished, last processed height {}",
                last_processed_height
            );

            task.abort();

            Some(false)
        } else if task.is_finished() {
            info!(
                "Catchup task finished, but catchup is not done yet, restarting from height {}",
                last_processed_height
            );

            Some(true)
        } else {
            debug!(
                "Catchup task is still running, last processed height {}",
                last_processed_height
            );
            None
        }
    }

    /// Try transition the catchup state based on the current status and policy.
    pub async fn try_transition(
        &mut self,
        last_processed_height: BlockHeight,
        sender: &tokio::sync::mpsc::Sender<SignedBlock>,
    ) -> anyhow::Result<()> {
        if matches!(self.status, DaCatchupStatus::Idle) && self.backfills.is_empty() {
            trace!("Catchup is already done");
            return Ok(());
        }

        if self.peers.is_empty() {
            warn!("No peers available for catchup, cannot proceed");
            return Ok(());
        }

        let new_status = std::mem::take(&mut self.status);

        self.status = match new_status {
            DaCatchupStatus::Idle => {
                let [lo, hi] = self.backfills.remove(0);

                info!("Starting backfill from height {} to {}", lo, hi);

                self.stop_height = Some(hi);

                DaCatchupStatus::Backfill {
                    interval: [lo, hi],
                    task: self
                        .start_task(lo, Some(hi), sender.clone())
                        .await
                        .context("Starting backfill task")?,
                }
            }
            DaCatchupStatus::Catchup {
                task,
                last_processed_height: old_last_processed_height,
            } => {
                match self
                    .catchup_task_needs_restart(last_processed_height, &task)
                    .await
                {
                    Some(false) => DaCatchupStatus::Idle,
                    Some(true) => {
                        let max_height =
                            std::cmp::max(old_last_processed_height, last_processed_height);
                        DaCatchupStatus::Catchup {
                            task: self
                                .start_task(max_height, None, sender.clone())
                                .await
                                .context("Restarting catchup task")?,
                            last_processed_height: max_height,
                        }
                    }
                    None => DaCatchupStatus::Catchup {
                        task,
                        last_processed_height: old_last_processed_height,
                    },
                }
            }
            DaCatchupStatus::Backfill {
                interval: [lo, hi],
                task,
            } => {
                match self
                    .catchup_task_needs_restart(last_processed_height, &task)
                    .await
                {
                    Some(false) => DaCatchupStatus::Idle,
                    Some(true) => {
                        let new_lo = if last_processed_height >= lo && last_processed_height < hi {
                            last_processed_height
                        } else {
                            lo
                        };

                        DaCatchupStatus::Backfill {
                            interval: [new_lo, hi],
                            task: self
                                .start_task(new_lo, Some(hi), sender.clone())
                                .await
                                .context("Restarting backfill task")?,
                        }
                    }
                    None => DaCatchupStatus::Backfill {
                        interval: [lo, hi],
                        task,
                    },
                }
            }
        };

        Ok(())
    }

    async fn start_task(
        &mut self,
        start_height: BlockHeight,
        auto_stop_height: Option<BlockHeight>,
        sender: tokio::sync::mpsc::Sender<SignedBlock>,
    ) -> anyhow::Result<JoinHandle<()>> {
        let peer = self
            .choose_random_peer()
            .context("No peers available for catchup")?;

        info!(
            "Starting catchup from height {} on peer {}",
            start_height, peer
        );

        let mut client = DataAvailabilityClient::connect_with_opts(
            "catchupper".to_string(),
            Some(self.da_max_frame_length),
            peer,
        )
        .await
        .context("Error occurred setting up the DA listener")?;

        client.send(DataAvailabilityRequest(start_height)).await?;

        Ok(tokio::spawn(async move {
            let timeout_duration = std::env::var("HYLE_DA_SLEEP_TIMEOUT")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .map(Duration::from_secs)
                .unwrap_or_else(|| Duration::from_secs(10));
            let mut deadline = Instant::now() + timeout_duration;

            loop {
                let sleep = sleep_until(deadline);
                tokio::pin!(sleep);

                tokio::select! {
                    _ = &mut sleep => {
                        warn!("Timeout expired while waiting for block.");
                        break;
                    }
                    received = client.recv() => {
                        match received {
                            None => {
                                break;
                            }
                            Some(DataAvailabilityEvent::SignedBlock(block)) => {
                                info!(
                                    "📦 Received block (height {}) from stream",
                                    block.consensus_proposal.slot
                                );

                                if let Some(auto_stop_height) = auto_stop_height{
                                    if block.height() + 1 >= auto_stop_height {
                                        info!("Stopping catchup task early at height #{} 0x{}", block.height(), block.hashed());
                                        // wait forever until the task is stopped, with a pending future
                                        drop(client);
                                        tokio::time::sleep(Duration::from_secs(1000)).await;
                                        break;
                                    }
                                }

                                if let Err(e) = sender.send(block).await {
                                    tracing::error!("Error while sending block over channel: {:#}", e);
                                    break;
                                }

                                // Reset the timeout ONLY when a block is received
                                deadline = Instant::now() + timeout_duration;
                            }
                            Some(_) => {
                                tracing::trace!("Dropped received message in catchup task");
                            }
                        }
                    }
                }
            }
        }))
    }

    pub fn is_backfill_block(&self, height: &BlockHeight) -> bool {
        matches!(
           self.status,
            DaCatchupStatus::Backfill { interval: [lo, hi], .. } if height < &hi && height >= &lo
        )
    }
}

#[derive(Default, Debug)]
enum DaCatchupStatus {
    Backfill {
        interval: [BlockHeight; 2],
        task: tokio::task::JoinHandle<()>,
    },
    Catchup {
        last_processed_height: BlockHeight,
        task: tokio::task::JoinHandle<()>,
    },
    #[default]
    Idle,
}

impl DataAvailability {
    pub async fn start(&mut self) -> Result<()> {
        info!(
            "📡  Starting DataAvailability module, listening for stream requests on port {}",
            self.config.da_server_port
        );

        let mut server: DaTcpServer = DataAvailabilityServer::start_with_opts(
            self.config.da_server_port,
            Some(self.config.da_max_frame_length),
            format!("DAServer-{}", self.config.id.clone()).as_str(),
        )
        .await?;

        let (catchup_block_sender, mut catchup_block_receiver) =
            tokio::sync::mpsc::channel::<SignedBlock>(100);

        // Used to send blocks to clients (indexers/peers)
        // // This is a JoinSet of tuples containing:
        // // - A vector of block hashes to send
        // // - The peer IP address to send the blocks to
        // // - The number of retries for sending the blocks
        let mut catchup_joinset: JoinSet<(Vec<ConsensusProposalHash>, String, usize)> =
            tokio::task::JoinSet::new();
        let mut catchup_task_checker_ticker =
            tokio::time::interval(std::time::Duration::from_millis(5000));

        module_handle_messages! {
            on_self self,
            listen<MempoolBlockEvent> evt => {
                _ = log_error!(self.handle_mempool_event(evt, &mut server, &catchup_block_sender).await, "Handling Mempool Event");
            }

            listen<MempoolStatusEvent> evt => {
                self.handle_mempool_status_event(evt, &mut server).await;
            }

            listen<GenesisEvent> cmd => {
                if let GenesisEvent::GenesisBlock(signed_block) = cmd {
                    debug!("🌱  Genesis block received with validators {:?}", signed_block.consensus_proposal.staking_actions.clone());
                    if let Ok(height) = log_error!(self.handle_signed_block(signed_block, &mut server).await.context("Handling Genesis block"),  "Handling GenesisBlock Event") {
                        _ = log_error!(self.catchupper.try_transition(height, &catchup_block_sender).await, "Transitioning catchup state after genesis block");
                    }
                }
                else {
                    self.catchupper.init_catchup(
                        self.last_processed_height,
                        &catchup_block_sender,
                    ).await?;
                }
            }

            listen<PeerEvent> PeerEvent::NewPeer { da_address, .. } => {
                self.catchupper.peers.push(da_address.clone());
                self.catchupper.init_catchup(
                    self.last_processed_height,
                    &catchup_block_sender,
                ).await?;
            }

            _ = catchup_task_checker_ticker.tick()/*, if self.catchupper.need_to_tick()*/ => {
                _ = log_error!(self.catchupper.try_transition(self.last_processed_height, &catchup_block_sender).await, "Catchup transition after tick");
            }

            Some(streamed_block) = catchup_block_receiver.recv() => {
                if let Some(height) = self.handle_signed_block(streamed_block, &mut server).await {
                    _ = log_error!(self.catchupper.try_transition(height, &catchup_block_sender).await, "Catchup transition after streamed block");
                }
            }

            Some(tcp_event) = server.listen_next() => {
                if let TcpEvent::Message { dest, data } = tcp_event {
                    _ = log_error!(self.start_streaming_to_peer(data.0, &mut catchup_joinset, &dest).await, "Starting streaming to peer");
                }
            }

            // Send one block to a peer as part of "catchup",
            // once we have sent all blocks the peer is presumably synchronised.
            Some(Ok((block_hashes, peer_ip, retries))) = catchup_joinset.join_next() => {

                #[cfg(test)]
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;

                _ = log_error!(
                    self.handle_send_next_block_to_peer(
                        block_hashes,
                        peer_ip.clone(),
                        retries,
                        &mut catchup_joinset,
                        &mut server
                    ).await,
                    "Send next block to peer"
                );
            }
        };

        Ok(())
    }

    async fn handle_send_next_block_to_peer(
        &mut self,
        mut block_hashes: Vec<ConsensusProposalHash>,
        peer_ip: String,
        retries: usize,
        catchup_joinset: &mut JoinSet<(Vec<ConsensusProposalHash>, String, usize)>,
        server: &mut DaTcpServer,
    ) -> Result<()> {
        if let Some(hash) = block_hashes.pop() {
            debug!("📡  Sending block {} to peer {}", &hash, &peer_ip);
            if let Ok(Some(signed_block)) = self.blocks.get(&hash) {
                // Errors will be handled when sending new blocks, ignore here.
                if server
                    .try_send(
                        peer_ip.clone(),
                        DataAvailabilityEvent::SignedBlock(signed_block),
                    )
                    .is_ok()
                {
                    catchup_joinset.spawn(async move { (block_hashes, peer_ip, 0) });
                } else if retries > 10 {
                    warn!("Failed to send block {} to peer {}", &hash, &peer_ip);
                    server.drop_peer_stream(peer_ip);
                } else {
                    // Retry sending the block
                    block_hashes.push(hash);
                    catchup_joinset.spawn(async move {
                        tokio::time::sleep(Duration::from_millis(100 * (retries as u64))).await;
                        (block_hashes, peer_ip, retries + 1)
                    });
                }
            }
        }
        Ok(())
    }

    async fn handle_mempool_event(
        &mut self,
        evt: MempoolBlockEvent,
        tcp_server: &mut DaTcpServer,
        sender: &tokio::sync::mpsc::Sender<SignedBlock>,
    ) -> Result<()> {
        match evt {
            MempoolBlockEvent::BuiltSignedBlock(signed_block) => {
                debug!(
                    "📦  Received built block (height {}) from Mempool",
                    signed_block.height()
                );
                if let Some(height) = self.handle_signed_block(signed_block, tcp_server).await {
                    self.catchupper.try_transition(height, sender).await?;
                }
            }
            MempoolBlockEvent::StartedBuildingBlocks(height) => {
                debug!(
                    "Received started building block (at height {}) from Mempool",
                    height
                );
                self.catchupper.stop_height = Some(height);
            }
        }

        Ok(())
    }

    async fn handle_mempool_status_event(
        &mut self,
        evt: MempoolStatusEvent,
        tcp_server: &mut DaTcpServer,
    ) {
        let errors = tcp_server
            .broadcast(DataAvailabilityEvent::MempoolStatusEvent(evt))
            .await;

        for (peer, error) in errors {
            warn!("Error while broadcasting mempool status event {:#}", error);
            tcp_server.drop_peer_stream(peer.clone());
        }
    }

    /// if handled, returns the highest height of the processed blocks
    async fn handle_signed_block(
        &mut self,
        block: SignedBlock,
        tcp_server: &mut DaTcpServer,
    ) -> Option<BlockHeight> {
        let hash = block.hashed();
        // if new block is already handled, ignore it
        if self.blocks.contains(&hash) {
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
        else if !self.blocks.contains(block.parent_hash()) {
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

        if self.catchupper.is_backfill_block(&block.height()) {
            // If we are in fast catchup, we need to backfill the block
            _ = log_error!(
                self.backfill_block(block.clone()).await,
                "Backfilling block"
            );
        } else {
            // store block
            _ = log_error!(
                self.add_processed_block(block.clone(), tcp_server).await,
                "Adding processed block"
            );
        }
        let highest_processed_height = self.pop_buffer(hash, tcp_server).await;
        _ = log_error!(self.blocks.persist(), "Persisting blocks");

        let height = block.height();

        self.last_processed_height = highest_processed_height.unwrap_or(height);

        Some(self.last_processed_height)
    }

    /// Returns the highest height of the processed blocks
    async fn pop_buffer(
        &mut self,
        mut last_block_hash: ConsensusProposalHash,
        tcp_server: &mut DaTcpServer,
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
                .add_processed_block(first_buffered.clone(), tcp_server)
                .await
                .is_ok()
            {
                res = res.map_or(Some(height), |r: BlockHeight| Some(r.max(height)))
            }
        }

        res
    }

    async fn add_processed_block(
        &mut self,
        block: SignedBlock,
        tcp_server: &mut DaTcpServer,
    ) -> anyhow::Result<()> {
        // TODO: if we don't have streaming peers, we could just pass the block here
        // and avoid a clone + drop cost (which can be substantial for large blocks).
        self.blocks
            .put(block.clone())
            .context(format!("Storing block {}", block.height()))?;

        trace!("Block {} {}: {:#?}", block.height(), block.hashed(), block);

        if block.height().0 % 10 == 0 || block.has_txs() {
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

        // TODO: use retain once async closures are supported ?
        //
        let errors = tcp_server
            .broadcast(DataAvailabilityEvent::SignedBlock(block.clone()))
            .await;

        for (peer, error) in errors {
            warn!(
                "Error while broadcasting block {}: {:#}",
                block.hashed(),
                error
            );
            tcp_server.drop_peer_stream(peer.clone());
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

    /// Analog to add_processed_block, but without broadcasting the block to peers and NodeState.
    async fn backfill_block(&mut self, block: SignedBlock) -> anyhow::Result<()> {
        let height = block.height();

        trace!("Block {} {}: {:#?}", height, block.hashed(), &block);

        if block.height().0 % 10 == 0 || block.has_txs() {
            info!(
                "Backfill block #{} 0x{} with {} txs",
                block.height(),
                block.hashed(),
                block.count_txs(),
            );
        }
        debug!(
            "Backfill block #{} 0x{} with {} transactions: {}",
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

        self.blocks
            .put(block)
            .context(format!("Storing block {}", height))?;

        Ok(())
    }

    async fn start_streaming_to_peer(
        &mut self,
        start_height: BlockHeight,
        catchup_joinset: &mut JoinSet<(Vec<ConsensusProposalHash>, String, usize)>,
        peer_ip: &str,
    ) -> Result<()> {
        // Finally, stream past blocks as required.
        // We'll create a copy of the range so we don't stream everything.
        // We will safely stream everything as any new block will be sent
        // because we registered in the struct beforehand.
        // Like pings, this just sends a message processed in the main select! loop.
        let mut processed_block_hashes: Vec<_> = self
            .blocks
            .range(
                start_height,
                self.blocks
                    .last()
                    .map_or(start_height, |block| block.height())
                    + 1,
            )
            .filter_map(|item| item.ok())
            .collect();
        processed_block_hashes.reverse();

        let peer_ip = peer_ip.to_string();

        catchup_joinset.spawn(async move { (processed_block_hashes, peer_ip, 0) });

        Ok(())
    }
}

#[cfg(test)]
pub mod tests {
    #![allow(clippy::indexing_slicing)]
    use std::time::Duration;

    use super::Blocks;
    use super::{module_bus_client, DaTcpServer};
    use crate::node_state::NodeState;
    use crate::{
        bus::BusClientSender,
        consensus::CommittedConsensusProposal,
        model::*,
        node_state::module::{NodeStateBusClient, NodeStateEvent},
        utils::{conf::Conf, integration_test::find_available_port},
    };
    use anyhow::Result;
    use hyle_modules::log_error;
    use hyle_modules::utils::da_codec::{
        DataAvailabilityClient, DataAvailabilityEvent, DataAvailabilityRequest,
        DataAvailabilityServer,
    };
    use staking::state::Staking;

    /// For use in integration tests
    pub struct DataAvailabilityTestCtx {
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

            let node_state = NodeState::create(config.id.clone(), "data_availability");

            config.da_server_port = find_available_port().await;
            config.da_public_address = format!("127.0.0.1:{}", config.da_server_port);
            let da = super::DataAvailability {
                config: config.into(),
                bus,
                blocks,
                buffered_signed_blocks: Default::default(),
                last_processed_height: BlockHeight(0),
                catchupper: Default::default(),
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
            tcp_server: &mut DaTcpServer,
        ) {
            self.da.handle_signed_block(block.clone(), tcp_server).await;
            // TODO: we use this in autobahn_testing, but it'd be cleaner to separate it.
            let Ok(full_block) = self.node_state.handle_signed_block(&block) else {
                tracing::warn!("Error while handling signed block {}", block.hashed());
                return;
            };
            _ = log_error!(
                self.node_state_bus
                    .send_waiting_if_full(NodeStateEvent::NewBlock(Box::new(full_block)))
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

        let mut server = DataAvailabilityServer::start(7898, "DaServer")
            .await
            .unwrap();

        let bus = super::DABusClient::new_from_bus(crate::bus::SharedMessageBus::new(
            crate::bus::metrics::BusMetrics::global("global".to_string()),
        ))
        .await;
        let mut da = super::DataAvailability {
            config: Default::default(),
            bus,
            blocks,
            buffered_signed_blocks: Default::default(),
            last_processed_height: BlockHeight(0),
            catchupper: Default::default(),
        };
        let mut block = SignedBlock::default();
        let mut blocks = vec![];
        for i in 1..10000 {
            blocks.push(block.clone());
            block.consensus_proposal.parent_hash = block.hashed();
            block.consensus_proposal.slot = i;
        }
        blocks.reverse();
        for block in blocks {
            if block.height().0 == 0 {
                assert_eq!(
                    da.handle_signed_block(block, &mut server).await,
                    Some(BlockHeight(9998))
                );
            } else {
                assert_eq!(da.handle_signed_block(block, &mut server).await, None);
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

        let global_bus = crate::bus::SharedMessageBus::new(
            crate::bus::metrics::BusMetrics::global("global".to_string()),
        );
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
            last_processed_height: BlockHeight(0),
            catchupper: Default::default(),
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
            .send(DataAvailabilityRequest(BlockHeight(0)))
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
            .send(DataAvailabilityRequest(BlockHeight(0)))
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
    async fn test_da_catchup() {
        let sender_global_bus = crate::bus::SharedMessageBus::new(
            crate::bus::metrics::BusMetrics::global("global".to_string()),
        );
        let mut block_sender = TestBusClient::new_from_bus(sender_global_bus.new_handle()).await;
        let mut da_sender = DataAvailabilityTestCtx::new(sender_global_bus).await;
        let mut server = DataAvailabilityServer::start(7890, "DaServer")
            .await
            .unwrap();

        let receiver_global_bus = crate::bus::SharedMessageBus::new(
            crate::bus::metrics::BusMetrics::global("global".to_string()),
        );
        let mut da_receiver = DataAvailabilityTestCtx::new(receiver_global_bus).await;

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
        let (tx, mut rx) = tokio::sync::mpsc::channel(200);
        da_receiver
            .da
            .catchupper
            .peers
            .push(da_sender_address.clone());
        _ = da_receiver
            .da
            .catchupper
            .init_catchup(BlockHeight(0), &tx)
            .await;

        // Waiting a bit to push the block ten in the middle of all other 1..9 blocks
        tokio::time::sleep(Duration::from_millis(200)).await;
        _ = block_sender.send(MempoolBlockEvent::BuiltSignedBlock(block_ten.clone()));

        let mut received_blocks = vec![];
        while let Some(streamed_block) = rx.recv().await {
            da_receiver
                .handle_signed_block(streamed_block.clone(), &mut server)
                .await;
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
            .init_catchup(BlockHeight(0), &tx)
            .await
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
}

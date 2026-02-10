//! Minimal block storage layer for data availability.

// Pick one of the two implementations
use hyli_modules::modules::data_availability::blocks_fjall::Blocks;
use hyli_modules::utils::da_codec::DataAvailabilityServer;
//use hyli_modules::modules::data_availability::blocks_memory::Blocks;
use hyli_modules::modules::da_listener::{DaStreamPoll, SignedDaStream};
use hyli_modules::telemetry::{global_meter_or_panic, Counter, Gauge, KeyValue};
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
    utils::{conf::SharedConf, rng::deterministic_rng},
};
use anyhow::{Context, Result};
use core::str;
use rand::seq::IndexedRandom;
use std::{
    collections::{BTreeSet, HashMap, VecDeque},
    time::Duration,
};
use tokio::task::JoinSet;
use tracing::{debug, error, info, trace, warn};

use crate::model::SharedRunContext;

impl Module for DataAvailability {
    type Context = SharedRunContext;

    async fn build(bus: SharedMessageBus, ctx: Self::Context) -> anyhow::Result<Self> {
        let bus = DABusClient::new_from_bus(bus.new_handle()).await;

        let mut blocks = Blocks::new(&ctx.config.data_directory.join("data_availability.db"))?;
        blocks.set_metrics_context(ctx.config.id.clone());
        let highest_block = blocks.highest();

        // When fast catchup is enabled, we load the node state from disk to load blocks

        let catchup_policy = if ctx.config.consensus.solo {
            None
        } else {
            Some(DaCatchupPolicy {
                floor: if ctx.config.run_fast_catchup {
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
                },
                backfill: ctx.config.fast_catchup_backfill,
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
            buffered_signed_blocks: BTreeSet::new(),
            catchupper: DaCatchupper::new(catchup_policy, ctx.config.da_max_frame_length),
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

    buffered_signed_blocks: BTreeSet<SignedBlock>,

    catchupper: DaCatchupper,

    // Track blocks to send to each streaming peer (ensures ordering)
    peer_send_queues: HashMap<String, VecDeque<ConsensusProposalHash>>,
}

/// Catchup configuration for the Data Availability module.
#[derive(Default, Debug, Clone)]
struct DaCatchupPolicy {
    floor: Option<BlockHeight>,
    backfill: bool,
}

#[derive(Debug, Clone)]
struct DaCatchupMetrics {
    start: Counter<u64>,
    restart: Counter<u64>,
    timeout: Counter<u64>,
    stream_closed: Counter<u64>,
    start_height: Gauge<u64>,
}

impl Default for DaCatchupMetrics {
    fn default() -> Self {
        Self::global()
    }
}

impl DaCatchupMetrics {
    pub fn global() -> DaCatchupMetrics {
        let my_meter = global_meter_or_panic();
        DaCatchupMetrics {
            start: my_meter.u64_counter("da_catchup_start").build(),
            restart: my_meter.u64_counter("da_catchup_restart").build(),
            timeout: my_meter.u64_counter("da_catchup_timeout").build(),
            stream_closed: my_meter.u64_counter("da_catchup_stream_closed").build(),
            start_height: my_meter.u64_gauge("da_catchup_start_height").build(),
        }
    }

    fn start(&self, peer: &str, height: u64) {
        let labels = [KeyValue::new("peer", peer.to_string())];
        self.start.add(1, &labels);
        self.start_height.record(height, &labels);
    }

    fn restart(&self, peer: &str, height: u64) {
        let labels = [KeyValue::new("peer", peer.to_string())];
        self.restart.add(1, &labels);
        self.start_height.record(height, &labels);
    }

    fn timeout(&self, peer: &str) {
        self.timeout
            .add(1, &[KeyValue::new("peer", peer.to_string())]);
    }

    fn stream_closed(&self, peer: &str) {
        self.stream_closed
            .add(1, &[KeyValue::new("peer", peer.to_string())]);
    }
}

#[derive(Debug, Default)]
struct DaCatchupper {
    policy: Option<DaCatchupPolicy>,
    status: Option<(tokio::task::JoinHandle<anyhow::Result<()>>, BlockHeight)>,
    backfill_start_height: Option<BlockHeight>,
    pub peers: Vec<String>,
    pub stop_height: Option<BlockHeight>,
    da_max_frame_length: usize,
    metrics: DaCatchupMetrics,
}

impl DaCatchupper {
    pub fn new(policy: Option<DaCatchupPolicy>, da_max_frame_length: usize) -> Self {
        DaCatchupper {
            policy,
            status: None,
            backfill_start_height: None,
            peers: vec![],
            da_max_frame_length,
            stop_height: None,
            metrics: DaCatchupMetrics::global(),
        }
    }

    pub fn is_fast_catchup_initial_block(&self, height: &BlockHeight) -> bool {
        matches!(
            self.policy,
            Some(DaCatchupPolicy { floor: Some(floor), .. }) if height == &floor
        )
    }

    pub fn need_to_tick(&self) -> bool {
        self.policy.as_ref().is_some_and(|p| p.backfill) || self.status.is_some()
    }

    #[cfg(test)]
    pub fn stop_task(&mut self) {
        if let Some((task, _)) = &mut self.status {
            task.abort();
            self.status = None;
        }
    }

    pub fn choose_random_peer(&self) -> Vec<String> {
        let Some(primary) = self.peers.choose(&mut deterministic_rng()).cloned() else {
            return vec![];
        };
        let mut ordered = Vec::with_capacity(self.peers.len());
        ordered.push(primary.clone());
        for peer in &self.peers {
            if peer != &primary {
                ordered.push(peer.clone());
            }
        }
        ordered
    }

    pub fn init_catchup(
        &mut self,
        from_height: BlockHeight,
        sender: &tokio::sync::mpsc::Sender<SignedBlock>,
    ) -> anyhow::Result<()> {
        let mut start_height = from_height;

        if let Some(DaCatchupPolicy {
            floor: Some(floor), ..
        }) = &self.policy
        {
            start_height = *floor;
        }

        self.catchup_from(start_height, sender)
    }

    /// Start catchup workflow based on the current policy
    pub fn catchup_from(
        &mut self,
        from_height: BlockHeight,
        sender: &tokio::sync::mpsc::Sender<SignedBlock>,
    ) -> anyhow::Result<()> {
        if self.policy.is_none() {
            debug!("No catchup policy set, stopping catchup task");
            return Ok(());
        }

        if self.status.is_some() {
            debug!("Catchup is already in progress, no need to start a new task");
            return Ok(());
        }

        if self.stop_height.is_some_and(|height| height <= from_height) {
            debug!("Catchup is already done, no need to start a new task");
            return Ok(());
        }

        let peers = self.choose_random_peer();
        if peers.is_empty() {
            info!("choose_random_peer returned no peers");
            return Ok(());
        }
        #[expect(clippy::unwrap_used, reason = "gated above")]
        let peer = peers.first().unwrap();

        debug!(
            "Starting catchup from height {} to {:?} on peer {}",
            from_height, self.stop_height, peer
        );

        self.status = Some((
            Self::start_task(
                peers,
                self.da_max_frame_length,
                from_height,
                sender.clone(),
                self.metrics.clone(),
            ),
            from_height,
        ));

        Ok(())
    }

    /// Try transition the catchup state based on the current status and policy.    
    pub fn manage_catchup(
        &mut self,
        processed_height: BlockHeight,
        sender: &tokio::sync::mpsc::Sender<SignedBlock>,
    ) -> anyhow::Result<()> {
        if self.policy.is_none() {
            debug!("No catchup policy set, skipping catchup");
            return Ok(());
        };

        if self.status.is_none() {
            if let Some(policy) = &mut self.policy {
                // In case status is None, we check if we need to start a new catchup task up to the floor height
                if policy.backfill && policy.floor.is_some() {
                    if let Some(start_height) = self.backfill_start_height {
                        policy.backfill = false; // Disable backfill after the first catchup
                        self.stop_height = policy.floor; // Set stop height to the floor if backfill is enabled

                        debug!(
                            "Starting backfill catchup from height {} to {:?}",
                            start_height, policy.floor
                        );

                        self.catchup_from(start_height, sender)?;
                    }
                } else {
                    trace!("Catchup is already done");
                }
            }

            return Ok(());
        };

        let peers = self.choose_random_peer();
        if peers.is_empty() {
            info!("choose_random_peer returned no peers");
            return Ok(());
        }
        #[expect(clippy::unwrap_used, reason = "gated above")]
        let peer = peers.first().unwrap();

        let Some((task, old_height)) = &mut self.status else {
            unreachable!("Status was already checked");
        };

        if self
            .stop_height
            .is_some_and(|height| height <= processed_height)
        {
            info!(
                "Catchup task finished, last processed height {}",
                processed_height
            );
            task.abort();
            self.status = None;
        } else if task.is_finished() {
            info!(
                "Catchup task finished, but catchup is not done yet, restarting from height {}",
                processed_height
            );
            let from = processed_height.max(*old_height);

            self.metrics.restart(peer, from.0);
            let new_task = Self::start_task(
                peers,
                self.da_max_frame_length,
                from,
                sender.clone(),
                self.metrics.clone(),
            );
            self.status = Some((new_task, from));
        } else {
            debug!(
                "Catchup task is still running, last processed height {}",
                processed_height
            );
            *old_height = processed_height;
        }

        Ok(())
    }

    fn start_task(
        peers: Vec<String>,
        da_max_frame_length: usize,
        start_height: BlockHeight,
        sender: tokio::sync::mpsc::Sender<SignedBlock>,
        metrics: DaCatchupMetrics,
    ) -> JoinHandle<anyhow::Result<()>> {
        let peer_label = peers
            .first()
            .cloned()
            .unwrap_or_else(|| "unknown".to_string());
        info!(
            "Starting catchup from height {} on peer {}",
            start_height, peer_label
        );

        metrics.start(&peer_label, start_height.0);

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
                stream.start_client().await,
                "Error occurred setting up the DA listener"
            )?;

            loop {
                match stream.listen_next().await? {
                    DaStreamPoll::Timeout => {
                        warn!("Timeout expired while waiting for block.");
                        metrics.timeout(&peer_label);
                    }
                    DaStreamPoll::StreamClosed => {
                        metrics.stream_closed(&peer_label);
                    }
                    DaStreamPoll::Event(event) => match event {
                        DataAvailabilityEvent::SignedBlock(block) => {
                            let blocks = stream.on_signed_block(block).await?;
                            for block in blocks {
                                info!(
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

impl DataAvailability {
    pub fn start_scanning_for_first_hole(
        &self,
    ) -> tokio::sync::mpsc::Receiver<Option<BlockHeight>> {
        let blocks_handle = self.blocks.new_handle();

        let (first_hole_sender, first_hole_receiver) =
            tokio::sync::mpsc::channel::<Option<BlockHeight>>(10);

        if let Some(DaCatchupPolicy { backfill: true, .. }) = self.catchupper.policy {
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
            format!("DAServer-{}", self.config.id.clone()).as_str(),
        )
        .await?;

        let (catchup_block_sender, mut catchup_block_receiver) =
            tokio::sync::mpsc::channel::<SignedBlock>(100);

        let mut first_hole_receiver = self.start_scanning_for_first_hole();

        // Used to send blocks to clients (indexers/peers)
        // This is a JoinSet of tuples containing:
        // - The peer IP address to send the blocks to
        // - The number of retries for sending the blocks
        let mut catchup_joinset: JoinSet<(String, usize)> = tokio::task::JoinSet::new();
        let mut catchup_task_checker_ticker =
            tokio::time::interval(std::time::Duration::from_millis(5000));
        let mut storage_metrics_ticker = tokio::time::interval(std::time::Duration::from_secs(30));

        module_handle_messages! {
            on_self self,
            listen<MempoolBlockEvent> evt => {
                _ = log_error!(self.handle_mempool_event(evt, &mut server, &catchup_block_sender, &mut catchup_joinset).await, "Handling Mempool Event");
            }

            listen<MempoolStatusEvent> evt => {
                self.handle_mempool_status_event(evt, &mut server).await;
            }

            listen<GenesisEvent> cmd => {
                if let GenesisEvent::GenesisBlock(signed_block) = cmd {
                    debug!("🌱  Genesis block received with validators {:?}", signed_block.consensus_proposal.staking_actions.clone());
                    _ = log_error!(self.handle_signed_block(signed_block, &mut server, &mut catchup_joinset).await.context("Handling Genesis block"),  "Handling GenesisBlock Event");
                }
                else {
                    _ = log_error!(
                        self.catchupper.init_catchup(
                            self.blocks.highest(),
                            &catchup_block_sender,
                        ),
                        "Init catchup on new peer"
                    );
                }
            }

            listen<PeerEvent> PeerEvent::NewPeer { da_address, .. } => {
                self.catchupper.peers.push(da_address.clone());
                info!("New peer {}", da_address);
                _ = log_error!(
                    self.catchupper.init_catchup(
                        self.blocks.highest(),
                        &catchup_block_sender,
                    ),
                    "Init catchup on new peer"
                );
            }

            _ = catchup_task_checker_ticker.tick(), if self.catchupper.need_to_tick() => {
                let highest_block = self.blocks.highest();
                _ = log_error!(self.catchupper.manage_catchup(highest_block, &catchup_block_sender), "Catchup transition after tick");
            }

            Some(streamed_block) = catchup_block_receiver.recv() => {
                if let Some(height) = self.handle_signed_block(streamed_block, &mut server, &mut catchup_joinset).await {
                    _ = log_error!(self.catchupper.manage_catchup(height, &catchup_block_sender), "Catchup transition after streamed block");
                }
            }

            Some(tcp_event) = server.listen_next() => {
                match tcp_event {
                    TcpEvent::Message { socket_addr, data, .. } => {
                        match data {
                            DataAvailabilityRequest::StreamFromHeight(start_height) => {
                                _ = log_error!(
                                    self.start_streaming_to_peer(start_height, &mut catchup_joinset, &socket_addr).await,
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
                self.catchupper.backfill_start_height = hole;
                let highest_block = self.blocks.highest();
                _ = log_error!(self.catchupper.manage_catchup(highest_block, &catchup_block_sender), "Catchup transition after tick");

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
        if let Ok(Some(signed_block)) = self.blocks.get(&hash) {
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
        match self.blocks.get_by_height(block_height) {
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
        sender: &tokio::sync::mpsc::Sender<SignedBlock>,
        catchup_joinset: &mut JoinSet<(String, usize)>,
    ) -> Result<()> {
        match evt {
            MempoolBlockEvent::BuiltSignedBlock(signed_block) => {
                debug!(
                    "📦  Received built block (height {}) from Mempool",
                    signed_block.height()
                );
                if let Some(height) = self
                    .handle_signed_block(signed_block, tcp_server, catchup_joinset)
                    .await
                {
                    self.catchupper.manage_catchup(height, sender)?;
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

        if block.height() < self.blocks.highest() {
            // If we are in fast catchup, we need to backfill the block
            _ = log_error!(self.store_block(&block), "Backfilling block");
        } else {
            // store block
            _ = log_error!(
                self.add_processed_block(block.clone(), tcp_server, catchup_joinset)
                    .await,
                "Adding processed block"
            );
        }

        let highest_processed_height = self.pop_buffer(hash, tcp_server, catchup_joinset).await;
        _ = log_error!(self.blocks.persist(), "Persisting blocks");

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

    fn store_block(&mut self, block: &SignedBlock) -> Result<()> {
        self.blocks
            .put(block.clone())
            .context(format!("Storing block {}", block.height()))?;

        trace!("Block {} {}: {:#?}", block.height(), block.hashed(), block);

        if block.height().0.is_multiple_of(10) || block.has_txs() {
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
        self.store_block(&block)?;

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
    ) -> Result<()> {
        let range_start = std::time::Instant::now();
        // Collect all blocks from start_height to current highest
        let processed_block_hashes: VecDeque<_> = self
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
        self.blocks
            .record_op("range_collect", "by_height", range_start.elapsed());

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
            self.da
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

        let mut server = DataAvailabilityServer::start(7898, "DaServer")
            .await
            .unwrap();

        let bus = super::DABusClient::new_from_bus(crate::bus::SharedMessageBus::new()).await;
        let mut da = super::DataAvailability {
            config: Default::default(),
            bus,
            blocks,
            buffered_signed_blocks: Default::default(),
            catchupper: Default::default(),
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
        let mut server = DataAvailabilityServer::start(7890, "DaServer")
            .await
            .unwrap();

        let receiver_global_bus = crate::bus::SharedMessageBus::new();
        let mut da_receiver = DataAvailabilityTestCtx::new(receiver_global_bus).await;
        da_receiver.da.catchupper.policy = Some(DaCatchupPolicy {
            floor: None,
            backfill: false,
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
        let (tx, mut rx) = tokio::sync::mpsc::channel(200);
        da_receiver
            .da
            .catchupper
            .peers
            .push(da_sender_address.clone());

        _ = da_receiver.da.catchupper.catchup_from(BlockHeight(0), &tx);

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
            .init_catchup(BlockHeight(15), &tx)
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
        let mut server = DataAvailabilityServer::start(7891, "DaServer")
            .await
            .unwrap();

        let receiver_global_bus = crate::bus::SharedMessageBus::new();
        let mut da_receiver = DataAvailabilityTestCtx::new(receiver_global_bus).await;
        da_receiver.da.catchupper.policy = Some(DaCatchupPolicy {
            floor: Some(BlockHeight(8)),
            backfill: true,
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
        let (tx, mut rx) = tokio::sync::mpsc::channel(200);
        da_receiver
            .da
            .catchupper
            .peers
            .push(da_sender_address.clone());

        // first init catchup should get last blocks after the floor = 8
        _ = da_receiver.da.catchupper.init_catchup(BlockHeight(0), &tx);
        _ = block_sender.send(MempoolBlockEvent::BuiltSignedBlock(block_ten.clone()));

        let mut received_blocks = vec![];
        while let Some(streamed_block) = rx.recv().await {
            da_receiver
                .handle_signed_block(streamed_block.clone(), &mut server)
                .await;
            received_blocks.push(streamed_block);
            if received_blocks.len() == 3 {
                break;
            }
        }

        assert_eq!(received_blocks.len(), 3);

        for i in 8..11 {
            assert!(received_blocks.iter().any(|b| b.height().0 == i));
        }

        // Stop the task
        da_receiver.da.catchupper.stop_height = Some(BlockHeight(10));
        _ = da_receiver
            .da
            .catchupper
            .manage_catchup(BlockHeight(10), &tx);

        // should not start backfill
        _ = da_receiver
            .da
            .catchupper
            .manage_catchup(BlockHeight(10), &tx);

        assert!(rx.try_recv().is_err());

        da_receiver.da.catchupper.backfill_start_height = Some(BlockHeight(5));

        // should start backfill from height 5
        _ = da_receiver
            .da
            .catchupper
            .manage_catchup(BlockHeight(10), &tx);

        let mut received_blocks = vec![];
        while let Some(streamed_block) = rx.recv().await {
            da_receiver
                .handle_signed_block(streamed_block.clone(), &mut server)
                .await;
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
}

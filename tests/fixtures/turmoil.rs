#![allow(dead_code)]
#![cfg(feature = "turmoil")]
#![cfg(test)]

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use bytes::Bytes;
use client_sdk::rest_client::{NodeApiClient, NodeApiHttpClient};
use hyli::{bus::SharedMessageBus, entrypoint::common_main, utils::conf::Conf};
use hyli_crypto::BlstCrypto;
use hyli_net::net::Sim;
use hyli_net::tcp::intercept::{set_message_hook_scoped, MessageAction};
use hyli_net::tcp::{decode_tcp_payload, P2PTcpMessage};
use hyli_turmoil_shims::rng::set_deterministic_seed;
use rand::{rngs::StdRng, RngCore, SeedableRng};
use tempfile::TempDir;
use tokio::sync::Mutex;
use tracing::info;

use anyhow::Result;

use crate::fixtures::test_helpers::ConfMaker;
use hyli::consensus::ConsensusNetMessage;
use hyli::mempool::MempoolNetMessage;
use hyli::p2p::network::{MsgWithHeader, NetMessage};
use hyli_modules::telemetry::init_test_meter_provider;

pub struct NetMessageInterceptor {
    _guard: hyli_net::tcp::intercept::MessageHookGuard,
}

pub fn install_net_message_dropper<F>(mut should_drop: F) -> NetMessageInterceptor
where
    F: FnMut(&NetMessage) -> bool + Send + 'static,
{
    let guard = set_message_hook_scoped(move |bytes| {
        let (_, message) = match decode_tcp_payload::<P2PTcpMessage<NetMessage>>(bytes) {
            Ok(message) => message,
            Err(_) => return MessageAction::Pass,
        };

        match message {
            P2PTcpMessage::Data(net_msg) => {
                if should_drop(&net_msg) {
                    MessageAction::Drop
                } else {
                    MessageAction::Pass
                }
            }
            P2PTcpMessage::Handshake(_) => MessageAction::Pass,
        }
    });

    NetMessageInterceptor { _guard: guard }
}

/// Install a message corrupter that can modify messages before they are sent.
/// The callback receives the decoded NetMessage and original bytes, and returns
/// `Some(corrupted_bytes)` to corrupt the message or `None` to leave it unchanged.
pub fn install_net_message_corrupter<F>(mut corrupt: F) -> NetMessageInterceptor
where
    F: FnMut(&NetMessage, &[u8]) -> Option<Bytes> + Send + 'static,
{
    let guard = set_message_hook_scoped(move |bytes| {
        let (_, message) = match decode_tcp_payload::<P2PTcpMessage<NetMessage>>(bytes) {
            Ok(message) => message,
            Err(_) => return MessageAction::Pass,
        };

        match message {
            P2PTcpMessage::Data(net_msg) => match corrupt(&net_msg, bytes) {
                Some(corrupted) => MessageAction::Replace(corrupted),
                None => MessageAction::Pass,
            },
            P2PTcpMessage::Handshake(_) => MessageAction::Pass,
        }
    });

    NetMessageInterceptor { _guard: guard }
}

/// Install a message corrupter that only targets consensus messages.
pub fn install_consensus_message_corrupter<F>(mut corrupt: F) -> NetMessageInterceptor
where
    F: FnMut(&MsgWithHeader<ConsensusNetMessage>, &[u8]) -> Option<Bytes> + Send + 'static,
{
    install_net_message_corrupter(move |message, bytes| match message {
        NetMessage::ConsensusMessage(consensus_msg) => corrupt(consensus_msg, bytes),
        NetMessage::MempoolMessage(_) => None,
    })
}

/// Install a message corrupter that only targets mempool messages.
pub fn install_mempool_message_corrupter<F>(mut corrupt: F) -> NetMessageInterceptor
where
    F: FnMut(&MsgWithHeader<MempoolNetMessage>, &[u8]) -> Option<Bytes> + Send + 'static,
{
    install_net_message_corrupter(move |message, bytes| match message {
        NetMessage::MempoolMessage(mempool_msg) => corrupt(mempool_msg, bytes),
        NetMessage::ConsensusMessage(_) => None,
    })
}
#[derive(Clone)]
pub struct TurmoilHost {
    pub conf: Conf,
    pub client: NodeApiHttpClient,
    pub bus: Arc<tokio::sync::OnceCell<SharedMessageBus>>,
}

impl TurmoilHost {
    pub async fn start(&self) -> anyhow::Result<()> {
        let crypto = Arc::new(BlstCrypto::new(&self.conf.id).context("Creating crypto")?);

        // Initialize metrics before creating the bus
        init_test_meter_provider();

        // Create the bus after metrics initialization
        let bus = SharedMessageBus::new();

        // Store the bus handle for later access
        let _ = self.bus.set(bus.new_handle());

        let mut handler = common_main(self.conf.clone(), Some(crypto), bus).await?;
        handler.exit_loop().await?;

        Ok(())
    }

    pub fn from(conf: &Conf) -> TurmoilHost {
        let client =
            NodeApiHttpClient::new(format!("http://{}:{}", conf.id, &conf.rest_server_port))
                .expect("Creating client");
        TurmoilHost {
            conf: conf.clone(),
            client: client.with_retry(3, Duration::from_millis(1000)),
            bus: Arc::new(tokio::sync::OnceCell::new()),
        }
    }
}

#[derive(Clone)]
pub struct TurmoilCtx {
    pub nodes: Vec<TurmoilHost>,
    folder: Arc<TempDir>,
    slot_duration: Duration,
    seed: u64,
    pub rng: StdRng,
    _seed_guard: hyli_turmoil_shims::rng::DeterministicSeedGuard,
}

impl TurmoilCtx {
    pub fn build_conf(temp_dir: &TempDir, i: usize) -> Conf {
        let mut node_conf = Conf {
            id: format!("node-{i}"),
            ..ConfMaker::default().default
        };

        node_conf.da_public_address = format!("{}:{}", node_conf.id, node_conf.da_server_port);
        node_conf.p2p.public_address = format!("{}:{}", node_conf.id, node_conf.p2p.server_port);
        node_conf.data_directory = temp_dir.path().into();
        node_conf.data_directory.push(node_conf.id.clone());
        node_conf
    }

    fn build_nodes(
        count: usize,
        slot_duration: Duration,
        seed: u64,
    ) -> (TempDir, Vec<TurmoilHost>) {
        let mut nodes = Vec::new();
        let mut peers = Vec::new();
        let mut confs = Vec::new();
        let mut genesis_stakers = std::collections::HashMap::new();

        let temp_dir = tempfile::Builder::new()
            .prefix(seed.to_string().as_str())
            .prefix("hyli-turmoil")
            .tempdir()
            .unwrap();

        for i in 0..count {
            let mut node_conf = Self::build_conf(&temp_dir, i + 1);
            node_conf.consensus.slot_duration = slot_duration;
            node_conf.p2p.peers = peers.clone();
            genesis_stakers.insert(node_conf.id.clone(), 100);
            peers.push(format!("{}:{}", node_conf.id, node_conf.p2p.server_port));
            confs.push(node_conf);
        }

        for node_conf in confs.iter_mut() {
            node_conf.genesis.stakers = genesis_stakers.clone();
            let node = TurmoilHost::from(node_conf);
            nodes.push(node);
        }
        (temp_dir, nodes)
    }

    pub fn new_multi(
        count: usize,
        slot_duration_ms: u64,
        seed: u64,
        sim: &mut Sim<'_>,
    ) -> Result<TurmoilCtx> {
        std::env::set_var("RISC0_DEV_MODE", "1");

        let seed_guard = set_deterministic_seed(seed);
        let rng = StdRng::seed_from_u64(seed);

        let slot_duration = Duration::from_millis(slot_duration_ms);

        let (temp, nodes) = Self::build_nodes(count, slot_duration, seed);

        _ = Self::setup_simulation(nodes.as_slice(), sim);

        info!("🚀 E2E test environment is ready!");

        Ok(TurmoilCtx {
            nodes,
            folder: Arc::new(temp),
            slot_duration: Duration::from_millis(slot_duration_ms),
            seed,
            rng,
            _seed_guard: seed_guard,
        })
    }

    pub fn add_node_to_simulation(&mut self, sim: &mut Sim<'_>) -> Result<NodeApiHttpClient> {
        let mut node_conf = Self::build_conf(&self.folder, self.nodes.len() + 1);
        node_conf.consensus.slot_duration = self.slot_duration;
        node_conf.p2p.peers = self
            .nodes
            .iter()
            .map(|node| format!("{}:{}", node.conf.id, node.conf.p2p.server_port))
            .collect();

        let node = TurmoilHost::from(&node_conf);

        _ = Self::setup_simulation(std::slice::from_ref(&node), sim);

        self.nodes.push(node);
        Ok(self.nodes.last().unwrap().client.clone())
    }

    fn setup_simulation(nodes: &[TurmoilHost], sim: &mut Sim<'_>) -> anyhow::Result<()> {
        let mut nodes = nodes.to_vec();
        nodes.reverse();

        let turmoil_node = nodes.pop().unwrap();
        {
            let id = turmoil_node.conf.id.clone();
            let cloned = Arc::new(Mutex::new(turmoil_node.clone())); // Permet de partager la variable

            let f = {
                let cloned = Arc::clone(&cloned); // Clonage pour éviter de déplacer
                move || {
                    let cloned = Arc::clone(&cloned);
                    async move {
                        let node = cloned.lock().await; // Accès mutable au nœud
                        _ = node.start().await;
                        Ok(())
                    }
                }
            };

            sim.host(id, f);
        }
        while let Some(turmoil_node) = nodes.pop() {
            let id = turmoil_node.conf.id.clone();
            let cloned = Arc::new(Mutex::new(turmoil_node.clone())); // Permet de partager la variable

            let f = {
                let cloned = Arc::clone(&cloned); // Clonage pour éviter de déplacer
                move || {
                    let cloned = Arc::clone(&cloned);
                    async move {
                        let node = cloned.lock().await; // Accès mutable au nœud
                        _ = node.start().await;
                        Ok(())
                    }
                }
            };

            sim.host(id, f);
        }
        Ok(())
    }

    pub fn client(&self) -> NodeApiHttpClient {
        self.nodes.first().unwrap().client.clone()
    }

    pub fn bus_handle(&self, node_id: &str) -> Option<SharedMessageBus> {
        self.nodes
            .iter()
            .find(|node| node.conf.id == node_id)
            .and_then(|node| node.bus.get().map(|bus| bus.new_handle()))
    }

    pub fn bus_handle_by_index(&self, index: usize) -> Option<SharedMessageBus> {
        self.nodes
            .get(index)
            .and_then(|node| node.bus.get().map(|bus| bus.new_handle()))
    }

    pub fn seed(&self) -> u64 {
        self.seed
    }

    pub fn conf(&self, n: u64) -> Conf {
        self.nodes.get((n - 1) as usize).unwrap().clone().conf
    }

    /// Generate a random number between specified `from` and `to`
    pub fn random_between(&mut self, from: u64, to: u64) -> u64 {
        from + (self.rng.next_u64() % (to - from + 1))
    }

    /// Pick randomly a node id of the current context
    pub fn random_id(&mut self) -> String {
        let random_n = self.random_between(1, self.nodes.len() as u64);
        self.conf(random_n).id
    }

    /// Pick randomly two **different** node ids of the current context
    pub fn random_id_pair(&mut self) -> (String, String) {
        let from = self.random_id();
        let to = loop {
            let candidate = self.random_id();
            if candidate != from {
                break candidate;
            }
        };

        (from, to)
    }

    /// Check that all nodes converge to the same height and commit root.
    pub async fn assert_cluster_converged(&self, min_height: u64) -> anyhow::Result<()> {
        self.assert_cluster_converged_with_max_delta(min_height, 0)
            .await
    }

    /// Check that all nodes converge to at least `min_height` within `max_delta` blocks.
    pub async fn assert_cluster_converged_with_max_delta(
        &self,
        min_height: u64,
        max_delta: u64,
    ) -> anyhow::Result<()> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(60);

        loop {
            let mut heights = Vec::new();
            for node in self.nodes.iter() {
                let height = node.client.get_block_height().await?;
                heights.push((node.conf.id.clone(), height.0));
            }

            let min = heights.iter().map(|(_, h)| *h).min().unwrap();
            let max = heights.iter().map(|(_, h)| *h).max().unwrap();

            if min >= min_height && max.saturating_sub(min) <= max_delta {
                return Ok(());
            }

            if tokio::time::Instant::now() >= deadline {
                anyhow::bail!(
                    "Cluster did not converge: min height {}, max height {}, expected >= {} with max delta {} ({:?})",
                    min,
                    max,
                    min_height,
                    max_delta,
                    heights
                );
            }

            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }
}

/// Block traffic in both directions between two node IDs.
pub fn hold_bidirectional(sim: &mut Sim<'_>, from: &str, to: &str) {
    sim.hold(from.to_string(), to.to_string());
    sim.hold(to.to_string(), from.to_string());
}

/// Restore traffic in both directions between two node IDs.
pub fn release_bidirectional(sim: &mut Sim<'_>, from: &str, to: &str) {
    sim.release(from.to_string(), to.to_string());
    sim.release(to.to_string(), from.to_string());
}

/// Isolate a single node by holding links to every other node in the cluster.
pub fn hold_node(ctx: &TurmoilCtx, sim: &mut Sim<'_>, node_id: &str) {
    for other in ctx.nodes.iter().filter(|n| n.conf.id != node_id) {
        hold_bidirectional(sim, node_id, &other.conf.id);
    }
}

/// Heal a single node by releasing links to every other node in the cluster.
pub fn release_node(ctx: &TurmoilCtx, sim: &mut Sim<'_>, node_id: &str) {
    for other in ctx.nodes.iter().filter(|n| n.conf.id != node_id) {
        release_bidirectional(sim, node_id, &other.conf.id);
    }
}

/// Partition the entire cluster by holding every pairwise link.
pub fn hold_all_links(ctx: &TurmoilCtx, sim: &mut Sim<'_>) {
    for i in 0..ctx.nodes.len() {
        for j in (i + 1)..ctx.nodes.len() {
            let from = ctx.nodes[i].conf.id.as_str();
            let to = ctx.nodes[j].conf.id.as_str();
            hold_bidirectional(sim, from, to);
        }
    }
}

/// Heal the entire cluster by releasing every pairwise link.
pub fn release_all_links(ctx: &TurmoilCtx, sim: &mut Sim<'_>) {
    for i in 0..ctx.nodes.len() {
        for j in (i + 1)..ctx.nodes.len() {
            let from = ctx.nodes[i].conf.id.as_str();
            let to = ctx.nodes[j].conf.id.as_str();
            release_bidirectional(sim, from, to);
        }
    }
}

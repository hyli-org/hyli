use std::{cmp::Ordering, collections::HashSet, sync::Arc, task::Poll, time::Duration};

use anyhow::{bail, Context};
use borsh::{BorshDeserialize, BorshSerialize};
use hyli_crypto::BlstCrypto;
use sdk::{hyli_model_utils::TimestampMs, SignedByValidator, ValidatorPublicKey};
use tokio::{
    task::{AbortHandle, JoinSet},
    time::Interval,
};
use tracing::{debug, error, info, trace, warn};

use crate::{
    clock::TimestampMsClock,
    metrics::P2PMetrics,
    ordered_join_set::OrderedJoinSet,
    tcp::{tcp_client::TcpClient, Handshake, TcpHeaders, TcpMessageLabel},
};
use hyli_turmoil_shims::collections::HashMap;

use super::{
    tcp_server::{TcpServer, TcpServerOptions},
    Canal, NodeConnectionData, P2PTcpMessage, TcpEvent,
};

#[derive(Clone, Debug)]
pub struct P2PTimeouts {
    pub poisoned_retry_interval: Duration,
    pub tcp_client_handshake_timeout: Duration,
    pub tcp_send_timeout: Duration,
    pub connect_retry_cooldown: Duration,
}

impl Default for P2PTimeouts {
    fn default() -> Self {
        Self {
            poisoned_retry_interval: Duration::from_secs(10),
            tcp_client_handshake_timeout: Duration::from_secs(10),
            tcp_send_timeout: Duration::from_secs(10),
            connect_retry_cooldown: Duration::from_secs(3),
        }
    }
}

#[derive(Debug)]
pub enum P2PServerEvent<Msg> {
    NewPeer {
        name: String,
        pubkey: ValidatorPublicKey,
        height: u64,
        da_address: String,
        start_timestamp: TimestampMs,
    },
    P2PMessage {
        msg: Msg,
        headers: TcpHeaders,
    },
}

#[derive(Debug)]
pub enum P2PTcpEvent<Data: BorshDeserialize + BorshSerialize> {
    TcpEvent(TcpEvent<Data>),
    HandShakeTcpClient(String, Box<TcpClient<Data, Data>>, Canal),
    PingPeers,
}

#[derive(Clone, Debug)]
pub struct PeerSocket {
    // Timestamp of the lastest handshake
    timestamp: TimestampMs,
    // This is the socket_addr used in the tcp_server for the current peer
    pub socket_addr: String,
    // Last time we marked/retried this socket as poisoned.
    // Note: on a TCP error we drop the tcp_server side, but p2p_server would still broadcast to
    // this socket until a new handshake overwrites it, generating noisy send errors. We flag it
    // as poisoned so sends are skipped until the next successful handshake replaces it.
    pub poisoned_at: Option<TimestampMs>,
}

#[derive(Clone, Debug)]
pub struct PeerInfo {
    // Hashmap containing a sockets for all canals of this peer
    pub canals: HashMap<Canal, PeerSocket>,
    // The address that will be used to reconnect to that peer
    #[allow(dead_code)]
    pub node_connection_data: NodeConnectionData,
}

type HandShakeJoinSet<Data> = JoinSet<(String, anyhow::Result<TcpClient<Data, Data>>, Canal)>;

type CanalJob = (
    HashSet<ValidatorPublicKey>,
    (Result<Vec<u8>, std::io::Error>, TcpHeaders, &'static str),
);
type CanalJobResult = (
    Canal,
    HashSet<ValidatorPublicKey>,
    (Result<Vec<u8>, std::io::Error>, TcpHeaders, &'static str),
);

#[derive(Debug)]
pub enum HandshakeOngoing {
    TcpClientStartedAt(TimestampMs, AbortHandle),
    HandshakeStartedAt(String, TimestampMs),
}

/// P2PServer is a wrapper around TcpServer that manages peer connections
/// Its role is to process a full handshake with a peer, in order to get its public key.
/// Once handshake is done, the peer is added to the list of peers.
/// The connection is kept alive, hance restarted if it disconnects.
pub struct P2PServer<Msg>
where
    Msg: std::fmt::Debug + BorshDeserialize + BorshSerialize + TcpMessageLabel,
{
    // Crypto object used to sign and verify messages
    crypto: Arc<BlstCrypto>,
    node_id: String,
    metrics: P2PMetrics,
    // Hashmap containing the last attempts to connect
    pub connecting: HashMap<(String, Canal), HandshakeOngoing>,
    node_p2p_public_address: String,
    node_da_public_address: String,
    pub current_height: u64,
    max_frame_length: Option<usize>,
    pub tcp_server: TcpServer<P2PTcpMessage<Msg>, P2PTcpMessage<Msg>>,
    pub peers: HashMap<ValidatorPublicKey, PeerInfo>,
    handshake_clients_tasks: HandShakeJoinSet<P2PTcpMessage<Msg>>,
    peers_ping_ticker: Interval,
    // Serialization of messages can take time so we offload them.
    canal_jobs: HashMap<Canal, OrderedJoinSet<CanalJob>>,
    timeouts: P2PTimeouts,
    start_timestamp: TimestampMs,
    _phantom: std::marker::PhantomData<Msg>,
}

impl<Msg> P2PServer<Msg>
where
    Msg: std::fmt::Debug + BorshDeserialize + BorshSerialize + TcpMessageLabel + Send + 'static,
{
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        crypto: Arc<BlstCrypto>,
        node_id: String,
        port: u16,
        max_frame_length: Option<usize>,
        node_p2p_public_address: String,
        node_da_public_address: String,
        canals: HashSet<Canal>,
        timeouts: P2PTimeouts,
        start_timestamp: TimestampMs,
    ) -> Result<Self, anyhow::Error> {
        Ok(Self {
            crypto,
            node_id: node_id.clone(),
            metrics: P2PMetrics::global(node_id.clone()),
            connecting: HashMap::default(),
            max_frame_length,
            node_p2p_public_address,
            node_da_public_address,
            current_height: 0,
            tcp_server: TcpServer::start_with_options(
                port,
                format!("P2P-{node_id}").as_str(),
                TcpServerOptions {
                    max_frame_length,
                    send_timeout: timeouts.tcp_send_timeout,
                },
            )
            .await?,
            peers: HashMap::new(),
            handshake_clients_tasks: JoinSet::new(),
            peers_ping_ticker: tokio::time::interval(std::time::Duration::from_secs(2)),
            canal_jobs: canals
                .into_iter()
                .map(|canal| (canal, OrderedJoinSet::new()))
                .collect(),
            timeouts,
            start_timestamp,
            _phantom: std::marker::PhantomData,
        })
    }

    fn poll_hashmap(
        jobs: &mut HashMap<Canal, OrderedJoinSet<CanalJob>>,
        cx: &mut std::task::Context,
    ) -> Poll<CanalJobResult> {
        for (canal, jobs) in jobs.iter_mut() {
            if let Poll::Ready(Some(result)) = jobs.poll_join_next(cx) {
                match result {
                    Ok((p, r)) => {
                        return Poll::Ready((canal.clone(), p, r));
                    }
                    Err(e) => {
                        warn!("Error in canal jobs: {:?}", e);
                    }
                }
            }
        }
        Poll::Pending
    }

    pub async fn listen_next(&mut self) -> P2PTcpEvent<P2PTcpMessage<Msg>> {
        // Await either of the joinsets in the self.canal_jobs hashmap

        loop {
            hyli_turmoil_shims::tokio_select_biased! {
                Some(tcp_event) = self.tcp_server.listen_next() => {
                    return P2PTcpEvent::TcpEvent(tcp_event);
                },
                Some(joinset_result) = self.handshake_clients_tasks.join_next() => {
                    match joinset_result {
                        Ok((public_addr, Ok(tcp_client), canal)) => {
                            if let Some(HandshakeOngoing::TcpClientStartedAt(started_at, _)) =
                                self.connecting.get(&(public_addr.clone(), canal.clone()))
                            {
                                let elapsed =
                                    (TimestampMsClock::now() - started_at.clone()).as_secs_f64();
                                self.metrics.connect_latency(canal.clone(), elapsed);
                            }
                            self.metrics.connect_result(
                                public_addr.clone(),
                                canal.clone(),
                                "ok",
                                "none",
                            );
                            return P2PTcpEvent::HandShakeTcpClient(public_addr, Box::new(tcp_client), canal);
                        }
                        Ok((public_addr, Err(err), canal)) => {
                            let reason = if err.to_string().contains("Timeout reached") {
                                "timeout"
                            } else {
                                "error"
                            };
                            if let Some(HandshakeOngoing::TcpClientStartedAt(started_at, _)) =
                                self.connecting.get(&(public_addr.clone(), canal.clone()))
                            {
                                let elapsed =
                                    (TimestampMsClock::now() - started_at.clone()).as_secs_f64();
                                self.metrics.connect_latency(canal.clone(), elapsed);
                            }
                            self.metrics.connect_result(
                                public_addr.clone(),
                                canal.clone(),
                                "error",
                                reason,
                            );
                            warn!(
                                "TcpClient connection failed (node={}, peer={}, canal={}): {:#}",
                                self.node_id, public_addr, canal, err
                            );
                            self.try_start_connection(public_addr, canal);
                            continue;
                        }
                        Err(err) => {
                            warn!(
                                "Handshake task failed to run to completion (node={}): {:?}",
                                self.node_id, err
                            );
                            continue;
                        }
                    }
                },
                (canal, pubkeys, (data, headers, message_label)) = std::future::poll_fn(|cx| Self::poll_hashmap(&mut self.canal_jobs, cx)) => {
                    let Ok(msg) = data else {
                        warn!("Error in canal jobs: {:?}", data);
                        continue
                    };
                    // TODO: handle errors?
                    self.actually_send_to(pubkeys, &canal, msg, headers, message_label).await;
                    if let Some(jobs) = self.canal_jobs.get(&canal) {
                        self.metrics
                            .canal_jobs_snapshot(canal.clone(), jobs.len() as u64);
                    }
                }
                _ = self.peers_ping_ticker.tick() => {
                    return P2PTcpEvent::PingPeers;
                }
            }
        }
    }

    /// Handle a P2PTCPEvent. This is done as separate function to easily handle async tasks
    pub async fn handle_p2p_tcp_event(
        &mut self,
        p2p_tcp_event: P2PTcpEvent<P2PTcpMessage<Msg>>,
    ) -> anyhow::Result<Option<P2PServerEvent<Msg>>> {
        match p2p_tcp_event {
            P2PTcpEvent::TcpEvent(tcp_event) => match tcp_event {
                TcpEvent::Message {
                    socket_addr,
                    data: P2PTcpMessage::Handshake(handshake),
                    ..
                } => self.handle_handshake(socket_addr, handshake).await,
                TcpEvent::Message {
                    socket_addr,
                    data: P2PTcpMessage::Data(msg),
                    headers,
                } => {
                    if let Some((peer_pubkey, canal, peer_name, peer_p2p_addr)) =
                        self.resolve_peer_context_from_socket_addr(&socket_addr)
                    {
                        self.metrics.message_received(peer_p2p_addr, canal.clone());
                        trace!(
                            "P2P recv data (node={}, peer={} ({}), canal={}, socket_addr={})",
                            self.node_id,
                            peer_name,
                            peer_pubkey,
                            canal,
                            socket_addr
                        );
                    } else {
                        self.metrics
                            .message_received(socket_addr.clone(), Canal("unknown".to_string()));
                        let unknown_canal = socket_addr
                            .split_once('/')
                            .map(|(_, canal_name)| Canal::new(canal_name.to_string()))
                            .unwrap_or_else(|| Canal::new("unknown"));
                        self.metrics
                            .unknown_peer("rx", unknown_canal, "unknown_peer");
                        trace!(
                            "P2P recv data (node={}, peer=unknown, socket_addr={})",
                            self.node_id,
                            socket_addr
                        );
                    }
                    Ok(Some(P2PServerEvent::P2PMessage { msg, headers }))
                }
                TcpEvent::Error { socket_addr, error } => {
                    self.handle_error_event(socket_addr, error).await;
                    Ok(None)
                }
                TcpEvent::Closed { socket_addr } => {
                    self.handle_closed_event(socket_addr);
                    Ok(None)
                }
            },
            P2PTcpEvent::HandShakeTcpClient(public_addr, tcp_client, canal) => {
                if let Err(e) = self
                    .do_handshake(public_addr.clone(), *tcp_client, canal.clone())
                    .await
                {
                    warn!("Error during handshake: {:?}", e);
                    self.try_start_connection(public_addr, canal);
                }
                Ok(None)
            }
            P2PTcpEvent::PingPeers => {
                let now = TimestampMsClock::now();
                let sockets: Vec<(ValidatorPublicKey, Canal, String, PeerSocket)> = self
                    .peers
                    .iter()
                    .flat_map(move |(k, v)| {
                        let cloned = k.clone();
                        let addr = v.node_connection_data.da_public_address.clone();
                        v.canals
                            .iter()
                            .map(move |(c, s)| (cloned.clone(), c.clone(), addr.clone(), s.clone()))
                    })
                    .collect();

                for (pubkey, canal, public_addr, socket) in sockets {
                    if socket.poisoned_at.is_some() {
                        let should_retry = socket
                            .poisoned_at
                            .map(|poisoned_at| {
                                now.clone() - poisoned_at >= self.timeouts.poisoned_retry_interval
                            })
                            .unwrap_or(true);
                        if should_retry {
                            self.metrics.poison_retry(pubkey.to_string(), canal.clone());
                            self.metrics.reconnect_attempt(
                                pubkey.to_string(),
                                canal.clone(),
                                "poison_retry",
                            );
                            if let Err(e) =
                                self.try_start_connection_for_peer(&pubkey, canal.clone())
                            {
                                self.metrics
                                    .rehandshake_error(pubkey.to_string(), canal.clone());
                                warn!(
                                    "Problem when retrying poisoned socket for peer {} on canal {}: {}",
                                    pubkey, canal, e
                                );
                            }
                        }
                        continue;
                    }
                    self.metrics.ping(public_addr, canal.clone());
                    if let Err(e) = self.tcp_server.ping(socket.socket_addr.clone()).await {
                        debug!("Error pinging peer {}: {:?}", socket.socket_addr, e);
                        self.mark_socket_poisoned(&socket.socket_addr);
                        let _ = self.try_start_connection_for_peer(&pubkey, canal.clone());
                    }
                }
                Ok(None)
            }
        }
    }

    pub fn find_socket_addr(&self, canal: &Canal, vid: &ValidatorPublicKey) -> Option<&String> {
        self.peers
            .get(vid)
            .and_then(|p| p.canals.get(canal).map(|socket| &socket.socket_addr))
    }

    pub fn get_socket_mut(
        &mut self,
        canal: &Canal,
        vid: &ValidatorPublicKey,
    ) -> Option<&mut PeerSocket> {
        self.peers
            .get_mut(vid)
            .and_then(|p| p.canals.get_mut(canal))
    }

    fn resolve_peer_context_from_socket_addr(
        &self,
        socket_addr: &String,
    ) -> Option<(ValidatorPublicKey, Canal, String, String)> {
        // Exact match: socket key is currently registered for a peer/canal.
        for (peer_pubkey, peer_info) in self.peers.iter() {
            for (canal, peer_socket) in peer_info.canals.iter() {
                if peer_socket.socket_addr == *socket_addr {
                    return Some((
                        peer_pubkey.clone(),
                        canal.clone(),
                        peer_info.node_connection_data.name.clone(),
                        peer_info.node_connection_data.p2p_public_address.clone(),
                    ));
                }
            }
        }

        let (public_addr, canal_name) = socket_addr.split_once('/')?;
        let (peer_pubkey, peer_info) = self
            .peers
            .iter()
            .find(|(_, info)| info.node_connection_data.p2p_public_address == public_addr)?;
        Some((
            peer_pubkey.clone(),
            Canal::new(canal_name.to_string()),
            peer_info.node_connection_data.name.clone(),
            peer_info.node_connection_data.p2p_public_address.clone(),
        ))
    }

    fn mark_socket_poisoned(&mut self, socket_addr: &String) {
        if let Some((pubkey, canal, _peer_name, _peer_p2p_addr)) =
            self.resolve_peer_context_from_socket_addr(socket_addr)
        {
            if let Some(peer_socket) = self.get_socket_mut(&canal, &pubkey) {
                peer_socket.poisoned_at = Some(TimestampMsClock::now());
                self.metrics
                    .poison_marked(pubkey.to_string(), canal.clone());
            }
        }
    }

    async fn handle_error_event(
        &mut self,
        socket_addr: String,
        error: String,
    ) -> Option<P2PServerEvent<Msg>> {
        let peer_ctx = self.resolve_peer_context_from_socket_addr(&socket_addr);
        if let Some((peer_pubkey, canal, peer_name, peer_p2p_addr)) = peer_ctx.as_ref() {
            warn!(
                "P2P TCP error (node={}, peer={} ({}), addr={}, canal={}, socket_addr={}): {}",
                self.node_id, peer_name, peer_pubkey, peer_p2p_addr, canal, socket_addr, error
            );
            self.metrics
                .message_error(peer_p2p_addr.to_string(), canal.clone());
            self.metrics
                .reconnect_attempt(peer_p2p_addr.to_string(), canal.clone(), "tcp_error");
        } else {
            self.metrics
                .message_error(socket_addr.clone(), Canal("unknown".to_string()));
            warn!(
                "P2P TCP error (node={}, socket_addr={}): {}",
                self.node_id, socket_addr, error
            );
        }
        // There was an error with the connection with the peer. We try to reconnect.

        // TODO: An error can happen when a message was no *sent* correctly. Investigate how to handle that specific case
        // TODO: match the error type to decide what to do
        self.mark_socket_poisoned(&socket_addr);
        self.tcp_server.drop_peer_stream(socket_addr.clone());

        if let Some((peer_pubkey, canal, peer_name, _)) = peer_ctx {
            trace!(
                "Will retry connection to peer {} canal {} after error {}",
                peer_name,
                canal,
                socket_addr
            );
            if let Err(e) = self.try_start_connection_for_peer(&peer_pubkey, canal.clone()) {
                self.metrics
                    .rehandshake_error(peer_pubkey.to_string(), canal.clone());
                warn!(
                    "Problem when retrying connection to peer {} after error: {}",
                    peer_pubkey, e
                );
            }
            self.metrics.tcp_error_event(Some(canal.clone()));
        } else {
            self.metrics.tcp_error_event(None);
            warn!(
                "Peer connection error on {} did not map to a known peer/canal on node={}",
                socket_addr, self.node_id
            );
        }
        None
    }

    fn handle_closed_event(&mut self, socket_addr: String) {
        // TODO: investigate how to properly handle this case
        // The connection has been closed by peer. We remove the peer and try to reconnect.
        let peer_ctx = self.resolve_peer_context_from_socket_addr(&socket_addr);
        if let Some((peer_pubkey, canal, peer_name, peer_p2p_addr)) = peer_ctx.as_ref() {
            warn!(
                "P2P TCP closed (node={}, peer={} ({}), addr={}, canal={}, socket_addr={})",
                self.node_id, peer_name, peer_pubkey, peer_p2p_addr, canal, socket_addr
            );
            self.metrics
                .message_closed(peer_p2p_addr.to_string(), canal.clone());
            self.metrics
                .reconnect_attempt(peer_p2p_addr.to_string(), canal.clone(), "tcp_closed");
        } else {
            self.metrics
                .message_closed(socket_addr.clone(), Canal("unknown".to_string()));
            warn!(
                "P2P TCP closed (node={}, socket_addr={})",
                self.node_id, socket_addr
            );
        }

        // When we receive a close event
        // It is a closed connection that need to be removed from tcp server clients in all cases
        // If it is a connection matching a canal/peer, it means we can retry
        self.mark_socket_poisoned(&socket_addr);
        self.tcp_server.drop_peer_stream(socket_addr.clone());

        if let Some((peer_pubkey, canal, peer_name, _)) = peer_ctx {
            trace!(
                "Will retry connection to peer {} canal {} after close {}",
                peer_name,
                canal,
                socket_addr
            );
            if let Err(e) = self.try_start_connection_for_peer(&peer_pubkey, canal.clone()) {
                self.metrics
                    .rehandshake_error(peer_pubkey.to_string(), canal.clone());
                warn!(
                    "Problem when retrying connection to peer {} after close: {}",
                    peer_pubkey, e
                );
            }
            self.metrics.tcp_closed_event(Some(canal.clone()));
        } else {
            self.metrics.tcp_closed_event(None);
            warn!(
                "Closed socket {} did not map to a known peer/canal on node={}",
                socket_addr, self.node_id
            );
        }
    }

    async fn handle_handshake(
        &mut self,
        socket_addr: String,
        handshake: Handshake,
    ) -> anyhow::Result<Option<P2PServerEvent<Msg>>> {
        match handshake {
            Handshake::Hello((canal, v, timestamp)) => {
                self.metrics
                    .handshake_hello_received(v.msg.p2p_public_address.clone(), canal.clone());

                // Verify message signature
                if let Err(e) = BlstCrypto::verify(&v).context("Error verifying Hello message") {
                    self.metrics.handshake_error(
                        v.msg.p2p_public_address.clone(),
                        canal.clone(),
                        "hello",
                        "verify",
                    );
                    return Err(e);
                }

                // Best-effort: set a readable peer label on the underlying TcpServer as early as possible.
                self.tcp_server
                    .set_peer_label(&socket_addr, Self::peer_socket_label(&v, &canal));

                info!(
                    "👋 [{}] Received Hello from {} ({}) on socket_addr={} ts={}",
                    canal, v.msg.name, v.signature.validator, socket_addr, timestamp
                );
                match self.create_signed_node_connection_data() {
                    Ok(verack) => {
                        // Send Verack response
                        if let Err(e) = self
                            .tcp_server
                            .send(
                                socket_addr.clone(),
                                P2PTcpMessage::<Msg>::Handshake(Handshake::Verack((
                                    canal.clone(),
                                    verack,
                                    timestamp.clone(),
                                ))),
                                vec![],
                            )
                            .await
                        {
                            self.metrics.handshake_error(
                                v.msg.p2p_public_address.clone(),
                                canal.clone(),
                                "hello",
                                "send_verack",
                            );
                            bail!(
                                "Error sending Verack to {} ({}) on socket_addr={}: {:?}",
                                v.msg.name,
                                v.signature.validator,
                                socket_addr,
                                e
                            );
                        }

                        self.metrics.handshake_verack_emitted(
                            v.msg.p2p_public_address.clone(),
                            canal.clone(),
                        );
                    }
                    Err(e) => {
                        self.metrics.handshake_error(
                            v.msg.p2p_public_address.clone(),
                            canal.clone(),
                            "hello",
                            "sign",
                        );
                        bail!("Error creating signed node connection data: {:?}", e);
                    }
                }

                Ok(self.handle_peer_update(canal, &v, timestamp, socket_addr))
            }
            Handshake::Verack((canal, v, timestamp)) => {
                self.metrics
                    .handshake_verack_received(v.msg.p2p_public_address.clone(), canal.clone());

                // Verify message signature
                if let Err(e) = BlstCrypto::verify(&v).context("Error verifying Verack message") {
                    self.metrics.handshake_error(
                        v.msg.p2p_public_address.clone(),
                        canal.clone(),
                        "verack",
                        "verify",
                    );
                    return Err(e);
                }

                // Best-effort: set a readable peer label on the underlying TcpServer as early as possible.
                self.tcp_server
                    .set_peer_label(&socket_addr, Self::peer_socket_label(&v, &canal));

                if let Some(elapsed) = self
                    .connecting
                    .get(&(v.msg.p2p_public_address.clone(), canal.clone()))
                    .and_then(|ongoing| match ongoing {
                        HandshakeOngoing::HandshakeStartedAt(_, started_at) => {
                            Some((TimestampMsClock::now() - started_at.clone()).as_secs_f64())
                        }
                        _ => None,
                    })
                {
                    self.metrics.handshake_latency(canal.clone(), elapsed);
                }

                info!(
                    "👋 [{}] Received Verack from {} ({}) on socket_addr={} ts={}",
                    canal, v.msg.name, v.signature.validator, socket_addr, timestamp
                );
                Ok(self.handle_peer_update(canal, &v, timestamp, socket_addr))
            }
        }
    }

    fn peer_socket_label(v: &SignedByValidator<NodeConnectionData>, canal: &Canal) -> String {
        format!(
            "{} ({}) {}/{}",
            v.msg.name, v.signature.validator, v.msg.p2p_public_address, canal
        )
    }

    fn handle_peer_update(
        &mut self,
        canal: Canal,
        v: &SignedByValidator<NodeConnectionData>,
        timestamp: TimestampMs,
        socket_addr: String,
    ) -> Option<P2PServerEvent<Msg>> {
        let peer_pubkey = v.signature.validator.clone();
        let local_pubkey = self.crypto.validator_pubkey().clone();

        // Once we received a signed handshake message from that peer, the connection attempt is no
        // longer "ongoing" from our PoV.
        self.connecting
            .remove(&(v.msg.p2p_public_address.clone(), canal.clone()));
        self.metrics
            .connecting_snapshot(self.connecting.len() as u64);

        let is_new_peer = !self.peers.contains_key(&peer_pubkey);
        let label = Self::peer_socket_label(v, &canal);

        let peer_info = self
            .peers
            .entry(peer_pubkey.clone())
            .or_insert_with(|| PeerInfo {
                canals: HashMap::new(),
                node_connection_data: v.msg.clone(),
            });

        // Refresh signed connection data (height/addresses may evolve).
        peer_info.node_connection_data = v.msg.clone();

        if let Some(existing_socket) = peer_info.canals.get_mut(&canal) {
            let should_replace = Self::should_replace_socket(
                &existing_socket.timestamp,
                &timestamp,
                &local_pubkey,
                &peer_pubkey,
            );

            let (socket_to_drop, kept_socket_addr) = if should_replace {
                let old_socket_addr =
                    std::mem::replace(&mut existing_socket.socket_addr, socket_addr);
                existing_socket.timestamp = timestamp;
                existing_socket.poisoned_at = None;
                (old_socket_addr, existing_socket.socket_addr.clone())
            } else {
                existing_socket.poisoned_at = None;
                (socket_addr, existing_socket.socket_addr.clone())
            };

            trace!(
                "Peer {} ({}) canal {} on node {} -> dropping socket_addr={} keeping socket_addr={}",
                v.msg.name,
                peer_pubkey,
                canal,
                self.node_id,
                socket_to_drop,
                kept_socket_addr
            );
            self.tcp_server.set_peer_label(&kept_socket_addr, label);
            if socket_to_drop != kept_socket_addr {
                self.tcp_server.drop_peer_stream(socket_to_drop);
            }
            return None;
        }

        // New canal for an existing peer, or first canal for a new peer.
        peer_info.canals.insert(
            canal.clone(),
            PeerSocket {
                timestamp,
                socket_addr: socket_addr.clone(),
                poisoned_at: None,
            },
        );
        self.tcp_server.set_peer_label(&socket_addr, label);
        let peers_on_canal = self
            .peers
            .values()
            .filter(|info| info.canals.contains_key(&canal))
            .count() as u64;
        self.metrics
            .peers_canal_snapshot(canal.clone(), peers_on_canal);

        if is_new_peer {
            self.metrics.peers_snapshot(self.peers.len() as u64);
            info!(
                "New peer connected on node {}: {} ({})",
                self.node_id, v.msg.name, peer_pubkey
            );
            return Some(P2PServerEvent::NewPeer {
                name: v.msg.name.to_string(),
                pubkey: v.signature.validator.clone(),
                da_address: v.msg.da_public_address.clone(),
                height: v.msg.current_height,
                start_timestamp: v.msg.start_timestamp.clone(),
            });
        }

        debug!(
            "Peer {} ({}) added/updated canal {} on node {} (socket_addr={})",
            v.msg.name, peer_pubkey, canal, self.node_id, socket_addr
        );
        None
    }

    fn should_replace_socket(
        existing_ts: &TimestampMs,
        new_ts: &TimestampMs,
        local_pubkey: &ValidatorPublicKey,
        peer_pubkey: &ValidatorPublicKey,
    ) -> bool {
        existing_ts < new_ts
            || (existing_ts == new_ts && local_pubkey.cmp(peer_pubkey) == Ordering::Less)
    }

    #[cfg(test)]
    pub fn remove_peer(&mut self, peer_pubkey: &ValidatorPublicKey, canal: Canal) {
        if let Some(peer_info) = self.peers.get_mut(peer_pubkey) {
            if let Some(removed) = peer_info.canals.remove(&canal) {
                self.tcp_server
                    .drop_peer_stream(removed.socket_addr.clone());
                let peers_on_canal = self
                    .peers
                    .values()
                    .filter(|info| info.canals.contains_key(&canal))
                    .count() as u64;
                self.metrics
                    .peers_canal_snapshot(canal.clone(), peers_on_canal);
            }
        }
    }

    // TODO: Make P2P server generic with this payload ? This data is 100% custom
    /// Create a payload with data that will be transmitted during handshake
    fn create_signed_node_connection_data(
        &self,
    ) -> anyhow::Result<SignedByValidator<NodeConnectionData>> {
        let node_connection_data = NodeConnectionData {
            version: 1,
            name: self.node_id.clone(),
            current_height: self.current_height,
            p2p_public_address: self.node_p2p_public_address.clone(),
            da_public_address: self.node_da_public_address.clone(),
            start_timestamp: self.start_timestamp.clone(),
        };
        self.crypto.sign(node_connection_data)
    }

    fn try_start_connection_for_peer(
        &mut self,
        pubkey: &ValidatorPublicKey,
        canal: Canal,
    ) -> anyhow::Result<()> {
        let now = TimestampMsClock::now();
        if let Some(peer_socket) = self.get_socket_mut(&canal, pubkey) {
            peer_socket.poisoned_at = Some(now.clone());
        }

        let peer = self
            .peers
            .get(pubkey)
            .context(format!("Peer not found {pubkey}"))?;

        info!(
            "Attempt to reconnect to {}/{}",
            peer.node_connection_data.p2p_public_address, canal
        );

        self.try_start_connection(peer.node_connection_data.p2p_public_address.clone(), canal);

        Ok(())
    }

    /// Checks if creating a fresh tcp client is relevant and do it if so
    /// Start a task, cancellation safe
    pub fn try_start_connection(&mut self, peer_address: String, canal: Canal) {
        trace!(
            "try_start_connection request to {} on canal {} (node={}, connecting={})",
            peer_address,
            canal,
            self.node_id,
            self.connecting.len()
        );
        if peer_address == self.node_p2p_public_address {
            trace!("Trying to connect to self");
            return;
        }

        let now = TimestampMsClock::now();

        // A connection is already started for this public address ? If it is too old, let 's try retry one
        // If it is recent, let's wait for it to finish
        if let Some(ongoing) = self.connecting.get(&(peer_address.clone(), canal.clone())) {
            match ongoing {
                HandshakeOngoing::TcpClientStartedAt(last_connect_attempt, abort_handle) => {
                    if now.clone() - last_connect_attempt.clone()
                        < self.timeouts.connect_retry_cooldown
                    {
                        self.metrics
                            .handshake_throttle_tcp_client(peer_address.clone(), canal.clone());
                        return;
                    }
                    abort_handle.abort();
                }
                HandshakeOngoing::HandshakeStartedAt(addr, last_handshake_started_at) => {
                    if now.clone() - last_handshake_started_at.clone()
                        < self.timeouts.connect_retry_cooldown
                    {
                        self.metrics
                            .handshake_throttle_handshake(peer_address.clone(), canal.clone());
                        return;
                    }
                    self.tcp_server.drop_peer_stream(addr.to_string());
                }
            }
        }

        self.start_connection_task(peer_address, canal);
    }

    /// Creates a task that attempts to create a tcp client
    pub fn start_connection_task(&mut self, peer_address: String, canal: Canal) {
        let mfl = self.max_frame_length;
        let now = TimestampMsClock::now();
        let peer_address_clone = peer_address.clone();
        let canal_clone = canal.clone();
        let handshake_timeout = self.timeouts.tcp_client_handshake_timeout;

        info!(
            "Starting connection attempt to {} on canal {} (node={})",
            peer_address, canal, self.node_id
        );
        self.metrics
            .connect_attempt(peer_address.clone(), canal.clone());

        let abort_handle = self.handshake_clients_tasks.spawn(async move {
            let handshake_task = TcpClient::connect_with_opts_and_timeout(
                "p2p_server_handshake",
                mfl,
                peer_address_clone.clone(),
                handshake_timeout,
            );

            let result = handshake_task.await;
            (peer_address_clone, result, canal_clone)
        });

        self.connecting.insert(
            (peer_address.clone(), canal.clone()),
            HandshakeOngoing::TcpClientStartedAt(now, abort_handle),
        );
        self.metrics
            .connecting_snapshot(self.connecting.len() as u64);

        self.metrics
            .handshake_connection_emitted(peer_address.clone(), canal);
    }

    async fn do_handshake(
        &mut self,
        public_addr: String,
        tcp_client: TcpClient<P2PTcpMessage<Msg>, P2PTcpMessage<Msg>>,
        canal: Canal,
    ) -> anyhow::Result<()> {
        let signed_node_connection_data = self.create_signed_node_connection_data()?;
        let timestamp = TimestampMsClock::now();

        debug!(
            "Starting handshake (node={}, peer_addr={}, peer_socket_addr={}, canal={})",
            self.node_id, public_addr, tcp_client.socket_addr, canal
        );

        let socket_addr = format!("{public_addr}/{canal}");

        self.connecting.insert(
            (public_addr.clone(), canal.clone()),
            HandshakeOngoing::HandshakeStartedAt(socket_addr.clone(), timestamp.clone()),
        );

        self.tcp_server
            .setup_client(socket_addr.clone(), tcp_client);
        self.tcp_server
            .send(
                socket_addr.clone(),
                P2PTcpMessage::<Msg>::Handshake(Handshake::Hello((
                    canal.clone(),
                    signed_node_connection_data.clone(),
                    timestamp,
                ))),
                vec![],
            )
            .await?;

        self.metrics.handshake_hello_emitted(public_addr, canal);

        Ok(())
    }

    #[cfg_attr(feature = "instrumentation", tracing::instrument(skip(self)))]
    pub async fn send(
        &mut self,
        validator_pub_key: ValidatorPublicKey,
        canal: Canal,
        msg: Msg,
    ) -> anyhow::Result<()> {
        let Some(peer) = self.peers.get(&validator_pub_key) else {
            warn!(
                "Send requested to unknown peer {} on canal {} (node={})",
                validator_pub_key, canal, self.node_id
            );
            self.metrics
                .unknown_peer("tx", canal.clone(), "unknown_peer");
            return Ok(());
        };

        let Some(peer_socket) = peer.canals.get(&canal) else {
            warn!(
                "Peer {} ({}) has no socket for canal {} on node {}; available canals: {:?}",
                peer.node_connection_data.name,
                validator_pub_key,
                canal,
                self.node_id,
                peer.canals.keys().collect::<Vec<_>>()
            );
            self.metrics
                .unknown_peer("tx", canal.clone(), "unknown_canal");
            return Ok(());
        };
        let socket_addr = peer_socket.socket_addr.clone();
        if peer_socket.poisoned_at.is_some() {
            debug!(
                "Peer {} socket {} for canal {} is poisoned; skipping send on node {}",
                validator_pub_key, socket_addr, canal, self.node_id
            );
            self.metrics
                .poison_send_skipped(validator_pub_key.to_string(), canal.clone());
            return Ok(());
        }

        let message_label = msg.message_label();
        let headers = crate::tcp::headers_from_span();
        if let Some(jobs) = self.canal_jobs.get_mut(&canal) {
            if !jobs.is_empty() {
                jobs.spawn(async move {
                    (
                        HashSet::from_iter(std::iter::once(validator_pub_key)),
                        (
                            borsh::to_vec(&P2PTcpMessage::Data(msg)),
                            headers,
                            message_label,
                        ),
                    )
                });
                self.metrics
                    .canal_jobs_snapshot(canal.clone(), jobs.len() as u64);
                return Ok(());
            }
        }

        let peer_name = peer.node_connection_data.name.clone();
        let peer_p2p_addr = peer.node_connection_data.p2p_public_address.clone();

        if let Err(e) = self
            .tcp_server
            .send(socket_addr.clone(), P2PTcpMessage::Data(msg), headers)
            .await
        {
            self.metrics
                .message_send_error(peer_p2p_addr.clone(), canal.clone());
            self.metrics
                .reconnect_attempt(peer_p2p_addr.clone(), canal.clone(), "send_error");
            self.mark_socket_poisoned(&socket_addr);
            if let Err(start_err) =
                self.try_start_connection_for_peer(&validator_pub_key, canal.clone())
            {
                self.metrics
                    .rehandshake_error(validator_pub_key.to_string(), canal.clone());
                return Err(start_err.context(format!(
                    "Re-handshaking after message sending error with peer {validator_pub_key}"
                )));
            }
            bail!(
                "Failed to send message (node={}, peer={} ({}), addr={}, canal={}, socket_addr={}): {:#}",
                self.node_id,
                peer_name,
                validator_pub_key,
                peer_p2p_addr,
                canal,
                socket_addr,
                e
            );
        }

        self.metrics.message_emitted(peer_p2p_addr, canal);
        Ok(())
    }

    #[cfg_attr(feature = "instrumentation", tracing::instrument(skip(self)))]
    pub fn broadcast(&mut self, msg: Msg, canal: Canal) {
        let Some(jobs) = self.canal_jobs.get_mut(&canal) else {
            error!("Canal {:?} does not exist in P2P server", canal);
            self.metrics
                .unknown_peer("tx", canal.clone(), "unknown_canal");
            return;
        };
        let message_label = msg.message_label();
        let peers = self.peers.keys().cloned().collect();
        let headers = crate::tcp::headers_from_span();
        jobs.spawn(async move {
            (
                peers,
                (
                    borsh::to_vec(&P2PTcpMessage::Data(msg)),
                    headers,
                    message_label,
                ),
            )
        });
        self.metrics
            .canal_jobs_snapshot(canal.clone(), jobs.len() as u64);
    }

    #[cfg_attr(feature = "instrumentation", tracing::instrument(skip(self)))]
    pub fn broadcast_only_for(
        &mut self,
        only_for: &HashSet<ValidatorPublicKey>,
        canal: Canal,
        msg: Msg,
    ) {
        let Some(jobs) = self.canal_jobs.get_mut(&canal) else {
            error!("Canal {:?} does not exist in P2P server", canal);
            self.metrics
                .unknown_peer("tx", canal.clone(), "unknown_canal");
            return;
        };
        let message_label = msg.message_label();
        let peers = only_for.clone();
        let headers = crate::tcp::headers_from_span();
        jobs.spawn(async move {
            (
                peers,
                (
                    borsh::to_vec(&P2PTcpMessage::Data(msg)),
                    headers,
                    message_label,
                ),
            )
        });
        self.metrics
            .canal_jobs_snapshot(canal.clone(), jobs.len() as u64);
    }

    async fn actually_send_to(
        &mut self,
        only_for: HashSet<ValidatorPublicKey>,
        canal: &Canal,
        msg: Vec<u8>,
        headers: TcpHeaders,
        message_label: &'static str,
    ) -> HashMap<ValidatorPublicKey, anyhow::Error> {
        let peer_addr_to_pubkey: HashMap<String, ValidatorPublicKey> = self
            .peers
            .iter()
            .filter_map(|(pubkey, peer)| {
                if only_for.contains(pubkey) {
                    peer.canals.get(canal).and_then(|socket| {
                        socket
                            .poisoned_at
                            .is_none()
                            .then_some((socket.socket_addr.clone(), pubkey.clone()))
                    })
                } else {
                    None
                }
            })
            .collect();

        let res = self
            .tcp_server
            .raw_send_parallel(
                peer_addr_to_pubkey.keys().cloned().collect(),
                msg,
                headers,
                message_label,
            )
            .await;
        self.metrics
            .broadcast_targets(canal.clone(), peer_addr_to_pubkey.len() as u64);
        self.metrics
            .broadcast_failures(canal.clone(), res.len() as u64);

        // Count successful sends for P2P metrics (broadcast path doesn't call message_emitted).
        for (socket_addr, pubkey) in peer_addr_to_pubkey.iter() {
            if !res.contains_key(socket_addr) {
                let peer_p2p_addr = self
                    .peers
                    .get(pubkey)
                    .map(|peer| peer.node_connection_data.p2p_public_address.clone())
                    .unwrap_or_else(|| socket_addr.clone());
                self.metrics.message_emitted(peer_p2p_addr, canal.clone());
            }
        }

        HashMap::from_iter(res.into_iter().filter_map(|(k, v)| {
            peer_addr_to_pubkey.get(&k).map(|pubkey| {
                let peer_p2p_addr = self
                    .peers
                    .get(pubkey)
                    .map(|peer| peer.node_connection_data.p2p_public_address.clone())
                    .unwrap_or_else(|| k.clone());
                self.metrics
                    .message_send_error(peer_p2p_addr.clone(), canal.clone());
                self.metrics.reconnect_attempt(
                    peer_p2p_addr.clone(),
                    canal.clone(),
                    "broadcast_send_error",
                );
                error!("Error sending message to {} during broadcast: {}", k, v);
                self.mark_socket_poisoned(&k);
                if let Err(e) = self.try_start_connection_for_peer(pubkey, canal.clone()) {
                    warn!("Problem when triggering re-handshake after message sending error with peer {}/{}: {}", pubkey, canal, e);
                }
                (pubkey.clone(), v)
            })
        }))
    }
}

#[cfg(test)]
pub mod tests {
    use std::collections::HashSet;
    use std::sync::{Arc, OnceLock};

    use anyhow::Result;
    use borsh::{BorshDeserialize, BorshSerialize};
    use hyli_crypto::BlstCrypto;
    use opentelemetry::KeyValue;
    use opentelemetry_sdk::metrics::{
        data::{self, Sum},
        reader::MetricReader,
        ManualReader, SdkMeterProvider,
    };
    use opentelemetry_sdk::Resource;
    use tokio::net::TcpListener;

    use crate::clock::TimestampMsClock;
    use crate::tcp::{
        p2p_server::P2PServer, Canal, Handshake, P2PTcpMessage, TcpEvent, TcpMessageLabel,
    };

    use super::P2PTcpEvent;

    pub async fn find_available_port() -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        addr.port()
    }
    // Simple message type for testing
    #[derive(Debug, Clone, BorshSerialize, BorshDeserialize, Eq, PartialEq, PartialOrd, Ord)]
    pub struct TestMessage(String);

    impl crate::tcp::TcpMessageLabel for TestMessage {
        fn message_label(&self) -> &'static str {
            "TestMessage"
        }
    }

    macro_rules! receive_and_handle_event {
        ($server:expr, $pattern:pat, $error_msg:expr) => {{
            let event = receive_event($server, $error_msg).await?;
            assert!(
                matches!(event, $pattern),
                "Expected {:?}, got {:?}",
                stringify!($pattern),
                event,
            );
            $server.handle_p2p_tcp_event(event).await?;
        }};
    }

    async fn receive_and_handle_until(
        server: &mut super::P2PServer<TestMessage>,
        error_msg: &str,
        predicate: impl Fn(&P2PTcpEvent<P2PTcpMessage<TestMessage>>) -> bool,
    ) -> Result<()> {
        loop {
            let event = receive_event(server, error_msg).await?;
            let matched = predicate(&event);
            server.handle_p2p_tcp_event(event).await?;
            if matched {
                return Ok(());
            }
        }
    }

    async fn setup_p2p_server_pair() -> Result<(
        (u16, super::P2PServer<TestMessage>),
        (u16, super::P2PServer<TestMessage>),
    )> {
        let crypto1 = BlstCrypto::new_random().unwrap();
        let crypto2 = BlstCrypto::new_random().unwrap();

        let port1 = find_available_port().await;
        let port2 = find_available_port().await;

        tracing::info!("Starting P2P server1 on port {port1}");
        tracing::info!("Starting P2P server2 on port {port2}");
        let p2p_server1 = P2PServer::new(
            crypto1.into(),
            "node1".to_string(),
            port1,
            None,
            format!("127.0.0.1:{port1}"),
            "127.0.0.1:4321".into(), // send some dummy address for DA,
            HashSet::from_iter(vec![Canal::new("A"), Canal::new("B")]),
            super::P2PTimeouts::default(),
            TimestampMsClock::now(),
        )
        .await?;
        let p2p_server2 = P2PServer::new(
            crypto2.into(),
            "node2".to_string(),
            port2,
            None,
            format!("127.0.0.1:{port2}"),
            "127.0.0.1:4321".into(), // send some dummy address for DA
            HashSet::from_iter(vec![Canal::new("A"), Canal::new("B")]),
            super::P2PTimeouts::default(),
            TimestampMsClock::now(),
        )
        .await?;

        Ok(((port1, p2p_server1), (port2, p2p_server2)))
    }

    async fn receive_event(
        p2p_server: &mut super::P2PServer<TestMessage>,
        error_msg: &str,
    ) -> Result<P2PTcpEvent<P2PTcpMessage<TestMessage>>> {
        loop {
            let timeout_duration = std::time::Duration::from_millis(100);

            match tokio::time::timeout(timeout_duration, p2p_server.listen_next()).await {
                Ok(event) => match event {
                    P2PTcpEvent::PingPeers => {
                        continue;
                    }
                    _ => return Ok(event),
                },
                Err(_) => {
                    anyhow::bail!("Timed out while waiting for message: {error_msg}");
                }
            }
        }
    }

    #[derive(Clone, Debug)]
    struct TestReader {
        inner: Arc<ManualReader>,
    }

    impl MetricReader for TestReader {
        fn register_pipeline(
            &self,
            pipeline: std::sync::Weak<opentelemetry_sdk::metrics::Pipeline>,
        ) {
            self.inner.register_pipeline(pipeline);
        }

        fn collect(
            &self,
            rm: &mut data::ResourceMetrics,
        ) -> opentelemetry_sdk::metrics::MetricResult<()> {
            self.inner.collect(rm)
        }

        fn force_flush(&self) -> opentelemetry_sdk::error::OTelSdkResult {
            self.inner.force_flush()
        }

        fn shutdown(&self) -> opentelemetry_sdk::error::OTelSdkResult {
            self.inner.shutdown()
        }

        fn temporality(
            &self,
            kind: opentelemetry_sdk::metrics::InstrumentKind,
        ) -> opentelemetry_sdk::metrics::Temporality {
            self.inner.temporality(kind)
        }
    }

    struct TestMetrics {
        reader: TestReader,
        _provider: SdkMeterProvider,
    }

    static TEST_METRICS: OnceLock<TestMetrics> = OnceLock::new();

    fn test_metrics() -> &'static TestMetrics {
        TEST_METRICS.get_or_init(|| {
            let reader = TestReader {
                inner: Arc::new(ManualReader::builder().build()),
            };
            let provider = SdkMeterProvider::builder()
                .with_reader(reader.clone())
                .build();
            opentelemetry::global::set_meter_provider(provider.clone());
            TestMetrics {
                reader,
                _provider: provider,
            }
        })
    }

    fn attrs_match(expected: &[KeyValue], actual: &[KeyValue]) -> bool {
        expected.iter().all(|expect| {
            actual
                .iter()
                .any(|kv| kv.key == expect.key && kv.value == expect.value)
        })
    }

    fn sum_metric_u64(
        reader: &TestReader,
        scope_name: &str,
        metric_name: &str,
        expected_attrs: &[KeyValue],
    ) -> u64 {
        let mut rm = data::ResourceMetrics {
            resource: Resource::builder().build(),
            scope_metrics: Vec::new(),
        };
        reader.collect(&mut rm).expect("collect metrics");

        rm.scope_metrics
            .into_iter()
            .filter(|scope_metrics| scope_metrics.scope.name() == scope_name)
            .flat_map(|scope_metrics| scope_metrics.metrics)
            .filter(|metric| metric.name == metric_name)
            .flat_map(|metric| {
                metric
                    .data
                    .as_any()
                    .downcast_ref::<Sum<u64>>()
                    .expect("sum metric")
                    .data_points
                    .iter()
                    .filter(|data_point| attrs_match(expected_attrs, &data_point.attributes))
                    .map(|data_point| data_point.value)
                    .collect::<Vec<_>>()
            })
            .sum()
    }

    async fn connect_single_canal(
        p2p_server1: &mut P2PServer<TestMessage>,
        p2p_server2: &mut P2PServer<TestMessage>,
        port2: u16,
        canal: Canal,
    ) -> Result<()> {
        p2p_server1.try_start_connection(format!("127.0.0.1:{port2}"), canal);
        receive_and_handle_event!(
            p2p_server1,
            P2PTcpEvent::HandShakeTcpClient(_, _, _),
            "Expected HandShake TCP Client connection"
        );
        receive_and_handle_event!(
            p2p_server2,
            P2PTcpEvent::TcpEvent(TcpEvent::Message {
                socket_addr: _,
                data: P2PTcpMessage::Handshake(Handshake::Hello(_)),
                ..
            }),
            "Expected HandShake Hello message"
        );
        receive_and_handle_event!(
            p2p_server1,
            P2PTcpEvent::TcpEvent(TcpEvent::Message {
                socket_addr: _,
                data: P2PTcpMessage::Handshake(Handshake::Verack(_)),
                ..
            }),
            "Expected HandShake Verack"
        );
        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn p2p_metrics_broadcast_counts_tx_rx() -> Result<()> {
        let metrics = test_metrics();
        let ((_, mut p2p_server1), (port2, mut p2p_server2)) = setup_p2p_server_pair().await?;
        let canal = Canal::new("A");

        connect_single_canal(&mut p2p_server1, &mut p2p_server2, port2, canal.clone()).await?;

        p2p_server1.broadcast(TestMessage("hello".to_string()), canal.clone());

        let evt = loop {
            p2p_server1.listen_next().await;
            if let Ok(evt) = receive_event(&mut p2p_server2, "Should be a TestMessage event").await
            {
                break evt;
            }
        };
        p2p_server2.handle_p2p_tcp_event(evt).await?;

        let tx_attrs = [
            KeyValue::new("direction", "tx"),
            KeyValue::new("result", "ok"),
            KeyValue::new("canal", "A"),
        ];
        let rx_attrs = [
            KeyValue::new("direction", "rx"),
            KeyValue::new("result", "ok"),
            KeyValue::new("canal", "A"),
        ];

        let tx_count = sum_metric_u64(&metrics.reader, "node1", "p2p_server_message", &tx_attrs);
        let rx_count = sum_metric_u64(&metrics.reader, "node2", "p2p_server_message", &rx_attrs);

        assert!(tx_count >= 1, "expected tx count >= 1, got {tx_count}");
        assert!(rx_count >= 1, "expected rx count >= 1, got {rx_count}");
        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn p2p_metrics_broadcast_send_error_counted() -> Result<()> {
        let metrics = test_metrics();
        let ((_, mut p2p_server1), (port2, mut p2p_server2)) = setup_p2p_server_pair().await?;
        let canal = Canal::new("A");

        connect_single_canal(&mut p2p_server1, &mut p2p_server2, port2, canal.clone()).await?;

        let peer_pubkey = p2p_server1.peers.keys().next().cloned().unwrap();
        let socket_addr = p2p_server1
            .find_socket_addr(&canal, &peer_pubkey)
            .cloned()
            .unwrap();
        p2p_server1.tcp_server.drop_peer_stream(socket_addr);

        let message = TestMessage("boom".to_string());
        let message_label = message.message_label();
        let msg = borsh::to_vec(&P2PTcpMessage::Data(message))?;
        let send_errors = p2p_server1
            .actually_send_to(
                HashSet::from_iter(vec![peer_pubkey]),
                &canal,
                msg,
                vec![],
                message_label,
            )
            .await;
        assert!(!send_errors.is_empty(), "expected broadcast send errors");

        let err_attrs = [
            KeyValue::new("direction", "tx"),
            KeyValue::new("result", "error"),
            KeyValue::new("canal", "A"),
        ];
        let err_count = sum_metric_u64(&metrics.reader, "node1", "p2p_server_message", &err_attrs);

        assert!(
            err_count >= 1,
            "expected tx error count >= 1, got {err_count}"
        );
        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn p2p_server_concurrent_handshake_test() -> Result<()> {
        let ((port1, mut p2p_server1), (port2, mut p2p_server2)) = setup_p2p_server_pair().await?;

        // Initiate handshake from p2p_server1 to p2p_server2
        p2p_server1.try_start_connection(format!("127.0.0.1:{port2}"), Canal::new("A"));

        // Initiate handshake from p2p_server2 to p2p_server1
        p2p_server2.try_start_connection(format!("127.0.0.1:{port1}"), Canal::new("A"));

        // For TcpClient to connect
        receive_and_handle_event!(
            &mut p2p_server1,
            P2PTcpEvent::HandShakeTcpClient(_, _, _),
            "Expected HandShake TCP Client connection"
        );
        receive_and_handle_event!(
            &mut p2p_server2,
            P2PTcpEvent::HandShakeTcpClient(_, _, _),
            "Expected HandShake TCP Client connection"
        );

        // For handshake Hello message
        receive_and_handle_event!(
            &mut p2p_server2,
            P2PTcpEvent::TcpEvent(TcpEvent::Message {
                socket_addr: _,
                data: P2PTcpMessage::Handshake(Handshake::Hello(_)),
                ..
            }),
            "Expected HandShake Hello message"
        );
        receive_and_handle_event!(
            &mut p2p_server1,
            P2PTcpEvent::TcpEvent(TcpEvent::Message {
                socket_addr: _,
                data: P2PTcpMessage::Handshake(Handshake::Hello(_)),
                ..
            }),
            "Expected HandShake Hello message"
        );

        // For handshake Verack message
        receive_and_handle_event!(
            &mut p2p_server1,
            P2PTcpEvent::TcpEvent(TcpEvent::Message {
                socket_addr: _,
                data: P2PTcpMessage::Handshake(Handshake::Verack(_)),
                ..
            }),
            "Expected HandShake Verack"
        );
        receive_and_handle_event!(
            &mut p2p_server2,
            P2PTcpEvent::TcpEvent(TcpEvent::Message {
                socket_addr: _,
                data: P2PTcpMessage::Handshake(Handshake::Verack(_)),
                ..
            }),
            "Expected HandShake Verack"
        );

        // Verify that both servers have each other in their peers map
        assert_eq!(p2p_server1.peers.len(), 1);
        assert_eq!(p2p_server2.peers.len(), 1);

        assert_eq!(p2p_server1.peers.values().last().unwrap().canals.len(), 1);
        assert_eq!(p2p_server2.peers.values().last().unwrap().canals.len(), 1);

        // Both peers should have each other's ValidatorPublicKey in their maps
        let p1_peer_key = p2p_server1.peers.keys().next().unwrap();
        let p2_peer_key = p2p_server2.peers.keys().next().unwrap();
        assert_eq!(p1_peer_key.0, p2p_server2.crypto.validator_pubkey().0);
        assert_eq!(p2_peer_key.0, p2p_server1.crypto.validator_pubkey().0);

        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn p2p_server_multi_canal_handshake() -> Result<()> {
        let ((port1, mut p2p_server1), (port2, mut p2p_server2)) = setup_p2p_server_pair().await?;

        // Initiate handshake from p2p_server1 to p2p_server2 on canal A
        p2p_server1.try_start_connection(format!("127.0.0.1:{port2}"), Canal::new("A"));

        // Initiate handshake from p2p_server2 to p2p_server1 on canal A
        p2p_server2.try_start_connection(format!("127.0.0.1:{port1}"), Canal::new("A"));

        // Initiate handshake from p2p_server1 to p2p_server2 on canal B
        p2p_server1.try_start_connection(format!("127.0.0.1:{port2}"), Canal::new("B"));

        // Initiate handshake from p2p_server2 to p2p_server1 on canal B
        p2p_server2.try_start_connection(format!("127.0.0.1:{port1}"), Canal::new("B"));

        // For TcpClient to connect
        receive_and_handle_event!(
            &mut p2p_server1,
            P2PTcpEvent::HandShakeTcpClient(_, _, _),
            "Expected HandShake TCP Client connection"
        );
        receive_and_handle_event!(
            &mut p2p_server2,
            P2PTcpEvent::HandShakeTcpClient(_, _, _),
            "Expected HandShake TCP Client connection"
        );
        receive_and_handle_event!(
            &mut p2p_server1,
            P2PTcpEvent::HandShakeTcpClient(_, _, _),
            "Expected HandShake TCP Client connection"
        );
        receive_and_handle_event!(
            &mut p2p_server2,
            P2PTcpEvent::HandShakeTcpClient(_, _, _),
            "Expected HandShake TCP Client connection"
        );

        // For handshake Hello message
        receive_and_handle_event!(
            &mut p2p_server2,
            P2PTcpEvent::TcpEvent(TcpEvent::Message {
                socket_addr: _,
                data: P2PTcpMessage::Handshake(Handshake::Hello(_)),
                ..
            }),
            "Expected HandShake Hello message canal A"
        );
        receive_and_handle_event!(
            &mut p2p_server2,
            P2PTcpEvent::TcpEvent(TcpEvent::Message {
                socket_addr: _,
                data: P2PTcpMessage::Handshake(Handshake::Hello(_)),
                ..
            }),
            "Expected HandShake Hello message canal B"
        );
        receive_and_handle_event!(
            &mut p2p_server1,
            P2PTcpEvent::TcpEvent(TcpEvent::Message {
                socket_addr: _,
                data: P2PTcpMessage::Handshake(Handshake::Hello(_)),
                ..
            }),
            "Expected HandShake Hello message canal A"
        );
        receive_and_handle_event!(
            &mut p2p_server1,
            P2PTcpEvent::TcpEvent(TcpEvent::Message {
                socket_addr: _,
                data: P2PTcpMessage::Handshake(Handshake::Hello(_)),
                ..
            }),
            "Expected HandShake Hello message canal B"
        );

        // For handshake Verack message
        receive_and_handle_event!(
            &mut p2p_server1,
            P2PTcpEvent::TcpEvent(TcpEvent::Message {
                socket_addr: _,
                data: P2PTcpMessage::Handshake(Handshake::Verack(_)),
                ..
            }),
            "Expected HandShake Verack"
        );
        receive_and_handle_event!(
            &mut p2p_server2,
            P2PTcpEvent::TcpEvent(TcpEvent::Message {
                socket_addr: _,
                data: P2PTcpMessage::Handshake(Handshake::Verack(_)),
                ..
            }),
            "Expected HandShake Verack"
        );

        receive_and_handle_event!(
            &mut p2p_server1,
            P2PTcpEvent::TcpEvent(TcpEvent::Message {
                socket_addr: _,
                data: P2PTcpMessage::Handshake(Handshake::Verack(_)),
                ..
            }),
            "Expected HandShake Verack"
        );
        receive_and_handle_event!(
            &mut p2p_server2,
            P2PTcpEvent::TcpEvent(TcpEvent::Message {
                socket_addr: _,
                data: P2PTcpMessage::Handshake(Handshake::Verack(_)),
                ..
            }),
            "Expected HandShake Verack"
        );
        // Verify that both servers have each other in their peers map
        assert_eq!(p2p_server1.peers.len(), 1);
        assert_eq!(p2p_server2.peers.len(), 1);

        assert_eq!(p2p_server1.peers.values().last().unwrap().canals.len(), 2);
        assert_eq!(p2p_server2.peers.values().last().unwrap().canals.len(), 2);

        assert_eq!(
            HashSet::<String>::from_iter(
                p2p_server1
                    .peers
                    .values()
                    .flat_map(|v| v.canals.values().map(|v2| v2.socket_addr.clone()))
                    .collect::<Vec<_>>()
            ),
            HashSet::from_iter(p2p_server1.tcp_server.connected_clients())
        );
        assert_eq!(
            HashSet::<String>::from_iter(
                p2p_server2
                    .peers
                    .values()
                    .flat_map(|v| v.canals.values().map(|v2| v2.socket_addr.clone()))
                    .collect::<Vec<_>>()
            ),
            HashSet::from_iter(p2p_server2.tcp_server.connected_clients())
        );

        // Both peers should have each other's ValidatorPublicKey in their maps
        let p1_peer_key = p2p_server1.peers.keys().next().unwrap();
        let p2_peer_key = p2p_server2.peers.keys().next().unwrap();
        assert_eq!(p1_peer_key.0, p2p_server2.crypto.validator_pubkey().0);
        assert_eq!(p2_peer_key.0, p2p_server1.crypto.validator_pubkey().0);

        _ = receive_event(&mut p2p_server2, "Should be a Closed event").await;
        _ = receive_event(&mut p2p_server1, "Should be a Closed event").await;
        _ = receive_event(&mut p2p_server2, "Should be a Closed event").await;
        _ = receive_event(&mut p2p_server1, "Should be a Closed event").await;

        // Canal A (1 -> 2)
        p2p_server1.broadcast(TestMessage("blabla".to_string()), Canal::new("A"));

        let evt = loop {
            p2p_server1.listen_next().await; // Process the waiting job.
            if let Ok(evt) = receive_event(&mut p2p_server2, "Should be a TestMessage event").await
            {
                break evt;
            }
        };

        assert!(matches!(
            evt,
            P2PTcpEvent::TcpEvent(TcpEvent::Message {
                socket_addr: _,
                data: P2PTcpMessage::Data(_),
                ..
            }),
        ));

        let P2PTcpEvent::TcpEvent(TcpEvent::Message {
            socket_addr: _,
            data: P2PTcpMessage::Data(data),
            ..
        }) = evt
        else {
            panic!("test");
        };

        assert_eq!(data, TestMessage("blabla".to_string()));

        // Canal B (1 -> 2)
        p2p_server1.broadcast(TestMessage("blabla2".to_string()), Canal::new("B"));
        let evt = loop {
            p2p_server1.listen_next().await; // Process the waiting job.
            if let Ok(evt) = receive_event(&mut p2p_server2, "Should be a TestMessage event").await
            {
                break evt;
            }
        };

        assert!(matches!(
            evt,
            P2PTcpEvent::TcpEvent(TcpEvent::Message {
                socket_addr: _,
                data: P2PTcpMessage::Data(_),
                ..
            }),
        ));

        let P2PTcpEvent::TcpEvent(TcpEvent::Message {
            socket_addr: _,
            data: P2PTcpMessage::Data(data),
            ..
        }) = evt
        else {
            panic!("test");
        };

        assert_eq!(data, TestMessage("blabla2".to_string()));

        // Canal A (2 -> 1)
        p2p_server2.broadcast(TestMessage("babal".to_string()), Canal::new("A"));

        let evt = loop {
            p2p_server2.listen_next().await; // Process the waiting job.
            if let Ok(evt) = receive_event(&mut p2p_server1, "Should be a TestMessage event").await
            {
                break evt;
            }
        };

        assert!(matches!(
            evt,
            P2PTcpEvent::TcpEvent(TcpEvent::Message {
                socket_addr: _,
                data: P2PTcpMessage::Data(_),
                ..
            }),
        ));

        let P2PTcpEvent::TcpEvent(TcpEvent::Message {
            socket_addr: _,
            data: P2PTcpMessage::Data(data),
            ..
        }) = evt
        else {
            panic!("test");
        };

        assert_eq!(data, TestMessage("babal".to_string()));

        // Canal B (2 -> 1)
        p2p_server2.broadcast(TestMessage("babal2".to_string()), Canal::new("B"));

        let evt = loop {
            p2p_server2.listen_next().await; // Process the waiting job.
            if let Ok(evt) = receive_event(&mut p2p_server1, "Should be a TestMessage event").await
            {
                break evt;
            }
        };

        assert!(matches!(
            evt,
            P2PTcpEvent::TcpEvent(TcpEvent::Message {
                socket_addr: _,
                data: P2PTcpMessage::Data(_),
                ..
            })
        ));

        let P2PTcpEvent::TcpEvent(TcpEvent::Message {
            socket_addr: _,
            data: P2PTcpMessage::Data(data),
            ..
        }) = evt
        else {
            panic!("test");
        };

        assert_eq!(data, TestMessage("babal2".to_string()));
        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn p2p_server_reconnection_test() -> Result<()> {
        let ((_, mut p2p_server1), (port2, mut p2p_server2)) = setup_p2p_server_pair().await?;

        // Initial connection
        p2p_server1.try_start_connection(format!("127.0.0.1:{port2}"), Canal::new("A"));

        // Server1 waits for TcpClient to connect
        receive_and_handle_event!(
            &mut p2p_server1,
            P2PTcpEvent::HandShakeTcpClient(_, _, _),
            "Expected HandShake TCP Client connection"
        );
        // Server2 receives Hello message
        receive_and_handle_event!(
            &mut p2p_server2,
            P2PTcpEvent::TcpEvent(TcpEvent::Message {
                socket_addr: _,
                data: P2PTcpMessage::Handshake(Handshake::Hello(_)),
                ..
            }),
            "Expected HandShake Hello message"
        );
        // Server1 receives Verack message
        receive_and_handle_event!(
            &mut p2p_server1,
            P2PTcpEvent::TcpEvent(TcpEvent::Message {
                socket_addr: _,
                data: P2PTcpMessage::Handshake(Handshake::Verack(_)),
                ..
            }),
            "Expected HandShake Verack"
        );

        // Verify initial connection
        assert_eq!(p2p_server1.peers.len(), 1);

        // Simulate disconnection by dropping peer from server2
        p2p_server2.remove_peer(p2p_server1.crypto.validator_pubkey(), Canal::new("A"));

        // Server1 receives Closed message
        receive_and_handle_event!(
            &mut p2p_server1,
            P2PTcpEvent::TcpEvent(TcpEvent::Closed { socket_addr: _ }),
            "Expected Tcp Error message"
        );

        // Server1 waits for TcpClient to reconnect
        receive_and_handle_event!(
            &mut p2p_server1,
            P2PTcpEvent::HandShakeTcpClient(_, _, _),
            "Expected HandShake TCP Client connection"
        );
        // Server2 receives Hello message
        receive_and_handle_event!(
            &mut p2p_server2,
            P2PTcpEvent::TcpEvent(TcpEvent::Message {
                socket_addr: _,
                data: P2PTcpMessage::Handshake(Handshake::Hello(_)),
                ..
            }),
            "Expected HandShake Hello message"
        );
        // Server1 receives Verack message
        receive_and_handle_event!(
            &mut p2p_server1,
            P2PTcpEvent::TcpEvent(TcpEvent::Message {
                socket_addr: _,
                data: P2PTcpMessage::Handshake(Handshake::Verack(_)),
                ..
            }),
            "Expected HandShake Verack"
        );

        assert_eq!(
            p2p_server1.peers.len(),
            1,
            "Server1 should connected to only server2"
        );
        assert_eq!(
            p2p_server2.peers.len(),
            1,
            "Server2 should have reconnected to server1"
        );

        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn p2p_server_reconnects_after_decode_error() -> Result<()> {
        let ((_, mut p2p_server1), (port2, mut p2p_server2)) = setup_p2p_server_pair().await?;

        // Initial connection
        p2p_server1.try_start_connection(format!("127.0.0.1:{port2}"), Canal::new("A"));

        // Server1 waits for TcpClient to connect
        receive_and_handle_event!(
            &mut p2p_server1,
            P2PTcpEvent::HandShakeTcpClient(_, _, _),
            "Expected HandShake TCP Client connection"
        );
        // Server2 receives Hello message
        receive_and_handle_event!(
            &mut p2p_server2,
            P2PTcpEvent::TcpEvent(TcpEvent::Message {
                socket_addr: _,
                data: P2PTcpMessage::Handshake(Handshake::Hello(_)),
                ..
            }),
            "Expected HandShake Hello message"
        );
        // Server1 receives Verack message
        receive_and_handle_event!(
            &mut p2p_server1,
            P2PTcpEvent::TcpEvent(TcpEvent::Message {
                socket_addr: _,
                data: P2PTcpMessage::Handshake(Handshake::Verack(_)),
                ..
            }),
            "Expected HandShake Verack"
        );

        assert_eq!(p2p_server1.peers.len(), 1);
        assert_eq!(p2p_server2.peers.len(), 1);

        let connected = p2p_server1.tcp_server.connected_clients();
        assert_eq!(connected.len(), 1, "Expected a single client socket");
        let socket_addr = connected.first().cloned().unwrap();

        let send_errors = p2p_server1
            .tcp_server
            .raw_send_parallel(vec![socket_addr], vec![255], vec![], "raw")
            .await;
        assert!(send_errors.is_empty(), "Expected raw send to succeed");

        // Server2 should see the decode error and attempt to reconnect.
        receive_and_handle_event!(
            &mut p2p_server2,
            P2PTcpEvent::TcpEvent(TcpEvent::Error {
                socket_addr: _,
                error: _
            }),
            "Expected Tcp Error message"
        );

        // Server2 waits for TcpClient to reconnect
        receive_and_handle_event!(
            &mut p2p_server2,
            P2PTcpEvent::HandShakeTcpClient(_, _, _),
            "Expected HandShake TCP Client connection"
        );
        // Server1 may see the close before the new handshake hello.
        receive_and_handle_until(
            &mut p2p_server1,
            "Expected HandShake Hello message",
            |event| {
                matches!(
                    event,
                    P2PTcpEvent::TcpEvent(TcpEvent::Message {
                        socket_addr: _,
                        data: P2PTcpMessage::Handshake(Handshake::Hello(_)),
                        ..
                    })
                )
            },
        )
        .await?;
        // Server2 may see an intermediate close before the verack.
        receive_and_handle_until(&mut p2p_server2, "Expected HandShake Verack", |event| {
            matches!(
                event,
                P2PTcpEvent::TcpEvent(TcpEvent::Message {
                    socket_addr: _,
                    data: P2PTcpMessage::Handshake(Handshake::Verack(_)),
                    ..
                })
            )
        })
        .await?;

        assert_eq!(p2p_server1.peers.len(), 1);
        assert_eq!(p2p_server2.peers.len(), 1);

        Ok(())
    }
}

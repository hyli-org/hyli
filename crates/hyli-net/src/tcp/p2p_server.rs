use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet},
    sync::Arc,
    task::Poll,
    time::Duration,
};

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
    tcp::{tcp_client::TcpClient, Handshake, TcpHeaders},
};

use super::{tcp_server::TcpServer, Canal, NodeConnectionData, P2PTcpMessage, TcpEvent};

const POISONED_RETRY_INTERVAL: Duration = Duration::from_secs(10);

#[derive(Debug)]
pub enum P2PServerEvent<Msg> {
    NewPeer {
        name: String,
        pubkey: ValidatorPublicKey,
        height: u64,
        da_address: String,
    },
    P2PMessage {
        msg: Msg,
        headers: TcpHeaders,
    },
}

#[derive(Debug)]
pub enum P2PTcpEvent<Data: BorshDeserialize + BorshSerialize> {
    TcpEvent(TcpEvent<Data>),
    HandShakeTcpClient(String, TcpClient<Data, Data>, Canal),
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
    (Result<Vec<u8>, std::io::Error>, TcpHeaders),
);
type CanalJobResult = (
    Canal,
    HashSet<ValidatorPublicKey>,
    (Result<Vec<u8>, std::io::Error>, TcpHeaders),
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
    Msg: std::fmt::Debug + BorshDeserialize + BorshSerialize,
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
    _phantom: std::marker::PhantomData<Msg>,
}

impl<Msg> P2PServer<Msg>
where
    Msg: std::fmt::Debug + BorshDeserialize + BorshSerialize + Send + 'static,
{
    pub async fn new(
        crypto: Arc<BlstCrypto>,
        node_id: String,
        port: u16,
        max_frame_length: Option<usize>,
        node_p2p_public_address: String,
        node_da_public_address: String,
        canals: HashSet<Canal>,
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
            tcp_server: TcpServer::start_with_opts(
                port,
                max_frame_length,
                format!("P2P-{node_id}").as_str(),
            )
            .await?,
            peers: HashMap::new(),
            handshake_clients_tasks: JoinSet::new(),
            peers_ping_ticker: tokio::time::interval(std::time::Duration::from_secs(2)),
            canal_jobs: canals
                .into_iter()
                .map(|canal| (canal, OrderedJoinSet::new()))
                .collect(),
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
            tokio::select! {
                Some(tcp_event) = self.tcp_server.listen_next() => {
                    return P2PTcpEvent::TcpEvent(tcp_event);
                },
                Some(joinset_result) = self.handshake_clients_tasks.join_next() => {
                    match joinset_result {
                        Ok(task_result) =>{
                            if let (public_addr, Ok(tcp_client), canal) = task_result {
                                return P2PTcpEvent::HandShakeTcpClient(public_addr, tcp_client, canal);
                            }
                            else {
                                warn!("Error during TcpClient connection, retrying on {}/{}", task_result.0, task_result.2);
                                _ = self.try_start_connection(task_result.0, task_result.2);
                                continue
                            }
                        },
                        Err(e) =>
                        {
                            debug!("Error during joinset execution of handshake task (probably): {:?}", e);
                            continue
                        }
                    }
                },
                (canal, pubkeys, (data, headers)) = std::future::poll_fn(|cx| Self::poll_hashmap(&mut self.canal_jobs, cx)) => {
                    let Ok(msg) = data else {
                        warn!("Error in canal jobs: {:?}", data);
                        continue
                    };
                    // TODO: handle errors?
                    self.actually_send_to(pubkeys, canal, msg, headers).await;
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
                    dest,
                    data: P2PTcpMessage::Handshake(handshake),
                    ..
                } => self.handle_handshake(dest, handshake).await,
                TcpEvent::Message {
                    dest,
                    data: P2PTcpMessage::Data(msg),
                    headers,
                } => {
                    let mut canal_label: Option<Canal> = None;
                    if let Some((_peer_pubkey, canal, peer_info, _)) =
                        self.get_peer_by_socket_addr(&dest)
                    {
                        self.metrics.message_received(
                            peer_info.node_connection_data.p2p_public_address.clone(),
                            canal.clone(),
                        );
                        canal_label = Some(canal.clone());
                    }
                    trace!(
                        "P2PServer delivering TcpEvent::Message canal={} dest={} node={}",
                        canal_label
                            .as_ref()
                            .map(|c| c.to_string())
                            .unwrap_or_else(|| "unknown".into()),
                        dest,
                        self.node_id
                    );
                    Ok(Some(P2PServerEvent::P2PMessage { msg, headers }))
                }
                TcpEvent::Error { dest, error } => {
                    if let Some((peer_pubkey, _canal, peer_info, _)) =
                        self.get_peer_by_socket_addr(&dest)
                    {
                        self.metrics.message_error(
                            peer_info.node_connection_data.p2p_public_address.clone(),
                            _canal.clone(),
                        );
                        warn!(
                            "P2P TCP error on socket {} (node={}, peer={}, canal={})",
                            dest, self.node_id, peer_pubkey, _canal
                        );
                    }
                    warn!(
                        "P2P TCP error on socket {} (node={}): {}",
                        dest, self.node_id, error
                    );
                    self.handle_error_event(dest, error).await;
                    Ok(None)
                }
                TcpEvent::Closed { dest } => {
                    if let Some((peer_pubkey, canal, peer_info, _)) =
                        self.get_peer_by_socket_addr(&dest)
                    {
                        self.metrics.message_closed(
                            peer_info.node_connection_data.p2p_public_address.clone(),
                            canal.clone(),
                        );
                        warn!(
                            "P2P TCP closed on socket {} (node={}, peer={}, canal={})",
                            dest, self.node_id, peer_pubkey, canal
                        );
                    }
                    self.handle_closed_event(dest);
                    Ok(None)
                }
            },
            P2PTcpEvent::HandShakeTcpClient(public_addr, tcp_client, canal) => {
                if let Err(e) = self
                    .do_handshake(public_addr.clone(), tcp_client, canal.clone())
                    .await
                {
                    warn!("Error during handshake: {:?}", e);
                    let _ = self.try_start_connection(public_addr, canal);
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
                            .map(|poisoned_at| now.clone() - poisoned_at >= POISONED_RETRY_INTERVAL)
                            .unwrap_or(true);
                        if should_retry {
                            if let Err(e) =
                                self.try_start_connection_for_peer(&pubkey, canal.clone())
                            {
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

    pub fn get_peer_by_socket_addr(
        &self,
        dest: &String,
    ) -> Option<(&ValidatorPublicKey, &Canal, &PeerInfo, &PeerSocket)> {
        self.peers.iter().find_map(|(pubkey, peer_info)| {
            peer_info.canals.iter().find_map(|(canal, peer_socket)| {
                (&peer_socket.socket_addr == dest).then_some((
                    pubkey,
                    canal,
                    peer_info,
                    peer_socket,
                ))
            })
        })
    }

    fn mark_socket_poisoned(&mut self, dest: &String) {
        let mut target = self
            .get_peer_by_socket_addr(dest)
            .map(|(pubkey, canal, _, _)| (pubkey.clone(), canal.clone()));

        if target.is_none() {
            if let Some((public_addr, canal_name)) = dest.split_once('/') {
                if let Some((pubkey, _)) = self
                    .peers
                    .iter()
                    .find(|(_, info)| info.node_connection_data.p2p_public_address == public_addr)
                {
                    target = Some((pubkey.clone(), Canal::new(canal_name.to_string())));
                }
            }
        }

        if let Some((pubkey, canal)) = target {
            if let Some(peer_socket) = self.get_socket_mut(&canal, &pubkey) {
                peer_socket.poisoned_at = Some(TimestampMsClock::now());
            }
        }
    }

    async fn handle_error_event(
        &mut self,
        dest: String,
        _error: String,
    ) -> Option<P2PServerEvent<Msg>> {
        warn!(
            "Error with peer connection on {} (node={}): {:?}",
            dest, self.node_id, _error
        );
        // There was an error with the connection with the peer. We try to reconnect.

        // TODO: An error can happen when a message was no *sent* correctly. Investigate how to handle that specific case
        // TODO: match the error type to decide what to do
        self.mark_socket_poisoned(&dest);
        self.tcp_server.drop_peer_stream(dest.clone());
        let retry_target =
            self.get_peer_by_socket_addr(&dest)
                .map(|(peer_pubkey, canal, info, _)| {
                    (
                        peer_pubkey.clone(),
                        canal.clone(),
                        info.node_connection_data.name.clone(),
                    )
                });

        if let Some((peer_pubkey, canal, peer_name)) = retry_target {
            trace!(
                "Will retry connection to peer {} canal {} after error {}",
                peer_name,
                canal,
                dest
            );
            if let Err(e) = self.try_start_connection_for_peer(&peer_pubkey, canal.clone()) {
                warn!(
                    "Problem when retrying connection to peer {} after error: {}",
                    peer_pubkey, e
                );
            }
        }
        None
    }

    fn handle_closed_event(&mut self, dest: String) {
        // TODO: investigate how to properly handle this case
        // The connection has been closed by peer. We remove the peer and try to reconnect.
        debug!("Peer connection closed on {} (node={})", dest, self.node_id);

        // When we receive a close event
        // It is a closed connection that need to be removed from tcp server clients in all cases
        // If it is a connection matching a canal/peer, it means we can retry
        self.mark_socket_poisoned(&dest);
        self.tcp_server.drop_peer_stream(dest.clone());
        let retry_target =
            self.get_peer_by_socket_addr(&dest)
                .map(|(peer_pubkey, canal, info, _)| {
                    (
                        peer_pubkey.clone(),
                        canal.clone(),
                        info.node_connection_data.name.clone(),
                    )
                });

        if let Some((peer_pubkey, canal, peer_name)) = retry_target {
            trace!(
                "Will retry connection to peer {} canal {} after close {}",
                peer_name,
                canal,
                dest
            );
            if let Err(e) = self.try_start_connection_for_peer(&peer_pubkey, canal.clone()) {
                warn!(
                    "Problem when retrying connection to peer {} after close: {}",
                    peer_pubkey, e
                );
            }
        } else {
            warn!(
                "Closed socket {} did not map to a known peer/canal on node={}",
                dest, self.node_id
            );
        }
    }

    async fn handle_handshake(
        &mut self,
        dest: String,
        handshake: Handshake,
    ) -> anyhow::Result<Option<P2PServerEvent<Msg>>> {
        match handshake {
            Handshake::Hello((canal, v, timestamp)) => {
                self.metrics
                    .handshake_hello_received(v.msg.p2p_public_address.clone(), canal.clone());

                // Verify message signature
                BlstCrypto::verify(&v).context("Error verifying Hello message")?;

                info!(
                    "👋 [{}] Processing Hello handshake message {:?}",
                    canal, v.msg
                );
                match self.create_signed_node_connection_data() {
                    Ok(verack) => {
                        // Send Verack response
                        if let Err(e) = self
                            .tcp_server
                            .send(
                                dest.clone(),
                                P2PTcpMessage::<Msg>::Handshake(Handshake::Verack((
                                    canal.clone(),
                                    verack,
                                    timestamp.clone(),
                                ))),
                                vec![],
                            )
                            .await
                        {
                            bail!("Error sending Verack message to {dest}: {:?}", e);
                        }

                        self.metrics.handshake_verack_emitted(
                            v.msg.p2p_public_address.clone(),
                            canal.clone(),
                        );
                    }
                    Err(e) => {
                        bail!("Error creating signed node connection data: {:?}", e);
                    }
                }

                Ok(self.handle_peer_update(canal, &v, timestamp, dest))
            }
            Handshake::Verack((canal, v, timestamp)) => {
                self.metrics
                    .handshake_verack_received(v.msg.p2p_public_address.clone(), canal.clone());

                // Verify message signature
                BlstCrypto::verify(&v).context("Error verifying Verack message")?;

                info!(
                    "👋 [{}] Processing Verack handshake message {:?}",
                    canal, v.msg
                );
                Ok(self.handle_peer_update(canal, &v, timestamp, dest))
            }
        }
    }

    fn handle_peer_update(
        &mut self,
        canal: Canal,
        v: &SignedByValidator<NodeConnectionData>,
        timestamp: TimestampMs,
        dest: String,
    ) -> Option<P2PServerEvent<Msg>> {
        let peer_pubkey = v.signature.validator.clone();

        // in case timestamps are equal -_-
        let local_pubkey = self.crypto.validator_pubkey().clone();

        self.connecting
            .remove(&(v.msg.p2p_public_address.clone(), canal.clone()));

        if let Some(peer_socket) = self.get_socket_mut(&canal, &peer_pubkey) {
            let (peer_addr_to_drop, kept_socket) = if peer_socket.timestamp < timestamp || {
                peer_socket.timestamp == timestamp
                    && local_pubkey.cmp(&peer_pubkey) == Ordering::Less
            } {
                debug!(
                    "Local peer {}/{} ({}): dropping socket {} in favor of more recent one {}",
                    v.msg.p2p_public_address, canal, peer_pubkey, peer_socket.socket_addr, dest
                );
                let socket_addr = peer_socket.socket_addr.clone();
                peer_socket.timestamp = timestamp;
                peer_socket.socket_addr = dest.clone();
                peer_socket.poisoned_at = None;
                (socket_addr.clone(), dest.clone())
            } else {
                debug!(
                    "Local peer {}/{} ({}): keeping socket {} and discard too old {}",
                    v.msg.p2p_public_address, canal, peer_pubkey, peer_socket.socket_addr, dest
                );
                peer_socket.poisoned_at = None;
                (dest.clone(), peer_socket.socket_addr.clone())
            };
            trace!(
                "Updating existing canal {} for peer {} on node {} -> dropping {} keeping {}",
                canal,
                peer_pubkey,
                self.node_id,
                peer_addr_to_drop,
                kept_socket
            );
            self.tcp_server.drop_peer_stream(peer_addr_to_drop);
            None
        } else {
            // If the validator exists, but not this canal, we create it
            if let Some(validator) = self.peers.get_mut(&peer_pubkey) {
                debug!(
                    "Local peer {}/{} ({}): creating canal for existing peer on socket {}",
                    v.msg.p2p_public_address, canal, peer_pubkey, dest
                );
                trace!(
                    "Node {} adding new canal {} to existing peer {} on socket {}",
                    self.node_id,
                    canal,
                    peer_pubkey,
                    dest
                );
                validator.canals.insert(
                    canal.clone(),
                    PeerSocket {
                        timestamp,
                        socket_addr: dest,
                        poisoned_at: None,
                    },
                );
            }
            // If the validator was never created before
            else {
                debug!(
                    "Local peer {}/{} ({}): creating new peer and canal on socket {}",
                    v.msg.p2p_public_address, canal, peer_pubkey, dest
                );
                trace!(
                    "Node {} storing new peer {} on canal {} via socket {}",
                    self.node_id,
                    peer_pubkey,
                    canal,
                    dest
                );
                let peer_info = PeerInfo {
                    canals: HashMap::from_iter(vec![(
                        canal.clone(),
                        PeerSocket {
                            timestamp,
                            socket_addr: dest,
                            poisoned_at: None,
                        },
                    )]),
                    node_connection_data: v.msg.clone(),
                };

                self.peers.insert(peer_pubkey.clone(), peer_info);
            }

            self.metrics.peers_snapshot(self.peers.len() as u64);
            tracing::info!("New peer connected on canal {}: {}", canal, peer_pubkey);
            Some(P2PServerEvent::NewPeer {
                name: v.msg.name.to_string(),
                pubkey: v.signature.validator.clone(),
                da_address: v.msg.da_public_address.clone(),
                height: v.msg.current_height,
            })
        }
    }

    pub fn remove_peer(&mut self, peer_pubkey: &ValidatorPublicKey, canal: Canal) {
        if let Some(peer_info) = self.peers.get_mut(peer_pubkey) {
            if let Some(removed) = peer_info.canals.remove(&canal) {
                self.tcp_server
                    .drop_peer_stream(removed.socket_addr.clone());
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

        tracing::info!(
            "Attempt to reconnect to {}/{}",
            peer.node_connection_data.p2p_public_address,
            canal
        );

        self.try_start_connection(peer.node_connection_data.p2p_public_address.clone(), canal)?;

        Ok(())
    }

    /// Checks if creating a fresh tcp client is relevant and do it if so
    /// Start a task, cancellation safe
    pub fn try_start_connection(
        &mut self,
        peer_address: String,
        canal: Canal,
    ) -> anyhow::Result<()> {
        trace!(
            "try_start_connection request to {} on canal {} (node={}, connecting={})",
            peer_address,
            canal,
            self.node_id,
            self.connecting.len()
        );
        if peer_address == self.node_p2p_public_address {
            trace!("Trying to connect to self");
            return Ok(());
        }

        let now = TimestampMsClock::now();

        // A connection is already started for this public address ? If it is too old, let 's try retry one
        // If it is recent, let's wait for it to finish
        if let Some(ongoing) = self.connecting.get(&(peer_address.clone(), canal.clone())) {
            match ongoing {
                HandshakeOngoing::TcpClientStartedAt(last_connect_attempt, abort_handle) => {
                    if now.clone() - last_connect_attempt.clone() < Duration::from_secs(3) {
                        {
                            return Ok(());
                        }
                    }
                    abort_handle.abort();
                }
                HandshakeOngoing::HandshakeStartedAt(addr, last_handshake_started_at) => {
                    if now.clone() - last_handshake_started_at.clone() < Duration::from_secs(3) {
                        {
                            return Ok(());
                        }
                    }
                    self.tcp_server.drop_peer_stream(addr.to_string());
                }
            }
        }

        self.start_connection_task(peer_address, canal);
        Ok(())
    }

    /// Creates a task that attempts to create a tcp client
    pub fn start_connection_task(&mut self, peer_address: String, canal: Canal) {
        let mfl = self.max_frame_length;
        let now = TimestampMsClock::now();
        let peer_address_clone = peer_address.clone();
        let canal_clone = canal.clone();

        tracing::info!("Starting connecting to {}/{}", peer_address, canal);

        let abort_handle = self.handshake_clients_tasks.spawn(async move {
            let handshake_task = TcpClient::connect_with_opts(
                "p2p_server_handshake",
                mfl,
                peer_address_clone.clone(),
            );

            let result = handshake_task.await;
            (peer_address_clone, result, canal_clone)
        });

        self.connecting.insert(
            (peer_address.clone(), canal.clone()),
            HandshakeOngoing::TcpClientStartedAt(now, abort_handle),
        );

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
            "Doing handshake on {}({})/{}",
            public_addr,
            tcp_client.socket_addr.to_string(),
            canal
        );

        let addr = format!("{public_addr}/{canal}");

        self.connecting.insert(
            (public_addr.clone(), canal.clone()),
            HandshakeOngoing::HandshakeStartedAt(addr.clone(), timestamp.clone()),
        );

        self.tcp_server.setup_client(addr.clone(), tcp_client);
        self.tcp_server
            .send(
                addr,
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
                "Trying to send message to unknown Peer {}/{}. Unable to proceed.",
                validator_pub_key, canal
            );
            return Ok(());
        };

        let Some(peer_socket) = peer.canals.get(&canal) else {
            warn!(
                "Peer {} has no socket for canal {} on node {}; available canals: {:?}",
                validator_pub_key,
                canal,
                self.node_id,
                peer.canals.keys().collect::<Vec<_>>()
            );
            return Ok(());
        };
        let socket_addr = peer_socket.socket_addr.clone();
        if peer_socket.poisoned_at.is_some() {
            debug!(
                "Peer {} socket {} for canal {} is poisoned; skipping send on node {}",
                validator_pub_key, socket_addr, canal, self.node_id
            );
            return Ok(());
        }

        let headers = crate::tcp::headers_from_span();
        if let Some(jobs) = self.canal_jobs.get_mut(&canal) {
            if !jobs.is_empty() {
                jobs.spawn(async move {
                    (
                        HashSet::from_iter(std::iter::once(validator_pub_key)),
                        (borsh::to_vec(&P2PTcpMessage::Data(msg)), headers),
                    )
                });
                return Ok(());
            }
        }

        let pub_addr = peer.node_connection_data.p2p_public_address.clone();

        if let Err(e) = self
            .tcp_server
            .send(
                socket_addr.clone(),
                P2PTcpMessage::Data(msg),
                headers,
            )
            .await
        {
            self.mark_socket_poisoned(&socket_addr);
            self.try_start_connection_for_peer(&validator_pub_key, canal)
                .context(format!(
                    "Re-handshaking after message sending error with peer {validator_pub_key}"
                ))?;
            bail!(
                "Failed to send message to peer {}: {:?}",
                validator_pub_key,
                e
            );
        }

        self.metrics.message_emitted(pub_addr, canal);
        Ok(())
    }

    #[cfg_attr(feature = "instrumentation", tracing::instrument(skip(self)))]
    pub fn broadcast(&mut self, msg: Msg, canal: Canal) {
        let Some(jobs) = self.canal_jobs.get_mut(&canal) else {
            error!("Canal {:?} does not exist in P2P server", canal);
            return;
        };
        let peers = self.peers.keys().cloned().collect();
        let headers = crate::tcp::headers_from_span();
        jobs.spawn(async move { (peers, (borsh::to_vec(&P2PTcpMessage::Data(msg)), headers)) });
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
            return;
        };
        let peers = only_for.clone();
        let headers = crate::tcp::headers_from_span();
        jobs.spawn(async move { (peers, (borsh::to_vec(&P2PTcpMessage::Data(msg)), headers)) });
    }

    async fn actually_send_to(
        &mut self,
        only_for: HashSet<ValidatorPublicKey>,
        canal: Canal,
        msg: Vec<u8>,
        headers: TcpHeaders,
    ) -> HashMap<ValidatorPublicKey, anyhow::Error> {
        let peer_addr_to_pubkey: HashMap<String, ValidatorPublicKey> = self
            .peers
            .iter()
            .filter_map(|(pubkey, peer)| {
                if only_for.contains(pubkey) {
                    peer.canals.get(&canal).and_then(|socket| {
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
            .raw_send_parallel(peer_addr_to_pubkey.keys().cloned().collect(), msg, headers)
            .await;

        HashMap::from_iter(res.into_iter().filter_map(|(k, v)| {
            peer_addr_to_pubkey.get(&k).map(|pubkey| {
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

    use anyhow::Result;
    use borsh::{BorshDeserialize, BorshSerialize};
    use hyli_crypto::BlstCrypto;
    use tokio::net::TcpListener;

    use crate::tcp::{p2p_server::P2PServer, Canal, Handshake, P2PTcpMessage, TcpEvent};

    use super::P2PTcpEvent;

    pub async fn find_available_port() -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        addr.port()
    }
    // Simple message type for testing
    #[derive(Debug, Clone, BorshSerialize, BorshDeserialize, Eq, PartialEq, PartialOrd, Ord)]
    pub struct TestMessage(String);

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

    #[test_log::test(tokio::test)]
    async fn p2p_server_concurrent_handshake_test() -> Result<()> {
        let ((port1, mut p2p_server1), (port2, mut p2p_server2)) = setup_p2p_server_pair().await?;

        // Initiate handshake from p2p_server1 to p2p_server2
        _ = p2p_server1.try_start_connection(format!("127.0.0.1:{port2}"), Canal::new("A"));

        // Initiate handshake from p2p_server2 to p2p_server1
        _ = p2p_server2.try_start_connection(format!("127.0.0.1:{port1}"), Canal::new("A"));

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
                dest: _,
                data: P2PTcpMessage::Handshake(Handshake::Hello(_)),
                ..
            }),
            "Expected HandShake Hello message"
        );
        receive_and_handle_event!(
            &mut p2p_server1,
            P2PTcpEvent::TcpEvent(TcpEvent::Message {
                dest: _,
                data: P2PTcpMessage::Handshake(Handshake::Hello(_)),
                ..
            }),
            "Expected HandShake Hello message"
        );

        // For handshake Verack message
        receive_and_handle_event!(
            &mut p2p_server1,
            P2PTcpEvent::TcpEvent(TcpEvent::Message {
                dest: _,
                data: P2PTcpMessage::Handshake(Handshake::Verack(_)),
                ..
            }),
            "Expected HandShake Verack"
        );
        receive_and_handle_event!(
            &mut p2p_server2,
            P2PTcpEvent::TcpEvent(TcpEvent::Message {
                dest: _,
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
        let _ = p2p_server1.try_start_connection(format!("127.0.0.1:{port2}"), Canal::new("A"));

        // Initiate handshake from p2p_server2 to p2p_server1 on canal A
        let _ = p2p_server2.try_start_connection(format!("127.0.0.1:{port1}"), Canal::new("A"));

        // Initiate handshake from p2p_server1 to p2p_server2 on canal B
        let _ = p2p_server1.try_start_connection(format!("127.0.0.1:{port2}"), Canal::new("B"));

        // Initiate handshake from p2p_server2 to p2p_server1 on canal B
        let _ = p2p_server2.try_start_connection(format!("127.0.0.1:{port1}"), Canal::new("B"));

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
                dest: _,
                data: P2PTcpMessage::Handshake(Handshake::Hello(_)),
                ..
            }),
            "Expected HandShake Hello message canal A"
        );
        receive_and_handle_event!(
            &mut p2p_server2,
            P2PTcpEvent::TcpEvent(TcpEvent::Message {
                dest: _,
                data: P2PTcpMessage::Handshake(Handshake::Hello(_)),
                ..
            }),
            "Expected HandShake Hello message canal B"
        );
        receive_and_handle_event!(
            &mut p2p_server1,
            P2PTcpEvent::TcpEvent(TcpEvent::Message {
                dest: _,
                data: P2PTcpMessage::Handshake(Handshake::Hello(_)),
                ..
            }),
            "Expected HandShake Hello message canal A"
        );
        receive_and_handle_event!(
            &mut p2p_server1,
            P2PTcpEvent::TcpEvent(TcpEvent::Message {
                dest: _,
                data: P2PTcpMessage::Handshake(Handshake::Hello(_)),
                ..
            }),
            "Expected HandShake Hello message canal B"
        );

        // For handshake Verack message
        receive_and_handle_event!(
            &mut p2p_server1,
            P2PTcpEvent::TcpEvent(TcpEvent::Message {
                dest: _,
                data: P2PTcpMessage::Handshake(Handshake::Verack(_)),
                ..
            }),
            "Expected HandShake Verack"
        );
        receive_and_handle_event!(
            &mut p2p_server2,
            P2PTcpEvent::TcpEvent(TcpEvent::Message {
                dest: _,
                data: P2PTcpMessage::Handshake(Handshake::Verack(_)),
                ..
            }),
            "Expected HandShake Verack"
        );

        receive_and_handle_event!(
            &mut p2p_server1,
            P2PTcpEvent::TcpEvent(TcpEvent::Message {
                dest: _,
                data: P2PTcpMessage::Handshake(Handshake::Verack(_)),
                ..
            }),
            "Expected HandShake Verack"
        );
        receive_and_handle_event!(
            &mut p2p_server2,
            P2PTcpEvent::TcpEvent(TcpEvent::Message {
                dest: _,
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
                dest: _,
                data: P2PTcpMessage::Data(_),
                ..
            }),
        ));

        let P2PTcpEvent::TcpEvent(TcpEvent::Message {
            dest: _,
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
                dest: _,
                data: P2PTcpMessage::Data(_),
                ..
            }),
        ));

        let P2PTcpEvent::TcpEvent(TcpEvent::Message {
            dest: _,
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
                dest: _,
                data: P2PTcpMessage::Data(_),
                ..
            }),
        ));

        let P2PTcpEvent::TcpEvent(TcpEvent::Message {
            dest: _,
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
                dest: _,
                data: P2PTcpMessage::Data(_),
                ..
            })
        ));

        let P2PTcpEvent::TcpEvent(TcpEvent::Message {
            dest: _,
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
        let _ = p2p_server1.try_start_connection(format!("127.0.0.1:{port2}"), Canal::new("A"));

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
                dest: _,
                data: P2PTcpMessage::Handshake(Handshake::Hello(_)),
                ..
            }),
            "Expected HandShake Hello message"
        );
        // Server1 receives Verack message
        receive_and_handle_event!(
            &mut p2p_server1,
            P2PTcpEvent::TcpEvent(TcpEvent::Message {
                dest: _,
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
            P2PTcpEvent::TcpEvent(TcpEvent::Closed { dest: _ }),
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
                dest: _,
                data: P2PTcpMessage::Handshake(Handshake::Hello(_)),
                ..
            }),
            "Expected HandShake Hello message"
        );
        // Server1 receives Verack message
        receive_and_handle_event!(
            &mut p2p_server1,
            P2PTcpEvent::TcpEvent(TcpEvent::Message {
                dest: _,
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
        let _ = p2p_server1.try_start_connection(format!("127.0.0.1:{port2}"), Canal::new("A"));

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
                dest: _,
                data: P2PTcpMessage::Handshake(Handshake::Hello(_)),
                ..
            }),
            "Expected HandShake Hello message"
        );
        // Server1 receives Verack message
        receive_and_handle_event!(
            &mut p2p_server1,
            P2PTcpEvent::TcpEvent(TcpEvent::Message {
                dest: _,
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
            .raw_send_parallel(vec![socket_addr], vec![255], vec![])
            .await;
        assert!(send_errors.is_empty(), "Expected raw send to succeed");

        // Server2 should see the decode error and attempt to reconnect.
        receive_and_handle_event!(
            &mut p2p_server2,
            P2PTcpEvent::TcpEvent(TcpEvent::Error { dest: _, error: _ }),
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
                        dest: _,
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
                    dest: _,
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

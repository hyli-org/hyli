use std::{
    collections::HashMap,
    io::ErrorKind,
    marker::PhantomData,
    net::Ipv4Addr,
    sync::{Arc, RwLock},
    time::Duration,
};

use anyhow::Context;
use borsh::{BorshDeserialize, BorshSerialize};
use bytes::Bytes;
use futures::stream::{SplitSink, SplitStream};
use futures::{FutureExt, SinkExt, StreamExt};
use tokio::sync::mpsc::{Receiver, Sender};

#[cfg(feature = "turmoil")]
use crate::tcp::intercept;
use crate::{
    clock::TimestampMsClock,
    logged_task::logged_task,
    metrics::TcpServerMetrics,
    net::TcpListener,
    tcp::{
        decode_tcp_payload, framed_stream, to_tcp_message, to_tcp_message_with_headers,
        FramedStream, TcpData, TcpHeaders, TcpMessage, TcpMessageLabel, TcpOutboundMessage,
    },
};
use hyli_deterministic::collections::DeterministicMap;
use tracing::{debug, error, trace, warn};

use super::{tcp_client::TcpClient, SocketStream, TcpEvent};

type TcpSender = SplitSink<FramedStream, Bytes>;
type TcpReceiver = SplitStream<FramedStream>;

fn peer_label_or_addr(peer_label: &RwLock<String>, socket_addr: &str) -> String {
    match peer_label.read() {
        Ok(guard) => guard.clone(),
        Err(err) => {
            warn!("Failed to read peer label: {}", err);
            socket_addr.to_string()
        }
    }
}

#[derive(Clone, Debug)]
pub struct TcpServerOptions {
    pub max_frame_length: Option<usize>,
    pub send_timeout: Duration,
}

impl Default for TcpServerOptions {
    fn default() -> Self {
        Self {
            max_frame_length: None,
            send_timeout: Duration::from_secs(10),
        }
    }
}

pub struct TcpServer<Req, Res>
where
    Res: BorshSerialize + std::fmt::Debug,
    Req: BorshDeserialize + std::fmt::Debug,
{
    pool_name: String,
    tcp_listener: TcpListener,
    max_frame_length: Option<usize>,
    send_timeout: Duration,
    pool_sender: Sender<Box<TcpEvent<Req>>>,
    pool_receiver: Receiver<Box<TcpEvent<Req>>>,
    ping_sender: Sender<String>,
    ping_receiver: Receiver<String>,
    sockets: DeterministicMap<String, SocketStream>,
    metrics: TcpServerMetrics,
    _marker: PhantomData<(Req, Res)>,
}

impl<Req, Res> TcpServer<Req, Res>
where
    Req: BorshSerialize + BorshDeserialize + std::fmt::Debug + Send + TcpMessageLabel + 'static,
    Res: BorshSerialize + BorshDeserialize + std::fmt::Debug + TcpMessageLabel,
{
    pub async fn start(port: u16, pool_name: &str) -> anyhow::Result<Self> {
        Self::start_with_options(port, pool_name, TcpServerOptions::default()).await
    }

    pub async fn start_with_opts(
        port: u16,
        max_frame_length: Option<usize>,
        pool_name: &str,
    ) -> anyhow::Result<Self> {
        Self::start_with_options(
            port,
            pool_name,
            TcpServerOptions {
                max_frame_length,
                ..Default::default()
            },
        )
        .await
    }

    pub async fn start_with_options(
        port: u16,
        pool_name: &str,
        options: TcpServerOptions,
    ) -> anyhow::Result<Self> {
        let tcp_listener = TcpListener::bind(&(Ipv4Addr::UNSPECIFIED, port)).await?;
        let (pool_sender, pool_receiver) = tokio::sync::mpsc::channel(100);
        let (ping_sender, ping_receiver) = tokio::sync::mpsc::channel(100);
        debug!(
            "Starting TcpConnectionPool {}, listening for stream requests on {} with max_frame_len: {:?}",
            &pool_name, port, options.max_frame_length
        );
        Ok(TcpServer {
            pool_name: pool_name.to_string(),
            sockets: DeterministicMap::new(),
            max_frame_length: options.max_frame_length,
            send_timeout: options.send_timeout,
            tcp_listener,
            pool_sender,
            pool_receiver,
            ping_sender,
            ping_receiver,
            metrics: TcpServerMetrics::global(pool_name.to_string()),
            _marker: PhantomData,
        })
    }

    pub async fn listen_next(&mut self) -> Option<TcpEvent<Req>> {
        loop {
            hyli_deterministic::tokio_select_biased! {
                Ok((stream, socket_addr)) = self.tcp_listener.accept() => {
                    if let Some(len) = self.max_frame_length {
                        debug!("Setting max frame length to {}", len);
                    }
                    let (sender, receiver) = framed_stream(stream, self.max_frame_length).split();
                    self.setup_stream(sender, receiver, &socket_addr.to_string());
                }

                Some(socket_addr) = self.ping_receiver.recv() => {
                    trace!("Received ping from {}", socket_addr);
                    if let Some(socket) = self.sockets.get_mut(&socket_addr) {
                        socket.last_ping = TimestampMsClock::now();
                    }
                }
                message = self.pool_receiver.recv() => {
                    let queued = self.pool_receiver.len();
                    if let Some(msg) = message.as_ref() {
                        match msg.as_ref() {
                            TcpEvent::Message { socket_addr, .. } => trace!(pool = %self.pool_name, "TcpServer event queue: message for {} ({} remaining)", socket_addr, queued),
                            TcpEvent::Closed { socket_addr } => trace!(pool = %self.pool_name, "TcpServer event queue: closed for {} ({} remaining)", socket_addr, queued),
                            TcpEvent::Error { socket_addr, error } => trace!(pool = %self.pool_name, "TcpServer event queue: error for {}: {} ({} remaining)", socket_addr, error, queued),
                        }
                    }
                    return message.map(|message| *message);
                }
            }
        }
    }

    #[cfg(test)]
    /// Local_addr of the underlying tcp_listener
    pub fn local_addr(&self) -> anyhow::Result<std::net::SocketAddr> {
        self.tcp_listener
            .local_addr()
            .context("Getting local_addr from TcpListener in TcpServer")
    }

    /// Adresses of currently connected clients (no health check)
    pub fn connected_clients(&self) -> Vec<String> {
        self.sockets.keys().cloned().collect::<Vec<String>>()
    }

    pub async fn broadcast(&mut self, msg: Res) -> HashMap<String, anyhow::Error> {
        let mut tasks = vec![];

        let message_label = msg.message_label();
        let Ok(binary_data) = to_tcp_message(&msg) else {
            return self
                .sockets
                .iter()
                .map(|addr| {
                    (
                        addr.0.clone(),
                        anyhow::anyhow!("Failed to serialize message"),
                    )
                })
                .collect();
        };
        debug!("Broadcasting msg {:?} to all", binary_data);
        for (name, socket) in self.sockets.iter_mut() {
            debug!(pool = %self.pool_name, " - to {}", name);
            tasks.push(
                socket
                    .sender
                    .send(TcpOutboundMessage {
                        message: binary_data.clone(),
                        message_label,
                    })
                    .map(|res| (name.clone(), res)),
            );
        }

        let all = futures::future::join_all(tasks).await;

        HashMap::from_iter(all.into_iter().filter_map(|(client_name, send_result)| {
            send_result.err().map(|error| {
                (
                    client_name.clone(),
                    anyhow::anyhow!(
                        "Sending message to client {} (pool={}): {}",
                        client_name,
                        self.pool_name,
                        error
                    ),
                )
            })
        }))
    }

    pub async fn raw_send_parallel(
        &mut self,
        socket_addrs: Vec<String>,
        msg: Vec<u8>,
        headers: TcpHeaders,
    ) -> HashMap<String, anyhow::Error> {
        // Getting targetted addrs that are not in the connected sockets list
        let unknown_socket_addrs = {
            let mut res = socket_addrs.clone();
            res.retain(|addr| !self.sockets.contains_key(addr));
            res
        };

        // Send the message to all targets concurrently and wait for them to finish.
        let all_sent = {
            let message = TcpMessage::Data(TcpData::with_headers(msg, headers));
            debug!("Broadcasting msg {:?} to all", message);
            let message_label = "raw";
            let mut tasks = vec![];
            for socket_addr in socket_addrs.iter() {
                if let Some(socket) = self.sockets.get_mut(socket_addr) {
                    let sender = socket.sender.clone();
                    let socket_addr = socket_addr.clone();
                    let message = message.clone();
                    debug!(" - to {}", socket_addr);
                    tasks.push(async move {
                        let res = sender
                            .send(TcpOutboundMessage {
                                message: message.clone(),
                                message_label,
                            })
                            .await;
                        (socket_addr, res)
                    });
                }
            }
            futures::future::join_all(tasks).await
        };

        // Regroup future results in a map keyed with addrs
        let mut result = HashMap::from_iter(all_sent.into_iter().filter_map(
            |(client_name, send_result)| {
                send_result.err().map(|error| {
                    (
                        client_name.clone(),
                        anyhow::anyhow!("Sending message to client {}: {}", client_name, error),
                    )
                })
            },
        ));

        // Filling the map with errors for unknown targets
        for unknown in unknown_socket_addrs {
            result.insert(
                unknown.clone(),
                anyhow::anyhow!("Unknown socket_addr {}", unknown),
            );
        }

        result
    }
    pub async fn send(
        &mut self,
        socket_addr: String,
        msg: Res,
        headers: TcpHeaders,
    ) -> anyhow::Result<()> {
        debug!(pool = %self.pool_name, "Sending msg {:?} to {}", msg, socket_addr);
        let message_label = msg.message_label();
        let stream = self
            .sockets
            .get_mut(&socket_addr)
            .context(format!("Retrieving client {socket_addr}"))?;

        let binary_data = to_tcp_message_with_headers(&msg, headers)?;
        stream
            .sender
            .send(TcpOutboundMessage {
                message: binary_data,
                message_label,
            })
            .await
            .map_err(|e| anyhow::anyhow!("Sending msg to client {}: {}", socket_addr, e))
    }

    pub fn try_send(&mut self, socket_addr: String, msg: Res) -> anyhow::Result<()> {
        debug!(pool = %self.pool_name, "Try Sending msg {:?} to {}", msg, socket_addr);
        let message_label = msg.message_label();
        let stream = self
            .sockets
            .get_mut(&socket_addr)
            .context(format!("Retrieving client {socket_addr}"))?;

        let binary_data = to_tcp_message(&msg)?;
        stream
            .sender
            .try_send(TcpOutboundMessage {
                message: binary_data,
                message_label,
            })
            .map_err(|e| anyhow::anyhow!("Try sending msg to client {}: {}", socket_addr, e))
    }

    pub async fn ping(&mut self, socket_addr: String) -> anyhow::Result<()> {
        let stream = self
            .sockets
            .get_mut(&socket_addr)
            .context(format!("Retrieving client {socket_addr}"))?;

        stream
            .sender
            .send(TcpOutboundMessage {
                message: TcpMessage::Ping,
                message_label: "ping",
            })
            .await
            .map_err(|e| anyhow::anyhow!("Sending ping to client {}: {}", socket_addr, e))
    }

    /// Setup stream in the managed list for a new client
    fn setup_stream(
        &mut self,
        mut sender: TcpSender,
        mut receiver: TcpReceiver,
        socket_addr: &String,
    ) {
        let send_timeout = self.send_timeout;
        // Start a task to process pings from the peer.
        // We do the processing in the main select! loop to keep things synchronous.
        // This makes it easier to store data in the same struct without mutexing.
        let ping_sender = self.ping_sender.clone();
        let pool_sender = self.pool_sender.clone();
        let cloned_socket_addr = socket_addr.clone();
        let metrics = self.metrics.clone();
        let pool_sender_for_sender = self.pool_sender.clone();
        let peer_label = Arc::new(RwLock::new(socket_addr.clone()));
        let pool = self.pool_name.clone();
        // Track how many frames we read per socket to detect stalls.
        let mut frames_received: u64 = 0;

        // This task is responsible for reception of ping and message.
        // If an error occurs and is not an InvalidData error, we assume the task is to be aborted.
        // If the stream is closed, we also assume the task is to be aborted.
        let abort_receiver_task = logged_task({
            let peer_label = peer_label.clone();
            let pool = pool.clone();
            async move {
                loop {
                    match receiver.next().await {
                        Some(Ok(bytes)) => {
                            if *bytes == *b"PING" {
                                _ = ping_sender.send(cloned_socket_addr.clone()).await;
                            } else {
                                let label = peer_label_or_addr(&peer_label, &cloned_socket_addr);
                                debug!(
                                    pool = %pool,
                                    "Received data from peer {} (socket_addr={}): {} bytes ({}...)",
                                    label,
                                    cloned_socket_addr,
                                    bytes.len(),
                                    hex::encode(bytes.iter().take(10).cloned().collect::<Vec<_>>())
                                );
                                frames_received += 1;
                                trace!(
                                    pool = %pool,
                                    "Peer {} (socket_addr={}) frame #{} ({} bytes) queued for decode",
                                    label,
                                    cloned_socket_addr,
                                    frames_received,
                                    bytes.len()
                                );
                                // Try non-blocking send first to detect channel pressure.
                                let event = match decode_tcp_payload::<Req>(&bytes) {
                                    Ok((headers, data)) => {
                                        let message_label = data.message_label();
                                        metrics.message_received(message_label);
                                        metrics.message_received_bytes(
                                            bytes.len() as u64,
                                            message_label,
                                        );
                                        TcpEvent::Message {
                                            socket_addr: cloned_socket_addr.clone(),
                                            data,
                                            headers,
                                        }
                                    }
                                    Err(io) => {
                                        metrics.message_error();
                                        warn!(
                                            pool = %pool,
                                            "Failed to decode TCP frame from peer {} (socket_addr={}, frame=#{}, {} bytes): {}. Closing socket.",
                                            label,
                                            cloned_socket_addr,
                                            frames_received,
                                            bytes.len(),
                                            io
                                        );
                                        // Treat decode failure as fatal: notify upstream and stop the loop.
                                        let _ = pool_sender
                                            .send(Box::new(TcpEvent::Error {
                                                socket_addr: cloned_socket_addr.clone(),
                                                error: io.to_string(),
                                            }))
                                            .await;
                                        break;
                                    }
                                };

                                match pool_sender.try_send(Box::new(event)) {
                                    Ok(_) => {}
                                    Err(tokio::sync::mpsc::error::TrySendError::Full(event)) => {
                                        warn!(
                                            pool = %pool,
                                            "TCP event channel full for peer {} (socket_addr={})",
                                            label, cloned_socket_addr
                                        );
                                        // Fallback to an awaited send to avoid dropping the event.
                                        let _ = pool_sender.send(event).await;
                                    }
                                    Err(tokio::sync::mpsc::error::TrySendError::Closed(event)) => {
                                        warn!(
                                            pool = %pool,
                                            "TCP event channel closed for peer {} (socket_addr={}), dropping event",
                                            label, cloned_socket_addr,
                                        );
                                        drop(event);
                                    }
                                }
                            }
                        }

                        Some(Err(err)) => {
                            let label = peer_label_or_addr(&peer_label, &cloned_socket_addr);
                            if err.kind() == ErrorKind::InvalidData {
                                error!(
                                    pool = %pool,
                                    "Received invalid data from peer {} (socket_addr={}): {}",
                                    label, cloned_socket_addr, err
                                );
                            } else {
                                // If the error is not invalid data, we can assume the socket is closed.
                                warn!(
                                    pool = %pool,
                                    "Closing socket for peer {} (socket_addr={}) after read error: {} (kind={:?})",
                                    label,
                                    cloned_socket_addr,
                                    err,
                                    err.kind()
                                );
                                metrics.message_error();
                                let _ = pool_sender
                                    .send(Box::new(TcpEvent::Error {
                                        socket_addr: cloned_socket_addr.clone(),
                                        error: err.to_string(),
                                    }))
                                    .await;
                                break;
                            }
                        }
                        None => {
                            // If we reach here, the stream has been closed.
                            let label = peer_label_or_addr(&peer_label, &cloned_socket_addr);
                            debug!(
                                pool = %pool,
                                "Socket closed for peer {} (socket_addr={}) after receiving {} frame(s)",
                                label, cloned_socket_addr, frames_received
                            );
                            metrics.message_closed();
                            let _ = pool_sender
                                .send(Box::new(TcpEvent::Closed {
                                    socket_addr: cloned_socket_addr.clone(),
                                }))
                                .await;
                            break;
                        }
                    }
                }
            }
        });

        let (sender_snd, mut sender_recv) = tokio::sync::mpsc::channel::<TcpOutboundMessage>(1000);
        let metrics = self.metrics.clone();

        let abort_sender_task = logged_task({
            let cloned_socket_addr = socket_addr.clone();
            let peer_label = peer_label.clone();
            let pool = pool.clone();
            let pool_sender = pool_sender_for_sender.clone();
            async move {
                while let Some(outbound) = sender_recv.recv().await {
                    let message_label = outbound.message_label;
                    #[cfg(feature = "turmoil")]
                    let should_check_drop = matches!(&outbound.message, TcpMessage::Data(_));
                    let Ok(msg_bytes) = Bytes::try_from(outbound.message) else {
                        let label = peer_label_or_addr(&peer_label, &cloned_socket_addr);
                        error!(
                            pool = %pool,
                            "Failed to serialize message to send to peer {} (socket_addr={})",
                            label, cloned_socket_addr
                        );
                        metrics.message_send_error();
                        break;
                    };
                    #[cfg(feature = "turmoil")]
                    let msg_bytes = if should_check_drop {
                        match intercept::intercept_message(&msg_bytes) {
                            intercept::MessageAction::Pass => msg_bytes,
                            intercept::MessageAction::Drop => {
                                let label = peer_label_or_addr(&peer_label, &cloned_socket_addr);
                                trace!(
                                    pool = %pool,
                                    "Dropping outbound TCP frame for peer {} (socket_addr={})",
                                    label,
                                    cloned_socket_addr
                                );
                                continue;
                            }
                            intercept::MessageAction::Replace(corrupted) => {
                                let label = peer_label_or_addr(&peer_label, &cloned_socket_addr);
                                trace!(
                                    pool = %pool,
                                    "Corrupting outbound TCP frame for peer {} (socket_addr={})",
                                    label,
                                    cloned_socket_addr
                                );
                                corrupted
                            }
                        }
                    } else {
                        msg_bytes
                    };
                    let start = std::time::Instant::now();
                    let nb_bytes: usize = msg_bytes.len();
                    match tokio::time::timeout(send_timeout, sender.send(msg_bytes)).await {
                        Err(e) => {
                            let label = peer_label_or_addr(&peer_label, &cloned_socket_addr);
                            error!(
                                pool = %pool,
                                "Timeout sending message to peer {} (socket_addr={}): {}",
                                label, cloned_socket_addr, e
                            );
                            let _ = pool_sender
                                .send(Box::new(TcpEvent::Error {
                                    socket_addr: cloned_socket_addr.clone(),
                                    error: format!("send_timeout: {e}"),
                                }))
                                .await;
                            metrics.message_send_error();
                            break;
                        }
                        Ok(Err(e)) => {
                            let label = peer_label_or_addr(&peer_label, &cloned_socket_addr);
                            error!(
                                pool = %pool,
                                "Sending message to peer {} (socket_addr={}): {}",
                                label, cloned_socket_addr, e
                            );
                            let _ = pool_sender
                                .send(Box::new(TcpEvent::Error {
                                    socket_addr: cloned_socket_addr.clone(),
                                    error: e.to_string(),
                                }))
                                .await;
                            metrics.message_send_error();
                            break;
                        }
                        Ok(Ok(_)) => {
                            metrics.message_emitted(message_label);
                            metrics.message_emitted_bytes(nb_bytes as u64, message_label);
                        }
                    }
                    metrics.message_send_time(start.elapsed().as_secs_f64(), message_label);
                }
            }
        });

        tracing::debug!(pool = %pool, "Socket {} connected", socket_addr);
        // Store socket in the list.
        self.metrics.peers_snapshot(self.sockets.len() as u64 + 1);
        self.sockets.insert(
            socket_addr.to_string(),
            SocketStream {
                last_ping: TimestampMsClock::now(),
                socket_label: peer_label,
                sender: sender_snd,
                abort_sender_task,
                abort_receiver_task,
            },
        );
    }

    pub fn setup_client(&mut self, addr: String, tcp_client: TcpClient<Req, Res>) {
        let (sender, receiver) = tcp_client.split();
        self.setup_stream(sender, receiver, &addr);
    }

    pub fn drop_peer_stream(&mut self, peer_ip: String) {
        if let Some(peer_stream) = self.sockets.remove(&peer_ip) {
            tracing::debug!(
                pool = %self.pool_name,
                "Dropping peer stream {} (remaining sockets: {})",
                peer_ip,
                self.sockets.len()
            );
            peer_stream.abort_sender_task.abort();
            peer_stream.abort_receiver_task.abort();
            tracing::debug!(pool = %self.pool_name, "Client {} dropped & disconnected", peer_ip);
            self.metrics.peers_snapshot(self.sockets.len() as u64);
        }
    }

    pub fn set_peer_label(&mut self, socket_addr: &str, label: String) {
        if let Some(stream) = self.sockets.get_mut(socket_addr) {
            let mut guard = match stream.socket_label.write() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
            *guard = label;
        }
    }
}

#[cfg(test)]
pub mod tests {
    use std::time::Duration;

    use super::TcpServer;
    use crate::tcp::{
        tcp_client::TcpClient, tcp_server::peer_label_or_addr, to_tcp_message, TcpEvent, TcpMessage,
    };
    use anyhow::Result;
    use bytes::Bytes;
    use futures::{SinkExt, TryStreamExt};
    use sdk::{BlockHeight, DataAvailabilityEvent, DataAvailabilityRequest};

    type DAServer = TcpServer<DataAvailabilityRequest, DataAvailabilityEvent>;
    type DAClient = TcpClient<DataAvailabilityRequest, DataAvailabilityEvent>;

    #[tokio::test]
    async fn tcp_test() -> Result<()> {
        let mut server = DAServer::start(2346, "DaServer").await?;

        let mut client = DAClient::connect("me".to_string(), "0.0.0.0:2346").await?;

        // Ping
        client.ping().await?;

        // Send data to server
        client.send(DataAvailabilityRequest(BlockHeight(2))).await?;

        tokio::time::sleep(Duration::from_secs(1)).await;

        let d = match server.listen_next().await.unwrap() {
            TcpEvent::Message { data, .. } => data,
            _ => panic!("Expected a Message event"),
        };

        assert_eq!(DataAvailabilityRequest(BlockHeight(2)), d);
        assert!(server.pool_receiver.try_recv().is_err());

        // From server to client
        _ = server
            .broadcast(DataAvailabilityEvent::SignedBlock(Default::default()))
            .await;

        assert_eq!(
            client.recv().await.unwrap(),
            DataAvailabilityEvent::SignedBlock(Default::default())
        );

        let client_socket_addr = server.connected_clients().first().unwrap().clone();

        server.ping(client_socket_addr).await?;

        assert_eq!(
            client.receiver.try_next().await.unwrap().unwrap(),
            TryInto::<Bytes>::try_into(TcpMessage::Ping).unwrap()
        );

        Ok(())
    }

    #[tokio::test]
    async fn tcp_broadcast() -> Result<()> {
        let mut server = DAServer::start(0, "DaServer").await?;

        let mut client1 = DAClient::connect(
            "me1".to_string(),
            format!("0.0.0.0:{}", server.local_addr().unwrap().port()),
        )
        .await?;
        _ = tokio::time::timeout(Duration::from_millis(200), server.listen_next()).await;

        let mut client2 = DAClient::connect(
            "me2".to_string(),
            format!("0.0.0.0:{}", server.local_addr().unwrap().port()),
        )
        .await?;
        _ = tokio::time::timeout(Duration::from_millis(200), server.listen_next()).await;

        tokio::time::sleep(Duration::from_millis(200)).await;

        server
            .broadcast(DataAvailabilityEvent::SignedBlock(Default::default()))
            .await;

        tokio::time::sleep(Duration::from_millis(100)).await;

        let res1 = client1.receiver.try_next().await;
        assert!(res1.is_ok());
        assert_eq!(
            res1.unwrap().unwrap(),
            TryInto::<Bytes>::try_into(
                to_tcp_message(&DataAvailabilityEvent::SignedBlock(Default::default())).unwrap()
            )
            .unwrap()
        );
        let res2 = client2.receiver.try_next().await;
        assert!(res2.is_ok());
        assert_eq!(
            res2.unwrap().unwrap(),
            TryInto::<Bytes>::try_into(
                to_tcp_message(&DataAvailabilityEvent::SignedBlock(Default::default())).unwrap()
            )
            .unwrap()
        );

        Ok(())
    }

    #[tokio::test]
    async fn tcp_send_parallel() -> Result<()> {
        let mut server = DAServer::start(0, "DAServer").await?;

        let mut client1 = DAClient::connect(
            "me1".to_string(),
            format!("0.0.0.0:{}", server.local_addr().unwrap().port()),
        )
        .await?;
        _ = tokio::time::timeout(Duration::from_millis(200), server.listen_next()).await;

        let client1_addr = server.connected_clients().clone().first().unwrap().clone();

        let mut client2 = DAClient::connect(
            "me2".to_string(),
            format!("0.0.0.0:{}", server.local_addr().unwrap().port()),
        )
        .await?;
        _ = tokio::time::timeout(Duration::from_millis(200), server.listen_next()).await;
        let client2_addr = server
            .connected_clients()
            .clone()
            .into_iter()
            .rfind(|addr| addr != &client1_addr)
            .unwrap();

        server
            .raw_send_parallel(
                vec![client2_addr.to_string()],
                borsh::to_vec(&DataAvailabilityEvent::SignedBlock(Default::default())).unwrap(),
                vec![],
            )
            .await;

        tokio::time::sleep(Duration::from_millis(100)).await;

        let res1 =
            tokio::time::timeout(Duration::from_millis(200), client1.receiver.try_next()).await;
        assert!(res1.is_err());

        let res2 = client2.receiver.try_next().await;
        assert!(res2.is_ok());
        assert_eq!(
            res2.unwrap().unwrap(),
            TryInto::<Bytes>::try_into(
                to_tcp_message(&DataAvailabilityEvent::SignedBlock(Default::default())).unwrap()
            )
            .unwrap()
        );

        Ok(())
    }

    #[tokio::test]
    async fn tcp_send() -> Result<()> {
        let mut server = DAServer::start(0, "DAServer").await?;

        let mut client1 = DAClient::connect(
            "me1".to_string(),
            format!("0.0.0.0:{}", server.local_addr().unwrap().port()),
        )
        .await?;
        _ = tokio::time::timeout(Duration::from_millis(200), server.listen_next()).await;
        let client1_addr = server.connected_clients().first().unwrap().clone();

        let mut client2 = DAClient::connect(
            "me2".to_string(),
            format!("0.0.0.0:{}", server.local_addr().unwrap().port()),
        )
        .await?;
        _ = tokio::time::timeout(Duration::from_millis(200), server.listen_next()).await;
        let client2_addr = server
            .connected_clients()
            .clone()
            .into_iter()
            .rfind(|addr| addr != &client1_addr)
            .unwrap();

        _ = server
            .send(
                client2_addr.to_string(),
                DataAvailabilityEvent::SignedBlock(Default::default()),
                vec![],
            )
            .await;

        tokio::time::sleep(Duration::from_millis(100)).await;

        let res1 =
            tokio::time::timeout(Duration::from_millis(200), client1.receiver.try_next()).await;
        assert!(res1.is_err());

        let res2 = client2.receiver.try_next().await;
        assert!(res2.is_ok());
        assert_eq!(
            res2.unwrap().unwrap(),
            TryInto::<Bytes>::try_into(
                to_tcp_message(&DataAvailabilityEvent::SignedBlock(Default::default())).unwrap()
            )
            .unwrap()
        );

        Ok(())
    }

    type BytesServer = TcpServer<Vec<u8>, Vec<u8>>;
    type BytesClient = TcpClient<Vec<u8>, Vec<u8>>;

    #[test_log::test(tokio::test)]
    async fn tcp_with_max_frame_length() -> Result<()> {
        let mut server = BytesServer::start_with_opts(0, Some(100), "Test").await?;

        let mut client = BytesClient::connect_with_opts(
            "me".to_string(),
            Some(100),
            format!("0.0.0.0:{}", server.local_addr().unwrap().port()),
        )
        .await?;

        // Send data to server
        // A vec will be prefixed with 4 bytes (u32) containing the size of the payload
        // Here we reach 100 bytes <= 100
        client.send(vec![0b_0; 88]).await?;

        let data = match server.listen_next().await.unwrap() {
            TcpEvent::Message { data, .. } => data,
            _ => panic!("Expected a Message event"),
        };

        assert_eq!(data.len(), 88);
        assert!(server.pool_receiver.try_recv().is_err());

        // Send data to server
        // Here we reach 101 bytes, it should explode the limit
        let sent = client.send(vec![0b_0; 89]).await;
        tracing::warn!("Sent: {:?}", sent);
        assert!(sent.is_err_and(|e| e.to_string().contains("frame size too big")));

        let mut client_relaxed = BytesClient::connect(
            "me".to_string(),
            format!("0.0.0.0:{}", server.local_addr().unwrap().port()),
        )
        .await?;

        // Should be ok server side
        client_relaxed.send(vec![0b_0; 88]).await?;

        let data = match server.listen_next().await.unwrap() {
            TcpEvent::Message { data, .. } => data,
            TcpEvent::Error { socket_addr, error } => panic!(
                "Expected a Message event, got Error for {}: {}",
                socket_addr, error
            ),
            TcpEvent::Closed { socket_addr } => {
                panic!("Expected a Message event, got Closed for {}", socket_addr)
            }
        };
        assert_eq!(data.len(), 88);

        // Should explode server side
        client_relaxed.send(vec![0b_0; 89]).await?;

        let received_data = server.listen_next().await;
        assert!(received_data.is_some_and(|tcp_event| matches!(tcp_event, TcpEvent::Closed { .. })));

        Ok(())
    }

    #[tokio::test]
    async fn tcp_decode_error_stops_processing() -> Result<()> {
        let mut server = DAServer::start(0, "DaServer").await?;
        let mut client = TcpClient::<Vec<u8>, Vec<u8>>::connect(
            "raw".to_string(),
            format!("0.0.0.0:{}", server.local_addr().unwrap().port()),
        )
        .await?;

        let _ = tokio::time::timeout(Duration::from_millis(200), server.listen_next()).await;

        client.sender.send(Bytes::from_static(&[0u8; 4])).await?;
        let valid = borsh::to_vec(&DataAvailabilityRequest(BlockHeight(1)))?;
        client.sender.send(Bytes::from(valid)).await?;

        let evt = tokio::time::timeout(Duration::from_millis(200), server.listen_next())
            .await
            .expect("timeout waiting for error event")
            .expect("expected an error event");
        assert!(matches!(evt, TcpEvent::Error { .. }));

        let followup = tokio::time::timeout(Duration::from_millis(200), server.listen_next()).await;
        assert!(
            followup.is_err(),
            "expected no further events after decode error"
        );

        Ok(())
    }

    #[tokio::test]
    async fn peer_label_updates_after_set() -> anyhow::Result<()> {
        let mut server = DAServer::start(0, "DaServer").await?;
        let _client = DAClient::connect(
            "me".to_string(),
            format!("0.0.0.0:{}", server.local_addr().unwrap().port()),
        )
        .await?;
        _ = tokio::time::timeout(Duration::from_millis(200), server.listen_next()).await;

        let socket_addr = server.connected_clients().first().unwrap().clone();
        {
            let stored = server
                .sockets
                .get(&socket_addr)
                .expect("socket should be registered");
            let initial_label = peer_label_or_addr(&stored.socket_label, &socket_addr);
            assert_eq!(initial_label, socket_addr);
        }

        server.set_peer_label(&socket_addr, "peer-A".to_string());
        let updated = {
            let stored = server
                .sockets
                .get(&socket_addr)
                .expect("socket should be registered");
            peer_label_or_addr(&stored.socket_label, &socket_addr)
        };
        assert_eq!(updated, "peer-A");
        Ok(())
    }
}

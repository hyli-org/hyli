use std::time::Duration;
use std::{collections::HashMap, sync::Arc};

use crate::bus::{BusClientSender, SharedMessageBus};
use crate::modules::Module;
use crate::{log_warn, module_bus_client, module_handle_messages};
use anyhow::{anyhow, Context, Error, Result};
use axum::extract::ConnectInfo;
use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    http::StatusCode,
    response::IntoResponse,
    routing::get,
    Router,
};
use futures::{
    sink::SinkExt,
    stream::{SplitSink, SplitStream, StreamExt},
};
use hyle_net::net::{HyleNetIntoMakeServiceWithconnectInfo, HyleNetSocketAddr};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tokio::{sync::Mutex, task::JoinSet};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

// ---- Bus ------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WsInMessage<T> {
    pub addr: String,
    pub message: T,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WsBroadcastMessage<T> {
    pub message: T,
}
impl<T> WsBroadcastMessage<T> {
    pub fn new(message: T) -> Self {
        Self { message }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WsTopicMessage<T> {
    pub topic: String,
    pub message: T,
}
impl<T> WsTopicMessage<T> {
    pub fn new(topic: impl Into<String>, message: T) -> Self {
        Self {
            topic: topic.into(),
            message,
        }
    }
}

module_bus_client! {
#[derive(Debug)]
pub struct WebSocketBusClient<In: Send + Sync + Clone + 'static, Out: Send + Sync + Clone + 'static> {
    sender(WsInMessage<In>),
    receiver(WsBroadcastMessage<Out>),
    receiver(WsTopicMessage<Out>),
}
}

// ---- WebSocket Module ----

/// Configuration for the WebSocket module
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebSocketConfig {
    /// The port number to bind the WebSocket server to
    pub port: u16,
    /// The endpoint path for WebSocket connections
    pub ws_path: String,
    /// The endpoint path for health checks
    pub health_path: String,
    /// The interval at which to check for new peers
    pub peer_check_interval: Duration,
}

impl Default for WebSocketConfig {
    fn default() -> Self {
        Self {
            port: 8080,
            ws_path: "/ws".to_string(),
            health_path: "/ws_health".to_string(),
            peer_check_interval: Duration::from_millis(100),
        }
    }
}

pub type PeerAddress = String;
pub type Topic = String;

/// A WebSocket module that sends messages from the bus to WebSocket clients
/// and vice versa
pub struct WebSocketModule<In, Out>
where
    In: Send + Sync + Clone + 'static,
    Out: Serialize + Send + Sync + Clone + 'static,
{
    bus: WebSocketBusClient<In, Out>,
    app: Option<Router>,
    peer_senders: HashMap<PeerAddress, SplitSink<WebSocket, Message>>,
    topic_listeners: HashMap<Topic, Vec<PeerAddress>>,
    #[allow(clippy::type_complexity)]
    peer_receivers: JoinSet<
        Option<(
            PeerAddress,
            SplitStream<WebSocket>,
            Result<WsMsg<In>, Error>,
        )>,
    >,
    new_peers: NewPeers,
    config: WebSocketConfig,
}

#[derive(Clone, Default)]
struct NewPeers(pub Arc<Mutex<Vec<(String, WebSocket)>>>);

impl<In, Out> Module for WebSocketModule<In, Out>
where
    In: DeserializeOwned + std::fmt::Debug + Send + Sync + Clone + 'static,
    Out: Serialize + Send + Sync + Clone + 'static,
{
    type Context = WebSocketConfig;

    async fn build(bus: SharedMessageBus, config: Self::Context) -> Result<Self> {
        let new_peers = NewPeers::default();
        let app = Router::new()
            .route(&config.ws_path, get(ws_handler))
            .route(&config.health_path, get(health_check))
            .with_state(new_peers.clone());

        Ok(Self {
            bus: WebSocketBusClient::new_from_bus(bus.new_handle()).await,
            app: Some(app),
            peer_senders: HashMap::new(),
            topic_listeners: HashMap::new(),
            peer_receivers: JoinSet::new(),
            new_peers,
            config,
        })
    }

    async fn run(&mut self) -> Result<()> {
        // Start the server
        let listener = hyle_net::net::bind_tcp_listener(self.config.port)
            .await
            .map_err(|e| anyhow!("Failed to bind to port {}: {}", self.config.port, e))?;

        let app = self
            .app
            .take()
            .ok_or_else(|| anyhow!("Router was already taken"))?;

        info!("WebSocket server listening on port {}", self.config.port);
        debug!("WebSocket server listening on {}", self.config.port);

        let axum_cancel_token = CancellationToken::new();
        let token = axum_cancel_token.clone();
        let server = tokio::spawn(async move {
            if let Err(e) = axum::serve(
                listener,
                HyleNetIntoMakeServiceWithconnectInfo(
                    app.into_make_service_with_connect_info::<HyleNetSocketAddr>(),
                ),
            )
            .with_graceful_shutdown(async move {
                token.cancelled().await;
            })
            .await
            {
                error!("Server error: {}", e);
            } else {
                info!("Server stopped");
            }
        });

        module_handle_messages! {
            on_bus self.bus,
            listen<WsBroadcastMessage<Out>> msg => {
                if let Err(e) = self.broadcast_message(msg.message).await {
                    error!("Error sending outbound message: {}", e);
                    break;
                }
            }
            listen<WsTopicMessage<Out>> msg => {
                if let Err(e) = self.topic_message(msg.topic, msg.message).await {
                    error!("Error sending outbound message: {}", e);
                    break;
                }
            }
            Some(Ok(Some(msg))) = self.peer_receivers.join_next() => {
                match msg {
                    (addr, socket_stream, Ok(msg)) => {
                        debug!("Received message: {:?}", msg);
                        match msg {
                            WsMsg::RegisterTopic(topic) => {
                                debug!("Registering topic: {} for {}", topic, addr);
                                self.topic_listeners.entry(topic.clone()).or_default().push(addr.clone());
                            }
                            WsMsg::Message(msg) => {
                                 self.handle_incoming_message(WsInMessage{addr: addr.clone(), message : msg}).await;
                            }
                        }
                        // Add it again to the receiver
                        self.peer_receivers.spawn(Self::process_websocket_incoming(addr, socket_stream));
                    }
                    (_, _, Err(e)) => {
                        error!("Error receiving message: {}", e);
                    }
                }
            }
            _ = axum_cancel_token.cancelled() => {
                // Shutdown signal received, break the loop
                tracing::warn!("WebSocket server shutdown initiated by axum");
                break;
            }
            _ = tokio::time::sleep(self.config.peer_check_interval) => {
                // Check for new peers
                let mut peers = self.new_peers.0.lock().await;
                for (addr, peer) in peers.drain(..) {
                    let (sender, receiver) = peer.split();
                    self.peer_senders.insert(addr.clone(), sender);
                    self.peer_receivers.spawn(Self::process_websocket_incoming(addr, receiver));
                }
            }
        };

        server.abort_handle().abort();
        Ok(())
    }
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<NewPeers>,
    ConnectInfo(addr): ConnectInfo<HyleNetSocketAddr>,
) -> impl IntoResponse {
    ws.on_upgrade(async move |socket| {
        debug!("New WebSocket connection established");
        let mut state = state.0.lock().await;
        let peer_id = format!("{}:{}", addr.ip(), addr.port());
        state.push((peer_id, socket));
    })
}

impl<In, Out> WebSocketModule<In, Out>
where
    In: DeserializeOwned + std::fmt::Debug + Send + Sync + Clone + 'static,
    Out: Serialize + Send + Sync + Clone + 'static,
{
    async fn handle_incoming_message(&mut self, msg: WsInMessage<In>) {
        let _ = log_warn!(self.bus.send(msg), "Sending WsInMessage message to bus.");
    }

    async fn topic_message(&mut self, topic: Topic, msg: Out) -> Result<()> {
        let text = serde_json::to_string(&msg).context("Failed to serialize outbound message")?;
        let text: Message = Message::Text(text.clone().into());

        if let Some(sender_addr) = self.topic_listeners.get(&topic) {
            for addr in sender_addr.clone() {
                let sender = self
                    .peer_senders
                    .get_mut(&addr)
                    .ok_or_else(|| anyhow!("No peer sender for topic: {}", topic))?;
                if let Err(e) = sender
                    .send(text.clone())
                    .await
                    .context("Failed to send message")
                {
                    debug!("Failed to send message to topic {topic}: {e}");
                    self.topic_listeners
                        .entry(topic.clone())
                        .or_default()
                        .retain(|x| *x != addr);
                }
            }
        } else {
            debug!("No sender for topic: {}", topic);
        }

        Ok(())
    }

    async fn broadcast_message(&mut self, msg: Out) -> Result<()> {
        let mut at_least_one_ok = false;

        let text = serde_json::to_string(&msg).context("Failed to serialize outbound message")?;
        let text: Message = Message::Text(text.clone().into());

        let send_futures: Vec<_> = self
            .peer_senders
            .iter_mut()
            .map(|(addr, peer)| {
                let text = text.clone();
                let addr = addr.clone();
                async move { (addr, peer.send(text).await) }
            })
            .collect();

        let results = futures::future::join_all(send_futures).await;

        for (addr, result) in results {
            match result {
                Ok(_) => {
                    at_least_one_ok = true;
                }
                Err(e) => {
                    debug!("Failed to send message to WebSocket {}: {}", addr, e);
                    self.peer_senders.remove(&addr);
                }
            }
        }

        if at_least_one_ok || self.peer_senders.is_empty() {
            Ok(())
        } else {
            Err(anyhow!("Failed to send message to all WebSocket peers"))
        }
    }

    async fn process_websocket_incoming(
        addr: String,
        mut receiver: SplitStream<WebSocket>,
    ) -> Option<(
        PeerAddress,
        SplitStream<WebSocket>,
        Result<WsMsg<In>, Error>,
    )> {
        while let Some(msg) = receiver.next().await {
            match msg {
                Ok(Message::Text(text)) => {
                    debug!("Received message: {:?}", text);
                    return Some((
                        addr,
                        receiver,
                        serde_json::from_str::<WsMsg<In>>(text.as_str())
                            .context("Failed to parse message"),
                    ));
                }
                Ok(Message::Close(_)) => {
                    debug!("Client initiated close");
                    break;
                }
                Err(e) => {
                    error!("WebSocket receive error: {}", e);
                    break;
                }
                _ => {
                    debug!("Ignoring non-text message");
                } // Ignore other message types
            }
        }
        None
    }
}

async fn health_check() -> impl IntoResponse {
    (StatusCode::OK, "OK")
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum WsMsg<T> {
    RegisterTopic(String),
    Message(T),
}

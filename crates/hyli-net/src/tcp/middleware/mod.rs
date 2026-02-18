//! TcpServer middleware helpers.
//!
//! This module provides a wrapper around `TcpServer` that preserves the
//! `listen_next()` API while allowing synchronous middleware actions
//! (drop-on-error) and listen-driven retries (send retry queue progressed
//! inside `listen_next()`).
//!
//! # Example
//! ```no_run
//! use std::time::Duration;
//! use hyli_net::tcp::{
//!     tcp_server::TcpServer,
//!     middleware::{preset, TcpServerExt},
//! };
//! # use hyli_net::tcp::{TcpEvent, TcpMessageLabel};
//! # use borsh::{BorshDeserialize, BorshSerialize};
//! #
//! # #[derive(Clone, Debug, BorshSerialize, BorshDeserialize)]
//! # struct Req;
//! # impl TcpMessageLabel for Req {
//! #     fn message_label(&self) -> &'static str { "Req" }
//! # }
//! # #[derive(Clone, Debug, BorshSerialize, BorshDeserialize)]
//! # struct Res;
//! # impl TcpMessageLabel for Res {
//! #     fn message_label(&self) -> &'static str { "Res" }
//! # }
//! #
//! # async fn example() -> anyhow::Result<()> {
//! let inner = TcpServer::<Req, Res>::start(0, "Example").await?;
//! let mut server = hyli_net::tcp_stack!(
//!     inner,
//!     preset::drop_on_error(),
//!     preset::retrying_send::<Res>(10, Duration::from_millis(100)),
//! );
//!
//! while let Some(event) = server.listen_next().await {
//!     match event {
//!         TcpEvent::Message { socket_addr, data, headers } => {
//!             // Handle inbound message...
//!             let _ = server.send(socket_addr, Res, headers);
//!         }
//!         TcpEvent::Closed { .. } | TcpEvent::Error { .. } => {
//!             // Drop-on-error is handled by middleware.
//!         }
//!     }
//! }
//! # Ok(())
//! # }
//! ```
//!
//! You can also map events to a different output type. This example maps
//! `TcpEvent::Message` to the `Req` payload and filters out `Error/Closed`.
//! ```no_run
//! # use hyli_net::tcp::{tcp_server::TcpServer, TcpEvent, TcpMessageLabel};
//! # use hyli_net::tcp::middleware::{TcpMiddleware, TcpServerWithMiddleware};
//! # use borsh::{BorshDeserialize, BorshSerialize};
//! # #[derive(Clone, Debug, BorshSerialize, BorshDeserialize)]
//! # struct Req;
//! # impl TcpMessageLabel for Req {
//! #     fn message_label(&self) -> &'static str { "Req" }
//! # }
//! # #[derive(Clone, Debug, BorshSerialize, BorshDeserialize)]
//! # struct Res;
//! # impl TcpMessageLabel for Res {
//! #     fn message_label(&self) -> &'static str { "Res" }
//! # }
//! #
//! # struct MessageOnly;
//! # impl TcpMiddleware<Req, Res> for MessageOnly {
//! #     type EventOut = Req;
//! #     fn on_event(&mut self, _server: &mut TcpServer<Req, Res>, event: TcpEvent<Req>) -> Option<Req> {
//! #         match event { TcpEvent::Message { data, .. } => Some(data), _ => None }
//! #     }
//! # }
//! #
//! # async fn example() -> anyhow::Result<()> {
//! let inner = TcpServer::<Req, Res>::start(0, "Example").await?;
//! let mut server = TcpServerWithMiddleware::new(inner, MessageOnly);
//! while let Some(req) = server.listen_next().await {
//!     // req is already the decoded payload
//! }
//! # Ok(())
//! # }
//! ```

use std::marker::PhantomData;
use tokio::time::Instant;

use borsh::{BorshDeserialize, BorshSerialize};

use crate::tcp::{tcp_server::TcpServer, TcpEvent, TcpHeaders, TcpMessageLabel};

mod impls;

pub use impls::{
    DropOnError, MessageOnly, MessageWithMeta, QueuedSendWithRetry, QueuedSenderMiddleware,
    RetryingSend, TcpInboundMessage,
};

pub mod preset {
    use std::time::Duration;

    use super::{DropOnError, QueuedSendWithRetry, RetryingSend};

    pub fn drop_on_error() -> DropOnError {
        DropOnError
    }

    pub fn retrying_send<Res>(max_retries: usize, base_delay: Duration) -> RetryingSend<Res> {
        RetryingSend::new(max_retries, base_delay)
    }

    pub fn drop_and_retry<Res>(
        max_retries: usize,
        base_delay: Duration,
    ) -> (DropOnError, RetryingSend<Res>) {
        (drop_on_error(), retrying_send(max_retries, base_delay))
    }

    pub fn queued_send_with_retry<Req, Res>(
        max_retries: usize,
        base_delay: Duration,
    ) -> QueuedSendWithRetry<Req, Res> {
        QueuedSendWithRetry::new(max_retries, base_delay)
    }
}

#[macro_export]
macro_rules! tcp_stack {
    ($server:expr, $($middleware:expr),+ $(,)?) => {{
        use $crate::tcp::middleware::{middleware_layer, TcpServerExt};
        let server = $server;
        $(let server = server.layer(middleware_layer($middleware));)+
        server
    }};
}

pub trait Layer<S, Req, Res> {
    type Service;
    fn layer(self, inner: S) -> Self::Service;
}

pub struct MiddlewareLayer<M> {
    middleware: M,
}

impl<M> MiddlewareLayer<M> {
    pub fn new(middleware: M) -> Self {
        Self { middleware }
    }
}

pub fn middleware_layer<M>(middleware: M) -> MiddlewareLayer<M> {
    MiddlewareLayer::new(middleware)
}

pub struct SendErrorContext<Res> {
    pub socket_addr: String,
    pub msg: Res,
    pub headers: TcpHeaders,
    pub error: anyhow::Error,
}

pub enum SendErrorOutcome {
    /// Middleware absorbed the error (e.g. logged only).
    Handled,
    /// Middleware scheduled a retry.
    RetryScheduled,
    /// Middleware requests dropping the peer.
    DropPeer,
    /// Middleware did not handle the error; propagate upstream.
    Unhandled(anyhow::Error),
}

pub trait TcpReqBound:
    BorshSerialize + BorshDeserialize + std::fmt::Debug + Send + TcpMessageLabel + 'static
{
}
impl<T> TcpReqBound for T where
    T: BorshSerialize + BorshDeserialize + std::fmt::Debug + Send + TcpMessageLabel + 'static
{
}

pub trait TcpResBound:
    BorshSerialize + BorshDeserialize + std::fmt::Debug + TcpMessageLabel
{
}
impl<T> TcpResBound for T where
    T: BorshSerialize + BorshDeserialize + std::fmt::Debug + TcpMessageLabel
{
}

/// Unified middleware trait. Implement this trait when one type needs to handle
/// inbound mapping, outbound errors and tick-based housekeeping.
pub trait TcpMiddleware<Req, Res>
where
    Req: TcpReqBound,
    Res: TcpResBound,
{
    type EventOut;

    fn on_event<S>(&mut self, _server: &mut S, event: TcpEvent<Req>) -> Option<Self::EventOut>
    where
        S: TcpServerLike<Req, Res, EventOut = TcpEvent<Req>>;

    fn on_send_error<S>(&mut self, _server: &mut S, ctx: &SendErrorContext<Res>) -> SendErrorOutcome
    where
        S: TcpServerLike<Req, Res, EventOut = TcpEvent<Req>>,
    {
        SendErrorOutcome::Unhandled(anyhow::anyhow!(ctx.error.to_string()))
    }

    fn on_tick<S>(&mut self, _server: &mut S)
    where
        S: TcpServerLike<Req, Res, EventOut = TcpEvent<Req>>,
    {
    }

    fn next_wakeup(&self) -> Option<Instant> {
        None
    }
}

/// Common interface for `TcpServer` and middleware wrappers.
pub trait TcpServerLike<Req, Res> {
    type EventOut;
    type ConnectedClients<'a>: Iterator<Item = &'a String>
    where
        Self: 'a;

    /// Receive the next inbound event (or mapped output if wrapped).
    async fn listen_next(&mut self) -> Option<Self::EventOut>;
    /// Send a response to a peer.
    fn send(&mut self, socket_addr: String, msg: Res, headers: TcpHeaders) -> anyhow::Result<()>;
    /// Send using borrowed payload/headers to avoid cloning on the success path.
    fn send_ref(&mut self, socket_addr: &str, msg: &Res, headers: &TcpHeaders) -> anyhow::Result<()>
    where
        Res: Clone,
    {
        self.send(socket_addr.to_string(), msg.clone(), headers.clone())
    }
    /// Return the currently connected peer socket addresses.
    fn connected_clients(&self) -> Self::ConnectedClients<'_>;
    /// Check whether a peer socket is currently connected.
    fn connected(&self, socket_addr: &str) -> bool {
        self.connected_clients().any(|addr| addr == socket_addr)
    }
    /// Drop and disconnect a peer socket.
    fn drop_peer_stream(&mut self, peer_ip: String);

    /// Broadcast by fanout over `connected_clients()` using `send()`.
    fn broadcast(&mut self, msg: Res, headers: TcpHeaders) -> Vec<(String, anyhow::Error)>
    where
        Res: Clone,
    {
        let peers: Vec<String> = self.connected_clients().cloned().collect();
        let mut errors = Vec::new();
        for peer in peers {
            if let Err(error) = self.send(peer.clone(), msg.clone(), headers.clone()) {
                errors.push((peer, error));
            }
        }
        errors
    }
}

pub struct InboundCx<'a, Req, Res, S>
where
    Req: BorshDeserialize,
    S: TcpServerLike<Req, Res, EventOut = TcpEvent<Req>>,
{
    server: &'a mut S,
    _marker: PhantomData<(Req, Res)>,
}

impl<'a, Req, Res, S> InboundCx<'a, Req, Res, S>
where
    Req: BorshDeserialize,
    S: TcpServerLike<Req, Res, EventOut = TcpEvent<Req>>,
{
    pub fn new(server: &'a mut S) -> Self {
        Self {
            server,
            _marker: PhantomData,
        }
    }

    pub fn connected(&self, socket_addr: &str) -> bool {
        self.server.connected(socket_addr)
    }

    pub fn drop_peer(&mut self, peer_ip: String) {
        self.server.drop_peer_stream(peer_ip);
    }

    pub fn send(
        &mut self,
        socket_addr: String,
        msg: Res,
        headers: TcpHeaders,
    ) -> anyhow::Result<()> {
        self.server.send(socket_addr, msg, headers)
    }

    pub fn send_ref(
        &mut self,
        socket_addr: &str,
        msg: &Res,
        headers: &TcpHeaders,
    ) -> anyhow::Result<()>
    where
        Res: Clone,
    {
        self.server.send_ref(socket_addr, msg, headers)
    }

    pub fn server_mut(&mut self) -> &mut S {
        self.server
    }
}

pub type OutboundCx<'a, Req, Res, S> = InboundCx<'a, Req, Res, S>;
pub type TickCx<'a, Req, Res, S> = InboundCx<'a, Req, Res, S>;

/// Tower-style layering helper for TCP servers and already-layered services.
///
/// # Example
/// ```ignore
/// let server = TcpServer::<Req, Res>::start(0, "Example").await?;
/// let mut server = server
///     .layer(middleware_layer(DropOnError))
///     .layer(middleware_layer(RetryingSend::new(10, Duration::from_millis(100))));
/// ```
pub trait TcpServerExt<Req, Res>: TcpServerLike<Req, Res> + Sized {
    fn layer<L>(self, layer: L) -> L::Service
    where
        L: Layer<Self, Req, Res>,
    {
        layer.layer(self)
    }
}

impl<T, Req, Res> TcpServerExt<Req, Res> for T where T: TcpServerLike<Req, Res> + Sized {}

pub struct TcpServerWithMiddleware<M, Req, Res, S = TcpServer<Req, Res>>
where
    Req: TcpReqBound,
    Res: TcpResBound,
{
    inner: S,
    middleware: M,
    _marker: PhantomData<(Req, Res)>,
}

impl<S, M, Req, Res> TcpServerWithMiddleware<M, Req, Res, S>
where
    Req: TcpReqBound,
    Res: TcpResBound,
{
    pub fn new(inner: S, middleware: M) -> Self {
        Self {
            inner,
            middleware,
            _marker: PhantomData,
        }
    }
}

impl<S, M, Req, Res> TcpServerWithMiddleware<M, Req, Res, S>
where
    Req: TcpReqBound,
    Res: TcpResBound + Clone,
    S: TcpServerLike<Req, Res, EventOut = TcpEvent<Req>>,
    M: TcpMiddleware<Req, Res> + QueuedSenderMiddleware<Req, Res>,
{
    /// Enqueue a message for ordered, retrying delivery to a specific peer.
    pub fn enqueue(
        &mut self,
        socket_addr: String,
        msg: Res,
        headers: TcpHeaders,
    ) -> anyhow::Result<()> {
        self.middleware.enqueue_to_peer(socket_addr, msg, headers);
        Ok(())
    }

    /// Immediate send through the underlying TCP server without middleware queueing.
    pub fn send_now(
        &mut self,
        socket_addr: String,
        msg: Res,
        headers: TcpHeaders,
    ) -> anyhow::Result<()> {
        self.inner.send(socket_addr, msg, headers)
    }

    /// Mark a peer as a streaming subscriber.
    pub fn register_streaming_peer(&mut self, socket_addr: String) {
        self.middleware.register_streaming_peer(socket_addr);
    }

    /// Queue a message to all registered streaming peers.
    pub fn enqueue_to_streaming_peers(&mut self, msg: Res, headers: TcpHeaders) {
        self.middleware.enqueue_to_streaming_peers(msg, headers);
    }

    /// Backward-compatible alias for `enqueue_to_streaming_peers`.
    pub fn send_to_streaming_peers(&mut self, msg: Res, headers: TcpHeaders) {
        self.enqueue_to_streaming_peers(msg, headers)
    }
}

impl<S, M, Req, Res> TcpServerLike<Req, Res> for TcpServerWithMiddleware<M, Req, Res, S>
where
    Req: TcpReqBound,
    Res: TcpResBound + Clone,
    S: TcpServerLike<Req, Res, EventOut = TcpEvent<Req>>,
    M: TcpMiddleware<Req, Res>,
{
    type EventOut = M::EventOut;
    type ConnectedClients<'a>
        = S::ConnectedClients<'a>
    where
        Self: 'a,
        S: 'a;

    async fn listen_next(&mut self) -> Option<Self::EventOut> {
        loop {
            self.middleware.on_tick(&mut self.inner);
            if let Some(deadline) = self.middleware.next_wakeup() {
                let now = Instant::now();
                if deadline <= now {
                    continue;
                }
                tokio::select! {
                    event = self.inner.listen_next() => {
                        let event = event?;
                        return self.middleware.on_event(&mut self.inner, event);
                    }
                    _ = tokio::time::sleep_until(deadline) => {
                        continue;
                    }
                }
            } else {
                let event = self.inner.listen_next().await?;
                return self.middleware.on_event(&mut self.inner, event);
            }
        }
    }

    fn send(&mut self, socket_addr: String, msg: Res, headers: TcpHeaders) -> anyhow::Result<()> {
        match self.inner.send_ref(&socket_addr, &msg, &headers) {
            Ok(()) => Ok(()),
            Err(error) => {
                let ctx = SendErrorContext {
                    socket_addr,
                    msg,
                    headers,
                    error,
                };
                match self.middleware.on_send_error(&mut self.inner, &ctx) {
                    SendErrorOutcome::Handled | SendErrorOutcome::RetryScheduled => Ok(()),
                    SendErrorOutcome::DropPeer => {
                        self.inner.drop_peer_stream(ctx.socket_addr.clone());
                        Ok(())
                    }
                    SendErrorOutcome::Unhandled(error) => Err(error),
                }
            }
        }
    }

    fn send_ref(&mut self, socket_addr: &str, msg: &Res, headers: &TcpHeaders) -> anyhow::Result<()>
    where
        Res: Clone,
    {
        self.inner.send_ref(socket_addr, msg, headers)
    }

    fn connected_clients(&self) -> Self::ConnectedClients<'_> {
        self.inner.connected_clients()
    }

    fn drop_peer_stream(&mut self, peer_ip: String) {
        self.inner.drop_peer_stream(peer_ip)
    }
}

impl<S, M, Req, Res> Layer<S, Req, Res> for MiddlewareLayer<M>
where
    Req: TcpReqBound,
    Res: TcpResBound + Clone,
    S: TcpServerLike<Req, Res, EventOut = TcpEvent<Req>>,
    M: TcpMiddleware<Req, Res>,
{
    type Service = TcpServerWithMiddleware<M, Req, Res, S>;

    fn layer(self, inner: S) -> Self::Service {
        TcpServerWithMiddleware::new(inner, self.middleware)
    }
}

impl<Req, Res> TcpServerLike<Req, Res> for TcpServer<Req, Res>
where
    Req: TcpReqBound,
    Res: TcpResBound,
{
    type EventOut = TcpEvent<Req>;
    type ConnectedClients<'a>
        = crate::tcp::tcp_server::ConnectedClients<'a>
    where
        Self: 'a;

    async fn listen_next(&mut self) -> Option<Self::EventOut> {
        TcpServer::listen_next(self).await
    }

    fn send(&mut self, socket_addr: String, msg: Res, headers: TcpHeaders) -> anyhow::Result<()> {
        TcpServer::send(self, socket_addr, msg, headers)
    }

    fn send_ref(&mut self, socket_addr: &str, msg: &Res, headers: &TcpHeaders) -> anyhow::Result<()>
    where
        Res: Clone,
    {
        TcpServer::send_ref(self, socket_addr, msg, headers)
    }

    fn connected_clients(&self) -> Self::ConnectedClients<'_> {
        TcpServer::connected_clients(self)
    }

    fn drop_peer_stream(&mut self, peer_ip: String) {
        TcpServer::drop_peer_stream(self, peer_ip)
    }
}

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
//!     middleware::{TcpServerWithMiddleware, DropOnErrorAndRetry},
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
//! let middleware = DropOnErrorAndRetry::new(10, Duration::from_millis(100));
//! let mut server = TcpServerWithMiddleware::new(inner, middleware);
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
//! # use hyli_net::tcp::middleware::{TcpServerWithMiddleware, TcpServerMiddleware};
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
//! # impl TcpServerMiddleware<Req, Res> for MessageOnly {
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

use tokio::time::Instant;

use crate::tcp::{tcp_server::TcpServer, TcpEvent, TcpHeaders};

mod impls;

pub use impls::{DropOnError, DropOnErrorAndRetry, RetryingSend};

pub struct SendErrorContext<Res> {
    pub socket_addr: String,
    pub msg: Res,
    pub headers: TcpHeaders,
    pub error: anyhow::Error,
}

pub trait TcpServerMiddleware<Req, Res> {
    type EventOut = TcpEvent<Req>;

    /// Transform or filter inbound events before they are exposed to callers.
    /// Returning `None` will cause the wrapper to keep listening.
    fn on_event(
        &mut self,
        _server: &mut TcpServer<Req, Res>,
        event: TcpEvent<Req>,
    ) -> Option<Self::EventOut>;

    /// Handle outbound send errors. The default behavior is to surface the error.
    /// Implementations can enqueue retries or drop peers.
    fn on_send_error(
        &mut self,
        _server: &mut TcpServer<Req, Res>,
        ctx: SendErrorContext<Res>,
    ) -> anyhow::Result<()> {
        Err(ctx.error)
    }

    /// Called on each `listen_next()` iteration before waiting for events.
    /// Use this to drive retry queues or housekeeping.
    fn on_tick(&mut self, _server: &mut TcpServer<Req, Res>) {}

    /// Optional wakeup time for the next middleware action. If present, the
    /// wrapper will `select!` between the next event and this deadline.
    fn next_wakeup(&self) -> Option<Instant> {
        None
    }
}

/// Common interface for `TcpServer` and middleware wrappers.
pub trait TcpServerLike<Req, Res> {
    type EventOut;

    /// Receive the next inbound event (or mapped output if wrapped).
    async fn listen_next(&mut self) -> Option<Self::EventOut>;
    /// Send a response to a peer.
    fn send(
        &mut self,
        socket_addr: String,
        msg: Res,
        headers: TcpHeaders,
    ) -> anyhow::Result<()>;
}

pub struct TcpServerWithMiddleware<M, Req, Res> {
    inner: TcpServer<Req, Res>,
    middleware: M,
}

impl<M, Req, Res> TcpServerWithMiddleware<M, Req, Res>
where
    M: TcpServerMiddleware<Req, Res>,
{
    pub fn new(inner: TcpServer<Req, Res>, middleware: M) -> Self {
        Self { inner, middleware }
    }

    pub fn inner(&self) -> &TcpServer<Req, Res> {
        &self.inner
    }

    pub fn inner_mut(&mut self) -> &mut TcpServer<Req, Res> {
        &mut self.inner
    }
}

impl<Req, Res> TcpServerLike<Req, Res> for TcpServer<Req, Res> {
    type EventOut = TcpEvent<Req>;

    async fn listen_next(&mut self) -> Option<Self::EventOut> {
        TcpServer::listen_next(self).await
    }

    fn send(
        &mut self,
        socket_addr: String,
        msg: Res,
        headers: TcpHeaders,
    ) -> anyhow::Result<()> {
        TcpServer::send(self, socket_addr, msg, headers)
    }
}

impl<M, Req, Res> TcpServerLike<Req, Res> for TcpServerWithMiddleware<M, Req, Res>
where
    M: TcpServerMiddleware<Req, Res>,
    Res: Clone,
{
    type EventOut = M::EventOut;

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

    fn send(
        &mut self,
        socket_addr: String,
        msg: Res,
        headers: TcpHeaders,
    ) -> anyhow::Result<()> {
        let msg_clone = msg.clone();
        let headers_clone = headers.clone();
        match self.inner.send(socket_addr.clone(), msg, headers) {
            Ok(()) => Ok(()),
            Err(error) => self.middleware.on_send_error(
                &mut self.inner,
                SendErrorContext {
                    socket_addr,
                    msg: msg_clone,
                    headers: headers_clone,
                    error,
                },
            ),
        }
    }
}

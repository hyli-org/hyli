use std::collections::{HashSet, VecDeque};
use std::marker::PhantomData;
use std::time::Duration;

use borsh::{BorshDeserialize, BorshSerialize};
use tokio::time::Instant;

use crate::tcp::{tcp_server::TcpServer, TcpEvent, TcpHeaders, TcpMessageLabel};

use super::{SendErrorContext, SendErrorOutcome, TcpServerMiddleware};

#[derive(Default)]
pub struct DropOnError;

impl<Req, Res> TcpServerMiddleware<Req, Res> for DropOnError
where
    Req: BorshSerialize + BorshDeserialize + std::fmt::Debug + Send + TcpMessageLabel + 'static,
    Res: BorshSerialize + BorshDeserialize + std::fmt::Debug + TcpMessageLabel,
{
    type EventOut = TcpEvent<Req>;

    fn on_event(
        &mut self,
        server: &mut TcpServer<Req, Res>,
        event: TcpEvent<Req>,
    ) -> Option<Self::EventOut> {
        match &event {
            TcpEvent::Error { socket_addr, .. } | TcpEvent::Closed { socket_addr } => {
                server.drop_peer_stream(socket_addr.clone());
            }
            _ => {}
        }
        Some(event)
    }
}

fn min_wakeup(lhs: Option<Instant>, rhs: Option<Instant>) -> Option<Instant> {
    match (lhs, rhs) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    }
}

pub struct EventPipeline<A, B> {
    first: A,
    second: B,
}

impl<A, B> EventPipeline<A, B> {
    pub fn new(first: A, second: B) -> Self {
        Self { first, second }
    }

    pub(crate) fn second_mut(&mut self) -> &mut B {
        &mut self.second
    }
}

impl<Req, Res, A, B> TcpServerMiddleware<Req, Res> for EventPipeline<A, B>
where
    Req: BorshSerialize + BorshDeserialize + std::fmt::Debug + Send + TcpMessageLabel + 'static,
    Res: BorshSerialize + BorshDeserialize + std::fmt::Debug + TcpMessageLabel + Clone,
    A: TcpServerMiddleware<Req, Res, EventOut = TcpEvent<Req>>,
    B: TcpServerMiddleware<Req, Res>,
{
    type EventOut = B::EventOut;

    fn on_event(
        &mut self,
        server: &mut TcpServer<Req, Res>,
        event: TcpEvent<Req>,
    ) -> Option<Self::EventOut> {
        let event = self.first.on_event(server, event)?;
        self.second.on_event(server, event)
    }

    fn on_send_error(
        &mut self,
        server: &mut TcpServer<Req, Res>,
        ctx: &SendErrorContext<Res>,
    ) -> SendErrorOutcome {
        match self.first.on_send_error(server, ctx) {
            SendErrorOutcome::Unhandled(_) => self.second.on_send_error(server, ctx),
            outcome => outcome,
        }
    }

    fn on_tick(&mut self, server: &mut TcpServer<Req, Res>) {
        self.first.on_tick(server);
        self.second.on_tick(server);
    }

    fn next_wakeup(&self) -> Option<Instant> {
        min_wakeup(self.first.next_wakeup(), self.second.next_wakeup())
    }
}

pub struct TcpInboundMessage<Req> {
    pub socket_addr: String,
    pub data: Req,
    pub headers: TcpHeaders,
}

#[derive(Default)]
pub struct MessageOnly;

impl<Req, Res> TcpServerMiddleware<Req, Res> for MessageOnly
where
    Req: BorshSerialize + BorshDeserialize + std::fmt::Debug + Send + TcpMessageLabel + 'static,
    Res: BorshSerialize + BorshDeserialize + std::fmt::Debug + TcpMessageLabel,
{
    type EventOut = Req;

    fn on_event(
        &mut self,
        _server: &mut TcpServer<Req, Res>,
        event: TcpEvent<Req>,
    ) -> Option<Self::EventOut> {
        match event {
            TcpEvent::Message { data, .. } => Some(data),
            TcpEvent::Closed { .. } | TcpEvent::Error { .. } => None,
        }
    }
}

#[derive(Default)]
pub struct MessageWithMeta;

impl<Req, Res> TcpServerMiddleware<Req, Res> for MessageWithMeta
where
    Req: BorshSerialize + BorshDeserialize + std::fmt::Debug + Send + TcpMessageLabel + 'static,
    Res: BorshSerialize + BorshDeserialize + std::fmt::Debug + TcpMessageLabel,
{
    type EventOut = TcpInboundMessage<Req>;

    fn on_event(
        &mut self,
        _server: &mut TcpServer<Req, Res>,
        event: TcpEvent<Req>,
    ) -> Option<Self::EventOut> {
        match event {
            TcpEvent::Message {
                socket_addr,
                data,
                headers,
            } => Some(TcpInboundMessage {
                socket_addr,
                data,
                headers,
            }),
            TcpEvent::Closed { .. } | TcpEvent::Error { .. } => None,
        }
    }
}

struct QueuedOutbound<Res> {
    msg: Res,
    headers: TcpHeaders,
    retries: usize,
    next_attempt_at: Instant,
}

pub struct QueuedSendWithRetry<Req, Res> {
    max_retries: usize,
    base_delay: Duration,
    max_per_tick: usize,
    streaming_peers: HashSet<String>,
    queues: std::collections::HashMap<String, VecDeque<QueuedOutbound<Res>>>,
    _marker: PhantomData<Req>,
}

impl<Req, Res> QueuedSendWithRetry<Req, Res> {
    pub fn new(max_retries: usize, base_delay: Duration) -> Self {
        Self {
            max_retries,
            base_delay,
            max_per_tick: 64,
            streaming_peers: HashSet::new(),
            queues: std::collections::HashMap::new(),
            _marker: PhantomData,
        }
    }

    pub fn max_per_tick(mut self, max_per_tick: usize) -> Self {
        self.max_per_tick = max_per_tick.max(1);
        self
    }

    pub fn register_streaming_peer(&mut self, socket_addr: String) {
        self.streaming_peers.insert(socket_addr);
    }

    pub fn unregister_streaming_peer(&mut self, socket_addr: &str) {
        self.streaming_peers.remove(socket_addr);
        self.queues.remove(socket_addr);
    }

    pub fn enqueue_to_peer(&mut self, socket_addr: String, msg: Res, headers: TcpHeaders) {
        self.queues
            .entry(socket_addr)
            .or_default()
            .push_back(QueuedOutbound {
                msg,
                headers,
                retries: 0,
                next_attempt_at: Instant::now(),
            });
    }

    pub fn enqueue_to_streaming_peers(&mut self, msg: Res, headers: TcpHeaders)
    where
        Res: Clone,
    {
        for peer in self.streaming_peers.clone() {
            self.enqueue_to_peer(peer, msg.clone(), headers.clone());
        }
    }
}

pub trait QueuedSenderMiddleware<Req, Res>
where
    Req: BorshSerialize + BorshDeserialize + std::fmt::Debug + Send + TcpMessageLabel + 'static,
    Res: BorshSerialize + BorshDeserialize + std::fmt::Debug + TcpMessageLabel + Clone,
{
    fn enqueue_to_peer(&mut self, socket_addr: String, msg: Res, headers: TcpHeaders);
    fn register_streaming_peer(&mut self, socket_addr: String);
    fn enqueue_to_streaming_peers(&mut self, msg: Res, headers: TcpHeaders);
}

impl<Req, Res> QueuedSenderMiddleware<Req, Res> for QueuedSendWithRetry<Req, Res>
where
    Req: BorshSerialize + BorshDeserialize + std::fmt::Debug + Send + TcpMessageLabel + 'static,
    Res: BorshSerialize + BorshDeserialize + std::fmt::Debug + TcpMessageLabel + Clone,
{
    fn enqueue_to_peer(&mut self, socket_addr: String, msg: Res, headers: TcpHeaders) {
        QueuedSendWithRetry::enqueue_to_peer(self, socket_addr, msg, headers);
    }

    fn register_streaming_peer(&mut self, socket_addr: String) {
        QueuedSendWithRetry::register_streaming_peer(self, socket_addr);
    }

    fn enqueue_to_streaming_peers(&mut self, msg: Res, headers: TcpHeaders) {
        QueuedSendWithRetry::enqueue_to_streaming_peers(self, msg, headers);
    }
}

impl<Req, Res, A, B> QueuedSenderMiddleware<Req, Res> for EventPipeline<A, B>
where
    Req: BorshSerialize + BorshDeserialize + std::fmt::Debug + Send + TcpMessageLabel + 'static,
    Res: BorshSerialize + BorshDeserialize + std::fmt::Debug + TcpMessageLabel + Clone,
    A: TcpServerMiddleware<Req, Res, EventOut = TcpEvent<Req>>,
    B: QueuedSenderMiddleware<Req, Res>,
{
    fn enqueue_to_peer(&mut self, socket_addr: String, msg: Res, headers: TcpHeaders) {
        self.second_mut().enqueue_to_peer(socket_addr, msg, headers);
    }

    fn register_streaming_peer(&mut self, socket_addr: String) {
        self.second_mut().register_streaming_peer(socket_addr);
    }

    fn enqueue_to_streaming_peers(&mut self, msg: Res, headers: TcpHeaders) {
        self.second_mut().enqueue_to_streaming_peers(msg, headers);
    }
}

impl<Req, Res> TcpServerMiddleware<Req, Res> for QueuedSendWithRetry<Req, Res>
where
    Req: BorshSerialize + BorshDeserialize + std::fmt::Debug + Send + TcpMessageLabel + 'static,
    Res: BorshSerialize + BorshDeserialize + std::fmt::Debug + TcpMessageLabel + Clone,
{
    type EventOut = TcpInboundMessage<Req>;

    fn on_event(
        &mut self,
        _server: &mut TcpServer<Req, Res>,
        event: TcpEvent<Req>,
    ) -> Option<Self::EventOut> {
        match event {
            TcpEvent::Message {
                socket_addr,
                data,
                headers,
            } => Some(TcpInboundMessage {
                socket_addr,
                data,
                headers,
            }),
            TcpEvent::Closed { socket_addr } => {
                self.unregister_streaming_peer(&socket_addr);
                None
            }
            TcpEvent::Error { socket_addr, .. } => {
                self.unregister_streaming_peer(&socket_addr);
                None
            }
        }
    }

    fn on_send_error(
        &mut self,
        server: &mut TcpServer<Req, Res>,
        ctx: &SendErrorContext<Res>,
    ) -> SendErrorOutcome {
        if !server.connected(&ctx.socket_addr) {
            self.unregister_streaming_peer(&ctx.socket_addr);
            return SendErrorOutcome::Unhandled(anyhow::anyhow!(ctx.error.to_string()));
        }
        self.enqueue_to_peer(ctx.socket_addr.clone(), ctx.msg.clone(), ctx.headers.clone());
        SendErrorOutcome::RetryScheduled
    }

    fn on_tick(&mut self, server: &mut TcpServer<Req, Res>) {
        if self.queues.is_empty() {
            return;
        }

        let mut processed = 0usize;
        let now = Instant::now();
        let peers: Vec<String> = self.queues.keys().cloned().collect();

        for peer in peers {
            if processed >= self.max_per_tick {
                break;
            }

            if !server.connected(&peer) {
                self.unregister_streaming_peer(&peer);
                continue;
            }

            let Some(queue) = self.queues.get_mut(&peer) else {
                continue;
            };

            let Some(front) = queue.front_mut() else {
                continue;
            };

            if front.next_attempt_at > now {
                continue;
            }

            match server.send(peer.clone(), front.msg.clone(), front.headers.clone()) {
                Ok(()) => {
                    queue.pop_front();
                }
                Err(_) => {
                    front.retries += 1;
                    if front.retries > self.max_retries {
                        server.drop_peer_stream(peer.clone());
                        self.unregister_streaming_peer(&peer);
                    } else {
                        front.next_attempt_at =
                            now + self.base_delay.mul_f64(front.retries as f64);
                    }
                }
            }

            processed += 1;
        }

        self.queues.retain(|_, queue| !queue.is_empty());
    }

    fn next_wakeup(&self) -> Option<Instant> {
        self.queues
            .values()
            .filter_map(|queue| queue.front().map(|pending| pending.next_attempt_at))
            .min()
    }
}

struct PendingSend<Res> {
    socket_addr: String,
    msg: Res,
    headers: TcpHeaders,
    retries: usize,
    next_attempt_at: Instant,
}

pub struct RetryingSend<Res> {
    max_retries: usize,
    base_delay: Duration,
    max_per_tick: usize,
    queue: VecDeque<PendingSend<Res>>,
}

impl<Res> RetryingSend<Res> {
    pub fn new(max_retries: usize, base_delay: Duration) -> Self {
        Self {
            max_retries,
            base_delay,
            max_per_tick: 64,
            queue: VecDeque::new(),
        }
    }

    pub fn max_per_tick(mut self, max_per_tick: usize) -> Self {
        self.max_per_tick = max_per_tick.max(1);
        self
    }
}

impl<Req, Res> TcpServerMiddleware<Req, Res> for RetryingSend<Res>
where
    Req: BorshSerialize + BorshDeserialize + std::fmt::Debug + Send + TcpMessageLabel + 'static,
    Res: BorshSerialize + BorshDeserialize + std::fmt::Debug + TcpMessageLabel + Clone,
{
    type EventOut = TcpEvent<Req>;

    fn on_event(
        &mut self,
        _server: &mut TcpServer<Req, Res>,
        event: TcpEvent<Req>,
    ) -> Option<Self::EventOut> {
        Some(event)
    }

    fn on_send_error(
        &mut self,
        server: &mut TcpServer<Req, Res>,
        ctx: &SendErrorContext<Res>,
    ) -> SendErrorOutcome {
        if !server.connected(&ctx.socket_addr) {
            return SendErrorOutcome::Unhandled(anyhow::anyhow!(ctx.error.to_string()));
        }
        self.queue.push_back(PendingSend {
            socket_addr: ctx.socket_addr.clone(),
            msg: ctx.msg.clone(),
            headers: ctx.headers.clone(),
            retries: 0,
            next_attempt_at: Instant::now() + self.base_delay,
        });
        SendErrorOutcome::RetryScheduled
    }

    fn on_tick(&mut self, server: &mut TcpServer<Req, Res>) {
        if self.queue.is_empty() {
            return;
        }

        let now = Instant::now();
        let mut processed = 0usize;
        let mut remaining = VecDeque::with_capacity(self.queue.len());

        while let Some(mut pending) = self.queue.pop_front() {
            if pending.next_attempt_at > now || processed >= self.max_per_tick {
                remaining.push_back(pending);
                continue;
            }

            if !server.connected(&pending.socket_addr) {
                continue;
            }

            match server.send(
                pending.socket_addr.clone(),
                pending.msg.clone(),
                pending.headers.clone(),
            ) {
                Ok(()) => {}
                Err(_) => {
                    let next_retries = pending.retries + 1;
                    if next_retries > self.max_retries {
                        server.drop_peer_stream(pending.socket_addr);
                    } else {
                        pending.retries = next_retries;
                        pending.next_attempt_at =
                            now + self.base_delay.mul_f64(next_retries as f64);
                        remaining.push_back(pending);
                    }
                }
            }

            processed += 1;
        }

        self.queue = remaining;
    }

    fn next_wakeup(&self) -> Option<Instant> {
        self.queue
            .iter()
            .map(|pending| pending.next_attempt_at)
            .min()
    }
}

pub struct DropOnErrorAndRetry<Res> {
    drop_on_error: DropOnError,
    retrying_send: RetryingSend<Res>,
}

impl<Res> DropOnErrorAndRetry<Res> {
    pub fn new(max_retries: usize, base_delay: Duration) -> Self {
        Self {
            drop_on_error: DropOnError,
            retrying_send: RetryingSend::new(max_retries, base_delay),
        }
    }

    pub fn max_per_tick(mut self, max_per_tick: usize) -> Self {
        self.retrying_send = self.retrying_send.max_per_tick(max_per_tick);
        self
    }
}

impl<Req, Res> TcpServerMiddleware<Req, Res> for DropOnErrorAndRetry<Res>
where
    Req: BorshSerialize + BorshDeserialize + std::fmt::Debug + Send + TcpMessageLabel + 'static,
    Res: BorshSerialize + BorshDeserialize + std::fmt::Debug + TcpMessageLabel + Clone,
{
    type EventOut = TcpEvent<Req>;

    fn on_event(
        &mut self,
        server: &mut TcpServer<Req, Res>,
        event: TcpEvent<Req>,
    ) -> Option<Self::EventOut> {
        self.drop_on_error.on_event(server, event)
    }

    fn on_send_error(
        &mut self,
        server: &mut TcpServer<Req, Res>,
        ctx: &SendErrorContext<Res>,
    ) -> SendErrorOutcome {
        self.retrying_send.on_send_error(server, ctx)
    }

    fn on_tick(&mut self, server: &mut TcpServer<Req, Res>) {
        self.retrying_send.on_tick(server)
    }

    fn next_wakeup(&self) -> Option<Instant> {
        <RetryingSend<Res> as TcpServerMiddleware<Req, Res>>::next_wakeup(&self.retrying_send)
    }
}

use std::collections::VecDeque;
use std::time::Duration;

use tokio::time::Instant;

use crate::tcp::{TcpEvent, TcpHeaders, TcpServerLike};

use super::{SendErrorContext, SendErrorOutcome, TcpMiddleware};

#[derive(Default)]
pub struct DropOnError;

impl<Req, Res> TcpMiddleware<Req, Res> for DropOnError
where
    Req: super::TcpReqBound,
    Res: super::TcpResBound,
{
    type EventOut = TcpEvent<Req>;

    fn on_event<S>(&mut self, server: &mut S, event: TcpEvent<Req>) -> Option<Self::EventOut>
    where
        S: TcpServerLike<Req, Res, EventOut = TcpEvent<Req>>,
    {
        match &event {
            TcpEvent::Error { socket_addr, .. } | TcpEvent::Closed { socket_addr } => {
                server.drop_peer_stream(socket_addr.clone());
            }
            _ => {}
        }
        Some(event)
    }
}

pub type TcpInboundMessage<Req> = (String, Req, TcpHeaders);

#[derive(Default)]
pub struct MessageOnly;

impl<Req, Res> TcpMiddleware<Req, Res> for MessageOnly
where
    Req: super::TcpReqBound,
    Res: super::TcpResBound,
{
    type EventOut = TcpInboundMessage<Req>;

    fn on_event<S>(&mut self, _server: &mut S, event: TcpEvent<Req>) -> Option<Self::EventOut>
    where
        S: TcpServerLike<Req, Res, EventOut = TcpEvent<Req>>,
    {
        match event {
            TcpEvent::Message {
                socket_addr,
                data,
                headers,
            } => Some((socket_addr, data, headers)),
            TcpEvent::Closed { .. } | TcpEvent::Error { .. } => None,
        }
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

    pub fn enqueue_to_peer(&mut self, socket_addr: String, msg: Res, headers: TcpHeaders) {
        self.queue.push_back(PendingSend {
            socket_addr,
            msg,
            headers,
            retries: 0,
            next_attempt_at: Instant::now(),
        });
    }

    pub fn drop_peer(&mut self, socket_addr: &str) {
        self.queue
            .retain(|pending| pending.socket_addr != socket_addr);
    }
}

impl<Req, Res> TcpMiddleware<Req, Res> for RetryingSend<Res>
where
    Req: super::TcpReqBound,
    Res: super::TcpResBound + Clone,
{
    type EventOut = TcpEvent<Req>;

    fn on_event<S>(&mut self, _server: &mut S, event: TcpEvent<Req>) -> Option<Self::EventOut>
    where
        S: TcpServerLike<Req, Res, EventOut = TcpEvent<Req>>,
    {
        Some(event)
    }

    fn on_send_error<S>(&mut self, server: &mut S, ctx: &SendErrorContext<Res>) -> SendErrorOutcome
    where
        S: TcpServerLike<Req, Res, EventOut = TcpEvent<Req>>,
    {
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

    fn on_tick<S>(&mut self, server: &mut S)
    where
        S: TcpServerLike<Req, Res, EventOut = TcpEvent<Req>>,
    {
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

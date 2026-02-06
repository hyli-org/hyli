use std::collections::VecDeque;
use std::time::Duration;

use tokio::time::Instant;

use crate::tcp::{tcp_server::TcpServer, TcpEvent, TcpHeaders};

use super::{SendErrorContext, TcpServerMiddleware};

#[derive(Default)]
pub struct DropOnError;

impl<Req, Res> TcpServerMiddleware<Req, Res> for DropOnError {
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
    Res: Clone,
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
        ctx: SendErrorContext<Res>,
    ) -> anyhow::Result<()> {
        if !server.connected(&ctx.socket_addr) {
            return Err(ctx.error);
        }
        self.queue.push_back(PendingSend {
            socket_addr: ctx.socket_addr,
            msg: ctx.msg,
            headers: ctx.headers,
            retries: 0,
            next_attempt_at: Instant::now() + self.base_delay,
        });
        Ok(())
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
    Res: Clone,
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
        ctx: SendErrorContext<Res>,
    ) -> anyhow::Result<()> {
        self.retrying_send.on_send_error(server, ctx)
    }

    fn on_tick(&mut self, server: &mut TcpServer<Req, Res>) {
        self.retrying_send.on_tick(server)
    }

    fn next_wakeup(&self) -> Option<Instant> {
        self.retrying_send.next_wakeup()
    }
}

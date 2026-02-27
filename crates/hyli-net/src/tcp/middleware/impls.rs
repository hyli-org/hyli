use std::collections::{HashMap, HashSet, VecDeque};
use std::time::Duration;

use tokio::time::Instant;

use crate::tcp::{TcpEvent, TcpHeaders, TcpServerLike};

use super::{SendCompletion, SendErrorContext, SendErrorOutcome, SendStatus, TcpMiddleware};

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
    ticket: u64,
    next_attempt_at: Instant,
}

pub struct RetryingSend<Res> {
    max_retries: usize,
    base_delay: Duration,
    max_per_tick: usize,
    queue: VecDeque<PendingSend<Res>>,
    next_ticket: u64,
    completions: VecDeque<SendCompletion>,
}

impl<Res> RetryingSend<Res> {
    pub fn new(max_retries: usize, base_delay: Duration) -> Self {
        Self {
            max_retries,
            base_delay,
            max_per_tick: 64,
            queue: VecDeque::new(),
            next_ticket: 1,
            completions: VecDeque::new(),
        }
    }

    pub fn max_per_tick(mut self, max_per_tick: usize) -> Self {
        self.max_per_tick = max_per_tick.max(1);
        self
    }

    pub fn enqueue_to_peer(&mut self, socket_addr: String, msg: Res, headers: TcpHeaders) {
        let ticket = self.next_ticket;
        self.next_ticket = self.next_ticket.wrapping_add(1).max(1);
        self.queue.push_back(PendingSend {
            socket_addr,
            msg,
            headers,
            retries: 0,
            ticket,
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
        let ticket = self.next_ticket;
        self.next_ticket = self.next_ticket.wrapping_add(1).max(1);
        self.queue.push_back(PendingSend {
            socket_addr: ctx.socket_addr.clone(),
            msg: ctx.msg.clone(),
            headers: ctx.headers.clone(),
            retries: 0,
            ticket,
            next_attempt_at: Instant::now() + self.base_delay,
        });
        SendErrorOutcome::RetryScheduled { ticket }
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
                self.completions.push_back(SendCompletion::Failed {
                    ticket: pending.ticket,
                });
                continue;
            }

            match server.send(
                pending.socket_addr.clone(),
                pending.msg.clone(),
                pending.headers.clone(),
            ) {
                Ok(_) => {
                    self.completions.push_back(SendCompletion::Delivered {
                        ticket: pending.ticket,
                    });
                }
                Err(_) => {
                    let next_retries = pending.retries + 1;
                    if next_retries > self.max_retries {
                        server.drop_peer_stream(pending.socket_addr);
                        self.completions.push_back(SendCompletion::Failed {
                            ticket: pending.ticket,
                        });
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

    fn poll_send_completion(&mut self) -> Option<SendCompletion> {
        self.completions.pop_front()
    }
}

#[derive(Debug, Clone, Copy)]
pub enum AdvanceOn {
    Accepted,
    Delivered,
}

struct QueuedItem<In> {
    input: In,
    headers: TcpHeaders,
}

pub struct DequeDispatch<In, State, Resolve, Res> {
    state: State,
    resolver: Resolve,
    advance_on: AdvanceOn,
    tick_interval: Duration,
    max_queue_len: Option<usize>,
    queues: HashMap<String, VecDeque<QueuedItem<In>>>,
    in_flight: HashSet<String>,
    ticket_to_peer: HashMap<u64, String>,
    _res: std::marker::PhantomData<Res>,
}

impl<In, State, Resolve, Res> DequeDispatch<In, State, Resolve, Res> {
    pub fn new(state: State, resolver: Resolve) -> Self {
        Self {
            state,
            resolver,
            advance_on: AdvanceOn::Delivered,
            tick_interval: Duration::from_millis(5),
            max_queue_len: None,
            queues: HashMap::new(),
            in_flight: HashSet::new(),
            ticket_to_peer: HashMap::new(),
            _res: std::marker::PhantomData,
        }
    }

    pub fn advance_on(mut self, advance_on: AdvanceOn) -> Self {
        self.advance_on = advance_on;
        self
    }

    pub fn tick_interval(mut self, interval: Duration) -> Self {
        self.tick_interval = interval;
        self
    }

    pub fn max_queue_len(mut self, max_queue_len: usize) -> Self {
        self.max_queue_len = Some(max_queue_len.max(1));
        self
    }

    pub fn enqueue(
        &mut self,
        socket_addr: String,
        input: In,
        headers: TcpHeaders,
    ) -> anyhow::Result<()> {
        if let Some(max_len) = self.max_queue_len {
            let total_len: usize = self.queues.values().map(VecDeque::len).sum();
            if total_len >= max_len {
                return Err(anyhow::anyhow!(
                    "DequeDispatch queue is full (max_len={max_len})"
                ));
            }
        }

        self.queues
            .entry(socket_addr)
            .or_default()
            .push_back(QueuedItem { input, headers });
        Ok(())
    }

    pub fn send_now(
        &mut self,
        socket_addr: String,
        input: In,
        headers: TcpHeaders,
    ) -> anyhow::Result<()> {
        if let Some(max_len) = self.max_queue_len {
            let total_len: usize = self.queues.values().map(VecDeque::len).sum();
            if total_len >= max_len {
                return Err(anyhow::anyhow!(
                    "DequeDispatch queue is full (max_len={max_len})"
                ));
            }
        }

        self.queues
            .entry(socket_addr)
            .or_default()
            .push_front(QueuedItem { input, headers });
        Ok(())
    }
}

impl<Req, In, State, Resolve, Res> TcpMiddleware<Req, Res>
    for DequeDispatch<In, State, Resolve, Res>
where
    Req: super::TcpReqBound,
    Res: super::TcpResBound + Clone + Send + 'static,
    Resolve: Fn(&State, In) -> anyhow::Result<Res> + Send + 'static,
{
    type EventOut = TcpEvent<Req>;

    fn on_event<S>(&mut self, _server: &mut S, event: TcpEvent<Req>) -> Option<Self::EventOut>
    where
        S: TcpServerLike<Req, Res, EventOut = TcpEvent<Req>>,
    {
        Some(event)
    }

    fn on_tick<S>(&mut self, server: &mut S)
    where
        S: TcpServerLike<Req, Res, EventOut = TcpEvent<Req>>,
    {
        while let Some(completion) = server.poll_send_completion() {
            let ticket = match completion {
                SendCompletion::Delivered { ticket } | SendCompletion::Failed { ticket } => ticket,
            };
            if let Some(peer) = self.ticket_to_peer.remove(&ticket) {
                self.in_flight.remove(&peer);
            }
        }

        let eligible: Vec<String> = self
            .queues
            .iter()
            .filter(|(peer, queue)| !queue.is_empty() && !self.in_flight.contains(*peer))
            .map(|(peer, _)| peer.clone())
            .collect();

        for peer in eligible {
            let Some(queue) = self.queues.get_mut(&peer) else {
                continue;
            };
            let Some(item) = queue.pop_front() else {
                continue;
            };
            if queue.is_empty() {
                self.queues.remove(&peer);
            }

            let headers = item.headers;
            match (self.resolver)(&self.state, item.input) {
                Ok(msg) => match server.send(peer.clone(), msg, headers) {
                    Ok(SendStatus::SentNow) => {}
                    Ok(SendStatus::RetryScheduled { ticket }) => match self.advance_on {
                        AdvanceOn::Accepted => {}
                        AdvanceOn::Delivered => {
                            self.ticket_to_peer.insert(ticket, peer.clone());
                            self.in_flight.insert(peer);
                        }
                    },
                    Err(err) => {
                        tracing::warn!("DequeDispatch send error for peer {}: {:#}", peer, err);
                    }
                },
                Err(err) => {
                    tracing::warn!("DequeDispatch resolver error for peer {}: {:#}", peer, err);
                }
            }
        }
    }

    fn next_wakeup(&self) -> Option<Instant> {
        if self.queues.values().any(|q| !q.is_empty()) || !self.in_flight.is_empty() {
            Some(Instant::now() + self.tick_interval)
        } else {
            None
        }
    }
}

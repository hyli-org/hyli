//! Core message bus primitives.
//!
//! This module provides:
//! - [`SharedMessageBus`] — the central hub shared by all modules
//! - [`BusMessage`] — marker trait for messages that travel on the bus
//! - [`BusClientSender`] / [`BusClientReceiver`] — auto-implemented send/receive traits
//! - [`bus_client!`] — macro to declare a module's typed message contract
//! - `handle_messages!` — macro to build a `tokio::select!`-based event loop
//!
//! In normal usage you never interact with [`SharedMessageBus`] directly.
//! Instead you create a typed client with [`bus_client!`] or [`module_bus_client!`](crate::module_bus_client)
//! and use it inside `handle_messages!`.

use crate::utils::static_type_map::Pick;
use anymap::{any::Any, Map};
use metrics::BusMetrics;
use std::{any::type_name, sync::Arc};
use tokio::sync::{broadcast, Mutex};
#[cfg(feature = "instrumentation")]
pub use tracing_opentelemetry::OpenTelemetrySpanExt;

pub mod command_response;
pub mod metrics;

/// Default broadcast-channel capacity for message types that do not override [`BusMessage::CAPACITY`].
///
/// When the channel is full, [`BusClientSender::send`] returns an error instead of blocking.
/// Use [`BusClientSender::send_waiting_if_full`] if you need back-pressure.
pub const DEFAULT_CAPACITY: usize = 100000;

/// Alternative capacity for high-throughput message types that don't need a deep backlog.
pub const LOW_CAPACITY: usize = 10000;

/// Marker trait for types that can be sent over the [`SharedMessageBus`].
///
/// Every message type needs `Clone` (broadcast channels clone messages for each subscriber)
/// plus this trait, which lets you tune the channel capacity per type.
///
/// # Example
///
/// ```rust,ignore
/// #[derive(Clone)]
/// struct MyEvent { data: u32 }
///
/// impl hyli_bus::BusMessage for MyEvent {
///     // Optional: lower the capacity for low-volume types.
///     const CAPACITY: usize = hyli_bus::LOW_CAPACITY;
/// }
/// ```
pub trait BusMessage {
    /// Number of messages that can be queued before senders start seeing errors.
    const CAPACITY: usize = DEFAULT_CAPACITY;
    /// Threshold used by [`BusClientSender::send`] to detect a near-full channel
    /// and return an error early, leaving a small safety margin.
    const CAPACITY_IF_WAITING: usize = Self::CAPACITY - 10;
}
impl BusMessage for () {}

#[cfg(test)]
impl BusMessage for usize {}

type AnyMap = Map<dyn Any + Send + Sync>;

/// Internal wrapper around a message that optionally carries an OpenTelemetry span context.
///
/// You rarely need to create or inspect `BusEnvelope` directly.
/// [`BusClientSender`] and [`BusClientReceiver`] unwrap it transparently.
/// The `span(ctx)` syntax in `handle_messages!` gives you access to the
/// propagated context when the `instrumentation` feature is enabled.
#[derive(Clone)]
pub struct BusEnvelope<M> {
    message: M,
    #[cfg(feature = "instrumentation")]
    context: opentelemetry::Context,
}

impl<M> std::ops::Deref for BusEnvelope<M> {
    type Target = M;

    fn deref(&self) -> &Self::Target {
        &self.message
    }
}

impl<M> std::fmt::Debug for BusEnvelope<M>
where
    M: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BusEnvelope")
            .field("message", &self.message)
            .finish()
    }
}

impl<M> PartialEq for BusEnvelope<M>
where
    M: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.message == other.message
    }
}

impl<M> Eq for BusEnvelope<M> where M: Eq {}

impl<M> BusEnvelope<M> {
    #[cfg(feature = "instrumentation")]
    pub fn from_message(message: M) -> Self {
        let context = tracing::Span::current().context();
        Self { message, context }
    }

    #[cfg(not(feature = "instrumentation"))]
    pub fn from_message(message: M) -> Self {
        Self { message }
    }

    #[cfg(feature = "instrumentation")]
    pub fn context(&self) -> opentelemetry::Context {
        self.context.clone()
    }

    pub fn into_message(self) -> M {
        self.message
    }
}

pub type BusSender<M> = broadcast::Sender<BusEnvelope<M>>;
pub type BusReceiver<M> = broadcast::Receiver<BusEnvelope<M>>;

/// Create a `tracing::info_span` and, when the `instrumentation` feature is enabled,
/// set its OpenTelemetry parent to `$ctx`.
///
/// This is a thin wrapper around [`tracing::info_span!`] that handles the
/// conditional propagation of distributed-tracing context across bus boundaries.
///
/// # Example
///
/// ```rust,ignore
/// handle_messages! {
///     on_bus self.bus,
///     listen<SomeEvent> ev, span(ctx) => {
///         let _span = info_span_ctx!("handle_some_event", ctx, event = ?ev);
///         // ... process ev
///     }
/// }
/// ```
#[macro_export]
macro_rules! info_span_ctx {
    ($name:expr, $ctx:expr $(, $($arg:tt)*)? ) => {{
        let span = tracing::info_span!($name $(, $($arg)*)?);
        #[cfg(feature = "instrumentation")]
        {
            use $crate::bus::OpenTelemetrySpanExt;
            span.set_parent($ctx);
        }
        #[cfg(not(feature = "instrumentation"))]
        let _ = &$ctx;
        span
    }};
}
pub use info_span_ctx;

/// Thread-safe, type-indexed collection of broadcast channels.
///
/// `SharedMessageBus` is the backbone of the whole system. It is an
/// `Arc<Mutex<AnyMap>>` that lazily creates one [`tokio::sync::broadcast`]
/// channel per message type the first time a sender or receiver is requested.
///
/// # Sharing handles
///
/// Call [`new_handle`](SharedMessageBus::new_handle) to get a cheap clone
/// that shares the same underlying channel map. Every module typically holds
/// its own handle, which it uses to subscribe to the channels it declared in
/// its [`module_bus_client!`](crate::module_bus_client) struct.
///
/// # Lifecycle guarantee
///
/// Build *all* your modules with [`ModulesHandler::build_module`](crate::ModulesHandler::build_module)
/// **before** calling [`start_modules`](crate::ModulesHandler::start_modules).
/// This ensures every receiver is subscribed before any sender fires, so no
/// message is silently dropped.
pub struct SharedMessageBus {
    channels: Arc<Mutex<AnyMap>>,
    pub metrics: BusMetrics,
}

impl SharedMessageBus {
    /// Returns a new handle that shares the same underlying channel map.
    ///
    /// This is cheap — it only clones an `Arc`.
    pub fn new_handle(&self) -> Self {
        SharedMessageBus {
            channels: Arc::clone(&self.channels),
            metrics: self.metrics.clone(),
        }
    }

    /// Creates a fresh, empty bus.
    ///
    /// Typically called once in `main` and then passed to [`ModulesHandler::new`](crate::ModulesHandler::new).
    pub fn new() -> Self {
        Self {
            channels: Arc::new(Mutex::new(AnyMap::new())),
            metrics: BusMetrics::global(),
        }
    }

    async fn receiver<M: Send + Sync + Clone + BusMessage + 'static>(&self) -> BusReceiver<M> {
        self.sender().await.subscribe()
    }

    async fn sender<M: Send + Sync + Clone + BusMessage + 'static>(&self) -> BusSender<M> {
        self.channels
            .lock()
            .await
            .entry::<BusSender<M>>()
            .or_insert_with(|| broadcast::channel(<M as BusMessage>::CAPACITY).0)
            .clone()
    }
}

pub mod dont_use_this {
    use super::*;
    /// Get a sender for a specific message type.
    /// Intended for use by BusClient implementations only.
    pub async fn get_sender<M: Send + Sync + Clone + BusMessage + 'static>(
        bus: &SharedMessageBus,
    ) -> BusSender<M> {
        bus.sender::<M>().await
    }

    pub async fn get_receiver<M: Send + Sync + Clone + BusMessage + 'static>(
        bus: &SharedMessageBus,
    ) -> BusReceiver<M> {
        bus.receiver::<M>().await
    }

    // why isn't this in the stdlib
    pub fn get_type_name<T: Any>(_: &T) -> &'static str {
        std::any::type_name::<T>()
    }
}

impl Default for SharedMessageBus {
    fn default() -> Self {
        Self::new()
    }
}

/// Sending capability automatically implemented for bus clients that own a [`BusSender<T>`].
///
/// You get this implementation for free when you declare `sender(T)` inside [`bus_client!`].
/// The trait is auto-implemented — you never implement it manually.
///
/// # Behaviour
///
/// - [`send`](BusClientSender::send) is fire-and-forget. It returns `Ok(())` even when there are
///   no receivers (messages are silently dropped). It returns `Err` only when the channel is
///   at [`BusMessage::CAPACITY_IF_WAITING`], indicating back-pressure.
/// - [`send_waiting_if_full`](BusClientSender::send_waiting_if_full) spins with 100 ms sleeps
///   until space is available, logging a warning every ~10 s.
/// - [`send_with_context`](BusClientSender::send_with_context) attaches a pre-built
///   OpenTelemetry context instead of capturing the current span automatically.
pub trait BusClientSender<T> {
    /// Fire-and-forget send. Returns `Err` if the channel is near capacity.
    fn send(&mut self, message: T) -> anyhow::Result<()>;

    /// Like [`send`](BusClientSender::send) but waits until the channel has room.
    ///
    /// Prefer this when you cannot afford to drop messages and the consumer may
    /// be temporarily slower than the producer.
    fn send_waiting_if_full(
        &mut self,
        message: T,
    ) -> impl std::future::Future<Output = anyhow::Result<()>> + Send
    where
        Self: Send,
        T: Send;

    /// Send with an explicit OpenTelemetry [`Context`](opentelemetry::Context) instead of
    /// capturing the current tracing span.
    fn send_with_context(
        &mut self,
        message: T,
        context: opentelemetry::Context,
    ) -> anyhow::Result<()>;
}

/// Receiving capability automatically implemented for bus clients that own a [`BusReceiver<T>`].
///
/// You get this implementation for free when you declare `receiver(T)` inside [`bus_client!`].
/// In practice you use the higher-level `handle_messages!` macro rather than calling
/// these methods directly.
pub trait BusClientReceiver<T> {
    /// Await the next message of type `T`.
    fn recv(
        &mut self,
    ) -> impl std::future::Future<Output = Result<T, tokio::sync::broadcast::error::RecvError>> + Send;

    /// Non-blocking receive. Returns [`TryRecvError::Empty`](tokio::sync::broadcast::error::TryRecvError) if no message is ready.
    fn try_recv(&mut self) -> Result<T, tokio::sync::broadcast::error::TryRecvError>;
}

/// Declare a typed bus client struct.
///
/// `bus_client!` generates a struct that:
/// - holds one [`BusSender<T>`] for every `sender(T)` entry
/// - holds one [`BusReceiver<T>`] for every `receiver(T)` entry
/// - implements [`BusClientSender<T>`] and [`BusClientReceiver<T>`] automatically
/// - implements `Clone` (receivers re-subscribe to the broadcast channel)
/// - provides `new_from_bus(bus)` to construct itself from a [`SharedMessageBus`]
///
/// The generated struct is also a compile-time contract: the type system
/// guarantees that a module can only send or receive the messages it declared.
///
/// For modules, prefer [`module_bus_client!`](crate::module_bus_client) which additionally
/// subscribes to [`ShutdownModule`](crate::modules::signal::ShutdownModule) and
/// [`PersistModule`](crate::modules::signal::PersistModule).
///
/// # Example
///
/// ```rust,ignore
/// use hyli_bus::bus_client;
///
/// bus_client! {
///     pub struct MempoolBusClient {
///         sender(MempoolStatusEvent),
///         receiver(ConsensusEvent),
///         receiver(NodeStateEvent),
///     }
/// }
///
/// // Later, in a test or main:
/// let client = MempoolBusClient::new_from_bus(bus.new_handle()).await;
/// ```
#[macro_export]
macro_rules! bus_client {
    (
        $(#[$meta:meta])*
        $pub:vis struct $name:ident $(< $( $lt:tt $( : $clt:tt $(+ $dlt:tt )* )? ),+ >)? {
            $(sender($sender:ty),)*
            $(receiver($receiver:ty),)*
        }
    ) => {
        $crate::utils::static_type_map::static_type_map! {
            $(#[$meta])*
            $pub struct $name $(< $( $lt $( : $clt $(+ $dlt )* )? ),+ >)? (
                $crate::bus::metrics::BusMetrics,
                $( $crate::bus::BusSender<$sender>,)*
                $( $crate::bus::BusReceiver<$receiver>,)*
            );
        }
        impl $(< $( $lt $( : $clt $(+ $dlt )* )? ),+ >)? $name $(< $( $lt ),+ >)? {
            pub async fn new_from_bus(bus: $crate::bus::SharedMessageBus) -> $name $(< $( $lt ),+ >)? {
                $name::new(
                    bus.metrics
                        .clone()
                        .with_client_name(stringify!($name).to_string()),
                    $($crate::bus::dont_use_this::get_sender::<$sender>(&bus).await,)*
                    $($crate::bus::dont_use_this::get_receiver::<$receiver>(&bus).await,)*
                )
            }
        }
        impl $(< $( $lt $( : $clt $(+ $dlt )* )? ),+ >)? Clone for $name $(< $( $lt ),+ >)? {
            fn clone(&self) -> Self {
                use $crate::utils::static_type_map::Pick;
                $name::new(
                    Pick::<$crate::bus::metrics::BusMetrics>::get(self).clone(),
                    $(Pick::<$crate::bus::BusSender<$sender>>::get(self).clone(),)*
                    $(Pick::<$crate::bus::BusReceiver<$receiver>>::get(self).resubscribe(),)*
                )

            }
        }
    };
}
pub use bus_client;

impl<Client, Msg: Clone + BusMessage + 'static> BusClientSender<Msg> for Client
where
    Client: Pick<BusSender<Msg>> + Pick<BusMetrics> + 'static,
{
    fn send(&mut self, message: Msg) -> anyhow::Result<()> {
        if Pick::<BusSender<Msg>>::get(self).receiver_count() > 0 {
            // We have a potential TOCTOU race here, so use a buffer.
            if Pick::<BusSender<Msg>>::get(self).len() >= <Msg as BusMessage>::CAPACITY_IF_WAITING {
                anyhow::bail!("Channel is full, cannot send message");
            }
            Pick::<BusMetrics>::get_mut(self).send::<Msg, Client>();
            Pick::<BusSender<Msg>>::get(self)
                .send(BusEnvelope::from_message(message))
                // Error is always "channel closed" so let's replace that
                .map_err(|_| anyhow::anyhow!("Failed to send message"))?;
        }
        Ok(())
    }

    #[allow(unused_variables)]
    fn send_with_context(
        &mut self,
        message: Msg,
        context: opentelemetry::Context,
    ) -> anyhow::Result<()> {
        if Pick::<BusSender<Msg>>::get(self).receiver_count() > 0 {
            // We have a potential TOCTOU race here, so use a buffer.
            if Pick::<BusSender<Msg>>::get(self).len() >= <Msg as BusMessage>::CAPACITY_IF_WAITING {
                anyhow::bail!("Channel is full, cannot send message");
            }
            Pick::<BusMetrics>::get_mut(self).send::<Msg, Client>();
            Pick::<BusSender<Msg>>::get(self)
                .send(BusEnvelope {
                    message,
                    #[cfg(feature = "instrumentation")]
                    context,
                })
                // Error is always "channel closed" so let's replace that
                .map_err(|_| anyhow::anyhow!("Failed to send message"))?;
        }
        Ok(())
    }

    async fn send_waiting_if_full(&mut self, message: Msg) -> anyhow::Result<()>
    where
        Client: Send,
        Msg: Send,
    {
        if Pick::<BusSender<Msg>>::get(self).receiver_count() > 0 {
            let mut i = 0;
            const HIGH_NB_OF_ATTEMPTS: usize = 100; // 10s limit, we assume longer would indicate an error
            loop {
                // We have a potential TOCTOU race here, so use a buffer.
                if Pick::<BusSender<Msg>>::get(self).len()
                    >= <Msg as BusMessage>::CAPACITY_IF_WAITING
                {
                    if i % HIGH_NB_OF_ATTEMPTS == 0 {
                        tracing::warn!(
                            "Channel {} is full (client {}), cannot send message, waiting another 10s...",
                            type_name::<Msg>(),
                            type_name::<Client>()
                        );
                    }
                    i += 1;
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                } else {
                    Pick::<BusMetrics>::get_mut(self).send::<Msg, Client>();
                    break Pick::<BusSender<Msg>>::get(self)
                        .send(BusEnvelope::from_message(message))
                        .map(|_| ())
                        // Error is always "channel closed" so let's replace that
                        .map_err(|_| anyhow::anyhow!("Failed to send message"))?;
                }
            }
        }
        Ok(())
    }
}

impl<Client, Msg: 'static + Clone + Send> BusClientReceiver<Msg> for Client
where
    Client: Pick<BusReceiver<Msg>> + Pick<BusMetrics> + 'static + Send,
{
    fn recv(
        &mut self,
    ) -> impl std::future::Future<Output = Result<Msg, tokio::sync::broadcast::error::RecvError>> + Send
    {
        Pick::<BusMetrics>::get_mut(self).receive::<Msg, Client>();
        async move {
            let envelope = Pick::<BusReceiver<Msg>>::get_mut(self).recv().await?;
            Ok(envelope.into_message())
        }
    }

    fn try_recv(&mut self) -> Result<Msg, tokio::sync::broadcast::error::TryRecvError> {
        Pick::<BusMetrics>::get_mut(self).receive::<Msg, Client>();
        let envelope = Pick::<BusReceiver<Msg>>::get_mut(self).try_recv()?;
        Ok(envelope.into_message())
    }
}

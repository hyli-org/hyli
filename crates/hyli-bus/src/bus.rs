//! Event bus used for messaging across components asynchronously.

use crate::utils::static_type_map::Pick;
use anymap::{Map, any::Any};
use metrics::BusMetrics;
use std::{any::type_name, sync::Arc};
use tokio::sync::{Mutex, broadcast};
#[cfg(feature = "instrumentation")]
pub use tracing_opentelemetry::OpenTelemetrySpanExt;

pub mod command_response;
pub mod metrics;

pub const DEFAULT_CAPACITY: usize = 100000;
pub const LOW_CAPACITY: usize = 10000;

pub trait BusMessage {
    const CAPACITY: usize = DEFAULT_CAPACITY;
    const CAPACITY_IF_WAITING: usize = Self::CAPACITY - 10;
}
impl BusMessage for () {}

#[cfg(test)]
impl BusMessage for usize {}

type AnyMap = Map<dyn Any + Send + Sync>;

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

/// Create an info span and optionally set its parent to the provided span/context.
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

pub struct SharedMessageBus {
    channels: Arc<Mutex<AnyMap>>,
    pub metrics: BusMetrics,
}

impl SharedMessageBus {
    pub fn new_handle(&self) -> Self {
        SharedMessageBus {
            channels: Arc::clone(&self.channels),
            metrics: self.metrics.clone(),
        }
    }

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
}

impl Default for SharedMessageBus {
    fn default() -> Self {
        Self::new()
    }
}

pub trait BusClientSender<T> {
    fn send(&mut self, message: T) -> anyhow::Result<()>;
    fn send_waiting_if_full(
        &mut self,
        message: T,
    ) -> impl std::future::Future<Output = anyhow::Result<()>> + Send
    where
        Self: Send,
        T: Send;

    fn send_with_context(
        &mut self,
        message: T,
        context: opentelemetry::Context,
    ) -> anyhow::Result<()>;
}
pub trait BusClientReceiver<T> {
    fn recv(
        &mut self,
    ) -> impl std::future::Future<Output = Result<T, tokio::sync::broadcast::error::RecvError>> + Send;
    fn try_recv(&mut self) -> Result<T, tokio::sync::broadcast::error::TryRecvError>;
}

/// Macro to create  a struct that registers sender/receiver using a shared bus.
/// This can be used to ensure that channels are open without locking in a typesafe manner.
/// It also serves as documentation for the types of messages used by each modules.
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

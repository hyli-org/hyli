//! Event bus used for messaging across components asynchronously.

use crate::utils::static_type_map::Pick;
use anymap::{any::Any, Map};
use metrics::BusMetrics;
use std::sync::Arc;
use tokio::sync::{broadcast, Mutex};

pub mod command_response;
pub mod metrics;

// Arbitrarily "high enough" value. Memory use is around 200Mb when setting this,
// we can lower it for some rarely used channels if needed.
pub const CHANNEL_CAPACITY: usize = 100000;
pub const CHANNEL_CAP_IF_WAITING: usize = CHANNEL_CAPACITY - 10;

type AnyMap = Map<dyn Any + Send + Sync>;

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

    pub fn new(metrics: BusMetrics) -> Self {
        Self {
            channels: Arc::new(Mutex::new(AnyMap::new())),
            metrics,
        }
    }

    async fn receiver<M: Send + Sync + Clone + 'static>(&self) -> broadcast::Receiver<M> {
        self.sender().await.subscribe()
    }

    async fn sender<M: Send + Sync + Clone + 'static>(&self) -> broadcast::Sender<M> {
        self.channels
            .lock()
            .await
            .entry::<broadcast::Sender<M>>()
            .or_insert_with(|| broadcast::channel(CHANNEL_CAPACITY).0)
            .clone()
    }
}

pub mod dont_use_this {
    use super::*;
    /// Get a sender for a specific message type.
    /// Intended for use by BusClient implementations only.
    pub async fn get_sender<M: Send + Sync + Clone + 'static>(
        bus: &SharedMessageBus,
    ) -> broadcast::Sender<M> {
        bus.sender::<M>().await
    }

    pub async fn get_receiver<M: Send + Sync + Clone + 'static>(
        bus: &SharedMessageBus,
    ) -> broadcast::Receiver<M> {
        bus.receiver::<M>().await
    }
}

impl Default for SharedMessageBus {
    fn default() -> Self {
        Self::new(BusMetrics::global("default".to_string()))
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
                $(tokio::sync::broadcast::Sender<$sender>,)*
                $(tokio::sync::broadcast::Receiver<$receiver>,)*
            );
        }
        impl $(< $( $lt $( : $clt $(+ $dlt )* )? ),+ >)? $name $(< $( $lt ),+ >)? {
            pub async fn new_from_bus(bus: $crate::bus::SharedMessageBus) -> $name $(< $( $lt ),+ >)? {
                $name::new(
                    bus.metrics.clone(),
                    $($crate::bus::dont_use_this::get_sender::<$sender>(&bus).await,)*
                    $($crate::bus::dont_use_this::get_receiver::<$receiver>(&bus).await,)*
                )
            }
        }
    };
}
pub use bus_client;

impl<Client, Msg: Clone + 'static> BusClientSender<Msg> for Client
where
    Client: Pick<tokio::sync::broadcast::Sender<Msg>> + Pick<BusMetrics> + 'static,
{
    fn send(&mut self, message: Msg) -> anyhow::Result<()> {
        if Pick::<tokio::sync::broadcast::Sender<Msg>>::get(self).receiver_count() > 0 {
            // We have a potential TOCTOU race here, so use a buffer.
            if Pick::<tokio::sync::broadcast::Sender<Msg>>::get(self).len()
                >= CHANNEL_CAP_IF_WAITING
            {
                anyhow::bail!("Channel is full, cannot send message");
            }
            Pick::<BusMetrics>::get_mut(self).send::<Msg, Client>();
            Pick::<tokio::sync::broadcast::Sender<Msg>>::get(self)
                .send(message)
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
        if Pick::<tokio::sync::broadcast::Sender<Msg>>::get(self).receiver_count() > 0 {
            let mut i = 0;
            const MAX_ATTEMPTS: usize = 100; // 10s limit, we assume longer would indicate an error
            loop {
                // We have a potential TOCTOU race here, so use a buffer.
                if Pick::<tokio::sync::broadcast::Sender<Msg>>::get(self).len()
                    >= CHANNEL_CAP_IF_WAITING
                {
                    if i >= MAX_ATTEMPTS {
                        anyhow::bail!("Channel is full, cannot send message");
                    }
                    i += 1;
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                } else {
                    Pick::<BusMetrics>::get_mut(self).send::<Msg, Client>();
                    break Pick::<tokio::sync::broadcast::Sender<Msg>>::get(self)
                        .send(message)
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
    Client: Pick<tokio::sync::broadcast::Receiver<Msg>> + Pick<BusMetrics> + 'static,
{
    fn recv(
        &mut self,
    ) -> impl std::future::Future<Output = Result<Msg, tokio::sync::broadcast::error::RecvError>> + Send
    {
        Pick::<BusMetrics>::get_mut(self).receive::<Msg, Client>();
        Pick::<tokio::sync::broadcast::Receiver<Msg>>::get_mut(self).recv()
    }

    fn try_recv(&mut self) -> Result<Msg, tokio::sync::broadcast::error::TryRecvError> {
        Pick::<BusMetrics>::get_mut(self).receive::<Msg, Client>();
        Pick::<tokio::sync::broadcast::Receiver<Msg>>::get_mut(self).try_recv()
    }
}

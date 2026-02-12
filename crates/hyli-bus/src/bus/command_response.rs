use std::sync::Arc;
use std::time::Duration;

use anyhow::bail;
use anyhow::Result;
use hyli_turmoil_shims::global_meter_or_panic;
use opentelemetry::{
    metrics::{Gauge, Histogram},
    KeyValue,
};
use tokio::sync::Mutex;

use crate::bus::{BusClientSender, BusMessage, BusReceiver};
use crate::modules::signal::shutdown_aware_timeout;
use crate::utils::profiling::LatencyMetricSink;
use crate::utils::static_type_map::Pick;

pub use hyli_turmoil_shims;

pub const CLIENT_TIMEOUT_SECONDS: u64 = 10;

#[derive(Clone, Debug)]
pub struct Query<Type, Answer>(Arc<Mutex<Option<InnerQuery<Type, Answer>>>>);
impl<Type, Answer> Query<Type, Answer> {
    pub fn take(self) -> Result<InnerQuery<Type, Answer>> {
        match self.0.try_lock() {
            Ok(mut guard) => match guard.take() {
                Some(inner) => Ok(inner),
                None => bail!("Query already answered"),
            },
            Err(_) => bail!("Query already answered"),
        }
    }
}

impl<Type, Answer> BusMessage for Query<Type, Answer> {}

#[derive(Debug)]
pub struct InnerQuery<Type, Answer> {
    pub callback: tokio::sync::oneshot::Sender<Result<Answer>>,
    pub data: Type,
}
impl<Cmd, Res> InnerQuery<Cmd, Res> {
    pub fn answer(self, data: Res) -> Result<()> {
        self.callback
            .send(Ok(data))
            .map_err(|_| anyhow::anyhow!("Error while sending response"))
    }
    pub fn bail<T>(self, error: T) -> Result<()>
    where
        T: Into<anyhow::Error>,
    {
        self.callback
            .send(Err(error.into()))
            .map_err(|_| anyhow::anyhow!("Error while sending response"))
    }
}

pub trait CmdRespClient<Cmd, Res>
where
    Cmd: Clone + Send + Sync + 'static,
    Res: Clone + Send + Sync + 'static,
{
    /// Sends a command and waits for a response.
    /// Prefer `shutdown_aware_request` if you want to handle shutdowns gracefully.
    fn request(&mut self, cmd: Cmd) -> impl std::future::Future<Output = Result<Res>> + Send;
    /// Sends a command and waits for a response,
    /// but will return early if a shutdown message for M is received.
    fn shutdown_aware_request<M: 'static>(
        &mut self,
        cmd: Cmd,
    ) -> impl std::future::Future<Output = Result<Res>> + Send
    where
        Self: Pick<BusReceiver<crate::modules::signal::ShutdownModule>>;
}

impl<Cmd, Res, T: BusClientSender<Query<Cmd, Res>> + Send> CmdRespClient<Cmd, Res> for T
where
    Cmd: Clone + Send + Sync + 'static,
    Res: Clone + Send + Sync + 'static,
{
    async fn request(&mut self, cmd: Cmd) -> Result<Res> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let query_cmd = Query(Arc::new(Mutex::new(Some(InnerQuery {
            callback: tx,
            data: cmd,
        }))));

        _ = self.send(query_cmd);

        match tokio::time::timeout(Duration::from_secs(CLIENT_TIMEOUT_SECONDS), rx).await {
            Ok(Ok(res)) => res,
            Ok(Err(e)) => bail!("Error while calling topic: {}", e),
            Err(timeouterror) => {
                bail!("Timeout triggered while calling topic with query: {timeouterror}",)
            }
        }
    }
    async fn shutdown_aware_request<M: 'static>(&mut self, cmd: Cmd) -> Result<Res>
    where
        Self: Pick<BusReceiver<crate::modules::signal::ShutdownModule>>,
    {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let query_cmd = Query(Arc::new(Mutex::new(Some(InnerQuery {
            callback: tx,
            data: cmd,
        }))));

        _ = self.send(query_cmd);

        match shutdown_aware_timeout::<M, _>(self, Duration::from_secs(CLIENT_TIMEOUT_SECONDS), rx)
            .await
        {
            Ok(Ok(res)) => res,
            Ok(Err(e)) => bail!("Error while calling topic: {}", e),
            Err(timeouterror) => {
                bail!("Timeout triggered while calling topic with query: {timeouterror}",)
            }
        }
    }
}

pub struct EventLoopMetrics {
    latency: Histogram<u64>,
    branch: Gauge<u64>,
}

impl EventLoopMetrics {
    pub fn global() -> EventLoopMetrics {
        let my_meter = global_meter_or_panic();

        EventLoopMetrics {
            latency: my_meter.u64_histogram("event_loop_latency").build(),
            branch: my_meter.u64_gauge("event_loop_branch_gauge").build(),
        }
    }

    pub fn record_branch(&self, branch_index: u64) {
        self.branch.record(branch_index, &[]);
    }
}
impl LatencyMetricSink for EventLoopMetrics {
    fn latency(&self, latency: u64, labels: &[KeyValue]) {
        self.latency.record(latency, labels);
    }
}

pub mod handle_messages_helpers {
    use crate::bus::metrics::BusMetrics;
    use crate::utils::static_type_map::Pick;

    use super::EventLoopMetrics;
    pub fn receive_bus_metrics<Msg: 'static, Client: Pick<BusMetrics> + 'static>(
        _bus: &mut Client,
    ) {
        Pick::<BusMetrics>::get_mut(_bus).receive::<Msg, Client>();
    }
    pub fn setup_metrics<T>(_t: &T) -> EventLoopMetrics {
        EventLoopMetrics::global()
    }
}

#[macro_export]
macro_rules! handle_messages {
    (on_bus $bus:expr, $($rest:tt)*) => {

        #[allow(unused_imports)]
        use $crate::utils::static_type_map::Pick;
        #[allow(unused_imports)]
        use $crate::bus::command_response::handle_messages_helpers::{receive_bus_metrics, setup_metrics};
        let event_loop_metrics = setup_metrics(&$bus);
        $crate::handle_messages! {
            metrics(event_loop_metrics) bus($bus) index(bus_receiver) branch_index(0u64) $($rest)*
        }
    };

    (
        $(processed $bind:pat = $fut:expr $(, if $cond:expr)? => $handle:block,)*
        metrics($metrics:expr) bus($bus:expr) index($index:ident) branch_index($branch_index:expr) $(,)?
        command_response<$command:ty, $response:ty> $res:pat $(, span($ctx:ident))? => $handler:block
        $($rest:tt)*
    ) => {
        // Create a receiver with a unique variable $index
        // Safety: this is disjoint.
        let $index = unsafe { &mut *Pick::<$crate::bus::BusReceiver<$crate::bus::command_response::Query<$command, $response>>>::splitting_get_mut(&mut $bus) };
        $crate::utils::static_type_map::paste::paste! {
        let [<branch_ $index>] = [
            Pick::<$crate::bus::metrics::BusMetrics>::get(&$bus).client_name(),
            $crate::KeyValue::new(
                "branch",
                $crate::bus::metrics::BusMetrics::simplified_name::<$crate::bus::command_response::Query<$command, $response>>(),
            ),
        ];
        $crate::handle_messages! {
            $(processed $bind = $fut $(, if $cond)? => $handle,)*
            processed Ok(_raw_query) = #[allow(clippy::macro_metavars_in_unsafe)] $index.recv() => {
                $metrics.record_branch($branch_index);
                receive_bus_metrics::<$crate::bus::command_response::Query<$command, $response>,_>(&mut $bus);
                let _latency = $crate::utils::profiling::LatencyTimer::new(&$metrics, &[<branch_ $index>]);
                $(
                    #[cfg(feature = "instrumentation")]
                    let $ctx = __envelope.context();
                    #[cfg(not(feature = "instrumentation"))]
                    let $ctx = ();
                )?
                if let Ok(mut _value) = _raw_query.into_message().take() {
                    let $res = &mut _value.data;
                    let res: Result<$response> = $handler;
                    match res {
                        Ok(res) => {
                            if let Err(e) = _value.answer(res) {
                                tracing::error!("Error while answering query: {}", e);
                            }
                        }
                        Err(e) => {
                            if let Err(e) = _value.bail(e) {
                                tracing::error!("Error while answering query: {}", e);
                            }
                        }
                    }
                } else {
                    tracing::error!("Query already answered");
                }
            },
            metrics($metrics) bus($bus) index([<$index a>]) branch_index($branch_index + 1u64) $($rest)*
        }
        }
    };

    (
        $(processed $bind:pat = $fut:expr $(, if $cond:expr)? => $handle:block,)*
        metrics($metrics:expr) bus($bus:expr) index($index:ident) branch_index($branch_index:expr) $(,)?
        listen<$message:ty> $res:pat $(, span($ctx:ident))? => $handler:block
        $($rest:tt)*
    ) => {
        // Safety: this is disjoint.
        let $index = unsafe { &mut *Pick::<$crate::bus::BusReceiver<$message>>::splitting_get_mut(&mut $bus) };
        $crate::utils::static_type_map::paste::paste! {
        let [<branch_ $index>] = [
            Pick::<$crate::bus::metrics::BusMetrics>::get(&$bus).client_name(),
            $crate::KeyValue::new(
                "branch",
                $crate::bus::metrics::BusMetrics::simplified_name::<$message>(),
            ),
        ];
        $crate::handle_messages! {
            $(processed $bind = $fut $(, if $cond)? => $handle,)*
            processed Ok(mut __envelope) = $index.recv() => {
                $metrics.record_branch($branch_index);
                receive_bus_metrics::<$message, _>(&mut $bus);
                let _latency = $crate::utils::profiling::LatencyTimer::new(&$metrics, &[<branch_ $index>]);
                $(
                    #[cfg(feature = "instrumentation")]
                    let $ctx = __envelope.context();
                    #[cfg(not(feature = "instrumentation"))]
                    let $ctx = ();
                )?
                let $res = __envelope.into_message();
                $handler
            },
            metrics($metrics) bus($bus) index([<$index a>]) branch_index($branch_index + 1u64) $($rest)*
        }
        }

        tracing::trace!("Remaining messages in topic {}: {}", stringify!($message), $index.len());
    };

    // Process default tokio case (only with blocks - no expressions for parsing)
    (
        $(processed $bind:pat = $fut:expr $(, if $cond:expr)? => $handle:block,)*
        metrics($metrics:expr) bus($bus:expr) index($index:ident) branch_index($branch_index:expr) $(,)?
        $bind2:pat = $fut2:expr $(, if $cond2:expr)? => $handler:block
        $($rest:tt)*
    ) => {
        $crate::utils::static_type_map::paste::paste! {
        let [<branch_ $index>] = [
            Pick::<$crate::bus::metrics::BusMetrics>::get(&$bus).client_name(),
            $crate::KeyValue::new("branch", stringify!($fut2)),
        ];
        $crate::handle_messages! {
            $(processed $bind = $fut $(, if $cond)? => $handle,)*
            processed $bind2 = $fut2 $(, if $cond2)? => {
                $metrics.record_branch($branch_index);
                let _latency = $crate::utils::profiling::LatencyTimer::new(&$metrics, &[<branch_ $index>]);
                $handler
            },
            metrics($metrics) bus($bus) index([<$index b>]) branch_index($branch_index + 1u64)
            $($rest)*
        }
        }
    };

    // Print all processed items in the tokio select
    (
        $(processed $bind:pat = $fut:expr $(, if $cond:expr)? => $handle:block,)*
        metrics($metrics:expr) bus($bus:expr) index($index:ident) branch_index($_branch_index:expr) $(,)?
    ) => {
        loop {
            // if false is necessary here so rust understands the loop can be broken
            // and avoid warnings like "unreachable code"
            if false {
                break;
            }
            let _core_loop_labels = [
                Pick::<$crate::bus::metrics::BusMetrics>::get(&$bus).client_name(),
                $crate::KeyValue::new("branch", "core_loop"),
            ];
            let _outer_latency =
                $crate::utils::profiling::LatencyTimer::new(&$metrics, &_core_loop_labels);
            $crate::bus::command_response::hyli_turmoil_shims::tokio_select_biased! {
                $($bind = $fut $(, if $cond)? => $handle,)*
            }
        }
    };
}

pub use handle_messages;

#[cfg(test)]
mod test {
    use super::*;
    use crate::bus::{bus_client, SharedMessageBus};

    bus_client!(
        struct TestBusClient {
            sender(Query<i32, u8>),
            receiver(Query<i32, u8>),
        }
    );

    #[tokio::test]
    async fn test_cmd_resp() {
        let shared_bus = SharedMessageBus::default();
        let mut sender = TestBusClient::new_from_bus(shared_bus.new_handle()).await;
        let mut receiver = TestBusClient::new_from_bus(shared_bus).await;

        // Spawn a task to handle the query
        tokio::spawn(async move {
            handle_messages! {
                on_bus receiver,
                command_response<i32, u8> _ => {
                    Ok(3)
                }
            }
        });
        let res = sender.request(42);

        assert_eq!(res.await.unwrap(), 3);
    }
}

use std::sync::Arc;

use opentelemetry::metrics::{Meter, MeterProvider};
use prometheus::Registry;

mod imp {
    use super::{Meter, MeterProvider};
    use std::sync::Arc;

    #[cfg(feature = "turmoil")]
    mod turmoil {
        use super::*;
        use opentelemetry::InstrumentationScope;
        use std::cell::RefCell;
        use std::collections::HashMap;
        use std::sync::OnceLock;
        use tracing::Subscriber;
        use tracing_subscriber::layer::Context;
        use tracing_subscriber::registry::LookupSpan;
        use tracing_subscriber::Layer;

        thread_local! {
            static CURRENT_HOST_STACK: RefCell<Vec<String>> = RefCell::new(Vec::new());
            static HOST_METER_PROVIDERS: RefCell<HashMap<String, Arc<dyn MeterProvider + Send + Sync>>> =
                RefCell::new(HashMap::new());
        }

        #[derive(Clone, Debug)]
        struct NodeName(String);

        #[derive(Default)]
        struct NodeNameVisitor {
            name: Option<String>,
        }

        impl tracing::field::Visit for NodeNameVisitor {
            fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
                if field.name() == "name" {
                    self.name = Some(value.to_string());
                }
            }

            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                if field.name() == "name" && self.name.is_none() {
                    self.name = Some(format!("{value:?}").trim_matches('"').to_string());
                }
            }
        }

        #[derive(Clone, Debug)]
        pub struct TurmoilHostSpanLayer;

        impl<S> Layer<S> for TurmoilHostSpanLayer
        where
            S: Subscriber + for<'a> LookupSpan<'a>,
        {
            fn on_new_span(
                &self,
                attrs: &tracing::span::Attributes<'_>,
                id: &tracing::Id,
                ctx: Context<'_, S>,
            ) {
                if attrs.metadata().name() != "node" {
                    return;
                }
                let mut visitor = NodeNameVisitor::default();
                attrs.record(&mut visitor);
                if let Some(name) = visitor.name {
                    if let Some(span) = ctx.span(id) {
                        span.extensions_mut().insert(NodeName(name));
                    }
                }
            }

            fn on_enter(&self, id: &tracing::Id, ctx: Context<'_, S>) {
                let Some(span) = ctx.span(id) else { return };
                let name = {
                    let extensions = span.extensions();
                    extensions.get::<NodeName>().map(|value| value.0.clone())
                };
                let Some(name) = name else { return };
                let _ = CURRENT_HOST_STACK.try_with(|stack| stack.borrow_mut().push(name));
            }

            fn on_exit(&self, id: &tracing::Id, ctx: Context<'_, S>) {
                let Some(span) = ctx.span(id) else { return };
                let name = {
                    let extensions = span.extensions();
                    extensions.get::<NodeName>().map(|value| value.0.clone())
                };
                let Some(name) = name else { return };
                let _ = CURRENT_HOST_STACK.try_with(|stack| {
                    let mut stack = stack.borrow_mut();
                    if stack.last().map(|v| v == &name).unwrap_or(false) {
                        stack.pop();
                    }
                });
            }
        }

        pub fn current_span_host_name() -> Option<String> {
            CURRENT_HOST_STACK
                .try_with(|stack| stack.borrow().last().cloned())
                .ok()
                .flatten()
        }

        pub fn register_host_meter_provider(
            host_name: impl Into<String>,
            provider: Arc<dyn MeterProvider + Send + Sync>,
        ) {
            let _ = HOST_METER_PROVIDERS.try_with(|map| {
                map.borrow_mut().insert(host_name.into(), provider);
            });
        }

        fn host_meter_provider(host_name: &str) -> Option<Arc<dyn MeterProvider + Send + Sync>> {
            HOST_METER_PROVIDERS
                .try_with(|map| map.borrow().get(host_name).cloned())
                .ok()
                .flatten()
        }

        pub fn init_global_meter_provider<P>(provider: P) -> Arc<dyn MeterProvider + Send + Sync>
        where
            P: MeterProvider + Send + Sync + Clone + 'static,
        {
            Arc::new(provider)
        }

        pub fn global_meter_or_panic() -> Meter {
            let host_name =
                current_span_host_name().expect("no current host span to get meter for");
            let provider =
                host_meter_provider(&host_name).expect("global meter provider is not initialized");
            let scope_name = format!("hyli/{}", host_name);
            tracing::info!(target: "hyli-turmoil-shims", "using meter scope {}", scope_name);
            provider.meter_with_scope(InstrumentationScope::builder(scope_name).build())
        }

        pub fn init_turmoil_test_tracing() -> Option<tracing::dispatcher::DefaultGuard> {
            use tracing_subscriber::layer::SubscriberExt;
            use tracing_subscriber::util::SubscriberInitExt;

            static TURMOIL_TEST_DISPATCH: OnceLock<tracing::Dispatch> = OnceLock::new();
            let _ = HOST_METER_PROVIDERS.try_with(|map| map.borrow_mut().clear());

            let make_subscriber = || {
                let mut filter = tracing_subscriber::EnvFilter::builder()
                    .with_default_directive(tracing::level_filters::LevelFilter::INFO.into())
                    .from_env_lossy();
                if let Ok(directive) = "hyli-turmoil-shims=info".parse() {
                    filter = filter.add_directive(directive);
                }

                tracing_subscriber::registry()
                    .with(TurmoilHostSpanLayer)
                    .with(filter)
                    .with(tracing_subscriber::fmt::layer().with_test_writer())
            };

            if make_subscriber().try_init().is_ok() {
                return None;
            }

            let dispatch =
                TURMOIL_TEST_DISPATCH.get_or_init(|| tracing::Dispatch::new(make_subscriber()));
            Some(tracing::dispatcher::set_default(dispatch))
        }
    }

    #[cfg(not(feature = "turmoil"))]
    mod non_turmoil {
        use super::*;
        use std::sync::OnceLock;

        static GLOBAL_METER_PROVIDER: OnceLock<Option<Arc<dyn MeterProvider + Send + Sync>>> =
            OnceLock::new();

        pub fn init_global_meter_provider<P>(provider: P) -> Arc<dyn MeterProvider + Send + Sync>
        where
            P: MeterProvider + Send + Sync + Clone + 'static,
        {
            opentelemetry::global::set_meter_provider(provider.clone());
            let provider = Arc::new(provider);
            let _ = GLOBAL_METER_PROVIDER.set(Some(provider.clone()));
            eprintln!("[hyli-turmoil-shims] global meter provider initialized");
            provider
        }

        pub fn global_meter_provider_or_panic() -> Arc<dyn MeterProvider + Send + Sync> {
            if let Some(Some(provider)) = GLOBAL_METER_PROVIDER.get() {
                return provider.clone();
            }

            if is_test_process() {
                let provider = GLOBAL_METER_PROVIDER.get_or_init(|| {
                    eprintln!("[hyli-turmoil-shims] initializing test meter provider");
                    Some(Arc::new(
                        opentelemetry_sdk::metrics::SdkMeterProvider::default(),
                    ))
                });
                return provider
                    .as_ref()
                    .expect("global meter provider is initialized for tests")
                    .clone();
            }

            panic!("global meter provider is not initialized");
        }

        pub fn global_meter_or_panic() -> Meter {
            global_meter_provider_or_panic().meter("hyli")
        }

        fn is_test_process() -> bool {
            std::env::var("RUST_TEST_THREADS").is_ok()
                || std::env::var("NEXTEST_EXECUTION").is_ok()
                || std::env::var("NEXTEST").is_ok()
                || std::env::args().any(|arg| arg.starts_with("--test-threads"))
        }
    }

    #[cfg(not(feature = "turmoil"))]
    pub use non_turmoil::{global_meter_or_panic, init_global_meter_provider};
    #[cfg(feature = "turmoil")]
    pub use turmoil::{
        current_span_host_name, global_meter_or_panic, init_global_meter_provider,
        init_turmoil_test_tracing, register_host_meter_provider, TurmoilHostSpanLayer,
    };
}

pub use imp::{global_meter_or_panic, init_global_meter_provider};

#[cfg(feature = "turmoil")]
pub use imp::{
    current_span_host_name, init_turmoil_test_tracing, register_host_meter_provider,
    TurmoilHostSpanLayer,
};

pub fn init_test_meter_provider() -> Arc<dyn MeterProvider + Send + Sync> {
    init_global_meter_provider(opentelemetry_sdk::metrics::SdkMeterProvider::default())
}

pub fn init_prometheus_registry_meter_provider(
) -> opentelemetry_sdk::metrics::MetricResult<Registry> {
    let registry = Registry::new();
    let provider = opentelemetry_sdk::metrics::SdkMeterProvider::builder()
        .with_reader(
            opentelemetry_prometheus::exporter()
                .with_registry(registry.clone())
                .build()?,
        )
        .build();

    init_global_meter_provider(provider);

    Ok(registry)
}

pub fn encode_registry_text(registry: &Registry) -> prometheus::Result<String> {
    use prometheus::{Encoder, TextEncoder};

    let mut buffer = Vec::new();
    let encoder = TextEncoder::new();
    encoder.encode(&registry.gather(), &mut buffer)?;
    String::from_utf8(buffer).map_err(|err| prometheus::Error::Msg(err.to_string()))
}

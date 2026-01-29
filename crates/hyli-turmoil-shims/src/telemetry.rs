use std::sync::Arc;

use opentelemetry::metrics::{Meter, MeterProvider};
use prometheus::Registry;

mod imp {
    use super::{Meter, MeterProvider};
    use std::sync::Arc;

    #[cfg(feature = "turmoil")]
    mod turmoil {
        use super::*;
        use std::cell::RefCell;

        thread_local! {
            static THREAD_METER_PROVIDER: RefCell<Option<Arc<dyn MeterProvider + Send + Sync>>> =
                const { RefCell::new(None) };
        }

        pub fn init_global_meter_provider<P>(provider: P) -> Arc<dyn MeterProvider + Send + Sync>
        where
            P: MeterProvider + Send + Sync + Clone + 'static,
        {
            let provider = Arc::new(provider);
            THREAD_METER_PROVIDER.with(|cell| {
                *cell.borrow_mut() = Some(provider.clone());
            });
            provider
        }

        pub fn global_meter_provider_or_panic() -> Arc<dyn MeterProvider + Send + Sync> {
            THREAD_METER_PROVIDER.with(|cell| {
                cell.borrow()
                    .clone()
                    .unwrap_or_else(|| panic!("global meter provider is not initialized"))
            })
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

        fn is_test_process() -> bool {
            std::env::var("RUST_TEST_THREADS").is_ok()
                || std::env::var("NEXTEST_EXECUTION").is_ok()
                || std::env::var("NEXTEST").is_ok()
                || std::env::args().any(|arg| arg.starts_with("--test-threads"))
        }
    }

    #[cfg(feature = "turmoil")]
    pub use turmoil::{global_meter_provider_or_panic, init_global_meter_provider};
    #[cfg(not(feature = "turmoil"))]
    pub use non_turmoil::{global_meter_provider_or_panic, init_global_meter_provider};

    pub fn global_meter_or_panic() -> Meter {
        global_meter_provider_or_panic().meter("hyli")
    }
}

pub use imp::{global_meter_or_panic, global_meter_provider_or_panic, init_global_meter_provider};

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

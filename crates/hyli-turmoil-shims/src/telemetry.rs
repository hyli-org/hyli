use std::sync::Arc;

pub use opentelemetry::{
    metrics::{Counter, Gauge, Histogram, Meter, MeterProvider},
    KeyValue,
};
pub use prometheus::Registry;

#[cfg(feature = "turmoil")]
use std::cell::RefCell;
#[cfg(not(feature = "turmoil"))]
use std::sync::OnceLock;

#[cfg(feature = "turmoil")]
thread_local! {
    static THREAD_METER_PROVIDER: RefCell<Option<Arc<dyn MeterProvider + Send + Sync>>> =
        const { RefCell::new(None) };
}

#[cfg(not(feature = "turmoil"))]
static GLOBAL_METER_PROVIDER: OnceLock<Option<Arc<dyn MeterProvider + Send + Sync>>> =
    OnceLock::new();

#[cfg(feature = "turmoil")]
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

#[cfg(not(feature = "turmoil"))]
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

#[cfg(feature = "turmoil")]
pub fn global_meter_provider_or_panic() -> Arc<dyn MeterProvider + Send + Sync> {
    THREAD_METER_PROVIDER.with(|cell| {
        cell.borrow()
            .clone()
            .unwrap_or_else(|| panic!("global meter provider is not initialized"))
    })
}

#[cfg(not(feature = "turmoil"))]
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

#[cfg(not(feature = "turmoil"))]
fn is_test_process() -> bool {
    std::env::var("RUST_TEST_THREADS").is_ok()
        || std::env::var("NEXTEST_EXECUTION").is_ok()
        || std::env::var("NEXTEST").is_ok()
}

pub fn global_meter_or_panic() -> Meter {
    global_meter_provider_or_panic().meter("hyli")
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

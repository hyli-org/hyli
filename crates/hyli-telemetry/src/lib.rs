use std::sync::{Arc, OnceLock};

pub use opentelemetry::{
    metrics::{Counter, Gauge, Histogram, Meter, MeterProvider},
    InstrumentationScope, KeyValue,
};
pub use prometheus::Registry;
use opentelemetry_sdk::metrics::MetricResult;
use prometheus::{Encoder, TextEncoder};

static GLOBAL_METER_PROVIDER: OnceLock<Option<Arc<dyn MeterProvider + Send + Sync>>> =
    OnceLock::new();

pub fn init_global_meter_provider<P>(provider: P) -> Arc<dyn MeterProvider + Send + Sync>
where
    P: MeterProvider + Send + Sync + Clone + 'static,
{
    opentelemetry::global::set_meter_provider(provider.clone());
    let provider = Arc::new(provider);
    let _ = GLOBAL_METER_PROVIDER.set(Some(provider.clone()));
    provider
}

pub fn global_meter_provider_or_panic() -> Arc<dyn MeterProvider + Send + Sync> {
    match GLOBAL_METER_PROVIDER.get() {
        Some(Some(provider)) => provider.clone(),
        _ => panic!("global meter provider is not initialized"),
    }
}

pub fn global_meter_with_scope_or_panic(scope: InstrumentationScope) -> Meter {
    global_meter_provider_or_panic().meter_with_scope(scope)
}

pub fn global_meter_with_id_or_panic(id: String) -> Meter {
    let scope = InstrumentationScope::builder(id).build();
    global_meter_with_scope_or_panic(scope)
}

pub fn global_meter_or_panic(name: &'static str) -> Meter {
    global_meter_provider_or_panic().meter(name)
}

pub fn init_prometheus_registry_meter_provider() -> MetricResult<Registry> {
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
    let mut buffer = Vec::new();
    let encoder = TextEncoder::new();
    encoder.encode(&registry.gather(), &mut buffer)?;
    String::from_utf8(buffer).map_err(|err| prometheus::Error::Msg(err.to_string()))
}

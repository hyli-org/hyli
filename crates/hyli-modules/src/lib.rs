// Re-export bus and macros from hyli-bus
pub use hyli_bus::bus;
pub use hyli_bus::{
    bus_client, handle_messages, info_span_ctx, log_debug, log_error, log_warn, module_bus_client,
    module_handle_messages,
};

pub mod modules;
pub mod node_state;
pub mod utils;
pub use hyli_turmoil_shims::tokio_select_biased;
pub mod telemetry {
    pub use hyli_turmoil_shims::{
        encode_registry_text, global_meter_or_panic, global_meter_provider_or_panic,
        init_global_meter_provider, init_prometheus_registry_meter_provider,
        init_test_meter_provider,
    };
    pub use opentelemetry::{
        KeyValue,
        metrics::{Counter, Gauge, Histogram, Meter, MeterProvider},
    };
    pub use prometheus::Registry;
}

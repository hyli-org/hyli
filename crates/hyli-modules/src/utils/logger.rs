use anyhow::{Context, Result};
use opentelemetry_otlp::{MetricExporter, Protocol, WithExportConfig};
use opentelemetry_sdk::metrics::exporter::PushMetricExporter;
use opentelemetry_sdk::resource::Resource;
use prometheus::Registry as PrometheusRegistry;
use std::time::Duration;
use tracing::info;
#[cfg(feature = "turmoil")]
use crate::utils::turmoil_time::{refresh_sim_elapsed, SimulatedTimeExporter};
#[cfg(feature = "instrumentation")]
use opentelemetry::trace::TracerProvider;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::Registry;
use tracing_subscriber::{layer::SubscriberExt, registry::LookupSpan, EnvFilter, Layer};

// Direct logging macros
/// Macro designed to log warnings
#[macro_export]
macro_rules! log_debug {
    // Pattern for format string with arguments
    ($result:expr, $fmt:literal, $($arg:tt)*) => {
        match $result {
            Err(e) => {
                let ae: anyhow::Error = e.into();
                let ae = ae.context(format!($fmt, $($arg)*));
                tracing::debug!(target: module_path!(), "{:#}", ae);
                Err(ae)
            }
            Ok(t) => Ok(t),
        }
    };
    // Pattern for a single expression (string or otherwise)
    ($result:expr, $context:expr) => {
        match $result {
            Err(e) => {
                let ae: anyhow::Error = e.into();
                let ae = ae.context($context);
                tracing::debug!(target: module_path!(), "{:#}", ae);
                Err(ae)
            }
            Ok(t) => Ok(t),
        }
    };
}

/// Macro designed to log warnings
#[macro_export]
macro_rules! log_warn {
    // Pattern for format string with arguments
    ($result:expr, $fmt:literal, $($arg:tt)*) => {
        match $result {
            Err(e) => {
                let ae: anyhow::Error = e.into();
                let ae = ae.context(format!($fmt, $($arg)*));
                tracing::warn!(target: module_path!(), "{:#}", ae);
                Err(ae)
            }
            Ok(t) => Ok(t),
        }
    };
    // Pattern for a single expression (string or otherwise)
    ($result:expr, $context:expr) => {
        match $result {
            Err(e) => {
                let ae: anyhow::Error = e.into();
                let ae = ae.context($context);
                tracing::warn!(target: module_path!(), "{:#}", ae);
                Err(ae)
            }
            Ok(t) => Ok(t),
        }
    };
}

/// Macro designed to log errors
#[macro_export]
macro_rules! log_error {
    // Pattern for format string with arguments
    ($result:expr, $fmt:literal, $($arg:tt)*) => {
        match $result {
            Err(e) => {
                let ae: anyhow::Error = e.into();
                let ae = ae.context(format!($fmt, $($arg)*));
                tracing::error!(target: module_path!(), "{:#}", ae);
                Err(ae)
            }
            Ok(t) => Ok(t),
        }
    };
    // Pattern for a single expression (string or otherwise)
    ($result:expr, $context:expr) => {
        match $result {
            Err(e) => {
                let ae: anyhow::Error = e.into();
                let ae = ae.context($context);
                tracing::error!(target: module_path!(), "{:#}", ae);
                Err(ae)
            }
            Ok(t) => Ok(t),
        }
    };
}

/// Custom formatter that appends node_name in front of full logs
struct NodeNameFormatter<T> {
    node_name: String,
    base_formatter: T,
}

impl<S, N, T> tracing_subscriber::fmt::FormatEvent<S, N> for NodeNameFormatter<T>
where
    S: tracing::Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> tracing_subscriber::fmt::FormatFields<'a> + 'static,
    T: tracing_subscriber::fmt::FormatEvent<S, N>,
{
    fn format_event(
        &self,
        ctx: &tracing_subscriber::fmt::FmtContext<'_, S, N>,
        mut writer: tracing_subscriber::fmt::format::Writer<'_>,
        event: &tracing::Event<'_>,
    ) -> std::fmt::Result {
        write!(&mut writer, "{} ", &self.node_name,)?;
        self.base_formatter.format_event(ctx, writer, event)
    }
}

pub enum TracingMode {
    /// Default tracing, for running a node locally
    Full,
    /// JSON tracing, for running a node in a container
    Json,
    /// Full tracing + node name, for e2e tests
    NodeName,
}

pub fn setup_tracing(log_format: &str, node_name: String) -> Result<()> {
    setup_otlp(log_format, node_name, false)
}

/// Setup tracing - stdout subscriber
/// stdout defaults to INFO to INFO even if RUST_LOG is set to e.g. debug
#[cfg_attr(not(feature = "instrumentation"), allow(unused_variables))]
pub fn setup_otlp(log_format: &str, node_name: String, tracing_enabled: bool) -> Result<()> {
    let mut filter = EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .from_env()?;

    let var = std::env::var("RUST_LOG").unwrap_or("".to_string());
    if !var.contains("risc0_zkvm") {
        filter = filter.add_directive("risc0_zkvm=info".parse()?);
    }
    if !var.contains("tokio") {
        filter = filter.add_directive("tokio=info".parse()?);
        filter = filter.add_directive("runtime=info".parse()?);
    }
    if !var.contains("fjall") {
        filter = filter.add_directive("fjall=warn".parse()?);
    }
    if !var.contains("sqlx") {
        filter = filter.add_directive("sqlx=warn".parse()?);
    }
    if !var.contains("opentelemetry") {
        filter = filter.add_directive("opentelemetry=warn".parse()?);
        filter = filter.add_directive("opentelemetry_sdk=warn".parse()?);
    }
    if !var.contains("risc0_zkvm") {
        std::env::set_var(
            "RUST_LOG",
            format!("{var},risc0_zkvm=warn,risc0_circuit_rv32im=warn,risc0_binfmt=warn"),
        );
        filter = filter.add_directive("risc0_zkvm=warn".parse()?);
    }

    // Can't use match inline because these are different return types
    let mode = match log_format {
        "json" => TracingMode::Json,
        "node" => TracingMode::NodeName,
        _ => TracingMode::Full,
    };
    let tracing = tracing_subscriber::registry();

    let log_layer = (match mode {
        TracingMode::Full => tracing_subscriber::fmt::layer().boxed(),
        TracingMode::Json => tracing_subscriber::fmt::layer().json().boxed(),
        TracingMode::NodeName => tracing_subscriber::fmt::layer()
            .event_format(NodeNameFormatter {
                node_name: node_name.clone(),
                base_formatter: tracing_subscriber::fmt::format(),
            })
            .boxed(),
    } as Box<dyn Layer<Registry> + Send + Sync>)
        .with_filter(filter);

    #[cfg(feature = "instrumentation")]
    if tracing_enabled {
        use opentelemetry_sdk::propagation::TraceContextPropagator;

        let endpoint =
            std::env::var("OTLP_ENDPOINT").unwrap_or_else(|_| "http://localhost:4317".to_string());

        opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());

        tracing
            .with(log_layer)
            .with(otlp_layer(endpoint, node_name).expect("Failed to create OTLP layer"))
            .init();
        return Ok(());
    }

    tracing.with(log_layer).init();

    Ok(())
}

#[cfg(feature = "turmoil")]
fn wrap_metric_exporter<E: PushMetricExporter>(inner: E) -> SimulatedTimeExporter<E> {
    SimulatedTimeExporter::new(inner)
}

#[cfg(not(feature = "turmoil"))]
fn wrap_metric_exporter<E: PushMetricExporter>(inner: E) -> E {
    inner
}

pub fn build_meter_provider(
    service_name: &str,
    otlp_metrics_endpoint: &str,
    otlp_metrics_export_interval_ms: u64,
) -> anyhow::Result<(opentelemetry_sdk::metrics::SdkMeterProvider, PrometheusRegistry)> {
    let registry = PrometheusRegistry::new();
    let resource = Resource::builder()
        .with_service_name(service_name.to_string())
        .with_attribute(opentelemetry::KeyValue::new(
            "service.instance.id",
            service_name.to_string(),
        ))
        .build();

    let mut meter_provider_builder = opentelemetry_sdk::metrics::SdkMeterProvider::builder()
        .with_resource(resource)
        .with_reader(
            opentelemetry_prometheus::exporter()
                .with_registry(registry.clone())
                .build()
                .context("starting prometheus exporter")?,
        );

    if !otlp_metrics_endpoint.is_empty() {
        let exporter = MetricExporter::builder()
            .with_http()
            .with_protocol(Protocol::HttpBinary)
            .with_endpoint(otlp_metrics_endpoint.to_string())
            .with_timeout(Duration::from_secs(5))
            .build()
            .context("starting otlp metrics exporter")?;

        let interval_ms = std::cmp::max(otlp_metrics_export_interval_ms, 1);
        let reader =
            opentelemetry_sdk::metrics::PeriodicReader::builder(wrap_metric_exporter(exporter))
                .with_interval(Duration::from_millis(interval_ms))
                .build();

        meter_provider_builder = meter_provider_builder.with_reader(reader);
        info!(
            "OTLP metrics exporter enabled toward {} ({}ms interval)",
            otlp_metrics_endpoint, interval_ms
        );
    } else {
        info!("OTLP metrics exporter disabled (no endpoint configured)");
    }

    Ok((meter_provider_builder.build(), registry))
}

pub fn spawn_metric_tasks(
    provider: opentelemetry_sdk::metrics::SdkMeterProvider,
    otlp_metrics_endpoint: &str,
) {
    if !otlp_metrics_endpoint.is_empty() {
        tokio::spawn({
            let provider = provider.clone();
            async move {
                let mut ticker = tokio::time::interval(Duration::from_secs(1));
                loop {
                    #[cfg(feature = "turmoil")]
                    refresh_sim_elapsed();
                    let _ = ticker.tick().await;
                    let _ = provider.force_flush();
                }
            }
        });
    }
}

#[cfg(feature = "instrumentation")]
/// Create an OTLP layer exporting tracing data.
fn otlp_layer<S>(
    endpoint: String,
    service_name: String,
) -> Result<impl tracing_subscriber::Layer<S>>
where
    S: tracing::Subscriber + for<'span> tracing_subscriber::registry::LookupSpan<'span>,
{
    use opentelemetry_otlp::{SpanExporter, WithExportConfig};
    use opentelemetry_sdk::{trace::SdkTracerProvider, Resource};

    let exporter = SpanExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint)
        .build()?;

    let resource = Resource::builder()
        .with_service_name(service_name.clone())
        .build();

    let provider = SdkTracerProvider::builder()
        .with_resource(resource)
        .with_batch_exporter(exporter)
        .build();

    let tracer = provider.tracer(service_name);

    Ok(tracing_opentelemetry::layer()
        .with_tracer(tracer)
        .with_filter(LevelFilter::INFO))
}

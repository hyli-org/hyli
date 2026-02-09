use anyhow::{Context, Result};
#[cfg(feature = "instrumentation")]
use opentelemetry::trace::TracerProvider;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::Registry;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer, layer::SubscriberExt, registry::LookupSpan};

#[cfg(feature = "instrumentation")]
mod otlp_metrics {
    use hyli_turmoil_shims::init_global_meter_provider;
    use opentelemetry_otlp::WithExportConfig;
    use opentelemetry_sdk::metrics::periodic_reader_with_async_runtime::PeriodicReader;
    use opentelemetry_sdk::metrics::{MetricResult, SdkMeterProvider};
    use opentelemetry_sdk::{Resource, runtime};

    pub fn init(
        endpoint: String,
        service_name: String,
        push_interval: Option<std::time::Duration>,
    ) -> MetricResult<()> {
        let exporter = opentelemetry_otlp::MetricExporter::builder()
            .with_tonic()
            .with_endpoint(endpoint)
            .build()?;

        // Use the Tokio runtime so periodic exports follow the simulation clock under turmoil.
        let mut reader_builder = PeriodicReader::builder(exporter, runtime::Tokio);
        if let Some(interval) = push_interval {
            reader_builder = reader_builder.with_interval(interval);
        }
        let reader = reader_builder.build();
        let provider = SdkMeterProvider::builder()
            .with_reader(reader)
            .with_resource(Resource::builder().with_service_name(service_name).build())
            .build();

        init_global_meter_provider(provider);

        Ok(())
    }
}

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
/// SAFETY: This function should be called early in main, before any threads are spawned, to ensure that the environment variable is set for all threads and subprocesses.
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
        // SAFETY: called before any threads are spawned (early in main).
        // Needed because risc0 spawns subprocesses that inherit RUST_LOG.
        unsafe {
            std::env::set_var(
                "RUST_LOG",
                format!("{var},risc0_zkvm=warn,risc0_circuit_rv32im=warn,risc0_binfmt=warn"),
            );
            filter = filter.add_directive("risc0_zkvm=warn".parse()?);
        }
    }

    let endpoint =
        std::env::var("OTLP_ENDPOINT").unwrap_or_else(|_| "http://localhost:4317".to_string());
    let push_interval = std::env::var("OTLP_METRICS_PUSH_INTERVAL_SECS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .map(std::time::Duration::from_secs);
    // Initialize the OTLP meter provider globally so that metrics can be emitted.
    #[cfg(feature = "instrumentation")]
    {
        otlp_metrics::init(endpoint.clone(), node_name.clone(), push_interval)
            .context("starting OTLP metrics exporter")?;
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
    use opentelemetry_sdk::{Resource, trace::SdkTracerProvider};

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

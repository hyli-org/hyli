use anyhow::Result;
use tracing::{level_filters::LevelFilter, Subscriber};
use tracing_subscriber::{
    fmt::{format, FormatEvent, FormatFields},
    prelude::*,
    registry::LookupSpan,
    EnvFilter,
};

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

impl<S, N, T> FormatEvent<S, N> for NodeNameFormatter<T>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
    T: FormatEvent<S, N>,
{
    fn format_event(
        &self,
        ctx: &tracing_subscriber::fmt::FmtContext<'_, S, N>,
        mut writer: format::Writer<'_>,
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

/// Setup tracing - stdout subscriber
/// stdout defaults to INFO to INFO even if RUST_LOG is set to e.g. debug
pub fn setup_tracing(log_format: &str, node_name: String) -> Result<()> {
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
    match mode {
        TracingMode::Full => register_global_subscriber(filter, tracing_subscriber::fmt::layer()),
        TracingMode::Json => register_global_subscriber(
            filter,
            tracing_subscriber::fmt::layer().event_format(tracing_subscriber::fmt::format().json()),
        ),
        TracingMode::NodeName => register_global_subscriber(
            filter,
            tracing_subscriber::fmt::layer().event_format(NodeNameFormatter {
                node_name,
                base_formatter: tracing_subscriber::fmt::format(),
            }),
        ),
    };

    Ok(())
}

fn register_global_subscriber<T, S>(filter: EnvFilter, fmt_layer: T)
where
    S: Subscriber,
    T: tracing_subscriber::Layer<S> + Send + Sync,
    tracing_subscriber::filter::Filtered<T, tracing_subscriber::EnvFilter, S>:
        tracing_subscriber::Layer<tracing_subscriber::Registry>,
{
    tracing_subscriber::registry()
        .with(fmt_layer.with_filter(filter))
        .init();
}

use crate::utils::conf;
use anyhow::Context;
use opentelemetry_otlp::{MetricExporter, Protocol, WithExportConfig};
use opentelemetry_sdk::metrics::exporter::PushMetricExporter;
use opentelemetry_sdk::resource::Resource;
use opentelemetry_sdk::{self};
use prometheus::Registry;
use std::time::Duration;
use tracing::info;

#[cfg(feature = "turmoil")]
use crate::utils::turmoil_time::SimulatedTimeExporter;
#[cfg(feature = "turmoil")]
use hyli_net::clock::TimestampMsClock;

#[cfg(feature = "turmoil")]
fn wrap_metric_exporter<E: PushMetricExporter>(inner: E) -> SimulatedTimeExporter<E> {
    SimulatedTimeExporter::new(inner)
}

#[cfg(not(feature = "turmoil"))]
fn wrap_metric_exporter<E: PushMetricExporter>(inner: E) -> E {
    inner
}

pub fn build_meter_provider(
    config: &conf::Conf,
) -> anyhow::Result<(opentelemetry_sdk::metrics::SdkMeterProvider, Registry)> {
    let registry = Registry::new();
    let resource = Resource::builder()
        .with_service_name(config.id.clone())
        .with_attribute(opentelemetry::KeyValue::new(
            "service.instance.id",
            config.id.clone(),
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

    if !config.otlp_metrics_endpoint.is_empty() {
        let exporter = MetricExporter::builder()
            .with_http()
            .with_protocol(Protocol::HttpBinary)
            .with_endpoint(config.otlp_metrics_endpoint.clone())
            .with_timeout(Duration::from_secs(5))
            .build()
            .context("starting otlp metrics exporter")?;

        let interval_ms = std::cmp::max(config.otlp_metrics_export_interval_ms, 1);
        let reader =
            opentelemetry_sdk::metrics::PeriodicReader::builder(wrap_metric_exporter(exporter))
                .with_interval(Duration::from_millis(interval_ms))
                .build();

        meter_provider_builder = meter_provider_builder.with_reader(reader);
        info!(
            "OTLP metrics exporter enabled toward {} ({}ms interval)",
            config.otlp_metrics_endpoint, interval_ms
        );
    } else {
        info!("OTLP metrics exporter disabled (no endpoint configured)");
    }

    Ok((meter_provider_builder.build(), registry))
}

pub fn spawn_metric_tasks(
    provider: opentelemetry_sdk::metrics::SdkMeterProvider,
    config: &conf::Conf,
) {
    if !config.otlp_metrics_endpoint.is_empty() {
        tokio::spawn({
            let provider = provider.clone();
            async move {
                let mut ticker = tokio::time::interval(Duration::from_secs(1));
                loop {
                    #[cfg(feature = "turmoil")]
                    TimestampMsClock::refresh_sim_elapsed();
                    let _ = ticker.tick().await;
                    let _ = provider.force_flush();
                }
            }
        });
    }
}

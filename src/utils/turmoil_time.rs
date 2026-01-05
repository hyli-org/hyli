#![cfg(feature = "turmoil")]

use hyli_net::clock::TimestampMsClock;
use opentelemetry_sdk::metrics::data::{Gauge, Histogram, Metric, ResourceMetrics, Sum};
use opentelemetry_sdk::metrics::exporter::PushMetricExporter;
use std::time::{Duration, SystemTime};
use tracing::debug;

fn simulated_system_time() -> SystemTime {
    let sim_ms = TimestampMsClock::now().0 as u64;
    SystemTime::UNIX_EPOCH + Duration::from_millis(sim_ms)
}

fn rewrite_metric_times(metric: &mut Metric, sim_time: SystemTime) {
    macro_rules! set_gauge {
        ($t:ty) => {
            if let Some(g) = metric.data.as_mut().as_mut().downcast_mut::<Gauge<$t>>() {
                g.start_time = Some(sim_time);
                g.time = sim_time;
                return;
            }
        };
    }
    macro_rules! set_sum {
        ($t:ty) => {
            if let Some(s) = metric.data.as_mut().as_mut().downcast_mut::<Sum<$t>>() {
                s.start_time = sim_time;
                s.time = sim_time;
                return;
            }
        };
    }
    macro_rules! set_hist {
        ($t:ty) => {
            if let Some(h) = metric
                .data
                .as_mut()
                .as_mut()
                .downcast_mut::<Histogram<$t>>()
            {
                h.start_time = sim_time;
                h.time = sim_time;
                return;
            }
        };
    }

    set_gauge!(u64);
    set_gauge!(i64);
    set_gauge!(f64);
    set_sum!(u64);
    set_sum!(i64);
    set_sum!(f64);
    set_hist!(u64);
    set_hist!(i64);
    set_hist!(f64);
}

fn rewrite_resource_metrics(rm: &mut ResourceMetrics, sim_time: SystemTime) {
    for scope in rm.scope_metrics.iter_mut() {
        for metric in scope.metrics.iter_mut() {
            rewrite_metric_times(metric, sim_time);
        }
    }
}

#[derive(Debug)]
pub struct SimulatedTimeExporter<E> {
    inner: E,
}

impl<E> SimulatedTimeExporter<E> {
    pub fn new(inner: E) -> Self {
        Self { inner }
    }
}

#[async_trait::async_trait]
impl<E> PushMetricExporter for SimulatedTimeExporter<E>
where
    E: PushMetricExporter,
{
    async fn export(
        &self,
        metrics: &mut ResourceMetrics,
    ) -> opentelemetry_sdk::error::OTelSdkResult {
        let scopes = metrics.scope_metrics.len();
        let metric_count: usize = metrics.scope_metrics.iter().map(|s| s.metrics.len()).sum();
        debug!(
            target: "otlp-export",
            scopes, metric_count, "SimulatedTimeExporter: rewriting times and exporting"
        );
        rewrite_resource_metrics(metrics, simulated_system_time());
        let res = self.inner.export(metrics).await;
        debug!(
            target: "otlp-export",
            result = format!("{:?}", res),
            "SimulatedTimeExporter: export completed"
        );
        res
    }

    async fn force_flush(&self) -> opentelemetry_sdk::error::OTelSdkResult {
        self.inner.force_flush().await
    }

    fn shutdown(&self) -> opentelemetry_sdk::error::OTelSdkResult {
        self.inner.shutdown()
    }

    fn temporality(&self) -> opentelemetry_sdk::metrics::Temporality {
        self.inner.temporality()
    }
}

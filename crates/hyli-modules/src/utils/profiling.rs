use std::time::Instant;

use opentelemetry::KeyValue;

pub trait LatencyMetricSink {
    fn latency(&self, latency: u64, labels: &[KeyValue]);
}

pub struct LatencyTimer<'a, T: LatencyMetricSink> {
    start: Instant,
    metrics: &'a T,
    labels: &'a [KeyValue],
}

impl<'a, T: LatencyMetricSink> LatencyTimer<'a, T> {
    pub fn new(metrics: &'a T, labels: &'a [KeyValue]) -> Self {
        Self {
            start: Instant::now(),
            metrics,
            labels,
        }
    }
}

impl<T: LatencyMetricSink> Drop for LatencyTimer<'_, T> {
    fn drop(&mut self) {
        self.metrics
            .latency(self.start.elapsed().as_millis() as u64, self.labels);
    }
}

use opentelemetry::{
    metrics::{Counter, Gauge},
    InstrumentationScope, KeyValue,
};

#[derive(Debug, Clone)]
pub struct GcsUploaderMetrics {
    module_name: &'static str,
    current_height: Gauge<u64>,
    success_total: Counter<u64>,
    failure_total: Counter<u64>,
}

impl GcsUploaderMetrics {
    pub fn global(node_name: String, module_name: &'static str) -> GcsUploaderMetrics {
        let scope = InstrumentationScope::builder(node_name).build();
        let my_meter = opentelemetry::global::meter_with_scope(scope);

        GcsUploaderMetrics {
            module_name,
            current_height: my_meter.u64_gauge("gcs_uploader_current_height").build(),
            success_total: my_meter.u64_counter("gcs_uploader_success_total").build(),
            failure_total: my_meter.u64_counter("gcs_uploader_failure_total").build(),
        }
    }

    pub fn record_success(&self, height: u64) {
        let labels = [KeyValue::new("module_name", self.module_name)];
        self.success_total.add(1, &labels);
        self.current_height.record(height, &labels);
    }

    pub fn record_failure(&self) {
        let labels = [KeyValue::new("module_name", self.module_name)];
        self.failure_total.add(1, &labels);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_creation() {
        let metrics = GcsUploaderMetrics::global("test-node".to_string(), "gcs_uploader");
        assert_eq!(metrics.module_name, "gcs_uploader");
    }

    #[test]
    fn test_metrics_methods_dont_panic() {
        let metrics = GcsUploaderMetrics::global("test-node".to_string(), "gcs_uploader");

        // These should not panic
        metrics.record_success(100);
        metrics.record_failure();
    }
}

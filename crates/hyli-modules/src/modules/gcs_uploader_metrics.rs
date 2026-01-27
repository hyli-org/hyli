use hyli_turmoil_shims::{global_meter_or_panic, Counter, Gauge, KeyValue};
use sdk::BlockHeight;

#[derive(Debug, Clone)]
pub struct GcsUploaderMetrics {
    module_name: &'static str,
    current_height: Gauge<u64>,
    success_total: Counter<u64>,
    failure_total: Counter<u64>,
}

impl GcsUploaderMetrics {
    pub fn global(module_name: &'static str) -> GcsUploaderMetrics {
        let my_meter = global_meter_or_panic();

        GcsUploaderMetrics {
            module_name,
            current_height: my_meter.u64_gauge("gcs_uploader_current_height").build(),
            success_total: my_meter.u64_counter("gcs_uploader_success_total").build(),
            failure_total: my_meter.u64_counter("gcs_uploader_failure_total").build(),
        }
    }

    pub fn record_success(&self, height: BlockHeight) {
        let labels = [KeyValue::new("module_name", self.module_name)];
        self.success_total.add(1, &labels);
        self.current_height.record(height.0, &labels);
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
        let metrics = GcsUploaderMetrics::global("gcs_uploader");
        assert_eq!(metrics.module_name, "gcs_uploader");
    }

    #[test]
    fn test_metrics_methods_dont_panic() {
        let metrics = GcsUploaderMetrics::global("gcs_uploader");

        // These should not panic
        metrics.record_success(BlockHeight(100));
        metrics.record_failure();
    }
}

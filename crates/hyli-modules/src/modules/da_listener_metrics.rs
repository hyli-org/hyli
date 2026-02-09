use crate::telemetry::{Counter, Gauge, KeyValue, global_meter_or_panic};

#[derive(Debug, Clone)]
pub struct DaTcpClientMetrics {
    module_name: &'static str,
    start: Counter<u64>,
    reconnect: Counter<u64>,
    start_height: Gauge<u64>,
}

impl DaTcpClientMetrics {
    pub fn global(module_name: &'static str) -> DaTcpClientMetrics {
        let my_meter = global_meter_or_panic();

        DaTcpClientMetrics {
            module_name,
            start: my_meter.u64_counter("da_tcp_client_start").build(),
            reconnect: my_meter.u64_counter("da_tcp_client_reconnect").build(),
            start_height: my_meter.u64_gauge("da_tcp_client_start_height").build(),
        }
    }

    pub fn start(&self, height: u64) {
        let labels = [KeyValue::new("module_name", self.module_name)];
        self.start.add(1, &labels);
        self.start_height.record(height, &labels);
    }

    pub fn reconnect(&self, reason: &'static str) {
        self.reconnect.add(
            1,
            &[
                KeyValue::new("module_name", self.module_name),
                KeyValue::new("reason", reason),
            ],
        );
    }
}

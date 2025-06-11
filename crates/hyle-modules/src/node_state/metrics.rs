use opentelemetry::{
    metrics::{Counter, Gauge},
    InstrumentationScope, KeyValue,
};

#[derive(Debug, Clone)]
pub struct NodeStateMetrics {
    module_name: &'static str,
    processed_blocks: Counter<u64>,
    unsettled_transactions: Gauge<u64>,
    scheduled_timeouts: Gauge<u64>,
    contracts: Gauge<u64>,
    settled_transactions: Counter<u64>,
    current_height: Gauge<u64>,
    triggered_timeouts: Counter<u64>,
}

impl NodeStateMetrics {
    pub fn global(node_name: String, module_name: &'static str) -> NodeStateMetrics {
        let scope = InstrumentationScope::builder(node_name).build();
        let my_meter = opentelemetry::global::meter_with_scope(scope);

        let node_state = "node_state";

        NodeStateMetrics {
            module_name,
            processed_blocks: my_meter
                .u64_counter(format!("{node_state}_processed_blocks"))
                .build(),
            unsettled_transactions: my_meter
                .u64_gauge(format!("{node_state}_unsettled_transactions"))
                .build(),
            scheduled_timeouts: my_meter
                .u64_gauge(format!("{node_state}_scheduled_timeouts"))
                .build(),
            contracts: my_meter
                .u64_gauge(format!("{node_state}_contracts"))
                .build(),
            settled_transactions: my_meter
                .u64_counter(format!("{node_state}_settled_transactions"))
                .build(),
            current_height: my_meter
                .u64_gauge(format!("{node_state}_current_height"))
                .build(),
            triggered_timeouts: my_meter
                .u64_counter(format!("{node_state}_triggered_timeouts"))
                .build(),
        }
    }

    pub fn add_processed_block(&self) {
        self.processed_blocks
            .add(1, &[KeyValue::new("module_name", self.module_name)]);
    }
    pub fn add_triggered_timeouts(&self) {
        self.triggered_timeouts
            .add(1, &[KeyValue::new("module_name", self.module_name)]);
    }
    pub fn add_settled_transactions(&self, value: u64) {
        self.settled_transactions
            .add(value, &[KeyValue::new("module_name", self.module_name)]);
    }
    pub fn record_unsettled_transactions(&self, value: u64) {
        self.unsettled_transactions
            .record(value, &[KeyValue::new("module_name", self.module_name)])
    }
    pub fn record_scheduled_timeouts(&self, value: u64) {
        self.scheduled_timeouts
            .record(value, &[KeyValue::new("module_name", self.module_name)])
    }
    pub fn record_contracts(&self, value: u64) {
        self.contracts
            .record(value, &[KeyValue::new("module_name", self.module_name)])
    }
    pub fn record_current_height(&self, value: u64) {
        self.current_height
            .record(value, &[KeyValue::new("module_name", self.module_name)])
    }
}

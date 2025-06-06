use opentelemetry::{
    metrics::{Counter, Histogram},
    InstrumentationScope, KeyValue,
};

#[derive(Debug, Clone)]
pub struct AutoProverMetrics {
    proofs_requested: Counter<u64>,
    proofs_successful: Counter<u64>,
    proofs_failed: Counter<u64>,
    proof_generation_time: Histogram<f64>,
    proof_size_bytes: Histogram<u64>,
    proof_num_retries: Histogram<u64>,
    contract_name: String,
    prover_name: String,
}

impl AutoProverMetrics {
    pub fn global(
        node_name: String,
        contract_name: String,
        prover_name: String,
    ) -> AutoProverMetrics {
        let scope = InstrumentationScope::builder(node_name).build();
        let my_meter = opentelemetry::global::meter_with_scope(scope);

        AutoProverMetrics {
            proofs_requested: my_meter
                .u64_counter("proof_client_proofs_requested")
                .build(),
            proofs_successful: my_meter
                .u64_counter("proof_client_proofs_successful")
                .build(),
            proofs_failed: my_meter.u64_counter("proof_client_proofs_failed").build(),
            proof_generation_time: my_meter
                .f64_histogram("proof_client_generation_time_seconds")
                .build(),
            proof_size_bytes: my_meter
                .u64_histogram("proof_client_proof_size_bytes")
                .build(),
            proof_num_retries: my_meter
                .u64_histogram("proof_client_proof_num_retries")
                .build(),
            contract_name,
            prover_name,
        }
    }

    fn get_labels(&self) -> Vec<KeyValue> {
        vec![
            KeyValue::new("prover", self.prover_name.clone()),
            KeyValue::new("contract_name", self.contract_name.to_string()),
        ]
    }

    pub fn record_proof_requested(&self) {
        self.proofs_requested.add(1, &self.get_labels());
    }

    pub fn record_proof_success(&self) {
        self.proofs_successful.add(1, &self.get_labels());
    }

    pub fn record_proof_failure(&self) {
        self.proofs_failed.add(1, &self.get_labels());
    }

    pub fn record_generation_time(&self, duration: f64) {
        self.proof_generation_time
            .record(duration, &self.get_labels());
    }

    pub fn record_proof_size(&self, size: u64) {
        self.proof_size_bytes.record(size, &self.get_labels());
    }

    pub fn record_proof_retry(&self, num_retries: u64) {
        self.proof_num_retries
            .record(num_retries, &self.get_labels());
    }
}

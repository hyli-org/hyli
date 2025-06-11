use opentelemetry::{
    metrics::{Counter, Gauge, Histogram},
    KeyValue,
};

#[derive(Debug, Clone)]
pub struct AutoProverMetrics {
    proofs_requested: Counter<u64>,
    proofs_successful: Counter<u64>,
    proofs_failed: Counter<u64>,
    proof_generation_time: Histogram<f64>,
    proof_size_bytes_histogram: Histogram<u64>,
    proof_size_bytes_counter: Counter<u64>,
    proof_num_retries: Counter<u64>,
    buffered_blobs: Gauge<u64>,
    unsettled_blobs: Gauge<u64>,
    contract_name: String,
    prover_name: String,
}

impl AutoProverMetrics {
    pub fn global(contract_name: String, prover_name: String) -> AutoProverMetrics {
        let my_meter = opentelemetry::global::meter("auto_prover");

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
            proof_size_bytes_histogram: my_meter
                .u64_histogram("proof_client_proof_size_bytes_histogram")
                .build(),
            proof_num_retries: my_meter
                .u64_counter("proof_client_proof_num_retries")
                .build(),
            proof_size_bytes_counter: my_meter
                .u64_counter("proof_client_proof_size_bytes_counter")
                .build(),
            buffered_blobs: my_meter.u64_gauge("proof_client_buffered_blobs").build(),
            unsettled_blobs: my_meter.u64_gauge("proof_client_unsettled_blobs").build(),
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
        self.proof_size_bytes_histogram
            .record(size, &self.get_labels());

        self.proof_size_bytes_counter.add(size, &self.get_labels());
    }

    pub fn record_proof_retry(&self) {
        self.proof_num_retries.add(1, &self.get_labels());
    }

    pub fn snapshot_buffered_blobs(&self, count: u64) {
        self.buffered_blobs.record(count, &self.get_labels());
    }

    pub fn snapshot_unsettled_blobs(&self, count: u64) {
        self.unsettled_blobs.record(count, &self.get_labels());
    }
}

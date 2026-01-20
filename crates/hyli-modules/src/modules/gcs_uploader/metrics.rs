use opentelemetry::{
    metrics::{Counter, Gauge, Histogram},
    InstrumentationScope,
};

#[derive(Debug, Clone)]
pub struct GcsUploaderMetrics {
    // Counters
    blocks_uploaded: Counter<u64>,
    blocks_failed: Counter<u64>,
    proofs_uploaded: Counter<u64>,
    proofs_failed: Counter<u64>,
    bytes_uploaded: Counter<u64>,
    retries: Counter<u64>,

    // Gauges
    queue_size: Gauge<u64>,
    retry_queue_size: Gauge<u64>,
    last_uploaded_height: Gauge<u64>,

    // Histograms
    upload_latency_seconds: Histogram<f64>,
    upload_size_bytes: Histogram<u64>,
}

impl GcsUploaderMetrics {
    pub fn global(node_name: String) -> Self {
        let scope = InstrumentationScope::builder(node_name).build();
        let meter = opentelemetry::global::meter_with_scope(scope);

        Self {
            blocks_uploaded: meter.u64_counter("gcs_blocks_uploaded").build(),
            blocks_failed: meter.u64_counter("gcs_blocks_failed").build(),
            proofs_uploaded: meter.u64_counter("gcs_proofs_uploaded").build(),
            proofs_failed: meter.u64_counter("gcs_proofs_failed").build(),
            bytes_uploaded: meter.u64_counter("gcs_bytes_uploaded").build(),
            retries: meter.u64_counter("gcs_retries").build(),
            queue_size: meter.u64_gauge("gcs_queue_size").build(),
            retry_queue_size: meter.u64_gauge("gcs_retry_queue_size").build(),
            last_uploaded_height: meter.u64_gauge("gcs_last_uploaded_height").build(),
            upload_latency_seconds: meter.f64_histogram("gcs_upload_latency_seconds").build(),
            upload_size_bytes: meter.u64_histogram("gcs_upload_size_bytes").build(),
        }
    }

    // Methods to record events
    pub fn record_block_uploaded(&self, bytes: u64, latency_secs: f64) {
        self.blocks_uploaded.add(1, &[]);
        self.bytes_uploaded.add(bytes, &[]);
        self.upload_latency_seconds.record(latency_secs, &[]);
        self.upload_size_bytes.record(bytes, &[]);
    }

    pub fn record_block_failed(&self) {
        self.blocks_failed.add(1, &[]);
    }

    pub fn record_proof_uploaded(&self, bytes: u64, latency_secs: f64) {
        self.proofs_uploaded.add(1, &[]);
        self.bytes_uploaded.add(bytes, &[]);
        self.upload_latency_seconds.record(latency_secs, &[]);
        self.upload_size_bytes.record(bytes, &[]);
    }

    pub fn record_proof_failed(&self) {
        self.proofs_failed.add(1, &[]);
    }

    pub fn record_retry(&self) {
        self.retries.add(1, &[]);
    }

    pub fn record_queue_size(&self, size: u64) {
        self.queue_size.record(size, &[]);
    }

    pub fn record_retry_queue_size(&self, size: u64) {
        self.retry_queue_size.record(size, &[]);
    }

    pub fn record_last_uploaded_height(&self, height: u64) {
        self.last_uploaded_height.record(height, &[]);
    }
}

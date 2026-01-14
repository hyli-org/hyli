use opentelemetry::{
    metrics::{Counter, Gauge, Histogram},
    InstrumentationScope, KeyValue,
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
    holes_detected: Counter<u64>,

    // Gauges
    queue_size: Gauge<u64>,
    retry_queue_size: Gauge<u64>,
    last_uploaded_height: Gauge<u64>,
    missing_blocks: Gauge<u64>,
    missing_ranges: Gauge<u64>,

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
            holes_detected: meter.u64_counter("gcs_holes_detected").build(),
            queue_size: meter.u64_gauge("gcs_queue_size").build(),
            retry_queue_size: meter.u64_gauge("gcs_retry_queue_size").build(),
            last_uploaded_height: meter.u64_gauge("gcs_last_uploaded_height").build(),
            missing_blocks: meter.u64_gauge("gcs_missing_blocks").build(),
            missing_ranges: meter.u64_gauge("gcs_missing_ranges").build(),
            upload_latency_seconds: meter.f64_histogram("gcs_upload_latency_seconds").build(),
            upload_size_bytes: meter.u64_histogram("gcs_upload_size_bytes").build(),
        }
    }

    // Methods to record events
    pub fn record_block_uploaded(&self, upload_type: &str, bytes: u64, latency_secs: f64) {
        let labels = [KeyValue::new("upload_type", upload_type.to_string())];
        self.blocks_uploaded.add(1, &labels);
        self.bytes_uploaded.add(bytes, &labels);
        self.upload_latency_seconds.record(latency_secs, &labels);
        self.upload_size_bytes.record(bytes, &labels);
    }

    pub fn record_block_failed(&self, upload_type: &str) {
        self.blocks_failed
            .add(1, &[KeyValue::new("upload_type", upload_type.to_string())]);
    }

    pub fn record_proof_uploaded(&self, bytes: u64, latency_secs: f64) {
        self.record_block_uploaded("proof", bytes, latency_secs);
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

    pub fn record_hole_detected(&self, blocks_count: u64) {
        self.holes_detected.add(1, &[]);
        self.missing_blocks.record(blocks_count, &[]);
    }

    pub fn record_missing_ranges(&self, count: u64) {
        self.missing_ranges.record(count, &[]);
    }

    pub fn record_missing_blocks(&self, count: u64) {
        self.missing_blocks.record(count, &[]);
    }
}

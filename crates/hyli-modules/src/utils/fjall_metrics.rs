use crate::telemetry::{global_meter_or_panic, Counter, Gauge, Histogram, KeyValue};
use fjall::{Database, Keyspace};
use tracing::warn;

#[derive(Debug, Clone)]
pub struct FjallMetrics {
    module_name: String,
    node_id: String,
    db_name: String,
    // DB-level metrics.
    db_outstanding_flushes: Gauge<u64>,      // Flush tasks queued.
    db_active_compactions: Gauge<u64>,       // Compactions currently running.
    db_compactions_completed: Gauge<u64>,    // Compactions completed since start.
    db_time_compacting_micros: Gauge<u64>,   // Total time spent compacting.
    db_journal_count: Gauge<u64>,            // Journal files currently on disk.
    db_journal_disk_space_bytes: Gauge<u64>, // Total journal disk usage.
    db_disk_space_bytes: Gauge<u64>,         // Total DB disk usage.
    db_write_buffer_size_bytes: Gauge<u64>,  // Memtable bytes (active + sealed).

    // Keyspace-level metrics (labelled by keyspace).
    keyspace_disk_space_bytes: Gauge<u64>, // Disk usage per keyspace.
    keyspace_approx_len: Gauge<u64>,       // Approximate item count.
    keyspace_table_file_cache_hit_rate: Gauge<f64>, // FD cache hit rate.
    // Block types:
    // - Data blocks store the actual key/value entries.
    // - Index blocks map key ranges to data-block locations.
    // - Filter blocks (Bloom-like) allow fast "definitely not present" checks.
    keyspace_block_cache_hit_rate: Gauge<f64>, // Aggregate block cache hit rate.
    keyspace_data_block_cache_hit_rate: Gauge<f64>, // Data block cache hit rate.
    keyspace_index_block_cache_hit_rate: Gauge<f64>, // Index block cache hit rate.
    keyspace_filter_block_cache_hit_rate: Gauge<f64>, // Filter block cache hit rate.
    keyspace_filter_efficiency: Gauge<f64>,    // Filter efficiency (IO avoided).
    keyspace_block_io_bytes: Gauge<u64>,       // Total block IO bytes.
    keyspace_data_block_io_bytes: Gauge<u64>,  // Data block IO bytes.
    keyspace_index_block_io_bytes: Gauge<u64>, // Index block IO bytes.
    keyspace_filter_block_io_bytes: Gauge<u64>, // Filter block IO bytes.

    // Operation timing totals (counter of elapsed microseconds).
    op_time_micros: Counter<u64>,
    // Operation latency distribution.
    op_latency_micros: Histogram<u64>,
    // Count operations whose latency exceeded the warning threshold.
    op_slow_total: Counter<u64>,
    // Count operations that exceeded configured timeout.
    op_timeout_total: Counter<u64>,
    // Count retry attempts triggered by failures/timeouts.
    op_retry_total: Counter<u64>,
    // Threshold used to emit "possible event-loop blocking" warning logs.
    slow_op_warn_threshold_micros: u64,
}

impl FjallMetrics {
    pub fn global(
        module_name: impl Into<String>,
        node_id: impl Into<String>,
        db_name: impl Into<String>,
    ) -> FjallMetrics {
        let meter = global_meter_or_panic();
        let slow_op_warn_threshold_micros = std::env::var("HYLI_FJALL_SLOW_OP_WARN_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .map(|ms| ms.saturating_mul(1_000))
            .unwrap_or(50_000);
        FjallMetrics {
            module_name: module_name.into(),
            node_id: node_id.into(),
            db_name: db_name.into(),
            db_outstanding_flushes: meter.u64_gauge("fjall_outstanding_flushes").build(),
            db_active_compactions: meter.u64_gauge("fjall_active_compactions").build(),
            db_compactions_completed: meter.u64_gauge("fjall_compactions_completed").build(),
            db_time_compacting_micros: meter.u64_gauge("fjall_time_compacting_micros").build(),
            db_journal_count: meter.u64_gauge("fjall_journal_count").build(),
            db_journal_disk_space_bytes: meter.u64_gauge("fjall_journal_disk_space_bytes").build(),
            db_disk_space_bytes: meter.u64_gauge("fjall_db_disk_space_bytes").build(),
            db_write_buffer_size_bytes: meter.u64_gauge("fjall_write_buffer_size_bytes").build(),
            keyspace_disk_space_bytes: meter.u64_gauge("fjall_keyspace_disk_space_bytes").build(),
            keyspace_approx_len: meter.u64_gauge("fjall_keyspace_approx_len").build(),
            keyspace_table_file_cache_hit_rate: meter
                .f64_gauge("fjall_keyspace_table_file_cache_hit_rate")
                .build(),
            keyspace_block_cache_hit_rate: meter
                .f64_gauge("fjall_keyspace_block_cache_hit_rate")
                .build(),
            keyspace_data_block_cache_hit_rate: meter
                .f64_gauge("fjall_keyspace_data_block_cache_hit_rate")
                .build(),
            keyspace_index_block_cache_hit_rate: meter
                .f64_gauge("fjall_keyspace_index_block_cache_hit_rate")
                .build(),
            keyspace_filter_block_cache_hit_rate: meter
                .f64_gauge("fjall_keyspace_filter_block_cache_hit_rate")
                .build(),
            keyspace_filter_efficiency: meter.f64_gauge("fjall_keyspace_filter_efficiency").build(),
            keyspace_block_io_bytes: meter.u64_gauge("fjall_keyspace_block_io_bytes").build(),
            keyspace_data_block_io_bytes: meter
                .u64_gauge("fjall_keyspace_data_block_io_bytes")
                .build(),
            keyspace_index_block_io_bytes: meter
                .u64_gauge("fjall_keyspace_index_block_io_bytes")
                .build(),
            keyspace_filter_block_io_bytes: meter
                .u64_gauge("fjall_keyspace_filter_block_io_bytes")
                .build(),
            op_time_micros: meter.u64_counter("fjall_op_time_micros").build(),
            op_latency_micros: meter.u64_histogram("fjall_op_latency_micros").build(),
            op_slow_total: meter.u64_counter("fjall_op_slow_total").build(),
            op_timeout_total: meter.u64_counter("fjall_op_timeout_total").build(),
            op_retry_total: meter.u64_counter("fjall_op_retry_total").build(),
            slow_op_warn_threshold_micros,
        }
    }

    pub fn record_db(&self, db: &Database) {
        let labels = [
            KeyValue::new("module", self.module_name.clone()),
            KeyValue::new("node_id", self.node_id.clone()),
            KeyValue::new("db", self.db_name.clone()),
        ];
        self.db_outstanding_flushes
            .record(db.outstanding_flushes() as u64, &labels);
        self.db_active_compactions
            .record(db.active_compactions() as u64, &labels);
        self.db_compactions_completed
            .record(db.compactions_completed() as u64, &labels);
        self.db_time_compacting_micros
            .record(db.time_compacting().as_micros() as u64, &labels);
        self.db_journal_count
            .record(db.journal_count() as u64, &labels);
        if let Ok(bytes) = db.journal_disk_space() {
            self.db_journal_disk_space_bytes.record(bytes, &labels);
        }
        if let Ok(bytes) = db.disk_space() {
            self.db_disk_space_bytes.record(bytes, &labels);
        }
        self.db_write_buffer_size_bytes
            .record(db.write_buffer_size(), &labels);
    }

    pub fn record_keyspace(&self, name: &'static str, keyspace: &Keyspace) {
        let labels = [
            KeyValue::new("module", self.module_name.clone()),
            KeyValue::new("node_id", self.node_id.clone()),
            KeyValue::new("db", self.db_name.clone()),
            KeyValue::new("keyspace", name),
        ];
        self.keyspace_disk_space_bytes
            .record(keyspace.disk_space(), &labels);
        self.keyspace_approx_len
            .record(keyspace.approximate_len() as u64, &labels);

        let metrics = keyspace.metrics();
        self.keyspace_table_file_cache_hit_rate
            .record(metrics.table_file_cache_hit_rate(), &labels);
        self.keyspace_block_cache_hit_rate
            .record(metrics.block_cache_hit_rate(), &labels);
        self.keyspace_data_block_cache_hit_rate
            .record(metrics.data_block_cache_hit_rate(), &labels);
        self.keyspace_index_block_cache_hit_rate
            .record(metrics.index_block_cache_hit_rate(), &labels);
        self.keyspace_filter_block_cache_hit_rate
            .record(metrics.filter_block_cache_hit_rate(), &labels);
        self.keyspace_filter_efficiency
            .record(metrics.filter_efficiency(), &labels);
        self.keyspace_block_io_bytes
            .record(metrics.block_io(), &labels);
        self.keyspace_data_block_io_bytes
            .record(metrics.data_block_io(), &labels);
        self.keyspace_index_block_io_bytes
            .record(metrics.index_block_io(), &labels);
        self.keyspace_filter_block_io_bytes
            .record(metrics.filter_block_io(), &labels);
    }

    pub fn record_op(&self, op: &'static str, keyspace: &'static str, micros: u64) {
        let labels = [
            KeyValue::new("module", self.module_name.clone()),
            KeyValue::new("node_id", self.node_id.clone()),
            KeyValue::new("db", self.db_name.clone()),
            KeyValue::new("keyspace", keyspace),
            KeyValue::new("op", op),
        ];
        self.op_time_micros.add(micros, &labels);
        self.op_latency_micros.record(micros, &labels);

        if micros >= self.slow_op_warn_threshold_micros {
            self.op_slow_total.add(1, &labels);

            let thread = std::thread::current();
            let thread_name = thread.name().unwrap_or("unnamed");
            let in_tokio_runtime = tokio::runtime::Handle::try_current().is_ok();

            warn!(
                module = %self.module_name,
                node_id = %self.node_id,
                db = %self.db_name,
                keyspace = keyspace,
                op = op,
                elapsed_micros = micros,
                slow_threshold_micros = self.slow_op_warn_threshold_micros,
                in_tokio_runtime,
                thread = thread_name,
                "Slow fjall operation observed; this may block async event loops when running on runtime threads"
            );
        }
    }

    pub fn record_timeout(&self, op: &'static str, keyspace: &'static str) {
        let labels = [
            KeyValue::new("module", self.module_name.clone()),
            KeyValue::new("node_id", self.node_id.clone()),
            KeyValue::new("db", self.db_name.clone()),
            KeyValue::new("keyspace", keyspace),
            KeyValue::new("op", op),
        ];
        self.op_timeout_total.add(1, &labels);
    }

    pub fn record_retry(&self, op: &'static str, keyspace: &'static str) {
        let labels = [
            KeyValue::new("module", self.module_name.clone()),
            KeyValue::new("node_id", self.node_id.clone()),
            KeyValue::new("db", self.db_name.clone()),
            KeyValue::new("keyspace", keyspace),
            KeyValue::new("op", op),
        ];
        self.op_retry_total.add(1, &labels);
    }
}

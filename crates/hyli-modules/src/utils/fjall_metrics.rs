use crate::telemetry::{global_meter_or_panic, Gauge, KeyValue};
use fjall::{Database, Keyspace};

#[derive(Debug, Clone)]
pub struct FjallMetrics {
    module_name: String,
    node_id: String,
    db_name: String,
    db_outstanding_flushes: Gauge<u64>,
    db_active_compactions: Gauge<u64>,
    db_compactions_completed: Gauge<u64>,
    db_time_compacting_micros: Gauge<u64>,
    db_journal_count: Gauge<u64>,
    db_journal_disk_space_bytes: Gauge<u64>,
    db_disk_space_bytes: Gauge<u64>,
    db_write_buffer_size_bytes: Gauge<u64>,
    keyspace_disk_space_bytes: Gauge<u64>,
    keyspace_approx_len: Gauge<u64>,
    keyspace_table_file_cache_hit_rate: Gauge<f64>,
    keyspace_block_cache_hit_rate: Gauge<f64>,
    keyspace_data_block_cache_hit_rate: Gauge<f64>,
    keyspace_index_block_cache_hit_rate: Gauge<f64>,
    keyspace_filter_block_cache_hit_rate: Gauge<f64>,
    keyspace_filter_efficiency: Gauge<f64>,
    keyspace_block_io_bytes: Gauge<u64>,
    keyspace_data_block_io_bytes: Gauge<u64>,
    keyspace_index_block_io_bytes: Gauge<u64>,
    keyspace_filter_block_io_bytes: Gauge<u64>,
}

impl FjallMetrics {
    pub fn global(
        module_name: impl Into<String>,
        node_id: impl Into<String>,
        db_name: impl Into<String>,
    ) -> FjallMetrics {
        let meter = global_meter_or_panic();
        FjallMetrics {
            module_name: module_name.into(),
            node_id: node_id.into(),
            db_name: db_name.into(),
            db_outstanding_flushes: meter.u64_gauge("fjall_outstanding_flushes").build(),
            db_active_compactions: meter.u64_gauge("fjall_active_compactions").build(),
            db_compactions_completed: meter
                .u64_gauge("fjall_compactions_completed")
                .build(),
            db_time_compacting_micros: meter
                .u64_gauge("fjall_time_compacting_micros")
                .build(),
            db_journal_count: meter.u64_gauge("fjall_journal_count").build(),
            db_journal_disk_space_bytes: meter
                .u64_gauge("fjall_journal_disk_space_bytes")
                .build(),
            db_disk_space_bytes: meter.u64_gauge("fjall_db_disk_space_bytes").build(),
            db_write_buffer_size_bytes: meter
                .u64_gauge("fjall_write_buffer_size_bytes")
                .build(),
            keyspace_disk_space_bytes: meter
                .u64_gauge("fjall_keyspace_disk_space_bytes")
                .build(),
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
            keyspace_filter_efficiency: meter
                .f64_gauge("fjall_keyspace_filter_efficiency")
                .build(),
            keyspace_block_io_bytes: meter
                .u64_gauge("fjall_keyspace_block_io_bytes")
                .build(),
            keyspace_data_block_io_bytes: meter
                .u64_gauge("fjall_keyspace_data_block_io_bytes")
                .build(),
            keyspace_index_block_io_bytes: meter
                .u64_gauge("fjall_keyspace_index_block_io_bytes")
                .build(),
            keyspace_filter_block_io_bytes: meter
                .u64_gauge("fjall_keyspace_filter_block_io_bytes")
                .build(),
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
}

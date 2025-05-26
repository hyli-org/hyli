use hyle_model::LaneId;
use opentelemetry::{
    metrics::{Counter, Gauge},
    InstrumentationScope, KeyValue,
};

use crate::model::ValidatorPublicKey;

use super::QueryNewCut;

#[derive(Clone)]
pub struct MempoolMetrics {
    api_tx: Counter<u64>,
    dp_vote: Counter<u64>,
    sync_request: Counter<u64>,
    sync_reply: Counter<u64>,
    mempool_sync: Counter<u64>,
    tx_waiting_dissemination: Gauge<u64>,
    new_cut: Counter<u64>,

    received_dp: Counter<u64>,
    hashed_dp: Counter<u64>,
    processed_dp: Counter<u64>,
    pub constructed_block: Counter<u64>,
    pub on_data_vote: Counter<u64>,
    // Number of individual DPs sent (counting one per validator)
    pub dp_disseminations: Counter<u64>,
    pub created_data_proposals: Counter<u64>,
}

impl MempoolMetrics {
    pub fn global(node_name: String) -> MempoolMetrics {
        let scope = InstrumentationScope::builder(node_name).build();
        let my_meter = opentelemetry::global::meter_with_scope(scope);

        let mempool = "mempool";

        MempoolMetrics {
            api_tx: my_meter.u64_counter(format!("{}_api_tx", mempool)).build(),
            dp_vote: my_meter.u64_counter(format!("{}_dp_vote", mempool)).build(),
            sync_request: my_meter
                .u64_counter(format!("{}_sync_request", mempool))
                .build(),
            sync_reply: my_meter
                .u64_counter(format!("{}_sync_reply", mempool))
                .build(),
            mempool_sync: my_meter.u64_counter(format!("{}_sync", mempool)).build(),
            tx_waiting_dissemination: my_meter
                .u64_gauge(format!("{}_tx_waiting_dissemination", mempool))
                .build(),
            new_cut: my_meter.u64_counter(format!("{}_new_cut", mempool)).build(),

            received_dp: my_meter
                .u64_counter(format!("{}_received_dp", mempool))
                .build(),
            hashed_dp: my_meter
                .u64_counter(format!("{}_hashed_dp", mempool))
                .build(),
            processed_dp: my_meter
                .u64_counter(format!("{}_processed_dp", mempool))
                .build(),
            constructed_block: my_meter
                .u64_counter(format!("{}_constructed_block", mempool))
                .build(),
            on_data_vote: my_meter
                .u64_counter(format!("{}_on_data_vote", mempool))
                .build(),
            dp_disseminations: my_meter
                .u64_counter(format!("{}_dp_disseminations", mempool))
                .build(),
            created_data_proposals: my_meter
                .u64_counter(format!("{}_created_data_proposals", mempool))
                .build(),
        }
    }

    pub fn query_new_cut(&self, nc: &QueryNewCut) {
        self.new_cut.add(
            1,
            &[KeyValue::new(
                "nb_validators",
                nc.0.bonded().len().to_string(),
            )],
        )
    }

    pub fn snapshot_pending_tx(&self, nb: usize) {
        self.tx_waiting_dissemination
            .record(nb as u64, &[KeyValue::new("status", "pending")])
    }

    pub fn add_api_tx(&self, kind: &'static str) {
        self.api_tx.add(
            1,
            &[
                KeyValue::new("status", "included"),
                KeyValue::new("tx_kind", kind),
            ],
        );
    }
    pub fn drop_api_tx(&self, kind: &'static str) {
        self.api_tx.add(
            1,
            &[
                KeyValue::new("status", "dropped"),
                KeyValue::new("tx_kind", kind),
            ],
        );
    }

    pub fn add_dp_vote(&self, sender: &ValidatorPublicKey, dest: &ValidatorPublicKey) {
        self.dp_vote.add(
            1,
            &[
                KeyValue::new("sender", format!("{}", sender)),
                KeyValue::new("dest", format!("{}", dest)),
            ],
        )
    }

    pub fn add_received_dp(&self, lane_id: &LaneId) {
        self.received_dp
            .add(1, &[KeyValue::new("lane_id", format!("{}", lane_id))])
    }

    pub fn add_hashed_dp(&self, lane_id: &LaneId) {
        self.hashed_dp
            .add(1, &[KeyValue::new("lane_id", format!("{}", lane_id))])
    }

    pub fn add_processed_dp(&self, lane_id: &LaneId) {
        self.processed_dp
            .add(1, &[KeyValue::new("lane_id", format!("{}", lane_id))])
    }

    /// *emitted* a sync request
    pub fn sync_request_send(&self, lane: &LaneId, requester: &ValidatorPublicKey) {
        self.sync_request.add(
            1,
            &[
                KeyValue::new("lane", format!("{}", lane)),
                KeyValue::new("requester", format!("{}", requester)),
            ],
        );
    }
    /// *received* a sync reply
    pub fn sync_reply_receive(&self, lane: &LaneId, requester: &ValidatorPublicKey) {
        self.sync_reply.add(
            1,
            &[
                KeyValue::new("lane", format!("{}", lane)),
                KeyValue::new("requester", format!("{}", requester)),
            ],
        )
    }

    /// MempoolSync: Prepare a sync reply to *send*
    pub fn mempool_sync_processed(&self, lane: &LaneId, requester: &ValidatorPublicKey) {
        self.mempool_sync.add(
            1,
            &[
                KeyValue::new("lane", format!("{}", lane)),
                KeyValue::new("requester", format!("{}", requester)),
                KeyValue::new("status", "processed"),
            ],
        );
    }

    /// MempoolSync: Prepare a sync reply to *send*
    pub fn mempool_sync_failure(&self, lane: &LaneId, requester: &ValidatorPublicKey) {
        self.mempool_sync.add(
            1,
            &[
                KeyValue::new("lane", format!("{}", lane)),
                KeyValue::new("requester", format!("{}", requester)),
                KeyValue::new("status", "failure"),
            ],
        );
    }

    /// MempoolSync: Throttle a sync reply instead of preparing it
    pub fn mempool_sync_throttled(&self, lane: &LaneId, requester: &ValidatorPublicKey) {
        self.mempool_sync.add(
            1,
            &[
                KeyValue::new("lane", format!("{}", lane)),
                KeyValue::new("requester", format!("{}", requester)),
                KeyValue::new("status", "throttled"),
            ],
        );
    }
}

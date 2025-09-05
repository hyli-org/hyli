use opentelemetry::{
    metrics::{Counter, Gauge},
    InstrumentationScope,
};

pub struct ConsensusMetrics {
    current_slot: Gauge<u64>,
    current_view: Gauge<u64>,

    last_started_round: Gauge<u64>,

    commit: Counter<u64>,

    pub on_prepare_ok: Counter<u64>,
    pub on_prepare_err: Counter<u64>,
    pub on_prepare_vote_ok: Counter<u64>,
    pub on_prepare_vote_err: Counter<u64>,
    pub on_confirm_ok: Counter<u64>,
    pub on_confirm_err: Counter<u64>,
    pub on_confirm_ack_ok: Counter<u64>,
    pub on_confirm_ack_err: Counter<u64>,
    pub on_timeout_ok: Counter<u64>,
    pub on_timeout_err: Counter<u64>,
    pub on_timeout_certificate_ok: Counter<u64>,
    pub on_timeout_certificate_err: Counter<u64>,
    pub on_sync_request_ok: Counter<u64>,
    pub on_sync_request_err: Counter<u64>,
    pub on_sync_reply_ok: Counter<u64>,
    pub on_sync_reply_err: Counter<u64>,
}

macro_rules! build {
    ($meter:ident, $type:ty, $name:expr) => {
        paste::paste! { $meter.[<u64_ $type>](stringify!([<"consensus_" $name>])).build() }
    };
}

impl ConsensusMetrics {
    pub fn global(id: String) -> ConsensusMetrics {
        let scope = InstrumentationScope::builder(id).build();
        let my_meter = opentelemetry::global::meter_with_scope(scope);

        ConsensusMetrics {
            current_slot: build!(my_meter, gauge, "current_slot"),
            current_view: build!(my_meter, gauge, "current_view"),
            last_started_round: build!(my_meter, gauge, "last_started_round"),
            commit: build!(my_meter, counter, "commit"),
            on_prepare_ok: build!(my_meter, counter, "on_prepare_ok"),
            on_prepare_err: build!(my_meter, counter, "on_prepare_err"),
            on_prepare_vote_ok: build!(my_meter, counter, "on_prepare_vote_ok"),
            on_prepare_vote_err: build!(my_meter, counter, "on_prepare_vote_err"),
            on_confirm_ok: build!(my_meter, counter, "on_confirm_ok"),
            on_confirm_err: build!(my_meter, counter, "on_confirm_err"),
            on_confirm_ack_ok: build!(my_meter, counter, "on_confirm_ack_ok"),
            on_confirm_ack_err: build!(my_meter, counter, "on_confirm_ack_err"),
            on_timeout_ok: build!(my_meter, counter, "on_timeout_ok"),
            on_timeout_err: build!(my_meter, counter, "on_timeout_err"),
            on_timeout_certificate_ok: build!(my_meter, counter, "on_timeout_certificate_ok"),
            on_timeout_certificate_err: build!(my_meter, counter, "on_timeout_certificate_err"),
            on_sync_request_ok: build!(my_meter, counter, "on_sync_request_ok"),
            on_sync_request_err: build!(my_meter, counter, "on_sync_request_err"),
            on_sync_reply_ok: build!(my_meter, counter, "on_sync_reply_ok"),
            on_sync_reply_err: build!(my_meter, counter, "on_sync_reply_err"),
        }
    }

    // Stuff I'm keeping
    pub fn commit(&self) {
        self.commit.add(1, &[]);
    }
    pub fn at_round(&self, slot: u64, view: u64) {
        self.current_slot.record(slot, &[]);
        self.current_view.record(view, &[]);
    }

    pub fn start_new_round(&self, slot: u64) {
        self.last_started_round.record(slot, &[]);
    }
}

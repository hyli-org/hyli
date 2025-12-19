use opentelemetry::{
    metrics::{Counter, Gauge, Histogram},
    InstrumentationScope, KeyValue,
};

use crate::tcp::Canal;

macro_rules! build {
    ($meter:ident, $type:ty, $name:expr) => {
        paste::paste! { $meter.[<u64_ $type>](stringify!([<"p2p_server_" $name>])).build() }
    };
}

pub(crate) struct P2PMetrics {
    ping: Counter<u64>,
    peers: Gauge<u64>,
    message: Counter<u64>,
    handshake: Counter<u64>,
    handshake_throttle: Counter<u64>,
    poison: Counter<u64>,
    tcp_event: Counter<u64>,
    rehandshake_error: Counter<u64>,
    handshake_latency: Histogram<f64>,
}

impl P2PMetrics {
    pub fn global(node_name: String) -> P2PMetrics {
        let scope = InstrumentationScope::builder(node_name).build();
        let my_meter = opentelemetry::global::meter_with_scope(scope);

        P2PMetrics {
            ping: build!(my_meter, counter, "ping"),
            peers: build!(my_meter, gauge, "peers"),
            message: build!(my_meter, counter, "message"),
            handshake: build!(my_meter, counter, "handshake"),
            handshake_throttle: build!(my_meter, counter, "handshake_throttle"),
            poison: build!(my_meter, counter, "poison"),
            tcp_event: build!(my_meter, counter, "tcp_event"),
            rehandshake_error: build!(my_meter, counter, "rehandshake_error"),
            handshake_latency: my_meter
                .f64_histogram("p2p_server_handshake_latency_seconds")
                .build(),
        }
    }

    pub fn peers_snapshot(&mut self, nb: u64) {
        self.peers.record(nb, &[]);
    }

    pub fn message_received(&self, from: String, canal: Canal) {
        self.message.add(
            1,
            &[
                KeyValue::new("from", from),
                KeyValue::new("direction", "rx"),
                KeyValue::new("result", "ok"),
                KeyValue::new("canal", canal.to_string()),
            ],
        );
    }

    pub fn message_error(&self, from: String, canal: Canal) {
        self.message.add(
            1,
            &[
                KeyValue::new("from", from),
                KeyValue::new("direction", "rx"),
                KeyValue::new("result", "error"),
                KeyValue::new("canal", canal.to_string()),
            ],
        );
    }

    pub fn message_closed(&self, from: String, canal: Canal) {
        self.message.add(
            1,
            &[
                KeyValue::new("from", from),
                KeyValue::new("direction", "rx"),
                KeyValue::new("result", "closed"),
                KeyValue::new("canal", canal.to_string()),
            ],
        );
    }

    pub fn ping(&self, peer: String, canal: Canal) {
        self.ping.add(
            1,
            &[
                KeyValue::new("peer", peer),
                KeyValue::new("canal", canal.to_string()),
            ],
        );
    }

    pub fn message_emitted(&self, to: String, canal: Canal) {
        self.message.add(
            1,
            &[
                KeyValue::new("to", to),
                KeyValue::new("direction", "tx"),
                KeyValue::new("result", "ok"),
                KeyValue::new("canal", canal.to_string()),
            ],
        );
    }

    pub fn handshake_connection_emitted(&self, to: String, canal: Canal) {
        self.handshake.add(
            1,
            &[
                KeyValue::new("to", to),
                KeyValue::new("phase", "connection"),
                KeyValue::new("direction", "tx"),
                KeyValue::new("canal", canal.to_string()),
            ],
        )
    }

    pub fn handshake_hello_emitted(&self, to: String, canal: Canal) {
        self.handshake.add(
            1,
            &[
                KeyValue::new("to", to),
                KeyValue::new("phase", "hello"),
                KeyValue::new("direction", "tx"),
                KeyValue::new("canal", canal.to_string()),
            ],
        )
    }

    pub fn handshake_hello_received(&self, from: String, canal: Canal) {
        self.handshake.add(
            1,
            &[
                KeyValue::new("from", from),
                KeyValue::new("phase", "hello"),
                KeyValue::new("direction", "rx"),
                KeyValue::new("canal", canal.to_string()),
            ],
        )
    }

    pub fn handshake_verack_emitted(&self, to: String, canal: Canal) {
        self.handshake.add(
            1,
            &[
                KeyValue::new("to", to),
                KeyValue::new("phase", "verack"),
                KeyValue::new("direction", "tx"),
                KeyValue::new("canal", canal.to_string()),
            ],
        )
    }

    pub fn handshake_verack_received(&self, from: String, canal: Canal) {
        self.handshake.add(
            1,
            &[
                KeyValue::new("from", from),
                KeyValue::new("phase", "verack"),
                KeyValue::new("direction", "rx"),
                KeyValue::new("canal", canal.to_string()),
            ],
        )
    }

    pub fn handshake_latency(&self, canal: Canal, seconds: f64) {
        self.handshake_latency
            .record(seconds, &[KeyValue::new("canal", canal.to_string())]);
    }

    pub fn poison_marked(&self, peer: String, canal: Canal) {
        self.poison.add(
            1,
            &[
                KeyValue::new("peer", peer),
                KeyValue::new("canal", canal.to_string()),
                KeyValue::new("action", "marked"),
            ],
        );
    }

    pub fn poison_send_skipped(&self, peer: String, canal: Canal) {
        self.poison.add(
            1,
            &[
                KeyValue::new("peer", peer),
                KeyValue::new("canal", canal.to_string()),
                KeyValue::new("action", "send_skipped"),
            ],
        );
    }

    pub fn poison_retry(&self, peer: String, canal: Canal) {
        self.poison.add(
            1,
            &[
                KeyValue::new("peer", peer),
                KeyValue::new("canal", canal.to_string()),
                KeyValue::new("action", "retry"),
            ],
        );
    }

    pub fn handshake_throttle_tcp_client(&self, peer: String, canal: Canal) {
        self.handshake_throttle.add(
            1,
            &[
                KeyValue::new("peer", peer),
                KeyValue::new("canal", canal.to_string()),
                KeyValue::new("phase", "tcp_client"),
            ],
        );
    }

    pub fn handshake_throttle_handshake(&self, peer: String, canal: Canal) {
        self.handshake_throttle.add(
            1,
            &[
                KeyValue::new("peer", peer),
                KeyValue::new("canal", canal.to_string()),
                KeyValue::new("phase", "handshake"),
            ],
        );
    }

    pub fn tcp_error_event(&self, canal: Option<Canal>) {
        self.tcp_event.add(
            1,
            &[
                KeyValue::new(
                    "canal",
                    canal
                        .map(|c| c.to_string())
                        .unwrap_or_else(|| "unknown".to_string()),
                ),
                KeyValue::new("type", "error"),
            ],
        );
    }

    pub fn tcp_closed_event(&self, canal: Option<Canal>) {
        self.tcp_event.add(
            1,
            &[
                KeyValue::new(
                    "canal",
                    canal
                        .map(|c| c.to_string())
                        .unwrap_or_else(|| "unknown".to_string()),
                ),
                KeyValue::new("type", "closed"),
            ],
        );
    }

    pub fn rehandshake_error(&self, peer: String, canal: Canal) {
        self.rehandshake_error.add(
            1,
            &[
                KeyValue::new("peer", peer),
                KeyValue::new("canal", canal.to_string()),
            ],
        );
    }
}

#[derive(Clone)]
pub struct TcpServerMetrics {
    peers: Gauge<u64>,
    message_received: Counter<u64>,
    message_received_bytes: Counter<u64>,
    message_emitted: Counter<u64>,
    message_emitted_bytes: Counter<u64>,
    message_error: Counter<u64>,
    message_closed: Counter<u64>,
    message_send_error: Counter<u64>,
    message_send_time: Histogram<f64>,
    server_name_label: Vec<KeyValue>,
}

impl TcpServerMetrics {
    pub fn global(pool_name: String) -> TcpServerMetrics {
        let scope = InstrumentationScope::builder(pool_name.clone()).build();
        let my_meter = opentelemetry::global::meter_with_scope(scope);
        TcpServerMetrics {
            peers: my_meter.u64_gauge("tcp_server_peers").build(),
            message_received: my_meter.u64_counter("tcp_server_message_received").build(),
            message_received_bytes: my_meter
                .u64_counter("tcp_server_message_received_bytes")
                .build(),
            message_emitted: my_meter.u64_counter("tcp_server_message_emitted").build(),
            message_emitted_bytes: my_meter
                .u64_counter("tcp_server_message_emitted_bytes")
                .build(),
            message_error: my_meter.u64_counter("tcp_server_message_error").build(),
            message_closed: my_meter.u64_counter("tcp_server_message_closed").build(),
            message_send_error: my_meter
                .u64_counter("tcp_server_message_send_error")
                .build(),
            message_send_time: my_meter
                .f64_histogram("tcp_server_message_send_time_seconds")
                .build(),
            server_name_label: vec![KeyValue::new("server_name", pool_name)],
        }
    }

    pub fn peers_snapshot(&self, nb: u64) {
        self.peers.record(nb, &self.server_name_label);
    }

    pub fn message_received(&self) {
        self.message_received.add(1, &self.server_name_label);
    }

    pub fn message_emitted(&self) {
        self.message_emitted.add(1, &self.server_name_label);
    }

    pub fn message_emitted_bytes(&self, len: u64) {
        self.message_emitted_bytes.add(len, &self.server_name_label);
    }

    pub fn message_error(&self) {
        self.message_error.add(1, &self.server_name_label);
    }

    pub fn message_closed(&self) {
        self.message_closed.add(1, &self.server_name_label);
    }

    pub fn message_send_error(&self) {
        self.message_send_error.add(1, &self.server_name_label);
    }

    pub fn message_send_time(&self, duration: f64) {
        self.message_send_time
            .record(duration, &self.server_name_label);
    }

    pub(crate) fn message_received_bytes(&self, len: u64) {
        self.message_received_bytes
            .add(len, &self.server_name_label);
    }
}

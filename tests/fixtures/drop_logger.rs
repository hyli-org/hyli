use std::fmt::{self as StdFmt};

use bytes::BytesMut;
use hyli_net::tcp::P2PTcpMessage;
use tokio_util::codec::{Decoder, LengthDelimitedCodec};
use tracing::field::{Field, Visit};
use tracing::Event;
use tracing_subscriber::fmt::format::Writer;
use tracing_subscriber::fmt::{self, layer, FmtContext, FormatEvent, FormatFields};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::{EnvFilter, Registry};

use hyli::p2p::network::NetMessage;
use hyli_model::MempoolStatusEvent;
use hyli_modules::utils::da_codec::{DataAvailabilityEvent, DataAvailabilityRequest};

/// Enable a scoped subscriber that decodes and logs dropped TCP payload types.
/// Currently gated to the drop-packets seed 729 (or `HYLI_LOG_DROPPED_MESSAGES=1`)
/// to avoid noisy output on every test run.
pub fn install_drop_logger_for_drop_packets() -> Option<tracing::subscriber::DefaultGuard> {
    let should_enable = matches!(
        std::env::var("HYLI_LOG_DROPPED_MESSAGES")
            .as_deref()
            .map(|v| v.eq_ignore_ascii_case("1") || v.eq_ignore_ascii_case("true")),
        Ok(true)
    ) || matches!(std::env::var("HYLI_TURMOIL_SEED").as_deref(), Ok("729"));

    if !should_enable {
        return None;
    }

    // Make sure the low-level turmoil `Drop` traces are recorded.
    // Keep high detail only for the turmoil drop traces; everything else at info to avoid noisy output.
    let mut env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    env_filter = env_filter.add_directive("turmoil=trace".parse().unwrap());

    let subscriber = Registry::default().with(env_filter).with(
        layer()
            // Use stderr instead of `test_writer` to avoid scoped TLS panics on background threads.
            .with_writer(std::io::stderr)
            .event_format(DropDecodeFormatter::new(fmt::format())),
    );

    tracing::warn!(target: "turmoil-drop", "Drop logger enabled");
    Some(tracing::subscriber::set_default(subscriber))
}

struct DropDecodeFormatter<F> {
    inner: F,
}

impl<F> DropDecodeFormatter<F> {
    fn new(inner: F) -> Self {
        Self { inner }
    }
}

impl<S, N, F> FormatEvent<S, N> for DropDecodeFormatter<F>
where
    S: tracing::Subscriber + for<'span> LookupSpan<'span>,
    N: for<'writer> FormatFields<'writer> + 'static,
    F: FormatEvent<S, N>,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, N>,
        writer: Writer<'_>,
        event: &Event<'_>,
    ) -> std::fmt::Result {
        let mut writer = writer;
        // Only decode turmoil drop traces; fall back to the default formatter otherwise.
        if event.metadata().target() == "turmoil" {
            let mut visitor = DropEventVisitor::default();
            event.record(&mut visitor);

            if let Some(protocol) = visitor.protocol.as_deref().and_then(parse_tcp_payload) {
                let kind = decode_message_kind(&protocol).unwrap_or_else(|| "unknown".to_string());
                let src = visitor
                    .src
                    .as_deref()
                    .map(format_endpoint)
                    .unwrap_or_default();
                let dst = visitor
                    .dst
                    .as_deref()
                    .map(format_endpoint)
                    .unwrap_or_default();
                let len = protocol.len();

                if kind == "ping" {
                    return Ok(());
                }

                // Emit the decoded classification as a dedicated line ahead of the default formatting.
                writeln!(
                    writer,
                    "WARN turmoil-drop: dropped tcp message {} -> {} kind={} len={}",
                    src, dst, kind, len
                )?;
                // Skip the default formatter to avoid printing raw turmoil trace events.
                return Ok(());
            }
            // If it was a turmoil event but we couldn't decode, still skip the default formatter to keep traces quiet.
            return Ok(());
        }

        self.inner.format_event(ctx, writer, event)
    }
}

#[derive(Default)]
struct DropEventVisitor {
    src: Option<String>,
    dst: Option<String>,
    protocol: Option<String>,
}

impl Visit for DropEventVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn StdFmt::Debug) {
        let val = format!("{value:?}");
        match field.name() {
            "src" => self.src = Some(val),
            "dst" => self.dst = Some(val),
            "protocol" => self.protocol = Some(val),
            _ => {}
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        let val = value.to_string();
        match field.name() {
            "src" => self.src = Some(val),
            "dst" => self.dst = Some(val),
            "protocol" => self.protocol = Some(val),
            _ => {}
        }
    }
}

fn parse_tcp_payload(protocol: &str) -> Option<Vec<u8>> {
    const PREFIX: &str = "TCP [";

    let protocol = protocol.trim_matches('"');
    let inner = protocol.strip_prefix(PREFIX)?.strip_suffix(']')?;
    if inner.trim().is_empty() {
        return Some(Vec::new());
    }

    let mut bytes = Vec::new();
    for part in inner.split(',') {
        let part = part.trim();
        let hex = part
            .strip_prefix("0x")
            .or_else(|| part.strip_prefix("0X"))?;
        let byte = u8::from_str_radix(hex, 16).ok()?;
        bytes.push(byte);
    }

    Some(bytes)
}

fn decode_message_kind(raw: &[u8]) -> Option<String> {
    // Try framing-aware decode (length-delimited borsh).
    let mut codec = LengthDelimitedCodec::new();
    codec.set_max_frame_length(usize::MAX);

    let mut buf = BytesMut::from(raw);
    if let Some(frame) = codec.decode(&mut buf).ok().flatten() {
        if frame.as_ref() == b"PING" {
            return Some("ping".to_string());
        }

        if let Some(kind) = decode_p2p_frame(&frame).or_else(|| decode_da_frame(&frame)) {
            return Some(kind);
        }
    }

    // Sometimes the raw payload is already a complete frame (no length prefix).
    if let Some(kind) = decode_p2p_frame(raw).or_else(|| decode_da_frame(raw)) {
        return Some(kind);
    }

    // Detect simple framed "PING".
    if raw.len() == 8 && raw.starts_with(&[0, 0, 0, 4]) && &raw[4..] == b"PING" {
        return Some("ping".to_string());
    }

    // Try to interpret as HTTP text.
    if raw
        .iter()
        .all(|b| b.is_ascii_graphic() || b.is_ascii_whitespace())
    {
        let text = String::from_utf8_lossy(raw);
        if text.starts_with("HTTP/") {
            return Some("http-response".to_string());
        }
        if text.starts_with("GET ") || text.starts_with("POST ") || text.starts_with("PUT ") {
            return Some("http-request".to_string());
        }
        return Some(format!("text:'{}'", text.trim()));
    }

    None
}

fn decode_p2p_frame(frame: &[u8]) -> Option<String> {
    let message: P2PTcpMessage<NetMessage> = borsh::from_slice(frame).ok()?;
    match message {
        P2PTcpMessage::Handshake(_) => Some("handshake".to_string()),
        P2PTcpMessage::Data(net_msg) => Some(match net_msg {
            NetMessage::MempoolMessage(msg) => {
                let variant: &'static str = msg.msg.into();
                format!("mempool::{variant}")
            }
            NetMessage::ConsensusMessage(msg) => {
                let variant: &'static str = msg.msg.into();
                format!("consensus::{variant}")
            }
        }),
    }
}

fn decode_da_frame(frame: &[u8]) -> Option<String> {
    if let Ok(req) = borsh::from_slice::<DataAvailabilityRequest>(frame) {
        // BlockHeight is a tuple struct.
        let height = (req.0).0;
        return Some(format!("da::request(height={height})"));
    }

    if let Ok(evt) = borsh::from_slice::<DataAvailabilityEvent>(frame) {
        let kind = match evt {
            DataAvailabilityEvent::SignedBlock(block) => {
                let height = block.height().0;
                let txs = block.count_txs();
                format!("da::SignedBlock(height={height}, txs={txs})")
            }
            DataAvailabilityEvent::MempoolStatusEvent(status) => match status {
                MempoolStatusEvent::WaitingDissemination {
                    parent_data_proposal_hash,
                    ..
                } => format!(
                    "da::MempoolStatusEvent::WaitingDissemination(parent={})",
                    parent_data_proposal_hash
                ),
                MempoolStatusEvent::DataProposalCreated {
                    data_proposal_hash,
                    txs_metadatas,
                    ..
                } => format!(
                    "da::MempoolStatusEvent::DataProposalCreated(hash={}, txs={})",
                    data_proposal_hash,
                    txs_metadatas.len()
                ),
            },
        };
        return Some(kind);
    }

    None
}

fn resolve_label(ip: &str) -> Option<&'static str> {
    match ip {
        "192.168.0.1" => Some("node-1"),
        "192.168.0.2" => Some("node-2"),
        "192.168.0.3" => Some("node-3"),
        "192.168.0.4" => Some("node-4"),
        "192.168.0.5" => Some("client node-1"),
        "192.168.0.6" => Some("client node-2"),
        "192.168.0.7" => Some("client node-3"),
        "192.168.0.8" => Some("client node-4"),
        _ => None,
    }
}

fn format_endpoint(addr: &str) -> String {
    let addr = addr.trim_matches('"');
    if let Some((ip, _port)) = addr.rsplit_once(':') {
        if let Some(label) = resolve_label(ip) {
            return format!("{label}({addr})");
        }
    }
    addr.to_string()
}

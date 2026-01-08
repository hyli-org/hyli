#[cfg(feature = "turmoil")]
pub mod intercept;
pub mod p2p_server;
pub mod tcp_client;
pub mod tcp_server;

use std::{
    fmt::Display,
    sync::{Arc, RwLock},
};

use borsh::{BorshDeserialize, BorshSerialize};
use bytes::Bytes;
use sdk::hyli_model_utils::TimestampMs;
use tokio::task::JoinHandle;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

use anyhow::Result;

use crate::net::TcpStream;

pub type TcpHeaders = Vec<(String, String)>;

pub(crate) type FramedStream = Framed<TcpStream, LengthDelimitedCodec>;

pub(crate) fn framed_stream(stream: TcpStream, max_frame_length: Option<usize>) -> FramedStream {
    let mut codec = LengthDelimitedCodec::new();
    if let Some(len) = max_frame_length {
        codec.set_max_frame_length(len);
    }

    Framed::new(stream, codec)
}

pub fn headers_from_span() -> TcpHeaders {
    #[cfg(feature = "instrumentation")]
    {
        let mut headers: TcpHeaders = Vec::new();
        opentelemetry::global::get_text_map_propagator(|propagator| {
            use tracing_opentelemetry::OpenTelemetrySpanExt;

            let mut carrier = std::collections::HashMap::new();
            let context = tracing::Span::current().context();
            propagator.inject_context(&context, &mut carrier);
            headers = carrier.into_iter().collect();
        });
        headers
    }
    #[cfg(not(feature = "instrumentation"))]
    {
        Vec::new()
    }
}

#[derive(Clone, BorshDeserialize, BorshSerialize, PartialEq)]
pub enum TcpMessage {
    Ping,
    Data(TcpData),
}

#[derive(Clone, BorshDeserialize, BorshSerialize, PartialEq, Default)]
pub struct TcpData {
    pub headers: TcpHeaders,
    pub payload: Arc<Vec<u8>>,
}

impl TcpData {
    pub fn new(payload: Vec<u8>) -> Self {
        Self {
            headers: Vec::new(),
            payload: Arc::new(payload),
        }
    }

    pub fn with_headers(payload: Vec<u8>, headers: TcpHeaders) -> Self {
        Self {
            headers,
            payload: Arc::new(payload),
        }
    }
}

#[derive(BorshDeserialize, BorshSerialize)]
struct TcpWireData {
    headers: TcpHeaders,
    payload: Vec<u8>,
}

impl TryFrom<TcpMessage> for Bytes {
    type Error = anyhow::Error;
    fn try_from(message: TcpMessage) -> Result<Self> {
        match message {
            // This is an untagged enum, if you send exactly "PING", it'll be treated as a ping.
            TcpMessage::Ping => Ok(Bytes::from_static(b"PING")),
            TcpMessage::Data(data) => {
                let wire = TcpWireData {
                    headers: data.headers.clone(),
                    payload: data.payload.as_ref().clone(),
                };
                Ok(Bytes::from(borsh::to_vec(&wire)?))
            }
        }
    }
}

fn to_tcp_message(data: &impl BorshSerialize) -> Result<TcpMessage> {
    let binary = borsh::to_vec(data)?;
    Ok(TcpMessage::Data(TcpData::new(binary)))
}

#[allow(dead_code)]
fn to_tcp_message_with_headers(
    data: &impl BorshSerialize,
    headers: TcpHeaders,
) -> Result<TcpMessage> {
    let binary = borsh::to_vec(data)?;
    Ok(TcpMessage::Data(TcpData::with_headers(binary, headers)))
}

pub fn decode_tcp_payload<Data: BorshDeserialize>(bytes: &[u8]) -> Result<(TcpHeaders, Data)> {
    match borsh::from_slice::<TcpWireData>(bytes) {
        Ok(wire) => Ok((wire.headers, borsh::from_slice::<Data>(&wire.payload)?)),
        Err(_) => Ok((Vec::new(), borsh::from_slice::<Data>(bytes)?)),
    }
}

#[test]
fn test_serialize_tcp_message() {
    let msg = TcpMessage::Ping;
    let bytes: Bytes = msg.try_into().unwrap();
    assert_eq!(bytes, Bytes::from_static(b"PING"));

    let data = TcpMessage::Data(TcpData::new(vec![1, 2, 3]));
    let bytes: Bytes = data.try_into().unwrap();
    assert_eq!(bytes, Bytes::from(vec![0, 0, 0, 0, 3, 0, 0, 0, 1, 2, 3]));

    let d: Vec<u8> = vec![1, 2, 3];
    let data = to_tcp_message(&d).unwrap();
    let bytes: Bytes = data.try_into().unwrap();
    assert_eq!(
        bytes,
        Bytes::from(vec![0, 0, 0, 0, 7, 0, 0, 0, 3, 0, 0, 0, 1, 2, 3])
    );

    let headers = vec![(
        "content-type".to_string(),
        "application/octet-stream".to_string(),
    )];
    let data_with_headers = to_tcp_message_with_headers(&d, headers.clone()).unwrap();
    let bytes: Bytes = data_with_headers.try_into().unwrap();
    let (decoded_headers, decoded_data): (TcpHeaders, Vec<u8>) =
        decode_tcp_payload(bytes.as_ref()).unwrap();
    assert_eq!(decoded_headers, headers);
    assert_eq!(decoded_data, d);
}

impl std::fmt::Debug for TcpMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TcpMessage::Ping => write!(f, "PING"),
            TcpMessage::Data(data) => write!(
                f,
                "DATA: {} bytes, {} headers ({:?})",
                data.payload.len(),
                data.headers.len(),
                match data.payload.len() {
                    0 => "empty".to_string(),
                    1..20 => hex::encode(data.payload.as_ref()),
                    _ => format!(
                        "{}...",
                        hex::encode(data.payload.iter().take(20).cloned().collect::<Vec<_>>())
                    ),
                },
            ),
        }
    }
}

#[derive(Debug, Clone, BorshDeserialize, BorshSerialize, PartialEq)]
pub enum P2PTcpMessage<Data: BorshDeserialize + BorshSerialize> {
    Handshake(Handshake),
    Data(Data),
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize, PartialEq)]
pub enum Handshake {
    Hello(
        (
            Canal,
            sdk::SignedByValidator<NodeConnectionData>,
            TimestampMs,
        ),
    ),
    Verack(
        (
            Canal,
            sdk::SignedByValidator<NodeConnectionData>,
            TimestampMs,
        ),
    ),
}

#[derive(
    Default, Debug, Clone, BorshSerialize, BorshDeserialize, Hash, PartialEq, Eq, PartialOrd, Ord,
)]
pub struct Canal(String);

impl Canal {
    pub fn new<T: Into<String>>(t: T) -> Canal {
        Canal(t.into())
    }
}

impl Display for Canal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize, PartialEq)]
pub struct NodeConnectionData {
    pub version: u16,
    pub name: String,
    pub current_height: u64,
    pub p2p_public_address: String,
    pub da_public_address: String,
    // TODO: add known peers
    // pub peers: Vec<String>, // List of known peers
}

#[derive(Debug, Clone)]
pub enum TcpEvent<Data: BorshDeserialize> {
    Message {
        socket_addr: String,
        data: Data,
        headers: TcpHeaders,
    },
    Error {
        socket_addr: String,
        error: String,
    },
    Closed {
        socket_addr: String,
    },
}

/// A socket abstraction to send a receive data
#[derive(Debug)]
struct SocketStream {
    /// Last timestamp we received a ping from the peer.
    last_ping: TimestampMs,
    /// Best-effort human label for logging (defaults to socket addr).
    socket_label: Arc<RwLock<String>>,
    /// Sender to stream data to the peer
    sender: tokio::sync::mpsc::Sender<TcpMessage>,
    /// Handle to abort the sending side of the stream
    abort_sender_task: JoinHandle<()>,
    /// Handle to abort the receiving side of the stream
    abort_receiver_task: JoinHandle<()>,
}

pub mod p2p_server;
pub mod tcp_client;
pub mod tcp_server;

use std::{fmt::Display, sync::Arc};

use borsh::{BorshDeserialize, BorshSerialize};
use bytes::Bytes;
use sdk::hyle_model_utils::TimestampMs;
use tokio::task::JoinHandle;

use anyhow::Result;
use std::future::Future;

use tokio::task::{self, JoinHandle};
use tracing::error;

/// Takes a task that is awaited in another task and logs a panic if it occurs.
pub fn logged_task<F>(task: F) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    task::spawn(async move {
        let result = task::spawn(task).await;

        match result {
            Ok(res) => res,
            Err(e) => {
                error!("Task failed to complete: {:?}", e);
                panic!("Task failed to complete: {:?}", e);
            }
        }
    })
}

#[derive(Clone, BorshDeserialize, BorshSerialize, PartialEq)]
pub enum TcpMessage {
    Ping,
    Data(Arc<Vec<u8>>),
}

impl TryFrom<TcpMessage> for Bytes {
    type Error = anyhow::Error;
    fn try_from(message: TcpMessage) -> Result<Self> {
        match message {
            // This is an untagged enum, if you send exactly "PING", it'll be treated as a ping.
            TcpMessage::Ping => Ok(Bytes::from_static(b"PING")),
            TcpMessage::Data(data) => Ok(Bytes::copy_from_slice(data.as_ref())),
        }
    }
}

fn to_tcp_message(data: &impl BorshSerialize) -> Result<TcpMessage> {
    let binary = borsh::to_vec(data)?;
    Ok(TcpMessage::Data(Arc::new(binary)))
}

#[test]
fn test_serialize_tcp_message() {
    let msg = TcpMessage::Ping;
    let bytes: Bytes = msg.try_into().unwrap();
    assert_eq!(bytes, Bytes::from_static(b"PING"));

    let data = TcpMessage::Data(Arc::new(vec![1, 2, 3]));
    let bytes: Bytes = data.try_into().unwrap();
    assert_eq!(bytes, Bytes::from(vec![1, 2, 3]));

    let d: Vec<u8> = vec![1, 2, 3];
    let data = to_tcp_message(&d).unwrap();
    let bytes: Bytes = data.try_into().unwrap();
    assert_eq!(bytes, Bytes::from(vec![3, 0, 0, 0, 1, 2, 3]));
}

impl std::fmt::Debug for TcpMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TcpMessage::Ping => write!(f, "PING"),
            TcpMessage::Data(data) => write!(
                f,
                "DATA: {} bytes ({:?})",
                data.len(),
                match data.len() {
                    0 => "empty".to_string(),
                    1..20 => hex::encode(data.as_ref()),
                    _ => format!(
                        "{}...",
                        hex::encode(data.iter().take(20).cloned().collect::<Vec<_>>())
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
    Message { dest: String, data: Data },
    Error { dest: String, error: String },
    Closed { dest: String },
}

/// A socket abstraction to send a receive data
#[derive(Debug)]
struct SocketStream {
    /// Last timestamp we received a ping from the peer.
    last_ping: TimestampMs,
    /// Sender to stream data to the peer
    sender: tokio::sync::mpsc::Sender<TcpMessage>,
    /// Handle to abort the sending side of the stream
    abort_sender_task: JoinHandle<()>,
    /// Handle to abort the receiving side of the stream
    abort_receiver_task: JoinHandle<()>,
}

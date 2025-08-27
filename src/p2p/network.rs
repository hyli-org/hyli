use crate::consensus::ConsensusNetMessage;
use crate::mempool::MempoolNetMessage;
use crate::model::ValidatorPublicKey;
use anyhow::Context;
use borsh::{BorshDeserialize, BorshSerialize};
use hyli_crypto::BlstCrypto;
use hyli_model::{BlockHeight, SignedByValidator};
use hyli_modules::bus::BusMessage;
use hyli_net::clock::TimestampMsClock;
use hyli_net::tcp::P2PTcpMessage;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fmt::{self, Display};
use strum_macros::IntoStaticStr;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OutboundMessage {
    SendMessage {
        validator_id: ValidatorPublicKey,
        msg: NetMessage,
    },
    BroadcastMessage(NetMessage),
    BroadcastMessageOnlyFor(HashSet<ValidatorPublicKey>, NetMessage),
}

impl OutboundMessage {
    pub fn broadcast<T: Into<NetMessage>>(msg: T) -> Self {
        OutboundMessage::BroadcastMessage(msg.into())
    }
    pub fn broadcast_only_for<T: Into<NetMessage>>(
        only_for: HashSet<ValidatorPublicKey>,
        msg: T,
    ) -> Self {
        OutboundMessage::BroadcastMessageOnlyFor(only_for, msg.into())
    }
    pub fn send<T: Into<NetMessage>>(validator_id: ValidatorPublicKey, msg: T) -> Self {
        OutboundMessage::SendMessage {
            validator_id,
            msg: msg.into(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum PeerEvent {
    NewPeer {
        name: String,
        pubkey: ValidatorPublicKey,
        da_address: String,
        height: BlockHeight,
    },
}

impl Display for NetMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let enum_variant: &'static str = self.into();
        match self {
            NetMessage::MempoolMessage(msg) => {
                _ = write!(f, "NetMessage::{enum_variant} ");
                write!(f, "{} (sent at {})", msg.msg, msg.header.msg.timestamp)
            }
            NetMessage::ConsensusMessage(msg) => {
                _ = write!(f, "NetMessage::{enum_variant} ");
                write!(f, "{} (sent at {})", msg.msg, msg.header.msg.timestamp)
            }
        }
    }
}

#[derive(
    Debug,
    Serialize,
    Deserialize,
    Clone,
    BorshSerialize,
    BorshDeserialize,
    Eq,
    PartialEq,
    IntoStaticStr,
)]
#[allow(
    clippy::large_enum_variant,
    reason = "TODO: consider if we should refactor this"
)]
pub enum NetMessage {
    MempoolMessage(MsgWithHeader<MempoolNetMessage>),
    ConsensusMessage(MsgWithHeader<ConsensusNetMessage>),
}

impl From<NetMessage> for P2PTcpMessage<NetMessage> {
    fn from(message: NetMessage) -> Self {
        P2PTcpMessage::Data(message)
    }
}

impl From<MsgWithHeader<MempoolNetMessage>> for NetMessage {
    fn from(msg: MsgWithHeader<MempoolNetMessage>) -> Self {
        NetMessage::MempoolMessage(msg)
    }
}

impl From<MsgWithHeader<ConsensusNetMessage>> for NetMessage {
    fn from(msg: MsgWithHeader<ConsensusNetMessage>) -> Self {
        NetMessage::ConsensusMessage(msg)
    }
}

impl NetMessage {
    pub fn to_binary(&self) -> anyhow::Result<Vec<u8>> {
        borsh::to_vec(self).context("Could not serialize NetMessage")
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, BorshSerialize, BorshDeserialize, Eq, PartialEq)]
pub struct HeaderSignableData(pub Vec<u8>);

// Can't be regular Into as I don't want to take ownership
pub trait IntoHeaderSignableData {
    fn to_header_signable_data(&self) -> HeaderSignableData;
}
#[derive(Debug, Serialize, Deserialize, Clone, BorshSerialize, BorshDeserialize, Eq, PartialEq)]
pub struct MsgHeader {
    pub timestamp: u128,
    pub hash: HeaderSignableData,
}

#[derive(Serialize, Deserialize, Clone, BorshSerialize, BorshDeserialize, Eq, PartialEq)]
pub struct MsgWithHeader<T: IntoHeaderSignableData> {
    pub header: SignedByValidator<MsgHeader>,
    pub msg: T,
}

impl<T: BusMessage + IntoHeaderSignableData> BusMessage for MsgWithHeader<T> {
    const CAPACITY: usize = T::CAPACITY;
}

impl<T: IntoHeaderSignableData + std::fmt::Debug> std::fmt::Debug for MsgWithHeader<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MsgWithHeader")
            .field("header", &{
                format!(
                    "MsgHeader {{ timestamp: {}, hash: {}, }}",
                    self.header.msg.timestamp,
                    match &self.header.msg.hash.0.len() {
                        0 => "empty".to_string(),
                        ..12 => hex::encode(&self.header.msg.hash.0),
                        _ => format!("{}...", {
                            let mut hash = hex::encode(&self.header.msg.hash.0);
                            hash.truncate(12);
                            hash
                        }),
                    }
                )
            })
            .field("msg", &self.msg)
            .finish()
    }
}
pub trait HeaderSigner {
    fn sign_msg_with_header<T: IntoHeaderSignableData>(
        &self,
        msg: T,
    ) -> anyhow::Result<MsgWithHeader<T>>;
}

impl HeaderSigner for BlstCrypto {
    fn sign_msg_with_header<T: IntoHeaderSignableData>(
        &self,
        msg: T,
    ) -> anyhow::Result<MsgWithHeader<T>> {
        let header = MsgHeader {
            timestamp: TimestampMsClock::now().0,
            hash: msg.to_header_signable_data(),
        };
        let signature = self.sign(header)?;
        Ok(MsgWithHeader::<T> {
            msg,
            header: signature,
        })
    }
}

impl BusMessage for OutboundMessage {}
impl BusMessage for NetMessage {}
impl BusMessage for PeerEvent {}

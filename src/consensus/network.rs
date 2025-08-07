use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};
use sha3::{Digest, Sha3_256};
use std::{fmt::Display, ops::Deref};
use strum_macros::IntoStaticStr;

use hyle_model::*;

use crate::p2p::network::{HeaderSignableData, IntoHeaderSignableData};

#[derive(
    Debug,
    Serialize,
    Deserialize,
    Clone,
    BorshSerialize,
    BorshDeserialize,
    PartialEq,
    Eq,
    IntoStaticStr,
    Hash,
    Ord,
    PartialOrd,
)]
pub enum TCKind {
    NilProposal(NilQC),
    PrepareQC((PrepareQC, ConsensusProposal)),
}

#[derive(
    Debug,
    Serialize,
    Deserialize,
    Clone,
    BorshSerialize,
    BorshDeserialize,
    PartialEq,
    Eq,
    IntoStaticStr,
    Hash,
    Ord,
    PartialOrd,
)]
/// There are two possible setups on timeout: either we should timeout with no proposal,
/// or there is a chance one had committed on some validator and we must propose the same.
/// To handle these two cases, we need specific data on top of the regular timeout message.
pub enum TimeoutKind {
    /// Sign a different message to signify 'nil proposal' for aggregation
    NilProposal(SignedByValidator<(Slot, View, ConsensusProposalHash, NilConsensusTimeoutMarker)>),
    /// Resending the prepare QC & matching proposal (for convenience for the leader to repropose)
    PrepareQC((PrepareQC, ConsensusProposal)),
}

impl TimeoutKind {
    pub fn is_aggregatable_with(&self, other: &Self) -> bool {
        match self {
            TimeoutKind::NilProposal(SignedByValidator {
                msg: (slot, view, parent_hash, _),
                ..
            }) => {
                if let TimeoutKind::NilProposal(SignedByValidator {
                    msg: (other_slot, other_view, other_parent_hash, _),
                    ..
                }) = other
                {
                    slot == other_slot && view == other_view && parent_hash == other_parent_hash
                } else {
                    false
                }
            }
            TimeoutKind::PrepareQC((_, cp)) => {
                if let TimeoutKind::PrepareQC((_, other_cp)) = other {
                    cp.hashed() == other_cp.hashed()
                } else {
                    false
                }
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
    PartialEq,
    Eq,
    Hash,
    Ord,
    PartialOrd,
)]
pub struct PrepareVoteMarker;
pub type PrepareVote = SignedByValidator<(ConsensusProposalHash, PrepareVoteMarker)>;

impl From<PrepareVote> for ConsensusNetMessage {
    fn from(vote: PrepareVote) -> Self {
        ConsensusNetMessage::PrepareVote(vote)
    }
}

#[derive(
    Debug,
    Serialize,
    Deserialize,
    Clone,
    BorshSerialize,
    BorshDeserialize,
    PartialEq,
    Eq,
    Hash,
    Ord,
    PartialOrd,
)]
pub struct ConfirmAckMarker;
pub type ConfirmAck = SignedByValidator<(ConsensusProposalHash, ConfirmAckMarker)>;

impl From<ConfirmAck> for ConsensusNetMessage {
    fn from(vote: ConfirmAck) -> Self {
        ConsensusNetMessage::ConfirmAck(vote)
    }
}

#[derive(
    Debug,
    Serialize,
    Deserialize,
    Clone,
    BorshSerialize,
    BorshDeserialize,
    PartialEq,
    Eq,
    Hash,
    Ord,
    PartialOrd,
)]
pub struct ConsensusTimeoutMarker;

#[derive(
    Debug,
    Serialize,
    Deserialize,
    Clone,
    BorshSerialize,
    BorshDeserialize,
    PartialEq,
    Eq,
    Hash,
    Ord,
    PartialOrd,
)]
pub struct NilConsensusTimeoutMarker;

/// This first message will be used in the TC to generate a proof of timeout
/// Here we add the parent hash purely to avoid replay attacks
/// The TimeoutKind is used to pick a specific TC type
pub type ConsensusTimeout = (
    SignedByValidator<(Slot, View, ConsensusProposalHash, ConsensusTimeoutMarker)>,
    TimeoutKind,
);

impl From<ConsensusTimeout> for ConsensusNetMessage {
    fn from(timeout: ConsensusTimeout) -> Self {
        ConsensusNetMessage::Timeout(timeout)
    }
}

#[derive(
    Debug,
    Serialize,
    Deserialize,
    Clone,
    BorshSerialize,
    BorshDeserialize,
    PartialEq,
    Eq,
    IntoStaticStr,
    Hash,
    Ord,
    PartialOrd,
)]
pub enum ConsensusNetMessage {
    Prepare(ConsensusProposal, Ticket, View),
    PrepareVote(PrepareVote),
    Confirm(PrepareQC, ConsensusProposalHash),
    ConfirmAck(ConfirmAck),
    Commit(CommitQC, ConsensusProposalHash),
    Timeout(ConsensusTimeout),
    TimeoutCertificate(TimeoutQC, TCKind, Slot, View),
    ValidatorCandidacy(SignedByValidator<ValidatorCandidacy>),
    SyncRequest(ConsensusProposalHash),
    SyncReply((ValidatorPublicKey, ConsensusProposal, Ticket, View)),
}

impl<T> Hashed<QuorumCertificateHash> for QuorumCertificate<T> {
    fn hashed(&self) -> QuorumCertificateHash {
        let mut hasher = Sha3_256::new();
        hasher.update(self.signature.0.clone());
        hasher.update(self.validators.len().to_le_bytes());
        for validator in self.validators.iter() {
            hasher.update(validator.0.clone());
        }
        QuorumCertificateHash(hasher.finalize().as_slice().to_owned())
    }
}

impl Display for Ticket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Ticket: {self:?}")
    }
}

impl Display for QuorumCertificateHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "0x{}",
            hex::encode(self.0.get(..HASH_DISPLAY_SIZE).unwrap_or(&self.0))
        )
    }
}

impl Display for ConsensusNetMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let enum_variant: &'static str = self.into();

        match self {
            ConsensusNetMessage::Prepare(cp, ticket, view) => {
                write!(f, "{enum_variant} CP: {cp}, ticket: {ticket}, view: {view}")
            }
            ConsensusNetMessage::PrepareVote(cphash) => {
                write!(f, "{} (CP hash: {})", enum_variant, cphash.msg.0)
            }
            ConsensusNetMessage::Confirm(cert, cphash) => {
                _ = writeln!(f, "{enum_variant} (CP hash: {cphash})");
                _ = write!(f, "Certificate {} with validators ", cert.signature);
                for v in cert.validators.iter() {
                    _ = write!(f, "{v},");
                }
                write!(f, "")
            }
            ConsensusNetMessage::ConfirmAck(cphash, ..) => {
                write!(f, "{} (CP hash: {})", enum_variant, cphash.msg.0)
            }
            ConsensusNetMessage::Commit(cert, cphash) => {
                _ = writeln!(f, "{enum_variant} (CP hash: {cphash})");
                _ = write!(f, "Certificate {} with validators ", cert.signature);
                for v in cert.validators.iter() {
                    _ = write!(f, "{v},");
                }
                write!(f, "")
            }

            ConsensusNetMessage::ValidatorCandidacy(candidacy) => {
                write!(f, "{enum_variant} (Candidacy {candidacy})")
            }
            ConsensusNetMessage::Timeout((signed_slot_view, tk)) => {
                _ = writeln!(
                    f,
                    "{} - Slot: {} View: {}",
                    enum_variant, signed_slot_view.msg.0, signed_slot_view.msg.1
                );
                match tk {
                    TimeoutKind::NilProposal(_) => {
                        _ = writeln!(f, "NilProposal Timeout");
                    }
                    TimeoutKind::PrepareQC((cert, cp)) => {
                        _ = writeln!(f, "PrepareQC certificate {}", cert.signature);
                        _ = write!(f, "CP: {cp}");
                        for v in cert.validators.iter() {
                            _ = write!(f, "{v},");
                        }
                    }
                }
                write!(f, "")
            }
            ConsensusNetMessage::TimeoutCertificate(cert, kindcert, slot, view) => {
                _ = writeln!(f, "{enum_variant} - Slot: {slot} View: {view}");
                _ = writeln!(f, "Validators {}", cert.signature);
                for v in cert.validators.iter() {
                    _ = write!(f, "{v},");
                }
                match kindcert {
                    TCKind::NilProposal(..) => {
                        _ = writeln!(f, "NilProposal certificate");
                    }
                    TCKind::PrepareQC((kindcert, cp)) => {
                        _ = writeln!(
                            f,
                            "PrepareQC certificate {} on CP hash {}",
                            kindcert.signature,
                            cp.hashed()
                        );
                        for v in kindcert.validators.iter() {
                            _ = write!(f, "{v},");
                        }
                    }
                }
                write!(f, "")
            }
            ConsensusNetMessage::SyncRequest(consensus_proposal_hash) => {
                write!(f, "{enum_variant} (CP hash: {consensus_proposal_hash})")
            }
            ConsensusNetMessage::SyncReply((sender, consensus_proposal, ticket, view)) => {
                write!(
                    f,
                    "{enum_variant} sender: {sender}, CP: {consensus_proposal}, ticket: {ticket}, view: {view}"
                )
            }
        }
    }
}

impl IntoHeaderSignableData for ConsensusNetMessage {
    // This signature is just for DOS / efficiency
    fn to_header_signable_data(&self) -> HeaderSignableData {
        HeaderSignableData(match self {
            ConsensusNetMessage::Prepare(cp, t, v) => {
                borsh::to_vec(&(cp.hashed(), t, v)).unwrap_or_default()
            }
            ConsensusNetMessage::PrepareVote(pv) => pv.msg.0.clone().0.into_bytes(),
            ConsensusNetMessage::Confirm(qc, cph) => {
                borsh::to_vec(&(&qc.signature, cph)).unwrap_or_default()
            }
            ConsensusNetMessage::ConfirmAck(ca) => ca.msg.0.clone().0.into_bytes(),
            ConsensusNetMessage::Commit(qc, cph) => borsh::to_vec(&(qc, cph)).unwrap_or_default(),
            ConsensusNetMessage::Timeout((SignedByValidator { signature, .. }, tk)) => match tk {
                TimeoutKind::NilProposal(..) => borsh::to_vec(signature),
                TimeoutKind::PrepareQC((qc, cp)) => {
                    borsh::to_vec(&(signature, &qc.signature, cp.hashed()))
                }
            }
            .unwrap_or_default(),
            ConsensusNetMessage::TimeoutCertificate(qc, tck, s, v) => match tck {
                TCKind::NilProposal(..) => borsh::to_vec(&(&qc.signature, s, v)),
                TCKind::PrepareQC((qc, cp)) => borsh::to_vec(&(&qc.signature, cp.hashed(), s, v)),
            }
            .unwrap_or_default(),
            ConsensusNetMessage::ValidatorCandidacy(vc) => borsh::to_vec(vc).unwrap_or_default(),
            ConsensusNetMessage::SyncRequest(cph) => borsh::to_vec(cph).unwrap_or_default(),
            ConsensusNetMessage::SyncReply((pb, cp, t, v)) => borsh::to_vec(&(
                match t {
                    Ticket::CommitQC(qc) => Some(&qc.signature),
                    Ticket::TimeoutQC(qc, ..) => Some(&qc.signature),
                    _ => None,
                },
                pb,
                cp.hashed(),
                t,
                v,
            ))
            .unwrap_or_default(),
        })
    }
}

#[derive(
    Debug,
    Serialize,
    Deserialize,
    Clone,
    BorshSerialize,
    BorshDeserialize,
    PartialEq,
    Eq,
    Default,
    Ord,
    PartialOrd,
)]
pub struct QuorumCertificateHash(pub Vec<u8>);

#[derive(
    Debug,
    Serialize,
    Deserialize,
    Clone,
    BorshSerialize,
    BorshDeserialize,
    PartialEq,
    Eq,
    Ord,
    PartialOrd,
    Hash,
)]
// Wrapper type with a marker to avoid confusion
pub struct QuorumCertificate<T>(pub AggregateSignature, pub T);

impl<T> Deref for QuorumCertificate<T> {
    type Target = AggregateSignature;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(
    Debug,
    Serialize,
    Deserialize,
    Clone,
    BorshSerialize,
    BorshDeserialize,
    PartialEq,
    Eq,
    Ord,
    PartialOrd,
)]
pub struct TimeoutCertificate(ConsensusProposalHash, TimeoutQC);

// A Ticket is necessary to send a valid prepare
#[derive(
    Debug,
    Serialize,
    Deserialize,
    Clone,
    BorshSerialize,
    BorshDeserialize,
    PartialEq,
    Eq,
    Hash,
    Ord,
    PartialOrd,
    IntoStaticStr,
)]
pub enum Ticket {
    // Special value for the initial Cut, needed because we don't have a quorum certificate for the genesis block.
    Genesis,
    CommitQC(CommitQC),
    TimeoutQC(TimeoutQC, TCKind),
    /// Technical value used internally that should never be used to start a slot
    ForcedCommitQc(View),
}

pub type PrepareQC = QuorumCertificate<PrepareVoteMarker>;
pub type CommitQC = QuorumCertificate<ConfirmAckMarker>;
pub type TimeoutQC = QuorumCertificate<ConsensusTimeoutMarker>;
pub type NilQC = QuorumCertificate<NilConsensusTimeoutMarker>;

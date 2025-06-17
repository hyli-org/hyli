use std::fmt::Display;

use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};
use sha3::{Digest, Sha3_256};
use utils::TimestampMs;
use utoipa::ToSchema;

use crate::{staking::*, *};

pub type Slot = u64;
pub type View = u64;

#[derive(Clone, Serialize, Deserialize, ToSchema, Debug)]
pub struct ConsensusInfo {
    pub slot: Slot,
    pub view: View,
    pub round_leader: ValidatorPublicKey,
    pub last_timestamp: TimestampMs,
    pub validators: Vec<ValidatorPublicKey>,
}

// -----------------------------
// --- Consensus data model ----
// -----------------------------

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
pub struct ValidatorCandidacy {
    // No need to store a specific pubkeey as it will be part of the signature of this message.
    pub peer_address: String,
}

impl Display for SignedByValidator<ValidatorCandidacy> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Pubkey: {}", self.signature.validator)
    }
}

#[cfg(feature = "sqlx")]
impl sqlx::Type<sqlx::Postgres> for ConsensusProposalHash {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        <String as sqlx::Type<sqlx::Postgres>>::type_info()
    }
}

#[cfg(feature = "sqlx")]
impl sqlx::Encode<'_, sqlx::Postgres> for ConsensusProposalHash {
    fn encode_by_ref(
        &self,
        buf: &mut sqlx::postgres::PgArgumentBuffer,
    ) -> std::result::Result<
        sqlx::encode::IsNull,
        std::boxed::Box<(dyn std::error::Error + std::marker::Send + std::marker::Sync + 'static)>,
    > {
        <String as sqlx::Encode<sqlx::Postgres>>::encode_by_ref(&self.0, buf)
    }
}

#[cfg(feature = "sqlx")]
impl<'r> sqlx::Decode<'r, sqlx::Postgres> for ConsensusProposalHash {
    fn decode(
        value: sqlx::postgres::PgValueRef<'r>,
    ) -> std::result::Result<
        ConsensusProposalHash,
        std::boxed::Box<(dyn std::error::Error + std::marker::Send + std::marker::Sync + 'static)>,
    > {
        let inner = <String as sqlx::Decode<sqlx::Postgres>>::decode(value)?;
        Ok(ConsensusProposalHash(inner))
    }
}

#[derive(
    Debug,
    Clone,
    Default,
    Serialize,
    Deserialize,
    BorshSerialize,
    BorshDeserialize,
    PartialEq,
    Eq,
    Ord,
    PartialOrd,
)]
pub struct ConsensusProposal {
    pub slot: Slot,
    pub parent_hash: ConsensusProposalHash,
    pub cut: Cut,
    pub staking_actions: Vec<ConsensusStakingAction>,
    pub timestamp: TimestampMs,
}

/// This is the hash of the proposal, signed by validators
/// Any consensus-critical data should be hashed here.
impl Hashed<ConsensusProposalHash> for ConsensusProposal {
    fn hashed(&self) -> ConsensusProposalHash {
        let mut hasher = Sha3_256::new();
        hasher.update(self.slot.to_le_bytes());
        self.cut.iter().for_each(|(lane_id, hash, _, _)| {
            hasher.update(&lane_id.0 .0);
            hasher.update(hash.0.as_bytes());
        });
        self.staking_actions.iter().for_each(|val| match val {
            ConsensusStakingAction::Bond { candidate } => {
                hasher.update(&candidate.signature.validator.0)
            }
            ConsensusStakingAction::PayFeesForDaDi {
                lane_id,
                cumul_size,
            } => {
                hasher.update(&lane_id.0 .0);
                hasher.update(cumul_size.0.to_le_bytes())
            }
        });
        hasher.update(self.timestamp.0.to_le_bytes());
        hasher.update(self.parent_hash.0.as_bytes());
        ConsensusProposalHash(hex::encode(hasher.finalize()))
    }
}

impl std::hash::Hash for ConsensusProposal {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        Hashed::<ConsensusProposalHash>::hashed(self).hash(state);
    }
}

impl Display for ConsensusProposal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Hash: {}, Parent Hash: {}, Slot: {}, Cut: {}, staking_actions: {:?}",
            self.hashed(),
            self.parent_hash,
            self.slot,
            CutDisplay(&self.cut),
            self.staking_actions,
        )
    }
}

impl Display for ConsensusProposalHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", &self.0)
    }
}

/// Represents the operations that can be performed by the consensus
#[derive(
    BorshSerialize,
    BorshDeserialize,
    Debug,
    Clone,
    Deserialize,
    Serialize,
    PartialEq,
    Eq,
    Ord,
    PartialOrd,
)]
pub enum ConsensusStakingAction {
    /// Bonding a new validator candidate
    Bond {
        // Boxed to reduce size of the enum
        // cf https://rust-lang.github.io/rust-clippy/master/index.html#large_enum_variant
        candidate: Box<SignedByValidator<ValidatorCandidacy>>,
    },

    /// DaDi = Data Dissemination
    PayFeesForDaDi {
        lane_id: LaneId,
        cumul_size: LaneBytesSize,
    },
}

impl From<SignedByValidator<ValidatorCandidacy>> for ConsensusStakingAction {
    fn from(val: SignedByValidator<ValidatorCandidacy>) -> Self {
        ConsensusStakingAction::Bond {
            candidate: Box::new(val),
        }
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn test_consensus_proposal_hash() {
        use super::*;
        let proposal = ConsensusProposal {
            slot: 1,
            cut: Cut::default(),
            staking_actions: vec![],
            timestamp: TimestampMs(1),
            parent_hash: ConsensusProposalHash("".to_string()),
        };
        let hash = proposal.hashed();
        assert_eq!(hash.0.len(), 64);
    }
    #[test]
    fn test_consensus_proposal_hash_all_items() {
        use super::*;
        let mut a = ConsensusProposal {
            slot: 1,
            cut: vec![(
                LaneId(ValidatorPublicKey(vec![1])),
                DataProposalHash("propA".to_string()),
                LaneBytesSize(1),
                AggregateSignature::default(),
            )],
            staking_actions: vec![SignedByValidator::<ValidatorCandidacy> {
                msg: ValidatorCandidacy {
                    peer_address: "peer".to_string(),
                },
                signature: ValidatorSignature {
                    signature: Signature(vec![1, 2, 3]),
                    validator: ValidatorPublicKey(vec![1, 2, 3]),
                },
            }
            .into()],
            timestamp: TimestampMs(1),
            parent_hash: ConsensusProposalHash("parent".to_string()),
        };
        let mut b = ConsensusProposal {
            slot: 1,
            cut: vec![(
                LaneId(ValidatorPublicKey(vec![1])),
                DataProposalHash("propA".to_string()),
                LaneBytesSize(1),
                AggregateSignature {
                    signature: Signature(vec![1, 2, 3]),
                    validators: vec![ValidatorPublicKey(vec![1, 2, 3])],
                },
            )],
            staking_actions: vec![SignedByValidator::<ValidatorCandidacy> {
                msg: ValidatorCandidacy {
                    peer_address: "different".to_string(),
                },
                signature: ValidatorSignature {
                    signature: Signature(vec![3, 4, 5]),
                    validator: ValidatorPublicKey(vec![3, 4, 5]),
                },
            }
            .into()],
            timestamp: TimestampMs(1),
            parent_hash: ConsensusProposalHash("parent".to_string()),
        };
        assert_ne!(a.hashed(), b.hashed());
        if let ConsensusStakingAction::Bond { candidate: a } =
            b.staking_actions.first_mut().unwrap()
        {
            a.msg.peer_address = "peer".to_string()
        }
        assert_ne!(a.hashed(), b.hashed());
        if let ConsensusStakingAction::Bond { candidate: a } =
            b.staking_actions.first_mut().unwrap()
        {
            a.signature = ValidatorSignature {
                signature: Signature(vec![1, 2, 3]),
                validator: ValidatorPublicKey(vec![1, 2, 3]),
            };
        }
        assert_eq!(a.hashed(), b.hashed());
        a.timestamp = TimestampMs(2);
        assert_ne!(a.hashed(), b.hashed());
        b.timestamp = TimestampMs(2);
        assert_eq!(a.hashed(), b.hashed());

        a.slot = 2;
        assert_ne!(a.hashed(), b.hashed());
        b.slot = 2;
        assert_eq!(a.hashed(), b.hashed());

        a.parent_hash = ConsensusProposalHash("different".to_string());
        assert_ne!(a.hashed(), b.hashed());
        b.parent_hash = ConsensusProposalHash("different".to_string());
        assert_eq!(a.hashed(), b.hashed());
    }
}

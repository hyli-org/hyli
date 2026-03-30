use crate::*;
use borsh::{BorshDeserialize, BorshSerialize};
use derive_more::derive::Display;
use serde::{Deserialize, Serialize};
use std::{cmp::Ordering, sync::Arc};

#[derive(Debug, Serialize, Deserialize, Clone, BorshSerialize, BorshDeserialize, Display)]
#[display("")]
pub struct SignedBlock {
    pub data_proposals: Vec<(LaneId, Vec<DataProposal>)>,
    pub consensus_proposal: ConsensusProposal,
    // NB: this can be different for different validators
    pub certificate: AggregateSignature,
}

#[derive(Debug, Serialize, Deserialize, Clone, BorshSerialize, BorshDeserialize, Display)]
#[display("")]
pub struct ArcSignedBlock {
    pub data_proposals: Vec<(LaneId, Vec<ArcBorsh<DataProposal>>)>,
    pub consensus_proposal: ConsensusProposal,
    // NB: this can be different for different validators
    pub certificate: AggregateSignature,
}

impl SignedBlock {
    pub fn parent_hash(&self) -> &ConsensusProposalHash {
        &self.consensus_proposal.parent_hash
    }

    pub fn height(&self) -> BlockHeight {
        BlockHeight(self.consensus_proposal.slot)
    }

    pub fn has_txs(&self) -> bool {
        for (_, _, txs) in self.iter_txs() {
            if !txs.is_empty() {
                return true;
            }
        }

        false
    }

    pub fn count_txs(&self) -> usize {
        self.iter_txs().map(|(_, _, txs)| txs.len()).sum()
    }

    pub fn iter_txs(&self) -> impl Iterator<Item = (LaneId, DataProposalHash, &Vec<Transaction>)> {
        self.data_proposals
            .iter()
            .flat_map(|(lane_id, dps)| std::iter::zip(std::iter::repeat(lane_id.clone()), dps))
            .map(|(lane_id, dp)| {
                (
                    lane_id.clone(),
                    dp.parent_data_proposal_hash.as_tx_parent_hash(),
                    &dp.txs,
                )
            })
    }

    pub fn iter_txs_with_id(&self) -> impl Iterator<Item = (LaneId, TxId, &Transaction)> {
        self.iter_txs().flat_map(move |(lane_id, dp_hash, txs)| {
            txs.iter()
                .map(move |tx| (lane_id.clone(), TxId(dp_hash.clone(), tx.hashed()), tx))
        })
    }
}

impl Hashed<ConsensusProposalHash> for SignedBlock {
    fn hashed(&self) -> ConsensusProposalHash {
        self.consensus_proposal.hashed()
    }
}

impl ArcSignedBlock {
    pub fn parent_hash(&self) -> &ConsensusProposalHash {
        &self.consensus_proposal.parent_hash
    }

    pub fn height(&self) -> BlockHeight {
        BlockHeight(self.consensus_proposal.slot)
    }

    pub fn has_txs(&self) -> bool {
        for (_, _, txs) in self.iter_txs() {
            if !txs.is_empty() {
                return true;
            }
        }

        false
    }

    pub fn count_txs(&self) -> usize {
        self.iter_txs().map(|(_, _, txs)| txs.len()).sum()
    }

    pub fn iter_txs(&self) -> impl Iterator<Item = (LaneId, DataProposalHash, &Vec<Transaction>)> {
        self.data_proposals
            .iter()
            .flat_map(|(lane_id, dps)| std::iter::zip(std::iter::repeat(lane_id.clone()), dps))
            .map(|(lane_id, dp)| {
                (
                    lane_id.clone(),
                    dp.parent_data_proposal_hash.as_tx_parent_hash(),
                    &dp.txs,
                )
            })
    }
}

impl From<SignedBlock> for ArcSignedBlock {
    fn from(value: SignedBlock) -> Self {
        Self {
            data_proposals: value
                .data_proposals
                .into_iter()
                .map(|(lane_id, dps)| {
                    (
                        lane_id,
                        dps.into_iter()
                            .map(|dp| ArcBorsh::new(Arc::new(dp)))
                            .collect(),
                    )
                })
                .collect(),
            consensus_proposal: value.consensus_proposal,
            certificate: value.certificate,
        }
    }
}

impl From<ArcSignedBlock> for SignedBlock {
    fn from(value: ArcSignedBlock) -> Self {
        Self {
            data_proposals: value
                .data_proposals
                .into_iter()
                .map(|(lane_id, dps)| {
                    (
                        lane_id,
                        dps.into_iter()
                            .map(|dp| Arc::unwrap_or_clone(dp.arc()))
                            .collect(),
                    )
                })
                .collect(),
            consensus_proposal: value.consensus_proposal,
            certificate: value.certificate,
        }
    }
}

impl Hashed<ConsensusProposalHash> for ArcSignedBlock {
    fn hashed(&self) -> ConsensusProposalHash {
        self.consensus_proposal.hashed()
    }
}

impl Ord for ArcSignedBlock {
    fn cmp(&self, other: &Self) -> Ordering {
        self.height().0.cmp(&other.height().0)
    }
}

impl PartialOrd for ArcSignedBlock {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for ArcSignedBlock {
    fn eq(&self, other: &Self) -> bool {
        self.hashed() == other.hashed()
    }
}

impl Eq for ArcSignedBlock {}

impl std::default::Default for ArcSignedBlock {
    fn default() -> Self {
        SignedBlock::default().into()
    }
}

impl Ord for SignedBlock {
    fn cmp(&self, other: &Self) -> Ordering {
        self.height().0.cmp(&other.height().0)
    }
}

impl PartialOrd for SignedBlock {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for SignedBlock {
    fn eq(&self, other: &Self) -> bool {
        self.hashed() == other.hashed()
    }
}

impl Eq for SignedBlock {}

impl std::default::Default for SignedBlock {
    fn default() -> Self {
        SignedBlock {
            consensus_proposal: ConsensusProposal::default(),
            data_proposals: vec![],
            certificate: AggregateSignature {
                signature: crate::Signature("signature".into()),
                validators: vec![],
            },
        }
    }
}

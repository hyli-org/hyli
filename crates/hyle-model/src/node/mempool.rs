use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};
use sha3::{Digest, Sha3_256};
use std::{fmt::Display, sync::RwLock};

use crate::*;

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub enum MempoolStatusEvent {
    WaitingDissemination {
        parent_data_proposal_hash: DataProposalHash,
        tx: Transaction,
    },
    DataProposalCreated {
        parent_data_proposal_hash: DataProposalHash,
        data_proposal_hash: DataProposalHash,
        txs_metadatas: Vec<TransactionMetadata>,
    },
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum MempoolBlockEvent {
    BuiltSignedBlock(SignedBlock),
    StartedBuildingBlocks(BlockHeight),
}

#[derive(Debug, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
#[readonly::make]
pub struct DataProposal {
    pub parent_data_proposal_hash: Option<DataProposalHash>,
    pub txs: Vec<Transaction>,
    /// Internal cache of the hash of the transaction
    #[borsh(skip)]
    hash_cache: RwLock<Option<DataProposalHash>>,
}

impl DataProposal {
    pub fn new(parent_data_proposal_hash: Option<DataProposalHash>, txs: Vec<Transaction>) -> Self {
        Self {
            parent_data_proposal_hash,
            txs,
            hash_cache: RwLock::new(None),
        }
    }

    pub fn remove_proofs(&mut self) {
        self.txs.iter_mut().for_each(|tx| {
            match &mut tx.transaction_data {
                TransactionData::VerifiedProof(proof_tx) => {
                    proof_tx.proof = None;
                }
                TransactionData::Proof(_) => {
                    // This can never happen.
                    // A DataProposal that has been processed has turned all TransactionData::Proof into TransactionData::VerifiedProof
                    unreachable!();
                }
                TransactionData::Blob(_) => {}
            }
        });
    }

    /// This is used to set the hash of the DataProposal when we can trust we know it
    /// (specifically - deserializating from local storage)
    /// # Safety
    /// Marked unsafe so you think twice before using it, but this is safe rust-wise.
    pub unsafe fn unsafe_set_hash(&mut self, hash: &DataProposalHash) {
        self.hash_cache.write().unwrap().replace(hash.clone());
    }
}

impl Clone for DataProposal {
    fn clone(&self) -> Self {
        let mut new = Self::default();
        new.parent_data_proposal_hash = self.parent_data_proposal_hash.clone();
        new.txs = self.txs.clone();
        new.hash_cache = RwLock::new(self.hash_cache.read().unwrap().clone());
        new
    }
}

impl PartialEq for DataProposal {
    fn eq(&self, other: &Self) -> bool {
        self.hashed() == other.hashed()
    }
}

impl Eq for DataProposal {}

impl DataSized for DataProposal {
    fn estimate_size(&self) -> usize {
        self.txs.iter().map(|tx| tx.estimate_size()).sum()
    }
}

#[derive(
    Default,
    Serialize,
    Deserialize,
    Debug,
    Clone,
    PartialEq,
    Eq,
    Hash,
    Ord,
    PartialOrd,
    BorshDeserialize,
    BorshSerialize,
)]
pub struct TxId(pub DataProposalHash, pub TxHash);

#[derive(
    Clone,
    Debug,
    Default,
    Serialize,
    Deserialize,
    BorshSerialize,
    BorshDeserialize,
    PartialEq,
    Eq,
    Hash,
    Ord,
    PartialOrd,
)]
#[cfg_attr(feature = "full", derive(utoipa::ToSchema))]
pub struct DataProposalHash(pub String);

impl Hashed<DataProposalHash> for DataProposal {
    fn hashed(&self) -> DataProposalHash {
        if let Some(hash) = self.hash_cache.read().unwrap().as_ref() {
            return hash.clone();
        }
        let mut hasher = Sha3_256::new();
        if let Some(ref parent_data_proposal_hash) = self.parent_data_proposal_hash {
            hasher.update(parent_data_proposal_hash.0.as_bytes());
        }
        for tx in self.txs.iter() {
            hasher.update(tx.hashed().0);
        }
        let hash = DataProposalHash(hex::encode(hasher.finalize()));
        *self.hash_cache.write().unwrap() = Some(hash.clone());
        hash
    }
}
impl Display for DataProposalHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
impl Display for DataProposal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.hashed())
    }
}

impl Display for LaneId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

pub type PoDA = AggregateSignature;
pub type Cut = Vec<(LaneId, DataProposalHash, LaneBytesSize, PoDA)>;

impl Display for TxId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", &self.0 .0, &self.1 .0)
    }
}

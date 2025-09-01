use std::{collections::BTreeMap, sync::Arc};

use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};
use sha3::{Digest, Sha3_256};
use utoipa::ToSchema;

use crate::*;

#[derive(Debug, Serialize, Deserialize, Clone, BorshSerialize, BorshDeserialize, Eq, PartialEq)]
pub enum DataEvent {
    OrderedSignedBlock(SignedBlock),
}

#[derive(
    Default,
    Debug,
    Clone,
    Serialize,
    Deserialize,
    ToSchema,
    BorshSerialize,
    BorshDeserialize,
    Eq,
    PartialEq,
)]
pub struct Contract {
    pub name: ContractName,
    pub program_id: ProgramId,
    pub state: StateCommitment,
    pub verifier: Verifier,
    pub timeout_window: TimeoutWindow,
}

#[derive(
    Default,
    Debug,
    Clone,
    PartialEq,
    Eq,
    BorshSerialize,
    BorshDeserialize,
    serde::Serialize,
    serde::Deserialize,
    ToSchema,
)]
pub struct UnsettledBlobTransaction {
    pub tx: BlobTransaction,
    pub tx_id: TxId,
    #[schema(value_type = TxContext)]
    pub tx_context: Arc<TxContext>,
    pub blobs_hash: BlobsHashes,
    pub possible_proofs: BTreeMap<BlobIndex, Vec<(ProgramId, Verifier, TxId, HyliOutput)>>, // ToSchema doesn't like the alias
}

pub type BlobProof = (ProgramId, Verifier, TxId, HyliOutput);

impl UnsettledBlobTransaction {
    pub fn iter_blobs(&self) -> impl Iterator<Item = (&Blob, &Vec<BlobProof>)> + Clone {
        std::iter::zip(self.tx.blobs.iter(), self.possible_proofs.values())
    }
}

#[derive(
    Debug,
    Default,
    Clone,
    serde::Serialize,
    serde::Deserialize,
    BorshSerialize,
    BorshDeserialize,
    Eq,
    PartialEq,
)]
pub struct HandledBlobProofOutput {
    pub proof_tx_hash: TxHash,
    pub blob_tx_hash: TxHash,
    pub blob_index: BlobIndex,
    pub contract_name: ContractName,
    pub verifier: Verifier,
    pub program_id: ProgramId,
    pub hyli_output: HyliOutput,
    pub blob_proof_output_index: usize,
}

#[derive(
    Debug, Default, Serialize, Deserialize, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize,
)]
pub struct BlobProofOutput {
    // TODO: this can be recovered from the hyli output
    pub blob_tx_hash: TxHash,
    // TODO: remove this?
    pub original_proof_hash: ProofDataHash,

    /// HyliOutput of the proof for this blob
    pub hyli_output: HyliOutput,
    /// Program ID used to verify the proof.
    pub program_id: ProgramId,
    /// verifier used to verify the proof.
    pub verifier: Verifier,
}

pub struct BlobProofOutputHash(pub Vec<u8>);

impl Hashed<BlobProofOutputHash> for BlobProofOutput {
    fn hashed(&self) -> BlobProofOutputHash {
        let mut hasher = Sha3_256::new();
        hasher.update(self.blob_tx_hash.0.as_bytes());
        hasher.update(self.original_proof_hash.0.as_bytes());
        hasher.update(self.program_id.0.clone());
        hasher.update(contract::Hashed::hashed(&self.hyli_output).0);
        BlobProofOutputHash(hasher.finalize().to_vec())
    }
}

pub struct HyliOutputHash(pub Vec<u8>);
impl Hashed<HyliOutputHash> for HyliOutput {
    fn hashed(&self) -> HyliOutputHash {
        let mut hasher = Sha3_256::new();
        hasher.update(self.version.to_le_bytes());
        hasher.update(self.initial_state.0.clone());
        hasher.update(self.next_state.0.clone());
        hasher.update(self.identity.0.as_bytes());
        hasher.update(self.index.0.to_le_bytes());
        for blob in &self.blobs {
            hasher.update(blob.0 .0.to_le_bytes());
            hasher.update(blob.1.contract_name.0.as_bytes());
            hasher.update(blob.1.data.0.as_slice());
        }
        hasher.update([self.success as u8]);
        hasher.update(self.onchain_effects.len().to_le_bytes());
        self.onchain_effects
            .iter()
            .for_each(|c| hasher.update(c.hashed().0));
        hasher.update(&self.program_outputs);
        HyliOutputHash(hasher.finalize().to_vec())
    }
}

#[derive(
    Debug, Clone, Serialize, Deserialize, ToSchema, BorshSerialize, BorshDeserialize, Eq, PartialEq,
)]
#[serde(tag = "name", content = "metadata")]
pub enum TransactionStateEvent {
    Sequenced,
    Error(String),
    NewProof {
        blob_index: BlobIndex,
        proof_tx_hash: TxHash,
        program_output: Vec<u8>,
    },
    Settled,
    SettledAsFailed,
    TimedOut,
    DroppedAsDuplicate,
}

#[derive(Default, Debug, Serialize, Deserialize, Clone, BorshSerialize, BorshDeserialize)]
pub struct BlockStakingData {
    pub new_bounded_validators: Vec<ValidatorPublicKey>,
    pub staking_actions: Vec<(Identity, StakingAction)>,
}

#[derive(Default, Debug, Serialize, Deserialize, Clone, BorshSerialize, BorshDeserialize)]
pub struct StatefulEvents {
    pub events: Vec<(TxId, StatefulEvent)>,
}

#[derive(Debug, Serialize, Deserialize, Clone, BorshSerialize, BorshDeserialize)]
pub enum StatefulEvent {
    SequencedTx(BlobTransaction, Arc<TxContext>),
    SettledTx(UnsettledBlobTransaction),
    FailedTx(UnsettledBlobTransaction),
    TimedOutTx(UnsettledBlobTransaction),
    ContractRegistration(ContractName, Contract, Option<Vec<u8>>),
    ContractUpdate(ContractName, Contract),
    ContractDelete(ContractName),
}

#[derive(Default, Debug, Serialize, Deserialize, Clone, BorshSerialize, BorshDeserialize)]
pub struct NodeStateBlock {
    pub signed_block: std::sync::Arc<SignedBlock>,
    pub parsed_block: std::sync::Arc<Block>,
    pub staking_data: std::sync::Arc<BlockStakingData>,
    pub stateful_events: std::sync::Arc<StatefulEvents>,
}

#[derive(Debug, Serialize, Deserialize, Clone, BorshSerialize, BorshDeserialize)]
pub enum NodeStateEvent {
    NewBlock(NodeStateBlock),
}

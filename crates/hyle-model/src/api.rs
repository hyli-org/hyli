use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use strum::IntoDiscriminant;
use utoipa::ToSchema;

use crate::{
    utils::TimestampMs, BlockHash, BlockHeight, ConsensusProposalHash, ContractName,
    DataProposalHash, Identity, LaneBytesSize, LaneId, ProgramId, StateCommitment, Transaction,
    TransactionKind, TxHash, ValidatorPublicKey, Verifier,
};

#[derive(Clone, Serialize, Deserialize, Debug, ToSchema)]
pub struct NodeInfo {
    pub id: String,
    pub pubkey: Option<ValidatorPublicKey>,
    pub da_address: String,
}

#[derive(Clone, Serialize, Deserialize, Debug, ToSchema)]
pub struct NetworkStats {
    pub total_transactions: i64,           // Total number of transactions
    pub txs_last_day: i64,                 // Number of transactions in the last day
    pub total_contracts: i64,              // Total number of contracts
    pub contracts_last_day: i64,           // Number of contracts in the last day
    pub graph_tx_volume: Vec<(i64, i64)>,  // Graph data for transactions volume
    pub graph_block_time: Vec<(i64, f64)>, // Graph data for block time
}

#[cfg_attr(feature = "sqlx", derive(sqlx::FromRow))]
#[derive(Clone, Serialize, Deserialize, Debug, ToSchema)]
pub struct ProofStat {
    pub verifier: String,
    pub proof_count: i64,
}

#[derive(Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct APIRegisterContract {
    pub verifier: Verifier,
    pub program_id: ProgramId,
    pub state_commitment: StateCommitment,
    pub contract_name: ContractName,
    pub timeout_window: Option<u64>,
    pub constructor_metadata: Option<Vec<u8>>,
}

/// Copy from Staking contract
#[derive(Debug, Default, Serialize, Deserialize, ToSchema, Clone, PartialEq, Eq)]
pub struct APIStaking {
    pub stakes: BTreeMap<Identity, u128>,
    pub delegations: BTreeMap<ValidatorPublicKey, Vec<Identity>>,
    /// When a validator distribute rewards, it is added in this list to
    /// avoid distributing twice the rewards for a same block
    pub rewarded: BTreeMap<ValidatorPublicKey, Vec<BlockHeight>>,

    /// List of validators that are part of consensus
    pub bonded: Vec<ValidatorPublicKey>,
    pub total_bond: u128,

    /// Struct to handle fees
    pub fees: APIFees,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone, PartialEq, Eq)]
pub struct APIFeesBalance {
    pub balance: i128,
    pub cumul_size: LaneBytesSize,
}

#[derive(Debug, Default, Serialize, Deserialize, ToSchema, Clone, PartialEq, Eq)]
pub struct APIFees {
    /// Balance of each validator
    pub balances: BTreeMap<ValidatorPublicKey, APIFeesBalance>,
}

#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct APIBlock {
    // Struct for the blocks table
    pub hash: ConsensusProposalHash,
    pub parent_hash: ConsensusProposalHash,
    pub height: u64,    // Corresponds to BlockHeight
    pub timestamp: i64, // UNIX timestamp
    pub total_txs: u64, // Total number of transactions in the block
}

#[cfg_attr(feature = "sqlx", derive(sqlx::Type))]
#[cfg_attr(
    feature = "sqlx",
    sqlx(type_name = "transaction_type", rename_all = "snake_case")
)]
#[derive(Debug, Serialize, Deserialize, ToSchema, Clone, PartialEq, Eq)]
pub enum TransactionTypeDb {
    BlobTransaction,
    ProofTransaction,
    RegisterContractTransaction,
    Stake,
}

#[cfg_attr(feature = "sqlx", derive(sqlx::Type))]
#[derive(Debug, Serialize, Deserialize, ToSchema, Clone, PartialEq, Eq)]
#[cfg_attr(
    feature = "sqlx",
    sqlx(type_name = "transaction_status", rename_all = "snake_case")
)]
pub enum TransactionStatusDb {
    WaitingDissemination,
    DataProposalCreated,
    Success,
    Failure,
    Sequenced,
    TimedOut,
}

impl TransactionTypeDb {
    pub fn from(transaction: &Transaction) -> Self {
        transaction.transaction_data.discriminant().into()
    }
}

impl From<TransactionKind> for TransactionTypeDb {
    fn from(value: TransactionKind) -> Self {
        match value {
            TransactionKind::Blob => TransactionTypeDb::BlobTransaction,
            TransactionKind::Proof => TransactionTypeDb::ProofTransaction,
            TransactionKind::VerifiedProof => TransactionTypeDb::ProofTransaction,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone, PartialEq, Eq)]
pub struct APITransaction {
    // Struct for the transactions table
    pub tx_hash: TxHash,                           // Transaction hash
    pub parent_dp_hash: DataProposalHash,          // Data proposal hash
    pub version: u32,                              // Transaction version
    pub transaction_type: TransactionTypeDb,       // Type of transaction
    pub transaction_status: TransactionStatusDb,   // Status of the transaction
    pub block_hash: Option<ConsensusProposalHash>, // Corresponds to the block hash
    pub index: Option<u32>,                        // Index of the transaction within the block
    pub timestamp: Option<TimestampMs>,            // Timestamp of the transaction (block timestamp)
    pub lane_id: Option<LaneId>,                   // Lane ID where the transaction got disseminated
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone, PartialEq, Eq)]
pub struct APITransactionEvents {
    pub block_hash: BlockHash,
    pub block_height: BlockHeight,
    pub events: Vec<serde_json::Value>,
}

#[derive(Serialize, Deserialize, ToSchema, Debug, Clone, PartialEq)]
pub struct TransactionWithBlobs {
    pub tx_hash: TxHash,
    pub parent_dp_hash: DataProposalHash,
    pub block_hash: ConsensusProposalHash,
    pub index: u32,
    pub version: u32,
    pub transaction_type: TransactionTypeDb,
    pub transaction_status: TransactionStatusDb,
    pub identity: String,
    pub blobs: Vec<BlobWithStatus>,
}

#[serde_as]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct BlobWithStatus {
    pub contract_name: String, // Contract name associated with the blob
    #[serde_as(as = "serde_with::hex::Hex")]
    pub data: Vec<u8>, // Actual blob data
    pub proof_outputs: Vec<serde_json::Value>, // outputs of proofs
}

#[serde_as]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct APIContract {
    // Struct for the contracts table
    pub tx_hash: TxHash,  // Corresponds to the registration transaction hash
    pub verifier: String, // Verifier of the contract
    #[serde_as(as = "serde_with::hex::Hex")]
    pub program_id: Vec<u8>, // Program ID
    #[serde_as(as = "serde_with::hex::Hex")]
    pub state_commitment: Vec<u8>, // State commitment of the contract
    pub contract_name: String, // Contract name
    pub total_tx: u64,    // Total number of transactions associated with the contract
    pub unsettled_tx: u64, // Total number of unsettled transactions
    pub earliest_unsettled: Option<BlockHeight>, // Earliest unsettled transaction block height
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct APIContractState {
    // Struct for the contract_state table
    pub contract_name: String,             // Name of the contract
    pub block_hash: ConsensusProposalHash, // Hash of the block where the state is captured
    pub state_commitment: Vec<u8>,         // The contract state stored in JSON format
}

#[serde_as]
#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct APIBlob {
    pub tx_hash: TxHash,       // Corresponds to the transaction hash
    pub blob_index: u32,       // Index of the blob within the transaction
    pub identity: String,      // Identity of the blob
    pub contract_name: String, // Contract name associated with the blob
    #[serde_as(as = "serde_with::hex::Hex")]
    pub data: Vec<u8>, // Actual blob data
    pub proof_outputs: Vec<serde_json::Value>, // outputs of proofs
    pub verified: bool,        // Verification status
}

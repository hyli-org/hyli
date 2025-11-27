//! State required for participation in consensus by the node.

use anyhow::{bail, Error, Result};
use borsh::{BorshDeserialize, BorshSerialize};
use contract_registration::validate_contract_registration_metadata;
use contract_registration::{validate_contract_name_tld, validate_state_commitment_size};
use hyli_tld::validate_hyli_contract_blobs;
use metrics::NodeStateMetrics;
use ordered_tx_map::OrderedTxMap;
use sdk::verifiers::{NativeVerifiers, NATIVE_VERIFIERS_CONTRACT_LIST};
use sdk::*;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use timeouts::Timeouts;
use tracing::{debug, error, info, trace};

use crate::node_state::hyli_tld::handle_blob_for_hyli_tld;
use crate::node_state::native_verifiers::verify_native_impl;

mod api;
pub mod contract_registration;
mod hyli_tld;
pub mod metrics;
pub mod module;
pub mod native_verifiers;
mod ordered_tx_map;
mod timeouts;

#[derive(Debug, Clone)]
// Similar to OnchainEffect but slightly more adapted to nodestate settlement
enum SideEffect {
    Register(Option<Vec<u8>>),
    UpdateState,
    UpdateProgramId,
    UpdateTimeoutWindow,
    Delete,
}

#[derive(Default, Debug, Clone)]
pub struct ModifiedContractFields {
    pub program_id: bool,
    pub state: bool,
    pub verifier: bool,
    pub timeout_window: bool,
}

impl ModifiedContractFields {
    pub fn all() -> Self {
        ModifiedContractFields {
            program_id: true,
            state: true,
            verifier: true,
            timeout_window: true,
        }
    }
}

// When processing blobs, maintain an up-to-date status of the contract,
// and keep track of which fields changed and the list of side effects we processed.
#[derive(Debug, Clone)]
struct ModifiedContractData {
    contract_status: ContractStatus,
    modified_fields: ModifiedContractFields,
    side_effects: Vec<SideEffect>,
}

#[derive(Debug, Clone, Default)]
struct ContractSettlementFlags {
    // to determine if the contract can be settled independently of the rest of the transaction's blobs
    can_settle_independently: bool,
    // to determine if the contract has received all the proofs for each of its blobs
    is_fully_proved: bool,
}

#[derive(Debug, Clone, Eq, PartialEq)]
enum ContractStatus {
    WaitingDeletion,
    Deleted,
    UnknownState,
    Updated(Contract),
    // We expect the next blob to be a blob containing the constructor metadata
    RegisterWithConstructor(Contract),
}

/// Represents the result of processing a proof
#[derive(Debug)]
enum ProofProcessingResult {
    /// The proof was processed successfully
    Success,
    /// The proof failed validation but we should try other proofs
    Invalid(String),
    /// The proof failed with a fatal error - should settle as failed immediately
    ProvenFailure(String),
}

/// Represents the result of on-chain processing of a blob
#[derive(Debug)]
enum BlobProcessingResult {
    /// The blob was processed successfully
    Success,
    /// The blob failed with a fatal error - should settle as failed immediately
    ProvenFailure(String),
    /// The blob cannot be executed on-chain.
    NotApplicable,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum SettlementStatus {
    // When starting to settle a BlobTx, we need to have a "TryToSettle" status that will be updated when a tx is flagged as failed, or as not ready to settle.
    // At the end of the settlement recursion, if status is still TryToSettle, then the tx is settled as success.
    TryingToSettle,
    SettleAsSuccess,
    SettleAsFailed,
    // This status is used to flag a tx not ready to settle.
    // This can happens when a blob did not receive any proof; and that any following blob did not settled as failed.
    NotReadyToSettle,
}

#[derive(Debug, Clone)]
struct SettlementResult {
    settlement_status: SettlementStatus,
    contract_changes: BTreeMap<ContractName, ModifiedContractData>,
    contract_flags: BTreeMap<ContractName, ContractSettlementFlags>,
    blob_proof_output_indices: Vec<usize>,
}

struct SettledTxOutput {
    // Original blob transaction, now settled.
    pub tx: UnsettledBlobTransaction,
    /// Result of the settlement
    pub settlement_result: SettlementResult,
}

/// How a new blob TX should be handled by the node.
#[derive(Debug)]
enum BlobTxHandled {
    /// The node should try to settle the TX right away.
    ShouldSettle(TxHash),
    /// The TX is a duplicate of another unsettled TX and should be ignored/
    Duplicate,
    /// No special handling.
    Ok,
}

#[derive(serde::Serialize, Debug)]
pub enum TxEvent<'a> {
    RejectedBlobTransaction(
        &'a TxId,
        &'a LaneId,
        u32,
        &'a BlobTransaction,
        &'a Arc<TxContext>,
    ),
    DuplicateBlobTransaction(&'a TxId),
    SequencedBlobTransaction(
        &'a TxId,
        &'a LaneId,
        u32,
        &'a BlobTransaction,
        &'a Arc<TxContext>,
    ),
    SequencedProofTransaction(&'a TxId, &'a LaneId, u32, &'a VerifiedProofTransaction),
    Settled(&'a TxId, &'a UnsettledBlobTransaction),
    SettledAsFailed(&'a TxId, &'a UnsettledBlobTransaction),
    TimedOut(&'a TxId, &'a UnsettledBlobTransaction),
    TxError(&'a TxId, &'a str),
    NewProof(
        &'a TxId,
        &'a Blob,
        BlobIndex,
        &'a (ProgramId, Verifier, TxId, HyliOutput),
        usize,
    ),
    // Same data as NewProof, but this time the blob is settled.
    BlobSettled(
        &'a TxId,
        &'a UnsettledBlobTransaction,
        &'a Blob,
        BlobIndex,
        Option<&'a (ProgramId, Verifier, TxId, HyliOutput)>,
        usize,
    ),
    ContractDeleted(&'a TxId, &'a ContractName),
    ContractRegistered(
        &'a TxId,
        &'a ContractName,
        &'a Contract,
        &'a Option<Vec<u8>>,
    ),
    ContractStateUpdated(
        &'a TxId,
        &'a ContractName,
        &'a Contract,
        &'a StateCommitment,
    ),
    ContractProgramIdUpdated(&'a TxId, &'a ContractName, &'a Contract, &'a ProgramId),
    ContractTimeoutWindowUpdated(&'a TxId, &'a ContractName, &'a Contract, &'a TimeoutWindow),
}

impl<'a> TxEvent<'a> {
    pub fn tx_id(&self) -> &TxId {
        match self {
            TxEvent::RejectedBlobTransaction(tx_id, ..) => tx_id,
            TxEvent::DuplicateBlobTransaction(tx_id) => tx_id,
            TxEvent::SequencedBlobTransaction(tx_id, ..) => tx_id,
            TxEvent::SequencedProofTransaction(tx_id, ..) => tx_id,
            TxEvent::Settled(tx_id, ..) => tx_id,
            TxEvent::SettledAsFailed(tx_id, ..) => tx_id,
            TxEvent::TimedOut(tx_id, ..) => tx_id,
            TxEvent::TxError(tx_id, _) => tx_id,
            TxEvent::NewProof(tx_id, _, _, _, _) => tx_id,
            TxEvent::BlobSettled(tx_id, _, _, _, _, _) => tx_id,
            TxEvent::ContractDeleted(tx_id, _) => tx_id,
            TxEvent::ContractRegistered(tx_id, ..) => tx_id,
            TxEvent::ContractStateUpdated(tx_id, ..) => tx_id,
            TxEvent::ContractProgramIdUpdated(tx_id, ..) => tx_id,
            TxEvent::ContractTimeoutWindowUpdated(tx_id, ..) => tx_id,
        }
    }
}

pub trait NodeStateCallback {
    fn on_event(&mut self, event: &TxEvent);
}

#[derive(Debug, Clone)]
pub struct NodeState {
    pub metrics: NodeStateMetrics,
    pub store: NodeStateStore,
}

struct NodeStateProcessing<'a> {
    pub this: &'a mut NodeState,
    pub callback: &'a mut (dyn NodeStateCallback + Send + Sync),
}

impl NodeState {
    pub fn create(node_id: String, module_name: &'static str) -> Self {
        NodeState {
            metrics: NodeStateMetrics::global(node_id, module_name),
            store: NodeStateStore::default(),
        }
    }
}

impl std::ops::Deref for NodeState {
    type Target = NodeStateStore;

    fn deref(&self) -> &Self::Target {
        &self.store
    }
}

impl std::ops::DerefMut for NodeState {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.store
    }
}

impl<'a> std::ops::Deref for NodeStateProcessing<'a> {
    type Target = NodeStateStore;

    fn deref(&self) -> &Self::Target {
        &self.this.store
    }
}

impl<'a> std::ops::DerefMut for NodeStateProcessing<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.this.store
    }
}

/// NodeState manages the flattened, up-to-date state of the chain.
/// It processes raw transactions and outputs more structured data for indexers.
/// See also: NodeStateModule for the actual module implementation.
#[derive(BorshSerialize, BorshDeserialize, Debug, Clone)]
pub struct NodeStateStore {
    timeouts: Timeouts,
    pub current_height: BlockHeight,
    // This field is public for testing purposes
    pub contracts: HashMap<ContractName, Contract>,
    unsettled_transactions: OrderedTxMap,
}

/// Make sure we register the hyli contract with the same values before genesis, and in the genesis block
pub fn hyli_contract_definition() -> Contract {
    Contract {
        name: "hyli".into(),
        program_id: ProgramId(vec![0, 0, 0, 0]),
        state: StateCommitment::default(),
        verifier: Verifier("hyli".to_owned()),
        timeout_window: TimeoutWindow::NoTimeout,
    }
}

// TODO: we should register the 'hyli' TLD in the genesis block.
impl Default for NodeStateStore {
    fn default() -> Self {
        let mut ret = Self {
            timeouts: Timeouts::default(),
            current_height: BlockHeight(0),
            contracts: HashMap::new(),
            unsettled_transactions: OrderedTxMap::default(),
        };
        let hyli_contract = hyli_contract_definition();
        ret.contracts
            .insert(hyli_contract.name.clone(), hyli_contract);
        ret
    }
}

impl NodeState {
    /// Convenience wrapper
    pub fn process_signed_block<T: NodeStateCallback + Send + Sync>(
        &mut self,
        signed_block: &SignedBlock,
        callback: &mut T,
    ) -> Result<()> {
        let mut processing_node_state = NodeStateProcessing {
            this: self,
            callback,
        };
        processing_node_state.process_signed_block(signed_block)?;
        Ok(())
    }

    /// Further convenience wrapper
    pub fn handle_signed_block(&mut self, signed_block: SignedBlock) -> Result<NodeStateBlock> {
        let mut callback = BlockNodeStateCallback::from_signed(&signed_block);
        self.process_signed_block(&signed_block, &mut callback)?;
        let (parsed_block, staking_data, stateful_events) = callback.take();
        Ok(NodeStateBlock {
            signed_block: signed_block.into(),
            parsed_block: parsed_block.into(),
            staking_data: staking_data.into(),
            stateful_events: stateful_events.into(),
        })
    }
}

impl<'any> NodeStateProcessing<'any> {
    fn process_signed_block(&mut self, signed_block: &SignedBlock) -> Result<()> {
        let next_block = self.current_height + 1 == signed_block.height();
        let initial_block = self.current_height.0 == 0 && signed_block.height().0 == 0;
        if !next_block && !initial_block {
            bail!(
                "Handling signed block of height {} while current height is {}",
                signed_block.height(),
                self.current_height
            );
        }
        debug!("Handling signed block: {:?}", signed_block.height());

        self.current_height = signed_block.height();

        self.clear_timeouts();

        let mut lane_context_cache = HashMap::new();
        let mut next_unsettled_txs = BTreeSet::new();

        // Handle all transactions
        for (i, (lane_id, tx_id, tx)) in signed_block.iter_txs_with_id().enumerate() {
            debug!("TX {} on lane {}", tx_id.1, lane_id);

            match &tx.transaction_data {
                TransactionData::Blob(blob_transaction) => {
                    // Cache lane context to reuse the same Arc object
                    let tx_context =
                        lane_context_cache
                            .entry(lane_id.clone())
                            .or_insert_with(|| {
                                Arc::new(TxContext {
                                    lane_id: lane_id.clone(),
                                    block_hash: signed_block.hashed(),
                                    block_height: signed_block.height(),
                                    timestamp: signed_block.consensus_proposal.timestamp.clone(),
                                    chain_id: HYLI_TESTNET_CHAIN_ID,
                                })
                            });
                    match self.handle_blob_tx(tx_id.0.clone(), blob_transaction, tx_context.clone())
                    {
                        Ok(BlobTxHandled::ShouldSettle(tx_hash)) => {
                            self.callback.on_event(&TxEvent::SequencedBlobTransaction(
                                &tx_id,
                                &lane_id,
                                i as u32,
                                blob_transaction,
                                tx_context,
                            ));
                            let mut blob_tx_to_try_and_settle = BTreeSet::new();
                            blob_tx_to_try_and_settle.insert(tx_hash);
                            // In case of a BlobTransaction with only native verifies, we need to trigger the
                            // settlement here as we will never get a ProofTransaction
                            next_unsettled_txs =
                                self.settle_txs_until_done(blob_tx_to_try_and_settle);
                        }
                        Ok(BlobTxHandled::Duplicate) => {
                            debug!(
                                "Blob transaction: {:?} is already in the unsettled map, ignoring.",
                                tx_id
                            );
                            self.callback
                                .on_event(&TxEvent::DuplicateBlobTransaction(&tx_id));
                        }
                        Ok(BlobTxHandled::Ok) => {
                            self.callback.on_event(&TxEvent::SequencedBlobTransaction(
                                &tx_id,
                                &lane_id,
                                i as u32,
                                blob_transaction,
                                tx_context,
                            ));
                        }
                        Err(e) => {
                            let err = format!("Failed to handle blob transaction: {e:?}");
                            error!(tx_hash = %tx_id.1, "{err}");
                            self.callback.on_event(&TxEvent::RejectedBlobTransaction(
                                &tx_id,
                                &lane_id,
                                i as u32,
                                blob_transaction,
                                tx_context,
                            ));
                        }
                    }
                }
                TransactionData::Proof(_) => {
                    error!("Unverified recursive proof transaction should not be in a block");
                }
                TransactionData::VerifiedProof(proof_tx) => {
                    self.callback.on_event(&TxEvent::SequencedProofTransaction(
                        &tx_id, &lane_id, i as u32, proof_tx,
                    ));
                    // First, store the proofs and check if we can settle the transaction
                    // NB: if some of the blob proof outputs are bad, we just ignore those
                    // but we don't actually fail the transaction.
                    debug!(
                        "Handling verified proof transaction with {} proven blobs for {} (hash: {})",
                        proof_tx.proven_blobs.len(),
                        proof_tx.contract_name,
                        &tx_id
                    );
                    let blob_tx_to_try_and_settle = proof_tx
                        .proven_blobs
                        .iter()
                        .filter_map(|blob_proof_data| {
                            match self.handle_blob_proof(
                                &tx_id,
                                blob_proof_data,
                            ) {
                                Ok(maybe_tx_hash) => maybe_tx_hash,
                                Err(err) => {
                                    let err = format!(
                                        "Failed to handle blob #{} in verified proof transaction {}: {err:#}",
                                        blob_proof_data.hyli_output.index, &tx_id);
                                    debug!("{err}");
                                    // If we can find a matching blob-tx, store that there (helps debugging settlement issues)
                                    if let Some((tx, _)) = self.this.store.unsettled_transactions.get_for_settlement(
                                        &blob_proof_data.blob_tx_hash,
                                    ) {
                                        self.callback.on_event(&TxEvent::TxError(
                                            &tx.tx_id,
                                            &err,
                                        ));
                                    }
                                    // Also note the error on the proof transaction
                                    // Open question: should this be a different type from blob tx errors?
                                    self.callback.on_event(&TxEvent::TxError(
                                        &tx_id,
                                        &err,
                                    ));
                                    None
                                }
                            }})
                            .collect::<BTreeSet<_>>();
                    // Then try to settle transactions when we can.
                    next_unsettled_txs = self.settle_txs_until_done(blob_tx_to_try_and_settle);
                }
            }
            // For each transaction that could not be settled, if it is the next one to be settled, set its timeout
            for unsettled_tx in next_unsettled_txs.iter() {
                if self.unsettled_transactions.is_next_to_settle(unsettled_tx) {
                    self.set_timeout(unsettled_tx);
                }
            }
            next_unsettled_txs.clear();
        }

        self.this
            .metrics
            .record_contracts(self.contracts.len() as u64);

        let schedule_timeouts_nb = self.timeouts.count_all() as u64;
        self.this
            .metrics
            .record_scheduled_timeouts(schedule_timeouts_nb);
        self.this
            .metrics
            .record_unsettled_transactions(self.unsettled_transactions.len() as u64);
        self.this.metrics.add_processed_block();
        self.this
            .metrics
            .record_current_height(self.current_height.0);

        debug!("Done handling signed block: {:?}", signed_block.height());

        Ok(())
    }

    fn handle_blob_tx(
        &mut self,
        parent_dp_hash: DataProposalHash,
        tx: &BlobTransaction,
        tx_context: Arc<TxContext>,
    ) -> Result<BlobTxHandled, Error> {
        let tx_hash = tx.hashed();
        debug!("Handle blob tx: {:?} (hash: {})", tx, tx_hash);

        tx.validate_identity()?;

        if tx.blobs.is_empty() {
            bail!("Blob Transaction must have at least one blob");
        }

        let (blob_tx_hash, blobs_hash) = (tx.hashed(), tx.blobs_hash());

        // Reject blob Tx with blobs for the 'hyli' contract if
        // the identity is not the TLD itself for
        // DeleteContractAction, UpdateContractProgramIdAction and UpdateContractTimeoutWindowAction actions
        if let Err(validation_error) = validate_hyli_contract_blobs(&self.contracts, tx) {
            bail!(
                "Blob Transaction contains invalid blobs for 'hyli' contract: {}",
                validation_error
            );
        }

        // For now, reject blob Tx if the first contract is unknown
        if !self.contracts.contains_key(&tx.blobs[0].contract_name) {
            bail!(
                "Blob Transaction's first contract {} is unknown",
                tx.blobs[0].contract_name
            );
        }

        // If we're behind other pending transactions, we can't settle yet.
        let Some(should_try_and_settle) =
            self.unsettled_transactions.add(UnsettledBlobTransaction {
                tx: tx.clone(),
                tx_id: TxId(parent_dp_hash.clone(), tx_hash.clone()),
                tx_context,
                blobs_hash,
                possible_proofs: BTreeMap::from_iter(
                    tx.blobs
                        .iter()
                        .enumerate()
                        .map(|(i, _)| (BlobIndex(i), vec![])),
                ),
                settleable_contracts: vec![],
            })
        else {
            return Ok(BlobTxHandled::Duplicate);
        };

        if self.unsettled_transactions.is_next_to_settle(&blob_tx_hash) {
            self.set_timeout(&blob_tx_hash);
        }

        if should_try_and_settle {
            Ok(BlobTxHandled::ShouldSettle(tx_hash))
        } else {
            Ok(BlobTxHandled::Ok)
        }
    }

    fn handle_blob_proof(
        &mut self,
        proof_tx_id: &TxId,
        blob_proof_data: &BlobProofOutput,
    ) -> Result<Option<TxHash>, Error> {
        let blob_tx_hash = blob_proof_data.blob_tx_hash.clone();
        // Find the blob being proven and whether we should try to settle the TX.
        let Some((unsettled_tx, should_settle_tx)) = self
            .this
            .store
            .unsettled_transactions
            .get_for_settlement(&blob_tx_hash)
        else {
            bail!("BlobTx {} not found", &blob_tx_hash);
        };

        // TODO: add diverse verifications ? (without the initial state checks!).
        // TODO: success to false is valid outcome and can be settled.

        Self::verify_hyli_output(unsettled_tx, &blob_proof_data.hyli_output)?;

        // If we arrived here, HyliOutput provided is OK and can now be saved
        debug!(
            "Saving a hyli_output for BlobTx {} index {}",
            blob_proof_data.hyli_output.tx_hash.0, blob_proof_data.hyli_output.index
        );

        let (Some(blob), Some(possible_proofs)) = (
            unsettled_tx
                .tx
                .blobs
                .get(blob_proof_data.hyli_output.index.0),
            unsettled_tx
                .possible_proofs
                .get_mut(&blob_proof_data.hyli_output.index),
        ) else {
            bail!(
                "blob at index {} not found in blob TX {}",
                blob_proof_data.hyli_output.index.0,
                &blob_tx_hash
            );
        };

        let blob_proof_output = (
            blob_proof_data.program_id.clone(),
            blob_proof_data.verifier.clone(),
            proof_tx_id.clone(),
            blob_proof_data.hyli_output.clone(),
        );

        self.callback.on_event(&TxEvent::NewProof(
            &unsettled_tx.tx_id,
            blob,
            blob_proof_data.hyli_output.index,
            &blob_proof_output,
            possible_proofs.len(),
        ));

        possible_proofs.push(blob_proof_output);

        Ok(match should_settle_tx {
            true => Some(unsettled_tx.tx_id.1.clone()),
            false => None,
        })
    }

    /// Settle all transactions that are ready to settle.
    /// Returns the list of new TXs next to be settled
    fn settle_txs_until_done(
        &mut self,
        mut blob_tx_to_try_and_settle: BTreeSet<TxHash>,
    ) -> BTreeSet<TxHash> {
        let mut unsettlable_txs = BTreeSet::default();
        loop {
            // TODO: investigate most performant order;
            let Some(bth) = blob_tx_to_try_and_settle.pop_first() else {
                break;
            };

            match self.try_to_settle_blob_tx(&bth) {
                Ok(SettledTxOutput {
                    tx: settled_tx,
                    settlement_result,
                }) => {
                    // Settle the TX and add any new TXs to try and settle next.
                    let mut txs = self.on_settled_blob_tx(bth, settled_tx, settlement_result);
                    blob_tx_to_try_and_settle.append(&mut txs)
                }
                Err(e) => {
                    unsettlable_txs.insert(bth.clone());
                    let e = format!("Failed to settle: {e}");
                    debug!(tx_hash = %bth, "{e}");
                    self.callback.on_event(&TxEvent::TxError(
                        // TODO: store TxID not TxHash in btreeset ?
                        &self
                            .unsettled_transactions
                            .get(&bth)
                            .map(|tx| tx.tx_id.clone())
                            .unwrap_or_default(),
                        &e,
                    ));
                }
            }
        }
        unsettlable_txs
    }

    fn try_to_settle_blob_tx(
        &mut self,
        unsettled_tx_hash: &TxHash,
    ) -> Result<SettledTxOutput, Error> {
        trace!("Trying to settle blob tx: {:?}", unsettled_tx_hash);

        let unsettled_tx = self
            .this
            .store
            .unsettled_transactions
            .get_mut(unsettled_tx_hash)
            .ok_or(anyhow::anyhow!(
                "Unsettled transaction not found in the state: {:?}",
                unsettled_tx_hash
            ))?;

        let updated_contracts = BTreeMap::new();

        /*
        Fail fast: try to find a stateless (native verifiers are considered stateless for now) contract
        with a hyli output to success false (in all possible combinations)
        */
        let settlement_result = if unsettled_tx.iter_blobs().any(|(blob, proofs)| {
            NATIVE_VERIFIERS_CONTRACT_LIST.contains(&blob.contract_name.0.as_str())
                && proofs
                    .iter()
                    .any(|possible_proof| !possible_proof.3.success)
        }) {
            let msg = "Settling fast as failed because native blob was failed";
            debug!("{msg}");
            self.callback
                .on_event(&TxEvent::TxError(&unsettled_tx.tx_id, msg));
            SettlementResult {
                settlement_status: SettlementStatus::SettleAsFailed,
                contract_changes: BTreeMap::new(),
                contract_flags: BTreeMap::new(),
                blob_proof_output_indices: vec![],
            }
        } else {
            Self::settle_blobs_recursively(
                unsettled_tx,
                &self.this.store.contracts,
                SettlementStatus::TryingToSettle,
                updated_contracts,
                BTreeMap::new(),
                unsettled_tx.iter_blobs().enumerate(),
                vec![],
                self.callback,
            )
        };

        match settlement_result.settlement_status {
            SettlementStatus::SettleAsSuccess => {
                if !self
                    .unsettled_transactions
                    .is_next_to_settle(unsettled_tx_hash)
                {
                    bail!(
                        "Transaction {} is not next to settle, skipping.",
                        unsettled_tx_hash
                    );
                };
            }
            SettlementStatus::NotReadyToSettle => {
                for (contract_name, flags) in settlement_result.contract_flags.into_iter() {
                    if flags.is_fully_proved && flags.can_settle_independently {
                        // Keep track of contracts that can be settled independently in the transaction. This will be specifically for settings transaction timeout
                        unsettled_tx.settleable_contracts.push(contract_name);
                    }
                }
                bail!("Tx: {} is not ready to settle.", unsettled_tx.tx_id);
            }
            SettlementStatus::SettleAsFailed => {
                // If some blobs are still sequenced behind others, we can only settle this TX as failed.
                // (failed TX won't change the state, so we can settle it right away).
            }
            SettlementStatus::TryingToSettle => {
                unreachable!(
                    "Settlement status should not be Idle when trying to settle a blob tx"
                );
            }
        }

        // We are OK to settle now.

        #[allow(clippy::unwrap_used, reason = "must exist because of above checks")]
        let unsettled_tx = self
            .unsettled_transactions
            .remove(unsettled_tx_hash)
            .unwrap();

        Ok(SettledTxOutput {
            tx: unsettled_tx,
            settlement_result,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn settle_blobs_recursively<'a>(
        unsettled_tx: &UnsettledBlobTransaction,
        contracts: &HashMap<ContractName, Contract>,
        mut settlement_status: SettlementStatus,
        mut contract_changes: BTreeMap<ContractName, ModifiedContractData>,
        mut contract_flags: BTreeMap<ContractName, ContractSettlementFlags>,
        mut blob_iter: impl Iterator<Item = (usize, (&'a Blob, &'a Vec<BlobProof>))> + Clone,
        mut blob_proof_output_indices: Vec<usize>,
        callback: &mut (dyn NodeStateCallback + Send + Sync),
    ) -> SettlementResult {
        // Recursion end-case: we successfully settled all prior blobs, so success.
        let Some((blob_index, (blob, possible_proofs))) = blob_iter.next() else {
            // Sanity checks
            for (contract_name, modified_contract_data) in contract_changes.iter() {
                let contract_status = &modified_contract_data.contract_status;
                tracing::trace!(
                    "sanity check - contract {contract_name:?} is in state {contract_status:?}"
                );
                // Sanity check: a contract state cannot be in RegisterWithConstructor as it would mean the constructor blob has not been sent
                if let ContractStatus::RegisterWithConstructor(_) = contract_status {
                    let msg = format!(
                            "Contract '{contract_name}' is in RegisterWithConstructor state at settlement end; constructor blob missing.",
                        );
                    debug!("{msg}");
                    callback.on_event(&TxEvent::TxError(&unsettled_tx.tx_id, &msg));
                    return SettlementResult {
                        settlement_status: SettlementStatus::SettleAsFailed,
                        contract_changes,
                        contract_flags,
                        blob_proof_output_indices,
                    };
                }
                // Sanity check: a contract state cannot be in WaitingDeletion as it would mean the deletion blob has not been sent
                if let ContractStatus::WaitingDeletion = contract_status {
                    let msg = format!(
                            "Contract '{contract_name}' is in WaitingDeletion state at settlement end; deletion blob missing.",
                        );
                    debug!("{msg}");
                    callback.on_event(&TxEvent::TxError(&unsettled_tx.tx_id, &msg));
                    return SettlementResult {
                        settlement_status: SettlementStatus::SettleAsFailed,
                        contract_changes,
                        contract_flags,
                        blob_proof_output_indices,
                    };
                }
            }
            tracing::trace!("Settlement - Done: {settlement_status:?}");
            if settlement_status == SettlementStatus::TryingToSettle {
                // All blobs have been processed, if settlement status is still idle, this means:
                // - no blobs are proven to be failing
                // - no blobs are proven to be not ready for settlement
                settlement_status = SettlementStatus::SettleAsSuccess;
            }
            return SettlementResult {
                settlement_status,
                contract_changes,
                contract_flags,
                blob_proof_output_indices,
            };
        };

        tracing::trace!(
            "Recursion on blob #{blob_index} (contract: {:?})",
            blob.contract_name
        );

        let contract_name = &blob.contract_name;

        // Need a placeholder for executed blobs, and otherwise we do use 0 anyways.
        blob_proof_output_indices.push(0);

        // Execute blob that needs onchain execution
        match Self::process_blob_on_chain_execution(
            unsettled_tx,
            contracts,
            &mut contract_changes,
            blob,
            &settlement_status,
        ) {
            BlobProcessingResult::NotApplicable => {
                // This isn't a blob that needs onchain execution. Continue with normal processing
            }
            BlobProcessingResult::Success => {
                tracing::trace!("OnChainExecution Settlement - OK");
                // We detect if the current blob is the last blob of the contract in this transaction.
                // If yes we can consider this contract as settlable independently of other blobs.
                if Self::is_last_blob_of_contract_in_tx(unsettled_tx, blob_index) {
                    contract_flags
                        .entry(contract_name.clone())
                        .or_default()
                        .is_fully_proved = true;
                }

                return Self::settle_blobs_recursively(
                    unsettled_tx,
                    contracts,
                    settlement_status.clone(),
                    contract_changes,
                    contract_flags,
                    blob_iter.clone(),
                    blob_proof_output_indices.clone(),
                    callback,
                );
            }
            BlobProcessingResult::ProvenFailure(msg) => {
                // Fatal error - settle as failed immediately
                let msg = format!("On-chain execution failed for blob {blob_index}: {msg}");
                debug!("{msg}");
                callback.on_event(&TxEvent::TxError(&unsettled_tx.tx_id, &msg));
                return SettlementResult {
                    settlement_status: SettlementStatus::SettleAsFailed,
                    contract_changes,
                    contract_flags,
                    blob_proof_output_indices,
                };
            }
        };

        // Regular case: go through each proof for this blob. If they settle, carry on recursively.
        for (i, proof_metadata) in possible_proofs.iter().enumerate() {
            #[allow(clippy::unwrap_used, reason = "pushed above so last must exist")]
            let proof_index = blob_proof_output_indices.last_mut().unwrap();
            *proof_index = i;

            // TODO: ideally make this CoW
            let mut current_contracts = contract_changes.clone();
            let mut current_flags = contract_flags.clone();

            tracing::trace!(
                "Processing proof #{} for blob index {} (contract {})",
                i,
                blob_index,
                contract_name
            );

            let proof_result = Self::process_proof(
                contracts,
                &mut current_contracts,
                &mut current_flags,
                contract_name,
                proof_metadata,
            );

            match proof_result {
                ProofProcessingResult::Success => {
                    // We detect if the current blob is the last blob of the contract in this transaction.
                    // If yes we can consider this contract as settlable independently of other blobs.
                    if Self::is_last_blob_of_contract_in_tx(unsettled_tx, blob_index) {
                        current_flags
                            .entry(contract_name.clone())
                            .or_default()
                            .is_fully_proved = true;
                    }

                    tracing::trace!("Settlement - OK blob");
                    let settlement_result = Self::settle_blobs_recursively(
                        unsettled_tx,
                        contracts,
                        settlement_status.clone(),
                        current_contracts,
                        current_flags,
                        blob_iter.clone(),
                        blob_proof_output_indices.clone(),
                        callback,
                    );
                    // If this proof settles, early return, otherwise try the next one (with continue for explicitness)
                    match settlement_result.settlement_status {
                        SettlementStatus::SettleAsSuccess | SettlementStatus::SettleAsFailed => {
                            return settlement_result;
                        }
                        SettlementStatus::NotReadyToSettle | SettlementStatus::TryingToSettle => {
                            Self::merge_contract_flags(
                                &mut contract_flags,
                                settlement_result.contract_flags,
                            );
                            continue;
                        }
                    }
                }
                ProofProcessingResult::Invalid(msg) => {
                    // Not a valid proof, log it and try the next one.
                    let msg = format!(
                        "Could not settle blob proof output #{i} on blob {blob_index} for contract '{contract_name}': {msg}"
                    );
                    debug!("{msg}");
                    callback.on_event(&TxEvent::TxError(&unsettled_tx.tx_id, &msg));
                    continue;
                }
                ProofProcessingResult::ProvenFailure(msg) => {
                    // Fatal error - settle as failed immediately
                    let msg = format!(
                        "Fatal error processing blob proof output #{i} on blob {blob_index} for contract '{contract_name}': {msg}"
                    );
                    debug!("{msg}");
                    callback.on_event(&TxEvent::TxError(&unsettled_tx.tx_id, &msg));
                    return SettlementResult {
                        settlement_status: SettlementStatus::SettleAsFailed,
                        contract_changes,
                        contract_flags,
                        blob_proof_output_indices,
                    };
                }
            }
        }

        // If we end up here we didn't manage to settle the current blob
        // We update the status of the contract in contract_changes; so that we can move on in recursion to find valid failing blobs.
        contract_changes
            .entry(contract_name.clone())
            .and_modify(|modified_contract_data| {
                modified_contract_data.contract_status = ContractStatus::UnknownState;
            })
            .or_insert(ModifiedContractData {
                contract_status: ContractStatus::UnknownState,
                modified_fields: ModifiedContractFields::all(),
                side_effects: vec![],
            });
        contract_flags
            .entry(contract_name.clone())
            .or_insert_with(|| ContractSettlementFlags {
                can_settle_independently: true,
                ..ContractSettlementFlags::default()
            });

        let remaining_settlement = Self::settle_blobs_recursively(
            unsettled_tx,
            contracts,
            SettlementStatus::NotReadyToSettle,
            contract_changes.clone(),
            contract_flags.clone(),
            blob_iter,
            blob_proof_output_indices.clone(),
            callback,
        );

        tracing::trace!(
            "Finalizing recursion on blob #{blob_index} (contract: {:?})",
            blob.contract_name
        );

        // If we found a failure in remaining blobs, return it
        if remaining_settlement.settlement_status == SettlementStatus::SettleAsFailed {
            return remaining_settlement;
        }

        Self::merge_contract_flags(&mut contract_flags, remaining_settlement.contract_flags);

        // If we end up here, the TX isn't ready yet.
        SettlementResult {
            settlement_status: remaining_settlement.settlement_status,
            contract_changes,
            contract_flags,
            blob_proof_output_indices,
        }
    }

    /// Handle a settled blob transaction.
    /// Handles the multiple side-effects of settling.
    /// This returns the list of new TXs to try and settle next,
    /// i.e. the "next" TXs for each contract.
    fn on_settled_blob_tx(
        &mut self,
        bth: TxHash,
        settled_tx: UnsettledBlobTransaction,
        settlement_result: SettlementResult,
    ) -> BTreeSet<TxHash> {
        // Transaction was settled, update our state.

        // Note all the TXs that we might want to try and settle next
        let mut next_txs_to_try_and_settle = self
            .unsettled_transactions
            .get_next_txs_blocked_by_tx(&settled_tx);

        match settlement_result.settlement_status {
            SettlementStatus::SettleAsFailed => {
                // If it's a failed settlement, mark it so and move on.
                self.callback
                    .on_event(&TxEvent::SettledAsFailed(&settled_tx.tx_id, &settled_tx));

                self.this.metrics.add_failed_transactions(1);
                info!("⛈️ Settled tx {} as failed", &bth);

                return next_txs_to_try_and_settle;
            }
            SettlementStatus::NotReadyToSettle | SettlementStatus::TryingToSettle => {
                unreachable!(
                        "Settlement status should not be NotReadyToSettle nor TryingToSettle when trying to settle a blob tx"
                    );
            }
            SettlementStatus::SettleAsSuccess => {
                // We can move on to settle the TX
            }
        }

        // Otherwise process the side effects.

        // Go through each blob and:
        // - keep track of which blob proof output we used to settle the TX for each blob.
        // - take note of staking actions
        for (blob_index, (blob, possible_proofs)) in settled_tx.iter_blobs().enumerate() {
            let proof_index = settlement_result
                .blob_proof_output_indices
                .get(blob_index)
                .cloned()
                .unwrap();
            self.callback.on_event(&TxEvent::BlobSettled(
                &settled_tx.tx_id,
                &settled_tx,
                blob,
                BlobIndex(blob_index),
                possible_proofs.get(proof_index),
                proof_index,
            ));
        }

        // Update contract states
        for (
            contract_name,
            ModifiedContractData {
                contract_status,
                modified_fields,
                side_effects,
                ..
            },
        ) in settlement_result.contract_changes.into_iter()
        {
            match contract_status {
                ContractStatus::UnknownState => {
                    unreachable!(
                        "Contract status should not be UnknownState when trying to settle a blob tx"
                    );
                }
                ContractStatus::RegisterWithConstructor(..) => {
                    unreachable!(
                        "Contract status should not be CreatedWithMetadata when trying to settle a blob tx"
                    );
                }
                ContractStatus::WaitingDeletion => {
                    unreachable!(
                        "Contract status should not be WaitingDeletion when trying to settle a blob tx"
                    );
                }
                ContractStatus::Deleted => {
                    debug!("✏️ Delete {} contract", contract_name);
                    self.contracts.remove(&contract_name);

                    let mut potentially_blocked_contracts = HashSet::new();

                    // Time-out all transactions for this contract
                    while let Some(tx_hash) = self
                        .unsettled_transactions
                        .get_next_unsettled_tx(&contract_name)
                        .cloned()
                    {
                        if let Some(popped_tx) = self.unsettled_transactions.remove(&tx_hash) {
                            info!("⏳ Timeout tx {} (from contract deletion)", &tx_hash);

                            potentially_blocked_contracts
                                .extend(OrderedTxMap::get_contracts_blocked_by_tx(&popped_tx));
                            self.callback
                                .on_event(&TxEvent::TimedOut(&popped_tx.tx_id, &popped_tx));
                        }
                    }

                    for contract in potentially_blocked_contracts {
                        if let Some(tx_hash) =
                            self.unsettled_transactions.get_next_unsettled_tx(&contract)
                        {
                            next_txs_to_try_and_settle.insert(tx_hash.clone());
                        }
                    }

                    self.callback
                        .on_event(&TxEvent::ContractDeleted(&settled_tx.tx_id, &contract_name));
                    continue;
                }
                ContractStatus::Updated(contract) => {
                    // Otherwise, apply any side effect and potentially note it in the map of registered contracts.
                    if !self.contracts.contains_key(&contract_name) {
                        info!("📝 Registering contract {}", contract_name);

                        // Let's find the metadata - for now it's unsupported to register the same contract twice in a single TX.
                        let metadata = side_effects.into_iter().find_map(|se| {
                            if let SideEffect::Register(m) = se {
                                Some(m)
                            } else {
                                None
                            }
                        });
                        if metadata.is_none() {
                            tracing::warn!(
                                "No register effect found for contract {} in TX {}",
                                contract_name,
                                bth
                            );
                        }
                        self.callback.on_event(&TxEvent::ContractRegistered(
                            &settled_tx.tx_id,
                            &contract_name,
                            &contract,
                            &metadata.unwrap_or_default(),
                        ));
                    }

                    self.contracts
                        .insert(contract.name.clone(), contract.clone());

                    if modified_fields.state {
                        debug!(
                            "✍️  Modify '{}' state to {}",
                            &contract_name,
                            hex::encode(&contract.state.0)
                        );

                        self.callback.on_event(&TxEvent::ContractStateUpdated(
                            &settled_tx.tx_id,
                            &contract_name,
                            &contract,
                            &contract.state,
                        ));
                    }
                    if modified_fields.program_id {
                        debug!(
                            "✍️  Modify '{}' program_id to {}",
                            &contract_name,
                            hex::encode(&contract.program_id.0)
                        );

                        self.callback.on_event(&TxEvent::ContractProgramIdUpdated(
                            &settled_tx.tx_id,
                            &contract_name,
                            &contract,
                            &contract.program_id,
                        ));
                    }
                    if modified_fields.timeout_window {
                        debug!(
                            "✍️  Modify '{}' timeout window to {}",
                            &contract_name, &contract.timeout_window
                        );

                        self.callback
                            .on_event(&TxEvent::ContractTimeoutWindowUpdated(
                                &settled_tx.tx_id,
                                &contract_name,
                                &contract,
                                &contract.timeout_window,
                            ));
                    }
                }
            }
        }

        self.callback
            .on_event(&TxEvent::Settled(&settled_tx.tx_id, &settled_tx));
        self.this.metrics.add_settled_transactions(1);
        self.this.metrics.add_successful_transactions(1);
        info!("✨ Settled tx {}", &bth);

        next_txs_to_try_and_settle
    }

    // Called when processing a verified proof TX - checks the proof is potentially valid for settlement.
    // This is an "internally coherent" check - you can't rely on any node_state data as
    // the state might be different when settling.
    fn verify_hyli_output(
        unsettled_tx: &UnsettledBlobTransaction,
        hyli_output: &HyliOutput,
    ) -> Result<(), Error> {
        // Identity verification
        if unsettled_tx.tx.identity != hyli_output.identity {
            bail!(
                "Proof identity '{}' does not correspond to BlobTx identity '{}'.",
                hyli_output.identity,
                unsettled_tx.tx.identity
            )
        }

        // Verify Tx hash matches
        if hyli_output.tx_hash != unsettled_tx.tx_id.1 {
            bail!(
                "Proof tx_hash '{}' does not correspond to BlobTx hash '{}'.",
                hyli_output.tx_hash,
                unsettled_tx.tx_id.1
            )
        }

        if let Some(tx_ctx) = &hyli_output.tx_ctx {
            if *tx_ctx != *unsettled_tx.tx_context {
                bail!(
                    "Proof tx_context '{:?}' does not correspond to BlobTx tx_context '{:?}'.",
                    tx_ctx,
                    unsettled_tx.tx_context
                )
            }
        }

        // blob_hash verification
        let extracted_blobs_hash = (&hyli_output.blobs).into();
        if !unsettled_tx.blobs_hash.includes_all(&extracted_blobs_hash) {
            bail!(
                "Proof blobs hash '{}' do not correspond to BlobTx blobs hash '{}'.",
                extracted_blobs_hash,
                unsettled_tx.blobs_hash
            )
        }

        Ok(())
    }

    // Helper for process_proof
    pub(self) fn get_contract<'a>(
        contracts: &'a HashMap<ContractName, Contract>,
        contract_changes: &'a BTreeMap<ContractName, ModifiedContractData>,
        contract_name: &ContractName,
    ) -> Result<&'a Contract, Error> {
        let contract = contract_changes
            .get(contract_name)
            .and_then(
                |ModifiedContractData {
                     contract_status, ..
                 }| match contract_status {
                    ContractStatus::Updated(contract) => Some(contract),
                    ContractStatus::RegisterWithConstructor(contract) => Some(contract),
                    ContractStatus::Deleted
                    | ContractStatus::WaitingDeletion
                    | ContractStatus::UnknownState => None,
                },
            )
            .or_else(|| contracts.get(contract_name))
            .ok_or_else(|| {
                Error::msg(format!(
                    "Cannot settle blob, contract '{contract_name}' does not exist"
                ))
            })?;
        Ok(contract)
    }

    // Called when trying to execute on-chain a blob
    fn process_blob_on_chain_execution(
        unsettled_tx: &UnsettledBlobTransaction,
        contracts: &HashMap<ContractName, Contract>,
        contract_changes: &mut BTreeMap<ContractName, ModifiedContractData>,
        blob: &Blob,
        settlement_status: &SettlementStatus,
    ) -> BlobProcessingResult {
        let contract_name = &blob.contract_name;

        // Handle native verifiers
        if let Some(contract) = contracts.get(contract_name) {
            if let Ok(verifier) = TryInto::<NativeVerifiers>::try_into(&contract.verifier) {
                tracing::trace!(
                    "Processing native verifier blob for contract {}",
                    contract_name
                );

                let (identity, success) = match verify_native_impl(blob, &verifier) {
                    Ok(v) => v,
                    Err(e) => {
                        return BlobProcessingResult::ProvenFailure(format!(
                            "Native blob verification failed: {e:?}"
                        ));
                    }
                };

                // Identity verification
                if unsettled_tx.tx.identity != identity {
                    return BlobProcessingResult::ProvenFailure(format!(
                        "NativeVerifier identity '{}' does not correspond to BlobTx identity '{}'.",
                        identity, unsettled_tx.tx.identity,
                    ));
                }

                if !success {
                    return BlobProcessingResult::ProvenFailure(
                        "Native verifier execution failed".to_string(),
                    );
                }

                tracing::trace!("NativeVerifier Settlement - OK blob");
                // Native verifiers don't change state, so we return success without updating contract_changes
                return BlobProcessingResult::Success;
            }
        }

        // Handle special contract operations for the "hyli" contract
        // We need to check the current state of 'current_contracts' to check validity,
        // so we really can't do this before we've settled the earlier blobs.
        if contract_name.0 == "hyli" {
            tracing::trace!("Settlement - processing for Hyli");
            return match handle_blob_for_hyli_tld(contracts, contract_changes, blob) {
                Ok(()) => BlobProcessingResult::Success,
                Err(err) => {
                    // We have a valid proof of failure, we short-circuit.
                    BlobProcessingResult::ProvenFailure(format!(
                        "Could not settle blob proof output for 'hyli': {err:?}"
                    ))
                }
            };
        }

        if let Some(ModifiedContractData {
            contract_status: current_status,
            modified_fields: _,
            side_effects: se,
            ..
        }) = contract_changes.get_mut(contract_name)
        {
            // Special case for contract registration
            // Two case scenario:
            // 1. The contract is created with metadata, so we expect the next blob to be a constructor blob.
            // 2. The contract is created without metadata, so we expect the next blob to be a regular blob.
            // Here we handle case 1.
            if let ContractStatus::RegisterWithConstructor(created_contract) = current_status {
                // current_blob is considered as constructor blob. It does not need to be proven.
                *current_status = ContractStatus::Updated(created_contract.clone());
                se.push(SideEffect::Register(Some(blob.data.0.clone())));

                tracing::trace!("Registration Settlement - OK blob");
                return BlobProcessingResult::Success;
            }
            // Special case for contract deletion from TLD
            if current_status == &mut ContractStatus::WaitingDeletion {
                if !blob.data.0.is_empty() {
                    // Non-empty blob is not a valid deletion
                    return BlobProcessingResult::ProvenFailure(format!("Trying to settle a blob for the deleted contract {contract_name:?} with non-empty data."));
                }

                *current_status = ContractStatus::Deleted;
                se.push(SideEffect::Delete);

                tracing::trace!("Deletion Settlement - OK blob");
                return BlobProcessingResult::Success;
            }
            // Special case for contract deletion
            if current_status == &mut ContractStatus::Deleted {
                return BlobProcessingResult::ProvenFailure(format!(
                    "Trying to settle a blob for a deleted contract {contract_name:?}"
                ));
            }
        } else if !contracts.contains_key(contract_name) {
            // Now processing a blob for a contract that is not registered yet
            // if all previous blobs have been proven (i.e. setttlement status still at TryingToSettle)
            // and none of them generate an OnChainEffect, Tx should fail
            if settlement_status == &SettlementStatus::TryingToSettle {
                return BlobProcessingResult::ProvenFailure(format!("Trying to settle a blob for an unknown and unregistered contract {contract_name:?}"));
            }
        }

        // Default case: no special processing needed
        BlobProcessingResult::NotApplicable
    }

    // Called when trying to actually settle a blob TX - processes a proof for settlement.
    // verify_hyli_output has already been called at this point.
    // Not called for the Hyli TLD.
    fn process_proof(
        contracts: &HashMap<ContractName, Contract>,
        contract_changes: &mut BTreeMap<ContractName, ModifiedContractData>,
        contract_flags: &mut BTreeMap<ContractName, ContractSettlementFlags>,
        contract_name: &ContractName,
        proof_metadata: &(ProgramId, Verifier, TxId, HyliOutput),
    ) -> ProofProcessingResult {
        if let Err(e) = validate_state_commitment_size(&proof_metadata.3.next_state) {
            return ProofProcessingResult::Invalid(e.to_string());
        }

        let contract = match Self::get_contract(contracts, contract_changes, contract_name) {
            Ok(contract) => contract.clone(),
            Err(e) => return ProofProcessingResult::Invalid(e.to_string()),
        };

        tracing::trace!("Contract {} with state {:?}", contract.name, contract.state);

        if proof_metadata.3.initial_state != contract.state {
            return ProofProcessingResult::Invalid(format!(
                "Initial state mismatch: {:?}, expected {:?}",
                proof_metadata.3.initial_state, contract.state
            ));
        }

        if proof_metadata.0 != contract.program_id {
            return ProofProcessingResult::Invalid(format!(
                "Program ID mismatch: {:?}, expected {:?} on {}",
                proof_metadata.0, contract.program_id, contract.name
            ));
        }

        if proof_metadata.1 != contract.verifier {
            return ProofProcessingResult::Invalid(format!(
                "Verifier mismatch: {:?}, expected {:?} on {}",
                proof_metadata.2, contract.verifier, contract.name
            ));
        }

        // Proof processed successfully, continue with recursion
        if !proof_metadata.3.success {
            return ProofProcessingResult::ProvenFailure(format!(
                "Execution failed: {:?}",
                String::from_utf8(proof_metadata.3.program_outputs.clone())
            ));
        }

        for state_read in &proof_metadata.3.state_reads {
            let other_contract =
                match Self::get_contract(contracts, contract_changes, &state_read.0) {
                    Ok(contract) => contract,
                    Err(e) => return ProofProcessingResult::Invalid(e.to_string()),
                };
            if state_read.1 != other_contract.state {
                return ProofProcessingResult::Invalid(format!(
                    "State read {:?} does not match other contract state {:?}",
                    state_read, other_contract.state
                ));
            }
            // If the execution read another contract state, then this contract is not independent anymore
            contract_flags
                .entry(contract_name.clone())
                .or_default()
                .can_settle_independently = false;
            // TODO: investigate if we should flag as not-independent the read contract as well
        }

        for effect in &proof_metadata.3.onchain_effects {
            match effect {
                OnchainEffect::RegisterContractWithConstructor(effect) => {
                    // Validation of contract registration metadata should cause immediate failure
                    if let Err(e) = validate_contract_registration_metadata(
                        &contract.name,
                        &effect.contract_name,
                        &effect.verifier,
                        &effect.program_id,
                        &effect.state_commitment,
                    ) {
                        return ProofProcessingResult::ProvenFailure(format!(
                            "Contract registration validation failed: {e}"
                        ));
                    }

                    contract_changes.insert(
                        effect.contract_name.clone(),
                        ModifiedContractData {
                            contract_status: ContractStatus::RegisterWithConstructor(Contract {
                                name: effect.contract_name.clone(),
                                program_id: effect.program_id.clone(),
                                state: effect.state_commitment.clone(),
                                verifier: effect.verifier.clone(),
                                timeout_window: effect
                                    .timeout_window
                                    .clone()
                                    .unwrap_or(contract.timeout_window.clone()),
                            }),
                            modified_fields: ModifiedContractFields::all(),
                            side_effects: vec![],
                        },
                    );
                    contract_flags
                        .entry(effect.contract_name.clone())
                        .or_default()
                        .can_settle_independently = false;
                }
                OnchainEffect::RegisterContract(effect) => {
                    // Validation of contract registration metadata should cause immediate failure
                    if let Err(e) = validate_contract_registration_metadata(
                        &contract.name,
                        &effect.contract_name,
                        &effect.verifier,
                        &effect.program_id,
                        &effect.state_commitment,
                    ) {
                        return ProofProcessingResult::ProvenFailure(format!(
                            "Contract registration validation failed: {e}"
                        ));
                    }

                    contract_changes.insert(
                        effect.contract_name.clone(),
                        ModifiedContractData {
                            contract_status: ContractStatus::Updated(Contract {
                                name: effect.contract_name.clone(),
                                program_id: effect.program_id.clone(),
                                state: effect.state_commitment.clone(),
                                verifier: effect.verifier.clone(),
                                timeout_window: effect
                                    .timeout_window
                                    .clone()
                                    .unwrap_or(contract.timeout_window.clone()),
                            }),
                            modified_fields: ModifiedContractFields::all(),
                            side_effects: vec![SideEffect::Register(None)],
                        },
                    );
                    contract_flags
                        .entry(effect.contract_name.clone())
                        .or_default()
                        .can_settle_independently = false;
                }
                OnchainEffect::DeleteContract(cn) => {
                    // Contract name validation for deletion should also cause immediate failure
                    if let Err(e) = validate_contract_name_tld(&contract.name, cn) {
                        return ProofProcessingResult::ProvenFailure(format!(
                            "Contract deletion validation failed: {e}"
                        ));
                    }
                    contract_changes
                        .entry(cn.clone())
                        .and_modify(|mcd| {
                            mcd.contract_status = ContractStatus::WaitingDeletion;
                            mcd.side_effects.push(SideEffect::Delete);
                        })
                        .or_insert_with(|| ModifiedContractData {
                            contract_status: ContractStatus::WaitingDeletion,
                            modified_fields: ModifiedContractFields::all(),
                            side_effects: vec![SideEffect::Delete],
                        });
                    contract_flags
                        .entry(cn.clone())
                        .or_default()
                        .can_settle_independently = false;
                }
                OnchainEffect::UpdateContractProgramId(cn, program_id) => {
                    // Only hyli and the contract itself can update its programId
                    if contract_name != &"hyli".into() && cn != contract_name {
                        return ProofProcessingResult::ProvenFailure(format!(
                            "Forbidden programId update: contract {contract_name} trying to upgrade {cn}"
                        ));
                    }
                    contract_changes
                        .entry(cn.clone())
                        .and_modify(|mcd| {
                            mcd.contract_status = ContractStatus::Updated(Contract {
                                program_id: program_id.clone(),
                                ..contract.clone()
                            });
                            mcd.modified_fields.program_id = true;
                            mcd.side_effects.push(SideEffect::UpdateProgramId);
                        })
                        .or_insert_with(|| ModifiedContractData {
                            contract_status: ContractStatus::Updated(Contract {
                                program_id: program_id.clone(),
                                ..contract.clone()
                            }),
                            modified_fields: ModifiedContractFields {
                                program_id: true,
                                ..ModifiedContractFields::default()
                            },
                            side_effects: vec![SideEffect::UpdateProgramId],
                        });
                    contract_flags
                        .entry(cn.clone())
                        .or_default()
                        .can_settle_independently = false;
                }
                OnchainEffect::UpdateTimeoutWindow(cn, timeout_window) => {
                    // Only hyli and the contract itself can update its TimeoutWindow
                    if contract_name != &"hyli".into() && cn != contract_name {
                        return ProofProcessingResult::ProvenFailure(format!(
                            "Forbidden TimeoutWindow update: contract {contract_name} trying to upgrade {cn}"
                        ));
                    }
                    contract_changes
                        .entry(cn.clone())
                        .and_modify(|mcd| {
                            mcd.contract_status = ContractStatus::Updated(Contract {
                                timeout_window: timeout_window.clone(),
                                ..contract.clone()
                            });
                            mcd.modified_fields.timeout_window = true;
                            mcd.side_effects.push(SideEffect::UpdateTimeoutWindow);
                        })
                        .or_insert_with(|| ModifiedContractData {
                            contract_status: ContractStatus::Updated(Contract {
                                timeout_window: timeout_window.clone(),
                                ..contract.clone()
                            }),
                            modified_fields: ModifiedContractFields {
                                timeout_window: true,
                                ..ModifiedContractFields::default()
                            },
                            side_effects: vec![SideEffect::UpdateTimeoutWindow],
                        });
                    contract_flags
                        .entry(cn.clone())
                        .or_default()
                        .can_settle_independently = false;
                }
            }
        }

        // Apply the generic state updates
        let contract_name = contract.name.clone();
        contract_changes
            .entry(contract_name.clone())
            .and_modify(|modified_contract_data| {
                if let ContractStatus::Updated(ref mut c) = modified_contract_data.contract_status {
                    c.state = proof_metadata.3.next_state.clone();
                    modified_contract_data.modified_fields.state = true;
                    modified_contract_data
                        .side_effects
                        .push(SideEffect::UpdateState);
                }
            })
            .or_insert_with(|| ModifiedContractData {
                contract_status: ContractStatus::Updated(Contract {
                    state: proof_metadata.3.next_state.clone(),
                    ..contract
                }),
                modified_fields: ModifiedContractFields {
                    state: true,
                    ..ModifiedContractFields::default()
                },
                side_effects: vec![SideEffect::UpdateState],
            });
        contract_flags
            .entry(contract_name)
            .or_insert_with(|| ContractSettlementFlags {
                can_settle_independently: true,
                ..ContractSettlementFlags::default()
            });

        ProofProcessingResult::Success
    }

    fn merge_contract_flags(
        target: &mut BTreeMap<ContractName, ContractSettlementFlags>,
        source: BTreeMap<ContractName, ContractSettlementFlags>,
    ) {
        for (contract_name, flags) in source {
            let entry = target.entry(contract_name).or_default();
            entry.is_fully_proved = flags.is_fully_proved;
            entry.can_settle_independently = flags.can_settle_independently;
        }
    }

    fn is_last_blob_of_contract_in_tx(
        unsettled_tx: &UnsettledBlobTransaction,
        blob_index: usize,
    ) -> bool {
        // Get the contract name of the blob at the given index
        let Some(current_blob) = unsettled_tx.tx.blobs.get(blob_index) else {
            return false;
        };
        let current_contract_name = &current_blob.contract_name;

        // Check if there are any subsequent blobs for the same contract using reverse iteration
        !unsettled_tx
            .tx
            .blobs
            .iter()
            .skip(blob_index + 1)
            .any(|blob| blob.contract_name == *current_contract_name)
    }

    /// Clear timeouts for transactions that have timed out.
    /// This happens in four steps:
    ///    1. Retrieve the transactions that have timed out
    ///    2. For each contract involved in these transactions, retrieve the next transaction to settle
    ///    3. Try to settle_until_done all descendant transactions
    ///    4. Among the remaining descendants, set a timeout for them
    fn clear_timeouts(&mut self) {
        let mut txs_at_timeout = self
            .this
            .store
            .timeouts
            .drop(&self.this.store.current_height);
        txs_at_timeout.retain(|tx| {
            if let Some(tx) = self.unsettled_transactions.remove(tx) {
                info!("⏰ Blob tx timed out: {}", &tx.tx_id);
                self.this.metrics.add_triggered_timeouts();
                self.callback.on_event(&TxEvent::TimedOut(&tx.tx_id, &tx));

                // Attempt to settle following transactions
                let blob_tx_to_try_and_settle: BTreeSet<TxHash> =
                    self.unsettled_transactions.get_next_txs_blocked_by_tx(&tx);

                // Then try to settle transactions when we can.
                let next_unsettled_txs = self.settle_txs_until_done(blob_tx_to_try_and_settle);

                // For each transaction that could not be settled, if it is the next one to be settled, reset its timeout
                for unsettled_tx in next_unsettled_txs {
                    if self.unsettled_transactions.is_next_to_settle(&unsettled_tx) {
                        self.set_timeout(&unsettled_tx);
                    }
                }

                true
            } else {
                false
            }
        });
    }

    fn set_timeout(&mut self, unsettled_tx_hash: &TxHash) {
        #[allow(clippy::unwrap_used, reason = "must exist because of above checks")]
        let unsettled_tx = self.unsettled_transactions.get(unsettled_tx_hash).unwrap();

        // Convert settleable contracts to a HashSet for efficient lookup
        let settleable_contracts = unsettled_tx
            .settleable_contracts
            .iter()
            .cloned()
            .collect::<HashSet<ContractName>>();

        let min_timeout = unsettled_tx
            .tx
            .blobs
            .iter()
            .filter_map(|blob| {
                let contract_name = &blob.contract_name;
                let contract = self.contracts.get(contract_name)?;

                let TimeoutWindow::Timeout {
                    hard_timeout,
                    soft_timeout,
                } = contract.timeout_window
                else {
                    // If the timeout_window is not a classic timeout, we ignore this contract
                    return None;
                };

                // Use soft_timeout for settleable contracts (can afford to wait longer)
                // Use hard_timeout for non-settleable contracts (cannot afford to wait longer)
                let timeout_value = if settleable_contracts.contains(contract_name) {
                    soft_timeout
                } else {
                    hard_timeout
                };
                Some(timeout_value)
            })
            .min();

        // Set the timeout if we found a valid timeout value
        if let Some(timeout_value) = min_timeout {
            let current_height = self.current_height;
            self.timeouts
                .set(unsettled_tx_hash.clone(), current_height, timeout_value);
        }
    }
}

#[derive(Default)]
pub struct BlockNodeStateCallback {
    block_under_construction: Block,
    staking_data: BlockStakingData,
    stateful_events: StatefulEvents,
}

impl BlockNodeStateCallback {
    pub fn from_signed(signed_block: &SignedBlock) -> Self {
        BlockNodeStateCallback {
            block_under_construction: Block {
                parent_hash: signed_block.parent_hash().clone(),
                hash: signed_block.hashed(),
                block_height: signed_block.height(),
                block_timestamp: signed_block.consensus_proposal.timestamp.clone(),
                ..Default::default()
            },
            staking_data: BlockStakingData {
                new_bounded_validators: signed_block
                    .consensus_proposal
                    .staking_actions
                    .iter()
                    .filter_map(|v| match v {
                        ConsensusStakingAction::Bond { candidate } => {
                            Some(candidate.signature.validator.clone())
                        }
                        _ => None,
                    })
                    .collect(),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    pub fn take(&mut self) -> (Block, BlockStakingData, StatefulEvents) {
        (
            std::mem::take(&mut self.block_under_construction),
            std::mem::take(&mut self.staking_data),
            std::mem::take(&mut self.stateful_events),
        )
    }
}

impl NodeStateCallback for BlockNodeStateCallback {
    fn on_event(&mut self, event: &TxEvent) {
        match *event {
            TxEvent::RejectedBlobTransaction(tx_id, ..) => {
                self.block_under_construction
                    .failed_txs
                    .push(tx_id.1.clone());
                self.block_under_construction
                    .dp_parent_hashes
                    .insert(tx_id.1.clone(), tx_id.0.clone());
            }
            TxEvent::DuplicateBlobTransaction(tx_id) => {
                self.block_under_construction
                    .dropped_duplicate_txs
                    .push(tx_id.clone());
                self.block_under_construction
                    .dp_parent_hashes
                    .insert(tx_id.1.clone(), tx_id.0.clone());
            }
            TxEvent::SequencedBlobTransaction(tx_id, lane_id, _, blob_tx, tx_context) => {
                self.block_under_construction
                    .txs
                    .push((tx_id.clone(), blob_tx.clone().into()));
                self.block_under_construction
                    .dp_parent_hashes
                    .insert(tx_id.1.clone(), tx_id.0.clone());
                self.block_under_construction
                    .lane_ids
                    .insert(tx_id.1.clone(), lane_id.clone());
                self.stateful_events.events.push((
                    tx_id.clone(),
                    StatefulEvent::SequencedTx(blob_tx.clone(), tx_context.clone()),
                ));
            }
            TxEvent::SequencedProofTransaction(tx_id, lane_id, _, proof_tx) => {
                self.block_under_construction
                    .txs
                    .push((tx_id.clone(), proof_tx.clone().into()));
                self.block_under_construction
                    .dp_parent_hashes
                    .insert(tx_id.1.clone(), tx_id.0.clone());
                self.block_under_construction
                    .lane_ids
                    .insert(tx_id.1.clone(), lane_id.clone());
            }
            TxEvent::Settled(tx_id, unsettled_tx) => {
                self.block_under_construction
                    .successful_txs
                    .push(tx_id.1.clone());
                self.block_under_construction
                    .dp_parent_hashes
                    .insert(tx_id.1.clone(), tx_id.0.clone());
                self.block_under_construction
                    .lane_ids
                    .insert(tx_id.1.clone(), unsettled_tx.tx_context.lane_id.clone());
                self.block_under_construction
                    .transactions_events
                    .entry(tx_id.1.clone())
                    .or_default()
                    .push(TransactionStateEvent::Settled);
                self.stateful_events.events.push((
                    tx_id.clone(),
                    StatefulEvent::SettledTx(unsettled_tx.clone()),
                ));
            }
            TxEvent::SettledAsFailed(tx_id, unsettled_tx) => {
                self.block_under_construction
                    .failed_txs
                    .push(tx_id.1.clone());
                self.block_under_construction
                    .dp_parent_hashes
                    .insert(tx_id.1.clone(), tx_id.0.clone());
                self.block_under_construction
                    .transactions_events
                    .entry(tx_id.1.clone())
                    .or_default()
                    .push(TransactionStateEvent::SettledAsFailed);
                self.stateful_events
                    .events
                    .push((tx_id.clone(), StatefulEvent::FailedTx(unsettled_tx.clone())));
            }
            TxEvent::TimedOut(tx_id, unsettled_tx) => {
                self.block_under_construction
                    .timed_out_txs
                    .push(tx_id.1.clone());
                self.block_under_construction
                    .dp_parent_hashes
                    .insert(tx_id.1.clone(), tx_id.0.clone());
                self.block_under_construction
                    .transactions_events
                    .entry(tx_id.1.clone())
                    .or_default()
                    .push(TransactionStateEvent::TimedOut);
                self.stateful_events.events.push((
                    tx_id.clone(),
                    StatefulEvent::TimedOutTx(unsettled_tx.clone()),
                ));
            }
            TxEvent::TxError(tx_id, err) => {
                self.block_under_construction
                    .transactions_events
                    .entry(tx_id.1.clone())
                    .or_default()
                    .push(TransactionStateEvent::Error(err.to_string()));
            }
            TxEvent::NewProof(tx_id, blob, blob_index, proof_data, blob_proof_index) => {
                self.block_under_construction
                    .dp_parent_hashes
                    .insert(tx_id.1.clone(), tx_id.0.clone());
                self.block_under_construction
                    .dp_parent_hashes
                    .insert(proof_data.2 .1.clone(), proof_data.2 .0.clone());
                self.block_under_construction
                    .transactions_events
                    .entry(tx_id.1.clone())
                    .or_default()
                    .push(TransactionStateEvent::NewProof {
                        blob_index,
                        proof_tx_hash: proof_data.2 .1.clone(),
                        program_output: proof_data.3.program_outputs.clone(),
                    });
                self.block_under_construction
                    .blob_proof_outputs
                    .push(HandledBlobProofOutput {
                        proof_tx_hash: proof_data.2 .1.clone(),
                        blob_tx_hash: tx_id.1.clone(),
                        blob_index,
                        contract_name: blob.contract_name.clone(),
                        verifier: proof_data.1.clone(),
                        program_id: proof_data.0.clone(),
                        hyli_output: proof_data.3.clone(),
                        blob_proof_output_index: blob_proof_index,
                    });
            }
            TxEvent::BlobSettled(tx_id, tx, blob, blob_index, _, blob_proof_index) => {
                self.block_under_construction.verified_blobs.push((
                    tx_id.1.clone(),
                    blob_index,
                    Some(blob_proof_index),
                ));
                // Keep track of all stakers
                if blob.contract_name.0 == "staking" {
                    if let Ok(structured_blob) = StructuredBlob::try_from(blob.clone()) {
                        let staking_action: StakingAction = structured_blob.data.parameters;
                        self.staking_data
                            .staking_actions
                            .push((tx.tx.identity.clone(), staking_action));
                    } else {
                        tracing::error!("Failed to parse StakingAction");
                    }
                }
            }
            TxEvent::ContractDeleted(tx_id, contract_name) => {
                self.block_under_construction
                    .registered_contracts
                    .remove(contract_name);
                self.block_under_construction
                    .deleted_contracts
                    .insert(contract_name.clone(), tx_id.1.clone());
            }
            TxEvent::ContractRegistered(tx_id, contract_name, contract, metadata) => {
                self.block_under_construction
                    .deleted_contracts
                    .remove(contract_name);
                self.block_under_construction.registered_contracts.insert(
                    contract_name.clone(),
                    (
                        tx_id.1.clone(),
                        RegisterContractEffect {
                            verifier: contract.verifier.clone(),
                            program_id: contract.program_id.clone(),
                            state_commitment: contract.state.clone(),
                            contract_name: contract_name.clone(),
                            timeout_window: Some(contract.timeout_window.clone()),
                        },
                        metadata.clone(),
                    ),
                );
                self.stateful_events.events.push((
                    tx_id.clone(),
                    StatefulEvent::ContractRegistration(
                        contract_name.clone(),
                        contract.clone(),
                        metadata.clone(),
                    ),
                ));
            }
            TxEvent::ContractStateUpdated(tx_id, contract_name, contract, state_commitment) => {
                self.block_under_construction
                    .updated_states
                    .insert(contract_name.clone(), state_commitment.clone());
                self.stateful_events.events.push((
                    tx_id.clone(),
                    StatefulEvent::ContractUpdate(contract_name.clone(), contract.clone()),
                ));
            }
            TxEvent::ContractProgramIdUpdated(tx_id, contract_name, contract, program_id) => {
                self.block_under_construction
                    .updated_program_ids
                    .insert(contract_name.clone(), program_id.clone());
                self.stateful_events.events.push((
                    tx_id.clone(),
                    StatefulEvent::ContractUpdate(contract_name.clone(), contract.clone()),
                ));
            }
            TxEvent::ContractTimeoutWindowUpdated(
                tx_id,
                contract_name,
                contract,
                timeout_window,
            ) => {
                self.block_under_construction
                    .updated_timeout_windows
                    .insert(contract_name.clone(), timeout_window.clone());
                self.stateful_events.events.push((
                    tx_id.clone(),
                    StatefulEvent::ContractUpdate(contract_name.clone(), contract.clone()),
                ));
            }
        };
    }
}

#[cfg(any(test, feature = "test"))]
#[allow(unused)]
pub mod test {
    mod contract_registration_tests;
    mod node_state_tests;

    use std::ops::Deref;

    use super::*;
    use hyli_net::clock::TimestampMsClock;
    use sdk::verifiers::ShaBlob;
    use sha3::Digest;

    pub(crate) async fn new_node_state() -> NodeState {
        NodeState {
            metrics: NodeStateMetrics::global("test".to_string(), "test"),
            store: NodeStateStore::default(),
        }
    }

    fn new_blob(contract: &str) -> Blob {
        Blob {
            contract_name: ContractName::new(contract),
            data: BlobData(vec![0, 1, 2, 3]),
        }
    }

    fn new_native_blob(contract: &str, identity: Identity) -> Blob {
        let data = vec![0, 1, 2, 3];
        let mut hasher = sha3::Sha3_256::new();
        hasher.update(&data);
        let sha = hasher.finalize().to_vec();

        let data = ShaBlob {
            identity,
            data,
            sha,
        };

        let data = borsh::to_vec(&data).unwrap();

        Blob {
            contract_name: ContractName::new(contract),
            data: BlobData(data),
        }
    }

    fn new_failing_native_blob(contract: &str, identity: Identity) -> Blob {
        let data = vec![0, 1, 2, 3];

        Blob {
            contract_name: ContractName::new(contract),
            data: BlobData(data),
        }
    }

    pub fn make_register_contract_tx(name: ContractName) -> BlobTransaction {
        let register_contract_action = RegisterContractAction {
            verifier: "test".into(),
            program_id: ProgramId(vec![]),
            state_commitment: StateCommitment(vec![0, 1, 2, 3]),
            contract_name: name.clone(),
            constructor_metadata: Some(vec![1]),
            ..Default::default()
        };
        let hyli_blob = register_contract_action.as_blob("hyli".into());

        let register_contract_blob = register_contract_action.as_blob(name);

        BlobTransaction::new("hyli@hyli", vec![hyli_blob, register_contract_blob])
    }
    pub fn make_register_contract_tx_with_actions(
        name: ContractName,
        blobs: Vec<Blob>,
    ) -> BlobTransaction {
        let register_contract_action = RegisterContractAction {
            verifier: "test".into(),
            program_id: ProgramId(vec![]),
            state_commitment: StateCommitment(vec![0, 1, 2, 3]),
            contract_name: name.clone(),
            constructor_metadata: Some(vec![1]),
            ..Default::default()
        };
        let hyli_blob = register_contract_action.as_blob("hyli".into());

        let register_contract_blob = register_contract_action.as_blob(name);
        let list = [vec![hyli_blob, register_contract_blob], blobs].concat();

        BlobTransaction::new("hyli@hyli", list)
    }

    pub fn make_register_contract_effect(contract_name: ContractName) -> RegisterContractEffect {
        RegisterContractEffect {
            verifier: "test".into(),
            program_id: ProgramId(vec![]),
            state_commitment: StateCommitment(vec![0, 1, 2, 3]),
            contract_name,
            timeout_window: None,
        }
    }

    pub fn make_register_native_contract_effect(
        contract_name: ContractName,
    ) -> RegisterContractEffect {
        RegisterContractEffect {
            verifier: Verifier("sha3_256".to_string()),
            program_id: ProgramId(vec![]),
            state_commitment: StateCommitment(vec![0, 1, 2, 3]),
            contract_name,
            timeout_window: None,
        }
    }

    pub fn new_proof_tx(
        contract: &ContractName,
        hyli_output: &HyliOutput,
        blob_tx_hash: &TxHash,
    ) -> VerifiedProofTransaction {
        let verifier = Verifier("test".to_string());
        let program_id = ProgramId(vec![]);
        let proof = ProofTransaction {
            contract_name: contract.clone(),
            proof: ProofData(borsh::to_vec(&vec![hyli_output.clone()]).unwrap()),
            verifier: verifier.clone(),
            program_id: program_id.clone(),
        };
        VerifiedProofTransaction {
            contract_name: contract.clone(),
            verifier: proof.verifier.clone(),
            program_id: proof.program_id.clone(),
            proven_blobs: vec![BlobProofOutput {
                hyli_output: hyli_output.clone(),
                program_id: proof.program_id.clone(),
                verifier: proof.verifier.clone(),
                blob_tx_hash: blob_tx_hash.clone(),
                original_proof_hash: proof.proof.hashed(),
            }],
            proof_hash: proof.proof.hashed(),
            proof_size: proof.estimate_size(),
            proof: Some(proof.proof),
            is_recursive: false,
        }
    }

    pub fn make_hyli_output(blob_tx: BlobTransaction, blob_index: BlobIndex) -> HyliOutput {
        HyliOutput {
            version: 1,
            identity: blob_tx.identity.clone(),
            index: blob_index,
            blobs: blob_tx.blobs.clone().into(),
            tx_blob_count: blob_tx.blobs.len(),
            initial_state: StateCommitment(vec![0, 1, 2, 3]),
            next_state: StateCommitment(vec![4, 5, 6]),
            success: true,
            tx_hash: blob_tx.hashed(),
            tx_ctx: None,
            state_reads: vec![],
            onchain_effects: vec![],
            program_outputs: vec![],
        }
    }

    pub fn make_hyli_output_bis(blob_tx: BlobTransaction, blob_index: BlobIndex) -> HyliOutput {
        HyliOutput {
            version: 1,
            identity: blob_tx.identity.clone(),
            index: blob_index,
            blobs: blob_tx.blobs.clone().into(),
            tx_blob_count: blob_tx.blobs.len(),
            initial_state: StateCommitment(vec![4, 5, 6]),
            next_state: StateCommitment(vec![7, 8, 9]),
            success: true,
            tx_hash: blob_tx.hashed(),
            tx_ctx: None,
            state_reads: vec![],
            onchain_effects: vec![],
            program_outputs: vec![],
        }
    }

    pub fn make_hyli_output_ter(blob_tx: BlobTransaction, blob_index: BlobIndex) -> HyliOutput {
        HyliOutput {
            version: 1,
            identity: blob_tx.identity.clone(),
            index: blob_index,
            blobs: blob_tx.blobs.clone().into(),
            tx_blob_count: blob_tx.blobs.len(),
            initial_state: StateCommitment(vec![7, 8, 9]),
            next_state: StateCommitment(vec![10, 11, 12]),
            success: true,
            tx_hash: blob_tx.hashed(),
            tx_ctx: None,
            state_reads: vec![],
            onchain_effects: vec![],
            program_outputs: vec![],
        }
    }
    pub fn make_hyli_output_with_state(
        blob_tx: BlobTransaction,
        blob_index: BlobIndex,
        initial_state: &[u8],
        next_state: &[u8],
    ) -> HyliOutput {
        HyliOutput {
            version: 1,
            identity: blob_tx.identity.clone(),
            index: blob_index,
            blobs: blob_tx.blobs.clone().into(),
            tx_blob_count: blob_tx.blobs.len(),
            initial_state: StateCommitment(initial_state.to_vec()),
            next_state: StateCommitment(next_state.to_vec()),
            success: true,
            tx_hash: blob_tx.hashed(),
            tx_ctx: None,
            state_reads: vec![],
            onchain_effects: vec![],
            program_outputs: vec![],
        }
    }

    pub fn craft_signed_block(height: u64, txs: Vec<Transaction>) -> SignedBlock {
        SignedBlock {
            certificate: AggregateSignature::default(),
            consensus_proposal: ConsensusProposal {
                slot: height,
                ..ConsensusProposal::default()
            },
            data_proposals: vec![(
                LaneId::default(),
                vec![DataProposal::new(
                    Some(DataProposalHash(format!("{height}"))),
                    txs,
                )],
            )],
        }
    }

    pub fn craft_signed_block_with_parent_dp_hash(
        height: u64,
        txs: Vec<Transaction>,
        parent_dp_hash: DataProposalHash,
    ) -> SignedBlock {
        SignedBlock {
            certificate: AggregateSignature::default(),
            consensus_proposal: ConsensusProposal {
                slot: height,
                ..ConsensusProposal::default()
            },
            data_proposals: vec![(
                LaneId::default(),
                vec![DataProposal::new(Some(parent_dp_hash), txs)],
            )],
        }
    }
    impl NodeState {
        fn for_testing(&'_ mut self) -> NodeStateProcessing<'_> {
            NodeStateProcessing {
                this: self,
                callback: Box::leak(Box::new(BlockNodeStateCallback::default())),
            }
        }

        // Convenience method to handle a signed block in tests.
        pub fn force_handle_block(&mut self, block: SignedBlock) -> NodeStateBlock {
            if block.consensus_proposal.slot <= self.store.current_height.0
                || block.consensus_proposal.slot == 0
            {
                panic!("Invalid block height");
            }
            self.store.current_height = BlockHeight(block.consensus_proposal.slot - 1);
            self.handle_signed_block(block).unwrap()
        }

        pub fn craft_new_block_and_handle(
            &mut self,
            height: u64,
            txs: Vec<Transaction>,
        ) -> NodeStateBlock {
            let block = craft_signed_block(height, txs);
            self.force_handle_block(block)
        }

        pub fn craft_block_and_handle(&mut self, height: u64, txs: Vec<Transaction>) -> Block {
            let block = craft_signed_block(height, txs);
            self.force_handle_block(block).parsed_block.deref().clone()
        }

        pub fn craft_block_and_handle_with_parent_dp_hash(
            &mut self,
            height: u64,
            txs: Vec<Transaction>,
            parent_dp_hash: DataProposalHash,
        ) -> Block {
            let block = craft_signed_block_with_parent_dp_hash(height, txs, parent_dp_hash);
            self.force_handle_block(block).parsed_block.deref().clone()
        }

        pub fn handle_register_contract_effect(&mut self, tx: &RegisterContractEffect) {
            info!("📝 Registering contract {}", tx.contract_name);
            self.store.contracts.insert(
                tx.contract_name.clone(),
                Contract {
                    name: tx.contract_name.clone(),
                    program_id: tx.program_id.clone(),
                    state: tx.state_commitment.clone(),
                    verifier: tx.verifier.clone(),
                    timeout_window: tx.timeout_window.clone().unwrap_or_default(),
                },
            );
        }

        pub fn get_earliest_unsettled_height(
            &self,
            contract_name: &ContractName,
        ) -> Option<BlockHeight> {
            self.store
                .unsettled_transactions
                .get_earliest_unsettled_height(contract_name)
        }
    }

    impl<'a> NodeStateProcessing<'a> {
        // Convenience method to handle a signed block in tests.
        pub fn force_handle_block(&mut self, block: SignedBlock) -> NodeStateBlock {
            self.this.force_handle_block(block)
        }

        pub fn craft_block_and_handle(&mut self, height: u64, txs: Vec<Transaction>) -> Block {
            self.this.craft_block_and_handle(height, txs)
        }

        pub fn craft_block_and_handle_with_parent_dp_hash(
            &mut self,
            height: u64,
            txs: Vec<Transaction>,
            parent_dp_hash: DataProposalHash,
        ) -> Block {
            self.this
                .craft_block_and_handle_with_parent_dp_hash(height, txs, parent_dp_hash)
        }

        pub fn handle_register_contract_effect(&mut self, tx: &RegisterContractEffect) {
            self.this.handle_register_contract_effect(tx)
        }

        pub fn get_earliest_unsettled_height(
            &self,
            contract_name: &ContractName,
        ) -> Option<BlockHeight> {
            self.this.get_earliest_unsettled_height(contract_name)
        }
    }

    fn bogus_tx_context() -> Arc<TxContext> {
        Arc::new(TxContext {
            lane_id: LaneId::default(),
            block_hash: ConsensusProposalHash("0xfedbeef".to_owned()),
            block_height: BlockHeight(133),
            timestamp: TimestampMsClock::now(),
            chain_id: HYLI_TESTNET_CHAIN_ID,
        })
    }
}

use sdk::{
    Block, ConsensusStakingAction, HandledBlobProofOutput, Hashed, RegisterContractEffect,
    SignedBlock, StakingAction, StructuredBlob, TransactionStateEvent,
};

use crate::node_state::{NodeStateCallback, TxEvent};

#[derive(Default)]
pub struct BlockNodeStateCallback {
    block_under_construction: Block,
}

impl BlockNodeStateCallback {
    pub fn new() -> Self {
        BlockNodeStateCallback {
            block_under_construction: Block::default(),
        }
    }

    pub fn from_signed(signed_block: &SignedBlock) -> Self {
        BlockNodeStateCallback {
            block_under_construction: Block {
                parent_hash: signed_block.parent_hash().clone(),
                hash: signed_block.hashed(),
                block_height: signed_block.height(),
                block_timestamp: signed_block.consensus_proposal.timestamp.clone(),
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
        }
    }

    pub fn get_block(&mut self) -> Block {
        std::mem::take(&mut self.block_under_construction)
    }
}

impl NodeStateCallback for BlockNodeStateCallback {
    fn on_event(&mut self, event: &TxEvent) {
        match *event {
            TxEvent::RejectedBlobTransaction(tx_id) => {
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
            TxEvent::SequencedBlobTransaction(tx_id, lane_id, _, blob_tx) => {
                self.block_under_construction
                    .txs
                    .push((tx_id.clone(), blob_tx.clone().into()));
                self.block_under_construction
                    .dp_parent_hashes
                    .insert(tx_id.1.clone(), tx_id.0.clone());
                self.block_under_construction
                    .lane_ids
                    .insert(tx_id.1.clone(), lane_id.clone());
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
            }
            TxEvent::SettledAsFailed(tx_id) => {
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
            }
            TxEvent::TimedOut(tx_id) => {
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
                        self.block_under_construction
                            .staking_actions
                            .push((tx.identity.clone(), staking_action));
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
            }
            TxEvent::ContractStateUpdated(_, contract_name, state_commitment) => {
                self.block_under_construction
                    .updated_states
                    .insert(contract_name.clone(), state_commitment.clone());
            }
            TxEvent::ContractProgramIdUpdated(_, contract_name, program_id) => {
                self.block_under_construction
                    .updated_program_ids
                    .insert(contract_name.clone(), program_id.clone());
            }
            TxEvent::ContractTimeoutWindowUpdated(_, contract_name, timeout_window) => {
                self.block_under_construction
                    .updated_timeout_windows
                    .insert(contract_name.clone(), timeout_window.clone());
            }
        };
    }
}

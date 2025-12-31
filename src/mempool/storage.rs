use anyhow::Result;
use async_stream::try_stream;
use borsh::{BorshDeserialize, BorshSerialize};
use futures::{Stream, StreamExt};
use hyli_crypto::BlstCrypto;
use hyli_model::{DataSized, LaneId, ProofData, TxHash};
use serde::{Deserialize, Serialize};
use staking::state::Staking;
use std::{collections::HashMap, future::Future, vec};
use tracing::{error, trace};

use crate::model::{
    Cut, DataProposal, DataProposalHash, Hashed, PoDA, SignedByValidator, ValidatorPublicKey,
};

use super::ValidatorDAG;

pub use hyli_model::LaneBytesSize;

pub enum CanBePutOnTop {
    Yes,
    No,
    AlreadyOnTop,
    Fork,
}

#[derive(Debug)]
pub enum EntryOrMissingHash {
    Entry(LaneEntryMetadata, DataProposal),
    MissingHash(DataProposalHash),
}

#[derive(Debug)]
pub enum MetadataOrMissingHash {
    Metadata(LaneEntryMetadata, DataProposalHash),
    MissingHash(DataProposalHash),
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize, PartialEq, Eq, Serialize, Deserialize)]
pub struct LaneEntryMetadata {
    pub parent_data_proposal_hash: Option<DataProposalHash>,
    pub cumul_size: LaneBytesSize,
    pub signatures: Vec<ValidatorDAG>,
}

pub trait Storage {
    fn persist(&self) -> Result<()>;

    fn contains(&self, lane_id: &LaneId, dp_hash: &DataProposalHash) -> bool;
    fn get_metadata_by_hash(
        &self,
        lane_id: &LaneId,
        dp_hash: &DataProposalHash,
    ) -> Result<Option<LaneEntryMetadata>>;
    fn get_dp_by_hash(
        &self,
        lane_id: &LaneId,
        dp_hash: &DataProposalHash,
    ) -> Result<Option<DataProposal>>;
    fn get_proofs_by_hash(
        &self,
        lane_id: &LaneId,
        dp_hash: &DataProposalHash,
    ) -> Result<Option<HashMap<TxHash, ProofData>>>;
    fn delete_proofs(&mut self, lane_id: &LaneId, dp_hash: &DataProposalHash) -> Result<()>;
    fn pop(
        &mut self,
        lane_id: LaneId,
    ) -> Result<Option<(DataProposalHash, (LaneEntryMetadata, DataProposal))>>;
    fn put_no_verification(
        &mut self,
        lane_id: LaneId,
        entry: (LaneEntryMetadata, DataProposal),
    ) -> Result<()>;

    fn add_signatures<T: IntoIterator<Item = ValidatorDAG>>(
        &mut self,
        lane_id: &LaneId,
        dp_hash: &DataProposalHash,
        vote_msgs: T,
    ) -> Result<Vec<ValidatorDAG>>;

    fn get_lane_ids(&self) -> Vec<LaneId>;
    fn get_lane_hash_tip(&self, lane_id: &LaneId) -> Option<DataProposalHash>;
    fn get_lane_size_tip(&self, lane_id: &LaneId) -> Option<LaneBytesSize>;

    fn update_lane_tip(
        &mut self,
        lane_id: LaneId,
        dp_hash: DataProposalHash,
        size: LaneBytesSize,
    ) -> Option<(DataProposalHash, LaneBytesSize)>;
    #[cfg(test)]
    fn remove_lane_entry(&mut self, lane_id: &LaneId, dp_hash: &DataProposalHash);

    fn get_latest_car(
        &self,
        lane_id: &LaneId,
        staking: &Staking,
        previous_committed_car: Option<&(LaneId, DataProposalHash, LaneBytesSize, PoDA)>,
    ) -> Result<Option<(DataProposalHash, LaneBytesSize, PoDA)>> {
        let bonded_validators = staking.bonded();

        // Start from tip; bail early if lane is empty.
        let mut current = match self.get_lane_hash_tip(lane_id) {
            Some(h) => h,
            None => return Ok(None),
        };

        trace!("Getting latest CAR for lane {lane_id} with tip {current}");

        loop {
            // We stop once the currently examined DP is the one from the committed cut.
            if let Some((_, prev_hash, prev_size, prev_poda)) = previous_committed_car {
                if current == *prev_hash {
                    trace!("Found matching previous committed CAR: {prev_hash}");
                    return Ok(Some((prev_hash.clone(), *prev_size, prev_poda.clone())));
                }
            }

            // This DP is on top of the current cut, does it have a CAR or should we fetch its parent?
            let Some(le) = self.get_metadata_by_hash(lane_id, &current)? else {
                return Ok(None);
            };

            let filtered: Vec<&SignedByValidator<(DataProposalHash, LaneBytesSize)>> = le
                .signatures
                .iter()
                .filter(|s| bonded_validators.contains(&s.signature.validator))
                .collect();

            // Compute voting power from the filtered validator set.
            let filtered_validators: Vec<ValidatorPublicKey> = filtered
                .iter()
                .map(|s| s.signature.validator.clone())
                .collect();

            // TODO: take by reference to avoid cloning above
            let voting_power = staking.compute_voting_power(&filtered_validators);
            let f = staking.compute_f();

            trace!("Checking for sufficient voting power: {voting_power} > {f} ?");

            // Enough votes: aggregate into PoDA and return.
            if voting_power > f {
                match BlstCrypto::aggregate((current.clone(), le.cumul_size), &filtered) {
                    Ok(poda) => {
                        return Ok(Some((current, le.cumul_size, poda.signature)));
                    }
                    Err(e) => {
                        error!("Could not aggregate signatures for lane {lane_id} and DP {current}: {e}");
                        return Ok(None);
                    }
                }
            }

            trace!("Not enough votes: moving to parent DP if available");

            // Not enough votes: move on to the parent if any, otherwise no CAR.
            if let Some(parent) = le.parent_data_proposal_hash {
                current = parent.clone();
            } else {
                return Ok(None);
            }
        }
    }

    /// Signs the data proposal before creating a new LaneEntry and puting it in the lane
    fn store_data_proposal(
        &mut self,
        crypto: &BlstCrypto,
        lane_id: &LaneId,
        data_proposal: DataProposal,
    ) -> Result<(DataProposalHash, LaneBytesSize)> {
        // Add DataProposal to validator's lane
        let data_proposal_hash = data_proposal.hashed();

        if self.contains(lane_id, &data_proposal_hash) {
            anyhow::bail!("DataProposal {} was already in lane", data_proposal_hash);
        }

        let verdict = self.can_be_put_on_top(
            lane_id,
            &data_proposal_hash,
            data_proposal.parent_data_proposal_hash.as_ref(),
        );

        let cumul_size = match verdict {
            CanBePutOnTop::No => {
                anyhow::bail!(
                    "Can't store DataProposal {}, as parent is unknown",
                    data_proposal_hash
                );
            }
            CanBePutOnTop::Fork => {
                let last_known_hash = self.get_lane_hash_tip(lane_id);
                anyhow::bail!(
                    "DataProposal cannot be put in lane because it creates a fork: tip dp hash {:?} while proposed parent_data_proposal_hash: {:?}",
                    last_known_hash,
                    data_proposal.parent_data_proposal_hash
                )
            }
            CanBePutOnTop::Yes => {
                let dp_size = data_proposal.estimate_size();
                let lane_size = self.get_lane_size_tip(lane_id).unwrap_or_default();
                let cumul_size = lane_size + dp_size;

                let msg = (data_proposal_hash.clone(), cumul_size);
                let signatures = vec![crypto.sign(msg)?];

                self.put_no_verification(
                    lane_id.clone(),
                    (
                        LaneEntryMetadata {
                            parent_data_proposal_hash: data_proposal
                                .parent_data_proposal_hash
                                .clone(),
                            cumul_size,
                            signatures,
                        },
                        data_proposal,
                    ),
                )?;
                // We optimistically update our lane tip here, we'll potentially clean this later.
                self.update_lane_tip(lane_id.clone(), data_proposal_hash.clone(), cumul_size);
                cumul_size
            }
            CanBePutOnTop::AlreadyOnTop => {
                // This can happen if the lane tip is updated (via a commit) before the data proposal is processed.
                // Store it anyways - this ensures caller end up in a consistent state.
                // The lane_size already contains the size of the current data proposal, so we don't need to adjust it.
                let cumul_size = self.get_lane_size_tip(lane_id).unwrap_or_default();
                let msg = (data_proposal_hash.clone(), cumul_size);
                let signatures = vec![crypto.sign(msg)?];
                self.put_no_verification(
                    lane_id.clone(),
                    (
                        LaneEntryMetadata {
                            parent_data_proposal_hash: data_proposal
                                .parent_data_proposal_hash
                                .clone(),
                            cumul_size,
                            signatures,
                        },
                        data_proposal,
                    ),
                )?;
                cumul_size
            }
        };

        Ok((data_proposal_hash, cumul_size))
    }

    // Implemented in the actual modules to potentially benefit from optimizations
    // in the underlying storage
    fn get_entries_between_hashes(
        &self,
        lane_id: &LaneId,
        from_data_proposal_hash: Option<DataProposalHash>,
        to_data_proposal_hash: Option<DataProposalHash>,
    ) -> impl Stream<Item = Result<EntryOrMissingHash>>;

    fn get_entries_metadata_between_hashes(
        &self,
        lane_id: &LaneId,
        from_data_proposal_hash: Option<DataProposalHash>,
        to_data_proposal_hash: Option<DataProposalHash>,
    ) -> impl Stream<Item = Result<MetadataOrMissingHash>> {
        // If no dp hash is provided, we use the tip of the lane
        let initial_dp_hash: Option<DataProposalHash> =
            to_data_proposal_hash.or(self.get_lane_hash_tip(lane_id));
        try_stream! {
            if let Some(mut some_dp_hash) = initial_dp_hash {
                while Some(&some_dp_hash) != from_data_proposal_hash.as_ref() {
                    let lane_entry = self.get_metadata_by_hash(lane_id, &some_dp_hash)?;
                    match lane_entry {
                        Some(lane_entry) => {
                            yield MetadataOrMissingHash::Metadata(lane_entry.clone(), some_dp_hash);
                            if let Some(parent_dp_hash) = lane_entry.parent_data_proposal_hash.clone() {
                                some_dp_hash = parent_dp_hash;
                            } else {
                                break;
                            }
                        }
                        None => {
                            yield MetadataOrMissingHash::MissingHash(some_dp_hash.clone());
                            break;
                        }
                    }
                }
            }
        }
    }

    fn get_lane_size_at(
        &self,
        lane_id: &LaneId,
        dp_hash: &DataProposalHash,
    ) -> Result<LaneBytesSize> {
        self.get_metadata_by_hash(lane_id, dp_hash)?.map_or_else(
            || Ok(LaneBytesSize::default()),
            |entry| Ok(entry.cumul_size),
        )
    }

    fn get_pending_entries_in_lane(
        &self,
        lane_id: &LaneId,
        last_cut: Option<Cut>,
    ) -> impl Stream<Item = Result<MetadataOrMissingHash>> {
        let lane_tip = self.get_lane_hash_tip(lane_id);

        let last_committed_dp_hash = match last_cut {
            Some(cut) => cut
                .iter()
                .find(|(v, _, _, _)| v == lane_id)
                .map(|(_, dp, _, _)| dp.clone()),
            None => None,
        };
        self.get_entries_metadata_between_hashes(lane_id, last_committed_dp_hash.clone(), lane_tip)
    }

    /// Get oldest entry in the lane wrt the last committed cut.
    fn get_oldest_pending_entry(
        &self,
        lane_id: &LaneId,
        last_cut: Option<Cut>,
    ) -> impl Future<Output = Result<Option<(LaneEntryMetadata, DataProposalHash)>>> {
        async move {
            let mut stream = Box::pin(self.get_pending_entries_in_lane(lane_id, last_cut));
            let mut last_entry = None;

            while let Some(entry) = stream.next().await {
                match entry? {
                    MetadataOrMissingHash::Metadata(metadata, dp_hash) => {
                        last_entry = Some((metadata, dp_hash));
                    }
                    MetadataOrMissingHash::MissingHash(_) => {
                        // Missing entry, should not happen
                        tracing::warn!(
                            "Missing entry in lane {} while trying to get oldest entry",
                            lane_id
                        );
                        return Ok(None);
                    }
                }
            }

            Ok(last_entry)
        }
    }

    /// For unknown DataProposals in the new cut, we need to remove all DataProposals that we have after the previous cut.
    /// This is necessary because it is difficult to determine if those DataProposals are part of a fork. --> This approach is suboptimal.
    /// Therefore, we update the lane_tip with the DataProposal from the new cut, creating a gap in the lane but allowing us to vote on new DataProposals.
    fn clean_and_update_lane(
        &mut self,
        lane_id: &LaneId,
        previous_committed_dp_hash: Option<&DataProposalHash>,
        new_committed_dp_hash: &DataProposalHash,
        new_committed_size: &LaneBytesSize,
    ) -> Result<()> {
        let tip_lane = self.get_lane_hash_tip(lane_id);
        // Check if lane is in a state between previous cut and new cut
        if tip_lane.as_ref() != previous_committed_dp_hash
            && tip_lane.as_ref() != Some(new_committed_dp_hash)
        {
            // Remove last element from the lane until we find the data proposal of the previous cut
            while let Some((dp_hash, le)) = self.pop(lane_id.clone())? {
                if Some(&dp_hash) == previous_committed_dp_hash {
                    // Reinsert the lane entry corresponding to the previous cut
                    self.put_no_verification(lane_id.clone(), le)?;
                    break;
                }
            }
        }
        // Update lane tip with new cut
        self.update_lane_tip(
            lane_id.clone(),
            new_committed_dp_hash.clone(),
            *new_committed_size,
        );
        Ok(())
    }

    /// Returns CanBePutOnTop::Yes if the DataProposal can be put in the lane
    /// Returns CanBePutOnTop::False if the DataProposal can't be put in the lane because the parent is unknown
    /// Returns CanBePutOnTop::Fork if the DataProposal creates a fork
    /// Returns CanBePutOnTop::AlreadyOnTop if the DataProposal is already on top of the lane
    fn can_be_put_on_top(
        &mut self,
        lane_id: &LaneId,
        data_proposal_hash: &DataProposalHash,
        parent_data_proposal_hash: Option<&DataProposalHash>,
    ) -> CanBePutOnTop {
        // Data proposal parent hash needs to match the lane tip of that validator
        if parent_data_proposal_hash == self.get_lane_hash_tip(lane_id).as_ref() {
            // LEGIT DATAPROPOSAL
            return CanBePutOnTop::Yes;
        }

        if self.get_lane_hash_tip(lane_id).as_ref() == Some(data_proposal_hash) {
            return CanBePutOnTop::AlreadyOnTop;
        }

        if let Some(dp_parent_hash) = parent_data_proposal_hash {
            if !self.contains(lane_id, dp_parent_hash) {
                // UNKNOWN PARENT
                return CanBePutOnTop::No;
            }
        }

        // NEITHER LEGIT NOR CORRECT PARENT --> FORK
        CanBePutOnTop::Fork
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::mempool::storage_memory::LanesStorage;
    use crate::model::*;
    use assertables::assert_none;
    use futures::StreamExt;
    use hyli_model::{DataSized, Identity, Signature, Transaction, ValidatorSignature};
    use staking::state::Staking;

    fn setup_storage() -> LanesStorage {
        let tmp_dir = tempfile::tempdir().unwrap().keep();
        LanesStorage::new(&tmp_dir, BTreeMap::default()).unwrap()
    }

    #[test_log::test(tokio::test)]
    async fn test_put_contains_get() {
        let crypto = BlstCrypto::new("1").unwrap();
        let lane_id = &LaneId(crypto.validator_pubkey().clone());
        let mut storage = setup_storage();

        let data_proposal = DataProposal::new(None, vec![]);
        let cumul_size: LaneBytesSize = LaneBytesSize(data_proposal.estimate_size() as u64);

        let entry = LaneEntryMetadata {
            parent_data_proposal_hash: None,
            cumul_size,
            signatures: vec![],
        };
        let dp_hash = data_proposal.hashed();
        storage
            .put_no_verification(lane_id.clone(), (entry.clone(), data_proposal.clone()))
            .unwrap();
        assert!(storage.contains(lane_id, &dp_hash));
        assert_eq!(
            storage
                .get_metadata_by_hash(lane_id, &dp_hash)
                .unwrap()
                .unwrap(),
            entry
        );
        assert_eq!(
            storage.get_dp_by_hash(lane_id, &dp_hash).unwrap().unwrap(),
            data_proposal
        );
    }

    #[test_log::test(tokio::test)]
    async fn test_store_proofs_separately_and_hydrate() {
        let crypto = BlstCrypto::new("proofs").unwrap();
        let lane_id = &LaneId(crypto.validator_pubkey().clone());
        let mut storage = setup_storage();

        // Build a DataProposal with a VerifiedProof tx that includes an inlined proof
        let proof = ProofData(vec![1, 2, 3, 4]);
        let proof_hash = ProofDataHash(proof.hashed().0);
        let vpt = VerifiedProofTransaction {
            contract_name: ContractName::new("test-contract"),
            program_id: ProgramId(vec![]),
            verifier: Verifier("test".into()),
            proof: Some(proof.clone()),
            proof_hash: proof_hash.clone(),
            proof_size: proof.0.len(),
            proven_blobs: vec![BlobProofOutput {
                original_proof_hash: proof_hash,
                blob_tx_hash: crate::model::TxHash("blob-tx".into()),
                program_id: ProgramId(vec![]),
                verifier: Verifier("test".into()),
                hyli_output: HyliOutput::default(),
            }],
            is_recursive: false,
        };
        let tx = Transaction::from(TransactionData::VerifiedProof(vpt.clone()));
        let tx_hash = tx.hashed();

        let dp = DataProposal::new(None, vec![tx]);
        let cumul_size: LaneBytesSize = LaneBytesSize(dp.estimate_size() as u64);
        let entry = LaneEntryMetadata {
            parent_data_proposal_hash: None,
            cumul_size,
            signatures: vec![],
        };
        let dp_hash = dp.hashed();

        // Store DP: implementation should strip proofs and store them separately
        storage
            .put_no_verification(lane_id.clone(), (entry.clone(), dp.clone()))
            .unwrap();

        // Stored DP must have proofs removed
        let stored_dp = storage
            .get_dp_by_hash(lane_id, &dp_hash)
            .unwrap()
            .expect("stored dp");
        match &stored_dp.txs.first().unwrap().transaction_data {
            TransactionData::VerifiedProof(v) => {
                assert!(v.proof.is_none(), "proof should be stripped in storage");
                assert_eq!(v.proof_size, proof.0.len());
            }
            _ => panic!("expected VerifiedProof tx"),
        }

        // Proofs must be available in the side-store
        let proofs = storage
            .get_proofs_by_hash(lane_id, &dp_hash)
            .unwrap()
            .expect("proofs stored");
        assert_eq!(proofs.get(&tx_hash), Some(&proof));

        // Hydration should restore proofs back into the DP for broadcasting
        let mut to_broadcast = stored_dp.clone();
        to_broadcast.hydrate_proofs(proofs);
        match &to_broadcast.txs.first().unwrap().transaction_data {
            TransactionData::VerifiedProof(v) => {
                assert_eq!(v.proof.as_ref(), Some(&proof));
            }
            _ => panic!("expected VerifiedProof tx"),
        }
    }

    #[test_log::test(tokio::test)]
    async fn test_update() {
        let crypto: BlstCrypto = BlstCrypto::new("1").unwrap();
        let lane_id = &LaneId(crypto.validator_pubkey().clone());
        let mut storage = setup_storage();
        let data_proposal = DataProposal::new(None, vec![]);
        let cumul_size: LaneBytesSize = LaneBytesSize(data_proposal.estimate_size() as u64);
        let mut entry = LaneEntryMetadata {
            parent_data_proposal_hash: None,
            cumul_size,
            signatures: vec![],
        };
        let dp_hash = data_proposal.hashed();
        storage
            .put_no_verification(lane_id.clone(), (entry.clone(), data_proposal.clone()))
            .unwrap();
        entry.signatures.push(SignedByValidator {
            msg: (dp_hash.clone(), cumul_size),
            signature: ValidatorSignature {
                validator: crypto.validator_pubkey().clone(),
                signature: Signature::default(),
            },
        });
        storage
            .put_no_verification(lane_id.clone(), (entry.clone(), data_proposal.clone()))
            .unwrap();
        let updated = storage
            .get_metadata_by_hash(lane_id, &dp_hash)
            .unwrap()
            .unwrap();
        assert_eq!(1, updated.signatures.len());
    }

    #[test_log::test(tokio::test)]
    async fn test_on_data_vote() {
        let crypto: BlstCrypto = BlstCrypto::new("1").unwrap();
        let lane_id = &LaneId(crypto.validator_pubkey().clone());
        let mut storage = setup_storage();

        let crypto2: BlstCrypto = BlstCrypto::new("2").unwrap();

        let data_proposal = DataProposal::new(None, vec![]);
        // 1 creates a DP
        let (dp_hash, cumul_size) = storage
            .store_data_proposal(&crypto, lane_id, data_proposal)
            .unwrap();

        let lane_entry = storage
            .get_metadata_by_hash(lane_id, &dp_hash)
            .unwrap()
            .unwrap();
        assert_eq!(1, lane_entry.signatures.len());

        // 2 votes on this DP
        let vote_msg = (dp_hash.clone(), cumul_size);
        let signed_msg = crypto2.sign(vote_msg).expect("Failed to sign message");

        let signatures = storage
            .add_signatures(lane_id, &dp_hash, std::iter::once(signed_msg))
            .unwrap();
        assert_eq!(2, signatures.len());
    }

    #[test_log::test(tokio::test)]
    async fn test_on_poda_update() {
        let crypto: BlstCrypto = BlstCrypto::new("1").unwrap();
        let mut storage = setup_storage();

        let crypto2: BlstCrypto = BlstCrypto::new("2").unwrap();
        let lane_id2 = &LaneId(crypto2.validator_pubkey().clone());
        let crypto3: BlstCrypto = BlstCrypto::new("3").unwrap();

        let dp = DataProposal::new(None, vec![]);

        // 1 stores DP in 2's lane
        let (dp_hash, cumul_size) = storage.store_data_proposal(&crypto, lane_id2, dp).unwrap();

        // 3 votes on this DP
        let vote_msg = (dp_hash.clone(), cumul_size);
        let signed_msg = crypto3.sign(vote_msg).expect("Failed to sign message");

        // 1 updates its lane with all signatures
        storage
            .add_signatures(lane_id2, &dp_hash, std::iter::once(signed_msg))
            .unwrap();

        let lane_entry = storage
            .get_metadata_by_hash(lane_id2, &dp_hash)
            .unwrap()
            .unwrap();
        assert_eq!(
            2,
            lane_entry.signatures.len(),
            "{lane_id2}'s lane entry: {lane_entry:?}"
        );
    }

    fn unwrap_entry(entry: &EntryOrMissingHash) -> (LaneEntryMetadata, DataProposal) {
        match entry {
            EntryOrMissingHash::Entry(metadata, data_proposal) => {
                (metadata.clone(), data_proposal.clone())
            }
            EntryOrMissingHash::MissingHash(_) => panic!("Expected an entry, got missing hash"),
        }
    }

    #[test_log::test(tokio::test)]
    async fn test_get_lane_entries_between_hashes() {
        let crypto: BlstCrypto = BlstCrypto::new("1").unwrap();
        let lane_id = &LaneId(crypto.validator_pubkey().clone());
        let mut storage = setup_storage();
        let dp1 = DataProposal::new(None, vec![]);
        let dp2 = DataProposal::new(Some(dp1.hashed()), vec![]);
        let dp3 = DataProposal::new(Some(dp2.hashed()), vec![]);

        storage
            .store_data_proposal(&crypto, lane_id, dp1.clone())
            .unwrap();
        storage
            .store_data_proposal(&crypto, lane_id, dp2.clone())
            .unwrap();
        storage
            .store_data_proposal(&crypto, lane_id, dp3.clone())
            .unwrap();

        // [start, end] == [1, 2, 3]
        let all_entries: Vec<_> = storage
            .get_entries_between_hashes(lane_id, None, None)
            .collect()
            .await;
        assert_eq!(3, all_entries.len());

        // ]1, end] == [3, 2]
        let entries_from_1_to_end: Vec<_> = storage
            .get_entries_between_hashes(lane_id, Some(dp1.hashed()), None)
            .collect()
            .await;
        assert_eq!(2, entries_from_1_to_end.len());
        assert_eq!(
            dp2,
            unwrap_entry(entries_from_1_to_end.last().unwrap().as_ref().unwrap()).1
        );
        assert_eq!(
            dp3,
            unwrap_entry(entries_from_1_to_end.first().unwrap().as_ref().unwrap()).1
        );

        // [start, 2] == [2, 1]

        let entries_from_start_to_2: Vec<_> = storage
            .get_entries_between_hashes(lane_id, None, Some(dp2.hashed()))
            .collect()
            .await;

        assert_eq!(2, entries_from_start_to_2.len());
        assert_eq!(
            dp1,
            unwrap_entry(entries_from_start_to_2.last().unwrap().as_ref().unwrap()).1
        );
        assert_eq!(
            dp2,
            unwrap_entry(entries_from_start_to_2.first().unwrap().as_ref().unwrap()).1
        );

        // ]1, 2] == [2]
        let entries_from_1_to_2: Vec<_> = storage
            .get_entries_between_hashes(lane_id, Some(dp1.hashed()), Some(dp2.hashed()))
            .collect()
            .await;
        assert_eq!(1, entries_from_1_to_2.len());
        assert_eq!(
            dp2,
            unwrap_entry(entries_from_1_to_2.first().unwrap().as_ref().unwrap()).1
        );

        // ]1, 3] == [3, 2]
        let entries_from_1_to_3: Vec<_> = storage
            .get_entries_between_hashes(lane_id, Some(dp1.hashed()), None)
            .collect()
            .await;
        assert_eq!(2, entries_from_1_to_3.len());
        assert_eq!(
            dp2,
            unwrap_entry(entries_from_1_to_3.last().unwrap().as_ref().unwrap()).1
        );
        assert_eq!(
            dp3,
            unwrap_entry(entries_from_1_to_3.first().unwrap().as_ref().unwrap()).1
        );

        // ]1, 1[ == []
        let entries_from_1_to_1: Vec<_> = storage
            .get_entries_between_hashes(lane_id, Some(dp1.hashed()), Some(dp1.hashed()))
            .collect()
            .await;
        assert_eq!(0, entries_from_1_to_1.len());
    }

    // Test to get oldest pending entry in a lane containing 3 DataProposals
    #[test_log::test(tokio::test)]
    async fn test_get_oldest_pending_entry() {
        let crypto: BlstCrypto = BlstCrypto::new("1").unwrap();
        let lane_id = &LaneId(crypto.validator_pubkey().clone());
        let mut storage = setup_storage();

        let dp1 = DataProposal::new(None, vec![]);
        let dp2 = DataProposal::new(Some(dp1.hashed()), vec![]);
        let dp3 = DataProposal::new(Some(dp2.hashed()), vec![]);

        storage
            .store_data_proposal(&crypto, lane_id, dp1.clone())
            .unwrap();
        storage
            .store_data_proposal(&crypto, lane_id, dp2.clone())
            .unwrap();
        storage
            .store_data_proposal(&crypto, lane_id, dp3.clone())
            .unwrap();

        // Get oldest pending entry
        let oldest_entry = storage
            .get_oldest_pending_entry(lane_id, None)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(dp1.hashed(), oldest_entry.1);
    }

    #[test_log::test]
    fn test_lane_size() {
        let crypto: BlstCrypto = BlstCrypto::new("1").unwrap();
        let lane_id = &LaneId(crypto.validator_pubkey().clone());
        let mut storage = setup_storage();

        let dp1 = DataProposal::new(None, vec![Transaction::default()]);

        let (_dp_hash, size) = storage
            .store_data_proposal(&crypto, lane_id, dp1.clone())
            .unwrap();
        assert_eq!(
            size,
            storage.get_lane_size_at(lane_id, &dp1.hashed()).unwrap()
        );
        assert_eq!(size, storage.get_lane_size_tip(lane_id).unwrap());
        assert_eq!(size.0, dp1.estimate_size() as u64);

        // Adding a new DP
        let dp2 = DataProposal::new(Some(dp1.hashed()), vec![Transaction::default()]);
        let (_hash, size) = storage
            .store_data_proposal(&crypto, lane_id, dp2.clone())
            .unwrap();
        assert_eq!(
            size,
            storage.get_lane_size_at(lane_id, &dp2.hashed()).unwrap()
        );
        assert_eq!(size, storage.get_lane_size_tip(lane_id).unwrap());
        assert_eq!(size.0, (dp1.estimate_size() + dp2.estimate_size()) as u64);
    }

    #[test_log::test(tokio::test)]
    async fn test_get_lane_pending_entries() {
        let crypto: BlstCrypto = BlstCrypto::new("1").unwrap();
        let lane_id = &LaneId(crypto.validator_pubkey().clone());
        let mut storage = setup_storage();
        let data_proposal = DataProposal::new(None, vec![]);
        let cumul_size: LaneBytesSize = LaneBytesSize(data_proposal.estimate_size() as u64);
        let entry = LaneEntryMetadata {
            parent_data_proposal_hash: None,
            cumul_size,
            signatures: vec![],
        };
        storage
            .put_no_verification(lane_id.clone(), (entry, data_proposal.clone()))
            .unwrap();
        storage.update_lane_tip(lane_id.clone(), data_proposal.hashed(), cumul_size);
        let pending: Vec<_> = storage
            .get_pending_entries_in_lane(lane_id, None)
            .collect()
            .await;
        assert_eq!(1, pending.len());
    }

    /// Add a bonded validator with a specified stake and delegators, using public methods for setup.
    fn add_bonded_validator_for_test(
        staking: &mut Staking,
        validator: ValidatorPublicKey,
        stake: u128,
        delegators: Vec<Identity>,
    ) {
        for delegator in &delegators {
            // Ignore error if already staked (for test setup)
            let _ = staking.stake(delegator.clone(), stake);
            // Ignore error if already delegated (for test setup)
            let _ = staking.delegate_to(delegator.clone(), validator.clone());
        }
        // Try to bond the validator (ignore error if already bonded)
        let _ = staking.bond(validator);
    }

    #[test_log::test(tokio::test)]
    async fn test_get_latest_car() {
        let mut storage = setup_storage();

        let crypto1: BlstCrypto = BlstCrypto::new("1").unwrap();
        let crypto2: BlstCrypto = BlstCrypto::new("2").unwrap();
        let mut staking = Staking::new();
        // Validator 2 is enough for PoDA, not 1 alone though.
        add_bonded_validator_for_test(
            &mut staking,
            crypto1.validator_pubkey().clone(),
            200,
            vec![Identity::new("john.hamm")],
        );
        add_bonded_validator_for_test(
            &mut staking,
            crypto2.validator_pubkey().clone(),
            1000,
            vec![Identity::new("jane.doe")],
        );
        let lane_id = &LaneId(crypto1.validator_pubkey().clone());

        // Helper lambdas so the repeated examples below are shorter.
        let add_signatures = |storage: &mut LanesStorage,
                              crypto: &BlstCrypto,
                              lane_id: &LaneId,
                              dp: &DataProposal| {
            let sig = crypto
                .sign((dp.hashed(), LaneBytesSize(dp.estimate_size() as u64)))
                .unwrap();
            storage
                .add_signatures(lane_id, &dp.hashed(), std::iter::once(sig))
                .unwrap()
        };
        let assert_latest_car_eq = |storage: &LanesStorage,
                                    lane_id: &LaneId,
                                    staking: &Staking,
                                    cut: Option<&DataProposalHash>,
                                    dp: &DataProposal| {
            let cut = cut.map(|cut| {
                (
                    lane_id.clone(),
                    cut.clone(),
                    LaneBytesSize(0),
                    PoDA::default(),
                )
            });
            assert_eq!(
                storage
                    .get_latest_car(lane_id, staking, cut.as_ref())
                    .unwrap()
                    .unwrap()
                    .0,
                dp.hashed()
            );
        };

        ////////////////////////////
        // Case 1: No CAR (no votes)
        let dp1 = DataProposal::new(None, vec![]);
        storage
            .store_data_proposal(&crypto1, lane_id, dp1.clone()) // signs it too.
            .unwrap();
        assert_none!(storage.get_latest_car(lane_id, &staking, None).unwrap());

        ////////////////////////////
        // Case 2: CAR
        // Sign it for PoDA
        add_signatures(&mut storage, &crypto2, lane_id, &dp1);
        assert_latest_car_eq(&mut storage, lane_id, &staking, None, &dp1);

        ////////////////////////////
        // Case 3: no CAR, parent is CAR
        let dp2 = DataProposal::new(Some(dp1.hashed()), vec![]);
        storage
            .store_data_proposal(&crypto1, lane_id, dp2.clone()) // signs it too.
            .unwrap();
        // Car is still dp1
        assert_latest_car_eq(&mut storage, lane_id, &staking, None, &dp1);
        let dp3 = DataProposal::new(Some(dp2.hashed()), vec![]);
        storage
            .store_data_proposal(&crypto1, lane_id, dp3.clone()) // signs it too.
            .unwrap();
        // Car is still dp1
        assert_latest_car_eq(&mut storage, lane_id, &staking, None, &dp1);

        ////////////////////////////
        // Case 4: we don't have the car for a dp but it's part of the Cut
        assert_latest_car_eq(&mut storage, lane_id, &staking, Some(&dp2.hashed()), &dp2);
        assert_latest_car_eq(&mut storage, lane_id, &staking, Some(&dp3.hashed()), &dp3);

        ////////////////////////////
        // Case 5: we have a CAR on top of the cut.
        add_signatures(&mut storage, &crypto2, lane_id, &dp2);
        assert_latest_car_eq(&mut storage, lane_id, &staking, None, &dp2);
        assert_latest_car_eq(&mut storage, lane_id, &staking, Some(&dp1.hashed()), &dp2);
        assert_latest_car_eq(&mut storage, lane_id, &staking, Some(&dp2.hashed()), &dp2);
        assert_latest_car_eq(&mut storage, lane_id, &staking, Some(&dp3.hashed()), &dp3);
    }
}

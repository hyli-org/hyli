use anyhow::Result;
use async_stream::try_stream;
use borsh::{BorshDeserialize, BorshSerialize};
use futures::Stream;
use hyle_crypto::BlstCrypto;
use hyle_model::{DataSized, LaneId};
use serde::{Deserialize, Serialize};
use staking::state::Staking;
use std::vec;
use tracing::error;

use crate::model::{
    Cut, DataProposal, DataProposalHash, Hashed, PoDA, SignedByValidator, ValidatorPublicKey,
};

use super::ValidatorDAG;

pub use hyle_model::LaneBytesSize;

pub enum CanBePutOnTop {
    Yes,
    No,
    Fork,
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

    fn pop(
        &mut self,
        lane_id: LaneId,
    ) -> Result<Option<(DataProposalHash, (LaneEntryMetadata, DataProposal))>>;
    fn put(&mut self, lane_id: LaneId, entry: (LaneEntryMetadata, DataProposal)) -> Result<()>;
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

    fn get_lane_ids(&self) -> impl Iterator<Item = &LaneId>;
    fn get_lane_hash_tip(&self, lane_id: &LaneId) -> Option<&DataProposalHash>;
    fn get_lane_size_tip(&self, lane_id: &LaneId) -> Option<&LaneBytesSize>;
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
        // We start from the tip of the lane, and go backup until we find a DP with enough signatures
        if let Some(tip_dp_hash) = self.get_lane_hash_tip(lane_id) {
            let mut dp_hash = tip_dp_hash.clone();
            while let Some(le) = self.get_metadata_by_hash(lane_id, &dp_hash)? {
                if let Some((_, hash, _, poda)) = previous_committed_car {
                    if &dp_hash == hash {
                        // Latest car has already been committed
                        return Ok(Some((hash.clone(), le.cumul_size, poda.clone())));
                    }
                }
                // Filter signatures on DataProposal to only keep the ones from the current validators
                let filtered_signatures: Vec<SignedByValidator<(DataProposalHash, LaneBytesSize)>> =
                    le.signatures
                        .iter()
                        .filter(|signed_msg| {
                            bonded_validators.contains(&signed_msg.signature.validator)
                        })
                        .cloned()
                        .collect();

                // Collect all filtered validators that signed the DataProposal
                let filtered_validators: Vec<ValidatorPublicKey> = filtered_signatures
                    .iter()
                    .map(|s| s.signature.validator.clone())
                    .collect();

                // Compute their voting power to check if the DataProposal received enough votes
                let voting_power = staking.compute_voting_power(filtered_validators.as_slice());
                let f = staking.compute_f();
                if voting_power < f + 1 {
                    // Check if previous DataProposals received enough votes
                    if let Some(parent_dp_hash) = le.parent_data_proposal_hash.clone() {
                        dp_hash = parent_dp_hash;
                        continue;
                    }
                    return Ok(None);
                }

                // Aggregate the signatures in a PoDA
                let poda = match BlstCrypto::aggregate(
                    (dp_hash.clone(), le.cumul_size),
                    &filtered_signatures.iter().collect::<Vec<_>>(),
                ) {
                    Ok(poda) => poda,
                    Err(e) => {
                        error!(
                        "Could not aggregate signatures for validator {} and data proposal hash {}: {}",
                        lane_id, dp_hash, e
                    );
                        break;
                    }
                };
                return Ok(Some((dp_hash.clone(), le.cumul_size, poda.signature)));
            }
        }

        Ok(None)
    }

    /// Signs the data proposal before creating a new LaneEntry and putting it in the lane
    fn store_data_proposal(
        &mut self,
        crypto: &BlstCrypto,
        lane_id: &LaneId,
        data_proposal: DataProposal,
    ) -> Result<(DataProposalHash, LaneBytesSize)> {
        // Add DataProposal to validator's lane
        let data_proposal_hash = data_proposal.hashed();

        let dp_size = data_proposal.estimate_size();
        let lane_size = self.get_lane_size_tip(lane_id).cloned().unwrap_or_default();
        let cumul_size = lane_size + dp_size;

        let msg = (data_proposal_hash.clone(), cumul_size);
        let signatures = vec![crypto.sign(msg)?];

        // FIXME: Investigate if we can directly use put_no_verification
        self.put(
            lane_id.clone(),
            (
                LaneEntryMetadata {
                    parent_data_proposal_hash: data_proposal.parent_data_proposal_hash.clone(),
                    cumul_size,
                    signatures,
                },
                data_proposal,
            ),
        )?;
        Ok((data_proposal_hash, cumul_size))
    }

    // Implemented in the actual modules to potentially benefit from optimizations
    // in the underlying storage
    fn get_entries_between_hashes(
        &self,
        lane_id: &LaneId,
        from_data_proposal_hash: Option<DataProposalHash>,
        to_data_proposal_hash: Option<DataProposalHash>,
    ) -> impl Stream<Item = Result<(LaneEntryMetadata, DataProposal)>>;

    fn get_entries_metadata_between_hashes(
        &self,
        lane_id: &LaneId,
        from_data_proposal_hash: Option<DataProposalHash>,
        to_data_proposal_hash: Option<DataProposalHash>,
    ) -> impl Stream<Item = Result<(LaneEntryMetadata, DataProposalHash)>> {
        // If no dp hash is provided, we use the tip of the lane
        let initial_dp_hash: Option<DataProposalHash> =
            to_data_proposal_hash.or(self.get_lane_hash_tip(lane_id).cloned());
        try_stream! {
            if let Some(mut some_dp_hash) = initial_dp_hash {
                while Some(&some_dp_hash) != from_data_proposal_hash.as_ref() {
                    let lane_entry = self.get_metadata_by_hash(lane_id, &some_dp_hash)?;
                    match lane_entry {
                        Some(lane_entry) => {
                            yield (lane_entry.clone(), some_dp_hash);
                            if let Some(parent_dp_hash) = lane_entry.parent_data_proposal_hash.clone() {
                                some_dp_hash = parent_dp_hash;
                            } else {
                                break;
                            }
                        }
                        None => {
                            Err(anyhow::anyhow!("Local lane is incomplete: could not find DP {}", some_dp_hash))?;
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
    ) -> impl Stream<Item = Result<(LaneEntryMetadata, DataProposalHash)>> {
        let lane_tip = self.get_lane_hash_tip(lane_id);

        let last_committed_dp_hash = match last_cut {
            Some(cut) => cut
                .iter()
                .find(|(v, _, _, _)| v == lane_id)
                .map(|(_, dp, _, _)| dp.clone()),
            None => None,
        };
        self.get_entries_metadata_between_hashes(
            lane_id,
            last_committed_dp_hash.clone(),
            lane_tip.cloned(),
        )
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
        if tip_lane != previous_committed_dp_hash && tip_lane != Some(new_committed_dp_hash) {
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
    fn can_be_put_on_top(
        &mut self,
        lane_id: &LaneId,
        parent_data_proposal_hash: Option<&DataProposalHash>,
    ) -> CanBePutOnTop {
        // Data proposal parent hash needs to match the lane tip of that validator
        if parent_data_proposal_hash == self.get_lane_hash_tip(lane_id) {
            // LEGIT DATAPROPOSAL
            return CanBePutOnTop::Yes;
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
    use futures::StreamExt;
    use hyle_model::{DataSized, Signature, Transaction, ValidatorSignature};
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
            .put(lane_id.clone(), (entry.clone(), data_proposal.clone()))
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
            .put(lane_id.clone(), (entry.clone(), data_proposal.clone()))
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
            "{lane_id2}'s lane entry: {:?}",
            lane_entry
        );
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
        dbg!(&dp2);
        dbg!(&dp3);
        dbg!(&entries_from_1_to_end);
        assert_eq!(
            dp2,
            entries_from_1_to_end.last().unwrap().as_ref().unwrap().1
        );
        assert_eq!(
            dp3,
            entries_from_1_to_end.first().unwrap().as_ref().unwrap().1
        );

        // [start, 2] == [2, 1]
        let entries_from_start_to_2: Vec<_> = storage
            .get_entries_between_hashes(lane_id, None, Some(dp2.hashed()))
            .collect()
            .await;
        assert_eq!(2, entries_from_start_to_2.len());
        assert_eq!(
            dp1,
            entries_from_start_to_2.last().unwrap().as_ref().unwrap().1
        );
        assert_eq!(
            dp2,
            entries_from_start_to_2.first().unwrap().as_ref().unwrap().1
        );

        // ]1, 2] == [2]
        let entries_from_1_to_2: Vec<_> = storage
            .get_entries_between_hashes(lane_id, Some(dp1.hashed()), Some(dp2.hashed()))
            .collect()
            .await;
        assert_eq!(1, entries_from_1_to_2.len());
        assert_eq!(
            dp2,
            entries_from_1_to_2.first().unwrap().as_ref().unwrap().1
        );

        // ]1, 3] == [3, 2]
        let entries_from_1_to_3: Vec<_> = storage
            .get_entries_between_hashes(lane_id, Some(dp1.hashed()), None)
            .collect()
            .await;
        assert_eq!(2, entries_from_1_to_3.len());
        assert_eq!(dp2, entries_from_1_to_3.last().unwrap().as_ref().unwrap().1);
        assert_eq!(
            dp3,
            entries_from_1_to_3.first().unwrap().as_ref().unwrap().1
        );

        // ]1, 1[ == []
        let entries_from_1_to_1: Vec<_> = storage
            .get_entries_between_hashes(lane_id, Some(dp1.hashed()), Some(dp1.hashed()))
            .collect()
            .await;
        assert_eq!(0, entries_from_1_to_1.len());
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
        assert_eq!(&size, storage.get_lane_size_tip(lane_id).unwrap());
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
        assert_eq!(&size, storage.get_lane_size_tip(lane_id).unwrap());
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
            .put(lane_id.clone(), (entry, data_proposal))
            .unwrap();
        let pending: Vec<_> = storage
            .get_pending_entries_in_lane(lane_id, None)
            .collect()
            .await;
        assert_eq!(1, pending.len());
    }

    #[test_log::test(tokio::test)]
    async fn test_get_latest_car() {
        let crypto: BlstCrypto = BlstCrypto::new("1").unwrap();
        let lane_id = &LaneId(crypto.validator_pubkey().clone());
        let mut storage = setup_storage();
        let staking = Staking::new();
        let data_proposal = DataProposal::new(None, vec![]);
        let cumul_size: LaneBytesSize = LaneBytesSize(data_proposal.estimate_size() as u64);
        let entry = LaneEntryMetadata {
            parent_data_proposal_hash: None,
            cumul_size,
            signatures: vec![],
        };
        storage
            .put(lane_id.clone(), (entry, data_proposal))
            .unwrap();
        let latest = storage.get_latest_car(lane_id, &staking, None).unwrap();
        assert!(latest.is_none());
    }
}

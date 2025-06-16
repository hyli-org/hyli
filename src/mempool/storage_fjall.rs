use std::{collections::BTreeMap, path::Path};

use anyhow::{bail, Result};
use async_stream::try_stream;
use fjall::{
    Config, Keyspace, KvSeparationOptions, PartitionCreateOptions, PartitionHandle, Slice,
};
use futures::Stream;
use hyle_model::{DataSized, LaneId};
use tracing::info;

use crate::{
    mempool::storage::MetadataOrMissingHash,
    model::{DataProposal, DataProposalHash, Hashed},
};
use hyle_modules::log_warn;

use super::{
    storage::{CanBePutOnTop, EntryOrMissingHash, LaneEntryMetadata, Storage},
    ValidatorDAG,
};

pub use hyle_model::LaneBytesSize;

#[derive(Clone)]
pub struct LanesStorage {
    pub lanes_tip: BTreeMap<LaneId, (DataProposalHash, LaneBytesSize)>,
    db: Keyspace,
    pub by_hash_metadata: PartitionHandle,
    pub by_hash_data: PartitionHandle,
}

impl LanesStorage {
    /// Create another set of handles, without the data stored in lanes_tip, to have access and methods to access mempool storage
    pub fn new_handle(&self) -> LanesStorage {
        LanesStorage {
            lanes_tip: Default::default(),
            db: self.db.clone(),
            by_hash_metadata: self.by_hash_metadata.clone(),
            by_hash_data: self.by_hash_data.clone(),
        }
    }

    pub fn new(
        path: &Path,
        lanes_tip: BTreeMap<LaneId, (DataProposalHash, LaneBytesSize)>,
    ) -> Result<Self> {
        let db = Config::new(path)
            .cache_size(256 * 1024 * 1024)
            .max_journaling_size(512 * 1024 * 1024)
            .max_write_buffer_size(512 * 1024 * 1024)
            .open()?;

        let by_hash_metadata = db.open_partition(
            "dp_metadata",
            PartitionCreateOptions::default()
                .with_kv_separation(
                    KvSeparationOptions::default().file_target_size(256 * 1024 * 1024),
                )
                .block_size(32 * 1024)
                .manual_journal_persist(true)
                .max_memtable_size(128 * 1024 * 1024),
        )?;

        let by_hash_data = db.open_partition(
            "dp_data",
            PartitionCreateOptions::default()
                .with_kv_separation(
                    KvSeparationOptions::default().file_target_size(256 * 1024 * 1024),
                )
                .block_size(32 * 1024)
                .manual_journal_persist(true)
                .max_memtable_size(128 * 1024 * 1024),
        )?;

        info!("{} DP(s) available", by_hash_metadata.len()?);

        Ok(LanesStorage {
            lanes_tip,
            db,
            by_hash_metadata,
            by_hash_data,
        })
    }
}

impl Storage for LanesStorage {
    fn persist(&self) -> Result<()> {
        self.db
            .persist(fjall::PersistMode::Buffer)
            .map_err(Into::into)
    }

    fn contains(&self, lane_id: &LaneId, dp_hash: &DataProposalHash) -> bool {
        self.by_hash_metadata
            .contains_key(format!("{}:{}", lane_id, dp_hash))
            .unwrap_or(false)
    }

    fn get_metadata_by_hash(
        &self,
        lane_id: &LaneId,
        dp_hash: &DataProposalHash,
    ) -> Result<Option<LaneEntryMetadata>> {
        let item = log_warn!(
            self.by_hash_metadata
                .get(format!("{}:{}", lane_id, dp_hash)),
            "Can't find DP metadata {} for validator {}",
            dp_hash,
            lane_id
        )?;
        item.map(decode_metadata_from_item).transpose()
    }

    fn get_dp_by_hash(
        &self,
        lane_id: &LaneId,
        dp_hash: &DataProposalHash,
    ) -> Result<Option<DataProposal>> {
        let item = log_warn!(
            self.by_hash_data.get(format!("{}:{}", lane_id, dp_hash)),
            "Can't find DP data {} for validator {}",
            dp_hash,
            lane_id
        )?;
        item.map(|s| {
            decode_data_proposal_from_item(s).map(|mut dp| {
                // SAFETY: we trust our own fjall storage
                unsafe {
                    dp.unsafe_set_hash(dp_hash);
                }
                dp
            })
        })
        .transpose()
    }

    fn pop(
        &mut self,
        lane_id: LaneId,
    ) -> Result<Option<(DataProposalHash, (LaneEntryMetadata, DataProposal))>> {
        if let Some((lane_hash_tip, _)) = self.lanes_tip.get(&lane_id).cloned() {
            if let Some(lane_entry) = self.get_metadata_by_hash(&lane_id, &lane_hash_tip)? {
                self.by_hash_metadata
                    .remove(format!("{}:{}", lane_id, lane_hash_tip))?;
                // Check if have the data locally after regardless - if we don't, print an error but delete metadata anyways for consistency.
                let Some(dp) = self.get_dp_by_hash(&lane_id, &lane_hash_tip)? else {
                    bail!(
                        "Can't find DP data {} for lane {} where metadata could be found",
                        lane_hash_tip,
                        lane_id
                    );
                };
                self.by_hash_data
                    .remove(format!("{}:{}", lane_id, lane_hash_tip))?;
                self.update_lane_tip(lane_id, lane_hash_tip.clone(), lane_entry.cumul_size);
                return Ok(Some((lane_hash_tip, (lane_entry, dp))));
            }
        }
        Ok(None)
    }

    fn put(
        &mut self,
        lane_id: LaneId,
        (lane_entry, data_proposal): (LaneEntryMetadata, DataProposal),
    ) -> Result<()> {
        let dp_hash = data_proposal.hashed();

        if self.contains(&lane_id, &dp_hash) {
            bail!("DataProposal {} was already in lane", dp_hash);
        }

        match self.can_be_put_on_top(&lane_id, lane_entry.parent_data_proposal_hash.as_ref()) {
            CanBePutOnTop::No => bail!(
                "Can't store DataProposal {}, as parent is unknown ",
                dp_hash
            ),
            CanBePutOnTop::Yes => {
                // Add DataProposal metadata to validator's lane
                self.by_hash_metadata.insert(
                    format!("{}:{}", lane_id, dp_hash),
                    encode_metadata_to_item(lane_entry.clone())?,
                )?;

                // Add DataProposal data to validator's lane
                self.by_hash_data.insert(
                    format!("{}:{}", lane_id, dp_hash),
                    encode_data_proposal_to_item(data_proposal)?,
                )?;

                // Validator's lane tip is only updated if DP-chain is respected
                self.update_lane_tip(lane_id, dp_hash, lane_entry.cumul_size);

                Ok(())
            }
            CanBePutOnTop::Fork => {
                let last_known_hash = self.lanes_tip.get(&lane_id);
                bail!(
                    "DataProposal cannot be put in lane because it creates a fork: last dp hash {:?} while proposed parent_data_proposal_hash: {:?}",
                    last_known_hash,
                    lane_entry.parent_data_proposal_hash
                )
            }
        }
    }

    fn put_no_verification(
        &mut self,
        lane_id: LaneId,
        (lane_entry, data_proposal): (LaneEntryMetadata, DataProposal),
    ) -> Result<()> {
        let dp_hash = data_proposal.hashed();
        self.by_hash_metadata.insert(
            format!("{}:{}", lane_id, dp_hash),
            encode_metadata_to_item(lane_entry)?,
        )?;
        self.by_hash_data.insert(
            format!("{}:{}", lane_id, dp_hash),
            encode_data_proposal_to_item(data_proposal)?,
        )?;
        Ok(())
    }

    fn add_signatures<T: IntoIterator<Item = ValidatorDAG>>(
        &mut self,
        lane_id: &LaneId,
        dp_hash: &DataProposalHash,
        vote_msgs: T,
    ) -> Result<Vec<ValidatorDAG>> {
        let key = format!("{}:{}", lane_id, dp_hash);
        let Some(mut lem) = log_warn!(
            self.by_hash_metadata.get(key.clone()),
            "Can't find lane entry metadata {} for lane {}",
            dp_hash,
            lane_id
        )?
        .map(decode_metadata_from_item)
        .transpose()?
        else {
            bail!(
                "Can't find lane entry metadata {} for lane {}",
                dp_hash,
                lane_id
            );
        };

        for msg in vote_msgs {
            let (dph, cumul_size) = &msg.msg;
            if &lem.cumul_size != cumul_size || dp_hash != dph {
                tracing::warn!(
                    "Received a DataVote message with wrong hash or size: {:?}",
                    msg.msg
                );
                continue;
            }
            // Insert the new messages if they're not already in
            match lem
                .signatures
                .binary_search_by(|probe| probe.signature.cmp(&msg.signature))
            {
                Ok(_) => {}
                Err(pos) => lem.signatures.insert(pos, msg),
            }
        }
        let signatures = lem.signatures.clone();
        self.by_hash_metadata
            .insert(key, encode_metadata_to_item(lem)?)?;
        Ok(signatures)
    }

    fn get_lane_ids(&self) -> impl Iterator<Item = &LaneId> {
        self.lanes_tip.keys()
    }

    fn get_lane_hash_tip(&self, lane_id: &LaneId) -> Option<&DataProposalHash> {
        self.lanes_tip.get(lane_id).map(|(hash, _)| hash)
    }

    fn get_lane_size_tip(&self, lane_id: &LaneId) -> Option<&LaneBytesSize> {
        self.lanes_tip.get(lane_id).map(|(_, size)| size)
    }

    fn update_lane_tip(
        &mut self,
        lane_id: LaneId,
        dp_hash: DataProposalHash,
        size: LaneBytesSize,
    ) -> Option<(DataProposalHash, LaneBytesSize)> {
        tracing::trace!("Updating lane tip for lane {} to {:?}", lane_id, dp_hash);
        self.lanes_tip.insert(lane_id, (dp_hash, size))
    }

    fn get_entries_between_hashes(
        &self,
        lane_id: &LaneId,
        from_data_proposal_hash: Option<DataProposalHash>,
        to_data_proposal_hash: Option<DataProposalHash>,
    ) -> impl Stream<Item = anyhow::Result<EntryOrMissingHash>> {
        info!(
            "Getting entries between hashes for lane {}: from {:?} to {:?}",
            lane_id, from_data_proposal_hash, to_data_proposal_hash
        );
        let metadata_stream = self.get_entries_metadata_between_hashes(
            lane_id,
            from_data_proposal_hash,
            to_data_proposal_hash,
        );

        try_stream! {
            for await md in metadata_stream {
                match md? {
                    MetadataOrMissingHash::Metadata(metadata, dp_hash) => {
                        match self.get_dp_by_hash(lane_id, &dp_hash)? {
                            Some(data_proposal) => {
                                yield EntryOrMissingHash::Entry(metadata, data_proposal);
                            }
                            None => {
                                yield EntryOrMissingHash::MissingHash(dp_hash);
                                break;
                            }
                        }
                    }

                    MetadataOrMissingHash::MissingHash(hash) =>  {
                        yield EntryOrMissingHash::MissingHash(hash);
                        break;
                    }
                }
            }
        }
    }

    #[cfg(test)]
    fn remove_lane_entry(&mut self, lane_id: &LaneId, dp_hash: &DataProposalHash) {
        self.by_hash_metadata
            .remove(format!("{}:{}", lane_id, dp_hash))
            .unwrap();
        self.by_hash_data
            .remove(format!("{}:{}", lane_id, dp_hash))
            .unwrap();
    }

    fn get_latest_car(
        &self,
        lane_id: &LaneId,
        staking: &staking::state::Staking,
        previous_committed_car: Option<&(
            LaneId,
            DataProposalHash,
            LaneBytesSize,
            hyle_model::PoDA,
        )>,
    ) -> Result<Option<(DataProposalHash, LaneBytesSize, hyle_model::PoDA)>> {
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
                let filtered_signatures: Vec<
                    hyle_model::SignedByValidator<(DataProposalHash, LaneBytesSize)>,
                > = le
                    .signatures
                    .iter()
                    .filter(|signed_msg| {
                        bonded_validators.contains(&signed_msg.signature.validator)
                    })
                    .cloned()
                    .collect();

                // Collect all filtered validators that signed the DataProposal
                let filtered_validators: Vec<hyle_model::ValidatorPublicKey> = filtered_signatures
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
                let poda = match hyle_crypto::BlstCrypto::aggregate(
                    (dp_hash.clone(), le.cumul_size),
                    &filtered_signatures.iter().collect::<Vec<_>>(),
                ) {
                    Ok(poda) => poda,
                    Err(e) => {
                        tracing::error!(
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

    fn store_data_proposal(
        &mut self,
        crypto: &hyle_crypto::BlstCrypto,
        lane_id: &LaneId,
        data_proposal: DataProposal,
    ) -> Result<(DataProposalHash, LaneBytesSize)> {
        // Add DataProposal to validator's lane
        let data_proposal_hash = data_proposal.hashed();

        let dp_size = data_proposal.estimate_size();
        let lane_size = self.get_lane_size_tip(lane_id).cloned().unwrap_or_default();
        let cumul_size = lane_size + dp_size;

        let msg = (data_proposal_hash.clone(), cumul_size);
        let signatures = std::vec![crypto.sign(msg)?];

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

    fn get_entries_metadata_between_hashes(
        &self,
        lane_id: &LaneId,
        from_data_proposal_hash: Option<DataProposalHash>,
        to_data_proposal_hash: Option<DataProposalHash>,
    ) -> impl Stream<Item = Result<MetadataOrMissingHash>> {
        // If no dp hash is provided, we use the tip of the lane
        let initial_dp_hash: Option<DataProposalHash> =
            to_data_proposal_hash.or(self.get_lane_hash_tip(lane_id).cloned());
        try_stream! {
            if let Some(mut some_dp_hash) = initial_dp_hash {
                while Some(&some_dp_hash) != from_data_proposal_hash.as_ref() {
                    let lane_entry = self.get_metadata_by_hash(lane_id, &some_dp_hash)?;
                    match lane_entry {
                        Some(lane_entry) => {
                            yield MetadataOrMissingHash::Metadata(lane_entry.clone(), some_dp_hash.clone());
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
        last_cut: Option<hyle_model::Cut>,
    ) -> impl Stream<Item = Result<MetadataOrMissingHash>> {
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

fn decode_metadata_from_item(item: Slice) -> Result<LaneEntryMetadata> {
    borsh::from_slice(&item).map_err(Into::into)
}

fn encode_metadata_to_item(metadata: LaneEntryMetadata) -> Result<Slice> {
    borsh::to_vec(&metadata)
        .map(Slice::from)
        .map_err(Into::into)
}

fn decode_data_proposal_from_item(item: Slice) -> Result<DataProposal> {
    borsh::from_slice(&item).map_err(Into::into)
}

fn encode_data_proposal_to_item(data_proposal: DataProposal) -> Result<Slice> {
    borsh::to_vec(&data_proposal)
        .map(Slice::from)
        .map_err(Into::into)
}

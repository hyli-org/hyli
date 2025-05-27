use std::{
    collections::{BTreeMap, HashMap},
    path::Path,
};

use anyhow::{bail, Result};
use async_stream::try_stream;
use futures::Stream;
use hyle_model::{LaneBytesSize, LaneId};
use tracing::info;

use super::{
    storage::{CanBePutOnTop, LaneEntryMetadata, Storage},
    ValidatorDAG,
};
use crate::model::{DataProposal, DataProposalHash, Hashed};

#[derive(Default)]
pub struct LanesStorage {
    pub lanes_tip: BTreeMap<LaneId, (DataProposalHash, LaneBytesSize)>,
    // NB: do not iterate on these as they're unordered
    pub by_hash: HashMap<LaneId, HashMap<DataProposalHash, (LaneEntryMetadata, DataProposal)>>,
}

impl LanesStorage {
    pub fn new_handle(&self) -> LanesStorage {
        LanesStorage::default()
    }
    pub fn new(
        _path: &Path,
        lanes_tip: BTreeMap<LaneId, (DataProposalHash, LaneBytesSize)>,
    ) -> Result<Self> {
        // FIXME: load from disk
        let by_hash = HashMap::default();

        info!("{} DP(s) available", by_hash.len());

        Ok(LanesStorage { lanes_tip, by_hash })
    }
}

impl Storage for LanesStorage {
    fn persist(&self) -> Result<()> {
        Ok(())
    }

    fn contains(&self, lane_id: &LaneId, dp_hash: &DataProposalHash) -> bool {
        if let Some(lane) = self.by_hash.get(lane_id) {
            return lane.contains_key(dp_hash);
        }
        false
    }

    fn get_metadata_by_hash(
        &self,
        lane_id: &LaneId,
        dp_hash: &DataProposalHash,
    ) -> Result<Option<LaneEntryMetadata>> {
        if let Some(lane) = self.by_hash.get(lane_id) {
            return Ok(lane.get(dp_hash).map(|(metadata, _)| metadata.clone()));
        }
        bail!("Can't find lane with ID {}", lane_id)
    }
    fn get_dp_by_hash(
        &self,
        lane_id: &LaneId,
        dp_hash: &DataProposalHash,
    ) -> Result<Option<DataProposal>> {
        if let Some(lane) = self.by_hash.get(lane_id) {
            return Ok(lane.get(dp_hash).map(|(_, data)| data.clone()));
        }
        bail!("Can't find validator {}", lane_id)
    }

    fn pop(
        &mut self,
        validator: LaneId,
    ) -> Result<Option<(DataProposalHash, (LaneEntryMetadata, DataProposal))>> {
        if let Some((lane_tip, _)) = self.lanes_tip.get(&validator).cloned() {
            if let Some(lane) = self.by_hash.get_mut(&validator) {
                if let Some(entry) = lane.remove(&lane_tip) {
                    self.update_lane_tip(validator, lane_tip.clone(), entry.0.cumul_size);
                    return Ok(Some((lane_tip.clone(), entry)));
                }
            }
        }
        Ok(None)
    }

    fn put(&mut self, lane_id: LaneId, entry: (LaneEntryMetadata, DataProposal)) -> Result<()> {
        let dp_hash = entry.1.hashed();
        if self.contains(&lane_id, &dp_hash) {
            bail!("DataProposal {} was already in lane", dp_hash);
        }
        match self.can_be_put_on_top(&lane_id, entry.0.parent_data_proposal_hash.as_ref()) {
            CanBePutOnTop::No => bail!(
                "Can't store DataProposal {}, as parent is unknown ",
                dp_hash
            ),
            CanBePutOnTop::Yes => {
                // Add both metadata and data to validator's lane
                self.by_hash
                    .entry(lane_id.clone())
                    .or_default()
                    .insert(dp_hash.clone(), entry.clone());

                // Validator's lane tip is only updated if DP-chain is respected
                self.update_lane_tip(lane_id, dp_hash, entry.0.cumul_size);

                Ok(())
            }
            CanBePutOnTop::Fork => {
                let last_known_hash = self.lanes_tip.get(&lane_id);
                bail!(
                    "DataProposal cannot be put in lane because it creates a fork: last dp hash {:?} while proposed parent_data_proposal_hash: {:?}",
                    last_known_hash,
                    entry.0.parent_data_proposal_hash
                )
            }
        }
    }

    fn put_no_verification(
        &mut self,
        lane_id: LaneId,
        entry: (LaneEntryMetadata, DataProposal),
    ) -> Result<()> {
        let dp_hash = entry.1.hashed();
        self.by_hash
            .entry(lane_id)
            .or_default()
            .insert(dp_hash, entry);
        Ok(())
    }

    fn add_signatures<T: IntoIterator<Item = ValidatorDAG>>(
        &mut self,
        lane_id: &LaneId,
        dp_hash: &DataProposalHash,
        vote_msgs: T,
    ) -> Result<Vec<ValidatorDAG>> {
        let Some(lane) = self.by_hash.get_mut(lane_id) else {
            bail!("Can't find validator {}", lane_id);
        };

        let Some((mut metadata, data_proposal)) = lane.get(dp_hash).cloned() else {
            bail!("Can't find DP {} for validator {}", dp_hash, lane_id);
        };

        for msg in vote_msgs {
            let (dph, cumul_size) = &msg.msg;
            if &metadata.cumul_size != cumul_size || dp_hash != dph {
                tracing::warn!(
                    "Received a DataVote message with wrong hash or size: {:?}",
                    msg.msg
                );
                continue;
            }
            // Insert the new messages if they're not already in
            match metadata
                .signatures
                .binary_search_by(|probe| probe.signature.cmp(&msg.signature))
            {
                Ok(_) => {}
                Err(pos) => metadata.signatures.insert(pos, msg),
            }
        }
        let signatures = metadata.signatures.clone();
        lane.insert(dp_hash.clone(), (metadata, data_proposal));
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
        self.lanes_tip.insert(lane_id, (dp_hash, size))
    }

    fn get_entries_between_hashes(
        &self,
        lane_id: &LaneId,
        from_data_proposal_hash: Option<DataProposalHash>,
        to_data_proposal_hash: Option<DataProposalHash>,
    ) -> impl Stream<Item = Result<(LaneEntryMetadata, DataProposal)>> {
        let metadata_stream = self.get_entries_metadata_between_hashes(
            lane_id,
            from_data_proposal_hash,
            to_data_proposal_hash,
        );

        try_stream! {
            for await md in metadata_stream {
                let (metadata, dp_hash) = md?;

                let data_proposal = self.get_dp_by_hash(lane_id, &dp_hash)?.ok_or_else(|| {
                    anyhow::anyhow!("Data proposal {} not found in lane {}", dp_hash, lane_id)
                })?;

                yield (metadata, data_proposal);
            }
        }
    }

    #[cfg(test)]
    fn remove_lane_entry(&mut self, lane_id: &LaneId, dp_hash: &DataProposalHash) {
        if let Some(lane) = self.by_hash.get_mut(lane_id) {
            lane.remove(dp_hash);
        }
    }
}

use std::{
    collections::{BTreeMap, HashMap},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, OnceLock, RwLock},
};

use anyhow::{bail, Result};
use async_stream::try_stream;
use futures::Stream;
use hyli_model::{LaneBytesSize, LaneId, ProofData, TxHash};
use tracing::{info, warn};

use super::{
    storage::{EntryOrMissingHash, LaneEntryMetadata, Storage},
    ValidatorDAG,
};
use crate::{
    mempool::storage::MetadataOrMissingHash,
    model::{DataProposal, DataProposalHash, Hashed},
};

#[derive(Clone)]
#[allow(clippy::type_complexity)]
pub struct LanesStorage {
    pub lanes_tip: Arc<RwLock<BTreeMap<LaneId, (DataProposalHash, LaneBytesSize)>>>,
    // NB: do not iterate on these as they're unordered
    pub by_hash:
        Arc<RwLock<HashMap<LaneId, HashMap<DataProposalHash, (LaneEntryMetadata, DataProposal)>>>>,
    // Full proofs store: key = (dp_hash, tx_hash)
    pub proofs: Arc<RwLock<HashMap<LaneId, HashMap<DataProposalHash, HashMap<TxHash, ProofData>>>>>,
}

impl Default for LanesStorage {
    fn default() -> Self {
        LanesStorage {
            lanes_tip: Arc::new(RwLock::new(BTreeMap::default())),
            by_hash: Arc::new(RwLock::new(HashMap::default())),
            proofs: Arc::new(RwLock::new(HashMap::default())),
        }
    }
}

static SHARED_LANES: OnceLock<Mutex<HashMap<PathBuf, LanesStorage>>> = OnceLock::new();

/// Shared handle between mempool (read/write) and dissemination (read-only),
/// keyed by `config.data_dir` to allow multiple nodes in one process.
pub fn shared_lanes_storage(path: &Path) -> Result<LanesStorage> {
    let registry = SHARED_LANES.get_or_init(|| Mutex::new(HashMap::new()));
    let mut guard = registry
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());

    if let Some(existing) = guard.get(path) {
        tracing::debug!(
            "Reusing existing shared lanes storage at {}",
            path.to_string_lossy()
        );
        return Ok(existing.clone());
    }

    tracing::debug!(
        "Creating new shared lanes storage at {}",
        path.to_string_lossy()
    );

    let lanes_tip = load_lanes_tip(path);
    let storage = LanesStorage::new(path, lanes_tip)?;
    guard.insert(path.to_path_buf(), storage.clone());
    Ok(storage)
}

fn load_lanes_tip(path: &Path) -> BTreeMap<LaneId, (DataProposalHash, LaneBytesSize)> {
    let file = path.join("mempool_lanes_tip.bin");
    match std::fs::File::open(&file) {
        Ok(mut reader) => match borsh::from_reader(&mut reader) {
            Ok(data) => {
                info!("Loaded data from disk {}", file.to_string_lossy());
                data
            }
            Err(err) => {
                warn!(
                    "Failed to load lanes tip from {}: {}",
                    file.to_string_lossy(),
                    err
                );
                BTreeMap::default()
            }
        },
        Err(_) => {
            info!(
                "File {} not found for lanes tip (using default)",
                file.to_string_lossy()
            );
            BTreeMap::default()
        }
    }
}

impl LanesStorage {
    pub fn new_handle(&self) -> LanesStorage {
        LanesStorage {
            lanes_tip: Arc::clone(&self.lanes_tip),
            by_hash: Arc::clone(&self.by_hash),
            proofs: Arc::clone(&self.proofs),
        }
    }

    pub fn lane_tips_snapshot(&self) -> BTreeMap<LaneId, (DataProposalHash, LaneBytesSize)> {
        #[allow(clippy::unwrap_used, reason = "RwLock cannot be poisoned in our usage")]
        let guard = self.lanes_tip.read().unwrap();
        guard.clone()
    }

    pub fn lane_tips_read(
        &self,
    ) -> std::sync::RwLockReadGuard<'_, BTreeMap<LaneId, (DataProposalHash, LaneBytesSize)>> {
        #[allow(clippy::unwrap_used, reason = "RwLock cannot be poisoned in our usage")]
        self.lanes_tip.read().unwrap()
    }
    pub fn new(
        _path: &Path,
        lanes_tip: BTreeMap<LaneId, (DataProposalHash, LaneBytesSize)>,
    ) -> Result<Self> {
        // FIXME: load from disk
        let by_hash = HashMap::default();

        info!("{} DP(s) available", by_hash.len());

        Ok(LanesStorage {
            lanes_tip: Arc::new(RwLock::new(lanes_tip)),
            by_hash: Arc::new(RwLock::new(by_hash)),
            proofs: Arc::new(RwLock::new(HashMap::default())),
        })
    }
}

impl Storage for LanesStorage {
    fn persist(&self) -> Result<()> {
        Ok(())
    }

    fn contains(&self, lane_id: &LaneId, dp_hash: &DataProposalHash) -> bool {
        #[allow(clippy::unwrap_used, reason = "RwLock cannot be poisoned in our usage")]
        let guard = self.by_hash.read().unwrap();
        guard
            .get(lane_id)
            .map(|lane| lane.contains_key(dp_hash))
            .unwrap_or(false)
    }

    fn get_metadata_by_hash(
        &self,
        lane_id: &LaneId,
        dp_hash: &DataProposalHash,
    ) -> Result<Option<LaneEntryMetadata>> {
        #[allow(clippy::unwrap_used, reason = "RwLock cannot be poisoned in our usage")]
        let guard = self.by_hash.read().unwrap();
        Ok(guard
            .get(lane_id)
            .and_then(|lane| lane.get(dp_hash).map(|(metadata, _)| metadata.clone())))
    }
    fn get_dp_by_hash(
        &self,
        lane_id: &LaneId,
        dp_hash: &DataProposalHash,
    ) -> Result<Option<DataProposal>> {
        #[allow(clippy::unwrap_used, reason = "RwLock cannot be poisoned in our usage")]
        let guard = self.by_hash.read().unwrap();
        Ok(guard
            .get(lane_id)
            .and_then(|lane| lane.get(dp_hash).map(|(_, data)| data.clone())))
    }

    fn get_proofs_by_hash(
        &self,
        lane_id: &LaneId,
        dp_hash: &DataProposalHash,
    ) -> Result<Option<HashMap<TxHash, ProofData>>> {
        #[allow(clippy::unwrap_used, reason = "RwLock cannot be poisoned in our usage")]
        let guard = self.proofs.read().unwrap();
        Ok(guard
            .get(lane_id)
            .and_then(|dp_map| dp_map.get(dp_hash))
            .cloned())
    }

    fn delete_proofs(&mut self, lane_id: &LaneId, dp_hash: &DataProposalHash) -> Result<()> {
        #[allow(clippy::unwrap_used, reason = "RwLock cannot be poisoned in our usage")]
        let mut guard = self.proofs.write().unwrap();
        if let Some(dp_map) = guard.get_mut(lane_id) {
            dp_map.remove(dp_hash);
        }
        Ok(())
    }

    fn pop(
        &mut self,
        validator: LaneId,
    ) -> Result<Option<(DataProposalHash, (LaneEntryMetadata, DataProposal))>> {
        let lane_tip = {
            #[allow(clippy::unwrap_used, reason = "RwLock cannot be poisoned in our usage")]
            let guard = self.lanes_tip.read().unwrap();
            guard.get(&validator).cloned()
        };
        if let Some((lane_tip, _)) = lane_tip {
            let entry = {
                #[allow(clippy::unwrap_used, reason = "RwLock cannot be poisoned in our usage")]
                let mut guard = self.by_hash.write().unwrap();
                guard
                    .get_mut(&validator)
                    .and_then(|lane| lane.remove(&lane_tip))
            };
            if let Some(entry) = entry {
                self.update_lane_tip(validator, lane_tip.clone(), entry.0.cumul_size);
                return Ok(Some((lane_tip.clone(), entry)));
            }
        }
        Ok(None)
    }

    fn put_no_verification(
        &mut self,
        lane_id: LaneId,
        mut entry: (LaneEntryMetadata, DataProposal),
    ) -> Result<()> {
        let dp_hash = entry.1.hashed();
        // Save full proofs separately and strip them from the stored DataProposal
        let proofs = entry.1.take_proofs();
        #[allow(clippy::unwrap_used, reason = "RwLock cannot be poisoned in our usage")]
        let mut guard = self.by_hash.write().unwrap();
        guard
            .entry(lane_id.clone())
            .or_default()
            .insert(dp_hash.clone(), entry);
        #[allow(clippy::unwrap_used, reason = "RwLock cannot be poisoned in our usage")]
        let mut proofs_guard = self.proofs.write().unwrap();
        proofs_guard
            .entry(lane_id)
            .or_default()
            .insert(dp_hash, proofs);
        Ok(())
    }

    fn add_signatures<T: IntoIterator<Item = ValidatorDAG>>(
        &mut self,
        lane_id: &LaneId,
        dp_hash: &DataProposalHash,
        vote_msgs: T,
    ) -> Result<Vec<ValidatorDAG>> {
        #[allow(clippy::unwrap_used, reason = "RwLock cannot be poisoned in our usage")]
        let mut guard = self.by_hash.write().unwrap();
        let Some(lane) = guard.get_mut(lane_id) else {
            bail!("Can't find validator {}", lane_id);
        };

        let Some((metadata, _data_proposal)) = lane.get_mut(dp_hash) else {
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
                Ok(_) => {
                    tracing::trace!(
                        "Received duplicate DataVote message for {dph} from {}",
                        msg.signature.validator
                    );
                }
                Err(pos) => metadata.signatures.insert(pos, msg),
            }
        }
        Ok(metadata.signatures.clone())
    }

    fn get_lane_ids(&self) -> Vec<LaneId> {
        #[allow(clippy::unwrap_used, reason = "RwLock cannot be poisoned in our usage")]
        let guard = self.lanes_tip.read().unwrap();
        guard.keys().cloned().collect()
    }

    fn get_lane_hash_tip(&self, lane_id: &LaneId) -> Option<DataProposalHash> {
        #[allow(clippy::unwrap_used, reason = "RwLock cannot be poisoned in our usage")]
        let guard = self.lanes_tip.read().unwrap();
        guard.get(lane_id).map(|(hash, _)| hash.clone())
    }

    fn get_lane_size_tip(&self, lane_id: &LaneId) -> Option<LaneBytesSize> {
        #[allow(clippy::unwrap_used, reason = "RwLock cannot be poisoned in our usage")]
        let guard = self.lanes_tip.read().unwrap();
        guard.get(lane_id).map(|(_, size)| *size)
    }

    fn update_lane_tip(
        &mut self,
        lane_id: LaneId,
        dp_hash: DataProposalHash,
        size: LaneBytesSize,
    ) -> Option<(DataProposalHash, LaneBytesSize)> {
        tracing::trace!("Updating lane tip for lane {} to {:?}", lane_id, dp_hash);
        #[allow(clippy::unwrap_used, reason = "RwLock cannot be poisoned in our usage")]
        let mut guard = self.lanes_tip.write().unwrap();
        guard.insert(lane_id, (dp_hash, size))
    }

    fn get_entries_between_hashes(
        &self,
        lane_id: &LaneId,
        from_data_proposal_hash: Option<DataProposalHash>,
        to_data_proposal_hash: Option<DataProposalHash>,
    ) -> impl Stream<Item = Result<EntryOrMissingHash>> {
        let metadata_stream = self.get_entries_metadata_between_hashes(
            lane_id,
            from_data_proposal_hash,
            to_data_proposal_hash,
        );

        try_stream! {
            for await md in metadata_stream {
                match md? {
                    MetadataOrMissingHash::MissingHash(dp_hash) => {
                        yield EntryOrMissingHash::MissingHash(dp_hash);
                        break;
                    }
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
                }
            }
        }
    }

    #[cfg(test)]
    fn remove_lane_entry(&mut self, lane_id: &LaneId, dp_hash: &DataProposalHash) {
        #[allow(clippy::unwrap_used, reason = "RwLock cannot be poisoned in our usage")]
        let mut guard = self.by_hash.write().unwrap();
        if let Some(lane) = guard.get_mut(lane_id) {
            lane.remove(dp_hash);
        }
    }
}

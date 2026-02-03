use std::{
    collections::{BTreeMap, HashMap},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, OnceLock, RwLock},
};

use anyhow::{bail, Result};
use async_stream::try_stream;
use fjall::{Database, Keyspace, KeyspaceCreateOptions, KvSeparationOptions, Slice};
use futures::Stream;
use hyli_model::{LaneId, ProofData, TxHash};
use tracing::{info, warn};

use crate::{
    mempool::storage::MetadataOrMissingHash,
    model::{DataProposal, DataProposalHash, Hashed, PoDA},
};
use hyli_modules::log_warn;

use super::{
    storage::{EntryOrMissingHash, LaneEntryMetadata, Storage},
    ValidatorDAG,
};

pub use hyli_model::LaneBytesSize;

#[derive(Clone)]
pub struct LanesStorage {
    lanes_tip: Arc<RwLock<BTreeMap<LaneId, (DataProposalHash, LaneBytesSize)>>>,
    db: Database,
    by_hash_metadata: Keyspace,
    by_hash_data: Keyspace,
    dp_proofs: Keyspace,
    // Used by the shared storage to know when it can drop this handle.
    ref_token: Arc<()>,
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

    // Drop cached storages that are only held by this map (ref_count == 1).
    // This allowes closing opened files and avoid breaking OS limits during tests.
    guard.retain(|_, storage| storage.ref_count() > 1);

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
    fn ref_count(&self) -> usize {
        Arc::strong_count(&self.ref_token)
    }

    /// Create another set of handles to share the same storage and lane tip view.
    pub fn new_handle(&self) -> LanesStorage {
        self.clone()
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
        path: &Path,
        lanes_tip: BTreeMap<LaneId, (DataProposalHash, LaneBytesSize)>,
    ) -> Result<Self> {
        let db = Database::builder(path)
            .cache_size(256 * 1024 * 1024)
            .max_journaling_size(512 * 1024 * 1024)
            .open()?;

        let by_hash_metadata = db.keyspace("dp_metadata", || {
            KeyspaceCreateOptions::default()
                .with_kv_separation(Some(
                    KvSeparationOptions::default().file_target_size(256 * 1024 * 1024),
                ))
                .manual_journal_persist(true)
                .max_memtable_size(128 * 1024 * 1024)
        })?;

        let by_hash_data = db.keyspace("dp_data", || {
            KeyspaceCreateOptions::default()
                .with_kv_separation(Some(
                    KvSeparationOptions::default().file_target_size(256 * 1024 * 1024),
                ))
                .manual_journal_persist(true)
                .max_memtable_size(128 * 1024 * 1024)
        })?;

        let dp_proofs = db.keyspace("dp_proofs", || {
            KeyspaceCreateOptions::default()
                .with_kv_separation(Some(
                    KvSeparationOptions::default().file_target_size(256 * 1024 * 1024),
                ))
                .manual_journal_persist(true)
                .max_memtable_size(64 * 1024 * 1024)
        })?;

        info!("{} DP(s) available", by_hash_metadata.len()?);

        Ok(LanesStorage {
            lanes_tip: Arc::new(RwLock::new(lanes_tip)),
            db,
            by_hash_metadata,
            by_hash_data,
            dp_proofs,
            ref_token: Arc::new(()),
        })
    }

    #[cfg(test)]
    pub fn put_metadata_only(
        &mut self,
        lane_id: &LaneId,
        dp_hash: &DataProposalHash,
        metadata: LaneEntryMetadata,
    ) -> Result<()> {
        Ok(self
            .by_hash_metadata
            .insert(format!("{lane_id}:{dp_hash}"), borsh::to_vec(&metadata)?)?)
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
            .contains_key(format!("{lane_id}:{dp_hash}"))
            .unwrap_or(false)
    }

    fn get_metadata_by_hash(
        &self,
        lane_id: &LaneId,
        dp_hash: &DataProposalHash,
    ) -> Result<Option<LaneEntryMetadata>> {
        let item = log_warn!(
            self.by_hash_metadata.get(format!("{lane_id}:{dp_hash}")),
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
            self.by_hash_data.get(format!("{lane_id}:{dp_hash}")),
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

    fn get_proofs_by_hash(
        &self,
        lane_id: &LaneId,
        dp_hash: &DataProposalHash,
    ) -> Result<Option<HashMap<TxHash, ProofData>>> {
        let item = log_warn!(
            self.dp_proofs.get(format!("{lane_id}:{dp_hash}")),
            "Can't find DP proofs {} for validator {}",
            dp_hash,
            lane_id
        )?;
        item.map(|s| borsh::from_slice(&s).map_err(Into::into))
            .transpose()
    }

    fn delete_proofs(&mut self, lane_id: &LaneId, dp_hash: &DataProposalHash) -> Result<()> {
        self.dp_proofs.remove(format!("{lane_id}:{dp_hash}"))?;
        // NOTE: Garbage collection is now automatic in fjall 3.0
        Ok(())
    }

    fn pop(
        &mut self,
        lane_id: LaneId,
    ) -> Result<Option<(DataProposalHash, (LaneEntryMetadata, DataProposal))>> {
        if let Some(lane_hash_tip) = self.get_lane_hash_tip(&lane_id) {
            if let Some(lane_entry) = self.get_metadata_by_hash(&lane_id, &lane_hash_tip)? {
                self.by_hash_metadata
                    .remove(format!("{lane_id}:{lane_hash_tip}"))?;
                // Check if have the data locally after regardless - if we don't, print an error but delete metadata anyways for consistency.
                let Some(dp) = self.get_dp_by_hash(&lane_id, &lane_hash_tip)? else {
                    bail!(
                        "Can't find DP data {} for lane {} where metadata could be found",
                        lane_hash_tip,
                        lane_id
                    );
                };
                self.by_hash_data
                    .remove(format!("{lane_id}:{lane_hash_tip}"))?;
                self.update_lane_tip(lane_id, lane_hash_tip.clone(), lane_entry.cumul_size);
                return Ok(Some((lane_hash_tip, (lane_entry, dp))));
            }
        }
        Ok(None)
    }

    fn put_no_verification(
        &mut self,
        lane_id: LaneId,
        (lane_entry, data_proposal): (LaneEntryMetadata, DataProposal),
    ) -> Result<()> {
        let dp_hash = data_proposal.hashed();
        let mut dp_to_store = data_proposal;
        // Save full proofs separately and strip them from the stored DataProposal
        let proofs = dp_to_store.take_proofs();
        let key = format!("{lane_id}:{dp_hash}");
        let metadata = encode_metadata_to_item(lane_entry)?;
        let data = encode_data_proposal_to_item(dp_to_store)?;
        let proofs = Slice::from(borsh::to_vec(&proofs)?);

        let mut batch = self.db.batch();
        batch.insert(&self.by_hash_metadata, key.clone(), metadata);
        batch.insert(&self.by_hash_data, key.clone(), data);
        batch.insert(&self.dp_proofs, key, proofs);
        batch.commit().map_err(Into::into)
    }

    fn add_signatures<T: IntoIterator<Item = ValidatorDAG>>(
        &mut self,
        lane_id: &LaneId,
        dp_hash: &DataProposalHash,
        vote_msgs: T,
    ) -> Result<Vec<ValidatorDAG>> {
        let key = format!("{lane_id}:{dp_hash}");
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

    fn set_cached_poda(
        &mut self,
        lane_id: &LaneId,
        dp_hash: &DataProposalHash,
        poda: PoDA,
    ) -> Result<()> {
        let key = format!("{lane_id}:{dp_hash}");
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

        lem.cached_poda = Some(poda);
        self.by_hash_metadata
            .insert(key, encode_metadata_to_item(lem)?)?;
        Ok(())
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
    ) -> impl Stream<Item = anyhow::Result<EntryOrMissingHash>> {
        tracing::trace!(
            "Getting entries between hashes for lane {}: from {:?} to {:?}",
            lane_id,
            from_data_proposal_hash,
            to_data_proposal_hash
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
            .remove(format!("{lane_id}:{dp_hash}"))
            .unwrap();
        self.by_hash_data
            .remove(format!("{lane_id}:{dp_hash}"))
            .unwrap();
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

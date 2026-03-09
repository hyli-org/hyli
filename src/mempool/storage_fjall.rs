use std::{
    collections::{BTreeMap, HashMap},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, OnceLock, RwLock},
};

use anyhow::{bail, Result};
use async_stream::try_stream;
use borsh::{BorshDeserialize, BorshSerialize};
use fjall::{
    Database, Keyspace, KeyspaceCreateOptions, KvSeparationOptions, OwnedWriteBatch, Slice,
};
use futures::Stream;
use hyli_model::{BlockHeight, DataSized, LaneId, ProofData};
use hyli_modules::utils::fjall_metrics::FjallMetrics;
use std::time::Instant;
use tracing::{info, warn};

use crate::{
    mempool::storage::MetadataOrMissingHash,
    model::{
        AggregateSignature, ConsensusProposal, ConsensusProposalHash, DataProposal,
        DataProposalHash, Hashed, PoDA, SignedBlock,
    },
};
use hyli_modules::log_warn;

use super::{
    storage::{EntryOrMissingHash, LaneEntryMetadata, Storage},
    ValidatorDAG,
};

mod blocks;

use self::blocks::{FjallHashKey, FjallHeightKey, FjallValue};

pub use hyli_model::LaneBytesSize;

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize, PartialEq, Eq)]
pub struct StoredSignedBlock {
    pub consensus_proposal: ConsensusProposal,
    pub certificate: AggregateSignature,
    // We only persist DP hashes here. Full DPs live in the lane store, where proofs are
    // intentionally stripped and kept in the side proof store.
    pub data_proposals: Vec<(LaneId, Vec<DataProposalHash>)>,
}

impl StoredSignedBlock {
    pub fn height(&self) -> BlockHeight {
        BlockHeight(self.consensus_proposal.slot)
    }
}

impl From<SignedBlock> for StoredSignedBlock {
    fn from(value: SignedBlock) -> Self {
        Self::from(&value)
    }
}

impl From<&SignedBlock> for StoredSignedBlock {
    fn from(value: &SignedBlock) -> Self {
        StoredSignedBlock {
            consensus_proposal: value.consensus_proposal.clone(),
            certificate: value.certificate.clone(),
            data_proposals: value
                .data_proposals
                .iter()
                .map(|(lane_id, dps)| {
                    (
                        lane_id.clone(),
                        dps.iter().map(crate::model::Hashed::hashed).collect(),
                    )
                })
                .collect(),
        }
    }
}

#[derive(Clone)]
pub struct LanesStorage {
    lanes_tip: Arc<RwLock<BTreeMap<LaneId, (DataProposalHash, LaneBytesSize)>>>,
    db: Database,
    by_hash_metadata: Keyspace,
    by_hash_data: Keyspace,
    dp_proofs: Keyspace,
    blocks_by_hash: Keyspace,
    block_hashes_by_height: Keyspace,
    metrics: FjallMetrics,
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

    pub fn record_metrics(&self) {
        self.metrics.record_db(&self.db);
        self.metrics
            .record_keyspace("dp_metadata", &self.by_hash_metadata);
        self.metrics.record_keyspace("dp_data", &self.by_hash_data);
        self.metrics.record_keyspace("dp_proofs", &self.dp_proofs);
        self.metrics
            .record_keyspace("blocks_by_hash", &self.blocks_by_hash);
        self.metrics
            .record_keyspace("block_hashes_by_height", &self.block_hashes_by_height);
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

        let blocks_by_hash = db.keyspace("blocks_by_hash", || {
            KeyspaceCreateOptions::default()
                .with_kv_separation(Some(
                    KvSeparationOptions::default().file_target_size(256 * 1024 * 1024),
                ))
                .manual_journal_persist(true)
                .max_memtable_size(128 * 1024 * 1024)
        })?;

        let block_hashes_by_height =
            db.keyspace("block_hashes_by_height", KeyspaceCreateOptions::default)?;

        info!("{} DP(s) available", by_hash_metadata.len()?);

        Ok(LanesStorage {
            lanes_tip: Arc::new(RwLock::new(lanes_tip)),
            db,
            by_hash_metadata,
            by_hash_data,
            dp_proofs,
            blocks_by_hash,
            block_hashes_by_height,
            metrics: FjallMetrics::global("mempool", "unknown", "mempool"),
            ref_token: Arc::new(()),
        })
    }

    pub fn set_metrics_context(&mut self, node_id: impl Into<String>) {
        self.metrics = FjallMetrics::global("mempool", node_id, "mempool");
    }

    pub fn record_op(
        &self,
        op: &'static str,
        keyspace: &'static str,
        elapsed: std::time::Duration,
    ) {
        self.metrics
            .record_op(op, keyspace, elapsed.as_micros() as u64);
    }

    pub fn store_signed_block(&mut self, block: &SignedBlock) -> Result<()> {
        let start = Instant::now();
        let mut batch = self.db.batch();

        for (lane_id, dps) in &block.data_proposals {
            if dps.is_empty() {
                continue;
            }

            let final_cumul_size = block
                .consensus_proposal
                .cut
                .iter()
                .find(|(cut_lane_id, _, _, _)| cut_lane_id == lane_id)
                .map(|(_, _, cumul_size, _)| *cumul_size)
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "Missing cut entry for lane {lane_id} while storing block {}",
                        block.hashed()
                    )
                })?;

            // Block payloads are proof-stripped before they reach DA storage, but
            // `VerifiedProofTransaction::estimate_size()` still accounts for the original
            // proof bytes via its persisted `proof_size` field. That lets us reconstruct the
            // original per-DP cumulative sizes without storing duplicate size metadata.
            let total_segment_size =
                LaneBytesSize(dps.iter().map(|dp| dp.estimate_size() as u64).sum());
            let mut running_cumul_size = LaneBytesSize(
                final_cumul_size
                    .0
                    .checked_sub(total_segment_size.0)
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "Invalid cut size {} for lane {lane_id} in block {}",
                            final_cumul_size.0,
                            block.hashed()
                        )
                    })?,
            );

            for dp in dps.iter().cloned() {
                running_cumul_size =
                    LaneBytesSize(running_cumul_size.0 + dp.estimate_size() as u64);
                self.stage_committed_data_proposal(&mut batch, lane_id, dp, running_cumul_size)?;
            }
        }

        let block = StoredSignedBlock::from(block);
        let block_hash = block.consensus_proposal.hashed();
        let block_height = block.height();
        let height_key = FjallHeightKey::new(block_height);

        if self.contains_block(&block_hash) {
            match self.block_hashes_by_height.get(height_key.as_ref())? {
                Some(existing_hash_value) => {
                    let existing_hash: ConsensusProposalHash =
                        borsh::from_slice(&existing_hash_value)?;
                    if existing_hash != block_hash {
                        bail!(
                            "Conflicting block index for height {}: existing {}, new {}",
                            block_height,
                            existing_hash,
                            block_hash
                        );
                    }
                }
                None => {
                    batch.insert(
                        &self.block_hashes_by_height,
                        height_key.as_ref(),
                        FjallValue::new_with_block_hash(&block_hash)?.0,
                    );
                }
            }
        } else {
            if let Some(existing_hash_value) =
                self.block_hashes_by_height.get(height_key.as_ref())?
            {
                let existing_hash: ConsensusProposalHash = borsh::from_slice(&existing_hash_value)?;
                if existing_hash != block_hash {
                    bail!(
                        "Conflicting block index for height {}: existing {}, new {}",
                        block_height,
                        existing_hash,
                        block_hash
                    );
                }
            }

            batch.insert(
                &self.blocks_by_hash,
                FjallHashKey(block_hash.clone()).as_ref(),
                FjallValue::new_with_block(&block)?.0,
            );
            batch.insert(
                &self.block_hashes_by_height,
                height_key.as_ref(),
                FjallValue::new_with_block_hash(&block_hash)?.0,
            );
        }

        batch.commit()?;

        self.metrics.record_op(
            "store_signed_block",
            "batch",
            start.elapsed().as_micros() as u64,
        );
        Ok(())
    }

    fn stage_committed_data_proposal(
        &self,
        batch: &mut OwnedWriteBatch,
        lane_id: &LaneId,
        data_proposal: DataProposal,
        cumul_size: LaneBytesSize,
    ) -> Result<()> {
        let data_proposal_hash = data_proposal.hashed();
        let new_metadata = LaneEntryMetadata {
            parent_data_proposal_hash: data_proposal.parent_data_proposal_hash.clone(),
            cumul_size,
            signatures: vec![],
            cached_poda: None,
        };

        let existing_metadata = self.get_metadata_by_hash(lane_id, &data_proposal_hash)?;
        let payload_exists = self.get_dp_by_hash(lane_id, &data_proposal_hash)?.is_some();
        let metadata = existing_metadata
            .clone()
            .map(|existing| LaneEntryMetadata {
                parent_data_proposal_hash: new_metadata.parent_data_proposal_hash.clone(),
                cumul_size: new_metadata.cumul_size,
                signatures: existing.signatures,
                cached_poda: existing.cached_poda,
            })
            .unwrap_or(new_metadata);

        match (existing_metadata.is_some(), payload_exists) {
            (true, true) => Ok(()),
            (false, true) => {
                anyhow::bail!(
                    "Inconsistent lane state for {lane_id}:{data_proposal_hash}: payload exists without metadata"
                );
            }
            (true, false) | (false, false) => {
                let mut dp_to_store = data_proposal;
                let proofs = dp_to_store.take_proofs();
                let key = format!("{lane_id}:{data_proposal_hash}");
                let metadata = encode_metadata_to_item(metadata)?;
                let data = encode_data_proposal_to_item(dp_to_store)?;
                let proofs = Slice::from(borsh::to_vec(&proofs)?);

                batch.insert(&self.by_hash_metadata, key.clone(), metadata);
                batch.insert(&self.by_hash_data, key.clone(), data);
                batch.insert(&self.dp_proofs, key, proofs);
                Ok(())
            }
        }
    }

    #[cfg(test)]
    pub fn put_metadata_only(
        &mut self,
        lane_id: &LaneId,
        dp_hash: &DataProposalHash,
        metadata: LaneEntryMetadata,
    ) -> Result<()> {
        let start = Instant::now();
        let res = self
            .by_hash_metadata
            .insert(format!("{lane_id}:{dp_hash}"), borsh::to_vec(&metadata)?)
            .map_err(Into::into);
        self.metrics.record_op(
            "put_metadata_only",
            "dp_metadata",
            start.elapsed().as_micros() as u64,
        );
        res
    }
}

impl Storage for LanesStorage {
    fn persist(&self) -> Result<()> {
        let start = Instant::now();
        let res = self
            .db
            .persist(fjall::PersistMode::Buffer)
            .map_err(Into::into);
        self.metrics
            .record_op("persist", "db", start.elapsed().as_micros() as u64);
        res
    }

    fn contains(&self, lane_id: &LaneId, dp_hash: &DataProposalHash) -> bool {
        let start = Instant::now();
        let res = self
            .by_hash_metadata
            .contains_key(format!("{lane_id}:{dp_hash}"))
            .unwrap_or(false);
        self.metrics.record_op(
            "contains",
            "dp_metadata",
            start.elapsed().as_micros() as u64,
        );
        res
    }

    fn get_metadata_by_hash(
        &self,
        lane_id: &LaneId,
        dp_hash: &DataProposalHash,
    ) -> Result<Option<LaneEntryMetadata>> {
        let start = Instant::now();
        let item = log_warn!(
            self.by_hash_metadata.get(format!("{lane_id}:{dp_hash}")),
            "Can't find DP metadata {} for validator {}",
            dp_hash,
            lane_id
        )?;
        let res = item.map(decode_metadata_from_item).transpose();
        self.metrics.record_op(
            "get_metadata_by_hash",
            "dp_metadata",
            start.elapsed().as_micros() as u64,
        );
        res
    }

    fn get_dp_by_hash(
        &self,
        lane_id: &LaneId,
        dp_hash: &DataProposalHash,
    ) -> Result<Option<DataProposal>> {
        let start = Instant::now();
        let item = log_warn!(
            self.by_hash_data.get(format!("{lane_id}:{dp_hash}")),
            "Can't find DP data {} for validator {}",
            dp_hash,
            lane_id
        )?;
        let res = item
            .map(|s| {
                decode_data_proposal_from_item(s).map(|mut dp| {
                    // SAFETY: we trust our own fjall storage
                    unsafe {
                        dp.unsafe_set_hash(dp_hash);
                    }
                    dp
                })
            })
            .transpose();
        self.metrics.record_op(
            "get_dp_by_hash",
            "dp_data",
            start.elapsed().as_micros() as u64,
        );
        res
    }

    fn get_proofs_by_hash(
        &self,
        lane_id: &LaneId,
        dp_hash: &DataProposalHash,
    ) -> Result<Option<Vec<(u64, ProofData)>>> {
        let start = Instant::now();
        let item = log_warn!(
            self.dp_proofs.get(format!("{lane_id}:{dp_hash}")),
            "Can't find DP proofs {} for validator {}",
            dp_hash,
            lane_id
        )?;
        let res = item
            .map(|s| borsh::from_slice(&s).map_err(Into::into))
            .transpose();
        self.metrics.record_op(
            "get_proofs_by_hash",
            "dp_proofs",
            start.elapsed().as_micros() as u64,
        );
        res
    }

    fn delete_proofs(&mut self, lane_id: &LaneId, dp_hash: &DataProposalHash) -> Result<()> {
        let start = Instant::now();
        self.dp_proofs.remove(format!("{lane_id}:{dp_hash}"))?;
        // NOTE: Garbage collection is now automatic in fjall 3.0
        self.metrics.record_op(
            "delete_proofs",
            "dp_proofs",
            start.elapsed().as_micros() as u64,
        );
        Ok(())
    }

    fn remove_by_hash(
        &mut self,
        lane_id: &LaneId,
        dp_hash: &DataProposalHash,
    ) -> Result<Option<(DataProposalHash, (LaneEntryMetadata, DataProposal))>> {
        let start = Instant::now();
        if let Some(lane_entry) = self.get_metadata_by_hash(lane_id, dp_hash)? {
            self.by_hash_metadata
                .remove(format!("{lane_id}:{dp_hash}"))?;
            // Check if have the data locally after regardless - if we don't, print an error but delete metadata anyways for consistency.
            let Some(dp) = self.get_dp_by_hash(lane_id, dp_hash)? else {
                bail!(
                    "Can't find DP data {} for lane {} where metadata could be found",
                    dp_hash,
                    lane_id
                );
            };
            self.by_hash_data.remove(format!("{lane_id}:{dp_hash}"))?;
            self.dp_proofs.remove(format!("{lane_id}:{dp_hash}"))?;
            self.metrics.record_op(
                "remove_by_hash",
                "dp_metadata",
                start.elapsed().as_micros() as u64,
            );
            return Ok(Some((dp_hash.clone(), (lane_entry, dp))));
        }
        self.metrics.record_op(
            "remove_by_hash",
            "dp_metadata",
            start.elapsed().as_micros() as u64,
        );
        Ok(None)
    }

    fn put_no_verification(
        &mut self,
        lane_id: LaneId,
        (lane_entry, data_proposal): (LaneEntryMetadata, DataProposal),
    ) -> Result<()> {
        let start = Instant::now();
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
        let res = batch.commit().map_err(Into::into);
        self.metrics.record_op(
            "put_no_verification",
            "batch",
            start.elapsed().as_micros() as u64,
        );
        res
    }

    fn add_signatures<T: IntoIterator<Item = ValidatorDAG>>(
        &mut self,
        lane_id: &LaneId,
        dp_hash: &DataProposalHash,
        vote_msgs: T,
    ) -> Result<Vec<ValidatorDAG>> {
        let start = Instant::now();
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
        self.metrics.record_op(
            "add_signatures",
            "dp_metadata",
            start.elapsed().as_micros() as u64,
        );
        Ok(signatures)
    }

    fn set_cached_poda(
        &mut self,
        lane_id: &LaneId,
        dp_hash: &DataProposalHash,
        poda: PoDA,
    ) -> Result<()> {
        let start = Instant::now();
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
        self.metrics.record_op(
            "set_cached_poda",
            "dp_metadata",
            start.elapsed().as_micros() as u64,
        );
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

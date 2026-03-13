use std::{
    collections::{BTreeMap, HashMap},
    path::{Path, PathBuf},
    sync::{Mutex, OnceLock},
};

use anyhow::{bail, Result};
use fjall::{Database, Keyspace, KeyspaceCreateOptions, KvSeparationOptions, Slice};
use hyli_model::{LaneId, ProofData};
use hyli_modules::utils::fjall_metrics::FjallMetrics;
use std::time::Instant;
use tracing::{info, warn};

use crate::model::{DataProposal, DataProposalHash, Hashed, PoDA};
use hyli_modules::log_warn;

use super::{
    storage::{LaneEntryMetadata, LanesStorage},
    ValidatorDAG,
};

pub use hyli_model::LaneBytesSize;

#[derive(Clone)]
pub struct ProposalStorage {
    db: Database,
    by_hash_metadata: Keyspace,
    by_hash_data: Keyspace,
    dp_proofs: Keyspace,
    metrics: FjallMetrics,
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
    // This closes unused Fjall handles and avoids hitting OS limits during tests.
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
    pub fn record_metrics(&self) {
        self.proposals.record_metrics();
    }

    pub fn set_metrics_context(&mut self, node_id: impl Into<String>) {
        self.proposals.set_metrics_context(node_id);
    }

    #[cfg(test)]
    pub fn put_metadata_only(
        &mut self,
        lane_id: &LaneId,
        dp_hash: &DataProposalHash,
        metadata: LaneEntryMetadata,
    ) -> Result<()> {
        self.proposals.put_metadata_only(lane_id, dp_hash, metadata)
    }
}

impl ProposalStorage {
    pub fn new(path: &Path) -> Result<Self> {
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

        Ok(Self {
            db,
            by_hash_metadata,
            by_hash_data,
            dp_proofs,
            metrics: FjallMetrics::global("mempool", "unknown", "mempool"),
        })
    }

    fn record_metrics(&self) {
        self.metrics.record_db(&self.db);
        self.metrics
            .record_keyspace("dp_metadata", &self.by_hash_metadata);
        self.metrics.record_keyspace("dp_data", &self.by_hash_data);
        self.metrics.record_keyspace("dp_proofs", &self.dp_proofs);
    }

    fn set_metrics_context(&mut self, node_id: impl Into<String>) {
        self.metrics = FjallMetrics::global("mempool", node_id, "mempool");
    }

    #[cfg(test)]
    fn put_metadata_only(
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

impl ProposalStorage {
    pub fn persist(&self) -> Result<()> {
        let start = Instant::now();
        let res = self
            .db
            .persist(fjall::PersistMode::Buffer)
            .map_err(Into::into);
        self.metrics
            .record_op("persist", "db", start.elapsed().as_micros() as u64);
        res
    }

    pub fn contains(&self, lane_id: &LaneId, dp_hash: &DataProposalHash) -> bool {
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

    pub fn get_metadata_by_hash(
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

    pub fn get_dp_by_hash(
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

    pub fn get_proofs_by_hash(
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

    pub fn delete_proofs(&mut self, lane_id: &LaneId, dp_hash: &DataProposalHash) -> Result<()> {
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

    pub fn remove_by_hash(
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

    pub fn put_no_verification(
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

    pub fn add_signatures<T: IntoIterator<Item = ValidatorDAG>>(
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

    pub fn set_cached_poda(
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

    #[cfg(test)]
    pub fn remove_lane_entry(&mut self, lane_id: &LaneId, dp_hash: &DataProposalHash) {
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

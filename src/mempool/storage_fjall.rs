use std::{
    collections::{BTreeMap, HashMap},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, OnceLock, RwLock},
    time::Duration,
};

use anyhow::{anyhow, bail, Result};
use async_stream::try_stream;
use fjall::{Database, Keyspace, KeyspaceCreateOptions, KvSeparationOptions, Slice};
use futures::Stream;
use hyli_model::{LaneId, ProofData};
use hyli_modules::utils::fjall_metrics::FjallMetrics;
use std::time::Instant;
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
    metrics: FjallMetrics,
    fjall_call_policy: FjallCallPolicy,
    // Used by the shared storage to know when it can drop this handle.
    ref_token: Arc<()>,
}

#[derive(Clone, Copy)]
struct FjallCallPolicy {
    timeout: Duration,
    max_retries: usize,
    retry_on_timeout: bool,
}

impl FjallCallPolicy {
    fn from_env() -> Self {
        let timeout = std::env::var("HYLI_FJALL_TIMEOUT_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .map(Duration::from_millis)
            .unwrap_or_else(|| Duration::from_secs(5));
        let max_retries = std::env::var("HYLI_FJALL_MAX_RETRIES")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(2);
        let retry_on_timeout = std::env::var("HYLI_FJALL_RETRY_ON_TIMEOUT")
            .ok()
            .map(|v| matches!(v.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
            .unwrap_or(false);
        Self {
            timeout,
            max_retries,
            retry_on_timeout,
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
            metrics: FjallMetrics::global("mempool", "unknown", "mempool"),
            fjall_call_policy: FjallCallPolicy::from_env(),
            ref_token: Arc::new(()),
        })
    }

    pub fn set_metrics_context(&mut self, node_id: impl Into<String>) {
        self.metrics = FjallMetrics::global("mempool", node_id, "mempool");
    }

    fn call_fjall<T, F, Op>(
        &self,
        op: &'static str,
        keyspace: &'static str,
        mut build: F,
    ) -> Result<T>
    where
        T: Send + 'static,
        F: FnMut() -> Op,
        Op: FnOnce() -> Result<T> + Send + 'static,
    {
        let policy = self.fjall_call_policy;
        let metrics = self.metrics.clone();
        let mut attempts = 0usize;
        loop {
            let start = Instant::now();
            let (tx, rx) = std::sync::mpsc::sync_channel(1);
            let op_fn = build();

            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                handle.spawn_blocking(move || {
                    let _ = tx.send(op_fn());
                });
            } else {
                std::thread::spawn(move || {
                    let _ = tx.send(op_fn());
                });
            }

            match rx.recv_timeout(policy.timeout) {
                Ok(result) => {
                    metrics.record_op(op, keyspace, start.elapsed().as_micros() as u64);
                    if result.is_err() && attempts < policy.max_retries {
                        attempts += 1;
                        metrics.record_retry(op, keyspace);
                        tracing::warn!(
                            op = op,
                            keyspace = keyspace,
                            attempt = attempts,
                            max_retries = policy.max_retries,
                            "retrying failed fjall call"
                        );
                        continue;
                    }
                    return result;
                }
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                    metrics.record_op(op, keyspace, start.elapsed().as_micros() as u64);
                    metrics.record_timeout(op, keyspace);
                    if policy.retry_on_timeout && attempts < policy.max_retries {
                        attempts += 1;
                        metrics.record_retry(op, keyspace);
                        tracing::warn!(
                            op = op,
                            keyspace = keyspace,
                            attempt = attempts,
                            max_retries = policy.max_retries,
                            timeout_ms = policy.timeout.as_millis(),
                            "fjall call timed out; retrying"
                        );
                        continue;
                    }
                    return Err(anyhow!(
                        "fjall op {} on {} exceeded timeout budget ({} ms)",
                        op,
                        keyspace,
                        policy.timeout.as_millis()
                    ));
                }
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                    metrics.record_op(op, keyspace, start.elapsed().as_micros() as u64);
                    if attempts < policy.max_retries {
                        attempts += 1;
                        metrics.record_retry(op, keyspace);
                        tracing::warn!(
                            op = op,
                            keyspace = keyspace,
                            attempt = attempts,
                            max_retries = policy.max_retries,
                            "retrying fjall call after worker disconnect"
                        );
                        continue;
                    }
                    return Err(anyhow!("fjall worker disconnected while running {}", op));
                }
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
        let by_hash_metadata = self.by_hash_metadata.clone();
        let key = format!("{lane_id}:{dp_hash}");
        let value = borsh::to_vec(&metadata)?;
        self.call_fjall("put_metadata_only", "dp_metadata", || {
            let by_hash_metadata = by_hash_metadata.clone();
            let key = key.clone();
            let value = value.clone();
            move || by_hash_metadata.insert(key, value).map_err(Into::into)
        })
    }
}

impl Storage for LanesStorage {
    fn persist(&self) -> Result<()> {
        let db = self.db.clone();
        self.call_fjall("persist", "db", || {
            let db = db.clone();
            move || db.persist(fjall::PersistMode::Buffer).map_err(Into::into)
        })
    }

    fn contains(&self, lane_id: &LaneId, dp_hash: &DataProposalHash) -> bool {
        let by_hash_metadata = self.by_hash_metadata.clone();
        let key = format!("{lane_id}:{dp_hash}");
        self.call_fjall("contains", "dp_metadata", || {
            let by_hash_metadata = by_hash_metadata.clone();
            let key = key.clone();
            move || by_hash_metadata.contains_key(key).map_err(Into::into)
        })
        .unwrap_or(false)
    }

    fn get_metadata_by_hash(
        &self,
        lane_id: &LaneId,
        dp_hash: &DataProposalHash,
    ) -> Result<Option<LaneEntryMetadata>> {
        let by_hash_metadata = self.by_hash_metadata.clone();
        let key = format!("{lane_id}:{dp_hash}");
        let lane_id_str = lane_id.to_string();
        let dp_hash_str = dp_hash.to_string();
        self.call_fjall("get_metadata_by_hash", "dp_metadata", move || {
            let by_hash_metadata = by_hash_metadata.clone();
            let key = key.clone();
            let lane_id_str = lane_id_str.clone();
            let dp_hash_str = dp_hash_str.clone();
            move || {
                let item = log_warn!(
                    by_hash_metadata.get(key),
                    "Can't find DP metadata {} for validator {}",
                    dp_hash_str,
                    lane_id_str
                )?;
                item.map(decode_metadata_from_item).transpose()
            }
        })
    }

    fn get_dp_by_hash(
        &self,
        lane_id: &LaneId,
        dp_hash: &DataProposalHash,
    ) -> Result<Option<DataProposal>> {
        let by_hash_data = self.by_hash_data.clone();
        let key = format!("{lane_id}:{dp_hash}");
        let dp_hash = dp_hash.clone();
        let lane_id_str = lane_id.to_string();
        let dp_hash_str = dp_hash.to_string();
        self.call_fjall("get_dp_by_hash", "dp_data", move || {
            let by_hash_data = by_hash_data.clone();
            let key = key.clone();
            let dp_hash = dp_hash.clone();
            let lane_id_str = lane_id_str.clone();
            let dp_hash_str = dp_hash_str.clone();
            move || {
                let item = log_warn!(
                    by_hash_data.get(key),
                    "Can't find DP data {} for validator {}",
                    dp_hash_str,
                    lane_id_str
                )?;
                item.map(|s| {
                    decode_data_proposal_from_item(s).map(|mut dp| {
                        // SAFETY: we trust our own fjall storage
                        unsafe {
                            dp.unsafe_set_hash(&dp_hash);
                        }
                        dp
                    })
                })
                .transpose()
            }
        })
    }

    fn get_proofs_by_hash(
        &self,
        lane_id: &LaneId,
        dp_hash: &DataProposalHash,
    ) -> Result<Option<Vec<(u64, ProofData)>>> {
        let dp_proofs = self.dp_proofs.clone();
        let key = format!("{lane_id}:{dp_hash}");
        let lane_id_str = lane_id.to_string();
        let dp_hash_str = dp_hash.to_string();
        self.call_fjall("get_proofs_by_hash", "dp_proofs", move || {
            let dp_proofs = dp_proofs.clone();
            let key = key.clone();
            let lane_id_str = lane_id_str.clone();
            let dp_hash_str = dp_hash_str.clone();
            move || {
                let item = log_warn!(
                    dp_proofs.get(key),
                    "Can't find DP proofs {} for validator {}",
                    dp_hash_str,
                    lane_id_str
                )?;
                item.map(|s| borsh::from_slice(&s).map_err(Into::into))
                    .transpose()
            }
        })
    }

    fn delete_proofs(&mut self, lane_id: &LaneId, dp_hash: &DataProposalHash) -> Result<()> {
        let dp_proofs = self.dp_proofs.clone();
        let key = format!("{lane_id}:{dp_hash}");
        self.call_fjall("delete_proofs", "dp_proofs", || {
            let dp_proofs = dp_proofs.clone();
            let key = key.clone();
            move || {
                dp_proofs.remove(key)?;
                Ok(())
            }
        })
    }

    fn pop(
        &mut self,
        lane_id: LaneId,
    ) -> Result<Option<(DataProposalHash, (LaneEntryMetadata, DataProposal))>> {
        let start = Instant::now();
        if let Some(lane_hash_tip) = self.get_lane_hash_tip(&lane_id) {
            if let Some(lane_entry) = self.get_metadata_by_hash(&lane_id, &lane_hash_tip)? {
                let by_hash_metadata = self.by_hash_metadata.clone();
                let key = format!("{lane_id}:{lane_hash_tip}");
                self.call_fjall("pop_remove_metadata", "dp_metadata", || {
                    let by_hash_metadata = by_hash_metadata.clone();
                    let key = key.clone();
                    move || {
                        by_hash_metadata.remove(key)?;
                        Ok(())
                    }
                })?;
                // Check if have the data locally after regardless - if we don't, print an error but delete metadata anyways for consistency.
                let Some(dp) = self.get_dp_by_hash(&lane_id, &lane_hash_tip)? else {
                    bail!(
                        "Can't find DP data {} for lane {} where metadata could be found",
                        lane_hash_tip,
                        lane_id
                    );
                };
                let by_hash_data = self.by_hash_data.clone();
                let key = format!("{lane_id}:{lane_hash_tip}");
                self.call_fjall("pop_remove_data", "dp_data", || {
                    let by_hash_data = by_hash_data.clone();
                    let key = key.clone();
                    move || {
                        by_hash_data.remove(key)?;
                        Ok(())
                    }
                })?;
                self.update_lane_tip(lane_id, lane_hash_tip.clone(), lane_entry.cumul_size);
                self.metrics
                    .record_op("pop", "dp_metadata", start.elapsed().as_micros() as u64);
                return Ok(Some((lane_hash_tip, (lane_entry, dp))));
            }
        }
        self.metrics
            .record_op("pop", "dp_metadata", start.elapsed().as_micros() as u64);
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
        let by_hash_metadata = self.by_hash_metadata.clone();
        let key = format!("{lane_id}:{dp_hash}");
        let dp_hash = dp_hash.clone();
        let lane_id = lane_id.clone();
        let votes: Vec<_> = vote_msgs.into_iter().collect();
        self.call_fjall("add_signatures", "dp_metadata", || {
            let by_hash_metadata = by_hash_metadata.clone();
            let key = key.clone();
            let dp_hash = dp_hash.clone();
            let lane_id = lane_id.clone();
            let votes = votes.clone();
            move || {
                let Some(mut lem) = log_warn!(
                    by_hash_metadata.get(key.clone()),
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

                for msg in &votes {
                    let (dph, cumul_size) = &msg.msg;
                    if &lem.cumul_size != cumul_size || dp_hash != *dph {
                        tracing::warn!(
                            "Received a DataVote message with wrong hash or size: {:?}",
                            msg.msg
                        );
                        continue;
                    }
                    match lem
                        .signatures
                        .binary_search_by(|probe| probe.signature.cmp(&msg.signature))
                    {
                        Ok(_) => {}
                        Err(pos) => lem.signatures.insert(pos, msg.clone()),
                    }
                }
                let signatures = lem.signatures.clone();
                by_hash_metadata.insert(key, encode_metadata_to_item(lem)?)?;
                Ok(signatures)
            }
        })
    }

    fn set_cached_poda(
        &mut self,
        lane_id: &LaneId,
        dp_hash: &DataProposalHash,
        poda: PoDA,
    ) -> Result<()> {
        let by_hash_metadata = self.by_hash_metadata.clone();
        let key = format!("{lane_id}:{dp_hash}");
        let lane_id = lane_id.clone();
        let dp_hash = dp_hash.clone();
        self.call_fjall("set_cached_poda", "dp_metadata", || {
            let by_hash_metadata = by_hash_metadata.clone();
            let key = key.clone();
            let lane_id = lane_id.clone();
            let dp_hash = dp_hash.clone();
            let poda = poda.clone();
            move || {
                let Some(mut lem) = log_warn!(
                    by_hash_metadata.get(key.clone()),
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
                by_hash_metadata.insert(key, encode_metadata_to_item(lem)?)?;
                Ok(())
            }
        })
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
        let by_hash_metadata = self.by_hash_metadata.clone();
        let key = format!("{lane_id}:{dp_hash}");
        let _ = self.call_fjall("remove_lane_entry_metadata", "dp_metadata", || {
            let by_hash_metadata = by_hash_metadata.clone();
            let key = key.clone();
            move || by_hash_metadata.remove(key).map_err(Into::into)
        });
        let by_hash_data = self.by_hash_data.clone();
        let key = format!("{lane_id}:{dp_hash}");
        let _ = self.call_fjall("remove_lane_entry_data", "dp_data", || {
            let by_hash_data = by_hash_data.clone();
            let key = key.clone();
            move || by_hash_data.remove(key).map_err(Into::into)
        });
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

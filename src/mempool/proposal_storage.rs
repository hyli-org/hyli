#![expect(clippy::unwrap_used, reason = "RwLock cannot be poisoned in our usage")]

use std::{
    collections::{BTreeMap, HashMap},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, OnceLock, RwLock},
};

use anyhow::Result;
use borsh::BorshSerialize;
use fjall::{Database, Keyspace, KeyspaceCreateOptions, KvSeparationOptions};
use hyli_model::{LaneId, ProofData};

use crate::model::{DataProposal, DataProposalHash, Hashed};

type ProposalKey = (LaneId, DataProposalHash);
type Proofs = Vec<(u64, ProofData)>;
type SharedProofs = Arc<Proofs>;

static SHARED_PROPOSALS: OnceLock<Mutex<HashMap<PathBuf, Arc<ProposalStorage>>>> = OnceLock::new();

pub trait KvEncode: Send + Sync + 'static {
    fn encode(&self) -> Result<Vec<u8>>;
}

impl<T> KvEncode for T
where
    T: BorshSerialize + Send + Sync + 'static,
{
    fn encode(&self) -> Result<Vec<u8>> {
        borsh::to_vec(self).map_err(Into::into)
    }
}

pub trait KvBackend: Send + Sync {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;
    fn put(&self, key: &[u8], value: Arc<dyn KvEncode>) -> Result<()>;
    fn contains_key(&self, key: &[u8]) -> Result<bool>;
    fn delete(&self, key: &[u8]) -> Result<()>;
    fn persist(&self) -> Result<()>;
}

#[derive(Clone, Default)]
pub struct InMemoryKvBackend {
    data: Arc<RwLock<BTreeMap<Vec<u8>, Vec<u8>>>>,
}

impl InMemoryKvBackend {
    pub fn new() -> Self {
        Self::default()
    }
}

impl KvBackend for InMemoryKvBackend {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.data.read().unwrap().get(key).cloned())
    }

    fn put(&self, key: &[u8], value: Arc<dyn KvEncode>) -> Result<()> {
        self.data
            .write()
            .unwrap()
            .insert(key.to_vec(), value.encode()?);
        Ok(())
    }

    fn contains_key(&self, key: &[u8]) -> Result<bool> {
        Ok(self.data.read().unwrap().contains_key(key))
    }

    fn delete(&self, key: &[u8]) -> Result<()> {
        self.data.write().unwrap().remove(key);
        Ok(())
    }

    fn persist(&self) -> Result<()> {
        Ok(())
    }
}

pub struct FjallKvBackend {
    db: Database,
    dp_data: Keyspace,
    dp_proofs: Keyspace,
}

impl FjallKvBackend {
    pub fn new(path: &Path) -> Result<Self> {
        let db = Database::builder(path)
            .cache_size(256 * 1024 * 1024)
            .max_journaling_size(512 * 1024 * 1024)
            .open()?;

        let dp_data = db.keyspace("dp_data", || {
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

        Ok(Self {
            db,
            dp_data,
            dp_proofs,
        })
    }

    fn keyspace_for_key(&self, key: &[u8]) -> &Keyspace {
        if key.starts_with(b"proofs:") {
            &self.dp_proofs
        } else {
            &self.dp_data
        }
    }
}

impl KvBackend for FjallKvBackend {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self
            .keyspace_for_key(key)
            .get(key)?
            .map(|slice| slice.to_vec()))
    }

    fn put(&self, key: &[u8], value: Arc<dyn KvEncode>) -> Result<()> {
        self.keyspace_for_key(key).insert(key, value.encode()?)?;
        Ok(())
    }

    fn contains_key(&self, key: &[u8]) -> Result<bool> {
        Ok(self.keyspace_for_key(key).contains_key(key)?)
    }

    fn delete(&self, key: &[u8]) -> Result<()> {
        self.keyspace_for_key(key).remove(key)?;
        Ok(())
    }

    fn persist(&self) -> Result<()> {
        self.db.persist(fjall::PersistMode::Buffer)?;
        Ok(())
    }
}

pub struct ProposalStorage {
    backend: Box<dyn KvBackend>,
    data_cache: RwLock<HashMap<ProposalKey, Arc<DataProposal>>>,
    proofs_cache: RwLock<HashMap<ProposalKey, SharedProofs>>,
}

impl ProposalStorage {
    pub fn shared(path: &Path) -> Result<Arc<Self>> {
        let registry = SHARED_PROPOSALS.get_or_init(|| Mutex::new(HashMap::new()));
        let mut guard = registry
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        if let Some(existing) = guard.get(path) {
            return Ok(Arc::clone(existing));
        }

        let storage = Arc::new(Self::new(path)?);
        guard.insert(path.to_path_buf(), Arc::clone(&storage));
        Ok(storage)
    }

    pub fn new(path: &Path) -> Result<Self> {
        Ok(Self {
            backend: Box::new(FjallKvBackend::new(path)?),
            data_cache: RwLock::new(HashMap::new()),
            proofs_cache: RwLock::new(HashMap::new()),
        })
    }

    pub fn persist(&self) -> Result<()> {
        self.backend.persist()
    }

    pub fn get_dp_by_hash(
        &self,
        lane_id: &LaneId,
        dp_hash: &DataProposalHash,
    ) -> Result<Option<DataProposal>> {
        let key = proposal_key(lane_id, dp_hash);
        let data_proposal =
            if let Some(data_proposal) = self.data_cache.read().unwrap().get(&key).cloned() {
                data_proposal
            } else {
                let Some(bytes) = self.backend.get(&data_key(lane_id, dp_hash)?)? else {
                    return Ok(None);
                };
                let data_proposal = Arc::new(borsh::from_slice::<DataProposal>(&bytes)?);
                self.data_cache
                    .write()
                    .unwrap()
                    .insert(key, Arc::clone(&data_proposal));
                data_proposal
            };

        let mut data_proposal = data_proposal.as_ref().clone();
        // SAFETY: this hash came from the storage key for this value.
        unsafe {
            data_proposal.unsafe_set_hash(dp_hash);
        }
        Ok(Some(data_proposal))
    }

    pub fn get_proofs_by_hash(
        &self,
        lane_id: &LaneId,
        dp_hash: &DataProposalHash,
    ) -> Result<Option<Proofs>> {
        let key = proposal_key(lane_id, dp_hash);
        if let Some(proofs) = self.proofs_cache.read().unwrap().get(&key).cloned() {
            return Ok(Some(proofs.as_ref().clone()));
        }

        let Some(bytes) = self.backend.get(&proofs_key(lane_id, dp_hash)?)? else {
            return Ok(None);
        };
        let proofs = Arc::new(borsh::from_slice::<Proofs>(&bytes)?);
        self.proofs_cache
            .write()
            .unwrap()
            .insert(key, Arc::clone(&proofs));
        Ok(Some(proofs.as_ref().clone()))
    }

    pub fn delete_proofs(&self, lane_id: &LaneId, dp_hash: &DataProposalHash) -> Result<()> {
        let key = proposal_key(lane_id, dp_hash);
        self.backend.delete(&proofs_key(lane_id, dp_hash)?)?;
        self.proofs_cache.write().unwrap().remove(&key);
        Ok(())
    }

    pub fn remove_by_hash(
        &self,
        lane_id: &LaneId,
        dp_hash: &DataProposalHash,
    ) -> Result<Option<(DataProposalHash, DataProposal)>> {
        let Some(data_proposal) = self.get_dp_by_hash(lane_id, dp_hash)? else {
            return Ok(None);
        };

        let key = proposal_key(lane_id, dp_hash);
        self.backend.delete(&data_key(lane_id, dp_hash)?)?;
        self.backend.delete(&proofs_key(lane_id, dp_hash)?)?;
        self.data_cache.write().unwrap().remove(&key);
        self.proofs_cache.write().unwrap().remove(&key);

        Ok(Some((dp_hash.clone(), data_proposal)))
    }

    pub fn put_no_verification(&self, lane_id: LaneId, data_proposal: DataProposal) -> Result<()> {
        let dp_hash = data_proposal.hashed();
        let key = (lane_id, dp_hash);
        let mut data_to_store = data_proposal;
        let proofs = data_to_store.take_proofs();
        let data = Arc::new(data_to_store);
        let proofs = Arc::new(proofs);

        self.backend.put(
            &data_key(&key.0, &key.1)?,
            Arc::clone(&data) as Arc<dyn KvEncode>,
        )?;
        self.backend.put(
            &proofs_key(&key.0, &key.1)?,
            Arc::clone(&proofs) as Arc<dyn KvEncode>,
        )?;
        self.data_cache.write().unwrap().insert(key.clone(), data);
        self.proofs_cache.write().unwrap().insert(key, proofs);
        Ok(())
    }
    #[cfg(test)]
    pub fn remove_lane_entry(&self, lane_id: &LaneId, dp_hash: &DataProposalHash) {
        let _ = self.remove_by_hash(lane_id, dp_hash);
    }
}

fn proposal_key(lane_id: &LaneId, dp_hash: &DataProposalHash) -> ProposalKey {
    (lane_id.clone(), dp_hash.clone())
}

fn data_key(lane_id: &LaneId, dp_hash: &DataProposalHash) -> Result<Vec<u8>> {
    namespaced_key(b"dp:", &(lane_id, dp_hash))
}

fn proofs_key(lane_id: &LaneId, dp_hash: &DataProposalHash) -> Result<Vec<u8>> {
    namespaced_key(b"proofs:", &(lane_id, dp_hash))
}

fn namespaced_key<K: BorshSerialize>(prefix: &[u8], key: &K) -> Result<Vec<u8>> {
    let mut encoded = prefix.to_vec();
    encoded.extend(borsh::to_vec(key)?);
    Ok(encoded)
}

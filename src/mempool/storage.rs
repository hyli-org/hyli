use anyhow::Result;
use async_stream::try_stream;
use borsh::{BorshDeserialize, BorshSerialize};
use futures::{Stream, StreamExt};
use hyli_crypto::BlstCrypto;
use hyli_model::{DataSized, LaneId};
use serde::{Deserialize, Serialize};
use staking::state::Staking;
use std::{
    collections::{BTreeMap, HashMap},
    ops::Deref,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, OnceLock, RwLock, RwLockReadGuard},
    vec,
};
use tracing::{error, info, trace, warn};

use crate::{
    mempool::proposal_storage::ProposalStorage,
    model::{
        Cut, DataProposal, DataProposalHash, DataProposalParent, Hashed, PoDA, SignedByValidator,
        ValidatorPublicKey,
    },
};

use super::ValidatorDAG;

pub use hyli_model::LaneBytesSize;

static SHARED_LANES: OnceLock<Mutex<HashMap<PathBuf, LanesStorage>>> = OnceLock::new();

pub fn shared_lanes_storage(path: &Path) -> Result<LanesStorage> {
    LanesStorage::shared(path)
}

pub enum CanBePutOnTop {
    Yes,
    No,
    AlreadyOnTop,
    Fork,
}

#[derive(Debug)]
#[expect(clippy::large_enum_variant)]
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
    pub parent_data_proposal_hash: DataProposalParent,
    pub cumul_size: LaneBytesSize,
    pub signatures: Vec<ValidatorDAG>,
    pub cached_poda: Option<PoDA>,
}

#[derive(Clone)]
pub struct LanesStorage {
    pub lanes_tip: Arc<RwLock<BTreeMap<LaneId, (DataProposalHash, LaneBytesSize)>>>,
    pub(crate) lane_entries: Arc<RwLock<HashMap<(LaneId, DataProposalHash), LaneEntryMetadata>>>,
    pub proposals: Arc<ProposalStorage>,
    // Used by the shared storage registry to know when it can drop this handle.
    ref_token: Arc<()>,
}

impl LanesStorage {
    /// Shared handle between mempool (read/write) and dissemination (read-only),
    /// keyed by `config.data_dir` to allow multiple nodes in one process.
    pub fn shared(path: &Path) -> Result<LanesStorage> {
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

    pub fn new(
        _path: &Path,
        lanes_tip: BTreeMap<LaneId, (DataProposalHash, LaneBytesSize)>,
    ) -> Result<Self> {
        Ok(Self {
            lanes_tip: Arc::new(RwLock::new(lanes_tip)),
            lane_entries: Arc::new(RwLock::new(HashMap::new())),
            proposals: Arc::new(ProposalStorage::new(_path)?),
            ref_token: Arc::new(()),
        })
    }

    pub fn new_handle(&self) -> Self {
        self.clone()
    }

    pub(crate) fn ref_count(&self) -> usize {
        Arc::strong_count(&self.ref_token)
    }

    pub fn record_metrics(&self) {}

    pub fn set_metrics_context(&mut self, _node_id: impl Into<String>) {}

    pub fn get_lane_ids(&self) -> Vec<LaneId> {
        #[allow(clippy::unwrap_used, reason = "RwLock cannot be poisoned in our usage")]
        let guard = self.lanes_tip.read().unwrap();
        guard.keys().cloned().collect()
    }

    pub fn get_lane_hash_tip(&self, lane_id: &LaneId) -> Option<DataProposalHash> {
        #[allow(clippy::unwrap_used, reason = "RwLock cannot be poisoned in our usage")]
        let guard = self.lanes_tip.read().unwrap();
        guard.get(lane_id).map(|(hash, _)| hash.clone())
    }

    pub fn get_lane_size_tip(&self, lane_id: &LaneId) -> Option<LaneBytesSize> {
        #[allow(clippy::unwrap_used, reason = "RwLock cannot be poisoned in our usage")]
        let guard = self.lanes_tip.read().unwrap();
        guard.get(lane_id).map(|(_, size)| *size)
    }

    pub fn update_lane_tip(
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

    pub fn lane_tips_snapshot(&self) -> BTreeMap<LaneId, (DataProposalHash, LaneBytesSize)> {
        #[allow(clippy::unwrap_used, reason = "RwLock cannot be poisoned in our usage")]
        let guard = self.lanes_tip.read().unwrap();
        guard.clone()
    }

    pub fn lane_tips_read(
        &self,
    ) -> RwLockReadGuard<'_, BTreeMap<LaneId, (DataProposalHash, LaneBytesSize)>> {
        #[allow(clippy::unwrap_used, reason = "RwLock cannot be poisoned in our usage")]
        self.lanes_tip.read().unwrap()
    }

    pub fn contains(&self, lane_id: &LaneId, dp_hash: &DataProposalHash) -> bool {
        #[allow(clippy::unwrap_used, reason = "RwLock cannot be poisoned in our usage")]
        self.lane_entries
            .read()
            .unwrap()
            .contains_key(&(lane_id.clone(), dp_hash.clone()))
    }

    pub fn get_metadata_by_hash(
        &self,
        lane_id: &LaneId,
        dp_hash: &DataProposalHash,
    ) -> Result<Option<LaneEntryMetadata>> {
        #[allow(clippy::unwrap_used, reason = "RwLock cannot be poisoned in our usage")]
        Ok(self
            .lane_entries
            .read()
            .unwrap()
            .get(&(lane_id.clone(), dp_hash.clone()))
            .cloned())
    }

    pub fn put_no_verification(
        &self,
        lane_id: LaneId,
        (lane_entry, data_proposal): (LaneEntryMetadata, DataProposal),
    ) -> Result<()> {
        let dp_hash = data_proposal.hashed();
        self.proposals
            .put_no_verification(lane_id.clone(), data_proposal)?;
        #[allow(clippy::unwrap_used, reason = "RwLock cannot be poisoned in our usage")]
        self.lane_entries
            .write()
            .unwrap()
            .insert((lane_id, dp_hash), lane_entry);
        Ok(())
    }

    pub fn add_signatures<T: IntoIterator<Item = ValidatorDAG>>(
        &self,
        lane_id: &LaneId,
        dp_hash: &DataProposalHash,
        vote_msgs: T,
    ) -> Result<Vec<ValidatorDAG>> {
        #[allow(clippy::unwrap_used, reason = "RwLock cannot be poisoned in our usage")]
        let mut entries = self.lane_entries.write().unwrap();
        let Some(metadata) = entries.get_mut(&(lane_id.clone(), dp_hash.clone())) else {
            anyhow::bail!(
                "Can't find lane entry metadata {} for lane {}",
                dp_hash,
                lane_id
            );
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
            match metadata
                .signatures
                .binary_search_by(|probe| probe.signature.cmp(&msg.signature))
            {
                Ok(_) => {}
                Err(pos) => metadata.signatures.insert(pos, msg),
            }
        }

        Ok(metadata.signatures.clone())
    }

    pub fn set_cached_poda(
        &self,
        lane_id: &LaneId,
        dp_hash: &DataProposalHash,
        poda: PoDA,
    ) -> Result<()> {
        #[allow(clippy::unwrap_used, reason = "RwLock cannot be poisoned in our usage")]
        let mut entries = self.lane_entries.write().unwrap();
        let Some(metadata) = entries.get_mut(&(lane_id.clone(), dp_hash.clone())) else {
            anyhow::bail!(
                "Can't find lane entry metadata {} for lane {}",
                dp_hash,
                lane_id
            );
        };

        metadata.cached_poda = Some(poda);
        Ok(())
    }

    pub fn remove_by_hash(
        &self,
        lane_id: &LaneId,
        dp_hash: &DataProposalHash,
    ) -> Result<Option<(DataProposalHash, (LaneEntryMetadata, DataProposal))>> {
        let Some(data_proposal) = self.proposals.remove_by_hash(lane_id, dp_hash)? else {
            return Ok(None);
        };
        #[allow(clippy::unwrap_used, reason = "RwLock cannot be poisoned in our usage")]
        let metadata = self
            .lane_entries
            .write()
            .unwrap()
            .remove(&(lane_id.clone(), dp_hash.clone()));

        let Some(metadata) = metadata else {
            anyhow::bail!(
                "Can't find lane entry metadata {} for lane {} where data could be found",
                dp_hash,
                lane_id
            );
        };

        Ok(Some((data_proposal.0, (metadata, data_proposal.1))))
    }

    pub fn get_latest_car(
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

            let f = staking.compute_f();

            if let Some(cached_poda) = &le.cached_poda {
                let cached_voting_power =
                    staking.compute_voting_power(cached_poda.validators.as_slice());
                if cached_voting_power > f {
                    return Ok(Some((current, le.cumul_size, cached_poda.clone())));
                }
            }

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
            match le.parent_data_proposal_hash {
                DataProposalParent::DP(parent) => {
                    current = parent.clone();
                }
                DataProposalParent::LaneRoot(_) => {
                    return Ok(None);
                }
            }
        }
    }

    /// Signs the data proposal before creating a new LaneEntry and puting it in the lane
    pub fn store_data_proposal(
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
            &data_proposal.parent_data_proposal_hash,
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
                            cached_poda: None,
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
                            cached_poda: None,
                        },
                        data_proposal,
                    ),
                )?;
                cumul_size
            }
        };

        Ok((data_proposal_hash, cumul_size))
    }

    pub fn get_entries_between_hashes<'a>(
        &'a self,
        lane_id: &'a LaneId,
        from_data_proposal_hash: Option<DataProposalHash>,
        to_data_proposal_hash: Option<DataProposalHash>,
    ) -> impl Stream<Item = Result<EntryOrMissingHash>> + 'a {
        try_stream! {
            let metadata_stream = self.get_entries_metadata_between_hashes(
                lane_id,
                from_data_proposal_hash,
                to_data_proposal_hash,
            );
            futures::pin_mut!(metadata_stream);

            while let Some(md) = metadata_stream.next().await {
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

    pub fn get_entries_metadata_between_hashes<'a>(
        &'a self,
        lane_id: &'a LaneId,
        from_data_proposal_hash: Option<DataProposalHash>,
        to_data_proposal_hash: Option<DataProposalHash>,
    ) -> impl Stream<Item = Result<MetadataOrMissingHash>> + 'a {
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
                            match lane_entry.parent_data_proposal_hash.clone() {
                                DataProposalParent::DP(parent_dp_hash) => {
                                    some_dp_hash = parent_dp_hash;
                                }
                                DataProposalParent::LaneRoot(_) => {
                                    break;
                                }
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

    pub fn get_lane_size_at(
        &self,
        lane_id: &LaneId,
        dp_hash: &DataProposalHash,
    ) -> Result<LaneBytesSize> {
        self.get_metadata_by_hash(lane_id, dp_hash)?
            .map(|entry| entry.cumul_size)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Missing lane size for DataProposal {} in lane {}",
                    dp_hash,
                    lane_id
                )
            })
    }

    pub fn get_pending_entries_in_lane<'a>(
        &'a self,
        lane_id: &'a LaneId,
        last_cut: Option<Cut>,
    ) -> impl Stream<Item = Result<MetadataOrMissingHash>> + 'a {
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
    pub async fn get_oldest_pending_entry(
        &self,
        lane_id: &LaneId,
        last_cut: Option<Cut>,
    ) -> Result<Option<(LaneEntryMetadata, DataProposalHash)>> {
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

    /// Returns CanBePutOnTop::Yes if the DataProposal can be put in the lane
    /// Returns CanBePutOnTop::No if the DataProposal can't be put in the lane because the parent is unknown
    /// Returns CanBePutOnTop::Fork if the DataProposal creates an identified fork
    /// Returns CanBePutOnTop::AlreadyOnTop if the DataProposal is already on top of the lane
    pub fn can_be_put_on_top(
        &mut self,
        lane_id: &LaneId,
        data_proposal_hash: &DataProposalHash,
        parent_data_proposal_hash: &DataProposalParent,
    ) -> CanBePutOnTop {
        let lane_tip = self.get_lane_hash_tip(lane_id);
        // Data proposal parent hash needs to match the lane tip of that validator
        match (parent_data_proposal_hash, &lane_tip) {
            (DataProposalParent::DP(a), Some(b)) if a == b => {
                // LEGIT DATAPROPOSAL
                CanBePutOnTop::Yes
            }
            (DataProposalParent::LaneRoot(a), None) if a == lane_id => {
                // Legit new lane
                CanBePutOnTop::Yes
            }
            (_, Some(b)) if b == data_proposal_hash => {
                // DP is the same as our current known lane tip
                CanBePutOnTop::AlreadyOnTop
            }
            (DataProposalParent::DP(a), _) if !self.contains(lane_id, a) => {
                // Unknown parent
                CanBePutOnTop::No
            }
            _ => {
                // NEITHER LEGIT NOR CORRECT PARENT --> FORK
                CanBePutOnTop::Fork
            }
        }
    }

    #[cfg(test)]
    pub fn remove_lane_entry(&mut self, lane_id: &LaneId, dp_hash: &DataProposalHash) {
        #[allow(clippy::unwrap_used, reason = "RwLock cannot be poisoned in our usage")]
        self.lane_entries
            .write()
            .unwrap()
            .remove(&(lane_id.clone(), dp_hash.clone()));
        self.proposals.remove_lane_entry(lane_id, dp_hash);
    }

    #[cfg(test)]
    pub fn put_metadata_only(
        &mut self,
        lane_id: &LaneId,
        dp_hash: &DataProposalHash,
        metadata: LaneEntryMetadata,
    ) -> Result<()> {
        #[allow(clippy::unwrap_used, reason = "RwLock cannot be poisoned in our usage")]
        self.lane_entries
            .write()
            .unwrap()
            .insert((lane_id.clone(), dp_hash.clone()), metadata);
        Ok(())
    }
}

impl Deref for LanesStorage {
    type Target = ProposalStorage;

    fn deref(&self) -> &Self::Target {
        &self.proposals
    }
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
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
        let lane_id = &LaneId::new(crypto.validator_pubkey().clone());
        let storage = setup_storage();

        let data_proposal = DataProposal::new_root(lane_id.clone(), vec![]);
        let cumul_size: LaneBytesSize = LaneBytesSize(data_proposal.estimate_size() as u64);

        let entry = LaneEntryMetadata {
            parent_data_proposal_hash: DataProposalParent::LaneRoot(lane_id.clone()),
            cumul_size,
            signatures: vec![],
            cached_poda: None,
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
            storage
                .proposals
                .get_dp_by_hash(lane_id, &dp_hash)
                .unwrap()
                .unwrap(),
            data_proposal
        );
    }

    #[test_log::test(tokio::test)]
    async fn test_store_proofs_separately_and_hydrate() {
        let crypto = BlstCrypto::new("proofs").unwrap();
        let lane_id = &LaneId::new(crypto.validator_pubkey().clone());
        let storage = setup_storage();

        // Build a DataProposal with a VerifiedProof tx that includes an inlined proof
        let proof = ProofData(vec![1, 2, 3, 4]);
        let proof_hash = proof.hashed();
        let vpt = VerifiedProofTransaction {
            contract_name: ContractName::new("test-contract"),
            program_id: ProgramId(vec![]),
            verifier: Verifier("test".into()),
            proof: Some(proof.clone()),
            proof_hash: proof_hash.clone(),
            proof_size: proof.0.len(),
            proven_blobs: vec![BlobProofOutput {
                original_proof_hash: proof_hash,
                blob_tx_hash: b"blob-tx".into(),
                program_id: ProgramId(vec![]),
                verifier: Verifier("test".into()),
                hyli_output: HyliOutput::default(),
            }],
            is_recursive: false,
        };
        let tx = Transaction::from(TransactionData::VerifiedProof(vpt.clone()));

        let dp = DataProposal::new_root(lane_id.clone(), vec![tx]);
        let cumul_size: LaneBytesSize = LaneBytesSize(dp.estimate_size() as u64);
        let entry = LaneEntryMetadata {
            parent_data_proposal_hash: DataProposalParent::LaneRoot(lane_id.clone()),
            cumul_size,
            signatures: vec![],
            cached_poda: None,
        };
        let dp_hash = dp.hashed();

        // Store DP: implementation should strip proofs and store them separately
        storage
            .put_no_verification(lane_id.clone(), (entry.clone(), dp.clone()))
            .unwrap();

        // Stored DP must have proofs removed
        let stored_dp = storage
            .proposals
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
            .proposals
            .get_proofs_by_hash(lane_id, &dp_hash)
            .unwrap()
            .expect("proofs stored");
        assert_eq!(proofs.first(), Some(&(0u64, proof.clone())));

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
        let lane_id = &LaneId::new(crypto.validator_pubkey().clone());
        let storage = setup_storage();
        let data_proposal = DataProposal::new_root(lane_id.clone(), vec![]);
        let cumul_size: LaneBytesSize = LaneBytesSize(data_proposal.estimate_size() as u64);
        let mut entry = LaneEntryMetadata {
            parent_data_proposal_hash: DataProposalParent::LaneRoot(lane_id.clone()),
            cumul_size,
            signatures: vec![],
            cached_poda: None,
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
        let lane_id = &LaneId::new(crypto.validator_pubkey().clone());
        let mut storage = setup_storage();

        let crypto2: BlstCrypto = BlstCrypto::new("2").unwrap();

        let data_proposal = DataProposal::new_root(lane_id.clone(), vec![]);
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
        let lane_id2 = &LaneId::new(crypto2.validator_pubkey().clone());
        let crypto3: BlstCrypto = BlstCrypto::new("3").unwrap();

        let dp = DataProposal::new_root(lane_id2.clone(), vec![]);

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
        let lane_id = &LaneId::new(crypto.validator_pubkey().clone());
        let mut storage = setup_storage();
        let dp1 = DataProposal::new_root(lane_id.clone(), vec![]);
        let dp2 = DataProposal::new(dp1.hashed(), vec![]);
        let dp3 = DataProposal::new(dp2.hashed(), vec![]);

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
        let lane_id = &LaneId::new(crypto.validator_pubkey().clone());
        let mut storage = setup_storage();

        let dp1 = DataProposal::new_root(lane_id.clone(), vec![]);
        let dp2 = DataProposal::new(dp1.hashed(), vec![]);
        let dp3 = DataProposal::new(dp2.hashed(), vec![]);

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
        let lane_id = &LaneId::new(crypto.validator_pubkey().clone());
        let mut storage = setup_storage();

        let dp1 = DataProposal::new_root(lane_id.clone(), vec![Transaction::default()]);

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
        let dp2 = DataProposal::new(dp1.hashed(), vec![Transaction::default()]);
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

    #[test_log::test]
    fn test_get_lane_size_at_missing_dp_errors() {
        let crypto: BlstCrypto = BlstCrypto::new("1").unwrap();
        let lane_id = &LaneId::new(crypto.validator_pubkey().clone());
        let storage = setup_storage();
        let missing_dp = DataProposalHash::from(b"missing-dp".as_slice());

        assert!(storage.get_lane_size_at(lane_id, &missing_dp).is_err());
    }

    #[test_log::test(tokio::test)]
    async fn test_get_lane_pending_entries() {
        let crypto: BlstCrypto = BlstCrypto::new("1").unwrap();
        let lane_id = &LaneId::new(crypto.validator_pubkey().clone());
        let mut storage = setup_storage();
        let data_proposal = DataProposal::new_root(lane_id.clone(), vec![]);
        let cumul_size: LaneBytesSize = LaneBytesSize(data_proposal.estimate_size() as u64);
        let entry = LaneEntryMetadata {
            parent_data_proposal_hash: DataProposalParent::LaneRoot(lane_id.clone()),
            cumul_size,
            signatures: vec![],
            cached_poda: None,
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
        let lane_id = &LaneId::new(crypto1.validator_pubkey().clone());

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
        let dp1 = DataProposal::new_root(lane_id.clone(), vec![]);
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
        let dp2 = DataProposal::new(dp1.hashed(), vec![]);
        storage
            .store_data_proposal(&crypto1, lane_id, dp2.clone()) // signs it too.
            .unwrap();
        // Car is still dp1
        assert_latest_car_eq(&mut storage, lane_id, &staking, None, &dp1);
        let dp3 = DataProposal::new(dp2.hashed(), vec![]);
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

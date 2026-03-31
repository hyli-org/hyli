use anyhow::{anyhow, bail, Context, Result};
use borsh::{BorshDeserialize, BorshSerialize};
use fjall::{Database, Keyspace, KeyspaceCreateOptions, KvSeparationOptions, Slice};
use google_cloud_storage::{
    client::{Storage as GcsStorageClient, StorageControl},
    model::ListObjectsRequest,
};
use hyli_model::{
    AggregateSignature, BlockHeight, ConsensusProposal, ConsensusProposalHash, DataProposalHash,
    Hashed, LaneId, SignedBlock,
};
use hyli_modules::{
    node_state::module::load_current_chain_timestamp, utils::fjall_metrics::FjallMetrics,
};
use std::{
    collections::{BTreeMap, HashMap},
    fmt::Debug,
    future::Future,
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
    time::Instant,
};
use tracing::{debug, info, trace};

use crate::{
    mempool::proposal_storage::ProposalStorage, shared_storage::gcs::timestamp_to_folder_name,
    utils::conf::DataProposalDurabilityConf,
};

#[derive(Clone, BorshSerialize, BorshDeserialize)]
struct StoredSignedBlock {
    data_proposals: Vec<(LaneId, Vec<DataProposalHash>)>,
    consensus_proposal: ConsensusProposal,
    certificate: AggregateSignature,
}

struct FjallHashKey(ConsensusProposalHash);
struct FjallHeightKey([u8; 8]);
struct FjallValue(Vec<u8>);

struct FjallBlocks {
    db: Database,
    by_hash: Keyspace,
    by_height: Keyspace,
    metrics: FjallMetrics,
    proposals: Arc<ProposalStorage>,
}

struct GcsBlocks {
    runtime: Arc<tokio::runtime::Runtime>,
    client: GcsStorageClient,
    control: StorageControl,
    bucket_path: String,
    gcs_prefix: String,
    data_directory: PathBuf,
    proposals: Arc<ProposalStorage>,
    state: RwLock<GcsBlockState>,
}

#[derive(Default)]
struct GcsBlockState {
    current_chain_timestamp: Option<String>,
    loaded_timestamp: Option<String>,
    by_height: BTreeMap<BlockHeight, ConsensusProposalHash>,
    by_hash: HashMap<ConsensusProposalHash, StoredSignedBlock>,
}

pub struct Blocks {
    backend: BlocksBackend,
}

enum BlocksBackend {
    Fjall(Box<FjallBlocks>),
    Gcs(Arc<GcsBlocks>),
}

impl AsRef<[u8]> for FjallHashKey {
    fn as_ref(&self) -> &[u8] {
        self.0 .0.as_slice()
    }
}

impl FjallHeightKey {
    fn new(height: BlockHeight) -> Self {
        Self(height.0.to_be_bytes())
    }
}

impl From<FjallHeightKey> for BlockHeight {
    fn from(value: FjallHeightKey) -> Self {
        BlockHeight(u64::from_be_bytes(value.0))
    }
}

impl AsRef<[u8]> for FjallHeightKey {
    fn as_ref(&self) -> &[u8] {
        self.0.as_slice()
    }
}

impl FjallValue {
    fn new_with_block(block: &SignedBlock) -> Result<Self> {
        Ok(Self(borsh::to_vec(&StoredSignedBlock::from_signed_block(
            block,
        ))?))
    }

    fn new_with_block_hash(block_hash: &ConsensusProposalHash) -> Result<Self> {
        Ok(Self(borsh::to_vec(block_hash)?))
    }
}

impl AsRef<[u8]> for FjallValue {
    fn as_ref(&self) -> &[u8] {
        self.0.as_slice()
    }
}

impl StoredSignedBlock {
    fn from_signed_block(block: &SignedBlock) -> Self {
        Self {
            data_proposals: block
                .data_proposals
                .iter()
                .map(|(lane_id, data_proposals)| {
                    (
                        lane_id.clone(),
                        data_proposals.iter().map(|dp| dp.hashed()).collect(),
                    )
                })
                .collect(),
            consensus_proposal: block.consensus_proposal.clone(),
            certificate: block.certificate.clone(),
        }
    }

    fn hydrate(self, proposals: &ProposalStorage) -> Result<SignedBlock> {
        let block_hash = self.consensus_proposal.hashed();
        let data_proposals = self
            .data_proposals
            .into_iter()
            .map(|(lane_id, hashes)| {
                let data_proposals = hashes
                    .into_iter()
                    .map(|dp_hash| {
                        proposals
                            .get_dp_by_hash(&lane_id, &dp_hash)?
                            .with_context(|| {
                                format!(
                                    "missing proposal {dp_hash} for lane {} while hydrating block {}",
                                    lane_id, block_hash
                                )
                            })
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok((lane_id, data_proposals))
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(SignedBlock {
            data_proposals,
            consensus_proposal: self.consensus_proposal,
            certificate: self.certificate,
        })
    }
}

impl FjallBlocks {
    fn new(path: &Path) -> Result<Self> {
        let db = Database::builder(path.join("data_availability.db"))
            .cache_size(256 * 1024 * 1024)
            .max_journaling_size(512 * 1024 * 1024)
            .open()?;
        let by_hash = db.keyspace("blocks_by_hash", || {
            KeyspaceCreateOptions::default()
                .with_kv_separation(Some(
                    KvSeparationOptions::default().file_target_size(256 * 1024 * 1024),
                ))
                .manual_journal_persist(true)
                .max_memtable_size(128 * 1024 * 1024)
        })?;
        let by_height = db.keyspace("block_hashes_by_height", KeyspaceCreateOptions::default)?;

        info!("{} block(s) available", by_hash.len()?);
        Ok(Self {
            db,
            by_hash,
            by_height,
            metrics: FjallMetrics::global("data_availability", "unknown", "data_availability.db"),
            proposals: ProposalStorage::shared(path)?,
        })
    }

    fn new_handle(&self) -> Self {
        Self {
            db: self.db.clone(),
            by_hash: self.by_hash.clone(),
            by_height: self.by_height.clone(),
            metrics: self.metrics.clone(),
            proposals: Arc::clone(&self.proposals),
        }
    }

    fn decode_block(&self, item: Slice) -> Result<SignedBlock> {
        let stored = borsh::from_slice::<StoredSignedBlock>(&item)?;
        stored.hydrate(&self.proposals)
    }

    fn decode_block_hash(item: Slice) -> Result<ConsensusProposalHash> {
        borsh::from_slice(&item).map_err(Into::into)
    }

    fn decode_height(item: Slice) -> Result<BlockHeight> {
        let key = item.first_chunk::<8>().context("Malformed key")?;
        Ok(BlockHeight::from(FjallHeightKey(*key)))
    }

    fn set_metrics_context(&mut self, node_id: impl Into<String>) {
        self.metrics =
            FjallMetrics::global("data_availability", node_id.into(), "data_availability.db");
    }

    fn is_empty(&self) -> bool {
        self.by_hash.is_empty().unwrap_or(true)
    }

    fn persist(&self) -> Result<()> {
        let start = Instant::now();
        self.proposals.persist()?;
        let res = self
            .db
            .persist(fjall::PersistMode::Buffer)
            .map_err(Into::into);
        self.record_op("persist", "db", start.elapsed());
        res
    }

    fn record_metrics(&self) {
        self.metrics.record_db(&self.db);
        self.metrics.record_keyspace("by_hash", &self.by_hash);
        self.metrics.record_keyspace("by_height", &self.by_height);
    }

    fn put(&mut self, block: SignedBlock) -> Result<()> {
        let start = Instant::now();
        let block_hash = block.hashed();
        if self.contains(&block_hash) {
            self.record_op("put", "by_hash", start.elapsed());
            return Ok(());
        }

        for (lane_id, data_proposals) in &block.data_proposals {
            for data_proposal in data_proposals {
                self.proposals
                    .put_no_verification(lane_id.clone(), data_proposal.clone())?;
            }
        }

        trace!("storing block metadata {}", block.height());
        self.by_hash.insert(
            FjallHashKey(block_hash.clone()).as_ref(),
            FjallValue::new_with_block(&block)?.as_ref(),
        )?;
        self.by_height.insert(
            FjallHeightKey::new(block.height()).as_ref(),
            FjallValue::new_with_block_hash(&block_hash)?.as_ref(),
        )?;
        self.record_op("put", "by_hash", start.elapsed());
        Ok(())
    }

    fn get(&self, block_hash: &ConsensusProposalHash) -> Result<Option<SignedBlock>> {
        let start = Instant::now();
        let Some(item) = self.by_hash.get(FjallHashKey(block_hash.clone()))? else {
            self.record_op("get", "by_hash", start.elapsed());
            return Ok(None);
        };
        let res = self.decode_block(item).map(Some);
        self.record_op("get", "by_hash", start.elapsed());
        res
    }

    fn get_by_height(&self, height: BlockHeight) -> Result<Option<SignedBlock>> {
        let start = Instant::now();
        let Some(bytes) = self.by_height.get(FjallHeightKey::new(height))? else {
            self.record_op("get_by_height", "by_height", start.elapsed());
            return Ok(None);
        };
        let block_hash = Self::decode_block_hash(bytes)?;
        let res = self.get(&block_hash);
        self.record_op("get_by_height", "by_height", start.elapsed());
        res
    }

    fn has_by_height(&self, height: BlockHeight) -> Result<bool> {
        let start = Instant::now();
        let res = self.by_height.contains_key(FjallHeightKey::new(height))?;
        self.record_op("has_by_height", "by_height", start.elapsed());
        Ok(res)
    }

    fn contains(&self, block_hash: &ConsensusProposalHash) -> bool {
        let start = Instant::now();
        let res = self
            .by_hash
            .contains_key(FjallHashKey(block_hash.clone()))
            .unwrap_or(false);
        self.record_op("contains", "by_hash", start.elapsed());
        res
    }

    fn record_op(&self, _op: &'static str, _keyspace: &'static str, _elapsed: std::time::Duration) {
    }

    fn first_hole_by_height(&self) -> Result<Option<BlockHeight>> {
        let Some(guard) = self.by_height.last_key_value() else {
            anyhow::bail!("Empty partition can't have holes");
        };
        let (k, _v) = guard.into_inner()?;
        let upper_bound = Self::decode_height(k)?;

        debug!(
            "Start scanning by_height partition to find first missing block up to {:?}",
            upper_bound
        );

        for i in 0..upper_bound.0 {
            if i % 1000 == 0 {
                trace!("Checking block #{} is present or not", i);
            }
            let key = FjallHeightKey::new(BlockHeight(i));
            if !self
                .by_height
                .contains_key(key)
                .map_err(|e| anyhow::anyhow!(e))?
            {
                info!("Found hole at height {}", i);
                return Ok(Some(BlockHeight(i)));
            }
        }

        debug!(
            "No holes found in by_height partition up to {:?}",
            upper_bound
        );

        Ok(None)
    }

    fn last(&self) -> Option<SignedBlock> {
        let guard = self.by_height.last_key_value()?;
        let (_k, v) = guard.into_inner().ok()?;
        let hash = Self::decode_block_hash(v).ok()?;
        self.get(&hash).ok().flatten()
    }

    fn highest(&self) -> BlockHeight {
        self.last().map_or(BlockHeight(0), |b| b.height())
    }

    fn last_block_hash(&self) -> Option<ConsensusProposalHash> {
        self.last().map(|b| b.hashed())
    }

    fn range(
        &mut self,
        min: BlockHeight,
        max: BlockHeight,
    ) -> impl Iterator<Item = Result<ConsensusProposalHash>> {
        self.by_height
            .range(FjallHeightKey::new(min)..FjallHeightKey::new(max))
            .map_while(|guard| {
                let (_k, v) = guard.into_inner().ok()?;
                Some(Self::decode_block_hash(v))
            })
    }
}

impl GcsBlockState {
    fn reset_for(&mut self, timestamp: String) {
        self.loaded_timestamp = Some(timestamp.clone());
        self.current_chain_timestamp = Some(timestamp);
        self.by_height.clear();
        self.by_hash.clear();
    }
}

impl GcsBlocks {
    fn new(path: &Path, conf: &DataProposalDurabilityConf) -> Result<Self> {
        let runtime = Arc::new(
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?,
        );

        let (client, control) = runtime.block_on(async {
            Ok::<_, anyhow::Error>((
                GcsStorageClient::builder().build().await?,
                StorageControl::builder().build().await?,
            ))
        })?;

        Ok(Self {
            runtime,
            client,
            control,
            bucket_path: bucket_path(&conf.gcs_bucket),
            gcs_prefix: conf.gcs_prefix.clone(),
            data_directory: path.to_path_buf(),
            proposals: ProposalStorage::shared(path)?,
            state: RwLock::new(GcsBlockState {
                current_chain_timestamp: load_current_chain_timestamp(path).ok(),
                ..Default::default()
            }),
        })
    }

    fn block_on<F, T>(&self, future: F) -> T
    where
        F: Future<Output = T>,
    {
        if tokio::runtime::Handle::try_current().is_ok() {
            tokio::task::block_in_place(|| self.runtime.block_on(future))
        } else {
            self.runtime.block_on(future)
        }
    }

    fn set_metrics_context(&self, _node_id: impl Into<String>) {}

    fn record_metrics(&self) {}

    fn is_empty(&self) -> bool {
        self.ensure_index_loaded().is_ok_and(|_| {
            self.state
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .by_height
                .is_empty()
        })
    }

    fn persist(&self) -> Result<()> {
        self.proposals.persist()
    }

    fn resolve_current_chain_timestamp(
        &self,
        block: Option<&SignedBlock>,
    ) -> Result<Option<String>> {
        {
            let guard = self
                .state
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if let Some(timestamp) = &guard.current_chain_timestamp {
                return Ok(Some(timestamp.clone()));
            }
        }

        if let Ok(timestamp) = load_current_chain_timestamp(&self.data_directory) {
            let mut guard = self
                .state
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            guard.current_chain_timestamp = Some(timestamp.clone());
            return Ok(Some(timestamp));
        }

        let Some(block) = block else {
            return Ok(None);
        };
        if block.height() != BlockHeight(0) {
            return Ok(None);
        }

        let timestamp = timestamp_to_folder_name(block.consensus_proposal.timestamp.0)?;
        let mut guard = self
            .state
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard.current_chain_timestamp = Some(timestamp.clone());
        Ok(Some(timestamp))
    }

    fn ensure_index_loaded(&self) -> Result<()> {
        let Some(current_chain_timestamp) = self.resolve_current_chain_timestamp(None)? else {
            return Ok(());
        };

        {
            let guard = self
                .state
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if guard.loaded_timestamp.as_deref() == Some(current_chain_timestamp.as_str()) {
                return Ok(());
            }
        }

        let prefix = block_object_prefix(&self.gcs_prefix, &current_chain_timestamp);
        let objects = self.block_on(self.list_objects(&prefix))?;

        let mut by_height = BTreeMap::new();
        let mut by_hash = HashMap::new();
        for object_name in objects {
            let Some(height) = parse_height_from_object_name(&prefix, &object_name) else {
                continue;
            };
            let stored = self.block_on(self.read_stored_signed_block(&object_name))?;
            let block_hash = stored.consensus_proposal.hashed();
            by_height.insert(height, block_hash.clone());
            by_hash.insert(block_hash, stored);
        }

        let mut guard = self
            .state
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard.current_chain_timestamp = Some(current_chain_timestamp.clone());
        guard.loaded_timestamp = Some(current_chain_timestamp);
        guard.by_height = by_height;
        guard.by_hash = by_hash;
        Ok(())
    }

    async fn list_objects(&self, prefix: &str) -> Result<Vec<String>> {
        let mut objects = Vec::new();
        let mut page_token = None;

        loop {
            let mut request = ListObjectsRequest::new()
                .set_parent(&self.bucket_path)
                .set_prefix(prefix);
            if let Some(token) = &page_token {
                request = request.set_page_token(token);
            }

            let response = self
                .control
                .list_objects()
                .with_request(request)
                .send()
                .await?;

            objects.extend(response.objects.into_iter().map(|object| object.name));

            if response.next_page_token.is_empty() {
                break;
            }
            page_token = Some(response.next_page_token);
        }

        Ok(objects)
    }

    async fn read_stored_signed_block(&self, object_name: &str) -> Result<StoredSignedBlock> {
        let mut reader = self
            .client
            .read_object(&self.bucket_path, object_name)
            .send()
            .await?;
        let mut bytes = Vec::new();
        while let Some(chunk) = reader.next().await.transpose()? {
            bytes.extend_from_slice(&chunk);
        }
        borsh::from_slice(&bytes).map_err(Into::into)
    }

    fn put(&self, block: SignedBlock) -> Result<()> {
        let block_hash = block.hashed();
        if self.contains(&block_hash) {
            return Ok(());
        }

        let current_chain_timestamp = self
            .resolve_current_chain_timestamp(Some(&block))?
            .context("Current chain timestamp is required for GCS block storage")?;

        for (lane_id, data_proposals) in &block.data_proposals {
            for data_proposal in data_proposals {
                self.proposals
                    .put_no_verification(lane_id.clone(), data_proposal.clone())?;
            }
        }

        let stored = StoredSignedBlock::from_signed_block(&block);
        let object_name =
            block_object_name(&self.gcs_prefix, &current_chain_timestamp, block.height());
        let payload = borsh::to_vec(&stored)?;

        match self.block_on(async {
            self.client
                .write_object(
                    self.bucket_path.clone(),
                    object_name,
                    bytes::Bytes::from(payload),
                )
                .set_if_generation_match(0_i64)
                .send_buffered()
                .await
        }) {
            Ok(_) => {}
            Err(err)
                if err
                    .status()
                    .is_some_and(|status| matches!(status.code as i32, 6 | 9)) => {}
            Err(err) => return Err(anyhow!(err)),
        }

        let mut guard = self
            .state
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if guard.loaded_timestamp.as_deref() != Some(current_chain_timestamp.as_str()) {
            guard.reset_for(current_chain_timestamp);
        }
        guard.by_height.insert(block.height(), block_hash.clone());
        guard.by_hash.insert(block_hash, stored);
        Ok(())
    }

    fn get(&self, block_hash: &ConsensusProposalHash) -> Result<Option<SignedBlock>> {
        self.ensure_index_loaded()?;
        let guard = self
            .state
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard
            .by_hash
            .get(block_hash)
            .cloned()
            .map(|stored| stored.hydrate(&self.proposals))
            .transpose()
    }

    fn get_by_height(&self, height: BlockHeight) -> Result<Option<SignedBlock>> {
        self.ensure_index_loaded()?;
        let block_hash = {
            let guard = self
                .state
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            guard.by_height.get(&height).cloned()
        };
        block_hash
            .as_ref()
            .map(|hash| self.get(hash))
            .transpose()
            .map(|opt| opt.flatten())
    }

    fn has_by_height(&self, height: BlockHeight) -> Result<bool> {
        self.ensure_index_loaded()?;
        Ok(self
            .state
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .by_height
            .contains_key(&height))
    }

    fn contains(&self, block_hash: &ConsensusProposalHash) -> bool {
        self.ensure_index_loaded().is_ok_and(|_| {
            self.state
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .by_hash
                .contains_key(block_hash)
        })
    }

    fn first_hole_by_height(&self) -> Result<Option<BlockHeight>> {
        self.ensure_index_loaded()?;
        let guard = self
            .state
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some((&upper_bound, _)) = guard.by_height.last_key_value() else {
            bail!("Empty partition can't have holes");
        };

        for height in 0..upper_bound.0 {
            if !guard.by_height.contains_key(&BlockHeight(height)) {
                return Ok(Some(BlockHeight(height)));
            }
        }

        Ok(None)
    }

    fn last(&self) -> Option<SignedBlock> {
        self.ensure_index_loaded().ok()?;
        let stored = {
            let guard = self
                .state
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let hash = guard.by_height.last_key_value()?.1.clone();
            guard.by_hash.get(&hash).cloned()?
        };
        stored.hydrate(&self.proposals).ok()
    }

    fn highest(&self) -> BlockHeight {
        self.last().map_or(BlockHeight(0), |b| b.height())
    }

    fn last_block_hash(&self) -> Option<ConsensusProposalHash> {
        self.ensure_index_loaded().ok()?;
        self.state
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .by_height
            .last_key_value()
            .map(|(_height, hash)| hash.clone())
    }

    fn range(
        &self,
        min: BlockHeight,
        max: BlockHeight,
    ) -> impl Iterator<Item = Result<ConsensusProposalHash>> {
        let values = match self.ensure_index_loaded() {
            Ok(()) => self
                .state
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .by_height
                .range(min..max)
                .map(|(_height, hash)| Ok(hash.clone()))
                .collect(),
            Err(err) => vec![Err(err)],
        };
        values.into_iter()
    }
}

impl Blocks {
    pub fn new(path: &Path) -> Result<Self> {
        Ok(Self {
            backend: BlocksBackend::Fjall(Box::new(FjallBlocks::new(path)?)),
        })
    }

    pub fn new_with_durability(path: &Path, conf: &DataProposalDurabilityConf) -> Result<Self> {
        let backend = if should_store_blocks_in_gcs(conf) {
            BlocksBackend::Gcs(Arc::new(GcsBlocks::new(path, conf)?))
        } else {
            BlocksBackend::Fjall(Box::new(FjallBlocks::new(path)?))
        };
        Ok(Self { backend })
    }

    pub fn new_handle(&self) -> Blocks {
        let backend = match &self.backend {
            BlocksBackend::Fjall(inner) => BlocksBackend::Fjall(Box::new(inner.new_handle())),
            BlocksBackend::Gcs(inner) => BlocksBackend::Gcs(Arc::clone(inner)),
        };
        Blocks { backend }
    }

    pub fn set_metrics_context(&mut self, node_id: impl Into<String>) {
        match &mut self.backend {
            BlocksBackend::Fjall(inner) => inner.set_metrics_context(node_id),
            BlocksBackend::Gcs(inner) => inner.set_metrics_context(node_id),
        }
    }

    pub fn is_empty(&self) -> bool {
        match &self.backend {
            BlocksBackend::Fjall(inner) => inner.is_empty(),
            BlocksBackend::Gcs(inner) => inner.is_empty(),
        }
    }

    pub fn persist(&self) -> Result<()> {
        match &self.backend {
            BlocksBackend::Fjall(inner) => inner.persist(),
            BlocksBackend::Gcs(inner) => inner.persist(),
        }
    }

    pub fn record_metrics(&self) {
        match &self.backend {
            BlocksBackend::Fjall(inner) => inner.record_metrics(),
            BlocksBackend::Gcs(inner) => inner.record_metrics(),
        }
    }

    pub fn put(&mut self, block: SignedBlock) -> Result<()> {
        match &mut self.backend {
            BlocksBackend::Fjall(inner) => inner.put(block),
            BlocksBackend::Gcs(inner) => inner.put(block),
        }
    }

    pub fn get(&self, block_hash: &ConsensusProposalHash) -> Result<Option<SignedBlock>> {
        match &self.backend {
            BlocksBackend::Fjall(inner) => inner.get(block_hash),
            BlocksBackend::Gcs(inner) => inner.get(block_hash),
        }
    }

    pub fn get_by_height(&self, height: BlockHeight) -> Result<Option<SignedBlock>> {
        match &self.backend {
            BlocksBackend::Fjall(inner) => inner.get_by_height(height),
            BlocksBackend::Gcs(inner) => inner.get_by_height(height),
        }
    }

    pub fn has_by_height(&self, height: BlockHeight) -> Result<bool> {
        match &self.backend {
            BlocksBackend::Fjall(inner) => inner.has_by_height(height),
            BlocksBackend::Gcs(inner) => inner.has_by_height(height),
        }
    }

    pub fn contains(&self, block_hash: &ConsensusProposalHash) -> bool {
        match &self.backend {
            BlocksBackend::Fjall(inner) => inner.contains(block_hash),
            BlocksBackend::Gcs(inner) => inner.contains(block_hash),
        }
    }

    pub fn record_op(
        &self,
        _op: &'static str,
        _keyspace: &'static str,
        _elapsed: std::time::Duration,
    ) {
    }

    pub fn first_hole_by_height(&self) -> Result<Option<BlockHeight>> {
        match &self.backend {
            BlocksBackend::Fjall(inner) => inner.first_hole_by_height(),
            BlocksBackend::Gcs(inner) => inner.first_hole_by_height(),
        }
    }

    pub fn last(&self) -> Option<SignedBlock> {
        match &self.backend {
            BlocksBackend::Fjall(inner) => inner.last(),
            BlocksBackend::Gcs(inner) => inner.last(),
        }
    }

    pub fn highest(&self) -> BlockHeight {
        match &self.backend {
            BlocksBackend::Fjall(inner) => inner.highest(),
            BlocksBackend::Gcs(inner) => inner.highest(),
        }
    }

    pub fn last_block_hash(&self) -> Option<ConsensusProposalHash> {
        match &self.backend {
            BlocksBackend::Fjall(inner) => inner.last_block_hash(),
            BlocksBackend::Gcs(inner) => inner.last_block_hash(),
        }
    }

    pub fn range(
        &mut self,
        min: BlockHeight,
        max: BlockHeight,
    ) -> impl Iterator<Item = Result<ConsensusProposalHash>> {
        match &mut self.backend {
            BlocksBackend::Fjall(inner) => inner.range(min, max).collect::<Vec<_>>().into_iter(),
            BlocksBackend::Gcs(inner) => inner.range(min, max).collect::<Vec<_>>().into_iter(),
        }
    }
}

impl Debug for Blocks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Blocks")
            .field("highest", &self.highest())
            .finish()
    }
}

fn should_store_blocks_in_gcs(conf: &DataProposalDurabilityConf) -> bool {
    !conf.gcs_bucket.trim().is_empty()
}

fn bucket_path(bucket: &str) -> String {
    if bucket.starts_with("projects/") {
        bucket.to_string()
    } else {
        format!("projects/_/buckets/{bucket}")
    }
}

fn block_object_prefix(gcs_prefix: &str, current_chain_timestamp: &str) -> String {
    format!("{gcs_prefix}/{current_chain_timestamp}/stored_signed_blocks/block_")
}

fn block_object_name(
    gcs_prefix: &str,
    current_chain_timestamp: &str,
    height: BlockHeight,
) -> String {
    format!(
        "{}{}.bin",
        block_object_prefix(gcs_prefix, current_chain_timestamp),
        height.0
    )
}

fn parse_height_from_object_name(prefix: &str, object_name: &str) -> Option<BlockHeight> {
    let height = object_name
        .strip_prefix(prefix)?
        .strip_suffix(".bin")?
        .parse()
        .ok()?;
    Some(BlockHeight(height))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::conf::DataProposalDurabilityConf;

    #[test]
    fn block_object_name_is_namespaced_under_stored_signed_blocks() {
        assert_eq!(
            block_object_name("camelot", "2026-03-31T10-00-00Z", BlockHeight(12)),
            "camelot/2026-03-31T10-00-00Z/stored_signed_blocks/block_12.bin"
        );
    }

    #[test]
    fn parse_height_from_object_name_matches_expected_format() {
        let prefix = block_object_prefix("camelot", "2026-03-31T10-00-00Z");
        assert_eq!(
            parse_height_from_object_name(
                &prefix,
                "camelot/2026-03-31T10-00-00Z/stored_signed_blocks/block_42.bin"
            ),
            Some(BlockHeight(42))
        );
        assert_eq!(
            parse_height_from_object_name(
                &prefix,
                "camelot/other/stored_signed_blocks/block_42.bin"
            ),
            None
        );
    }

    #[test]
    fn blocks_default_to_fjall_without_bucket() -> Result<()> {
        let tmpdir = tempfile::tempdir()?;
        let blocks = Blocks::new_with_durability(
            tmpdir.path(),
            &DataProposalDurabilityConf {
                gcs_bucket: String::new(),
                gcs_prefix: "camelot".to_string(),
                save_data_proposals: true,
            },
        )?;

        assert!(matches!(blocks.backend, BlocksBackend::Fjall(_)));
        Ok(())
    }
}

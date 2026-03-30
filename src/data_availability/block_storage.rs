use anyhow::{Context, Result};
use borsh::{BorshDeserialize, BorshSerialize};
use fjall::{Database, Keyspace, KeyspaceCreateOptions, KvSeparationOptions, Slice};
use hyli_model::{
    AggregateSignature, BlockHeight, ConsensusProposal, ConsensusProposalHash, DataProposalHash,
    Hashed, LaneId, SignedBlock,
};
use hyli_modules::utils::fjall_metrics::FjallMetrics;
use std::{fmt::Debug, path::Path, sync::Arc, time::Instant};
use tracing::{debug, info, trace};

use crate::mempool::proposal_storage::ProposalStorage;

#[derive(Clone, BorshSerialize, BorshDeserialize)]
struct StoredSignedBlock {
    data_proposals: Vec<(LaneId, Vec<DataProposalHash>)>,
    consensus_proposal: ConsensusProposal,
    certificate: AggregateSignature,
}

struct FjallHashKey(ConsensusProposalHash);
struct FjallHeightKey([u8; 8]);
struct FjallValue(Vec<u8>);

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

pub struct Blocks {
    db: Database,
    by_hash: Keyspace,
    by_height: Keyspace,
    metrics: FjallMetrics,
    proposals: Arc<ProposalStorage>,
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

impl Blocks {
    pub fn new_handle(&self) -> Blocks {
        Blocks {
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

    pub fn new(path: &Path) -> Result<Self> {
        let db = Database::builder(&path.join("data_availability.db"))
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
        Ok(Blocks {
            db,
            by_hash,
            by_height,
            metrics: FjallMetrics::global("data_availability", "unknown", "data_availability.db"),
            proposals: ProposalStorage::shared(path)?,
        })
    }

    pub fn set_metrics_context(&mut self, node_id: impl Into<String>) {
        self.metrics =
            FjallMetrics::global("data_availability", &node_id.into(), "data_availability.db");
    }

    pub fn is_empty(&self) -> bool {
        self.by_hash.is_empty().unwrap_or(true)
    }

    pub fn persist(&self) -> Result<()> {
        let start = Instant::now();
        self.proposals.persist()?;
        let res = self
            .db
            .persist(fjall::PersistMode::Buffer)
            .map_err(Into::into);
        self.record_op("persist", "db", start.elapsed());
        res
    }

    pub fn record_metrics(&self) {
        self.metrics.record_db(&self.db);
        self.metrics.record_keyspace("by_hash", &self.by_hash);
        self.metrics.record_keyspace("by_height", &self.by_height);
    }

    pub fn put(&mut self, block: SignedBlock) -> Result<()> {
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

    pub fn get(&self, block_hash: &ConsensusProposalHash) -> Result<Option<SignedBlock>> {
        let start = Instant::now();
        let Some(item) = self.by_hash.get(FjallHashKey(block_hash.clone()))? else {
            self.record_op("get", "by_hash", start.elapsed());
            return Ok(None);
        };
        let res = self.decode_block(item).map(Some);
        self.record_op("get", "by_hash", start.elapsed());
        res
    }

    pub fn get_by_height(&self, height: BlockHeight) -> Result<Option<SignedBlock>> {
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

    pub fn has_by_height(&self, height: BlockHeight) -> Result<bool> {
        let start = Instant::now();
        let res = self.by_height.contains_key(FjallHeightKey::new(height))?;
        self.record_op("has_by_height", "by_height", start.elapsed());
        Ok(res)
    }

    pub fn contains(&self, block_hash: &ConsensusProposalHash) -> bool {
        let start = Instant::now();
        let res = self
            .by_hash
            .contains_key(FjallHashKey(block_hash.clone()))
            .unwrap_or(false);
        self.record_op("contains", "by_hash", start.elapsed());
        res
    }

    pub fn record_op(
        &self,
        _op: &'static str,
        _keyspace: &'static str,
        _elapsed: std::time::Duration,
    ) {
    }

    pub fn first_hole_by_height(&self) -> Result<Option<BlockHeight>> {
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

    pub fn last(&self) -> Option<SignedBlock> {
        let guard = self.by_height.last_key_value()?;
        let (_k, v) = guard.into_inner().ok()?;
        let hash = Self::decode_block_hash(v).ok()?;
        self.get(&hash).ok().flatten()
    }

    pub fn highest(&self) -> BlockHeight {
        self.last().map_or(BlockHeight(0), |b| b.height())
    }

    pub fn last_block_hash(&self) -> Option<ConsensusProposalHash> {
        self.last().map(|b| b.hashed())
    }

    pub fn range(
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

impl Debug for Blocks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Blocks")
            .field("len", &self.by_height.len())
            .finish()
    }
}

use anyhow::Result;
use fjall::{
    Config, Keyspace, KvSeparationOptions, PartitionCreateOptions, PartitionHandle, Slice,
};
use sdk::{BlockHeight, ConsensusProposalHash, Hashed, SignedBlock};
use std::{fmt::Debug, path::Path};
use tracing::{error, info, trace};

struct FjallHashKey(ConsensusProposalHash);
struct FjallHeightKey([u8; 8]);
struct FjallValue(Vec<u8>);

impl AsRef<[u8]> for FjallHashKey {
    fn as_ref(&self) -> &[u8] {
        self.0 .0.as_bytes()
    }
}

impl FjallHeightKey {
    fn new(height: BlockHeight) -> Self {
        Self(height.0.to_be_bytes())
    }
}

impl AsRef<[u8]> for FjallHeightKey {
    fn as_ref(&self) -> &[u8] {
        self.0.as_slice()
    }
}

impl FjallValue {
    fn new_with_block(block: &SignedBlock) -> Result<Self> {
        Ok(Self(borsh::to_vec(block)?))
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
    db: Keyspace,
    by_hash: PartitionHandle,
    by_height: PartitionHandle,
}

impl Blocks {
    fn decode_block(item: Slice) -> Result<SignedBlock> {
        borsh::from_slice(&item).map_err(Into::into)
    }
    fn decode_block_hash(item: Slice) -> Result<ConsensusProposalHash> {
        borsh::from_slice(&item).map_err(Into::into)
    }

    pub fn new(path: &Path) -> Result<Self> {
        let db = Config::new(path)
            .cache_size(256 * 1024 * 1024)
            .max_journaling_size(512 * 1024 * 1024)
            .max_write_buffer_size(512 * 1024 * 1024)
            .open()?;
        let by_hash = db.open_partition(
            "blocks_by_hash",
            PartitionCreateOptions::default()
                // Up from default 128Mb
                .with_kv_separation(
                    KvSeparationOptions::default().file_target_size(256 * 1024 * 1024),
                )
                .block_size(32 * 1024)
                .manual_journal_persist(true)
                .max_memtable_size(128 * 1024 * 1024),
        )?;
        let by_height =
            db.open_partition("block_hashes_by_height", PartitionCreateOptions::default())?;

        info!("{} block(s) available", by_hash.len()?);

        Ok(Blocks {
            db,
            by_hash,
            by_height,
        })
    }

    pub fn is_empty(&self) -> bool {
        self.by_hash.is_empty().unwrap_or(true)
    }

    pub fn persist(&self) -> Result<()> {
        self.db
            .persist(fjall::PersistMode::Buffer)
            .map_err(Into::into)
    }

    pub fn put(&mut self, block: SignedBlock) -> Result<()> {
        let block_hash = block.hashed();
        if self.contains(&block_hash) {
            return Ok(());
        }
        trace!("📦 storing block in fjall {}", block.height());
        self.by_hash.insert(
            FjallHashKey(block_hash).as_ref(),
            FjallValue::new_with_block(&block)?.as_ref(),
        )?;
        self.by_height.insert(
            FjallHeightKey::new(block.height()).as_ref(),
            FjallValue::new_with_block_hash(&block.hashed())?.as_ref(),
        )?;
        Ok(())
    }

    pub fn get(&self, block_hash: &ConsensusProposalHash) -> Result<Option<SignedBlock>> {
        let item = self.by_hash.get(FjallHashKey(block_hash.clone()))?;
        item.map(Self::decode_block).transpose()
    }

    pub fn contains(&mut self, block: &ConsensusProposalHash) -> bool {
        self.by_hash
            .contains_key(FjallHashKey(block.clone()))
            .unwrap_or(false)
    }

    pub fn last(&self) -> Option<SignedBlock> {
        match self.by_height.last_key_value() {
            Ok(Some((_, v))) => {
                let Ok(hash) = Self::decode_block_hash(v) else {
                    return None;
                };
                self.get(&hash).ok().flatten()
            }
            Ok(None) => None,
            Err(e) => {
                error!("Error getting last block: {:?}", e);
                None
            }
        }
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
            .map_while(|maybe_item| match maybe_item {
                Ok((_, v)) => Some(Self::decode_block_hash(v)),
                Err(_) => None,
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

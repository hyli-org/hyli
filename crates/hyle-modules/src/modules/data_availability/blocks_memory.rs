#![allow(unused)]
use anyhow::Result;
use indexmap::IndexMap;
use sdk::{BlockHeight, ConsensusProposalHash, Hashed, SignedBlock};
use std::path::Path;
use tracing::{info, trace};

#[derive(Debug)]
pub struct Blocks {
    data: IndexMap<ConsensusProposalHash, SignedBlock>,
}

impl Blocks {
    pub fn new(_: &Path) -> Result<Self> {
        Ok(Self {
            data: IndexMap::new(),
        })
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn persist(&self) -> Result<()> {
        Ok(())
    }

    pub fn put(&mut self, data: SignedBlock) -> Result<()> {
        let block_hash = data.hashed();
        if self.contains(&block_hash) {
            return Ok(());
        }
        trace!("📦 storing block {}", data.height());
        self.data.insert(block_hash, data);
        Ok(())
    }

    pub fn get(&self, block_hash: &ConsensusProposalHash) -> Result<Option<SignedBlock>> {
        Ok(self.data.get(block_hash).cloned())
    }

    pub fn contains(&mut self, block_hash: &ConsensusProposalHash) -> bool {
        self.data.contains_key(block_hash)
    }

    pub fn last(&self) -> Option<SignedBlock> {
        self.data.last().map(|(_, block)| block.clone())
    }

    pub fn last_block_hash(&self) -> Option<ConsensusProposalHash> {
        self.last().map(|b| b.hashed())
    }

    pub fn range(
        &self,
        min: BlockHeight,
        max: BlockHeight,
    ) -> Box<dyn Iterator<Item = Result<ConsensusProposalHash>> + '_> {
        // Items are in order but we don't know where they are. Binary search.
        let Ok(min) = self
            .data
            .binary_search_by(|_, block| block.height().0.cmp(&min.0))
        else {
            return Box::new(::std::iter::empty());
        };
        let Ok(max) = self
            .data
            .binary_search_by(|_, block| block.height().0.cmp(&(max.0 - 1)))
        else {
            return Box::new(::std::iter::empty());
        };
        let Some(iter) = self.data.get_range(min..max + 1) else {
            return Box::new(::std::iter::empty());
        };
        Box::new(iter.values().map(|block| Ok(block.hashed().clone())))
    }
}

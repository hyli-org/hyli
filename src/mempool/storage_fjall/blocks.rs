use std::time::Instant;

use anyhow::{bail, Result};
use hyli_model::BlockHeight;

use crate::model::{ConsensusProposalHash, Hashed, SignedBlock};

use super::{LanesStorage, Storage, StoredSignedBlock};

pub(super) struct FjallHashKey(pub(super) ConsensusProposalHash);
pub(super) struct FjallHeightKey(pub(super) [u8; 8]);
pub(super) struct FjallValue(pub(super) Vec<u8>);

impl AsRef<[u8]> for FjallHashKey {
    fn as_ref(&self) -> &[u8] {
        self.0 .0.as_slice()
    }
}

impl FjallHeightKey {
    pub(super) fn new(height: BlockHeight) -> Self {
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
    pub(super) fn new_with_block(block: &StoredSignedBlock) -> Result<Self> {
        Ok(Self(borsh::to_vec(block)?))
    }

    pub(super) fn new_with_block_hash(block_hash: &ConsensusProposalHash) -> Result<Self> {
        Ok(Self(borsh::to_vec(block_hash)?))
    }
}

impl AsRef<[u8]> for FjallValue {
    fn as_ref(&self) -> &[u8] {
        self.0.as_slice()
    }
}

fn load_full_block_from_stored(
    stored: StoredSignedBlock,
    lanes: &LanesStorage,
) -> Result<SignedBlock> {
    // Reconstructed blocks intentionally reflect the persisted lane payloads. Proof bytes are
    // not inlined back here; DA consumers are expected to operate on verified, proof-stripped
    // DPs. This also encodes a trust assumption for catchup/replay: once a DP is committed and
    // recovered from DA-backed storage, we treat its persisted content as authoritative even
    // though we may no longer have the original mempool vote evidence attached to it.
    let data_proposals = stored
        .data_proposals
        .into_iter()
        .map(|(lane_id, dp_hashes)| {
            let dps = dp_hashes
                .into_iter()
                .map(|dp_hash| {
                    lanes.get_dp_by_hash(&lane_id, &dp_hash)?.ok_or_else(|| {
                        anyhow::anyhow!(
                            "Missing data proposal {dp_hash} for lane {lane_id} while loading block {}",
                            stored.consensus_proposal.hashed()
                        )
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            Ok((lane_id, dps))
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(SignedBlock {
        data_proposals,
        consensus_proposal: stored.consensus_proposal,
        certificate: stored.certificate,
    })
}

impl LanesStorage {
    fn ensure_block_height_points_to(
        &self,
        height_key: &FjallHeightKey,
        block_height: BlockHeight,
        block_hash: &ConsensusProposalHash,
    ) -> Result<bool> {
        let Some(existing_hash_value) = self.block_hashes_by_height.get(height_key.as_ref())?
        else {
            return Ok(false);
        };

        let existing_hash: ConsensusProposalHash = borsh::from_slice(&existing_hash_value)?;
        if existing_hash != *block_hash {
            bail!(
                "Conflicting block index for height {}: existing {}, new {}",
                block_height,
                existing_hash,
                block_hash
            );
        }

        Ok(true)
    }

    fn repair_missing_height_index(
        &mut self,
        height_key: &FjallHeightKey,
        block_hash: &ConsensusProposalHash,
    ) -> Result<()> {
        self.block_hashes_by_height.insert(
            height_key.as_ref(),
            FjallValue::new_with_block_hash(block_hash)?.as_ref(),
        )?;
        Ok(())
    }

    pub fn put_block(&mut self, block: StoredSignedBlock) -> Result<()> {
        let start = Instant::now();
        let block_hash = block.consensus_proposal.hashed();
        let block_height = block.height();
        let height_key = FjallHeightKey::new(block_height);

        let result = (|| {
            let has_height_index =
                self.ensure_block_height_points_to(&height_key, block_height, &block_hash)?;

            if self.contains_block(&block_hash) {
                if !has_height_index {
                    self.repair_missing_height_index(&height_key, &block_hash)?;
                }
                return Ok(());
            }

            let mut batch = self.db.batch();
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
            batch.commit()?;
            Ok(())
        })();

        self.metrics.record_op(
            "put_block",
            "blocks_by_hash",
            start.elapsed().as_micros() as u64,
        );
        result
    }

    pub fn contains_block(&self, hash: &ConsensusProposalHash) -> bool {
        let start = Instant::now();
        let res = self
            .blocks_by_hash
            .contains_key(FjallHashKey(hash.clone()))
            .unwrap_or(false);
        self.metrics.record_op(
            "contains_block",
            "blocks_by_hash",
            start.elapsed().as_micros() as u64,
        );
        res
    }

    pub fn get_stored_block(
        &self,
        hash: &ConsensusProposalHash,
    ) -> Result<Option<StoredSignedBlock>> {
        let start = Instant::now();
        let item = self.blocks_by_hash.get(FjallHashKey(hash.clone()))?;
        let res = item
            .map(|item| borsh::from_slice(&item).map_err(Into::into))
            .transpose();
        self.metrics.record_op(
            "get_stored",
            "blocks_by_hash",
            start.elapsed().as_micros() as u64,
        );
        res
    }

    pub fn get_stored_block_by_height(
        &self,
        height: BlockHeight,
    ) -> Result<Option<StoredSignedBlock>> {
        let start = Instant::now();
        let Some(hash_value) = self
            .block_hashes_by_height
            .get(FjallHeightKey::new(height))?
        else {
            self.metrics.record_op(
                "get_stored_by_height",
                "block_hashes_by_height",
                start.elapsed().as_micros() as u64,
            );
            return Ok(None);
        };

        let block_hash: ConsensusProposalHash = borsh::from_slice(&hash_value)?;
        let res = self.get_stored_block(&block_hash);
        self.metrics.record_op(
            "get_stored_by_height",
            "block_hashes_by_height",
            start.elapsed().as_micros() as u64,
        );
        res
    }

    pub fn has_block_by_height(&self, height: BlockHeight) -> Result<bool> {
        Ok(self
            .block_hashes_by_height
            .contains_key(FjallHeightKey::new(height))?)
    }

    pub fn first_block_hole_by_height(&self) -> Result<Option<BlockHeight>> {
        let Some(guard) = self.block_hashes_by_height.last_key_value() else {
            anyhow::bail!("Empty partition can't have holes");
        };
        let (k, _v) = guard.into_inner()?;
        let upper_bound = BlockHeight::from(FjallHeightKey(
            *k.first_chunk::<8>()
                .ok_or_else(|| anyhow::anyhow!("Malformed key"))?,
        ));

        for i in 0..upper_bound.0 {
            if !self
                .block_hashes_by_height
                .contains_key(FjallHeightKey::new(BlockHeight(i)))
                .map_err(|e| anyhow::anyhow!(e))?
            {
                return Ok(Some(BlockHeight(i)));
            }
        }

        Ok(None)
    }

    fn last_stored_block(&self) -> Option<StoredSignedBlock> {
        let guard = self.block_hashes_by_height.last_key_value()?;
        let (_k, v) = guard.into_inner().ok()?;
        let hash: ConsensusProposalHash = borsh::from_slice(&v).ok()?;
        self.get_stored_block(&hash).ok().flatten()
    }

    pub fn highest_block_height(&self) -> BlockHeight {
        self.last_stored_block()
            .map_or(BlockHeight(0), |b| b.height())
    }

    pub fn last_block_hash(&self) -> Option<ConsensusProposalHash> {
        self.last_stored_block()
            .map(|b| b.consensus_proposal.hashed())
    }

    pub fn block_range(
        &self,
        min: BlockHeight,
        max: BlockHeight,
    ) -> impl Iterator<Item = Result<ConsensusProposalHash>> {
        self.block_hashes_by_height
            .range(FjallHeightKey::new(min)..FjallHeightKey::new(max))
            .map_while(|guard| {
                let (_k, v) = guard.into_inner().ok()?;
                Some(borsh::from_slice(&v).map_err(Into::into))
            })
    }

    pub fn load_full_block(&self, hash: &ConsensusProposalHash) -> Result<Option<SignedBlock>> {
        self.get_stored_block(hash)?
            .map(|stored| load_full_block_from_stored(stored, self))
            .transpose()
    }

    pub fn load_full_block_by_height(&self, height: BlockHeight) -> Result<Option<SignedBlock>> {
        self.get_stored_block_by_height(height)?
            .map(|stored| load_full_block_from_stored(stored, self))
            .transpose()
    }
}

#[cfg(test)]
mod tests {
    use hyli_model::DataSized;

    use super::super::LaneBytesSize;
    use super::*;
    use crate::mempool::storage::{LaneEntryMetadata, Storage};
    use crate::model::{
        AggregateSignature, BlobProofOutput, ConsensusProposal, ContractName, DataProposal,
        DataProposalParent, LaneId, PoDA, ProgramId, ProofData, Transaction, TransactionData,
        VerifiedProofTransaction, Verifier,
    };

    #[test_log::test]
    fn put_repairs_missing_height_index_for_existing_block() {
        let tmpdir = tempfile::tempdir().unwrap().keep();
        let mut lanes = LanesStorage::new(&tmpdir, Default::default()).unwrap();
        let block = StoredSignedBlock::from(SignedBlock::default());
        let block_hash = block.consensus_proposal.hashed();
        let height_key = FjallHeightKey::new(block.height());

        lanes.put_block(block.clone()).unwrap();
        lanes
            .block_hashes_by_height
            .remove(height_key.as_ref())
            .unwrap();

        assert!(lanes
            .get_stored_block_by_height(block.height())
            .unwrap()
            .is_none());

        lanes.put_block(block).unwrap();

        let stored = lanes.get_stored_block_by_height(BlockHeight(0)).unwrap();
        assert_eq!(stored.unwrap().consensus_proposal.hashed(), block_hash);
    }

    #[test_log::test]
    fn store_signed_block_preserves_cumul_sizes_for_stripped_verified_proofs() {
        let tmpdir = tempfile::tempdir().unwrap().keep();
        let mut lanes = LanesStorage::new(&tmpdir, Default::default()).unwrap();
        let lane_id = LaneId::default();

        let proof = ProofData(vec![1, 2, 3, 4, 5, 6, 7, 8]);
        let proof_hash = proof.hashed();
        let make_dp = |parent: DataProposalParent, contract| {
            let txs = vec![Transaction::from(TransactionData::VerifiedProof(
                VerifiedProofTransaction {
                    contract_name: ContractName::new(contract),
                    program_id: ProgramId(vec![]),
                    verifier: Verifier("test".into()),
                    proof: Some(proof.clone()),
                    proof_hash: proof_hash.clone(),
                    proof_size: proof.0.len(),
                    proven_blobs: vec![BlobProofOutput::default()],
                    is_recursive: false,
                },
            ))];
            match parent {
                DataProposalParent::LaneRoot(lane_id) => DataProposal::new_root(lane_id, txs),
                DataProposalParent::DP(parent_hash) => DataProposal::new(parent_hash, txs),
            }
        };

        let mut dp1 = make_dp(DataProposalParent::LaneRoot(lane_id.clone()), "p1");
        let dp1_size = dp1.estimate_size() as u64;
        dp1.remove_proofs();

        let mut dp2 = make_dp(DataProposalParent::DP(dp1.hashed()), "p2");
        let dp2_size = dp2.estimate_size() as u64;
        dp2.remove_proofs();

        let dp1_hash = dp1.hashed();
        let dp2_hash = dp2.hashed();
        let block = SignedBlock {
            consensus_proposal: ConsensusProposal {
                slot: 1,
                cut: vec![(
                    lane_id.clone(),
                    dp2_hash.clone(),
                    LaneBytesSize(dp1_size + dp2_size),
                    PoDA::default(),
                )],
                ..Default::default()
            },
            certificate: AggregateSignature::default(),
            data_proposals: vec![(lane_id.clone(), vec![dp1, dp2])],
        };

        lanes.store_signed_block(&block).unwrap();

        let dp1_metadata: LaneEntryMetadata = lanes
            .get_metadata_by_hash(&lane_id, &dp1_hash)
            .unwrap()
            .unwrap();
        let dp2_metadata: LaneEntryMetadata = lanes
            .get_metadata_by_hash(&lane_id, &dp2_hash)
            .unwrap()
            .unwrap();

        assert_eq!(dp1_metadata.cumul_size, LaneBytesSize(dp1_size));
        assert_eq!(dp2_metadata.cumul_size, LaneBytesSize(dp1_size + dp2_size));
    }

    #[test_log::test]
    fn store_signed_block_materializes_block_when_committed_dps_already_exist() {
        let tmpdir = tempfile::tempdir().unwrap().keep();
        let mut lanes = LanesStorage::new(&tmpdir, Default::default()).unwrap();
        let lane_id = LaneId::default();

        let dp = DataProposal::new_root(lane_id.clone(), vec![Transaction::default()]);
        let dp_hash = dp.hashed();
        let dp_size = LaneBytesSize(dp.estimate_size() as u64);

        lanes
            .put_no_verification(
                lane_id.clone(),
                (
                    LaneEntryMetadata {
                        parent_data_proposal_hash: dp.parent_data_proposal_hash.clone(),
                        cumul_size: dp_size,
                        signatures: vec![],
                        cached_poda: None,
                    },
                    dp.clone(),
                ),
            )
            .unwrap();

        let block = SignedBlock {
            consensus_proposal: ConsensusProposal {
                slot: 1,
                cut: vec![(lane_id.clone(), dp_hash.clone(), dp_size, PoDA::default())],
                ..Default::default()
            },
            certificate: AggregateSignature::default(),
            data_proposals: vec![(lane_id.clone(), vec![dp])],
        };
        let block_hash = block.hashed();

        lanes.store_signed_block(&block).unwrap();

        let stored = lanes.get_stored_block(&block_hash).unwrap();
        assert!(stored.is_some());
        let by_height = lanes.get_stored_block_by_height(BlockHeight(1)).unwrap();
        assert!(by_height.is_some());
    }
}

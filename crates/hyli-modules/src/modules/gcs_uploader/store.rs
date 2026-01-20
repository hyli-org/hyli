use std::collections::BTreeMap;

use borsh::{BorshDeserialize, BorshSerialize};
use sdk::BlockHeight;
use sdk::SignedBlock;

#[derive(BorshSerialize, BorshDeserialize, Default)]
pub struct GcsUploaderStore {
    pub last_uploaded_height: BlockHeight,
    pub retry_queue: BTreeMap<BlockHeight, (SignedBlock, u8)>, // height -> (block, retry_count)
}


#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_block(height: u64) -> SignedBlock {
        // Create a minimal valid SignedBlock for testing
        use sdk::hyli_model_utils::TimestampMs;
        use sdk::{AggregateSignature, ConsensusProposal, ConsensusProposalHash, Cut};
        SignedBlock {
            data_proposals: vec![],
            consensus_proposal: ConsensusProposal {
                slot: height,
                parent_hash: ConsensusProposalHash(String::new()),
                cut: Cut::default(),
                staking_actions: vec![],
                timestamp: TimestampMs(0),
            },
            certificate: AggregateSignature::default(),
        }
    }

    #[test]
    fn test_retry_queue_basic() {
        let mut store = GcsUploaderStore::default();

        let block = create_test_block(100);
        store.retry_queue.insert(BlockHeight(100), (block, 0));

        assert_eq!(store.retry_queue.len(), 1);
        assert!(store.retry_queue.contains_key(&BlockHeight(100)));
    }

    #[test]
    fn test_last_uploaded_height() {
        let mut store = GcsUploaderStore::default();

        assert_eq!(store.last_uploaded_height.0, 0);

        store.last_uploaded_height = BlockHeight(1000);
        assert_eq!(store.last_uploaded_height.0, 1000);
    }

    #[test]
    fn test_serialization_deserialization() {
        let mut store = GcsUploaderStore::default();

        store.last_uploaded_height = BlockHeight(500);

        let block = create_test_block(99);
        store
            .retry_queue
            .insert(BlockHeight(99), (block.clone(), 2));

        // Serialize
        let serialized = borsh::to_vec(&store).expect("serialization failed");

        // Deserialize
        let deserialized: GcsUploaderStore =
            borsh::from_slice(&serialized).expect("deserialization failed");

        // Verify
        assert_eq!(deserialized.last_uploaded_height, BlockHeight(500));
        assert_eq!(deserialized.retry_queue.len(), 1);
        assert!(deserialized.retry_queue.contains_key(&BlockHeight(99)));
    }
}

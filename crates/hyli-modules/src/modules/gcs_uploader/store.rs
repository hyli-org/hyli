use std::collections::BTreeMap;

use borsh::{BorshDeserialize, BorshSerialize};
use sdk::BlockHeight;
use sdk::SignedBlock;

#[derive(BorshSerialize, BorshDeserialize, Default)]
pub struct GcsUploaderStore {
    pub last_uploaded_height: BlockHeight,
    pub retry_queue: BTreeMap<BlockHeight, (SignedBlock, u8)>, // height -> (block, retry_count)
    pub missing_ranges: BTreeMap<BlockHeight, BlockHeight>,    // start_height -> end_height
}

impl GcsUploaderStore {
    /// Add a missing block range, merging with adjacent ranges if possible
    pub fn add_missing_range(&mut self, start: BlockHeight, end: BlockHeight) {
        if start > end {
            return;
        }

        // Check if we can merge with existing ranges
        let mut merged_start = start;
        let mut merged_end = end;
        let mut to_remove = Vec::new();

        for (&range_start, &range_end) in &self.missing_ranges {
            // Check if ranges overlap or are adjacent
            if range_end.0 + 1 >= merged_start.0 && range_start.0 <= merged_end.0 + 1 {
                merged_start = BlockHeight(merged_start.0.min(range_start.0));
                merged_end = BlockHeight(merged_end.0.max(range_end.0));
                to_remove.push(range_start);
            }
        }

        // Remove merged ranges
        for start in to_remove {
            self.missing_ranges.remove(&start);
        }

        // Insert the merged range
        self.missing_ranges.insert(merged_start, merged_end);
    }

    /// Remove a block from missing ranges (when it arrives)
    pub fn remove_block_from_missing(&mut self, height: BlockHeight) {
        let mut to_update = Vec::new();

        for (&range_start, &range_end) in &self.missing_ranges {
            if height >= range_start && height <= range_end {
                to_update.push((range_start, range_end));
                break;
            }
        }

        for (range_start, range_end) in to_update {
            self.missing_ranges.remove(&range_start);

            // Split the range if block is in the middle
            if height > range_start {
                self.missing_ranges
                    .insert(range_start, BlockHeight(height.0 - 1));
            }
            if height < range_end {
                self.missing_ranges
                    .insert(BlockHeight(height.0 + 1), range_end);
            }
        }
    }

    /// Check if a block height is in a missing range
    pub fn is_missing(&self, height: BlockHeight) -> bool {
        for (&range_start, &range_end) in &self.missing_ranges {
            if height >= range_start && height <= range_end {
                return true;
            }
        }
        false
    }

    /// Get total count of missing blocks
    pub fn missing_blocks_count(&self) -> u64 {
        self.missing_ranges
            .iter()
            .map(|(start, end)| end.0 - start.0 + 1)
            .sum()
    }

    /// Get all missing ranges as a vector of (start, end) tuples
    pub fn get_missing_ranges(&self) -> Vec<(BlockHeight, BlockHeight)> {
        self.missing_ranges
            .iter()
            .map(|(&start, &end)| (start, end))
            .collect()
    }
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
    fn test_add_missing_range_simple() {
        let mut store = GcsUploaderStore::default();

        // Add a simple range
        store.add_missing_range(BlockHeight(10), BlockHeight(20));

        assert_eq!(store.missing_ranges.len(), 1);
        assert_eq!(store.missing_blocks_count(), 11); // 10-20 inclusive
    }

    #[test]
    fn test_add_missing_range_merge_adjacent() {
        let mut store = GcsUploaderStore::default();

        // Add two adjacent ranges
        store.add_missing_range(BlockHeight(10), BlockHeight(20));
        store.add_missing_range(BlockHeight(21), BlockHeight(30));

        // Should merge into one range
        assert_eq!(store.missing_ranges.len(), 1);
        assert_eq!(store.missing_blocks_count(), 21); // 10-30 inclusive
        assert!(store.missing_ranges.contains_key(&BlockHeight(10)));
        assert_eq!(
            *store.missing_ranges.get(&BlockHeight(10)).unwrap(),
            BlockHeight(30)
        );
    }

    #[test]
    fn test_add_missing_range_merge_overlapping() {
        let mut store = GcsUploaderStore::default();

        // Add overlapping ranges
        store.add_missing_range(BlockHeight(10), BlockHeight(25));
        store.add_missing_range(BlockHeight(20), BlockHeight(35));

        // Should merge into one range
        assert_eq!(store.missing_ranges.len(), 1);
        assert_eq!(store.missing_blocks_count(), 26); // 10-35 inclusive
        assert!(store.missing_ranges.contains_key(&BlockHeight(10)));
        assert_eq!(
            *store.missing_ranges.get(&BlockHeight(10)).unwrap(),
            BlockHeight(35)
        );
    }

    #[test]
    fn test_add_missing_range_separate() {
        let mut store = GcsUploaderStore::default();

        // Add non-adjacent ranges
        store.add_missing_range(BlockHeight(10), BlockHeight(20));
        store.add_missing_range(BlockHeight(30), BlockHeight(40));

        // Should remain separate
        assert_eq!(store.missing_ranges.len(), 2);
        assert_eq!(store.missing_blocks_count(), 22); // 11 + 11
    }

    #[test]
    fn test_add_missing_range_merge_multiple() {
        let mut store = GcsUploaderStore::default();

        // Add several ranges
        store.add_missing_range(BlockHeight(10), BlockHeight(20));
        store.add_missing_range(BlockHeight(30), BlockHeight(40));
        store.add_missing_range(BlockHeight(50), BlockHeight(60));

        // Add a range that bridges all of them
        store.add_missing_range(BlockHeight(15), BlockHeight(55));

        // Should merge into one range
        assert_eq!(store.missing_ranges.len(), 1);
        assert_eq!(store.missing_blocks_count(), 51); // 10-60 inclusive
    }

    #[test]
    fn test_remove_block_from_missing_start() {
        let mut store = GcsUploaderStore::default();

        store.add_missing_range(BlockHeight(10), BlockHeight(20));
        store.remove_block_from_missing(BlockHeight(10));

        // Should have range 11-20
        assert_eq!(store.missing_ranges.len(), 1);
        assert_eq!(store.missing_blocks_count(), 10); // 11-20 inclusive
        assert!(store.missing_ranges.contains_key(&BlockHeight(11)));
    }

    #[test]
    fn test_remove_block_from_missing_end() {
        let mut store = GcsUploaderStore::default();

        store.add_missing_range(BlockHeight(10), BlockHeight(20));
        store.remove_block_from_missing(BlockHeight(20));

        // Should have range 10-19
        assert_eq!(store.missing_ranges.len(), 1);
        assert_eq!(store.missing_blocks_count(), 10); // 10-19 inclusive
        assert_eq!(
            *store.missing_ranges.get(&BlockHeight(10)).unwrap(),
            BlockHeight(19)
        );
    }

    #[test]
    fn test_remove_block_from_missing_middle() {
        let mut store = GcsUploaderStore::default();

        store.add_missing_range(BlockHeight(10), BlockHeight(20));
        store.remove_block_from_missing(BlockHeight(15));

        // Should split into two ranges: 10-14 and 16-20
        assert_eq!(store.missing_ranges.len(), 2);
        assert_eq!(store.missing_blocks_count(), 10); // 5 + 5
        assert!(store.missing_ranges.contains_key(&BlockHeight(10)));
        assert!(store.missing_ranges.contains_key(&BlockHeight(16)));
    }

    #[test]
    fn test_remove_block_from_missing_single() {
        let mut store = GcsUploaderStore::default();

        store.add_missing_range(BlockHeight(15), BlockHeight(15));
        store.remove_block_from_missing(BlockHeight(15));

        // Should remove the range entirely
        assert_eq!(store.missing_ranges.len(), 0);
        assert_eq!(store.missing_blocks_count(), 0);
    }

    #[test]
    fn test_remove_block_not_in_missing() {
        let mut store = GcsUploaderStore::default();

        store.add_missing_range(BlockHeight(10), BlockHeight(20));
        store.remove_block_from_missing(BlockHeight(50));

        // Should not change anything
        assert_eq!(store.missing_ranges.len(), 1);
        assert_eq!(store.missing_blocks_count(), 11);
    }

    #[test]
    fn test_is_missing() {
        let mut store = GcsUploaderStore::default();

        store.add_missing_range(BlockHeight(10), BlockHeight(20));
        store.add_missing_range(BlockHeight(30), BlockHeight(40));

        // Test blocks in ranges
        assert!(store.is_missing(BlockHeight(10)));
        assert!(store.is_missing(BlockHeight(15)));
        assert!(store.is_missing(BlockHeight(20)));
        assert!(store.is_missing(BlockHeight(30)));
        assert!(store.is_missing(BlockHeight(35)));
        assert!(store.is_missing(BlockHeight(40)));

        // Test blocks not in ranges
        assert!(!store.is_missing(BlockHeight(9)));
        assert!(!store.is_missing(BlockHeight(21)));
        assert!(!store.is_missing(BlockHeight(25)));
        assert!(!store.is_missing(BlockHeight(41)));
        assert!(!store.is_missing(BlockHeight(100)));
    }

    #[test]
    fn test_missing_blocks_count_empty() {
        let store = GcsUploaderStore::default();
        assert_eq!(store.missing_blocks_count(), 0);
    }

    #[test]
    fn test_get_missing_ranges() {
        let mut store = GcsUploaderStore::default();

        store.add_missing_range(BlockHeight(10), BlockHeight(20));
        store.add_missing_range(BlockHeight(30), BlockHeight(40));

        let ranges = store.get_missing_ranges();
        assert_eq!(ranges.len(), 2);

        // BTreeMap maintains order
        assert_eq!(ranges[0], (BlockHeight(10), BlockHeight(20)));
        assert_eq!(ranges[1], (BlockHeight(30), BlockHeight(40)));
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
        store.add_missing_range(BlockHeight(100), BlockHeight(150));
        store.add_missing_range(BlockHeight(200), BlockHeight(250));

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
        assert_eq!(deserialized.missing_ranges.len(), 2);
        assert_eq!(deserialized.missing_blocks_count(), 102); // 51 + 51
        assert_eq!(deserialized.retry_queue.len(), 1);
        assert!(deserialized.retry_queue.contains_key(&BlockHeight(99)));
    }
}

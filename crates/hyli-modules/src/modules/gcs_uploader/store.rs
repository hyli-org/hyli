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

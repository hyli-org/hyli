use std::collections::BTreeMap;

use borsh::{BorshDeserialize, BorshSerialize};
use sdk::{BlockHeight, SignedBlock};

#[derive(BorshSerialize, BorshDeserialize, Default)]
pub struct GcsUploaderStore {
    pub last_uploaded_height: BlockHeight,
    pub retry_queue: BTreeMap<BlockHeight, (SignedBlock, u8)>, // height -> (block, retry_count)
}

use std::collections::HashMap;

use borsh::{BorshDeserialize, BorshSerialize};
use sdk::{BlockHeight, TxHash};

#[derive(Default, Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct Timeouts {
    by_block: HashMap<BlockHeight, Vec<TxHash>>,
    by_tx: HashMap<TxHash, BlockHeight>,
}

impl Timeouts {
    pub fn drop(&mut self, at: &BlockHeight) -> Vec<TxHash> {
        let txs = self.by_block.remove(at).unwrap_or_default();
        for tx in &txs {
            self.by_tx.remove(tx);
        }
        txs
    }

    /// Set or replace the timeout for a tx.
    pub fn set(&mut self, tx: TxHash, block_height: BlockHeight, timeout_window: BlockHeight) {
        let scheduled_at = block_height + timeout_window;

        if let Some(previous_block) = self.by_tx.insert(tx.clone(), scheduled_at) {
            self.remove_from_block(&previous_block, &tx);
        }

        self.by_block.entry(scheduled_at).or_default().push(tx);
    }

    pub fn count_all(&self) -> usize {
        self.by_block.values().map(|v| v.len()).sum()
    }

    fn remove_from_block(&mut self, at: &BlockHeight, tx: &TxHash) {
        let Some(txs) = self.by_block.get_mut(at) else {
            return;
        };

        txs.retain(|scheduled_tx| scheduled_tx != tx);
        if txs.is_empty() {
            self.by_block.remove(at);
        }
    }
}

#[cfg(any(test, feature = "test"))]
#[allow(unused)]
pub mod tests {
    use super::*;

    fn list_timeouts(t: &Timeouts, at: BlockHeight) -> Option<&Vec<TxHash>> {
        t.by_block.get(&at)
    }

    pub fn get(t: &Timeouts, tx: &TxHash) -> Option<BlockHeight> {
        t.by_tx.get(tx).copied()
    }

    #[test]
    fn timeout() {
        let mut t = Timeouts::default();
        let b1 = BlockHeight(0);
        let b2 = BlockHeight(1);
        let tx1: TxHash = b"tx1".into();
        let window = BlockHeight(100);

        t.set(tx1.clone(), b1, window);

        assert_eq!(list_timeouts(&t, b1 + window).unwrap().len(), 1);
        assert_eq!(list_timeouts(&t, b2 + window), None);
        assert_eq!(get(&t, &tx1), Some(b1 + window));

        t.set(tx1.clone(), b2, window);

        assert_eq!(t.drop(&(b1 + window)), Vec::<TxHash>::new());
        assert_eq!(get(&t, &tx1), Some(b2 + window));
        assert_eq!(list_timeouts(&t, b1 + window), None);
        assert_eq!(list_timeouts(&t, b2 + window).unwrap().len(), 1);

        assert_eq!(t.drop(&(b2 + window)), vec![tx1.clone()]);
        assert_eq!(get(&t, &tx1), None);
        assert_eq!(list_timeouts(&t, b2 + window), None);
    }

    #[test]
    fn resetting_timeout_replaces_previous_schedule() {
        let mut t = Timeouts::default();
        let tx1: TxHash = b"tx1".into();

        t.set(tx1.clone(), BlockHeight(10), BlockHeight(5));
        t.set(tx1.clone(), BlockHeight(20), BlockHeight(7));

        assert_eq!(get(&t, &tx1), Some(BlockHeight(27)));
        assert_eq!(list_timeouts(&t, BlockHeight(15)), None);
        assert_eq!(list_timeouts(&t, BlockHeight(27)), Some(&vec![tx1]));
    }
}

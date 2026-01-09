use super::*;

fn tx_hashes_for_event<F>(events: &StatefulEvents, predicate: F) -> Vec<TxHash>
where
    F: Fn(&StatefulEvent) -> bool,
{
    events
        .events
        .iter()
        .filter_map(|(tx_id, event)| {
            if predicate(event) {
                Some(tx_id.1.clone())
            } else {
                None
            }
        })
        .collect()
}

pub fn successful_txs(events: &StatefulEvents) -> Vec<TxHash> {
    tx_hashes_for_event(events, |event| matches!(event, StatefulEvent::SettledTx(_)))
}

pub fn failed_txs(events: &StatefulEvents) -> Vec<TxHash> {
    tx_hashes_for_event(events, |event| matches!(event, StatefulEvent::FailedTx(_)))
}

pub fn timed_out_txs(events: &StatefulEvents) -> Vec<TxHash> {
    tx_hashes_for_event(events, |event| {
        matches!(event, StatefulEvent::TimedOutTx(_))
    })
}

pub fn has_event_for_tx<F>(events: &StatefulEvents, tx_hash: &TxHash, predicate: F) -> bool
where
    F: Fn(&StatefulEvent) -> bool,
{
    events
        .events
        .iter()
        .any(|(tx_id, event)| tx_id.1 == *tx_hash && predicate(event))
}

pub fn signed_block_tx_hashes(signed_block: &SignedBlock) -> Vec<TxHash> {
    signed_block
        .iter_txs_with_id()
        .map(|(_, tx_id, _)| tx_id.1.clone())
        .collect()
}

pub trait NodeStateBlockExt {
    fn successful_txs(&self) -> Vec<TxHash>;
    fn failed_txs(&self) -> Vec<TxHash>;
    fn timed_out_txs(&self) -> Vec<TxHash>;
    fn has_event_for_tx<F>(&self, tx_hash: &TxHash, predicate: F) -> bool
    where
        F: Fn(&StatefulEvent) -> bool;
}

impl NodeStateBlockExt for NodeStateBlock {
    fn successful_txs(&self) -> Vec<TxHash> {
        successful_txs(&self.stateful_events)
    }

    fn failed_txs(&self) -> Vec<TxHash> {
        failed_txs(&self.stateful_events)
    }

    fn timed_out_txs(&self) -> Vec<TxHash> {
        timed_out_txs(&self.stateful_events)
    }

    fn has_event_for_tx<F>(&self, tx_hash: &TxHash, predicate: F) -> bool
    where
        F: Fn(&StatefulEvent) -> bool,
    {
        has_event_for_tx(&self.stateful_events, tx_hash, predicate)
    }
}

pub fn is_failed_or_rejected(state: &NodeState, block: &NodeStateBlock, tx_hash: &TxHash) -> bool {
    if block.failed_txs().contains(tx_hash) {
        return true;
    }
    !block.successful_txs().contains(tx_hash) && state.unsettled_transactions.get(tx_hash).is_none()
}

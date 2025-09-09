use sdk::{BlobIndex, BlobTransaction, Calldata, Hashed, HyliOutput, TxContext};

use crate::transaction_builder::TxExecutorHandler;

#[track_caller]
pub fn assert_handle<C: TxExecutorHandler>(
    contract: &mut C,
    tx: &BlobTransaction,
    index: BlobIndex,
) -> HyliOutput {
    assert_handle_with_ctx(contract, tx, index, None)
}

#[track_caller]
pub fn assert_handle_with_ctx<C: TxExecutorHandler>(
    contract: &mut C,
    tx: &BlobTransaction,
    index: BlobIndex,
    tx_ctx: Option<TxContext>,
) -> HyliOutput {
    let calldata = Calldata {
        tx_hash: tx.hashed(),
        identity: tx.identity.clone(),
        blobs: tx.blobs.clone().into(),
        tx_blob_count: tx.blobs.len(),
        index,
        tx_ctx,
        private_input: vec![],
    };

    let handle_result = contract.handle(&calldata);
    assert!(handle_result.is_ok(), "Handle failed: {:#?}", handle_result);
    handle_result.unwrap()
}

use sdk::{BlobIndex, BlobTransaction, Calldata, Hashed, HyliOutput, TxContext, ZkContract};

use crate::transaction_builder::TxExecutorHandler;

/// Asserts that the given transaction can be handled by the provided TxExecutorHandler implementation
/// at the specified blob index. It constructs the necessary calldata and verifies that the outputs
/// from the handler and contract execution match.
#[track_caller]
pub fn assert_handle<H>(contract: &mut H, tx: &BlobTransaction, index: BlobIndex) -> HyliOutput
where
    H: TxExecutorHandler,
    H::Contract: ZkContract + sdk::TransactionalZkContract + borsh::BorshDeserialize + 'static,
{
    assert_handle_with_ctx::<H>(contract, tx, index, None)
}

/// Asserts that the given transaction can be handled by the provided TxExecutorHandler implementation
/// at the specified blob index with an optional transaction context. It constructs the necessary calldata
/// and verifies that the outputs from the handler and contract execution match.
#[track_caller]
pub fn assert_handle_with_ctx<H>(
    contract: &mut H,
    tx: &BlobTransaction,
    index: BlobIndex,
    tx_ctx: Option<TxContext>,
) -> HyliOutput
where
    H: TxExecutorHandler,
    H::Contract: ZkContract + sdk::TransactionalZkContract + borsh::BorshDeserialize + 'static,
{
    let calldata = Calldata {
        tx_hash: tx.hashed(),
        identity: tx.identity.clone(),
        blobs: tx.blobs.clone().into(),
        tx_blob_count: tx.blobs.len(),
        index,
        tx_ctx,
        private_input: vec![],
    };

    let blob = tx.blobs.get(index.0).expect("Blob index out of range");

    let cm = contract.build_commitment_metadata(blob);
    assert!(cm.is_ok(), "Failed to build commitment metadata: {cm:#?}");
    let cm = cm.unwrap();

    let handle_result = contract.handle(&calldata);
    assert!(
        handle_result.is_ok(),
        "TxExecutorHandler::handle() failed: {handle_result:#?}"
    );
    let handle_result = handle_result.unwrap();

    let output = sdk::guest::execute::<H::Contract>(&cm, &[calldata]);
    assert!(
        !output.is_empty(),
        "Contract execution returned empty output"
    );
    assert_eq!(
        output.len(),
        1,
        "Contract execution returned multiple outputs for a single calldata"
    );

    assert_eq!(
        output[0], handle_result,
        "Contract execution output does not match TxExecutorHandler::handle() output."
    );

    handle_result
}

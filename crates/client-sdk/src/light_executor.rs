use anyhow::Result;
use borsh::BorshDeserialize;
use sdk::{BlobIndex, BlobTransaction, StructuredBlob, TxContext};

pub struct LightExecutorOutput {
    pub success: bool,
    pub program_outputs: Vec<u8>,
}

/// Defines convenience methods for handling light contract execution with minimal overhead.
pub trait LightContractExecutor<'tx, 'extra_data> {
    type Scratchpad;
    type ExtraData;

    /// Prepares for executing a transaction, returning any data necessary to revert/commit changes.
    fn prepare_for_tx(
        &mut self,
        tx: &'tx BlobTransaction,
        index: BlobIndex,
        tx_ctx: Option<&'tx TxContext>,
        extra_data: Self::ExtraData,
    ) -> Result<Self::Scratchpad>;

    /// Handles the execution of a transaction, returning the output of the execution.
    fn handle_blob(
        &mut self,
        tx: &'tx BlobTransaction,
        index: BlobIndex,
        tx_ctx: Option<&'tx TxContext>,
        extra_data: Self::ExtraData,
    ) -> Result<LightExecutorOutput>;

    /// Should be called after a transaction fails, allowing the executor to revert side effects.
    fn on_failure(&mut self, scratchpad: Self::Scratchpad) -> Result<()>;

    /// Should be called after a transaction succeeds, allowing the executor to commit side effects.
    fn on_success(&mut self, scratchpad: Self::Scratchpad) -> Result<()>;
}

/// Variant of parse_structured_blob that returns a structured blob from a transaction.
pub fn parse_structured_blob_from_tx<Action>(
    tx: &BlobTransaction,
    index: BlobIndex,
) -> Option<StructuredBlob<Action>>
where
    Action: BorshDeserialize,
{
    let blob = match tx.blobs.get(index.0) {
        Some(v) => v,
        None => {
            return None;
        }
    };

    let parsed_blob: StructuredBlob<Action> = match StructuredBlob::try_from(blob.clone()) {
        Ok(v) => v,
        Err(_) => {
            return None;
        }
    };
    Some(parsed_blob)
}

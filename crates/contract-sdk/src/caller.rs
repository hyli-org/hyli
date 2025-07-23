use alloc::string::String;
use alloc::vec::Vec;
use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};

use hyli_model::{Blob, ContractName, Identity, StructuredBlob};

/// ExecutionContext provides an implementation of data for the CallerCallee trait
#[derive(Debug, Serialize, Deserialize, Clone, BorshSerialize, BorshDeserialize, Default)]
pub struct ExecutionContext {
    pub callees_blobs: Vec<Blob>,
    pub caller: Identity,
    pub contract_name: ContractName,
}

impl ExecutionContext {
    pub fn new(caller: Identity, contract_name: ContractName) -> Self {
        ExecutionContext {
            callees_blobs: Vec::new(),
            caller,
            contract_name,
        }
    }

    pub fn is_in_callee_blobs<U>(
        &mut self,
        contract_name: &ContractName,
        action: U,
    ) -> Result<(), String>
    where
        U: BorshDeserialize + PartialEq,
        StructuredBlob<U>: TryFrom<Blob>,
    {
        let index = self.callees_blobs.iter().position(|blob| {
            if &blob.contract_name != contract_name {
                return false;
            };
            let Ok(blob) = StructuredBlob::try_from(blob.clone()) else {
                return false;
            };
            blob.data.parameters == action
        });

        match index {
            Some(index) => {
                self.callees_blobs.remove(index);
                Ok(())
            }
            None => Err(alloc::format!(
                "Blob with contract name {contract_name} not found in callees"
            )),
        }
    }
}

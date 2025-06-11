use crate::UuidTld;
use client_sdk::{
    helpers::risc0::Risc0Prover,
    transaction_builder::{
        StateUpdater, TxExecutorBuilder, TxExecutorHandler, TxExecutorHandlerContext,
        TxExecutorHandlerResult,
    },
};
use sdk::{
    utils::as_hyle_output, Blob, Calldata, ContractName, RegisterContractEffect, StateCommitment,
    ZkContract,
};

pub mod metadata {
    pub const UUID_TLD_ELF: &[u8] = include_bytes!("../../uuid-tld.img");
    pub const PROGRAM_ID: [u8; 32] = sdk::str_to_u8(include_str!("../../uuid-tld.txt"));
}

impl UuidTld {
    pub fn setup_builder<S: StateUpdater>(
        &self,
        contract_name: ContractName,
        builder: &mut TxExecutorBuilder<S>,
    ) {
        builder.init_with(contract_name, Risc0Prover::new(metadata::UUID_TLD_ELF));
    }
}

impl TxExecutorHandler for UuidTld {
    fn build_commitment_metadata(&self, _blob: &Blob) -> TxExecutorHandlerResult<Vec<u8>> {
        borsh::to_vec(self).context("Failed to serialize UuidTld")
    }

    fn handle(&mut self, calldata: &Calldata) -> TxExecutorHandlerResult<sdk::HyleOutput> {
        let initial_state_commitment = <Self as ZkContract>::commit(self);
        let mut res = <Self as ZkContract>::execute(self, calldata);
        let next_state_commitment = <Self as ZkContract>::commit(self);
        Ok(as_hyle_output(
            initial_state_commitment,
            next_state_commitment,
            calldata,
            &mut res,
        ))
    }

    fn construct_state(
        _register_blob: &RegisterContractEffect,
        _metadata: &Option<Vec<u8>>,
    ) -> TxExecutorHandlerResult<Self> {
        Ok(Self::default())
    }

    fn get_state_commitment(&self) -> StateCommitment {
        self.commit()
    }
}

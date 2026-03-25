use crate::UuidTld;
use client_sdk::transaction_builder::{
    StateUpdater, TxExecutorBuilder, TxExecutorHandler, TxExecutorHandlerContext,
    TxExecutorHandlerResult,
};
use sdk::{utils::as_hyli_output, Calldata, ContractName, StateCommitment, ZkContract};

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
        #[cfg(feature = "risc0")]
        let prover = client_sdk::helpers::risc0::Risc0Prover::new(
            metadata::UUID_TLD_ELF.to_vec(),
            metadata::PROGRAM_ID,
        );
        #[cfg(not(feature = "risc0"))]
        let prover = client_sdk::helpers::NoopProver::<UuidTld>::new(&metadata::PROGRAM_ID);
        builder.init_with(contract_name, prover);
    }
}

impl TxExecutorHandler for UuidTld {
    type Contract = UuidTld;

    fn build_commitment_metadata(&self, _calldata: &Calldata) -> TxExecutorHandlerResult<Vec<u8>> {
        borsh::to_vec(self).context("Failed to serialize UuidTld")
    }

    fn handle(&mut self, calldata: &Calldata) -> TxExecutorHandlerResult<sdk::HyliOutput> {
        let initial_state_commitment = <Self as ZkContract>::commit(self);
        let mut res = <Self as ZkContract>::execute(self, calldata);
        let next_state_commitment = <Self as ZkContract>::commit(self);
        Ok(as_hyli_output(
            initial_state_commitment,
            next_state_commitment,
            calldata,
            &mut res,
        ))
    }

    fn construct_state(
        _: &sdk::ContractName,
        _: &sdk::Contract,
        _: &Option<Vec<u8>>,
    ) -> TxExecutorHandlerResult<Self> {
        Ok(Self::default())
    }

    fn get_state_commitment(&self) -> StateCommitment {
        self.commit()
    }
}

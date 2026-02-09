use crate::{Hydentity, HydentityAction};
use anyhow::{Context, Result};
use client_sdk::{
    helpers::risc0::Risc0Prover,
    transaction_builder::{ProvableBlobTx, StateUpdater, TxExecutorBuilder, TxExecutorHandler},
};
use sdk::{Blob, Calldata, ContractName, StateCommitment, ZkContract, utils::as_hyli_output};

pub mod metadata {
    pub const HYDENTITY_ELF: &[u8] = include_bytes!("../../hydentity.img");
    pub const PROGRAM_ID: [u8; 32] = sdk::str_to_u8(include_str!("../../hydentity.txt"));
}
use metadata::*;

impl TxExecutorHandler for Hydentity {
    type Contract = Hydentity;

    fn build_commitment_metadata(&self, _blob: &Blob) -> Result<Vec<u8>> {
        borsh::to_vec(self).context("Failed to serialize Hydentity")
    }

    fn handle(&mut self, calldata: &Calldata) -> Result<sdk::HyliOutput> {
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
    ) -> Result<Self> {
        Ok(Self::default())
    }

    fn get_state_commitment(&self) -> StateCommitment {
        self.commit()
    }
}

impl Hydentity {
    pub fn setup_builder<S: StateUpdater>(
        &self,
        contract_name: ContractName,
        builder: &mut TxExecutorBuilder<S>,
    ) {
        builder.init_with(
            contract_name,
            Risc0Prover::new(HYDENTITY_ELF.to_vec(), PROGRAM_ID),
        );
    }
}

pub fn verify_identity(
    builder: &mut ProvableBlobTx,
    contract_name: ContractName,
    state: &Hydentity,
    password: String,
) -> anyhow::Result<()> {
    let nonce = state
        .get_nonce(builder.identity.0.as_str())
        .map_err(|e| anyhow::anyhow!(e))?;

    let password = password.into_bytes().to_vec();

    builder.add_action(
        contract_name,
        HydentityAction::VerifyIdentity {
            account: builder.identity.0.clone(),
            nonce,
        },
        Some(password),
        None,
        None,
    )?;
    Ok(())
}

pub fn register_identity(
    builder: &mut ProvableBlobTx,
    contract_name: ContractName,
    password: String,
) -> anyhow::Result<()> {
    let name = builder.identity.0.clone();
    let account = Hydentity::build_id(&name, &password);

    builder.add_action(
        contract_name,
        HydentityAction::RegisterIdentity { account },
        Some(password.into_bytes().to_vec()),
        None,
        None,
    )?;
    Ok(())
}

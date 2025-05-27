use anyhow::{Context, Result};
use client_sdk::{
    helpers::risc0::Risc0Prover,
    transaction_builder::{ProvableBlobTx, StateUpdater, TxExecutorBuilder, TxExecutorHandler},
};
use hyllar::HyllarAction;
use sdk::{
    utils::as_hyle_output, Blob, BlobIndex, Calldata, ContractName, RegisterContractEffect,
    ZkContract,
};

use crate::{Amm, AmmAction};

pub mod metadata {
    pub const AMM_ELF: &[u8] = include_bytes!("../../amm.img");
    pub const PROGRAM_ID: [u8; 32] = sdk::str_to_u8(include_str!("../../amm.txt"));
}
use metadata::*;

impl TxExecutorHandler for Amm {
    fn build_commitment_metadata(&self, _blob: &Blob) -> Result<Vec<u8>> {
        borsh::to_vec(self).context("Failed to serialize Hyllar")
    }

    fn handle(&mut self, calldata: &Calldata) -> Result<sdk::HyleOutput> {
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
    ) -> Result<Self> {
        Ok(Self::default())
    }
}

impl Amm {
    pub fn setup_builder<S: StateUpdater>(
        &self,
        contract_name: ContractName,
        builder: &mut TxExecutorBuilder<S>,
    ) {
        builder.init_with(contract_name, Risc0Prover::new(AMM_ELF));
    }
}

pub fn new_pair(
    builder: &mut ProvableBlobTx,
    contract_name: ContractName,
    pair: (ContractName, ContractName),
    amounts: (u128, u128),
) -> anyhow::Result<()> {
    let idx = builder.blobs.len();
    builder.add_action(
        contract_name.clone(),
        AmmAction::NewPair {
            pair: (pair.0.to_string(), pair.1.to_string()),
            amounts,
        },
        None,
        None,
        Some(vec![BlobIndex(idx + 1), BlobIndex(idx + 2)]),
    )?;
    builder.add_action(
        pair.0,
        HyllarAction::TransferFrom {
            owner: builder.identity.0.clone(),
            recipient: contract_name.to_string(),
            amount: amounts.0,
        },
        None,
        Some(BlobIndex(idx)),
        None,
    )?;
    builder.add_action(
        pair.1,
        HyllarAction::TransferFrom {
            owner: builder.identity.0.clone(),
            recipient: contract_name.to_string(),
            amount: amounts.1,
        },
        None,
        Some(BlobIndex(idx)),
        None,
    )?;
    Ok(())
}

pub fn swap(
    builder: &mut ProvableBlobTx,
    contract_name: ContractName,
    pair: (ContractName, ContractName),
    amounts: (u128, u128),
) -> anyhow::Result<()> {
    let idx = builder.blobs.len();
    builder.add_action(
        contract_name.clone(),
        AmmAction::Swap {
            pair: (pair.0.to_string(), pair.1.to_string()),
            amounts,
        },
        None,
        None,
        Some(vec![BlobIndex(idx + 1), BlobIndex(idx + 2)]),
    )?;

    builder.add_action(
        pair.0,
        HyllarAction::TransferFrom {
            owner: builder.identity.0.clone(),
            recipient: contract_name.to_string(),
            amount: amounts.0,
        },
        None,
        Some(BlobIndex(idx)),
        None,
    )?;
    builder.add_action(
        pair.1,
        HyllarAction::Transfer {
            recipient: builder.identity.0.clone(),
            amount: amounts.1,
        },
        None,
        Some(BlobIndex(idx)),
        None,
    )?;

    Ok(())
}

use anyhow::{Context, Result};
use client_sdk::{
    helpers::risc0::Risc0Prover,
    transaction_builder::{ProvableBlobTx, StateUpdater, TxExecutorBuilder, TxExecutorHandler},
};
use sdk::{
    utils::as_hyle_output, Blob, Calldata, ContractName, RegisterContractEffect, ZkContract,
};

use crate::{Hyllar, HyllarAction};

pub mod metadata {
    pub const HYLLAR_ELF: &[u8] = include_bytes!("../../hyllar.img");
    pub const PROGRAM_ID: [u8; 32] = sdk::str_to_u8(include_str!("../../hyllar.txt"));
}
use metadata::*;

impl TxExecutorHandler for Hyllar {
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

impl Hyllar {
    pub fn setup_builder<S: StateUpdater>(
        &self,
        contract_name: ContractName,
        builder: &mut TxExecutorBuilder<S>,
    ) {
        builder.init_with(
            contract_name.clone(),
            Risc0Prover::new(HYLLAR_ELF, contract_name.0.clone()),
        );
    }
}

pub fn transfer(
    builder: &mut ProvableBlobTx,
    contract_name: ContractName,
    recipient: String,
    amount: u128,
) -> anyhow::Result<()> {
    builder.add_action(
        contract_name,
        HyllarAction::Transfer { recipient, amount },
        None,
        None,
        None,
    )?;
    Ok(())
}

pub fn transfer_from(
    builder: &mut ProvableBlobTx,
    contract_name: ContractName,
    sender: String,
    recipient: String,
    amount: u128,
) -> anyhow::Result<()> {
    builder.add_action(
        contract_name,
        HyllarAction::TransferFrom {
            owner: sender,
            recipient,
            amount,
        },
        None,
        None,
        None,
    )?;
    Ok(())
}

pub fn approve(
    builder: &mut ProvableBlobTx,
    contract_name: ContractName,
    spender: String,
    amount: u128,
) -> anyhow::Result<()> {
    builder.add_action(
        contract_name,
        HyllarAction::Approve { spender, amount },
        None,
        None,
        None,
    )?;
    Ok(())
}

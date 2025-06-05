use anyhow::Result;
use client_sdk::{
    helpers::risc0::Risc0Prover,
    transaction_builder::{ProvableBlobTx, StateUpdater, TxExecutorBuilder, TxExecutorHandler},
};
use sdk::{
    api::{APIFees, APIFeesBalance, APIStaking},
    utils::as_hyle_output,
    Blob, Calldata, ContractName, RegisterContractEffect, StakingAction, ValidatorPublicKey,
    ZkContract,
};

use crate::{
    fees::{Fees, ValidatorFeeState},
    state::Staking,
};

pub mod metadata {
    pub const STAKING_ELF: &[u8] = include_bytes!("../../staking.img");
    pub const PROGRAM_ID: [u8; 32] = sdk::str_to_u8(include_str!("../../staking.txt"));
}
use metadata::*;

impl TxExecutorHandler for Staking {
    fn build_commitment_metadata(&self, _blob: &Blob) -> Result<Vec<u8>> {
        borsh::to_vec(self).map_err(Into::into)
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

impl Staking {
    pub fn setup_builder<S: StateUpdater>(
        &self,
        contract_name: ContractName,
        builder: &mut TxExecutorBuilder<S>,
    ) {
        builder.init_with(
            contract_name.clone(),
            Risc0Prover::new(STAKING_ELF, contract_name.0.clone()),
        );
    }
}

impl From<Staking> for APIStaking {
    fn from(val: Staking) -> Self {
        APIStaking {
            stakes: val.stakes,
            rewarded: val.rewarded,
            bonded: val.bonded,
            delegations: val.delegations,
            total_bond: val.total_bond,
            fees: val.fees.into(),
        }
    }
}

impl From<Fees> for APIFees {
    fn from(val: Fees) -> Self {
        APIFees {
            balances: val
                .balances
                .into_iter()
                .map(|(val, b)| {
                    (
                        val,
                        APIFeesBalance {
                            balance: b.balance,
                            cumul_size: b.paid_cumul_size,
                        },
                    )
                })
                .collect(),
        }
    }
}

impl From<APIStaking> for Staking {
    fn from(val: APIStaking) -> Self {
        Staking {
            stakes: val.stakes,
            rewarded: val.rewarded,
            bonded: val.bonded,
            delegations: val.delegations,
            total_bond: val.total_bond,
            fees: val.fees.into(),
        }
    }
}

impl From<APIFees> for Fees {
    fn from(val: APIFees) -> Self {
        Fees {
            pending_fees: vec![],
            balances: val
                .balances
                .into_iter()
                .map(|(val, b)| {
                    (
                        val,
                        ValidatorFeeState {
                            balance: b.balance,
                            paid_cumul_size: b.cumul_size,
                        },
                    )
                })
                .collect(),
        }
    }
}

pub fn deposit_for_fees(
    builder: &mut ProvableBlobTx,
    contract_name: ContractName,
    holder: ValidatorPublicKey,
    amount: u128,
) -> anyhow::Result<()> {
    builder.add_action(
        contract_name,
        StakingAction::DepositForFees {
            holder: holder.clone(),
            amount,
        },
        None,
        None,
        None,
    )?;
    Ok(())
}

impl Staking {
    pub fn to_bytes(&self) -> Vec<u8> {
        borsh::to_vec(self).expect("Failed to encode Balances")
    }
}

pub fn stake(
    builder: &mut ProvableBlobTx,
    contract_name: ContractName,
    amount: u128,
) -> anyhow::Result<()> {
    builder.add_action(
        contract_name,
        StakingAction::Stake { amount },
        None,
        None,
        None,
    )?;
    Ok(())
}

pub fn delegate(builder: &mut ProvableBlobTx, validator: ValidatorPublicKey) -> anyhow::Result<()> {
    // FIXME: hardcoded contract names
    let staking_contract_name = ContractName("staking".to_string());
    builder.add_action(
        staking_contract_name,
        StakingAction::Delegate {
            validator: validator.clone(),
        },
        None,
        None,
        None,
    )?;
    Ok(())
}

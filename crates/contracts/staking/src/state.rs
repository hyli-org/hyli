use std::collections::BTreeMap;

use anyhow::Result;
use borsh::{BorshDeserialize, BorshSerialize};
use sdk::{info, BlockHeight, Identity, LaneBytesSize, LaneId, ValidatorPublicKey};
use serde::{Deserialize, Serialize};

use crate::fees::Fees;

#[derive(Debug, Serialize, Deserialize, Clone, BorshSerialize, BorshDeserialize, PartialEq, Eq)]
pub struct Staking {
    pub(crate) stakes: BTreeMap<Identity, u128>,
    pub(crate) delegations: BTreeMap<ValidatorPublicKey, Vec<Identity>>,
    /// When a validator distribute rewards, it is added in this list to
    /// avoid distributing twice the rewards for a same block
    pub(crate) rewarded: BTreeMap<ValidatorPublicKey, Vec<BlockHeight>>,

    /// List of validators that are part of consensus
    pub(crate) bonded: Vec<ValidatorPublicKey>,
    pub(crate) total_bond: u128,

    /// Struct to handle fees
    pub(crate) fees: Fees,
}

/// Minimal stake necessary to be part of consensus
pub const MIN_STAKE: u128 = 32;

impl Staking {
    pub fn new() -> Self {
        Staking {
            stakes: BTreeMap::new(),
            delegations: BTreeMap::new(),
            rewarded: BTreeMap::new(),
            bonded: Vec::new(),
            total_bond: 0,
            fees: Fees::default(),
        }
    }

    pub fn is_known(&self, key: &ValidatorPublicKey) -> bool {
        self.delegations.keys().any(|v| v == key)
    }

    pub fn bonded(&self) -> &Vec<ValidatorPublicKey> {
        &self.bonded
    }
    /// Get the total bonded amount
    pub fn total_bond(&self) -> u128 {
        self.total_bond
    }
    pub fn is_bonded(&self, pubkey: &ValidatorPublicKey) -> bool {
        self.bonded.iter().any(|v| v == pubkey)
    }

    /// Bond a staking validator
    pub fn bond(&mut self, validator: ValidatorPublicKey) -> Result<(), String> {
        if self.is_bonded(&validator) {
            return Err("Validator already bonded".to_string());
        }

        info!("🔐 Bonded validator {}", validator);
        if let Some(stake) = self.get_stake(&validator) {
            if stake < MIN_STAKE {
                return Err("Validator does not have enough stake".to_string());
            }
            self.bonded.push(validator);
            self.bonded.sort(); // TODO insert in order?
            self.total_bond += stake;
            Ok(())
        } else {
            Err("Validator does not have enough stake".to_string())
        }
    }

    /// Compute f value
    pub fn compute_f(&self) -> u128 {
        self.total_bond().div_euclid(3)
    }

    pub fn compute_voting_power(&self, validators: &[ValidatorPublicKey]) -> u128 {
        // Deduplicate validators before computing voting power
        let mut unique_validators = validators.to_vec();
        unique_validators.sort();
        unique_validators.dedup();
        unique_validators
            .iter()
            .flat_map(|v| self.get_stake(v))
            .sum::<u128>()
    }

    pub fn get_stake(&self, validator: &ValidatorPublicKey) -> Option<u128> {
        self.delegations.get(validator).map(|delegations| {
            delegations
                .iter()
                .map(|delegator| self.stakes.get(delegator).unwrap_or(&0))
                .sum()
        })
    }

    pub fn stake(&mut self, staker: Identity, amount: u128) -> Result<String, String> {
        // Staking is only allowed if the staker is not already staked
        if self.stakes.contains_key(&staker) {
            return Err(format!(
                "Staking balance is frozen for {} (already staked)",
                staker
            ));
        }

        info!("💰 Adding {} to stake for {}", amount, staker);
        self.stakes
            .entry(staker)
            .and_modify(|e| *e += amount)
            .or_insert(amount);
        Ok("Staked".to_string())
    }

    /// Delegate to a validator, or fail if already delegated to another validator
    pub fn delegate_to(
        &mut self,
        staker: Identity,
        validator: ValidatorPublicKey,
    ) -> Result<String, String> {
        info!("🤝 New delegation from {} to {}", staker, validator);
        if self.delegations.values().flatten().any(|v| v == &staker) {
            return Err("Already delegated".to_string());
        }

        self.delegations
            .entry(validator)
            .and_modify(|e| e.push(staker.clone()))
            .or_insert_with(|| vec![staker]);
        Ok("Delegated".to_string())
    }

    //    ----------
    //      Fees
    //    ----------

    /// Deposit funds to be distributed as fees
    /// This function is meant to be called from BlobTransaction
    pub fn deposit_for_fees(
        &mut self,
        holder: ValidatorPublicKey,
        amount: u128,
    ) -> Result<String, String> {
        self.fees.deposit_for_fees(holder, amount);
        Ok("Deposited".to_string())
    }

    /// Store the fees to be distributed
    /// DaDi = Data dissemination
    /// This function is meant to be called by the consensus
    pub fn pay_for_dadi(
        &mut self,
        lane_id: LaneId,
        cumul_size: LaneBytesSize,
    ) -> Result<(), String> {
        // TODO: allow more complex mechanisms - for now 1-1 mapping between a validator and a lane
        self.fees.pay_for_dadi(lane_id.0, cumul_size)?;
        Ok(())
    }

    /// Distribute the fees to the bonded validators
    /// This function is meant to be called by the consensus
    pub fn distribute(&mut self) -> Result<(), String> {
        self.fees.distribute(&self.bonded)
    }

    /// Update the state of staking with staking actions in a block
    #[cfg(feature = "client")]
    pub fn process_block(&mut self, block: &sdk::Block) -> Result<(), String> {
        use sdk::StakingAction;
        for action in &block.staking_actions {
            match action.clone() {
                (identity, StakingAction::Stake { amount }) => {
                    self.stake(identity, amount)?;
                }
                (identity, StakingAction::Delegate { validator }) => {
                    self.delegate_to(identity, validator)?;
                }
                (_identity, StakingAction::Distribute { claim: _ }) => todo!(),
                (_identity, StakingAction::DepositForFees { holder, amount }) => {
                    self.deposit_for_fees(holder, amount)?;
                }
            }
        }
        for validator in block.new_bounded_validators.iter() {
            self.bond(validator.clone())?;
        }
        Ok(())
    }
}

impl Default for Staking {
    fn default() -> Self {
        Self::new()
    }
}

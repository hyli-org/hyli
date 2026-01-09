use std::collections::BTreeMap;

use borsh::{BorshDeserialize, BorshSerialize};
use sdk::{LaneBytesSize, LaneId, ValidatorPublicKey};
use serde::{Deserialize, Serialize};

#[derive(
    Debug, Default, Serialize, Deserialize, Clone, BorshSerialize, BorshDeserialize, PartialEq, Eq,
)]
pub struct ValidatorFeeState {
    /// balance could go negative, the validator would then not be able to
    /// disseminate anymore, and would need to increase its balance first.
    pub(crate) balance: i128,
    /// Cumulative size of the data disseminated per lane that has already been paid for
    pub(crate) paid_cumul_sizes: BTreeMap<LaneId, LaneBytesSize>,
}

#[derive(
    Debug, Default, Serialize, Deserialize, Clone, BorshSerialize, BorshDeserialize, PartialEq, Eq,
)]
pub struct Fees {
    /// Cumulative size of the data disseminated per lane, pending fee distribution
    pub(crate) pending_fees: Vec<(LaneId, LaneBytesSize)>,

    /// Balance of each validator
    pub(crate) balances: BTreeMap<ValidatorPublicKey, ValidatorFeeState>,
}

impl Fees {
    /// Deposit funds to be distributed as fees
    pub fn deposit_for_fees(&mut self, holder: ValidatorPublicKey, amount: u128) {
        self.balances.entry(holder).or_default().balance += amount as i128;
    }

    /// Store the fees to be distributed
    /// DaDi = Data dissemination
    pub(crate) fn pay_for_dadi(
        &mut self,
        lane_id: LaneId,
        cumul_size: LaneBytesSize,
    ) -> Result<(), String> {
        self.pending_fees.push((lane_id, cumul_size));

        Ok(())
    }

    /// Distribute the fees to the bonded validators
    /// The current strategy is quite dummy, it can be improved!
    ///
    /// We could imagine other strategies of distribution, like distributing
    /// the fees for the validators that voted on these DP. For this we would
    /// need to pass the PoDa to the pay_for_dadi function
    pub(crate) fn distribute(&mut self, bonded: &[ValidatorPublicKey]) -> Result<(), String> {
        let fee_per_byte = 1; // TODO: this value could be computed & change over time
        for (lane_id, cumul_size) in self.pending_fees.drain(..) {
            let Some(disseminator) = self.balances.get_mut(&lane_id.operator) else {
                // We should never come here, as the disseminator should have a balance
                // It should be checked by the validator when voting on the DataProposal
                // TODO: I think we sould not fail here, as it will hang the consensus...
                return Err("Logic issue: disseminator not found in balances".to_string());
            };

            let prev_cumul_size = disseminator
                .paid_cumul_sizes
                .get(&lane_id)
                .cloned()
                .unwrap_or_default();

            if cumul_size.0 < prev_cumul_size.0 {
                // We should never come here, as the cumul_size should always increase
                // It should be checked by the validator when voting on the DataProposal
                return Err(format!(
                    "Logic issue: cumul_size should always increase. {} < {}",
                    cumul_size.0, prev_cumul_size.0
                ));
            }

            let unpaid_size = cumul_size.0 - prev_cumul_size.0;
            let fee = (unpaid_size * fee_per_byte) as i128;
            disseminator.balance -= fee;
            disseminator.paid_cumul_sizes.insert(lane_id, cumul_size);

            // TODO: we might loose some token here as the division is rounded
            let fee_per_validator = fee / bonded.len() as i128;
            for validator in bonded.iter() {
                let state = self.balances.entry(validator.clone()).or_default();
                state.balance += fee_per_validator;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deposit_for_fees() {
        let mut fees = Fees::default();
        let validator = ValidatorPublicKey::default();
        fees.deposit_for_fees(validator.clone(), 100);
        assert_eq!(fees.balances.get(&validator).unwrap().balance, 100);
    }

    #[test]
    fn test_pay_for_dadi() {
        let mut fees = Fees::default();
        let validator = ValidatorPublicKey::default();
        let lane_id = LaneId::new(validator.clone());
        let cumul_size = LaneBytesSize(100);
        fees.pay_for_dadi(lane_id.clone(), cumul_size).unwrap();
        assert_eq!(fees.pending_fees.len(), 1);
        assert_eq!(fees.pending_fees[0], (lane_id, cumul_size));
    }

    #[test]
    fn test_distribute() {
        let mut fees = Fees::default();
        let validator1 = ValidatorPublicKey::new_for_tests("p1");
        let validator2 = ValidatorPublicKey::new_for_tests("p2");
        let lane_id = LaneId::new(validator1.clone());
        let cumul_size = LaneBytesSize(100);

        fees.deposit_for_fees(validator1.clone(), 200);
        fees.pay_for_dadi(lane_id, cumul_size).unwrap();
        fees.distribute(&[validator1.clone(), validator2.clone()])
            .unwrap();

        assert_eq!(fees.balances.get(&validator1).unwrap().balance, 150);
        assert_eq!(fees.balances.get(&validator2).unwrap().balance, 50);
    }

    #[test]
    fn test_distribute_with_no_balance() {
        let mut fees = Fees::default();
        let validator1 = ValidatorPublicKey::new_for_tests("p1");
        let validator2 = ValidatorPublicKey::new_for_tests("p2");
        let lane_id = LaneId::new(validator1.clone());
        let cumul_size = LaneBytesSize(100);

        fees.pay_for_dadi(lane_id, cumul_size).unwrap();
        let result = fees.distribute(&[validator1.clone(), validator2.clone()]);
        assert!(result.is_err());
    }

    #[test]
    fn test_distribute_with_decreasing_cumul_size() {
        let mut fees = Fees::default();
        let validator = ValidatorPublicKey::default();
        let lane_id = LaneId::new(validator.clone());
        let cumul_size1 = LaneBytesSize(100);
        let cumul_size2 = LaneBytesSize(50);

        fees.deposit_for_fees(validator.clone(), 200);
        fees.pay_for_dadi(lane_id.clone(), cumul_size1).unwrap();
        fees.pay_for_dadi(lane_id, cumul_size2).unwrap();
        let result = fees.distribute(std::slice::from_ref(&validator));
        assert!(result.is_err());
    }
}

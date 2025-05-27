use hyllar::HyllarAction;
use sdk::{
    utils::parse_calldata, BlobIndex, Calldata, IndexedBlobs, RunResult, StakingAction, ZkContract,
};
use sha2::{Digest, Sha256};
use state::Staking;

#[cfg(feature = "client")]
pub mod client;

pub mod fees;
pub mod state;

impl ZkContract for Staking {
    fn execute(&mut self, calldata: &Calldata) -> RunResult {
        let (action, execution_ctx) = parse_calldata::<StakingAction>(calldata)?;

        let output = match action {
            StakingAction::Stake { amount } => {
                check_transfer_blob(&calldata.blobs, calldata.index + 1, amount)?;
                self.stake(execution_ctx.caller.clone(), amount)
            }
            StakingAction::Delegate { validator } => {
                self.delegate_to(execution_ctx.caller.clone(), validator)
            }
            StakingAction::Distribute { claim: _ } => todo!(),
            StakingAction::DepositForFees { holder, amount } => {
                check_transfer_blob(&calldata.blobs, calldata.index + 1, amount)?;
                self.deposit_for_fees(holder, amount)
            }
        };

        match output {
            Err(e) => Err(e),
            Ok(output) => Ok((output.into_bytes(), execution_ctx, vec![])),
        }
    }

    /// On-chain state is a hash of parts of the state that are altered only
    /// by BlobTransactions
    /// Other parts of the states (handled by consensus) are not part of on-chain state
    fn commit(&self) -> sdk::StateCommitment {
        let mut hasher = Sha256::new();
        for s in self.stakes.iter() {
            hasher.update(&s.0 .0);
            hasher.update(s.1.to_le_bytes());
        }
        for d in self.delegations.iter() {
            hasher.update(&d.0 .0);
            for i in d.1 {
                hasher.update(&i.0);
            }
        }
        for r in self.rewarded.iter() {
            hasher.update(&r.0 .0);
            for i in r.1 {
                hasher.update(i.0.to_le_bytes());
            }
        }
        sdk::StateCommitment(hasher.finalize().to_vec())
    }
}

fn check_transfer_blob(blobs: &IndexedBlobs, index: BlobIndex, amount: u128) -> Result<(), String> {
    let transfer_action = sdk::utils::parse_structured_blob::<HyllarAction>(blobs, &index)
        .ok_or("No transfer blob found".to_string())?;
    match transfer_action.data.parameters {
        HyllarAction::Transfer {
            recipient,
            amount: transfer_amount,
        } => {
            if recipient != "staking" {
                return Err(format!(
                    "Transfer recipient should be 'staking' but was {}",
                    &recipient
                ));
            }

            let transfer_contract = transfer_action.contract_name;
            if transfer_contract.0 != "hyllar" {
                return Err(format!(
                    "Only hyllar token are accepted to stake. Got {transfer_contract}."
                ));
            }

            if amount != transfer_amount {
                return Err(format!(
                    "Transfer amount {transfer_amount} mismatch Stake amount {amount}"
                ));
            }

            Ok(())
        }
        els => Err(format!(
            "Wrong HyllarAction, should be a transfer {:?} to 'staking' but was {:?}",
            amount, els
        )),
    }
}

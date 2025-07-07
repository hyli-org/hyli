use anyhow::{anyhow, bail, Context, Result};
use borsh::{BorshDeserialize, BorshSerialize};
use client_sdk::light_executor::{
    parse_structured_blob_from_tx, LightContractExecutor, LightExecutorOutput,
};
use sdk::{BlobIndex, BlobTransaction, Identity, TxContext};
use std::collections::{BTreeMap, HashMap};

use crate::{account::Account, SmtTokenAction, FAUCET_ID, TOTAL_SUPPLY};

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct LightSmtExecutor {
    pub balances: HashMap<Identity, Account>,
}

impl Default for LightSmtExecutor {
    fn default() -> Self {
        let mut balances = HashMap::new();
        // Initialize the faucet account with a total supply
        let faucet_account = Account {
            address: FAUCET_ID.into(),
            balance: TOTAL_SUPPLY,
            allowances: BTreeMap::new(),
        };
        balances.insert(faucet_account.address.clone(), faucet_account);
        LightSmtExecutor { balances }
    }
}

impl LightContractExecutor<'_, '_> for LightSmtExecutor {
    type Scratchpad = ();
    type ExtraData = ();

    fn prepare_for_tx(
        &mut self,
        _tx: &BlobTransaction,
        _index: BlobIndex,
        _tx_ctx: Option<&TxContext>,
        _extra_data: Self::ExtraData,
    ) -> Result<Self::Scratchpad> {
        Ok(())
    }

    fn handle_blob(
        &mut self,
        tx: &BlobTransaction,
        index: BlobIndex,
        _tx_ctx: Option<&TxContext>,
        _extra_data: Self::ExtraData,
    ) -> Result<LightExecutorOutput> {
        let Some(parsed_blob) = parse_structured_blob_from_tx::<SmtTokenAction>(tx, index) else {
            return Err(anyhow!("Failed to parse structured blob from transaction"));
        };

        self.inner_handle(parsed_blob.data.parameters)
            .map(|ok| LightExecutorOutput {
                success: true,
                program_outputs: ok.into_bytes(),
            })
            .or_else(|err| {
                Ok(LightExecutorOutput {
                    success: false,
                    program_outputs: err.to_string().into_bytes(),
                })
            })
    }

    // Nothing to do on failure / success, we don't actually change the state.
    fn on_failure(&mut self, _scratchpad: Self::Scratchpad) -> Result<()> {
        Ok(())
    }
    fn on_success(&mut self, _scratchpad: Self::Scratchpad) -> Result<()> {
        Ok(())
    }
}

impl LightSmtExecutor {
    pub fn inner_handle(&mut self, action: SmtTokenAction) -> Result<String> {
        match action {
            SmtTokenAction::Transfer {
                sender,
                recipient,
                amount,
            } => {
                let sender_account = self
                    .balances
                    .get_mut(&sender)
                    .ok_or(anyhow!("Sender account {sender} not found"))?;
                if sender == recipient {
                    return Ok(format!("Transferred {amount} to {recipient}"));
                } else {
                    sender_account.balance = sender_account
                        .balance
                        .checked_sub(amount)
                        .context("Insufficient balance")?;
                }
                let recipient_account = self.balances.entry(recipient).or_default();
                match recipient_account.balance.checked_add(amount) {
                    Some(new_balance) => {
                        recipient_account.balance = new_balance;
                    }
                    None => {
                        // Revert sub for idempotency
                        let sender_account = self.balances.get_mut(&sender).unwrap();
                        sender_account.balance += amount;
                        return Err(anyhow!("Overflow in recipient balance"));
                    }
                }

                Ok(format!(
                    "Transferred {} to {}",
                    amount, recipient_account.address
                ))
            }
            SmtTokenAction::TransferFrom {
                owner,
                spender,
                recipient,
                amount,
            } => {
                let owner_account = self
                    .balances
                    .get_mut(&owner)
                    .ok_or(anyhow!("Owner account {} not found", owner))?;
                if owner == recipient {
                    return Ok(format!("Transferred {amount} to {recipient}"));
                } else {
                    // Check allowance
                    let allowance = owner_account.allowances.get(&spender).cloned().unwrap_or(0);
                    if allowance < amount {
                        bail!(
                            "Allowance exceeded for spender={} owner={} allowance={}",
                            spender,
                            owner_account.address,
                            allowance
                        );
                    }

                    owner_account.balance = owner_account
                        .balance
                        .checked_sub(amount)
                        .context("Insufficient balance")?;

                    // Do this second for idempotency
                    owner_account.update_allowances(
                        spender.clone(),
                        allowance
                            .checked_sub(amount)
                            .context("Allowance underflow")?,
                    );
                }
                let recipient_account = self.balances.entry(recipient).or_default();
                match recipient_account
                    .balance
                    .checked_add(amount)
                    .context("Overflow in recipient balance")
                {
                    Ok(new_balance) => {
                        recipient_account.balance = new_balance;
                    }
                    Err(err) => {
                        // Revert sub for idempotency
                        let owner_account = self.balances.get_mut(&owner).unwrap();
                        owner_account.balance += amount;
                        owner_account.update_allowances(
                            spender.clone(),
                            owner_account.allowances.get(&spender).cloned().unwrap_or(0) + amount,
                        );
                        return Err(err);
                    }
                }

                Ok(format!(
                    "Transferred {} to {}",
                    amount, recipient_account.address
                ))
            }
            SmtTokenAction::Approve {
                owner,
                spender,
                amount,
            } => {
                let owner_account = self
                    .balances
                    .get_mut(&owner)
                    .ok_or(anyhow!("Owner account {} not found", owner))?;
                owner_account.update_allowances(spender.clone(), amount);
                Ok(format!("Approved {amount} to {spender}"))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use hyle_smt_token::client::light_executor::LightSmtExecutor;
    use hyle_smt_token::{SmtTokenAction, FAUCET_ID, TOTAL_SUPPLY};
    use sdk::Identity;

    fn identity(name: &str) -> Identity {
        Identity::from(name)
    }

    #[test]
    fn test_transfer() {
        let mut exec = LightSmtExecutor::default();
        let sender = Identity::from(FAUCET_ID);
        let recipient = identity("bob@hydentity");
        let amount = 1000u128;
        let action = SmtTokenAction::Transfer {
            sender: sender.clone(),
            recipient: recipient.clone(),
            amount,
        };
        let res = exec.inner_handle(action).unwrap();
        assert_eq!(res, format!("Transferred {amount} to {recipient}"));
        assert_eq!(exec.balances[&sender].balance, TOTAL_SUPPLY - amount);
        assert_eq!(exec.balances[&recipient].balance, amount);
    }

    #[test]
    fn test_transfer_insufficient_balance() {
        let mut exec = LightSmtExecutor::default();
        let sender = identity("bob@hydentity");
        let recipient = identity("alice@hydentity");
        let amount = 1000u128;
        let action = SmtTokenAction::Transfer {
            sender: sender.clone(),
            recipient: recipient.clone(),
            amount,
        };
        let err = exec.inner_handle(action).unwrap_err();
        assert!(err.to_string().contains("not found"));
    }

    #[test]
    fn test_approve_and_transfer_from() {
        let mut exec = LightSmtExecutor::default();
        let owner = Identity::from(FAUCET_ID);
        let spender = identity("bob@hydentity");
        let recipient = identity("alice@hydentity");
        let amount = 500u128;
        // Approve
        let approve = SmtTokenAction::Approve {
            owner: owner.clone(),
            spender: spender.clone(),
            amount,
        };
        exec.inner_handle(approve).unwrap();
        assert_eq!(exec.balances[&owner].allowances[&spender], amount);
        // TransferFrom
        let transfer_from = SmtTokenAction::TransferFrom {
            owner: owner.clone(),
            spender: spender.clone(),
            recipient: recipient.clone(),
            amount,
        };
        let res = exec.inner_handle(transfer_from).unwrap();
        assert_eq!(res, format!("Transferred {amount} to {recipient}"));
        assert_eq!(exec.balances[&owner].balance, TOTAL_SUPPLY - amount);
        assert_eq!(exec.balances[&owner].allowances[&spender], 0);
        assert_eq!(exec.balances[&recipient].balance, amount);
    }

    #[test]
    fn test_transfer_from_allowance_exceeded() {
        let mut exec = LightSmtExecutor::default();
        let owner = Identity::from(FAUCET_ID);
        let spender = identity("bob@hydentity");
        let recipient = identity("alice@hydentity");
        let amount = 500u128;
        // Approve less than amount
        let approve = SmtTokenAction::Approve {
            owner: owner.clone(),
            spender: spender.clone(),
            amount: 100,
        };
        exec.inner_handle(approve).unwrap();
        let transfer_from = SmtTokenAction::TransferFrom {
            owner: owner.clone(),
            spender: spender.clone(),
            recipient: recipient.clone(),
            amount,
        };
        let err = exec.inner_handle(transfer_from).unwrap_err();
        assert!(err.to_string().contains("Allowance exceeded"));
    }
}

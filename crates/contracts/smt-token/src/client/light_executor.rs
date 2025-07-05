use anyhow::{anyhow, bail, Context, Result};
use borsh::{BorshDeserialize, BorshSerialize};
use sdk::{utils::parse_calldata, Calldata, Identity};
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

pub struct LightExecutorOutput {
    pub success: bool,
    pub program_outputs: Vec<u8>,
}

impl LightSmtExecutor {
    pub fn handle(&mut self, calldata: &Calldata) -> Result<LightExecutorOutput> {
        let (action, _) =
            parse_calldata::<SmtTokenAction>(calldata).map_err(|e| anyhow::anyhow!(e))?;

        self.inner_handle(action)
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
    fn inner_handle(&mut self, action: SmtTokenAction) -> Result<String> {
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
                recipient_account.balance = recipient_account
                    .balance
                    .checked_add(amount)
                    .context("Overflow in recipient balance")?;

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

                    owner_account.update_allowances(
                        spender.clone(),
                        allowance
                            .checked_sub(amount)
                            .context("Allowance underflow")?,
                    );

                    owner_account.balance = owner_account
                        .balance
                        .checked_sub(amount)
                        .context("Insufficient balance")?;
                }
                let recipient_account = self.balances.entry(recipient).or_default();
                recipient_account.balance = recipient_account
                    .balance
                    .checked_add(amount)
                    .context("Overflow in recipient balance")?;

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

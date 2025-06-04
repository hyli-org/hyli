use std::collections::{BTreeMap, HashMap};

use anyhow::{anyhow, bail, Context, Result};
use client_sdk::{
    helpers::risc0::Risc0Prover,
    transaction_builder::{ProvableBlobTx, StateUpdater, TxExecutorBuilder, TxExecutorHandler},
};
use sdk::{
    merkle_utils::BorshableMerkleProof,
    utils::{as_hyle_output, parse_calldata},
    Calldata, ContractName, HyleOutput, Identity, RegisterContractEffect, StateCommitment,
    StructuredBlob,
};

pub mod metadata {
    pub const SMT_TOKEN_ELF: &[u8] = include_bytes!("../../smt-token.img");
    pub const PROGRAM_ID: [u8; 32] = sdk::str_to_u8(include_str!("../../smt-token.txt"));
}
use metadata::*;
use sparse_merkle_tree::{traits::StoreReadOps, SparseMerkleTree};

use crate::{
    account::{Account, AccountSMT},
    SmtTokenAction, SmtTokenContract,
};

pub type SmtTokenProvableState = AccountSMT;

impl SmtTokenProvableState {
    pub fn get_state(&self) -> HashMap<Identity, Account> {
        self.0
            .store()
            .leaves_map()
            .iter()
            .map(|(_, account)| (account.address.clone(), account.clone()))
            .collect()
    }

    pub fn get_account(&self, address: &Identity) -> anyhow::Result<Option<Account>> {
        let key = Account::compute_key(address);
        self.0.store().get_leaf(&key).map_err(anyhow::Error::from)
    }
}

impl Clone for SmtTokenProvableState {
    fn clone(&self) -> Self {
        let store = self.0.store().clone();
        let root = *self.0.root();
        let trie = SparseMerkleTree::new(root, store);
        Self(trie)
    }
}

impl TxExecutorHandler for SmtTokenProvableState {
    /// !!! WARNINGS !!!
    /// This function is only here to keep track of the balances.
    /// No checks are done to verify that this is a legit action.
    fn handle(&mut self, calldata: &Calldata) -> Result<HyleOutput> {
        let root = *self.0.root();
        let initial_state_commitment = StateCommitment(Into::<[u8; 32]>::into(root).to_vec());
        let (action, execution_ctx) =
            parse_calldata::<SmtTokenAction>(calldata).map_err(|e| anyhow::anyhow!(e))?;

        let output = match action {
            SmtTokenAction::Transfer {
                sender,
                recipient,
                amount,
            } => {
                let mut sender_account = self
                    .get_account(&sender)?
                    .ok_or(anyhow!("Sender account {} not found", sender))?;
                if sender == recipient {
                    Ok(format!("Transferred {} to {}", amount, recipient))
                } else {
                    let mut recipient_account = self
                        .get_account(&recipient)?
                        .unwrap_or(Account::new(recipient, 0));

                    let sender_key = sender_account.get_key();
                    let recipient_key = recipient_account.get_key();

                    sender_account.balance -= amount;
                    recipient_account.balance += amount;

                    if let Err(e) = self.0.update(sender_key, sender_account) {
                        bail!("Failed to update sender account: {e}");
                    }
                    if let Err(e) = self.0.update(recipient_key, recipient_account.clone()) {
                        bail!("Failed to update recipient account: {e}");
                    }
                    Ok(format!(
                        "Transferred {} to {}",
                        amount, recipient_account.address
                    ))
                }
            }
            SmtTokenAction::TransferFrom {
                owner,
                spender: _,
                recipient,
                amount,
            } => {
                let mut owner_account = self
                    .get_account(&owner)?
                    .ok_or(anyhow!("Owner account {} not found", owner))?;
                if owner == recipient {
                    Ok(format!("Transferred {} to {}", amount, recipient))
                } else {
                    let mut recipient_account = self
                        .get_account(&recipient)?
                        .unwrap_or(Account::new(recipient, 0));

                    let owner_key = owner_account.get_key();
                    let recipient_key = recipient_account.get_key();

                    owner_account.balance -= amount;
                    recipient_account.balance += amount;
                    if let Err(e) = self.0.update(owner_key, owner_account) {
                        bail!("Failed to update owner account: {e}");
                    }
                    if let Err(e) = self.0.update(recipient_key, recipient_account.clone()) {
                        bail!("Failed to update recipient account: {e}");
                    }
                    Ok(format!(
                        "Transferred {} to {}",
                        amount, recipient_account.address
                    ))
                }
            }
            SmtTokenAction::Approve {
                owner,
                spender,
                amount,
            } => {
                let mut owner_account = self
                    .get_account(&owner)?
                    .ok_or(anyhow!("Owner account {} not found", owner))?;
                let owner_key = owner_account.get_key();
                owner_account.update_allowances(spender.clone(), amount);
                if let Err(e) = self.0.update(owner_key, owner_account) {
                    bail!("Failed to update owner account: {e}");
                }
                Ok(format!("Approved {} to {}", amount, spender))
            }
        };
        let new_rooot = *self.0.root();
        let next_state_commitment = StateCommitment(Into::<[u8; 32]>::into(new_rooot).to_vec());

        let mut res = match output {
            Err(e) => Err(e),
            Ok(output) => Ok((output.into_bytes(), execution_ctx, vec![])),
        };
        Ok(as_hyle_output(
            initial_state_commitment,
            next_state_commitment,
            calldata,
            &mut res,
        ))
    }

    /// This function provides the metadata needed to reconstruct the SMT Token contract's state.
    /// This state is made up of the rootHash of the MerkleTrie, and the merkle proof used to prove the accounts used in the action.
    fn build_commitment_metadata(&self, blob: &sdk::Blob) -> Result<Vec<u8>> {
        let parsed_blob: StructuredBlob<SmtTokenAction> =
            match StructuredBlob::try_from(blob.clone()) {
                Ok(v) => v,
                Err(_) => {
                    bail!("Failed to parse blob: {:?}", blob);
                }
            };

        let action = parsed_blob.data.parameters;

        let root = *self.0.root();
        let (proof, accounts) = match action {
            SmtTokenAction::Transfer {
                sender,
                recipient,
                amount: _,
            } => {
                let sender_account = self
                    .get_account(&sender)?
                    .ok_or(anyhow!("Sender account {} not found", sender))?;
                let recipient_account = self
                    .get_account(&recipient)?
                    .unwrap_or(Account::new(recipient.clone(), 0));

                // Create keys for the accounts
                let key1 = sender_account.get_key();
                let key2 = recipient_account.get_key();

                let keys = if sender == recipient {
                    vec![key1]
                } else {
                    vec![key1, key2]
                };

                (
                    BorshableMerkleProof(
                        self.0.merkle_proof(keys).expect("Failed to generate proof"),
                    ),
                    BTreeMap::from([
                        (sender_account.address.clone(), sender_account.clone()),
                        (recipient_account.address.clone(), recipient_account),
                    ]),
                )
            }
            SmtTokenAction::TransferFrom {
                owner,
                spender: _,
                recipient,
                amount,
            } => {
                let owner_account = self
                    .get_account(&owner)?
                    .ok_or(anyhow!("Owner account {} not found", owner))?;
                let recipient_account = self
                    .get_account(&recipient)?
                    .unwrap_or(Account::new(recipient.clone(), amount));

                // Create keys for the accounts
                let key1 = owner_account.get_key();
                let key2 = recipient_account.get_key();

                let keys = if owner == recipient {
                    vec![key1]
                } else {
                    vec![key1, key2]
                };

                (
                    BorshableMerkleProof(
                        self.0.merkle_proof(keys).expect("Failed to generate proof"),
                    ),
                    BTreeMap::from([
                        (owner_account.address.clone(), owner_account.clone()),
                        (recipient_account.address.clone(), recipient_account),
                    ]),
                )
            }
            SmtTokenAction::Approve {
                owner,
                spender: _,
                amount: _,
            } => {
                let owner_account = self
                    .get_account(&owner)?
                    .ok_or(anyhow!("Owner account {} not found", owner))?;
                let key = owner_account.get_key();
                (
                    BorshableMerkleProof(
                        self.0
                            .merkle_proof(vec![key])
                            .expect("Failed to generate proof"),
                    ),
                    BTreeMap::from([(owner_account.address.clone(), owner_account)]),
                )
            }
        };
        borsh::to_vec(&SmtTokenContract::new(
            StateCommitment(Into::<[u8; 32]>::into(root).to_vec()),
            proof,
            accounts,
        ))
        .context("Failed to serialize SMT Token contract")
    }

    fn construct_state(
        _register_blob: &RegisterContractEffect,
        _metadata: &Option<Vec<u8>>,
    ) -> Result<Self> {
        Ok(Self::default())
    }

    fn merge_commitment_metadata(
        &self,
        initial: Vec<u8>,
        next: Vec<u8>,
    ) -> anyhow::Result<Vec<u8>, String> {
        let mut initial_commitment: SmtTokenContract =
            borsh::from_slice(&initial).map_err(|e| e.to_string())?;
        let next_commitment: SmtTokenContract =
            borsh::from_slice(&next).map_err(|e| e.to_string())?;

        initial_commitment
            .steps
            .insert(0, next_commitment.steps[0].clone());

        borsh::to_vec(&initial_commitment).map_err(|e| e.to_string())
    }
}

impl SmtTokenProvableState {
    pub fn setup_builder<S: StateUpdater>(
        &self,
        contract_name: ContractName,
        builder: &mut TxExecutorBuilder<S>,
    ) {
        builder.init_with(contract_name, Risc0Prover::new(SMT_TOKEN_ELF));
    }

    pub fn transfer(
        &self,
        builder: &mut ProvableBlobTx,
        contract_name: ContractName,
        sender: Identity,
        recipient: Identity,
        amount: u128,
    ) -> anyhow::Result<()> {
        let sender_account = match self.get_account(&sender) {
            Ok(Some(account)) => account,
            Ok(None) => return Err(anyhow::anyhow!("Sender account not found")),
            Err(e) => return Err(e),
        };

        let recipient_account = match self.get_account(&recipient) {
            Ok(Some(account)) => account,
            Ok(None) => Account::new(recipient, 0),
            Err(e) => return Err(e),
        };

        builder.add_action(
            contract_name,
            SmtTokenAction::Transfer {
                sender: sender_account.address.clone(),
                recipient: recipient_account.address.clone(),
                amount,
            },
            None,
            None,
            None,
        )?;
        Ok(())
    }

    pub fn transfer_from(
        &self,
        builder: &mut ProvableBlobTx,
        contract_name: ContractName,
        owner: Identity,
        spender: Identity,
        recipient: Identity,
        amount: u128,
    ) -> anyhow::Result<()> {
        let owner_account = match self.get_account(&owner) {
            Ok(Some(account)) => account,
            Ok(None) => return Err(anyhow::anyhow!("Sender account not found")),
            Err(e) => return Err(e),
        };

        let recipient_account = match self.get_account(&recipient) {
            Ok(Some(account)) => account,
            Ok(None) => Account::new(recipient, 0),
            Err(e) => return Err(e),
        };

        builder.add_action(
            contract_name,
            SmtTokenAction::TransferFrom {
                owner: owner_account.address.clone(),
                spender,
                recipient: recipient_account.address.clone(),
                amount,
            },
            None,
            None,
            None,
        )?;
        Ok(())
    }

    pub fn approve(
        &self,
        builder: &mut ProvableBlobTx,
        contract_name: ContractName,
        owner: Identity,
        spender: Identity,
        amount: u128,
    ) -> anyhow::Result<()> {
        let owner_account = match self.get_account(&owner) {
            Ok(Some(account)) => account,
            Ok(None) => return Err(anyhow::anyhow!("Sender account not found")),
            Err(e) => return Err(e),
        };

        builder.add_action(
            contract_name,
            SmtTokenAction::Approve {
                owner: owner_account.address.clone(),
                spender,
                amount,
            },
            None,
            None,
            None,
        )?;
        Ok(())
    }
}

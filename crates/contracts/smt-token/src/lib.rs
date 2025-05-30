use std::collections::BTreeMap;

use account::Account;
use borsh::{BorshDeserialize, BorshSerialize};
use sdk::merkle_utils::{BorshableMerkleProof, SHA256Hasher};
use sdk::utils::parse_calldata;
use sdk::{
    Blob, BlobData, BlobIndex, Calldata, ContractAction, ContractName, Identity, StateCommitment,
    StructuredBlobData,
};
use sdk::{RunResult, ZkContract};
use sparse_merkle_tree::traits::Value;

extern crate alloc;

pub mod account;
#[cfg(feature = "client")]
pub mod client;
#[cfg(feature = "client")]
pub mod indexer;

pub const TOTAL_SUPPLY: u128 = 100_000_000_000;
pub const FAUCET_ID: &str = "faucet@secp256k1";

/// Enum representing possible calls to Token contract functions.
#[derive(Debug, Clone, PartialEq, BorshDeserialize, BorshSerialize)]
pub enum SmtTokenAction {
    Transfer {
        sender: Identity,
        recipient: Identity,
        amount: u128,
    },
    TransferFrom {
        owner: Identity,
        spender: Identity,
        recipient: Identity,
        amount: u128,
    },
    Approve {
        owner: Identity,
        spender: Identity,
        amount: u128,
    },
}

/// Struct representing the SMT token.
/// Each attributes of this struct is what is needed in order to verify the state of the contract, and update it.
#[derive(BorshSerialize, BorshDeserialize, Debug, Clone)]
pub struct SmtTokenContract {
    pub commitment: sdk::StateCommitment,
    /// 1 step per calldata, in reverse order (last step is 1st calldata)
    pub steps: Vec<SmtTokenStep>,
}

#[derive(BorshSerialize, BorshDeserialize, Debug, Clone)]
pub struct SmtTokenStep {
    pub proof: BorshableMerkleProof,
    pub accounts: BTreeMap<Identity, Account>,
}

impl SmtTokenContract {
    pub fn new(
        commitment: sdk::StateCommitment,
        proof: BorshableMerkleProof,
        accounts: BTreeMap<Identity, Account>,
    ) -> Self {
        SmtTokenContract {
            commitment,
            steps: vec![SmtTokenStep { proof, accounts }],
        }
    }
}

impl ZkContract for SmtTokenContract {
    fn execute(&mut self, calldata: &Calldata) -> RunResult {
        let (action, execution_ctx) = parse_calldata::<SmtTokenAction>(calldata)?;

        let output = match action {
            SmtTokenAction::Transfer {
                sender,
                recipient,
                amount,
            } => self.transfer(sender, recipient, amount),
            SmtTokenAction::TransferFrom {
                owner,
                spender,
                recipient,
                amount,
            } => self.transfer_from(owner, spender, recipient, amount),
            SmtTokenAction::Approve {
                owner,
                spender,
                amount,
            } => self.approve(owner, spender, amount),
        };

        match output {
            Err(e) => Err(e),
            Ok(output) => Ok((output.into_bytes(), execution_ctx, vec![])),
        }
    }

    fn commit(&self) -> sdk::StateCommitment {
        self.commitment.clone()
    }
}

impl SmtTokenContract {
    pub fn to_bytes(&self) -> Vec<u8> {
        borsh::to_vec(self).expect("Failed to encode Balances")
    }
}

impl SmtTokenContract {
    pub fn transfer(
        &mut self,
        sender: Identity,
        recipient: Identity,
        amount: u128,
    ) -> Result<String, String> {
        let SmtTokenStep {
            mut accounts,
            proof,
        } = self.steps.pop().unwrap();
        {
            let sender_account = accounts
                .get(&sender)
                .ok_or_else(|| "Sender account not found".to_string())?;
            let recipient_account = accounts
                .get(&recipient)
                .ok_or_else(|| "Recipient account not found".to_string())?;

            let sender_key = sender_account.get_key();
            let recipient_key = recipient_account.get_key();

            let leaves = if sender == recipient {
                vec![(sender_key, sender_account.to_h256())]
            } else {
                vec![
                    (sender_key, sender_account.to_h256()),
                    (recipient_key, recipient_account.to_h256()),
                ]
            };

            let verified = proof
                .0
                .clone()
                .verify::<SHA256Hasher>(
                    &TryInto::<[u8; 32]>::try_into(self.commitment.0.clone())
                        .unwrap()
                        .into(),
                    leaves,
                )
                .expect("Failed to verify proof");

            if !verified {
                return Err("Merkle proof invalid".to_string());
            }
        }

        // update sender and recipient balances
        accounts.get_mut(&sender).unwrap().balance -= amount;
        accounts.get_mut(&recipient).unwrap().balance += amount;

        let sender_account = accounts.get(&sender).unwrap();
        let recipient_account = accounts.get(&recipient).unwrap();
        let leaves = if sender == recipient {
            vec![(sender_account.get_key(), sender_account.to_h256())]
        } else {
            vec![
                (sender_account.get_key(), sender_account.to_h256()),
                (recipient_account.get_key(), recipient_account.to_h256()),
            ]
        };
        let new_root = proof
            .0
            .clone()
            .compute_root::<SHA256Hasher>(leaves)
            .expect("Failed to compute new root");

        self.commitment = StateCommitment(Into::<[u8; 32]>::into(new_root).to_vec());

        Ok(format!(
            "Transferred {} to {}",
            amount, recipient_account.address
        ))
    }

    pub fn transfer_from(
        &mut self,
        owner: Identity,
        spender: Identity,
        recipient: Identity,
        amount: u128,
    ) -> Result<String, String> {
        let SmtTokenStep { accounts, proof } = self.steps.pop().unwrap();
        let owner_account = accounts
            .get(&owner)
            .ok_or_else(|| "Owner account not found".to_string())?;
        let recipient_account = accounts
            .get(&recipient)
            .ok_or_else(|| "Recipient account not found".to_string())?;

        if owner_account.address != owner {
            return Err("Owner address mismatch".to_string());
        }
        if recipient_account.address != recipient {
            return Err("Recipient address mismatch".to_string());
        }
        if owner_account.allowances.get(&spender).unwrap_or(&0) < &amount {
            return Err(format!(
                "Allowance exceeded for spender={} owner={} allowance={}",
                spender,
                owner_account.address,
                owner_account.allowances.get(&spender).unwrap_or(&0)
            ));
        }

        // re-add it to be pop-ed by transfer()
        // note: we pop it at the beginning of transfer_from to remove it
        // even in case of early return and still be able to verify next calldata
        self.steps.push(SmtTokenStep { proof, accounts });

        self.transfer(owner, recipient, amount)
        // TODO: update allowance
    }

    pub fn approve(
        &mut self,
        owner: Identity,
        spender: Identity,
        amount: u128,
    ) -> Result<String, String> {
        let SmtTokenStep {
            mut accounts,
            proof,
        } = self.steps.pop().unwrap();
        {
            let owner_account = accounts
                .get(&owner)
                .ok_or_else(|| "Owner account not found".to_string())?;

            let owner_key = owner_account.get_key();

            let verified = proof
                .0
                .clone()
                .verify::<SHA256Hasher>(
                    &TryInto::<[u8; 32]>::try_into(self.commitment.0.clone())
                        .unwrap()
                        .into(),
                    vec![(owner_key, owner_account.to_h256())],
                )
                .expect("Failed to verify proof");

            if !verified {
                return Err("Merkle proof invalid".to_string());
            }
        }

        accounts
            .get_mut(&owner)
            .unwrap()
            .update_allowances(spender.clone(), amount);

        let owner_account = accounts.get(&owner).unwrap();
        let owner_key = owner_account.get_key();

        let new_root = proof
            .0
            .clone()
            .compute_root::<SHA256Hasher>(vec![(owner_key, owner_account.to_h256())])
            .expect("Failed to compute new root");

        self.commitment = StateCommitment(Into::<[u8; 32]>::into(new_root).to_vec());
        Ok(format!("Approved {} to {}", amount, spender))
    }
}

impl ContractAction for SmtTokenAction {
    fn as_blob(
        &self,
        contract_name: ContractName,
        caller: Option<BlobIndex>,
        callees: Option<Vec<BlobIndex>>,
    ) -> Blob {
        Blob {
            contract_name,
            data: BlobData::from(StructuredBlobData {
                caller,
                callees,
                parameters: self.clone(),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::account::AccountSMT;

    use super::*;

    #[test_log::test]
    fn test_smt_token_transfer() {
        // Create a new empty SMT
        let mut smt = AccountSMT::default();

        // Create some test accounts
        let mut account1 = Account::new(FAUCET_ID.into(), 10000);
        let mut account2 = Account::new(Identity::from("alice"), 100);

        // Create keys for the accounts
        let key1 = account1.get_key();
        let key2 = account2.get_key();

        // Insert accounts into SMT
        smt.0
            .update(key1, account1.clone())
            .expect("Failed to update SMT");
        smt.0
            .update(key2, account2.clone())
            .expect("Failed to update SMT");

        // Generate merkle proof for account1
        let proof = smt
            .0
            .merkle_proof(vec![key1, key2])
            .expect("Failed to generate proof");

        // Compute initial root
        let root = *smt.0.root();
        let mut smt_token = SmtTokenContract::new(
            StateCommitment(Into::<[u8; 32]>::into(root).to_vec()),
            BorshableMerkleProof(proof.clone()),
            BTreeMap::from([
                (account1.address.clone(), account1.clone()),
                (account2.address.clone(), account2.clone()),
            ]),
        );

        // Verify the existence proof
        let verified = proof
            .clone()
            .verify::<SHA256Hasher>(
                &root,
                vec![(key1, account1.to_h256()), (key2, account2.to_h256())],
            )
            .expect("Failed to verify proof");

        assert!(verified);

        // Transfer 100 tokens from account1 to account2 in the contract
        smt_token
            .transfer(account1.address.clone(), account2.address.clone(), 100)
            .unwrap();

        // Transfer 100 tokens from account1 to account2
        account1.balance -= 100;
        account2.balance += 100;
        let expected_root = smt
            .0
            .update_all(vec![
                (account1.get_key(), account1),
                (account2.get_key(), account2),
            ])
            .unwrap();

        assert_eq!(
            StateCommitment(Into::<[u8; 32]>::into(*expected_root).to_vec()),
            smt_token.commit()
        );
    }

    #[test_log::test]
    fn test_smt_token_self_transfer() {
        // Create a new empty SMT
        let mut smt = AccountSMT::default();

        // Create some test accounts
        let account1 = Account::new(Identity::from("alice"), 100);
        let account2 = Account::new(Identity::from("alice"), 100);

        // Create keys for the accounts
        let key1 = account1.get_key();
        let key2 = account2.get_key();

        // Insert accounts into SMT
        smt.0
            .update(key1, account1.clone())
            .expect("Failed to update SMT");
        smt.0
            .update(key2, account2.clone())
            .expect("Failed to update SMT");

        // Generate merkle proof for account1
        let proof = smt
            .0
            .merkle_proof(vec![key1])
            .expect("Failed to generate proof");

        // Compute initial root
        let root = *smt.0.root();
        let mut smt_token = SmtTokenContract::new(
            StateCommitment(Into::<[u8; 32]>::into(root).to_vec()),
            BorshableMerkleProof(proof.clone()),
            BTreeMap::from([
                (account1.address.clone(), account1.clone()),
                (account2.address.clone(), account2.clone()),
            ]),
        );

        // Verify the existence proof
        let verified = proof
            .clone()
            .verify::<SHA256Hasher>(&root, vec![(key1, account1.to_h256())])
            .expect("Failed to verify proof");

        assert!(verified, "Merkle proof verification failed");

        // Transfer 100 tokens from account1 to account2 in the contract
        smt_token
            .transfer(account1.address.clone(), account2.address.clone(), 100)
            .unwrap();

        let expected_root = smt
            .0
            .update_all(vec![
                (account1.get_key(), account1),
                (account2.get_key(), account2),
            ])
            .unwrap();

        assert_eq!(
            StateCommitment(Into::<[u8; 32]>::into(*expected_root).to_vec()),
            smt_token.commit()
        );
    }

    #[test_log::test]
    fn test_smt_token_new_account_tranfer() {
        // Create a new empty SMT
        let mut smt = AccountSMT::default();

        // Create some test accounts
        let mut account1 = Account::new(FAUCET_ID.into(), 10000);
        let mut account2 = Account::new(Identity::from("alice"), 0);

        // Create keys for the accounts
        let key1 = account1.get_key();
        let key2 = account2.get_key();

        // Insert accounts into SMT
        smt.0
            .update(key1, account1.clone())
            .expect("Failed to update SMT");

        // Generate merkle proof for account1
        let proof = smt
            .0
            .merkle_proof(vec![key1, key2])
            .expect("Failed to generate proof");

        // Compute initial root
        let root = *smt.0.root();
        let mut smt_token = SmtTokenContract::new(
            StateCommitment(Into::<[u8; 32]>::into(root).to_vec()),
            BorshableMerkleProof(proof.clone()),
            BTreeMap::from([
                (account1.address.clone(), account1.clone()),
                (account2.address.clone(), account2.clone()),
            ]),
        );

        // Verify the existence proof
        let verified = proof
            .clone()
            .verify::<SHA256Hasher>(
                smt.0.root(),
                vec![(key1, account1.to_h256()), (key2, account2.to_h256())],
            )
            .expect("Failed to verify proof");

        assert!(verified);

        // Double-check that the account really doesn't exist
        let value = smt.0.get(&key2).expect("Failed to get value");
        assert_eq!(value, Account::default());

        // Transfer 100 tokens from account1 to account2 in the contract
        smt_token
            .transfer(account1.address.clone(), account2.address.clone(), 100)
            .unwrap();

        // Transfer 100 tokens from account1 to account2
        account1.balance -= 100;
        account2.balance += 100;
        let expected_root = smt
            .0
            .update_all(vec![
                (account1.get_key(), account1),
                (account2.get_key(), account2),
            ])
            .unwrap();

        assert_eq!(
            StateCommitment(Into::<[u8; 32]>::into(*expected_root).to_vec()),
            smt_token.commit()
        );
    }

    #[test_log::test]
    fn test_smt_token_transfer_from() {
        // Create a new empty SMT
        let mut smt = AccountSMT::default();

        // Create some test accounts
        let mut owner_account = Account::new(Identity::from("owner"), 10000);
        let mut recipient_account = Account::new(Identity::from("recipient"), 0);
        let spender = Identity::from("spender");

        // Set allowance for spender
        owner_account.update_allowances(spender.clone(), 500);

        // Create keys for the accounts
        let owner_key = owner_account.get_key();
        let recipient_key = recipient_account.get_key();

        // Insert account into SMT
        smt.0
            .update(owner_key, owner_account.clone())
            .expect("Failed to update SMT");

        // Generate merkle proof for the accounts
        let proof = smt
            .0
            .merkle_proof(vec![owner_key, recipient_key])
            .expect("Failed to generate proof");

        // Compute initial root
        let root = *smt.0.root();
        let mut smt_token = SmtTokenContract::new(
            StateCommitment(Into::<[u8; 32]>::into(root).to_vec()),
            BorshableMerkleProof(proof.clone()),
            BTreeMap::from([
                (owner_account.address.clone(), owner_account.clone()),
                (recipient_account.address.clone(), recipient_account.clone()),
            ]),
        );

        // Verify the existence proof
        let verified = proof
            .clone()
            .verify::<SHA256Hasher>(
                &root,
                vec![
                    (owner_key, owner_account.to_h256()),
                    (recipient_key, recipient_account.to_h256()),
                ],
            )
            .expect("Failed to verify proof");

        assert!(verified);

        // Transfer 200 tokens from owner to recipient via spender in the contract
        smt_token
            .transfer_from(
                owner_account.address.clone(),
                spender.clone(),
                recipient_account.address.clone(),
                200,
            )
            .unwrap();

        // Update balances and allowance
        owner_account.balance -= 200;
        recipient_account.balance += 200;

        let expected_root = smt
            .0
            .update_all(vec![
                (owner_account.get_key(), owner_account),
                (recipient_account.get_key(), recipient_account),
            ])
            .unwrap();

        assert_eq!(
            StateCommitment(Into::<[u8; 32]>::into(*expected_root).to_vec()),
            smt_token.commit()
        );
    }

    #[test_log::test]
    fn test_smt_token_approve() {
        // Create a new empty SMT
        let mut smt = AccountSMT::default();

        // Create a test account
        let mut owner_account = Account::new(Identity::from("owner"), 10000);
        let spender = Identity::from("spender");

        // Create key for the account
        let owner_key = owner_account.get_key();

        // Insert account into SMT
        smt.0
            .update(owner_key, owner_account.clone())
            .expect("Failed to update SMT");

        // Generate merkle proof for the account
        let proof = smt
            .0
            .merkle_proof(vec![owner_key])
            .expect("Failed to generate proof");

        // Compute initial root
        let root = *smt.0.root();
        let mut smt_token = SmtTokenContract::new(
            StateCommitment(Into::<[u8; 32]>::into(root).to_vec()),
            BorshableMerkleProof(proof.clone()),
            BTreeMap::from([(owner_account.address.clone(), owner_account.clone())]),
        );

        // Verify the existence proof
        let verified = proof
            .clone()
            .verify::<SHA256Hasher>(&root, vec![(owner_key, owner_account.to_h256())])
            .expect("Failed to verify proof");

        assert!(verified);

        // Approve 500 tokens for spender in the contract
        smt_token
            .approve(owner_account.address.clone(), spender.clone(), 500)
            .unwrap();

        // Update allowance
        owner_account.update_allowances(spender, 500);

        let expected_root = smt
            .0
            .update_all(vec![(owner_account.get_key(), owner_account)])
            .unwrap();

        assert_eq!(
            StateCommitment(Into::<[u8; 32]>::into(*expected_root).to_vec()),
            smt_token.commit()
        );
    }
}

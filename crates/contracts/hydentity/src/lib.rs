use anyhow::Context;
use borsh::{BorshDeserialize, BorshSerialize};
use identity_provider::IdentityVerification;
use sdk::{utils::parse_raw_calldata, Blob, Calldata, ContractAction, ContractName};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use sdk::{RunResult, ZkContract};
use sha2::{Digest, Sha256};

#[cfg(feature = "client")]
pub mod client;
#[cfg(feature = "client")]
pub mod indexer;

pub mod identity_provider;

impl ZkContract for Hydentity {
    fn execute(&mut self, calldata: &Calldata) -> RunResult {
        let (action, exec_ctx) = parse_raw_calldata(calldata)?;
        let private_input =
            std::str::from_utf8(&calldata.private_input).map_err(|_| "Invalid UTF-8 sequence")?;
        let output = self.execute_identity_action(action, private_input);

        match output {
            Err(e) => Err(e),
            Ok(output) => Ok((output.into_bytes(), exec_ctx, vec![])),
        }
    }

    fn commit(&self) -> sdk::StateCommitment {
        let mut hasher = Sha256::new();
        for (account, info) in &self.identities {
            hasher.update(account.as_bytes());
            hasher.update(info.hash.as_bytes());
            hasher.update(info.nonce.to_be_bytes());
        }
        sdk::StateCommitment(hasher.finalize().to_vec())
    }
}

#[derive(BorshSerialize, BorshDeserialize, Serialize, Deserialize, Debug, Clone, Default)]
pub struct Hydentity {
    identities: BTreeMap<String, AccountInfo>,
}

/// Enum representing the actions that can be performed by the IdentityVerification contract.
#[derive(Serialize, Deserialize, BorshSerialize, BorshDeserialize, Debug, Clone)]
pub enum HydentityAction {
    RegisterIdentity { account: String },
    VerifyIdentity { account: String, nonce: u32 },
    GetIdentityInfo { account: String },
}

#[derive(BorshSerialize, BorshDeserialize, Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct AccountInfo {
    pub hash: String,
    pub nonce: u32,
}

impl Hydentity {
    pub fn to_bytes(&self) -> Vec<u8> {
        borsh::to_vec(self).expect("Failed to encode Balances")
    }

    pub fn get_nonce(&self, username: &str) -> Result<u32, &'static str> {
        let info = self.get_identity_info(username)?;
        let state: AccountInfo =
            serde_json::from_str(&info).map_err(|_| "Failed to parse account info")?;
        Ok(state.nonce)
    }

    pub fn build_id(account: &str, private_input: &str) -> String {
        let id = format!("{account}:{private_input}");

        let mut hasher = Sha256::new();
        hasher.update(id.as_bytes());
        let hash_bytes = hasher.finalize();
        let hash = hex::encode(hash_bytes);

        format!("{account}:{hash}")
    }

    pub fn parse_id(id: &str) -> anyhow::Result<(String, String)> {
        let mut split = id.split(':');
        let name = split
            .next()
            .context("No account name found in registration")?
            .to_string();
        let hash = split
            .next()
            .context("No hash found in registration")?
            .to_string();
        Ok((name, hash))
    }

    pub fn as_bytes(&self) -> anyhow::Result<Vec<u8>> {
        borsh::to_vec(self).map_err(|_| anyhow::anyhow!("Failed to serialize"))
    }
}

impl IdentityVerification for Hydentity {
    fn register_identity(
        &mut self,
        registration: &str,
        private_input: &str,
    ) -> Result<(), &'static str> {
        let mut split = registration.split(':');
        let name = split
            .next()
            .ok_or("No account name found in registration")?;
        let hash = split.next().ok_or("No hash found in registratio")?;

        let id = format!("{name}:{private_input}");
        let mut hasher = Sha256::new();
        hasher.update(id.as_bytes());
        let hash_bytes = hasher.finalize();
        let account_info = AccountInfo {
            hash: hex::encode(hash_bytes),
            nonce: 0,
        };
        if !hash.eq(&account_info.hash) {
            return Err("Invalid hash or password");
        }
        if self
            .identities
            .insert(name.to_string(), account_info)
            .is_some()
        {
            return Err("Identity already exists");
        }
        Ok(())
    }

    fn verify_identity(
        &mut self,
        account: &str,
        nonce: u32,
        private_input: &str,
    ) -> Result<bool, &'static str> {
        match self.identities.get_mut(account) {
            Some(stored_info) => {
                if nonce != stored_info.nonce {
                    return Err("Invalid nonce");
                }
                let id = format!("{account}:{private_input}");
                let mut hasher = Sha256::new();
                hasher.update(id.as_bytes());
                let hashed = hex::encode(hasher.finalize());
                if *stored_info.hash != hashed {
                    return Ok(false);
                }
                stored_info.nonce += 1;
                Ok(true)
            }
            None => Err("Identity not found"),
        }
    }

    fn get_identity_info(&self, account: &str) -> Result<String, &'static str> {
        match self.identities.get(account) {
            Some(info) => Ok(serde_json::to_string(&info).map_err(|_| "Failed to serialize")?),
            None => Err("Identity not found"),
        }
    }
}

impl HydentityAction {
    pub fn as_blob(&self, contract_name: ContractName) -> Blob {
        <Self as ContractAction>::as_blob(self, contract_name, None, None)
    }
}
impl ContractAction for HydentityAction {
    fn as_blob(
        &self,
        contract_name: ContractName,
        _caller: Option<sdk::BlobIndex>,
        _callees: Option<Vec<sdk::BlobIndex>>,
    ) -> Blob {
        Blob {
            contract_name,
            data: sdk::BlobData(borsh::to_vec(self).expect("failed to encode program inputs")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha256};

    #[test]
    fn test_register_identity() {
        let mut hydentity = Hydentity::default();
        let account = "test_account";
        let private_input = "test_input";
        let id = &Hydentity::build_id("test_account", private_input);

        assert!(hydentity.register_identity(id, private_input).is_ok());

        let id = format!("{account}:{private_input}");
        let mut hasher = Sha256::new();
        hasher.update(id.as_bytes());
        let hash_bytes = hasher.finalize();
        let expected_hash = hex::encode(hash_bytes);

        let registered = hydentity.identities.get(account).unwrap();
        assert_eq!(registered.hash, expected_hash);
        assert_eq!(registered.nonce, 0);
    }

    #[test]
    fn test_register_identity_that_already_exists() {
        let mut hydentity = Hydentity::default();
        let private_input = "test_input";
        let id = &Hydentity::build_id("test_account", private_input);

        assert!(hydentity.register_identity(id, private_input).is_ok());
        assert!(hydentity.register_identity(id, private_input).is_err());
    }

    #[test]
    fn test_verify_identity() {
        let mut hydentity = Hydentity::default();
        let account = "test_account";
        let private_input = "test_input";
        let id = &Hydentity::build_id("test_account", private_input);

        hydentity.register_identity(id, private_input).unwrap();

        assert!(hydentity
            .verify_identity(account, 1, private_input)
            .is_err());
        assert!(hydentity
            .verify_identity(account, 0, private_input)
            .unwrap());
        assert!(hydentity
            .verify_identity(account, 0, private_input)
            .is_err()); // Same is wrong as nonce increased
        assert!(hydentity
            .verify_identity(account, 1, private_input)
            .unwrap());
        assert!(hydentity
            .verify_identity("nonexistent_account", 0, private_input)
            .is_err());
    }

    #[test]
    fn test_get_identity_info() {
        let mut hydentity = Hydentity::default();
        let account = "test_account";
        let private_input = "test_input";

        let id = &Hydentity::build_id("test_account", private_input);
        hydentity.register_identity(id, private_input).unwrap();

        let id = format!("{account}:{private_input}");
        let mut hasher = Sha256::new();
        hasher.update(id.as_bytes());
        let hash_bytes = hasher.finalize();
        let expected_hash = hex::encode(hash_bytes);
        let expected_info = serde_json::to_string(&AccountInfo {
            hash: expected_hash.clone(),
            nonce: 0,
        });

        assert_eq!(
            hydentity.get_identity_info(account).unwrap(),
            expected_info.unwrap()
        );
        assert!(hydentity.get_identity_info("nonexistent_account").is_err());
    }
}

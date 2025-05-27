use std::{collections::BTreeMap, hash::Hasher};

use borsh::{BorshDeserialize, BorshSerialize};
use rand::Rng;
use rand_seeder::SipHasher;
use sdk::{
    info,
    utils::{parse_calldata, parse_raw_calldata},
    Blob, BlobData, BlobIndex, Calldata, ContractAction, ContractName, OnchainEffect,
    RegisterContractAction, RunResult, ZkContract,
};
use uuid::Uuid;

#[cfg(feature = "client")]
pub mod client;

#[derive(Clone, BorshSerialize, BorshDeserialize)]
pub enum UuidTldAction {
    Claim,
}

impl UuidTldAction {
    pub fn as_blob(&self, contract_name: ContractName) -> Blob {
        <Self as ContractAction>::as_blob(self, contract_name, None, None)
    }
}

impl ContractAction for UuidTldAction {
    fn as_blob(
        &self,
        contract_name: ContractName,
        _caller: Option<BlobIndex>,
        _callees: Option<Vec<BlobIndex>>,
    ) -> Blob {
        #[allow(clippy::expect_used)]
        Blob {
            contract_name,
            data: BlobData(borsh::to_vec(self).expect("failed to encode BlstSignatureBlob")),
        }
    }
}

impl ZkContract for UuidTld {
    fn execute(&mut self, calldata: &Calldata) -> RunResult {
        // Not an identity provider
        if calldata.identity.0.ends_with(&format!(
            "@{}",
            calldata.blobs.get(&calldata.index).unwrap().contract_name.0
        )) {
            return Err("Invalid identity".to_string());
        }

        if let Ok((action, exec_ctx)) = parse_raw_calldata::<UuidTldAction>(calldata) {
            match action {
                UuidTldAction::Claim => {
                    let id = self.claim_contract(calldata)?;
                    let uuid = Uuid::from_u128(id);
                    return Ok((format!("claimed {}", uuid).into_bytes(), exec_ctx, vec![]));
                }
            }
        }

        if let Ok((action, exec_ctx)) = parse_calldata::<RegisterContractAction>(calldata) {
            // Extract UUID from contract name
            let parts: Vec<&str> = action.contract_name.0.split('.').collect();
            let uuid = match Uuid::parse_str(parts[0]) {
                Ok(uuid) => uuid,
                Err(_) => return Err("Invalid UUID in contract name".to_string()),
            };

            self.register_contract(uuid.as_u128(), calldata.identity.0.clone())?;

            return Ok((
                format!("registered {} ({})", uuid, uuid.as_u128()).into_bytes(),
                exec_ctx,
                vec![OnchainEffect::RegisterContract(action.into())],
            ));
        }

        Err("Unknown action".to_string())
    }

    fn commit(&self) -> sdk::StateCommitment {
        sdk::StateCommitment(self.serialize().unwrap())
    }
}

#[derive(Default, Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct UuidTld {
    // Maps UUID to the identity that claimed it
    registered_contracts: BTreeMap<u128, String>,
}

impl UuidTld {
    fn serialize(&self) -> Result<Vec<u8>, String> {
        borsh::to_vec(self).map_err(|e| e.to_string())
    }

    pub fn deserialize(data: &[u8]) -> Result<Self, String> {
        borsh::from_slice(data).map_err(|e| e.to_string())
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        self.serialize().expect("Failed to encode UuidTldState")
    }

    pub fn claim_contract(&mut self, calldata: &Calldata) -> Result<u128, String> {
        let Some(ref tx_ctx) = calldata.tx_ctx else {
            return Err("Missing tx context".to_string());
        };

        // Create UUID
        let mut hasher = SipHasher::new();
        hasher.write(&self.commit().0);
        hasher.write(calldata.tx_hash.0.as_bytes());
        hasher.write(tx_ctx.block_hash.0.as_bytes());
        hasher.write_u128(tx_ctx.timestamp.0);
        let mut hasher_rng = hasher.into_rng();
        let id = uuid::Builder::from_random_bytes(hasher_rng.random())
            .into_uuid()
            .as_u128();

        // _really_ shouldn't happen but let's handle it regardless.
        if self.registered_contracts.contains_key(&id) {
            return Err("Contract already registered".to_string());
        }

        self.registered_contracts
            .insert(id, calldata.identity.0.clone());
        let uuid = Uuid::from_u128(id);
        info!("Claimed new contract with UUID {} ({})", uuid, id);
        Ok(id)
    }

    pub fn register_contract(&mut self, uuid: u128, identity: String) -> Result<(), String> {
        match self.registered_contracts.get(&uuid) {
            Some(claimer) if claimer == &identity => {
                info!(
                    "Registering contract with UUID {} ({})",
                    Uuid::from_u128(uuid),
                    uuid
                );
                Ok(())
            }
            Some(_) => Err("UUID claimed by different identity".to_string()),
            None => Err("UUID not claimed".to_string()),
        }
    }
}

#[cfg(test)]
mod test {
    use crate::*;
    use hyle_model_utils::TimestampMs;
    use sdk::*;

    fn make_calldata<T: ContractAction>(action: T, identity: &str) -> Calldata {
        Calldata {
            identity: identity.into(),
            tx_hash: TxHash::default(),
            tx_ctx: Some(TxContext {
                block_hash: ConsensusProposalHash("0xcafefade".to_owned()),
                timestamp: TimestampMs(3745916),
                ..TxContext::default()
            }),
            private_input: vec![],
            blobs: vec![
                Blob {
                    contract_name: "test".into(),
                    data: BlobData(vec![]),
                },
                action.as_blob("uuid".into(), None, None),
            ]
            .into(),
            tx_blob_count: 1,
            index: BlobIndex(1),
        }
    }

    #[test]
    fn test_execute() {
        let mut state = UuidTld::default();

        // First claim a UUID
        let claim_calldata = make_calldata(UuidTldAction::Claim, "toto@test");
        let (msg_bytes, _, _) = state.execute(&claim_calldata).unwrap();
        let msg = String::from_utf8(msg_bytes).unwrap();
        let uuid = msg.replace("claimed ", "");

        // Then register it with the same identity
        let register_action = RegisterContractAction {
            contract_name: format!("{}.test", uuid).into(),
            verifier: "test".into(),
            program_id: ProgramId(vec![1, 2, 3]),
            state_commitment: StateCommitment(vec![0, 1, 2, 3]),
            ..Default::default()
        };

        let contract_input = make_calldata(register_action.clone(), "toto@test");
        let (_, _, onchain_effects) = state.execute(&contract_input).unwrap();

        let OnchainEffect::RegisterContract(effect) = onchain_effects.first().unwrap() else {
            panic!("Expected RegisterContract effect");
        };
        assert_eq!(effect.contract_name.0, format!("{}.test", uuid));
        assert_eq!(effect.verifier, Verifier("test".into()));
        assert_eq!(effect.program_id, ProgramId(vec![1, 2, 3]));
        assert_eq!(effect.state_commitment, StateCommitment(vec![0, 1, 2, 3]));

        // Try to register with a different identity
        let contract_input = make_calldata(register_action.clone(), "other@test");
        assert!(state.execute(&contract_input).is_err());

        // Try to register with unclaimed UUID
        let unclaimed_uuid = Uuid::from_u128(12345);
        let register_action = RegisterContractAction {
            contract_name: format!("{}.test", unclaimed_uuid).into(),
            verifier: "test".into(),
            program_id: ProgramId(vec![1, 2, 3]),
            state_commitment: StateCommitment(vec![0, 1, 2, 3]),
            ..Default::default()
        };

        let contract_input = make_calldata(register_action, "toto@test");
        assert!(state.execute(&contract_input).is_err());
    }
}

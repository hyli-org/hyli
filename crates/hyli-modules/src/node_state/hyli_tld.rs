use crate::node_state::{
    contract_registration::validate_contract_registration_metadata, ContractStatus,
    ModifiedContractData, ModifiedContractFields, NodeStateProcessing,
};
use anyhow::{bail, Result};
use borsh::BorshDeserialize;
use sdk::*;
use std::collections::{BTreeMap, HashMap};

use super::SideEffect;

pub const HYLI_TLD_ID: &str = "hyli@wallet";

pub fn handle_blob_for_hyli_tld(
    contracts: &HashMap<ContractName, Contract>,
    contract_changes: &mut BTreeMap<ContractName, ModifiedContractData>,
    current_blob: &Blob,
) -> Result<()> {
    if let Ok(reg) = RegisterContractAction::try_from_slice(&current_blob.data.0) {
        handle_register_blob(contracts, contract_changes, &reg)?;
    } else if let Ok(del) = DeleteContractAction::try_from_slice(&current_blob.data.0) {
        handle_delete_blob(contracts, contract_changes, &del)?;
    } else if let Ok(updt) = UpdateContractProgramIdAction::try_from_slice(&current_blob.data.0) {
        handle_update_program_id_blob(contracts, contract_changes, &updt)?;
    } else if let Ok(updt) = UpdateContractTimeoutWindowAction::try_from_slice(&current_blob.data.0)
    {
        handle_update_timeout_window_blob(contracts, contract_changes, &updt)?;
    } else {
        bail!("Invalid blob data for TLD");
    }
    Ok(())
}

fn handle_register_blob(
    contracts: &HashMap<ContractName, Contract>,
    contract_changes: &mut BTreeMap<ContractName, ModifiedContractData>,
    reg: &RegisterContractAction,
) -> Result<()> {
    // Check name, it's either a direct subdomain or a TLD
    validate_contract_registration_metadata(
        &"hyli".into(),
        &reg.contract_name,
        &reg.verifier,
        &reg.program_id,
        &reg.state_commitment,
    )?;

    // Check it's not already registered
    if reg.contract_name.0 != "hyli" && contracts.contains_key(&reg.contract_name)
        || contract_changes.contains_key(&reg.contract_name)
    {
        bail!("Contract {} is already registered", reg.contract_name.0);
    }

    let (contract_status, side_effects) = if reg.constructor_metadata.is_some() {
        (
            ContractStatus::RegisterWithConstructor(Contract {
                name: reg.contract_name.clone(),
                program_id: reg.program_id.clone(),
                state: reg.state_commitment.clone(),
                verifier: reg.verifier.clone(),
                timeout_window: reg.timeout_window.clone().unwrap_or_default(),
            }),
            vec![],
        )
    } else {
        (
            ContractStatus::Updated(Contract {
                name: reg.contract_name.clone(),
                program_id: reg.program_id.clone(),
                state: reg.state_commitment.clone(),
                verifier: reg.verifier.clone(),
                timeout_window: reg.timeout_window.clone().unwrap_or_default(),
            }),
            vec![SideEffect::Register(reg.constructor_metadata.clone())],
        )
    };

    contract_changes
        .entry(reg.contract_name.clone())
        .and_modify(|mcd| {
            mcd.contract_status = contract_status.clone();
            mcd.modified_fields = ModifiedContractFields::all();
            mcd.side_effects.extend(side_effects.clone());
        })
        .or_insert_with(|| ModifiedContractData {
            contract_status,
            modified_fields: ModifiedContractFields::all(),
            side_effects,
        });
    Ok(())
}

fn handle_delete_blob(
    contracts: &HashMap<ContractName, Contract>,
    contract_changes: &mut BTreeMap<ContractName, ModifiedContractData>,
    delete: &DeleteContractAction,
) -> Result<()> {
    // For now, Hyli is allowed to delete all contracts but itself
    if delete.contract_name.0 == "hyli" {
        bail!("Cannot delete Hyli contract");
    }

    // Check it's registered
    if contracts.contains_key(&delete.contract_name)
        || contract_changes.contains_key(&delete.contract_name)
    {
        contract_changes
            .entry(delete.contract_name.clone())
            .and_modify(|mcd| {
                mcd.contract_status = ContractStatus::WaitingDeletion;
                mcd.modified_fields = ModifiedContractFields::all();
            })
            .or_insert_with(|| ModifiedContractData {
                contract_status: ContractStatus::WaitingDeletion,
                modified_fields: ModifiedContractFields::all(),
                side_effects: vec![],
            });
        Ok(())
    } else {
        bail!("Contract {} does not exist", delete.contract_name.0);
    }
}

fn handle_update_program_id_blob(
    contracts: &HashMap<ContractName, Contract>,
    contract_changes: &mut BTreeMap<ContractName, ModifiedContractData>,
    update: &UpdateContractProgramIdAction,
) -> Result<()> {
    // For now, Hyli is allowed to delete all contracts but itself
    if update.contract_name.0 == "hyli" {
        bail!("Cannot update Hyli contract");
    }

    let contract =
        NodeStateProcessing::get_contract(contracts, contract_changes, &update.contract_name)?
            .clone();

    contract_changes
        .entry(update.contract_name.clone())
        .and_modify(|mcd| {
            if let ContractStatus::Updated(ref mut contract) = mcd.contract_status {
                contract.program_id = update.program_id.clone();
            }
            mcd.modified_fields.program_id = true;
            mcd.side_effects.push(SideEffect::UpdateProgramId);
        })
        .or_insert_with(|| ModifiedContractData {
            contract_status: ContractStatus::Updated(Contract {
                program_id: update.program_id.clone(),
                ..contract
            }),
            modified_fields: ModifiedContractFields {
                program_id: true,
                ..ModifiedContractFields::default()
            },
            side_effects: vec![SideEffect::UpdateProgramId],
        });
    Ok(())
}

fn handle_update_timeout_window_blob(
    contracts: &HashMap<ContractName, Contract>,
    contract_changes: &mut BTreeMap<ContractName, ModifiedContractData>,
    update: &UpdateContractTimeoutWindowAction,
) -> Result<()> {
    // For now, Hyli is allowed to delete all contracts but itself
    if update.contract_name.0 == "hyli" {
        bail!("Cannot update Hyli contract");
    }

    let contract =
        NodeStateProcessing::get_contract(contracts, contract_changes, &update.contract_name)?
            .clone();

    let new_update = SideEffect::UpdateTimeoutWindow;
    contract_changes
        .entry(update.contract_name.clone())
        .and_modify(|mcd| {
            if let ContractStatus::Updated(ref mut contract) = mcd.contract_status {
                contract.timeout_window = update.timeout_window.clone();
            }
            mcd.modified_fields.timeout_window = true;
            mcd.side_effects.push(new_update.clone());
        })
        .or_insert_with(|| ModifiedContractData {
            contract_status: ContractStatus::Updated(Contract {
                timeout_window: update.timeout_window.clone(),
                ..contract
            }),
            modified_fields: ModifiedContractFields {
                timeout_window: true,
                ..ModifiedContractFields::default()
            },
            side_effects: vec![new_update],
        });
    Ok(())
}

/// Validates hyli contract blobs by ensuring actions are authorized and properly signed
///
/// This function ensures that:
/// 1. Only authorized identities (HYLI_TLD_ID) can perform DeleteContractAction, UpdateContractProgramIdAction and UpdateContractTimeoutWindowAction actions
/// 2. Any other unsupported action is rejected
pub fn validate_hyli_contract_blobs(
    contracts: &HashMap<ContractName, Contract>,
    tx: &BlobTransaction,
) -> Result<(), String> {
    // Collect NukeTxAction blobs and secp256k1 blobs
    for blob in tx.blobs.iter() {
        if blob.contract_name.0 == "hyli" {
            // Check identity authorization for privileged actions
            if DeleteContractAction::try_from_slice(&blob.data.0).is_ok()
                || UpdateContractProgramIdAction::try_from_slice(&blob.data.0).is_ok()
                || UpdateContractTimeoutWindowAction::try_from_slice(&blob.data.0).is_ok()
            {
                if tx.identity.0 != HYLI_TLD_ID {
                    return Err(format!(
                        "Unauthorized action for 'hyli' TLD from identity: {}",
                        tx.identity.0
                    ));
                }
            } else if let Ok(registration_blob) =
                RegisterContractAction::try_from_slice(&blob.data.0)
            {
                if contracts.contains_key(&registration_blob.contract_name) {
                    return Err(format!(
                        "Contract {} is already registered, cannot register again",
                        registration_blob.contract_name.0
                    ));
                }
            } else {
                return Err(format!(
                    "Unsupported permissioned action on hyli contract: {blob:?}"
                ));
            }
        }
    }

    Ok(())
}

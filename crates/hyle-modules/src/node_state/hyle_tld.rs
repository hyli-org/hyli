use crate::node_state::{
    contract_registration::validate_contract_registration_metadata, ContractStatus,
    ModifiedContractData, ModifiedContractFields, NodeState,
};
use anyhow::{bail, Result};
use sdk::*;
use std::collections::{BTreeMap, HashMap};

use super::SideEffect;

pub const HYLI_TLD_ID: &str = "hyli@wallet";

pub fn handle_blob_for_hyle_tld(
    contracts: &HashMap<ContractName, Contract>,
    contract_changes: &mut BTreeMap<ContractName, ModifiedContractData>,
    current_blob: &Blob,
) -> Result<()> {
    // TODO: check the identity of the caller here.

    if let Ok(reg) = borsh::from_slice::<RegisterContractAction>(&current_blob.data.0) {
        handle_register_blob(contracts, contract_changes, &reg)?;
    } else if let Ok(del) = borsh::from_slice::<DeleteContractAction>(&current_blob.data.0) {
        handle_delete_blob(contracts, contract_changes, &del)?;
    } else if let Ok(updt) =
        borsh::from_slice::<UpdateContractProgramIdAction>(&current_blob.data.0)
    {
        handle_update_program_id_blob(contracts, contract_changes, &updt)?;
    } else if let Ok(updt) =
        borsh::from_slice::<UpdateContractTimeoutWindowAction>(&current_blob.data.0)
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
        &"hyle".into(),
        &reg.contract_name,
        &reg.verifier,
        &reg.program_id,
        &reg.state_commitment,
    )?;

    // Check it's not already registered
    if reg.contract_name.0 != "hyle" && contracts.contains_key(&reg.contract_name)
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
        .and_modify(|c| {
            c.0 = contract_status.clone();
            c.1 = ModifiedContractFields::all();
            c.2.extend(side_effects.clone());
        })
        .or_insert_with(|| (contract_status, ModifiedContractFields::all(), side_effects));
    Ok(())
}

fn handle_delete_blob(
    contracts: &HashMap<ContractName, Contract>,
    contract_changes: &mut BTreeMap<ContractName, ModifiedContractData>,
    delete: &DeleteContractAction,
) -> Result<()> {
    // For now, Hyli is allowed to delete all contracts but itself
    if delete.contract_name.0 == "hyle" {
        bail!("Cannot delete Hyli contract");
    }

    // Check it's registered
    if contracts.contains_key(&delete.contract_name)
        || contract_changes.contains_key(&delete.contract_name)
    {
        contract_changes
            .entry(delete.contract_name.clone())
            .and_modify(|c| {
                c.0 = ContractStatus::WaitingDeletion;
                c.1 = ModifiedContractFields::all();
            })
            .or_insert_with(|| {
                (
                    ContractStatus::WaitingDeletion,
                    ModifiedContractFields::all(),
                    vec![],
                )
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
    if update.contract_name.0 == "hyle" {
        bail!("Cannot udpate Hyli contract");
    }

    let contract =
        NodeState::get_contract(contracts, contract_changes, &update.contract_name)?.clone();

    contract_changes
        .entry(update.contract_name.clone())
        .and_modify(|c| {
            if let ContractStatus::Updated(ref mut contract) = c.0 {
                contract.program_id = update.program_id.clone();
            }
            c.1.program_id = true;
            c.2.push(SideEffect::UpdateProgramId);
        })
        .or_insert_with(|| {
            (
                ContractStatus::Updated(Contract {
                    program_id: update.program_id.clone(),
                    ..contract
                }),
                ModifiedContractFields {
                    program_id: true,
                    ..ModifiedContractFields::default()
                },
                vec![SideEffect::UpdateProgramId],
            )
        });
    Ok(())
}

fn handle_update_timeout_window_blob(
    contracts: &HashMap<ContractName, Contract>,
    contract_changes: &mut BTreeMap<ContractName, ModifiedContractData>,
    update: &UpdateContractTimeoutWindowAction,
) -> Result<()> {
    // For now, Hyli is allowed to delete all contracts but itself
    if update.contract_name.0 == "hyle" {
        bail!("Cannot udpate Hyli contract");
    }

    let contract =
        NodeState::get_contract(contracts, contract_changes, &update.contract_name)?.clone();

    let new_update = SideEffect::UpdateTimeoutWindow;
    contract_changes
        .entry(update.contract_name.clone())
        .and_modify(|c| {
            if let ContractStatus::Updated(ref mut contract) = c.0 {
                contract.timeout_window = update.timeout_window.clone();
            }
            c.1.timeout_window = true;
            c.2.push(new_update.clone());
        })
        .or_insert_with(|| {
            (
                ContractStatus::Updated(Contract {
                    timeout_window: update.timeout_window.clone(),
                    ..contract
                }),
                ModifiedContractFields {
                    timeout_window: true,
                    ..ModifiedContractFields::default()
                },
                vec![new_update],
            )
        });
    Ok(())
}

/// Validates hyle contract blobs by ensuring actions are authorized and properly signed
///
/// This function ensures that:
/// 1. Only authorized identities (HYLI_TLD_ID) can perform DeleteContractAction, UpdateContractProgramIdAction and UpdateContractTimeoutWindowAction actions
/// 2. Any other unsupported action is rejected
pub fn validate_hyle_contract_blobs(
    contracts: &HashMap<ContractName, Contract>,
    tx: &BlobTransaction,
) -> Result<(), String> {
    // Collect NukeTxAction blobs and secp256k1 blobs
    for blob in tx.blobs.iter() {
        if blob.contract_name.0 == "hyle" {
            // Check identity authorization for privileged actions
            if borsh::from_slice::<DeleteContractAction>(&blob.data.0).is_ok()
                || borsh::from_slice::<UpdateContractProgramIdAction>(&blob.data.0).is_ok()
                || borsh::from_slice::<UpdateContractTimeoutWindowAction>(&blob.data.0).is_ok()
            {
                if tx.identity.0 != HYLI_TLD_ID {
                    return Err(format!(
                        "Unauthorized action for 'hyle' TLD from identity: {}",
                        tx.identity.0
                    ));
                }
            } else if let Ok(registration_blob) =
                borsh::from_slice::<RegisterContractAction>(&blob.data.0)
            {
                if contracts.contains_key(&registration_blob.contract_name) {
                    return Err(format!(
                        "Contract {} is already registered, cannot register again",
                        registration_blob.contract_name.0
                    ));
                }
            } else {
                return Err(format!(
                    "Unsupported permissioned action on hyle contract: {blob:?}"
                ));
            }
        }
    }

    Ok(())
}

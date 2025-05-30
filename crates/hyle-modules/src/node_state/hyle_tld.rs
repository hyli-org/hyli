use crate::node_state::contract_registration::validate_contract_registration_metadata;
use anyhow::{bail, Result};
use sdk::*;
use std::collections::{BTreeMap, HashMap};

use super::SideEffect;

pub const HYLI_TLD_SECP256K1: &str = "hyle@secp256k1";

pub fn handle_blob_for_hyle_tld(
    contracts: &HashMap<ContractName, Contract>,
    contract_changes: &mut BTreeMap<ContractName, SideEffect>,
    current_blob: &Blob,
    identity: &Identity,
) -> Result<()> {
    // TODO: check the identity of the caller here.

    // TODO: support unstructured blobs as well ?
    if let Ok(reg) =
        StructuredBlobData::<RegisterContractAction>::try_from(current_blob.data.clone())
    {
        handle_register_blob(contracts, contract_changes, &reg.parameters)?;
    } else if let Ok(reg) =
        StructuredBlobData::<DeleteContractAction>::try_from(current_blob.data.clone())
    {
        handle_delete_blob(contracts, contract_changes, &reg.parameters, identity)?;
    } else {
        bail!("Invalid blob data for TLD");
    }
    Ok(())
}

fn handle_register_blob(
    contracts: &HashMap<ContractName, Contract>,
    contract_changes: &mut BTreeMap<ContractName, SideEffect>,
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

    contract_changes.insert(
        reg.contract_name.clone(),
        SideEffect::Register(
            Contract {
                name: reg.contract_name.clone(),
                program_id: reg.program_id.clone(),
                state: reg.state_commitment.clone(),
                verifier: reg.verifier.clone(),
                timeout_window: reg.timeout_window.clone().unwrap_or_default(),
            },
            reg.constructor_metadata.clone(),
        ),
    );
    Ok(())
}

fn handle_delete_blob(
    contracts: &HashMap<ContractName, Contract>,
    contract_changes: &mut BTreeMap<ContractName, SideEffect>,
    delete: &DeleteContractAction,
    identity: &Identity,
) -> Result<()> {
    // We permission contract deletions to this single identity generated in the genesis
    if identity != &Identity(HYLI_TLD_SECP256K1.into()) {
        bail!(
            "{} is not allowed to delete contract {}",
            identity,
            delete.contract_name
        );
    }

    // For now, Hyli is allowed to delete all contracts but itself
    if delete.contract_name.0 == "hyle" {
        bail!("Cannot delete Hyli contract");
    }

    // Check it's registered
    if contracts.contains_key(&delete.contract_name)
        || contract_changes.contains_key(&delete.contract_name)
    {
        contract_changes.insert(
            delete.contract_name.clone(),
            SideEffect::Delete(delete.contract_name.clone()),
        );
        Ok(())
    } else {
        bail!("Contract {} is already registered", delete.contract_name.0);
    }
}

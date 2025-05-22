use crate::{
    alloc::string::{String, ToString},
    caller::ExecutionContext,
    Identity, StructuredBlobData,
};
use alloc::{format, vec};
use borsh::{BorshDeserialize, BorshSerialize};
use core::result::Result;

use hyle_model::{
    Blob, BlobIndex, Calldata, DropEndOfReader, HyleOutput, IndexedBlobs, StateCommitment,
    StructuredBlob,
};

/// This function is used to parse the contract input blob data into a given template `Action`
/// It assumes that the blob data is the `Action` serialized with borsh.
/// It returns a tuple with the parsed `Action` and an [ExecutionContext] that can be used
/// by the contract, and will be needed by the sdk to build the [HyleOutput].
///
/// Alternative: [parse_calldata]
pub fn parse_raw_calldata<Action>(calldata: &Calldata) -> Result<(Action, ExecutionContext), String>
where
    Action: BorshDeserialize,
{
    let blobs = &calldata.blobs;
    let index = &calldata.index;

    let blob = match blobs.get(index) {
        Some(v) => v,
        None => {
            return Err(format!("Could not find Blob at index {index}"));
        }
    };

    let Ok(parameters) = borsh::from_slice::<Action>(blob.data.0.as_slice()) else {
        return Err(format!("Could not deserialize Blob at index {index}"));
    };

    let exec_ctx = ExecutionContext::new(calldata.identity.clone(), blob.contract_name.clone());
    Ok((parameters, exec_ctx))
}

/// This function is used to parse the contract input blob data.
/// It assumes that the blob data is a [StructuredBlobData] serialized with borsh.
/// It returns a tuple with the parsed `Action` and an [ExecutionContext] that can be used
/// by the contract, and will be needed by the sdk to build the [HyleOutput].
///
/// The [ExecutionContext] will holds the caller/callees information.
/// See [StructuredBlobData] page for more information on caller/callees.
///
/// Alternative: [parse_raw_calldata]
pub fn parse_calldata<Action>(calldata: &Calldata) -> Result<(Action, ExecutionContext), String>
where
    Action: BorshSerialize + BorshDeserialize,
{
    let parsed_blob = parse_structured_blob::<Action>(&calldata.blobs, &calldata.index);

    let parsed_blob = parsed_blob.ok_or("Failed to parse input blob".to_string())?;

    let caller = check_caller_callees::<Action>(calldata, &parsed_blob)?;

    let mut callees_blobs = vec::Vec::new();
    for (_, blob) in &calldata.blobs {
        if let Ok(structured_blob) = blob.data.clone().try_into() {
            let structured_blob: StructuredBlobData<DropEndOfReader> = structured_blob; // for type inference
            if structured_blob.caller == Some(calldata.index) {
                callees_blobs.push(blob.clone());
            }
        };
    }

    let ctx = ExecutionContext {
        callees_blobs,
        caller,
        contract_name: parsed_blob.contract_name.clone(),
    };

    Ok((parsed_blob.data.parameters, ctx))
}

pub fn parse_blob<Action>(blobs: &[Blob], index: &BlobIndex) -> Option<Action>
where
    Action: BorshDeserialize,
{
    let blob = match blobs.get(index.0) {
        Some(v) => v,
        None => {
            return None;
        }
    };

    let Ok(parameters) = borsh::from_slice::<Action>(blob.data.0.as_slice()) else {
        return None;
    };

    Some(parameters)
}

pub fn parse_structured_blob<Action>(
    blobs: &IndexedBlobs,
    index: &BlobIndex,
) -> Option<StructuredBlob<Action>>
where
    Action: BorshDeserialize,
{
    let blob = match blobs.get(index) {
        Some(v) => v,
        None => {
            return None;
        }
    };

    let parsed_blob: StructuredBlob<Action> = match StructuredBlob::try_from(blob.clone()) {
        Ok(v) => v,
        Err(_) => {
            return None;
        }
    };
    Some(parsed_blob)
}

fn fail(
    calldata: &Calldata,
    initial_state_commitment: StateCommitment,
    message: &str,
) -> HyleOutput {
    HyleOutput {
        version: 1,
        initial_state: initial_state_commitment.clone(),
        next_state: initial_state_commitment,
        identity: calldata.identity.clone(),
        index: calldata.index,
        blobs: calldata.blobs.clone(),
        tx_blob_count: calldata.tx_blob_count,
        success: false,
        tx_hash: calldata.tx_hash.clone(),
        state_reads: vec![],
        tx_ctx: calldata.tx_ctx.clone(),
        onchain_effects: vec![],
        program_outputs: message.to_string().into_bytes(),
    }
}

pub fn as_hyle_output(
    initial_state_commitment: StateCommitment,
    next_state_commitment: StateCommitment,
    calldata: &Calldata,
    res: &mut crate::RunResult,
) -> HyleOutput {
    match res {
        Ok((ref mut program_output, execution_context, ref mut onchain_effects)) => {
            if !execution_context.callees_blobs.is_empty() {
                return fail(
                    calldata,
                    initial_state_commitment,
                    &format!(
                        "Execution context has not been fully consumed {:?}",
                        execution_context.callees_blobs
                    ),
                );
            }
            HyleOutput {
                version: 1,
                initial_state: initial_state_commitment,
                next_state: next_state_commitment,
                identity: calldata.identity.clone(),
                index: calldata.index,
                blobs: calldata.blobs.clone(),
                tx_blob_count: calldata.tx_blob_count,
                success: true,
                tx_hash: calldata.tx_hash.clone(),
                state_reads: vec![],
                tx_ctx: calldata.tx_ctx.clone(),
                onchain_effects: core::mem::take(onchain_effects),
                program_outputs: core::mem::take(program_output),
            }
        }
        Err(message) => fail(calldata, initial_state_commitment, message),
    }
}

pub fn check_caller_callees<Action>(
    calldata: &Calldata,
    parameters: &StructuredBlob<Action>,
) -> Result<Identity, String>
where
    Action: BorshSerialize + BorshDeserialize,
{
    // Check that callees has this blob as caller
    if let Some(callees) = parameters.data.callees.as_ref() {
        // Check that the number of callees matches the number of blobs that have this blob as caller
        let num_callees = callees.len();
        let num_blobs_with_this_as_caller = calldata
            .blobs
            .iter()
            .filter(|(_, blob)| {
                if let Ok(structured_blob) =
                    blob.data.clone().try_into() as Result<StructuredBlobData<DropEndOfReader>, _>
                {
                    structured_blob.caller == Some(calldata.index)
                } else {
                    false
                }
            })
            .count();
        if num_callees != num_blobs_with_this_as_caller {
            return Err("Number of callees does not match number of blobs that reference this blob as caller".to_string());
        }
    }
    // Extract the correct caller
    if let Some(caller_index) = parameters.data.caller.as_ref() {
        if *caller_index == calldata.index {
            return Err("Self-reference as callee is forbidden".to_string());
        }

        if let Some(caller_blob) = calldata.blobs.get(caller_index).cloned() {
            let caller_structured_blob: StructuredBlobData<DropEndOfReader> = caller_blob
                .data
                .try_into()
                .map_err(|_| format!("Could not parse blob {caller_index} as a StructuredBlob"))?;
            // Check that caller has this blob as callee
            let Some(caller_callees) = caller_structured_blob.callees else {
                return Err("Caller does not have any callees".to_string());
            };
            if !caller_callees.contains(&calldata.index) {
                return Err("Incorrect Caller for this blob".to_string());
            }
            return Ok(caller_blob.contract_name.0.clone().into());
        } else {
            return Err(format!("Caller index {caller_index} not found in blobs"));
        }
    }

    // No callers detected, use the identity
    Ok(calldata.identity.clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use hyle_model::{Blob, BlobData, ContractName, TxHash};

    fn make_calldata(
        identity: Identity,
        index: BlobIndex,
        blobs: IndexedBlobs,
        tx_blob_count: usize,
    ) -> Calldata {
        Calldata {
            identity,
            index,
            blobs,
            tx_blob_count,
            tx_hash: TxHash::default(),
            tx_ctx: None,
            private_input: vec![],
        }
    }

    fn make_blob(
        contract: &str,
        caller: Option<BlobIndex>,
        callees: Option<Vec<BlobIndex>>,
    ) -> Blob {
        Blob {
            contract_name: ContractName::new(contract),
            data: BlobData(
                borsh::to_vec(&StructuredBlobData {
                    caller,
                    callees,
                    parameters: (),
                })
                .unwrap(),
            ),
        }
    }

    fn make_test_case(
        blob_specs: Vec<(BlobIndex, &str, Option<BlobIndex>, Option<Vec<BlobIndex>>)>,
        test_index: BlobIndex,
    ) -> (Calldata, StructuredBlob<()>) {
        let blobs: Vec<(BlobIndex, Blob)> = blob_specs
            .iter()
            .map(|(idx, contract, caller, callees)| {
                (*idx, make_blob(contract, *caller, callees.clone()))
            })
            .collect();
        let calldata = make_calldata(Identity::new("user"), test_index, IndexedBlobs(blobs), 2);
        // Find the blob spec for test_index
        let (_, _, caller, callees) = blob_specs
            .iter()
            .find(|(idx, _, _, _)| *idx == test_index)
            .expect("test_index must be present in blob_specs");
        let parameters = StructuredBlob {
            contract_name: ContractName::new("test"),
            data: StructuredBlobData {
                caller: *caller,
                callees: callees.clone(),
                parameters: (),
            },
        };
        (calldata, parameters)
    }

    fn expect_error(
        blob_specs: Vec<(BlobIndex, &str, Option<BlobIndex>, Option<Vec<BlobIndex>>)>,
        test_index: BlobIndex,
        expected_error: &str,
    ) {
        let (calldata, parameters) = make_test_case(blob_specs, test_index);
        let result = check_caller_callees(&calldata, &parameters);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), expected_error);
    }

    fn expect_ok(
        blob_specs: Vec<(BlobIndex, &str, Option<BlobIndex>, Option<Vec<BlobIndex>>)>,
        test_index: BlobIndex,
    ) {
        let (calldata, parameters) = make_test_case(blob_specs, test_index);
        let result = check_caller_callees(&calldata, &parameters);
        assert!(result.is_ok());
    }

    #[test]
    fn test_valid_caller_and_callee() {
        expect_ok(
            vec![
                (BlobIndex(0), "caller", None, Some(vec![BlobIndex(1)])),
                (BlobIndex(1), "callee", Some(BlobIndex(0)), None),
            ],
            BlobIndex(1),
        );
    }

    #[test]
    fn test_callee_does_not_have_this_blob_as_caller() {
        expect_error(
            vec![
                (BlobIndex(0), "caller", None, Some(vec![BlobIndex(1)])),
                (BlobIndex(1), "callee", None, None),
            ],
            BlobIndex(0),
            "Number of callees does not match number of blobs that reference this blob as caller",
        );
    }

    #[test]
    fn test_caller_does_not_have_this_blob_as_callee() {
        expect_error(
            vec![
                (BlobIndex(0), "caller", None, Some(vec![BlobIndex(2)])),
                (BlobIndex(1), "callee", Some(BlobIndex(0)), None),
            ],
            BlobIndex(1),
            "Incorrect Caller for this blob",
        );
    }

    #[test]
    fn test_callee_index_does_not_exist() {
        expect_error(
            vec![(BlobIndex(0), "caller", None, Some(vec![BlobIndex(1)]))],
            BlobIndex(0),
            "Number of callees does not match number of blobs that reference this blob as caller",
        );
    }

    #[test]
    fn test_caller_index_does_not_exist() {
        expect_error(
            vec![(BlobIndex(1), "callee", Some(BlobIndex(0)), None)],
            BlobIndex(1),
            "Caller index 0 not found in blobs",
        );
    }

    #[test]
    fn test_caller_has_no_callees() {
        expect_error(
            vec![
                (BlobIndex(0), "caller", None, None),
                (BlobIndex(1), "callee", Some(BlobIndex(0)), None),
            ],
            BlobIndex(1),
            "Caller does not have any callees",
        );
    }

    #[test]
    fn test_caller_has_empty_callees() {
        expect_error(
            vec![
                (BlobIndex(0), "caller", None, Some(vec![])),
                (BlobIndex(1), "callee", Some(BlobIndex(0)), None),
            ],
            BlobIndex(1),
            "Incorrect Caller for this blob",
        );
    }

    #[test]
    fn test_no_caller_no_callees_returns_identity() {
        let (calldata, parameters) =
            make_test_case(vec![(BlobIndex(0), "caller", None, None)], BlobIndex(0));
        let result = check_caller_callees(&calldata, &parameters);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), calldata.identity);
    }

    #[test]
    fn test_both_caller_and_callees_valid() {
        // Blob 0 is caller, Blob 1 is callee, both reference each other, and Blob 1 also has a callee
        expect_ok(
            vec![
                (BlobIndex(0), "caller", None, Some(vec![BlobIndex(1)])),
                (
                    BlobIndex(1),
                    "callee",
                    Some(BlobIndex(0)),
                    Some(vec![BlobIndex(2)]),
                ),
                (BlobIndex(2), "next", Some(BlobIndex(1)), None),
            ],
            BlobIndex(1),
        );
    }

    #[test]
    fn test_self_reference_as_caller() {
        // Blob references itself as caller / callee (forbidden)
        expect_error(
            vec![(
                BlobIndex(0),
                "self",
                Some(BlobIndex(0)),
                Some(vec![BlobIndex(0)]),
            )],
            BlobIndex(0),
            "Self-reference as callee is forbidden",
        );
    }

    #[test]
    fn test_caller_has_callees_but_not_this_blob() {
        // Caller exists, has callees, but not the current blob (forbidden)
        expect_error(
            vec![
                (BlobIndex(0), "caller", None, Some(vec![BlobIndex(2)])),
                (BlobIndex(1), "callee", Some(BlobIndex(0)), None),
                (BlobIndex(2), "other", Some(BlobIndex(0)), None),
            ],
            BlobIndex(1),
            "Incorrect Caller for this blob",
        );
    }

    #[test]
    fn test_incorrect_number_of_callees() {
        // Blob 1 and Blob 2 both reference Blob 0 as their caller,
        // but Blob 0 only lists one callee.
        expect_error(
            vec![
                (BlobIndex(0), "main", None, Some(vec![BlobIndex(1)])), // Only one callee listed
                (BlobIndex(1), "a", Some(BlobIndex(0)), None),
                (BlobIndex(2), "b", Some(BlobIndex(0)), None),
            ],
            BlobIndex(0),
            "Number of callees does not match number of blobs that reference this blob as caller",
        );
        // Inverse
        expect_error(
            vec![
                (
                    BlobIndex(0),
                    "main",
                    None,
                    Some(vec![BlobIndex(1), BlobIndex(2)]),
                ),
                (BlobIndex(1), "a", Some(BlobIndex(0)), None),
                (BlobIndex(2), "b", None, None),
            ],
            BlobIndex(0),
            "Number of callees does not match number of blobs that reference this blob as caller",
        );
    }
}

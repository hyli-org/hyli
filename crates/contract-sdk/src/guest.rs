#![allow(clippy::needless_doctest_main)]
/*!
The `guest` module provides the `GuestEnv` trait that defines a common interface for the guests accross different zkvm.
It provides `Risc0Env` and `SP1Env` structs that implement the `GuestEnv` trait for both zkvm.

The [execute] function is used to execute an action on a given contract using the provided state and contract input.

The `fail` function is used to generate a failure output for a contract action.

## Example usage of this module for risc0:

This is a code snippet of a Risc0 guest entrypoint (e.g. file `methods/guest/src/main.rs` in a risc0 template project).

`Hydentity` struct has to implements [ZkContract] and [BorshDeserialize] traits.

```rust,no_run,compile_fail
#![no_main]

use hyle_hydentity::Hydentity;
use sdk::guest::{execute, GuestEnv, Risc0Env};

risc0_zkvm::guest::entry!(main);

fn main() {
   let env = Risc0Env {};
   let zk_program_input = env.read();
   let output = execute::<Hydentity>(&zk_program_input);
   env.commit(&output);
}
```

## Example usage of this module for sp1:

This is a code snippet of a SP1 guest entrypoint (e.g. file `program/src/main.rs` in a sp1 template project).


`IdentityContractState` struct has to implements [ZkContract] and [BorshDeserialize] traits.

```rust,no_run,compile_fail
#![no_main]

use contract::IdentityContractState;
use sdk::guest::execute;
use sdk::guest::GuestEnv;
use sdk::guest::SP1Env;

sp1_zkvm::entrypoint!(main);

fn main() {
    let env = SP1Env {};
    let input = env.read();
    let output = execute::<IdentityContractState>(&input);
    env.commit(&output);
}
```

*/

use alloc::vec::Vec;
use borsh::BorshDeserialize;
use hyle_model::Calldata;

use crate::{utils::as_hyle_output, HyleOutput};
use crate::{RunResult, ZkContract};

pub trait GuestEnv {
    fn log(&self, message: &str);
    fn commit(&self, output: Vec<HyleOutput>);
    fn read<T: BorshDeserialize + 'static>(&self) -> T;
}

pub struct Risc0Env;

#[cfg(feature = "risc0")]
use alloc::vec;
#[cfg(feature = "risc0")]
impl GuestEnv for Risc0Env {
    fn log(&self, message: &str) {
        risc0_zkvm::guest::env::log(message);
    }

    fn commit(&self, output: Vec<HyleOutput>) {
        risc0_zkvm::guest::env::commit(&output);
    }

    fn read<T: BorshDeserialize>(&self) -> T {
        let len: usize = risc0_zkvm::guest::env::read();
        let mut slice = vec![0u8; len];
        risc0_zkvm::guest::env::read_slice(&mut slice);
        #[allow(clippy::unwrap_used, reason = "should panic here")]
        borsh::from_slice(&slice).unwrap()
    }
}

pub struct SP1Env;

// For coverage tests, assume risc0 if both are active
#[cfg(feature = "sp1")]
impl GuestEnv for SP1Env {
    fn log(&self, message: &str) {
        // TODO: this does nothing actually
        sp1_zkvm::io::hint(&message);
    }

    fn commit(&self, output: Vec<HyleOutput>) {
        #[allow(clippy::unwrap_used, reason = "should panic here")]
        let vec = borsh::to_vec(&output).unwrap();
        sp1_zkvm::io::commit_slice(&vec);
    }

    fn read<T: BorshDeserialize>(&self) -> T {
        let vec = sp1_zkvm::io::read_vec();
        #[allow(clippy::unwrap_used, reason = "should panic here")]
        borsh::from_slice(&vec).unwrap()
    }
}

/// Executes an action on a given contract using the provided commitment metadata and the contract calldata.
/// This is the execution function that will be proved by the zkvm.
///
/// # Arguments
///
/// * `commitment_metadata` - This is the minimum data required to reconstruct the commitment of the state.
/// * `calldata` - This is the data that the contract will use to execute.
///
/// # Type Parameters
///
/// * `State` - The type of the state that must implement the `ZkProgram` and `BorshDeserialize` traits.
///
/// # Returns
///
/// The contract output as `HyleOutput`.
///
/// # Panics
///
/// Panics if the contract initialization fails.
pub fn execute<Z>(commitment_metadata: &[u8], calldata: &[Calldata]) -> Vec<HyleOutput>
where
    Z: ZkContract + BorshDeserialize + 'static,
{
    #[allow(clippy::expect_used, reason = "Required to generate valid proofs")]
    let mut contract: Z =
        borsh::from_slice(commitment_metadata).expect("Failed to decode commitment metadata");
    let mut initial_state_commitment = contract.commit();

    let mut outputs = Vec::with_capacity(calldata.len());
    if let Err(e) = contract.initialize() {
        for calldata in calldata.iter() {
            outputs.push(as_hyle_output(
                initial_state_commitment.clone(),
                initial_state_commitment.clone(),
                calldata,
                &mut Err(e.clone()),
            ));
        }
        return outputs;
    }
    for calldata in calldata.iter() {
        let mut res: RunResult = contract.execute(calldata);

        let next_state_commitment = contract.commit();

        outputs.push(as_hyle_output(
            initial_state_commitment,
            next_state_commitment.clone(),
            calldata,
            &mut res,
        ));
        initial_state_commitment = next_state_commitment;
    }
    outputs
}

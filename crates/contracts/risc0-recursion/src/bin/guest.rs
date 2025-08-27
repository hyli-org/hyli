#![no_main]
#![no_std]

extern crate alloc;

use alloc::vec::Vec;
use hyli_risc0_recursion::{ProofInput, Risc0Journal, Risc0ProgramId};
use risc0_zkvm::guest::env;

risc0_zkvm::guest::entry!(main);

fn main() {
    let mut input: Vec<ProofInput> = env::read();
    let mut outputs: Vec<(Risc0ProgramId, Risc0Journal)> = Vec::new();
    input.iter_mut().for_each(|input| {
        risc0_zkvm::guest::env::verify(input.image_id, &input.journal)
            .expect("Verification failed");
        outputs.push((
            core::mem::take(&mut input.image_id),
            core::mem::take(&mut input.journal),
        ));
    });
    risc0_zkvm::guest::env::commit(&outputs);
}

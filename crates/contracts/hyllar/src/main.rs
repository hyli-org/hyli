#![no_main]
#![no_std]

extern crate alloc;

use alloc::vec::Vec;
use hyli_hyllar::Hyllar;
use sdk::{
    Calldata,
    guest::{GuestEnv, Risc0Env, execute},
};

risc0_zkvm::guest::entry!(main);

fn main() {
    let env = Risc0Env {};
    let (commitment_metadata, calldatas): (Vec<u8>, Vec<Calldata>) = env.read();

    let output = execute::<Hyllar>(&commitment_metadata, &calldatas);
    env.commit(output);
}

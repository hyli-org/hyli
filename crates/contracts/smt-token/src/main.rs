#![no_main]
#![no_std]

extern crate alloc;

use alloc::vec::Vec;
use hyle_smt_token::SmtTokenContract;
use sdk::{
    guest::{execute, GuestEnv, Risc0Env},
    Calldata,
};

risc0_zkvm::guest::entry!(main);

fn main() {
    let env = Risc0Env {};
    let (commitment_metadata, calldatas): (Vec<u8>, Vec<Calldata>) = env.read();

    let output = execute::<SmtTokenContract>(&commitment_metadata, &calldatas);
    env.commit(output);
}

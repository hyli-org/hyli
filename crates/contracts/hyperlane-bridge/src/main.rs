#![no_main]

use hyli_hyperlane_bridge::HyperlaneBridgeState;
use sdk::{
    guest::{execute, GuestEnv, Risc0Env},
    Calldata,
};

risc0_zkvm::guest::entry!(main);

fn main() {
    let env = Risc0Env {};
    let (commitment_metadata, calldatas): (Vec<u8>, Vec<Calldata>) = env.read();
    let output = execute::<HyperlaneBridgeState>(&commitment_metadata, &calldatas);
    env.commit(output);
}

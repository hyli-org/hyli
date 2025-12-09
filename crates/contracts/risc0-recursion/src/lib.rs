#![no_std]

#[cfg(feature = "client")]
pub mod metadata {
    pub const RISC0_RECURSION_ELF: &[u8] = include_bytes!("../risc0-recursion.img");
    pub const PROGRAM_ID: [u8; 32] = sdk::str_to_u8(include_str!("../risc0-recursion.txt"));
}

extern crate alloc;

#[cfg(feature = "client")]
pub mod client;

use alloc::vec::Vec;

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct ProofInput {
    pub image_id: [u8; 32],
    pub journal: Vec<u8>, // Should be a serde::to_vec<sdk::HyliOutput>,
}

pub type Risc0ProgramId = [u8; 32];
pub type Risc0Journal = Vec<u8>;

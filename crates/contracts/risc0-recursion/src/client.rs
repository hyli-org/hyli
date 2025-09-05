pub mod metadata {
    pub const RISC0_RECURSION_ELF: &[u8] = include_bytes!("../risc0-recursion.img");
    pub const PROGRAM_ID: [u8; 32] = sdk::str_to_u8(include_str!("../risc0-recursion.txt"));
}

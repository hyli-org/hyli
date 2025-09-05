#[allow(unused)]
#[cfg(all(not(clippy), feature = "nonreproducible"))]
mod methods {
    include!(concat!(env!("OUT_DIR"), "/methods.rs"));
}

#[cfg(all(not(clippy), feature = "nonreproducible", feature = "all"))]
mod metadata {
    pub const AMM_ELF: &[u8] = crate::methods::AMM_ELF;
    pub const AMM_ID: [u8; 32] = sdk::to_u8_array(&crate::methods::AMM_ID);

    pub const HYDENTITY_ELF: &[u8] = crate::methods::HYDENTITY_ELF;
    pub const HYDENTITY_ID: [u8; 32] = sdk::to_u8_array(&crate::methods::HYDENTITY_ID);

    pub const HYLLAR_ELF: &[u8] = crate::methods::HYLLAR_ELF;
    pub const HYLLAR_ID: [u8; 32] = sdk::to_u8_array(&crate::methods::HYLLAR_ID);

    pub const SMT_TOKEN_ELF: &[u8] = crate::methods::SMT_TOKEN_ELF;
    pub const SMT_TOKEN_ID: [u8; 32] = sdk::to_u8_array(&crate::methods::SMT_TOKEN_ID);

    pub const STAKING_ELF: &[u8] = crate::methods::STAKING_ELF;
    pub const STAKING_ID: [u8; 32] = sdk::to_u8_array(&crate::methods::STAKING_ID);

    pub const RISC0_RECURSION_ELF: &[u8] = crate::methods::RISC0_RECURSION_ELF;
    pub const RISC0_RECURSION_ID: [u8; 32] = sdk::to_u8_array(&crate::methods::RISC0_RECURSION_ID);

    pub const UUID_TLD_ELF: &[u8] = crate::methods::UUID_TLD_ELF;
    pub const UUID_TLD_ID: [u8; 32] = sdk::to_u8_array(&crate::methods::UUID_TLD_ID);
}

#[cfg(any(clippy, not(feature = "nonreproducible")))]
mod metadata {
    pub const AMM_ELF: &[u8] = amm::client::tx_executor_handler::metadata::AMM_ELF;
    pub const AMM_ID: [u8; 32] = amm::client::tx_executor_handler::metadata::PROGRAM_ID;

    pub const HYDENTITY_ELF: &[u8] =
        hydentity::client::tx_executor_handler::metadata::HYDENTITY_ELF;
    pub const HYDENTITY_ID: [u8; 32] = hydentity::client::tx_executor_handler::metadata::PROGRAM_ID;

    pub const HYLLAR_ELF: &[u8] = hyllar::client::tx_executor_handler::metadata::HYLLAR_ELF;
    pub const HYLLAR_ID: [u8; 32] = hyllar::client::tx_executor_handler::metadata::PROGRAM_ID;

    pub const SMT_TOKEN_ELF: &[u8] =
        smt_token::client::tx_executor_handler::metadata::SMT_TOKEN_ELF;
    pub const SMT_TOKEN_ID: [u8; 32] = smt_token::client::tx_executor_handler::metadata::PROGRAM_ID;

    pub const STAKING_ELF: &[u8] = staking::client::tx_executor_handler::metadata::STAKING_ELF;
    pub const STAKING_ID: [u8; 32] = staking::client::tx_executor_handler::metadata::PROGRAM_ID;

    pub const RISC0_RECURSION_ELF: &[u8] = risc0_recursion::client::metadata::RISC0_RECURSION_ELF;
    pub const RISC0_RECURSION_ID: [u8; 32] = risc0_recursion::client::metadata::PROGRAM_ID;

    pub const UUID_TLD_ELF: &[u8] = uuid_tld::client::tx_executor_handler::metadata::UUID_TLD_ELF;
    pub const UUID_TLD_ID: [u8; 32] = uuid_tld::client::tx_executor_handler::metadata::PROGRAM_ID;
}

pub use metadata::*;

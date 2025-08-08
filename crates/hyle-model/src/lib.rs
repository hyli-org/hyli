#[cfg(feature = "full")]
mod block;
#[cfg(feature = "full")]
mod node;
#[cfg(feature = "full")]
mod transaction;

pub mod utils;
pub mod verifiers;

#[cfg(feature = "full")]
pub use block::*;
#[cfg(feature = "full")]
pub use node::*;
#[cfg(feature = "full")]
pub use transaction::*;

#[cfg(feature = "full")]
pub mod api;

mod contract;
mod staking;
pub use contract::*;
pub use staking::*;

pub const HASH_DISPLAY_SIZE: usize = 3;

pub const HYLE_TESTNET_CHAIN_ID: u128 = 0x68796C655F746573746E6574;

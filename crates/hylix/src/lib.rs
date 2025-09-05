//! Hylix - Build, test & deploy verifiable apps on Hyli
//!
//! Hylix is a developer toolbox and CLI to build vApps on Hyli,
//! the new proof-powered L1 to build the next generation of apps onchain.

pub mod commands;
pub mod config;
pub mod error;
pub mod logging;

// Re-export commonly used types
pub use config::{BackendType, HylixConfig};
pub use error::{HylixError, HylixResult};

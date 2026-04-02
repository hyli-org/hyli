//! Hylix - Build, test & deploy verifiable apps on Hyli
//!
//! Hylix is a developer toolbox and CLI to build vApps on Hyli,
//! a high-performance blockchain with built-in privacy.

pub mod commands;
pub mod config;
pub mod constants;
pub mod docker;
pub mod env_builder;
pub mod error;
pub mod logging;
pub mod process;
pub mod validation;

// Re-export commonly used types
pub use config::{BackendType, HylixConfig};
pub use error::{HylixError, HylixResult};

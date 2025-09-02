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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_backend_type_serialization() {
        let sp1 = BackendType::Sp1;
        let risc0 = BackendType::Risc0;
        
        assert_eq!(sp1, BackendType::Sp1);
        assert_eq!(risc0, BackendType::Risc0);
    }

    #[test]
    fn test_config_default() {
        let config = HylixConfig::default();
        assert_eq!(config.default_backend, BackendType::Sp1);
        assert_eq!(config.devnet.node_port, 8080);
        assert_eq!(config.devnet.wallet_port, 4000);
        assert_eq!(config.devnet.indexer_port, 8081);
    }

    #[test]
    fn test_error_creation() {
        let error = HylixError::project("Test error");
        assert!(matches!(error, HylixError::Project(_)));
        
        let error = HylixError::build("Build failed");
        assert!(matches!(error, HylixError::Build(_)));
    }
}
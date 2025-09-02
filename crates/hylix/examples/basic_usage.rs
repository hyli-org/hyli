//! Basic usage example for Hylix CLI
//!
//! This example demonstrates how to use the Hylix CLI programmatically.

use hylix::{BackendType, HylixConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Load configuration
    let config = HylixConfig::load()?;
    println!("Loaded config: {:?}", config);

    // Example of creating a new project configuration
    let new_config = HylixConfig {
        default_backend: BackendType::Risc0,
        devnet: hylix::config::DevnetConfig {
            node_port: 8080,
            explorer_port: 3000,
            indexer_port: 8081,
            auto_start: true,
        },
        ..Default::default()
    };

    // Save the new configuration
    new_config.save()?;
    println!("Saved new configuration");

    Ok(())
}

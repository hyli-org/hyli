//! Basic usage example for Hylix CLI
//!
//! This example demonstrates how to use the Hylix CLI programmatically.

use hylix::{HylixConfig, BackendType};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Load configuration
    let config = HylixConfig::load()?;
    println!("Loaded config: {:?}", config);

    // Example of creating a new project configuration
    let mut new_config = HylixConfig::default();
    new_config.default_backend = BackendType::Risc0;
    new_config.devnet.node_port = 9000;
    
    // Save the new configuration
    new_config.save()?;
    println!("Saved new configuration");

    Ok(())
}

//! Basic usage example for Hylix CLI
//!
//! This example demonstrates how to use the Hylix CLI programmatically.

use hylix::{BackendType, HylixConfig, config::ContainerEnvConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Load configuration
    let config = HylixConfig::load()?;
    println!("Loaded config: {config:?}");

    // Example of creating a new project configuration
    let new_config = HylixConfig {
        default_backend: BackendType::Risc0,
        devnet: hylix::config::DevnetConfig {
            node_image: "ghcr.io/hyli-org/hyli:latest".to_string(),
            wallet_server_image: "ghcr.io/hyli-org/wallet/wallet-server:main".to_string(),
            wallet_ui_image: "ghcr.io/hyli-org/wallet/wallet-ui:main".to_string(),
            node_port: 8080,
            da_port: 4141,
            node_rust_log: "info".to_string(),
            wallet_api_port: 3000,
            wallet_ws_port: 8081,
            wallet_ui_port: 8080,
            indexer_port: 8081,
            postgres_port: 5432,
            auto_start: true,
            container_env: ContainerEnvConfig::default(),
            registry_server_image: "ghcr.io/hyli-org/hyli-registry/zkvm-registry-server:main"
                .to_string(),
            registry_ui_image: "ghcr.io/hyli-org/hyli-registry/zkvm-registry-ui:main".to_string(),
            registry_server_port: 9003,
            registry_ui_port: 8005,
        },
        ..Default::default()
    };

    // Save the new configuration
    new_config.save()?;
    println!("Saved new configuration");

    Ok(())
}

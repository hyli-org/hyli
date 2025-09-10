use crate::config::HylixConfig;
use crate::error::HylixResult;
use crate::logging::log_info;
use std::path::PathBuf;

/// Execute the `hy config` command
pub async fn execute() -> HylixResult<()> {
    log_info("Loading current configuration...");

    // Load the current configuration
    let config = HylixConfig::load()?;

    // Display the configuration in a readable format
    display_config(&config)?;

    Ok(())
}

/// Display the configuration in a readable format
fn display_config(config: &HylixConfig) -> HylixResult<()> {
    println!("\n📋 Hylix Configuration");
    println!("{}", "=".repeat(50));

    // General settings
    println!("\n🔧 General Settings:");
    println!("  Default Backend: {:?}", config.default_backend);
    println!("  Scaffold Repository: {}", config.scaffold_repo);
    println!("  Bake Profile: {}", config.bake_profile);

    // Devnet configuration
    println!("\n🌐 Devnet Configuration:");
    println!("  Node Image: {}", config.devnet.node_image);
    println!("  Wallet Server Image: {}", config.devnet.wallet_server_image);
    println!("  Wallet UI Image: {}", config.devnet.wallet_ui_image);
    println!("  Node Port: {}", config.devnet.node_port);
    println!("  DA Port: {}", config.devnet.da_port);
    println!("  Indexer Port: {}", config.devnet.indexer_port);
    println!("  Postgres Port: {}", config.devnet.postgres_port);
    println!("  Wallet API Port: {}", config.devnet.wallet_api_port);
    println!("  Wallet WS Port: {}", config.devnet.wallet_ws_port);
    println!("  Wallet UI Port: {}", config.devnet.wallet_ui_port);
    println!("  Auto Start: {}", config.devnet.auto_start);

    // Build configuration
    println!("\n🔨 Build Configuration:");
    println!("  Release Mode: {}", config.build.release);
    if let Some(jobs) = config.build.jobs {
        println!("  Parallel Jobs: {}", jobs);
    }
    if !config.build.extra_flags.is_empty() {
        println!("  Extra Flags: {}", config.build.extra_flags.join(" "));
    }

    // Container environment variables
    if !config.devnet.container_env.node.is_empty()
        || !config.devnet.container_env.indexer.is_empty()
        || !config.devnet.container_env.wallet_server.is_empty()
        || !config.devnet.container_env.wallet_ui.is_empty()
        || !config.devnet.container_env.postgres.is_empty()
    {
        println!("\n🐳 Container Environment Variables:");
        if !config.devnet.container_env.node.is_empty() {
            println!("  Node: {}", config.devnet.container_env.node.join(", "));
        }
        if !config.devnet.container_env.indexer.is_empty() {
            println!("  Indexer: {}", config.devnet.container_env.indexer.join(", "));
        }
        if !config.devnet.container_env.wallet_server.is_empty() {
            println!("  Wallet Server: {}", config.devnet.container_env.wallet_server.join(", "));
        }
        if !config.devnet.container_env.wallet_ui.is_empty() {
            println!("  Wallet UI: {}", config.devnet.container_env.wallet_ui.join(", "));
        }
        if !config.devnet.container_env.postgres.is_empty() {
            println!("  Postgres: {}", config.devnet.container_env.postgres.join(", "));
        }
    }

    // Configuration file path
    if let Ok(config_path) = get_config_path() {
        println!("\n📁 Configuration File:");
        println!("  Path: {}", config_path.display());
    }

    println!("\n{}", "=".repeat(50));

    Ok(())
}

/// Get the configuration file path
fn get_config_path() -> HylixResult<PathBuf> {
    let config_dir = dirs::config_dir()
        .ok_or_else(|| crate::error::HylixError::config("Could not find config directory"))?;

    Ok(config_dir.join("hylix").join("config.toml"))
}

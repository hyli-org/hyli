use crate::config::{BackendType, HylixConfig};
use crate::error::HylixResult;
use crate::logging::{log_info, log_success};
use std::path::PathBuf;

/// Configuration action enumeration
#[derive(Debug, Clone)]
pub enum ConfigAction {
    /// Display current configuration
    Show,
    /// Edit configuration values
    Edit { key: String, value: String },
    /// Reset configuration to defaults
    Reset,
}

/// Execute the `hy config` command
pub async fn execute(action: ConfigAction) -> HylixResult<()> {
    match action {
        ConfigAction::Show => {
            let config = HylixConfig::load()?;
            display_config(&config)?;
        }
        ConfigAction::Edit { key, value } => {
            edit_config(&key, &value).await?;
        }
        ConfigAction::Reset => {
            reset_config().await?;
        }
    }

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
    println!(
        "  Wallet Server Image: {}",
        config.devnet.wallet_server_image
    );
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
            println!(
                "  Indexer: {}",
                config.devnet.container_env.indexer.join(", ")
            );
        }
        if !config.devnet.container_env.wallet_server.is_empty() {
            println!(
                "  Wallet Server: {}",
                config.devnet.container_env.wallet_server.join(", ")
            );
        }
        if !config.devnet.container_env.wallet_ui.is_empty() {
            println!(
                "  Wallet UI: {}",
                config.devnet.container_env.wallet_ui.join(", ")
            );
        }
        if !config.devnet.container_env.postgres.is_empty() {
            println!(
                "  Postgres: {}",
                config.devnet.container_env.postgres.join(", ")
            );
        }
    }

    // Test configuration
    println!("\n🧪 Test Configuration:");
    println!("  Print Server Logs: {}", config.test.print_server_logs);
    println!("  Clean Server Data: {}", config.test.clean_server_data);

    // Run configuration
    println!("\n🏃 Run Configuration:");
    println!("  Clean Server Data: {}", config.run.clean_server_data);
    println!("  Server Port: {}", config.run.server_port);

    // Configuration file path
    if let Ok(config_path) = get_config_path() {
        println!("\n📁 Configuration File:");
        println!("  Path: {}", config_path.display());
    }

    println!("\n{}", "=".repeat(50));
    println!("\n💡 Usage Examples:");
    println!("  hy config edit default_backend risc0");
    println!("  hy config edit devnet.node_port 4321");
    println!("  hy config edit build.release true");
    println!("  hy config reset");

    Ok(())
}

/// Get the configuration file path
fn get_config_path() -> HylixResult<PathBuf> {
    let config_dir = dirs::config_dir()
        .ok_or_else(|| crate::error::HylixError::config("Could not find config directory"))?;

    Ok(config_dir.join("hylix").join("config.toml"))
}

/// Edit a configuration value
async fn edit_config(key: &str, value: &str) -> HylixResult<()> {
    let mut config = HylixConfig::load()?;

    // Parse and set the configuration value
    set_config_value(&mut config, key, value)?;

    // Save the updated configuration
    config.save()?;

    log_success(&format!("Configuration updated: {} = {}", key, value));
    Ok(())
}

/// Reset configuration to defaults
async fn reset_config() -> HylixResult<()> {
    // Backup the current configuration
    HylixConfig::backup()?;
    log_info("Resetting configuration to defaults...");

    // Reset to defaults
    let default_config = HylixConfig::default();
    default_config.save()?;

    log_success("Configuration reset to defaults");
    Ok(())
}

/// Set a configuration value using a dot-notation key
fn set_config_value(config: &mut HylixConfig, key: &str, value: &str) -> HylixResult<()> {
    match key {
        // General settings
        "default_backend" => {
            let backend = match value.to_lowercase().as_str() {
                "sp1" => BackendType::Sp1,
                "risc0" => BackendType::Risc0,
                _ => {
                    return Err(crate::error::HylixError::config(
                        "Invalid backend type. Must be 'sp1' or 'risc0'",
                    ))
                }
            };
            config.default_backend = backend;
        }
        "scaffold_repo" => {
            config.scaffold_repo = value.to_string();
        }
        "bake_profile" => {
            config.bake_profile = value.to_string();
        }

        // Devnet settings
        "devnet.node_image" => {
            config.devnet.node_image = value.to_string();
        }
        "devnet.node_rust_log" => {
            config.devnet.node_rust_log = value.to_string();
        }
        "devnet.wallet_server_image" => {
            config.devnet.wallet_server_image = value.to_string();
        }
        "devnet.wallet_ui_image" => {
            config.devnet.wallet_ui_image = value.to_string();
        }
        "devnet.node_port" => {
            config.devnet.node_port = value
                .parse()
                .map_err(|_| crate::error::HylixError::config("Invalid port number"))?;
        }
        "devnet.da_port" => {
            config.devnet.da_port = value
                .parse()
                .map_err(|_| crate::error::HylixError::config("Invalid port number"))?;
        }
        "devnet.indexer_port" => {
            config.devnet.indexer_port = value
                .parse()
                .map_err(|_| crate::error::HylixError::config("Invalid port number"))?;
        }
        "devnet.postgres_port" => {
            config.devnet.postgres_port = value
                .parse()
                .map_err(|_| crate::error::HylixError::config("Invalid port number"))?;
        }
        "devnet.wallet_api_port" => {
            config.devnet.wallet_api_port = value
                .parse()
                .map_err(|_| crate::error::HylixError::config("Invalid port number"))?;
        }
        "devnet.wallet_ws_port" => {
            config.devnet.wallet_ws_port = value
                .parse()
                .map_err(|_| crate::error::HylixError::config("Invalid port number"))?;
        }
        "devnet.wallet_ui_port" => {
            config.devnet.wallet_ui_port = value
                .parse()
                .map_err(|_| crate::error::HylixError::config("Invalid port number"))?;
        }
        "devnet.auto_start" => {
            config.devnet.auto_start = value.parse().map_err(|_| {
                crate::error::HylixError::config("Invalid boolean value. Use 'true' or 'false'")
            })?;
        }

        // Build settings
        "build.release" => {
            config.build.release = value.parse().map_err(|_| {
                crate::error::HylixError::config("Invalid boolean value. Use 'true' or 'false'")
            })?;
        }
        "build.jobs" => {
            if value == "null" || value.is_empty() {
                config.build.jobs = None;
            } else {
                config.build.jobs = Some(
                    value
                        .parse()
                        .map_err(|_| crate::error::HylixError::config("Invalid number of jobs"))?,
                );
            }
        }

        // Test settings
        "test.print_server_logs" => {
            config.test.print_server_logs = value.parse().map_err(|_| {
                crate::error::HylixError::config("Invalid boolean value. Use 'true' or 'false'")
            })?;
        }
        "test.clean_server_data" => {
            config.test.clean_server_data = value.parse().map_err(|_| {
                crate::error::HylixError::config("Invalid boolean value. Use 'true' or 'false'")
            })?;
        }

        // Run settings
        "run.clean_server_data" => {
            config.run.clean_server_data = value.parse().map_err(|_| {
                crate::error::HylixError::config("Invalid boolean value. Use 'true' or 'false'")
            })?;
        }
        "run.server_port" => {
            config.run.server_port = value
                .parse()
                .map_err(|_| crate::error::HylixError::config("Invalid port number"))?;
        }

        _ => {
            return Err(crate::error::HylixError::config(format!(
                "Unknown configuration key: {}. Use 'hy config show' to see available keys",
                key
            )));
        }
    }

    Ok(())
}

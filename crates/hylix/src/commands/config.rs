use crate::config::{
    BackendType, BuildConfig, ContainerEnvConfig, DevnetConfig, HylixConfig, RunConfig, TestConfig,
};
use crate::error::HylixResult;
use crate::logging::{log_info, log_success};
use std::path::PathBuf;

/// Configuration action enumeration
#[derive(Debug, Clone, clap::Subcommand)]
pub enum ConfigAction {
    /// Display current configuration
    Show,
    /// Edit configuration values
    #[command(alias = "set")]
    Edit {
        /// Configuration key to edit (e.g., "default_backend", "devnet.node_port")
        key: String,
        /// New value for the configuration key
        value: String,
    },
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
    const W: usize = 34;

    // Destructure everything so the compiler errors if a field is added but not displayed.
    let HylixConfig {
        version: _,
        default_backend,
        scaffold_repo,
        bake_profile,
        devnet:
            DevnetConfig {
                node_image,
                wallet_server_image,
                wallet_ui_image,
                registry_server_image,
                registry_ui_image,
                node_port,
                da_port,
                node_rust_log,
                wallet_api_port,
                wallet_ws_port,
                wallet_ui_port,
                indexer_port,
                postgres_port,
                registry_server_port,
                registry_ui_port,
                auto_start,
                container_env:
                    ContainerEnvConfig {
                        node: env_node,
                        indexer: env_indexer,
                        wallet_server: env_wallet_server,
                        wallet_ui: env_wallet_ui,
                        postgres: env_postgres,
                        registry_server: env_registry_server,
                    },
            },
        build:
            BuildConfig {
                release,
                jobs,
                extra_flags,
            },
        test:
            TestConfig {
                print_server_logs,
                clean_server_data: test_clean,
            },
        run:
            RunConfig {
                clean_server_data: run_clean,
                server_port,
            },
    } = config;

    println!("\n Hylix Configuration");
    println!("{}", "=".repeat(60));
    println!(" Edit any value with: hy config edit <key> <value>\n");

    // General settings
    println!(" General Settings:");
    println!("  {:W$} = {default_backend:?}", "default_backend");
    println!("  {:W$} = {scaffold_repo}", "scaffold_repo");
    println!("  {:W$} = {bake_profile}", "bake_profile");

    // Devnet configuration
    println!("\n Devnet Configuration:");
    println!("  {:W$} = {node_image}", "devnet.node_image");
    println!("  {:W$} = {wallet_server_image}", "devnet.wallet_server_image");
    println!("  {:W$} = {wallet_ui_image}", "devnet.wallet_ui_image");
    println!("  {:W$} = {registry_server_image}", "devnet.registry_server_image");
    println!("  {:W$} = {registry_ui_image}", "devnet.registry_ui_image");
    println!("  {:W$} = {node_rust_log}", "devnet.node_rust_log");
    println!("  {:W$} = {node_port}", "devnet.node_port");
    println!("  {:W$} = {da_port}", "devnet.da_port");
    println!("  {:W$} = {indexer_port}", "devnet.indexer_port");
    println!("  {:W$} = {postgres_port}", "devnet.postgres_port");
    println!("  {:W$} = {wallet_api_port}", "devnet.wallet_api_port");
    println!("  {:W$} = {wallet_ws_port}", "devnet.wallet_ws_port");
    println!("  {:W$} = {wallet_ui_port}", "devnet.wallet_ui_port");
    println!("  {:W$} = {registry_server_port}", "devnet.registry_server_port");
    println!("  {:W$} = {registry_ui_port}", "devnet.registry_ui_port");
    println!("  {:W$} = {auto_start}", "devnet.auto_start");

    // Build configuration
    println!("\n Build Configuration:");
    println!("  {:W$} = {release}", "build.release");
    println!(
        "  {:W$} = {}",
        "build.jobs",
        match jobs {
            Some(j) => j.to_string(),
            None => "(unset)".to_string(),
        }
    );
    if !extra_flags.is_empty() {
        println!(
            "  {:W$} = {}",
            "build.extra_flags",
            extra_flags.join(" ")
        );
    }

    // Container environment variables (read-only, edited via TOML)
    let has_env = !env_node.is_empty()
        || !env_indexer.is_empty()
        || !env_wallet_server.is_empty()
        || !env_wallet_ui.is_empty()
        || !env_postgres.is_empty()
        || !env_registry_server.is_empty();
    if has_env {
        println!("\n Container Environment Variables (edit in config.toml):");
        if !env_node.is_empty() {
            println!("  {:W$} = {}", "devnet.container_env.node", env_node.join(", "));
        }
        if !env_indexer.is_empty() {
            println!("  {:W$} = {}", "devnet.container_env.indexer", env_indexer.join(", "));
        }
        if !env_wallet_server.is_empty() {
            println!("  {:W$} = {}", "devnet.container_env.wallet_server", env_wallet_server.join(", "));
        }
        if !env_wallet_ui.is_empty() {
            println!("  {:W$} = {}", "devnet.container_env.wallet_ui", env_wallet_ui.join(", "));
        }
        if !env_postgres.is_empty() {
            println!("  {:W$} = {}", "devnet.container_env.postgres", env_postgres.join(", "));
        }
        if !env_registry_server.is_empty() {
            println!("  {:W$} = {}", "devnet.container_env.registry_server", env_registry_server.join(", "));
        }
    }

    // Test configuration
    println!("\n Test Configuration:");
    println!("  {:W$} = {print_server_logs}", "test.print_server_logs");
    println!("  {:W$} = {test_clean}", "test.clean_server_data");

    // Run configuration
    println!("\n Run Configuration:");
    println!("  {:W$} = {run_clean}", "run.clean_server_data");
    println!("  {:W$} = {server_port}", "run.server_port");

    // Configuration file path
    if let Ok(config_path) = get_config_path() {
        println!("\n Config file: {}", config_path.display());
    }

    println!();
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

    log_success(&format!("Configuration updated: {key} = {value}"));
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
        "devnet.registry_server_image" => {
            config.devnet.registry_server_image = value.to_string();
        }
        "devnet.registry_ui_image" => {
            config.devnet.registry_ui_image = value.to_string();
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
        "devnet.registry_server_port" => {
            config.devnet.registry_server_port = value
                .parse()
                .map_err(|_| crate::error::HylixError::config("Invalid port number"))?;
        }
        "devnet.registry_ui_port" => {
            config.devnet.registry_ui_port = value
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
                "Unknown configuration key: {key}. Use 'hy config show' to see available keys"
            )));
        }
    }

    Ok(())
}

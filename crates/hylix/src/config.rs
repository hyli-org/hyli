use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

use crate::logging::{log_error, log_info};

/// Default configuration version
fn default_config_version() -> String {
    "0.6.0".to_string()
}

/// Hylix configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HylixConfig {
    /// Configuration schema version
    #[serde(default = "default_config_version")]
    pub version: String,
    /// Default backend type for new projects
    pub default_backend: BackendType,
    /// Default scaffold repository URL
    pub scaffold_repo: String,
    /// Local devnet configuration
    pub devnet: DevnetConfig,
    /// Build configuration
    pub build: BuildConfig,
    /// Bake profile configuration
    pub bake_profile: String,
    /// Testing configuration
    pub test: TestConfig,
    /// Run configuration
    pub run: RunConfig,
}

/// Testing configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestConfig {
    /// Print logs to console
    pub print_server_logs: bool,
    /// Clean data directory before running tests
    pub clean_server_data: bool,
}

/// Run configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunConfig {
    /// Clean data directory before running
    pub clean_server_data: bool,
    /// Server port
    pub server_port: u16,
}

/// Backend type enumeration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, clap::ValueEnum)]
pub enum BackendType {
    Sp1,
    Risc0,
}

/// Devnet configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DevnetConfig {
    /// Custom image for the Hyli node and indexer
    pub node_image: String,
    /// Custom image for the wallet server
    pub wallet_server_image: String,
    /// Custom image for the wallet UI
    pub wallet_ui_image: String,
    /// Default port for the local node
    pub node_port: u16,
    /// Default port for the DA server
    pub da_port: u16,
    /// Default value for node'sRUST_LOG environment variable
    pub node_rust_log: String,
    /// Default port for the wallet app
    pub wallet_api_port: u16,
    /// Default port for the wallet WS
    pub wallet_ws_port: u16,
    /// Default port for the wallet UI
    pub wallet_ui_port: u16,
    /// Default port for the indexer
    pub indexer_port: u16,
    /// Default port for the postgres server
    pub postgres_port: u16,
    /// Auto-start devnet on test command
    pub auto_start: bool,
    /// Custom environment variables for containers
    pub container_env: ContainerEnvConfig,
}

/// Container environment variables configuration
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ContainerEnvConfig {
    /// Custom environment variables for the node container
    pub node: Vec<String>,
    /// Custom environment variables for the indexer container
    pub indexer: Vec<String>,
    /// Custom environment variables for the wallet server container
    pub wallet_server: Vec<String>,
    /// Custom environment variables for the wallet UI container
    pub wallet_ui: Vec<String>,
    /// Custom environment variables for the postgres container
    pub postgres: Vec<String>,
}

/// Build configuration
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct BuildConfig {
    /// Build in release mode by default
    pub release: bool,
    /// Number of parallel build jobs
    pub jobs: Option<u32>,
    /// Additional cargo build flags
    pub extra_flags: Vec<String>,
}

/// Bake profile configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BakeProfile {
    /// Name of the profile
    pub name: String,
    /// Accounts to create
    pub accounts: Vec<AccountConfig>,
    /// Funds to send to accounts
    pub funds: Vec<FundConfig>,
}

/// Account configuration for baking
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountConfig {
    /// Account name
    pub name: String,
    /// Account password
    pub password: String,
    /// Account type (e.g., "vip")
    pub invite_code: String,
}

/// Fund configuration for baking
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FundConfig {
    /// Source account name
    pub from: String,
    /// Source account password
    pub from_password: String,
    /// Amount to send
    pub amount: u64,
    /// Token type (e.g., "oranj", "oxygen")
    pub token: String,
    /// Destination account name
    pub to: String,
}

impl Default for HylixConfig {
    fn default() -> Self {
        Self {
            version: default_config_version(),
            default_backend: BackendType::Risc0,
            scaffold_repo: "https://github.com/hyli-org/app-scaffold".to_string(),
            devnet: DevnetConfig::default(),
            build: BuildConfig::default(),
            bake_profile: "bobalice".to_string(),
            test: TestConfig::default(),
            run: RunConfig::default(),
        }
    }
}

impl Default for DevnetConfig {
    fn default() -> Self {
        Self {
            node_image: "ghcr.io/hyli-org/hyli:0.14.0-rc3".to_string(),
            wallet_server_image: "ghcr.io/hyli-org/wallet/wallet-server:main".to_string(),
            wallet_ui_image: "ghcr.io/hyli-org/wallet/wallet-ui:main".to_string(),
            da_port: 4141,
            node_rust_log: "info".to_string(),
            node_port: 4321,
            indexer_port: 4322,
            postgres_port: 5432,
            wallet_ui_port: 8080,
            wallet_api_port: 4000,
            wallet_ws_port: 8081,
            auto_start: true,
            container_env: ContainerEnvConfig::default(),
        }
    }
}

impl Default for TestConfig {
    fn default() -> Self {
        Self {
            print_server_logs: false,
            clean_server_data: true,
        }
    }
}

impl Default for RunConfig {
    fn default() -> Self {
        Self {
            clean_server_data: false,
            server_port: 9002,
        }
    }
}

impl HylixConfig {
    /// Load configuration from file or create default
    pub fn load() -> crate::error::HylixResult<Self> {
        let config_path = Self::config_path()?;

        if config_path.exists() {
            let content = std::fs::read_to_string(&config_path)?;

            // Parse as TOML value to check version
            let mut toml_value: toml::Value = toml::from_str(&content)
                .map_err(crate::error::HylixError::Toml)
                .with_context(|| {
                    format!("Failed to parse TOML from file {}", config_path.display())
                })?;

            // Check version and migrate if needed
            let file_version = toml_value
                .get("version")
                .and_then(|v| v.as_str())
                .unwrap_or("legacy")
                .to_string();

            let current_version = default_config_version();

            if file_version != current_version {
                log_info(&format!(
                    "Upgrading configuration from version '{}' to '{}'",
                    file_version, current_version
                ));

                // Backup the old config before migration
                Self::backup()?;

                // Migrate the TOML value
                toml_value = Self::migrate_toml(toml_value, file_version)?;

                // Write the migrated config back to file
                let migrated_content = toml::to_string_pretty(&toml_value)?;
                std::fs::write(&config_path, migrated_content)?;

                log_info("Configuration successfully upgraded and saved");
            }

            // Now parse the (possibly migrated) config
            let config: Self = toml::from_str(&toml::to_string(&toml_value)?)
                .map_err(crate::error::HylixError::Toml)
                .with_context(|| {
                    format!(
                        "Failed to load configuration from file {}",
                        config_path.display()
                    )
                })?;

            Ok(config)
        } else {
            let config = Self::default();
            config.save()?;
            log_info(&format!(
                "Created default configuration in file {}",
                config_path.display()
            ));
            Ok(config)
        }
    }

    /// Migrate TOML configuration from previous versions
    fn migrate_toml(
        mut toml_value: toml::Value,
        file_version: String,
    ) -> crate::error::HylixResult<toml::Value> {
        let current_version = default_config_version();

        // Migration from legacy (no version field) to v1
        match file_version.as_str() {
            "legacy" => {
                log_info("Migrating from legacy configuration");

                if let Some(table) = toml_value.as_table_mut() {
                    // Add version field
                    table.insert(
                        "version".to_string(),
                        toml::Value::String(current_version.clone()),
                    );

                    // Add node_rust_log field if devnet section exists
                    if let Some(devnet) = table.get_mut("devnet") {
                        if let Some(devnet_table) = devnet.as_table_mut() {
                            if !devnet_table.contains_key("node_rust_log") {
                                devnet_table.insert(
                                    "node_rust_log".to_string(),
                                    toml::Value::String("info".to_string()),
                                );
                            }
                        }
                    } else {
                        log_error("Devnet section not found in configuration");
                        log_info("Failed to migrate configuration. Please check your configuration file.");
                        log_info(&format!(
                            "You can reset to default configuration by running `{}`",
                            console::style("hy config reset").bold().green()
                        ));
                        return Err(crate::error::HylixError::config(
                            "Devnet section not found in configuration".to_string(),
                        ));
                    }
                }
            }
            _ => {
                log_error(&format!(
                    "Unsupported configuration version: {}",
                    file_version
                ));
                log_info("Failed to migrate configuration. Please check your configuration file.");
                log_info(&format!(
                    "You can reset to default configuration by running `{}`",
                    console::style("hy config reset").bold().green()
                ));
                return Err(crate::error::HylixError::config(
                    "Unsupported configuration version".to_string(),
                ));
            }
        }

        Ok(toml_value)
    }

    /// Save configuration to file
    pub fn save(&self) -> crate::error::HylixResult<()> {
        let config_path = Self::config_path()?;
        let config_dir = config_path.parent().unwrap();

        std::fs::create_dir_all(config_dir)?;

        let content = toml::to_string_pretty(self)?;
        std::fs::write(&config_path, content)?;

        Ok(())
    }

    /// Backup configuration to file
    pub fn backup() -> crate::error::HylixResult<()> {
        let config_path = Self::config_path()?;
        let config_dir = config_path.parent().unwrap();
        let backup_path = config_dir.join(format!(
            "config.toml.{}.backup",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs()
        ));
        std::fs::copy(&config_path, &backup_path)?;
        log_info(&format!(
            "Backed up configuration to {}",
            backup_path.display()
        ));
        Ok(())
    }

    /// Get the configuration file path
    fn config_path() -> crate::error::HylixResult<PathBuf> {
        let config_dir = dirs::config_dir()
            .ok_or_else(|| crate::error::HylixError::config("Could not find config directory"))?;

        Ok(config_dir.join("hylix").join("config.toml"))
    }

    /// Get the profiles directory path
    fn profiles_dir() -> crate::error::HylixResult<PathBuf> {
        let config_dir = dirs::config_dir()
            .ok_or_else(|| crate::error::HylixError::config("Could not find config directory"))?;

        Ok(config_dir.join("hylix").join("profiles"))
    }

    /// Load a bake profile by name
    pub fn load_bake_profile(&self, profile_name: &str) -> crate::error::HylixResult<BakeProfile> {
        let profiles_dir = Self::profiles_dir()?;
        let profile_path = profiles_dir.join(format!("{}.toml", profile_name));

        if !profile_path.exists() {
            return Err(crate::error::HylixError::config(format!(
                "Profile '{}' not found at {}",
                profile_name,
                profile_path.display()
            )));
        }

        let content = std::fs::read_to_string(&profile_path)?;
        let profile: BakeProfile = toml::from_str(&content)
            .map_err(crate::error::HylixError::Toml)
            .with_context(|| {
                format!(
                    "Failed to load profile from file {}",
                    profile_path.display()
                )
            })?;

        log_info(&format!(
            "Loaded profile '{}' from {}",
            profile_name,
            profile_path.display()
        ));

        Ok(profile)
    }

    /// Create default bobalice profile if it doesn't exist
    pub fn create_default_profile(&self) -> crate::error::HylixResult<()> {
        let profiles_dir = Self::profiles_dir()?;
        std::fs::create_dir_all(&profiles_dir)?;

        let profile_path = profiles_dir.join("bobalice.toml");

        if !profile_path.exists() {
            let default_profile = BakeProfile {
                name: "bobalice".to_string(),
                accounts: vec![
                    AccountConfig {
                        name: "bob".to_string(),
                        password: "hylisecure".to_string(),
                        invite_code: "vip".to_string(),
                    },
                    AccountConfig {
                        name: "alice".to_string(),
                        password: "hylisecure".to_string(),
                        invite_code: "vip".to_string(),
                    },
                ],
                funds: vec![
                    FundConfig {
                        from: "hyli".to_string(),
                        from_password: "hylisecure".to_string(),
                        amount: 1000,
                        token: "oranj".to_string(),
                        to: "bob".to_string(),
                    },
                    FundConfig {
                        from: "hyli".to_string(),
                        from_password: "hylisecure".to_string(),
                        amount: 1000,
                        token: "oranj".to_string(),
                        to: "alice".to_string(),
                    },
                    FundConfig {
                        from: "hyli".to_string(),
                        from_password: "hylisecure".to_string(),
                        amount: 500,
                        token: "oxygen".to_string(),
                        to: "bob".to_string(),
                    },
                    FundConfig {
                        from: "bob".to_string(),
                        from_password: "hylisecure".to_string(),
                        amount: 50,
                        token: "oxygen".to_string(),
                        to: "alice".to_string(),
                    },
                ],
            };

            let content = toml::to_string_pretty(&default_profile)?;
            std::fs::write(&profile_path, content)?;

            log_info(&format!(
                "Created default bobalice profile at {}",
                profile_path.display()
            ));
        }

        Ok(())
    }
}

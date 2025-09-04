use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

use crate::logging::log_info;

/// Hylix configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HylixConfig {
    /// Default backend type for new projects
    pub default_backend: BackendType,
    /// Default scaffold repository URL
    pub scaffold_repo: String,
    /// Local devnet configuration
    pub devnet: DevnetConfig,
    /// Build configuration
    pub build: BuildConfig,
    /// Bake profile configuration
    pub bake_profile: String 
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
#[derive(Debug, Clone, Serialize, Deserialize)]
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

impl Default for ContainerEnvConfig {
    fn default() -> Self {
        Self {
            node: Vec::new(),
            indexer: Vec::new(),
            wallet_server: Vec::new(),
            wallet_ui: Vec::new(),
            postgres: Vec::new(),
        }
    }
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
            default_backend: BackendType::Sp1,
            scaffold_repo: "https://github.com/hyli-org/app-scaffold".to_string(),
            devnet: DevnetConfig::default(),
            build: BuildConfig::default(),
            bake_profile: "bobalice".to_string(),
        }
    }
}

impl Default for DevnetConfig {
    fn default() -> Self {
        Self {
            node_image: "ghcr.io/hyli-org/hyli:0.14.0-rc1".to_string(),
            wallet_server_image: "ghcr.io/hyli-org/wallet/wallet-server:main".to_string(),
            wallet_ui_image: "ghcr.io/hyli-org/wallet/wallet-ui:main".to_string(),
            da_port: 4141,
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

impl HylixConfig {
    /// Load configuration from file or create default
    pub fn load() -> crate::error::HylixResult<Self> {
        let config_path = Self::config_path()?;

        if config_path.exists() {
            let content = std::fs::read_to_string(&config_path)?;
            let config: Self = toml::from_str(&content)
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

    /// Save configuration to file
    pub fn save(&self) -> crate::error::HylixResult<()> {
        let config_path = Self::config_path()?;
        let config_dir = config_path.parent().unwrap();

        std::fs::create_dir_all(config_dir)?;

        let content = toml::to_string_pretty(self)?;
        std::fs::write(&config_path, content)?;

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

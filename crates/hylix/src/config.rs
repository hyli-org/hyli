use serde::{Deserialize, Serialize};
use std::path::PathBuf;

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
    /// Default port for the local node
    pub node_port: u16,
    /// Default port for the explorer
    pub explorer_port: u16,
    /// Default port for the indexer
    pub indexer_port: u16,
    /// Auto-start devnet on test command
    pub auto_start: bool,
}

/// Build configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BuildConfig {
    /// Build in release mode by default
    pub release: bool,
    /// Number of parallel build jobs
    pub jobs: Option<u32>,
    /// Additional cargo build flags
    pub extra_flags: Vec<String>,
}

impl Default for HylixConfig {
    fn default() -> Self {
        Self {
            default_backend: BackendType::Sp1,
            scaffold_repo: "https://github.com/hyli-org/app-scaffold".to_string(),
            devnet: DevnetConfig::default(),
            build: BuildConfig::default(),
        }
    }
}

impl Default for DevnetConfig {
    fn default() -> Self {
        Self {
            node_port: 8080,
            explorer_port: 3000,
            indexer_port: 8081,
            auto_start: true,
        }
    }
}

impl Default for BuildConfig {
    fn default() -> Self {
        Self {
            release: false,
            jobs: None,
            extra_flags: Vec::new(),
        }
    }
}

impl HylixConfig {
    /// Load configuration from file or create default
    pub fn load() -> crate::error::HylixResult<Self> {
        let config_path = Self::config_path()?;
        
        if config_path.exists() {
            let content = std::fs::read_to_string(&config_path)?;
            let config: Self = toml::from_str(&content)?;
            Ok(config)
        } else {
            let config = Self::default();
            config.save()?;
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
}

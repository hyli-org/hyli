use thiserror::Error;

/// Main error type for Hylix CLI
#[derive(Error, Debug)]
pub enum HylixError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("JSON serialization error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("TOML parsing error: {0}")]
    Toml(#[from] toml::de::Error),

    #[error("TOML serialization error: {0}")]
    TomlSer(#[from] toml::ser::Error),

    #[error("Process execution error: {0}")]
    Process(String),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Project error: {0}")]
    Project(String),

    #[error("Network error: {0}")]
    Network(String),

    #[error("Build error: {0}")]
    Build(String),

    #[error("Test error: {0}")]
    Test(String),

    #[error("Devnet error: {0}")]
    Devnet(String),

    #[error("Backend error: {0}")]
    Backend(String),

    #[error("Validation error: {0}")]
    Validation(String),

    #[error("Client SDK error: {0}")]
    ClientSdk(#[from] anyhow::Error),
}

#[allow(dead_code)]
impl HylixError {
    /// Create a new process execution error
    pub fn process(msg: impl Into<String>) -> Self {
        Self::Process(msg.into())
    }

    /// Create a new configuration error
    pub fn config(msg: impl Into<String>) -> Self {
        Self::Config(msg.into())
    }

    /// Create a new project error
    pub fn project(msg: impl Into<String>) -> Self {
        Self::Project(msg.into())
    }

    /// Create a new network error
    pub fn network(msg: impl Into<String>) -> Self {
        Self::Network(msg.into())
    }

    /// Create a new build error
    pub fn build(msg: impl Into<String>) -> Self {
        Self::Build(msg.into())
    }

    /// Create a new test error
    pub fn test(msg: impl Into<String>) -> Self {
        Self::Test(msg.into())
    }

    /// Create a new devnet error
    pub fn devnet(msg: impl Into<String>) -> Self {
        Self::Devnet(msg.into())
    }

    /// Create a new backend error
    pub fn backend(msg: impl Into<String>) -> Self {
        Self::Backend(msg.into())
    }

    /// Create a new validation error
    pub fn validation(msg: impl Into<String>) -> Self {
        Self::Validation(msg.into())
    }
}

/// Result type alias for Hylix operations
pub type HylixResult<T> = Result<T, HylixError>;

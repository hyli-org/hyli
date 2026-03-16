use anyhow::Result;
use clap::Parser;
use config::{Config, Environment, File, FileFormat};
use serde::Deserialize;

/// CLI arguments — only meta-arguments + overrides live here.
/// Everything else comes from the config file or environment variables.
#[derive(Parser, Debug)]
#[command(name = "bridge-server", about = "Hyperlane Bridge Server for Hyli")]
pub struct Args {
    /// Path(s) to a TOML config file (can be repeated; later files override earlier ones)
    #[arg(long, default_value = "bridge-server.toml")]
    pub config_file: Vec<String>,

    /// Override data directory
    #[arg(long)]
    pub data_directory: Option<String>,
}

/// Fully-resolved configuration for the bridge server.
#[derive(Debug, Deserialize, Clone)]
pub struct Conf {
    pub node_url: String,
    /// JSON-RPC proxy port (Hyperlane agents point here)
    pub rpc_port: u16,
    /// REST API port for health/info endpoints
    pub rest_port: u16,
    /// Log format: "plain", "json", or "node"
    pub log_format: String,
    /// Domain ID shown to Hyperlane agents
    pub hyli_chain_id: u64,
    /// 32-byte hex state root of the Ethereum chain with Hyperlane contracts deployed
    pub eth_state_root: String,
    pub bridge_cn: String,
    pub hyperlane_cn: String,
    pub token_cn: String,
    /// Hex-encoded secp256k1 private key used to sign Hyli transactions (relayer path)
    pub relayer_key: Option<String>,
    /// Directory name to store module state
    pub data_directory: String,
    /// Skip contract deployment on startup
    pub noinit: bool,
}

impl Conf {
    pub fn new(config_files: Vec<String>, data_directory: Option<String>) -> Result<Self> {
        let mut builder = Config::builder().add_source(File::from_str(
            include_str!("conf_defaults.toml"),
            FileFormat::Toml,
        ));

        for file in config_files {
            builder = builder.add_source(File::with_name(&file).required(false));
        }

        let conf: Self = builder
            .add_source(
                Environment::with_prefix("BRIDGE")
                    .separator("__")
                    .prefix_separator("_")
                    .try_parsing(true),
            )
            .set_override_option("data_directory", data_directory)?
            .build()?
            .try_deserialize()?;

        Ok(conf)
    }
}

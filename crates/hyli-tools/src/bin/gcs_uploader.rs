use std::{path::PathBuf, sync::Arc};

use anyhow::{Context, Result};
use clap::Parser;

use hyli_modules::{
    bus::{SharedMessageBus, metrics::BusMetrics},
    modules::{
        ModulesHandler,
        block_processor::BusOnlyProcessor,
        da_listener::DAListenerConf,
        da_listener::SignedDAListener,
        gcs_uploader::{GCSConf, GcsUploader, GcsUploaderCtx},
    },
    utils::logger::setup_tracing,
};
use serde::{Deserialize, Serialize};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Args {
    #[arg(long, default_value = "config.toml")]
    pub config_file: Vec<String>,
}

pub type SharedConf = Arc<GcsUploaderCtx>;

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let config = Conf::new(args.config_file).context("reading config file")?;

    let node_name = "gcs_block_uploader".to_string();

    setup_tracing(&config.log_format, node_name.clone())?;

    std::fs::create_dir_all(&config.data_directory).context("creating data directory")?;

    tracing::info!("Starting GCS block uploader");

    let bus = SharedMessageBus::new(BusMetrics::global("gcs_block_uploader".to_string()));

    tracing::info!("Setting up modules");

    // Initialize modules
    let mut handler = ModulesHandler::new(&bus, config.data_directory.clone()).await;

    let (last_uploaded_height, genesis_timestamp_folder) =
        GcsUploader::get_last_uploaded_block(&config.gcs).await?;

    handler
        .build_module::<SignedDAListener<BusOnlyProcessor>>(DAListenerConf {
            data_directory: config.data_directory.clone(),
            da_read_from: config.da_read_from.clone(),
            start_block: Some(last_uploaded_height),
            timeout_client_secs: 10,
            processor_config: (),
        })
        .await?;

    handler
        .build_module::<GcsUploader>(GcsUploaderCtx {
            gcs_config: config.gcs.clone(),
            data_directory: config.data_directory.clone(),
            node_name,
            last_uploaded_height,
            genesis_timestamp_folder,
        })
        .await?;

    tracing::info!("Starting modules");

    // Run forever
    handler.start_modules().await?;
    handler.exit_process().await?;

    Ok(())
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct Conf {
    /// The log format to use - "json", "node" or "full" (default)
    pub log_format: String,

    /// Directory name to store node state.
    pub data_directory: PathBuf,

    /// URL to connect to.
    pub da_read_from: String,

    pub gcs: GCSConf,
}

impl Conf {
    pub fn new(config_files: Vec<String>) -> Result<Self, anyhow::Error> {
        let mut s = config::Config::builder().add_source(config::File::from_str(
            include_str!("gcs_conf_defaults.toml"),
            config::FileFormat::Toml,
        ));
        // Priority order: config file, then environment variables, then CLI
        for config_file in config_files {
            s = s.add_source(config::File::with_name(&config_file).required(false));
        }
        let conf: Self = s
            .add_source(
                config::Environment::with_prefix("hyli")
                    .separator("__")
                    .prefix_separator("_"),
            )
            .build()?
            .try_deserialize()?;
        Ok(conf)
    }
}

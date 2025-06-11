use std::sync::Arc;

use anyhow::{Context, Result};
use clap::{Parser, command};

use hyle_contract_sdk::BlockHeight;
use hyle_modules::{
    bus::{SharedMessageBus, metrics::BusMetrics},
    modules::ModulesHandler,
    utils::logger::setup_tracing,
};
use hyli_tools::gcs_block_uploader::GcsBlockUploaderCtx;
use hyli_tools::gcs_block_uploader::{Conf, GcsBlockUploader};
use hyli_tools::signed_da_listener::DAListenerConf;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Args {
    #[arg(long, default_value = "config.toml")]
    pub config_file: Vec<String>,
}

pub type SharedConf = Arc<Conf>;

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let config = Conf::new(args.config_file).context("reading config file")?;

    setup_tracing(&config.log_format, "gcs block uploader".to_string())?;

    tracing::info!("Starting GCS block uploader");

    let bus = SharedMessageBus::new(BusMetrics::global("gcs_block_uploader".to_string()));

    tracing::info!("Setting up modules");

    // Initialize modules
    let mut handler = ModulesHandler::new(&bus).await;

    handler
        .build_module::<hyli_tools::signed_da_listener::DAListener>(DAListenerConf {
            data_directory: config.data_directory.clone(),
            da_read_from: config.da_read_from.clone(),
            start_block: Some(BlockHeight(0)),
        })
        .await?;

    handler
        .build_module::<GcsBlockUploader>(GcsBlockUploaderCtx { config })
        .await?;

    tracing::info!("Starting modules");

    // Run forever
    handler.start_modules().await?;
    handler.exit_process().await?;

    Ok(())
}

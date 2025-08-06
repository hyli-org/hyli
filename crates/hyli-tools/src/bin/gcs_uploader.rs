use std::sync::Arc;

use anyhow::{Context, Result};
use clap::{command, Parser};

use hyle_contract_sdk::BlockHeight;
use hyle_modules::{
    bus::{metrics::BusMetrics, SharedMessageBus},
    modules::{
        da_listener::DAListenerConf,
        gcs_uploader::{GcsUploader, GcsUploaderCtx},
        signed_da_listener::SignedDAListener,
        ModulesHandler,
    },
    utils::{conf::Conf, logger::setup_tracing},
};

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
    let config = Conf::new(args.config_file, None, None).context("reading config file")?;

    setup_tracing(&config.log_format, "gcs block uploader".to_string())?;

    tracing::info!("Starting GCS block uploader");

    let bus = SharedMessageBus::new(BusMetrics::global("gcs_block_uploader".to_string()));

    tracing::info!("Setting up modules");

    // Initialize modules
    let mut handler = ModulesHandler::new(&bus).await;

    handler
        .build_module::<SignedDAListener>(DAListenerConf {
            data_directory: config.data_directory.clone(),
            da_read_from: config.da_read_from.clone(),
            start_block: Some(BlockHeight(0)),
            timeout_client_secs: 10,
        })
        .await?;

    handler
        .build_module::<GcsUploader>(GcsUploaderCtx {
            gcs_config: config.gcs.clone(),
            data_directory: config.data_directory.clone(),
        })
        .await?;

    tracing::info!("Starting modules");

    // Run forever
    handler.start_modules().await?;
    handler.exit_process().await?;

    Ok(())
}

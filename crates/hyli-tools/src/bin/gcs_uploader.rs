use std::{path::PathBuf, sync::Arc};

use anyhow::{Context, Result};
use clap::Parser;

use hyli_modules::{
    bus::{SharedMessageBus, metrics::BusMetrics},
    modules::{
        ModulesHandler,
        da_listener::DAListenerConf,
        da_listener::SignedDAListener,
        gcs_uploader::{
            GCSConf, GcsStorageBackend, GcsUploader, GcsUploaderCtx, LocalStorageBackend,
            StorageBackend,
        },
    },
    utils::logger::setup_otlp,
};
use serde::{Deserialize, Serialize};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Args {
    #[arg(long, default_value = "config.toml")]
    pub config_file: Vec<String>,

    #[arg(long, default_value = "false")]
    pub tracing: bool,

    /// Run audit mode to check for missing blocks in storage
    #[arg(long, default_value = "false")]
    pub audit: bool,
}

pub type SharedConf = Arc<GcsUploaderCtx>;

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let config = Conf::new(args.config_file).context("reading config file")?;

    setup_otlp(
        &config.log_format,
        "gcs block uploader".to_string(),
        args.tracing,
    )?;

    // Run audit mode if requested
    if args.audit {
        tracing::info!("Running storage audit");
        return run_audit(&config).await;
    }

    tracing::info!("Starting GCS block uploader");

    let bus = SharedMessageBus::new(BusMetrics::global("gcs_block_uploader".to_string()));

    tracing::info!("Setting up modules");

    // Initialize modules
    let mut handler = ModulesHandler::new(&bus).await;

    let start_block = GcsUploader::read_last_uploaded_height(&config.data_directory);

    handler
        .build_module::<SignedDAListener>(DAListenerConf {
            data_directory: config.data_directory.clone(),
            da_read_from: config.da_read_from.clone(),
            start_block: Some(start_block),
            timeout_client_secs: 10,
            da_fallback_addresses: vec![],
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

async fn run_audit(config: &Conf) -> Result<()> {
    tracing::info!("Creating storage backend for audit");

    // Create storage backend based on config
    let storage: Box<dyn StorageBackend> = match config.gcs.storage_backend.as_str() {
        "local" => {
            tracing::info!("Using local storage backend");
            Box::new(LocalStorageBackend::new(
                config.data_directory.join("gcs_storage"),
            )?)
        }
        "gcs" => {
            tracing::info!("Using GCS storage backend");
            Box::new(
                GcsStorageBackend::new(
                    config.gcs.gcs_bucket.clone(),
                    config.gcs.gcs_prefix.clone(),
                )
                .await?,
            )
        }
        _ => {
            return Err(anyhow::anyhow!(
                "Unknown storage backend: {}",
                config.gcs.storage_backend
            ));
        }
    };

    tracing::info!(
        "Running audit on storage with prefix: {}",
        config.gcs.gcs_prefix
    );

    // Run audit
    let result = storage.audit_storage(&config.gcs.gcs_prefix).await?;

    // Print report
    result.print_report();

    tracing::info!("Audit complete");

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

use std::{path::PathBuf, sync::Arc};

use anyhow::{Context, Result};
use clap::{Parser, command};

use client_sdk::{
    contract_indexer::utoipa::OpenApi, helpers::risc0::Risc0Prover, rest_client::NodeApiHttpClient,
};
use hyle_contract_sdk::api::NodeInfo;
use hyle_modules::{
    bus::{SharedMessageBus, metrics::BusMetrics},
    modules::{
        BuildApiContextInner, ModulesHandler,
        admin::{AdminApi, AdminApiRunContext},
        da_listener::{DAListener, DAListenerConf},
        prover::{AutoProver, AutoProverCtx},
        rest::{ApiDoc, RestApi, RestApiRunContext, Router},
    },
    utils::logger::setup_tracing,
};
use prometheus::Registry;
use serde::{Deserialize, Serialize};
use smt_token::client::tx_executor_handler::SmtTokenProvableState;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Args {
    #[arg(long, default_value = "config.toml")]
    pub config_file: Vec<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let config = Conf::new(args.config_file).context("reading config file")?;

    setup_tracing(&config.log_format, "smt auto prover".to_string())?;

    std::fs::create_dir_all(&config.data_directory).context("creating data directory")?;

    tracing::info!("Starting smt auto prover");

    let bus = SharedMessageBus::new(BusMetrics::global("smt_auto_prover".to_string()));

    tracing::info!("Setting up modules");

    let registry = Registry::new();
    // Init global metrics meter we expose as an endpoint
    let provider = opentelemetry_sdk::metrics::SdkMeterProvider::builder()
        .with_reader(
            opentelemetry_prometheus::exporter()
                .with_registry(registry.clone())
                .build()
                .context("starting prometheus exporter")?,
        )
        .build();

    opentelemetry::global::set_meter_provider(provider.clone());

    let node_client =
        Arc::new(NodeApiHttpClient::new(config.node_url.clone()).context("build node client")?);

    let build_api_ctx = Arc::new(BuildApiContextInner {
        router: std::sync::Mutex::new(Some(Router::new())),
        openapi: std::sync::Mutex::new(ApiDoc::openapi()),
    });

    // Initialize modules
    let mut handler = ModulesHandler::new(&bus).await;

    handler
        .build_module::<AutoProver<SmtTokenProvableState>>(Arc::new(AutoProverCtx {
            data_directory: config.data_directory.clone(),
            prover: Arc::new(Risc0Prover::new(
                smt_token::client::tx_executor_handler::metadata::SMT_TOKEN_ELF,
            )),
            contract_name: config.contract_name.clone().into(),
            node: node_client.clone(),
            api: Some(build_api_ctx.clone()),
            default_state: Default::default(),
            buffer_blocks: config.buffer_blocks,
            max_txs_per_proof: config.max_txs_per_proof,
            tx_working_window_size: config.tx_working_window_size,
        }))
        .await?;

    handler
        .build_module::<DAListener>(DAListenerConf {
            start_block: None,
            data_directory: config.data_directory.clone(),
            da_read_from: config.da_read_from.clone(),
            timeout_client_secs: 10,
        })
        .await?;

    let router = build_api_ctx
        .router
        .lock()
        .expect("Context router should be available.")
        .take()
        .expect("Context router should be available.");
    let openapi = build_api_ctx
        .openapi
        .lock()
        .expect("OpenAPI should be available")
        .clone();

    if config.run_admin_server {
        handler
            .build_module::<AdminApi>(AdminApiRunContext::new(
                config.admin_server_port,
                Router::new(),
                config.admin_server_max_body_size,
                config.data_directory.clone(),
            ))
            .await?;
    }

    handler
        .build_module::<RestApi>(
            RestApiRunContext::new(
                config.rest_server_port,
                NodeInfo {
                    id: "smt_auto_prover".to_string(),
                    pubkey: None,
                    da_address: config.da_read_from.clone(),
                },
                router,
                config.rest_server_max_body_size,
                openapi,
            )
            .with_registry(registry),
        )
        .await?;

    tracing::info!("Starting modules");

    // Run forever
    handler.start_modules().await?;
    handler.exit_process().await?;

    Ok(())
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
struct Conf {
    /// The log format to use - "json", "node" or "full" (default)
    pub log_format: String,

    /// Directory name to store node state.
    pub data_directory: PathBuf,

    /// URL to connect to.
    pub node_url: String,

    /// URL to connect to.
    pub da_read_from: String,

    pub buffer_blocks: u32,
    pub max_txs_per_proof: usize,
    pub tx_working_window_size: usize,

    /// Contract name to prove
    pub contract_name: String,

    pub rest_server_port: u16,
    pub rest_server_max_body_size: usize,

    pub run_admin_server: bool,
    pub admin_server_port: u16,
    pub admin_server_max_body_size: usize,
}

impl Conf {
    pub fn new(config_files: Vec<String>) -> Result<Self, anyhow::Error> {
        let mut s = config::Config::builder().add_source(config::File::from_str(
            include_str!("../smt_auto_prover_conf_defaults.toml"),
            config::FileFormat::Toml,
        ));
        // Priority order: config file, then environment variables, then CLI
        for config_file in config_files {
            s = s.add_source(config::File::with_name(&config_file).required(false));
        }
        let conf: Self = s
            .add_source(
                config::Environment::with_prefix("hyle")
                    .separator("__")
                    .prefix_separator("_"),
            )
            .build()?
            .try_deserialize()?;
        Ok(conf)
    }
}

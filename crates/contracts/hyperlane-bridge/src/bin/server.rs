use anyhow::{Context, Result};
use axum::Router;
use clap::Parser;
use client_sdk::rest_client::NodeApiHttpClient;
use hyli_hyperlane_bridge::server::{
    conf::{Args, Conf},
    init::init_contracts,
    rpc_proxy::{HylaneRpcProxyCtx, HylaneRpcProxyModule},
};
use hyli_modules::modules::{
    rest::{RestApi, RestApiRunContext},
    BuildApiContextInner, ModulesHandler, ModulesHandlerOptions,
};
use sdk::{api::NodeInfo, ContractName, Identity};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tracing::info;

#[tokio::main]
async fn main() {
    if let Err(e) = actual_main().await {
        eprintln!("bridge-server failed: {e:#}");
        std::process::exit(1);
    }
}

async fn actual_main() -> Result<()> {
    let args = Args::parse();
    let conf = Conf::new(args.config_file, args.data_directory).context("Reading config")?;

    hyli_modules::utils::logger::setup_tracing(&conf.log_format, "bridge-server".to_string())
        .context("Setting up tracing")?;

    info!("Starting Hyperlane Bridge Server");
    info!("  node_url     = {}", conf.node_url);
    info!("  rpc_port     = {}", conf.rpc_port);
    info!("  rest_port    = {}", conf.rest_port);
    info!("  chain_id     = {}", conf.hyli_chain_id);
    info!("  bridge_cn    = {}", conf.bridge_cn);
    info!("  hyperlane_cn = {}", conf.hyperlane_cn);
    info!("  token_cn     = {}", conf.token_cn);
    info!("  data_dir     = {}", conf.data_directory);

    let node_client = Arc::new(
        NodeApiHttpClient::new(conf.node_url.clone()).context("Creating node HTTP client")?,
    );

    // Deploy contracts unless noinit = true
    if !conf.noinit {
        init_contracts(&conf, node_client.clone()).await?;
    }

    // Derive relayer identity from relayer_key (or use default)
    let relayer_identity = parse_relayer_identity(&conf);

    // Set up bus and module handler
    let bus = hyli_modules::bus::SharedMessageBus::new();
    let data_dir = PathBuf::from(&conf.data_directory);
    std::fs::create_dir_all(&data_dir).context("Creating data directory")?;

    let mut handler = ModulesHandler::new(&bus, data_dir, ModulesHandlerOptions::default())?;

    // JSON-RPC proxy module (runs its own server on rpc_port)
    handler
        .build_module::<HylaneRpcProxyModule>(HylaneRpcProxyCtx {
            rpc_port: conf.rpc_port,
            node_url: conf.node_url.clone(),
            hyli_chain_id: conf.hyli_chain_id,
            bridge_cn: ContractName(conf.bridge_cn.clone()),
            hyperlane_cn: ContractName(conf.hyperlane_cn.clone()),
            token_cn: ContractName(conf.token_cn.clone()),
            relayer_identity,
        })
        .await?;

    // Minimal REST API (health / info)
    let build_api_ctx = Arc::new(BuildApiContextInner {
        router: Mutex::new(Some(Router::new())),
        openapi: Default::default(),
    });

    let router = build_api_ctx
        .router
        .lock()
        .expect("router mutex")
        .take()
        .unwrap_or_default();
    let openapi = build_api_ctx.openapi.lock().expect("openapi mutex").clone();

    handler
        .build_module::<RestApi>(RestApiRunContext::new(
            conf.rest_port,
            NodeInfo {
                id: "bridge-server".to_string(),
                pubkey: None,
                da_address: String::new(),
            },
            router,
            10 * 1024 * 1024, // 10 MB
            openapi,
        ))
        .await?;

    _ = handler.start_modules().await;
    handler.exit_process().await?;

    Ok(())
}

fn parse_relayer_identity(conf: &Conf) -> Identity {
    if let Some(key_hex) = &conf.relayer_key {
        let key_hex = key_hex.trim_start_matches("0x");
        if let Ok(key_bytes) = hex::decode(key_hex) {
            if let Ok(sk) = k256::ecdsa::SigningKey::from_slice(&key_bytes) {
                let vk = sk.verifying_key();
                let pk_bytes = vk.to_encoded_point(true);
                let pk_hex = hex::encode(pk_bytes.as_bytes());
                return Identity(format!("0x{pk_hex}@hyperlane-bridge"));
            }
        }
    }
    Identity("relayer@hyperlane-bridge".to_string())
}

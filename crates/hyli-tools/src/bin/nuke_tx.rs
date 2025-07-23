use anyhow::{Context, Result};
use clap::{Parser, command};
use std::collections::BTreeMap;

use hyli_model::{HyliOutput, TxHash};
use hyli_modules::utils::logger::setup_tracing;

use hyli_tools::nuke_tx_module::{Conf, NukeTxModule, NukeTxModuleCtx};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Args {
    #[arg(long, default_value = "config.toml")]
    pub config_file: Vec<String>,
    // List of transaction hashes to nuke
    #[arg(long, required = true, value_delimiter = ',')]
    pub tx_hashes: Vec<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let config = Conf::new(args.config_file).context("reading config file")?;
    setup_tracing(&config.log_format, "nuke_tx".to_string())?;

    tracing::info!("Starting nuke tx tool");

    // Parse transaction hashes
    let mut txs = BTreeMap::new();

    for tx_hash in args.tx_hashes {
        txs.insert(
            TxHash(tx_hash),
            vec![HyliOutput {
                success: false,
                ..Default::default()
            }],
        );
    }

    tracing::info!("Will nuke {} transactions: {:?}", txs.len(), txs.keys());

    // Create message bus
    let bus = hyli_modules::bus::SharedMessageBus::new(
        hyli_modules::bus::metrics::BusMetrics::global("nuke_tx".to_string()),
    );

    // Initialize modules
    let mut handler = hyli_modules::modules::ModulesHandler::new(&bus).await;

    // Add NukeTx module
    handler
        .build_module::<NukeTxModule>(NukeTxModuleCtx { config, txs })
        .await?;

    tracing::info!("Starting modules");

    // Run until all transactions are nuked
    handler.start_modules().await?;
    handler.exit_process().await?;

    Ok(())
}

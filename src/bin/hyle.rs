use anyhow::{Context, Result};
use clap::Parser;
use hyle::{entrypoint::RunPg, utils::conf};
use hyle_crypto::BlstCrypto;
use hyle_modules::{log_error, utils::logger::setup_tracing};
use std::sync::Arc;
use tracing::info;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Args {
    #[arg(short, long, action = clap::ArgAction::SetTrue)]
    pub client: Option<bool>,

    #[arg(long, default_value =  None)]
    pub data_directory: Option<String>,

    #[arg(long)]
    pub run_indexer: Option<bool>,

    #[arg(long, default_value = "config.toml")]
    pub config_file: Vec<String>,

    #[clap(long, action)]
    pub pg: bool,
}

#[cfg(feature = "dhat")]
#[global_allocator]
/// Use dhat to profile memory usage
static ALLOC: dhat::Alloc = dhat::Alloc;

#[cfg(all(feature = "monitoring", not(feature = "dhat")))]
#[global_allocator]
static GLOBAL_ALLOC: alloc_metrics::MetricAlloc<std::alloc::System> =
    alloc_metrics::MetricAlloc::new(std::alloc::System);

// We have some modules that have long-ish tasks, but for now we won't bother giving them
// their own runtime, so to avoid contention we keep a safe number of worker threads
#[tokio::main(worker_threads = 6)]
async fn main() -> Result<()> {
    #[cfg(feature = "dhat")]
    let _profiler = {
        info!("Running with dhat memory profiler");
        dhat::Profiler::new_heap()
    };

    let args = Args::parse();
    let mut config = conf::Conf::new(args.config_file, args.data_directory, args.run_indexer)
        .context("reading config file")?;

    let crypto = Arc::new(BlstCrypto::new(&config.id).context("Could not create crypto")?);
    let pubkey = Some(crypto.validator_pubkey().clone());

    setup_tracing(
        &config.log_format,
        format!(
            "{}({})",
            config.id.clone(),
            pubkey.clone().unwrap_or_default()
        ),
    )?;

    info!("Loaded key {:?} for validator", pubkey);

    let _pg = if args.pg {
        Some(RunPg::new(&mut config).await?)
    } else {
        None
    };

    #[cfg(feature = "sp1")]
    {
        hyle_verifiers::sp1_4::init();
    }

    log_error!(
        hyle::entrypoint::main_process(config, Some(crypto)).await,
        "Error running hyle"
    )?;

    Ok(())
}

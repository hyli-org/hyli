use anyhow::{Context, Result};
use clap::Parser;
use hyli::{entrypoint::RunPg, utils::conf};
use hyli_crypto::BlstCrypto;
use hyli_modules::{log_error, utils::logger::setup_otlp};
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

    #[arg(long, default_value = "false")]
    pub tracing: bool,

    #[clap(long, action)]
    pub pg: bool,
}

#[cfg(feature = "dhat")]
#[global_allocator]
/// Use dhat to profile memory usage
static ALLOC: dhat::Alloc = dhat::Alloc;

#[cfg(all(
    feature = "monitoring",
    not(feature = "dhat"),
    not(feature = "alloc-track")
))]
#[global_allocator]
static GLOBAL_ALLOC: alloc_metrics::MetricAlloc<std::alloc::System> =
    alloc_metrics::MetricAlloc::new(std::alloc::System);

#[cfg(all(
    feature = "alloc-track",
    not(feature = "dhat"),
    not(feature = "monitoring")
))]
#[global_allocator]
static GLOBAL_ALLOC: alloc_track::AllocTrack<std::alloc::System> = alloc_track::AllocTrack::new(
    std::alloc::System,
    alloc_track::BacktraceMode::Backtrace(100, 1024 * 1024),
);

// We have some modules that have long-ish tasks, but for now we won't bother giving them
// their own runtime, so to avoid contention we keep a safe number of worker threads
fn main() {
    eprintln!("Building tokio runtime with LIFO slot disabled");
    let rt = match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(6)
        .disable_lifo_slot()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(err) => {
            eprintln!("Failed to build tokio runtime: {err}");
            std::process::exit(1);
        }
    };

    if let Err(err) = rt.block_on(inner_main()) {
        eprintln!("hyli failed: {err:#}");
        std::process::exit(1);
    }
}
async fn inner_main() -> Result<()> {
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

    setup_otlp(
        &config.log_format,
        format!(
            "{}-{}",
            config.id.clone(),
            pubkey.clone().unwrap_or_default()
        ),
        args.tracing,
    )?;

    info!("Loaded key {:?} for validator", pubkey);

    let _pg = if args.pg {
        Some(RunPg::new(&mut config).await?)
    } else {
        None
    };

    #[cfg(feature = "sp1")]
    {
        hyli_verifiers::sp1_4::init();
    }

    log_error!(
        hyli::entrypoint::main_process(config, Some(crypto)).await,
        "Error running hyli"
    )?;

    #[cfg(feature = "alloc-track")]
    std::fs::write(
        "alloc_report.csv",
        alloc_track::backtrace_report(|_, _| true).csv(),
    )
    .context("writing alloc report")?;

    Ok(())
}

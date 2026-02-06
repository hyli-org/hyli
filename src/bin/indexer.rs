use anyhow::{Context, Result};
use clap::Parser;
use hyli::{
    entrypoint::RunPg,
    utils::conf::{self, P2pMode},
};
use hyli_modules::{log_error, utils::logger::setup_otlp};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Args {
    #[arg(long, default_value = "config.toml")]
    pub config_file: Vec<String>,

    #[clap(long, action)]
    pub pg: bool,

    #[arg(long, default_value = "false")]
    pub tracing: bool,
}

#[cfg(feature = "dhat")]
#[global_allocator]
/// Use dhat to profile memory usage
static ALLOC: dhat::Alloc = dhat::Alloc;

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
        eprintln!("indexer failed: {err:#}");
        std::process::exit(1);
    }
}

async fn inner_main() -> Result<()> {
    #[cfg(feature = "dhat")]
    let _profiler = {
        tracing::info!("Running with dhat memory profiler");
        dhat::Profiler::new_heap()
    };

    let args = Args::parse();
    let mut config =
        conf::Conf::new(args.config_file, None, None).context("reading config file")?;
    // The indexer binary runs none of the consensus/p2p layer
    config.p2p.mode = P2pMode::None;
    // The indexer binary skips the TCP server
    config.run_tcp_server = false;

    setup_otlp(
        &config.log_format,
        format!("{}(nopkey)", config.id.clone()),
        args.tracing,
    )?;

    let _pg = if args.pg {
        Some(RunPg::new(&mut config).await?)
    } else {
        None
    };

    log_error!(
        hyli::entrypoint::main_process(config, None).await,
        "Error running hyli indexer"
    )?;

    Ok(())
}

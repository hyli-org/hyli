use clap::Parser;
use hyle_model::ConsensusInfo;
use hyle_modules::utils::logger::setup_tracing;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Node base URL
    #[arg(long, default_value = "http://localhost:4321")]
    node_base_url: String,

    /// Maximum number of unsettled transactions allowed
    #[arg(long, default_value_t = 2000)]
    max_unsettled_tx: u64,

    /// Maximum age of the last block in seconds
    #[arg(long, default_value_t = 300)]
    max_timestamp_age_secs: u64,

    /// Log format (optional)
    #[arg(long, default_value = "json")]
    log_format: String,
}

fn main() {
    let args = Args::parse();
    setup_tracing(&args.log_format, "gcs block uploader".to_string())
        .expect("Failed to set up tracing");

    let base_url = &args.node_base_url;
    let max_unsettled_tx = args.max_unsettled_tx;
    let max_timestamp_age_secs = args.max_timestamp_age_secs;

    // Check age of last block
    let consensus_info = ureq::get(format!("{}/v1/consensus/info", base_url))
        .header("Accept", "application/json")
        .call()
        .expect("Failed to get consensus info")
        .body_mut()
        .read_json::<ConsensusInfo>()
        .expect("Failed to parse consensus info");

    tracing::debug!("Last timestamp: {}", consensus_info.last_timestamp.0);

    if consensus_info.last_timestamp.0
        > SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("System time error")
            .as_millis()
            + max_timestamp_age_secs as u128 * 1000
    {
        tracing::error!("Invalid last_timestamp in consensus info");
        std::process::exit(1);
    }

    // Check NB of unsettled transactions
    let unsettled_count = ureq::get(format!("{}/v1/unsettled_txs_count", base_url))
        .call()
        .expect("Failed to get unsettled tx count")
        .body_mut()
        .read_json::<u64>()
        .expect("Failed to parse unsettled tx count");

    tracing::debug!("Unsettled transactions count: {}", unsettled_count);

    if unsettled_count > max_unsettled_tx {
        tracing::error!("Too many unsettled txs: {}", unsettled_count);
        std::process::exit(1);
    }

    tracing::debug!("ok");
}

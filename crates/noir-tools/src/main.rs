use std::io::Read;

use tracing::level_filters::LevelFilter;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Layer};

fn main() -> std::io::Result<()> {
    // setup tracing
    let filter = EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .from_env()
        .unwrap();
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_filter(filter))
        .init();

    let args: Vec<String> = std::env::args().collect();
    let Some(proof) = args.get(1) else {
        eprintln!("No Proof filepath provided.");
        return Ok(());
    };

    let Some(vkey) = args.get(2) else {
        eprintln!("No Vkey filepath provided.");
        return Ok(());
    };

    let mut proof = std::fs::File::open(proof)?;
    let mut proof_data = Vec::new();
    proof.read_to_end(&mut proof_data)?;

    let mut vkey = std::fs::File::open(vkey)?;
    let mut vkey_data = Vec::new();
    vkey.read_to_end(&mut vkey_data)?;

    let ho =
        hyle_verifiers::noir_utils::parse_noir_output(&proof_data, &vkey_data).map_err(|e| {
            eprintln!("Error parsing output: {}", e);
            std::io::Error::new(std::io::ErrorKind::InvalidData, e)
        })?;

    println!("Parsed output: {:?}", ho);

    Ok(())
}

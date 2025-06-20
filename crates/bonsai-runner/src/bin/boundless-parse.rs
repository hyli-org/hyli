use std::str::FromStr;

use anyhow::Context;
use boundless_market::{
    alloy::{
        primitives::{utils::parse_ether, Bytes},
        signers::local::PrivateKeySigner,
        transports::http::reqwest::Url,
    },
    client::ClientBuilder,
    deployments::NamedChain,
    storage::storage_provider_from_env,
    Deployment, StorageProvider,
};
use risc0_zkvm::{
    compute_image_id, sha::Digestible, Groth16Receipt, InnerReceipt, Journal, MaybePruned, Receipt,
    ReceiptClaim, SuccinctReceipt,
};
use tracing::{info, Level};

#[tokio::main]
async fn main() {
    // Initialisation de tracing
    tracing_subscriber::fmt()
        .with_max_level(Level::DEBUG)
        .init();

    // let journal: Journal = Journal::new(journal.to_vec());

    // let receipt_claim = ReceiptClaim::ok(image_id, MaybePruned::Pruned(journal.digest()));

    // let claim = ReceiptClaim::ok(image_id, journal.to_vec());
    // let receipt = boundless_client
    //     .set_verifier
    //     .fetch_receipt_with_claim(seal, claim, journal.to_vec())
    //     .await?;

    // let receipt = Receipt::new(
    //     InnerReceipt::Groth16(Groth16Receipt::new(
    //         seal,
    //         MaybePruned::Pruned(journal.digest()),
    //         image_id,
    //     )),
    //     journal.bytes,
    // );
    //
    toto().await.unwrap();
}

async fn toto() -> anyhow::Result<()> {
    // read journal.bin & seal.bin
    let journal_path = "journal.bin";
    let seal_path = "seal.bin";

    let journal = match std::fs::read(journal_path) {
        Ok(data) => data,
        Err(e) => {
            tracing::error!("Failed to read journal: {}", e);
            return Ok(());
        }
    };
    let journal: Bytes =
        bincode::deserialize(journal.as_slice()).expect("Failed to deserialize journal");

    let seal = match std::fs::read(seal_path) {
        Ok(data) => data,
        Err(e) => {
            tracing::error!("Failed to read seal: {}", e);
            return Ok(());
        }
    };

    let seal: Bytes = Bytes::from(seal);

    let elf = include_bytes!("../../../../../wallet/contracts/wallet/wallet.img");
    let image_id = compute_image_id(elf).unwrap();
    let chain_id = std::env::var("BOUNDLESS_CHAIN_ID").unwrap_or("11155111".to_string());
    let offchain = std::env::var("BOUNDLESS_OFFCHAIN").unwrap_or_default() == "true";
    let wallet_private_key = std::env::var("BOUNDLESS_WALLET_PRIVATE_KEY").unwrap_or_default();
    let rpc_url = std::env::var("BOUNDLESS_RPC_URL").unwrap_or_default();

    let min_price_per_mcycle =
        std::env::var("BOUNDLESS_MIN_PRICE_PER_MCYCLE").unwrap_or_else(|_| "0.000001".to_string());
    let max_price_per_mcycle =
        std::env::var("BOUNDLESS_MAX_PRICE_PER_MCYCLE").unwrap_or_else(|_| "0.000005".to_string());
    let timeout = std::env::var("BOUNDLESS_TIMEOUT").unwrap_or_else(|_| "120".to_string());
    let lock_timeout = std::env::var("BOUNDLESS_LOCK_TIMEOUT").unwrap_or_else(|_| "60".to_string());
    let ramp_up_period =
        std::env::var("BOUNDLESS_RAMP_UP_PERIOD").unwrap_or_else(|_| "15".to_string());

    let chain_id: u64 = chain_id.parse()?;
    let min_price_per_mcycle = parse_ether(&min_price_per_mcycle)?;
    let max_price_per_mcycle = parse_ether(&max_price_per_mcycle)?;
    let timeout: u32 = timeout.parse()?;
    let lock_timeout: u32 = lock_timeout.parse()?;
    let ramp_up_period: u32 = ramp_up_period.parse()?;

    // Creates a storage provider based on the environment variables.
    //
    // If the environment variable `RISC0_DEV_MODE` is set, a temporary file storage provider is used.
    // Otherwise, the following environment variables are checked in order:
    // - `PINATA_JWT`, `PINATA_API_URL`, `IPFS_GATEWAY_URL`: Pinata storage provider;
    // - `S3_ACCESS`, `S3_SECRET`, `S3_BUCKET`, `S3_URL`, `AWS_REGION`: S3 storage provider.
    // TODO: gcp storage provider
    let storage_provider = storage_provider_from_env()?;

    // let image_url = storage_provider.upload_program(elf).await?;
    // info!("Uploaded image to {}", image_url);

    let wallet_private_key = PrivateKeySigner::from_str(&wallet_private_key)?;
    let rpc_url = Url::parse(&rpc_url)?;

    let dep = Deployment::from_chain_id(chain_id);

    let mut dep = dep.unwrap();
    dep.chain_id = Some(NamedChain::BaseSepolia as u64);

    info!("Using deployment: {:?}", dep);

    // Create a Boundless client from the provided parameters.
    let boundless_client = ClientBuilder::new()
        .with_rpc_url(rpc_url)
        .with_deployment(dep)
        .with_storage_provider(Some(storage_provider))
        .with_private_key(wallet_private_key)
        .build()
        .await
        .context("failed to build boundless client")?;

    let image_id = compute_image_id(elf).unwrap();
    info!("Image ID: {}", image_id);
    info!("Journal digest: {}", journal.digest());
    info!("Journal size: {}", journal.len());

    // let claim = ReceiptClaim::ok(image_id, MaybePruned::Pruned(journal.digest()));

    // let claim = ReceiptClaim::ok(image_id, journal.to_vec());
    let receipt = boundless_client
        .set_verifier
        .fetch_receipt(seal, image_id, journal.to_vec())
        .await
        .unwrap();

    let receipt = receipt
        .root
        .ok_or(anyhow::anyhow!(
            "Failed to get root from receipt, this is likely a bug in the SDK"
        ))
        .unwrap();

    receipt.verify(image_id).unwrap();
    Ok(())
}

use anyhow::{Context, Result};
use clap::{Parser, command};
use std::collections::BTreeSet;
use std::env;

// Core types
use hyle_model::{
    BlobTransaction, ContractAction, Hashed, Identity, TxHash, verifiers::Secp256k1Blob,
};
use hyle_modules::{node_state::NukeTxAction, utils::logger::setup_tracing};

// HTTP client for sending transactions
use client_sdk::rest_client::{NodeApiClient, NodeApiHttpClient};

// Crypto
use secp256k1::{Message, PublicKey, Secp256k1, SecretKey};
use sha2::Digest;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Args {
    // List of transaction hashes to nuke
    #[arg(long, required = true, value_delimiter = ',')]
    pub tx_hashes: Vec<String>,

    /// Node URL (can also be set via HYLI_NODE_URL env var)
    #[arg(long)]
    pub node_url: Option<String>,

    /// Secret key in hex format (can also be set via HYLI_SECRET_KEY env var)  
    #[arg(long)]
    pub secret_key: Option<String>,
}

fn sign_data(secret_key: &SecretKey, expected_data: &[u8]) -> ([u8; 32], [u8; 64]) {
    // Hash the expected data
    let mut hasher = sha2::Sha256::new();
    hasher.update(expected_data);
    let data_hash = hasher.finalize().into();
    let message = Message::from_digest(data_hash);
    let mut signature = secret_key.sign_ecdsa(message);
    signature.normalize_s();

    (data_hash, signature.serialize_compact())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    setup_tracing("full", "nuke_tx".to_string())?;

    tracing::info!("Starting nuke tx tool");

    // Get node URL from args or environment
    let node_url = args
        .node_url
        .or_else(|| env::var("HYLI_NODE_URL").ok())
        .context("Node URL must be provided via --node-url or HYLI_NODE_URL env var")?;

    // Get secret key from args or environment
    let secret_key_hex = args
        .secret_key
        .or_else(|| env::var("HYLI_SECRET_KEY").ok())
        .context("Secret key must be provided via --secret-key or HYLI_SECRET_KEY env var")?;

    // Parse secret key
    let secret_key_bytes =
        hex::decode(&secret_key_hex).context("Failed to decode secret key hex")?;
    let secret_key = SecretKey::from_slice(&secret_key_bytes).context("Invalid secret key")?;

    let secp = Secp256k1::new();
    let public_key = PublicKey::from_secret_key(&secp, &secret_key);

    // Parse transaction hashes
    let tx_hashes: BTreeSet<TxHash> = args.tx_hashes.into_iter().map(TxHash).collect();

    tracing::info!(
        "Will nuke {} transactions: {:?}",
        tx_hashes.len(),
        tx_hashes
    );

    // Create the NukeTxAction
    let nuke_action = NukeTxAction {
        txs: tx_hashes
            .iter()
            .map(|tx_hash| (tx_hash.clone(), hyle_model::HyleOutput::default()))
            .collect(),
    };

    // Create the nuke blob
    let nuke_blob = nuke_action.as_blob("hyle".into(), None, None);

    // Create secp256k1 blob that signs the transaction hash data
    let expected_data = borsh::to_vec(&nuke_action.txs).context("Failed to serialize tx_hashes")?;

    let (data_hash, signature) = sign_data(&secret_key, &expected_data);

    let secp_blob_data = Secp256k1Blob {
        identity: Identity::new("hyle@hyle"),
        data: data_hash,
        public_key: public_key.serialize(),
        signature,
    };

    let secp_blob = secp_blob_data.as_blob();

    // Create the nuke transaction
    let nuke_tx = BlobTransaction::new(Identity::new("hyle@hyle"), vec![secp_blob, nuke_blob]);

    tracing::info!("Created nuke transaction with hash: {}", nuke_tx.hashed());

    // Send transaction to node
    let client = NodeApiHttpClient::new(node_url).context("Failed to create HTTP client")?;

    let tx_hash = client
        .send_tx_blob(nuke_tx)
        .await
        .context("Failed to send nuke transaction")?;

    tracing::info!("✅ Nuke transaction sent successfully!");
    tracing::info!("Transaction hash: {}", tx_hash);
    println!("Nuke transaction sent with hash: {}", tx_hash);

    Ok(())
}

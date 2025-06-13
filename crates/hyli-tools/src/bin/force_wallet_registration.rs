use anyhow::{Context, Result};
use clap::{Parser, command};
use client_sdk::{rest_client::NodeApiClient, transaction_builder::TxExecutorHandler};
use hyle_net::http::HttpClient;
use secp256k1::{PublicKey, Secp256k1, SecretKey};
use serde::Serialize;
use std::{
    collections::BTreeMap,
    env,
    fs::File,
    io::{self, BufRead},
};
use wallet::client::tx_executor_handler::{Wallet, WalletConstructor};

use hyle_model::{
    Blob, BlobData, BlobIndex, BlobTransaction, Calldata, Hashed, HyleOutput, Identity,
    StateCommitment,
};
use hyle_modules::utils::logger::setup_tracing;

use hyli_tools::nuke_tx_module::{Conf, NukeTxModule, NukeTxModuleCtx};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Args {
    #[arg(long, default_value = "config.toml")]
    pub config_file: Vec<String>,

    // Path to file containing transaction hashes
    #[arg(long, required = true)]
    pub tx_file: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let config = Conf::new(args.config_file).context("reading config file")?;
    setup_tracing(&config.log_format, "nuke_tx".to_string())?;

    tracing::info!("Starting force wallet registration tool");

    // Read transaction hashes from file
    let file = File::open(&args.tx_file).context("Failed to open transaction file")?;
    let reader = io::BufReader::new(file);

    let mut txs = BTreeMap::new();

    // Wallet specific code
    let secp = Secp256k1::new();
    let secret_key =
        hex::decode(env::var("INVITE_CODE_PKEY").unwrap_or(
            "0000000000000001000000000000000100000000000000010000000000000001".to_string(),
        ))
        .expect("INVITE_CODE_PKEY must be a hex string");
    let secret_key = SecretKey::from_slice(&secret_key).expect("32 bytes, within curve order");
    let public_key = PublicKey::from_secret_key(&secp, &secret_key);

    let hyli_password = env::var("HYLI_PASSWORD").unwrap_or("hylisecure".to_string());
    let wallet_constructor = WalletConstructor::new(hyli_password, public_key.serialize());
    let mut wallet = Wallet::new(&Some(wallet_constructor.clone())).expect("must succeed");

    let wallet_url = env::var("HYLI_WALLET_URL").unwrap_or("hylisecure".to_string());
    let client = HttpClient {
        url: wallet_url.parse()?,
        api_key: None,
        retry: None,
    };

    // Lines are expected to be formed like:
    //{"RegisterIdentity": {"account":"account_name","nonce":1749745289204,"salt":"woxekl7t3r","auth_method":{"Password":{"hash":"150..."}},"invite_code":"I7XE47QT3D"}}
    for line in reader.lines() {
        let line = line.context("Failed to read line from file")?;

        if let Ok(wallet_action) = serde_json::from_str::<wallet::WalletAction>(&line) {
            if let wallet::WalletAction::RegisterIdentity {
                ref account,
                ref invite_code,
                ref auth_method,
                ..
            } = wallet_action
            {
                // Regenerate password hash
                let password_hash = match auth_method {
                    wallet::AuthMethod::Password { hash } => hash,
                    _ => panic!("Invalid auth method"),
                };

                let check_secret_blob = Blob {
                    contract_name: "check_secret".into(),
                    data: BlobData(hex::decode(password_hash)?),
                };
                let check_secret_blob_hyle_output = HyleOutput {
                    index: BlobIndex(1),
                    initial_state: StateCommitment(vec![0; 4]),
                    next_state: StateCommitment(vec![0; 4]),
                    success: true,
                    ..Default::default()
                };

                // Consume invite code
                let invite_code_blob: Blob = client
                    .post_json(
                        "api/consume_invite",
                        &ConsumeInviteBody {
                            code: invite_code.clone(),
                            wallet: account.clone(),
                        },
                    )
                    .await
                    .context("Sending tx blob")?;

                let tx = BlobTransaction::new(
                    Identity::new(format!("{}@wallet", account)),
                    vec![
                        wallet_action.as_blob("wallet".into()),
                        check_secret_blob,
                        invite_code_blob,
                    ],
                );

                // Send blob transaction
                let tx_hash =
                    client_sdk::rest_client::NodeApiHttpClient::new(config.node_url.clone())?
                        .send_tx_blob(tx.clone())
                        .await
                        .context("Failed to send blob transaction")?;
                // Get calldata
                let calldata = Calldata {
                    identity: tx.identity.clone(),
                    index: BlobIndex(0),
                    blobs: tx.blobs.clone().into(),
                    tx_blob_count: tx.blobs.len(),
                    tx_hash: tx.hashed(),
                    tx_ctx: None,
                    private_input: vec![],
                };
                let hyle_output = wallet.handle(&calldata)?;
                tracing::info!(
                    "Executed transaction {:?}: {:?}",
                    tx.hashed(),
                    str::from_utf8(&hyle_output.program_outputs).unwrap_or("no output")
                );
                txs.insert(tx_hash, vec![hyle_output, check_secret_blob_hyle_output]);
            }
        } else {
            tracing::warn!("Failed to parse wallet action: {}", line);
        }
    }

    tracing::info!("Will nuke {} transactions: {:?}", txs.len(), txs.keys());

    // Create message bus
    let bus = hyle_modules::bus::SharedMessageBus::new(
        hyle_modules::bus::metrics::BusMetrics::global("nuke_tx".to_string()),
    );

    // Initialize modules
    let mut handler = hyle_modules::modules::ModulesHandler::new(&bus).await;

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

#[derive(Debug, Serialize)]
pub struct ConsumeInviteBody {
    pub code: String,
    pub wallet: String,
}

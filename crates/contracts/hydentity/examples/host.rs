use clap::{Parser, Subcommand};
use client_sdk::contract_states;
use client_sdk::rest_client::IndexerApiHttpClient;
use client_sdk::rest_client::NodeApiClient;
use client_sdk::rest_client::NodeApiHttpClient;
use client_sdk::transaction_builder::ProvableBlobTx;
use client_sdk::transaction_builder::TxExecutor;
use client_sdk::transaction_builder::TxExecutorBuilder;
use client_sdk::transaction_builder::TxExecutorHandler;
use hyle_hydentity::Hydentity;
use hyle_hydentity::HydentityAction;
use sdk::Hashed;
use sdk::{Blob, Calldata, ContractName, HyleOutput};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
#[command(propagate_version = true)]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    #[arg(long, default_value = "http://localhost:4321")]
    pub host: String,
}

#[derive(Subcommand)]
enum Commands {
    RegisterIdentity {
        identity: String,
        password: String,
    },
    VerifyIdentity {
        identity: String,
        password: String,
        nonce: u32,
    },
}
contract_states!(
    #[derive(Debug, Clone)]
    pub struct States {
        pub hydentity: Hydentity,
    }
);

async fn build_ctx(client: &IndexerApiHttpClient) -> TxExecutor<States> {
    // Fetch the initial state from the node
    let initial_state: Hydentity = client
        .fetch_current_state(&"hydentity".into())
        .await
        .unwrap();

    TxExecutorBuilder::new(States {
        hydentity: initial_state,
    })
    .build()
}

#[tokio::main]
async fn main() {
    // Initialize tracing. In order to view logs, run `RUST_LOG=info cargo run`
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::filter::EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();

    let node = NodeApiHttpClient::new(cli.host.clone()).unwrap();
    let indexer = IndexerApiHttpClient::new(cli.host).unwrap();

    let mut ctx = build_ctx(&indexer).await;

    match cli.command {
        Commands::RegisterIdentity { identity, password } => {
            // ----
            // Build the blob transaction
            // ----

            let mut transaction = ProvableBlobTx::new(identity.clone().into());

            hyle_hydentity::client::tx_executor_handler::register_identity(
                &mut transaction,
                ContractName::new("hydentity"),
                password,
            )
            .unwrap();

            let transaction = ctx.process(transaction).unwrap();

            // Send the blob transaction
            let blob_tx_hash = node.send_tx_blob(transaction.to_blob_tx()).await.unwrap();
            println!("✅ Blob tx sent. Tx hash: {}", blob_tx_hash);

            // ----
            // Prove the state transition
            // ----
            for proof in transaction.iter_prove() {
                let tx = proof.await.unwrap();

                let tx_hash = tx.hashed();
                let contract_name = tx.contract_name.clone();

                node.send_tx_proof(tx).await.unwrap();
                println!(
                    "✅ Proof tx sent for {}. Tx hash: {}",
                    contract_name, tx_hash
                );
            }
        }
        Commands::VerifyIdentity {
            identity,
            password,
            nonce,
        } => {
            // ----
            // Build the blob transaction
            // ----

            let mut transaction = ProvableBlobTx::new(identity.clone().into());

            transaction
                .add_action(
                    "hydentity".into(),
                    HydentityAction::VerifyIdentity {
                        account: identity.clone(),
                        nonce,
                    },
                    Some(password.into_bytes().to_vec()),
                    None,
                    None,
                )
                .unwrap();

            let transaction = ctx.process(transaction).unwrap();

            // Send the blob transaction
            let blob_tx_hash = node.send_tx_blob(transaction.to_blob_tx()).await.unwrap();
            println!("✅ Blob tx sent. Tx hash: {}", blob_tx_hash);

            // ----
            // Prove the state transition
            // ----
            for proof in transaction.iter_prove() {
                let tx = proof.await.unwrap();
                let tx_hash = tx.hashed();
                let contract_name = tx.contract_name.clone();
                node.send_tx_proof(tx).await.unwrap();
                println!(
                    "✅ Proof tx sent for {}. Tx hash: {}",
                    contract_name, tx_hash
                );
            }
        }
    }
}

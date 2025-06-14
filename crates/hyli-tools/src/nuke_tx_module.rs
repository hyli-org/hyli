use anyhow::{Context, Result};
use client_sdk::rest_client::{NodeApiClient, NodeApiHttpClient};
use hyle_model::{
    BlobTransaction, ContractAction, Identity, NodeStateEvent, TxHash, verifiers::Secp256k1Blob,
};
use hyle_modules::{
    bus::SharedMessageBus, module_bus_client, modules::Module, node_state::NukeTxAction,
};
use secp256k1::{Message, PublicKey, Secp256k1, SecretKey};
use serde::{Deserialize, Serialize};
use sha2::Digest;
use std::{collections::BTreeSet, path::PathBuf};

module_bus_client! {
    #[derive(Debug)]
    struct NukeTxBusClient {
        receiver(NodeStateEvent),
    }
}
pub struct NukeTxModuleCtx {
    pub config: Conf,
    pub tx_hashes: BTreeSet<TxHash>,
}

pub struct NukeTxModule {
    _bus: NukeTxBusClient,
    remaining_txs: BTreeSet<TxHash>,
    node_client: NodeApiHttpClient,
    secret_key: SecretKey,
    as_failed: bool,
}

impl Module for NukeTxModule {
    type Context = NukeTxModuleCtx;

    async fn build(bus: SharedMessageBus, ctx: Self::Context) -> Result<Self> {
        let bus = NukeTxBusClient::new_from_bus(bus.new_handle()).await;
        let secret_key_bytes =
            hex::decode(&ctx.config.secret_key).context("Failed to decode secret key hex")?;
        let secret_key = SecretKey::from_slice(&secret_key_bytes).context("Invalid secret key")?;

        Ok(NukeTxModule {
            _bus: bus,
            as_failed: ctx.config.as_failed,
            node_client: NodeApiHttpClient::new(ctx.config.node_url)?,
            secret_key,
            remaining_txs: ctx.tx_hashes.clone(),
        })
    }

    async fn run(&mut self) -> Result<()> {
        self.start().await?;
        Ok(())
    }
}

impl NukeTxModule {
    async fn start(&mut self) -> Result<()> {
        let txs_to_nuke = self.remaining_txs.clone();
        self.create_and_send_nuke_tx(&txs_to_nuke).await?;
        Ok(())
    }

    fn sign_data(&self, expected_data: &[u8]) -> ([u8; 32], [u8; 64]) {
        let mut hasher = sha2::Sha256::new();
        hasher.update(expected_data);
        let data_hash = hasher.finalize().into();
        let message = Message::from_digest(data_hash);
        let mut signature = self.secret_key.sign_ecdsa(message);
        signature.normalize_s();

        (data_hash, signature.serialize_compact())
    }

    async fn create_and_send_nuke_tx(&self, tx_hashes: &BTreeSet<TxHash>) -> Result<()> {
        let secp = Secp256k1::new();
        let public_key = PublicKey::from_secret_key(&secp, &self.secret_key);

        // Create the NukeTxAction
        let nuke_action = if self.as_failed {
            let hyle_output = hyle_model::HyleOutput {
                success: false,
                ..Default::default()
            };
            NukeTxAction {
                txs: tx_hashes
                    .iter()
                    .map(|tx_hash| (tx_hash.clone(), hyle_output.clone()))
                    .collect(),
            }
        } else {
            // TODO Compute correct hyle output
            NukeTxAction {
                txs: tx_hashes
                    .iter()
                    .map(|tx_hash| (tx_hash.clone(), hyle_model::HyleOutput::default()))
                    .collect(),
            }
        };

        // Create the nuke blob
        let nuke_blob = nuke_action.as_blob("hyle".into(), None, None);

        // Create secp256k1 blob that signs the transaction hash data
        let expected_data =
            borsh::to_vec(&nuke_action.txs).context("Failed to serialize tx_hashes")?;
        let (data_hash, signature) = self.sign_data(&expected_data);

        let secp_blob_data = Secp256k1Blob {
            identity: Identity::new("hyle@hyle"),
            data: data_hash,
            public_key: public_key.serialize(),
            signature,
        };

        let secp_blob = secp_blob_data.as_blob();

        // Create and send the nuke transaction
        let nuke_tx = BlobTransaction::new(Identity::new("hyle@hyle"), vec![secp_blob, nuke_blob]);
        let tx_hash = self
            .node_client
            .send_tx_blob(nuke_tx)
            .await
            .context("Failed to send nuke transaction")?;

        tracing::info!("✅ Nuke transaction sent successfully for block!");
        tracing::info!("Transaction hash: {}", tx_hash);

        Ok(())
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct Conf {
    /// The log format to use - "json", "node" or "full" (default)
    pub log_format: String,

    /// Directory name to store node state.
    pub data_directory: PathBuf,

    /// URL to connect to.
    pub da_read_from: String,

    pub node_url: String,
    pub secret_key: String,
    pub as_failed: bool,
}

impl Conf {
    pub fn new(config_files: Vec<String>) -> Result<Self, anyhow::Error> {
        let mut s = config::Config::builder().add_source(config::File::from_str(
            include_str!("nuke_tx_conf_defaults.toml"),
            config::FileFormat::Toml,
        ));
        // Priority order: config file, then environment variables, then CLI
        for config_file in config_files {
            s = s.add_source(config::File::with_name(&config_file).required(false));
        }
        let conf: Self = s
            .add_source(
                config::Environment::with_prefix("hyle")
                    .separator("__")
                    .prefix_separator("_"),
            )
            .build()?
            .try_deserialize()?;
        Ok(conf)
    }
}

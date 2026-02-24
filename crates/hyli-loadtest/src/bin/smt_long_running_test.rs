use std::path::Path;
use std::{collections::HashSet, path::PathBuf, sync::Arc, time::Duration};

use anyhow::{Context, Result};
use clap::Parser;
use client_sdk::helpers::risc0::Risc0Prover;
use client_sdk::rest_client::{NodeApiClient, NodeApiHttpClient};
use client_sdk::transaction_builder::ProvableBlobTx;
use hyli_contract_sdk::{BlobTransaction, ContractName, Identity, RegisterContractAction};
use hyli_modules::{
    bus::SharedMessageBus,
    module_bus_client, module_handle_messages,
    modules::{
        contract_listener::{ContractListener, ContractListenerConf},
        prover::{AutoProver, AutoProverCtx},
        Module, ModulesHandler,
    },
    utils::logger::setup_tracing,
};
use rand::Rng;
use serde::{Deserialize, Serialize};
use smt_token::client::tx_executor_handler::{
    metadata::PROGRAM_ID as SMT_TOKEN_PROGRAM_ID, SmtTokenProvableState,
};
use smt_token::{
    account::{Account, AccountSMT},
    SmtTokenAction,
};

const BOOTSTRAP_CONTRACT_FILE: &str = "smt_long_running_test_contract_name.txt";

#[derive(Debug, Parser)]
#[command(name = "smt_long_running_test")]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(long, default_value = "config.toml")]
    pub config_file: Vec<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
struct Conf {
    pub log_format: String,
    pub host: String,
    pub port: u32,
    pub node_api_key: String,
    pub interval_ms: u64,
    pub database_url: String,
    pub data_directory: PathBuf,
    pub contract_name_prefix: String,
    pub initial_account_balance: u64,
    pub contract_listener_poll_interval_secs: u64,
    pub tx_buffer_size: usize,
    pub max_txs_per_proof: usize,
    pub tx_working_window_size: usize,
    pub idle_flush_interval_secs: u64,
    pub registration_settle_wait_secs: u64,
}

impl Conf {
    pub fn new(config_files: Vec<String>) -> Result<Self, anyhow::Error> {
        let mut s = config::Config::builder().add_source(config::File::from_str(
            include_str!("../smt_long_running_test_conf_defaults.toml"),
            config::FileFormat::Toml,
        ));

        for config_file in config_files {
            s = s.add_source(config::File::with_name(&config_file).required(false));
        }

        let conf: Self = s
            .add_source(
                config::Environment::with_prefix("hyli")
                    .separator("__")
                    .prefix_separator("_"),
            )
            .build()?
            .try_deserialize()?;
        Ok(conf)
    }
}

#[derive(Debug, Clone)]
struct BootstrapCtx {
    node_url: String,
    node_api_key: String,
    contract_name_prefix: String,
    initial_account_balance: u64,
}

#[derive(Debug, Clone)]
struct BootstrapOutput {
    contract_name: ContractName,
}

struct ContractBootstrap;

impl ContractBootstrap {
    async fn build(ctx: BootstrapCtx) -> Result<BootstrapOutput> {
        let mut node = NodeApiHttpClient::new(ctx.node_url).context("build node client")?;
        node.api_key = Some(ctx.node_api_key);

        loop {
            let random_number: u64 = rand::rng().random();
            let contract_name: ContractName =
                format!("{}{}", ctx.contract_name_prefix, random_number).into();

            if node.get_contract(contract_name.clone()).await.is_ok() {
                tracing::info!(
                    "Build phase: contract {} already exists, retrying",
                    contract_name
                );
                continue;
            }

            let initial_state =
                build_smt_state_with_accounts(&contract_name, ctx.initial_account_balance)?;
            let register_tx = BlobTransaction::new(
                Identity::new("hyli@hyli"),
                vec![RegisterContractAction {
                    contract_name: contract_name.clone(),
                    verifier: "risc0-3".into(),
                    program_id: SMT_TOKEN_PROGRAM_ID.to_vec().into(),
                    state_commitment: smt_state_commitment(&initial_state),
                    ..Default::default()
                }
                .as_blob("hyli".into())],
            );

            match node.send_tx_blob(register_tx).await {
                Ok(tx_hash) => {
                    tracing::info!(
                        "Build phase: registered contract {} with tx {}",
                        contract_name,
                        tx_hash
                    );
                    return Ok(BootstrapOutput { contract_name });
                }
                Err(err) => {
                    if node.get_contract(contract_name.clone()).await.is_ok() {
                        tracing::warn!(
                            "Build phase: contract {} existed during registration race, retrying",
                            contract_name
                        );
                        continue;
                    }
                    return Err(err).context("register SMT contract");
                }
            }
        }
    }
}

fn bootstrap_contract_path(data_directory: &Path) -> PathBuf {
    data_directory.join(BOOTSTRAP_CONTRACT_FILE)
}

fn load_bootstrap_contract_name(data_directory: &Path) -> Result<Option<ContractName>> {
    let path = bootstrap_contract_path(data_directory);
    if !path.exists() {
        return Ok(None);
    }
    let raw = std::fs::read_to_string(&path)
        .with_context(|| format!("read bootstrap contract file {}", path.display()))?;
    let name = raw.trim();
    if name.is_empty() {
        return Ok(None);
    }
    Ok(Some(name.into()))
}

fn persist_bootstrap_contract_name(
    data_directory: &Path,
    contract_name: &ContractName,
) -> Result<()> {
    std::fs::create_dir_all(data_directory)
        .with_context(|| format!("create data directory {}", data_directory.display()))?;
    let path = bootstrap_contract_path(data_directory);
    std::fs::write(&path, format!("{}\n", contract_name))
        .with_context(|| format!("write bootstrap contract file {}", path.display()))?;
    Ok(())
}

#[derive(Debug, Clone)]
struct TxSenderCtx {
    node_url: String,
    node_api_key: String,
    interval_ms: u64,
    contract_name: ContractName,
}

module_bus_client! {
    #[derive(Debug)]
    struct SmtTxSenderBusClient {}
}

struct SmtTxSender {
    bus: SmtTxSenderBusClient,
    node: NodeApiHttpClient,
    interval: Duration,
    contract_name: ContractName,
    bob: Identity,
    alice: Identity,
}

impl Module for SmtTxSender {
    type Context = TxSenderCtx;

    async fn build(bus: SharedMessageBus, ctx: Self::Context) -> Result<Self> {
        let mut node = NodeApiHttpClient::new(ctx.node_url.clone()).context("build node client")?;
        node.api_key = Some(ctx.node_api_key);

        let bob = Identity::from(format!("bob@{}", ctx.contract_name.0));
        let alice = Identity::from(format!("alice@{}", ctx.contract_name.0));

        Ok(Self {
            bus: SmtTxSenderBusClient::new_from_bus(bus.new_handle()).await,
            node,
            interval: Duration::from_millis(ctx.interval_ms),
            contract_name: ctx.contract_name,
            bob,
            alice,
        })
    }

    async fn run(&mut self) -> Result<()> {
        let mut ticker = tokio::time::interval(self.interval);
        module_handle_messages! {
            on_self self,
            _ = ticker.tick() => {
            let random_amount: u128 = rand::rng().random_range(1..=1_000_000u128);

            let mut bob_to_alice = ProvableBlobTx::new(self.bob.clone());
            bob_to_alice.add_action(
                self.contract_name.clone(),
                SmtTokenAction::Transfer {
                    sender: self.bob.clone(),
                    recipient: self.alice.clone(),
                    amount: random_amount,
                },
                None,
                None,
                None,
            )?;

            let mut alice_to_bob = ProvableBlobTx::new(self.alice.clone());
            alice_to_bob.add_action(
                self.contract_name.clone(),
                SmtTokenAction::Transfer {
                    sender: self.alice.clone(),
                    recipient: self.bob.clone(),
                    amount: random_amount,
                },
                None,
                None,
                None,
            )?;

            match self
                .node
                .send_tx_blob(BlobTransaction::from(bob_to_alice))
                .await
            {
                Ok(hash_1) => match self
                    .node
                    .send_tx_blob(BlobTransaction::from(alice_to_bob))
                    .await
                {
                    Ok(hash_2) => {
                        tracing::info!(
                            "SMT txs sent: {} and {} (amount={}, contract={})",
                            hash_1,
                            hash_2,
                            random_amount,
                            self.contract_name
                        );
                    }
                    Err(err) => tracing::warn!("Failed to send alice->bob SMT tx: {err:?}"),
                },
                Err(err) => tracing::warn!("Failed to send bob->alice SMT tx: {err:?}"),
            }
            }
        };
        Ok(())
    }
}

fn smt_state_commitment(state: &SmtTokenProvableState) -> hyli_contract_sdk::StateCommitment {
    let root = *state.0.root();
    hyli_contract_sdk::StateCommitment(Into::<[u8; 32]>::into(root).to_vec())
}

fn build_smt_state_with_accounts(
    contract_name: &ContractName,
    initial_account_balance: u64,
) -> Result<SmtTokenProvableState> {
    let mut state = AccountSMT::default();
    let bob = Account::new(
        Identity::from(format!("bob@{}", contract_name.0)),
        u128::from(initial_account_balance),
    );
    let alice = Account::new(
        Identity::from(format!("alice@{}", contract_name.0)),
        u128::from(initial_account_balance),
    );

    state
        .0
        .update(bob.get_key(), bob)
        .context("insert bob account in initial SMT state")?;
    state
        .0
        .update(alice.get_key(), alice)
        .context("insert alice account in initial SMT state")?;

    Ok(state)
}

async fn resolve_bootstrap(config: &Conf, node_url: &str) -> Result<BootstrapOutput> {
    let mut node = NodeApiHttpClient::new(node_url.to_string()).context("build node client")?;
    node.api_key = Some(config.node_api_key.clone());

    if let Some(contract_name) = load_bootstrap_contract_name(&config.data_directory)? {
        match node.get_contract(contract_name.clone()).await {
            Ok(_) => {
                tracing::info!("Build phase: reusing existing contract {}", contract_name);
                return Ok(BootstrapOutput {
                    contract_name: contract_name.clone(),
                });
            }
            Err(err) => {
                tracing::warn!(
                    "Build phase: stored contract {} not found anymore ({err:?}), registering a new one",
                    contract_name
                );
            }
        }
    }

    let bootstrap = ContractBootstrap::build(BootstrapCtx {
        node_url: node_url.to_string(),
        node_api_key: config.node_api_key.clone(),
        contract_name_prefix: config.contract_name_prefix.clone(),
        initial_account_balance: config.initial_account_balance,
    })
    .await?;
    persist_bootstrap_contract_name(&config.data_directory, &bootstrap.contract_name)?;
    Ok(bootstrap)
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let config = Conf::new(args.config_file).context("reading config file")?;

    setup_tracing(&config.log_format, "LongSMTRunningTest".to_string()).context("setup tracing")?;

    let bus = SharedMessageBus::new();
    let mut handler = ModulesHandler::new(&bus, config.data_directory.clone())
        .context("initialize modules handler")?;

    let node_url = format!("http://{}:{}/", config.host, config.port);
    let bootstrap = resolve_bootstrap(&config, &node_url)
        .await
        .context("resolve bootstrap contract")?;

    tokio::time::sleep(Duration::from_secs(config.registration_settle_wait_secs)).await;

    persist_bootstrap_contract_name(&config.data_directory, &bootstrap.contract_name)
        .context("persist bootstrap contract marker")?;

    let mut node_client =
        NodeApiHttpClient::new(node_url.clone()).context("build node client for auto prover")?;
    node_client.api_key = Some(config.node_api_key.clone());
    let prover_node = Arc::new(node_client);

    let auto_prover_ctx = Arc::new(AutoProverCtx {
        data_directory: config.data_directory.clone(),
        prover: Arc::new(Risc0Prover::new(
            smt_token::client::tx_executor_handler::metadata::SMT_TOKEN_ELF.to_vec(),
            smt_token::client::tx_executor_handler::metadata::PROGRAM_ID,
        )),
        contract_name: bootstrap.contract_name.clone(),
        node: prover_node,
        api: None,
        tx_buffer_size: config.tx_buffer_size,
        max_txs_per_proof: config.max_txs_per_proof,
        tx_working_window_size: config.tx_working_window_size,
        idle_flush_interval: Duration::from_secs(config.idle_flush_interval_secs),
    });

    handler
        .build_module::<AutoProver<SmtTokenProvableState, Risc0Prover>>(auto_prover_ctx)
        .await
        .context("build auto prover module")?;

    handler
        .build_module::<ContractListener>(ContractListenerConf {
            database_url: config.database_url.clone(),
            data_directory: config.data_directory.clone(),
            contracts: HashSet::from([bootstrap.contract_name.clone()]),
            poll_interval: Duration::from_secs(config.contract_listener_poll_interval_secs),
            replay_settled_from_start: true,
        })
        .await
        .context("build contract listener module")?;

    handler
        .build_module::<SmtTxSender>(TxSenderCtx {
            node_url,
            node_api_key: config.node_api_key.clone(),
            interval_ms: config.interval_ms,
            contract_name: bootstrap.contract_name.clone(),
        })
        .await
        .context("build tx sender module")?;

    tracing::info!(
        "Runtime phase: starting modules for contract {}",
        bootstrap.contract_name
    );
    handler.start_modules().await.context("start modules")?;
    handler.exit_process().await.context("shutdown modules")?;

    Ok(())
}

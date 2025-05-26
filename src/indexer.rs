//! Index system for historical data.

mod api;

use crate::node_state::module::NodeStateEvent;
use crate::{model::*, utils::conf::SharedConf};
use anyhow::{bail, Context, Error, Result};
use api::*;
use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Path, State,
    },
    response::IntoResponse,
    routing::get,
    Router,
};
use chrono::{DateTime, Utc};
use futures::{SinkExt, StreamExt};
use hyle_contract_sdk::TxHash;
use hyle_model::api::{
    BlobWithStatus, TransactionStatusDb, TransactionTypeDb, TransactionWithBlobs,
};
use hyle_model::utils::TimestampMs;
use hyle_modules::{
    bus::SharedMessageBus,
    log_error, log_warn, module_handle_messages,
    modules::{module_bus_client, Module, SharedBuildApiCtx},
};
use sqlx::{postgres::PgPoolOptions, PgPool, Pool, Postgres};
use sqlx::{PgExecutor, QueryBuilder, Row};
use std::collections::{HashMap, HashSet};
use std::ops::DerefMut;
use tokio::select;
use tokio::sync::{broadcast, mpsc};
use tracing::{debug, error, info, trace};
use utoipa::OpenApi;
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

module_bus_client! {
#[derive(Debug)]
struct IndexerBusClient {
    receiver(NodeStateEvent),
    receiver(MempoolStatusEvent),
}
}

// TODO: generalize for all tx types
type Subscribers = HashMap<ContractName, Vec<broadcast::Sender<TransactionWithBlobs>>>;

#[derive(Debug, Clone)]
pub struct IndexerApiState {
    db: PgPool,
    new_sub_sender: mpsc::Sender<(ContractName, WebSocket)>,
}

#[derive(Debug)]
pub struct Indexer {
    bus: IndexerBusClient,
    state: IndexerApiState,
    new_sub_receiver: tokio::sync::mpsc::Receiver<(ContractName, WebSocket)>,
    subscribers: Subscribers,
}

pub static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./src/indexer/migrations");

impl Module for Indexer {
    type Context = (SharedConf, SharedBuildApiCtx);

    async fn build(bus: SharedMessageBus, ctx: Self::Context) -> Result<Self> {
        let bus = IndexerBusClient::new_from_bus(bus.new_handle()).await;

        let pool = PgPoolOptions::new()
            .max_connections(20)
            .acquire_timeout(std::time::Duration::from_secs(1))
            .connect(&ctx.0.database_url)
            .await
            .context("Failed to connect to the database")?;

        tokio::time::timeout(tokio::time::Duration::from_secs(60), MIGRATOR.run(&pool)).await??;

        let (new_sub_sender, new_sub_receiver) = tokio::sync::mpsc::channel(100);

        let subscribers = HashMap::new();

        let indexer = Indexer {
            bus,
            state: IndexerApiState {
                db: pool,
                new_sub_sender,
            },
            new_sub_receiver,
            subscribers,
        };

        if let Ok(mut guard) = ctx.1.router.lock() {
            if let Some(router) = guard.take() {
                guard.replace(router.nest("/v1/indexer", indexer.api(Some(&ctx.1))));
                return Ok(indexer);
            }
        }

        if let Ok(mut guard) = ctx.1.openapi.lock() {
            tracing::info!("Adding OpenAPI for Indexer");
            let openapi = guard.clone().nest("/v1/indexer", IndexerAPI::openapi());
            *guard = openapi;
        } else {
            tracing::error!("Failed to add OpenAPI for Indexer");
        }

        anyhow::bail!("context router should be available");
    }

    fn run(&mut self) -> impl futures::Future<Output = Result<()>> + Send {
        self.start()
    }
}

impl Indexer {
    pub async fn start(&mut self) -> Result<()> {
        module_handle_messages! {
            on_bus self.bus,
            listen<NodeStateEvent> event => {
                _ = log_error!(self.handle_node_state_event(event)
                    .await,
                    "Indexer handling node state event");
            }

            listen<MempoolStatusEvent> event => {
                _ = log_error!(self.handle_mempool_status_event(event)
                    .await,
                    "Indexer handling mempool status event");
            }

            Some((contract_name, socket)) = self.new_sub_receiver.recv() => {

                let (tx, mut rx) = broadcast::channel(100);
                // Append tx to the list of subscribers for contract_name
                self.subscribers.entry(contract_name)
                    .or_default()
                    .push(tx);

                tokio::spawn(async move {
                        let (mut ws_tx, mut ws_rx) = socket.split();

                        loop {
                            select! {
                                maybe_transaction = rx.recv() => {
                                    match maybe_transaction {
                                        Ok(transaction) => {
                                            if let Ok(json) = log_error!(serde_json::to_vec(&transaction),
                                                "Serialize transaction to JSON") {
                                                if ws_tx.send(Message::Binary(json.into())).await.is_err() {
                                                    break;
                                                }
                                            }
                                        }
                                        _ => break,
                                    }
                                },
                                // Branch to handle incoming messages from ws
                                maybe_msg = ws_rx.next() => {
                                    match maybe_msg {
                                        Some(Ok(message)) => {
                                            if let Message::Close(frame) = message {
                                                info!("WS closed by client: {:?}", frame);
                                                let _ = ws_tx.send(Message::Close(frame)).await;
                                                break;
                                            }
                                        }
                                        Some(Err(e)) => {
                                            error!("Error while getting message from WS: {}", e);
                                            break;
                                        }
                                        None => break,
                                    }
                                }
                            }
                        }
                    });
            }
        };
        Ok(())
    }

    pub async fn get_last_block(&self) -> Result<Option<BlockHeight>> {
        let rows = sqlx::query("SELECT max(height) as max FROM blocks")
            .fetch_one(&self.state.db)
            .await?;
        Ok(rows
            .try_get("max")
            .map(|m: i64| Some(BlockHeight(m as u64)))
            .unwrap_or(None))
    }

    pub fn api(&self, ctx: Option<&SharedBuildApiCtx>) -> Router<()> {
        #[derive(OpenApi)]
        struct IndexerAPI;

        let (router, api) = OpenApiRouter::with_openapi(IndexerAPI::openapi())
            // stats
            .routes(routes!(api::get_stats))
            .routes(routes!(api::get_proof_stats))
            // block
            .routes(routes!(api::get_blocks))
            .routes(routes!(api::get_last_block))
            .routes(routes!(api::get_block))
            .routes(routes!(api::get_block_by_hash))
            // transaction
            .routes(routes!(api::get_transactions))
            .routes(routes!(api::get_transactions_by_height))
            .routes(routes!(api::get_transactions_by_contract))
            .routes(routes!(api::get_transaction_with_hash))
            .routes(routes!(api::get_transaction_events))
            .routes(routes!(api::get_blob_transactions_by_contract))
            .route(
                "/blob_transactions/contract/{contract_name}/ws",
                get(Self::get_blob_transactions_by_contract_ws_handler),
            )
            // proof transaction
            .routes(routes!(api::get_proofs))
            .routes(routes!(api::get_proofs_by_height))
            .routes(routes!(api::get_proof_with_hash))
            // blob
            .routes(routes!(api::get_blobs_by_tx_hash))
            .routes(routes!(api::get_blob))
            // contract
            .routes(routes!(api::list_contracts))
            .routes(routes!(api::get_contract))
            .routes(routes!(api::get_contract_state_by_height))
            .split_for_parts();

        if let Some(ctx) = ctx {
            if let Ok(mut o) = ctx.openapi.lock() {
                *o = o.clone().nest("/v1/indexer", api);
            }
        }

        router.with_state(self.state.clone())
    }

    async fn get_blob_transactions_by_contract_ws_handler(
        ws: WebSocketUpgrade,
        Path(contract_name): Path<String>,
        State(state): State<IndexerApiState>,
    ) -> impl IntoResponse {
        ws.on_upgrade(move |socket| {
            Self::get_blob_transactions_by_contract_ws(socket, contract_name, state.new_sub_sender)
        })
    }

    async fn get_blob_transactions_by_contract_ws(
        socket: WebSocket,
        contract_name: String,
        new_sub_sender: mpsc::Sender<(ContractName, WebSocket)>,
    ) {
        // TODO: properly handle errors and ws messages
        _ = new_sub_sender
            .send((ContractName(contract_name), socket))
            .await;
    }

    async fn handle_node_state_event(&mut self, event: NodeStateEvent) -> Result<(), Error> {
        match event {
            NodeStateEvent::NewBlock(block) => self.handle_processed_block(*block).await,
        }
    }

    async fn handle_mempool_status_event(&mut self, event: MempoolStatusEvent) -> Result<()> {
        let mut transaction = self.state.db.begin().await?;
        match event {
            MempoolStatusEvent::WaitingDissemination {
                parent_data_proposal_hash,
                tx,
            } => {
                let parent_data_proposal_hash_db: DataProposalHashDb =
                    parent_data_proposal_hash.into();
                let tx_hash: TxHash = tx.hashed();
                let version = i32::try_from(tx.version)
                    .map_err(|_| anyhow::anyhow!("Tx version is too large to fit into an i32"))?;

                // Insert the transaction into the transactions table
                let tx_type = TransactionTypeDb::from(&tx);
                let tx_hash: &TxHashDb = &tx_hash.into();

                // If the TX is already present, we can assume it's more up-to-date so do nothing.
                sqlx::query(
                    "INSERT INTO transactions (tx_hash, parent_dp_hash, version, transaction_type, transaction_status)
                    VALUES ($1, $2, $3, $4, 'waiting_dissemination')
                    ON CONFLICT(tx_hash, parent_dp_hash) DO NOTHING",
                )
                    .bind(tx_hash)
                    .bind(parent_data_proposal_hash_db.clone())
                    .bind(version)
                    .bind(tx_type)
                    .execute(&mut *transaction)
                    .await?;

                _ = log_warn!(
                    self.insert_tx_data(
                        transaction.deref_mut(),
                        tx_hash,
                        &tx,
                        parent_data_proposal_hash_db,
                    )
                    .await,
                    "Inserting tx data at status 'waiting dissemination'"
                );
            }

            MempoolStatusEvent::DataProposalCreated {
                data_proposal_hash: _,
                txs_metadatas,
            } => {
                let mut seen = HashSet::new();
                let unique_txs_metadatas: Vec<_> = txs_metadatas
                    .into_iter()
                    .filter(|value| {
                        let key = (value.id.1.clone(), value.id.0.clone());
                        seen.insert(key)
                    })
                    .collect();

                let mut query_builder = QueryBuilder::new("INSERT INTO transactions (tx_hash, parent_dp_hash, version, transaction_type, transaction_status)");

                query_builder.push_values(unique_txs_metadatas, |mut b, value| {
                    let tx_type: TransactionTypeDb = value.transaction_kind.into();
                    let version = log_error!(
                        i32::try_from(value.version).map_err(|_| anyhow::anyhow!(
                            "Tx version is too large to fit into an i32"
                        )),
                        "Converting version number into i32"
                    )
                    .unwrap_or(0);

                    let tx_hash: TxHashDb = value.id.1.into();
                    let parent_data_proposal_hash_db: DataProposalHashDb = value.id.0.into();

                    b.push_bind(tx_hash)
                        .push_bind(parent_data_proposal_hash_db)
                        .push_bind(version)
                        .push_bind(tx_type)
                        .push_bind(TransactionStatusDb::DataProposalCreated);
                });

                // If the TX is already present, we try to update its status, only if the status is lower ('waiting_dissemination').
                query_builder.push(" ON CONFLICT(tx_hash, parent_dp_hash) DO UPDATE SET ");

                query_builder.push("transaction_status=");
                query_builder.push_bind(TransactionStatusDb::DataProposalCreated);
                query_builder
                    .push(" WHERE transactions.transaction_status='waiting_dissemination'");

                query_builder
                    .build()
                    .execute(transaction.deref_mut())
                    .await
                    .context("Upserting data at status data_proposal_created")?;
            }
        }

        transaction.commit().await?;

        Ok(())
    }

    async fn insert_block<'a, T: PgExecutor<'a>>(
        &mut self,
        transaction: T,
        block: &Block,
    ) -> Result<()> {
        // Insert the block into the blocks table
        let block_hash = &block.hash;
        let block_height = i64::try_from(block.block_height.0)
            .map_err(|_| anyhow::anyhow!("Block height is too large to fit into an i64"))?;
        let total_txs = block.txs.len() as i64;

        let block_timestamp =
            into_utc_date_time(&block.block_timestamp).context("Block's timestamp is incorrect")?;

        sqlx::query(
            "INSERT INTO blocks (hash, parent_hash, height, timestamp, total_txs) VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(block_hash)
        .bind(block.parent_hash.clone())
        .bind(block_height)
        .bind(block_timestamp)
        .bind(total_txs)
        .execute(transaction)
        .await?;

        Ok(())
    }

    async fn insert_tx_data<'a, T>(
        &mut self,
        transaction: T,
        tx_hash: &TxHashDb,
        tx: &Transaction,
        parent_data_proposal_hash: DataProposalHashDb,
    ) -> Result<()>
    where
        T: PgExecutor<'a>,
    {
        match &tx.transaction_data {
            TransactionData::Blob(blob_tx) => {
                let mut query_builder = QueryBuilder::<Postgres>::new(
                        "INSERT INTO blobs (tx_hash, parent_dp_hash, blob_index, identity, contract_name, data, verified) ",
                    );

                query_builder.push_values(
                    blob_tx.blobs.iter().enumerate(),
                    |mut b, (blob_index, blob)| {
                        let blob_index = log_error!(
                            i32::try_from(blob_index),
                            "Blob index is too large to fit into an i32"
                        )
                        .unwrap_or_default();

                        let identity = &blob_tx.identity.0;
                        let contract_name = &blob.contract_name.0;
                        let blob_data = &blob.data.0;
                        b.push_bind(tx_hash)
                            .push_bind(parent_data_proposal_hash.clone())
                            .push_bind(blob_index)
                            .push_bind(identity)
                            .push_bind(contract_name)
                            .push_bind(blob_data)
                            .push_bind(false);
                    },
                );

                query_builder.push(" ON CONFLICT DO NOTHING");

                let query = query_builder.build();
                query.execute(transaction).await?;
            }
            TransactionData::VerifiedProof(tx_data) => {
                // Then insert the proof in to the proof table.
                match &tx_data.proof {
                    Some(proof_data) => {
                        sqlx::query("INSERT INTO proofs (parent_dp_hash, tx_hash, proof) VALUES ($1, $2, $3) ON CONFLICT(parent_dp_hash, tx_hash) DO NOTHING")
                            .bind(parent_data_proposal_hash)
                            .bind(tx_hash)
                            .bind(proof_data.0.clone())
                            .execute(transaction)
                            .await?;
                    }
                    None => {
                        tracing::trace!(
                            "Verified proof TX {:?} does not contain a proof",
                            &tx_hash
                        );
                    }
                };
            }
            _ => {
                bail!("Unsupported transaction type");
            }
        }
        Ok(())
    }

    async fn handle_processed_block(&mut self, block: Block) -> Result<(), Error> {
        trace!("Indexing block at height {:?}", block.block_height);
        let mut transaction: sqlx::PgTransaction = self.state.db.begin().await?;

        self.insert_block(transaction.deref_mut(), &block).await?;
        let block_height = i64::try_from(block.block_height.0)
            .map_err(|_| anyhow::anyhow!("Block height is too large to fit into an i64"))?;

        let mut i: i32 = 0;
        #[allow(clippy::explicit_counter_loop)]
        for (tx_id, tx) in block.txs {
            let version = i32::try_from(tx.version)
                .map_err(|_| anyhow::anyhow!("Tx version is too large to fit into an i32"))?;

            // Insert the transaction into the transactions table
            let tx_type = TransactionTypeDb::from(&tx);
            let tx_status = match tx.transaction_data {
                TransactionData::Blob(_) => TransactionStatusDb::Sequenced,
                TransactionData::Proof(_) => TransactionStatusDb::Success,
                TransactionData::VerifiedProof(_) => TransactionStatusDb::Success,
            };

            let lane_id: LaneIdDb = block
                .lane_ids
                .get(&tx_id.1)
                .context(format!("No lane id present for tx {}", tx_id))?
                .clone()
                .into();

            let parent_data_proposal_hash: &DataProposalHashDb = &tx_id.0.into();
            let tx_hash: &TxHashDb = &tx_id.1.into();

            // Make sure transaction exists (Missed Mempool Status event)
            log_warn!(sqlx::query(
                "INSERT INTO transactions (tx_hash, parent_dp_hash, version, transaction_type, transaction_status, block_hash, block_height, index, lane_id)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                ON CONFLICT(tx_hash, parent_dp_hash) DO UPDATE SET transaction_status=$5, block_hash=$6, block_height=$7, index=$8",
            )
            .bind(tx_hash)
            .bind(parent_data_proposal_hash.clone())
            .bind(version)
            .bind(tx_type.clone())
            .bind(tx_status.clone())
            .bind(block.hash.clone())
            .bind(block_height)
            .bind(i)
            .bind(lane_id)
            .execute(&mut *transaction)
            .await,
            "Inserting transaction {:?}", tx_hash)?;

            _ = log_warn!(
                self.insert_tx_data(
                    transaction.deref_mut(),
                    tx_hash,
                    &tx,
                    parent_data_proposal_hash.clone(),
                )
                .await,
                "Inserting tx data when tx in block"
            );

            if let TransactionData::Blob(blob_tx) = &tx.transaction_data {
                // Send the transaction to all websocket subscribers
                self.send_blob_transaction_to_websocket_subscribers(
                    blob_tx,
                    tx_hash,
                    parent_data_proposal_hash,
                    &block.hash,
                    i as u32,
                    tx.version,
                );
            }

            i += 1;
        }

        for (i, (tx_hash, events)) in (0..).zip(block.transactions_events.into_iter()) {
            let tx_hash_db: &TxHashDb = &tx_hash.clone().into();
            let parent_data_proposal_hash: DataProposalHashDb = block
                .dp_parent_hashes
                .get(&tx_hash)
                .context(format!(
                    "No parent data proposal hash present for tx {}",
                    &tx_hash
                ))?
                .clone()
                .into();
            let serialized_events = serde_json::to_string(&events)?;
            debug!("Inserting transaction state event {tx_hash}: {serialized_events}");

            log_warn!(sqlx::query(
                "INSERT INTO transaction_state_events (block_hash, block_height, index, tx_hash, parent_dp_hash, events)
                VALUES ($1, $2, $3, $4, $5, $6::jsonb)",
            )
            .bind(block.hash.clone())
            .bind(block_height)
            .bind(i)
            .bind(tx_hash_db)
            .bind(parent_data_proposal_hash)
            .bind(serialized_events)
            .execute(&mut *transaction)
            .await,
            "Inserting transaction state event {:?}", tx_hash)?;
        }

        // Handling new stakers
        for _staker in block.staking_actions {
            // TODO: add new table with stakers at a given height
        }

        // Handling settled blob transactions
        for settled_blob_tx_hash in block.successful_txs {
            let dp_hash_db: DataProposalHashDb = block
                .dp_parent_hashes
                .get(&settled_blob_tx_hash)
                .context(format!(
                    "No parent data proposal hash present for settled blob tx {}",
                    settled_blob_tx_hash.0
                ))?
                .clone()
                .into();
            let tx_hash: &TxHashDb = &settled_blob_tx_hash.into();
            sqlx::query("UPDATE transactions SET transaction_status = $1 WHERE tx_hash = $2 AND parent_dp_hash = $3")
                .bind(TransactionStatusDb::Success)
                .bind(tx_hash)
                .bind(dp_hash_db)
                .execute(&mut *transaction)
                .await?;
        }

        for failed_blob_tx_hash in block.failed_txs {
            let dp_hash_db: DataProposalHashDb = block
                .dp_parent_hashes
                .get(&failed_blob_tx_hash)
                .context(format!(
                    "No parent data proposal hash present for failed blob tx {}",
                    failed_blob_tx_hash.0
                ))?
                .clone()
                .into();
            let tx_hash: &TxHashDb = &failed_blob_tx_hash.into();
            sqlx::query("UPDATE transactions SET transaction_status = $1 WHERE tx_hash = $2 AND parent_dp_hash = $3")
                .bind(TransactionStatusDb::Failure)
                .bind(tx_hash)
                .bind(dp_hash_db)
                .execute(&mut *transaction)
                .await?;
        }

        // Handling timed out blob transactions
        for timed_out_tx_hash in block.timed_out_txs {
            let dp_hash_db: DataProposalHashDb = block
                .dp_parent_hashes
                .get(&timed_out_tx_hash)
                .context(format!(
                    "No parent data proposal hash present for timed out tx {}",
                    timed_out_tx_hash.0
                ))?
                .clone()
                .into();
            let tx_hash: &TxHashDb = &timed_out_tx_hash.into();
            sqlx::query("UPDATE transactions SET transaction_status = $1 WHERE tx_hash = $2 AND parent_dp_hash = $3")
                .bind(TransactionStatusDb::TimedOut)
                .bind(tx_hash)
                .bind(dp_hash_db)
                .execute(&mut *transaction)
                .await?;
        }

        for handled_blob_proof_output in block.blob_proof_outputs {
            let proof_dp_hash: DataProposalHashDb = block
                .dp_parent_hashes
                .get(&handled_blob_proof_output.proof_tx_hash)
                .context(format!(
                    "No parent data proposal hash present for proof tx {}",
                    handled_blob_proof_output.proof_tx_hash.0
                ))?
                .clone()
                .into();
            let blob_dp_hash: DataProposalHashDb = block
                .dp_parent_hashes
                .get(&handled_blob_proof_output.blob_tx_hash)
                .context(format!(
                    "No parent data proposal hash present for blob tx {}",
                    handled_blob_proof_output.blob_tx_hash.0
                ))?
                .clone()
                .into();
            let proof_tx_hash: &TxHashDb = &handled_blob_proof_output.proof_tx_hash.into();
            let blob_tx_hash: &TxHashDb = &handled_blob_proof_output.blob_tx_hash.into();
            let blob_index = i32::try_from(handled_blob_proof_output.blob_index.0)
                .map_err(|_| anyhow::anyhow!("Blob index is too large to fit into an i32"))?;
            let blob_proof_output_index =
                i32::try_from(handled_blob_proof_output.blob_proof_output_index).map_err(|_| {
                    anyhow::anyhow!("Blob proof output index is too large to fit into an i32")
                })?;
            let serialized_hyle_output =
                serde_json::to_string(&handled_blob_proof_output.hyle_output)?;

            sqlx::query(
                "INSERT INTO blob_proof_outputs (proof_tx_hash, proof_parent_dp_hash, blob_tx_hash, blob_parent_dp_hash, blob_index, blob_proof_output_index, contract_name, hyle_output, settled)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, false)",
            )
            .bind(proof_tx_hash)
            .bind(proof_dp_hash)
            .bind(blob_tx_hash)
            .bind(blob_dp_hash)
            .bind(blob_index)
            .bind(blob_proof_output_index)
            .bind(handled_blob_proof_output.contract_name.0)
            .bind(serialized_hyle_output)
            .execute(&mut *transaction)
            .await?;
        }

        // Handling verified blob (! must come after blob proof output, as it updates that)
        for (blob_tx_hash, blob_index, blob_proof_output_index) in block.verified_blobs {
            let blob_tx_parent_dp_hash: DataProposalHashDb = block
                .dp_parent_hashes
                .get(&blob_tx_hash)
                .context(format!(
                    "No parent data proposal hash present for verified blob tx {}",
                    blob_tx_hash.0
                ))?
                .clone()
                .into();
            let blob_tx_hash: &TxHashDb = &blob_tx_hash.into();
            let blob_index = i32::try_from(blob_index.0)
                .map_err(|_| anyhow::anyhow!("Blob index is too large to fit into an i32"))?;

            sqlx::query("UPDATE blobs SET verified = true WHERE tx_hash = $1 AND parent_dp_hash = $2 AND blob_index = $3")
                .bind(blob_tx_hash)
                .bind(blob_tx_parent_dp_hash.clone())
                .bind(blob_index)
                .execute(&mut *transaction)
                .await?;

            if let Some(blob_proof_output_index) = blob_proof_output_index {
                let blob_proof_output_index =
                    i32::try_from(blob_proof_output_index).map_err(|_| {
                        anyhow::anyhow!("Blob proof output index is too large to fit into an i32")
                    })?;

                sqlx::query("UPDATE blob_proof_outputs SET settled = true WHERE blob_tx_hash = $1 AND blob_parent_dp_hash = $2 AND blob_index = $3 AND blob_proof_output_index = $4")
                    .bind(blob_tx_hash)
                    .bind(blob_tx_parent_dp_hash)
                    .bind(blob_index)
                    .bind(blob_proof_output_index)
                    .execute(&mut *transaction)
                    .await?;
            }
        }

        // After TXes as it refers to those (for now)
        for (tx_hash, contract, _) in block.registered_contracts {
            let verifier = &contract.verifier.0;
            let program_id = &contract.program_id.0;
            let state_commitment = &contract.state_commitment.0;
            let contract_name = &contract.contract_name.0;
            let tx_parent_dp_hash: DataProposalHashDb = block
                .dp_parent_hashes
                .get(&tx_hash)
                .context(format!(
                    "No parent data proposal hash present for registered contract tx {}",
                    tx_hash.0
                ))?
                .clone()
                .into();
            let tx_hash: &TxHashDb = &tx_hash.into();

            // Adding to Contract table
            sqlx::query(
                "INSERT INTO contracts (tx_hash, parent_dp_hash, verifier, program_id, state_commitment, contract_name)
                VALUES ($1, $2, $3, $4, $5, $6)",
            )
            .bind(tx_hash)
            .bind(tx_parent_dp_hash)
            .bind(verifier)
            .bind(program_id)
            .bind(state_commitment)
            .bind(contract_name)
            .execute(&mut *transaction)
            .await?;

            // Adding to ContractState table
            sqlx::query(
                "INSERT INTO contract_state (contract_name, block_hash, state_commitment)
                VALUES ($1, $2, $3)",
            )
            .bind(contract_name)
            .bind(block.hash.clone())
            .bind(state_commitment)
            .execute(&mut *transaction)
            .await?;
        }

        // Handling updated contract state
        for (contract_name, state_commitment) in block.updated_states {
            let contract_name = &contract_name.0;
            let state_commitment = &state_commitment.0;
            sqlx::query(
                "UPDATE contract_state SET state_commitment = $1 WHERE contract_name = $2 AND block_hash = $3",
            )
            .bind(state_commitment.clone())
            .bind(contract_name.clone())
            .bind(block.hash.clone())
            .execute(&mut *transaction)
            .await?;

            sqlx::query("UPDATE contracts SET state_commitment = $1 WHERE contract_name = $2")
                .bind(state_commitment)
                .bind(contract_name)
                .execute(&mut *transaction)
                .await?;
        }

        // Commit the transaction
        transaction.commit().await?;

        tracing::debug!("Indexed block at height {:?}", block.block_height);

        Ok(())
    }

    fn send_blob_transaction_to_websocket_subscribers(
        &self,
        tx: &BlobTransaction,
        tx_hash: &TxHashDb,
        dp_hash: &DataProposalHashDb,
        block_hash: &ConsensusProposalHash,
        index: u32,
        version: u32,
    ) {
        for (contrat_name, senders) in self.subscribers.iter() {
            if tx
                .blobs
                .iter()
                .any(|blob| &blob.contract_name == contrat_name)
            {
                let enriched_tx = TransactionWithBlobs {
                    tx_hash: tx_hash.0.clone(),
                    parent_dp_hash: dp_hash.0.clone(),
                    block_hash: block_hash.clone(),
                    index,
                    version,
                    transaction_type: TransactionTypeDb::BlobTransaction,
                    transaction_status: TransactionStatusDb::Sequenced,
                    identity: tx.identity.0.clone(),
                    blobs: tx
                        .blobs
                        .iter()
                        .map(|blob| BlobWithStatus {
                            contract_name: blob.contract_name.0.clone(),
                            data: blob.data.0.clone(),
                            proof_outputs: vec![],
                        })
                        .collect(),
                };
                senders.iter().for_each(|sender| {
                    let _ = sender.send(enriched_tx.clone());
                });
            }
        }
    }
}

impl std::ops::Deref for Indexer {
    type Target = Pool<Postgres>;

    fn deref(&self) -> &Self::Target {
        &self.state.db
    }
}

pub fn into_utc_date_time(ts: &TimestampMs) -> Result<DateTime<Utc>> {
    DateTime::from_timestamp_millis(ts.0.try_into().context("Converting u64 into i64")?)
        .context("Converting i64 into UTC DateTime")
}

#[cfg(test)]
mod test {
    use assert_json_diff::assert_json_include;
    use axum_test::TestServer;
    use hyle_contract_sdk::{BlobIndex, HyleOutput, Identity, ProgramId, StateCommitment, TxHash};
    use hyle_model::api::{APIBlob, APIBlock, APIContract, APITransaction, APITransactionEvents};
    use serde_json::json;
    use std::future::IntoFuture;
    use utils::TimestampMs;

    use crate::{
        bus::SharedMessageBus,
        model::{
            Blob, BlobData, BlobProofOutput, ProofData, SignedBlock, Transaction, TransactionData,
            VerifiedProofTransaction,
        },
        node_state::{metrics::NodeStateMetrics, NodeState, NodeStateStore},
    };

    use super::*;

    use sqlx::postgres::PgPoolOptions;
    use testcontainers_modules::{
        postgres::Postgres,
        testcontainers::{runners::AsyncRunner, ImageExt},
    };

    async fn setup_test_server(indexer: &Indexer) -> Result<TestServer> {
        let router = indexer.api(None);
        TestServer::new(router)
    }

    async fn new_indexer(pool: PgPool) -> Indexer {
        let (new_sub_sender, new_sub_receiver) = tokio::sync::mpsc::channel(100);

        Indexer {
            bus: IndexerBusClient::new_from_bus(SharedMessageBus::default()).await,
            state: IndexerApiState {
                db: pool,
                new_sub_sender,
            },
            new_sub_receiver,
            subscribers: HashMap::new(),
        }
    }

    fn new_register_tx(
        contract_name: ContractName,
        state_commitment: StateCommitment,
    ) -> BlobTransaction {
        BlobTransaction::new(
            "hyle@hyle",
            vec![RegisterContractAction {
                verifier: "test".into(),
                program_id: ProgramId(vec![3, 2, 1]),
                state_commitment,
                contract_name,
                ..Default::default()
            }
            .as_blob("hyle".into(), None, None)],
        )
    }

    fn new_blob_tx(
        identity: Identity,
        first_contract_name: ContractName,
        second_contract_name: ContractName,
    ) -> Transaction {
        Transaction {
            version: 1,
            transaction_data: TransactionData::Blob(BlobTransaction::new(
                identity,
                vec![
                    Blob {
                        contract_name: first_contract_name,
                        data: BlobData(vec![1, 2, 3]),
                    },
                    Blob {
                        contract_name: second_contract_name,
                        data: BlobData(vec![1, 2, 3]),
                    },
                ],
            )),
        }
    }

    fn new_proof_tx(
        identity: Identity,
        contract_name: ContractName,
        blob_index: BlobIndex,
        blob_transaction: &Transaction,
        initial_state: StateCommitment,
        next_state: StateCommitment,
    ) -> Transaction {
        let TransactionData::Blob(blob_tx) = &blob_transaction.transaction_data else {
            panic!("Expected BlobTransaction");
        };
        let proof = ProofData(initial_state.0.clone());
        Transaction {
            version: 1,
            transaction_data: TransactionData::VerifiedProof(VerifiedProofTransaction {
                contract_name: contract_name.clone(),
                proof_hash: proof.hashed(),
                proof_size: proof.0.len(),
                proven_blobs: vec![BlobProofOutput {
                    original_proof_hash: proof.hashed(),
                    program_id: ProgramId(vec![3, 2, 1]),
                    blob_tx_hash: blob_transaction.hashed(),
                    hyle_output: HyleOutput {
                        version: 1,
                        initial_state,
                        next_state,
                        identity,
                        tx_hash: blob_transaction.hashed(),
                        tx_ctx: None,
                        index: blob_index,
                        tx_blob_count: blob_tx.blobs.len(),
                        blobs: blob_tx.blobs.clone().into(),
                        success: true,
                        state_reads: vec![],
                        onchain_effects: vec![],
                        program_outputs: vec![],
                    },
                }],
                is_recursive: false,
                proof: Some(proof),
            }),
        }
    }

    async fn assert_tx_status(
        server: &TestServer,
        tx_hash: TxHash,
        tx_status: TransactionStatusDb,
    ) {
        let transactions_response = server
            .get(format!("/transaction/hash/{}", tx_hash).as_str())
            .await;
        transactions_response.assert_status_ok();
        let json_response = transactions_response.json::<APITransaction>();
        assert_eq!(json_response.transaction_status, tx_status);
    }

    async fn assert_tx_not_found(server: &TestServer, tx_hash: TxHash) {
        let transactions_response = server
            .get(format!("/transaction/hash/{}", tx_hash).as_str())
            .await;
        transactions_response.assert_status_not_found();
    }

    #[test_log::test(tokio::test)]
    async fn test_indexer_handle_block_flow() -> Result<()> {
        let container = Postgres::default()
            .with_tag("17-alpine")
            .start()
            .await
            .unwrap();
        let db = PgPoolOptions::new()
            .max_connections(5)
            .connect(&format!(
                "postgresql://postgres:postgres@localhost:{}/postgres",
                container.get_host_port_ipv4(5432).await.unwrap()
            ))
            .await
            .unwrap();
        MIGRATOR.run(&db).await.unwrap();

        let mut indexer = new_indexer(db).await;
        let server = setup_test_server(&indexer).await?;

        let initial_state = StateCommitment(vec![1, 2, 3]);
        let next_state = StateCommitment(vec![4, 5, 6]);
        let first_contract_name = ContractName::new("c1");
        let second_contract_name = ContractName::new("c2");

        let register_tx_1 = new_register_tx(first_contract_name.clone(), initial_state.clone());
        let register_tx_2 = new_register_tx(second_contract_name.clone(), initial_state.clone());

        let blob_transaction = new_blob_tx(
            Identity::new("test@c1"),
            first_contract_name.clone(),
            second_contract_name.clone(),
        );
        let blob_transaction_hash = blob_transaction.hashed();

        let proof_tx_1 = new_proof_tx(
            Identity::new("test@c1"),
            first_contract_name.clone(),
            BlobIndex(0),
            &blob_transaction,
            initial_state.clone(),
            next_state.clone(),
        );

        let proof_tx_2 = new_proof_tx(
            Identity::new("test@c1"),
            second_contract_name.clone(),
            BlobIndex(1),
            &blob_transaction,
            initial_state.clone(),
            next_state.clone(),
        );

        let other_blob_transaction = new_blob_tx(
            Identity::new("test@c1"),
            second_contract_name.clone(),
            first_contract_name.clone(),
        );
        let other_blob_transaction_hash = other_blob_transaction.hashed();
        // Send two proofs for the same blob
        let proof_tx_3 = new_proof_tx(
            Identity::new("test@c1"),
            first_contract_name.clone(),
            BlobIndex(1),
            &other_blob_transaction,
            StateCommitment(vec![7, 7, 7]),
            StateCommitment(vec![9, 9, 9]),
        );
        let proof_tx_4 = new_proof_tx(
            Identity::new("test@c1"),
            first_contract_name.clone(),
            BlobIndex(1),
            &other_blob_transaction,
            StateCommitment(vec![8, 8]),
            StateCommitment(vec![9, 9]),
        );

        let txs = vec![
            register_tx_1.into(),
            register_tx_2.into(),
            blob_transaction,
            proof_tx_1,
            proof_tx_2,
            other_blob_transaction,
            proof_tx_3,
            proof_tx_4,
        ];

        let mut node_state = NodeState {
            store: NodeStateStore::default(),
            metrics: NodeStateMetrics::global("test".to_string(), "test"),
        };

        // Handling a block containing txs

        let parent_data_proposal = DataProposal::new(None, txs);
        let mut signed_block = SignedBlock::default();
        signed_block.consensus_proposal.slot = 1;
        signed_block.data_proposals.push((
            LaneId(ValidatorPublicKey("ttt".into())),
            vec![parent_data_proposal.clone()],
        ));
        let block = node_state.force_handle_block(&signed_block);

        indexer
            .handle_processed_block(block)
            .await
            .expect("Failed to handle block");

        //
        // Handling MempoolStatusEvent
        //

        let initial_state_wd = StateCommitment(vec![1, 2, 3]);
        let next_state_wd = StateCommitment(vec![4, 5, 6]);
        let first_contract_name_wd = ContractName::new("wd1");
        let second_contract_name_wd = ContractName::new("wd2");

        let register_tx_1_wd =
            new_register_tx(first_contract_name_wd.clone(), initial_state_wd.clone());
        let register_tx_2_wd =
            new_register_tx(second_contract_name_wd.clone(), initial_state_wd.clone());

        let blob_transaction_wd = new_blob_tx(
            Identity::new("test@wd1"),
            first_contract_name_wd.clone(),
            second_contract_name_wd.clone(),
        );

        let proof_tx_1_wd = new_proof_tx(
            Identity::new("test@wd1"),
            first_contract_name_wd.clone(),
            BlobIndex(0),
            &blob_transaction_wd,
            initial_state_wd.clone(),
            next_state_wd.clone(),
        );

        let register_tx_1_wd = Transaction {
            version: 1,
            transaction_data: TransactionData::Blob(register_tx_1_wd),
        };
        let register_tx_2_wd = Transaction {
            version: 1,
            transaction_data: TransactionData::Blob(register_tx_2_wd),
        };

        indexer
            .handle_mempool_status_event(MempoolStatusEvent::WaitingDissemination {
                parent_data_proposal_hash: parent_data_proposal.hashed(),
                tx: register_tx_1_wd.clone(),
            })
            .await
            .expect("MempoolStatusEvent");

        assert_tx_status(
            &server,
            register_tx_1_wd.hashed(),
            TransactionStatusDb::WaitingDissemination,
        )
        .await;

        indexer
            .handle_mempool_status_event(MempoolStatusEvent::WaitingDissemination {
                parent_data_proposal_hash: parent_data_proposal.hashed(),
                tx: register_tx_2_wd.clone(),
            })
            .await
            .expect("MempoolStatusEvent");

        assert_tx_status(
            &server,
            register_tx_2_wd.hashed(),
            TransactionStatusDb::WaitingDissemination,
        )
        .await;

        indexer
            .handle_mempool_status_event(MempoolStatusEvent::WaitingDissemination {
                parent_data_proposal_hash: parent_data_proposal.hashed(),
                tx: blob_transaction_wd.clone(),
            })
            .await
            .expect("MempoolStatusEvent");

        assert_tx_status(
            &server,
            blob_transaction_wd.hashed(),
            TransactionStatusDb::WaitingDissemination,
        )
        .await;

        assert_tx_not_found(&server, proof_tx_1_wd.hashed()).await;

        let parent_data_proposal_hash = parent_data_proposal.hashed();

        let data_proposal = DataProposal::new(
            Some(parent_data_proposal_hash.clone()),
            vec![
                register_tx_1_wd.clone(),
                register_tx_2_wd.clone(),
                blob_transaction_wd.clone(),
                blob_transaction_wd.clone(),
                proof_tx_1_wd.clone(),
            ],
        );

        let data_proposal_created_event = MempoolStatusEvent::DataProposalCreated {
            data_proposal_hash: data_proposal.hashed(),
            txs_metadatas: vec![
                register_tx_1_wd.metadata(parent_data_proposal_hash.clone()),
                register_tx_2_wd.metadata(parent_data_proposal_hash.clone()),
                blob_transaction_wd.metadata(parent_data_proposal_hash.clone()),
                blob_transaction_wd.metadata(parent_data_proposal_hash.clone()),
                proof_tx_1_wd.metadata(parent_data_proposal_hash.clone()),
            ],
        };

        indexer
            .handle_mempool_status_event(data_proposal_created_event.clone())
            .await
            .expect("MempoolStatusEvent");

        assert_tx_status(
            &server,
            register_tx_1_wd.hashed(),
            TransactionStatusDb::DataProposalCreated,
        )
        .await;
        assert_tx_status(
            &server,
            register_tx_2_wd.hashed(),
            TransactionStatusDb::DataProposalCreated,
        )
        .await;
        assert_tx_status(
            &server,
            blob_transaction_wd.hashed(),
            TransactionStatusDb::DataProposalCreated,
        )
        .await;
        assert_tx_not_found(&server, proof_tx_1_wd.hashed()).await;

        let mut signed_block = SignedBlock::default();
        signed_block.consensus_proposal.timestamp = TimestampMs(12345);
        signed_block.consensus_proposal.slot = 2;
        signed_block.data_proposals.push((
            LaneId(ValidatorPublicKey("ttt".into())),
            vec![data_proposal],
        ));
        let block_2 = node_state.force_handle_block(&signed_block);
        let block_2_hash = block_2.hash.clone();
        indexer
            .handle_processed_block(block_2)
            .await
            .expect("Failed to handle block");

        assert_tx_status(
            &server,
            register_tx_1_wd.hashed(),
            TransactionStatusDb::Success,
        )
        .await;
        assert_tx_status(
            &server,
            register_tx_2_wd.hashed(),
            TransactionStatusDb::Success,
        )
        .await;
        assert_tx_status(
            &server,
            blob_transaction_wd.hashed(),
            TransactionStatusDb::Sequenced,
        )
        .await;
        assert_tx_not_found(&server, proof_tx_1_wd.hashed()).await;

        // Check a mempool status event does not change a Success/Sequenced status
        indexer
            .handle_mempool_status_event(data_proposal_created_event.clone())
            .await
            .expect("MempoolStatusEvent");

        // Check blocks have correct data
        let blocks = server.get("/blocks").await.json::<Vec<APIBlock>>();
        assert_eq!(blocks.len(), 2);
        assert_eq!(blocks.last().unwrap().timestamp, 0);
        assert_eq!(blocks.first().unwrap().timestamp, 12345);

        let transactions_response = server.get("/contract/c1").await;
        transactions_response.assert_status_ok();
        let json_response = transactions_response.json::<APIContract>();
        assert_eq!(json_response.state_commitment, next_state.0);

        let transactions_response = server.get("/contract/c2").await;
        transactions_response.assert_status_ok();
        let json_response = transactions_response.json::<APIContract>();
        assert_eq!(json_response.state_commitment, next_state.0);

        let transactions_response = server.get("/contract/d1").await;
        transactions_response.assert_status_not_found();

        let blob_transactions_response = server.get("/blob_transactions/contract/c1").await;
        blob_transactions_response.assert_status_ok();
        assert_json_include!(
            actual: blob_transactions_response.json::<serde_json::Value>(),
            expected: json!([
                {
                    "blobs": [{
                        "contract_name": "c1",
                        "data": hex::encode([1,2,3]),
                        "proof_outputs": [
                            {
                                "initial_state": [7,7,7],
                            },
                            {
                                "initial_state": [8,8],
                            }
                        ]
                    }],
                    "transaction_status": "Sequenced",
                    "tx_hash": other_blob_transaction_hash.to_string(),
                    "index": 5,
                },
                {
                    "blobs": [{
                        "contract_name": "c1",
                        "data": hex::encode([1,2,3]),
                        "proof_outputs": [{}]
                    }],
                    "tx_hash": blob_transaction_hash.to_string(),
                    "index": 2,
                }
            ])
        );
        let all_txs = server.get("/transactions/block/1").await;
        all_txs.assert_status_ok();
        assert_json_include!(
            actual: all_txs.json::<serde_json::Value>(),
            expected: json!([
                { "index": 5, "transaction_type": "BlobTransaction", "transaction_status": "Sequenced" },
                { "index": 2, "transaction_type": "BlobTransaction", "transaction_status": "Success" },
                { "index": 1, "transaction_type": "BlobTransaction", "transaction_status": "Success" },
                { "index": 0, "transaction_type": "BlobTransaction", "transaction_status": "Success" },
            ])
        );

        let blob_transactions_response = server.get("/blob_transactions/contract/c2").await;
        blob_transactions_response.assert_status_ok();
        assert_json_include!(
            actual: blob_transactions_response.json::<serde_json::Value>(),
            expected: json!([
                {
                    "blobs": [{
                        "contract_name": "c2",
                        "data": hex::encode([1,2,3]),
                        "proof_outputs": []
                    }],
                    "tx_hash": other_blob_transaction_hash.to_string(),
                },
                {
                    "blobs": [{
                        "contract_name": "c2",
                        "data": hex::encode([1,2,3]),
                        "proof_outputs": [{}]
                    }],
                    "tx_hash": blob_transaction_hash.to_string(),
                }
            ])
        );

        // Test proof transaction endpoints
        let proofs_response = server.get("/proofs").await;
        proofs_response.assert_status_ok();
        assert_json_include!(
            actual: proofs_response.json::<serde_json::Value>(),
            expected: json!([
                { "index": 4, "transaction_type": "ProofTransaction", "transaction_status": "Success", "block_hash": block_2_hash },
                { "index": 7, "transaction_type": "ProofTransaction", "transaction_status": "Success" },
                { "index": 6, "transaction_type": "ProofTransaction", "transaction_status": "Success" },
                { "index": 4, "transaction_type": "ProofTransaction", "transaction_status": "Success" },
                { "index": 3, "transaction_type": "ProofTransaction", "transaction_status": "Success" },
            ])
        );

        let proofs_by_height = server.get("/proofs/block/1").await;
        proofs_by_height.assert_status_ok();

        assert_json_include!(
            actual: proofs_by_height.json::<serde_json::Value>(),
            expected: json!([
                { "index": 7, "transaction_type": "ProofTransaction", "transaction_status": "Success" },
                { "index": 6, "transaction_type": "ProofTransaction", "transaction_status": "Success" },
                { "index": 4, "transaction_type": "ProofTransaction", "transaction_status": "Success" },
                { "index": 3, "transaction_type": "ProofTransaction", "transaction_status": "Success" },
            ])
        );

        let proof_by_hash = server
            .get(format!("/proof/hash/{}", proof_tx_1_wd.hashed()).as_str())
            .await;
        proof_by_hash.assert_status_ok();
        assert_json_include!(
            actual: proof_by_hash.json::<serde_json::Value>(),
            expected: json!({
                "index": 4,
                "transaction_type": "ProofTransaction",
                "transaction_status": "Success"
            })
        );

        // Test non-existent proof
        let non_existent_proof = server
            .get("/proof/hash/1111111111111111111111111111111111111111111111111111111111111111")
            .await;
        non_existent_proof.assert_status_not_found();

        Ok(())
    }

    // In case of duplicate tx hash, should return information of the tx with the highest block height
    // or index (position in the block)
    #[test_log::test(tokio::test)]
    async fn test_indexer_api_doubles() -> Result<()> {
        let container = Postgres::default()
            .with_tag("17-alpine")
            .start()
            .await
            .unwrap();
        let db = PgPoolOptions::new()
            .max_connections(5)
            .connect(&format!(
                "postgresql://postgres:postgres@localhost:{}/postgres",
                container.get_host_port_ipv4(5432).await.unwrap()
            ))
            .await
            .unwrap();
        MIGRATOR.run(&db).await.unwrap();
        sqlx::raw_sql(include_str!("../tests/fixtures/test_data.sql"))
            .execute(&db)
            .await
            .context("insert test data")?;

        let indexer = new_indexer(db).await;
        let server = setup_test_server(&indexer).await?;

        // Multiple txs with same hash -- all in different blocks

        let transactions_response = server
            .get("/transaction/hash/test_tx_hash_2aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
            .await;
        transactions_response.assert_status_ok();
        let result = transactions_response.json::<APITransaction>();
        assert_eq!(
            result.parent_dp_hash.0,
            "dp_hashbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_string()
        );

        // Multiple txs with same hash -- one not yet in a block, should return the pending one

        let transactions_response = server
            .get("/proof/hash/test_tx_hash_3aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
            .await;
        transactions_response.assert_status_ok();
        let result = transactions_response.json::<APITransaction>();
        assert_eq!(
            result.parent_dp_hash.0,
            "dp_hashbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_string()
        );
        assert_eq!(result.block_hash, None);

        // Get blobs by tx hash

        let transactions_response = server
            .get("/blobs/hash/test_tx_hash_2aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
            .await;
        transactions_response.assert_status_ok();
        let result = transactions_response.json::<Vec<APIBlob>>();
        assert!(result.len() == 1);
        assert_eq!(
            result.first().unwrap().data,
            "{\"data\": \"blob_data_2_bis\"}".as_bytes()
        );

        // Get blob by tx hash

        let transactions_response = server
            .get("/blob/hash/test_tx_hash_2aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/index/0")
            .await;
        transactions_response.assert_status_ok();
        let result = transactions_response.json::<APIBlob>();
        assert_eq!(result.data, "{\"data\": \"blob_data_2_bis\"}".as_bytes());

        // Get proof by tx hash

        let transactions_response = server
            .get("/proof/hash/test_tx_hash_3aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
            .await;
        transactions_response.assert_status_ok();
        let result = transactions_response.json::<APITransaction>();
        assert_eq!(
            result.parent_dp_hash.0,
            "dp_hashbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_string()
        );
        assert_eq!(result.block_hash, None);

        // Get transaction state event, the latest one

        let transactions_response = server
            .get("/transaction/hash/test_tx_hash_2aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/events")
            .await;
        transactions_response.assert_status_ok();
        let result = transactions_response.json::<Vec<APITransactionEvents>>();
        assert_eq!(
            result,
            vec![APITransactionEvents {
                block_hash: ConsensusProposalHash(
                    "block3aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string()
                ),
                block_height: BlockHeight(3),
                events: vec![serde_json::json!({
                    "name": "Success"
                })]
            }]
        );

        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn test_indexer_api() -> Result<()> {
        let container = Postgres::default()
            .with_tag("17-alpine")
            .start()
            .await
            .unwrap();
        let db = PgPoolOptions::new()
            .max_connections(5)
            .connect(&format!(
                "postgresql://postgres:postgres@localhost:{}/postgres",
                container.get_host_port_ipv4(5432).await.unwrap()
            ))
            .await
            .unwrap();
        MIGRATOR.run(&db).await.unwrap();
        sqlx::raw_sql(include_str!("../tests/fixtures/test_data.sql"))
            .execute(&db)
            .await
            .context("insert test data")?;

        let mut indexer = new_indexer(db).await;
        let server = setup_test_server(&indexer).await?;

        // Blocks
        // Get all blocks
        let transactions_response = server.get("/blocks").await;
        transactions_response.assert_status_ok();
        assert!(!transactions_response.text().is_empty());

        // Test pagination
        let transactions_response = server.get("/blocks?nb_results=1").await;
        transactions_response.assert_status_ok();
        assert_eq!(transactions_response.json::<Vec<APIBlock>>().len(), 1);
        assert_eq!(
            transactions_response
                .json::<Vec<APIBlock>>()
                .first()
                .unwrap()
                .height,
            3
        );
        let transactions_response = server.get("/blocks?nb_results=1&start_block=1").await;
        transactions_response.assert_status_ok();
        assert_eq!(transactions_response.json::<Vec<APIBlock>>().len(), 1);
        assert_eq!(
            transactions_response
                .json::<Vec<APIBlock>>()
                .first()
                .unwrap()
                .height,
            1
        );
        // Test negative end of blocks
        let transactions_response = server.get("/blocks?nb_results=10&start_block=4").await;
        transactions_response.assert_status_ok();

        // Get the last block
        let transactions_response = server.get("/block/last").await;
        transactions_response.assert_status_ok();
        assert!(!transactions_response.text().is_empty());

        // Get block by height
        let transactions_response = server.get("/block/height/1").await;
        transactions_response.assert_status_ok();
        assert!(!transactions_response.text().is_empty());

        // Get block by hash
        let transactions_response = server
            .get("/block/hash/block1aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
            .await;
        transactions_response.assert_status_ok();
        assert!(!transactions_response.text().is_empty());

        // Transactions
        // Get all transactions
        let transactions_response = server.get("/transactions").await;
        transactions_response.assert_status_ok();
        assert!(!transactions_response.text().is_empty());

        // Get all transactions by height
        let transactions_response = server.get("/transactions/block/2").await;
        transactions_response.assert_status_ok();
        assert!(!transactions_response.text().is_empty());

        // Get an existing transaction by name
        let transactions_response = server.get("/transactions/contract/contract_1").await;
        transactions_response.assert_status_ok();
        assert!(!transactions_response.text().is_empty());

        // Get an unknown transaction by name
        let transactions_response = server.get("/transactions/contract/unknown_contract").await;
        transactions_response.assert_status_ok();
        assert_eq!(transactions_response.text(), "[]");

        // Get an existing transaction by hash
        let transactions_response = server
            .get("/transaction/hash/test_tx_hash_1aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
            .await;
        transactions_response.assert_status_ok();
        assert!(!transactions_response.text().is_empty());

        // Get an existing transaction, waiting for dissemination by hash
        let transactions_response = server
            .get("/transaction/hash/test_tx_hash_0aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
            .await;
        transactions_response.assert_status_ok();
        assert!(!transactions_response.text().is_empty());

        // Get an unknown transaction by hash
        let unknown_tx = server.get("/transaction/hash/1111111111111111111111111111111111111111111111111111111111111111").await;
        unknown_tx.assert_status_not_found();

        // Blobs
        // Get all transactions for a specific contract name
        let transactions_response = server.get("/blob_transactions/contract/contract_1").await;
        transactions_response.assert_status_ok();
        assert!(!transactions_response.text().is_empty());

        // Get blobs by tx_hash
        let transactions_response = server
            .get("/blobs/hash/test_tx_hash_2aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
            .await;
        transactions_response.assert_status_ok();
        assert!(!transactions_response.text().is_empty());

        // Get unknown blobs by tx_hash
        let transactions_response = server
            .get("/blobs/hash/test_tx_hash_1aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
            .await;
        transactions_response.assert_status_ok();
        assert_eq!(transactions_response.text(), "[]");

        // Get blob by tx_hash and index
        let transactions_response = server
            .get("/blob/hash/test_tx_hash_2aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/index/0")
            .await;
        transactions_response.assert_status_ok();
        assert!(!transactions_response.text().is_empty());

        // Get blob by tx_hash and unknown index
        let transactions_response = server
            .get("/blob/hash/test_tx_hash_2aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/index/1000")
            .await;
        transactions_response.assert_status_not_found();

        // Contracts
        // Get contract by name
        let transactions_response = server.get("/contract/contract_1").await;
        transactions_response.assert_status_ok();
        assert!(!transactions_response.text().is_empty());

        // Get contract state by name and height
        let transactions_response = server.get("/state/contract/contract_1/block/1").await;
        transactions_response.assert_status_ok();
        assert!(!transactions_response.text().is_empty());

        // Websocket
        let listener = hyle_net::net::bind_tcp_listener(0).await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(axum::serve(listener, indexer.api(None)).into_future());

        let _ = tokio_tungstenite::connect_async(format!(
            "ws://{addr}/blob_transactions/contract/contract_1/ws"
        ))
        .await
        .unwrap();

        if let Some(tx) = indexer.new_sub_receiver.recv().await {
            let (contract_name, _) = tx;
            assert_eq!(contract_name, ContractName::new("contract_1"));
        }

        Ok(())
    }
}

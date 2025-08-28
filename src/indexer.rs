//! Index system for historical data.

use crate::{
    explorer::{
        api::{DataProposalHashDb, LaneIdDb, TimeoutWindowDb, TxHashDb},
        WsExplorerBlobTx,
    },
    model::*,
    utils::conf::{Conf, SharedConf},
};
use anyhow::{Context, Error, Result};
use chrono::{DateTime, Utc};
use hyli_model::api::{TransactionStatusDb, TransactionTypeDb};
use hyli_model::utils::TimestampMs;
use hyli_modules::{bus::BusClientSender, node_state::BlockNodeStateCallback};
use hyli_modules::{
    bus::SharedMessageBus,
    log_error, module_handle_messages,
    modules::{gcs_uploader::GCSRequest, module_bus_client, Module, SharedBuildApiCtx},
    node_state::{module::NodeStateModule, NodeState, NodeStateCallback, NodeStateStore, TxEvent},
};
use hyli_net::clock::TimestampMsClock;
use sqlx::{postgres::PgPoolOptions, Acquire, PgPool, Pool, Postgres, QueryBuilder, Row};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    ops::Deref,
};
use tokio::io::ReadBuf;

module_bus_client! {
#[derive(Debug)]
struct IndexerBusClient {
    sender(WsExplorerBlobTx),
    sender(NodeStateEvent),
    sender(GCSRequest),
    receiver(DataEvent),
    receiver(MempoolStatusEvent),
}
}

pub struct Indexer {
    bus: IndexerBusClient,
    db: PgPool,
    node_state: NodeState,
    handler_store: IndexerHandlerStore,
    conf: Conf,
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

        // Load node state from node_state.bin if it exists or create a new default
        let node_state_path = ctx.0.data_directory.join("indexer_node_state.bin");
        let node_state_store =
            NodeStateModule::load_from_disk_or_default::<NodeStateStore>(&node_state_path);

        let mut node_state = NodeState::create(ctx.0.id.clone(), "indexer");
        node_state.store = node_state_store;

        let conf: Conf = ctx.0.deref().clone();

        let indexer = Indexer {
            bus,
            db: pool,
            node_state,
            handler_store: IndexerHandlerStore::default(),
            conf,
        };

        Ok(indexer)
    }

    fn run(&mut self) -> impl futures::Future<Output = Result<()>> + Send {
        self.start()
    }

    async fn persist(&mut self) -> Result<()> {
        NodeStateModule::save_on_disk(
            &self.conf.data_directory.join("indexer_node_state.bin"),
            &self.node_state.store,
        )
        .context("Failed to save node state to disk")?;
        let persisted_da_start_height = BlockHeight(self.node_state.current_height.0 + 1);

        tracing::debug!(
            "Indexer saving DA start height: {}",
            &persisted_da_start_height
        );

        NodeStateModule::save_on_disk(
            &self.conf.data_directory.join("da_start_height.bin"),
            &persisted_da_start_height,
        )
        .context("Failed to save DA start height to disk")?;
        Ok(())
    }
}

impl Indexer {
    pub async fn start(&mut self) -> Result<()> {
        module_handle_messages! {
            on_self self,
            delay_shutdown_until {
                _ = log_error!(self.dump_store_to_db()
                    .await,
                    "Indexer failed to dump store to DB");
                self.empty_store()
            },
            listen<DataEvent> DataEvent::OrderedSignedBlock(signed_block) => {
                _ = log_error!(self.handle_signed_block(signed_block)
                    .await,
                    "Indexer handling node state event");
            }

            listen<MempoolStatusEvent> event => {
                _ = log_error!(self.handle_mempool_status_event(event)
                    .await,
                    "Indexer handling mempool status event");
            }

        };
        Ok(())
    }

    pub async fn get_last_block(&self) -> Result<Option<BlockHeight>> {
        let rows = sqlx::query("SELECT max(height) as max FROM blocks")
            .fetch_one(&self.db)
            .await?;
        Ok(rows
            .try_get("max")
            .map(|m: i64| Some(BlockHeight(m as u64)))
            .unwrap_or(None))
    }
}

impl std::ops::Deref for Indexer {
    type Target = Pool<Postgres>;

    fn deref(&self) -> &Self::Target {
        &self.db
    }
}

#[derive(Default)]
pub(crate) struct IndexerHandlerStore {
    sql_queries: Vec<
        sqlx::query::Query<
            'static,
            Postgres,
            <sqlx::Postgres as sqlx::Database>::Arguments<'static>,
        >,
    >,
    block_height: BlockHeight,
    block_hash: BlockHash,
    last_update: TimestampMs,

    // Intended to be temporary, for CSI & co.
    block_callback: BlockNodeStateCallback,

    blocks: Vec<BlockStore>,
    txs: VecDeque<TxStore>,
    tx_status_update: HashMap<TxId, TransactionStatusDb>,
    tx_events: StreamableData,
    blobs: StreamableData,
    blob_proof_outputs: StreamableData,
    contract_inserts: Vec<ContractInsertStore>,
    contract_updates: HashMap<ContractName, ContractUpdateStore>,
}

impl std::fmt::Debug for IndexerHandlerStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IndexerHandlerStore")
            .field("sql_queries", &self.sql_queries.len())
            .finish()
    }
}

#[derive(Debug)]
pub struct TxStore {
    pub tx_hash: TxHashDb,
    pub dp_hash: DataProposalHashDb,
    pub transaction_type: TransactionTypeDb,
    pub block_hash: Option<ConsensusProposalHash>,
    pub block_height: BlockHeight,
    pub lane_id: Option<LaneIdDb>,
    pub index: i32,
    pub identity: Option<String>,
}

pub struct BlockStore {
    pub block_hash: ConsensusProposalHash,
    pub parent_hash: ConsensusProposalHash,
    pub block_height: BlockHeight,
    pub timestamp: DateTime<Utc>,
    pub total_txs: i64,
}

struct ContractInsertStore {
    pub contract_name: ContractName,
    pub verifier: String,
    pub program_id: Vec<u8>,
    pub timeout_window: TimeoutWindowDb,
    pub state_commitment: Vec<u8>,
    pub parent_dp_hash: DataProposalHashDb,
    pub tx_hash: TxHashDb,
    pub metadata: Option<Vec<u8>>,
}

#[derive(Default)]
struct ContractUpdateStore {
    pub verifier: Option<String>,
    pub program_id: Option<Vec<u8>>,
    pub timeout_window: Option<TimeoutWindowDb>,
    pub state_commitment: Option<Vec<u8>>,
    pub deleted_at_height: Option<i32>,
}

impl Indexer {
    #[cfg(test)]
    pub(crate) async fn force_handle_signed_block(
        &mut self,
        block: SignedBlock,
    ) -> Result<(), Error> {
        self.node_state.current_height = BlockHeight(block.height().0 - 1);
        self.handle_signed_block(block).await
    }

    pub async fn handle_signed_block(&mut self, block: SignedBlock) -> Result<(), Error> {
        {
            self.handler_store.block_height = block.height();
            self.handler_store.block_hash = block.hashed();

            self.handler_store.blocks.push(BlockStore {
                block_hash: self.handler_store.block_hash.clone(),
                parent_hash: block.parent_hash().clone(),
                block_height: self.handler_store.block_height,
                timestamp: into_utc_date_time(&block.consensus_proposal.timestamp)
                    .unwrap_or_else(|_| Utc::now()),
                total_txs: block.count_txs() as i64,
            });

            self.node_state
                .process_signed_block(&block, &mut self.handler_store)?;

            // We use the indexer as node-state-processor for CSI
            // TODO: refactor this away it conflicts with running the indexer in the full node as we send all events twice.
            let (parsed_block, staking_data) = self.handler_store.block_callback.take();
            self.bus.send(NodeStateEvent::NewBlock(NodeStateBlock {
                signed_block: block.into(),
                parsed_block: parsed_block.into(),
                staking_data: staking_data.into(),
            }))?;
        }

        // if last block is newer than 5sec dump store to db
        let now = TimestampMsClock::now();
        if self.handler_store.tx_events.0.len() > self.conf.indexer.query_buffer_size
            || self.handler_store.last_update.0 + 5000 < now.0
        {
            log_error!(self.dump_store_to_db().await, "dumping to DB")?;
            self.handler_store.last_update = now;
        }

        Ok(())
    }

    pub async fn handle_mempool_status_event(&mut self, event: MempoolStatusEvent) -> Result<()> {
        match event {
            MempoolStatusEvent::WaitingDissemination {
                parent_data_proposal_hash,
                tx,
            } => {
                self.handler_store.txs.push_front(TxStore {
                    tx_hash: TxHashDb(tx.hashed().clone()),
                    dp_hash: DataProposalHashDb(parent_data_proposal_hash.clone()),
                    transaction_type: TransactionTypeDb::BlobTransaction,
                    block_hash: None,
                    block_height: BlockHeight(0),
                    lane_id: None, // TODO: we know the lane here so not sure why this used to be an option
                    index: 0,
                    identity: match tx.transaction_data {
                        TransactionData::Blob(ref blob_tx) => Some(blob_tx.identity.clone().0),
                        _ => None,
                    },
                });
                // We skip the blobs here or they'll conflict later and it's easier.
            }
            MempoolStatusEvent::DataProposalCreated { txs_metadatas, .. } => {
                for tx_metadata in txs_metadatas {
                    self.handler_store
                        .tx_status_update
                        .entry(tx_metadata.id)
                        .or_insert(TransactionStatusDb::DataProposalCreated);
                }
            }
        }
        Ok(())
    }

    pub(crate) fn empty_store(&self) -> bool {
        self.handler_store.sql_queries.is_empty()
    }
}

impl NodeStateCallback for IndexerHandlerStore {
    fn on_event(&mut self, event: &TxEvent) {
        self.block_callback.on_event(event);

        match *event {
            TxEvent::DuplicateBlobTransaction(..) | TxEvent::RejectedBlobTransaction(..) => {
                // Return early, we want to skip events or it will violate the foreign key
                return;
            }
            TxEvent::SequencedBlobTransaction(tx_id, lane_id, index, blob_tx) => {
                self.txs.push_front(TxStore {
                    tx_hash: TxHashDb(tx_id.1.clone()),
                    dp_hash: DataProposalHashDb(tx_id.0.clone()),
                    transaction_type: TransactionTypeDb::BlobTransaction,
                    block_hash: Some(self.block_hash.clone()),
                    block_height: self.block_height,
                    lane_id: Some(LaneIdDb(lane_id.clone())),
                    index: index as i32,
                    identity: Some(blob_tx.identity.clone().0),
                });
                for (index, blob) in blob_tx.blobs.iter().enumerate() {
                    self.blobs.0.push(format!(
                        "{}\t{}\t{}\t{}\t{}\t\\\\x{}\n",
                        tx_id.0,
                        tx_id.1,
                        index,
                        blob_tx.identity,
                        blob.contract_name,
                        hex::encode(&blob.data.0),
                    ));
                }
                self.tx_status_update
                    .entry(tx_id.clone())
                    .and_modify(|status| {
                        if *status != TransactionStatusDb::Success
                            && *status != TransactionStatusDb::Failure
                            && *status != TransactionStatusDb::TimedOut
                        {
                            *status = TransactionStatusDb::Sequenced;
                        }
                    })
                    .or_insert(TransactionStatusDb::Sequenced);
            }
            TxEvent::SequencedProofTransaction(tx_id, lane_id, index, ..) => {
                self.txs.push_front(TxStore {
                    tx_hash: TxHashDb(tx_id.1.clone()),
                    dp_hash: DataProposalHashDb(tx_id.0.clone()),
                    transaction_type: TransactionTypeDb::ProofTransaction,
                    block_hash: Some(self.block_hash.clone()),
                    block_height: self.block_height,
                    lane_id: Some(LaneIdDb(lane_id.clone())),
                    index: index as i32,
                    identity: None,
                });
                self.tx_status_update
                    .insert(tx_id.clone(), TransactionStatusDb::Success);
            }
            TxEvent::Settled(tx_id, ..) => {
                self.tx_status_update
                    .insert(tx_id.clone(), TransactionStatusDb::Success);
            }
            TxEvent::SettledAsFailed(tx_id) => {
                self.tx_status_update
                    .insert(tx_id.clone(), TransactionStatusDb::Failure);
            }
            TxEvent::TimedOut(tx_id) => {
                self.tx_status_update
                    .insert(tx_id.clone(), TransactionStatusDb::TimedOut);
            }
            TxEvent::TxError(..) => {}
            // Skip registering unproven blobs
            TxEvent::NewProof(..) => {}
            TxEvent::BlobSettled(tx_id, _tx, blob, blob_index, proof_data, blob_proof_index) => {
                // Can be None for executed blobs
                if let Some((_, _, proof_tx_id, hyli_output)) = proof_data {
                    self.blob_proof_outputs.0.push(format!(
                        "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
                        tx_id.0,
                        tx_id.1,
                        proof_tx_id.0,
                        proof_tx_id.1,
                        blob_index,
                        blob_proof_index, // blob_proof_output_index <- this needed?
                        blob.contract_name,
                        serde_json::to_string(hyli_output).unwrap_or_default(),
                        true, // settled
                    ));
                }
            }
            TxEvent::ContractRegistered(tx_id, contract_name, contract, metadata) => {
                self.contract_inserts.push(ContractInsertStore {
                    contract_name: contract_name.clone(),
                    verifier: contract.verifier.0.clone(),
                    program_id: contract.program_id.0.clone(),
                    timeout_window: TimeoutWindowDb(contract.timeout_window.clone()),
                    state_commitment: contract.state.0.clone(),
                    parent_dp_hash: DataProposalHashDb(tx_id.0.clone()),
                    tx_hash: TxHashDb(tx_id.1.clone()),
                    metadata: metadata.clone(),
                });
                self.contract_updates.remove(contract_name);
                // Don't push events
                return;
            }
            TxEvent::ContractDeleted(_, contract_name) => {
                self.contract_updates
                    .entry(contract_name.clone())
                    .and_modify(|e| {
                        e.deleted_at_height = Some(self.block_height.0 as i32);
                    })
                    .or_insert(ContractUpdateStore {
                        deleted_at_height: Some(self.block_height.0 as i32),
                        ..Default::default()
                    });
                // Don't push events
                return;
            }
            TxEvent::ContractStateUpdated(_, contract_name, state_commitment) => {
                self.contract_updates
                    .entry(contract_name.clone())
                    .and_modify(|e| {
                        e.state_commitment = Some(state_commitment.0.clone());
                    })
                    .or_insert(ContractUpdateStore {
                        state_commitment: Some(state_commitment.0.clone()),
                        ..Default::default()
                    });
                // Don't push events
                return;
            }
            TxEvent::ContractProgramIdUpdated(_, contract_name, program_id) => {
                self.contract_updates
                    .entry(contract_name.clone())
                    .and_modify(|e| {
                        e.program_id = Some(program_id.0.clone());
                    })
                    .or_insert(ContractUpdateStore {
                        program_id: Some(program_id.0.clone()),
                        ..Default::default()
                    });
                // Don't push events
                return;
            }
            TxEvent::ContractTimeoutWindowUpdated(_, contract_name, timeout_window) => {
                self.contract_updates
                    .entry(contract_name.clone())
                    .and_modify(|e| {
                        e.timeout_window = Some(TimeoutWindowDb(timeout_window.clone()));
                    })
                    .or_insert(ContractUpdateStore {
                        timeout_window: Some(TimeoutWindowDb(timeout_window.clone())),
                        ..Default::default()
                    });
                // Don't push events
                return;
            }
        }
        self.tx_events.0.push(format!(
            "{}\t{}\t{}\t{}\t{}\t{}\n",
            self.block_hash,
            self.block_height,
            event.tx_id().0,
            event.tx_id().1,
            self.tx_events.0.len(),
            serde_json::to_value(event)
                .unwrap_or(serde_json::Value::Null)
                .to_string()
                .replace("\\", "\\\\"),
        ));
    }
}

impl Indexer {
    pub(crate) async fn dump_store_to_db(&mut self) -> Result<()> {
        tracing::debug!("Dumping SQL queries to database");

        //let mut transaction = self.db.begin().await?;
        let transaction = &self.db;
        let mut transaction = transaction.acquire().await?;
        let transaction = transaction.acquire().await?;

        // First insert blocks
        for block in self.handler_store.blocks.drain(..) {
            sqlx::query("INSERT INTO blocks (hash, parent_hash, height, timestamp, total_txs) VALUES ($1, $2, $3, $4, $5)")
                .bind(block.block_hash)
                .bind(block.parent_hash)
                .bind(block.block_height.0 as i64)
                .bind(block.timestamp)
                .bind(block.total_txs)
                .execute(&mut *transaction)
                .await?;
        }

        // Then transactions
        let batch_size = calculate_optimal_batch_size(10);
        while !self.handler_store.txs.is_empty() {
            let chunk = self
                .handler_store
                .txs
                .drain(..std::cmp::min(batch_size, self.handler_store.txs.len()))
                .collect::<Vec<_>>();
            let mut query_builder = QueryBuilder::<Postgres>::new(
                    "INSERT INTO transactions (parent_dp_hash, tx_hash, version, transaction_type, transaction_status, block_hash, block_height, lane_id, index, identity) VALUES ",
                );
            // PG won't let us have the same TX twice in the insert into values, so do this as a workaround.
            let mut already_inserted: HashSet<TxId> = HashSet::new();
            let mut add_comma = false;
            for tx in chunk.into_iter() {
                if already_inserted.insert(TxId(tx.dp_hash.0.clone(), tx.tx_hash.0.clone())) {
                    if add_comma {
                        query_builder.push(",");
                    }
                    query_builder.push("(");
                    query_builder.push_bind(tx.dp_hash);
                    query_builder.push(",");
                    query_builder.push_bind(tx.tx_hash);
                    query_builder.push(",");
                    query_builder.push_bind(1);
                    query_builder.push(",");
                    query_builder.push_bind(tx.transaction_type);
                    query_builder.push(",");
                    query_builder.push_bind(TransactionStatusDb::WaitingDissemination);
                    query_builder.push(",");
                    query_builder.push_bind(tx.block_hash);
                    query_builder.push(",");
                    query_builder.push_bind(tx.block_height.0 as i32);
                    query_builder.push(",");
                    query_builder.push_bind(tx.lane_id);
                    query_builder.push(",");
                    query_builder.push_bind(tx.index);
                    query_builder.push(",");
                    query_builder.push_bind(tx.identity);
                    query_builder.push(")");
                    add_comma = true;
                }
            }
            // for genesis block verified proof tx
            query_builder.push(
                " ON CONFLICT (parent_dp_hash, tx_hash)
                DO UPDATE SET
                    index = GREATEST(transactions.index, excluded.index),
                    lane_id = COALESCE(excluded.lane_id, transactions.lane_id),
                    block_hash = COALESCE(excluded.block_hash, transactions.block_hash),
                    block_height = GREATEST(excluded.block_height, transactions.block_height)",
            );
            if already_inserted.is_empty() {
                continue;
            }
            _ = log_error!(
                query_builder.build().execute(&mut *transaction).await,
                "Inserting transactions"
            )?;
        }

        // Then status updates
        {
            // Create a temporary table to insert updates
            sqlx::query("CREATE TEMPORARY TABLE IF NOT EXISTS updates (parent_dp_hash TEXT NOT NULL, tx_hash TEXT NOT NULL, transaction_status transaction_status NOT NULL)")
                .execute(&mut *transaction)
                .await?;

            let batch_size = calculate_optimal_batch_size(3);
            while !self.handler_store.tx_status_update.is_empty() {
                let mut entries = vec![];
                #[allow(clippy::unwrap_used, reason = "Must exist from check above")]
                for _ in 0..std::cmp::min(batch_size, self.handler_store.tx_status_update.len()) {
                    let key = self
                        .handler_store
                        .tx_status_update
                        .keys()
                        .next()
                        .unwrap()
                        .clone();
                    entries.push((
                        self.handler_store.tx_status_update.remove(&key).unwrap(),
                        key,
                    ));
                }
                // Insert status updates into the temporary table
                let mut query_builder = QueryBuilder::<Postgres>::new(
                    "INSERT INTO updates (parent_dp_hash, tx_hash, transaction_status) ",
                );
                query_builder.push_values(entries.into_iter(), |mut b, (status, tx_id)| {
                    b.push_bind(DataProposalHashDb(tx_id.0))
                        .push_bind(TxHashDb(tx_id.1))
                        .push_bind(status);
                });
                _ = log_error!(
                    query_builder.build().execute(&mut *transaction).await,
                    "Inserting status updates into temporary table"
                )?;
            }

            // Batch update transaction statuses from the temporary table
            sqlx::query(
                "UPDATE transactions SET transaction_status = updates.transaction_status
                FROM updates
                WHERE transactions.tx_hash = updates.tx_hash AND transactions.parent_dp_hash = updates.parent_dp_hash"
            )
            .execute(&mut *transaction)
            .await?;

            // Truncate the temporary table
            sqlx::query("TRUNCATE TABLE updates")
                .execute(&mut *transaction)
                .await?;
        }

        // Contracts, annoyingly, can be added-deleted-added and so on, so we'll insert them one at a time for simplicity.
        for contract in self.handler_store.contract_inserts.drain(..) {
            sqlx::query("INSERT INTO contracts (contract_name, verifier, program_id, timeout_window, state_commitment, parent_dp_hash, tx_hash, metadata) VALUES ($1, $2, $3, $4, $5, $6, $7, $8) ON CONFLICT (contract_name) DO UPDATE SET
                verifier = EXCLUDED.verifier,
                program_id = EXCLUDED.program_id,
                timeout_window = EXCLUDED.timeout_window,
                state_commitment = EXCLUDED.state_commitment,
                parent_dp_hash = EXCLUDED.parent_dp_hash,
                tx_hash = EXCLUDED.tx_hash,
                metadata = EXCLUDED.metadata,
                deleted_at_height = NULL")
                .bind(contract.contract_name.0)
                .bind(contract.verifier)
                .bind(contract.program_id)
                .bind(contract.timeout_window)
                .bind(contract.state_commitment)
                .bind(contract.parent_dp_hash)
                .bind(contract.tx_hash)
                .bind(contract.metadata)
                .execute(&mut *transaction)
                .await?;
        }

        // COPY seems about 2x faster than INSERT
        let mut copy = transaction.copy_in_raw("COPY transaction_state_events (block_hash, block_height, parent_dp_hash, tx_hash, index, events) FROM STDIN WITH (FORMAT TEXT)").await?;
        copy.read_from(&mut self.handler_store.tx_events).await?;
        copy.finish().await?;

        let mut copy = transaction.copy_in_raw("COPY blobs (parent_dp_hash, tx_hash, blob_index, identity, contract_name, data) FROM STDIN WITH (FORMAT TEXT)").await?;
        copy.read_from(&mut self.handler_store.blobs).await?;
        copy.finish().await?;

        let mut copy = transaction.copy_in_raw("COPY blob_proof_outputs (blob_parent_dp_hash, blob_tx_hash, proof_parent_dp_hash, proof_tx_hash, blob_index, blob_proof_output_index, contract_name, hyli_output, settled) FROM STDIN WITH (FORMAT TEXT)").await?;
        copy.read_from(&mut self.handler_store.blob_proof_outputs)
            .await?;
        copy.finish().await?;

        // Then contract updates
        {
            // Create a temporary table to insert updates
            sqlx::query("CREATE TEMPORARY TABLE IF NOT EXISTS contract_updates (contract_name TEXT PRIMARY KEY NOT NULL, verifier TEXT, program_id BYTEA, timeout_window BIGINT, state_commitment BYTEA, deleted_at_height INT)")
                .execute(&mut *transaction)
                .await?;

            let batch_size = calculate_optimal_batch_size(6);
            while !self.handler_store.contract_updates.is_empty() {
                let mut entries = vec![];
                #[allow(clippy::unwrap_used, reason = "Must exist from check above")]
                for _ in 0..std::cmp::min(batch_size, self.handler_store.contract_updates.len()) {
                    let key = self
                        .handler_store
                        .contract_updates
                        .keys()
                        .next()
                        .unwrap()
                        .clone();
                    entries.push((
                        self.handler_store.contract_updates.remove(&key).unwrap(),
                        key,
                    ));
                }
                // Insert contract updates into the temporary table
                let mut query_builder = QueryBuilder::<Postgres>::new(
                    "INSERT INTO contract_updates (contract_name, verifier, program_id, timeout_window, state_commitment, deleted_at_height) ",
                );
                query_builder.push_values(entries.into_iter(), |mut b, (update, contract_name)| {
                    b.push_bind(contract_name.0)
                        .push_bind(update.verifier)
                        .push_bind(update.program_id)
                        .push_bind(update.timeout_window)
                        .push_bind(update.state_commitment)
                        .push_bind(update.deleted_at_height);
                });
                _ = log_error!(
                    query_builder.build().execute(&mut *transaction).await,
                    "Inserting contract updates into temporary table"
                )?;
            }

            // Batch update contract updates from the temporary table
            sqlx::query(
                "UPDATE contracts SET
                    verifier = COALESCE(contract_updates.verifier, contracts.verifier),
                    program_id = COALESCE(contract_updates.program_id, contracts.program_id),
                    timeout_window = COALESCE(contract_updates.timeout_window, contracts.timeout_window),
                    state_commitment = COALESCE(contract_updates.state_commitment, contracts.state_commitment),
                    deleted_at_height = COALESCE(contract_updates.deleted_at_height, contracts.deleted_at_height)
                FROM contract_updates
                WHERE contracts.contract_name = contract_updates.contract_name"
            )
            .execute(&mut *transaction)
            .await?;

            // Truncate the temporary table
            sqlx::query("TRUNCATE TABLE contract_updates")
                .execute(&mut *transaction)
                .await?;
        }

        Ok(())
    }
}

fn calculate_optimal_batch_size(params_per_item: usize) -> usize {
    let max_params: usize = 65000; // Security margin
    if params_per_item == 0 {
        return 1;
    }

    let optimal_size = max_params / params_per_item;
    // Assert there is at least 1 elem per batch
    std::cmp::max(1, optimal_size)
}

pub fn into_utc_date_time(ts: &TimestampMs) -> Result<DateTime<Utc>> {
    DateTime::from_timestamp_millis(ts.0.try_into().context("Converting u64 into i64")?)
        .context("Converting i64 into UTC DateTime")
}

#[derive(Default)]
pub struct StreamableData(pub Vec<String>, usize);

impl StreamableData {
    pub fn new(data: Vec<String>) -> Self {
        StreamableData(data, 0)
    }
}

#[allow(clippy::indexing_slicing, reason = "data-byte exist by construction")]
impl tokio::io::AsyncRead for StreamableData {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut core::task::Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> core::task::Poll<std::result::Result<(), std::io::Error>> {
        let this = self.get_mut();
        let Some(data) = this.0.pop() else {
            return core::task::Poll::Ready(Ok(()));
        };
        let mut v = data.as_bytes();
        if this.1 > 0 {
            v = &v[this.1..]; // Should never panic by construction
        }
        if v.len() > buf.remaining() {
            // Fill the buffer then start over (rare so allowed to be a little inefficient)
            this.1 += buf.remaining();
            buf.put_slice(&v[..buf.remaining()]); // always safe
            this.0.push(data);
        } else {
            buf.put_slice(v);
            this.1 = 0;
        }
        core::task::Poll::Ready(Ok(()))
    }
}

#[cfg(test)]
mod tests;

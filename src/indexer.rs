//! Index system for historical data.

use crate::{
    explorer::{
        api::{DataProposalHashDb, LaneIdDb, TimeoutWindowDb, TxHashDb},
        WsExplorerBlobTx,
    },
    model::*,
    node_state::module::NodeStateEvent,
    utils::conf::{Conf, SharedConf},
};
use anyhow::{bail, Context, Error, Result};
use chrono::{DateTime, Utc};
use hyle_contract_sdk::TxHash;
use hyle_model::api::{TransactionStatusDb, TransactionTypeDb};
use hyle_model::utils::TimestampMs;
use hyle_modules::{
    bus::{BusClientSender, SharedMessageBus},
    log_error, log_warn, module_handle_messages,
    modules::{gcs_uploader::GCSRequest, module_bus_client, Module, SharedBuildApiCtx},
    node_state::{
        module::NodeStateModule, NodeState, NodeStateCallback, NodeStateProcessing, NodeStateStore,
        TxEvent,
    },
};
use hyle_net::clock::TimestampMsClock;
use sqlx::{postgres::PgPoolOptions, Acquire, PgPool, Pool, Postgres, QueryBuilder, Row};
use std::{
    collections::{HashMap, HashSet},
    ops::{Deref, DerefMut},
    sync::Arc,
};
use tracing::{debug, info, trace};

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
    node_state: Option<Box<NodeState>>,
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
            node_state: Some(Box::new(node_state)),
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
            &self.node_state.as_ref().expect("should exist").store,
        )
        .context("Failed to save node state to disk")?;
        let persisted_da_start_height =
            BlockHeight(self.node_state.as_ref().unwrap().current_height.0 + 1);

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

            listen<MempoolStatusEvent> _event => {
                // _ = log_error!(self.handle_mempool_status_event(event)
                //     .await,
                //     "Indexer handling mempool status event");
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

    #[allow(clippy::too_many_arguments)]
    fn send_blob_transaction_to_websocket_subscribers(
        &mut self,
        tx: &BlobTransaction,
        tx_hash: TxHashDb,
        dp_hash: DataProposalHashDb,
        block_hash: &ConsensusProposalHash,
        index: u32,
        version: u32,
        lane_id: Option<LaneId>,
        timestamp: Option<TimestampMs>,
    ) {
        let _ = self.bus.send(WsExplorerBlobTx {
            tx: tx.clone(),
            tx_hash,
            dp_hash,
            block_hash: block_hash.clone(),
            index,
            version,
            lane_id,
            timestamp,
        });
    }
}

impl std::ops::Deref for Indexer {
    type Target = Pool<Postgres>;

    fn deref(&self) -> &Self::Target {
        &self.db
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

    blocks: Vec<BlockStore>,
    txs: Vec<TxStore>,
    tx_status_update: HashMap<TxId, TransactionStatusDb>,
    tx_events: Vec<TxEventStore>,
}

impl std::fmt::Debug for IndexerHandlerStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IndexerHandlerStore")
            .field("sql_queries", &self.sql_queries.len())
            .finish()
    }
}

impl Indexer {
    pub async fn handle_signed_block(&mut self, block: SignedBlock) -> Result<(), Error> {
        {
            let mut ns = self.node_state.take().unwrap();
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

            NodeStateProcessing {
                this: &mut ns,
                callback: self,
            }
            .process_signed_block(&block)?;
            self.node_state = Some(ns);
        }
        //self.handle_processed_block(block.clone())?;

        if self.handler_store.sql_queries.len() >= self.conf.indexer.query_buffer_size {
            // If we have more than configured blocks, we dump the store to the database
            self.dump_store_to_db().await?;
        }
        // if last block is newer than 5sec dump store to db
        let now = TimestampMsClock::now();
        if self.handler_store.last_update.0 + 5000 < now.0 {
            self.dump_store_to_db().await?;
            self.handler_store.last_update = now;
        }

        /*self.bus
        .send(NodeStateEvent::NewBlock(Box::new(block.clone())))?;*/

        Ok(())
    }

    pub(crate) fn empty_store(&self) -> bool {
        self.handler_store.sql_queries.is_empty()
    }

    pub(crate) async fn dump_store_to_db(&mut self) -> Result<()> {
        /*if self.handler_store.sql_queries.is_empty() {
            return Ok(());
        }*/

        info!("Dumping SQL queries to database");

        //let mut transaction = self.db.begin().await?;
        let transaction = &self.db;

        // First insert blocks
        for block in self.handler_store.blocks.drain(..) {
            sqlx::query("INSERT INTO blocks (hash, parent_hash, height, timestamp, total_txs) VALUES ($1, $2, $3, $4, $5)")
                .bind(block.block_hash)
                .bind(block.parent_hash)
                .bind(block.block_height.0 as i64)
                .bind(block.timestamp)
                .bind(block.total_txs)
                .execute(transaction)
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
                    "INSERT INTO transactions (parent_dp_hash, tx_hash, version, transaction_type, transaction_status, block_hash, block_height, lane_id, index, identity) ",
                );
            query_builder.push_values(chunk.into_iter(), |mut b, tx| {
                b.push_bind(tx.dp_hash)
                    .push_bind(tx.tx_hash)
                    .push_bind(1)
                    .push_bind(tx.transaction_type)
                    .push_bind(TransactionStatusDb::Sequenced)
                    .push_bind(tx.block_hash)
                    .push_bind(tx.block_height.0 as i32)
                    .push_bind(tx.lane_id)
                    .push_bind(tx.index)
                    .push_bind(tx.identity);
            });
            // for genesis block verified proof tx
            query_builder.push(" ON CONFLICT DO NOTHING");
            _ = log_error!(
                query_builder.build().execute(transaction).await,
                "Inserting transactions"
            )?;
        }

        // Then status updates
        /*for (tx_id, status) in self.handler_store.tx_status_update.drain() {
            sqlx::query("UPDATE transactions SET transaction_status = $1 WHERE tx_hash = $2 AND parent_dp_hash = $3")
                .bind(status)
                .bind(TxHashDb(tx_id.1))
                .bind(DataProposalHashDb(tx_id.0))
                .execute(&mut *transaction)
                .await?;
        }*/

        // Then events
        let batch_size = calculate_optimal_batch_size(5); // 5 params per event
        while !self.handler_store.tx_events.is_empty() {
            let chunk = self
                .handler_store
                .tx_events
                .drain(..std::cmp::min(batch_size, self.handler_store.tx_events.len()))
                .collect::<Vec<_>>();
            let mut query_builder = QueryBuilder::<Postgres>::new(
            "INSERT INTO transaction_state_events (block_hash, block_height, tx_hash, parent_dp_hash, events) ",
            );
            query_builder.push_values(chunk.into_iter(), |mut b, event| {
                b.push_bind(event.block_hash)
                    .push_bind(event.block_height.0 as i64)
                    .push_bind(event.tx_hash)
                    .push_bind(event.dp_hash)
                    .push_bind(event.event_data);
            });
            _ = log_error!(
                query_builder.build().execute(transaction).await,
                "Inserting transaction state events"
            )?;
        }

        for sql_update in self.handler_store.sql_queries.drain(..) {
            _ = log_error!(
                sql_update.execute(transaction).await,
                "Executing SQL update"
            )?;
        }

        //transaction.commit().await?;

        Ok(())
    }
}

#[derive(Debug)]
pub struct TxStore {
    pub tx_hash: TxHashDb,
    pub dp_hash: DataProposalHashDb,
    pub transaction_type: TransactionTypeDb,
    pub block_hash: ConsensusProposalHash,
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

pub struct TxEventStore {
    pub block_hash: ConsensusProposalHash,
    pub block_height: BlockHeight,
    pub tx_hash: TxHashDb,
    pub dp_hash: DataProposalHashDb,
    pub event_data: serde_json::Value,
}

impl NodeStateCallback for Indexer {
    fn on_event(&mut self, event: &TxEvent) {
        match *event {
            TxEvent::DuplicateBlobTransaction(ref tx_id) => {
                // Return early, we want to skip events or it will violate the foreign key
                return;
            }
            TxEvent::SequencedBlobTransaction(ref tx_id, lane_id, index, ref blob_tx) => {
                self.handler_store.txs.push(TxStore {
                    tx_hash: TxHashDb(tx_id.1.clone()),
                    dp_hash: DataProposalHashDb(tx_id.0.clone()),
                    transaction_type: TransactionTypeDb::BlobTransaction,
                    block_hash: self.handler_store.block_hash.clone(),
                    block_height: self.handler_store.block_height,
                    lane_id: Some(LaneIdDb(lane_id.clone())),
                    index: index as i32,
                    identity: Some(blob_tx.identity.clone().0),
                });
            }
            TxEvent::SequencedProofTransaction(ref tx_id, lane_id, index, ref proof_tx) => {
                self.handler_store.txs.push(TxStore {
                    tx_hash: TxHashDb(tx_id.1.clone()),
                    dp_hash: DataProposalHashDb(tx_id.0.clone()),
                    transaction_type: TransactionTypeDb::ProofTransaction,
                    block_hash: self.handler_store.block_hash.clone(),
                    block_height: self.handler_store.block_height,
                    lane_id: Some(LaneIdDb(lane_id.clone())),
                    index: index as i32,
                    identity: None,
                });
            }
            TxEvent::Settled(tx_id, ref _unsettled_tx) => {
                self.handler_store
                    .tx_status_update
                    .insert(tx_id.clone(), TransactionStatusDb::Success);
            }
            TxEvent::SettledAsFailed(tx_id) => {
                self.handler_store
                    .tx_status_update
                    .insert(tx_id.clone(), TransactionStatusDb::Failure);
            }
            TxEvent::TimedOut(tx_id) => {
                self.handler_store
                    .tx_status_update
                    .insert(tx_id.clone(), TransactionStatusDb::TimedOut);
            }
            TxEvent::TxError(ref tx_id, ref err) => {}
            TxEvent::NewProof(ref tx_id, ref tx_hash, ref unsettled_tx, ref proof_output) => {}
            TxEvent::BlobSettled(ref tx_id, ref unsettled_blob_metadata, blob_index, size) => {}
            TxEvent::ContractDeleted(ref tx_id, ref contract_name) => {}
            TxEvent::ContractRegistered(ref tx_id, ref contract_name) => {}
            TxEvent::ContractStateUpdated(ref tx_id, ref contract_name, ref state_commitment) => {}
            TxEvent::ContractProgramIdUpdated(ref tx_id, ref contract_name, ref program_id) => {}
            TxEvent::ContractTimeoutWindowUpdated(
                ref tx_id,
                ref contract_name,
                ref timeout_window,
            ) => {}
        }
        self.handler_store.tx_events.push(TxEventStore {
            block_hash: self.handler_store.block_hash.clone(),
            block_height: self.handler_store.block_height,
            tx_hash: TxHashDb(event.tx_id().1.clone()),
            dp_hash: DataProposalHashDb(event.tx_id().0.clone()),
            event_data: serde_json::to_value(event).unwrap_or(serde_json::Value::Null),
        });
    }
}

pub fn into_utc_date_time(ts: &TimestampMs) -> Result<DateTime<Utc>> {
    DateTime::from_timestamp_millis(ts.0.try_into().context("Converting u64 into i64")?)
        .context("Converting i64 into UTC DateTime")
}

#[cfg(test)]
mod tests;

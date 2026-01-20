//! Index system for historical data.

use crate::{
    explorer::WsExplorerBlobTx,
    model::*,
    utils::conf::{Conf, SharedConf},
};
use anyhow::{Context, Error, Result};
use chrono::{DateTime, Utc};
use hyli_bus::modules::ModulePersistOutput;
use hyli_model::api::{ContractChangeType, TransactionStatusDb, TransactionTypeDb};
use hyli_model::utils::TimestampMs;
use hyli_modules::{
    bus::BusClientSender, modules::indexer::MIGRATOR, node_state::NodeStateEventCallback,
};
use hyli_modules::{
    bus::SharedMessageBus,
    log_error, module_handle_messages,
    modules::{gcs_uploader::GCSRequest, module_bus_client, Module, SharedBuildApiCtx},
    node_state::{module::NodeStateModule, NodeState, NodeStateCallback, NodeStateStore, TxEvent},
};
use hyli_net::clock::TimestampMsClock;
use serde::Serialize;
use sqlx::{
    postgres::PgPoolOptions, Acquire, PgConnection, PgPool, Pool, Postgres, QueryBuilder, Row,
};
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

const BLOCK_NOTIFICATION_CHANNEL: &str = "block_notifications";

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
            NodeStateModule::load_from_disk_or_default::<NodeStateStore>(&node_state_path)?;

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

    async fn persist(&mut self) -> Result<ModulePersistOutput> {
        let node_state_file = self.conf.data_directory.join("indexer_node_state.bin");
        let checksum = NodeStateModule::save_on_disk(&node_state_file, &self.node_state.store)
            .context("Failed to save node state to disk")?;

        let persisted_da_start_height = BlockHeight(self.node_state.current_height.0 + 1);
        tracing::debug!(
            "Indexer saving DA start height: {}",
            &persisted_da_start_height
        );

        let da_start_file = self.conf.data_directory.join("da_start_height.bin");
        let da_start_checksum =
            NodeStateModule::save_on_disk(&da_start_file, &persisted_da_start_height)
                .context("Failed to save DA start height to disk")?;

        Ok(vec![
            (node_state_file, checksum),
            (da_start_file, da_start_checksum),
        ])
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
    block_time: TimestampMs,
    last_update: TimestampMs,

    // Intended to be temporary, for CSI & co.
    block_callback: NodeStateEventCallback,

    blocks: Vec<BlockStore>,
    txs: VecDeque<TxStore>,
    tx_status_update: HashMap<TxId, TransactionStatusDb>,
    tx_events: StreamableData,
    blobs: StreamableData,
    blob_proof_outputs: StreamableData,
    contract_inserts: Vec<ContractInsertStore>,
    contract_updates: HashMap<ContractName, ContractUpdateStore>,
    contract_history: HashMap<ContractHistoryKey, ContractHistoryStore>,
    tx_index_map: HashMap<TxId, i32>,
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
    pub tx_hash: TxHash,
    pub dp_hash: DataProposalHash,
    pub transaction_type: TransactionTypeDb,
    pub block_hash: Option<ConsensusProposalHash>,
    pub block_height: BlockHeight,
    pub lane_id: Option<LaneId>,
    pub index: i32,
    pub identity: Option<String>,
    pub contract_names: HashSet<ContractName>,
}

pub struct BlockStore {
    pub block_hash: ConsensusProposalHash,
    pub parent_hash: ConsensusProposalHash,
    pub block_height: BlockHeight,
    pub timestamp: DateTime<Utc>,
    pub total_txs: i64,
}

#[derive(Debug, Serialize)]
struct BlockNotification {
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
    pub timeout_window: TimeoutWindow,
    pub state_commitment: Vec<u8>,
    pub parent_dp_hash: DataProposalHash,
    pub tx_hash: TxHash,
    pub metadata: Option<Vec<u8>>,
}

#[derive(Default)]
struct ContractUpdateStore {
    pub verifier: Option<String>,
    pub program_id: Option<Vec<u8>>,
    pub timeout_window: Option<TimeoutWindow>,
    pub state_commitment: Option<Vec<u8>>,
    pub deleted_at_height: Option<i32>,
}

#[derive(Debug)]
struct ContractHistoryStore {
    pub contract_name: ContractName,
    pub block_height: BlockHeight,
    pub tx_index: i32,
    pub change_type: Vec<ContractChangeType>,
    pub verifier: String,
    pub program_id: Vec<u8>,
    pub state_commitment: Vec<u8>,
    pub soft_timeout: Option<i64>,
    pub hard_timeout: Option<i64>,
    pub deleted_at_height: Option<i32>,
    pub parent_dp_hash: DataProposalHash,
    pub tx_hash: TxHash,
}

#[derive(Debug, Hash, PartialEq, Eq)]
struct ContractHistoryKey {
    pub contract_name: ContractName,
    pub block_height: BlockHeight,
    pub tx_index: i32,
}

impl IndexerHandlerStore {
    fn record_contract_history(&mut self, history: ContractHistoryStore) {
        let key = ContractHistoryKey {
            contract_name: history.contract_name.clone(),
            block_height: history.block_height,
            tx_index: history.tx_index,
        };

        self.contract_history
            .entry(key)
            .and_modify(|entry| {
                for change_type in &history.change_type {
                    if !entry.change_type.contains(change_type) {
                        entry.change_type.push(change_type.clone());
                    }
                }
                entry.verifier = history.verifier.clone();
                entry.program_id = history.program_id.clone();
                entry.state_commitment = history.state_commitment.clone();
                entry.soft_timeout = history.soft_timeout;
                entry.hard_timeout = history.hard_timeout;
                entry.deleted_at_height = history.deleted_at_height;
                entry.parent_dp_hash = history.parent_dp_hash.clone();
                entry.tx_hash = history.tx_hash.clone();
            })
            .or_insert(history);
    }
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
            self.handler_store.block_time = block.consensus_proposal.timestamp.clone();

            self.handler_store.blocks.push(BlockStore {
                block_hash: self.handler_store.block_hash.clone(),
                parent_hash: block.parent_hash().clone(),
                block_height: self.handler_store.block_height,
                timestamp: into_utc_date_time(&block.consensus_proposal.timestamp)
                    .unwrap_or_else(|_| Utc::now()),
                total_txs: block.count_txs() as i64,
            });

            self.handler_store.block_callback = NodeStateEventCallback::from_signed(&block);
            self.node_state
                .process_signed_block(&block, &mut self.handler_store)?;

            // We use the indexer as node-state-processor for CSI
            // TODO: refactor this away it conflicts with running the indexer in the full node as we send all events twice.
            let (staking_data, stateful_events) = self.handler_store.block_callback.take();
            self.bus.send(NodeStateEvent::NewBlock(NodeStateBlock {
                signed_block: block.into(),
                staking_data: staking_data.into(),
                stateful_events: stateful_events.into(),
            }))?;
        }

        // Occasionally dump to DB:
        // - if we have more than conf.indexer.query_buffer_size events
        // - if it's been more than 5s since last dump
        // - if the block is recent (less than 1min old) we stream as fast as we can (buffering is mostly useful when catching up)
        let now = TimestampMsClock::now();
        if self.handler_store.tx_events.0.len() > self.conf.indexer.query_buffer_size
            || self.handler_store.last_update.0 + 5000 < now.0
            || now.0.saturating_sub(self.handler_store.block_time.0) < 60 * 1000
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
                txs,
            } => {
                for tx in txs {
                    self.handler_store.txs.push_front(TxStore {
                        tx_hash: tx.hashed().clone(),
                        dp_hash: parent_data_proposal_hash.clone(),
                        transaction_type: match tx.transaction_data {
                            TransactionData::Blob(_) => TransactionTypeDb::BlobTransaction,
                            TransactionData::Proof(_) => TransactionTypeDb::ProofTransaction,
                            TransactionData::VerifiedProof(_) => {
                                TransactionTypeDb::ProofTransaction
                            }
                        },
                        block_hash: None,
                        block_height: BlockHeight(0),
                        lane_id: None, // TODO: we know the lane here so not sure why this used to be an option
                        index: 0,
                        identity: match tx.transaction_data {
                            TransactionData::Blob(ref blob_tx) => Some(blob_tx.identity.clone().0),
                            _ => None,
                        },
                        contract_names: match tx.transaction_data {
                            TransactionData::Blob(ref blob_tx) => blob_tx
                                .blobs
                                .iter()
                                .map(|b| b.contract_name.clone())
                                .collect(),
                            _ => HashSet::new(),
                        },
                    });
                }
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
            TxEvent::DuplicateBlobTransaction(..) => {
                // Return early, we want to skip events or it will violate the foreign key
                return;
            }
            TxEvent::SequencedBlobTransaction(tx_id, lane_id, index, blob_tx, _tx_context)
            | TxEvent::RejectedBlobTransaction(tx_id, lane_id, index, blob_tx, _tx_context) => {
                self.tx_index_map.insert(tx_id.clone(), index as i32);
                self.txs.push_front(TxStore {
                    tx_hash: tx_id.1.clone(),
                    dp_hash: tx_id.0.clone(),
                    transaction_type: TransactionTypeDb::BlobTransaction,
                    block_hash: Some(self.block_hash.clone()),
                    block_height: self.block_height,
                    lane_id: Some(lane_id.clone()),
                    index: index as i32,
                    identity: Some(blob_tx.identity.clone().0),
                    contract_names: blob_tx
                        .blobs
                        .iter()
                        .map(|b| b.contract_name.clone())
                        .collect(),
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
                match *event {
                    TxEvent::RejectedBlobTransaction(..) => {
                        self.tx_status_update
                            .entry(tx_id.clone())
                            .and_modify(|status| {
                                *status = TransactionStatusDb::Failure;
                            })
                            .or_insert(TransactionStatusDb::Failure);
                    }
                    _ => {
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
                }
            }
            TxEvent::SequencedProofTransaction(tx_id, lane_id, index, ..) => {
                self.tx_index_map.insert(tx_id.clone(), index as i32);
                self.txs.push_front(TxStore {
                    tx_hash: tx_id.1.clone(),
                    dp_hash: tx_id.0.clone(),
                    transaction_type: TransactionTypeDb::ProofTransaction,
                    block_hash: Some(self.block_hash.clone()),
                    block_height: self.block_height,
                    lane_id: Some(lane_id.clone()),
                    index: index as i32,
                    identity: None,
                    contract_names: HashSet::new(),
                });
                self.tx_status_update
                    .insert(tx_id.clone(), TransactionStatusDb::Success);
            }
            TxEvent::Settled(tx_id, ..) => {
                self.tx_status_update
                    .insert(tx_id.clone(), TransactionStatusDb::Success);
            }
            TxEvent::SettledAsFailed(tx_id, ..) => {
                self.tx_status_update
                    .insert(tx_id.clone(), TransactionStatusDb::Failure);
            }
            TxEvent::TimedOut(tx_id, ..) => {
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
                tracing::info!("ContractRegistered event for contract: {}", contract_name);
                let tx_index = self.tx_index_map.get(tx_id).copied().unwrap_or(0);
                let (soft_timeout, hard_timeout) =
                    timeout_columns(&contract.timeout_window).unwrap_or_default();

                self.record_contract_history(ContractHistoryStore {
                    contract_name: contract_name.clone(),
                    block_height: self.block_height,
                    tx_index,
                    change_type: vec![ContractChangeType::Registered],
                    verifier: contract.verifier.0.clone(),
                    program_id: contract.program_id.0.clone(),
                    state_commitment: contract.state.0.clone(),
                    soft_timeout,
                    hard_timeout,
                    deleted_at_height: None,
                    parent_dp_hash: tx_id.0.clone(),
                    tx_hash: tx_id.1.clone(),
                });

                self.contract_inserts.push(ContractInsertStore {
                    contract_name: contract_name.clone(),
                    verifier: contract.verifier.0.clone(),
                    program_id: contract.program_id.0.clone(),
                    timeout_window: contract.timeout_window.clone(),
                    state_commitment: contract.state.0.clone(),
                    parent_dp_hash: tx_id.0.clone(),
                    tx_hash: tx_id.1.clone(),
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
            TxEvent::ContractStateUpdated(tx_id, contract_name, contract, state_commitment) => {
                let tx_index = self.tx_index_map.get(tx_id).copied().unwrap_or(0);
                let (soft_timeout, hard_timeout) =
                    timeout_columns(&contract.timeout_window).unwrap_or_default();

                self.record_contract_history(ContractHistoryStore {
                    contract_name: contract_name.clone(),
                    block_height: self.block_height,
                    tx_index,
                    change_type: vec![ContractChangeType::StateUpdated],
                    verifier: contract.verifier.0.clone(),
                    program_id: contract.program_id.0.clone(),
                    state_commitment: state_commitment.0.clone(),
                    soft_timeout,
                    hard_timeout,
                    deleted_at_height: None,
                    parent_dp_hash: tx_id.0.clone(),
                    tx_hash: tx_id.1.clone(),
                });

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
            TxEvent::ContractProgramIdUpdated(tx_id, contract_name, contract, program_id) => {
                tracing::info!(
                    "ContractProgramIdUpdated event for contract: {}",
                    contract_name
                );
                let tx_index = self.tx_index_map.get(tx_id).copied().unwrap_or(0);
                let (soft_timeout, hard_timeout) =
                    timeout_columns(&contract.timeout_window).unwrap_or_default();

                self.record_contract_history(ContractHistoryStore {
                    contract_name: contract_name.clone(),
                    block_height: self.block_height,
                    tx_index,
                    change_type: vec![ContractChangeType::ProgramIdUpdated],
                    verifier: contract.verifier.0.clone(),
                    program_id: program_id.0.clone(),
                    state_commitment: contract.state.0.clone(),
                    soft_timeout,
                    hard_timeout,
                    deleted_at_height: None,
                    parent_dp_hash: tx_id.0.clone(),
                    tx_hash: tx_id.1.clone(),
                });

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
            TxEvent::ContractTimeoutWindowUpdated(
                tx_id,
                contract_name,
                contract,
                timeout_window,
            ) => {
                let tx_index = self.tx_index_map.get(tx_id).copied().unwrap_or(0);
                let (soft_timeout, hard_timeout) =
                    timeout_columns(timeout_window).unwrap_or_default();

                self.record_contract_history(ContractHistoryStore {
                    contract_name: contract_name.clone(),
                    block_height: self.block_height,
                    tx_index,
                    change_type: vec![ContractChangeType::TimeoutUpdated],
                    verifier: contract.verifier.0.clone(),
                    program_id: contract.program_id.0.clone(),
                    state_commitment: contract.state.0.clone(),
                    soft_timeout,
                    hard_timeout,
                    deleted_at_height: None,
                    parent_dp_hash: tx_id.0.clone(),
                    tx_hash: tx_id.1.clone(),
                });

                self.contract_updates
                    .entry(contract_name.clone())
                    .and_modify(|e| {
                        e.timeout_window = Some(timeout_window.clone());
                    })
                    .or_insert(ContractUpdateStore {
                        timeout_window: Some(timeout_window.clone()),
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
        let mut block_notifications = Vec::new();
        // Only transaction that have some contracts in tx_store.contract_names will be notified
        // Hence, only sequenced blob transactions will be notified
        // FIXME: Add notification for contracts at the block where a tx for that contract settles
        // TODO: Investigate if we should have a channel per transaction status update
        let mut contract_notifications: HashMap<ContractName, HashSet<BlockHeight>> =
            HashMap::new();

        // First insert blocks
        for block in self.handler_store.blocks.drain(..) {
            block_notifications.push(BlockNotification {
                block_hash: block.block_hash.clone(),
                parent_hash: block.parent_hash.clone(),
                block_height: block.block_height,
                timestamp: block.timestamp,
                total_txs: block.total_txs,
            });
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
            let mut query_builder_ctx = QueryBuilder::<Postgres>::new(
                "INSERT INTO txs_contracts (parent_dp_hash, tx_hash, contract_name) VALUES ",
            );
            // PG won't let us have the same TX twice in the insert into values, so do this as a workaround.
            let mut already_inserted: HashSet<TxId> = HashSet::new();
            let mut add_comma = false;
            let mut add_comma_ctx = false;
            for tx in chunk.into_iter() {
                if already_inserted.insert(TxId(tx.dp_hash.clone(), tx.tx_hash.clone())) {
                    let contract_names: Vec<_> = tx.contract_names.into_iter().collect();
                    if !contract_names.is_empty() {
                        for contract_name in &contract_names {
                            contract_notifications
                                .entry(contract_name.clone())
                                .or_default()
                                .insert(tx.block_height);
                        }
                    }
                    if add_comma {
                        query_builder.push(",");
                    }

                    for contract_name in contract_names.into_iter() {
                        if add_comma_ctx {
                            query_builder_ctx.push(",");
                        }
                        query_builder_ctx.push("(");
                        query_builder_ctx.push_bind(tx.dp_hash.clone());
                        query_builder_ctx.push(",");
                        query_builder_ctx.push_bind(tx.tx_hash.clone());
                        query_builder_ctx.push(",");
                        query_builder_ctx.push_bind(contract_name.0);
                        query_builder_ctx.push(")");
                        add_comma_ctx = true;
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
            if add_comma_ctx {
                query_builder_ctx.push(" ON CONFLICT DO NOTHING");
                _ = log_error!(
                    query_builder_ctx.build().execute(&mut *transaction).await,
                    "Inserting txs_contracts"
                )?;
            }
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
                    b.push_bind(tx_id.0).push_bind(tx_id.1).push_bind(status);
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
                WHERE transactions.tx_hash = updates.tx_hash
                  AND transactions.parent_dp_hash = updates.parent_dp_hash
                  AND (
                    CASE transactions.transaction_status
                        WHEN 'waiting_dissemination' THEN 1
                        WHEN 'data_proposal_created' THEN 2
                        WHEN 'sequenced' THEN 3
                        WHEN 'success' THEN 4
                        WHEN 'failure' THEN 4
                        WHEN 'timed_out' THEN 4
                    END
                  ) < (
                    CASE updates.transaction_status
                        WHEN 'waiting_dissemination' THEN 1
                        WHEN 'data_proposal_created' THEN 2
                        WHEN 'sequenced' THEN 3
                        WHEN 'success' THEN 4
                        WHEN 'failure' THEN 4
                        WHEN 'timed_out' THEN 4
                    END
                  )",
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
            let (soft_timeout, hard_timeout) = timeout_columns(&contract.timeout_window)?;
            sqlx::query("INSERT INTO contracts (contract_name, verifier, program_id, soft_timeout, hard_timeout, state_commitment, parent_dp_hash, tx_hash, metadata) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) ON CONFLICT (contract_name) DO UPDATE SET
                verifier = EXCLUDED.verifier,
                program_id = EXCLUDED.program_id,
                soft_timeout = EXCLUDED.soft_timeout,
                hard_timeout = EXCLUDED.hard_timeout,
                state_commitment = EXCLUDED.state_commitment,
                parent_dp_hash = EXCLUDED.parent_dp_hash,
                tx_hash = EXCLUDED.tx_hash,
                metadata = EXCLUDED.metadata,
                deleted_at_height = NULL")
                .bind(contract.contract_name.0)
                .bind(contract.verifier)
                .bind(contract.program_id)
                .bind(soft_timeout)
                .bind(hard_timeout)
                .bind(contract.state_commitment)
                .bind(contract.parent_dp_hash)
                .bind(contract.tx_hash)
                .bind(contract.metadata)
                .execute(&mut *transaction)
                .await?;
        }

        // Insert contract history
        for history in self
            .handler_store
            .contract_history
            .drain()
            .map(|(_, history)| history)
        {
            tracing::info!(
                "🌴 Inserting contract history for contract {}: {:?}",
                history.contract_name,
                history
            );
            sqlx::query(
                "INSERT INTO contract_history (contract_name, block_height, tx_index, change_type, verifier, program_id, state_commitment, soft_timeout, hard_timeout, deleted_at_height, parent_dp_hash, tx_hash)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                 ON CONFLICT (contract_name, block_height, tx_index) DO NOTHING"
            )
            .bind(history.contract_name.0)
            .bind(history.block_height.0 as i64)
            .bind(history.tx_index)
            .bind(history.change_type)
            .bind(history.verifier)
            .bind(history.program_id)
            .bind(history.state_commitment)
            .bind(history.soft_timeout)
            .bind(history.hard_timeout)
            .bind(history.deleted_at_height)
            .bind(history.parent_dp_hash)
            .bind(history.tx_hash)
            .execute(&mut *transaction)
            .await?;
        }

        // COPY seems about 2x faster than INSERT
        let mut copy = transaction.copy_in_raw("COPY transaction_state_events (block_hash, block_height, parent_dp_hash, tx_hash, index, event) FROM STDIN WITH (FORMAT TEXT)").await?;
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
            sqlx::query("CREATE TEMPORARY TABLE IF NOT EXISTS contract_updates (contract_name TEXT PRIMARY KEY NOT NULL, verifier TEXT, program_id BYTEA, soft_timeout BIGINT, hard_timeout BIGINT, state_commitment BYTEA, deleted_at_height INT)")
                .execute(&mut *transaction)
                .await?;

            let batch_size = calculate_optimal_batch_size(7);
            while !self.handler_store.contract_updates.is_empty() {
                let mut entries = vec![];
                #[allow(clippy::unwrap_used, reason = "Must exist from check above")]
                for _ in 0..std::cmp::min(batch_size, self.handler_store.contract_updates.len()) {
                    let contract_name = self
                        .handler_store
                        .contract_updates
                        .keys()
                        .next()
                        .unwrap()
                        .clone();
                    let update = self
                        .handler_store
                        .contract_updates
                        .remove(&contract_name)
                        .unwrap();
                    let timeouts = timeout_columns_opt(&update.timeout_window)?;
                    entries.push((update, contract_name, timeouts));
                }
                // Insert contract updates into the temporary table
                let mut query_builder = QueryBuilder::<Postgres>::new(
                    "INSERT INTO contract_updates (contract_name, verifier, program_id, soft_timeout, hard_timeout, state_commitment, deleted_at_height) ",
                );
                query_builder.push_values(
                    entries.into_iter(),
                    |mut b, (update, contract_name, (soft_timeout, hard_timeout))| {
                        b.push_bind(contract_name.0)
                            .push_bind(update.verifier)
                            .push_bind(update.program_id)
                            .push_bind(soft_timeout)
                            .push_bind(hard_timeout)
                            .push_bind(update.state_commitment)
                            .push_bind(update.deleted_at_height);
                    },
                );
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
                    soft_timeout = COALESCE(contract_updates.soft_timeout, contracts.soft_timeout),
                    hard_timeout = COALESCE(contract_updates.hard_timeout, contracts.hard_timeout),
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

        if !contract_notifications.is_empty() {
            send_contract_notifications(&mut *transaction, contract_notifications).await?;
        }
        if !block_notifications.is_empty() {
            send_block_notifications(&mut *transaction, block_notifications).await?;
        }

        Ok(())
    }
}

fn timeout_columns(tw: &TimeoutWindow) -> Result<(Option<i64>, Option<i64>)> {
    match tw {
        TimeoutWindow::NoTimeout => Ok((None, None)),
        TimeoutWindow::Timeout {
            hard_timeout,
            soft_timeout,
        } => Ok((
            Some(
                soft_timeout
                    .0
                    .try_into()
                    .context("soft_timeout overflows i64")?,
            ),
            Some(
                hard_timeout
                    .0
                    .try_into()
                    .context("hard_timeout overflows i64")?,
            ),
        )),
    }
}

fn timeout_columns_opt(tw: &Option<TimeoutWindow>) -> Result<(Option<i64>, Option<i64>)> {
    match tw {
        None => Ok((None, None)),
        Some(tw) => timeout_columns(tw),
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

async fn send_contract_notifications(
    transaction: &mut PgConnection,
    notifications: HashMap<ContractName, HashSet<BlockHeight>>,
) -> Result<()> {
    for (contract_name, blocks) in notifications {
        for block_height in blocks {
            sqlx::query("SELECT pg_notify($1, $2)")
                .bind(&contract_name.0)
                .bind(block_height.0.to_string())
                .execute(&mut *transaction)
                .await?;
        }
    }

    Ok(())
}

async fn send_block_notifications(
    transaction: &mut PgConnection,
    blocks: Vec<BlockNotification>,
) -> Result<()> {
    for block in blocks {
        let payload = serde_json::to_string(&block)?;
        sqlx::query("SELECT pg_notify($1, $2)")
            .bind(BLOCK_NOTIFICATION_CHANNEL)
            .bind(payload)
            .execute(&mut *transaction)
            .await?;
    }

    Ok(())
}

#[cfg(test)]
mod tests;

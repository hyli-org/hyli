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
use sqlx::{postgres::PgPoolOptions, PgConnection, PgPool, Pool, Postgres, QueryBuilder, Row};
use std::{collections::HashMap, collections::HashSet, ops::Deref, path::PathBuf};
use tokio::io::ReadBuf;
use tracing::warn;

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
        let node_state_file = PathBuf::from("indexer_node_state.bin");
        let node_state_store = match NodeStateModule::load_from_disk::<NodeStateStore>(
            &ctx.0.data_directory,
            &node_state_file,
        )? {
            Some(s) => s,
            None => {
                warn!("Starting Indexer's NodeStateStore from default.");
                NodeStateStore::default()
            }
        };

        let mut node_state = NodeState::create("indexer");
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
        let node_state_file = PathBuf::from("indexer_node_state.bin");
        let checksum = NodeStateModule::save_on_disk(
            &self.conf.data_directory,
            &node_state_file,
            &self.node_state.store,
        )
        .context("Failed to save node state to disk")?;

        let persisted_da_start_height = BlockHeight(self.node_state.current_height.0 + 1);
        tracing::debug!(
            "Indexer saving DA start height: {}",
            &persisted_da_start_height
        );

        let da_start_file = PathBuf::from("da_start_height.bin");
        let da_start_checksum = NodeStateModule::save_on_disk(
            &self.conf.data_directory,
            &da_start_file,
            &persisted_da_start_height,
        )
        .context("Failed to save DA start height to disk")?;

        Ok(vec![
            (self.conf.data_directory.join(&node_state_file), checksum),
            (
                self.conf.data_directory.join(&da_start_file),
                da_start_checksum,
            ),
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
    txs: HashMap<TxId, TxStore>,
    tx_events: Vec<TxEventRow>,
    blobs: HashMap<BlobKey, BlobRow>,
    blob_proof_outputs: HashMap<BlobProofOutputKey, BlobProofOutputRow>,
    contract_inserts: HashMap<ContractName, ContractInsertStore>,
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
    pub transaction_status: TransactionStatusDb,
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
    pub deleted_at_height: Option<i32>,
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

#[derive(Debug, Hash, PartialEq, Eq)]
struct BlobKey {
    parent_dp_hash: DataProposalHash,
    tx_hash: TxHash,
    blob_index: i32,
}

#[derive(Debug)]
struct BlobRow {
    parent_dp_hash: DataProposalHash,
    tx_hash: TxHash,
    blob_index: i32,
    identity: String,
    contract_name: ContractName,
    data: Vec<u8>,
}

#[derive(Debug, Hash, PartialEq, Eq)]
struct BlobProofOutputKey {
    blob_parent_dp_hash: DataProposalHash,
    blob_tx_hash: TxHash,
    proof_parent_dp_hash: DataProposalHash,
    proof_tx_hash: TxHash,
    blob_index: i32,
    blob_proof_output_index: i32,
}

#[derive(Debug)]
struct BlobProofOutputRow {
    blob_parent_dp_hash: DataProposalHash,
    blob_tx_hash: TxHash,
    proof_parent_dp_hash: DataProposalHash,
    proof_tx_hash: TxHash,
    blob_index: i32,
    blob_proof_output_index: i32,
    contract_name: ContractName,
    hyli_output: serde_json::Value,
    settled: bool,
}

#[derive(Debug)]
struct TxEventRow {
    block_hash: BlockHash,
    block_height: BlockHeight,
    parent_dp_hash: DataProposalHash,
    tx_hash: TxHash,
    index: i32,
    event: serde_json::Value,
}

#[derive(Debug)]
struct ContractUpdateRow {
    contract_name: String,
    verifier: Option<String>,
    program_id: Option<Vec<u8>>,
    soft_timeout: Option<i64>,
    hard_timeout: Option<i64>,
    state_commitment: Option<Vec<u8>>,
    deleted_at_height: Option<i32>,
}

#[derive(Debug)]
struct TxContractRow {
    parent_dp_hash: DataProposalHash,
    tx_hash: TxHash,
    contract_name: String,
}

#[derive(Debug)]
struct ContractUpsertRow {
    contract_name: String,
    verifier: String,
    program_id: Vec<u8>,
    soft_timeout: Option<i64>,
    hard_timeout: Option<i64>,
    state_commitment: Vec<u8>,
    parent_dp_hash: DataProposalHash,
    tx_hash: TxHash,
    metadata: Option<Vec<u8>>,
    deleted_at_height: Option<i32>,
}

#[derive(Debug)]
struct ContractHistoryRow {
    contract_name: String,
    block_height: i64,
    tx_index: i32,
    change_type: Vec<ContractChangeType>,
    verifier: String,
    program_id: Vec<u8>,
    state_commitment: Vec<u8>,
    soft_timeout: Option<i64>,
    hard_timeout: Option<i64>,
    deleted_at_height: Option<i32>,
    parent_dp_hash: DataProposalHash,
    tx_hash: TxHash,
}

impl IndexerHandlerStore {
    fn status_rank(status: &TransactionStatusDb) -> u8 {
        match status {
            TransactionStatusDb::WaitingDissemination => 1,
            TransactionStatusDb::DataProposalCreated => 2,
            TransactionStatusDb::Sequenced => 3,
            TransactionStatusDb::Success
            | TransactionStatusDb::Failure
            | TransactionStatusDb::TimedOut => 4,
        }
    }

    fn max_status(a: TransactionStatusDb, b: TransactionStatusDb) -> TransactionStatusDb {
        if Self::status_rank(&b) > Self::status_rank(&a) {
            b
        } else {
            a
        }
    }

    fn merge_tx(&mut self, tx_id: TxId, tx: TxStore) {
        use std::collections::hash_map::Entry;
        match self.txs.entry(tx_id) {
            Entry::Occupied(mut entry) => {
                let existing = entry.get_mut();
                existing.transaction_status = Self::max_status(
                    existing.transaction_status.clone(),
                    tx.transaction_status.clone(),
                );
                existing.contract_names.extend(tx.contract_names);

                if tx.index > existing.index {
                    existing.index = tx.index;
                }
                if tx.block_height > existing.block_height {
                    existing.block_height = tx.block_height;
                }
                if tx.block_hash.is_some() {
                    existing.block_hash = tx.block_hash;
                }
                if tx.lane_id.is_some() {
                    existing.lane_id = tx.lane_id;
                }
                if tx.identity.is_some() {
                    existing.identity = tx.identity;
                }
                existing.transaction_type = tx.transaction_type;
            }
            Entry::Vacant(entry) => {
                entry.insert(tx);
            }
        }
    }

    fn merge_tx_status(&mut self, tx_id: TxId, status: TransactionStatusDb) {
        if let Some(existing) = self.txs.get_mut(&tx_id) {
            existing.transaction_status =
                Self::max_status(existing.transaction_status.clone(), status);
            return;
        }

        let tx_hash = tx_id.1.clone();
        let dp_hash = tx_id.0.clone();
        self.txs.insert(
            tx_id,
            TxStore {
                tx_hash,
                dp_hash,
                transaction_type: TransactionTypeDb::ProofTransaction,
                block_hash: None,
                block_height: BlockHeight(0),
                lane_id: None,
                index: 0,
                identity: None,
                transaction_status: status,
                contract_names: HashSet::new(),
            },
        );
    }

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
        if self.handler_store.tx_events.len() > self.conf.indexer.query_buffer_size
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
                    let tx_hash = tx.hashed().clone();
                    self.handler_store.merge_tx(
                        TxId(parent_data_proposal_hash.clone(), tx_hash.clone()),
                        TxStore {
                            tx_hash,
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
                                TransactionData::Blob(ref blob_tx) => {
                                    Some(blob_tx.identity.clone().0)
                                }
                                _ => None,
                            },
                            transaction_status: TransactionStatusDb::WaitingDissemination,
                            contract_names: match tx.transaction_data {
                                TransactionData::Blob(ref blob_tx) => blob_tx
                                    .blobs
                                    .iter()
                                    .map(|b| b.contract_name.clone())
                                    .collect(),
                                _ => HashSet::new(),
                            },
                        },
                    );
                }
                // We skip the blobs here or they'll conflict later and it's easier.
            }
            MempoolStatusEvent::DataProposalCreated { txs_metadatas, .. } => {
                for tx_metadata in txs_metadatas {
                    self.handler_store
                        .merge_tx_status(tx_metadata.id, TransactionStatusDb::DataProposalCreated);
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
                self.merge_tx(
                    tx_id.clone(),
                    TxStore {
                        tx_hash: tx_id.1.clone(),
                        dp_hash: tx_id.0.clone(),
                        transaction_type: TransactionTypeDb::BlobTransaction,
                        block_hash: Some(self.block_hash.clone()),
                        block_height: self.block_height,
                        lane_id: Some(lane_id.clone()),
                        index: index as i32,
                        identity: Some(blob_tx.identity.clone().0),
                        transaction_status: match *event {
                            TxEvent::RejectedBlobTransaction(..) => TransactionStatusDb::Failure,
                            _ => TransactionStatusDb::Sequenced,
                        },
                        contract_names: blob_tx
                            .blobs
                            .iter()
                            .map(|b| b.contract_name.clone())
                            .collect(),
                    },
                );
                for (index, blob) in blob_tx.blobs.iter().enumerate() {
                    let row = BlobRow {
                        parent_dp_hash: tx_id.0.clone(),
                        tx_hash: tx_id.1.clone(),
                        blob_index: index as i32,
                        identity: blob_tx.identity.0.clone(),
                        contract_name: blob.contract_name.clone(),
                        data: blob.data.0.clone(),
                    };
                    let key = BlobKey {
                        parent_dp_hash: row.parent_dp_hash.clone(),
                        tx_hash: row.tx_hash.clone(),
                        blob_index: row.blob_index,
                    };
                    self.blobs.entry(key).or_insert(row);
                }
            }
            TxEvent::SequencedProofTransaction(tx_id, lane_id, index, ..) => {
                self.tx_index_map.insert(tx_id.clone(), index as i32);
                self.merge_tx(
                    tx_id.clone(),
                    TxStore {
                        tx_hash: tx_id.1.clone(),
                        dp_hash: tx_id.0.clone(),
                        transaction_type: TransactionTypeDb::ProofTransaction,
                        block_hash: Some(self.block_hash.clone()),
                        block_height: self.block_height,
                        lane_id: Some(lane_id.clone()),
                        index: index as i32,
                        identity: None,
                        transaction_status: TransactionStatusDb::Success,
                        contract_names: HashSet::new(),
                    },
                );
            }
            TxEvent::Settled(tx_id, ..) => {
                self.merge_tx_status(tx_id.clone(), TransactionStatusDb::Success);
            }
            TxEvent::SettledAsFailed(tx_id, ..) => {
                self.merge_tx_status(tx_id.clone(), TransactionStatusDb::Failure);
            }
            TxEvent::TimedOut(tx_id, ..) => {
                self.merge_tx_status(tx_id.clone(), TransactionStatusDb::TimedOut);
            }
            TxEvent::TxError(..) => {}
            // Skip registering unproven blobs
            TxEvent::NewProof(..) => {}
            TxEvent::BlobSettled(tx_id, _tx, blob, blob_index, proof_data, blob_proof_index) => {
                // Can be None for executed blobs
                if let Some((_, _, proof_tx_id, hyli_output)) = proof_data {
                    let row = BlobProofOutputRow {
                        blob_parent_dp_hash: tx_id.0.clone(),
                        blob_tx_hash: tx_id.1.clone(),
                        proof_parent_dp_hash: proof_tx_id.0.clone(),
                        proof_tx_hash: proof_tx_id.1.clone(),
                        blob_index: blob_index.0 as i32,
                        blob_proof_output_index: blob_proof_index as i32,
                        contract_name: blob.contract_name.clone(),
                        hyli_output: serde_json::to_value(hyli_output)
                            .unwrap_or(serde_json::Value::Null),
                        settled: true,
                    };
                    let key = BlobProofOutputKey {
                        blob_parent_dp_hash: row.blob_parent_dp_hash.clone(),
                        blob_tx_hash: row.blob_tx_hash.clone(),
                        proof_parent_dp_hash: row.proof_parent_dp_hash.clone(),
                        proof_tx_hash: row.proof_tx_hash.clone(),
                        blob_index: row.blob_index,
                        blob_proof_output_index: row.blob_proof_output_index,
                    };
                    self.blob_proof_outputs.entry(key).or_insert(row);
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

                self.contract_inserts.insert(
                    contract_name.clone(),
                    ContractInsertStore {
                        contract_name: contract_name.clone(),
                        verifier: contract.verifier.0.clone(),
                        program_id: contract.program_id.0.clone(),
                        timeout_window: contract.timeout_window.clone(),
                        state_commitment: contract.state.0.clone(),
                        parent_dp_hash: tx_id.0.clone(),
                        tx_hash: tx_id.1.clone(),
                        metadata: metadata.clone(),
                        deleted_at_height: None,
                    },
                );
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
        self.tx_events.push(TxEventRow {
            block_hash: self.block_hash.clone(),
            block_height: self.block_height,
            parent_dp_hash: event.tx_id().0.clone(),
            tx_hash: event.tx_id().1.clone(),
            index: self.tx_events.len() as i32,
            event: serde_json::to_value(event).unwrap_or(serde_json::Value::Null),
        });
    }
}

impl Indexer {
    pub(crate) async fn dump_store_to_db(&mut self) -> Result<()> {
        tracing::debug!("Dumping SQL queries to database");

        let mut transaction = self.db.begin().await?;
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
        let tx_rows: Vec<TxStore> = self.handler_store.txs.drain().map(|(_, tx)| tx).collect();
        upsert_transactions(&mut transaction, tx_rows.as_slice()).await?;

        // Then txs_contracts
        let mut tx_contract_rows = Vec::new();
        for tx in &tx_rows {
            for contract_name in &tx.contract_names {
                contract_notifications
                    .entry(contract_name.clone())
                    .or_default()
                    .insert(tx.block_height);
                tx_contract_rows.push(TxContractRow {
                    parent_dp_hash: tx.dp_hash.clone(),
                    tx_hash: tx.tx_hash.clone(),
                    contract_name: contract_name.0.clone(),
                });
            }
        }
        insert_txs_contracts(&mut transaction, tx_contract_rows).await?;

        // Merge contract updates into inserts when both affect the same row in this flush.
        let pending_contract_updates = std::mem::take(&mut self.handler_store.contract_updates);
        for (contract_name, update) in pending_contract_updates {
            if let Some(insert) = self.handler_store.contract_inserts.get_mut(&contract_name) {
                if let Some(verifier) = update.verifier {
                    insert.verifier = verifier;
                }
                if let Some(program_id) = update.program_id {
                    insert.program_id = program_id;
                }
                if let Some(timeout_window) = update.timeout_window {
                    insert.timeout_window = timeout_window;
                }
                if let Some(state_commitment) = update.state_commitment {
                    insert.state_commitment = state_commitment;
                }
                if let Some(deleted_at_height) = update.deleted_at_height {
                    insert.deleted_at_height = Some(deleted_at_height);
                }
            } else {
                self.handler_store
                    .contract_updates
                    .insert(contract_name, update);
            }
        }

        let mut contract_rows = Vec::new();
        for (_, contract) in self.handler_store.contract_inserts.drain() {
            let (soft_timeout, hard_timeout) = timeout_columns(&contract.timeout_window)?;
            contract_rows.push(ContractUpsertRow {
                contract_name: contract.contract_name.0,
                verifier: contract.verifier,
                program_id: contract.program_id,
                soft_timeout,
                hard_timeout,
                state_commitment: contract.state_commitment,
                parent_dp_hash: contract.parent_dp_hash,
                tx_hash: contract.tx_hash,
                metadata: contract.metadata,
                deleted_at_height: contract.deleted_at_height,
            });
        }
        upsert_contracts(&mut transaction, contract_rows).await?;

        // Insert contract history
        let contract_history_rows: Vec<ContractHistoryRow> = self
            .handler_store
            .contract_history
            .drain()
            .map(|(_, history)| ContractHistoryRow {
                contract_name: history.contract_name.0,
                block_height: history.block_height.0 as i64,
                tx_index: history.tx_index,
                change_type: history.change_type,
                verifier: history.verifier,
                program_id: history.program_id,
                state_commitment: history.state_commitment,
                soft_timeout: history.soft_timeout,
                hard_timeout: history.hard_timeout,
                deleted_at_height: history.deleted_at_height,
                parent_dp_hash: history.parent_dp_hash,
                tx_hash: history.tx_hash,
            })
            .collect();
        insert_contract_history(&mut transaction, contract_history_rows).await?;

        let tx_event_rows = std::mem::take(&mut self.handler_store.tx_events);
        insert_or_copy_tx_events(&mut transaction, tx_event_rows).await?;

        let blob_rows: Vec<_> = self
            .handler_store
            .blobs
            .drain()
            .map(|(_, row)| row)
            .collect();
        insert_or_copy_blobs(&mut transaction, blob_rows).await?;

        let proof_rows: Vec<_> = self
            .handler_store
            .blob_proof_outputs
            .drain()
            .map(|(_, row)| row)
            .collect();
        insert_or_copy_blob_proof_outputs(&mut transaction, proof_rows).await?;

        let mut contract_update_rows =
            Vec::with_capacity(self.handler_store.contract_updates.len());
        for (contract_name, update) in self.handler_store.contract_updates.drain() {
            let (soft_timeout, hard_timeout) = timeout_columns_opt(&update.timeout_window)?;
            contract_update_rows.push(ContractUpdateRow {
                contract_name: contract_name.0,
                verifier: update.verifier,
                program_id: update.program_id,
                soft_timeout,
                hard_timeout,
                state_commitment: update.state_commitment,
                deleted_at_height: update.deleted_at_height,
            });
        }
        apply_contract_updates(&mut transaction, contract_update_rows).await?;

        if !contract_notifications.is_empty() {
            send_contract_notifications(&mut *transaction, contract_notifications).await?;
        }
        if !block_notifications.is_empty() {
            send_block_notifications(&mut *transaction, block_notifications).await?;
        }

        transaction.commit().await?;
        Ok(())
    }
}

const INLINE_INSERT_THRESHOLD: usize = 400;
const MAX_BLOB_BYTES_PER_BATCH: usize = 200 * 1024 * 1024;
const MAX_BLOB_ROWS_PER_BATCH: usize = 4096;

fn transaction_type_text(value: &TransactionTypeDb) -> &'static str {
    match value {
        TransactionTypeDb::BlobTransaction => "blob_transaction",
        TransactionTypeDb::ProofTransaction => "proof_transaction",
        TransactionTypeDb::RegisterContractTransaction => "register_contract_transaction",
        TransactionTypeDb::Stake => "stake",
    }
}

fn transaction_status_text(value: &TransactionStatusDb) -> &'static str {
    match value {
        TransactionStatusDb::WaitingDissemination => "waiting_dissemination",
        TransactionStatusDb::DataProposalCreated => "data_proposal_created",
        TransactionStatusDb::Success => "success",
        TransactionStatusDb::Failure => "failure",
        TransactionStatusDb::Sequenced => "sequenced",
        TransactionStatusDb::TimedOut => "timed_out",
    }
}

fn contract_change_type_text(value: &ContractChangeType) -> &'static str {
    match value {
        ContractChangeType::Registered => "registered",
        ContractChangeType::ProgramIdUpdated => "program_id_updated",
        ContractChangeType::StateUpdated => "state_updated",
        ContractChangeType::TimeoutUpdated => "timeout_updated",
        ContractChangeType::Deleted => "deleted",
    }
}

fn contract_change_type_array_text(values: &[ContractChangeType]) -> String {
    let mut out = String::from("{");
    for (i, value) in values.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push_str(contract_change_type_text(value));
    }
    out.push('}');
    out
}

async fn upsert_transactions(
    transaction: &mut sqlx::Transaction<'_, Postgres>,
    rows: &[TxStore],
) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }

    if rows.len() <= INLINE_INSERT_THRESHOLD {
        let tx_batch_size = calculate_optimal_batch_size(10);
        for chunk in rows.chunks(tx_batch_size) {
            let mut query_builder = QueryBuilder::<Postgres>::new(
                "INSERT INTO transactions (parent_dp_hash, tx_hash, version, transaction_type, transaction_status, block_hash, block_height, lane_id, index, identity) ",
            );
            query_builder.push_values(chunk.iter(), |mut b, tx| {
                b.push_bind(tx.dp_hash.clone())
                    .push_bind(tx.tx_hash.clone())
                    .push_bind(1_i32)
                    .push_bind(tx.transaction_type.clone())
                    .push_bind(tx.transaction_status.clone())
                    .push_bind(tx.block_hash.clone())
                    .push_bind(tx.block_height.0 as i64)
                    .push_bind(tx.lane_id.clone())
                    .push_bind(tx.index)
                    .push_bind(tx.identity.clone());
            });

            query_builder.push(
                " ON CONFLICT (parent_dp_hash, tx_hash)
                DO UPDATE SET
                    index = GREATEST(transactions.index, excluded.index),
                    lane_id = COALESCE(excluded.lane_id, transactions.lane_id),
                    block_hash = COALESCE(excluded.block_hash, transactions.block_hash),
                    block_height = GREATEST(excluded.block_height, transactions.block_height),
                    identity = COALESCE(excluded.identity, transactions.identity),
                    transaction_status = CASE
                        WHEN (
                            CASE transactions.transaction_status
                                WHEN 'waiting_dissemination' THEN 1
                                WHEN 'data_proposal_created' THEN 2
                                WHEN 'sequenced' THEN 3
                                WHEN 'success' THEN 4
                                WHEN 'failure' THEN 4
                                WHEN 'timed_out' THEN 4
                            END
                        ) < (
                            CASE excluded.transaction_status
                                WHEN 'waiting_dissemination' THEN 1
                                WHEN 'data_proposal_created' THEN 2
                                WHEN 'sequenced' THEN 3
                                WHEN 'success' THEN 4
                                WHEN 'failure' THEN 4
                                WHEN 'timed_out' THEN 4
                            END
                        )
                        THEN excluded.transaction_status
                        ELSE transactions.transaction_status
                    END",
            );
            query_builder.build().execute(&mut **transaction).await?;
        }
        return Ok(());
    }

    sqlx::query(
        "CREATE TEMPORARY TABLE transactions_stage (
            parent_dp_hash TEXT NOT NULL,
            tx_hash TEXT NOT NULL,
            version INT NOT NULL,
            transaction_type transaction_type NOT NULL,
            transaction_status transaction_status NOT NULL,
            block_hash TEXT,
            block_height BIGINT,
            lane_id TEXT,
            index INT,
            identity TEXT
        ) ON COMMIT DROP",
    )
    .execute(&mut **transaction)
    .await?;

    let mut lines = Vec::with_capacity(rows.len());
    for tx in rows {
        lines.push(format!(
            "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
            tx.dp_hash,
            tx.tx_hash,
            1_i32,
            transaction_type_text(&tx.transaction_type),
            transaction_status_text(&tx.transaction_status),
            copy_optional_owned_text(tx.block_hash.as_ref().map(ToString::to_string)),
            tx.block_height.0,
            copy_optional_owned_text(tx.lane_id.as_ref().map(ToString::to_string)),
            tx.index,
            copy_optional_text(tx.identity.as_deref()),
        ));
    }

    let mut stream = StreamableData::new(lines);
    let mut copy = transaction
        .copy_in_raw(
            "COPY transactions_stage (parent_dp_hash, tx_hash, version, transaction_type, transaction_status, block_hash, block_height, lane_id, index, identity) FROM STDIN WITH (FORMAT TEXT)",
        )
        .await?;
    copy.read_from(&mut stream).await?;
    copy.finish().await?;

    sqlx::query(
        "INSERT INTO transactions (parent_dp_hash, tx_hash, version, transaction_type, transaction_status, block_hash, block_height, lane_id, index, identity)
         SELECT parent_dp_hash, tx_hash, version, transaction_type, transaction_status, block_hash, block_height, lane_id, index, identity
         FROM transactions_stage
         ON CONFLICT (parent_dp_hash, tx_hash)
         DO UPDATE SET
            index = GREATEST(transactions.index, excluded.index),
            lane_id = COALESCE(excluded.lane_id, transactions.lane_id),
            block_hash = COALESCE(excluded.block_hash, transactions.block_hash),
            block_height = GREATEST(excluded.block_height, transactions.block_height),
            identity = COALESCE(excluded.identity, transactions.identity),
            transaction_status = CASE
                WHEN (
                    CASE transactions.transaction_status
                        WHEN 'waiting_dissemination' THEN 1
                        WHEN 'data_proposal_created' THEN 2
                        WHEN 'sequenced' THEN 3
                        WHEN 'success' THEN 4
                        WHEN 'failure' THEN 4
                        WHEN 'timed_out' THEN 4
                    END
                ) < (
                    CASE excluded.transaction_status
                        WHEN 'waiting_dissemination' THEN 1
                        WHEN 'data_proposal_created' THEN 2
                        WHEN 'sequenced' THEN 3
                        WHEN 'success' THEN 4
                        WHEN 'failure' THEN 4
                        WHEN 'timed_out' THEN 4
                    END
                )
                THEN excluded.transaction_status
                ELSE transactions.transaction_status
            END",
    )
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn insert_txs_contracts(
    transaction: &mut sqlx::Transaction<'_, Postgres>,
    rows: Vec<TxContractRow>,
) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }

    if rows.len() <= INLINE_INSERT_THRESHOLD {
        let batch_size = calculate_optimal_batch_size(3);
        for chunk in rows.chunks(batch_size) {
            let mut query_builder = QueryBuilder::<Postgres>::new(
                "INSERT INTO txs_contracts (parent_dp_hash, tx_hash, contract_name) ",
            );
            query_builder.push_values(chunk.iter(), |mut b, row| {
                b.push_bind(row.parent_dp_hash.clone())
                    .push_bind(row.tx_hash.clone())
                    .push_bind(row.contract_name.clone());
            });
            query_builder.push(" ON CONFLICT DO NOTHING");
            query_builder.build().execute(&mut **transaction).await?;
        }
        return Ok(());
    }

    sqlx::query(
        "CREATE TEMPORARY TABLE txs_contracts_stage (
            parent_dp_hash TEXT NOT NULL,
            tx_hash TEXT NOT NULL,
            contract_name TEXT NOT NULL
        ) ON COMMIT DROP",
    )
    .execute(&mut **transaction)
    .await?;

    let mut lines = Vec::with_capacity(rows.len());
    for row in rows {
        lines.push(format!(
            "{}\t{}\t{}\n",
            row.parent_dp_hash,
            row.tx_hash,
            escape_copy_text(row.contract_name.as_str())
        ));
    }
    let mut stream = StreamableData::new(lines);
    let mut copy = transaction
        .copy_in_raw(
            "COPY txs_contracts_stage (parent_dp_hash, tx_hash, contract_name) FROM STDIN WITH (FORMAT TEXT)",
        )
        .await?;
    copy.read_from(&mut stream).await?;
    copy.finish().await?;

    sqlx::query(
        "INSERT INTO txs_contracts (parent_dp_hash, tx_hash, contract_name)
         SELECT parent_dp_hash, tx_hash, contract_name FROM txs_contracts_stage
         ON CONFLICT DO NOTHING",
    )
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn upsert_contracts(
    transaction: &mut sqlx::Transaction<'_, Postgres>,
    rows: Vec<ContractUpsertRow>,
) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }

    if rows.len() <= INLINE_INSERT_THRESHOLD {
        let batch_size = calculate_optimal_batch_size(10);
        for chunk in rows.chunks(batch_size) {
            let mut query_builder = QueryBuilder::<Postgres>::new(
                "INSERT INTO contracts (contract_name, verifier, program_id, soft_timeout, hard_timeout, state_commitment, parent_dp_hash, tx_hash, metadata, deleted_at_height) ",
            );
            query_builder.push_values(chunk.iter(), |mut b, row| {
                b.push_bind(row.contract_name.clone())
                    .push_bind(row.verifier.clone())
                    .push_bind(row.program_id.clone())
                    .push_bind(row.soft_timeout)
                    .push_bind(row.hard_timeout)
                    .push_bind(row.state_commitment.clone())
                    .push_bind(row.parent_dp_hash.clone())
                    .push_bind(row.tx_hash.clone())
                    .push_bind(row.metadata.clone())
                    .push_bind(row.deleted_at_height);
            });
            query_builder.push(
                " ON CONFLICT (contract_name) DO UPDATE SET
                    verifier = EXCLUDED.verifier,
                    program_id = EXCLUDED.program_id,
                    soft_timeout = EXCLUDED.soft_timeout,
                    hard_timeout = EXCLUDED.hard_timeout,
                    state_commitment = EXCLUDED.state_commitment,
                    parent_dp_hash = EXCLUDED.parent_dp_hash,
                    tx_hash = EXCLUDED.tx_hash,
                    metadata = EXCLUDED.metadata,
                    deleted_at_height = EXCLUDED.deleted_at_height",
            );
            query_builder.build().execute(&mut **transaction).await?;
        }
        return Ok(());
    }

    sqlx::query(
        "CREATE TEMPORARY TABLE contracts_stage (
            contract_name TEXT PRIMARY KEY NOT NULL,
            verifier TEXT NOT NULL,
            program_id BYTEA NOT NULL,
            soft_timeout BIGINT,
            hard_timeout BIGINT,
            state_commitment BYTEA NOT NULL,
            parent_dp_hash TEXT NOT NULL,
            tx_hash TEXT NOT NULL,
            metadata BYTEA,
            deleted_at_height INT
        ) ON COMMIT DROP",
    )
    .execute(&mut **transaction)
    .await?;

    let mut lines = Vec::with_capacity(rows.len());
    for row in rows {
        lines.push(format!(
            "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
            escape_copy_text(row.contract_name.as_str()),
            escape_copy_text(row.verifier.as_str()),
            format!("\\\\x{}", hex::encode(row.program_id)),
            copy_optional_i64(row.soft_timeout),
            copy_optional_i64(row.hard_timeout),
            format!("\\\\x{}", hex::encode(row.state_commitment)),
            row.parent_dp_hash,
            row.tx_hash,
            copy_optional_bytea(row.metadata.as_deref()),
            copy_optional_i32(row.deleted_at_height),
        ));
    }
    let mut stream = StreamableData::new(lines);
    let mut copy = transaction
        .copy_in_raw(
            "COPY contracts_stage (contract_name, verifier, program_id, soft_timeout, hard_timeout, state_commitment, parent_dp_hash, tx_hash, metadata, deleted_at_height) FROM STDIN WITH (FORMAT TEXT)",
        )
        .await?;
    copy.read_from(&mut stream).await?;
    copy.finish().await?;

    sqlx::query(
        "INSERT INTO contracts (contract_name, verifier, program_id, soft_timeout, hard_timeout, state_commitment, parent_dp_hash, tx_hash, metadata, deleted_at_height)
         SELECT contract_name, verifier, program_id, soft_timeout, hard_timeout, state_commitment, parent_dp_hash, tx_hash, metadata, deleted_at_height
         FROM contracts_stage
         ON CONFLICT (contract_name) DO UPDATE SET
            verifier = EXCLUDED.verifier,
            program_id = EXCLUDED.program_id,
            soft_timeout = EXCLUDED.soft_timeout,
            hard_timeout = EXCLUDED.hard_timeout,
            state_commitment = EXCLUDED.state_commitment,
            parent_dp_hash = EXCLUDED.parent_dp_hash,
            tx_hash = EXCLUDED.tx_hash,
            metadata = EXCLUDED.metadata,
            deleted_at_height = EXCLUDED.deleted_at_height",
    )
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn insert_contract_history(
    transaction: &mut sqlx::Transaction<'_, Postgres>,
    rows: Vec<ContractHistoryRow>,
) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }

    if rows.len() <= INLINE_INSERT_THRESHOLD {
        let batch_size = calculate_optimal_batch_size(12);
        for chunk in rows.chunks(batch_size) {
            let mut query_builder = QueryBuilder::<Postgres>::new(
                "INSERT INTO contract_history (contract_name, block_height, tx_index, change_type, verifier, program_id, state_commitment, soft_timeout, hard_timeout, deleted_at_height, parent_dp_hash, tx_hash) ",
            );
            query_builder.push_values(chunk.iter(), |mut b, row| {
                b.push_bind(row.contract_name.clone())
                    .push_bind(row.block_height)
                    .push_bind(row.tx_index)
                    .push_bind(row.change_type.clone())
                    .push_bind(row.verifier.clone())
                    .push_bind(row.program_id.clone())
                    .push_bind(row.state_commitment.clone())
                    .push_bind(row.soft_timeout)
                    .push_bind(row.hard_timeout)
                    .push_bind(row.deleted_at_height)
                    .push_bind(row.parent_dp_hash.clone())
                    .push_bind(row.tx_hash.clone());
            });
            query_builder.push(" ON CONFLICT (contract_name, block_height, tx_index) DO NOTHING");
            query_builder.build().execute(&mut **transaction).await?;
        }
        return Ok(());
    }

    sqlx::query(
        "CREATE TEMPORARY TABLE contract_history_stage (
            contract_name TEXT NOT NULL,
            block_height BIGINT NOT NULL,
            tx_index INT NOT NULL,
            change_type contract_change_type[] NOT NULL,
            verifier TEXT NOT NULL,
            program_id BYTEA NOT NULL,
            state_commitment BYTEA NOT NULL,
            soft_timeout BIGINT,
            hard_timeout BIGINT,
            deleted_at_height INT,
            parent_dp_hash TEXT NOT NULL,
            tx_hash TEXT NOT NULL
        ) ON COMMIT DROP",
    )
    .execute(&mut **transaction)
    .await?;

    let mut lines = Vec::with_capacity(rows.len());
    for row in rows {
        lines.push(format!(
            "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
            escape_copy_text(row.contract_name.as_str()),
            row.block_height,
            row.tx_index,
            contract_change_type_array_text(&row.change_type),
            escape_copy_text(row.verifier.as_str()),
            format!("\\\\x{}", hex::encode(row.program_id)),
            format!("\\\\x{}", hex::encode(row.state_commitment)),
            copy_optional_i64(row.soft_timeout),
            copy_optional_i64(row.hard_timeout),
            copy_optional_i32(row.deleted_at_height),
            row.parent_dp_hash,
            row.tx_hash,
        ));
    }

    let mut stream = StreamableData::new(lines);
    let mut copy = transaction
        .copy_in_raw(
            "COPY contract_history_stage (contract_name, block_height, tx_index, change_type, verifier, program_id, state_commitment, soft_timeout, hard_timeout, deleted_at_height, parent_dp_hash, tx_hash) FROM STDIN WITH (FORMAT TEXT)",
        )
        .await?;
    copy.read_from(&mut stream).await?;
    copy.finish().await?;

    sqlx::query(
        "INSERT INTO contract_history (contract_name, block_height, tx_index, change_type, verifier, program_id, state_commitment, soft_timeout, hard_timeout, deleted_at_height, parent_dp_hash, tx_hash)
         SELECT contract_name, block_height, tx_index, change_type, verifier, program_id, state_commitment, soft_timeout, hard_timeout, deleted_at_height, parent_dp_hash, tx_hash
         FROM contract_history_stage
         ON CONFLICT (contract_name, block_height, tx_index) DO NOTHING",
    )
    .execute(&mut **transaction)
    .await?;

    Ok(())
}

async fn insert_or_copy_tx_events(
    transaction: &mut sqlx::Transaction<'_, Postgres>,
    rows: Vec<TxEventRow>,
) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }

    if rows.len() <= INLINE_INSERT_THRESHOLD {
        let mut query_builder = QueryBuilder::<Postgres>::new(
            "INSERT INTO transaction_state_events (block_hash, block_height, parent_dp_hash, tx_hash, index, event) ",
        );
        query_builder.push_values(rows.into_iter(), |mut b, row| {
            b.push_bind(row.block_hash)
                .push_bind(row.block_height.0 as i64)
                .push_bind(row.parent_dp_hash)
                .push_bind(row.tx_hash)
                .push_bind(row.index)
                .push_bind(row.event);
        });
        query_builder.build().execute(&mut **transaction).await?;
        return Ok(());
    }

    let mut lines = Vec::with_capacity(rows.len());
    for row in rows {
        lines.push(format!(
            "{}\t{}\t{}\t{}\t{}\t{}\n",
            row.block_hash,
            row.block_height,
            row.parent_dp_hash,
            row.tx_hash,
            row.index,
            escape_copy_text(row.event.to_string().as_str()),
        ));
    }
    let mut stream = StreamableData::new(lines);
    let mut copy = transaction
        .copy_in_raw("COPY transaction_state_events (block_hash, block_height, parent_dp_hash, tx_hash, index, event) FROM STDIN WITH (FORMAT TEXT)")
        .await?;
    copy.read_from(&mut stream).await?;
    copy.finish().await?;
    Ok(())
}

async fn insert_or_copy_blobs(
    transaction: &mut sqlx::Transaction<'_, Postgres>,
    rows: Vec<BlobRow>,
) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }

    // For large BYTEA payloads, prefer bind-based VALUES with byte-size chunking.
    // COPY text would hex-expand payload (~2x bytes) and can be much slower.
    let mut batch = Vec::new();
    let mut batch_bytes = 0_usize;

    for row in rows {
        let row_bytes = row.data.len();
        let would_overflow_rows = batch.len() >= MAX_BLOB_ROWS_PER_BATCH;
        let would_overflow_bytes =
            !batch.is_empty() && batch_bytes.saturating_add(row_bytes) > MAX_BLOB_BYTES_PER_BATCH;
        if would_overflow_rows || would_overflow_bytes {
            insert_blobs_values(transaction, std::mem::take(&mut batch)).await?;
            batch_bytes = 0;
        }
        batch_bytes = batch_bytes.saturating_add(row_bytes);
        batch.push(row);
    }

    if !batch.is_empty() {
        insert_blobs_values(transaction, batch).await?;
    }
    Ok(())
}

async fn insert_blobs_values(
    transaction: &mut sqlx::Transaction<'_, Postgres>,
    rows: Vec<BlobRow>,
) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    let mut query_builder = QueryBuilder::<Postgres>::new(
        "INSERT INTO blobs (parent_dp_hash, tx_hash, blob_index, identity, contract_name, data) ",
    );
    query_builder.push_values(rows.into_iter(), |mut b, row| {
        b.push_bind(row.parent_dp_hash)
            .push_bind(row.tx_hash)
            .push_bind(row.blob_index)
            .push_bind(row.identity)
            .push_bind(row.contract_name.0)
            .push_bind(row.data);
    });
    query_builder.build().execute(&mut **transaction).await?;
    Ok(())
}

async fn insert_or_copy_blob_proof_outputs(
    transaction: &mut sqlx::Transaction<'_, Postgres>,
    rows: Vec<BlobProofOutputRow>,
) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }

    if rows.len() <= INLINE_INSERT_THRESHOLD {
        let mut query_builder = QueryBuilder::<Postgres>::new(
            "INSERT INTO blob_proof_outputs (blob_parent_dp_hash, blob_tx_hash, proof_parent_dp_hash, proof_tx_hash, blob_index, blob_proof_output_index, contract_name, hyli_output, settled) ",
        );
        query_builder.push_values(rows.into_iter(), |mut b, row| {
            b.push_bind(row.blob_parent_dp_hash)
                .push_bind(row.blob_tx_hash)
                .push_bind(row.proof_parent_dp_hash)
                .push_bind(row.proof_tx_hash)
                .push_bind(row.blob_index)
                .push_bind(row.blob_proof_output_index)
                .push_bind(row.contract_name.0)
                .push_bind(row.hyli_output)
                .push_bind(row.settled);
        });
        query_builder.build().execute(&mut **transaction).await?;
        return Ok(());
    }

    let mut lines = Vec::with_capacity(rows.len());
    for row in rows {
        lines.push(format!(
            "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
            row.blob_parent_dp_hash,
            row.blob_tx_hash,
            row.proof_parent_dp_hash,
            row.proof_tx_hash,
            row.blob_index,
            row.blob_proof_output_index,
            row.contract_name,
            escape_copy_text(row.hyli_output.to_string().as_str()),
            row.settled,
        ));
    }
    let mut stream = StreamableData::new(lines);
    let mut copy = transaction
        .copy_in_raw("COPY blob_proof_outputs (blob_parent_dp_hash, blob_tx_hash, proof_parent_dp_hash, proof_tx_hash, blob_index, blob_proof_output_index, contract_name, hyli_output, settled) FROM STDIN WITH (FORMAT TEXT)")
        .await?;
    copy.read_from(&mut stream).await?;
    copy.finish().await?;
    Ok(())
}

async fn apply_contract_updates(
    transaction: &mut sqlx::Transaction<'_, Postgres>,
    rows: Vec<ContractUpdateRow>,
) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }

    if rows.len() <= INLINE_INSERT_THRESHOLD {
        let mut query_builder = QueryBuilder::<Postgres>::new(
            "UPDATE contracts SET
                verifier = COALESCE(contract_updates.verifier, contracts.verifier),
                program_id = COALESCE(contract_updates.program_id, contracts.program_id),
                soft_timeout = COALESCE(contract_updates.soft_timeout, contracts.soft_timeout),
                hard_timeout = COALESCE(contract_updates.hard_timeout, contracts.hard_timeout),
                state_commitment = COALESCE(contract_updates.state_commitment, contracts.state_commitment),
                deleted_at_height = COALESCE(contract_updates.deleted_at_height, contracts.deleted_at_height)
            FROM (",
        );
        query_builder.push_values(rows.into_iter(), |mut b, row| {
            b.push_bind(row.contract_name)
                .push_bind(row.verifier)
                .push_bind(row.program_id)
                .push_bind(row.soft_timeout)
                .push_bind(row.hard_timeout)
                .push_bind(row.state_commitment)
                .push_bind(row.deleted_at_height);
        });
        query_builder.push(
            ") AS contract_updates(contract_name, verifier, program_id, soft_timeout, hard_timeout, state_commitment, deleted_at_height)
            WHERE contracts.contract_name = contract_updates.contract_name",
        );
        query_builder.build().execute(&mut **transaction).await?;
        return Ok(());
    }

    sqlx::query(
        "CREATE TEMPORARY TABLE contract_updates (
            contract_name TEXT PRIMARY KEY NOT NULL,
            verifier TEXT,
            program_id BYTEA,
            soft_timeout BIGINT,
            hard_timeout BIGINT,
            state_commitment BYTEA,
            deleted_at_height INT
        ) ON COMMIT DROP",
    )
    .execute(&mut **transaction)
    .await?;

    let mut updates_stream = StreamableData::default();
    for row in rows {
        updates_stream.0.push(format!(
            "{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
            escape_copy_text(row.contract_name.as_str()),
            copy_optional_text(row.verifier.as_deref()),
            copy_optional_bytea(row.program_id.as_deref()),
            copy_optional_i64(row.soft_timeout),
            copy_optional_i64(row.hard_timeout),
            copy_optional_bytea(row.state_commitment.as_deref()),
            copy_optional_i32(row.deleted_at_height),
        ));
    }

    let mut copy = transaction
        .copy_in_raw(
            "COPY contract_updates (contract_name, verifier, program_id, soft_timeout, hard_timeout, state_commitment, deleted_at_height) FROM STDIN WITH (FORMAT TEXT)",
        )
        .await?;
    copy.read_from(&mut updates_stream).await?;
    copy.finish().await?;

    sqlx::query(
        "UPDATE contracts SET
            verifier = COALESCE(contract_updates.verifier, contracts.verifier),
            program_id = COALESCE(contract_updates.program_id, contracts.program_id),
            soft_timeout = COALESCE(contract_updates.soft_timeout, contracts.soft_timeout),
            hard_timeout = COALESCE(contract_updates.hard_timeout, contracts.hard_timeout),
            state_commitment = COALESCE(contract_updates.state_commitment, contracts.state_commitment),
            deleted_at_height = COALESCE(contract_updates.deleted_at_height, contracts.deleted_at_height)
        FROM contract_updates
        WHERE contracts.contract_name = contract_updates.contract_name",
    )
    .execute(&mut **transaction)
    .await?;

    Ok(())
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

fn escape_copy_text(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('\t', "\\t")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
}

fn copy_optional_text(value: Option<&str>) -> String {
    value
        .map(escape_copy_text)
        .unwrap_or_else(|| "\\N".to_string())
}

fn copy_optional_owned_text(value: Option<String>) -> String {
    value
        .as_deref()
        .map(escape_copy_text)
        .unwrap_or_else(|| "\\N".to_string())
}

fn copy_optional_bytea(value: Option<&[u8]>) -> String {
    value
        .map(|v| format!("\\\\x{}", hex::encode(v)))
        .unwrap_or_else(|| "\\N".to_string())
}

fn copy_optional_i64(value: Option<i64>) -> String {
    value
        .map(|v| v.to_string())
        .unwrap_or_else(|| "\\N".to_string())
}

fn copy_optional_i32(value: Option<i32>) -> String {
    value
        .map(|v| v.to_string())
        .unwrap_or_else(|| "\\N".to_string())
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

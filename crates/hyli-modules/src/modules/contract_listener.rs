use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::{path::PathBuf, time::Duration};

use anyhow::{Context, Result};
use borsh::{BorshDeserialize, BorshSerialize};
use hyli_bus::modules::ModulePersistOutput;
use hyli_model::api::ContractChangeType;
use hyli_model::utils::TimestampMs;
use hyli_model::{BlockHeight, ContractName, Identity, TxHash};
use indexmap::IndexMap;
use sdk::api::TransactionStatusDb;
use sdk::{
    Blob, BlobData, BlobIndex, BlobTransaction, BlockHash, DataProposalHash, IndexedBlobs, LaneId,
    TxContext, TxId, HYLI_TESTNET_CHAIN_ID,
};
use sqlx::postgres::{PgListener, PgPoolOptions, PgRow};
use sqlx::{PgPool, Row};
use tracing::{debug, info, trace, warn};

use crate::bus::{BusClientSender, BusMessage, SharedMessageBus};
use crate::modules::Module;
use crate::{module_bus_client, module_handle_messages};

#[derive(Debug, Clone)]
pub struct ContractListenerConf {
    pub database_url: String,
    pub data_directory: PathBuf,
    pub contracts: HashSet<ContractName>,
    /// How often to poll for missed events.
    pub poll_interval: Duration,
    /// When true, all settled transactions are replayed from the beginning on startup.
    pub replay_settled_from_start: bool,
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize, PartialEq)]
pub struct ContractTx {
    pub tx_id: TxId,
    pub tx: BlobTransaction,
    pub tx_ctx: Arc<TxContext>,
    pub status: TransactionStatusDb,
    pub contract_changes: HashMap<ContractName, ContractChangeData>,
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize, PartialEq)]
pub struct ContractChangeData {
    pub change_types: Vec<ContractChangeType>,
    pub metadata: Option<Vec<u8>>,
    pub verifier: String,
    pub program_id: Vec<u8>,
    pub state_commitment: Vec<u8>,
    pub soft_timeout: Option<i64>,
    pub hard_timeout: Option<i64>,
    pub deleted_at_height: Option<i32>,
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize, PartialEq)]
pub enum ContractListenerEvent {
    SequencedTx(ContractTx),
    SettledTx(ContractTx),
    BackfillComplete(ContractName),
}

impl BusMessage for ContractListenerEvent {}

module_bus_client! {
#[derive(Debug)]
struct ContractListenerBusClient {
    sender(ContractListenerEvent),
}
}

pub struct ContractListener {
    bus: ContractListenerBusClient,
    pool: PgPool,
    listener: PgListener,
    conf: ContractListenerConf,
    store: ContractListenerStore,
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize, PartialEq)]
struct BlockCursor {
    height: BlockHeight,
    index: i32,
}

impl Default for BlockCursor {
    fn default() -> Self {
        Self {
            height: BlockHeight(0),
            index: -1,
        }
    }
}

#[derive(Debug, Default, Clone, BorshSerialize, BorshDeserialize, PartialEq)]
struct ContractListenerStore {
    last_sequenced_block_cursor: BlockCursor,
    last_settled_block_cursor: BlockCursor,
}

const CONTRACT_LISTENER_STATE_FILE: &str = "contract_listener.bin";

impl Module for ContractListener {
    type Context = ContractListenerConf;

    async fn build(bus: SharedMessageBus, ctx: Self::Context) -> Result<Self> {
        let bus = ContractListenerBusClient::new_from_bus(bus.new_handle()).await;

        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(&ctx.database_url)
            .await
            .context("connect postgres for ContractPgListener")?;

        let listener = PgListener::connect(&ctx.database_url)
            .await
            .context("connect listener for ContractPgListener")?;

        info!(
            "📡 ContractPgListener listening on {} contract channels",
            ctx.contracts.len()
        );

        let state_path = PathBuf::from(CONTRACT_LISTENER_STATE_FILE);
        let store = match Self::load_from_disk::<ContractListenerStore>(
            &ctx.data_directory,
            &state_path,
        )? {
            Some(s) => s,
            None => {
                warn!("Starting ContractListenerStore from default.");
                ContractListenerStore::default()
            }
        };
        let mut store = store;
        if ctx.replay_settled_from_start {
            store.last_settled_block_cursor = BlockCursor::default();
        }

        Ok(Self {
            bus,
            pool,
            listener,
            conf: ctx.clone(),
            store,
        })
    }

    async fn run(&mut self) -> Result<()> {
        self.start().await
    }

    async fn persist(&mut self) -> Result<ModulePersistOutput> {
        // Resetting last_seen_block_cursor to default.
        // This forces reprocessing of all sequenced transactions on restart.
        self.store.last_sequenced_block_cursor = BlockCursor::default();
        if self.conf.replay_settled_from_start {
            self.store.last_settled_block_cursor = BlockCursor::default();
        }

        let file = PathBuf::from(CONTRACT_LISTENER_STATE_FILE);
        let checksum = Self::save_on_disk(&self.conf.data_directory, &file, &self.store)?;
        Ok(vec![(self.conf.data_directory.join(file), checksum)])
    }
}

impl ContractListener {
    async fn start(&mut self) -> Result<()> {
        // Subscribe to postgres channels for each contract so notifications are received.
        for contract in &self.conf.contracts {
            self.listener
                .listen(contract.0.as_str())
                .await
                .with_context(|| format!("listening to contract channel {}", contract))?;
        }

        info!(
            "📡 ContractPgListener initial global cursors: sequenced=({}, {}), settled=({}, {})",
            self.store.last_sequenced_block_cursor.height.0,
            self.store.last_sequenced_block_cursor.index,
            self.store.last_settled_block_cursor.height.0,
            self.store.last_settled_block_cursor.index
        );

        // Dispatch any unprocessed txs at startup.
        self.handle_sequenced_txs().await?;
        self.handle_settled_txs().await?;
        for contract_name in self.conf.contracts.clone() {
            self.bus.send(ContractListenerEvent::BackfillComplete(
                contract_name.clone(),
            ))?;
        }

        module_handle_messages! {
            on_self self,
            notif = self.listener.recv() => {
                match notif {
                    Ok(notification) => {
                        let contract_name = notification.channel().into();
                        debug!("🔔 Received notification for contract {}", contract_name);
                        if self.conf.contracts.contains(&contract_name) {
                            if let Ok(block_height) = serde_json::from_str::<BlockHeight>(notification.payload()) {
                                debug!("🔔 Contract {} new block notification at height {}", contract_name, block_height);
                            } else {
                                debug!(
                                    "🔔 Contract {} notification payload={}, refreshing cursors",
                                    contract_name,
                                    notification.payload()
                                );
                            }
                            self.handle_sequenced_txs().await?;
                            self.handle_settled_txs().await?;
                        }
                    }
                    Err(err) => {
                        warn!("Listener error: {err}");
                    }
                }
            }
            _ = tokio::time::sleep(self.conf.poll_interval) => {
                // Periodic poll to catch any missed events for settled transactions only.
                // This is actually useful only if no new notifications have been received
                // for more than 5 seconds and that we missed some.
                trace!("⏱️  Periodic poll for missed settlement events");
                self.handle_settled_txs().await?;
            }
        };

        Ok(())
    }

    async fn handle_sequenced_txs(&mut self) -> Result<()> {
        let (sequenced_cursor, sequenced_txs) = self.query_sequenced_txs().await?;
        self.send_sequenced_txs(sequenced_txs)?;
        Self::update_cursor(
            &mut self.store.last_sequenced_block_cursor,
            sequenced_cursor,
        );
        Ok(())
    }

    async fn handle_settled_txs(&mut self) -> Result<()> {
        let (settled_cursor, settled_txs) = self.query_settled_txs().await?;
        self.send_settled_txs(settled_txs)?;
        Self::update_cursor(&mut self.store.last_settled_block_cursor, settled_cursor);
        Ok(())
    }

    async fn query_sequenced_txs(&self) -> Result<(BlockCursor, Vec<ContractTx>)> {
        let after_cursor = self.store.last_sequenced_block_cursor.clone();
        let contract_names: Vec<String> = self.conf.contracts.iter().map(|c| c.0.clone()).collect();
        let rows = sqlx::query(
            r#"
            WITH contract_txs AS (
                SELECT DISTINCT t.parent_dp_hash, t.tx_hash, t.block_hash, t.transaction_status, t.block_height, t.index, t.lane_id, blk.timestamp
                FROM transactions t
                JOIN blocks blk
                ON blk.hash = t.block_hash
                WHERE (t.block_height, t.index) > ($2, $3)
                AND EXISTS (
                    SELECT 1
                    FROM txs_contracts tx_c
                    WHERE tx_c.parent_dp_hash = t.parent_dp_hash
                    AND tx_c.tx_hash = t.tx_hash
                    AND tx_c.contract_name = ANY($1)
                )
                AND t.transaction_type = 'blob_transaction'
                AND t.transaction_status = 'sequenced'
            )
            SELECT ct.parent_dp_hash, ct.tx_hash, ct.index, ct.lane_id, b.identity, ct.timestamp, ct.block_hash, ct.transaction_status, ct.block_height, b.blob_index, b.data, b.contract_name,
                   NULL::contract_change_type[] AS contract_change_type,
                   NULL::bytea AS contract_metadata,
                   NULL::text AS contract_verifier,
                   NULL::bytea AS contract_program_id,
                   NULL::bytea AS contract_state_commitment,
                   NULL::bigint AS contract_soft_timeout,
                   NULL::bigint AS contract_hard_timeout,
                   NULL::int AS contract_deleted_at_height,
                   NULL::text AS contract_change_contract_name
            FROM contract_txs ct
            JOIN blobs b
            ON b.parent_dp_hash = ct.parent_dp_hash
            AND b.tx_hash = ct.tx_hash
            ORDER BY ct.block_height, ct.index, b.blob_index
            "#,
        )
        .bind(&contract_names)
        .bind(after_cursor.height.0 as i64)
        .bind(after_cursor.index)
        .fetch_all(&self.pool)
        .await?;

        let (latest_cursor, txs) = rows_to_txs(rows)?;
        debug!("Processing {} sequenced txs", txs.len());
        Ok((latest_cursor, txs))
    }

    async fn query_settled_txs(&self) -> Result<(BlockCursor, Vec<ContractTx>)> {
        let last_settled = self.store.last_settled_block_cursor.clone();
        let contract_names: Vec<String> = self.conf.contracts.iter().map(|c| c.0.clone()).collect();
        let rows = sqlx::query(
            r#"
            WITH contract_txs AS (
                SELECT DISTINCT t.parent_dp_hash, t.tx_hash, t.block_hash, t.transaction_status, t.block_height, t.index, t.lane_id, blk.timestamp
                FROM transactions t
                JOIN blocks blk
                ON blk.hash = t.block_hash
                WHERE (t.block_height, t.index) > ($2, $3)
                AND t.transaction_type = 'blob_transaction'
                AND (
                    t.transaction_status = 'success'
                    OR t.transaction_status = 'failure'
                    OR t.transaction_status = 'timed_out'
                )
                AND (
                    EXISTS (
                        SELECT 1
                        FROM txs_contracts tx_c
                        WHERE tx_c.parent_dp_hash = t.parent_dp_hash
                        AND tx_c.tx_hash = t.tx_hash
                        AND tx_c.contract_name = ANY($1)
                    )
                    OR EXISTS (
                        SELECT 1
                        FROM contract_history ch
                        WHERE ch.parent_dp_hash = t.parent_dp_hash
                        AND ch.tx_hash = t.tx_hash
                        AND ch.contract_name = ANY($1)
                    )
                )
            )
            SELECT ct.parent_dp_hash, ct.tx_hash, ct.index, ct.lane_id, b.identity, ct.timestamp, ct.block_hash, ct.transaction_status, ct.block_height, b.blob_index, b.data, b.contract_name,
                   ch.change_type AS contract_change_type,
                   ch.metadata AS contract_metadata,
                   ch.verifier AS contract_verifier,
                   ch.program_id AS contract_program_id,
                   ch.state_commitment AS contract_state_commitment,
                   ch.soft_timeout AS contract_soft_timeout,
                   ch.hard_timeout AS contract_hard_timeout,
                   ch.deleted_at_height AS contract_deleted_at_height,
                   ch.contract_name AS contract_change_contract_name
            FROM contract_txs ct
            JOIN blobs b
            ON b.parent_dp_hash = ct.parent_dp_hash
            AND b.tx_hash = ct.tx_hash
            LEFT JOIN contract_history ch
            ON ch.parent_dp_hash = ct.parent_dp_hash
            AND ch.tx_hash = ct.tx_hash
            AND ch.contract_name = ANY($1)
            ORDER BY ct.block_height, ct.index, b.blob_index
            "#,
        )
        .bind(&contract_names)
        .bind(last_settled.height.0 as i64)
        .bind(last_settled.index)
        .fetch_all(&self.pool)
        .await?;

        let (settled_cursor, txs) = rows_to_txs(rows)?;
        debug!("Processing {} settled txs", txs.len());
        Ok((settled_cursor, txs))
    }

    fn send_sequenced_txs(&mut self, txs: Vec<ContractTx>) -> Result<()> {
        debug!("Sending {} sequenced txs", txs.len());
        for tx in txs {
            trace!("Sending sequenced tx {}", tx.tx_id);
            self.bus.send(ContractListenerEvent::SequencedTx(tx))?;
        }
        Ok(())
    }

    fn send_settled_txs(&mut self, txs: Vec<ContractTx>) -> Result<()> {
        debug!("Sending {} settled txs", txs.len());
        for tx in txs {
            trace!("Sending settled tx {}", tx.tx_id);
            self.bus.send(ContractListenerEvent::SettledTx(tx))?;
        }
        Ok(())
    }

    fn update_cursor(cursor: &mut BlockCursor, new_cursor: BlockCursor) {
        if new_cursor.height.0 > cursor.height.0
            || (new_cursor.height.0 == cursor.height.0 && new_cursor.index > cursor.index)
        {
            *cursor = new_cursor;
        }
    }
}

fn rows_to_txs(rows: Vec<PgRow>) -> Result<(BlockCursor, Vec<ContractTx>)> {
    #[derive(Debug, Clone)]
    struct TxDataBuilder {
        tx_id: TxId,
        identity: Identity,
        blobs: IndexedBlobs,
        tx_ctx: Arc<TxContext>,
        status: TransactionStatusDb,
        contract_changes: HashMap<ContractName, ContractChangeData>,
    }

    // Group rows by TxId (parent_dp_hash + tx_hash) to rebuild each transaction with its full blob set.
    let mut txs: IndexMap<TxId, TxDataBuilder> = IndexMap::new();
    let mut latest_cursor = BlockCursor::default();

    for row in rows {
        let tx_hash: TxHash = row.try_get("tx_hash")?;
        let parent_dp_hash: DataProposalHash = row.try_get("parent_dp_hash")?;
        let block_hash: BlockHash = row.try_get("block_hash")?;
        let lane_id: LaneId = row.try_get("lane_id")?;
        let timestamp: TimestampMs = row.try_get("timestamp")?;
        let transaction_status: TransactionStatusDb = row.try_get("transaction_status")?;
        let identity: Identity = Identity(row.try_get("identity")?);
        let blob_contract_name: ContractName = row.try_get("contract_name")?;
        let tx_block_height = BlockHeight(row.try_get::<i64, _>("block_height")? as u64);
        let tx_index = row.try_get::<i32, _>("index")?;
        let contract_change = parse_contract_change_data(&row)?;

        let blob_index = BlobIndex(row.try_get::<i32, _>("blob_index")? as usize);
        let blob_data = row.try_get::<Vec<u8>, _>("data")?;
        let blob = Blob {
            data: BlobData(blob_data),
            contract_name: blob_contract_name,
        };
        if tx_block_height.0 > latest_cursor.height.0
            || (tx_block_height.0 == latest_cursor.height.0 && tx_index > latest_cursor.index)
        {
            latest_cursor = BlockCursor {
                height: tx_block_height,
                index: tx_index,
            };
        }

        let tx_ctx = TxContext {
            block_height: tx_block_height,
            lane_id,
            block_hash,
            timestamp,
            chain_id: HYLI_TESTNET_CHAIN_ID,
        };

        let tx_id = TxId(parent_dp_hash.clone(), tx_hash.clone());
        let entry = txs.entry(tx_id.clone()).or_insert_with(|| TxDataBuilder {
            tx_id,
            identity: identity.clone(),
            blobs: IndexedBlobs::default(),
            tx_ctx: Arc::new(tx_ctx),
            status: transaction_status,
            contract_changes: HashMap::new(),
        });
        if let Some((contract_name, contract_change)) = contract_change {
            if let Some(existing_change) = entry.contract_changes.get_mut(&contract_name) {
                merge_contract_change_data(existing_change, contract_change);
            } else {
                entry
                    .contract_changes
                    .insert(contract_name, contract_change);
            }
        }

        if entry.blobs.iter().all(|(idx, _)| *idx != blob_index) {
            entry.blobs.push((blob_index, blob));
        }
    }

    // Convert indexed blobs into ordered vectors to build BlobTransaction payloads.
    let mut out = Vec::with_capacity(txs.len());
    for builder in txs.into_values() {
        let blobs = indexed_blobs_to_vec(&builder.blobs)?;
        let tx = BlobTransaction::new(builder.identity, blobs);
        out.push(ContractTx {
            tx_id: builder.tx_id,
            tx,
            tx_ctx: builder.tx_ctx,
            status: builder.status,
            contract_changes: builder.contract_changes,
        });
    }

    Ok((latest_cursor, out))
}

fn parse_contract_change_data(row: &PgRow) -> Result<Option<(ContractName, ContractChangeData)>> {
    let Some(change_types) =
        row.try_get::<Option<Vec<ContractChangeType>>, _>("contract_change_type")?
    else {
        return Ok(None);
    };
    if change_types.is_empty() {
        return Ok(None);
    }
    let contract_name = row
        .try_get::<Option<ContractName>, _>("contract_change_contract_name")?
        .ok_or_else(|| anyhow::anyhow!("missing contract_change_contract_name"))?;
    Ok(Some((
        contract_name,
        ContractChangeData {
            change_types,
            metadata: row.try_get::<Option<Vec<u8>>, _>("contract_metadata")?,
            verifier: row.try_get::<String, _>("contract_verifier")?,
            program_id: row.try_get::<Vec<u8>, _>("contract_program_id")?,
            state_commitment: row.try_get::<Vec<u8>, _>("contract_state_commitment")?,
            soft_timeout: row.try_get::<Option<i64>, _>("contract_soft_timeout")?,
            hard_timeout: row.try_get::<Option<i64>, _>("contract_hard_timeout")?,
            deleted_at_height: row.try_get::<Option<i32>, _>("contract_deleted_at_height")?,
        },
    )))
}

fn merge_contract_change_data(existing: &mut ContractChangeData, mut incoming: ContractChangeData) {
    for change_type in incoming.change_types.drain(..) {
        if !existing.change_types.contains(&change_type) {
            existing.change_types.push(change_type);
        }
    }
    existing.verifier = incoming.verifier;
    existing.program_id = incoming.program_id;
    existing.state_commitment = incoming.state_commitment;
    existing.soft_timeout = incoming.soft_timeout;
    existing.hard_timeout = incoming.hard_timeout;
    existing.deleted_at_height = incoming.deleted_at_height;
    if incoming.metadata.is_some() {
        existing.metadata = incoming.metadata;
    }
}

fn indexed_blobs_to_vec(blobs: &IndexedBlobs) -> Result<Vec<Blob>> {
    if blobs.is_empty() {
        return Ok(vec![]);
    }
    // Rebuild a contiguous, ordered blob list and validate indices are unique.
    let max_index = blobs.iter().map(|(i, _)| i.0).max().unwrap_or(0);
    let mut ordered: Vec<Option<Blob>> = vec![None; max_index + 1];
    for (idx, blob) in blobs.iter() {
        let slot = ordered
            .get_mut(idx.0)
            .ok_or_else(|| anyhow::anyhow!("blob index {} out of bounds", idx.0))?;
        if slot.is_some() {
            anyhow::bail!("duplicate blob index {}", idx.0);
        }
        *slot = Some(blob.clone());
    }
    let mut out = Vec::with_capacity(ordered.len());
    for (i, blob) in ordered.into_iter().enumerate() {
        out.push(blob.ok_or_else(|| anyhow::anyhow!("missing blob index {}", i))?);
    }
    Ok(out)
}

#[cfg(test)]
mod contract_listener_tests;

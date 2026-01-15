use std::collections::{HashMap, HashSet};
use std::{path::PathBuf, time::Duration};

use anyhow::{Context, Result};
use borsh::{BorshDeserialize, BorshSerialize};
use hyli_model::utils::TimestampMs;
use hyli_model::{BlockHeight, ContractName, TxHash};
use indexmap::IndexMap;
use sdk::api::TransactionStatusDb;
use sdk::{
    Blob, BlobData, BlobIndex, BlockHash, IndexedBlobs, LaneId, TxContext, HYLI_TESTNET_CHAIN_ID,
};
use sqlx::postgres::{PgListener, PgPoolOptions, PgRow};
use sqlx::{PgPool, Row};
use tracing::{debug, info, warn};

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
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize, PartialEq)]
pub enum ContractListenerEvent {
    SequencedTx(TxHash, IndexedBlobs, TxContext),
    SettledTx(TxHash, IndexedBlobs, TxContext, TransactionStatusDb),
}

impl BusMessage for ContractListenerEvent {}

module_bus_client! {
#[derive(Debug)]
struct ContractListenerBusClient {
    sender(ContractListenerEvent),
}
}

type TxData = (IndexedBlobs, TxContext, TransactionStatusDb);

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
    last_sequenced_block_cursor: HashMap<ContractName, BlockCursor>,
    last_settled_block_cursor: HashMap<ContractName, BlockCursor>,
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

        let state_path = ctx.data_directory.join(CONTRACT_LISTENER_STATE_FILE);
        let store = Self::load_from_disk_or_default::<ContractListenerStore>(state_path.as_path());

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

    async fn persist(&mut self) -> Result<()> {
        // Resetting last_seen_block_cursor to default.
        // This forces reprocessing of all sequenced transactions on restart.
        for cursor in self.store.last_sequenced_block_cursor.values_mut() {
            *cursor = BlockCursor::default();
        }

        Self::save_on_disk(
            self.conf
                .data_directory
                .join(CONTRACT_LISTENER_STATE_FILE)
                .as_path(),
            &self.store,
        )
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

        // Initialize last seen heights per contract so startup dispatch can backfill.
        for contract in &self.conf.contracts {
            let last_sequenced_seen = self
                .store
                .last_sequenced_block_cursor
                .entry(contract.clone())
                .or_default();
            let last_settled_seen = self
                .store
                .last_settled_block_cursor
                .entry(contract.clone())
                .or_default();
            info!(
                "📡 ContractPgListener initial cursor for contract {}: sequenced=({}, {}), settled=({}, {})",
                contract,
                last_sequenced_seen.height.0,
                last_sequenced_seen.index,
                last_settled_seen.height.0,
                last_settled_seen.index
            );
        }

        // Dispatch any unprocessed txs at startup.
        for contract_name in self.conf.contracts.clone() {
            self.handle_sequenced_txs(&contract_name).await?;
            self.handle_settled_txs(&contract_name).await?;
        }

        module_handle_messages! {
            on_self self,
            notif = self.listener.recv() => {
                match notif {
                    Ok(notification) => {
                        let contract_name = notification.channel().into();
                        if self.conf.contracts.contains(&contract_name) {
                            match serde_json::from_str::<BlockHeight>(notification.payload()) {
                                Ok(block_height) => {
                                        debug!("🔔 Contract {} new block notification at height {}", contract_name, block_height);
                                        self.handle_sequenced_txs(&contract_name).await?;
                                        self.handle_settled_txs(&contract_name).await?;
                                }
                                Err(err) => {
                                    warn!("Failed to decode payload for {} ({}): {err}", notification.channel(), notification.payload());
                                }
                            }
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
                for contract_name in self.conf.contracts.clone() {
                    debug!("⏱️  Contract {} periodic poll for missed settlement events", contract_name);
                    self.handle_settled_txs(&contract_name).await?;
                }
            }
        };

        Ok(())
    }

    async fn handle_sequenced_txs(&mut self, contract_name: &ContractName) -> Result<()> {
        let (sequenced_cursor, sequenced_txs) = self.query_sequenced_txs(contract_name).await?;
        self.send_sequenced_txs(sequenced_txs)?;
        Self::update_cursor(
            &mut self.store.last_sequenced_block_cursor,
            contract_name,
            sequenced_cursor,
        );
        Ok(())
    }

    async fn handle_settled_txs(&mut self, contract_name: &ContractName) -> Result<()> {
        let (settled_cursor, settled_txs) = self.query_settled_txs(contract_name).await?;
        self.send_settled_txs(settled_txs)?;

        Self::update_cursor(
            &mut self.store.last_settled_block_cursor,
            contract_name,
            settled_cursor,
        );

        Ok(())
    }

    async fn query_sequenced_txs(
        &self,
        contract_name: &ContractName,
    ) -> Result<(BlockCursor, IndexMap<TxHash, TxData>)> {
        let after_cursor = self
            .store
            .last_sequenced_block_cursor
            .get(contract_name)
            .cloned()
            .unwrap_or_default();
        let rows = sqlx::query(
            r#"
            WITH contract_txs AS (
                SELECT DISTINCT b.parent_dp_hash, b.tx_hash, t.block_hash, t.transaction_status, t.block_height, t.index, t.lane_id, blk.timestamp
                FROM transactions t
                JOIN blobs b
                ON b.parent_dp_hash = t.parent_dp_hash
                JOIN blocks blk
                ON blk.hash = t.block_hash
                AND b.tx_hash = t.tx_hash
                WHERE b.contract_name = $1
                AND (t.block_height, t.index) > ($2, $3)
                AND t.transaction_status = 'sequenced'
            )
            SELECT ct.tx_hash, ct.index, ct.lane_id, ct.timestamp, ct.block_hash, ct.transaction_status, ct.block_height, b.blob_index, b.data, b.contract_name
            FROM contract_txs ct
            JOIN blobs b
            ON b.parent_dp_hash = ct.parent_dp_hash
            AND b.tx_hash = ct.tx_hash
            ORDER BY ct.block_height, ct.index, b.blob_index
            "#,
        )
        .bind(&contract_name.0)
        .bind(after_cursor.height.0 as i64)
        .bind(after_cursor.index)
        .fetch_all(&self.pool)
        .await?;

        let (latest_cursor, txs) = rows_to_txs(rows)?;
        debug!(
            "Processing {} sequenced txs for contract {}",
            txs.len(),
            contract_name
        );
        Ok((latest_cursor, txs))
    }

    async fn query_settled_txs(
        &self,
        contract_name: &ContractName,
    ) -> Result<(BlockCursor, IndexMap<TxHash, TxData>)> {
        let last_settled = self
            .store
            .last_settled_block_cursor
            .get(contract_name)
            .cloned()
            .unwrap_or_default();
        let rows = sqlx::query(
            r#"
            WITH contract_txs AS (
                SELECT DISTINCT b.parent_dp_hash, b.tx_hash, t.block_hash, t.transaction_status, t.block_height, t.index, t.lane_id, blk.timestamp
                FROM transactions t
                JOIN blobs b
                ON b.parent_dp_hash = t.parent_dp_hash
                JOIN blocks blk
                ON blk.hash = t.block_hash
                AND b.tx_hash = t.tx_hash
                WHERE b.contract_name = $1
                AND (t.block_height, t.index) > ($2, $3)
                AND (
                    t.transaction_status = 'success'
                    OR t.transaction_status = 'failure'
                    OR t.transaction_status = 'timed_out'
                )
            )
            SELECT ct.tx_hash, ct.index, ct.lane_id, ct.timestamp, ct.block_hash, ct.transaction_status, ct.block_height, b.blob_index, b.data, b.contract_name
            FROM contract_txs ct
            JOIN blobs b
            ON b.parent_dp_hash = ct.parent_dp_hash
            AND b.tx_hash = ct.tx_hash
            ORDER BY ct.block_height, ct.index, b.blob_index
            "#,
        )
        .bind(&contract_name.0)
        .bind(last_settled.height.0 as i64)
        .bind(last_settled.index)
        .fetch_all(&self.pool)
        .await?;

        let (settled_cursor, txs) = rows_to_txs(rows)?;
        debug!(
            "Processing {} settled txs for contract {}",
            txs.len(),
            contract_name
        );
        Ok((settled_cursor, txs))
    }

    fn send_sequenced_txs(&mut self, txs: IndexMap<TxHash, TxData>) -> Result<()> {
        for (tx_hash, (indexed_blobs, tx_ctx, _status)) in txs {
            self.bus.send(ContractListenerEvent::SequencedTx(
                tx_hash,
                indexed_blobs,
                tx_ctx,
            ))?;
        }
        Ok(())
    }

    fn send_settled_txs(&mut self, txs: IndexMap<TxHash, TxData>) -> Result<()> {
        for (tx_hash, (indexed_blobs, tx_ctx, status)) in txs {
            self.bus.send(ContractListenerEvent::SettledTx(
                tx_hash,
                indexed_blobs,
                tx_ctx,
                status,
            ))?;
        }
        Ok(())
    }

    fn update_cursor(
        cursors: &mut HashMap<ContractName, BlockCursor>,
        contract_name: &ContractName,
        new_cursor: BlockCursor,
    ) {
        let entry = cursors.entry(contract_name.clone()).or_default();
        if new_cursor.height.0 > entry.height.0
            || (new_cursor.height.0 == entry.height.0 && new_cursor.index > entry.index)
        {
            *entry = new_cursor;
        }
    }
}

fn rows_to_txs(rows: Vec<PgRow>) -> Result<(BlockCursor, IndexMap<TxHash, TxData>)> {
    let mut txs: IndexMap<TxHash, TxData> = IndexMap::new();
    let mut latest_cursor = BlockCursor::default();

    for row in rows {
        let tx_hash = TxHash(row.try_get("tx_hash")?);
        let block_hash: BlockHash = row.try_get("block_hash")?;
        let lane_id: LaneId = row.try_get("lane_id")?;
        let timestamp: TimestampMs = row.try_get("timestamp")?;
        let transaction_status: TransactionStatusDb = row.try_get("transaction_status")?;
        let blob_contract_name: ContractName = row.try_get("contract_name")?;
        let tx_block_height = BlockHeight(row.try_get::<i64, _>("block_height")? as u64);
        let tx_index = row.try_get::<i32, _>("index")?;

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

        let entry =
            txs.entry(tx_hash)
                .or_insert((IndexedBlobs::default(), tx_ctx, transaction_status));

        entry.0.push((blob_index, blob));
    }

    Ok((latest_cursor, txs))
}

#[cfg(test)]
mod contract_listener_tests;

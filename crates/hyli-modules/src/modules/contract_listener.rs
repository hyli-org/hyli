use std::collections::HashSet;
use std::{collections::HashMap, time::Duration};

use anyhow::{Context, Result};
use borsh::{BorshDeserialize, BorshSerialize};
use hyli_model::utils::TimestampMs;
use hyli_model::{BlockHeight, ContractName, TxHash};
use indexmap::IndexMap;
use sdk::api::TransactionStatusDb;
use sdk::{
    Blob, BlobData, BlobIndex, BlockHash, IndexedBlobs, LaneId, TxContext, HYLI_TESTNET_CHAIN_ID,
};
use sqlx::postgres::{PgListener, PgPoolOptions};
use sqlx::{PgPool, Row};
use tracing::{debug, info, warn};

use crate::bus::{BusClientSender, BusMessage, SharedMessageBus};
use crate::modules::Module;
use crate::{module_bus_client, module_handle_messages};

#[derive(Debug, Clone)]
pub struct ContractListenerConf {
    pub database_url: String,
    pub contracts: HashSet<ContractName>,
    /// How often to poll for missed events.
    pub poll_interval: Duration,
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize, PartialEq)]
pub enum ContractListenerEvent {
    NewTx(TxHash, IndexedBlobs, TxContext, TransactionStatusDb),
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
    last_processed_block_height: HashMap<ContractName, BlockHeight>,
}

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

        Ok(Self {
            bus,
            pool,
            listener,
            conf: ctx.clone(),
            last_processed_block_height: HashMap::new(),
        })
    }

    async fn run(&mut self) -> Result<()> {
        self.start().await
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

        // Initialize last seen heights with current max heights per contract (covers missed events before startup).
        for contract in &self.conf.contracts {
            let height = self.fetch_latest_settled_height(contract).await?;
            info!(
                "📡 ContractPgListener initial height for contract {}: {}",
                contract, height.0
            );
            self.last_processed_block_height
                .insert(contract.clone(), height);
        }

        // Dispatch any unprocessed txs at startup.
        self.dispatch_unprocessed_blocks().await?;

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
                                        self.handle_block(contract_name, block_height).await?;
                                }
                                Err(err) => warn!("Failed to decode payload for {}: {err}", notification.channel()),
                            }
                        }
                    }
                    Err(err) => {
                        warn!("Listener error: {err}");
                    }
                }
            }
            _ = tokio::time::sleep(self.conf.poll_interval) => {
                // Periodic poll to catch any missed events.
                // This is actually useful only if no new notifications have been received
                // for more than 5 seconds and that we missed some.
                self.dispatch_unprocessed_blocks().await?;
            }
        };

        Ok(())
    }

    async fn handle_block(
        &mut self,
        contract_name: ContractName,
        block_height: BlockHeight,
    ) -> Result<()> {
        let txs = self.query_txs(&contract_name, block_height).await?;

        debug!(
            "🔔 Contract {} has {} txs at height {}",
            contract_name,
            txs.len(),
            block_height.0
        );
        for (tx_hash, (indexed_blobs, tx_ctx, status)) in txs {
            self.bus.send(ContractListenerEvent::NewTx(
                tx_hash,
                indexed_blobs,
                tx_ctx,
                status,
            ))?;
        }

        Ok(())
    }

    async fn fetch_latest_settled_height(&self, contract: &ContractName) -> Result<BlockHeight> {
        let rec = sqlx::query(
            r#"
            SELECT MAX(block_height) as block_height
            FROM transactions t
            JOIN txs_contracts tc
            ON tc.parent_dp_hash = t.parent_dp_hash
            AND tc.tx_hash = t.tx_hash
            WHERE tc.contract_name = $1
            AND (
                t.transaction_status = 'success'
                OR t.transaction_status = 'failure'
                OR t.transaction_status = 'timed_out'
            )
            "#,
        )
        .bind(&contract.0)
        .fetch_optional(&self.pool)
        .await?;

        Ok(rec
            .and_then(|r| r.try_get::<Option<i64>, _>("block_height").ok())
            .flatten()
            .map(|h| BlockHeight(h as u64))
            .unwrap_or(BlockHeight(0)))
    }

    async fn dispatch_unprocessed_blocks(&mut self) -> Result<()> {
        for contract_name in &self.conf.contracts {
            let Some(height) = self.last_processed_block_height.get(contract_name).copied() else {
                continue;
            };

            let rows = sqlx::query(
                r#"
                WITH contract_txs AS (
                    SELECT DISTINCT b.parent_dp_hash, b.tx_hash, t.transaction_status, t.block_hash, t.block_height, t.index, t.lane_id, blk.timestamp
                    FROM transactions t
                    JOIN blobs b
                    ON b.parent_dp_hash = t.parent_dp_hash
                    JOIN blocks blk
                    ON blk.hash = t.block_hash
                    AND b.tx_hash = t.tx_hash
                    WHERE b.contract_name = $1
                    AND t.block_height >= $2
                )
                SELECT ct.tx_hash, ct.index, ct.lane_id, ct.timestamp, ct.transaction_status, ct.block_hash, ct.block_height, b.blob_index, b.data, b.contract_name
                FROM contract_txs ct
                JOIN blobs b
                ON b.parent_dp_hash = ct.parent_dp_hash
                AND b.tx_hash = ct.tx_hash
                ORDER BY ct.block_height, ct.index, b.blob_index
                "#,
            )
            .bind(&contract_name.0)
            .bind(height.0 as i64)
            .fetch_all(&self.pool)
            .await?;

            let mut txs: IndexMap<TxHash, (IndexedBlobs, TxContext, TransactionStatusDb)> =
                IndexMap::new();

            for row in rows {
                let tx_hash = TxHash(row.try_get("tx_hash")?);
                let block_hash: BlockHash = row.try_get("block_hash")?;
                let lane_id: LaneId = row.try_get("lane_id")?;
                let timestamp: TimestampMs = row.try_get("timestamp")?;
                let blob_contract_name: ContractName = row.try_get("contract_name")?;
                let block_height = BlockHeight(row.try_get::<i64, _>("block_height")? as u64);
                let transaction_status: TransactionStatusDb = row.try_get("transaction_status")?;

                let blob_index = BlobIndex(row.try_get::<i32, _>("blob_index")? as usize);
                let blob_data = row.try_get::<Vec<u8>, _>("data")?;
                let blob = Blob {
                    data: BlobData(blob_data),
                    contract_name: blob_contract_name,
                };

                let tx_ctx = TxContext {
                    block_height,
                    lane_id,
                    block_hash,
                    timestamp,
                    chain_id: HYLI_TESTNET_CHAIN_ID,
                };

                let entry = txs.entry(tx_hash).or_insert((
                    IndexedBlobs::default(),
                    tx_ctx,
                    transaction_status,
                ));

                entry.0.push((blob_index, blob));
            }

            debug!(
                "Processing {} txs for contract {}",
                txs.len(),
                contract_name
            );

            // Update latest_seen_height with the highest block height from processed txs
            if let Some((_, (_, tx_ctx, status))) = txs.last() {
                match status {
                    TransactionStatusDb::Sequenced => {
                        self.last_processed_block_height
                            .insert(contract_name.clone(), tx_ctx.block_height);
                    }
                    TransactionStatusDb::Success
                    | TransactionStatusDb::Failure
                    | TransactionStatusDb::TimedOut => {
                        self.last_processed_block_height.insert(
                            contract_name.clone(),
                            BlockHeight(tx_ctx.block_height.0 + 1),
                        );
                    }
                    _ => {}
                }
            }

            for (tx_hash, (indexed_blobs, tx_ctx, status)) in txs {
                self.bus.send(ContractListenerEvent::NewTx(
                    tx_hash,
                    indexed_blobs,
                    tx_ctx,
                    status,
                ))?;
            }
        }

        Ok(())
    }

    async fn query_txs(
        &mut self,
        contract_name: &ContractName,
        block_height: BlockHeight,
    ) -> Result<IndexMap<TxHash, (IndexedBlobs, TxContext, TransactionStatusDb)>> {
        let height_entry = self
            .last_processed_block_height
            .entry(contract_name.clone())
            .or_insert(BlockHeight(0));
        if block_height <= *height_entry {
            // Already processed or older
            warn!(
                "🔔 Contract {} notification for already seen height {}, ignoring",
                contract_name, block_height
            );
            return Ok(IndexMap::new());
        }

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
                AND t.transaction_status = 'sequenced'
                AND t.block_height = $2
            )
            SELECT ct.tx_hash, ct.index, ct.lane_id, ct.timestamp, ct.block_hash, ct.transaction_status, b.blob_index, b.data, b.contract_name
            FROM contract_txs ct
            JOIN blobs b
            ON b.parent_dp_hash = ct.parent_dp_hash
            AND b.tx_hash = ct.tx_hash
            ORDER BY ct.block_height, ct.index, b.blob_index
            "#,
        )
        .bind(&contract_name.0)
        .bind(block_height.0 as i64)
        .fetch_all(&self.pool)
        .await?;

        let mut txs: IndexMap<TxHash, (IndexedBlobs, TxContext, TransactionStatusDb)> =
            IndexMap::new();

        for row in rows {
            let tx_hash = TxHash(row.try_get("tx_hash")?);
            let block_hash: BlockHash = row.try_get("block_hash")?;
            let lane_id: LaneId = row.try_get("lane_id")?;
            let timestamp: TimestampMs = row.try_get("timestamp")?;
            let transaction_status: TransactionStatusDb = row.try_get("transaction_status")?;
            let blob_contract_name: ContractName = row.try_get("contract_name")?;

            let blob_index = BlobIndex(row.try_get::<i32, _>("blob_index")? as usize);
            let blob_data = row.try_get::<Vec<u8>, _>("data")?;
            let blob = Blob {
                data: BlobData(blob_data),
                contract_name: blob_contract_name,
            };

            let tx_ctx = TxContext {
                block_height,
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

        *height_entry = block_height;

        Ok(txs)
    }
}

#[cfg(test)]
mod contract_listener_tests;

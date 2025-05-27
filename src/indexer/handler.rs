use super::api::*;
use crate::model::*;
use crate::node_state::module::NodeStateEvent;
use anyhow::{bail, Context, Error, Result};
use chrono::{DateTime, Utc};
use hyle_contract_sdk::TxHash;
use hyle_model::api::{TransactionStatusDb, TransactionTypeDb};
use hyle_model::utils::TimestampMs;
use hyle_modules::{log_error, log_warn};
use hyle_net::clock::TimestampMsClock;
use sqlx::QueryBuilder;
use sqlx::{Execute, Postgres};
use std::collections::HashSet;
use std::ops::DerefMut;
use std::sync::Arc;
use tracing::{debug, info, trace};

use super::Indexer;

#[derive(Debug)]
pub struct TxDataStore {
    pub tx_hash: TxHashDb,
    pub parent_data_proposal_hash: DataProposalHashDb,
    pub blob_index: i32,
    pub identity: String,
    pub contract_name: String,
    pub blob_data: Vec<u8>,
    pub verified: bool,
}

#[derive(Debug)]
pub struct TxProofStore {
    pub tx_hash: TxHashDb,
    pub parent_data_proposal_hash: DataProposalHashDb,
    pub proof: Vec<u8>,
}

#[derive(Debug)]
pub struct TxEventStore {
    pub block_hash: ConsensusProposalHash,
    pub block_height: i64,
    pub index: i32,
    pub tx_hash: TxHashDb,
    pub parent_data_proposal_hash: DataProposalHashDb,
    pub events: String,
}

#[derive(Debug, Default)]
pub(crate) struct IndexerHandlerStore {
    blocks: Vec<Arc<Block>>,
    block_txs: Vec<(i32, TxId, Arc<Block>, Transaction)>,
    tx_data: Vec<TxDataStore>,
    tx_data_proofs: Vec<TxProofStore>,
    transactions_events: Vec<TxEventStore>,
    sql_updates: Vec<String>,
}

impl Indexer {
    pub async fn handle_node_state_event(&mut self, event: NodeStateEvent) -> Result<(), Error> {
        match event {
            NodeStateEvent::NewBlock(block) => self.handle_processed_block(*block).await?,
        };

        if self.handler_store.blocks.len() >= 1000 {
            // If we have more than 1000 blocks, we dump the store to the database
            self.dump_store_to_db().await?;
        }
        if let Some(block) = self.handler_store.blocks.last() {
            // if last block is newer than 5sec dump store to db
            let now = TimestampMsClock::now();
            if block.block_timestamp > TimestampMs(now.0 - 5000) {
                self.dump_store_to_db().await?;
            }
        }

        Ok(())
    }

    async fn dump_store_to_db(&mut self) -> Result<()> {
        if self.handler_store.blocks.is_empty() {
            return Ok(());
        }

        let mut transaction = self.state.db.begin().await?;

        // Insert blocks into the database
        if !self.handler_store.blocks.is_empty() {
            let mut query_builder = QueryBuilder::<Postgres>::new(
                "INSERT INTO blocks (hash, parent_hash, height, timestamp, total_txs) ",
            );
            query_builder
                .push_values(self.handler_store.blocks.drain(..), |mut b, block| {
                    let block_hash = block.hash.clone();
                    let block_height = log_error!(
                        i64::try_from(block.block_height.0).map_err(|_| anyhow::anyhow!(
                            "Block height is too large to fit into an i64"
                        )),
                        "Converting block height into i64"
                    )
                    .unwrap_or_default();
                    let total_txs = block.txs.len() as i64;

                    let block_timestamp = into_utc_date_time(&block.block_timestamp)
                        .context("Block's timestamp is incorrect")
                        .unwrap_or_else(|_| {
                            // If the timestamp is incorrect, we can use the current time as a fallback.
                            Utc::now()
                        });

                    b.push_bind(block_hash)
                        .push_bind(block.parent_hash.clone())
                        .push_bind(block_height)
                        .push_bind(block_timestamp)
                        .push_bind(total_txs);
                })
                .build()
                .execute(&mut *transaction)
                .await
                .context("Inserting blocks")?;
        }

        // Insert transactions into the database
        if !self.handler_store.block_txs.is_empty() {
            let mut query_builder = QueryBuilder::<Postgres>::new(
                "INSERT INTO transactions (tx_hash, parent_dp_hash, version, transaction_type, transaction_status, block_hash, block_height, index, lane_id) ",
            );

            query_builder.push_values(
                self.handler_store.block_txs.drain(..),
                |mut b, (index, tx_id, block, tx)| {
                    let version = log_error!(
                        i32::try_from(tx.version).map_err(|_| anyhow::anyhow!(
                            "Tx version is too large to fit into an i32"
                        )),
                        "Converting tx version into i32"
                    )
                    .unwrap_or_default();

                    let block_height = log_error!(
                        i64::try_from(block.block_height.0).map_err(|_| anyhow::anyhow!(
                            "Block height is too large to fit into an i64"
                        )),
                        "Converting block height into i64"
                    )
                    .unwrap_or_default();

                    let tx_type = TransactionTypeDb::from(&tx);
                    let tx_status = match tx.transaction_data {
                        TransactionData::Blob(_) => TransactionStatusDb::Sequenced,
                        TransactionData::Proof(_) => TransactionStatusDb::Success,
                        TransactionData::VerifiedProof(_) => TransactionStatusDb::Success,
                    };

                    let lane_id: LaneIdDb = log_error!(
                        block
                            .lane_ids
                            .get(&tx_id.1)
                            .context(format!("No lane id present for tx {}", tx_id)),
                        "Getting lane id for tx"
                    )
                    .unwrap_or(&LaneId::default())
                    .clone()
                    .into();

                    let parent_data_proposal_hash: &DataProposalHashDb = &tx_id.0.into();
                    let tx_hash: TxHashDb = tx_id.1.into();

                    b.push_bind(tx_hash)
                        .push_bind(parent_data_proposal_hash.clone())
                        .push_bind(version)
                        .push_bind(tx_type)
                        .push_bind(tx_status)
                        .push_bind(block.hash.clone())
                        .push_bind(block_height)
                        .push_bind(index)
                        .push_bind(lane_id);
                },
            );
            query_builder.push(" ON CONFLICT(tx_hash, parent_dp_hash) DO UPDATE SET ");
            query_builder.push("transaction_status=EXCLUDED.transaction_status, block_hash=EXCLUDED.block_hash, block_height=EXCLUDED.block_height, index=EXCLUDED.index");

            query_builder
                .build()
                .execute(&mut *transaction)
                .await
                .context("Inserting transactions")?;
        }

        // Insert blobs into the database
        if !self.handler_store.tx_data.is_empty() {
            let mut query_builder = QueryBuilder::<Postgres>::new(
                "INSERT INTO blobs (tx_hash, parent_dp_hash, blob_index, identity, contract_name, data, verified) ",
            );

            query_builder.push_values(self.handler_store.tx_data.drain(..), |mut b, s| {
                let TxDataStore {
                    tx_hash,
                    parent_data_proposal_hash,
                    blob_index,
                    identity,
                    contract_name,
                    blob_data,
                    verified,
                } = s;

                b.push_bind(tx_hash)
                    .push_bind(parent_data_proposal_hash)
                    .push_bind(blob_index)
                    .push_bind(identity)
                    .push_bind(contract_name)
                    .push_bind(blob_data)
                    .push_bind(verified);
            });

            query_builder.push(" ON CONFLICT DO NOTHING");

            query_builder
                .build()
                .execute(&mut *transaction)
                .await
                .context("Inserting blobs")?;
        }

        // Insert proofs into the database
        if !self.handler_store.tx_data_proofs.is_empty() {
            let mut query_builder = QueryBuilder::<Postgres>::new(
                "INSERT INTO proofs (parent_dp_hash, tx_hash, proof) ",
            );

            query_builder.push_values(self.handler_store.tx_data_proofs.drain(..), |mut b, s| {
                let TxProofStore {
                    tx_hash,
                    parent_data_proposal_hash,
                    proof,
                } = s;

                b.push_bind(parent_data_proposal_hash)
                    .push_bind(tx_hash)
                    .push_bind(proof);
            });

            query_builder.push(" ON CONFLICT(parent_dp_hash, tx_hash) DO NOTHING");

            query_builder
                .build()
                .execute(&mut *transaction)
                .await
                .context("Inserting proofs")?;
        }

        // Insert transaction events into the database
        if !self.handler_store.transactions_events.is_empty() {
            let mut query_builder = QueryBuilder::<Postgres>::new(
                "INSERT INTO transaction_state_events (block_hash, block_height, index, tx_hash, parent_dp_hash, events) ",
            );
            query_builder.push_values(
                self.handler_store.transactions_events.drain(..),
                |mut b, s| {
                    let TxEventStore {
                        block_hash,
                        block_height,
                        index,
                        tx_hash,
                        parent_data_proposal_hash,
                        events,
                    } = s;

                    b.push_bind(block_hash)
                        .push_bind(block_height)
                        .push_bind(index)
                        .push_bind(tx_hash)
                        .push_bind(parent_data_proposal_hash)
                        .push_bind(events)
                        .push_unseparated("::jsonb");
                },
            );

            query_builder
                .build()
                .execute(&mut *transaction)
                .await
                .context("Inserting transactions events")?;
        }

        if !self.handler_store.sql_updates.is_empty() {
            let mut query_builder = QueryBuilder::<Postgres>::new("");
            let mut separated = query_builder.separated("; ");
            for sql_update in self.handler_store.sql_updates.drain(..) {
                separated.push(sql_update);
            }
            query_builder
                .build()
                .execute(&mut *transaction)
                .await
                .context("Executing SQL updates")?;
        }

        transaction.commit().await?;

        Ok(())
    }

    pub async fn handle_mempool_status_event(&mut self, event: MempoolStatusEvent) -> Result<()> {
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
                    self.insert_tx_data(tx_hash, &tx, parent_data_proposal_hash_db,)
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

    async fn insert_tx_data(
        &mut self,
        tx_hash: &TxHashDb,
        tx: &Transaction,
        parent_data_proposal_hash: DataProposalHashDb,
    ) -> Result<()> {
        match &tx.transaction_data {
            TransactionData::Blob(blob_tx) => {
                blob_tx
                    .blobs
                    .iter()
                    .enumerate()
                    .for_each(|(blob_index, blob)| {
                        let blob_index = log_error!(
                            i32::try_from(blob_index),
                            "Blob index is too large to fit into an i32"
                        )
                        .unwrap_or_default();

                        let identity = blob_tx.identity.0.clone();
                        let contract_name = blob.contract_name.0.clone();
                        let blob_data = blob.data.0.clone();
                        let tx_hash = tx_hash.clone();
                        let parent_data_proposal_hash = parent_data_proposal_hash.clone();

                        self.handler_store.tx_data.push(TxDataStore {
                            tx_hash,
                            parent_data_proposal_hash,
                            blob_index,
                            identity,
                            contract_name,
                            blob_data,
                            verified: false,
                        });
                    });
            }
            TransactionData::VerifiedProof(tx_data) => {
                // Then insert the proof in to the proof table.
                match &tx_data.proof {
                    Some(proof_data) => {
                        self.handler_store.tx_data_proofs.push(TxProofStore {
                            tx_hash: tx_hash.clone(),
                            parent_data_proposal_hash: parent_data_proposal_hash.clone(),
                            proof: proof_data.0.clone(),
                        });
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

    pub(crate) async fn handle_processed_block(&mut self, block: Block) -> Result<(), Error> {
        if block.block_height.0 % 1000 == 0 {
            // Log every 1000th block
            info!("Indexing block at height {:?}", block.block_height);
        } else {
            trace!("Indexing block at height {:?}", block.block_height);
        }

        let mut transaction: sqlx::PgTransaction = self.state.db.begin().await?;

        let arc_block = Arc::new(block.clone());

        self.handler_store.blocks.push(arc_block.clone());

        let block_height = i64::try_from(block.block_height.0)
            .map_err(|_| anyhow::anyhow!("Block height is too large to fit into an i64"))?;

        let mut i: i32 = 0;
        #[allow(clippy::explicit_counter_loop)]
        for (tx_id, tx) in block.txs {
            self.handler_store
                .block_txs
                .push((i, tx_id.clone(), arc_block.clone(), tx.clone()));

            let tx_hash: &TxHashDb = &tx_id.1.into();
            let parent_data_proposal_hash: &DataProposalHashDb = &tx_id.0.into();

            _ = log_warn!(
                self.insert_tx_data(tx_hash, &tx, parent_data_proposal_hash.clone(),)
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

            self.handler_store.transactions_events.push(TxEventStore {
                block_hash: block.hash.clone(),
                block_height,
                index: i,
                tx_hash: tx_hash_db.clone(),
                parent_data_proposal_hash,
                events: serialized_events,
            });
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
            self.handler_store.sql_updates.push(
                sqlx::query("UPDATE transactions SET transaction_status = $1 WHERE tx_hash = $2 AND parent_dp_hash = $3")
                    .bind(TransactionStatusDb::Success)
                    .bind(tx_hash)
                    .bind(dp_hash_db)
                    .sql()
                    .to_string()
            );
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
            self.handler_store.sql_updates.push(
                sqlx::query("UPDATE transactions SET transaction_status = $1 WHERE tx_hash = $2 AND parent_dp_hash = $3")
                    .bind(TransactionStatusDb::Failure)
                    .bind(tx_hash)
                    .bind(dp_hash_db)
                    .sql()
                    .to_string()
            );
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
            self.handler_store.sql_updates.push(
                sqlx::query("UPDATE transactions SET transaction_status = $1 WHERE tx_hash = $2 AND parent_dp_hash = $3")
                    .bind(TransactionStatusDb::TimedOut)
                    .bind(tx_hash)
                    .bind(dp_hash_db)
                    .sql()
                    .to_string()
            );
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

            self.handler_store.sql_updates.push(
                sqlx::query("UPDATE blobs SET verified = true WHERE tx_hash = $1 AND parent_dp_hash = $2 AND blob_index = $3")
                    .bind(blob_tx_hash)
                    .bind(blob_tx_parent_dp_hash.clone())
                    .bind(blob_index)
                    .sql()
                    .to_string()
            );

            if let Some(blob_proof_output_index) = blob_proof_output_index {
                let blob_proof_output_index =
                    i32::try_from(blob_proof_output_index).map_err(|_| {
                        anyhow::anyhow!("Blob proof output index is too large to fit into an i32")
                    })?;

                self.handler_store.sql_updates.push(
                    sqlx::query("UPDATE blob_proof_outputs SET settled = true WHERE blob_tx_hash = $1 AND blob_parent_dp_hash = $2 AND blob_index = $3 AND blob_proof_output_index = $4")
                        .bind(blob_tx_hash)
                        .bind(blob_tx_parent_dp_hash)
                        .bind(blob_index)
                        .bind(blob_proof_output_index)
                        .sql()
                        .to_string()
                );
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
            self.handler_store.sql_updates.push(
                sqlx::query(
                    "UPDATE contract_state SET state_commitment = $1 WHERE contract_name = $2 AND block_hash = $3",
                )
                    .bind(state_commitment.clone())
                    .bind(contract_name.clone())
                    .bind(block.hash.clone())
                    .sql()
                    .to_string()
            );

            self.handler_store.sql_updates.push(
                sqlx::query::<Postgres>(
                    "UPDATE contracts SET state_commitment = $1 WHERE contract_name = $2",
                )
                .bind(state_commitment)
                .bind(contract_name)
                .sql()
                .to_string(),
            );
        }

        // Commit the transaction
        transaction.commit().await?;

        tracing::debug!("Indexed block at height {:?}", block.block_height);

        Ok(())
    }
}

pub fn into_utc_date_time(ts: &TimestampMs) -> Result<DateTime<Utc>> {
    DateTime::from_timestamp_millis(ts.0.try_into().context("Converting u64 into i64")?)
        .context("Converting i64 into UTC DateTime")
}

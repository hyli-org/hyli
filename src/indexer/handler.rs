use super::api::*;
use crate::model::*;
use crate::node_state::module::NodeStateEvent;
use anyhow::{bail, Context, Error, Result};
use chrono::{DateTime, Utc};
use hyle_contract_sdk::TxHash;
use hyle_model::api::{TransactionStatusDb, TransactionTypeDb};
use hyle_model::utils::TimestampMs;
use hyle_modules::{log_error, log_warn};
use sqlx::Postgres;
use sqlx::{PgExecutor, QueryBuilder};
use std::collections::HashSet;
use std::ops::DerefMut;
use tracing::{debug, trace};

use super::Indexer;

impl Indexer {
    pub async fn handle_node_state_event(&mut self, event: NodeStateEvent) -> Result<(), Error> {
        match event {
            NodeStateEvent::NewBlock(block) => self.handle_processed_block(*block).await,
        }
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

    pub(crate) async fn handle_processed_block(&mut self, block: Block) -> Result<(), Error> {
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
}

pub fn into_utc_date_time(ts: &TimestampMs) -> Result<DateTime<Utc>> {
    DateTime::from_timestamp_millis(ts.0.try_into().context("Converting u64 into i64")?)
        .context("Converting i64 into UTC DateTime")
}

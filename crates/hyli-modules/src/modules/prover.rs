use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;
use std::time::Duration;
use std::{fmt::Debug, path::PathBuf, sync::Arc};

use crate::bus::SharedMessageBus;
use crate::modules::contract_listener::{ContractChangeData, ContractListenerEvent, ContractTx};
use crate::modules::signal::shutdown_aware_timeout;
use crate::modules::SharedBuildApiCtx;
use crate::{log_error, module_bus_client, module_handle_messages, modules::Module};
use anyhow::{anyhow, Context, Result};
use axum::extract::State;
use axum::Router;
use borsh::{BorshDeserialize, BorshSerialize};
use client_sdk::rest_client::NodeApiClient;
use client_sdk::{helpers::ClientSdkProver, transaction_builder::TxExecutorHandler};
use futures::future::BoxFuture;
use hyli_bus::modules::ModulePersistOutput;
use hyli_model::api::ContractChangeType;
use hyli_net::logged_task::logged_task;
use indexmap::IndexMap;
use sdk::api::TransactionStatusDb;
use sdk::{
    BlobIndex, BlobTransaction, Calldata, ContractName, Hashed, ProgramId, ProofTransaction,
    StateCommitment, TxContext, TxId,
};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

use super::prover_metrics::AutoProverMetrics;

/// `AutoProver` is a module that handles the proving of transactions.
/// It listens to node state events and processes all blobs in the transaction stream
/// for a given contract.
/// It asynchronously generates ProofTransactions to prove all concerned blobs in order.
/// If a BlobTransaction times out, or is settled as failed, all blobs that are "after"
/// the failed transaction are re-executed and re-proved.
/// This module requires the ELF to support multiproof. i.e. it requires the ELF to read
/// a `Vec<Calldata>` as input.
pub struct AutoProver<
    Contract: Send + Sync + Clone + 'static,
    Prover: ClientSdkProver<Vec<Calldata>> + Send + Sync,
> {
    bus: AutoProverBusClient,
    ctx: Arc<AutoProverCtx<Contract, Prover>>,
    provers: HashMap<ProgramId, Arc<Prover>>,
    store: AutoProverStore<Contract>,
    metrics: AutoProverMetrics,
    current_program_id: ProgramId,
    catching_up: bool,
    catching_up_state: StateCommitment,

    // Catch-up buffer for sequenced txs that arrived while we were replaying settled history.
    // Invariant: tx is always sequenced before being settled.
    catching_txs: IndexMap<TxId, (BlobTransaction, Arc<TxContext>)>,
    catching_success_txs: Vec<(TxId, BlobTransaction, Arc<TxContext>)>,

    pending_replay_from: Option<usize>,

    router_state: Arc<Mutex<RouterData>>,
}

type BufferedBlobs = Vec<(TxId, Vec<BlobIndex>, BlobTransaction, Arc<TxContext>)>;

#[derive(Default)]
pub struct RouterData {
    pub is_proving: bool,
}

#[derive(Default, BorshSerialize, BorshDeserialize)]
pub struct AutoProverStore<Contract> {
    // These are other unsettled transactions that are waiting to be proved
    unsettled_txs: Vec<(BlobTransaction, Arc<TxContext>, TxId)>,
    // These are the transactions that are currently being proved
    proving_txs: Vec<(BlobTransaction, Arc<TxContext>, TxId)>,
    state_history: BTreeMap<TxId, (Contract, bool)>,
    tx_chain: Vec<TxId>,
    // blobs extracted from proving_txs, waiting to be batched into proofs.
    buffered_blobs: BufferedBlobs,
    batch_id: u64,
}

module_bus_client! {
#[derive(Debug)]
pub struct AutoProverBusClient {
    receiver(ContractListenerEvent),
}
}

pub struct AutoProverCtx<Contract, Prover: ClientSdkProver<Vec<Calldata>> + Send + Sync> {
    pub data_directory: PathBuf,
    pub prover: Arc<Prover>,
    pub contract_name: ContractName,
    pub node: Arc<dyn NodeApiClient + Send + Sync>,
    // Optional API for readiness information
    pub api: Option<SharedBuildApiCtx>,
    pub default_state: Contract,
    /// Minimum number of transactions to buffer before generating proofs.
    /// If set to 0, proofs are flushed only on idle or max_txs_per_proof.
    pub tx_buffer_size: usize,
    pub max_txs_per_proof: usize,
    pub tx_working_window_size: usize,
    /// Flush buffered txs if idle for this duration.
    pub idle_flush_interval: Duration,
}

impl<Contract, Prover> Module for AutoProver<Contract, Prover>
where
    Contract: TxExecutorHandler
        + BorshSerialize
        + BorshDeserialize
        + Debug
        + Send
        + Sync
        + Clone
        + 'static,
    Prover: ClientSdkProver<Vec<Calldata>> + Send + Sync + 'static,
{
    type Context = Arc<AutoProverCtx<Contract, Prover>>;

    /// Build the prover module, initialize state/provers, and start catch-up mode.
    async fn build(bus: SharedMessageBus, ctx: Self::Context) -> Result<Self> {
        let bus = AutoProverBusClient::new_from_bus(bus.new_handle()).await;

        let file = PathBuf::from(format!("autoprover_{}.bin", ctx.contract_name));

        let mut store =
            match Self::load_from_disk::<AutoProverStore<Contract>>(&ctx.data_directory, &file)? {
                Some(store) => store,
                None => AutoProverStore::<Contract> {
                    unsettled_txs: vec![],
                    proving_txs: vec![],
                    state_history: BTreeMap::new(),
                    tx_chain: vec![],
                    buffered_blobs: vec![],
                    batch_id: 0,
                },
            };

        let infos = ctx.prover.info();
        let mut provers = HashMap::new();
        provers.insert(ctx.prover.program_id(), ctx.prover.clone());

        let metrics = AutoProverMetrics::global(ctx.contract_name.to_string(), infos);

        let router_state = Arc::new(Mutex::new(RouterData::default()));
        if let Some(api) = &ctx.api {
            use axum::routing::get;
            if let Ok(mut guard) = api.router.lock() {
                if let Some(router) = guard.take() {
                    guard.replace(
                        router.nest(
                            "/v1/prover",
                            Router::new()
                                .route("/ready", get(is_ready))
                                .with_state(router_state.clone()),
                        ),
                    );
                }
            }
        }

        let contract_state = ctx.node.get_contract(ctx.contract_name.clone()).await?;
        let current_program_id = contract_state.program_id.clone();
        if !provers.contains_key(&current_program_id) {
            let prover = Arc::new(
                Prover::new_from_registry(&ctx.contract_name, current_program_id.clone())
                    .await
                    .context("Creating prover for current program ID")?,
            );
            provers.insert(current_program_id.clone(), prover);
        }
        let catching_up_state = contract_state.state_commitment;
        let catching_up = true;

        info!(
            cn =% ctx.contract_name,
            "Catching up from settled history"
        );

        store.unsettled_txs.clear();
        store.proving_txs.clear();
        store.state_history.clear();
        store.tx_chain.clear();
        store.buffered_blobs.clear();

        let catching_txs = IndexMap::new();

        Ok(AutoProver {
            bus,
            store,
            ctx,
            provers,
            metrics,
            current_program_id,
            catching_up,
            catching_up_state,
            catching_success_txs: vec![],
            catching_txs,
            pending_replay_from: None,
            router_state,
        })
    }

    /// Main event loop: handle contract events and idle flush ticks.
    async fn run(&mut self) -> Result<()> {
        let mut idle_flush_ticker = tokio::time::interval(self.ctx.idle_flush_interval);
        module_handle_messages! {
            on_self self,
            listen<ContractListenerEvent> event => {
                debug!(cn =% self.ctx.contract_name, "Received ContractListenerEvent: {:?}", event);
                let res = log_error!(self.handle_contract_listener_event(event).await, "handle contract listener event");
                self.metrics.snapshot_buffered_blobs(self.store.buffered_blobs.len() as u64);
                self.metrics
                    .snapshot_unsettled_blobs(self.store.proving_txs.len() as u64);
                if res.is_err() {
                    break;
                }
            }
            _ = idle_flush_ticker.tick() => {
                let res = log_error!(self.handle_idle_flush().await, "handle idle flush");
                self.metrics.snapshot_buffered_blobs(self.store.buffered_blobs.len() as u64);
                self.metrics
                    .snapshot_unsettled_blobs(self.store.proving_txs.len() as u64);
                if res.is_err() {
                    break;
                }
            }
        };

        Ok(())
    }

    /// Persist the prover store to disk for crash recovery.
    async fn persist(&mut self) -> Result<ModulePersistOutput> {
        let file = PathBuf::from(format!("autoprover_{}.bin", self.ctx.contract_name));
        let checksum = Self::save_on_disk::<AutoProverStore<Contract>>(
            &self.ctx.data_directory,
            &file,
            &self.store,
        )?;
        Ok(vec![(self.ctx.data_directory.join(file), checksum)])
    }
}

/// HTTP readiness endpoint: OK when prover is caught up and proving.
pub async fn is_ready(
    State(state): State<Arc<Mutex<RouterData>>>,
) -> Result<impl axum::response::IntoResponse, axum::http::StatusCode> {
    let state = state.lock().unwrap();
    if state.is_proving {
        Ok(axum::http::StatusCode::OK)
    } else {
        Err(axum::http::StatusCode::SERVICE_UNAVAILABLE)
    }
}

impl<Contract, Prover> AutoProver<Contract, Prover>
where
    Contract: TxExecutorHandler + Debug + Clone + Send + Sync + 'static,
    Prover: ClientSdkProver<Vec<Calldata>> + Send + Sync + 'static,
{
    /// Dispatch contract listener events to the relevant handler.
    async fn handle_contract_listener_event(&mut self, event: ContractListenerEvent) -> Result<()> {
        match event {
            ContractListenerEvent::SequencedTx(ContractTx {
                tx_id, tx, tx_ctx, ..
            }) => {
                self.handle_sequenced_tx(tx_id, tx, tx_ctx).await?;
            }
            ContractListenerEvent::SettledTx(tx_data) => {
                self.handle_settled_tx(tx_data).await?;
            }
            ContractListenerEvent::BackfillComplete(contract_name) => {
                if contract_name == self.ctx.contract_name {
                    self.handle_backfill_complete().await?;
                }
            }
        }
        Ok(())
    }

    /// Finalize catch-up: replay settled successes, verify state, then start proving.
    async fn handle_backfill_complete(&mut self) -> Result<()> {
        if !self.catching_up {
            return Ok(());
        }

        // Build blobs to execute from catching_success_txs
        let mut blobs: Vec<(TxId, BlobIndex, BlobTransaction, Arc<TxContext>)> = vec![];
        for (tx_id, tx, tx_ctx) in self.catching_success_txs.iter() {
            for (index, blob) in tx.blobs.iter().enumerate() {
                if blob.contract_name == self.ctx.contract_name {
                    blobs.push((tx_id.clone(), index.into(), tx.clone(), tx_ctx.clone()));
                }
            }
        }

        info!(
            cn =% self.ctx.contract_name,
            "✅ Catching up finished, {} blobs to process",
            blobs.len()
        );

        let mut contract = self.ctx.default_state.clone();
        let last_tx_id = blobs.last().map(|(tx_id, ..)| tx_id);
        for (tx_id, blob_index, tx, tx_ctx) in &blobs {
            let calldata = Calldata {
                identity: tx.identity.clone(),
                tx_hash: tx.hashed(),
                private_input: vec![],
                blobs: tx.blobs.clone().into(),
                index: *blob_index,
                tx_ctx: Some((**tx_ctx).clone()),
                tx_blob_count: tx.blobs.len(),
            };

            match contract.handle(&calldata) {
                Err(e) => {
                    error!(
                        cn =% self.ctx.contract_name,
                        tx_id =% tx_id,
                        tx_height =% tx_ctx.block_height,
                        "Error while executing settled tx: {e:#}"
                    );
                    error!(
                        cn =% self.ctx.contract_name,
                        tx_id =% tx_id,
                        tx_height =% tx_ctx.block_height,
                        "This is likely a bug in the prover, please report it to the Hyli team."
                    );
                }
                Ok(hyli_output) => {
                    debug!(
                        cn =% self.ctx.contract_name,
                        tx_id =% tx_id,
                        tx_height =% tx_ctx.block_height,
                        "Executed contract: {}. Success: {}",
                        String::from_utf8_lossy(&hyli_output.program_outputs),
                        hyli_output.success
                    );
                    if !hyli_output.success {
                        error!(
                            cn =% self.ctx.contract_name,
                            tx_id =% tx_id,
                            tx_height =% tx_ctx.block_height,
                            "Executed tx as failed but it was settled as success!",
                        );
                        error!(
                            cn =% self.ctx.contract_name,
                            tx_id =% tx_id,
                            tx_height =% tx_ctx.block_height,
                            "This is likely a bug in the prover, please report it to the Hyli team."
                        );
                    }
                }
            }
        }

        if let Ok(current_state) = self
            .ctx
            .node
            .get_contract(self.ctx.contract_name.clone())
            .await
        {
            self.catching_up_state = current_state.state_commitment;
        }

        #[cfg(not(test))]
        {
            let final_state = contract.get_state_commitment();
            info!(
                cn =% self.ctx.contract_name,
                "Final state after catching up: {:?}",
                final_state
            );

            if self.catching_up_state != final_state {
                error!(
                    cn =% self.ctx.contract_name,
                    "Onchain state does not match final state after catching up. Onchain: {:?}, Final: {:?}",
                    self.catching_up_state, final_state
                );
                error!(
                    cn =% self.ctx.contract_name,
                    "This is likely a bug in the prover, please report it to the Hyli team."
                );
                anyhow::bail!(
                    "Onchain state does not match final state after catching up. Onchain: {:?}, Final: {:?}",
                    self.catching_up_state,
                    final_state
                );
            }
        }

        // Mark ourselves ready to prove.
        self.router_state.lock().unwrap().is_proving = true;
        self.catching_up = false;

        if let Some(last_tx_id) = last_tx_id {
            self.store.tx_chain = vec![last_tx_id.clone()];
            self.store
                .state_history
                .insert(last_tx_id.clone(), (contract, true));
        }

        let buffered = std::mem::take(&mut self.catching_txs);
        for (tx_id, (tx, tx_ctx)) in buffered.into_iter() {
            self.handle_sequenced_tx(tx_id, tx, tx_ctx).await?;
        }

        self.catching_success_txs.clear();
        Ok(())
    }

    /// Flush buffered txs when idle to avoid indefinite buffering.
    async fn handle_idle_flush(&mut self) -> Result<()> {
        if self.catching_up {
            return Ok(());
        }
        let has_pending = !self.store.unsettled_txs.is_empty()
            || !self.store.proving_txs.is_empty()
            || !self.store.buffered_blobs.is_empty();
        if !has_pending {
            return Ok(());
        }
        self.flush_pending(true).await
    }

    /// Record a newly sequenced transaction for future proving.
    async fn handle_sequenced_tx(
        &mut self,
        tx_id: TxId,
        tx: BlobTransaction,
        tx_ctx: Arc<TxContext>,
    ) -> Result<()> {
        if tx
            .blobs
            .iter()
            .all(|b| b.contract_name != self.ctx.contract_name)
        {
            return Ok(());
        }

        if self.catching_up {
            self.catching_txs.insert(tx_id, (tx, tx_ctx));
            return Ok(());
        }

        self.store.tx_chain.push(tx_id.clone());
        self.add_tx_to_waiting(&tx, &tx_ctx, &tx_id);
        self.flush_pending(false).await?;
        Ok(())
    }

    /// Apply settlement (success/failure/timeout) and trigger replay if needed.
    async fn handle_settled_tx(&mut self, tx_data: ContractTx) -> Result<()> {
        let ContractTx {
            tx_id,
            tx,
            tx_ctx,
            status,
            contract_changes,
            ..
        } = tx_data;

        if let Some(contract_change) = contract_changes.get(&self.ctx.contract_name) {
            self.handle_contract_change(contract_change).await?;
        }

        if self.catching_up {
            // Invariant: settled events are emitted only after their matching sequenced event.
            // If this is ever violated, catch-up replay assumptions no longer hold.
            match status {
                TransactionStatusDb::Success => {
                    self.catching_success_txs.push((tx_id.clone(), tx, tx_ctx));
                }
                TransactionStatusDb::Failure | TransactionStatusDb::TimedOut => {}
                _ => {}
            }
            self.catching_txs.shift_remove(&tx_id);
            return Ok(());
        }

        match status {
            TransactionStatusDb::Success => {
                self.settle_tx_success(&tx_id)?;
            }
            TransactionStatusDb::Failure | TransactionStatusDb::TimedOut => {
                self.settle_tx_failed(&tx_id)?;
            }
            _ => {}
        }

        self.flush_pending(false).await?;
        Ok(())
    }

    /// React to onchain contract changes reported by contract-listener.
    async fn handle_contract_change(&mut self, contract_change: &ContractChangeData) -> Result<()> {
        for change_type in &contract_change.change_types {
            match change_type {
                ContractChangeType::Registered => {}
                ContractChangeType::ProgramIdUpdated => {
                    let updated_program_id = ProgramId(contract_change.program_id.clone());
                    self.ensure_prover_available(
                        &updated_program_id,
                        "Program ID updated from contract change",
                    )
                    .await?;
                }
                ContractChangeType::Deleted => {
                    self.router_state.lock().unwrap().is_proving = false;
                    anyhow::bail!(
                        "Contract {} has been deleted (at height {:?}), stopping AutoProver",
                        self.ctx.contract_name,
                        contract_change.deleted_at_height
                    );
                }
                _ => {}
            }
        }

        Ok(())
    }

    /// Flush buffered blobs based on thresholds or a forced flush.
    async fn flush_pending(&mut self, force_flush: bool) -> Result<()> {
        // If a previously successful tx later failed, replay proofs from the earliest affected slot.
        let replay_from = self.pending_replay_from.take();
        if let Some(replay_from) = replay_from {
            let post_failure_blobs = self
                .store
                .proving_txs
                .iter()
                .skip(replay_from)
                .map(|(tx, tx_ctx, tx_id)| {
                    self.get_provable_blobs(tx_id.clone(), tx.clone(), tx_ctx.clone())
                })
                .collect::<Vec<_>>();
            let mut join_handles = Vec::new();
            self.prove_supported_blob(post_failure_blobs, &mut join_handles)
                .await?;
        }

        // Decide whether to flush buffered blobs based on thresholds or idle flush.
        let buffered = self.select_buffered_blobs(force_flush);

        if let Some(buffered) = buffered {
            let mut join_handles = Vec::new();
            self.prove_supported_blob(buffered, &mut join_handles)
                .await?;
            let join_handles_fut = async {
                for handle in join_handles {
                    _ = log_error!(handle.await, "In proving task");
                }
            };
            let res = shutdown_aware_timeout::<Self, _>(
                &mut self.bus,
                Duration::from_secs(30),
                join_handles_fut,
            )
            .await;
            if res.is_err() {
                info!(
                    cn =% self.ctx.contract_name,
                    "Proving tasks timed out after 30 seconds, continuing"
                );
            }
        }

        Ok(())
    }

    /// Decide whether to flush buffered blobs and return the batch if so.
    fn select_buffered_blobs(&mut self, force_flush: bool) -> Option<BufferedBlobs> {
        if force_flush {
            // Idle flush: avoid indefinite buffering when there is no new traffic.
            self.populate_unsettled_if_empty();
            if !self.store.buffered_blobs.is_empty() {
                debug!(
                    cn =% self.ctx.contract_name,
                    "Idle flush: processing {} blobs.",
                    self.store.buffered_blobs.len()
                );
                return Some(self.store.buffered_blobs.drain(..).collect::<Vec<_>>());
            }
            return None;
        }

        self.populate_unsettled_if_empty();

        if self.store.buffered_blobs.is_empty() {
            return None;
        }

        if (self.ctx.tx_buffer_size > 0
            && self.store.buffered_blobs.len() >= self.ctx.tx_buffer_size)
            || self.store.buffered_blobs.len() >= self.ctx.max_txs_per_proof
        {
            // Threshold met, flush the buffer.
            debug!(
                cn =% self.ctx.contract_name,
                "Buffered txs threshold met, processing {} blobs",
                self.store.buffered_blobs.len()
            );
            return Some(self.store.buffered_blobs.drain(..).collect::<Vec<_>>());
        }

        None
    }

    /// Move waiting txs into the proving window and buffer their blobs.
    fn populate_unsettled_if_empty(&mut self) {
        let available = self.store.unsettled_txs.len();
        if available == 0 {
            return;
        }

        let current = self.store.proving_txs.len();
        let target = self.ctx.tx_working_window_size;
        if current >= target {
            return;
        }

        // Move up to the working window into proving.
        let pop_waiting = std::cmp::min(available, target - current);
        debug!(
            cn =% self.ctx.contract_name,
            "Moving {} waiting txs to unsettled",
            pop_waiting
        );

        let newly_added: Vec<_> = self.store.unsettled_txs.drain(..pop_waiting).collect();
        self.store.proving_txs.extend(newly_added.iter().cloned());

        // Only buffer newly added txs to avoid re-proving already buffered ones.
        let buffered = newly_added
            .into_iter()
            .map(|(tx, tx_ctx, tx_id)| self.get_provable_blobs(tx_id, tx, tx_ctx))
            .collect::<Vec<_>>();
        self.store.buffered_blobs.extend(buffered);
    }

    /// Enqueue a tx as waiting to be proved.
    fn add_tx_to_waiting(&mut self, tx: &BlobTransaction, tx_ctx: &Arc<TxContext>, tx_id: &TxId) {
        debug!(
            cn =% self.ctx.contract_name,
            tx_hash =% tx.hashed(),
            "Adding waiting tx {}",
            tx.hashed()
        );
        self.store
            .unsettled_txs
            .push((tx.clone(), tx_ctx.clone(), tx_id.clone()));
    }

    /// Extract blob indexes for this contract from a transaction.
    fn get_provable_blobs(
        &self,
        tx_id: TxId,
        tx: BlobTransaction,
        tx_ctx: Arc<TxContext>,
    ) -> (TxId, Vec<BlobIndex>, BlobTransaction, Arc<TxContext>) {
        let mut indexes = vec![];
        for (index, blob) in tx.blobs.iter().enumerate() {
            if blob.contract_name == self.ctx.contract_name {
                indexes.push(index.into());
            }
        }
        (tx_id, indexes, tx, tx_ctx)
    }

    /// Finalize a successful settlement and prune history.
    fn settle_tx_success(&mut self, tx_id: &TxId) -> Result<()> {
        let prev_tx = self
            .store
            .tx_chain
            .iter()
            .enumerate()
            .find(|(_, h)| *h == tx_id)
            .and_then(|(i, _)| {
                if i > 0 {
                    self.store.tx_chain.get(i - 1)
                } else {
                    None
                }
            });
        if let Some(prev_tx) = prev_tx {
            debug!(
                cn =% self.ctx.contract_name,
                tx_id =% tx_id,
                "🔥 Removing state history for tx {}",
                prev_tx
            );
            self.store.state_history.remove(prev_tx);
        }
        let pos_chain = self.store.tx_chain.iter().position(|h| h == tx_id);
        if let Some(pos_chain) = pos_chain {
            debug!(
                cn =% self.ctx.contract_name,
                tx_id =% tx_id,
                "Settling tx {}. Previous tx: {:?}, Position in chain: {}",
                tx_id,
                prev_tx,
                pos_chain
            );
            self.store.tx_chain = self.store.tx_chain.split_off(pos_chain);
        }
        self.remove_from_unsettled_txs(tx_id);
        Ok(())
    }

    /// Handle failed/timeout settlement and schedule replays if needed.
    fn settle_tx_failed(&mut self, tx_id: &TxId) -> Result<()> {
        if let Some(pos) = self.remove_from_unsettled_txs(tx_id) {
            info!(
                cn =% self.ctx.contract_name,
                tx_hash =% tx_id,
                "🔥 Failed tx, removing state history for tx {}",
               tx_id
            );
            let found = self.store.state_history.remove(tx_id);
            self.store.tx_chain.retain(|h| h != tx_id);
            if let Some((_, success)) = found {
                if success {
                    self.pending_replay_from =
                        Some(std::cmp::min(self.pending_replay_from.unwrap_or(pos), pos));
                    self.clear_state_history_after_failed(pos)?;
                } else {
                    debug!(
                        cn =% self.ctx.contract_name,
                        tx_hash =% tx_id,
                        "🔀 Tx {} already executed as failed, nothing to re-execute",
                        tx_id
                    );
                }
            } else {
                debug!(
                    cn =% self.ctx.contract_name,
                    tx_hash =% tx_id,
                    "🔀 No state history found for tx {}, nothing to re-execute",
                    tx_id
                );
            }
        }
        Ok(())
    }

    /// Remove a tx from waiting/proving sets, returning its position if found.
    fn remove_from_unsettled_txs(&mut self, tx_id: &TxId) -> Option<usize> {
        let tx = self
            .store
            .proving_txs
            .iter()
            .position(|(_, _, tx_id2)| *tx_id2 == *tx_id);
        if let Some(pos) = tx {
            self.store.proving_txs.remove(pos);
            self.store
                .buffered_blobs
                .retain(|(tx_id2, _, _, _)| *tx_id2 != *tx_id);
            return Some(pos);
        } else {
            let tx = self
                .store
                .unsettled_txs
                .iter()
                .position(|(_, _, tx_id2)| *tx_id2 == *tx_id);
            if let Some(pos) = tx {
                self.store.unsettled_txs.remove(pos);
                return Some(pos);
            }
        }
        None
    }

    /// Find the most recent known state before the given tx.
    fn get_state_of_prev_tx(&self, tx_id: &TxId) -> Option<Contract> {
        let pos = self
            .store
            .tx_chain
            .iter()
            .position(|tx_id2| tx_id2 == tx_id);

        let Some(pos) = pos else {
            warn!(
                cn =% self.ctx.contract_name,
                tx_hash =% tx_id,
                "No previous tx, returning default state"
            );
            return Some(self.ctx.default_state.clone());
        };

        // Walk backwards to find the most recent state we have.
        for idx in (0..pos).rev() {
            let prev_tx = &self.store.tx_chain[idx];
            if let Some(contract) = self.store.state_history.get(prev_tx).cloned() {
                debug!(
                    cn =% self.ctx.contract_name,
                    tx_hash =% tx_id,
                    "Found previous state from tx {:?}",
                    prev_tx
                );
                return Some(contract.0);
            }
        }

        warn!(
            cn =% self.ctx.contract_name,
            tx_hash =% tx_id,
            "No previous state found in history, returning default state"
        );
        Some(self.ctx.default_state.clone())
    }

    /// Drop cached state for txs after a failed one to force re-execution.
    fn clear_state_history_after_failed(&mut self, idx: usize) -> Result<()> {
        for (_, _, tx_id) in self.store.proving_txs.clone().iter().skip(idx) {
            debug!(
                cn =% self.ctx.contract_name,
                tx_id =% tx_id,
                "🔥 Re-execute tx after failure, removing state history for tx {}",
                tx_id
            );
            self.store.state_history.remove(tx_id);
        }
        Ok(())
    }

    /// Execute blobs, update state history, and spawn proof tasks for a batch.
    async fn prove_supported_blob(
        &mut self,
        mut blobs: Vec<(TxId, Vec<BlobIndex>, BlobTransaction, Arc<TxContext>)>,
        join_handles: &mut Vec<JoinHandle<()>>,
    ) -> Result<()> {
        let remaining_blobs = if blobs.len() > self.ctx.max_txs_per_proof {
            let remaining_blobs = blobs.split_off(self.ctx.max_txs_per_proof);
            info!(
                cn =% self.ctx.contract_name,
                "Too many blobs to prove in one go: {} / {}. Splitting into multiple proofs. ({} remaining)",
                blobs.len() + remaining_blobs.len(),
                self.ctx.max_txs_per_proof,
                remaining_blobs.len()
            );
            remaining_blobs
        } else {
            vec![]
        };

        let blobs = blobs; // remove mut

        if blobs.is_empty() {
            return Ok(());
        }
        let mut current_program_id = self.current_program_id.clone();
        let mut calldatas = vec![];
        let mut initial_commitment_metadata = None;
        for (tx_id, blob_indexes, tx, tx_ctx) in blobs {
            let mut contract = self
                .get_state_of_prev_tx(&tx_id)
                .ok_or_else(|| anyhow!("Failed to get state of previous tx {}", tx_id))?;
            //let initial_contract = contract.clone();
            let mut error: Option<String> = None;
            let mut updated_program_id = None;

            for blob_index in blob_indexes {
                let blob = tx.blobs.get(blob_index.0).ok_or_else(|| {
                    anyhow!("Failed to get blob {} from tx {}", blob_index, tx.hashed())
                })?;
                let blobs = tx.blobs.clone();

                let state = contract
                    .build_commitment_metadata(blob)
                    .map_err(|e| anyhow!(e))
                    .context("Failed to build commitment metadata");

                // If failed to build commitment metadata, we skip the tx, but continue with next ones
                if let Err(e) = state {
                    error!(
                        cn =% self.ctx.contract_name,
                        tx_hash =% tx.hashed(),
                        tx_height =% tx_ctx.block_height,
                        "{e:#}"
                    );
                    error = Some(e.to_string());
                    break;
                }
                let state = state.unwrap();

                let commitment_metadata = state;

                if initial_commitment_metadata.is_none() {
                    initial_commitment_metadata = Some(commitment_metadata.clone());
                } else {
                    initial_commitment_metadata = Some(
                        contract
                            .merge_commitment_metadata(
                                initial_commitment_metadata.unwrap(),
                                commitment_metadata.clone(),
                            )
                            .map_err(|e| anyhow!(e))
                            .context("Merging commitment_metadata")?,
                    );
                }

                let calldata = Calldata {
                    identity: tx.identity.clone(),
                    tx_hash: tx_id.1.clone(),
                    private_input: vec![],
                    blobs: blobs.clone().into(),
                    index: blob_index,
                    tx_ctx: Some((*tx_ctx).clone()),
                    tx_blob_count: blobs.len(),
                };

                match contract.handle(&calldata).map_err(|e| anyhow!(e)) {
                    Err(e) => {
                        warn!(
                            cn =% self.ctx.contract_name,
                            tx_hash =% tx.hashed(),
                            tx_height =% tx_ctx.block_height,
                            "⚠️ Error executing contract, no proof generated: {e}"
                        );
                        error = Some(e.to_string());
                        break;
                    }
                    Ok(hyli_output) => {
                        info!(
                            cn =% self.ctx.contract_name,
                            tx_hash =% tx.hashed(),
                            tx_height =% tx_ctx.block_height,
                            "🔧 Executed contract: {}. Success: {}",
                            String::from_utf8_lossy(&hyli_output.program_outputs),
                            hyli_output.success
                        );
                        if !hyli_output.success {
                            error = Some(format!(
                                "Executed contract with error :{}",
                                String::from_utf8_lossy(&hyli_output.program_outputs),
                            ));
                            // don't break here, we want this calldata to be stored
                        }
                        let detected_program_id =
                            hyli_output.onchain_effects.iter().find_map(|eff| {
                                if let sdk::OnchainEffect::UpdateContractProgramId(
                                    _contract_name,
                                    program_id,
                                ) = eff
                                {
                                    Some(program_id.clone())
                                } else {
                                    None
                                }
                            });
                        if let Some(updated_program_id) = &detected_program_id {
                            log_error!(
                                self.ensure_prover_available(
                                    updated_program_id,
                                    &format!("Program ID updated for tx {}", tx_id),
                                )
                                .await,
                                "Adding new prover after program ID update"
                            )?;
                        }
                        if detected_program_id.is_some() {
                            updated_program_id = detected_program_id;
                        }
                    }
                }

                calldatas.push(calldata);
                if error.is_some() {
                    break;
                }
            }
            if let Some(e) = error {
                debug!(
                    cn =% self.ctx.contract_name,
                    tx_id =% tx_id,
                    tx_height =% tx_ctx.block_height,
                    "Tx {} failed, storing initial state. Error was: {e}",
                    tx_id
                );
                // Must exist - we failed above otherwise.
                let initial_contract = self.get_state_of_prev_tx(&tx_id).unwrap();
                self.store
                    .state_history
                    .insert(tx_id, (initial_contract, false));
            } else {
                debug!(
                    cn =% self.ctx.contract_name,
                    tx_id =% tx_id,
                    tx_height =% tx_ctx.block_height,
                    "Adding state history for tx {}",
                    tx.hashed()
                );
                self.store.state_history.insert(tx_id, (contract, true));
            }

            if let Some(next_program_id) = updated_program_id {
                if let Some(commitment_metadata) = initial_commitment_metadata.take() {
                    if !calldatas.is_empty() {
                        let batch_id = self.store.batch_id;
                        self.store.batch_id += 1;
                        self.spawn_proof_for_batch(
                            &current_program_id,
                            commitment_metadata,
                            std::mem::take(&mut calldatas),
                            batch_id,
                            join_handles,
                        )?;
                    }
                }
                current_program_id = next_program_id;
            }
        }

        self.current_program_id = current_program_id;

        if calldatas.is_empty() {
            if !remaining_blobs.is_empty() {
                self.prove_supported_blob_boxed(remaining_blobs, join_handles)
                    .await?;
            }
            return Ok(());
        }

        let Some(commitment_metadata) = initial_commitment_metadata else {
            if !remaining_blobs.is_empty() {
                self.prove_supported_blob_boxed(remaining_blobs, join_handles)
                    .await?;
            }
            return Ok(());
        };
        let batch_id = self.store.batch_id;
        self.store.batch_id += 1;
        self.spawn_proof_for_batch(
            &self.current_program_id,
            commitment_metadata,
            calldatas,
            batch_id,
            join_handles,
        )?;
        if !remaining_blobs.is_empty() {
            self.prove_supported_blob_boxed(remaining_blobs, join_handles)
                .await?;
        }
        Ok(())
    }

    /// Boxed wrapper to allow recursive async calls on prove_supported_blob.
    fn prove_supported_blob_boxed<'a>(
        &'a mut self,
        blobs: Vec<(TxId, Vec<BlobIndex>, BlobTransaction, Arc<TxContext>)>,
        join_handles: &'a mut Vec<JoinHandle<()>>,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(self.prove_supported_blob(blobs, join_handles))
    }

    /// Download and register a prover for the given program ID.
    async fn add_prover(&mut self, program_id: &ProgramId) -> Result<()> {
        let prover = Arc::new(
            Prover::new_from_registry(&self.ctx.contract_name, program_id.clone())
                .await
                .context("Creating new prover with updated ELF")?,
        );

        self.provers.insert(program_id.clone(), prover);

        info!(
            cn =% self.ctx.contract_name,
            "Prover ELF downloaded for program ID: {}",
            program_id
        );
        Ok(())
    }

    /// Ensures a prover is available for the given program ID.
    /// If not present, downloads the prover from the registry.
    async fn ensure_prover_available(
        &mut self,
        program_id: &ProgramId,
        context_info: &str,
    ) -> Result<()> {
        if !self.provers.contains_key(program_id) {
            info!(
                cn =% self.ctx.contract_name,
                "{}, Trying to download prover for {}",
                context_info,
                program_id
            );

            self.add_prover(program_id).await?;
        }
        Ok(())
    }

    /// Spawn an async task to generate and submit a proof for a batch.
    fn spawn_proof_for_batch(
        &self,
        program_id: &ProgramId,
        commitment_metadata: Vec<u8>,
        calldatas: Vec<Calldata>,
        batch_id: u64,
        join_handles: &mut Vec<JoinHandle<()>>,
    ) -> Result<()> {
        let node_client = self.ctx.node.clone();
        let prover = self
            .provers
            .get(program_id)
            .ok_or_else(|| anyhow!("No prover found for program ID: {}", program_id))?
            .clone();
        let contract_name = self.ctx.contract_name.clone();
        let metrics = self.metrics.clone();
        let len = calldatas.len();

        info!(
            cn =% contract_name,
            "Handling {} txs. Batch ID: {batch_id}",
            len
        );

        let handle = logged_task(async move {
            let mut retries = 0;
            const MAX_RETRIES: u32 = 30;

            loop {
                info!(
                    cn =% contract_name,
                    "Proving {} txs. Batch id: {batch_id}, Retries: {retries}",
                    len,
                );
                let start = std::time::Instant::now();
                metrics.record_proof_requested();
                match prover
                    .prove(commitment_metadata.clone(), calldatas.clone())
                    .await
                {
                    Ok(proof) => {
                        let elapsed = start.elapsed();
                        metrics.record_generation_time(elapsed.as_secs_f64());
                        metrics.record_proof_size(proof.data.0.len() as u64);
                        metrics.record_proof_success();
                        if let Some(cycles) = proof.metadata.cycles {
                            metrics.record_proof_cycles(cycles);
                        }
                        let tx = ProofTransaction {
                            contract_name: contract_name.clone(),
                            program_id: prover.program_id(),
                            verifier: prover.verifier(),
                            proof: proof.data,
                        };
                        // If we are in nosend mode, we just log the proof and don't send it (for debugging)
                        if std::env::var("HYLI_PROVER_NOSEND")
                            .map(|v| v == "1" || v.to_lowercase() == "true")
                            .unwrap_or(false)
                        {
                            info!("✅ Proved {len} txs in {elapsed:?}, Batch id: {batch_id}.");
                        } else {
                            match node_client.send_tx_proof(tx).await {
                                Ok(tx_hash) => {
                                    info!(
                                        "✅ Proved {len} txs in {elapsed:?}, Batch id: {batch_id}, Proof TX hash: {tx_hash}"
                                    );
                                }
                                Err(e) => {
                                    error!("Failed to send proof: {e:#}");
                                }
                            }
                        }
                        break;
                    }
                    Err(e) => {
                        metrics.record_proof_failure();

                        let should_retry =
                            e.to_string().contains("SessionCreateErr") && retries < MAX_RETRIES;
                        if should_retry {
                            warn!(
                                "Batch id: {batch_id}, Session creation error, retrying ({}/{}). {e:#}",
                                retries, MAX_RETRIES
                            );
                            retries += 1;
                            metrics.record_proof_retry();
                            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                            continue;
                        }
                        error!("Error proving tx: {:?}. Batch id: {batch_id}", e);
                        break;
                    }
                };
            }
        });
        join_handles.push(handle);
        Ok(())
    }
}

#[cfg(test)]
mod prover_tests;

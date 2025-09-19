use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::Duration;
use std::{fmt::Debug, path::PathBuf, sync::Arc};

use crate::bus::{BusClientSender, BusMessage, SharedMessageBus};
use crate::modules::signal::shutdown_aware_timeout;
use crate::modules::SharedBuildApiCtx;
use crate::{log_error, module_bus_client, module_handle_messages, modules::Module};
use anyhow::{anyhow, bail, Context, Result};
use axum::extract::State;
use axum::Router;
use borsh::{BorshDeserialize, BorshSerialize};
use client_sdk::rest_client::NodeApiClient;
use client_sdk::{helpers::ClientSdkProver, transaction_builder::TxExecutorHandler};
use hyli_net::logged_task::logged_task;
use indexmap::IndexMap;
use sdk::{
    BlobIndex, BlobTransaction, BlockHeight, Calldata, ContractName, Hashed, NodeStateEvent,
    ProofTransaction, StateCommitment, StatefulEvent, StatefulEvents, TxContext, TxId,
};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

use super::prover_metrics::AutoProverMetrics;

/// `AutoProver` is a module that handles the proving of transactions
/// It listens to the node state events and processes all blobs in the block's transactions
/// for a given contract.
/// It asynchronously generates 1 ProofTransaction to prove all concerned blobs in a block
/// If a passed BlobTransaction times out, or is settled as failed, all blobs that are "after"
/// the failed transaction in the block are re-executed, and prooved all at once, even if in
/// multiple blocks.
/// This module requires the ELF to support multiproof. i.e. it requires the ELF to read
/// a `Vec<Calldata>` as input.
pub struct AutoProver<Contract: Send + Sync + Clone + 'static> {
    bus: AutoProverBusClient<Contract>,
    ctx: Arc<AutoProverCtx<Contract>>,
    store: AutoProverStore<Contract>,
    metrics: AutoProverMetrics,
    // If Some, the block to catch up to
    catching_up: Option<BlockHeight>,
    catching_up_state: StateCommitment,

    catching_txs: IndexMap<TxId, (BlobTransaction, Arc<TxContext>)>,
    catching_success_txs: Vec<(TxId, BlobTransaction, Arc<TxContext>)>,

    router_state: Arc<Mutex<RouterData>>,
}

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
    buffered_blobs: Vec<(TxId, Vec<BlobIndex>, BlobTransaction, Arc<TxContext>)>,
    buffered_blocks_count: u32,
    batch_id: u64,
    next_height: BlockHeight,
}

module_bus_client! {
#[derive(Debug)]
pub struct AutoProverBusClient<Contract: Send + Sync + Clone + 'static> {
    sender(AutoProverEvent<Contract>),
    receiver(NodeStateEvent),
}
}

pub struct AutoProverCtx<Contract> {
    pub data_directory: PathBuf,
    pub prover: Arc<dyn ClientSdkProver<Vec<Calldata>> + Send + Sync>,
    pub contract_name: ContractName,
    pub node: Arc<dyn NodeApiClient + Send + Sync>,
    // Optional API for readiness information
    pub api: Option<SharedBuildApiCtx>,
    pub default_state: Contract,
    /// How many blocks should we buffer before generating proofs ?
    pub buffer_blocks: u32,
    pub max_txs_per_proof: usize,
    pub tx_working_window_size: usize,
}

#[derive(Debug, Clone)]
pub enum AutoProverEvent<Contract> {
    /// Event sent when a blob is executed as failed
    /// proof will be generated & sent to the node
    FailedTx(TxId, String),
    /// Event sent when a blob is executed as success
    /// proof will be generated & sent to the node
    SuccessTx(TxId, Contract),
}

impl<Contract> BusMessage for AutoProverEvent<Contract> {
    const CAPACITY: usize = crate::bus::LOW_CAPACITY;
}

impl<Contract> Module for AutoProver<Contract>
where
    Contract: TxExecutorHandler
        + BorshSerialize
        + BorshDeserialize
        + Debug
        + Send
        + Sync
        + Clone
        + 'static,
{
    type Context = Arc<AutoProverCtx<Contract>>;

    async fn build(bus: SharedMessageBus, ctx: Self::Context) -> Result<Self> {
        let bus = AutoProverBusClient::<Contract>::new_from_bus(bus.new_handle()).await;

        let file = ctx
            .data_directory
            .join(format!("autoprover_{}.bin", ctx.contract_name).as_str());

        let mut store = match Self::load_from_disk::<AutoProverStore<Contract>>(file.as_path()) {
            Some(store) => store,
            None => AutoProverStore::<Contract> {
                unsettled_txs: vec![],
                proving_txs: vec![],
                state_history: BTreeMap::new(),
                tx_chain: vec![],
                buffered_blobs: vec![],
                buffered_blocks_count: 0,
                batch_id: 0,
                #[cfg(test)]
                next_height: BlockHeight(1),
                #[cfg(not(test))]
                next_height: BlockHeight(0),
            },
        };

        let infos = ctx.prover.info();

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
        let catching_up_state = contract_state.state_commitment;
        let catching_up = match contract_state.state_block_height.0 > 0 {
            true => Some(contract_state.state_block_height),
            false => None,
        };

        info!(
            cn =% ctx.contract_name,
            "Catching up to {:?}",
            catching_up
        );

        let catching_txs = if catching_up.is_some() && !store.tx_chain.is_empty() {
            // If we are restarting from serialized data and are catching up, we need to do some setup.
            // Move all unsettled transactions to catching_txs and restart.
            let mut txs = std::mem::take(&mut store.proving_txs);
            txs.extend(std::mem::take(&mut store.unsettled_txs));

            // Clear the rest
            store.tx_chain.truncate(1);
            let history_start = store
                .state_history
                .remove(store.tx_chain.first().expect("must exist"))
                .expect("We should have at least one transaction in the tx_chain");
            store.state_history = BTreeMap::new();
            store.state_history.insert(
                store.tx_chain.last().expect("must exist").clone(),
                history_start.clone(),
            );
            store.buffered_blobs.clear();
            store.buffered_blocks_count = 0;
            tracing::info!(
                cn =% ctx.contract_name,
                "Loaded {} unsettled transactions from disk, restarting from {}",
                txs.len(),
                store.tx_chain.last().expect("must exist")
            );
            IndexMap::from_iter(txs.into_iter().map(|(tx, tx_ctx, id)| (id, (tx, tx_ctx))))
        } else {
            IndexMap::new()
        };

        Ok(AutoProver {
            bus,
            store,
            ctx,
            metrics,
            catching_up,
            catching_up_state,
            catching_success_txs: vec![],
            catching_txs,
            router_state,
        })
    }

    async fn run(&mut self) -> Result<()> {
        module_handle_messages! {
            on_self self,
            listen<NodeStateEvent> event => {
                let NodeStateEvent::NewBlock(block) = event;
                let res = log_error!(self.handle_block(block.signed_block.height(), block.stateful_events).await, "handle note state event");
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

    async fn persist(&mut self) -> Result<()> {
        log_error!(
            Self::save_on_disk::<AutoProverStore<Contract>>(
                self.ctx
                    .data_directory
                    .join(format!("autoprover_{}.bin", self.ctx.contract_name))
                    .as_path(),
                &self.store,
            ),
            "Saving prover"
        )
    }
}

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

impl<Contract> AutoProver<Contract>
where
    Contract: TxExecutorHandler + Debug + Clone + Send + Sync + 'static,
{
    async fn handle_block(
        &mut self,
        block_height: BlockHeight,
        block: Arc<StatefulEvents>,
    ) -> Result<()> {
        if block_height.0 < self.store.next_height.0 {
            info!(
                cn =% self.ctx.contract_name,
                "Ignoring already proved block {}. Expecting block {}",
                block_height,
                self.store.next_height
            );
            return Ok(());
        } else if block_height.0 > self.store.next_height.0 {
            bail!(
                "Received future block {} but expected block {}",
                block_height,
                self.store.next_height
            );
        }
        self.store.next_height = block_height + 1;

        if self.catching_up.is_some_and(|h| block_height.0 <= h.0) {
            self.handle_catchup_block(block)
                .await
                .context("Failed to handle settled block")?;
            if self.catching_up.is_some_and(|h| block_height.0 == h.0) {
                let current_state = self
                    .ctx
                    .node
                    .get_contract(self.ctx.contract_name.clone())
                    .await?;
                // If we took enough time catchup up, recompute a new catchup target.
                if current_state.state_block_height.0 > self.catching_up.unwrap().0 + 10 {
                    info!(
                        cn =% self.ctx.contract_name,
                        "🚅 Updating catch up target from {} to {}",
                        self.catching_up.unwrap(),
                        current_state.state_block_height
                    );
                    // Set the new catching up target.
                    self.catching_up = Some(current_state.state_block_height);
                    self.catching_up_state = current_state.state_commitment;
                    return Ok(());
                }

                // Build blobs to execute from catching_txs
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
                // Clear our flag.
                self.catching_up = None;

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
                info!(
                    cn =% self.ctx.contract_name,
                    "All catching blobs processed, catching up finished at block {} with tx {}",
                    block_height,
                    last_tx_id.as_ref().map_or_else(
                        || "None".to_string(),
                        |tx| tx.to_string()
                    )
                );

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
                          self.catching_up_state, final_state
                        );
                    }
                }

                // Mark ourselves ready to prove.
                self.router_state.lock().unwrap().is_proving = true;

                if let Some(last_tx_id) = last_tx_id {
                    self.store.tx_chain = vec![last_tx_id.clone()];
                    self.store
                        .state_history
                        .insert(last_tx_id.clone(), (contract, true));
                }

                // Now any remaining TX is to be buffered and handled on the next block
                info!(
                    cn =% self.ctx.contract_name,
                    "Storing remaining {} unsettled TXs after catching up",
                    self.catching_txs.len()
                );
                // Store all TXs in our waiting buffer.
                self.store
                    .tx_chain
                    .extend(self.catching_txs.keys().cloned());

                self.store.unsettled_txs.extend(
                    std::mem::take(&mut self.catching_txs)
                        .into_iter()
                        .map(|(id, (tx, tx_ctx))| (tx, tx_ctx, id)),
                );
            }
        } else {
            self.handle_processed_block(block_height, block).await?;
        }

        Ok(())
    }

    async fn handle_catchup_block(&mut self, block: Arc<StatefulEvents>) -> Result<()> {
        for (tx_id, event) in &block.events {
            match event {
                StatefulEvent::SequencedTx(tx, tx_ctx) => {
                    if tx
                        .blobs
                        .iter()
                        .all(|b| b.contract_name != self.ctx.contract_name)
                    {
                        continue;
                    }
                    self.catching_txs
                        .insert(tx_id.clone(), (tx.clone(), tx_ctx.clone()));
                }
                // Only used to reduce size of catching_txs
                StatefulEvent::TimedOutTx(..) | StatefulEvent::FailedTx(..) => {
                    self.catching_txs.retain(|t, _| t != tx_id);
                }
                StatefulEvent::SettledTx(_tx) => {
                    // TODO: we no longer need to store the data.
                    if let Some((tx, tx_ctx)) = self.catching_txs.shift_remove(tx_id) {
                        self.catching_success_txs.push((tx_id.clone(), tx, tx_ctx));
                    }
                }
                StatefulEvent::ContractDelete(..)
                | StatefulEvent::ContractRegistration(..)
                | StatefulEvent::ContractUpdate(..) => {
                    // Ignore
                }
            }
        }
        Ok(())
    }

    async fn handle_processed_block(
        &mut self,
        block_height: BlockHeight,
        block: Arc<StatefulEvents>,
    ) -> Result<()> {
        tracing::trace!(
            cn =% self.ctx.contract_name,
            block_height =% block_height,
            "Handling processed block {}",
            block_height
        );
        if block_height.0.is_multiple_of(1000) {
            info!(
                cn =% self.ctx.contract_name,
                block_height =% block_height,
                "Processing block {}",
                block_height
            );
        }

        let mut replay_from = None;

        let mut last_contract_state = None;
        for (tx_id, event) in &block.events {
            match event {
                StatefulEvent::SequencedTx(tx, tx_ctx) => {
                    if tx
                        .blobs
                        .iter()
                        .all(|b| b.contract_name != self.ctx.contract_name)
                    {
                        continue;
                    }
                    self.store.tx_chain.push(tx_id.clone());
                    self.add_tx_to_waiting(tx, tx_ctx, tx_id);
                }
                StatefulEvent::TimedOutTx(..) | StatefulEvent::FailedTx(..) => {
                    self.settle_tx_failed(&mut replay_from, tx_id)?;
                }
                StatefulEvent::SettledTx(_tx) => {
                    self.settle_tx_success(tx_id)?;
                }
                StatefulEvent::ContractDelete(..) | StatefulEvent::ContractRegistration(..) => {
                    // Ignore
                }
                StatefulEvent::ContractUpdate(contract_name, contract) => {
                    if *contract_name != self.ctx.contract_name {
                        continue;
                    }
                    last_contract_state = Some(&contract.state);
                }
            }
        }

        // Check last state
        if let Some(last_contract_state) = last_contract_state {
            if let Some(prover_state) = self
                .store
                .tx_chain
                .first()
                .and_then(|first| self.store.state_history.get(first))
            {
                if &prover_state.0.get_state_commitment() != last_contract_state {
                    error!(
                        cn =% self.ctx.contract_name,
                        block_height =% block_height,
                        "Contract state in store does not match the one onchain. Onchain: {:?}, Store: {:?}",
                        last_contract_state, prover_state
                    );
                    error!(
                        cn =% self.ctx.contract_name,
                        block_height =% block_height,
                        "This is likely a bug in the prover, please report it to the Hyli team."
                    );
                    bail!(
                        "Contract state in store does not match the one onchain. Onchain: {:?}, Store: {:?}",
                        last_contract_state, prover_state
                    );
                }
            } else {
                debug!(
                    cn =% self.ctx.contract_name,
                    block_height =% block_height,
                    "No previous state found in store"
                );
            }
        }

        if let Some(replay_from) = replay_from {
            // TODO: we have to replay them immediately, to re-populate state_history
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
            self.prove_supported_blob(post_failure_blobs, &mut join_handles)?;
            // Don't wait, we'll want to prove the other successful proofs.
        }

        if self.store.proving_txs.is_empty()
            && self.store.unsettled_txs.len() >= self.ctx.tx_working_window_size
        {
            // If we have no unsettled TXs, but we have enough TXs, we can populate them
            self.populate_unsettled_if_empty();
        }

        let buffered = if !self.store.buffered_blobs.is_empty() {
            debug!(
                cn =% self.ctx.contract_name,
                "Buffer is full, processing {} blobs.",
                self.store.buffered_blobs.len()
            );
            self.store.buffered_blocks_count = 0;
            Some(self.store.buffered_blobs.drain(..).collect::<Vec<_>>())
        } else if self.store.buffered_blocks_count >= self.ctx.buffer_blocks {
            // Check if we should prove some things.
            self.populate_unsettled_if_empty();

            if !self.store.buffered_blobs.is_empty() {
                debug!(
                    cn =% self.ctx.contract_name,
                    "Buffered blocks achieved, processing {} blobs",
                    self.store.buffered_blobs.len()
                );
                self.store.buffered_blocks_count = 0;
                Some(self.store.buffered_blobs.drain(..).collect::<Vec<_>>())
            } else {
                None
            }
        } else {
            self.store.buffered_blocks_count += 1;
            None
        };

        if let Some(buffered) = buffered {
            let mut join_handles = Vec::new();
            self.prove_supported_blob(buffered, &mut join_handles)?;
            // Wait for all join handles, but with a 30 second timeout for the whole batch,
            // after which we'll move on.
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

    fn populate_unsettled_if_empty(&mut self) {
        if self.store.proving_txs.is_empty() {
            // Check if we should move some TXs from waiting to unsettled
            let pop_waiting = std::cmp::min(
                self.store.unsettled_txs.len(),
                self.ctx.tx_working_window_size,
            );
            if pop_waiting > 0 {
                debug!(
                    cn =% self.ctx.contract_name,
                    "Moving {} waiting txs to unsettled",
                    pop_waiting
                );

                self.store
                    .proving_txs
                    .extend(self.store.unsettled_txs.drain(..pop_waiting));

                // Reset blob buffer
                self.store.buffered_blobs = self
                    .store
                    .proving_txs
                    .iter()
                    .map(|(tx, tx_ctx, tx_id)| {
                        self.get_provable_blobs(tx_id.clone(), tx.clone(), tx_ctx.clone())
                    })
                    .collect::<Vec<_>>();
            }
        }
    }

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

    fn settle_tx_failed(&mut self, replay_from: &mut Option<usize>, tx_id: &TxId) -> Result<()> {
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
                    *replay_from = Some(std::cmp::min(replay_from.unwrap_or(pos), pos));
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

    fn get_state_of_prev_tx(&self, tx_id: &TxId) -> Option<Contract> {
        let prev_tx = self
            .store
            .tx_chain
            .iter()
            .enumerate()
            .find(|(_, tx_id2)| *tx_id2 == tx_id)
            .and_then(|(i, _)| {
                if i > 0 {
                    self.store.tx_chain.get(i - 1)
                } else {
                    None
                }
            });
        if let Some(prev_tx) = prev_tx {
            let prev_state = self.store.state_history.get(prev_tx).cloned();
            if let Some(contract) = prev_state {
                debug!(
                    cn =% self.ctx.contract_name,
                    tx_hash =% tx_id,
                    "Found previous state from tx {:?}",
                    prev_tx
                );
                return Some(contract.0);
            } else {
                error!(
                    cn =% self.ctx.contract_name,
                    tx_hash =% tx_id,
                    "No state history for previous tx {:?}, returning None",
                    prev_tx
                );
                error!("This is likely a bug in the prover, please report it to the Hyli team.");
                error!(cn =% self.ctx.contract_name, tx_hash =% tx_id, "State history: {:?}", self.store.state_history.keys());
                error!(
                    cn =% self.ctx.contract_name,
                    tx_hash =% tx_id,
                    "Unsettled txs: {:?}",
                    self.store.proving_txs.iter().map(|(t, _, _)| t.hashed()).collect::<Vec<_>>()
                );
            }
        } else {
            warn!(cn =% self.ctx.contract_name, tx_hash =% tx_id, "No previous tx, returning default state");
            return Some(self.ctx.default_state.clone());
        }
        None
    }

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

    fn prove_supported_blob(
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
        let batch_id = self.store.batch_id;
        self.store.batch_id += 1;
        info!(
            cn =% self.ctx.contract_name,
            "Handling {} txs. Batch ID: {batch_id}",
            blobs.len()
        );
        let mut calldatas = vec![];
        let mut initial_commitment_metadata = None;
        let len = blobs.len();
        for (tx_id, blob_indexes, tx, tx_ctx) in blobs {
            let mut contract = self
                .get_state_of_prev_tx(&tx_id)
                .ok_or_else(|| anyhow!("Failed to get state of previous tx {}", tx_id))?;
            //let initial_contract = contract.clone();
            let mut error: Option<String> = None;

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
                self.bus.send(AutoProverEvent::FailedTx(tx_id.clone(), e))?;
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
                /*self.bus.send(AutoProverEvent::SuccessTx(
                    tx_hash.clone(),
                    contract.clone(),
                ))?;*/
                self.store.state_history.insert(tx_id, (contract, true));
            }
        }

        if calldatas.is_empty() {
            if !remaining_blobs.is_empty() {
                self.prove_supported_blob(remaining_blobs, join_handles)?;
            }
            return Ok(());
        }

        let Some(commitment_metadata) = initial_commitment_metadata else {
            if !remaining_blobs.is_empty() {
                self.prove_supported_blob(remaining_blobs, join_handles)?;
            }
            return Ok(());
        };

        let node_client = self.ctx.node.clone();
        let prover = self.ctx.prover.clone();
        let contract_name = self.ctx.contract_name.clone();

        let metrics = self.metrics.clone();
        let handle = logged_task(async move {
            let mut retries = 0;
            const MAX_RETRIES: u32 = 30;

            loop {
                info!(
                    cn =% contract_name,
                    "Proving {} txs. Batch id: {batch_id}, Retries: {retries}",
                    calldatas.len(),
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
                                    info!("✅ Proved {len} txs in {elapsed:?}, Batch id: {batch_id}, Proof TX hash: {tx_hash}");
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
        if !remaining_blobs.is_empty() {
            self.prove_supported_blob(remaining_blobs, join_handles)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod prover_tests;

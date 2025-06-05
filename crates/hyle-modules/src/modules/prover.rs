use std::collections::BTreeMap;
use std::{fmt::Debug, path::PathBuf, sync::Arc};

use crate::bus::{BusClientSender, SharedMessageBus};
use crate::{log_error, module_bus_client, module_handle_messages, modules::Module};
use anyhow::{anyhow, Context, Result};
use borsh::{BorshDeserialize, BorshSerialize};
use client_sdk::rest_client::NodeApiClient;
use client_sdk::{helpers::ClientSdkProver, transaction_builder::TxExecutorHandler};
use sdk::{
    BlobIndex, BlobTransaction, Block, BlockHeight, Calldata, ContractName, Hashed, NodeStateEvent,
    ProofTransaction, TransactionData, TxContext, TxHash, HYLE_TESTNET_CHAIN_ID,
};
use tracing::{debug, error, info, trace, warn};

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
    // The last block where the contract is settled
    settled_height: BlockHeight,
    // If Some, represents the block height we need to start generating proofs
    catching_up: Option<BlockHeight>,
    catching_blobs: Vec<(BlobIndex, BlobTransaction, TxContext)>,
}

#[derive(Default, BorshSerialize, BorshDeserialize)]
pub struct AutoProverStore<Contract> {
    unsettled_txs: Vec<(BlobTransaction, TxContext)>,
    state_history: BTreeMap<TxHash, Contract>,
    tx_chain: Vec<TxHash>,
    contract: Contract,
    buffered_blobs: Vec<(BlobIndex, BlobTransaction, TxContext)>,
    buffered_blocks_count: u32,
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
    pub default_state: Contract,
    /// How many blocks should we buffer before generating proofs ?
    pub buffer_blocks: u32,
    pub max_txs_per_proof: usize,
}

#[derive(Debug, Clone)]
pub enum AutoProverEvent<Contract> {
    /// Event sent when a blob is executed as failed
    /// proof will be generated & sent to the node
    FailedTx(TxHash, String),
    /// Event sent when a blob is executed as success
    /// proof will be generated & sent to the node
    SuccessTx(TxHash, Contract),
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

        let store = match Self::load_from_disk::<AutoProverStore<Contract>>(file.as_path()) {
            Some(store) => store,
            None => AutoProverStore::<Contract> {
                contract: ctx.default_state.clone(),
                unsettled_txs: vec![],
                state_history: BTreeMap::new(),
                tx_chain: vec![],
                buffered_blobs: vec![],
                buffered_blocks_count: 0,
            },
        };

        let settled_height = ctx
            .node
            .get_settled_height(ctx.contract_name.clone())
            .await?;

        info!(
            cn =% ctx.contract_name,
            "Settled height received is {}",
            settled_height
        );

        let current_block = ctx.node.get_block_height().await?;
        let catching_up = if settled_height.0 < current_block.0 {
            Some(current_block)
        } else {
            None
        };

        Ok(AutoProver {
            bus,
            store,
            ctx,
            catching_up,
            catching_blobs: vec![],
            settled_height,
        })
    }

    async fn run(&mut self) -> Result<()> {
        module_handle_messages! {
            on_bus self.bus,
            listen<NodeStateEvent> event => {
                _ = log_error!(self.handle_node_state_event(event).await, "handle note state event")
            }
        };

        let _ = log_error!(
            Self::save_on_disk::<AutoProverStore<Contract>>(
                self.ctx
                    .data_directory
                    .join(format!("autoprover_{}.bin", self.ctx.contract_name))
                    .as_path(),
                &self.store,
            ),
            "Saving prover"
        );

        Ok(())
    }
}

impl<Contract> AutoProver<Contract>
where
    Contract: TxExecutorHandler + Debug + Clone + Send + Sync + 'static,
{
    async fn handle_node_state_event(&mut self, event: NodeStateEvent) -> Result<()> {
        let NodeStateEvent::NewBlock(block) = event;
        self.handle_processed_block(*block).await?;

        Ok(())
    }

    async fn handle_processed_block(&mut self, block: Block) -> Result<()> {
        tracing::trace!(
            cn =% self.ctx.contract_name,
            block_height =% block.block_height,
            "Handling processed block {}",
            block.block_height
        );
        let mut blobs = vec![];
        if block.block_height.0 % 1000 == 0 {
            info!(
                cn =% self.ctx.contract_name,
                block_height =% block.block_height,
                "Processing block {}",
                block.block_height
            );
        }
        for (_, tx) in block.txs {
            if let TransactionData::Blob(tx) = tx.transaction_data {
                if tx
                    .blobs
                    .iter()
                    .all(|b| b.contract_name != self.ctx.contract_name)
                {
                    continue;
                }
                self.store.tx_chain.push(tx.hashed());

                let tx_ctx = TxContext {
                    block_height: block.block_height,
                    block_hash: block.hash.clone(),
                    timestamp: block.block_timestamp.clone(),
                    lane_id: block
                        .lane_ids
                        .get(&tx.hashed())
                        .ok_or_else(|| anyhow!("Missing lane id in block for {}", tx.hashed()))?
                        .clone(),
                    chain_id: HYLE_TESTNET_CHAIN_ID,
                };
                blobs.extend(self.handle_blob(tx, tx_ctx));
            }
        }

        for tx in block.successful_txs {
            self.settle_tx_success(&tx)?;
        }

        for tx in block.timed_out_txs {
            self.settle_tx_failed(&tx)?;
        }

        for tx in block.failed_txs {
            self.settle_tx_failed(&tx)?;
        }

        if let Some(catching_up) = self.catching_up {
            if block.block_height.0 >= catching_up.0 {
                debug!(
                    cn =% self.ctx.contract_name,
                    "Catching up finished at block {}",
                    block.block_height
                );
                self.catching_up = None;
                self.catching_blobs.extend(blobs);
                blobs = self.catching_blobs.drain(..).collect();
                debug!(
                    cn =% self.ctx.contract_name,
                    "Catching up finished, {} blobs to process",
                    blobs.len()
                );
            }
        }

        if self.store.buffered_blobs.len() + blobs.len() >= self.ctx.max_txs_per_proof {
            let remaining_count =
                (self.store.buffered_blobs.len() + blobs.len()) % self.ctx.max_txs_per_proof;
            debug!(
                cn =% self.ctx.contract_name,
                "Buffer is full, processing {} blobs. Max: {}, remaining blobs: {}",
                self.store.buffered_blobs.len() + blobs.len(),
                self.ctx.max_txs_per_proof,
                remaining_count
            );
            let remaining_blobs = blobs.split_off(blobs.len() - remaining_count);

            let mut buffered = self.store.buffered_blobs.drain(..).collect::<Vec<_>>();
            self.store.buffered_blocks_count = 0;
            buffered.extend(blobs);
            self.prove_supported_blob(buffered)?;
            self.store.buffered_blobs = remaining_blobs;
        } else if self.store.buffered_blocks_count >= self.ctx.buffer_blocks {
            let mut buffered = self.store.buffered_blobs.drain(..).collect::<Vec<_>>();
            self.store.buffered_blocks_count = 0;
            buffered.extend(blobs);
            if !buffered.is_empty() {
                debug!(
                    cn =% self.ctx.contract_name,
                    "Buffered blocks achieved, processing {} blobs",
                    buffered.len()
                );
            }

            self.prove_supported_blob(buffered)?;
        } else {
            self.store.buffered_blobs.append(&mut blobs);
            self.store.buffered_blocks_count += 1;

            trace!(
                cn =% self.ctx.contract_name,
                "Buffered {} / {} blobs, {} / {} blocks.",
                self.store.buffered_blobs.len(),
                self.ctx.max_txs_per_proof,
                self.store.buffered_blocks_count,
                self.ctx.buffer_blocks
            );
        }

        Ok(())
    }

    fn handle_blob(
        &mut self,
        tx: BlobTransaction,
        tx_ctx: TxContext,
    ) -> Vec<(BlobIndex, BlobTransaction, TxContext)> {
        let mut blobs = vec![];
        for (index, blob) in tx.blobs.iter().enumerate() {
            if blob.contract_name == self.ctx.contract_name {
                blobs.push((index.into(), tx.clone(), tx_ctx.clone()));
            }
        }
        self.store.unsettled_txs.push((tx, tx_ctx));
        blobs
    }

    fn settle_tx_success(&mut self, tx: &TxHash) -> Result<()> {
        let prev_tx = self
            .store
            .tx_chain
            .iter()
            .enumerate()
            .find(|(_, h)| *h == tx)
            .and_then(|(i, _)| {
                if i > 0 {
                    self.store.tx_chain.get(i - 1)
                } else {
                    None
                }
            });
        if let Some(prev_tx) = prev_tx {
            self.store.state_history.remove(prev_tx);
        }
        let pos_chain = self.store.tx_chain.iter().position(|h| h == tx);
        if let Some(pos_chain) = pos_chain {
            self.store.tx_chain = self.store.tx_chain.split_off(pos_chain);
        }
        self.settle_tx(tx);
        Ok(())
    }

    fn settle_tx_failed(&mut self, tx: &TxHash) -> Result<()> {
        if let Some(pos) = self.settle_tx(tx) {
            self.handle_all_next_blobs(pos, tx)?;
            self.store.state_history.remove(tx);
            self.store.tx_chain.retain(|h| h != tx);
        }
        Ok(())
    }

    fn settle_tx(&mut self, hash: &TxHash) -> Option<usize> {
        if !self.catching_blobs.is_empty() {
            if let Some((_, blob_transaction, _)) = self.catching_blobs.first() {
                if blob_transaction.hashed() == *hash {
                    debug!(
                        cn =% self.ctx.contract_name,
                        tx_hash =% blob_transaction.hashed(),
                        "Catching blob {} has settled. Removed from the queue.",
                        blob_transaction.hashed()
                    );
                    self.catching_blobs.remove(0);
                }
            }
        }

        let tx = self
            .store
            .unsettled_txs
            .iter()
            .position(|(t, _)| t.hashed() == *hash);
        if let Some(pos) = tx {
            self.store.unsettled_txs.remove(pos);
            return Some(pos);
        }
        None
    }

    fn handle_all_next_blobs(&mut self, idx: usize, failed_tx: &TxHash) -> Result<()> {
        let tx_history = self.store.state_history.keys().collect::<Vec<_>>();
        let prev_tx = self
            .store
            .tx_chain
            .iter()
            .enumerate()
            .find(|(_, h)| *h == failed_tx)
            .and_then(|(mut i, _)| {
                while i > 0 {
                    let prev_tx = self.store.tx_chain.get(i - 1);
                    if let Some(prev_tx) = prev_tx {
                        if self.store.state_history.contains_key(prev_tx) {
                            return Some(prev_tx);
                        }
                    }
                    i -= 1;
                }
                None
            });

        tracing::debug!(
            cn =% self.ctx.contract_name,
            tx_hash =% failed_tx,
            "Handling failed tx {}. Previous tx: {:?}, Unsettled idx: {idx}, History: {:?}",
            failed_tx,
            prev_tx,
            tx_history
        );

        let prev_state =
            prev_tx.and_then(|prev_tx_hash| self.store.state_history.get(prev_tx_hash).cloned());

        if let Some(contract) = prev_state {
            debug!(cn =% self.ctx.contract_name, tx_hash =% failed_tx, "Reverting to previous state from tx {:?}", prev_tx);
            self.store.contract = contract.clone();
        } else {
            warn!(cn =% self.ctx.contract_name, tx_hash =% failed_tx, "Reverting to default state. History: {tx_history:?}");
            self.store.contract = self.ctx.default_state.clone();
        }
        let mut blobs = vec![];
        for (tx, ctx) in self.store.unsettled_txs.clone().iter().skip(idx) {
            for (index, blob) in tx.blobs.iter().enumerate() {
                if blob.contract_name == self.ctx.contract_name {
                    debug!(
                        cn =% self.ctx.contract_name,
                        "Re-execute blob for tx {} after a previous tx failure",
                        tx.hashed()
                    );
                    self.store.state_history.remove(&tx.hashed());
                    blobs.push((index.into(), tx.clone(), ctx.clone()));
                }
            }
        }
        self.prove_supported_blob(blobs)
    }

    fn prove_supported_blob(
        &mut self,
        mut blobs: Vec<(BlobIndex, BlobTransaction, TxContext)>,
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
        debug!(
            cn =% self.ctx.contract_name,
            "Handling {} txs",
            blobs.len()
        );
        let mut calldatas = vec![];
        let mut initial_commitment_metadata = None;
        let len = blobs.len();
        for (blob_index, tx, tx_ctx) in blobs {
            let already_settled_tx = tx_ctx.block_height.0 <= self.settled_height.0;

            if !already_settled_tx {
                if let Some(catching_up) = self.catching_up {
                    debug!(
                        cn =% self.ctx.contract_name,
                        tx_hash =% tx.hashed(),
                        tx_height =% tx_ctx.block_height,
                        catching_up =% catching_up,
                        "Catching up, buffering blobs",
                    );

                    if !self
                        .catching_blobs
                        .iter()
                        .any(|(_, t, _)| t.hashed() == tx.hashed())
                    {
                        self.catching_blobs.push((blob_index, tx, tx_ctx));
                    }
                    continue;
                }
            }

            let blob = tx.blobs.get(blob_index.0).ok_or_else(|| {
                anyhow!("Failed to get blob {} from tx {}", blob_index, tx.hashed())
            })?;
            let blobs = tx.blobs.clone();
            let tx_hash = tx.hashed();

            let initial_store_contract = self.store.contract.clone();

            let state = self
                .store
                .contract
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
                if !already_settled_tx {
                    self.bus
                        .send(AutoProverEvent::FailedTx(tx_hash.clone(), e.to_string()))?;
                }
                continue;
            }
            let state = state.unwrap();

            let commitment_metadata = state;

            if initial_commitment_metadata.is_none() {
                initial_commitment_metadata = Some(commitment_metadata.clone());
            } else {
                initial_commitment_metadata = Some(
                    self.store
                        .contract
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
                tx_hash: tx_hash.clone(),
                private_input: vec![],
                blobs: blobs.clone().into(),
                index: blob_index,
                tx_ctx: Some(tx_ctx.clone()),
                tx_blob_count: blobs.len(),
            };

            match self
                .store
                .contract
                .handle(&calldata)
                .map_err(|e| anyhow!(e))
            {
                Err(e) => {
                    info!(
                        cn =% self.ctx.contract_name,
                        tx_hash =% tx.hashed(),
                        tx_height =% tx_ctx.block_height,
                        "Error while executing contract: {e}"
                    );
                    if !already_settled_tx {
                        self.bus
                            .send(AutoProverEvent::FailedTx(tx_hash.clone(), e.to_string()))?;
                    }
                    self.store.contract = initial_store_contract;
                }
                Ok(hyle_output) => {
                    info!(
                        cn =% self.ctx.contract_name,
                        tx_hash =% tx.hashed(),
                        tx_height =% tx_ctx.block_height,
                        "🔧 Executed contract: {}",
                        String::from_utf8_lossy(&hyle_output.program_outputs)
                    );
                    if !already_settled_tx {
                        self.bus.send(AutoProverEvent::SuccessTx(
                            tx_hash.clone(),
                            self.store.contract.clone(),
                        ))?;
                    }
                    if !hyle_output.success {
                        if let Some(intermediary_state) = self.store.state_history.get(&tx_hash) {
                            self.store.contract = intermediary_state.clone();
                        } else {
                            self.store.contract = initial_store_contract;
                        }
                    } else {
                        self.store
                            .state_history
                            .insert(tx_hash.clone(), self.store.contract.clone());
                    }
                }
            }

            if already_settled_tx {
                debug!(
                    cn =% self.ctx.contract_name,
                    tx_hash =% tx.hashed(),
                    tx_height =% tx_ctx.block_height,
                    "Skipping already settled tx",
                );
                continue;
            }

            calldatas.push(calldata);
        }

        if calldatas.is_empty() {
            return Ok(());
        }

        let Some(commitment_metadata) = initial_commitment_metadata else {
            return Ok(());
        };

        let node_client = self.ctx.node.clone();
        let prover = self.ctx.prover.clone();
        let contract_name = self.ctx.contract_name.clone();
        tokio::task::spawn(async move {
            let mut retries = 0;
            const MAX_RETRIES: u32 = 30;

            loop {
                debug!(
                    cn =% contract_name,
                    "Proving {} txs with commitment metadata: {:?}",
                    calldatas.len(),
                    commitment_metadata
                );
                match prover
                    .prove(commitment_metadata.clone(), calldatas.clone())
                    .await
                {
                    Ok(proof) => {
                        let tx = ProofTransaction {
                            contract_name: contract_name.clone(),
                            proof,
                        };
                        match node_client.send_tx_proof(tx).await {
                            Ok(tx_hash) => {
                                info!("✅ Proved {len} txs, proof TX hash: {tx_hash}");
                            }
                            Err(e) => {
                                error!("Failed to send proof: {e:#}");
                            }
                        }
                        break;
                    }
                    Err(e) => {
                        let should_retry =
                            e.to_string().contains("SessionCreateErr") && retries < MAX_RETRIES;
                        if should_retry {
                            warn!(
                                "Session creation error, retrying ({}/{}). {e:#}",
                                retries, MAX_RETRIES
                            );
                            retries += 1;
                            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                            continue;
                        }
                        error!("Error proving tx: {:?}", e);
                        break;
                    }
                };
            }
        });
        self.prove_supported_blob(remaining_blobs)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        bus::metrics::BusMetrics,
        node_state::{test::new_node_state, NodeState},
    };

    use super::*;
    use client_sdk::helpers::test::TxExecutorTestProver;
    use client_sdk::rest_client::test::NodeApiMockClient;
    use sdk::*;
    use std::sync::Arc;
    use tempfile::tempdir;

    #[derive(Debug, Clone, Default, BorshSerialize, BorshDeserialize)]
    struct TestContract {
        value: u32,
    }

    impl ZkContract for TestContract {
        fn execute(&mut self, calldata: &Calldata) -> sdk::RunResult {
            let (action, execution_ctx) = sdk::utils::parse_raw_calldata::<u32>(calldata)?;
            tracing::info!(
                "Executing contract (val = {}) with action: {:?}",
                self.value,
                action
            );
            self.value += action;
            if calldata.identity.0.starts_with("failing_") {
                return Err("This transaction is failing".to_string());
            }
            Ok(("ok".to_string().into_bytes(), execution_ctx, vec![]))
        }

        fn commit(&self) -> sdk::StateCommitment {
            sdk::StateCommitment(
                borsh::to_vec(self)
                    .map_err(|e| anyhow!(e))
                    .context("Failed to commit state")
                    .unwrap(),
            )
        }
    }

    impl TxExecutorHandler for TestContract {
        fn build_commitment_metadata(&self, blob: &Blob) -> Result<Vec<u8>> {
            let action = borsh::from_slice::<u32>(&blob.data.0)
                .context("Failed to parse action from blob data")?;
            if action == 66 {
                return Err(anyhow!("Order 66 is forbidden. Jedi are safe."));
            }
            borsh::to_vec(self).map_err(Into::into)
        }

        fn handle(&mut self, calldata: &Calldata) -> Result<sdk::HyleOutput> {
            let initial_state = self.commit();
            let mut res = self.execute(calldata);
            let next_state = self.commit();
            Ok(sdk::utils::as_hyle_output(
                initial_state,
                next_state,
                calldata,
                &mut res,
            ))
        }

        fn construct_state(
            _register_blob: &RegisterContractEffect,
            _metadata: &Option<Vec<u8>>,
        ) -> Result<Self> {
            Ok(Self::default())
        }
    }

    async fn setup() -> Result<(NodeState, AutoProver<TestContract>, Arc<NodeApiMockClient>)> {
        let mut node_state = new_node_state().await;
        let register = RegisterContractEffect {
            verifier: "test".into(),
            program_id: ProgramId(vec![]),
            state_commitment: TestContract::default().commit(),
            contract_name: "test".into(),
            timeout_window: Some(TimeoutWindow::Timeout(BlockHeight(5))),
        };
        node_state.handle_register_contract_effect(&register);

        let api_client = Arc::new(NodeApiMockClient::new());

        let auto_prover = new_simple_auto_prover(api_client.clone()).await?;

        Ok((node_state, auto_prover, api_client))
    }

    async fn new_simple_auto_prover(
        api_client: Arc<NodeApiMockClient>,
    ) -> Result<AutoProver<TestContract>> {
        new_buffering_auto_prover(api_client, 0, 100).await
    }

    async fn new_buffering_auto_prover(
        api_client: Arc<NodeApiMockClient>,
        buffer_blocks: u32,
        max_txs_per_proof: usize,
    ) -> Result<AutoProver<TestContract>> {
        let temp_dir = tempdir()?;
        let data_dir = temp_dir.path().to_path_buf();
        let ctx = Arc::new(AutoProverCtx {
            data_directory: data_dir,
            prover: Arc::new(TxExecutorTestProver::<TestContract>::new()),
            contract_name: ContractName("test".into()),
            node: api_client,
            default_state: TestContract::default(),
            buffer_blocks,
            max_txs_per_proof,
        });

        let bus = SharedMessageBus::new(BusMetrics::global("default".to_string()));
        AutoProver::<TestContract>::build(bus.new_handle(), ctx).await
    }

    async fn get_txs(api_client: &Arc<NodeApiMockClient>) -> Vec<Transaction> {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let mut gard = api_client.pending_proofs.lock().unwrap();
        let txs = gard.drain(..).collect::<Vec<ProofTransaction>>();
        txs.into_iter()
            .map(|t| {
                let hyle_outputs = borsh::from_slice::<Vec<HyleOutput>>(&t.proof.0)
                    .context("parsing test proof")
                    .unwrap();
                for hyle_output in &hyle_outputs {
                    tracing::info!(
                        "Initial state: {:?}, Next state: {:?}",
                        hyle_output.initial_state,
                        hyle_output.next_state
                    );
                }

                let proven_blobs = hyle_outputs
                    .into_iter()
                    .map(|hyle_output| {
                        let blob_tx_hash = hyle_output.tx_hash.clone();
                        BlobProofOutput {
                            hyle_output,
                            program_id: ProgramId(vec![]),
                            blob_tx_hash,
                            original_proof_hash: t.proof.hashed(),
                        }
                    })
                    .collect();
                VerifiedProofTransaction {
                    contract_name: t.contract_name.clone(),
                    proven_blobs,
                    proof_hash: t.proof.hashed(),
                    proof_size: t.estimate_size(),
                    proof: Some(t.proof),
                    is_recursive: false,
                }
                .into()
            })
            .collect()
    }

    fn count_hyle_outputs(proof: &Transaction) -> usize {
        if let TransactionData::VerifiedProof(VerifiedProofTransaction { proven_blobs, .. }) =
            &proof.transaction_data
        {
            proven_blobs.len()
        } else {
            tracing::info!("No Hyle outputs in this transaction");
            0
        }
    }

    fn new_blob_tx(val: u32) -> Transaction {
        // random id to have a different tx hash
        let id: usize = rand::random();
        BlobTransaction::new(
            format!("{id}@test"),
            vec![Blob {
                contract_name: "test".into(),
                data: BlobData(borsh::to_vec(&val).unwrap()),
            }],
        )
        .into()
    }

    fn new_failing_blob_tx(val: u32) -> Transaction {
        // random id to have a different tx hash
        let id: usize = rand::random();
        BlobTransaction::new(
            format!("failing_{id}@test"),
            vec![Blob {
                contract_name: "test".into(),
                data: BlobData(borsh::to_vec(&val).unwrap()),
            }],
        )
        .into()
    }

    fn read_contract_state(node_state: &NodeState) -> TestContract {
        let state = node_state
            .contracts
            .get(&"test".into())
            .unwrap()
            .state
            .clone();

        borsh::from_slice::<TestContract>(&state.0).expect("Failed to decode contract state")
    }

    #[test_log::test(tokio::test)]
    async fn test_auto_prover_basic() -> Result<()> {
        let (mut node_state, mut auto_prover, api_client) = setup().await?;

        tracing::info!("✨ Block 1");
        let block_1 = node_state.craft_block_and_handle(1, vec![new_blob_tx(1)]);

        auto_prover.handle_processed_block(block_1).await?;

        let proofs = get_txs(&api_client).await;
        assert_eq!(proofs.len(), 1);

        tracing::info!("✨ Block 2");
        node_state.craft_block_and_handle(2, proofs);

        assert_eq!(read_contract_state(&node_state).value, 1);

        tracing::info!("✨ Block 3");
        let block_3 = node_state.craft_block_and_handle(
            3,
            vec![
                new_blob_tx(3), /* this one will timeout */
                new_blob_tx(3), /* this one will timeout */
                new_blob_tx(3),
            ],
        );
        auto_prover.handle_processed_block(block_3).await?;

        // Proofs 3 won't be sent, to trigger a timeout
        let proofs_3 = get_txs(&api_client).await;
        assert_eq!(proofs_3.len(), 1);

        tracing::info!("✨ Block 4");
        let block_4 = node_state
            .craft_block_and_handle(4, vec![new_blob_tx(4), new_blob_tx(4), new_blob_tx(4)]);
        auto_prover.handle_processed_block(block_4).await?;
        let proofs_4 = get_txs(&api_client).await;
        assert_eq!(proofs_4.len(), 1);

        for i in 5..15 {
            tracing::info!("✨ Block {i}");
            let block = node_state.craft_block_and_handle(i, vec![]);
            auto_prover.handle_processed_block(block).await?;
        }

        let proofs = get_txs(&api_client).await;
        assert_eq!(proofs.len(), 2);

        let _block_11 = node_state.craft_block_and_handle(16, proofs);
        assert_eq!(read_contract_state(&node_state).value, 16);

        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn test_auto_prover_tx_failed() -> Result<()> {
        let (mut node_state, mut auto_prover, api_client) = setup().await?;

        tracing::info!("✨ Block 1");
        let block_1 = node_state.craft_block_and_handle(1, vec![new_failing_blob_tx(1)]);

        auto_prover.handle_processed_block(block_1).await?;

        let proofs = get_txs(&api_client).await;
        assert_eq!(proofs.len(), 1);

        tracing::info!("✨ Block 2");
        node_state.craft_block_and_handle(2, proofs);

        assert_eq!(read_contract_state(&node_state).value, 0);

        tracing::info!("✨ Block 3");
        let block_3 = node_state.craft_block_and_handle(3, vec![new_blob_tx(3)]);
        auto_prover.handle_processed_block(block_3).await?;

        let proofs_3 = get_txs(&api_client).await;
        assert_eq!(proofs_3.len(), 1);

        tracing::info!("✨ Block 4");
        node_state.craft_block_and_handle(4, proofs_3);

        assert_eq!(read_contract_state(&node_state).value, 3);

        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn test_auto_prover_tx_middle_failed() -> Result<()> {
        let (mut node_state, mut auto_prover, api_client) = setup().await?;

        tracing::info!("✨ Block 1");
        let block_1 = node_state.craft_block_and_handle(1, vec![new_blob_tx(1)]);

        auto_prover.handle_processed_block(block_1).await?;

        let proofs = get_txs(&api_client).await;
        assert_eq!(proofs.len(), 1);

        tracing::info!("✨ Block 2");
        node_state.craft_block_and_handle(2, proofs);

        assert_eq!(read_contract_state(&node_state).value, 1);

        tracing::info!("✨ Block 3");
        let block_3 = node_state.craft_block_and_handle(
            3,
            vec![
                new_failing_blob_tx(3),
                new_blob_tx(3),
                new_failing_blob_tx(3),
                new_failing_blob_tx(3),
                new_failing_blob_tx(3),
                new_failing_blob_tx(3),
                new_blob_tx(3),
                new_failing_blob_tx(3),
                new_failing_blob_tx(3),
            ],
        );
        auto_prover.handle_processed_block(block_3).await?;

        let proofs_3 = get_txs(&api_client).await;
        assert_eq!(proofs_3.len(), 1);

        tracing::info!("✨ Block 4");
        node_state.craft_block_and_handle(4, proofs_3);

        assert_eq!(read_contract_state(&node_state).value, 1 + 3 + 3);

        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn test_auto_prover_lot_tx_failed() -> Result<()> {
        let (mut node_state, mut auto_prover, api_client) = setup().await?;

        tracing::info!("✨ Block 1");
        let block_1 = node_state.craft_block_and_handle(
            1,
            vec![
                new_failing_blob_tx(1),
                new_failing_blob_tx(1),
                new_failing_blob_tx(1),
            ],
        );

        auto_prover.handle_processed_block(block_1).await?;

        let proofs = get_txs(&api_client).await;
        assert_eq!(proofs.len(), 1);

        tracing::info!("✨ Block 2");
        node_state.craft_block_and_handle(2, proofs);

        assert_eq!(read_contract_state(&node_state).value, 0);

        tracing::info!("✨ Block 3");
        let block_3 = node_state.craft_block_and_handle(3, vec![new_blob_tx(3)]);
        auto_prover.handle_processed_block(block_3).await?;

        let proofs_3 = get_txs(&api_client).await;
        assert_eq!(proofs_3.len(), 1);

        tracing::info!("✨ Block 4");
        node_state.craft_block_and_handle(4, proofs_3);

        assert_eq!(read_contract_state(&node_state).value, 3);

        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn test_auto_prover_tx_commitment_metadata_failed() -> Result<()> {
        let (mut node_state, mut auto_prover, api_client) = setup().await?;

        tracing::info!("✨ Block 1");
        let block_1 = node_state.craft_block_and_handle(1, vec![new_blob_tx(66)]);

        let _ = auto_prover.handle_processed_block(block_1).await;

        let proofs = get_txs(&api_client).await;
        assert_eq!(proofs.len(), 0);

        for i in 2..7 {
            tracing::info!("✨ Block {i}");
            let block = node_state.craft_block_and_handle(i, vec![]);
            auto_prover.handle_processed_block(block).await?;
        }

        tracing::info!("✨ Block 7");
        let block_7 = node_state.craft_block_and_handle(7, vec![new_blob_tx(7)]);
        auto_prover.handle_processed_block(block_7).await?;

        let proofs_7 = get_txs(&api_client).await;
        assert_eq!(proofs_7.len(), 1);

        tracing::info!("✨ Block 8");
        let block_8 = node_state.craft_block_and_handle(8, proofs_7);
        auto_prover.handle_processed_block(block_8).await?;

        assert_eq!(read_contract_state(&node_state).value, 7);

        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn test_auto_prover_catchup() -> Result<()> {
        let (mut node_state, mut auto_prover, api_client) = setup().await?;

        let block_1 = node_state.craft_block_and_handle(1, vec![new_blob_tx(1)]);
        let block_2 = node_state.craft_block_and_handle(2, vec![new_blob_tx(2)]);
        let block_3 = node_state.craft_block_and_handle(3, vec![new_blob_tx(3)]);

        auto_prover.handle_processed_block(block_1.clone()).await?;
        auto_prover.handle_processed_block(block_2.clone()).await?;
        auto_prover.handle_processed_block(block_3.clone()).await?;

        let block_4 = node_state.craft_block_and_handle(4, vec![new_blob_tx(4)]);
        auto_prover.handle_processed_block(block_4.clone()).await?;

        let proofs = get_txs(&api_client).await;
        assert_eq!(proofs.len(), 4);

        let block_5 = node_state.craft_block_and_handle(5, proofs);
        auto_prover.handle_processed_block(block_5.clone()).await?;

        assert_eq!(read_contract_state(&node_state).value, 10);

        let block_6 = node_state.craft_block_and_handle(6, vec![new_blob_tx(6)]);
        let block_7 = node_state.craft_block_and_handle(7, vec![new_blob_tx(7)]);
        let block_8 = node_state.craft_block_and_handle(8, vec![new_blob_tx(8)]);

        tracing::info!("✨ New prover catching up with blocks 6 and 7");
        api_client.set_block_height(BlockHeight(8));
        api_client.set_settled_height(BlockHeight(5));

        let mut auto_prover_catchup = new_simple_auto_prover(api_client.clone())
            .await
            .expect("Failed to create new auto prover");

        auto_prover_catchup
            .handle_processed_block(block_1.clone())
            .await?;
        auto_prover_catchup
            .handle_processed_block(block_2.clone())
            .await?;
        auto_prover_catchup
            .handle_processed_block(block_3.clone())
            .await?;
        auto_prover_catchup
            .handle_processed_block(block_4.clone())
            .await?;
        auto_prover_catchup
            .handle_processed_block(block_5.clone())
            .await?;
        auto_prover_catchup
            .handle_processed_block(block_6.clone())
            .await?;
        auto_prover_catchup
            .handle_processed_block(block_7.clone())
            .await?;
        auto_prover_catchup
            .handle_processed_block(block_8.clone())
            .await?;

        let proofs = get_txs(&api_client).await;
        assert_eq!(proofs.len(), 1); // Txs from mutliple catching blocs are batched
        let _ = node_state.craft_block_and_handle(9, proofs);

        assert_eq!(read_contract_state(&node_state).value, 10 + 6 + 7 + 8);
        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn test_auto_prover_catchup_timeout_1() -> Result<()> {
        let (mut node_state, mut auto_prover, api_client) = setup().await?;

        let block_1 = node_state.craft_block_and_handle(1, vec![new_blob_tx(1)]);
        let block_2 = node_state.craft_block_and_handle(2, vec![new_blob_tx(2)]);
        let block_3 = node_state.craft_block_and_handle(3, vec![new_blob_tx(3)]);

        auto_prover.handle_processed_block(block_1.clone()).await?;
        auto_prover.handle_processed_block(block_2.clone()).await?;
        auto_prover.handle_processed_block(block_3.clone()).await?;

        let block_4 = node_state.craft_block_and_handle(4, vec![new_blob_tx(4)]);
        auto_prover.handle_processed_block(block_4.clone()).await?;

        let proofs = get_txs(&api_client).await;
        assert_eq!(proofs.len(), 4);

        let block_5 = node_state.craft_block_and_handle(5, proofs);
        auto_prover.handle_processed_block(block_5.clone()).await?;

        assert_eq!(read_contract_state(&node_state).value, 10);

        let block_6 = node_state.craft_block_and_handle(
            6,
            vec![
                new_blob_tx(6), /* This one will timeout */
                new_blob_tx(6), /* This one will timeout */
                new_blob_tx(6),
                new_blob_tx(6),
            ],
        );

        let block_7 = node_state.craft_block_and_handle(7, vec![new_blob_tx(7)]);
        let block_8 = node_state.craft_block_and_handle(8, vec![new_blob_tx(8)]);

        let mut blocks = vec![
            block_1, block_2, block_3, block_4, block_5, block_6, block_7, block_8,
        ];
        for i in 9..20 {
            tracing::info!("✨ Block {i}");
            let block = node_state.craft_block_and_handle(i, vec![]);
            blocks.push(block);
        }

        tracing::info!("✨ New prover catching up with blocks");
        api_client.set_block_height(BlockHeight(19));
        api_client.set_settled_height(BlockHeight(5));

        let mut auto_prover_catchup = new_simple_auto_prover(api_client.clone())
            .await
            .expect("Failed to create new auto prover");

        for block in blocks {
            auto_prover_catchup.handle_processed_block(block).await?;
        }

        let proofs = get_txs(&api_client).await;
        assert_eq!(proofs.len(), 1); // Txs from mutliple catching blocs are batched
        assert_eq!(count_hyle_outputs(&proofs[0]), 4);
        let _ = node_state.craft_block_and_handle(20, proofs);

        assert_eq!(read_contract_state(&node_state).value, 10 + 6 + 6 + 7 + 8);
        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn test_auto_prover_catchup_timeout_2() -> Result<()> {
        let (mut node_state, mut auto_prover, api_client) = setup().await?;

        let block_1 = node_state.craft_block_and_handle(1, vec![new_blob_tx(1)]);
        let block_2 = node_state.craft_block_and_handle(2, vec![new_blob_tx(2)]);
        let block_3 = node_state.craft_block_and_handle(3, vec![new_blob_tx(3)]);

        auto_prover.handle_processed_block(block_1.clone()).await?;
        auto_prover.handle_processed_block(block_2.clone()).await?;
        auto_prover.handle_processed_block(block_3.clone()).await?;

        let block_4 = node_state.craft_block_and_handle(4, vec![new_blob_tx(4)]);
        auto_prover.handle_processed_block(block_4.clone()).await?;

        let proofs = get_txs(&api_client).await;
        assert_eq!(proofs.len(), 4);

        let block_5 = node_state.craft_block_and_handle(5, proofs);
        auto_prover.handle_processed_block(block_5.clone()).await?;

        assert_eq!(read_contract_state(&node_state).value, 10);

        let block_6 = node_state.craft_block_and_handle(
            6,
            vec![
                new_blob_tx(6), /* This one will timeout */
                new_blob_tx(6), /* This one will timeout */
                new_blob_tx(6), /* This one will timeout in first catching block */
                new_blob_tx(6),
            ],
        );

        let block_7 = node_state.craft_block_and_handle(7, vec![new_blob_tx(7)]);
        let block_8 = node_state.craft_block_and_handle(8, vec![new_blob_tx(8)]);

        let mut blocks = vec![
            block_1, block_2, block_3, block_4, block_5, block_6, block_7, block_8,
        ];
        let stop_height = 22;
        for i in 9..stop_height {
            tracing::info!("✨ Block {i}");
            let block = node_state.craft_block_and_handle(i, vec![]);
            blocks.push(block);
        }

        tracing::info!("✨ New prover catching up");
        api_client.set_block_height(BlockHeight(stop_height - 1));
        api_client.set_settled_height(BlockHeight(5));

        let mut auto_prover_catchup = new_simple_auto_prover(api_client.clone())
            .await
            .expect("Failed to create new auto prover");

        for block in blocks {
            auto_prover_catchup.handle_processed_block(block).await?;
        }

        let proofs = get_txs(&api_client).await;
        assert_eq!(proofs.len(), 1);
        assert_eq!(count_hyle_outputs(&proofs[0]), 3);
        let _ = node_state.craft_block_and_handle(stop_height, proofs);

        assert_eq!(read_contract_state(&node_state).value, 10 + 6 + 7 + 8);
        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn test_auto_prover_catchup_timeout_multiple_blocks() -> Result<()> {
        let (mut node_state, mut auto_prover, api_client) = setup().await?;

        let block_1 = node_state.craft_block_and_handle(1, vec![new_blob_tx(1)]);
        let block_2 = node_state.craft_block_and_handle(2, vec![new_blob_tx(2)]);
        let block_3 = node_state.craft_block_and_handle(3, vec![new_blob_tx(3)]);

        auto_prover.handle_processed_block(block_1.clone()).await?;
        auto_prover.handle_processed_block(block_2.clone()).await?;
        auto_prover.handle_processed_block(block_3.clone()).await?;

        let block_4 = node_state.craft_block_and_handle(4, vec![new_blob_tx(4)]);
        auto_prover.handle_processed_block(block_4.clone()).await?;

        let proofs = get_txs(&api_client).await;
        assert_eq!(proofs.len(), 4);

        let block_5 = node_state.craft_block_and_handle(5, proofs);
        auto_prover.handle_processed_block(block_5.clone()).await?;

        assert_eq!(read_contract_state(&node_state).value, 10);

        let block_6 =
            node_state.craft_block_and_handle(6, vec![new_blob_tx(6) /* This one will timeout */]);

        let block_7 =
            node_state.craft_block_and_handle(7, vec![new_blob_tx(7) /* This one will timeout */]);
        let block_8 =
            node_state.craft_block_and_handle(8, vec![new_blob_tx(8) /* This one will timeout */]);
        let block_9 = node_state.craft_block_and_handle(9, vec![new_blob_tx(9)]);
        let block_10 = node_state.craft_block_and_handle(10, vec![new_blob_tx(10)]);

        let mut blocks = vec![
            block_1, block_2, block_3, block_4, block_5, block_6, block_7, block_8, block_9,
            block_10,
        ];
        let stop_height = 24;
        for i in 11..stop_height {
            tracing::info!("✨ Block {i}");
            let block = node_state.craft_block_and_handle(i, vec![]);
            blocks.push(block);
        }

        tracing::info!("✨ New prover catching up");
        api_client.set_block_height(BlockHeight(stop_height - 1));
        api_client.set_settled_height(BlockHeight(5));

        let mut auto_prover_catchup = new_simple_auto_prover(api_client.clone())
            .await
            .expect("Failed to create new auto prover");

        for block in blocks {
            auto_prover_catchup.handle_processed_block(block).await?;
        }

        let proofs = get_txs(&api_client).await;
        assert_eq!(proofs.len(), 1); // Txs from mutliple catching blocs are batched
        assert_eq!(count_hyle_outputs(&proofs[0]), 2);
        let _ = node_state.craft_block_and_handle(stop_height, proofs);

        assert_eq!(read_contract_state(&node_state).value, 10 + 9 + 10);
        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn test_auto_prover_catchup_timeout_between_settled() -> Result<()> {
        let (mut node_state, mut auto_prover, api_client) = setup().await?;

        let block_1 = node_state.craft_block_and_handle(1, vec![new_blob_tx(1)]);
        let block_2 = node_state.craft_block_and_handle(2, vec![new_blob_tx(2)]);
        let block_3 = node_state.craft_block_and_handle(3, vec![new_blob_tx(3)]);

        auto_prover.handle_processed_block(block_1.clone()).await?;
        auto_prover.handle_processed_block(block_2.clone()).await?;
        auto_prover.handle_processed_block(block_3.clone()).await?;

        let block_4 = node_state.craft_block_and_handle(4, vec![new_blob_tx(4)]);
        auto_prover.handle_processed_block(block_4.clone()).await?;

        let proofs = get_txs(&api_client).await;
        assert_eq!(proofs.len(), 4);

        let block_5 = node_state.craft_block_and_handle(5, proofs);
        auto_prover.handle_processed_block(block_5.clone()).await?;

        assert_eq!(read_contract_state(&node_state).value, 10);

        let block_6 =
            node_state.craft_block_and_handle(6, vec![new_blob_tx(6) /* This one will timeout */]);

        let block_7 =
            node_state.craft_block_and_handle(7, vec![new_blob_tx(7) /* This one will timeout */]);
        let block_8 =
            node_state.craft_block_and_handle(8, vec![new_blob_tx(8) /* This one will timeout */]);
        let block_9 = node_state.craft_block_and_handle(9, vec![new_blob_tx(9)]);
        let block_10 = node_state.craft_block_and_handle(10, vec![new_blob_tx(10)]);

        let mut blocks = vec![
            block_1, block_2, block_3, block_4, block_5, block_6, block_7, block_8, block_9,
            block_10,
        ];
        let stop_height = 24;
        for i in 11..stop_height {
            tracing::info!("✨ Block {i}");
            let block = node_state.craft_block_and_handle(i, vec![]);
            blocks.push(block);
        }

        for i in 6..stop_height {
            tracing::info!("♻️ Handle block {}", i);
            auto_prover
                .handle_processed_block(blocks[i as usize - 1].clone())
                .await?;
        }
        let proofs = get_txs(&api_client).await;
        let block_24 = node_state.craft_block_and_handle(stop_height, proofs);
        let block_25 = node_state.craft_block_and_handle(stop_height + 1, vec![new_blob_tx(25)]);
        blocks.push(block_24);
        blocks.push(block_25);

        tracing::info!("✨ New prover catching up");
        api_client.set_block_height(BlockHeight(stop_height + 1));
        api_client.set_settled_height(BlockHeight(stop_height));

        let mut auto_prover_catchup = new_simple_auto_prover(api_client.clone())
            .await
            .expect("Failed to create new auto prover");

        for block in blocks {
            auto_prover_catchup.handle_processed_block(block).await?;
        }

        let proofs = get_txs(&api_client).await;
        assert_eq!(proofs.len(), 1);
        let _ = node_state.craft_block_and_handle(stop_height + 2, proofs);

        assert_eq!(read_contract_state(&node_state).value, 10 + 9 + 10 + 25);
        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn test_auto_prover_catchup_first_txs_timeout() -> Result<()> {
        let (mut node_state, _, api_client) = setup().await?;

        let block_1 =
            node_state.craft_block_and_handle(1, vec![new_blob_tx(1) /* This one will timeout */]);
        let block_2 =
            node_state.craft_block_and_handle(2, vec![new_blob_tx(2) /* This one will timeout */]);
        let block_3 =
            node_state.craft_block_and_handle(3, vec![new_blob_tx(3) /* This one will timeout */]);

        let mut blocks = vec![block_1, block_2, block_3];
        let stop_height = 20;
        for i in 4..=stop_height {
            tracing::info!("✨ Block {i}");
            let block = node_state.craft_block_and_handle(i, vec![]);
            blocks.push(block);
        }

        tracing::info!("✨ New prover catching up with blocks");
        api_client.set_block_height(BlockHeight(stop_height));
        api_client.set_settled_height(BlockHeight(stop_height - 1));

        let mut auto_prover_catchup = new_simple_auto_prover(api_client.clone())
            .await
            .expect("Failed to create new auto prover");

        for block in blocks {
            auto_prover_catchup.handle_processed_block(block).await?;
        }

        let proofs = get_txs(&api_client).await;
        assert_eq!(proofs.len(), 0);

        tracing::info!("✨ Block 21");
        let block_21 = node_state.craft_block_and_handle(21, vec![new_blob_tx(21)]);

        auto_prover_catchup
            .handle_processed_block(block_21.clone())
            .await?;
        let proofs = get_txs(&api_client).await;
        assert_eq!(proofs.len(), 1);
        assert_eq!(count_hyle_outputs(&proofs[0]), 1);
        tracing::info!("✨ Block 22");
        let _ = node_state.craft_block_and_handle(22, proofs);

        assert_eq!(read_contract_state(&node_state).value, 21);
        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn test_auto_prover_buffer_2_blocks() -> Result<()> {
        let (mut node_state, _, api_client) = setup().await?;

        let block_1 = node_state.craft_block_and_handle(1, vec![new_blob_tx(1)]);
        let block_2 = node_state.craft_block_and_handle(2, vec![new_blob_tx(2)]);
        let block_3 = node_state.craft_block_and_handle(3, vec![new_blob_tx(3)]);
        let block_4 = node_state.craft_block_and_handle(4, vec![new_blob_tx(4)]);

        let blocks = vec![block_1, block_2, block_3, block_4];

        let mut auto_prover = new_buffering_auto_prover(api_client.clone(), 2, 100).await?;

        for block in blocks.clone() {
            auto_prover.handle_processed_block(block).await?;
        }

        let proofs = get_txs(&api_client).await;
        assert_eq!(proofs.len(), 1);
        tracing::info!("✨ Block 5");
        let block_5 = node_state.craft_block_and_handle(5, proofs);
        auto_prover.handle_processed_block(block_5).await?;

        assert_eq!(read_contract_state(&node_state).value, 1 + 2 + 3);

        let block_6 = node_state.craft_block_and_handle(6, vec![new_blob_tx(6)]);
        auto_prover.handle_processed_block(block_6).await?;
        let proofs_6 = get_txs(&api_client).await;
        assert_eq!(proofs_6.len(), 1);
        tracing::info!("✨ Block 7");
        let _ = node_state.craft_block_and_handle(7, proofs_6);
        assert_eq!(read_contract_state(&node_state).value, 1 + 2 + 3 + 4 + 6);

        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn test_auto_prover_buffer_2_txs() -> Result<()> {
        let (mut node_state, _, api_client) = setup().await?;

        let block_1 = node_state.craft_block_and_handle(1, vec![new_blob_tx(1)]);
        let block_2 = node_state.craft_block_and_handle(2, vec![new_blob_tx(2)]);
        let block_3 = node_state.craft_block_and_handle(3, vec![new_blob_tx(3)]);
        let block_4 = node_state.craft_block_and_handle(4, vec![new_blob_tx(4)]);

        let blocks = vec![block_1, block_2, block_3, block_4];

        let mut auto_prover = new_buffering_auto_prover(api_client.clone(), 100, 3).await?;

        for block in blocks.clone() {
            auto_prover.handle_processed_block(block).await?;
        }

        let proofs = get_txs(&api_client).await;
        assert_eq!(proofs.len(), 1);
        tracing::info!("✨ Block 5");
        let block_5 = node_state.craft_block_and_handle(5, proofs);
        auto_prover.handle_processed_block(block_5).await?;

        assert_eq!(read_contract_state(&node_state).value, 1 + 2 + 3);

        let block_6 = node_state.craft_block_and_handle(6, vec![new_blob_tx(6), new_blob_tx(6)]);
        auto_prover.handle_processed_block(block_6).await?;
        let proofs_6 = get_txs(&api_client).await;
        assert_eq!(proofs_6.len(), 1);
        tracing::info!("✨ Block 7");
        let _ = node_state.craft_block_and_handle(7, proofs_6);
        assert_eq!(
            read_contract_state(&node_state).value,
            1 + 2 + 3 + 4 + 6 + 6
        );

        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn test_auto_prover_buffer_max_txs_per_proof() -> Result<()> {
        let (mut node_state, _, api_client) = setup().await?;

        let block_1 = node_state.craft_block_and_handle(1, vec![new_blob_tx(1)]);
        let block_2 = node_state.craft_block_and_handle(2, vec![new_blob_tx(2)]);
        let block_3 = node_state.craft_block_and_handle(3, vec![new_blob_tx(3)]);
        let block_4 = node_state.craft_block_and_handle(4, vec![new_blob_tx(4)]);

        let blocks = vec![block_1, block_2, block_3, block_4];

        let mut auto_prover = new_buffering_auto_prover(api_client.clone(), 100, 2).await?;

        for block in blocks.clone() {
            auto_prover.handle_processed_block(block).await?;
        }

        let proofs = get_txs(&api_client).await;
        assert_eq!(proofs.len(), 2);
        tracing::info!("✨ Block 5");
        let block_5 = node_state.craft_block_and_handle(5, proofs);
        auto_prover.handle_processed_block(block_5).await?;

        assert_eq!(read_contract_state(&node_state).value, 1 + 2 + 3 + 4);

        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn test_auto_prover_buffer_one_block_max_txs_per_proof() -> Result<()> {
        let (mut node_state, _, api_client) = setup().await?;

        let block = node_state.craft_block_and_handle(
            1,
            vec![
                new_blob_tx(1),
                new_blob_tx(2),
                new_blob_tx(3),
                new_blob_tx(4),
            ],
        );

        let mut auto_prover = new_buffering_auto_prover(api_client.clone(), 100, 2).await?;

        auto_prover.handle_processed_block(block).await?;

        let proofs = get_txs(&api_client).await;
        assert_eq!(proofs.len(), 2);
        tracing::info!("✨ Block 5");
        let block_5 = node_state.craft_block_and_handle(5, proofs);
        auto_prover.handle_processed_block(block_5).await?;

        assert_eq!(read_contract_state(&node_state).value, 1 + 2 + 3 + 4);

        Ok(())
    }
}

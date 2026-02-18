use crate::node_state::{test::new_node_state, NodeState};

use super::*;
use crate::modules::contract_listener::{ContractListenerEvent, ContractTx};
use client_sdk::helpers::test::TxExecutorTestProver;
use client_sdk::rest_client::test::NodeApiMockClient;
use rand::{rng, Rng};
use sdk::api::TransactionStatusDb;
use sdk::*;
use std::collections::VecDeque;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Duration;
use tempfile::TempDir;

#[derive(Debug, Clone, Default, BorshSerialize, BorshDeserialize)]
struct TestContract {
    value: u32,
}

impl sdk::FullStateRevert for TestContract {}

impl ZkContract for TestContract {
    fn execute(&mut self, calldata: &Calldata) -> sdk::RunResult {
        let (action, execution_ctx) = sdk::utils::parse_raw_calldata::<u32>(calldata)?;
        tracing::info!(
            tx_hash =% calldata.tx_hash,
            "Executing contract (val = {}) with action: {:?}",
            self.value,
            action,
        );
        self.value += action;
        if calldata.identity.0.starts_with("failing_") {
            return Err("This transaction is failing".to_string());
        }
        if action == 77 {
            return Ok((
                "ok".to_string().into_bytes(),
                execution_ctx,
                vec![sdk::OnchainEffect::UpdateContractProgramId(
                    "test".into(),
                    ProgramId(vec![7, 7, 7, 7]),
                )],
            ));
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
    type Contract = TestContract;

    fn build_commitment_metadata(&self, blob: &Blob) -> Result<Vec<u8>> {
        let action = borsh::from_slice::<u32>(&blob.data.0)
            .context("Failed to parse action from blob data")?;
        if action == 66 {
            return Err(anyhow!("Order 66 is forbidden. Jedi are safe."));
        }
        borsh::to_vec(self).map_err(Into::into)
    }

    fn handle(&mut self, calldata: &Calldata) -> Result<sdk::HyliOutput> {
        let initial_state = ZkContract::commit(self);
        let mut res = self.execute(calldata);
        let next_state = ZkContract::commit(self);
        Ok(sdk::utils::as_hyli_output(
            initial_state,
            next_state,
            calldata,
            &mut res,
        ))
    }

    fn construct_state(
        _: &sdk::ContractName,
        _: &sdk::Contract,
        _: &Option<Vec<u8>>,
    ) -> Result<Self> {
        Ok(Self::default())
    }
    fn get_state_commitment(&self) -> StateCommitment {
        self.commit()
    }
}

type TestAutoProver = AutoProver<TestContract, TxExecutorTestProver<TestContract>>;
static TEST_TEMP_DIRS: LazyLock<Mutex<Vec<TempDir>>> = LazyLock::new(|| Mutex::new(Vec::new()));

async fn setup_with_timeout(
    timeout: u64,
) -> Result<(NodeState, TestAutoProver, Arc<NodeApiMockClient>)> {
    let mut node_state = new_node_state().await;
    let register = RegisterContractEffect {
        verifier: "test".into(),
        program_id: ProgramId(vec![]),
        state_commitment: TestContract::default().commit(),
        contract_name: "test".into(),
        timeout_window: Some(TimeoutWindow::timeout(
            BlockHeight(timeout),
            BlockHeight(timeout),
        )),
    };
    node_state.handle_register_contract_effect(&register);

    let api_client = Arc::new(NodeApiMockClient::new());
    api_client.add_contract(Contract {
        name: "test".into(),
        state: TestContract::default().commit(),
        verifier: "test".into(),
        program_id: ProgramId(vec![]),
        timeout_window: TimeoutWindow::timeout(BlockHeight(timeout), BlockHeight(timeout)),
    });

    let mut auto_prover = new_simple_auto_prover(api_client.clone()).await?;
    auto_prover.ensure_backfill_complete().await?;

    Ok((node_state, auto_prover, api_client))
}

async fn setup() -> Result<(NodeState, TestAutoProver, Arc<NodeApiMockClient>)> {
    setup_with_timeout(5).await
}

async fn new_simple_auto_prover(api_client: Arc<NodeApiMockClient>) -> Result<TestAutoProver> {
    new_buffering_auto_prover(api_client, 0, 100).await
}

async fn new_buffering_auto_prover(
    api_client: Arc<NodeApiMockClient>,
    tx_buffer_size: usize,
    max_txs_per_proof: usize,
) -> Result<TestAutoProver> {
    let temp_dir = tempfile::Builder::new()
        .prefix("hyli-autoprover-tests-")
        .tempdir()?;
    let data_dir = temp_dir.path().to_path_buf();
    let ctx = Arc::new(AutoProverCtx {
        data_directory: data_dir,
        prover: Arc::new(TxExecutorTestProver::<TestContract>::new()),
        contract_name: ContractName("test".into()),
        api: None,
        node: api_client,
        default_state: TestContract::default(),
        tx_buffer_size,
        max_txs_per_proof,
        tx_working_window_size: max_txs_per_proof,
        idle_flush_interval: Duration::from_secs(5),
    });

    let bus = SharedMessageBus::new();
    let auto_prover = TestAutoProver::build(bus.new_handle(), ctx).await?;
    TEST_TEMP_DIRS
        .lock()
        .expect("test temp dir registry lock poisoned")
        .push(temp_dir);
    Ok(auto_prover)
}

async fn get_txs(api_client: &Arc<NodeApiMockClient>) -> Vec<Transaction> {
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let mut gard = api_client.pending_proofs.lock().unwrap();
    let txs = gard.drain(..).collect::<Vec<ProofTransaction>>();
    txs.into_iter()
        .map(|t| {
            let hyli_outputs = borsh::from_slice::<Vec<HyliOutput>>(&t.proof.0)
                .context("parsing test proof")
                .unwrap();
            for hyli_output in &hyli_outputs {
                tracing::info!(
                    "Initial state: {:?}, Next state: {:?}",
                    hyli_output.initial_state,
                    hyli_output.next_state
                );
            }

            let proven_blobs = hyli_outputs
                .into_iter()
                .map(|hyli_output| {
                    let blob_tx_hash = hyli_output.tx_hash.clone();
                    BlobProofOutput {
                        hyli_output,
                        program_id: ProgramId(vec![]),
                        verifier: "test".into(),
                        blob_tx_hash,
                        original_proof_hash: t.proof.hashed(),
                    }
                })
                .collect();
            VerifiedProofTransaction {
                contract_name: t.contract_name.clone(),
                program_id: t.program_id.clone(),
                verifier: t.verifier.clone(),
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

fn count_hyli_outputs(proof: &Transaction) -> usize {
    if let TransactionData::VerifiedProof(VerifiedProofTransaction { proven_blobs, .. }) =
        &proof.transaction_data
    {
        proven_blobs.len()
    } else {
        tracing::info!("No Hyli outputs in this transaction");
        0
    }
}

fn new_blob_tx(val: u32) -> Transaction {
    // random id to have a different tx hash
    let mut rng = rng();
    let id: usize = rng.random::<u64>() as usize;
    let tx = BlobTransaction::new(
        format!("{id}@test"),
        vec![Blob {
            contract_name: "test".into(),
            data: BlobData(borsh::to_vec(&val).unwrap()),
        }],
    );
    tracing::info!(
        "📦️ Created new blob tx: {} with value: {}",
        tx.hashed(),
        val
    );
    tx.into()
}

fn new_failing_blob_tx(val: u32) -> Transaction {
    // random id to have a different tx hash
    let mut rng = rng();
    let id: usize = rng.random::<u64>() as usize;
    let tx = BlobTransaction::new(
        format!("failing_{id}@test"),
        vec![Blob {
            contract_name: "test".into(),
            data: BlobData(borsh::to_vec(&val).unwrap()),
        }],
    );
    tracing::info!(
        "📦️ Created new failing blob tx: {} with value: {}",
        tx.hashed(),
        val
    );
    tx.into()
}

fn new_blob_tx_with_values(values: &[u32]) -> BlobTransaction {
    let mut rng = rng();
    let id: usize = rng.random::<u64>() as usize;
    let blobs = values
        .iter()
        .map(|v| Blob {
            contract_name: "test".into(),
            data: BlobData(borsh::to_vec(v).unwrap()),
        })
        .collect::<Vec<_>>();
    BlobTransaction::new(format!("{id}@test"), blobs)
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

async fn setup_buffered(
    tx_buffer_size: usize,
    max_txs_per_proof: usize,
) -> Result<(NodeState, TestAutoProver, Arc<NodeApiMockClient>)> {
    let mut node_state = new_node_state().await;
    let register = RegisterContractEffect {
        verifier: "test".into(),
        program_id: ProgramId(vec![]),
        state_commitment: TestContract::default().commit(),
        contract_name: "test".into(),
        timeout_window: Some(TimeoutWindow::default()),
    };
    node_state.handle_register_contract_effect(&register);
    let api_client = Arc::new(NodeApiMockClient::new());
    api_client.add_contract(Contract {
        name: "test".into(),
        state: TestContract::default().commit(),
        verifier: "test".into(),
        program_id: ProgramId(vec![]),
        timeout_window: TimeoutWindow::default(),
    });
    let mut auto_prover =
        new_buffering_auto_prover(api_client.clone(), tx_buffer_size, max_txs_per_proof).await?;
    auto_prover.ensure_backfill_complete().await?;
    Ok((node_state, auto_prover, api_client))
}

async fn setup_catching_with_timeout(
    timeout: u64,
) -> Result<(NodeState, TestAutoProver, Arc<NodeApiMockClient>)> {
    let mut node_state = new_node_state().await;
    let register = RegisterContractEffect {
        verifier: "test".into(),
        program_id: ProgramId(vec![]),
        state_commitment: TestContract::default().commit(),
        contract_name: "test".into(),
        timeout_window: Some(TimeoutWindow::timeout(
            BlockHeight(timeout),
            BlockHeight(timeout),
        )),
    };
    node_state.handle_register_contract_effect(&register);
    let api_client = Arc::new(NodeApiMockClient::new());
    api_client.add_contract(Contract {
        name: "test".into(),
        state: TestContract::default().commit(),
        verifier: "test".into(),
        program_id: ProgramId(vec![]),
        timeout_window: TimeoutWindow::timeout(BlockHeight(timeout), BlockHeight(timeout)),
    });
    let auto_prover = new_simple_auto_prover(api_client.clone()).await?;
    Ok((node_state, auto_prover, api_client))
}

async fn process_block(
    node_state: &mut NodeState,
    auto_prover: &mut TestAutoProver,
    api_client: &Arc<NodeApiMockClient>,
    height: u64,
    txs: Vec<Transaction>,
) -> Result<Vec<Transaction>> {
    let block = node_state.craft_block_and_handle(height, txs);
    auto_prover.handle_node_state_block(block).await?;
    let proofs = get_txs(api_client).await;
    for proof in &proofs {
        let outputs = count_hyli_outputs(proof);
        assert!(outputs > 0);
        assert!(outputs <= auto_prover.ctx.max_txs_per_proof);
    }
    Ok(proofs)
}

async fn run_blocks_only(
    node_state: &mut NodeState,
    auto_prover: &mut TestAutoProver,
    api_client: &Arc<NodeApiMockClient>,
    start_height: u64,
    blob_blocks: Vec<Vec<Transaction>>,
) -> Result<(u64, Vec<Transaction>)> {
    let mut height = start_height;
    let mut pending = vec![];
    for txs in blob_blocks {
        pending.extend(process_block(node_state, auto_prover, api_client, height, txs).await?);
        height += 1;
    }

    Ok((height, pending))
}

async fn settle_pending_proofs(
    node_state: &mut NodeState,
    auto_prover: &mut TestAutoProver,
    api_client: &Arc<NodeApiMockClient>,
    start_height: u64,
) -> Result<u64> {
    settle_pending_proofs_with_delay(node_state, auto_prover, api_client, start_height, 0).await
}

async fn settle_pending_proofs_with_delay(
    node_state: &mut NodeState,
    auto_prover: &mut TestAutoProver,
    api_client: &Arc<NodeApiMockClient>,
    start_height: u64,
    empty_blocks_before_settle: u64,
) -> Result<u64> {
    let height = start_height;
    let pending = get_txs(api_client).await;
    settle_with_pending_proofs(
        node_state,
        auto_prover,
        api_client,
        height,
        pending,
        empty_blocks_before_settle,
    )
    .await
}

async fn settle_with_pending_proofs(
    node_state: &mut NodeState,
    auto_prover: &mut TestAutoProver,
    api_client: &Arc<NodeApiMockClient>,
    mut height: u64,
    mut pending: Vec<Transaction>,
    empty_blocks_before_settle: u64,
) -> Result<u64> {
    for _ in 0..empty_blocks_before_settle {
        pending.extend(process_block(node_state, auto_prover, api_client, height, vec![]).await?);
        height += 1;
    }

    while !pending.is_empty() {
        let current = std::mem::take(&mut pending);
        pending.extend(process_block(node_state, auto_prover, api_client, height, current).await?);
        height += 1;
    }

    Ok(height)
}

async fn settle_with_pending_proofs_deterministic_counts(
    node_state: &mut NodeState,
    auto_prover: &mut TestAutoProver,
    api_client: &Arc<NodeApiMockClient>,
    mut height: u64,
    pending: Vec<Transaction>,
) -> Result<(u64, Vec<usize>)> {
    let mut queue: VecDeque<Transaction> = pending.into();
    let mut counts = vec![];

    while let Some(proof) = queue.pop_front() {
        counts.push(count_hyli_outputs(&proof));
        let next = process_block(node_state, auto_prover, api_client, height, vec![proof]).await?;
        queue.extend(next);
        height += 1;
    }

    Ok((height, counts))
}

async fn run_blocks_and_settle(
    node_state: &mut NodeState,
    auto_prover: &mut TestAutoProver,
    api_client: &Arc<NodeApiMockClient>,
    start_height: u64,
    blob_blocks: Vec<Vec<Transaction>>,
    empty_blocks_before_settle: u64,
) -> Result<u64> {
    let (next_height, pending) = run_blocks_only(
        node_state,
        auto_prover,
        api_client,
        start_height,
        blob_blocks,
    )
    .await?;
    settle_with_pending_proofs(
        node_state,
        auto_prover,
        api_client,
        next_height,
        pending,
        empty_blocks_before_settle,
    )
    .await
}

impl<Contract, Prover> AutoProver<Contract, Prover>
where
    Contract: TxExecutorHandler + Debug + Clone + Send + Sync + 'static,
    Prover: ClientSdkProver<Vec<Calldata>> + Send + Sync + 'static,
{
    async fn ensure_backfill_complete(&mut self) -> Result<()> {
        if self.catching_up {
            self.handle_contract_listener_event(
                crate::modules::contract_listener::ContractListenerEvent::BackfillComplete(
                    self.ctx.contract_name.clone(),
                ),
            )
            .await?;
        }
        Ok(())
    }

    async fn handle_node_state_block(&mut self, block: NodeStateBlock) -> Result<()> {
        for (tx_id, event) in &block.stateful_events.events {
            match event {
                StatefulEvent::SequencedTx(tx, tx_ctx) => {
                    self.handle_contract_listener_event(ContractListenerEvent::SequencedTx(
                        ContractTx {
                            tx_id: tx_id.clone(),
                            tx: tx.clone(),
                            tx_ctx: tx_ctx.clone(),
                            status: TransactionStatusDb::Sequenced,
                        },
                    ))
                    .await?;
                }
                StatefulEvent::SettledTx(unsettled) => {
                    self.handle_contract_listener_event(ContractListenerEvent::SettledTx(
                        ContractTx {
                            tx_id: unsettled.tx_id.clone(),
                            tx: unsettled.tx.clone(),
                            tx_ctx: unsettled.tx_context.clone(),
                            status: TransactionStatusDb::Success,
                        },
                    ))
                    .await?;
                }
                StatefulEvent::FailedTx(unsettled) => {
                    self.handle_contract_listener_event(ContractListenerEvent::SettledTx(
                        ContractTx {
                            tx_id: unsettled.tx_id.clone(),
                            tx: unsettled.tx.clone(),
                            tx_ctx: unsettled.tx_context.clone(),
                            status: TransactionStatusDb::Failure,
                        },
                    ))
                    .await?;
                }
                StatefulEvent::TimedOutTx(unsettled) => {
                    self.handle_contract_listener_event(ContractListenerEvent::SettledTx(
                        ContractTx {
                            tx_id: unsettled.tx_id.clone(),
                            tx: unsettled.tx.clone(),
                            tx_ctx: unsettled.tx_context.clone(),
                            status: TransactionStatusDb::TimedOut,
                        },
                    ))
                    .await?;
                }
                StatefulEvent::ContractUpdate(contract_name, contract) => {
                    if *contract_name == self.ctx.contract_name {
                        self.ensure_prover_available(
                            &contract.program_id,
                            &format!(
                                "Program ID changed for contract {} during tests",
                                contract_name
                            ),
                        )
                        .await?;
                    }
                }
                StatefulEvent::ContractDelete(..) | StatefulEvent::ContractRegistration(..) => {}
            }
        }

        self.flush_pending(true).await?;
        Ok(())
    }
}

/// Verifies the happy path for sequencing, proving, and settling simple transactions.
#[test_log::test(tokio::test)]
async fn test_auto_prover_simple() -> Result<()> {
    let (mut node_state, mut auto_prover, api_client) = setup().await?;
    run_blocks_and_settle(
        &mut node_state,
        &mut auto_prover,
        &api_client,
        1,
        vec![vec![new_blob_tx(2), new_blob_tx(3)]],
        0,
    )
    .await?;
    assert_eq!(read_contract_state(&node_state).value, 5);
    Ok(())
}

/// Verifies timeout handling and replay behavior after skipped proof settlement.
#[test_log::test(tokio::test)]
async fn test_auto_prover_basic() -> Result<()> {
    let (mut node_state, mut auto_prover, api_client) = setup_with_timeout(1).await?;
    run_blocks_and_settle(
        &mut node_state,
        &mut auto_prover,
        &api_client,
        1,
        vec![vec![new_blob_tx(2), new_blob_tx(5)]],
        2,
    )
    .await?;
    assert_eq!(read_contract_state(&node_state).value, 0);
    Ok(())
}

/// Verifies that a failing transaction does not update contract state and later success still applies.
#[test_log::test(tokio::test)]
async fn test_auto_prover_tx_failed() -> Result<()> {
    let (mut node_state, mut auto_prover, api_client) = setup().await?;
    run_blocks_and_settle(
        &mut node_state,
        &mut auto_prover,
        &api_client,
        1,
        vec![vec![new_failing_blob_tx(4), new_blob_tx(2)]],
        0,
    )
    .await?;
    assert_eq!(read_contract_state(&node_state).value, 2);
    Ok(())
}

/// Verifies mixed success and failures within one block keep valid state transitions.
#[test_log::test(tokio::test)]
async fn test_auto_prover_tx_middle_failed() -> Result<()> {
    let (mut node_state, mut auto_prover, api_client) = setup().await?;
    run_blocks_and_settle(
        &mut node_state,
        &mut auto_prover,
        &api_client,
        1,
        vec![vec![new_blob_tx(1), new_failing_blob_tx(2), new_blob_tx(3)]],
        0,
    )
    .await?;
    assert_eq!(read_contract_state(&node_state).value, 4);
    Ok(())
}

/// Verifies failure after earlier success in the same settlement window does not corrupt buffered flow.
#[test_log::test(tokio::test)]
async fn test_auto_prover_tx_failed_after_success_in_same_block() -> Result<()> {
    let (mut node_state, mut auto_prover, api_client) = setup().await?;
    run_blocks_and_settle(
        &mut node_state,
        &mut auto_prover,
        &api_client,
        1,
        vec![vec![new_blob_tx(1), new_failing_blob_tx(2), new_blob_tx(3)]],
        0,
    )
    .await?;
    assert_eq!(read_contract_state(&node_state).value, 4);
    Ok(())
}

/// Verifies many failing transactions are ignored and later successful transactions are still proven.
#[test_log::test(tokio::test)]
async fn test_auto_prover_lot_tx_failed() -> Result<()> {
    let (mut node_state, mut auto_prover, api_client) = setup().await?;
    let mut txs = (0..8)
        .map(|i| new_failing_blob_tx(i + 1))
        .collect::<Vec<_>>();
    txs.push(new_blob_tx(99));
    run_blocks_and_settle(
        &mut node_state,
        &mut auto_prover,
        &api_client,
        1,
        vec![txs],
        0,
    )
    .await?;
    assert_eq!(read_contract_state(&node_state).value, 99);
    Ok(())
}

/// Verifies instant failure paths when a transaction includes an invalid contract blob.
#[test_log::test(tokio::test)]
async fn test_auto_prover_instant_failed() -> Result<()> {
    let (mut node_state, mut auto_prover, api_client) = setup().await?;
    run_blocks_and_settle(
        &mut node_state,
        &mut auto_prover,
        &api_client,
        1,
        vec![vec![new_blob_tx_with_values(&[66]).into(), new_blob_tx(4)]],
        0,
    )
    .await?;
    assert_eq!(read_contract_state(&node_state).value, 0);
    Ok(())
}

/// Verifies duplicated transactions are not re-proven and catch-up remains consistent.
#[test_log::test(tokio::test)]
async fn test_auto_prover_duplicated_tx() -> Result<()> {
    let (mut node_state, mut auto_prover, api_client) = setup().await?;
    let tx = new_blob_tx(2);
    run_blocks_and_settle(
        &mut node_state,
        &mut auto_prover,
        &api_client,
        1,
        vec![vec![tx.clone(), tx]],
        0,
    )
    .await?;
    assert_eq!(read_contract_state(&node_state).value, 2);
    Ok(())
}

/// Verifies proving logic handles multiple blobs within a single transaction correctly.
#[test_log::test(tokio::test)]
async fn test_auto_prover_two_blobs_in_tx() -> Result<()> {
    let (mut node_state, mut auto_prover, api_client) = setup().await?;
    run_blocks_and_settle(
        &mut node_state,
        &mut auto_prover,
        &api_client,
        1,
        vec![vec![new_blob_tx_with_values(&[2, 3]).into()]],
        0,
    )
    .await?;
    assert_eq!(read_contract_state(&node_state).value, 5);
    Ok(())
}

/// Verifies commitment metadata failures block proof generation until valid transactions arrive.
#[test_log::test(tokio::test)]
async fn test_auto_prover_tx_commitment_metadata_failed() -> Result<()> {
    let (mut node_state, mut auto_prover, api_client) = setup().await?;
    run_blocks_and_settle(
        &mut node_state,
        &mut auto_prover,
        &api_client,
        1,
        vec![vec![
            new_blob_tx(1),
            new_blob_tx_with_values(&[66]).into(),
            new_blob_tx(2),
        ]],
        0,
    )
    .await?;
    assert_eq!(read_contract_state(&node_state).value, 1);
    Ok(())
}

/// Verifies catch-up mode batches buffered historical transactions correctly.
#[test_log::test(tokio::test)]
async fn test_auto_prover_catchup_n() -> Result<()> {
    let (mut node_state, mut auto_prover, api_client) = setup_catching_with_timeout(5).await?;
    let _ = run_blocks_only(
        &mut node_state,
        &mut auto_prover,
        &api_client,
        1,
        vec![vec![new_blob_tx(1), new_blob_tx(2)], vec![new_blob_tx(3)]],
    )
    .await?;
    auto_prover.ensure_backfill_complete().await?;
    settle_pending_proofs(&mut node_state, &mut auto_prover, &api_client, 3).await?;
    assert_eq!(read_contract_state(&node_state).value, 0);
    Ok(())
}

/// Verifies catch-up handles timeout scenarios and still settles remaining valid transactions.
#[test_log::test(tokio::test)]
async fn test_auto_prover_catchup_timeout_1() -> Result<()> {
    let (mut node_state, mut auto_prover, api_client) = setup_catching_with_timeout(1).await?;
    let _ = run_blocks_only(
        &mut node_state,
        &mut auto_prover,
        &api_client,
        1,
        vec![vec![new_blob_tx(5), new_blob_tx(2)], vec![new_blob_tx(3)]],
    )
    .await?;
    auto_prover.ensure_backfill_complete().await?;
    settle_pending_proofs_with_delay(&mut node_state, &mut auto_prover, &api_client, 3, 2).await?;
    assert_eq!(read_contract_state(&node_state).value, 0);
    Ok(())
}

/// Verifies catch-up timeout handling when multiple transactions in a block can expire.
#[test_log::test(tokio::test)]
async fn test_auto_prover_catchup_timeout_2() -> Result<()> {
    let (mut node_state, mut auto_prover, api_client) = setup_catching_with_timeout(1).await?;
    let _ = run_blocks_only(
        &mut node_state,
        &mut auto_prover,
        &api_client,
        1,
        vec![
            vec![
                new_blob_tx(1),
                new_blob_tx(9),
                new_blob_tx(7),
                new_blob_tx(2),
            ],
            vec![new_blob_tx(3)],
        ],
    )
    .await?;
    auto_prover.ensure_backfill_complete().await?;
    settle_pending_proofs_with_delay(&mut node_state, &mut auto_prover, &api_client, 3, 2).await?;
    assert!(read_contract_state(&node_state).value <= 15);
    Ok(())
}

/// Verifies catch-up timeout handling when timed-out transactions span multiple blocks.
#[test_log::test(tokio::test)]
async fn test_auto_prover_catchup_timeout_multiple_blocks() -> Result<()> {
    let (mut node_state, mut auto_prover, api_client) = setup_catching_with_timeout(1).await?;
    let _ = run_blocks_only(
        &mut node_state,
        &mut auto_prover,
        &api_client,
        1,
        vec![
            vec![new_blob_tx(2)],
            vec![new_blob_tx(8)],
            vec![new_blob_tx(4)],
        ],
    )
    .await?;
    auto_prover.ensure_backfill_complete().await?;
    settle_pending_proofs_with_delay(&mut node_state, &mut auto_prover, &api_client, 4, 2).await?;
    assert!(read_contract_state(&node_state).value <= 14);
    Ok(())
}

/// Verifies catch-up behavior when timeouts happen between already settled transactions.
#[test_log::test(tokio::test)]
async fn test_auto_prover_catchup_timeout_between_settled() -> Result<()> {
    let (mut node_state, mut auto_prover, api_client) = setup_catching_with_timeout(1).await?;
    let _ = run_blocks_only(
        &mut node_state,
        &mut auto_prover,
        &api_client,
        1,
        vec![
            vec![new_blob_tx(2), new_blob_tx(7), new_blob_tx(3)],
            vec![new_blob_tx(4)],
        ],
    )
    .await?;
    auto_prover.ensure_backfill_complete().await?;
    settle_pending_proofs_with_delay(&mut node_state, &mut auto_prover, &api_client, 3, 2).await?;
    assert!(read_contract_state(&node_state).value <= 16);
    Ok(())
}

/// Verifies catch-up drops early timed-out transactions and resumes proving new ones.
#[test_log::test(tokio::test)]
async fn test_auto_prover_catchup_first_txs_timeout() -> Result<()> {
    let (mut node_state, mut auto_prover, api_client) = setup_catching_with_timeout(1).await?;
    let _ = run_blocks_only(
        &mut node_state,
        &mut auto_prover,
        &api_client,
        1,
        vec![
            vec![new_blob_tx(9), new_blob_tx(8), new_blob_tx(2)],
            vec![new_blob_tx(5)],
        ],
    )
    .await?;
    auto_prover.ensure_backfill_complete().await?;
    settle_pending_proofs_with_delay(&mut node_state, &mut auto_prover, &api_client, 3, 2).await?;
    assert!(read_contract_state(&node_state).value <= 24);
    Ok(())
}

/// Verifies block-based buffering threshold controls when proofs are emitted.
#[test_log::test(tokio::test)]
async fn test_auto_prover_buffer_2_blocks() -> Result<()> {
    let (mut node_state, mut auto_prover, api_client) = setup_buffered(2, 100).await?;
    run_blocks_and_settle(
        &mut node_state,
        &mut auto_prover,
        &api_client,
        1,
        vec![vec![new_blob_tx(1)], vec![new_blob_tx(2)]],
        0,
    )
    .await?;
    assert_eq!(read_contract_state(&node_state).value, 3);
    Ok(())
}

/// Verifies transaction-count buffering threshold controls when proofs are emitted.
#[test_log::test(tokio::test)]
async fn test_auto_prover_buffer_2_txs() -> Result<()> {
    let (mut node_state, mut auto_prover, api_client) = setup_buffered(2, 100).await?;
    run_blocks_and_settle(
        &mut node_state,
        &mut auto_prover,
        &api_client,
        1,
        vec![vec![new_blob_tx(4), new_blob_tx(5)]],
        0,
    )
    .await?;
    assert_eq!(read_contract_state(&node_state).value, 9);
    Ok(())
}

/// Verifies max transactions per proof splits proofs into multiple batches.
#[test_log::test(tokio::test)]
async fn test_auto_prover_buffer_max_txs_per_proof() -> Result<()> {
    let (mut node_state, mut auto_prover, api_client) = setup_buffered(0, 2).await?;
    let (next_height, pending) = run_blocks_only(
        &mut node_state,
        &mut auto_prover,
        &api_client,
        1,
        vec![vec![
            new_blob_tx(1),
            new_blob_tx(2),
            new_blob_tx(3),
            new_blob_tx(4),
            new_blob_tx(5),
        ]],
    )
    .await?;
    let (_, counts) = settle_with_pending_proofs_deterministic_counts(
        &mut node_state,
        &mut auto_prover,
        &api_client,
        next_height,
        pending,
    )
    .await?;
    assert_eq!(counts, vec![2, 2, 1]);
    assert_eq!(read_contract_state(&node_state).value, 15);
    Ok(())
}

/// Verifies max transactions per proof splitting when many transactions are in one block.
#[test_log::test(tokio::test)]
async fn test_auto_prover_buffer_one_block_max_txs_per_proof() -> Result<()> {
    let (mut node_state, mut auto_prover, api_client) = setup_buffered(0, 2).await?;
    let (next_height, pending) = run_blocks_only(
        &mut node_state,
        &mut auto_prover,
        &api_client,
        1,
        vec![vec![
            new_blob_tx(1),
            new_blob_tx(2),
            new_blob_tx(3),
            new_blob_tx(4),
            new_blob_tx(5),
        ]],
    )
    .await?;
    let (_, counts) = settle_with_pending_proofs_deterministic_counts(
        &mut node_state,
        &mut auto_prover,
        &api_client,
        next_height,
        pending,
    )
    .await?;
    assert_eq!(counts, vec![2, 2, 1]);
    assert_eq!(read_contract_state(&node_state).value, 15);
    Ok(())
}

/// Verifies behavior when an artificial middle-blob failure occurs without buffering.
#[test_log::test(tokio::test)]
async fn test_auto_prover_artificial_middle_blob_failure_nobuffering() -> Result<()> {
    let (mut node_state, mut auto_prover, api_client) = setup().await?;
    run_blocks_and_settle(
        &mut node_state,
        &mut auto_prover,
        &api_client,
        1,
        vec![vec![new_blob_tx_with_values(&[1, 66, 3]).into()]],
        0,
    )
    .await?;
    assert_eq!(read_contract_state(&node_state).value, 0);
    Ok(())
}

/// Verifies behavior when an artificial middle-blob failure occurs with buffering enabled.
#[test_log::test(tokio::test)]
async fn test_auto_prover_artificial_middle_blob_failure_buffering() -> Result<()> {
    let (mut node_state, mut auto_prover, api_client) = setup_buffered(2, 100).await?;
    run_blocks_and_settle(
        &mut node_state,
        &mut auto_prover,
        &api_client,
        1,
        vec![vec![
            new_blob_tx_with_values(&[1, 66, 3]).into(),
            new_blob_tx(5),
        ]],
        0,
    )
    .await?;
    assert_eq!(read_contract_state(&node_state).value, 0);
    Ok(())
}

/// Verifies early failures while buffered do not block later successful settlement.
#[test_log::test(tokio::test)]
async fn test_auto_prover_early_fail_while_buffered() -> Result<()> {
    let (mut node_state, mut auto_prover, api_client) = setup_buffered(3, 100).await?;
    run_blocks_and_settle(
        &mut node_state,
        &mut auto_prover,
        &api_client,
        1,
        vec![vec![new_failing_blob_tx(4), new_blob_tx(2), new_blob_tx(3)]],
        0,
    )
    .await?;
    assert_eq!(read_contract_state(&node_state).value, 5);
    Ok(())
}

/// Verifies catch-up handles mixed pending, timed-out, failed, and settled transactions.
#[test_log::test(tokio::test)]
async fn test_auto_prover_catchup_mixed_pending_and_failures() -> Result<()> {
    let (mut node_state, mut auto_prover, api_client) = setup_catching_with_timeout(1).await?;
    let _ = run_blocks_only(
        &mut node_state,
        &mut auto_prover,
        &api_client,
        1,
        vec![
            vec![new_blob_tx(2), new_blob_tx(9), new_blob_tx(7)],
            vec![new_blob_tx(4)],
        ],
    )
    .await?;
    auto_prover.ensure_backfill_complete().await?;
    settle_pending_proofs_with_delay(&mut node_state, &mut auto_prover, &api_client, 3, 2).await?;
    assert!(read_contract_state(&node_state).value <= 22);
    Ok(())
}

/// Verifies prover state can be serialized to disk and resumed correctly.
#[test_log::test(tokio::test)]
async fn test_auto_prover_serialize_and_resume() -> Result<()> {
    let (mut node_state, mut auto_prover, api_client) = setup().await?;
    run_blocks_and_settle(
        &mut node_state,
        &mut auto_prover,
        &api_client,
        1,
        vec![vec![new_blob_tx(1)]],
        0,
    )
    .await?;
    let previous_batch_id = auto_prover.store.batch_id;
    let ctx = auto_prover.ctx.clone();

    let persisted = auto_prover.persist().await?;
    hyli_bus::utils::checksums::write_manifest(&ctx.data_directory, &persisted)?;

    let bus = SharedMessageBus::new();
    let mut resumed = TestAutoProver::build(bus.new_handle(), ctx).await?;
    resumed.ensure_backfill_complete().await?;
    assert_eq!(resumed.store.batch_id, previous_batch_id);

    run_blocks_and_settle(
        &mut node_state,
        &mut resumed,
        &api_client,
        3,
        vec![vec![new_blob_tx(2)]],
        0,
    )
    .await?;
    assert_eq!(read_contract_state(&node_state).value, 1);
    Ok(())
}

/// Verifies prover switches to a new program ID after contract update events.
#[test_log::test(tokio::test)]
async fn test_auto_prover_contract_update_program_id() -> Result<()> {
    let (mut node_state, mut auto_prover, api_client) = setup().await?;
    let next_program_id = ProgramId(vec![7, 7, 7, 7]);
    let contract = Contract {
        name: "test".into(),
        state: TestContract::default().commit(),
        verifier: "test".into(),
        program_id: next_program_id.clone(),
        timeout_window: TimeoutWindow::default(),
    };
    api_client.add_contract(contract.clone());

    let block = NodeStateBlock {
        stateful_events: Arc::new(StatefulEvents {
            events: vec![(
                TxId(vec![9].into(), TxHash(vec![1, 2, 3])),
                StatefulEvent::ContractUpdate("test".into(), contract),
            )],
        }),
        ..Default::default()
    };
    auto_prover.handle_node_state_block(block).await?;
    assert!(auto_prover.provers.contains_key(&next_program_id));
    run_blocks_and_settle(
        &mut node_state,
        &mut auto_prover,
        &api_client,
        1,
        vec![vec![new_blob_tx(1)]],
        0,
    )
    .await?;
    assert_eq!(read_contract_state(&node_state).value, 1);
    Ok(())
}

/// Verifies idle flush drains buffered transactions and triggers proving when thresholds are not met.
#[test_log::test(tokio::test)]
async fn test_auto_prover_idle_flush() -> Result<()> {
    let (mut node_state, mut auto_prover, api_client) = setup_buffered(10, 100).await?;
    let block = node_state.craft_block_and_handle(1, vec![new_blob_tx(3)]);
    for (tx_id, event) in &block.stateful_events.events {
        if let StatefulEvent::SequencedTx(tx, tx_ctx) = event {
            auto_prover
                .handle_contract_listener_event(ContractListenerEvent::SequencedTx(ContractTx {
                    tx_id: tx_id.clone(),
                    tx: tx.clone(),
                    tx_ctx: tx_ctx.clone(),
                    status: TransactionStatusDb::Sequenced,
                }))
                .await?;
        }
    }

    assert_eq!(auto_prover.store.buffered_blobs.len(), 1);
    auto_prover.handle_idle_flush().await?;
    assert_eq!(auto_prover.store.buffered_blobs.len(), 0);

    let proofs = get_txs(&api_client).await;
    assert_eq!(proofs.len(), 1);
    assert_eq!(count_hyli_outputs(&proofs[0]), 1);

    auto_prover.handle_idle_flush().await?;
    let proofs_after_second_idle_flush = get_txs(&api_client).await;
    assert!(proofs_after_second_idle_flush.is_empty());
    Ok(())
}

/// Verifies program-id upgrade onchain effect with buffered splitting covers recursive proving path.
#[test_log::test(tokio::test)]
async fn test_auto_prover_program_upgrade_split_and_recurse() -> Result<()> {
    let (mut node_state, mut auto_prover, api_client) = setup_buffered(2, 100).await?;
    let next_program_id = ProgramId(vec![7, 7, 7, 7]);
    auto_prover
        .provers
        .insert(next_program_id.clone(), auto_prover.ctx.prover.clone());

    let block = node_state.craft_block_and_handle(
        1,
        vec![
            new_blob_tx_with_values(&[1]).into(),
            new_blob_tx_with_values(&[77]).into(),
        ],
    );
    auto_prover.handle_node_state_block(block).await?;

    assert_eq!(auto_prover.current_program_id, next_program_id);
    let proofs = get_txs(&api_client).await;
    assert_eq!(proofs.len(), 1);
    assert_eq!(count_hyli_outputs(&proofs[0]), 2);
    Ok(())
}

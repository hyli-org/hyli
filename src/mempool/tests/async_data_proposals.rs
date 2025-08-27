use std::sync::atomic::AtomicI32;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Result};
use assertables::assert_ok;
use hyli_contract_sdk::{Blob, BlobData, HyliOutput, Identity, ProgramId, StateCommitment};
use tracing::info;

use crate::bus::command_response::Query;
use crate::bus::{bus_client, BusClientReceiver, BusClientSender};
use crate::consensus::ConsensusEvent;
use crate::genesis::{Genesis, GenesisEvent};
use crate::mempool::api::RestApiMessage;
use crate::mempool::{MempoolNetMessage, QueryNewCut};
use crate::model::*;
use crate::p2p::network::{HeaderSigner, MsgWithHeader};
use crate::rest::RestApi;
use crate::utils::integration_test::NodeIntegrationCtxBuilder;

bus_client! {
    struct Client {
        sender(GenesisEvent),
        sender(RestApiMessage),
        sender(MsgWithHeader<MempoolNetMessage>),
        sender(Query<QueryNewCut, Cut>),
        receiver(NodeStateEvent),
        receiver(ConsensusEvent),
    }
}

// Need at least two worker threads for this test, as we want parallel execution.
#[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn test_mempool_isnt_blocked_by_proof_verification() {
    assert_ok!(impl_test_mempool_isnt_blocked_by_proof_verification().await);
}

async fn impl_test_mempool_isnt_blocked_by_proof_verification() -> Result<()> {
    let mut node_modules = NodeIntegrationCtxBuilder::new().await;
    node_modules.conf.consensus.slot_duration = Duration::from_millis(400);

    let mut node_modules = node_modules
        .skip::<Genesis>()
        .skip::<RestApi>()
        .build()
        .await?;

    let mut node_client = Client::new_from_bus(node_modules.bus.new_handle()).await;

    let contract_name = ContractName::new("test1");

    node_client.send(GenesisEvent::GenesisBlock(SignedBlock {
        data_proposals: vec![(
            LaneId(node_modules.crypto.validator_pubkey().clone()),
            vec![DataProposal::new(
                None,
                vec![BlobTransaction::new(
                    "test@hyli",
                    vec![RegisterContractAction {
                        verifier: "test-slow".into(),
                        program_id: ProgramId(vec![]),
                        state_commitment: StateCommitment(vec![0, 1, 2, 3]),
                        contract_name: contract_name.clone(),
                        ..Default::default()
                    }
                    .as_blob("hyli".into())],
                )
                .into()],
            )],
        )],
        certificate: AggregateSignature::default(),
        consensus_proposal: ConsensusProposal::default(),
    }))?;

    // Wait until we process the genesis block
    node_modules.wait_for_processed_genesis().await?;

    info!("Processed genesis block");

    // Test setup: count the number of commits during the slow proof verification
    // if we're blocking the consensus, this will be lower than expected.
    //let staking = node1.consensus_ctx.staking();
    let start_time = std::time::Instant::now();
    let mut counting_client = Client::new_from_bus(node_modules.bus.new_handle()).await;
    let count = Arc::new(AtomicI32::new(0));
    let count2 = count.clone();
    let counting_task = tokio::spawn(async move {
        loop {
            let _: ConsensusEvent = counting_client.recv().await.expect("Should get cut");
            count2.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    });

    // Now send the slow proof TX. Channels are ordered so these will be handled in order.
    let blob_tx = BlobTransaction::new(
        Identity(format!("toto@{}", contract_name.0)),
        vec![Blob {
            contract_name: contract_name.clone(),
            data: BlobData(vec![]),
        }],
    );
    let blob_tx_hash = blob_tx.hashed();
    let proof = ProofData(
        serde_json::to_vec(&vec![HyliOutput {
            success: true,
            identity: blob_tx.identity.clone(),
            blobs: blob_tx.blobs.clone().into(),
            tx_hash: blob_tx_hash.clone(),
            ..HyliOutput::default()
        }])
        .unwrap(),
    );
    let proof_hash = proof.hashed();

    node_client.send(RestApiMessage::NewTx(blob_tx.clone().into()))?;

    // Send as many TXs as needed to hung all the workers if we were calling spawn
    for _ in 0..tokio::runtime::Handle::current().metrics().num_workers() {
        node_client.send(RestApiMessage::NewTx(
            ProofTransaction {
                contract_name: contract_name.clone(),
                verifier: "test-slow".into(),
                program_id: ProgramId(vec![]),
                proof: proof.clone(),
            }
            .into(),
        ))?;
        info!("Sent new tx");
    }

    // Wait until we commit this TX
    // Store the data prop hash as we need it below
    let mut data_prop_hash = DataProposalHash::default();
    loop {
        let cut: ConsensusEvent = node_client.recv().await?;
        match cut {
            ConsensusEvent::CommitConsensusProposal(ccp) => {
                info!("Got CommitConsensusProposal");
                if let Some(cut) = ccp.consensus_proposal.cut.first() {
                    data_prop_hash = cut.1.clone();
                }
            }
        }
        let evt: NodeStateEvent = node_client.recv().await?;
        match evt {
            NodeStateEvent::NewBlock(block) => {
                info!("Got Block");
                if block.txs.iter().any(|(tx_id, tx)| {
                    if let TransactionData::VerifiedProof(data) = &tx.transaction_data {
                        info!("Got TX {} in block {}", tx_id, block.block_height);
                        data.contract_name == contract_name
                    } else {
                        false
                    }
                }) {
                    break;
                }
            }
        }
    }

    counting_task.abort();
    counting_task.await.expect_err("Should abort counting task");

    info!(
        "Counted {} commits",
        count.load(std::sync::atomic::Ordering::SeqCst)
    );
    let expected_commits = (start_time.elapsed().as_millis()
        / node_modules.conf.consensus.slot_duration.as_millis()) as i32;

    // Add a little bit of leeway
    if count.load(std::sync::atomic::Ordering::SeqCst) < expected_commits - 1 {
        bail!(
            "Should have more than {} commits, have {}",
            expected_commits,
            count.load(std::sync::atomic::Ordering::SeqCst)
        );
    }

    tracing::warn!("Starting part 2 - processing data proposals.");

    let mut txs = vec![];
    // Send as many TXs as needed to hung all the workers if we were calling spawn
    for _ in 0..tokio::runtime::Handle::current().metrics().num_workers() {
        txs.push(
            VerifiedProofTransaction {
                contract_name: contract_name.clone(),
                verifier: "test-slow".into(),
                program_id: ProgramId(vec![]),
                proof: Some(proof.clone()),
                proof_hash: proof_hash.clone(),
                proven_blobs: vec![BlobProofOutput {
                    original_proof_hash: proof_hash.clone(),
                    blob_tx_hash: blob_tx_hash.clone(),
                    program_id: ProgramId(vec![]),
                    verifier: "test-slow".into(),
                    hyli_output: HyliOutput::default(),
                }],
                is_recursive: false,
                proof_size: proof.0.len(),
            }
            .into(),
        );
    }
    let data_proposal = DataProposal::new(Some(data_prop_hash), txs);

    // Test setup 2: count the number of commits during the slow proof verification
    // if we're blocking the consensus, this will be lower than expected.
    //let staking = node1.consensus_ctx.staking();
    let start_time = std::time::Instant::now();
    let mut counting_client = Client::new_from_bus(node_modules.bus.new_handle()).await;
    let count = Arc::new(AtomicI32::new(0));
    let count2 = count.clone();
    let counting_task = tokio::spawn(async move {
        loop {
            let _: ConsensusEvent = counting_client.recv().await.expect("Should get cut");
            count2.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    });

    node_client.send(node_modules.crypto.sign_msg_with_header(
        MempoolNetMessage::DataProposal(data_proposal.hashed(), data_proposal),
    )?)?;

    // Wait until we commit this TX
    loop {
        let cut: NodeStateEvent = node_client.recv().await?;
        match cut {
            NodeStateEvent::NewBlock(block) => {
                if block.txs.iter().any(|(_tx_id, tx)| {
                    if let TransactionData::VerifiedProof(data) = &tx.transaction_data {
                        data.contract_name == contract_name
                    } else {
                        false
                    }
                }) {
                    break;
                }
            }
        }
    }

    counting_task.abort();
    counting_task.await.expect_err("Should abort counting task");

    info!(
        "Counted {} commits",
        count.load(std::sync::atomic::Ordering::SeqCst)
    );
    let expected_commits = (start_time.elapsed().as_millis()
        / node_modules.conf.consensus.slot_duration.as_millis()) as i32;

    // Add a little bit of leeway
    if count.load(std::sync::atomic::Ordering::SeqCst) < expected_commits - 1 {
        bail!(
            "Should have more than {} commits, have {}",
            expected_commits,
            count.load(std::sync::atomic::Ordering::SeqCst)
        );
    }

    Ok(())
}

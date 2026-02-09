use anyhow::Result;
use assertables::assert_ok;
use std::time::Duration;
use tracing::info;

use crate::bus::{BusClientSender, bus_client};
use crate::mempool::api::RestApiMessage;
use crate::model::*;
use crate::utils::conf::OwnLaneConf;
use crate::utils::integration_test::NodeIntegrationCtxBuilder;
use hyli_contract_sdk::{Blob, BlobData, HyliOutput, ProgramId, StateCommitment};
use hyli_modules::modules::rest::RestApi;

bus_client! {
    struct Client {
        sender(RestApiMessage),
    }
}

#[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn test_mempool_multi_lane() {
    assert_ok!(impl_test_mempool_multi_lane().await);
}

async fn impl_test_mempool_multi_lane() -> Result<()> {
    let mut node_modules = NodeIntegrationCtxBuilder::new().await;
    node_modules.conf.consensus.slot_duration = Duration::from_millis(100);
    node_modules.conf.id = "node-1".to_string();
    node_modules.conf.consensus.solo = false;
    node_modules.conf.own_lanes = OwnLaneConf {
        suffixes: vec![
            "default-blob".to_string(),
            "default-proof".to_string(),
            "other-lane".to_string(),
        ],
        default_blob_suffix: "default-blob".to_string(),
        default_proof_suffix: "default-proof".to_string(),
    };

    let mut node_modules_2 = NodeIntegrationCtxBuilder::new().await;
    node_modules_2.conf.consensus.slot_duration = Duration::from_millis(100);
    node_modules_2.conf.id = "node-2".to_string();
    node_modules_2.conf.consensus.solo = false;
    node_modules_2.conf.own_lanes = OwnLaneConf {
        suffixes: vec!["other-lane-2".to_string()],
        default_blob_suffix: "other-lane-2".to_string(),
        default_proof_suffix: "other-lane-2".to_string(),
    };

    node_modules.conf.da_read_from = node_modules_2.conf.da_public_address.clone();
    node_modules_2.conf.da_read_from = node_modules.conf.da_public_address.clone();
    node_modules
        .conf
        .p2p
        .peers
        .push(node_modules_2.conf.p2p.public_address.clone());
    node_modules_2
        .conf
        .p2p
        .peers
        .push(node_modules.conf.p2p.public_address.clone());
    node_modules.conf.genesis.stakers = vec![
        (node_modules.conf.id.clone(), 1_000_000),
        (node_modules_2.conf.id.clone(), 1_000_000),
    ]
    .into_iter()
    .collect();
    node_modules_2.conf.genesis.stakers = node_modules.conf.genesis.stakers.clone();

    let mut node_modules = node_modules.skip::<RestApi>().build().await?;
    let mut node_client = Client::new_from_bus(node_modules.bus.new_handle()).await;

    let mut node_modules_2 = node_modules_2.skip::<RestApi>().build().await?;
    let mut node_client_2 = Client::new_from_bus(node_modules_2.bus.new_handle()).await;

    // Wait until we process the genesis block
    node_modules.wait_for_processed_genesis().await?;
    info!("Processed genesis block");

    // Register a test contract
    let contract_name = ContractName::new("test1");

    let contract_tx = BlobTransaction::new(
        "hyli@hyli",
        vec![
            RegisterContractAction {
                verifier: Verifier("test".into()),
                program_id: ProgramId(vec![1, 2, 3]),
                state_commitment: StateCommitment(vec![1, 2, 3]),
                contract_name: contract_name.clone(),
                timeout_window: None,
                constructor_metadata: None,
            }
            .as_blob(ContractName::new("hyli")),
        ],
    );

    node_client.send(RestApiMessage::NewTx {
        tx: contract_tx.clone().into(),
        lane_suffix: None,
    })?;

    node_modules
        .wait_for_settled_tx(contract_tx.hashed())
        .await?;

    // Test setup: send a a blob to the default lane, a proof to the default lane, and a blob to a different lane with the proof to that lane too.
    // We don't actually use the rest API, just the events.

    // Send a blob to the default lane
    let blob_tx_1 = BlobTransaction::new(
        format!("test@{contract_name}"),
        vec![Blob {
            contract_name: contract_name.clone(),
            data: BlobData(vec![1, 2, 3, 4, 5]),
        }],
    );
    node_client.send(RestApiMessage::NewTx {
        tx: blob_tx_1.clone().into(),
        lane_suffix: None,
    })?;

    // Send a proof to the default lane
    node_client.send(RestApiMessage::NewTx {
        tx: ProofTransaction {
            contract_name: contract_name.clone(),
            program_id: ProgramId(vec![1, 2, 3]),
            verifier: Verifier("test".into()),
            proof: ProofData(
                borsh::to_vec(&vec![HyliOutput {
                    version: 1,
                    identity: format!("test@{contract_name}").into(),
                    index: BlobIndex(0),
                    blobs: blob_tx_1.blobs.clone().into(),
                    tx_blob_count: blob_tx_1.blobs.len(),
                    initial_state: StateCommitment(vec![1, 2, 3]),
                    next_state: StateCommitment(vec![4, 5, 6]),
                    success: true,
                    tx_hash: blob_tx_1.hashed(),
                    tx_ctx: None,
                    state_reads: vec![],
                    onchain_effects: vec![],
                    program_outputs: vec![],
                }])
                .unwrap(),
            ),
        }
        .into(),
        lane_suffix: None,
    })?;

    node_modules.wait_for_settled_tx(blob_tx_1.hashed()).await?;

    // Send a blob to a different lane
    let checked_tx_2 = BlobTransaction::new(
        format!("test@{contract_name}"),
        vec![Blob {
            contract_name: contract_name.clone(),
            data: BlobData(vec![5, 4, 3, 2]),
        }],
    );
    node_client.send(RestApiMessage::NewTx {
        tx: checked_tx_2.clone().into(),
        lane_suffix: Some("other-lane".to_string()),
    })?;

    // Wait til it's sequenced to avoid data races with the proof
    node_modules
        .wait_for_sequenced_tx(checked_tx_2.hashed())
        .await?;

    // Send a proof to the different node, to a different lane
    node_client_2.send(RestApiMessage::NewTx {
        tx: ProofTransaction {
            contract_name: contract_name.clone(),
            program_id: ProgramId(vec![1, 2, 3]),
            verifier: Verifier("test".into()),
            proof: ProofData(
                borsh::to_vec(&vec![HyliOutput {
                    version: 1,
                    identity: format!("test@{contract_name}").into(),
                    index: BlobIndex(0),
                    blobs: checked_tx_2.blobs.clone().into(),
                    tx_blob_count: checked_tx_2.blobs.len(),
                    initial_state: StateCommitment(vec![4, 5, 6]),
                    next_state: StateCommitment(vec![7, 8, 9]),
                    success: true,
                    tx_hash: checked_tx_2.hashed(),
                    tx_ctx: None,
                    state_reads: vec![],
                    onchain_effects: vec![],
                    program_outputs: vec![],
                }])
                .unwrap(),
            ),
        }
        .into(),
        lane_suffix: Some("other-lane-2".to_string()),
    })?;

    // Then wait until we see the txs in node-state events
    node_modules_2
        .wait_for_settled_tx(checked_tx_2.hashed())
        .await?;

    Ok(())
}

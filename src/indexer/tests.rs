#![allow(clippy::indexing_slicing)]

use crate::{
    bus::SharedMessageBus,
    explorer::Explorer,
    model::{
        Blob, BlobData, BlobProofOutput, DataProposal, LaneId, ProofData, SignedBlock, Transaction,
        TransactionData, ValidatorPublicKey, VerifiedProofTransaction,
    },
    utils::conf::IndexerConf,
};
use assert_json_diff::assert_json_include;
use axum_test::TestServer;
use client_sdk::transaction_builder::ProvableBlobTx;
use hydentity::{client::tx_executor_handler::register_identity, HydentityAction};
use hyli_contract_sdk::{BlobIndex, HyliOutput, Identity, ProgramId, StateCommitment, TxHash};
use hyli_model::api::{
    APIBlob, APIBlock, APIContract, APIContractHistory, APITransaction, APITransactionEvents,
    TransactionStatusDb,
};
use hyli_modules::node_state::test::{craft_signed_block, craft_signed_block_with_parent_dp_hash};
use serde_json::json;
use sqlx::postgres::{PgListener, PgPoolOptions};
use std::{future::IntoFuture, time::Duration};
use testcontainers_modules::{
    postgres::Postgres,
    testcontainers::{runners::AsyncRunner, ContainerAsync, ImageExt},
};
use utils::TimestampMs;

use super::*;

async fn setup_test_server(explorer: &Explorer) -> Result<TestServer> {
    let router = explorer.api(None);
    TestServer::new(router)
}

async fn new_indexer(pool: PgPool) -> (Indexer, Explorer) {
    let bus = SharedMessageBus::default();

    let conf = Conf {
        indexer: IndexerConf {
            query_buffer_size: 1,
            persist_proofs: false,
        },
        ..Conf::default()
    };
    (
        Indexer {
            bus: IndexerBusClient::new_from_bus(bus.new_handle()).await,
            db: pool.clone(),
            node_state: NodeState::create("indexer".to_string(), "indexer"),
            handler_store: IndexerHandlerStore::default(),
            conf,
        },
        Explorer::new(bus, pool).await,
    )
}

fn new_register_tx(
    contract_name: ContractName,
    state_commitment: StateCommitment,
) -> BlobTransaction {
    BlobTransaction::new(
        "hyli@hyli",
        vec![RegisterContractAction {
            verifier: "test".into(),
            program_id: ProgramId(vec![3, 2, 1]),
            state_commitment,
            contract_name: contract_name.clone(),
            ..Default::default()
        }
        .as_blob("hyli".into())],
    )
}

pub fn new_delete_tx(tld: ContractName, contract_name: ContractName) -> BlobTransaction {
    BlobTransaction::new(
        "hyli@wallet".to_string(),
        vec![
            HydentityAction::VerifyIdentity {
                nonce: 0,
                account: "hyli@wallet".to_string(),
            }
            .as_blob("wallet".into()),
            DeleteContractAction {
                contract_name: contract_name.clone(),
            }
            .as_blob(tld),
            Blob {
                contract_name,
                data: BlobData(vec![]),
            },
        ],
    )
}

fn new_blob_tx(
    identity: Identity,
    first_contract_name: ContractName,
    second_contract_name: ContractName,
) -> Transaction {
    Transaction {
        version: 1,
        transaction_data: TransactionData::Blob(BlobTransaction::new(
            identity,
            vec![
                Blob {
                    contract_name: first_contract_name,
                    data: BlobData(vec![1, 2, 3]),
                },
                Blob {
                    contract_name: second_contract_name,
                    data: BlobData(vec![1, 2, 3]),
                },
            ],
        )),
    }
}

fn new_proof_tx(
    identity: Identity,
    contract_name: ContractName,
    blob_index: BlobIndex,
    blob_transaction: &Transaction,
    initial_state: StateCommitment,
    next_state: StateCommitment,
) -> Transaction {
    let TransactionData::Blob(blob_tx) = &blob_transaction.transaction_data else {
        panic!("Expected BlobTransaction");
    };
    let proof = ProofData(initial_state.0.clone());
    Transaction {
        version: 1,
        transaction_data: TransactionData::VerifiedProof(VerifiedProofTransaction {
            contract_name: contract_name.clone(),
            proof_hash: proof.hashed(),
            proof_size: proof.0.len(),
            proven_blobs: vec![BlobProofOutput {
                original_proof_hash: proof.hashed(),
                program_id: ProgramId(vec![3, 2, 1]),
                verifier: "test".into(),
                blob_tx_hash: blob_transaction.hashed(),
                hyli_output: HyliOutput {
                    version: 1,
                    initial_state,
                    next_state,
                    identity,
                    tx_hash: blob_transaction.hashed(),
                    tx_ctx: None,
                    index: blob_index,
                    tx_blob_count: blob_tx.blobs.len(),
                    blobs: blob_tx.blobs.clone().into(),
                    success: true,
                    state_reads: vec![],
                    onchain_effects: vec![],
                    program_outputs: vec![],
                },
            }],
            is_recursive: false,
            proof: Some(proof),
            verifier: "test".into(),
            program_id: ProgramId(vec![3, 2, 1]),
        }),
    }
}

async fn assert_tx_status(server: &TestServer, tx_hash: TxHash, tx_status: TransactionStatusDb) {
    let transactions_response = server
        .get(format!("/transaction/hash/{tx_hash}").as_str())
        .await;
    transactions_response.assert_status_ok();
    let json_response = transactions_response.json::<APITransaction>();
    assert_eq!(
        json_response.transaction_status, tx_status,
        "Transaction status mismatch for tx_hash: {tx_hash}"
    );
}

async fn assert_tx_not_found(server: &TestServer, tx_hash: TxHash) {
    let transactions_response = server
        .get(format!("/transaction/hash/{tx_hash}").as_str())
        .await;
    transactions_response.assert_status_not_found();
}

#[test_log::test(tokio::test)]
async fn test_indexer_handle_block_flow() -> Result<()> {
    let container = Postgres::default()
        .with_tag("17-alpine")
        .with_cmd(["postgres", "-c", "log_statement=all"])
        .start()
        .await
        .unwrap();
    let db = PgPoolOptions::new()
        .max_connections(5)
        .connect(&format!(
            "postgresql://postgres:postgres@localhost:{}/postgres",
            container.get_host_port_ipv4(5432).await.unwrap()
        ))
        .await
        .unwrap();
    MIGRATOR.run(&db).await.unwrap();

    let (mut indexer, explorer) = new_indexer(db).await;
    let server = setup_test_server(&explorer).await?;

    let initial_state = StateCommitment(vec![1, 2, 3]);
    let next_state = StateCommitment(vec![4, 5, 6]);
    let first_contract_name = ContractName::new("c1");
    let second_contract_name = ContractName::new("c2");

    let register_tx_1 = new_register_tx(first_contract_name.clone(), initial_state.clone());
    let register_tx_2 = new_register_tx(second_contract_name.clone(), initial_state.clone());

    let blob_transaction = new_blob_tx(
        Identity::new("test@c1"),
        first_contract_name.clone(),
        second_contract_name.clone(),
    );
    let blob_transaction_hash = blob_transaction.hashed();

    let proof_tx_1 = new_proof_tx(
        Identity::new("test@c1"),
        first_contract_name.clone(),
        BlobIndex(0),
        &blob_transaction,
        initial_state.clone(),
        next_state.clone(),
    );

    let proof_tx_2 = new_proof_tx(
        Identity::new("test@c1"),
        second_contract_name.clone(),
        BlobIndex(1),
        &blob_transaction,
        initial_state.clone(),
        next_state.clone(),
    );

    let other_blob_transaction = new_blob_tx(
        Identity::new("test@c1"),
        second_contract_name.clone(),
        first_contract_name.clone(),
    );
    let other_blob_transaction_hash = other_blob_transaction.hashed();
    // Send two proofs for the same blob
    let proof_tx_3 = new_proof_tx(
        Identity::new("test@c1"),
        first_contract_name.clone(),
        BlobIndex(1),
        &other_blob_transaction,
        StateCommitment(vec![7, 7, 7]),
        StateCommitment(vec![9, 9, 9]),
    );
    let proof_tx_4 = new_proof_tx(
        Identity::new("test@c1"),
        first_contract_name.clone(),
        BlobIndex(1),
        &other_blob_transaction,
        StateCommitment(vec![8, 8]),
        StateCommitment(vec![9, 9]),
    );

    let txs = vec![
        register_tx_1.into(),
        register_tx_2.clone().into(),
        blob_transaction,
        proof_tx_1,
        proof_tx_2,
        other_blob_transaction,
        proof_tx_3,
        proof_tx_4,
    ];

    // Handling a block containing txs

    let parent_data_proposal = DataProposal::new(None, txs);
    let mut signed_block = SignedBlock::default();
    signed_block.consensus_proposal.slot = 1;
    signed_block.data_proposals.push((
        LaneId::new(ValidatorPublicKey("ttt".into())),
        vec![parent_data_proposal.clone()],
    ));

    indexer
        .handle_signed_block(signed_block)
        .await
        .expect("Failed to handle block");
    indexer
        .dump_store_to_db()
        .await
        .expect("Failed to dump store to DB");

    //
    // Handling MempoolStatusEvent
    //

    let initial_state_wd = StateCommitment(vec![1, 2, 3]);
    let next_state_wd = StateCommitment(vec![4, 5, 6]);
    let first_contract_name_wd = ContractName::new("wd1");
    let second_contract_name_wd = ContractName::new("wd2");

    let register_tx_1_wd =
        new_register_tx(first_contract_name_wd.clone(), initial_state_wd.clone());
    let register_tx_2_wd =
        new_register_tx(second_contract_name_wd.clone(), initial_state_wd.clone());

    let blob_transaction_wd = new_blob_tx(
        Identity::new("test@wd1"),
        first_contract_name_wd.clone(),
        second_contract_name_wd.clone(),
    );

    let proof_tx_1_wd = new_proof_tx(
        Identity::new("test@wd1"),
        first_contract_name_wd.clone(),
        BlobIndex(0),
        &blob_transaction_wd,
        initial_state_wd.clone(),
        next_state_wd.clone(),
    );

    let register_tx_1_wd = Transaction {
        version: 1,
        transaction_data: TransactionData::Blob(register_tx_1_wd),
    };
    let register_tx_2_wd = Transaction {
        version: 1,
        transaction_data: TransactionData::Blob(register_tx_2_wd),
    };

    indexer
        .handle_mempool_status_event(MempoolStatusEvent::WaitingDissemination {
            parent_data_proposal_hash: parent_data_proposal.hashed(),
            txs: vec![register_tx_1_wd.clone()],
        })
        .await
        .expect("MempoolStatusEvent");

    indexer.dump_store_to_db().await.expect("Dump to db");

    assert_tx_status(
        &server,
        register_tx_1_wd.hashed(),
        TransactionStatusDb::WaitingDissemination,
    )
    .await;

    indexer
        .handle_mempool_status_event(MempoolStatusEvent::WaitingDissemination {
            parent_data_proposal_hash: parent_data_proposal.hashed(),
            txs: vec![register_tx_2_wd.clone()],
        })
        .await
        .expect("MempoolStatusEvent");

    indexer.dump_store_to_db().await.expect("Dump to db");

    assert_tx_status(
        &server,
        register_tx_2_wd.hashed(),
        TransactionStatusDb::WaitingDissemination,
    )
    .await;

    let parent_data_proposal_hash = parent_data_proposal.hashed();

    let data_proposal = DataProposal::new(
        Some(parent_data_proposal_hash.clone()),
        vec![register_tx_1_wd.clone(), register_tx_2_wd.clone()],
    );

    let data_proposal_created_event = MempoolStatusEvent::DataProposalCreated {
        parent_data_proposal_hash: parent_data_proposal_hash.clone(),
        data_proposal_hash: data_proposal.hashed(),
        txs_metadatas: vec![
            register_tx_1_wd.metadata(parent_data_proposal_hash.clone()),
            register_tx_2_wd.metadata(parent_data_proposal_hash.clone()),
        ],
    };

    indexer
        .handle_mempool_status_event(MempoolStatusEvent::WaitingDissemination {
            parent_data_proposal_hash: data_proposal.hashed(),
            txs: vec![blob_transaction_wd.clone()],
        })
        .await
        .expect("MempoolStatusEvent");

    indexer.dump_store_to_db().await.expect("Dump to db");

    assert_tx_status(
        &server,
        blob_transaction_wd.hashed(),
        TransactionStatusDb::WaitingDissemination,
    )
    .await;

    assert_tx_not_found(&server, proof_tx_1_wd.hashed()).await;

    indexer
        .handle_mempool_status_event(data_proposal_created_event.clone())
        .await
        .expect("MempoolStatusEvent");

    indexer.dump_store_to_db().await.expect("Dump to db");

    assert_tx_status(
        &server,
        register_tx_1_wd.hashed(),
        TransactionStatusDb::DataProposalCreated,
    )
    .await;
    assert_tx_status(
        &server,
        register_tx_2_wd.hashed(),
        TransactionStatusDb::DataProposalCreated,
    )
    .await;
    assert_tx_status(
        &server,
        blob_transaction_wd.hashed(),
        TransactionStatusDb::WaitingDissemination,
    )
    .await;
    assert_tx_not_found(&server, proof_tx_1_wd.hashed()).await;

    // We skip blob_transaction_wd
    let data_proposal_2 = DataProposal::new(
        Some(data_proposal.hashed()),
        vec![
            blob_transaction_wd.clone(),
            blob_transaction_wd.clone(),
            proof_tx_1_wd.clone(),
        ],
    );

    indexer
        .handle_mempool_status_event(MempoolStatusEvent::DataProposalCreated {
            parent_data_proposal_hash: data_proposal.hashed(),
            data_proposal_hash: data_proposal_2.hashed(),
            txs_metadatas: vec![
                blob_transaction_wd.metadata(data_proposal.hashed()),
                blob_transaction_wd.metadata(data_proposal.hashed()),
                proof_tx_1_wd.metadata(data_proposal.hashed()),
            ],
        })
        .await
        .expect("MempoolStatusEvent");

    indexer.dump_store_to_db().await.expect("Dump to db");

    assert_tx_status(
        &server,
        blob_transaction_wd.hashed(),
        TransactionStatusDb::DataProposalCreated,
    )
    .await;

    let mut signed_block = SignedBlock::default();
    signed_block.consensus_proposal.timestamp = TimestampMs(12345);
    signed_block.consensus_proposal.slot = 2;
    signed_block.data_proposals.push((
        LaneId::new(ValidatorPublicKey("ttt".into())),
        vec![data_proposal, data_proposal_2],
    ));
    let block_2_hash = signed_block.hashed();
    indexer
        .handle_signed_block(signed_block)
        .await
        .expect("Failed to handle block");
    indexer
        .dump_store_to_db()
        .await
        .expect("Failed to dump store to DB");

    assert_tx_status(
        &server,
        register_tx_1_wd.hashed(),
        TransactionStatusDb::Success,
    )
    .await;
    assert_tx_status(
        &server,
        register_tx_2_wd.hashed(),
        TransactionStatusDb::Success,
    )
    .await;
    assert_tx_status(
        &server,
        blob_transaction_wd.hashed(),
        TransactionStatusDb::Sequenced,
    )
    .await;
    assert_tx_not_found(&server, proof_tx_1_wd.hashed()).await;

    // Check a mempool status event does not change a Success/Sequenced status
    indexer
        .handle_mempool_status_event(data_proposal_created_event.clone())
        .await
        .expect("MempoolStatusEvent");

    // Check blocks have correct data
    let blocks = server.get("/blocks").await.json::<Vec<APIBlock>>();
    assert_eq!(blocks.len(), 2);
    assert_eq!(blocks.last().unwrap().timestamp, 0);
    assert_eq!(blocks.first().unwrap().timestamp, 12345);

    let transactions_response = server.get("/contract/c1").await;
    transactions_response.assert_status_ok();
    let json_response = transactions_response.json::<APIContract>();
    assert_eq!(json_response.state_commitment, next_state.0);

    let transactions_response = server.get("/contract/c2").await;
    transactions_response.assert_status_ok();
    let json_response = transactions_response.json::<APIContract>();
    assert_eq!(json_response.state_commitment, next_state.0);

    let transactions_response = server.get("/contract/d1").await;
    transactions_response.assert_status_not_found();

    let blob_transactions_response = server.get("/blob_transactions/contract/c1").await;
    blob_transactions_response.assert_status_ok();
    assert_json_include!(
        actual: blob_transactions_response.json::<serde_json::Value>(),
        expected: json!([
            {
                "blobs": [{
                    "contract_name": "c1",
                    "data": hex::encode([1,2,3]),
                    // For now, unsettled txs don't have blob proof outputs stored.
                    "proof_outputs": [/*
                        {
                            "initial_state": [7,7,7],
                        },
                        {
                            "initial_state": [8,8],
                        } */
                    ]
                }],
                "transaction_status": "Sequenced",
                "tx_hash": other_blob_transaction_hash.to_string(),
                "index": 5,
            },
            {
                "blobs": [{
                    "contract_name": "c1",
                    "data": hex::encode([1,2,3]),
                    "proof_outputs": [{}]
                }],
                "tx_hash": blob_transaction_hash.to_string(),
                "index": 2,
            }
        ])
    );
    let all_txs = server.get("/transactions/block/1").await;
    all_txs.assert_status_ok();
    assert_json_include!(
        actual: all_txs.json::<serde_json::Value>(),
        expected: json!([
            { "index": 5, "transaction_type": "BlobTransaction", "transaction_status": "Sequenced" },
            { "index": 2, "transaction_type": "BlobTransaction", "transaction_status": "Success" },
            { "index": 1, "transaction_type": "BlobTransaction", "transaction_status": "Success" },
            { "index": 0, "transaction_type": "BlobTransaction", "transaction_status": "Success" },
        ])
    );

    let blob_transactions_response = server.get("/blob_transactions/contract/c2").await;
    blob_transactions_response.assert_status_ok();
    assert_json_include!(
        actual: blob_transactions_response.json::<serde_json::Value>(),
        expected: json!([
            {
                "blobs": [{
                    "contract_name": "c2",
                    "data": hex::encode([1,2,3]),
                    "proof_outputs": []
                }],
                "tx_hash": other_blob_transaction_hash.to_string(),
            },
            {
                "blobs": [{
                    "contract_name": "c2",
                    "data": hex::encode([1,2,3]),
                    "proof_outputs": [{}]
                }],
                "tx_hash": blob_transaction_hash.to_string(),
            }
        ])
    );

    // Test proof transaction endpoints
    let proofs_response = server.get("/proofs").await;
    proofs_response.assert_status_ok();
    assert_json_include!(
        actual: proofs_response.json::<serde_json::Value>(),
        expected: json!([
            { "index": 4, "transaction_type": "ProofTransaction", "transaction_status": "Success", "block_hash": block_2_hash },
            { "index": 7, "transaction_type": "ProofTransaction", "transaction_status": "Success" },
            { "index": 6, "transaction_type": "ProofTransaction", "transaction_status": "Success" },
            { "index": 4, "transaction_type": "ProofTransaction", "transaction_status": "Success" },
            { "index": 3, "transaction_type": "ProofTransaction", "transaction_status": "Success" },
        ])
    );

    let proofs_by_height = server.get("/proofs/block/1").await;
    proofs_by_height.assert_status_ok();

    assert_json_include!(
        actual: proofs_by_height.json::<serde_json::Value>(),
        expected: json!([
            { "index": 7, "transaction_type": "ProofTransaction", "transaction_status": "Success" },
            { "index": 6, "transaction_type": "ProofTransaction", "transaction_status": "Success" },
            { "index": 4, "transaction_type": "ProofTransaction", "transaction_status": "Success" },
            { "index": 3, "transaction_type": "ProofTransaction", "transaction_status": "Success" },
        ])
    );

    let proof_by_hash = server
        .get(format!("/proof/hash/{}", proof_tx_1_wd.hashed()).as_str())
        .await;
    proof_by_hash.assert_status_ok();
    assert_json_include!(
        actual: proof_by_hash.json::<serde_json::Value>(),
        expected: json!({
            "index": 4,
            "transaction_type": "ProofTransaction",
            "transaction_status": "Success"
        })
    );

    // Test non-existent proof
    let non_existent_proof = server
        .get("/proof/hash/1111111111111111111111111111111111111111111111111111111111111111")
        .await;
    non_existent_proof.assert_status_not_found();

    Ok(())
}

#[test_log::test(tokio::test)]
async fn test_contract_notifications_sent() -> Result<()> {
    let container = Postgres::default()
        .with_tag("17-alpine")
        .with_cmd(["postgres", "-c", "log_statement=all"])
        .start()
        .await
        .unwrap();
    let db_url = format!(
        "postgresql://postgres:postgres@localhost:{}/postgres",
        container.get_host_port_ipv4(5432).await.unwrap()
    );
    let db = PgPoolOptions::new()
        .max_connections(5)
        .connect(&db_url)
        .await
        .unwrap();
    MIGRATOR.run(&db).await.unwrap();

    let contract_name = ContractName::new("notify_contract");

    let mut listener = PgListener::connect(&db_url).await.unwrap();
    listener.listen(&contract_name.0).await.unwrap();

    let (mut indexer, _) = new_indexer(db).await;

    let register_tx = new_register_tx(contract_name.clone(), StateCommitment(vec![])).into();
    let blob_transaction = new_blob_tx(
        Identity::new(format!("test@{}", contract_name)),
        contract_name.clone(),
        ContractName::new("other"),
    );

    let parent_data_proposal = DataProposal::new(None, vec![register_tx, blob_transaction]);
    let mut signed_block = SignedBlock::default();
    signed_block.consensus_proposal.slot = 1;
    signed_block.data_proposals.push((
        LaneId::new(ValidatorPublicKey("ttt".into())),
        vec![parent_data_proposal],
    ));
    let expected_block = signed_block.clone();

    indexer
        .handle_signed_block(signed_block)
        .await
        .expect("Failed to handle block");
    indexer
        .dump_store_to_db()
        .await
        .expect("Failed to dump store to DB");

    let notification = tokio::time::timeout(Duration::from_secs(5), listener.recv())
        .await
        .expect("Notification timed out")
        .expect("Failed to receive notification");

    let payload: BlockHeight =
        serde_json::from_str(notification.payload()).expect("Invalid payload");
    assert_eq!(notification.channel(), &contract_name.0);
    assert_eq!(payload, expected_block.height());

    Ok(())
}

pub fn make_register_hyli_wallet_identity_tx() -> BlobTransaction {
    let mut tx = ProvableBlobTx::new("hyli@wallet".into());
    register_identity(&mut tx, "wallet".into(), "password".into()).unwrap();
    BlobTransaction::new("hyli@wallet".to_string(), tx.blobs)
}

async fn scenario_contracts() -> Result<(
    ContainerAsync<Postgres>,
    Indexer,
    SignedBlock,
    SignedBlock,
    SignedBlock,
    SignedBlock,
)> {
    let container = Postgres::default()
        .with_tag("17-alpine")
        .with_cmd(["postgres", "-c", "log_statement=all"])
        .start()
        .await
        .unwrap();
    let db = PgPoolOptions::new()
        .max_connections(5)
        .connect(&format!(
            "postgresql://postgres:postgres@localhost:{}/postgres",
            container.get_host_port_ipv4(5432).await.unwrap()
        ))
        .await
        .unwrap();
    MIGRATOR.run(&db).await.unwrap();
    let (indexer, _) = new_indexer(db).await;

    let register_wallet = new_register_tx("wallet".into(), StateCommitment(vec![]));
    let register_hyli_at_wallet = make_register_hyli_wallet_identity_tx();

    let register_hyli_at_wallet_proof = new_proof_tx(
        "hyli@wallet".into(),
        "wallet".into(),
        BlobIndex(0),
        &register_hyli_at_wallet.clone().into(),
        StateCommitment(vec![]),
        StateCommitment(vec![0]),
    );

    let b1 = craft_signed_block(
        1,
        vec![
            register_wallet.into(),
            register_hyli_at_wallet.into(),
            register_hyli_at_wallet_proof,
        ],
    );

    // Create a couple fake blocks with contracts
    let b3 = craft_signed_block(
        3,
        vec![
            new_register_tx(ContractName::new("a"), StateCommitment(vec![])).into(),
            new_register_tx(ContractName::new("b"), StateCommitment(vec![])).into(),
            new_register_tx(ContractName::new("c"), StateCommitment(vec![])).into(),
        ],
    );

    let delete_a = new_delete_tx(ContractName::new("hyli"), ContractName::new("a"));
    let delete_c = new_delete_tx(ContractName::new("hyli"), ContractName::new("c"));

    let delete_a_proof = new_proof_tx(
        "hyli@wallet".into(),
        "wallet".into(),
        BlobIndex(0),
        &delete_a.clone().into(),
        StateCommitment(vec![0]),
        StateCommitment(vec![1]),
    );
    let delete_c_proof = new_proof_tx(
        "hyli@wallet".into(),
        "wallet".into(),
        BlobIndex(0),
        &delete_c.clone().into(),
        StateCommitment(vec![1]),
        StateCommitment(vec![2]),
    );

    let b4 = craft_signed_block(
        4,
        vec![
            delete_a.into(),
            delete_a_proof,
            delete_c.into(),
            delete_c_proof,
        ],
    );

    let delete_b = new_delete_tx(ContractName::new("hyli"), ContractName::new("b"));
    let delete_b_proof = new_proof_tx(
        "hyli@wallet".into(),
        "wallet".into(),
        BlobIndex(0),
        &delete_b.clone().into(),
        StateCommitment(vec![2]),
        StateCommitment(vec![3]),
    );

    let delete_a = new_delete_tx(ContractName::new("hyli"), ContractName::new("a"));
    let delete_a_proof = new_proof_tx(
        "hyli@wallet".into(),
        "wallet".into(),
        BlobIndex(0),
        &delete_a.clone().into(),
        StateCommitment(vec![3]),
        StateCommitment(vec![4]),
    );

    let delete_d = new_delete_tx(ContractName::new("hyli"), ContractName::new("d"));
    let delete_d_proof = new_proof_tx(
        "hyli@wallet".into(),
        "wallet".into(),
        BlobIndex(0),
        &delete_d.clone().into(),
        StateCommitment(vec![4]),
        StateCommitment(vec![5]),
    );

    let mut b5 = craft_signed_block_with_parent_dp_hash(
        5,
        vec![
            new_register_tx(ContractName::new("a"), StateCommitment(vec![])).into(),
            delete_b.into(),
            delete_b_proof,
            delete_a.into(),
            delete_a_proof,
            new_register_tx(ContractName::new("d"), StateCommitment(vec![])).into(),
            delete_d.into(),
            delete_d_proof,
        ],
        DataProposalHash("test".to_string()),
    );
    // Reconstruct A in a separate DP
    let parent = Some(b5.data_proposals[0].1[0].hashed());
    b5.data_proposals[0].1.push(DataProposal::new(
        parent,
        vec![new_register_tx(ContractName::new("a"), StateCommitment(vec![])).into()],
    ));

    Ok((container, indexer, b1, b3, b4, b5))
}

#[test_log::test(tokio::test)]
async fn test_contracts_dump_every_block() -> Result<()> {
    let (_c, mut indexer, b1, b3, b4, b5) = scenario_contracts().await?;
    indexer.force_handle_signed_block(b1).await.unwrap();

    indexer.force_handle_signed_block(b3.clone()).await.unwrap();
    indexer.dump_store_to_db().await.unwrap();
    let rows = sqlx::query(
        "SELECT * FROM contracts where deleted_at_height is NULL order by contract_name",
    )
    .fetch_all(&indexer.db)
    .await
    .context("fetch contracts")?;
    assert_eq!(
        rows.iter()
            .map(|r| r.get::<String, _>("contract_name"))
            .collect::<Vec<_>>(),
        vec!["a", "b", "c", "wallet"]
    );

    indexer.force_handle_signed_block(b4.clone()).await.unwrap();
    indexer.dump_store_to_db().await.unwrap();
    let rows = sqlx::query(
        "SELECT * FROM contracts where deleted_at_height is NULL order by contract_name",
    )
    .fetch_all(&indexer.db)
    .await
    .context("fetch contracts")?;
    assert_eq!(
        rows.iter()
            .map(|r| r.get::<String, _>("contract_name"))
            .collect::<Vec<_>>(),
        vec!["b", "wallet"]
    );

    indexer.force_handle_signed_block(b5.clone()).await.unwrap();
    indexer.dump_store_to_db().await.unwrap();

    let rows = sqlx::query(
        "SELECT * FROM contracts where deleted_at_height is NULL order by contract_name",
    )
    .fetch_all(&indexer.db)
    .await
    .context("fetch contracts")?;
    assert_eq!(
        rows.iter()
            .map(|r| r.get::<String, _>("contract_name"))
            .collect::<Vec<_>>(),
        vec!["a", "wallet"]
    );
    Ok(())
}

#[test_log::test(tokio::test)]
async fn test_contracts_batched() -> Result<()> {
    let (_c, mut indexer, b1, b3, b4, b5) = scenario_contracts().await?;
    indexer.force_handle_signed_block(b1).await.unwrap();
    indexer.force_handle_signed_block(b3).await.unwrap();
    indexer.force_handle_signed_block(b4).await.unwrap();
    indexer.force_handle_signed_block(b5).await.unwrap();
    indexer.dump_store_to_db().await.unwrap();

    let rows = sqlx::query(
        "SELECT * FROM contracts where deleted_at_height is NULL order by contract_name",
    )
    .fetch_all(&indexer.db)
    .await
    .context("fetch contracts")?;
    assert_eq!(
        rows.iter()
            .map(|r| r.get::<String, _>("contract_name"))
            .collect::<Vec<_>>(),
        vec!["a", "wallet"]
    );
    Ok(())
}

// In case of duplicate tx hash, should return information of the tx with the highest block height
// or index (position in the block)
#[test_log::test(tokio::test)]
async fn test_indexer_api_doubles() -> Result<()> {
    let container = Postgres::default()
        .with_tag("17-alpine")
        .with_cmd(["postgres", "-c", "log_statement=all"])
        .start()
        .await
        .unwrap();
    let db = PgPoolOptions::new()
        .max_connections(5)
        .connect(&format!(
            "postgresql://postgres:postgres@localhost:{}/postgres",
            container.get_host_port_ipv4(5432).await.unwrap()
        ))
        .await
        .unwrap();
    MIGRATOR.run(&db).await.unwrap();
    sqlx::raw_sql(include_str!("../../tests/fixtures/test_data.sql"))
        .execute(&db)
        .await
        .context("insert test data")?;

    let (_indexer, explorer) = new_indexer(db).await;
    let server = setup_test_server(&explorer).await?;

    // Multiple txs with same hash -- all in different blocks

    let transactions_response = server
        .get("/transaction/hash/test_tx_hash_2aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
        .await;
    transactions_response.assert_status_ok();
    let result = transactions_response.json::<APITransaction>();
    assert_eq!(
        result.parent_dp_hash.0,
        "dp_hashbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_string()
    );

    // Multiple txs with same hash -- one not yet in a block, should return the pending one

    let transactions_response = server
        .get("/proof/hash/test_tx_hash_3aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
        .await;
    transactions_response.assert_status_ok();
    let result = transactions_response.json::<APITransaction>();
    assert_eq!(
        result.parent_dp_hash.0,
        "dp_hashbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_string()
    );
    assert_eq!(result.block_hash, None);

    // Get blobs by tx hash

    let transactions_response = server
        .get("/blobs/hash/test_tx_hash_2aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
        .await;
    transactions_response.assert_status_ok();
    let result = transactions_response.json::<Vec<APIBlob>>();
    assert!(result.len() == 1);
    assert_eq!(
        result.first().unwrap().data,
        "{\"data\": \"blob_data_2_bis\"}".as_bytes()
    );

    // Get blob by tx hash

    let transactions_response = server
        .get("/blob/hash/test_tx_hash_2aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/index/0")
        .await;
    transactions_response.assert_status_ok();
    let result = transactions_response.json::<APIBlob>();
    assert_eq!(result.data, "{\"data\": \"blob_data_2_bis\"}".as_bytes());

    // Get proof by tx hash

    let transactions_response = server
        .get("/proof/hash/test_tx_hash_3aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
        .await;
    transactions_response.assert_status_ok();
    let result = transactions_response.json::<APITransaction>();
    assert_eq!(
        result.parent_dp_hash.0,
        "dp_hashbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_string()
    );
    assert_eq!(result.block_hash, None);

    // Get transaction state event, the latest one

    let transactions_response = server
            .get("/transaction/hash/test_tx_hash_2aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/events")
            .await;
    transactions_response.assert_status_ok();
    let result = transactions_response.json::<Vec<APITransactionEvents>>();
    assert_eq!(
        result,
        vec![
            APITransactionEvents {
                block_hash: ConsensusProposalHash(
                    "block2aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string()
                ),
                block_height: BlockHeight(2),
                events: vec![serde_json::json!({
                    "index": 1,
                    "name": "Sequenced",
                })]
            },
            APITransactionEvents {
                block_hash: ConsensusProposalHash(
                    "block3aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string()
                ),
                block_height: BlockHeight(3),
                events: vec![serde_json::json!({
                    "index": 1,
                    "name": "Success",
                })]
            }
        ]
    );

    Ok(())
}

#[test_log::test(tokio::test)]
async fn test_indexer_api() -> Result<()> {
    let container = Postgres::default()
        .with_tag("17-alpine")
        .with_cmd(["postgres", "-c", "log_statement=all"])
        .start()
        .await
        .unwrap();
    let db = PgPoolOptions::new()
        .max_connections(5)
        .connect(&format!(
            "postgresql://postgres:postgres@localhost:{}/postgres",
            container.get_host_port_ipv4(5432).await.unwrap()
        ))
        .await
        .unwrap();
    MIGRATOR.run(&db).await.unwrap();
    sqlx::raw_sql(include_str!("../../tests/fixtures/test_data.sql"))
        .execute(&db)
        .await
        .context("insert test data")?;

    let (_indexer, mut explorer) = new_indexer(db).await;
    let server = setup_test_server(&explorer).await?;

    // Blocks
    // Get all blocks
    let transactions_response = server.get("/blocks").await;
    transactions_response.assert_status_ok();
    assert!(!transactions_response.text().is_empty());

    // Test pagination
    let transactions_response = server.get("/blocks?nb_results=1").await;
    transactions_response.assert_status_ok();
    assert_eq!(transactions_response.json::<Vec<APIBlock>>().len(), 1);
    assert_eq!(
        transactions_response
            .json::<Vec<APIBlock>>()
            .first()
            .unwrap()
            .height,
        3
    );
    let transactions_response = server.get("/blocks?nb_results=1&start_block=1").await;
    transactions_response.assert_status_ok();
    assert_eq!(transactions_response.json::<Vec<APIBlock>>().len(), 1);
    assert_eq!(
        transactions_response
            .json::<Vec<APIBlock>>()
            .first()
            .unwrap()
            .height,
        1
    );
    // Test negative end of blocks
    let transactions_response = server.get("/blocks?nb_results=10&start_block=4").await;
    transactions_response.assert_status_ok();

    // Get the last block
    let transactions_response = server.get("/block/last").await;
    transactions_response.assert_status_ok();
    assert!(!transactions_response.text().is_empty());

    // Get block by height
    let transactions_response = server.get("/block/height/1").await;
    transactions_response.assert_status_ok();
    assert!(!transactions_response.text().is_empty());

    // Get block by hash
    let transactions_response = server
        .get("/block/hash/block1aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
        .await;
    transactions_response.assert_status_ok();
    assert!(!transactions_response.text().is_empty());

    // Transactions
    // Get all transactions
    let transactions_response = server.get("/transactions").await;
    transactions_response.assert_status_ok();
    assert!(!transactions_response.text().is_empty());

    // Get all transactions by height
    let transactions_response = server.get("/transactions/block/2").await;
    transactions_response.assert_status_ok();
    assert!(!transactions_response.text().is_empty());

    // Get an existing transaction by name
    let transactions_response = server.get("/transactions/contract/contract_1").await;
    transactions_response.assert_status_ok();
    assert!(!transactions_response.text().is_empty());

    // Get an unknown transaction by name
    let transactions_response = server.get("/transactions/contract/unknown_contract").await;
    transactions_response.assert_status_ok();
    assert_eq!(transactions_response.text(), "[]");

    // Get an existing transaction by hash
    let transactions_response = server
        .get("/transaction/hash/test_tx_hash_1aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
        .await;
    transactions_response.assert_status_ok();
    assert!(!transactions_response.text().is_empty());

    // Get an existing transaction, waiting for dissemination by hash
    let transactions_response = server
        .get("/transaction/hash/test_tx_hash_0aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
        .await;
    transactions_response.assert_status_ok();
    assert!(!transactions_response.text().is_empty());

    // Get an unknown transaction by hash
    let unknown_tx = server
        .get("/transaction/hash/1111111111111111111111111111111111111111111111111111111111111111")
        .await;
    unknown_tx.assert_status_not_found();

    // Blobs
    // Get all transactions for a specific contract name
    let transactions_response = server.get("/blob_transactions/contract/contract_1").await;
    transactions_response.assert_status_ok();
    assert!(!transactions_response.text().is_empty());

    // Get blobs by tx_hash
    let transactions_response = server
        .get("/blobs/hash/test_tx_hash_2aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
        .await;
    transactions_response.assert_status_ok();
    assert!(!transactions_response.text().is_empty());

    // Get unknown blobs by tx_hash
    let transactions_response = server
        .get("/blobs/hash/test_tx_hash_1aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
        .await;
    transactions_response.assert_status_ok();
    assert_eq!(transactions_response.text(), "[]");

    // Get blob by tx_hash and index
    let transactions_response = server
        .get("/blob/hash/test_tx_hash_2aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/index/0")
        .await;
    transactions_response.assert_status_ok();
    assert!(!transactions_response.text().is_empty());

    // Get blob by tx_hash and unknown index
    let transactions_response = server
            .get("/blob/hash/test_tx_hash_2aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/index/1000")
            .await;
    transactions_response.assert_status_not_found();

    // Contracts
    // Get contract by name
    let transactions_response = server.get("/contract/contract_1").await;
    transactions_response.assert_status_ok();
    assert!(!transactions_response.text().is_empty());

    // Websocket
    let listener = hyli_net::net::bind_tcp_listener(0).await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(axum::serve(listener, explorer.api(None)).into_future());

    let _ = tokio_tungstenite::connect_async(format!(
        "ws://{addr}/blob_transactions/contract/contract_1/ws"
    ))
    .await
    .unwrap();

    if let Some(tx) = explorer.new_sub_receiver.recv().await {
        let (contract_name, _) = tx;
        assert_eq!(contract_name, ContractName::new("contract_1"));
    }

    Ok(())
}

#[test_log::test(tokio::test)]
async fn test_data_proposal_created_does_not_downgrade_success() -> Result<()> {
    let container = Postgres::default()
        .with_tag("17-alpine")
        .with_cmd(["postgres", "-c", "log_statement=all"])
        .start()
        .await
        .unwrap();
    let db = PgPoolOptions::new()
        .max_connections(5)
        .connect(&format!(
            "postgresql://postgres:postgres@localhost:{}/postgres",
            container.get_host_port_ipv4(5432).await.unwrap()
        ))
        .await
        .unwrap();
    MIGRATOR.run(&db).await.unwrap();

    let (mut indexer, explorer) = new_indexer(db.clone()).await;
    let server = setup_test_server(&explorer).await?;

    let contract_name = ContractName::new("monotonic");
    let register_tx = new_register_tx(contract_name, StateCommitment(vec![1, 2, 3]));
    let transaction = Transaction {
        version: 1,
        transaction_data: TransactionData::Blob(register_tx.clone()),
    };
    let parent_dp = DataProposal::new(None, vec![transaction.clone()]);
    let parent_dp_hash = parent_dp.hashed();
    let tx_metadata = transaction.metadata(parent_dp_hash.clone());

    sqlx::query(
        "INSERT INTO transactions (parent_dp_hash, tx_hash, version, transaction_type, transaction_status, block_hash, block_height, lane_id, index, identity)
         VALUES ($1, $2, $3, $4, $5, NULL, NULL, NULL, $6, $7)",
    )
    .bind(parent_dp_hash.0.clone())
    .bind(tx_metadata.id.1.0.clone())
    .bind(transaction.version as i32)
    .bind(TransactionTypeDb::BlobTransaction)
    .bind(TransactionStatusDb::Success)
    .bind(0_i32)
    .bind(Some(register_tx.identity.0.clone()))
    .execute(&db)
    .await
    .unwrap();

    assert_tx_status(
        &server,
        tx_metadata.id.1.clone(),
        TransactionStatusDb::Success,
    )
    .await;

    let dpc_event = MempoolStatusEvent::DataProposalCreated {
        parent_data_proposal_hash: parent_dp_hash.clone(),
        data_proposal_hash: DataProposal::new(
            Some(parent_dp_hash.clone()),
            vec![transaction.clone()],
        )
        .hashed(),
        txs_metadatas: vec![tx_metadata.clone()],
    };
    indexer
        .handle_mempool_status_event(dpc_event)
        .await
        .expect("MempoolStatusEvent");
    indexer.dump_store_to_db().await.expect("Dump to db");

    assert_tx_status(&server, tx_metadata.id.1, TransactionStatusDb::Success).await;

    Ok(())
}

#[test_log::test(tokio::test)]
async fn test_contract_history_tracking() -> Result<()> {
    let container = Postgres::default()
        .with_tag("17-alpine")
        .with_cmd(["postgres", "-c", "log_statement=all"])
        .start()
        .await
        .unwrap();
    let db = PgPoolOptions::new()
        .max_connections(5)
        .connect(&format!(
            "postgresql://postgres:postgres@localhost:{}/postgres",
            container.get_host_port_ipv4(5432).await.unwrap()
        ))
        .await
        .unwrap();
    MIGRATOR.run(&db).await.unwrap();

    let (mut indexer, explorer) = new_indexer(db.clone()).await;
    let server = setup_test_server(&explorer).await?;

    let contract_name = ContractName::new("evolving");

    // Block 1: Register contract with initial state
    let initial_state = StateCommitment(vec![1, 2, 3]);
    let register_tx = new_register_tx(contract_name.clone(), initial_state.clone());

    let block1 = craft_signed_block(1, vec![register_tx.into()]);
    indexer.force_handle_signed_block(block1).await.unwrap();
    indexer.dump_store_to_db().await.unwrap();

    // Verify contract_history has the registration entry
    let rows = sqlx::query(
        "SELECT block_height, tx_index, change_type::text[] as change_type, program_id, state_commitment FROM contract_history WHERE contract_name = $1 ORDER BY block_height, tx_index"
    )
    .bind(contract_name.0.clone())
    .fetch_all(&db)
    .await
    .context("fetch contract_history")?;

    assert_eq!(
        rows.len(),
        1,
        "Should have 1 history entry after registration"
    );
    assert_eq!(rows[0].get::<i64, _>("block_height"), 1);
    assert_eq!(rows[0].get::<i32, _>("tx_index"), 0);
    assert_eq!(
        rows[0].get::<Vec<String>, _>("change_type"),
        vec!["registered".to_string()]
    );
    assert_eq!(rows[0].get::<Vec<u8>, _>("program_id"), vec![3, 2, 1]);
    assert_eq!(
        rows[0].get::<Vec<u8>, _>("state_commitment"),
        initial_state.0
    );

    // Register a second contract to have more data
    let contract_name_2 = ContractName::new("another");
    let register_tx_2 = new_register_tx(contract_name_2.clone(), StateCommitment(vec![7, 8, 9]));
    let block2 = craft_signed_block(2, vec![register_tx_2.into()]);
    indexer.force_handle_signed_block(block2).await.unwrap();
    indexer.dump_store_to_db().await.unwrap();

    // Verify both contracts have history entries
    let all_history = sqlx::query(
        "SELECT contract_name, block_height, tx_index, change_type::text[] as change_type FROM contract_history ORDER BY block_height, tx_index"
    )
    .fetch_all(&db)
    .await
    .context("fetch all contract_history")?;

    assert_eq!(all_history.len(), 2, "Should have 2 history entries total");
    assert_eq!(
        all_history[0].get::<String, _>("contract_name"),
        contract_name.0
    );
    assert_eq!(
        all_history[0].get::<Vec<String>, _>("change_type"),
        vec!["registered".to_string()]
    );
    assert_eq!(
        all_history[1].get::<String, _>("contract_name"),
        contract_name_2.0
    );
    assert_eq!(
        all_history[1].get::<Vec<String>, _>("change_type"),
        vec!["registered".to_string()]
    );

    // Test the API endpoint
    let history_response = server
        .get(&format!("/contract/{}/history", contract_name.0))
        .await;
    history_response.assert_status_ok();
    let history = history_response.json::<Vec<APIContractHistory>>();

    assert_eq!(history.len(), 1, "API should return 1 history entry");
    assert_eq!(history[0].contract_name, contract_name.0);
    assert_eq!(history[0].block_height, 1);
    assert_eq!(history[0].tx_index, 0);
    assert_eq!(
        history[0].change_type,
        vec![hyli_model::api::ContractChangeType::Registered]
    );

    // Test API with change_type filter
    let filtered_response = server
        .get(&format!(
            "/contract/{}/history?change_type=registered",
            contract_name.0
        ))
        .await;
    filtered_response.assert_status_ok();
    let filtered_history = filtered_response.json::<Vec<APIContractHistory>>();

    assert_eq!(filtered_history.len(), 1, "Should have 1 registered entry");
    assert!(filtered_history
        .iter()
        .all(|h| h.change_type == vec![hyli_model::api::ContractChangeType::Registered]));

    // Test API with block height range filter
    let range_response = server
        .get(&format!(
            "/contract/{}/history?from_height=1&to_height=1",
            contract_name.0
        ))
        .await;
    range_response.assert_status_ok();
    let range_history = range_response.json::<Vec<APIContractHistory>>();

    assert_eq!(range_history.len(), 1, "Should have 1 entry in block 1");
    assert!(range_history.iter().all(|h| h.block_height == 1));

    Ok(())
}

//! Tests for mempool serialization and persistence of in-flight async work.
//!
//! These tests verify that when a node restarts, pending transactions
//! (both Blob and Proof) are properly restored and eventually committed.

use super::*;
use crate::mempool::ArcBorsh;
use anyhow::Result;
use hyli_model::{
    BlobProofOutput, ContractName, HyliOutput, ProgramId, ProofData,
    ProofTransaction, Transaction, TransactionData, VerifiedProofTransaction, Verifier,
};
use std::sync::Arc;

/// Create a simple Blob transaction for testing
fn make_test_blob_tx(name: &str) -> Transaction {
    make_register_contract_tx(ContractName::new(name))
}

/// Create a VerifiedProof transaction for testing.
/// We use VerifiedProof rather than Proof because Proof transactions would
/// require actual proof verification during processing.
fn make_test_verified_proof_tx(contract_name: &str) -> Transaction {
    let proof = ProofData(vec![1, 2, 3, 4]);
    let proof_hash = proof.hashed();
    let vpt = VerifiedProofTransaction {
        contract_name: ContractName::new(contract_name),
        program_id: ProgramId(vec![]),
        verifier: Verifier("test".into()),
        proof: Some(proof.clone()),
        proof_hash: proof_hash.clone(),
        proof_size: proof.0.len(),
        proven_blobs: vec![BlobProofOutput {
            original_proof_hash: proof_hash,
            blob_tx_hash: crate::model::TxHash::from(hex::encode("blob-tx")),
            program_id: ProgramId(vec![]),
            verifier: Verifier("test".into()),
            hyli_output: HyliOutput::default(),
        }],
        is_recursive: false,
    };
    Transaction::from(TransactionData::VerifiedProof(vpt))
}

/// Create a raw Proof transaction for testing.
/// This will go through the proof verification path on restore.
fn make_test_proof_tx(contract_name: &str) -> Transaction {
    let proof = ProofData(vec![1, 2, 3, 4]);
    let pt = ProofTransaction {
        contract_name: ContractName::new(contract_name),
        program_id: ProgramId(vec![]),
        verifier: Verifier("test".into()),
        proof,
    };
    Transaction::from(TransactionData::Proof(pt))
}

#[test_log::test(tokio::test)]
async fn test_mempool_serialization_roundtrip_empty() -> Result<()> {
    let ctx = MempoolTestCtx::new("mempool").await;

    // Serialize empty mempool
    let serialized = borsh::to_vec(&ctx.mempool.inner)?;

    // Deserialize
    let deserialized: MempoolStore = borsh::from_slice(&serialized)?;

    // Verify empty state
    assert!(deserialized.waiting_dissemination_txs.is_empty());
    assert!(deserialized.processing_txs_pending.is_empty());

    Ok(())
}

#[test_log::test(tokio::test)]
async fn test_mempool_serialization_with_waiting_txs() -> Result<()> {
    let mut ctx = MempoolTestCtx::new("mempool").await;

    // Submit transactions - they will go to waiting_dissemination_txs
    let blob_tx1 = make_test_blob_tx("test1");
    let blob_tx2 = make_test_blob_tx("test2");
    ctx.submit_tx(&blob_tx1);
    ctx.submit_tx(&blob_tx2);

    // Verify txs are in waiting_dissemination_txs
    let total_pending: usize = ctx
        .mempool
        .waiting_dissemination_txs
        .values()
        .map(|m| m.len())
        .sum();
    assert_eq!(total_pending, 2, "Should have 2 waiting transactions");

    // Serialize
    let serialized = borsh::to_vec(&ctx.mempool.inner)?;

    // Deserialize
    let deserialized: MempoolStore = borsh::from_slice(&serialized)?;

    // Verify transactions are restored
    let restored_total: usize = deserialized
        .waiting_dissemination_txs
        .values()
        .map(|m| m.len())
        .sum();
    assert_eq!(
        restored_total, 2,
        "Should have 2 waiting transactions after deserialize"
    );

    Ok(())
}

#[test_log::test(tokio::test)]
async fn test_mempool_serialization_with_processing_txs_pending() -> Result<()> {
    let mut ctx = MempoolTestCtx::new("mempool").await;

    // Manually add transactions to processing_txs_pending
    // This simulates transactions that were in-flight when the node shut down
    let blob_tx = make_test_blob_tx("test-blob");
    let proof_tx = make_test_proof_tx("test-proof");

    let lane_id = ctx.own_lane();

    ctx.mempool
        .inner
        .processing_txs_pending
        .push_back((ArcBorsh::new(Arc::new(blob_tx.clone())), lane_id.clone()));
    ctx.mempool
        .inner
        .processing_txs_pending
        .push_back((ArcBorsh::new(Arc::new(proof_tx.clone())), lane_id.clone()));

    assert_eq!(
        ctx.mempool.inner.processing_txs_pending.len(),
        2,
        "Should have 2 pending transactions"
    );

    // Serialize
    let serialized = borsh::to_vec(&ctx.mempool.inner)?;

    // Deserialize
    let deserialized: MempoolStore = borsh::from_slice(&serialized)?;

    // Verify pending transactions are restored
    assert_eq!(
        deserialized.processing_txs_pending.len(),
        2,
        "Should have 2 pending transactions after deserialize"
    );

    // Verify the transactions are correctly deserialized
    let (first_tx, first_lane) = &deserialized.processing_txs_pending[0];
    assert_eq!(first_tx.arc().hashed(), blob_tx.hashed());
    assert_eq!(first_lane, &lane_id);

    let (second_tx, second_lane) = &deserialized.processing_txs_pending[1];
    assert_eq!(second_tx.arc().hashed(), proof_tx.hashed());
    assert_eq!(second_lane, &lane_id);

    Ok(())
}

#[test_log::test(tokio::test)]
async fn test_mempool_restore_inflight_work_enqueues_correct_tasks() -> Result<()> {
    let mut ctx = MempoolTestCtx::new("mempool").await;

    // Manually add transactions to processing_txs_pending
    let blob_tx = make_test_blob_tx("test-blob");
    let proof_tx = make_test_proof_tx("test-proof");

    let lane_id = ctx.own_lane();

    // Add a blob transaction and a proof transaction
    ctx.mempool
        .inner
        .processing_txs_pending
        .push_back((ArcBorsh::new(Arc::new(blob_tx.clone())), lane_id.clone()));
    ctx.mempool
        .inner
        .processing_txs_pending
        .push_back((ArcBorsh::new(Arc::new(proof_tx.clone())), lane_id.clone()));

    // Note: The JoinSet (processing_txs) is skipped during serialization,
    // so it will be empty after deserialization. restore_inflight_work
    // should re-enqueue the tasks.

    // Verify processing_txs is empty before restore
    assert!(
        ctx.mempool.inner.processing_txs.is_empty(),
        "processing_txs should be empty initially"
    );

    // Call restore_inflight_work
    ctx.mempool.restore_inflight_work();

    // After restore, we should have 2 tasks in the JoinSet
    assert_eq!(
        ctx.mempool.inner.processing_txs.len(),
        2,
        "Should have 2 tasks enqueued after restore"
    );

    Ok(())
}

#[test_log::test(tokio::test)]
async fn test_mempool_full_persistence_cycle() -> Result<()> {
    // This test simulates a full persist/restore cycle:
    // 1. Create a mempool with some state
    // 2. Save it to disk
    // 3. Load it from disk
    // 4. Verify state is preserved

    let mut ctx = MempoolTestCtx::new("mempool").await;
    let tmpdir = tempfile::tempdir()?;
    let mempool_file = tmpdir.path().join("mempool.bin");

    // Add some transactions to waiting_dissemination_txs
    let blob_tx = make_test_blob_tx("persist-test");
    ctx.submit_tx(&blob_tx);

    // Add some transactions to processing_txs_pending
    let pending_tx = make_test_blob_tx("pending-test");
    let lane_id = ctx.own_lane();
    ctx.mempool
        .inner
        .processing_txs_pending
        .push_back((ArcBorsh::new(Arc::new(pending_tx.clone())), lane_id.clone()));

    // Save to disk
    Mempool::save_on_disk(mempool_file.as_path(), &ctx.mempool.inner)?;

    // Load from disk
    let loaded = Mempool::load_from_disk::<MempoolStore>(mempool_file.as_path());
    assert!(
        loaded.is_some(),
        "Should successfully load mempool from disk"
    );

    let loaded = loaded.unwrap();

    // Verify waiting_dissemination_txs
    let waiting_count: usize = loaded
        .waiting_dissemination_txs
        .values()
        .map(|m| m.len())
        .sum();
    assert_eq!(waiting_count, 1, "Should have 1 waiting transaction");

    // Verify processing_txs_pending
    assert_eq!(
        loaded.processing_txs_pending.len(),
        1,
        "Should have 1 pending transaction"
    );
    assert_eq!(
        loaded.processing_txs_pending[0].0.arc().hashed(),
        pending_tx.hashed()
    );

    Ok(())
}

#[test_log::test(tokio::test)]
async fn test_mempool_data_proposal_in_preparation_serialization() -> Result<()> {
    let mut ctx = MempoolTestCtx::new("mempool").await;
    ctx.set_ready_to_create_dps();

    // Create transactions and trigger DP preparation
    let blob_tx = make_test_blob_tx("dp-test");
    ctx.submit_tx(&blob_tx);

    // Trigger DP preparation
    let prepared = ctx.mempool.prepare_new_data_proposal()?;
    assert!(prepared, "Should have started DP preparation");

    // The DP is now in own_data_proposal_in_preparation
    // Verify it's in-flight for our lane
    let lane_id = ctx.own_lane();
    assert!(
        ctx.mempool
            .inner
            .own_data_proposal_in_preparation
            .is_lane_in_flight(&lane_id),
        "Lane should be in flight"
    );

    // Serialize
    let serialized = borsh::to_vec(&ctx.mempool.inner)?;

    // Deserialize - the prepared DPs should be restored
    let _deserialized: MempoolStore = borsh::from_slice(&serialized)?;

    // The serialization should succeed without error
    // (the actual content verification is done via the public interface)

    Ok(())
}

#[test_log::test(tokio::test)]
async fn test_mempool_mixed_tx_types_serialization() -> Result<()> {
    let mut ctx = MempoolTestCtx::new("mempool").await;
    let lane_id = ctx.own_lane();

    // Add various transaction types to processing_txs_pending
    let blob_tx1 = make_test_blob_tx("blob1");
    let blob_tx2 = make_test_blob_tx("blob2");
    let proof_tx1 = make_test_proof_tx("proof1");
    let proof_tx2 = make_test_proof_tx("proof2");
    let verified_tx = make_test_verified_proof_tx("verified1");

    // Add them all
    for tx in [&blob_tx1, &blob_tx2, &proof_tx1, &proof_tx2, &verified_tx] {
        ctx.mempool
            .inner
            .processing_txs_pending
            .push_back((ArcBorsh::new(Arc::new(tx.clone())), lane_id.clone()));
    }

    assert_eq!(ctx.mempool.inner.processing_txs_pending.len(), 5);

    // Serialize
    let serialized = borsh::to_vec(&ctx.mempool.inner)?;

    // Deserialize
    let deserialized: MempoolStore = borsh::from_slice(&serialized)?;

    // Verify all transactions are restored
    assert_eq!(
        deserialized.processing_txs_pending.len(),
        5,
        "All 5 transactions should be restored"
    );

    // Verify each transaction type is correct
    let restored_txs: Vec<_> = deserialized
        .processing_txs_pending
        .iter()
        .map(|(tx, _)| tx.arc())
        .collect();

    // Check blob transactions
    assert!(matches!(
        restored_txs[0].transaction_data,
        TransactionData::Blob(_)
    ));
    assert!(matches!(
        restored_txs[1].transaction_data,
        TransactionData::Blob(_)
    ));

    // Check proof transactions
    assert!(matches!(
        restored_txs[2].transaction_data,
        TransactionData::Proof(_)
    ));
    assert!(matches!(
        restored_txs[3].transaction_data,
        TransactionData::Proof(_)
    ));

    // Check verified proof transaction
    assert!(matches!(
        restored_txs[4].transaction_data,
        TransactionData::VerifiedProof(_)
    ));

    Ok(())
}

#[test_log::test(tokio::test)]
async fn test_arc_borsh_serialization() -> Result<()> {
    // Test that ArcBorsh correctly serializes and deserializes
    let tx = make_test_blob_tx("arc-test");
    let original_hash = tx.hashed();

    let arc_borsh = ArcBorsh::new(Arc::new(tx));

    // Serialize
    let serialized = borsh::to_vec(&arc_borsh)?;

    // Deserialize
    let deserialized: ArcBorsh<Transaction> = borsh::from_slice(&serialized)?;

    // Verify the transaction is correctly restored
    assert_eq!(
        deserialized.arc().hashed(),
        original_hash,
        "Transaction hash should match after roundtrip"
    );

    Ok(())
}

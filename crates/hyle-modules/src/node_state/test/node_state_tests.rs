use assertables::assert_err;
use sdk::hyle_model_utils::TimestampMs;

use super::*;

#[test_log::test(tokio::test)]
async fn happy_path_with_tx_context() {
    let mut state = new_node_state().await;
    let c1 = ContractName::new("c1");
    let register_c1 = make_register_contract_effect(c1.clone());
    state.handle_register_contract_effect(&register_c1);

    let identity = Identity::new("test@c1");
    let blob_tx = BlobTransaction::new(identity.clone(), vec![new_blob("c1")]);

    let blob_tx_id = blob_tx.hashed();

    let ctx = bogus_tx_context();
    state
        .handle_blob_tx(DataProposalHash::default(), &blob_tx, ctx.clone())
        .unwrap();

    let mut hyle_output = make_hyle_output(blob_tx.clone(), BlobIndex(0));
    hyle_output.tx_ctx = Some(ctx.clone());
    let verified_proof = new_proof_tx(&c1, &hyle_output, &blob_tx_id);
    // Modify something so it would fail.
    let mut ctx = ctx.clone();
    ctx.timestamp = TimestampMs(1234);
    hyle_output.tx_ctx = Some(ctx);
    let verified_proof_bad = new_proof_tx(&c1, &hyle_output, &blob_tx_id);

    let block =
        state.craft_block_and_handle(1, vec![verified_proof_bad.into(), verified_proof.into()]);
    assert_eq!(block.blob_proof_outputs.len(), 1);
    // We don't actually fail proof txs with blobs that fail
    assert_eq!(block.failed_txs.len(), 0);
    assert_eq!(block.successful_txs.len(), 1);

    assert_eq!(state.contracts.get(&c1).unwrap().state.0, vec![4, 5, 6]);
}

async fn assert_two_transactions_with_different_contracts_using_same_native_contract_settle_whatever_the_native_blob_output(
    native_failure: bool,
    blob_order_reversed: bool,
) {
    let mut state = new_node_state().await;
    let c1 = ContractName::new("c1");
    let register_c1 = make_register_contract_effect(c1.clone());
    let n1 = ContractName::new("sha3_256");
    let register_n1 = make_register_native_contract_effect(n1.clone());
    state.handle_register_contract_effect(&register_c1);
    state.handle_register_contract_effect(&register_n1);

    let identity_1 = Identity::new("test@c1");
    let mut blobs = vec![new_blob("c1")];

    if native_failure {
        blobs.push(new_failing_native_blob("sha3_256", identity_1.clone()));
    } else {
        blobs.push(new_native_blob("sha3_256", identity_1.clone()));
    }

    if blob_order_reversed {
        blobs.reverse();
    }

    let blob_tx_1 = BlobTransaction::new(identity_1.clone(), blobs);

    let blob_tx_id_1 = blob_tx_1.hashed();

    let ctx = bogus_tx_context();
    state
        .handle_blob_tx(DataProposalHash::default(), &blob_tx_1, ctx.clone())
        .unwrap();

    let hyle_output_1 = make_hyle_output(blob_tx_1.clone(), BlobIndex(0));
    let verified_proof_1 = new_proof_tx(&c1, &hyle_output_1, &blob_tx_id_1);

    // Register another tx depending on native contract

    let d1 = ContractName::new("d1");
    let register_d1 = make_register_contract_effect(d1.clone());
    state.handle_register_contract_effect(&register_d1);

    let identity_2 = Identity::new("test@d1");
    let blob_tx_2 = BlobTransaction::new(
        identity_2.clone(),
        vec![
            new_blob("d1"),
            new_native_blob("sha3_256", identity_2.clone()),
        ],
    );

    let blob_tx_id_2 = blob_tx_2.hashed();

    state
        .handle_blob_tx(DataProposalHash::default(), &blob_tx_2, ctx.clone())
        .unwrap();

    let hyle_output_2 = make_hyle_output(blob_tx_2.clone(), BlobIndex(0));
    let verified_proof_2 = new_proof_tx(&c1, &hyle_output_2, &blob_tx_id_2);

    // Create a block by settling the second tx without verifying the first
    // Native contract should not block
    let block = state.craft_block_and_handle(1, vec![verified_proof_2.into()]);
    assert_eq!(block.blob_proof_outputs.len(), 1);
    assert_eq!(block.failed_txs.len(), 0);
    assert_eq!(block.successful_txs.len(), 1);

    // Check state transitionned correctly
    assert_eq!(state.contracts.get(&d1).unwrap().state.0, vec![4, 5, 6]);

    // Now settle the first one
    let block = state.craft_block_and_handle(2, vec![verified_proof_1.into()]);
    assert_eq!(block.blob_proof_outputs.len(), 1);
    assert_eq!(block.failed_txs.len(), 0);
    assert_eq!(block.successful_txs.len(), 1);
    assert_eq!(state.contracts.get(&c1).unwrap().state.0, vec![4, 5, 6]);
}

#[test_log::test(tokio::test)]
async fn native_blobs_should_not_block_settlement_of_different_contracts_if_failure() {
    assert_two_transactions_with_different_contracts_using_same_native_contract_settle_whatever_the_native_blob_output(true, false);
}

#[test_log::test(tokio::test)]
async fn native_blobs_should_not_block_settlement_of_different_contracts_if_failure_reversed() {
    assert_two_transactions_with_different_contracts_using_same_native_contract_settle_whatever_the_native_blob_output(true, true);
}

#[test_log::test(tokio::test)]
async fn native_blobs_should_not_block_settlement_of_different_contracts_if_success() {
    assert_two_transactions_with_different_contracts_using_same_native_contract_settle_whatever_the_native_blob_output(false, false);
}

#[test_log::test(tokio::test)]
async fn native_blobs_should_not_block_settlement_of_different_contracts_if_success_reversed() {
    assert_two_transactions_with_different_contracts_using_same_native_contract_settle_whatever_the_native_blob_output(false, true);
}

async fn assert_two_transactions_with_same_contract_using_same_native_contract_settle_whatever_the_native_blob_output(
    native_failure: bool,
    blob_order_reversed: bool,
) {
    let mut state = new_node_state().await;
    let c1 = ContractName::new("c1");
    let register_c1 = make_register_contract_effect(c1.clone());
    state.handle_register_contract_effect(&register_c1);
    let n1 = ContractName::new("sha3_256");
    let register_n1 = make_register_native_contract_effect(n1.clone());
    state.handle_register_contract_effect(&register_n1);

    let identity_1 = Identity::new("test@c1");
    let identity_2 = Identity::new("test2@c1");

    let mut blobs = vec![new_blob("c1")];

    if native_failure {
        blobs.push(new_failing_native_blob("sha3_256", identity_1.clone()));
    } else {
        blobs.push(new_native_blob("sha3_256", identity_1.clone()));
    }

    if blob_order_reversed {
        blobs.reverse();
    }

    let blob_tx_1 = BlobTransaction::new(identity_1.clone(), blobs);
    let blob_tx_2 = BlobTransaction::new(
        identity_2.clone(),
        vec![
            new_blob("c1"),
            new_native_blob("sha3_256", identity_2.clone()),
        ],
    );

    let blob_tx_id_1 = blob_tx_1.hashed();
    let blob_tx_id_2 = blob_tx_2.hashed();

    let ctx = bogus_tx_context();

    let hyle_output_1 = make_hyle_output(
        blob_tx_1.clone(),
        if blob_order_reversed {
            // Provable blob is the last one of the tx
            BlobIndex(1)
        } else {
            BlobIndex(0)
        },
    );
    let verified_proof_1 = new_proof_tx(&c1, &hyle_output_1, &blob_tx_id_1);

    // Submit failing tx with native blob failing
    let block =
        state.craft_block_and_handle(1, vec![blob_tx_1.clone().into(), blob_tx_2.clone().into()]);
    assert_eq!(block.blob_proof_outputs.len(), 0);
    assert_eq!(block.failed_txs.len(), 0);
    assert_eq!(block.successful_txs.len(), 0);

    if native_failure {
        // Submitting a proof for c1 should do nothing (no settlement)
        let block = state.craft_block_and_handle(2, vec![verified_proof_1.clone().into()]);
        assert_eq!(block.blob_proof_outputs.len(), 1);
        assert_eq!(block.failed_txs.len(), 1);
        assert_eq!(block.successful_txs.len(), 0);

        assert_eq!(state.contracts.get(&c1).unwrap().state.0, vec![0, 1, 2, 3]);

        let hyle_output_2 = make_hyle_output(blob_tx_2.clone(), BlobIndex(0));
        let verified_proof_2 = new_proof_tx(&c1, &hyle_output_2, &blob_tx_id_2);

        let block = state.craft_block_and_handle(3, vec![verified_proof_2.into()]);
        // Check state did not transition

        // Settlement of the second tx should be ok
        assert_eq!(block.blob_proof_outputs.len(), 1);
        assert_eq!(block.failed_txs.len(), 0);
        assert_eq!(block.successful_txs.len(), 1);

        // Second tx should settle
        assert_eq!(state.contracts.get(&c1).unwrap().state.0, vec![4, 5, 6]);
    } else {
        let block = state.craft_block_and_handle(2, vec![verified_proof_1.clone().into()]);
        assert_eq!(block.blob_proof_outputs.len(), 1);
        assert_eq!(block.failed_txs.len(), 0);
        assert_eq!(block.successful_txs.len(), 1);

        assert_eq!(state.contracts.get(&c1).unwrap().state.0, vec![4, 5, 6]);

        let hyle_output_2 = make_hyle_output(blob_tx_2.clone(), BlobIndex(0));
        let verified_proof_2 = new_proof_tx(&c1, &hyle_output_2, &blob_tx_id_2);

        let block = state.craft_block_and_handle(3, vec![verified_proof_2.into()]);

        // verified_proof_2 was not right, the state of c1 was updated
        assert_eq!(state.contracts.get(&c1).unwrap().state.0, vec![4, 5, 6]);
    }
}

#[test_log::test(tokio::test)]
async fn native_blobs_should_not_block_settlement_of_one_contract_if_failure() {
    assert_two_transactions_with_same_contract_using_same_native_contract_settle_whatever_the_native_blob_output(true, false).await;
}

#[test_log::test(tokio::test)]
async fn native_blobs_should_not_block_settlement_of_one_contract_if_failure_reversed() {
    assert_two_transactions_with_same_contract_using_same_native_contract_settle_whatever_the_native_blob_output(true, true).await;
}

#[test_log::test(tokio::test)]
async fn native_blobs_should_not_block_settlement_of_one_contract_if_success() {
    assert_two_transactions_with_same_contract_using_same_native_contract_settle_whatever_the_native_blob_output(false, false).await;
}

#[test_log::test(tokio::test)]
async fn native_blobs_should_not_block_settlement_of_one_contract_if_success_reversed() {
    assert_two_transactions_with_same_contract_using_same_native_contract_settle_whatever_the_native_blob_output(false, true).await;
}

#[test_log::test(tokio::test)]
async fn native_blobs_should_fail_tx_if_failure_if_regular_blob_settled_as_failed() {
    let mut state = new_node_state().await;

    let c1 = ContractName::new("c1");
    let register_c1 = make_register_contract_effect(c1.clone());
    state.handle_register_contract_effect(&register_c1);

    let n1 = ContractName::new("sha3_256");
    let register_n1 = make_register_native_contract_effect(n1.clone());
    state.handle_register_contract_effect(&register_n1);

    let identity_1 = Identity::new("test@c1");
    let blob_tx_1 = BlobTransaction::new(
        identity_1.clone(),
        vec![
            new_blob("c1"),
            new_native_blob("sha3_256", identity_1.clone()),
        ],
    );

    let blob_tx_id_1 = blob_tx_1.hashed();

    let ctx = bogus_tx_context();

    let mut hyle_output_1 = make_hyle_output(blob_tx_1.clone(), BlobIndex(0));
    hyle_output_1.success = false;
    let verified_proof_1 = new_proof_tx(&c1, &hyle_output_1, &blob_tx_id_1);

    // Submit failing tx with native blob failing
    let block = state.craft_block_and_handle(1, vec![blob_tx_1.clone().into()]);
    assert_eq!(block.blob_proof_outputs.len(), 0);
    assert_eq!(block.failed_txs.len(), 0);
    assert_eq!(block.successful_txs.len(), 0);

    // Submitting a proof for c1 should do nothing (no settlement)
    let block = state.craft_block_and_handle(2, vec![verified_proof_1.clone().into()]);
    assert_eq!(block.blob_proof_outputs.len(), 1);
    assert_eq!(block.failed_txs.len(), 1);
    assert_eq!(block.successful_txs.len(), 0);

    // Check state did not transition
    assert_eq!(state.contracts.get(&c1).unwrap().state.0, vec![0, 1, 2, 3]);
}

// Create a native blob with index 1, with a blob on index 2
#[test_log::test(tokio::test)]
async fn native_blobs_dont_mess_blob_indexes() {
    let mut state = new_node_state().await;

    let c1 = ContractName::new("c1");
    let register_c1 = make_register_contract_effect(c1.clone());
    state.handle_register_contract_effect(&register_c1);

    let n1 = ContractName::new("sha3_256");
    let register_n1 = make_register_native_contract_effect(n1.clone());
    state.handle_register_contract_effect(&register_n1);

    let identity_1 = Identity::new("test@c1");
    let blob_tx_1 = BlobTransaction::new(
        identity_1.clone(),
        vec![
            new_blob("c1"),
            new_native_blob("sha3_256", identity_1.clone()),
            new_blob("c1"),
        ],
    );

    let blob_tx_id_1 = blob_tx_1.hashed();

    let ctx = bogus_tx_context();

    // Transition state from 0123 to 456
    let mut hyle_output_1 = make_hyle_output(blob_tx_1.clone(), BlobIndex(0));
    let verified_proof_1 = new_proof_tx(&c1, &hyle_output_1, &blob_tx_id_1);

    // Transition state from 456 to 789 in same tx (blob after successful native one)
    let mut hyle_output_2 = make_hyle_output_bis(blob_tx_1.clone(), BlobIndex(2));
    let verified_proof_2 = new_proof_tx(&c1, &hyle_output_2, &blob_tx_id_1);

    // Submit tx with successful native blob
    let block = state.craft_block_and_handle(1, vec![blob_tx_1.clone().into()]);
    assert_eq!(block.blob_proof_outputs.len(), 0);
    assert_eq!(block.failed_txs.len(), 0);
    assert_eq!(block.successful_txs.len(), 0);

    // Submitting proofs for blob 0 and 2, on same contract, should settle.
    let block = state.craft_block_and_handle(
        2,
        vec![
            verified_proof_1.clone().into(),
            verified_proof_2.clone().into(),
        ],
    );
    assert_eq!(block.blob_proof_outputs.len(), 2);
    assert_eq!(block.failed_txs.len(), 0);
    assert_eq!(block.successful_txs.len(), 1);

    // Check state did not transition twice
    assert_eq!(state.contracts.get(&c1).unwrap().state.0, vec![7, 8, 9]);
}

#[test_log::test(tokio::test)]
async fn iterate_over_blobs_in_the_right_order() {
    let mut state = new_node_state().await;

    let c1 = ContractName::new("c1");
    let mut register_c1 = make_register_contract_effect(c1.clone());
    register_c1.state_commitment = StateCommitment(vec![0]);
    state.handle_register_contract_effect(&register_c1);

    let c2 = ContractName::new("c2");
    let mut register_c2 = make_register_contract_effect(c2.clone());
    register_c2.state_commitment = StateCommitment(vec![0]);
    state.handle_register_contract_effect(&register_c2);

    let identity_1 = Identity::new("test@c1");

    let nb_blobs = 10;

    let blob_tx = BlobTransaction::new(
        identity_1.clone(),
        (0..nb_blobs)
            .flat_map(|_| vec![new_blob("c1"), new_blob("c2")])
            .collect::<Vec<_>>(),
    );

    let blob_tx_id = blob_tx.hashed();

    let ctx = bogus_tx_context();

    let create_verified_proofs = |index: usize| -> Vec<VerifiedProofTransaction> {
        let mut hyle_output = make_hyle_output(blob_tx.clone(), BlobIndex(2 * index));
        hyle_output.initial_state = StateCommitment(vec![index as u8]);
        hyle_output.next_state = StateCommitment(vec![(index + 1) as u8]);
        let p1 = new_proof_tx(&c1, &hyle_output, &blob_tx_id);

        let mut hyle_output = make_hyle_output(blob_tx.clone(), BlobIndex(2 * index + 1));
        hyle_output.initial_state = StateCommitment(vec![index as u8]);
        hyle_output.next_state = StateCommitment(vec![(index + 1) as u8]);
        let p2 = new_proof_tx(&c1, &hyle_output, &blob_tx_id);

        vec![p1, p2]
    };

    // Submit tx with successful native blob
    let block = state.craft_block_and_handle(1, vec![blob_tx.clone().into()]);
    assert_eq!(block.blob_proof_outputs.len(), 0);
    assert_eq!(block.failed_txs.len(), 0);
    assert_eq!(block.successful_txs.len(), 0);

    // Submitting proofs for blob 0 and 2, on same contract, should settle.
    let block = state.craft_block_and_handle(
        2,
        (0..nb_blobs)
            .flat_map(create_verified_proofs)
            .map(|el| el.into())
            .collect::<Vec<_>>(),
    );
    assert_eq!(block.blob_proof_outputs.len(), 2 * nb_blobs);
    assert_eq!(block.failed_txs.len(), 0);
    assert_eq!(block.successful_txs.len(), 1);

    // Check state did not transition twice
    assert_eq!(
        state.contracts.get(&c1).unwrap().state.0,
        vec![nb_blobs as u8]
    );
    assert_eq!(
        state.contracts.get(&c2).unwrap().state.0,
        vec![nb_blobs as u8]
    );
}

#[test_log::test(tokio::test)]
async fn blob_tx_without_blobs() {
    let mut state = new_node_state().await;
    let identity = Identity::new("test@c1");

    let blob_tx = BlobTransaction::new(identity.clone(), vec![]);

    assert_err!(state.handle_blob_tx(DataProposalHash::default(), &blob_tx, bogus_tx_context()));
}

#[test_log::test(tokio::test)]
async fn blob_tx_with_incorrect_identity() {
    let mut state = new_node_state().await;
    let identity = Identity::new("incorrect_id");

    let blob_tx = BlobTransaction::new(identity.clone(), vec![new_blob("test")]);

    assert_err!(state.handle_blob_tx(DataProposalHash::default(), &blob_tx, bogus_tx_context()));
}

#[test_log::test(tokio::test)]
async fn two_proof_for_one_blob_tx() {
    let mut state = new_node_state().await;
    let c1 = ContractName::new("c1");
    let c2 = ContractName::new("c2");
    let identity = Identity::new("test@c1");

    let register_c1 = make_register_contract_effect(c1.clone());
    let register_c2 = make_register_contract_effect(c2.clone());

    let blob_tx = BlobTransaction::new(identity.clone(), vec![new_blob(&c1.0), new_blob(&c2.0)]);

    let blob_tx_hash = blob_tx.hashed();

    state.handle_register_contract_effect(&register_c1);
    state.handle_register_contract_effect(&register_c2);
    state
        .handle_blob_tx(DataProposalHash::default(), &blob_tx, bogus_tx_context())
        .unwrap();

    let hyle_output_c1 = make_hyle_output(blob_tx.clone(), BlobIndex(0));

    let verified_proof_c1 = new_proof_tx(&c1, &hyle_output_c1, &blob_tx_hash);

    let hyle_output_c2 = make_hyle_output(blob_tx.clone(), BlobIndex(1));

    let verified_proof_c2 = new_proof_tx(&c2, &hyle_output_c2, &blob_tx_hash);

    state.craft_block_and_handle(10, vec![verified_proof_c1.into(), verified_proof_c2.into()]);

    assert_eq!(state.contracts.get(&c1).unwrap().state.0, vec![4, 5, 6]);
    assert_eq!(state.contracts.get(&c2).unwrap().state.0, vec![4, 5, 6]);
}

#[test_log::test(tokio::test)]
async fn multiple_failing_proofs() {
    let mut state = new_node_state().await;
    let c1 = ContractName::new("c1");
    let identity = Identity::new("test@c1");
    let register_c1 = make_register_contract_effect(c1.clone());
    state.handle_register_contract_effect(&register_c1);

    let blob_tx = BlobTransaction::new("test1@c1", vec![new_blob(&c1.0)]);
    let blob_tx_hash = blob_tx.hashed();
    let blob_tx_2 = BlobTransaction::new("test2@c1", vec![new_blob(&c1.0)]);
    let blob_tx_2_hash = blob_tx_2.hashed();
    let blob_tx_3 = BlobTransaction::new("test3@c1", vec![new_blob(&c1.0)]);
    let blob_tx_3_hash = blob_tx_3.hashed();

    let mut hyle_output_1 = make_hyle_output(blob_tx.clone(), BlobIndex(0));
    hyle_output_1.success = false;
    let verified_proof_1 = new_proof_tx(&c1, &hyle_output_1, &blob_tx_hash);
    let mut hyle_output_2 = make_hyle_output(blob_tx_2.clone(), BlobIndex(0));
    hyle_output_2.success = false;
    let verified_proof_2 = new_proof_tx(&c1, &hyle_output_2, &blob_tx_2_hash);
    let mut hyle_output_3 = make_hyle_output(blob_tx_3.clone(), BlobIndex(0));
    hyle_output_3.success = false;
    let verified_proof_3 = new_proof_tx(&c1, &hyle_output_3, &blob_tx_3_hash);

    let res = state.craft_block_and_handle(
        10,
        vec![
            blob_tx.clone().into(),
            blob_tx_2.clone().into(),
            verified_proof_2.into(),
            blob_tx_3.clone().into(),
            verified_proof_3.into(),
            verified_proof_1.into(),
        ],
    );

    assert_eq!(res.failed_txs.len(), 3);
}

#[test_log::test(tokio::test)]
async fn wrong_blob_index_for_contract() {
    let mut state = new_node_state().await;
    let c1 = ContractName::new("c1");
    let c2 = ContractName::new("c2");

    let register_c1 = make_register_contract_effect(c1.clone());
    let register_c2 = make_register_contract_effect(c2.clone());

    let blob_tx_1 = BlobTransaction::new(
        Identity::new("test@c1"),
        vec![new_blob(&c1.0), new_blob(&c2.0)],
    );
    let blob_tx_hash_1 = blob_tx_1.hashed();

    state.handle_register_contract_effect(&register_c1);
    state.handle_register_contract_effect(&register_c2);
    state
        .handle_blob_tx(DataProposalHash::default(), &blob_tx_1, bogus_tx_context())
        .unwrap();

    let hyle_output_c1 = make_hyle_output(blob_tx_1.clone(), BlobIndex(1)); // Wrong index

    let verified_proof_c1 = new_proof_tx(&c1, &hyle_output_c1, &blob_tx_hash_1);

    state.craft_block_and_handle(10, vec![verified_proof_c1.into()]);

    // Check that we did not settle
    assert_eq!(state.contracts.get(&c1).unwrap().state.0, vec![0, 1, 2, 3]);
    assert_eq!(state.contracts.get(&c2).unwrap().state.0, vec![0, 1, 2, 3]);
}

#[test_log::test(tokio::test)]
async fn two_proof_for_same_blob() {
    let mut state = new_node_state().await;
    let c1 = ContractName::new("c1");
    let c2 = ContractName::new("c2");

    let register_c1 = make_register_contract_effect(c1.clone());
    let register_c2 = make_register_contract_effect(c2.clone());

    let blob_tx = BlobTransaction::new(
        Identity::new("test@c1"),
        vec![new_blob(&c1.0), new_blob(&c2.0)],
    );
    let blob_tx_hash = blob_tx.hashed();

    state.handle_register_contract_effect(&register_c1);
    state.handle_register_contract_effect(&register_c2);
    state
        .handle_blob_tx(DataProposalHash::default(), &blob_tx, bogus_tx_context())
        .unwrap();

    let hyle_output_c1 = make_hyle_output(blob_tx.clone(), BlobIndex(0));

    let verified_proof_c1 = new_proof_tx(&c1, &hyle_output_c1, &blob_tx_hash);

    state.craft_block_and_handle(
        10,
        vec![verified_proof_c1.clone().into(), verified_proof_c1.into()],
    );

    assert_eq!(
        state
            .unsettled_transactions
            .get(&blob_tx_hash)
            .unwrap()
            .blobs
            .get(&BlobIndex(0))
            .unwrap()
            .possible_proofs
            .len(),
        2
    );
    // Check that we did not settled
    assert_eq!(state.contracts.get(&c1).unwrap().state.0, vec![0, 1, 2, 3]);
    assert_eq!(state.contracts.get(&c2).unwrap().state.0, vec![0, 1, 2, 3]);
}

#[test_log::test(tokio::test)]
async fn two_proof_with_some_invalid_blob_proof_output() {
    let mut state = new_node_state().await;
    let c1 = ContractName::new("c1");

    let register_c1 = make_register_contract_effect(c1.clone());

    let blob_tx = BlobTransaction::new(
        Identity::new("test@c1"),
        vec![new_blob(&c1.0), new_blob(&c1.0)],
    );

    let blob_tx_hash = blob_tx.hashed();

    state.handle_register_contract_effect(&register_c1);
    state
        .handle_blob_tx(DataProposalHash::default(), &blob_tx, bogus_tx_context())
        .unwrap();

    let hyle_output = make_hyle_output(blob_tx.clone(), BlobIndex(0));
    let verified_proof = new_proof_tx(&c1, &hyle_output, &blob_tx_hash);
    let invalid_output = make_hyle_output(blob_tx.clone(), BlobIndex(4));
    let mut invalid_verified_proof = new_proof_tx(&c1, &invalid_output, &blob_tx_hash);

    invalid_verified_proof
        .proven_blobs
        .insert(0, verified_proof.proven_blobs.first().unwrap().clone());

    let block = state.craft_block_and_handle(5, vec![invalid_verified_proof.into()]);

    // We don't fail.
    assert_eq!(block.failed_txs.len(), 0);
    // We only store one of the two.
    assert_eq!(block.blob_proof_outputs.len(), 1);
}

#[test_log::test(tokio::test)]
async fn settle_with_multiple_state_reads() {
    let mut state = new_node_state().await;
    let c1 = ContractName::new("c1");
    let c2 = ContractName::new("c2");

    state.craft_block_and_handle(
        10,
        vec![
            make_register_contract_tx(c1.clone()).into(),
            make_register_contract_tx(c2.clone()).into(),
        ],
    );

    let blob_tx = BlobTransaction::new(Identity::new("test@c1"), vec![new_blob(&c1.0)]);
    let mut ho = make_hyle_output(blob_tx.clone(), BlobIndex(0));
    // Add an incorrect state read
    ho.state_reads
        .push((c2.clone(), StateCommitment(vec![9, 8, 7])));

    let effects = state.craft_block_and_handle(
        11,
        vec![
            blob_tx.clone().into(),
            new_proof_tx(&c1, &ho, &blob_tx.hashed()).into(),
        ],
    );

    assert!(effects
        .transactions_events
        .get(&blob_tx.hashed())
        .unwrap()
        .iter()
        .any(|e| {
            let TransactionStateEvent::SettleEvent(errmsg) = e else {
                return false;
            };
            errmsg.contains("does not match other contract state")
        }));

    let mut ho = make_hyle_output(blob_tx.clone(), BlobIndex(0));
    // Now correct state reads (some redundant ones to validate that this works)
    ho.state_reads
        .push((c2.clone(), state.contracts.get(&c2).unwrap().state.clone()));
    ho.state_reads
        .push((c2.clone(), state.contracts.get(&c2).unwrap().state.clone()));
    ho.state_reads
        .push((c1.clone(), state.contracts.get(&c1).unwrap().state.clone()));

    let effects =
        state.craft_block_and_handle(12, vec![new_proof_tx(&c1, &ho, &blob_tx.hashed()).into()]);
    assert_eq!(effects.blob_proof_outputs.len(), 1);
    assert_eq!(effects.successful_txs.len(), 1);
    assert_eq!(state.contracts.get(&c1).unwrap().state.0, vec![4, 5, 6]);
}

#[test_log::test(tokio::test)]
async fn change_same_contract_state_multiple_times_in_same_tx() {
    let mut state = new_node_state().await;
    let c1 = ContractName::new("c1");

    let register_c1 = make_register_contract_effect(c1.clone());

    let first_blob = new_blob(&c1.0);
    let second_blob = new_blob(&c1.0);
    let third_blob = new_blob(&c1.0);

    let blob_tx = BlobTransaction::new(
        Identity::new("test@c1"),
        vec![first_blob, second_blob, third_blob],
    );
    let blob_tx_hash = blob_tx.hashed();

    state.handle_register_contract_effect(&register_c1);
    state
        .handle_blob_tx(DataProposalHash::default(), &blob_tx, bogus_tx_context())
        .unwrap();

    let first_hyle_output = make_hyle_output(blob_tx.clone(), BlobIndex(0));

    let verified_first_proof = new_proof_tx(&c1, &first_hyle_output, &blob_tx_hash);

    let mut second_hyle_output = make_hyle_output(blob_tx.clone(), BlobIndex(1));
    second_hyle_output.initial_state = first_hyle_output.next_state.clone();
    second_hyle_output.next_state = StateCommitment(vec![7, 8, 9]);

    let verified_second_proof = new_proof_tx(&c1, &second_hyle_output, &blob_tx_hash);

    let mut third_hyle_output = make_hyle_output(blob_tx.clone(), BlobIndex(2));
    third_hyle_output.initial_state = second_hyle_output.next_state.clone();
    third_hyle_output.next_state = StateCommitment(vec![10, 11, 12]);

    let verified_third_proof = new_proof_tx(&c1, &third_hyle_output, &blob_tx_hash);

    state.craft_block_and_handle(
        10,
        vec![
            verified_first_proof.into(),
            verified_second_proof.into(),
            verified_third_proof.into(),
        ],
    );

    // Check that we did settled with the last state
    assert_eq!(state.contracts.get(&c1).unwrap().state.0, vec![10, 11, 12]);
}

#[test_log::test(tokio::test)]
async fn dead_end_in_proving_settles_still() {
    let mut state = new_node_state().await;

    let c1 = ContractName::new("c1");
    let register_c1 = make_register_contract_effect(c1.clone());

    let first_blob = new_blob(&c1.0);
    let second_blob = new_blob(&c1.0);
    let third_blob = new_blob(&c1.0);
    let blob_tx = BlobTransaction::new(
        Identity::new("test@c1"),
        vec![first_blob, second_blob, third_blob],
    );

    let blob_tx_hash = blob_tx.hashed();

    state.handle_register_contract_effect(&register_c1);
    state
        .handle_blob_tx(DataProposalHash::default(), &blob_tx, bogus_tx_context())
        .unwrap();

    // The test is that we send a proof for the first blob, then a proof the second blob with next_state B,
    // then a proof for the second blob with next_state C, then a proof for the third blob with initial_state C,
    // and it should settle, ignoring the initial 'dead end'.

    let first_proof_tx = new_proof_tx(
        &c1,
        &make_hyle_output_with_state(blob_tx.clone(), BlobIndex(0), &[0, 1, 2, 3], &[2]),
        &blob_tx_hash,
    );

    let second_proof_tx_b = new_proof_tx(
        &c1,
        &make_hyle_output_with_state(blob_tx.clone(), BlobIndex(1), &[2], &[3]),
        &blob_tx_hash,
    );

    let second_proof_tx_c = new_proof_tx(
        &c1,
        &make_hyle_output_with_state(blob_tx.clone(), BlobIndex(1), &[2], &[4]),
        &blob_tx_hash,
    );

    let third_proof_tx = new_proof_tx(
        &c1,
        &make_hyle_output_with_state(blob_tx.clone(), BlobIndex(2), &[4], &[5]),
        &blob_tx_hash,
    );

    let block = state.craft_block_and_handle(
        4,
        vec![
            first_proof_tx.into(),
            second_proof_tx_b.into(),
            second_proof_tx_c.into(),
            third_proof_tx.into(),
        ],
    );

    assert_eq!(
        block
            .verified_blobs
            .iter()
            .map(|(_, _, idx)| idx.unwrap())
            .collect::<Vec<_>>(),
        vec![0, 1, 0]
    );
    // Check that we did settled with the last state
    assert_eq!(state.contracts.get(&c1).unwrap().state.0, vec![5]);
}

#[test_log::test(tokio::test)]
async fn duplicate_proof_with_inconsistent_state_should_never_settle() {
    let mut state = new_node_state().await;
    let c1 = ContractName::new("c1");

    let register_c1 = make_register_contract_effect(c1.clone());

    let first_blob = new_blob(&c1.0);
    let second_blob = new_blob(&c1.0);

    let blob_tx = BlobTransaction::new(Identity::new("test@c1"), vec![first_blob, second_blob]);

    let blob_tx_hash = blob_tx.hashed();

    state.handle_register_contract_effect(&register_c1);
    state
        .handle_blob_tx(DataProposalHash::default(), &blob_tx, bogus_tx_context())
        .unwrap();

    // Create legitimate proof for Blob1
    let first_hyle_output = make_hyle_output(blob_tx.clone(), BlobIndex(0));
    let verified_first_proof = new_proof_tx(&c1, &first_hyle_output, &blob_tx_hash);

    // Create hacky proof for Blob1
    let mut another_first_hyle_output = make_hyle_output(blob_tx.clone(), BlobIndex(0));
    another_first_hyle_output.initial_state = first_hyle_output.next_state.clone();
    another_first_hyle_output.next_state = first_hyle_output.initial_state.clone();

    let another_verified_first_proof = new_proof_tx(&c1, &another_first_hyle_output, &blob_tx_hash);

    let mut second_hyle_output = make_hyle_output(blob_tx.clone(), BlobIndex(1));
    second_hyle_output.initial_state = another_first_hyle_output.next_state.clone();
    second_hyle_output.next_state = StateCommitment(vec![7, 8, 9]);

    let verified_second_proof = new_proof_tx(&c1, &second_hyle_output, &blob_tx_hash);

    state.craft_block_and_handle(
        10,
        vec![
            verified_first_proof.into(),
            another_verified_first_proof.into(),
            verified_second_proof.into(),
        ],
    );

    // Check that we did not settled
    assert_eq!(state.contracts.get(&c1).unwrap().state.0, vec![0, 1, 2, 3]);
}

#[test_log::test(tokio::test)]
async fn duplicate_proof_with_inconsistent_state_should_never_settle_another() {
    let mut state = new_node_state().await;
    let c1 = ContractName::new("c1");

    let register_c1 = make_register_contract_effect(c1.clone());

    let first_blob = new_blob(&c1.0);
    let second_blob = new_blob(&c1.0);
    let third_blob = new_blob(&c1.0);

    let blob_tx = BlobTransaction::new(
        Identity::new("test@c1"),
        vec![first_blob, second_blob, third_blob],
    );

    let blob_tx_hash = blob_tx.hashed();

    state.handle_register_contract_effect(&register_c1);
    state
        .handle_blob_tx(DataProposalHash::default(), &blob_tx, bogus_tx_context())
        .unwrap();

    // Create legitimate proof for Blob1
    let first_hyle_output = make_hyle_output(blob_tx.clone(), BlobIndex(0));
    let verified_first_proof = new_proof_tx(&c1, &first_hyle_output, &blob_tx_hash);

    let mut second_hyle_output = make_hyle_output(blob_tx.clone(), BlobIndex(1));
    second_hyle_output.initial_state = first_hyle_output.next_state.clone();
    second_hyle_output.next_state = StateCommitment(vec![7, 8, 9]);

    let verified_second_proof = new_proof_tx(&c1, &second_hyle_output, &blob_tx_hash);

    let mut third_hyle_output = make_hyle_output(blob_tx.clone(), BlobIndex(2));
    third_hyle_output.initial_state = first_hyle_output.next_state.clone();
    third_hyle_output.next_state = StateCommitment(vec![10, 11, 12]);

    let verified_third_proof = new_proof_tx(&c1, &third_hyle_output, &blob_tx_hash);

    state.craft_block_and_handle(
        10,
        vec![
            verified_first_proof.into(),
            verified_second_proof.into(),
            verified_third_proof.into(),
        ],
    );

    // Check that we did not settled
    assert_eq!(state.contracts.get(&c1).unwrap().state.0, vec![0, 1, 2, 3]);
}

#[test_log::test(tokio::test)]
async fn test_auto_settle_next_txs_after_settle() {
    let mut state = new_node_state().await;

    let c1 = ContractName::new("c1");
    let c2 = ContractName::new("c2");
    let register_c1 = make_register_contract_tx(c1.clone());
    let register_c2 = make_register_contract_tx(c2.clone());

    // Add four transactions - A blocks B/C, B blocks D.
    // Send proofs for B, C, D before A.
    let tx_a = BlobTransaction::new(
        Identity::new("test@c1"),
        vec![new_blob(&c1.0), new_blob(&c2.0)],
    );
    let tx_b = BlobTransaction::new(Identity::new("test@c1"), vec![new_blob(&c1.0)]);
    let tx_c = BlobTransaction::new(Identity::new("test@c2"), vec![new_blob(&c2.0)]);
    let tx_d = BlobTransaction::new(Identity::new("test2@c1"), vec![new_blob(&c1.0)]);

    let tx_a_hash = tx_a.hashed();
    let hyle_output = make_hyle_output_with_state(tx_a.clone(), BlobIndex(0), &[0, 1, 2, 3], &[12]);
    let tx_a_proof_1 = new_proof_tx(&c1, &hyle_output, &tx_a_hash);
    let hyle_output = make_hyle_output_with_state(tx_a.clone(), BlobIndex(1), &[0, 1, 2, 3], &[22]);
    let tx_a_proof_2 = new_proof_tx(&c2, &hyle_output, &tx_a_hash);

    let tx_b_hash = tx_b.hashed();
    let hyle_output = make_hyle_output_with_state(tx_b.clone(), BlobIndex(0), &[12], &[13]);
    let tx_b_proof = new_proof_tx(&c1, &hyle_output, &tx_b_hash);

    let tx_c_hash = tx_c.hashed();
    let hyle_output = make_hyle_output_with_state(tx_c.clone(), BlobIndex(0), &[22], &[23]);
    let tx_c_proof = new_proof_tx(&c1, &hyle_output, &tx_c_hash);

    let tx_d_hash = tx_d.hashed();
    let hyle_output = make_hyle_output_with_state(tx_d.clone(), BlobIndex(0), &[13], &[14]);
    let tx_d_proof = new_proof_tx(&c1, &hyle_output, &tx_d_hash);

    state.craft_block_and_handle(
        104,
        vec![
            register_c1.into(),
            register_c2.into(),
            tx_a.into(),
            tx_b.into(),
            tx_b_proof.into(),
            tx_d.into(),
            tx_d_proof.into(),
        ],
    );

    state.craft_block_and_handle(108, vec![tx_c.into(), tx_c_proof.into()]);

    // Now settle the first, which should auto-settle the pending ones, then the ones waiting for these.
    assert_eq!(
        state
            .craft_block_and_handle(110, vec![tx_a_proof_1.into(), tx_a_proof_2.into(),])
            .successful_txs,
        vec![tx_a_hash, tx_b_hash, tx_d_hash, tx_c_hash]
    );
}
#[test_log::test(tokio::test)]
async fn test_tx_timeout_simple() {
    let mut state = new_node_state().await;
    let c1 = ContractName::new("c1");
    let register_c1 = make_register_contract_tx(c1.clone());

    // First basic test - Time out a TX.
    let blob_tx = BlobTransaction::new(
        Identity::new("test@c1"),
        vec![new_blob(&c1.0), new_blob(&c1.0)],
    );

    let txs = vec![register_c1.into(), blob_tx.clone().into()];

    let blob_tx_hash = blob_tx.hashed();

    state.craft_block_and_handle(3, txs);

    // This should trigger the timeout
    let timed_out_tx_hashes = state.craft_block_and_handle(103, vec![]).timed_out_txs;

    // Check that the transaction has timed out
    assert!(timed_out_tx_hashes.contains(&blob_tx_hash));
    assert!(state.unsettled_transactions.get(&blob_tx_hash).is_none());
}

#[test_log::test(tokio::test)]
async fn test_tx_no_timeout_once_settled() {
    let mut state = new_node_state().await;
    let c1 = ContractName::new("c1");
    let register_c1 = make_register_contract_tx(c1.clone());

    // Add a new transaction and settle it.
    let blob_tx = BlobTransaction::new(Identity::new("test@c1"), vec![new_blob(&c1.0)]);

    let crafted_block = craft_signed_block(
        104,
        vec![register_c1.clone().into(), blob_tx.clone().into()],
    );

    let blob_tx_hash = blob_tx.hashed();

    state.force_handle_block(&crafted_block);

    assert_eq!(
        timeouts::tests::get(&state.timeouts, &blob_tx_hash),
        Some(BlockHeight(204))
    );

    let first_hyle_output = make_hyle_output(blob_tx, BlobIndex(0));
    let verified_first_proof = new_proof_tx(&c1, &first_hyle_output, &blob_tx_hash);

    // Settle TX
    assert_eq!(
        state
            .craft_block_and_handle(105, vec![verified_first_proof.into(),])
            .successful_txs,
        vec![blob_tx_hash.clone()]
    );

    assert!(state.unsettled_transactions.get(&blob_tx_hash).is_none());
    // The TX remains in the map
    assert_eq!(
        timeouts::tests::get(&state.timeouts, &blob_tx_hash),
        Some(BlockHeight(204))
    );

    // Time out
    let timed_out_tx_hashes = state.craft_block_and_handle(204, vec![]).timed_out_txs;

    // Check that the transaction remains settled and cleared from the timeout map
    assert!(!timed_out_tx_hashes.contains(&blob_tx_hash));
    assert!(state.unsettled_transactions.get(&blob_tx_hash).is_none());
    assert_eq!(timeouts::tests::get(&state.timeouts, &blob_tx_hash), None);
}

#[test_log::test(tokio::test)]
async fn test_tx_on_timeout_settle_next_txs() {
    let mut state = new_node_state().await;
    let c1 = ContractName::new("c1");
    let c2 = ContractName::new("c2");
    let register_c1 = make_register_contract_tx(c1.clone());
    let register_c2 = make_register_contract_tx(c2.clone());

    // Add Three transactions - the first blocks the next two, but the next two are ready to settle.
    let blocking_tx = BlobTransaction::new(
        Identity::new("test@c1"),
        vec![new_blob(&c1.0), new_blob(&c2.0)],
    );
    let blocking_tx_hash = blocking_tx.hashed();

    let ready_same_block = BlobTransaction::new(Identity::new("test@c1"), vec![new_blob(&c1.0)]);
    let ready_later_block = BlobTransaction::new(Identity::new("test@c2"), vec![new_blob(&c2.0)]);
    let ready_same_block_hash = ready_same_block.hashed();
    let ready_later_block_hash = ready_later_block.hashed();
    let hyle_output = make_hyle_output(ready_same_block.clone(), BlobIndex(0));
    let ready_same_block_verified_proof = new_proof_tx(&c1, &hyle_output, &ready_same_block_hash);

    let hyle_output = make_hyle_output(ready_later_block.clone(), BlobIndex(0));
    let ready_later_block_verified_proof = new_proof_tx(&c2, &hyle_output, &ready_later_block_hash);

    let crafted_block = craft_signed_block(
        104,
        vec![
            register_c1.into(),
            register_c2.into(),
            blocking_tx.into(),
            ready_same_block.into(),
            ready_same_block_verified_proof.into(),
        ],
    );

    state.force_handle_block(&crafted_block);

    let later_crafted_block = craft_signed_block(
        108,
        vec![
            ready_later_block.into(),
            ready_later_block_verified_proof.into(),
        ],
    );

    state.force_handle_block(&later_crafted_block);

    // Time out
    let block = state.craft_block_and_handle(204, vec![]);

    // Only the blocking TX should be timed out
    assert_eq!(block.timed_out_txs, vec![blocking_tx_hash]);

    // The others have been settled
    [ready_same_block_hash, ready_later_block_hash]
        .iter()
        .for_each(|tx_hash| {
            assert!(!block.timed_out_txs.contains(tx_hash));
            assert!(state.unsettled_transactions.get(tx_hash).is_none());
            assert!(block.successful_txs.contains(tx_hash));
        });
}

#[test_log::test(tokio::test)]
async fn test_tx_reset_timeout_on_tx_settlement() {
    // Create four transactions that are inter dependent
    // Tx1 --> Tx2 (ready to be settled)
    //     |-> Tx3 -> Tx4

    // We want to test that when Tx1 times out:
    // - Tx2 gets settled
    // - Tx3's timeout is reset
    // - Tx4 is neither resetted nor timedout.

    // We then want to test that when Tx3 settles:
    // - Tx4's timeout is set

    let mut state = new_node_state().await;
    let c1 = ContractName::new("c1");
    let c2 = ContractName::new("c2");
    let register_c1 = make_register_contract_tx(c1.clone());
    let register_c2 = make_register_contract_tx(c2.clone());

    const TIMEOUT_WINDOW: BlockHeight = BlockHeight(100);

    // Add Three transactions - the first blocks the next two, and the next two are NOT ready to settle.
    let tx1 = BlobTransaction::new(
        Identity::new("test@c1"),
        vec![new_blob(&c1.0), new_blob(&c2.0)],
    );
    let tx2 = BlobTransaction::new(Identity::new("test@c1"), vec![new_blob(&c1.0)]);
    let tx3 = BlobTransaction::new(Identity::new("test@c2"), vec![new_blob(&c2.0)]);
    let tx4 = BlobTransaction::new(Identity::new("test2@c2"), vec![new_blob(&c2.0)]);
    let tx1_hash = tx1.hashed();
    let tx2_hash = tx2.hashed();
    let tx3_hash = tx3.hashed();
    let tx4_hash = tx4.hashed();

    let hyle_output = make_hyle_output(tx2.clone(), BlobIndex(0));
    let tx2_verified_proof = new_proof_tx(&c1, &hyle_output, &tx2_hash);
    let hyle_output = make_hyle_output(tx3.clone(), BlobIndex(0));
    let tx3_verified_proof = new_proof_tx(&c2, &hyle_output, &tx3_hash);

    state.craft_block_and_handle(
        104,
        vec![
            register_c1.into(),
            register_c2.into(),
            tx1.into(),
            tx2.into(),
            tx2_verified_proof.into(),
            tx3.into(),
            tx4.into(),
        ],
    );

    // Assert timeout only contains tx1
    assert_eq!(
        timeouts::tests::get(&state.timeouts, &tx1_hash),
        Some(104 + TIMEOUT_WINDOW)
    );
    assert_eq!(timeouts::tests::get(&state.timeouts, &tx2_hash), None);
    assert_eq!(timeouts::tests::get(&state.timeouts, &tx3_hash), None);
    assert_eq!(timeouts::tests::get(&state.timeouts, &tx4_hash), None);

    // Time out
    let block = state.craft_block_and_handle(204, vec![]);

    // Assert that only tx1 has timed out
    assert_eq!(block.timed_out_txs, vec![tx1_hash.clone()]);
    assert_eq!(timeouts::tests::get(&state.timeouts, &tx1_hash), None);

    // Assert that tx2 has settled
    assert_eq!(state.unsettled_transactions.get(&tx2_hash), None);
    assert_eq!(timeouts::tests::get(&state.timeouts, &tx2_hash), None);

    // Assert that tx3 timeout is reset
    assert_eq!(
        timeouts::tests::get(&state.timeouts, &tx3_hash),
        Some(204 + TIMEOUT_WINDOW)
    );

    // Assert that tx4 has no timeout
    assert_eq!(timeouts::tests::get(&state.timeouts, &tx4_hash), None);

    // Tx3 settles
    state.craft_block_and_handle(250, vec![tx3_verified_proof.into()]);

    // Assert that tx3 has settled.
    assert_eq!(state.unsettled_transactions.get(&tx3_hash), None);
    assert_eq!(timeouts::tests::get(&state.timeouts, &tx1_hash), None);
    assert_eq!(timeouts::tests::get(&state.timeouts, &tx2_hash), None);

    // Assert that tx4 timeout is set with remaining timeout window
    assert_eq!(
        timeouts::tests::get(&state.timeouts, &tx4_hash),
        Some(104 + TIMEOUT_WINDOW)
    );
}

// Check hyle-modules/src/node_state.rs l127 for the timeout window value
#[test_log::test(tokio::test)]
async fn test_tx_with_hyle_blob_should_have_specific_timeout() {
    let hyle_timeout_window = BlockHeight(5);

    let mut state = new_node_state().await;
    let a1 = ContractName::new("a1");
    let register_a1 = make_register_contract_tx(a1.clone());
    let c1 = ContractName::new("c1");
    let blob_a1 = new_blob("a1");
    let mut register_c1 = make_register_contract_tx_with_actions(c1.clone(), vec![blob_a1]);
    let tx_hash = register_c1.hashed();

    let block = state.craft_block_and_handle(100, vec![register_a1.into(), register_c1.into()]);

    // Assert no timeout
    assert_eq!(block.timed_out_txs, vec![]);

    // Time out
    let block = state.craft_block_and_handle(100 + hyle_timeout_window.0, vec![]);

    // Assert that tx has timed out
    assert_eq!(block.timed_out_txs, vec![tx_hash.clone()]);
}

// We can't put a register action with its blobs in the same tx for now
#[ignore]
#[test_log::test(tokio::test)]
async fn test_tx_with_hyle_blob_should_have_specific_timeout_in_same_tx() {
    let mut state = new_node_state().await;
    let a1 = ContractName::new("a1");
    let blob_a1 = new_blob("a1");
    let register_and_blob_a1 = make_register_contract_tx_with_actions(a1.clone(), vec![blob_a1]);
    let tx_hash = register_and_blob_a1.hashed();

    let block = state.craft_block_and_handle(100, vec![register_and_blob_a1.into()]);

    // Assert no timeout
    assert_eq!(block.timed_out_txs, vec![]);

    // Time out
    let block = state.craft_block_and_handle(102, vec![]);

    // Assert that tx has timed out
    assert_eq!(block.timed_out_txs, vec![tx_hash.clone()]);
}

#[test_log::test(tokio::test)]
async fn test_duplicate_tx_timeout() {
    let mut state = new_node_state().await;
    let c1 = ContractName::new("c1");
    let register_c1 = make_register_contract_tx(c1.clone());

    // First register the contract
    state.craft_block_and_handle(1, vec![register_c1.into()]);

    // Create a transaction
    let blob_tx = BlobTransaction::new(Identity::new("test@c1"), vec![new_blob(&c1.0)]);
    let blob_tx_hash = blob_tx.hashed();

    // Submit the same transaction multiple times in different blocks
    state.craft_block_and_handle(2, vec![blob_tx.clone().into()]);

    // Sanity check for timeout
    assert_eq!(
        timeouts::tests::get(&state.timeouts, &blob_tx_hash),
        Some(BlockHeight(2) + BlockHeight(100))
    );

    state.craft_block_and_handle(3, vec![blob_tx.clone().into()]);
    let block = state.craft_block_and_handle(4, vec![blob_tx.clone().into()]);

    assert!(block.failed_txs.is_empty());
    assert!(block.successful_txs.is_empty());

    // Verify only one instance of the transaction is tracked
    assert_eq!(state.unsettled_transactions.len(), 1);
    assert!(state.unsettled_transactions.get(&blob_tx_hash).is_some());

    // Check the timeout is still the same
    assert_eq!(
        timeouts::tests::get(&state.timeouts, &blob_tx_hash),
        Some(BlockHeight(2) + BlockHeight(100)) // Timeout should be based on first appearance
    );

    // Time out the transaction
    let block = state.craft_block_and_handle(102, vec![]);

    // Verify the transaction was timed out
    assert_eq!(block.timed_out_txs, vec![blob_tx_hash.clone()]);
    assert!(state.unsettled_transactions.get(&blob_tx_hash).is_none());
    assert_eq!(timeouts::tests::get(&state.timeouts, &blob_tx_hash), None);

    // Submit the same transaction again after timeout
    state.craft_block_and_handle(103, vec![blob_tx.clone().into()]);

    // Verify it's treated as a new transaction
    assert_eq!(state.unsettled_transactions.len(), 1);
    assert!(state.unsettled_transactions.get(&blob_tx_hash).is_some());
    assert_eq!(
        timeouts::tests::get(&state.timeouts, &blob_tx_hash),
        Some(BlockHeight(103) + BlockHeight(100))
    );
}

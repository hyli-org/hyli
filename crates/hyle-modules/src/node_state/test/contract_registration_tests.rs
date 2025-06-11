use std::collections::HashSet;

use super::*;

pub fn make_register_tx(
    sender: Identity,
    tld: ContractName,
    name: ContractName,
) -> BlobTransaction {
    BlobTransaction::new(
        sender,
        vec![RegisterContractAction {
            verifier: "test".into(),
            program_id: ProgramId(vec![]),
            state_commitment: StateCommitment(vec![0, 1, 2, 3]),
            contract_name: name,
            ..Default::default()
        }
        .as_blob(tld, None, None)],
    )
}

#[test_log::test(tokio::test)]
async fn test_register_contract_simple_hyle() {
    let mut state = new_node_state().await;

    let register_c1 = make_register_tx("hyle@hyle".into(), "hyle".into(), "c1".into());
    let register_c2 = make_register_tx("hyle@hyle".into(), "hyle".into(), "c2.hyle".into());
    let register_c3 = make_register_tx("hyle@hyle".into(), "hyle".into(), "c3".into());

    state.craft_block_and_handle(1, vec![register_c1.clone().into()]);

    state.craft_block_and_handle(2, vec![register_c2.into(), register_c3.into()]);

    assert_eq!(
        state.contracts.keys().collect::<HashSet<_>>(),
        HashSet::from_iter(vec![
            &"hyle".into(),
            &"c1".into(),
            &"c2.hyle".into(),
            &"c3".into()
        ])
    );

    let block = state.craft_block_and_handle(3, vec![register_c1.clone().into()]);

    assert_eq!(block.failed_txs, vec![register_c1.hashed()]);
    assert_eq!(state.contracts.len(), 4);
}

#[test_log::test(tokio::test)]
async fn test_register_contract_failure() {
    let mut state = new_node_state().await;

    let register_1 = make_register_tx("hyle@hyle".into(), "hyle".into(), "c1.hyle.lol".into());
    let register_2 = make_register_tx("other@hyle".into(), "hyle".into(), "c2.hyle.hyle".into());
    let register_3 = make_register_tx("hyle@hyle".into(), "hyle".into(), "c3.other".into());
    let register_4 = make_register_tx("hyle@hyle".into(), "hyle".into(), ".hyle".into());
    let register_5 = BlobTransaction::new(
        "hyle@hyle",
        vec![Blob {
            contract_name: "hyle".into(),
            data: BlobData(vec![0, 1, 2, 3]),
        }],
    );
    let register_good = make_register_tx("hyle@hyle".into(), "hyle".into(), "c1.hyle".into());

    let signed_block = craft_signed_block(
        1,
        vec![
            register_1.clone().into(),
            register_2.clone().into(),
            register_3.clone().into(),
            register_4.clone().into(),
            register_5.clone().into(),
            register_good.clone().into(),
        ],
    );

    let block = state.force_handle_block(&signed_block);

    assert_eq!(state.contracts.len(), 2);
    assert_eq!(block.successful_txs, vec![register_good.hashed()]);
    assert_eq!(
        block.failed_txs,
        vec![
            register_1.hashed(),
            register_2.hashed(),
            register_3.hashed(),
            register_4.hashed(),
            register_5.hashed(),
        ]
    );
}

#[test_log::test(tokio::test)]
async fn test_register_contract_proof_mismatch() {
    let mut state = new_node_state().await;

    // Create a valid registration transaction
    let register_parent_tx =
        make_register_tx("hyle@hyle".into(), "hyle".into(), "test.hyle".into());
    let register_parent_tx_hash = register_parent_tx.hashed();
    let register_tx = make_register_tx(
        "hyle@test.hyle".into(),
        "test.hyle".into(),
        "sub.test.hyle".into(),
    );
    let tx_hash = register_tx.hashed();

    // Create a proof with mismatched registration effect
    let mut output = make_hyle_output(register_tx.clone(), BlobIndex(0));
    output
        .onchain_effects
        .push(OnchainEffect::RegisterContract(RegisterContractEffect {
            verifier: "test".into(),
            program_id: ProgramId(vec![]),
            state_commitment: StateCommitment(vec![9, 9, 9, 9]), // Different state_commitment than in the blob action
            contract_name: "sub.test.hyle".into(),
            timeout_window: None,
        }));

    let proof_tx = new_proof_tx(&"test.hyle".into(), &output, &tx_hash);

    // Submit both transactions
    let block = state.craft_block_and_handle(
        1,
        vec![
            register_parent_tx.into(),
            register_tx.into(),
            proof_tx.into(),
        ],
    );

    // The transaction should fail because the proof's registration effect doesn't match the blob action
    tracing::warn!("{:?}", state.contracts);
    assert_eq!(state.contracts.len(), 2); // sub.test.hyle shouldn't exist
    assert_eq!(block.successful_txs, vec![register_parent_tx_hash]); // No successful transactions
}

#[test_log::test(tokio::test)]
async fn test_register_contract_composition() {
    let mut state = new_node_state().await;
    let register = make_register_tx("hyle@hyle".into(), "hyle".into(), "hydentity".into());
    let block = state.craft_block_and_handle(1, vec![register.clone().into()]);

    check_block_is_ok(&block);

    assert_eq!(state.contracts.len(), 2);

    let compositing_register_willfail = BlobTransaction::new(
        "test@hydentity",
        vec![
            RegisterContractAction {
                verifier: "test".into(),
                program_id: ProgramId(vec![]),
                state_commitment: StateCommitment(vec![0, 1, 2, 3]),
                contract_name: "c1".into(),
                ..Default::default()
            }
            .as_blob("hyle".into(), None, None),
            Blob {
                contract_name: "hydentity".into(),
                data: BlobData(vec![0, 1, 2, 3]),
            },
        ],
    );
    // Try to register the same contract validly later.
    // Change identity to change blob tx hash
    let compositing_register_good = BlobTransaction::new(
        "test2@hydentity",
        compositing_register_willfail.blobs.clone(),
    );

    let crafted_block = craft_signed_block(
        102,
        vec![
            compositing_register_willfail.clone().into(),
            compositing_register_good.clone().into(),
        ],
    );

    let block = state.force_handle_block(&crafted_block);
    assert_eq!(state.contracts.len(), 2);

    check_block_is_ok(&block);

    let proof_tx = new_proof_tx(
        &"hyle".into(),
        &make_hyle_output(compositing_register_good.clone(), BlobIndex(1)),
        &compositing_register_good.hashed(),
    );

    let block = state.craft_block_and_handle(103, vec![proof_tx.into()]);

    check_block_is_ok(&block);

    assert_eq!(state.contracts.len(), 2);

    // Send a third one that will fail early on settlement of the second because duplication
    // (and thus test the early-failure settlement path)

    let third_tx = BlobTransaction::new(
        "test3@hydentity",
        compositing_register_willfail.blobs.clone(),
    );
    let proof_tx = new_proof_tx(
        &"hyle".into(),
        &make_hyle_output(third_tx.clone(), BlobIndex(1)),
        &third_tx.hashed(),
    );

    let block = state.craft_block_and_handle(104, vec![third_tx.clone().into()]);

    check_block_is_ok(&block);

    assert_eq!(state.contracts.len(), 2);

    let mut block = state.craft_block_and_handle(105, vec![proof_tx.clone().into()]);

    check_block_is_ok(&block);

    let block = state.craft_block_and_handle(102 + 5, vec![]);

    check_block_is_ok(&block);

    assert_eq!(
        block.timed_out_txs,
        vec![compositing_register_willfail.hashed()]
    );
    assert_eq!(state.contracts.len(), 3);
}

fn check_block_is_ok(block: &Block) {
    let dp_hashes: Vec<TxHash> = block.dp_parent_hashes.clone().into_keys().collect();

    for tx_hash in block.successful_txs.iter() {
        assert!(dp_hashes.contains(tx_hash));
    }

    for tx_hash in block.failed_txs.iter() {
        assert!(dp_hashes.contains(tx_hash));
    }

    for tx_hash in block.timed_out_txs.iter() {
        assert!(dp_hashes.contains(tx_hash));
    }

    for (tx_hash, _) in block.transactions_events.iter() {
        assert!(dp_hashes.contains(tx_hash));
    }
}

pub fn make_delete_tx(
    sender: Identity,
    tld: ContractName,
    contract_name: ContractName,
) -> BlobTransaction {
    BlobTransaction::new(
        sender,
        vec![DeleteContractAction { contract_name }.as_blob(tld, None, None)],
    )
}

#[test_log::test(tokio::test)]
async fn test_register_contract_and_delete_hyle() {
    let mut state = new_node_state().await;

    let register_c1 = make_register_tx("hyle@hyle".into(), "hyle".into(), "c1".into());
    let register_c2 = make_register_tx("hyle@hyle".into(), "hyle".into(), "c2.hyle".into());
    // This technically doesn't matter as it's actually the proof that does the work
    let register_sub_c2 = make_register_tx(
        "toto@c2.hyle".into(),
        "c2.hyle".into(),
        "sub.c2.hyle".into(),
    );

    let mut output = make_hyle_output(register_sub_c2.clone(), BlobIndex(0));
    output
        .onchain_effects
        .push(OnchainEffect::RegisterContract(RegisterContractEffect {
            verifier: "test".into(),
            program_id: ProgramId(vec![]),
            state_commitment: StateCommitment(vec![0, 1, 2, 3]),
            contract_name: "sub.c2.hyle".into(),
            timeout_window: None,
        }));
    let sub_c2_proof = new_proof_tx(&"c2.hyle".into(), &output, &register_sub_c2.hashed());

    let block = state.craft_block_and_handle(
        1,
        vec![
            register_c1.into(),
            register_c2.into(),
            register_sub_c2.into(),
            sub_c2_proof.into(),
        ],
    );
    assert_eq!(
        block
            .registered_contracts
            .keys()
            .map(|cn| cn.0.clone())
            .collect::<Vec<_>>(),
        vec!["c1", "c2.hyle", "sub.c2.hyle"]
    );
    assert_eq!(state.contracts.len(), 4);

    // Now delete them.
    let self_delete_tx = make_delete_tx("c1@c1".into(), "c1".into(), "c1".into());
    let delete_sub_tx = make_delete_tx(
        "toto@c2.hyle".into(),
        "c2.hyle".into(),
        "sub.c2.hyle".into(),
    );
    let delete_tx = make_delete_tx("hyle@hyle".into(), "hyle".into(), "c2.hyle".into());

    let mut output = make_hyle_output(self_delete_tx.clone(), BlobIndex(0));
    output
        .onchain_effects
        .push(OnchainEffect::DeleteContract("c1".into()));
    let delete_self_proof = new_proof_tx(&"c1.hyle".into(), &output, &self_delete_tx.hashed());

    let mut output =
        make_hyle_output_with_state(delete_sub_tx.clone(), BlobIndex(0), &[4, 5, 6], &[1]);
    output
        .onchain_effects
        .push(OnchainEffect::DeleteContract("sub.c2.hyle".into()));
    let delete_sub_proof = new_proof_tx(&"c2.hyle".into(), &output, &delete_sub_tx.hashed());

    let block = state.craft_block_and_handle(
        2,
        vec![
            self_delete_tx.into(),
            delete_sub_tx.into(),
            delete_self_proof.into(),
            delete_sub_proof.into(),
            delete_tx.into(),
        ],
    );

    assert_eq!(
        block
            .deleted_contracts
            .keys()
            .map(|dce| dce.0.clone())
            .collect::<Vec<_>>(),
        vec!["c1", "c2.hyle", "sub.c2.hyle"]
    );
    assert_eq!(state.contracts.len(), 1);
}

#[test_log::test(tokio::test)]
async fn test_hyle_sub_delete() {
    let mut state = new_node_state().await;

    let register_c2 = make_register_tx("hyle@hyle".into(), "hyle".into(), "c2.hyle".into());
    // This technically doesn't matter as it's actually the proof that does the work
    let register_sub_c2 = make_register_tx(
        "toto@c2.hyle".into(),
        "c2.hyle".into(),
        "sub.c2.hyle".into(),
    );

    let mut output = make_hyle_output(register_sub_c2.clone(), BlobIndex(0));
    output
        .onchain_effects
        .push(OnchainEffect::RegisterContract(RegisterContractEffect {
            verifier: "test".into(),
            program_id: ProgramId(vec![]),
            state_commitment: StateCommitment(vec![0, 1, 2, 3]),
            contract_name: "sub.c2.hyle".into(),
            timeout_window: None,
        }));
    let sub_c2_proof = new_proof_tx(&"c2.hyle".into(), &output, &register_sub_c2.hashed());

    state.craft_block_and_handle(
        1,
        vec![
            register_c2.into(),
            register_sub_c2.into(),
            sub_c2_proof.into(),
        ],
    );
    assert_eq!(state.contracts.len(), 3);

    // Now delete the intermediate contract first, then delete the sub-contract via hyle
    let delete_tx = make_delete_tx("hyle@hyle".into(), "hyle".into(), "c2.hyle".into());
    let delete_sub_tx = make_delete_tx("hyle@hyle".into(), "hyle".into(), "sub.c2.hyle".into());

    let block = state.craft_block_and_handle(2, vec![delete_tx.into(), delete_sub_tx.into()]);

    assert_eq!(
        block
            .deleted_contracts
            .keys()
            .map(|dce| dce.0.clone())
            .collect::<Vec<_>>(),
        vec!["c2.hyle", "sub.c2.hyle"]
    );
    assert_eq!(state.contracts.len(), 1);
}

#[test_log::test(tokio::test)]
async fn test_register_update_delete_combinations_hyle() {
    let register_tx = make_register_tx("hyle@hyle".into(), "hyle".into(), "c.hyle".into());
    let delete_tx = make_delete_tx("hyle@hyle".into(), "hyle".into(), "c.hyle".into());
    let delete_self_tx = make_delete_tx("hyle@c.hyle".into(), "c.hyle".into(), "c.hyle".into());
    let update_tx = make_register_tx("test@c.hyle".into(), "c.hyle".into(), "c.hyle".into());

    let mut output = make_hyle_output(update_tx.clone(), BlobIndex(0));
    output
        .onchain_effects
        .push(OnchainEffect::RegisterContract(RegisterContractEffect {
            verifier: "test".into(),
            program_id: ProgramId(vec![]),
            state_commitment: StateCommitment(vec![0, 1, 2, 3]),
            contract_name: "c.hyle".into(),
            timeout_window: None,
        }));
    let proof_update = new_proof_tx(&"c.hyle".into(), &output, &update_tx.hashed());

    let mut output =
        make_hyle_output_with_state(delete_self_tx.clone(), BlobIndex(0), &[4, 5, 6], &[1]);
    output
        .onchain_effects
        .push(OnchainEffect::DeleteContract("c.hyle".into()));
    let proof_delete = new_proof_tx(&"c.hyle".into(), &output, &delete_self_tx.hashed());

    async fn test_combination(
        proofs: Option<&[&VerifiedProofTransaction]>,
        txs: &[&BlobTransaction],
        expected_ct: usize,
        expected_txs: usize,
    ) {
        let mut state = new_node_state().await;
        let mut txs = txs
            .iter()
            .map(|tx| (*tx).clone().into())
            .collect::<Vec<_>>();
        if let Some(proofs) = proofs {
            txs.extend(proofs.iter().map(|p| (*p).clone().into()));
        }
        let block = state.craft_block_and_handle(1, txs);

        assert_eq!(state.contracts.len(), expected_ct);
        assert_eq!(block.successful_txs.len(), expected_txs);
        info!("done");
    }

    // Test all combinations
    test_combination(None, &[&register_tx], 2, 1).await;
    test_combination(None, &[&delete_tx], 1, 0).await;
    test_combination(None, &[&register_tx, &delete_tx], 1, 2).await;
    test_combination(Some(&[&proof_update]), &[&register_tx, &update_tx], 2, 2).await;
    test_combination(
        Some(&[&proof_update]),
        &[&register_tx, &update_tx, &delete_tx],
        1,
        3,
    )
    .await;
    test_combination(
        Some(&[&proof_update, &proof_delete]),
        &[&register_tx, &update_tx, &delete_self_tx],
        1,
        3,
    )
    .await;
}

#[test_log::test(tokio::test)]
async fn test_unknown_contract_and_delete_cleanup() {
    let mut state = new_node_state().await;

    // 1. Create a blob transaction for an unknown contract
    let unknown_contract_tx = BlobTransaction::new(
        "test@unknown",
        vec![Blob {
            contract_name: "unknown".into(),
            data: BlobData(vec![1, 2, 3, 4]),
        }],
    );

    // This transaction should be rejected immediately since the contract doesn't exist
    let block = state.craft_block_and_handle(1, vec![unknown_contract_tx.clone().into()]);
    assert_eq!(block.failed_txs, vec![unknown_contract_tx.hashed()]);

    // 2. Register a contract
    let register_tx = make_register_tx("hyle@hyle".into(), "hyle".into(), "to_delete".into());
    state.craft_block_and_handle(2, vec![register_tx.clone().into()]);

    // 3. Submit blob transactions for the contract but don't settle them
    // The first will delete it, the others are just there to time out.
    let blob_tx1 = make_delete_tx(
        "hyle@to_delete".into(),
        "to_delete".into(),
        "to_delete".into(),
    );
    let blob_tx2 = BlobTransaction::new(
        "test@to_delete",
        vec![Blob {
            contract_name: "to_delete".into(),
            data: BlobData(vec![1, 2, 3, 4]),
        }],
    );
    let blob_tx3 = BlobTransaction::new(
        "test2@to_delete",
        vec![Blob {
            contract_name: "to_delete".into(),
            data: BlobData(vec![5, 6, 7, 8]),
        }],
    );
    let blob_tx1_hash = blob_tx1.hashed();
    let blob_tx2_hash = blob_tx2.hashed();
    let blob_tx3_hash = blob_tx3.hashed();

    state.craft_block_and_handle(
        3,
        vec![blob_tx1.clone().into(), blob_tx2.into(), blob_tx3.into()],
    );

    // Verify transactions are in the unsettled map
    assert!(state.unsettled_transactions.get(&blob_tx1_hash).is_some());
    assert!(state.unsettled_transactions.get(&blob_tx2_hash).is_some());
    assert!(state.unsettled_transactions.get(&blob_tx3_hash).is_some());

    // 4. Delete the contract
    // Create proof for the deletion
    let mut output = make_hyle_output(blob_tx1.clone(), BlobIndex(0));
    output
        .onchain_effects
        .push(OnchainEffect::DeleteContract("to_delete".into()));
    let proof_tx = new_proof_tx(&"hyle".into(), &output, &blob_tx1_hash);
    // Execute the deletion
    state.craft_block_and_handle(4, vec![proof_tx.into()]);

    // Verify contract was deleted
    assert!(!state.contracts.contains_key(&"to_delete".into()));

    // Verify all associated transactions were removed from the unsettled map
    assert!(state.unsettled_transactions.get(&blob_tx1_hash).is_none());
    assert!(state.unsettled_transactions.get(&blob_tx2_hash).is_none());
    assert!(state.unsettled_transactions.get(&blob_tx3_hash).is_none());
    assert!(state
        .unsettled_transactions
        .get_next_unsettled_tx(&"to_delete".into())
        .is_none());
}

#[test_log::test(tokio::test)]
async fn test_custom_timeout_then_upgrade_with_none() {
    let mut state = new_node_state().await;

    let c1 = ContractName::new("c1");
    let register_c1 = make_register_contract_tx(c1.clone());

    // Register the contract
    state.craft_block_and_handle(1, vec![register_c1.into()]);

    let custom_timeout = BlockHeight(150);

    // Upgrade the contract with a custom timeout
    {
        let action = RegisterContractAction {
            verifier: "test".into(),
            program_id: ProgramId(vec![]),
            state_commitment: StateCommitment(vec![0, 1, 2, 3]),
            contract_name: c1.clone(),
            timeout_window: Some(TimeoutWindow::Timeout(custom_timeout)),
            ..Default::default()
        };
        let upgrade_with_timeout = BlobTransaction::new(
            Identity::new("test@c1"),
            vec![action.clone().as_blob("c1".into(), None, None)],
        );

        let upgrade_with_timeout_hash = upgrade_with_timeout.hashed();
        state.craft_block_and_handle(2, vec![upgrade_with_timeout.clone().into()]);

        // Verify the timeout is set correctly - this is the old timeout
        assert_eq!(
            timeouts::tests::get(&state.timeouts, &upgrade_with_timeout_hash),
            Some(BlockHeight(2) + BlockHeight(100))
        );

        // Settle it
        let mut hyle_output = make_hyle_output(upgrade_with_timeout, BlobIndex(0));
        hyle_output
            .onchain_effects
            .push(OnchainEffect::RegisterContract(action.into()));
        let upgrade_with_timeout_proof =
            new_proof_tx(&c1, &hyle_output, &upgrade_with_timeout_hash);
        state.craft_block_and_handle(3, vec![upgrade_with_timeout_proof.into()]);
    }

    // Upgrade the contract again with a None timeout
    {
        let action = RegisterContractAction {
            verifier: "test".into(),
            program_id: ProgramId(vec![]),
            state_commitment: StateCommitment(vec![4, 5, 6]),
            contract_name: c1.clone(),
            timeout_window: None,
            ..Default::default()
        };
        let upgrade_with_none = BlobTransaction::new(
            Identity::new("test@c1"),
            vec![action.clone().as_blob("c1".into(), None, None)],
        );

        let upgrade_with_none_hash = upgrade_with_none.hashed();
        state.craft_block_and_handle(4, vec![upgrade_with_none.clone().into()]);

        // Verify the timeout is the custom timeout
        assert_eq!(
            timeouts::tests::get(&state.timeouts, &upgrade_with_none_hash),
            Some(BlockHeight(4) + custom_timeout)
        );
        // Settle it
        let mut hyle_output =
            make_hyle_output_with_state(upgrade_with_none, BlobIndex(0), &[4, 5, 6], &[4, 5, 6]);
        hyle_output
            .onchain_effects
            .push(OnchainEffect::RegisterContract(action.into()));
        let upgrade_with_none_proof = new_proof_tx(&c1, &hyle_output, &upgrade_with_none_hash);
        state.craft_block_and_handle(5, vec![upgrade_with_none_proof.into()]);
    }

    // Upgrade the contract again with another custom timeout
    let another_custom_timeout = BlockHeight(200);
    {
        let action = RegisterContractAction {
            verifier: "test".into(),
            program_id: ProgramId(vec![]),
            state_commitment: StateCommitment(vec![4, 5, 6]),
            contract_name: c1.clone(),
            timeout_window: Some(TimeoutWindow::Timeout(another_custom_timeout)),
            ..Default::default()
        };
        let upgrade_with_another_timeout = BlobTransaction::new(
            Identity::new("test@c1"),
            vec![action.clone().as_blob("c1".into(), None, None)],
        );

        let upgrade_with_another_timeout_hash = upgrade_with_another_timeout.hashed();
        state.craft_block_and_handle(6, vec![upgrade_with_another_timeout.clone().into()]);

        // Verify the timeout is still the OG custom timeout
        assert_eq!(
            timeouts::tests::get(&state.timeouts, &upgrade_with_another_timeout_hash),
            Some(BlockHeight(6) + custom_timeout)
        );
        // Settle it
        let mut hyle_output = make_hyle_output_with_state(
            upgrade_with_another_timeout,
            BlobIndex(0),
            &[4, 5, 6],
            &[4, 5, 6],
        );
        hyle_output
            .onchain_effects
            .push(OnchainEffect::RegisterContract(action.into()));
        let upgrade_with_another_timeout_proof =
            new_proof_tx(&c1, &hyle_output, &upgrade_with_another_timeout_hash);
        state.craft_block_and_handle(7, vec![upgrade_with_another_timeout_proof.into()]);
    }

    // Send a final transaction with no timeout and Check it uses the new timeout
    let final_tx = BlobTransaction::new(
        Identity::new("test@c1"),
        vec![Blob {
            contract_name: c1.clone(),
            data: BlobData(vec![0, 1, 2, 3]),
        }],
    );

    let final_tx_hash = final_tx.hashed();
    state.craft_block_and_handle(8, vec![final_tx.into()]);

    // Verify the timeout remains the same as the last custom timeout
    assert_eq!(
        timeouts::tests::get(&state.timeouts, &final_tx_hash),
        Some(BlockHeight(8) + another_custom_timeout)
    );
}

#![cfg(test)]

use std::collections::HashSet;

use client_sdk::transaction_builder::ProvableBlobTx;
use hydentity::{
    client::tx_executor_handler::{register_identity, verify_identity},
    Hydentity, HydentityAction,
};

pub const HYLI_WALLET: &str = "wallet";
use crate::node_state::hyli_tld::HYLI_TLD_ID;

use super::*;

pub fn make_register_tx_with_constructor(
    sender: Identity,
    tld: ContractName,
    name: ContractName,
) -> BlobTransaction {
    let register_contract_action = RegisterContractAction {
        verifier: "test".into(),
        program_id: ProgramId(vec![]),
        state_commitment: StateCommitment(vec![0, 1, 2, 3]),
        contract_name: name.clone(),
        constructor_metadata: Some(vec![1]),
        ..Default::default()
    };
    let tld_blob = register_contract_action.as_blob(tld);

    let register_contract_blob = register_contract_action.as_blob(name);

    BlobTransaction::new(sender, vec![tld_blob, register_contract_blob])
}

pub fn make_update_tx_with_registration(
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
        .as_blob(tld)],
    )
}

pub fn make_register_hyli_wallet_identity_tx() -> BlobTransaction {
    let mut tx = ProvableBlobTx::new(HYLI_TLD_ID.into());
    register_identity(&mut tx, HYLI_WALLET.into(), "password".into());
    BlobTransaction::new(HYLI_TLD_ID.to_string(), tx.blobs)
}

#[test_log::test(tokio::test)]
async fn test_register_contract_simple_hyli() {
    let mut state = new_node_state().await;

    let register_c1 =
        make_register_tx_with_constructor("hyli@hyli".into(), "hyli".into(), "c1".into());
    let register_c2 =
        make_register_tx_with_constructor("hyli@hyli".into(), "hyli".into(), "c2.hyli".into());
    let register_c3 =
        make_register_tx_with_constructor("hyli@hyli".into(), "hyli".into(), "c3".into());

    state.craft_block_and_handle(1, vec![register_c1.clone().into()]);

    state.craft_block_and_handle(2, vec![register_c2.into(), register_c3.into()]);

    assert_eq!(
        state.contracts.keys().collect::<HashSet<_>>(),
        HashSet::from_iter(vec![
            &"hyli".into(),
            &"c1".into(),
            &"c2.hyli".into(),
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

    let register_1 =
        make_register_tx_with_constructor("hyli@hyli".into(), "hyli".into(), "c1.hyli.lol".into());
    let register_2 = make_register_tx_with_constructor(
        "other@hyli".into(),
        "hyli".into(),
        "c2.hyli.hyli".into(),
    );
    let register_3 =
        make_register_tx_with_constructor("hyli@hyli".into(), "hyli".into(), "c3.other".into());
    let register_4 =
        make_register_tx_with_constructor("hyli@hyli".into(), "hyli".into(), ".hyli".into());
    let register_5 = BlobTransaction::new(
        "hyli@hyli",
        vec![Blob {
            contract_name: "hyli".into(),
            data: BlobData(vec![0, 1, 2, 3]),
        }],
    );
    // "hyli" blob alone should not be able to register with metadata
    let mut register_6 = BlobTransaction::new(
        "hyli@hyli",
        vec![RegisterContractAction {
            verifier: "test".into(),
            program_id: ProgramId(vec![]),
            state_commitment: StateCommitment(vec![0, 1, 2, 3]),
            contract_name: "register_6".into(),
            constructor_metadata: Some(vec![1]),
            ..Default::default()
        }
        .as_blob("hyli".into())],
    );
    // if the "hyli" blob is not here, should not be able to register
    let mut register_7 = BlobTransaction::new(
        "hyli@hyli",
        vec![RegisterContractAction {
            verifier: "test".into(),
            program_id: ProgramId(vec![]),
            state_commitment: StateCommitment(vec![0, 1, 2, 3]),
            contract_name: "register_7".into(),
            ..Default::default()
        }
        .as_blob("register_7".into())],
    );
    let register_good =
        make_register_tx_with_constructor("hyli@hyli".into(), "hyli".into(), "c1.hyli".into());

    let signed_block = craft_signed_block(
        1,
        vec![
            register_1.clone().into(),
            register_2.clone().into(),
            register_3.clone().into(),
            register_4.clone().into(),
            register_5.clone().into(),
            register_6.clone().into(),
            register_7.clone().into(),
            register_good.clone().into(),
        ],
    );

    let block = state.force_handle_block(signed_block).parsed_block;

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
            register_6.hashed(),
            register_7.hashed(),
        ]
    );
}

#[test_log::test(tokio::test)]
async fn test_register_contract_composition() {
    let mut state = new_node_state().await;
    let register =
        make_register_tx_with_constructor("hyli@hyli".into(), "hyli".into(), "hydentity".into());
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
                constructor_metadata: Some("constructor_metadata".as_bytes().to_vec()),
                ..Default::default()
            }
            .as_blob("hyli".into()),
            Blob {
                contract_name: "c1".into(),
                data: BlobData("constructor_metadata".as_bytes().to_vec()),
            },
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

    let block = state.force_handle_block(crafted_block).parsed_block;
    assert_eq!(state.contracts.len(), 2);

    check_block_is_ok(&block);

    let mut proof_tx = new_proof_tx(
        &"hyli".into(),
        &make_hyli_output(compositing_register_good.clone(), BlobIndex(2)),
        &compositing_register_good.hashed(),
    );
    proof_tx.verifier = Verifier("hyli".to_string());
    proof_tx.program_id = ProgramId(vec![0, 0, 0, 0]);
    proof_tx.proven_blobs.get_mut(0).unwrap().program_id = ProgramId(vec![]);
    proof_tx.proven_blobs.get_mut(0).unwrap().verifier = Verifier("test".to_string());

    let block = state.craft_block_and_handle(103, vec![proof_tx.into()]);

    check_block_is_ok(&block);

    assert_eq!(state.contracts.len(), 2);

    // Send a third one that will fail early on settlement of the second because duplication
    // (and thus test the early-failure settlement path)

    let third_tx = BlobTransaction::new(
        "test3@hydentity",
        compositing_register_willfail.blobs.clone(),
    );
    let mut proof_tx = new_proof_tx(
        &"hyli".into(),
        &make_hyli_output(third_tx.clone(), BlobIndex(1)),
        &third_tx.hashed(),
    );
    proof_tx.verifier = Verifier("hyli".to_string());
    proof_tx.program_id = ProgramId(vec![0, 0, 0, 0]);
    proof_tx.proven_blobs.get_mut(0).unwrap().program_id = ProgramId(vec![]);
    proof_tx.proven_blobs.get_mut(0).unwrap().verifier = Verifier("test".to_string());

    let block = state.craft_block_and_handle(104, vec![third_tx.clone().into()]);

    check_block_is_ok(&block);

    assert_eq!(state.contracts.len(), 2);

    let mut block = state.craft_block_and_handle(105, vec![proof_tx.clone().into()]);

    check_block_is_ok(&block);

    let block = state.craft_block_and_handle(102 + 100, vec![]);

    check_block_is_ok(&block);

    assert_eq!(
        block.timed_out_txs,
        vec![compositing_register_willfail.hashed()]
    );
    assert_eq!(state.contracts.len(), 3);
}

#[test_log::test(tokio::test)]
async fn test_registration_from_contract_without_onchain_effect_do_not_block_settlement() {
    let mut state = new_node_state().await;

    // 1. Send tx to register c1
    let register_c1 =
        make_register_tx_with_constructor("hyli@hyli".into(), "hyli".into(), "c1".into());
    state.craft_block_and_handle(1, vec![register_c1.clone().into()]);

    // Verify that c1 is registered
    assert!(state.contracts.contains_key(&"c1".into()));

    // 2. Send a second transaction on c1 and c2 (c2 does not exist yet)
    let tx_c1_c2 = BlobTransaction::new(
        "user@c1",
        vec![
            Blob {
                contract_name: "c1".into(),
                data: BlobData(vec![1, 2, 3]),
            },
            Blob {
                contract_name: "c2".into(),
                data: BlobData(vec![4, 5, 6]),
            },
        ],
    );
    let tx_c1_c2_hash = tx_c1_c2.hashed();

    let block2 = state.craft_block_and_handle(2, vec![tx_c1_c2.clone().into()]);

    // 3. Check second tx is still sequenced
    assert!(!block2.failed_txs.contains(&tx_c1_c2_hash));
    assert!(!block2.successful_txs.contains(&tx_c1_c2_hash));
    assert!(state.unsettled_transactions.get(&tx_c1_c2_hash).is_some());

    // 4. Send proof_transaction on c1 blob without OnChaineffect::RegisterContract
    let mut output = make_hyli_output(tx_c1_c2.clone(), BlobIndex(0));
    // Pas d'OnchainEffect ajouté volontairement
    let proof_tx_c1 = new_proof_tx(&"c1".into(), &output, &tx_c1_c2_hash);

    let block3 = state.craft_block_and_handle(3, vec![proof_tx_c1.into()]);

    // 5. Assert transaction 2 settled as failed
    assert!(block3.failed_txs.contains(&tx_c1_c2_hash));
    assert!(!block3.successful_txs.contains(&tx_c1_c2_hash));
    assert!(state.unsettled_transactions.get(&tx_c1_c2_hash).is_none());
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

    for (tx_hash, ev) in block.transactions_events.iter() {
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
        vec![
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

pub fn make_delete_tx_with_hyli(tld: ContractName, contract_name: ContractName) -> BlobTransaction {
    BlobTransaction::new(
        HYLI_TLD_ID.to_string(),
        vec![
            HydentityAction::VerifyIdentity {
                nonce: 0,
                account: HYLI_TLD_ID.to_string(),
            }
            .as_blob(HYLI_WALLET.into()),
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

pub fn make_update_timeout_window_tx_with_hyli(
    tld: ContractName,
    contract_name: ContractName,
    timeout_window: TimeoutWindow,
) -> BlobTransaction {
    BlobTransaction::new(
        HYLI_TLD_ID.to_string(),
        vec![
            HydentityAction::VerifyIdentity {
                nonce: 0,
                account: HYLI_TLD_ID.to_string(),
            }
            .as_blob(HYLI_WALLET.into()),
            UpdateContractTimeoutWindowAction {
                contract_name,
                timeout_window,
            }
            .as_blob(tld),
        ],
    )
}

pub fn make_update_program_id_tx_with_hyli(
    tld: ContractName,
    contract_name: ContractName,
    program_id: ProgramId,
) -> BlobTransaction {
    BlobTransaction::new(
        HYLI_TLD_ID.to_string(),
        vec![
            HydentityAction::VerifyIdentity {
                nonce: 0,
                account: HYLI_TLD_ID.to_string(),
            }
            .as_blob(HYLI_WALLET.into()),
            UpdateContractProgramIdAction {
                contract_name,
                program_id,
            }
            .as_blob(tld),
        ],
    )
}
#[test_log::test(tokio::test)]
async fn test_register_contract_and_delete_hyli() {
    let mut state = new_node_state().await;

    let register_wallet =
        make_register_tx_with_constructor("hyli@hyli".into(), "hyli".into(), "wallet".into());
    let register_hyli_at_wallet = make_register_hyli_wallet_identity_tx();

    let mut output = make_hyli_output(register_hyli_at_wallet.clone(), BlobIndex(0));
    let register_hyli_at_wallet_proof =
        new_proof_tx(&"wallet".into(), &output, &register_hyli_at_wallet.hashed());

    let register_c1 =
        make_register_tx_with_constructor("hyli@hyli".into(), "hyli".into(), "c1".into());
    let register_c2 =
        make_register_tx_with_constructor("hyli@hyli".into(), "hyli".into(), "c2.hyli".into());
    // This technically doesn't matter as it's actually the proof that does the work
    let register_sub_c2 = make_register_tx_with_constructor(
        "toto@c2.hyli".into(),
        "c2.hyli".into(),
        "sub.c2.hyli".into(),
    );

    let mut output = make_hyli_output(register_sub_c2.clone(), BlobIndex(0));
    output
        .onchain_effects
        .push(OnchainEffect::RegisterContractWithConstructor(
            RegisterContractEffect {
                verifier: "test".into(),
                program_id: ProgramId(vec![]),
                state_commitment: StateCommitment(vec![0, 1, 2, 3]),
                contract_name: "sub.c2.hyli".into(),
                timeout_window: None,
            },
        ));
    let sub_c2_proof = new_proof_tx(&"c2.hyli".into(), &output, &register_sub_c2.hashed());

    let block = state.craft_block_and_handle(
        1,
        vec![
            register_wallet.into(),
            register_hyli_at_wallet.into(),
            register_hyli_at_wallet_proof.into(),
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
        vec!["c1", "c2.hyli", "sub.c2.hyli", "wallet"]
    );
    assert_eq!(state.contracts.len(), 5);

    // Now delete them.
    let self_delete_tx = make_delete_tx("c1@c1".into(), "c1".into(), "c1".into());
    let delete_sub_tx = make_delete_tx(
        "toto@c2.hyli".into(),
        "c2.hyli".into(),
        "sub.c2.hyli".into(),
    );
    let delete_tx = make_delete_tx_with_hyli("hyli".into(), "c2.hyli".into());
    let mut output = make_hyli_output_bis(delete_tx.clone(), BlobIndex(0));
    let delete_tx_proof = new_proof_tx(&"c2.hyli".into(), &output, &delete_tx.hashed());

    let mut output = make_hyli_output(self_delete_tx.clone(), BlobIndex(0));
    output
        .onchain_effects
        .push(OnchainEffect::DeleteContract("c1".into()));
    let delete_self_proof = new_proof_tx(&"c1".into(), &output, &self_delete_tx.hashed());

    let mut output =
        make_hyli_output_with_state(delete_sub_tx.clone(), BlobIndex(0), &[4, 5, 6], &[1]);
    output
        .onchain_effects
        .push(OnchainEffect::DeleteContract("sub.c2.hyli".into()));
    let delete_sub_proof = new_proof_tx(&"c2.hyli".into(), &output, &delete_sub_tx.hashed());

    let block = state.craft_block_and_handle(
        2,
        vec![
            self_delete_tx.into(),
            delete_sub_tx.into(),
            delete_self_proof.into(),
            delete_sub_proof.into(),
            delete_tx.into(),
            delete_tx_proof.into(),
        ],
    );

    assert_eq!(
        block
            .deleted_contracts
            .keys()
            .map(|dce| dce.0.clone())
            .collect::<Vec<_>>(),
        vec!["c1", "c2.hyli", "sub.c2.hyli"]
    );
    assert_eq!(state.contracts.len(), 2);
}

#[test_log::test(tokio::test)]
async fn test_hyli_contract_update_timeout_window() {
    let mut state = new_node_state().await;
    let register_wallet =
        make_register_tx_with_constructor("hyli@hyli".into(), "hyli".into(), "wallet".into());
    let register_hyli_at_wallet = make_register_hyli_wallet_identity_tx();

    let mut output = make_hyli_output(register_hyli_at_wallet.clone(), BlobIndex(0));
    let register_hyli_at_wallet_proof =
        new_proof_tx(&"wallet".into(), &output, &register_hyli_at_wallet.hashed());

    let register_contract =
        make_register_tx_with_constructor("hyli@hyli".into(), "hyli".into(), "contract".into());

    state.craft_block_and_handle(
        1,
        vec![
            register_wallet.into(),
            register_hyli_at_wallet.into(),
            register_hyli_at_wallet_proof.into(),
            register_contract.into(),
        ],
    );

    assert_eq!(state.contracts.len(), 3);
    assert_eq!(
        state
            .contracts
            .get(&ContractName::new("contract"))
            .unwrap()
            .timeout_window,
        TimeoutWindow::Timeout(BlockHeight(100))
    );

    let timeout_window_update_tx = make_update_timeout_window_tx_with_hyli(
        "hyli".into(),
        "contract".into(),
        TimeoutWindow::Timeout(BlockHeight(45)),
    );

    let mut output = make_hyli_output_bis(timeout_window_update_tx.clone(), BlobIndex(0));
    let verify_hyli_proof = new_proof_tx(
        &"wallet".into(),
        &output,
        &timeout_window_update_tx.hashed(),
    );

    let block = state.craft_block_and_handle(
        2,
        vec![
            timeout_window_update_tx.into(),
            verify_hyli_proof.clone().into(),
        ],
    );

    assert_eq!(block.deleted_contracts.len(), 0);
    assert_eq!(state.contracts.len(), 3);
    assert_eq!(
        state
            .contracts
            .get(&ContractName::new("contract"))
            .unwrap()
            .timeout_window,
        TimeoutWindow::Timeout(BlockHeight(45))
    );
}

#[test_log::test(tokio::test)]
async fn test_hyli_sub_delete() {
    let mut state = new_node_state().await;

    let register_wallet =
        make_register_tx_with_constructor("hyli@hyli".into(), "hyli".into(), "wallet".into());
    let register_hyli_at_wallet = make_register_hyli_wallet_identity_tx();

    let mut output = make_hyli_output(register_hyli_at_wallet.clone(), BlobIndex(0));
    let register_hyli_at_wallet_proof =
        new_proof_tx(&"wallet".into(), &output, &register_hyli_at_wallet.hashed());

    let register_c2 =
        make_register_tx_with_constructor("hyli@hyli".into(), "hyli".into(), "c2.hyli".into());
    // This technically doesn't matter as it's actually the proof that does the work
    let register_sub_c2 = make_register_tx_with_constructor(
        "toto@c2.hyli".into(),
        "c2.hyli".into(),
        "sub.c2.hyli".into(),
    );

    let mut output = make_hyli_output(register_sub_c2.clone(), BlobIndex(0));
    output
        .onchain_effects
        .push(OnchainEffect::RegisterContractWithConstructor(
            RegisterContractEffect {
                verifier: "test".into(),
                program_id: ProgramId(vec![]),
                state_commitment: StateCommitment(vec![0, 1, 2, 3]),
                contract_name: "sub.c2.hyli".into(),
                timeout_window: None,
            },
        ));
    let sub_c2_proof = new_proof_tx(&"c2.hyli".into(), &output, &register_sub_c2.hashed());

    state.craft_block_and_handle(
        1,
        vec![
            register_wallet.into(),
            register_hyli_at_wallet.into(),
            register_hyli_at_wallet_proof.into(),
            register_c2.into(),
            register_sub_c2.into(),
            sub_c2_proof.into(),
        ],
    );
    assert_eq!(state.contracts.len(), 4);

    // Now delete the intermediate contract first, then delete the sub-contract via hyli
    let delete_tx = make_delete_tx_with_hyli("hyli".into(), "c2.hyli".into());

    let mut output = make_hyli_output_bis(delete_tx.clone(), BlobIndex(0));
    let verify_hyli_proof = new_proof_tx(&"wallet".into(), &output, &delete_tx.hashed());

    let delete_sub_tx = make_delete_tx_with_hyli("hyli".into(), "sub.c2.hyli".into());
    let mut output = make_hyli_output_ter(delete_sub_tx.clone(), BlobIndex(0));
    let verify_hyli_proof2 = new_proof_tx(&"wallet".into(), &output, &delete_sub_tx.hashed());

    let block = state.craft_block_and_handle(
        2,
        vec![
            delete_tx.into(),
            verify_hyli_proof.into(),
            delete_sub_tx.into(),
            verify_hyli_proof2.into(),
        ],
    );

    assert_eq!(
        block
            .deleted_contracts
            .keys()
            .map(|dce| dce.0.clone())
            .collect::<Vec<_>>(),
        vec!["c2.hyli", "sub.c2.hyli"]
    );
    assert_eq!(state.contracts.len(), 2);
}

#[test_log::test(tokio::test)]
async fn test_register_update_delete_combinations_hyli() {
    let register_tx =
        make_register_tx_with_constructor("hyli@hyli".into(), "hyli".into(), "c.hyli".into());
    let delete_tx = make_delete_tx_with_hyli("hyli".into(), "c.hyli".into());
    let delete_self_tx = make_delete_tx("hyli@c.hyli".into(), "c.hyli".into(), "c.hyli".into());
    let update_tx =
        make_update_tx_with_registration("test@c.hyli".into(), "c.hyli".into(), "c.hyli".into());

    let mut output = make_hyli_output(update_tx.clone(), BlobIndex(0));
    output
        .onchain_effects
        .push(OnchainEffect::RegisterContract(RegisterContractEffect {
            verifier: "test".into(),
            program_id: ProgramId(vec![]),
            state_commitment: StateCommitment(vec![0, 1, 2, 3]),
            contract_name: "c.hyli".into(),
            timeout_window: None,
        }));
    let proof_update = new_proof_tx(&"c.hyli".into(), &output, &update_tx.hashed());

    let mut output =
        make_hyli_output_with_state(delete_self_tx.clone(), BlobIndex(0), &[4, 5, 6], &[1]);
    output
        .onchain_effects
        .push(OnchainEffect::DeleteContract("c.hyli".into()));
    let proof_delete = new_proof_tx(&"c.hyli".into(), &output, &delete_self_tx.hashed());

    let mut output = make_hyli_output_bis(delete_tx.clone(), BlobIndex(0));
    let delete_tx_proof = new_proof_tx(&"wallet".into(), &output, &delete_tx.hashed());

    async fn test_combination(
        proofs: Option<&[&VerifiedProofTransaction]>,
        txs: &[&BlobTransaction],
        expected_ct: usize,
        expected_txs: usize,
    ) {
        let mut state = new_node_state().await;

        let register_wallet =
            make_register_tx_with_constructor("hyli@hyli".into(), "hyli".into(), "wallet".into());
        let register_hyli_at_wallet = make_register_hyli_wallet_identity_tx();

        let mut output = make_hyli_output(register_hyli_at_wallet.clone(), BlobIndex(0));
        let register_hyli_at_wallet_proof =
            new_proof_tx(&"wallet".into(), &output, &register_hyli_at_wallet.hashed());

        state.craft_block_and_handle(
            1,
            vec![
                register_wallet.clone().into(),
                register_hyli_at_wallet.clone().into(),
                register_hyli_at_wallet_proof.clone().into(),
            ],
        );
        let mut txs = txs
            .iter()
            .map(|tx| (*tx).clone().into())
            .collect::<Vec<_>>();
        if let Some(proofs) = proofs {
            txs.extend(proofs.iter().map(|p| (*p).clone().into()));
        }
        let block = state.craft_block_and_handle(2, txs);

        assert_eq!(state.contracts.len(), expected_ct);
        assert_eq!(block.successful_txs.len(), expected_txs);
        info!("done");
    }

    // Test all combinations
    info!("➡️ First combination: register only");
    test_combination(None, &[&register_tx], 3, 1).await;
    info!("➡️ Second combination: delete only");
    test_combination(None, &[&delete_tx], 2, 0).await;
    info!("➡️ Third combination: register - delete");
    test_combination(Some(&[&delete_tx_proof]), &[&register_tx, &delete_tx], 2, 2).await;
    info!("➡️ Fourth combination: register - update");
    test_combination(Some(&[&proof_update]), &[&register_tx, &update_tx], 3, 2).await;
    info!("➡️ Fifth combination: register - update - delete");
    test_combination(
        Some(&[&proof_update, &delete_tx_proof]),
        &[&register_tx, &update_tx, &delete_tx],
        2,
        3,
    )
    .await;
    info!("➡️ Sixth combination: register - delete_self");
    test_combination(
        Some(&[&proof_update, &proof_delete]),
        &[&register_tx, &update_tx, &delete_self_tx],
        2,
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
    let register_tx =
        make_register_tx_with_constructor("hyli@hyli".into(), "hyli".into(), "to_delete".into());
    state.craft_block_and_handle(2, vec![register_tx.clone().into()]);

    // 3. Submit blob transactions for the contract but don't settle them
    // The first will delete it, the others are just there to time out.
    let blob_tx1 = make_delete_tx(
        "hyli@to_delete".into(),
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
    let mut output = make_hyli_output(blob_tx1.clone(), BlobIndex(0));
    output
        .onchain_effects
        .push(OnchainEffect::DeleteContract("to_delete".into()));
    let mut proof_tx = new_proof_tx(&"hyli".into(), &output, &blob_tx1_hash);
    proof_tx.verifier = Verifier("hyli".to_string());
    proof_tx.program_id = ProgramId(vec![0, 0, 0, 0]);
    proof_tx.proven_blobs.get_mut(0).unwrap().program_id = ProgramId(vec![]);
    proof_tx.proven_blobs.get_mut(0).unwrap().verifier = Verifier("test".to_string());

    let block = state.craft_block_and_handle(4, vec![proof_tx.into()]);

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
            vec![action.clone().as_blob("c1".into())],
        );

        let upgrade_with_timeout_hash = upgrade_with_timeout.hashed();
        state.craft_block_and_handle(2, vec![upgrade_with_timeout.clone().into()]);

        // Verify the timeout is set correctly - this is the old timeout
        assert_eq!(
            timeouts::tests::get(&state.timeouts, &upgrade_with_timeout_hash),
            Some(BlockHeight(2) + BlockHeight(100))
        );

        // Settle it
        let mut hyli_output = make_hyli_output(upgrade_with_timeout, BlobIndex(0));
        hyli_output
            .onchain_effects
            .push(OnchainEffect::RegisterContract(action.into()));
        let upgrade_with_timeout_proof =
            new_proof_tx(&c1, &hyli_output, &upgrade_with_timeout_hash);
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
            vec![action.clone().as_blob("c1".into())],
        );

        let upgrade_with_none_hash = upgrade_with_none.hashed();
        state.craft_block_and_handle(4, vec![upgrade_with_none.clone().into()]);

        // Verify the timeout is the custom timeout
        assert_eq!(
            timeouts::tests::get(&state.timeouts, &upgrade_with_none_hash),
            Some(BlockHeight(4) + custom_timeout)
        );
        // Settle it
        let mut hyli_output =
            make_hyli_output_with_state(upgrade_with_none, BlobIndex(0), &[4, 5, 6], &[4, 5, 6]);
        hyli_output
            .onchain_effects
            .push(OnchainEffect::RegisterContract(action.into()));
        let upgrade_with_none_proof = new_proof_tx(&c1, &hyli_output, &upgrade_with_none_hash);
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
            vec![action.clone().as_blob("c1".into())],
        );

        let upgrade_with_another_timeout_hash = upgrade_with_another_timeout.hashed();
        state.craft_block_and_handle(6, vec![upgrade_with_another_timeout.clone().into()]);

        // Verify the timeout is still the OG custom timeout
        assert_eq!(
            timeouts::tests::get(&state.timeouts, &upgrade_with_another_timeout_hash),
            Some(BlockHeight(6) + custom_timeout)
        );
        // Settle it
        let mut hyli_output = make_hyli_output_with_state(
            upgrade_with_another_timeout,
            BlobIndex(0),
            &[4, 5, 6],
            &[4, 5, 6],
        );
        hyli_output
            .onchain_effects
            .push(OnchainEffect::RegisterContract(action.into()));
        let upgrade_with_another_timeout_proof =
            new_proof_tx(&c1, &hyli_output, &upgrade_with_another_timeout_hash);
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

#[test_log::test(tokio::test)]
async fn test_pending_tx_then_contract_upgrade_and_settlement_order() {
    // 1. Register contract via hyli
    let mut state = new_node_state().await;
    let contract_name = ContractName::new("foo");
    let register_1 =
        make_register_tx_with_constructor("hyli@hyli".into(), "hyli".into(), contract_name.clone());
    state.craft_block_and_handle(1, vec![register_1.clone().into()]);
    assert!(state.contracts.contains_key(&contract_name));

    // 2. Send a pending TX to that contract (TX2)
    let tx2 = BlobTransaction::new(
        Identity::new("user@foo"),
        vec![Blob {
            contract_name: contract_name.clone(),
            data: BlobData(vec![1, 2, 3]),
        }],
    );
    let tx2_hash = tx2.hashed();
    state.craft_block_and_handle(2, vec![tx2.clone().into()]);
    assert!(state.unsettled_transactions.get(&tx2_hash).is_some());

    // 3. Register the same contract again via hyli (TX3)
    let register_2 =
        make_register_tx_with_constructor("hyli@hyli".into(), "hyli".into(), contract_name.clone());
    let register_2_hash = register_2.hashed();
    let block3 = state.craft_block_and_handle(3, vec![register_2.clone().into()]);

    // Check block3: TX3 should be in failed_txs
    assert!(block3.failed_txs.contains(&register_2_hash));

    // 4. Send another TX to the contract (TX4)
    let tx4 = BlobTransaction::new(
        Identity::new("user2@foo"),
        vec![Blob {
            contract_name: contract_name.clone(),
            data: BlobData(vec![4, 5, 6]),
        }],
    );
    let tx4_hash = tx4.hashed();
    state.craft_block_and_handle(4, vec![tx4.clone().into()]);
    assert!(state.unsettled_transactions.get(&tx4_hash).is_some());

    // 5. Submit the proof for TX4
    let mut output4 = make_hyli_output_with_state(tx4.clone(), BlobIndex(0), &[4, 5, 6], &[1]);
    let proof4 = new_proof_tx(&contract_name, &output4, &tx4_hash);
    let block5 = state.craft_block_and_handle(5, vec![proof4.clone().into()]);
    // TX4 should not be settled yet, because TX2 is still pending
    assert!(state.unsettled_transactions.get(&tx4_hash).is_some());

    // 6. Submit the proof for TX2
    let mut output2 = make_hyli_output(tx2.clone(), BlobIndex(0));
    let proof2 = new_proof_tx(&contract_name, &output2, &tx2_hash);
    let block6 = state.craft_block_and_handle(6, vec![proof2.clone().into()]);

    // 7. Now TX3 (the duplicate register) should have failed, and TX4 should be settled immediately
    // Check block6: TX4 should be settled (successful_txs)
    assert!(block6.successful_txs.contains(&tx4_hash));
    // TX2 should also be settled
    assert!(block6.successful_txs.contains(&tx2_hash));
    // Both TX2 and TX4 should be removed from unsettled
    assert!(state.unsettled_transactions.get(&tx2_hash).is_none());
    assert!(state.unsettled_transactions.get(&tx4_hash).is_none());
}

#[test_log::test(tokio::test)]
async fn domino_settlement_after_contract_delete() {
    // Register contracts 'a' and 'b' via crafted blocks
    let mut state = new_node_state().await;
    state.contracts.insert(
        ContractName::new("wallet"),
        Contract {
            name: ContractName::new("wallet"),
            program_id: ProgramId(vec![]),
            state: StateCommitment(vec![0, 1, 2, 3]),
            verifier: Verifier("test".into()),
            timeout_window: TimeoutWindow::Timeout(BlockHeight(100)),
        },
    );
    let a = ContractName::new("a");
    let b = ContractName::new("b");
    let register_a = make_register_contract_tx(a.clone());
    let register_b = make_register_contract_tx(b.clone());
    state.craft_block_and_handle(
        1,
        vec![register_a.clone().into(), register_b.clone().into()],
    );

    // Send a transaction to B
    let identity_b = Identity::new("user@b");
    let tx_b = BlobTransaction::new(identity_b.clone(), vec![new_blob("b")]);
    let tx_b_id = tx_b.hashed();
    state.craft_block_and_handle(2, vec![tx_b.clone().into()]);

    // Delete B via hyli (as a tx)
    let delete_b = make_delete_tx_with_hyli(ContractName::new("hyli"), b.clone());
    // And proof for 'wallet'
    let hyli_output_wallet = make_hyli_output(delete_b.clone(), BlobIndex(0));
    let proof_wallet = new_proof_tx(
        &ContractName::new("wallet"),
        &hyli_output_wallet,
        &delete_b.hashed(),
    );
    state.craft_block_and_handle(3, vec![delete_b.clone().into(), proof_wallet.into()]);

    // Send a transaction with blobs for both B and A
    let identity_ab = Identity::new("user@a");
    let tx_ab = BlobTransaction::new(identity_ab.clone(), vec![new_blob("b"), new_blob("a")]);
    let tx_ab_id = tx_ab.hashed();
    state.craft_block_and_handle(4, vec![tx_ab.clone().into()]);

    // Send another transaction for just A
    let identity_a = Identity::new("user@a");
    let tx_a = BlobTransaction::new(identity_a.clone(), vec![new_blob("a")]);
    let tx_a_id = tx_a.hashed();
    state.craft_block_and_handle(5, vec![tx_a.clone().into()]);

    // Send proof to make it ready to settle
    let hyli_output_a = make_hyli_output(tx_a.clone(), BlobIndex(0));
    let proof_a = new_proof_tx(&a, &hyli_output_a, &tx_a_id);
    let block_a = state.craft_block_and_handle(6, vec![proof_a.into()]);

    // Now send the proof for the first transaction (to B)
    let hyli_output_b = make_hyli_output(tx_b.clone(), BlobIndex(0));
    let proof_b = new_proof_tx(&b, &hyli_output_b, &tx_b_id);
    let block_b = state.craft_block_and_handle(7, vec![proof_b.into()]);
    // This should domino through and settle the tx_ab and tx_a if not already settled
    assert!(block_b.successful_txs.contains(&tx_a_id));
}

// Helper function to test multiple delete attempts with different TLD configurations
async fn test_multiple_delete_attempts_helper(tld_name: &str, contract_name: &str) {
    let mut state = new_node_state().await;

    // Register wallet and hyli identity
    let register_wallet =
        make_register_tx_with_constructor("hyli@hyli".into(), "hyli".into(), "wallet".into());
    let register_hyli_at_wallet = make_register_hyli_wallet_identity_tx();
    let mut output = make_hyli_output(register_hyli_at_wallet.clone(), BlobIndex(0));
    let register_hyli_at_wallet_proof =
        new_proof_tx(&"wallet".into(), &output, &register_hyli_at_wallet.hashed());

    // Register wrong_tld
    let register_wrong_tld =
        make_register_tx_with_constructor("hyli@hyli".into(), "hyli".into(), "wrong_tld".into());

    let mut initial_txs = vec![
        register_wallet.into(),
        register_hyli_at_wallet.into(),
        register_hyli_at_wallet_proof.into(),
        register_wrong_tld.into(),
    ];

    // Register custom TLD if it's not "hyli"
    if tld_name != "hyli" {
        let register_custom_tld =
            make_register_tx_with_constructor("hyli@hyli".into(), "hyli".into(), tld_name.into());
        initial_txs.push(register_custom_tld.into());

        // Register the test contract under the custom TLD
        let register_test_contract = make_register_tx_with_constructor(
            format!("user@{tld_name}").into(),
            tld_name.into(),
            contract_name.into(),
        );
        let mut output = make_hyli_output(register_test_contract.clone(), BlobIndex(0));
        output
            .onchain_effects
            .push(OnchainEffect::RegisterContractWithConstructor(
                RegisterContractEffect {
                    verifier: "test".into(),
                    program_id: ProgramId(vec![]),
                    state_commitment: StateCommitment(vec![0, 1, 2, 3]),
                    contract_name: contract_name.into(),
                    timeout_window: None,
                },
            ));
        let register_test_contract_proof =
            new_proof_tx(&tld_name.into(), &output, &register_test_contract.hashed());
        initial_txs.push(register_test_contract.into());
        initial_txs.push(register_test_contract_proof.into());
    } else {
        // For "hyli" TLD, just register the contract directly
        let register_test_contract = make_register_tx_with_constructor(
            "hyli@hyli".into(),
            "hyli".into(),
            contract_name.into(),
        );
        initial_txs.push(register_test_contract.into());
    }

    state.craft_block_and_handle(1, initial_txs);

    async fn test_scenario(
        mut state: NodeState,
        txs: &[Transaction],
        proofs: &[Transaction],
        expected_failing_tx: &[TxHash],
        expected_successful_tx: &[TxHash],
        expected_remaining_contracts: usize,
    ) {
        // Execute all attempts in one block
        let block = state.craft_block_and_handle(2, [txs, proofs].concat());

        // Verify only the last attempt succeeded
        assert_eq!(block.successful_txs, expected_successful_tx);
        assert_eq!(block.failed_txs, expected_failing_tx);
        assert_eq!(block.deleted_contracts.len(), 1);
        assert_eq!(state.contracts.len(), expected_remaining_contracts);
    }

    // Attempt 1: Delete without hyli@wallet blob
    let deletion_attempt1 = BlobTransaction::new(
        format!("attempt1@{contract_name}"),
        vec![
            DeleteContractAction {
                contract_name: contract_name.into(),
            }
            .as_blob(tld_name.into()),
            Blob {
                contract_name: contract_name.into(),
                data: BlobData(vec![]),
            },
        ],
    );

    // Attempt 2: Delete with wrong identity in hyli blob
    let deletion_attempt2 = BlobTransaction::new(
        "attempt2@identity",
        vec![
            HydentityAction::VerifyIdentity {
                nonce: 0,
                account: "attempt2@identity".to_string(),
            }
            .as_blob(HYLI_WALLET.into()),
            DeleteContractAction {
                contract_name: contract_name.into(),
            }
            .as_blob(tld_name.into()),
            Blob {
                contract_name: contract_name.into(),
                data: BlobData(vec![]),
            },
        ],
    );

    // Attempt 3: Delete with wrong TLD
    let deletion_attempt3 = BlobTransaction::new(
        HYLI_TLD_ID.to_string(),
        vec![
            HydentityAction::VerifyIdentity {
                nonce: 0,
                account: HYLI_TLD_ID.to_string(),
            }
            .as_blob(HYLI_WALLET.into()),
            DeleteContractAction {
                contract_name: contract_name.into(),
            }
            .as_blob("wrong_tld".into()),
            Blob {
                contract_name: contract_name.into(),
                data: BlobData(vec![]),
            },
        ],
    );

    // Attempt 4: Delete with missing contract blob
    let deletion_attempt4 = BlobTransaction::new(
        HYLI_TLD_ID.to_string(),
        vec![
            HydentityAction::VerifyIdentity {
                nonce: 0,
                account: HYLI_TLD_ID.to_string(),
            }
            .as_blob(HYLI_WALLET.into()),
            DeleteContractAction {
                contract_name: contract_name.into(),
            }
            .as_blob(tld_name.into()),
        ],
    );

    // Attempt 5: Correct delete transaction
    let deletion_attempt5 = BlobTransaction::new(
        HYLI_TLD_ID.to_string(),
        vec![
            HydentityAction::VerifyIdentity {
                nonce: 0,
                account: HYLI_TLD_ID.to_string(),
            }
            .as_blob(HYLI_WALLET.into()),
            DeleteContractAction {
                contract_name: contract_name.into(),
            }
            .as_blob(tld_name.into()),
            Blob {
                contract_name: contract_name.into(),
                data: BlobData(vec![]),
            },
        ],
    );

    // proofs for attempt3
    let mut wrong_tld_output = make_hyli_output(deletion_attempt3.clone(), BlobIndex(1));
    wrong_tld_output
        .onchain_effects
        .push(OnchainEffect::DeleteContract(contract_name.into()));

    let attempt3_wrong_tld_proof = new_proof_tx(
        &"wrong_tld".into(),
        &wrong_tld_output,
        &deletion_attempt3.hashed(),
    );

    if tld_name == "hyli" {
        info!("➡️ Hyli tld attempt, attempt5 is supposed to delete contract");
        let mut output = make_hyli_output_bis(deletion_attempt5.clone(), BlobIndex(0));
        let delete_success_wallet_proof =
            new_proof_tx(&"wallet".into(), &output, &deletion_attempt5.hashed());

        test_scenario(
            state.clone(),
            &[
                deletion_attempt1.clone().into(),
                deletion_attempt2.clone().into(),
                deletion_attempt3.clone().into(),
                deletion_attempt4.clone().into(),
                deletion_attempt5.clone().into(),
            ],
            &[
                attempt3_wrong_tld_proof.into(),
                delete_success_wallet_proof.into(),
            ],
            &[
                deletion_attempt1.hashed(),
                deletion_attempt2.hashed(),
                deletion_attempt3.hashed(),
                deletion_attempt4.hashed(),
            ],
            &[deletion_attempt5.hashed()],
            3,
        )
        .await;
    } else {
        info!("➡️ Curstom tld attempt, attempt5 is supposed to delete contract");
        // Adding proofs for attempt4
        let mut wallet_output = make_hyli_output_bis(deletion_attempt4.clone(), BlobIndex(0));
        let attempt4_wallet_proof = new_proof_tx(
            &"wallet".into(),
            &wallet_output,
            &deletion_attempt4.hashed(),
        );

        let mut tld_output = make_hyli_output_bis(deletion_attempt4.clone(), BlobIndex(1));
        tld_output
            .onchain_effects
            .push(OnchainEffect::DeleteContract(contract_name.into()));
        let attempt4_tld_proof =
            new_proof_tx(&tld_name.into(), &tld_output, &deletion_attempt4.hashed());

        // Adding proof for attempt5
        let mut wallet_output = make_hyli_output_bis(deletion_attempt5.clone(), BlobIndex(0));
        let attempt5_wallet_proof = new_proof_tx(
            &"wallet".into(),
            &wallet_output,
            &deletion_attempt5.hashed(),
        );

        let mut tld_output = make_hyli_output_bis(deletion_attempt5.clone(), BlobIndex(1));
        tld_output
            .onchain_effects
            .push(OnchainEffect::DeleteContract(contract_name.into()));
        let attempt5_tld_proof =
            new_proof_tx(&tld_name.into(), &tld_output, &deletion_attempt5.hashed());

        let mut contract_output = make_hyli_output_bis(deletion_attempt5.clone(), BlobIndex(2));
        let attempt5_contract_proof = new_proof_tx(
            &contract_name.into(),
            &contract_output,
            &deletion_attempt5.hashed(),
        );

        test_scenario(
            state.clone(),
            &[
                deletion_attempt3.clone().into(),
                deletion_attempt4.clone().into(),
                deletion_attempt5.clone().into(),
            ],
            &[
                attempt3_wrong_tld_proof.clone().into(),
                attempt4_wallet_proof.into(),
                attempt4_tld_proof.into(),
                attempt5_wallet_proof.into(),
                attempt5_tld_proof.into(),
                attempt5_contract_proof.into(),
            ],
            &[deletion_attempt3.hashed(), deletion_attempt4.hashed()],
            &[deletion_attempt5.hashed()],
            4,
        )
        .await;
    }
}

#[test_log::test(tokio::test)]
async fn test_multiple_delete_attempts_hyli_tld() {
    test_multiple_delete_attempts_helper("hyli", "test_contract").await;
}

#[test_log::test(tokio::test)]
async fn test_multiple_delete_attempts_custom_tld() {
    test_multiple_delete_attempts_helper("custom", "test_contract.custom").await;
}

// Helper function to test multiple update attempts with different TLD configurations
async fn test_multiple_update_attempts_helper(tld_name: &str, contract_name: &str) {
    let mut state = new_node_state().await;

    // Register wallet and hyli identity
    let register_wallet =
        make_register_tx_with_constructor("hyli@hyli".into(), "hyli".into(), "wallet".into());
    let register_hyli_at_wallet = make_register_hyli_wallet_identity_tx();
    let mut output = make_hyli_output(register_hyli_at_wallet.clone(), BlobIndex(0));
    let register_hyli_at_wallet_proof =
        new_proof_tx(&"wallet".into(), &output, &register_hyli_at_wallet.hashed());

    // Register wrong_tld
    let register_wrong_tld = make_register_tx_with_constructor(
        "hyli@hyli".into(),
        "hyli".into(),
        "wrong_tld.hyli".into(),
    );

    let mut initial_txs = vec![
        register_wallet.into(),
        register_hyli_at_wallet.into(),
        register_hyli_at_wallet_proof.into(),
        register_wrong_tld.into(),
    ];

    // Register custom TLD if it's not "hyli"
    if tld_name != "hyli" {
        let register_custom_tld =
            make_register_tx_with_constructor("hyli@hyli".into(), "hyli".into(), tld_name.into());
        initial_txs.push(register_custom_tld.into());

        // Register the test contract under the custom TLD
        let register_test_contract = make_register_tx_with_constructor(
            format!("user@{tld_name}").into(),
            tld_name.into(),
            contract_name.into(),
        );
        let mut output = make_hyli_output(register_test_contract.clone(), BlobIndex(0));
        output
            .onchain_effects
            .push(OnchainEffect::RegisterContractWithConstructor(
                RegisterContractEffect {
                    verifier: "test".into(),
                    program_id: ProgramId(vec![]),
                    state_commitment: StateCommitment(vec![0, 1, 2, 3]),
                    contract_name: contract_name.into(),
                    timeout_window: None,
                },
            ));
        let register_test_contract_proof =
            new_proof_tx(&tld_name.into(), &output, &register_test_contract.hashed());
        initial_txs.push(register_test_contract.into());
        initial_txs.push(register_test_contract_proof.into());
    } else {
        // For "hyli" TLD, just register the contract directly
        let register_test_contract = make_register_tx_with_constructor(
            "hyli@hyli".into(),
            "hyli".into(),
            contract_name.into(),
        );
        initial_txs.push(register_test_contract.into());
    }

    state.craft_block_and_handle(1, initial_txs);

    // Test UpdateProgramId operations
    info!("➡️ Testing UpdateProgramId operations");

    // Invalid UpdateProgramId - missing hyli@wallet blob
    let update_program_id_missing_wallet = BlobTransaction::new(
        HYLI_TLD_ID.to_string(),
        vec![
            UpdateContractProgramIdAction {
                contract_name: contract_name.into(),
                program_id: ProgramId(vec![]),
            }
            .as_blob(tld_name.into()),
            UpdateContractProgramIdAction {
                contract_name: contract_name.into(),
                program_id: ProgramId(vec![]),
            }
            .as_blob(contract_name.into()),
        ],
    );

    // Invalid UpdateProgramId - wrong TLD
    let update_program_id_wrong_tld = BlobTransaction::new(
        HYLI_TLD_ID.to_string(),
        vec![
            HydentityAction::VerifyIdentity {
                nonce: 0,
                account: HYLI_TLD_ID.to_string(),
            }
            .as_blob(HYLI_WALLET.into()),
            UpdateContractProgramIdAction {
                contract_name: contract_name.into(),
                program_id: ProgramId(vec![]),
            }
            .as_blob("wrong_tld.hyli".into()),
            UpdateContractProgramIdAction {
                contract_name: contract_name.into(),
                program_id: ProgramId(vec![]),
            }
            .as_blob(contract_name.into()),
        ],
    );

    // Add proofs for wrong_tld transaction
    let mut wrong_tld_output = make_hyli_output(update_program_id_wrong_tld.clone(), BlobIndex(1));
    wrong_tld_output
        .onchain_effects
        .push(OnchainEffect::UpdateContractProgramId(
            contract_name.into(),
            ProgramId(vec![]),
        ));

    let update_program_id_wrong_tld_proof = new_proof_tx(
        &"wrong_tld".into(),
        &wrong_tld_output,
        &update_program_id_wrong_tld.hashed(),
    );

    // Build transaction vectors based on TLD
    let mut failing_txs = vec![
        update_program_id_missing_wallet.clone().into(),
        update_program_id_wrong_tld.clone().into(),
        update_program_id_wrong_tld_proof.into(),
    ];

    let mut expected_failed_txs = vec![
        update_program_id_missing_wallet.hashed(),
        update_program_id_wrong_tld.hashed(),
    ];

    // Only include wrong_identity test for "hyli" TLD
    if tld_name == "hyli" {
        let update_program_id_wrong_identity = BlobTransaction::new(
            format!("wrong@{contract_name}"),
            vec![
                UpdateContractProgramIdAction {
                    contract_name: contract_name.into(),
                    program_id: ProgramId(vec![]),
                }
                .as_blob(tld_name.into()),
                UpdateContractProgramIdAction {
                    contract_name: contract_name.into(),
                    program_id: ProgramId(vec![]),
                }
                .as_blob(contract_name.into()),
            ],
        );
        failing_txs.push(update_program_id_wrong_identity.clone().into());
        expected_failed_txs.push(update_program_id_wrong_identity.hashed());
    }

    let mut all_txs = failing_txs.clone();

    // UpdateProgramId only via hyli TLD (no contract blob)
    let update_program_id_tld_only = BlobTransaction::new(
        HYLI_TLD_ID.to_string(),
        vec![
            HydentityAction::VerifyIdentity {
                nonce: 0,
                account: HYLI_TLD_ID.to_string(),
            }
            .as_blob(HYLI_WALLET.into()),
            UpdateContractProgramIdAction {
                contract_name: contract_name.into(),
                program_id: ProgramId(vec![]),
            }
            .as_blob("hyli".into()),
        ],
    );

    // UpdateProgramId only via contract with side effect
    let update_program_id_contract_side_effect = BlobTransaction::new(
        format!("user@{contract_name}"),
        vec![UpdateContractProgramIdAction {
            contract_name: contract_name.into(),
            program_id: ProgramId(vec![]),
        }
        .as_blob(contract_name.into())],
    );

    all_txs.extend(vec![
        // These will succeed:
        update_program_id_tld_only.clone().into(),
        update_program_id_contract_side_effect.clone().into(),
    ]);

    let block = state.craft_block_and_handle(2, all_txs);

    // Verify failed transactions
    for expected_failed in &expected_failed_txs {
        assert!(block.failed_txs.contains(expected_failed));
    }

    // Add proofs for update_program_id_tld_only
    let mut wallet_output = make_hyli_output_bis(update_program_id_tld_only.clone(), BlobIndex(0));
    let update_tld_only_wallet_proof = new_proof_tx(
        &"wallet".into(),
        &wallet_output,
        &update_program_id_tld_only.hashed(),
    );

    // Add proofs for update_program_id_contract_side_effect
    let mut contract_output =
        make_hyli_output(update_program_id_contract_side_effect.clone(), BlobIndex(0));
    contract_output
        .onchain_effects
        .push(OnchainEffect::UpdateContractProgramId(
            contract_name.into(),
            ProgramId(vec![]),
        ));
    let update_contract_side_effect_proof = new_proof_tx(
        &contract_name.into(),
        &contract_output,
        &update_program_id_contract_side_effect.hashed(),
    );

    let block_proofs = state.craft_block_and_handle(
        3,
        vec![
            // update_program_id_tld_only
            update_tld_only_wallet_proof.into(),
            // update_program_id_contract_side_effect
            update_contract_side_effect_proof.into(),
        ],
    );

    // Verify successful transactions
    assert!(block_proofs
        .successful_txs
        .contains(&update_program_id_tld_only.hashed()));
    assert!(block_proofs
        .successful_txs
        .contains(&update_program_id_contract_side_effect.hashed()));
}

#[test_log::test(tokio::test)]
async fn test_multiple_update_attempts_hyli_tld() {
    test_multiple_update_attempts_helper("hyli", "test_contract").await;
}

#[test_log::test(tokio::test)]
async fn test_multiple_update_attempts_custom_tld() {
    test_multiple_update_attempts_helper("custom", "test_contract.custom").await;
}

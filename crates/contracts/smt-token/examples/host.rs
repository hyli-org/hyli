use std::collections::BTreeMap;

use client_sdk::helpers::risc0::Risc0Prover;
use hyle_smt_token::{
    account::{Account, AccountSMT},
    client::tx_executor_handler::metadata::SMT_TOKEN_ELF,
    SmtTokenAction, SmtTokenContract,
};
use sdk::{
    merkle_utils::BorshableMerkleProof, BlobIndex, Calldata, ContractAction, Identity,
    StateCommitment, TxHash,
};

#[tokio::main]
async fn main() {
    // Create a new empty SMT
    let mut smt = AccountSMT::default();

    // Create some test accounts
    for user in 0..1000 {
        let account = Account::new(Identity::from(user.to_string()), 100);
        let key = account.get_key();
        smt.0.update(key, account).expect("Failed to update SMT");
    }

    let account1 = Account::new(Identity::from("1"), 100);
    let account2 = Account::new(Identity::from("2"), 100);

    // Create keys for the accounts
    let key1 = account1.get_key();
    let key2 = account2.get_key();

    // Generate merkle proof for account1
    let merkle_proof = smt
        .0
        .merkle_proof(vec![key1, key2])
        .expect("Failed to generate proof");

    // Compute initial root
    let root = *smt.0.root();
    let smt_token = SmtTokenContract::new(
        StateCommitment(Into::<[u8; 32]>::into(root).to_vec()),
        BorshableMerkleProof(merkle_proof),
        BTreeMap::from([
            (account1.address.clone(), account1.clone()),
            (account2.address.clone(), account2.clone()),
        ]),
    );

    let token_action = SmtTokenAction::Transfer {
        sender: account1.address,
        recipient: account2.address,
        amount: 100,
    };

    let commitment_metadata = borsh::to_vec(&smt_token).unwrap();
    let calldata = Calldata {
        identity: "alice".into(),
        blobs: vec![token_action.as_blob("smt-token".into(), None, None)].into(),
        tx_blob_count: 1,
        index: BlobIndex(0),
        tx_hash: TxHash::default(),
        tx_ctx: None,
        private_input: vec![],
    };

    let prover = Risc0Prover::new(SMT_TOKEN_ELF, "smt-token".to_string());

    let proof = prover.prove(commitment_metadata, vec![calldata]).await;

    if let Err(err) = proof {
        println!("Error: {:?}", err);
        return;
    }
    println!("proof size: {:?}", proof.unwrap().0.len());
    return;
}

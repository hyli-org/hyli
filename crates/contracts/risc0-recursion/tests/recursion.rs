use client_sdk::{
    contract_states,
    transaction_builder::{ProvableBlobTx, TxExecutorBuilder, TxExecutorHandler},
};
use hydentity::{Hydentity, client::tx_executor_handler::register_identity};
use hyli_risc0_recursion::ProofInput;
use sdk::{Blob, Calldata, ContractName, HyliOutput};

contract_states!(
    struct States {
        hydentity: Hydentity,
    }
);
#[test_log::test(tokio::test)]
async fn test_recursion() {
    // SAFETY: This test is the only one setting this env var and runs before any concurrent access.
    unsafe { std::env::set_var("RISC0_DEV_MODE", "1") };

    let mut executor = TxExecutorBuilder::new(States {
        hydentity: Hydentity::default(),
    })
    .build();

    let mut tx = ProvableBlobTx::new("bob@hydentity".into());
    register_identity(&mut tx, "hydentity".into(), "password".into()).unwrap();
    let first_proof = executor
        .process(tx)
        .unwrap()
        .iter_prove()
        .next()
        .unwrap()
        .await
        .unwrap()
        .proof;

    let mut tx = ProvableBlobTx::new("alice@hydentity".into());
    register_identity(&mut tx, "hydentity".into(), "password".into()).unwrap();
    let second_proof = executor
        .process(tx)
        .unwrap()
        .iter_prove()
        .next()
        .unwrap()
        .await
        .unwrap()
        .proof;

    let first_receipt = borsh::from_slice::<risc0_zkvm::Receipt>(first_proof.0.as_slice())
        .expect("Failed to decode first receipt");
    let second_receipt = borsh::from_slice::<risc0_zkvm::Receipt>(second_proof.0.as_slice())
        .expect("Failed to decode second receipt");

    let env = risc0_zkvm::ExecutorEnv::builder()
        .add_assumption(first_receipt.clone())
        .add_assumption(second_receipt.clone())
        .write(&vec![
            ProofInput {
                image_id: hyli_contracts::HYDENTITY_ID,
                journal: first_receipt.journal.bytes,
            },
            ProofInput {
                image_id: hyli_contracts::HYDENTITY_ID,
                journal: second_receipt.journal.bytes,
            },
        ])
        .unwrap()
        .build()
        .unwrap();

    let receipt = risc0_zkvm::default_prover()
        .prove(env, hyli_contracts::RISC0_RECURSION_ELF)
        .unwrap()
        .receipt;

    receipt.verify(hyli_contracts::RISC0_RECURSION_ID).unwrap();

    let outputs: Vec<([u8; 32], Vec<u8>)> =
        receipt.journal.decode().expect("Failed to decode journal");
    let outputs = outputs
        .iter()
        .map(|x| {
            (
                x.0,
                risc0_zkvm::serde::from_slice::<Vec<HyliOutput>, _>(&x.1).unwrap(),
            )
        })
        .collect::<Vec<_>>();

    assert_eq!(outputs.first().unwrap().0, hyli_contracts::HYDENTITY_ID,);
    assert_eq!(
        String::from_utf8(outputs.first().unwrap().1[0].program_outputs.clone()).unwrap(),
        "Successfully registered identity for account: bob@hydentity:f08bf16792a8efe265e37068568042c25aa6e2c3df0d0cb3c5eed7c78abc7348"
    );
    assert_eq!(outputs.last().unwrap().0, hyli_contracts::HYDENTITY_ID,);
    assert_eq!(
        String::from_utf8(outputs.last().unwrap().1[0].program_outputs.clone()).unwrap(),
        "Successfully registered identity for account: alice@hydentity:8333a333420eb181556e8e021a7b682dc1fa58a2560547938eb3c11bc74ecdbd"
    );

    assert_eq!(outputs.len(), 2);
}

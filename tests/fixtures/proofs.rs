#![allow(unused)]

use risc0_recursion::ProofInput;

pub async fn generate_recursive_proof(program_ids: &[[u8; 32]], proofs: &[&[u8]]) -> Vec<u8> {
    let receipts = proofs
        .iter()
        .map(|proof| {
            borsh::from_slice::<risc0_zkvm::Receipt>(proof).expect("Failed to decode receipt")
        })
        .collect::<Vec<_>>();

    let mut env = risc0_zkvm::ExecutorEnv::builder();
    receipts.iter().for_each(|receipt| {
        env.add_assumption(receipt.clone());
    });
    env.write(
        &std::iter::zip(program_ids, receipts)
            .map(|(pid, r)| ProofInput {
                image_id: *pid,
                journal: r.journal.bytes,
            })
            .collect::<Vec<ProofInput>>(),
    )
    .unwrap();
    let env = env.build().unwrap();

    let receipt = risc0_zkvm::default_prover()
        .prove(env, hyli_contracts::RISC0_RECURSION_ELF)
        .unwrap()
        .receipt;

    borsh::to_vec(&receipt).unwrap()
}

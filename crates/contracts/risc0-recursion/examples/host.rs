use std::fs;

use hyli_risc0_recursion::ProofInput;

fn main() {
    // Collect command line arguments
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: {} <program_id> <path1> <path2> ...", args[0]);
        std::process::exit(1);
    }

    let program_id: [u8; 32] = hex::decode(&args[1])
        .expect("Failed to decode program ID")
        .try_into()
        .expect("Invalid program ID length");
    let paths = &args[2..];

    // Read the contents of each file
    let mut proof_receipts: Vec<risc0_zkvm::Receipt> = paths
        .iter()
        .map(|path| {
            borsh::from_slice::<risc0_zkvm::Receipt>(
                fs::read(path).expect("Unable to read file").as_slice(),
            )
            .expect("Failed to decode first receipt")
        })
        .collect();

    let mut env = risc0_zkvm::ExecutorEnv::builder();
    env.write(
        &proof_receipts
            .iter()
            .map(|receipt| ProofInput {
                image_id: program_id,
                journal: receipt.journal.bytes.clone(),
            })
            .collect::<Vec<_>>(),
    )
    .unwrap();

    proof_receipts.drain(..).for_each(|receipt| {
        env.add_assumption(receipt);
    });
    let env = env.build().unwrap();

    let receipt = risc0_zkvm::default_prover()
        .prove(env, hyli_risc0_recursion::metadata::RISC0_RECURSION_ELF)
        .unwrap()
        .receipt;

    receipt
        .verify(hyli_risc0_recursion::metadata::PROGRAM_ID)
        .unwrap();

    println!("Proof generated successfully, writing to recursed_proof.proof");
    std::fs::write("recursed_proof.proof", borsh::to_vec(&receipt).unwrap())
        .expect("Failed to write proof");
}

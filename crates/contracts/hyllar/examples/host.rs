use client_sdk::helpers::risc0::Risc0Prover;
use hyle_hyllar::erc20::ERC20;
use hyle_hyllar::{
    client::tx_executor_handler::metadata::HYLLAR_ELF, Hyllar, HyllarAction, FAUCET_SECP256K1,
};
use sdk::{BlobIndex, Calldata, ContractAction, TxHash};

#[tokio::main]
async fn main() {
    let mut hyllar = Hyllar::default();
    let users = 1000;
    for n in 0..users {
        let ident = &format!("{n}");
        hyllar
            .transfer(FAUCET_SECP256K1, ident, 0)
            .map_err(|e| anyhow::anyhow!(e))
            .unwrap();
    }

    let hyllar_action = HyllarAction::Transfer {
        recipient: "alice".to_string(),
        amount: 0,
    };

    let commitment_metadata = hyllar.to_bytes();
    let calldata = Calldata {
        identity: FAUCET_SECP256K1.into(),
        blobs: vec![hyllar_action.as_blob("hyllar".into(), None, None)].into(),
        tx_blob_count: 1,
        index: BlobIndex(0),
        tx_hash: TxHash::default(),
        tx_ctx: None,
        private_input: vec![],
    };

    let prover = Risc0Prover::new(HYLLAR_ELF);
    let proof = prover.prove(commitment_metadata, vec![calldata]).await;

    if let Err(err) = proof {
        println!("Error: {:?}", err);
        return;
    }
    println!("proof size: {:?}", proof.unwrap().0.len());
    return;
}

use borsh::{BorshDeserialize, BorshSerialize};
use client_sdk::transaction_builder::TxExecutorHandler;
use hyle_verifiers::native::verify;
use hyllar::FAUCET_ID;
use sdk::verifiers::NativeVerifiers;

use crate::node_state::HYLI_TLD_ID;

/// Convenience utility for verifying blobs for native verifiers.
#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct NativeVerifierHandler {
    pubkey: [u8; 33],
}

impl TxExecutorHandler for NativeVerifierHandler {
    fn handle(&mut self, calldata: &sdk::Calldata) -> anyhow::Result<sdk::HyleOutput> {
        let Some(blob) = calldata.blobs.get(&calldata.index) else {
            return Err(anyhow::anyhow!(
                "Blob not found for index {}",
                calldata.index
            ));
        };
        let blobs = calldata
            .blobs
            .iter()
            .map(|b| b.1.clone())
            .collect::<Vec<_>>();
        let cloned_identity = calldata.identity.0.clone();
        let hyli_pubkey = self.pubkey;
        Ok(verify(
            calldata.tx_hash.clone(),
            calldata.index,
            &blobs,
            match blob.contract_name.0.as_str() {
                "blst" => NativeVerifiers::Blst,
                "sha3_256" => NativeVerifiers::Sha3_256,
                "secp256k1" => NativeVerifiers::Secp256k1,
                _ => anyhow::bail!("Unknown native verifier: {}", blob.contract_name),
            },
            move |pk| {
                if cloned_identity == HYLI_TLD_ID || cloned_identity == FAUCET_ID {
                    return Some(pk == hyli_pubkey);
                }

                None
            },
        ))
    }

    fn build_commitment_metadata(&self, _: &sdk::Blob) -> anyhow::Result<Vec<u8>> {
        Ok(vec![])
    }

    fn construct_state(
        _: &sdk::RegisterContractEffect,
        _: &Option<Vec<u8>>,
    ) -> anyhow::Result<Self> {
        Ok(Self { pubkey: [0; 33] })
    }
}

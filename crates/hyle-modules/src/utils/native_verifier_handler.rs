use client_sdk::transaction_builder::TxExecutorHandler;
use hyle_verifiers::native::verify;
use sdk::verifiers::NativeVerifiers;

/// Convenience utility for verifying blobs for native verifiers.
pub struct NativeVerifierHandler;

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
        ))
    }

    fn build_commitment_metadata(&self, _: &sdk::Blob) -> anyhow::Result<Vec<u8>> {
        Ok(vec![])
    }

    fn construct_state(
        _: &sdk::RegisterContractEffect,
        _: &Option<Vec<u8>>,
    ) -> anyhow::Result<Self> {
        Ok(Self)
    }
}

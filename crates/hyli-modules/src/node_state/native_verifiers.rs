use ::secp256k1::*;
use hyli_crypto::BlstCrypto;
use sdk::{
    verifiers::{BlstSignatureBlob, NativeVerifiers, Secp256k1Blob, ShaBlob},
    *,
};
use sha3::Digest;

pub fn verify(
    tx_hash: TxHash,
    index: BlobIndex,
    blobs: &[Blob],
    verifier: &NativeVerifiers,
) -> HyliOutput {
    #[allow(clippy::expect_used, reason = "Logic error in the code")]
    let blob = blobs.get(index.0).expect("Invalid blob index");
    let blobs: IndexedBlobs = blobs.iter().cloned().into();

    let (identity, success) = match verify_native_impl(blob, verifier) {
        Ok(v) => v,
        Err(e) => {
            tracing::trace!("Native blob verification failed: {:?}", e);
            (Identity::default(), false)
        }
    };

    if success {
        tracing::debug!("✅ Native blob verified on {tx_hash}:{index}");
    } else {
        tracing::debug!("❌ Native blob verification failed on {tx_hash}:{index}.");
        tracing::error!("Native blob verification failed: {verifier:?}");
    }

    HyliOutput {
        version: 1,
        initial_state: StateCommitment::default(),
        next_state: StateCommitment::default(),
        identity,
        index,
        tx_blob_count: blobs.len(),
        blobs,
        success,
        tx_hash,
        tx_ctx: None,
        state_reads: vec![],
        onchain_effects: vec![],
        program_outputs: vec![],
    }
}

pub fn verify_native_impl(
    blob: &Blob,
    verifier: &NativeVerifiers,
) -> anyhow::Result<(Identity, bool)> {
    match verifier {
        NativeVerifiers::Blst => {
            let blst_blob = borsh::from_slice::<BlstSignatureBlob>(&blob.data.0)?;

            let msg = [blst_blob.data, blst_blob.identity.0.as_bytes().to_vec()].concat();
            // TODO: refacto BlstCrypto to avoid using ValidatorPublicKey here
            let msg = Signed {
                msg,
                signature: ValidatorSignature {
                    signature: Signature(blst_blob.signature),
                    validator: ValidatorPublicKey(blst_blob.public_key),
                },
            };
            BlstCrypto::verify(&msg)?;
            Ok((blst_blob.identity, true))
        }
        NativeVerifiers::Sha3_256 => {
            let sha3_256_blob = borsh::from_slice::<ShaBlob>(&blob.data.0)?;

            let mut hasher = sha3::Sha3_256::new();
            hasher.update(sha3_256_blob.data);
            let res = hasher.finalize().to_vec();

            Ok((sha3_256_blob.identity, res == sha3_256_blob.sha))
        }
        NativeVerifiers::Secp256k1 => {
            let secp256k1_blob = borsh::from_slice::<Secp256k1Blob>(&blob.data.0)?;

            // Convert the public key bytes to a secp256k1 PublicKey
            let public_key = PublicKey::from_slice(&secp256k1_blob.public_key)
                .map_err(|e| anyhow::anyhow!("Invalid public key: {}", e))?;

            // Convert the signature bytes to a secp256k1 Signature
            let signature = ecdsa::Signature::from_compact(&secp256k1_blob.signature)
                .map_err(|e| anyhow::anyhow!("Invalid signature: {}", e))?;

            // Create a message from the data
            let message = Message::from_digest(secp256k1_blob.data);

            // Verify the signature
            let secp = Secp256k1::new();
            let success = secp.verify_ecdsa(message, &signature, &public_key).is_ok();

            Ok((secp256k1_blob.identity, success))
        }
    }
}

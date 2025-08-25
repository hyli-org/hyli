use hyle_crypto::BlstCrypto;
use hyle_model::verifiers::*;
use hyle_model::*;
use secp256k1::{ecdsa::Signature, Message, PublicKey, Secp256k1};
use sha3::Digest;

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
                    signature: hyle_model::Signature(blst_blob.signature),
                    validator: hyle_model::ValidatorPublicKey(blst_blob.public_key),
                },
            };
            Ok((blst_blob.identity, BlstCrypto::verify(&msg)?))
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
            let signature = Signature::from_compact(&secp256k1_blob.signature)
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

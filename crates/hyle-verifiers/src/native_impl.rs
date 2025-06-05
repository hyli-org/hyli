use hyle_crypto::BlstCrypto;
use hyle_model::verifiers::*;
use hyle_model::*;
use secp256k1::{ecdsa::Signature, Message, PublicKey, Secp256k1};
use sha3::Digest;

pub(crate) fn verify_native_impl<F>(
    blob: &Blob,
    verifier: NativeVerifiers,
    check_secp_pubkey: F,
) -> anyhow::Result<(Identity, bool)>
where
    F: Fn([u8; 33]) -> Option<bool>,
{
    match verifier {
        NativeVerifiers::Blst => {
            let blob = borsh::from_slice::<BlstSignatureBlob>(&blob.data.0)?;

            let msg = [blob.data, blob.identity.0.as_bytes().to_vec()].concat();
            // TODO: refacto BlstCrypto to avoid using ValidatorPublicKey here
            let msg = Signed {
                msg,
                signature: ValidatorSignature {
                    signature: hyle_model::Signature(blob.signature),
                    validator: hyle_model::ValidatorPublicKey(blob.public_key),
                },
            };
            Ok((blob.identity, BlstCrypto::verify(&msg)?))
        }
        NativeVerifiers::Sha3_256 => {
            let blob = borsh::from_slice::<ShaBlob>(&blob.data.0)?;

            let mut hasher = sha3::Sha3_256::new();
            hasher.update(blob.data);
            let res = hasher.finalize().to_vec();
            Ok((blob.identity, res == blob.sha))
        }
        NativeVerifiers::Secp256k1 => {
            let blob = borsh::from_slice::<Secp256k1Blob>(&blob.data.0)?;

            // Convert the public key bytes to a secp256k1 PublicKey
            let public_key = PublicKey::from_slice(&blob.public_key)
                .map_err(|e| anyhow::anyhow!("Invalid public key: {}", e))?;

            // Convert the signature bytes to a secp256k1 Signature
            let signature = Signature::from_compact(&blob.signature)
                .map_err(|e| anyhow::anyhow!("Invalid signature: {}", e))?;

            // Create a message from the data
            let message = Message::from_digest(blob.data);

            // Verify the signature
            let secp = Secp256k1::new();
            let mut success = secp.verify_ecdsa(message, &signature, &public_key).is_ok();

            // Check pubkey is correct
            if let Some(res) = check_secp_pubkey(blob.public_key) {
                success &= res;
            };

            Ok((blob.identity, success))
        }
    }
}

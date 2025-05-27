#![warn(unused_crate_dependencies)]

use std::fmt::Write;
use std::io::Read;

use anyhow::{bail, Context, Error};
use hyle_model::{HyleOutput, ProgramId, ProofData, Verifier};
use rand::Rng;

#[cfg(feature = "sp1")]
use sp1_sdk::{ProverClient, SP1ProofWithPublicValues, SP1VerifyingKey};
use tracing::debug;

mod native_impl;
pub mod noir_utils;

pub fn verify(
    verifier: &Verifier,
    proof: &ProofData,
    program_id: &ProgramId,
) -> Result<Vec<HyleOutput>, Error> {
    match verifier.0.as_str() {
        #[cfg(feature = "risc0")]
        hyle_model::verifiers::RISC0_1 => risc0_1::verify(proof, program_id),
        hyle_model::verifiers::NOIR => noir::verify(proof, program_id),
        #[cfg(feature = "sp1")]
        hyle_model::verifiers::SP1_4 => sp1_4::verify(proof, program_id),
        _ => Err(anyhow::anyhow!("{} verifier not implemented yet", verifier)),
    }
}

pub fn validate_program_id(verifier: &Verifier, program_id: &ProgramId) -> Result<(), Error> {
    match verifier.0.as_str() {
        #[cfg(feature = "risc0")]
        hyle_model::verifiers::RISC0_1 => risc0_1::validate_program_id(program_id),
        #[cfg(feature = "sp1")]
        hyle_model::verifiers::SP1_4 => sp1_4::validate_program_id(program_id),
        _ => Ok(()),
    }
}

#[cfg(feature = "risc0")]
pub mod risc0_1 {
    use super::*;

    pub type Risc0ProgramId = [u8; 32];
    pub type Risc0Journal = Vec<u8>;

    pub fn verify(proof: &ProofData, program_id: &ProgramId) -> Result<Vec<HyleOutput>, Error> {
        let journal = risc0_proof_verifier(&proof.0, &program_id.0)?;
        // First try to decode it as a single HyleOutput
        Ok(
            match std::panic::catch_unwind(|| journal.decode::<Vec<HyleOutput>>()).unwrap_or(Err(
                risc0_zkvm::serde::Error::Custom("Failed to decode single HyleOutput".into()),
            )) {
                Ok(ho) => ho,
                Err(_) => {
                    debug!("Failed to decode Vec<HyleOutput>, trying to decode as HyleOutput");
                    let hyle_output = journal
                        .decode::<HyleOutput>()
                        .context("Failed to extract HyleOuput from Risc0's journal")?;

                    vec![hyle_output]
                }
            },
        )
    }

    pub fn verify_recursive(
        proof: &ProofData,
        program_id: &ProgramId,
    ) -> Result<(Vec<ProgramId>, Vec<HyleOutput>), Error> {
        let journal = risc0_proof_verifier(&proof.0, &program_id.0)?;
        let mut output = journal
            .decode::<Vec<(Risc0ProgramId, Risc0Journal)>>()
            .context("Failed to extract HyleOuput from Risc0's journal")?;

        // Doesn't actually work to just deserialize in one go.
        output
            .drain(..)
            .map(|o| {
                risc0_zkvm::serde::from_slice::<Vec<HyleOutput>, _>(&o.1)
                    .map(|h| (ProgramId(o.0.to_vec()), h))
            })
            .collect::<Result<(Vec<_>, Vec<_>), _>>()
            .map(|(ids, outputs)| (ids, outputs.into_iter().flatten().collect()))
            .context("Failed to decode HyleOutput")
    }

    pub fn risc0_proof_verifier(
        encoded_receipt: &[u8],
        image_id: &[u8],
    ) -> Result<risc0_zkvm::Journal, Error> {
        let receipt = borsh::from_slice::<risc0_zkvm::Receipt>(encoded_receipt)
            .context("Error while decoding Risc0 proof's receipt")?;

        let image_bytes: risc0_zkvm::sha::Digest =
            image_id.try_into().context("Invalid Risc0 image ID")?;

        receipt
            .verify(image_bytes)
            .context("Risc0 proof verification failed")?;

        tracing::info!("✅ Risc0 proof verified.");

        Ok(receipt.journal)
    }

    pub fn validate_program_id(program_id: &ProgramId) -> Result<(), Error> {
        std::convert::TryInto::<risc0_zkvm::sha::Digest>::try_into(program_id.0.as_slice())
            .map_err(|e| anyhow::anyhow!("Invalid Risc0 image ID: {}", e))?;
        Ok(())
    }
}

pub mod noir {
    use super::*;

    /// At present, we are using binary to facilitate the integration of the Noir verifier.
    /// This is not meant to be a permanent solution.
    pub fn verify(proof: &ProofData, image_id: &ProgramId) -> Result<Vec<HyleOutput>, Error> {
        // Define a struct with Drop implementation for cleanup
        struct TempFiles {
            proof_path: String,
            vk_path: String,
        }

        impl Drop for TempFiles {
            fn drop(&mut self) {
                if std::env::var("HYLE_KEEP_NOIR_TMP_FILES").unwrap_or_else(|_| "false".to_string())
                    != "true"
                {
                    let _ = std::fs::remove_file(&self.proof_path);
                    let _ = std::fs::remove_file(&self.vk_path);
                }
            }
        }

        let mut rng = rand::rng();
        let salt: [u8; 16] = rng.random();
        let mut salt_hex = String::with_capacity(salt.len() * 2);
        for b in &salt {
            write!(salt_hex, "{:02x}", b).unwrap();
        }

        // Create the temp files struct which will auto-clean on function exit
        let temp_files = TempFiles {
            proof_path: format!("/tmp/noir-proof-{salt_hex}"),
            vk_path: format!("/tmp/noir-vk-{salt_hex}"),
        };

        // Write proof and publicKey to files
        std::fs::write(&temp_files.proof_path, &proof.0)?;
        std::fs::write(&temp_files.vk_path, &image_id.0)?;

        debug!(
            "Proof path: {} VK path: {}",
            temp_files.proof_path, temp_files.vk_path
        );

        // Verifying proof
        let verification_output = std::process::Command::new("bb")
            .arg("verify")
            .arg("-p")
            .arg(&temp_files.proof_path)
            .arg("-k")
            .arg(&temp_files.vk_path)
            .output()?;

        if !verification_output.status.success() {
            bail!(
                "Noir proof verification failed: {}",
                String::from_utf8_lossy(&verification_output.stderr)
            );
        }

        // Extracting outputs
        let mut file =
            std::fs::File::open(&temp_files.proof_path).context("Failed to open proof file")?;
        let mut proof = Vec::new();
        file.read_to_end(&mut proof)
            .context("Failed to read proof file content")?;

        // TODO: support multi-output proofs.
        let hyle_output = crate::noir_utils::parse_noir_output(&proof, &image_id.0)?;

        tracing::info!("✅ Noir proof verified.");

        Ok(vec![hyle_output])
        // temp_files is automatically dropped here, cleaning up all files
    }
}

/// The following environment variables are used to configure the prover:
/// - `SP1_PROVER`: The type of prover to use. Must be one of `mock`, `local`, `cuda`, or `network`.
#[cfg(feature = "sp1")]
pub mod sp1_4 {
    use super::*;
    use once_cell::sync::Lazy;

    static SP1_CLIENT: Lazy<sp1_sdk::EnvProver> = Lazy::new(|| {
        tracing::trace!("Setup sp1 prover client from env");
        ProverClient::from_env()
    });

    pub fn init() {
        tracing::info!("Initializing sp1 verifier");
        let _client = &*SP1_CLIENT;
    }

    pub fn verify(
        proof_bin: &ProofData,
        verification_key: &ProgramId,
    ) -> Result<Vec<HyleOutput>, Error> {
        let client = &*SP1_CLIENT;

        let proof: SP1ProofWithPublicValues =
            bincode::deserialize(&proof_bin.0).context("Error while decoding SP1 proof.")?;

        // Deserialize verification key from JSON
        let vk: SP1VerifyingKey =
            serde_json::from_slice(&verification_key.0).context("Invalid SP1 image ID")?;

        // Verify the proof.
        tracing::trace!("Verifying SP1 proof");
        client
            .verify(&proof, &vk)
            .context("SP1 proof verification failed")?;

        tracing::trace!("Extract HyleOutput");
        let hyle_outputs =
            match borsh::from_slice::<Vec<HyleOutput>>(proof.public_values.as_slice()) {
                Ok(outputs) => outputs,
                Err(_) => {
                    debug!("Failed to decode Vec<HyleOutput>, trying to decode as HyleOutput");
                    vec![
                        borsh::from_slice::<HyleOutput>(proof.public_values.as_slice())
                            .context("Failed to extract HyleOuput from SP1 proof")?,
                    ]
                }
            };

        tracing::info!("✅ SP1 proof verified.",);

        Ok(hyle_outputs)
    }

    pub fn validate_program_id(program_id: &ProgramId) -> Result<(), Error> {
        serde_json::from_slice::<SP1VerifyingKey>(program_id.0.as_slice())
            .map_err(|e| anyhow::anyhow!("Invalid SP1 image ID: {}", e))?;
        Ok(())
    }
}

pub mod native {
    use super::*;
    use hyle_model::{
        verifiers::NativeVerifiers, Blob, BlobIndex, Identity, IndexedBlobs, StateCommitment,
        TxHash,
    };

    pub fn verify(
        tx_hash: TxHash,
        index: BlobIndex,
        blobs: &[Blob],
        verifier: NativeVerifiers,
    ) -> HyleOutput {
        #[allow(clippy::expect_used, reason = "Logic error in the code")]
        let blob = blobs.get(index.0).expect("Invalid blob index");
        let blobs: IndexedBlobs = blobs.iter().cloned().into();

        let (identity, success) = match crate::native_impl::verify_native_impl(blob, verifier) {
            Ok((identity, success)) => (identity, success),
            Err(e) => {
                tracing::trace!("Native blob verification failed: {:?}", e);
                (Identity::default(), false)
            }
        };

        if success {
            tracing::info!("✅ Native blob verified on {tx_hash}:{index}");
        } else {
            tracing::info!("❌ Native blob verification failed on {tx_hash}:{index}.");
        }

        HyleOutput {
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
}

#[cfg(test)]
mod tests {
    use std::{fs::File, io::Read};

    use hyle_model::{
        Blob, BlobData, BlobIndex, HyleOutput, Identity, IndexedBlobs, ProgramId, ProofData,
        StateCommitment, TxHash,
    };

    use super::noir::verify as noir_proof_verifier;
    #[cfg(feature = "risc0")]
    use super::risc0_1::validate_program_id as validate_risc0_program_id;

    fn load_file_as_bytes(path: &str) -> Vec<u8> {
        let mut file = File::open(path).expect("Failed to open file");
        let mut encoded_receipt = Vec::new();
        file.read_to_end(&mut encoded_receipt)
            .expect("Failed to read file content");
        encoded_receipt
    }

    /*
        For this test, the proof/vk and the output are obtained running this simple Noir code
        ```
            fn main(
            version: pub u32,
            initial_state_len: pub u32,
            initial_state: pub [u8; 4],
            next_state_len: pub u32,
            next_state: pub [u8; 4],
            identity_len: pub u8,
            identity: pub str<56>,
            tx_hash_len: pub u32,
            tx_hash: pub [u8; 0],
            index: pub u32,
            blobs_len: pub u32,
            blobs: pub [Field; 10],
            success: pub bool
            ) {}
        ```
        With the values:
        ```
            version = 1
            blobs = [3, 1, 1, 2, 1, 1, 2, 1, 1, 0]
            initial_state_len = 4
            initial_state = [0, 0, 0, 0]
            next_state_len = 4
            next_state = [0, 0, 0, 0]
            identity_len = 56
            identity = "3f368bf90c71946fc7b0cde9161ace42985d235f@ecdsa_secp256r1"
            tx_hash_len = 0
            tx_hash = []
            blobs_len = 9
            index = 0
            success = 1
        ```
    */
    #[ignore = "manual test"]
    #[test_log::test]
    fn test_noir_proof_verifier() {
        let noir_proof = load_file_as_bytes("./tests/proofs/webauthn.noir.proof");
        let image_id = load_file_as_bytes("./tests/proofs/webauthn.noir.vk");

        let result = noir_proof_verifier(&ProofData(noir_proof), &ProgramId(image_id));
        match result {
            Ok(outputs) => {
                assert_eq!(
                    outputs,
                    vec![HyleOutput {
                        version: 1,
                        initial_state: StateCommitment(vec![0, 0, 0, 0]),
                        next_state: StateCommitment(vec![0, 0, 0, 0]),
                        identity: Identity(
                            "3f368bf90c71946fc7b0cde9161ace42985d235f@ecdsa_secp256r1".to_owned()
                        ),
                        index: BlobIndex(0),
                        blobs: IndexedBlobs(vec![(
                            BlobIndex(0),
                            Blob {
                                contract_name: "webauthn".into(),
                                data: BlobData(vec![3, 1, 1, 2, 1, 1, 2, 1, 1, 0])
                            }
                        )]),
                        tx_blob_count: 1,
                        success: true,
                        tx_hash: TxHash::default(), // TODO
                        state_reads: vec![],
                        tx_ctx: None,
                        onchain_effects: vec![],
                        program_outputs: vec![]
                    }]
                );
            }
            Err(e) => panic!("Noir verification failed: {:?}", e),
        }
    }

    #[test]
    #[cfg(feature = "risc0")]
    fn test_check_risc0_program_id() {
        let valid_program_id = ProgramId(vec![0; 32]); // Assuming a valid 32-byte ID
        let invalid_program_id = ProgramId(vec![0; 31]); // Invalid length

        assert!(validate_risc0_program_id(&valid_program_id).is_ok());
        assert!(validate_risc0_program_id(&invalid_program_id).is_err());
    }
}

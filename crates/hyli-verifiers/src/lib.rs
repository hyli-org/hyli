use std::fmt::Write;
use std::io::Read;

use anyhow::{bail, Context, Error};
use hyli_model::{HyliOutput, ProgramId, ProofData, Verifier};
use rand::Rng;

#[cfg(feature = "sp1")]
use sp1_sdk::{ProverClient, SP1ProofWithPublicValues, SP1VerifyingKey};
use tracing::debug;

pub mod noir_utils;

pub fn verify(
    verifier: &Verifier,
    proof: &ProofData,
    program_id: &ProgramId,
) -> Result<Vec<HyliOutput>, Error> {
    match verifier.0.as_str() {
        #[cfg(feature = "cairo-m")]
        hyli_model::verifiers::CAIRO_M => cairo_m::verify(proof, program_id),
        #[cfg(feature = "risc0")]
        hyli_model::verifiers::RISC0_1 => risc0_1::verify(proof, program_id),
        hyli_model::verifiers::NOIR => noir::verify(proof, program_id),
        #[cfg(feature = "sp1")]
        hyli_model::verifiers::SP1_4 => sp1_4::verify(proof, program_id),
        _ => Err(anyhow::anyhow!("{} verifier not implemented yet", verifier)),
    }
}

#[cfg(feature = "cairo-m")]
pub mod cairo_m {
    use super::*;
    use cairo_m_prover::{verifier::verify_cairo_m, Proof};
    use serde::{Deserialize, Serialize};
    use stwo_prover::core::vcs::blake2_merkle::{Blake2sMerkleChannel, Blake2sMerkleHasher};

    #[derive(Serialize, Deserialize)]
    pub struct HyliOutputCairoM {
        pub hyli_output: HyliOutput,
        pub proof: Proof<Blake2sMerkleHasher>,
    }

    pub fn verify(
        proof_bytes: &ProofData,
        program_id: &ProgramId,
    ) -> Result<Vec<HyliOutput>, Error> {
        let hyli_output_cairo_m: HyliOutputCairoM = sonic_rs::from_slice(&proof_bytes.0)?;
        let proof_program_id = hyli_output_cairo_m.proof.program_id().0.to_le_bytes();

        if program_id.0 != proof_program_id {
            return Err(anyhow::anyhow!("Invalid Cairo M program ID"));
        };

        verify_cairo_m::<Blake2sMerkleChannel>(hyli_output_cairo_m.proof, None)?;

        Ok(vec![hyli_output_cairo_m.hyli_output])
    }
}

#[cfg(feature = "risc0")]
pub mod risc0_1 {
    use super::*;

    pub type Risc0ProgramId = [u8; 32];
    pub type Risc0Journal = Vec<u8>;

    pub fn verify(proof: &ProofData, program_id: &ProgramId) -> Result<Vec<HyliOutput>, Error> {
        let journal = risc0_proof_verifier(&proof.0, &program_id.0)?;
        // First try to decode it as a single HyliOutput
        Ok(
            match std::panic::catch_unwind(|| journal.decode::<Vec<HyliOutput>>()).unwrap_or(Err(
                risc0_zkvm::serde::Error::Custom("Failed to decode single HyliOutput".into()),
            )) {
                Ok(ho) => ho,
                Err(_) => {
                    debug!("Failed to decode Vec<HyliOutput>, trying to decode as HyliOutput");
                    let hyli_output = journal
                        .decode::<HyliOutput>()
                        .context("Failed to extract HyliOuput from Risc0's journal")?;

                    vec![hyli_output]
                }
            },
        )
    }

    pub fn verify_recursive(
        proof: &ProofData,
        program_id: &ProgramId,
    ) -> Result<(Vec<ProgramId>, Vec<HyliOutput>), Error> {
        let journal = risc0_proof_verifier(&proof.0, &program_id.0)?;
        let mut output = journal
            .decode::<Vec<(Risc0ProgramId, Risc0Journal)>>()
            .context("Failed to extract HyliOuput from Risc0's journal")?;

        // Doesn't actually work to just deserialize in one go.
        output
            .drain(..)
            .map(|o| {
                risc0_zkvm::serde::from_slice::<Vec<HyliOutput>, _>(&o.1)
                    .map(|h| (ProgramId(o.0.to_vec()), h))
            })
            .collect::<Result<(Vec<_>, Vec<_>), _>>()
            .map(|(ids, outputs)| (ids, outputs.into_iter().flatten().collect()))
            .context("Failed to decode HyliOutput")
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
}

pub mod noir {
    use super::*;

    /// At present, we are using binary to facilitate the integration of the Noir verifier.
    /// This is not meant to be a permanent solution.
    pub fn verify(proof: &ProofData, image_id: &ProgramId) -> Result<Vec<HyliOutput>, Error> {
        // Define a struct with Drop implementation for cleanup
        struct TempFiles {
            proof_path: String,
            vk_path: String,
        }

        impl Drop for TempFiles {
            fn drop(&mut self) {
                if std::env::var("HYLI_KEEP_NOIR_TMP_FILES").unwrap_or_else(|_| "false".to_string())
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
            write!(salt_hex, "{b:02x}").unwrap();
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
        let hyli_output = crate::noir_utils::parse_noir_output(&proof, &image_id.0)?;

        tracing::info!("✅ Noir proof verified.");

        Ok(vec![hyli_output])
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
    ) -> Result<Vec<HyliOutput>, Error> {
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

        tracing::trace!("Extract HyliOutput");
        let hyli_outputs =
            match borsh::from_slice::<Vec<HyliOutput>>(proof.public_values.as_slice()) {
                Ok(outputs) => outputs,
                Err(_) => {
                    debug!("Failed to decode Vec<HyliOutput>, trying to decode as HyliOutput");
                    vec![
                        borsh::from_slice::<HyliOutput>(proof.public_values.as_slice())
                            .context("Failed to extract HyliOuput from SP1 proof")?,
                    ]
                }
            };

        tracing::info!("✅ SP1 proof verified.",);

        Ok(hyli_outputs)
    }
}

#[cfg(test)]
mod tests {
    use std::{fs::File, io::Read};

    use hyli_model::{
        Blob, BlobData, BlobIndex, HyliOutput, Identity, IndexedBlobs, ProgramId, ProofData,
        StateCommitment, TxHash,
    };

    use super::noir::verify as noir_proof_verifier;

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
                    vec![HyliOutput {
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
            Err(e) => panic!("Noir verification failed: {e:?}"),
        }
    }
}

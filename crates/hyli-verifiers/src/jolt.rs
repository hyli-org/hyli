use anyhow::{Context, Error};
use hyli_model::{verifiers::jolt::JoltProofData, HyliOutput, ProgramId, ProofData};
use jolt_sdk::{JoltProof, JoltVerifierPreprocessing, Serializable};

pub fn verifier_preprocessing_to_program_id(verifier_preprocessing: &[u8]) -> ProgramId {
    use sha3::{Digest, Sha3_256};
    let mut hasher = Sha3_256::new();
    hasher.update(verifier_preprocessing);
    let hash_bytes = hasher.finalize();
    ProgramId(hash_bytes.to_vec())
}

pub fn verify(proof: &ProofData, program_id: &ProgramId) -> Result<Vec<HyliOutput>, Error> {
    let JoltProofData {
        input,
        output,
        proof,
        verifier_preprocessing: preprocessing,
    } = proof.try_into()?;

    if verifier_preprocessing_to_program_id(&preprocessing) != *program_id {
        return Err(anyhow::anyhow!(
            "verifier preprocessing hash does not match program id"
        ));
    }

    let preprocessing = JoltVerifierPreprocessing::deserialize_from_bytes(&preprocessing)
        .map_err(|e| anyhow::anyhow!("deserializing Jolt verifier preprocessing: {e}"))?;

    let proof = JoltProof::deserialize_from_bytes(&proof)
        .map_err(|e| anyhow::anyhow!("deserializing Jolt proof: {e}"))?;

    jolt_sdk::host_utils::guest::verifier::verify(&input, None, &output, proof, &preprocessing)
        .context("verifying proof with Jolt")?;

    borsh::from_slice::<Vec<HyliOutput>>(&output).context("parsing proof output as HyliOutput")
}

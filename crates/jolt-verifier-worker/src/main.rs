use anyhow::{Context, Result};
use borsh::to_vec;
use hyli_model::{
    verifier_worker::{VerifyRequest, VerifyResponse},
    verifiers::jolt::JoltProofData,
    verifiers::JOLT_0_1,
    HyliOutput, ProgramId, ProofData,
};
use hyli_verifier_worker_core::{init_worker_tracing, run_worker_loop};
use jolt_sdk::{JoltProof, JoltVerifierPreprocessing, Serializable};
use sha3::Digest;
use tokio::io::{self, BufReader, BufWriter};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    init_worker_tracing("info")?;

    let stdin = io::stdin();
    let stdout = io::stdout();
    run_worker_loop(
        BufReader::new(stdin),
        BufWriter::new(stdout),
        handle_request,
    )
    .await
}

async fn handle_request(request: VerifyRequest) -> Result<VerifyResponse> {
    if request.verifier != JOLT_0_1 {
        return Ok(VerifyResponse {
            ok: false,
            outputs: vec![],
            error: format!("unsupported verifier '{}'", request.verifier),
        });
    }

    let outputs = verify_jolt(&ProofData(request.proof), &ProgramId(request.program_id))
        .context("verifying Jolt proof")?;

    Ok(VerifyResponse {
        ok: true,
        outputs: to_vec(&outputs).context("serializing proof outputs")?,
        error: String::new(),
    })
}

fn verifier_preprocessing_to_program_id(verifier_preprocessing: &[u8]) -> ProgramId {
    let mut hasher = sha3::Sha3_256::new();
    hasher.update(verifier_preprocessing);
    ProgramId(hasher.finalize().to_vec())
}

fn verify_jolt(proof: &ProofData, program_id: &ProgramId) -> Result<Vec<HyliOutput>> {
    let JoltProofData {
        input,
        output,
        proof,
        verifier_preprocessing,
    } = proof
        .try_into()
        .context("decoding Jolt proof payload from ProofData")?;

    if verifier_preprocessing_to_program_id(&verifier_preprocessing) != *program_id {
        anyhow::bail!("verifier preprocessing hash does not match program id");
    }

    let verifier_preprocessing =
        JoltVerifierPreprocessing::deserialize_from_bytes(&verifier_preprocessing)
            .map_err(|e| anyhow::anyhow!("deserializing Jolt verifier preprocessing: {e}"))?;
    let proof = JoltProof::deserialize_from_bytes(&proof)
        .map_err(|e| anyhow::anyhow!("deserializing Jolt proof: {e}"))?;

    jolt_sdk::host_utils::guest::verifier::verify(
        &input,
        None,
        &output,
        proof,
        &verifier_preprocessing,
    )
    .context("verifying proof with Jolt")?;

    borsh::from_slice::<Vec<HyliOutput>>(&output)
        .map_err(|e| anyhow::anyhow!("parsing proof output as HyliOutput: {e}"))
}

use anyhow::{Context, Result};
use borsh::to_vec;
use hyli_jolt_model::JoltRegistryEntry;
use hyli_model::{
    verifier_worker::{VerifyRequest, VerifyResponse},
    verifiers::jolt::JoltProofData,
    verifiers::JOLT_0_1,
    HyliOutput, ProgramId, ProofData,
};
use hyli_verifier_worker_core::{init_worker_tracing, run_worker_loop};
use jolt_sdk::{JoltProof, JoltVerifierPreprocessing, Serializable};
use tracing::{error, info};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    init_worker_tracing("info")?;

    run_worker_loop(handle_request).await
}

async fn handle_request(request: VerifyRequest) -> Result<VerifyResponse> {
    if request.verifier != JOLT_0_1 {
        return Ok(VerifyResponse {
            ok: false,
            outputs: vec![],
            error: format!("unsupported verifier '{}'", request.verifier),
        });
    }

    let outputs = match verify_jolt(&ProofData(request.proof), &ProgramId(request.program_id))
        .await
        .context("verifying Jolt proof")
    {
        Ok(outputs) => outputs,
        Err(err) => {
            error!("❌ Jolt proof verification failed: {err:#}");
            return Err(err);
        }
    };

    Ok(VerifyResponse {
        ok: true,
        outputs: to_vec(&outputs).context("serializing proof outputs")?,
        error: String::new(),
    })
}

async fn verify_jolt(proof: &ProofData, program_id: &ProgramId) -> Result<Vec<HyliOutput>> {
    info!(
        "⚡ Verifying Jolt proof for program_id {}",
        hex::encode(&program_id.0)
    );
    let JoltProofData {
        input,
        output,
        proof,
    } = proof
        .try_into()
        .context("decoding Jolt proof payload from ProofData")?;

    let binary = hyli_registry::download_elf_by_program_id(&hex::encode(&program_id.0)).await?;
    info!(
        "Downloaded ELF binary for program_id {}",
        hex::encode(&program_id.0)
    );
    info!("ELF binary size: {} bytes", binary.len());
    let registry_entry: JoltRegistryEntry = borsh::from_slice(&binary)
        .map_err(|e| anyhow::anyhow!("deserializing Jolt registry entry: {e}"))?;

    let verifier_preprocessing = JoltVerifierPreprocessing::from(&registry_entry.preprocessing.0);

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

    jolt_sdk::postcard::from_bytes::<Vec<HyliOutput>>(&output)
        .map_err(|e| anyhow::anyhow!("parsing proof output as HyliOutput: {e}"))
}

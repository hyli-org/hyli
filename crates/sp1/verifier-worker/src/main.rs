use anyhow::{Context, Result};
use borsh::to_vec;
use hyli_model::{
    verifier_worker::{VerifyRequest, VerifyResponse},
    verifiers::SP1_4,
    HyliOutput,
};
use hyli_verifier_worker_core::{init_worker_tracing, run_worker_loop};
use once_cell::sync::Lazy;
use sp1_sdk::{ProverClient, SP1ProofWithPublicValues, SP1VerifyingKey};
use tracing::{debug, error, info};

static SP1_CLIENT: Lazy<sp1_sdk::EnvProver> = Lazy::new(|| {
    tracing::trace!("Setup sp1 prover client from env");
    ProverClient::from_env()
});

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    init_worker_tracing("info")?;

    // Initialize the client eagerly so startup errors surface early.
    let _client = &*SP1_CLIENT;

    run_worker_loop(|request| async move { handle_request(request).await }).await
}

async fn handle_request(request: VerifyRequest) -> Result<VerifyResponse> {
    if request.verifier != SP1_4 {
        return Ok(VerifyResponse {
            ok: false,
            outputs: vec![],
            error: format!("unsupported verifier '{}'", request.verifier),
        });
    }

    let outputs = match verify_sp1(&request.proof, &request.program_id)
        .await
        .context("verifying SP1 proof")
    {
        Ok(outputs) => outputs,
        Err(err) => {
            error!("❌ SP1 proof verification failed: {err:#}");
            return Err(err);
        }
    };

    Ok(VerifyResponse {
        ok: true,
        outputs: to_vec(&outputs).context("serializing proof outputs")?,
        error: String::new(),
    })
}

async fn verify_sp1(proof_bytes: &[u8], program_id_bytes: &[u8]) -> Result<Vec<HyliOutput>> {
    info!("⚡ Verifying SP1 proof");

    let client = &*SP1_CLIENT;

    let proof: SP1ProofWithPublicValues =
        bincode::deserialize(proof_bytes).context("decoding SP1 proof")?;

    let vk: SP1VerifyingKey =
        serde_json::from_slice(program_id_bytes).context("decoding SP1 verifying key")?;

    client
        .verify(&proof, &vk)
        .context("SP1 proof verification failed")?;

    let hyli_outputs = match borsh::from_slice::<Vec<HyliOutput>>(proof.public_values.as_slice()) {
        Ok(outputs) => outputs,
        Err(_) => {
            debug!("Failed to decode Vec<HyliOutput>, trying to decode as HyliOutput");
            vec![
                borsh::from_slice::<HyliOutput>(proof.public_values.as_slice())
                    .context("Failed to extract HyliOutput from SP1 proof")?,
            ]
        }
    };

    info!("✅ SP1 proof verified.");

    Ok(hyli_outputs)
}

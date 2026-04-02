use anyhow::{Context, Result};
use borsh::to_vec;
use hyli_model::{
    verifier_worker::{VerifyRequest, VerifyResponse},
    verifiers::RETH,
};
use hyli_verifier_worker_core::{init_worker_tracing, run_worker_loop};
use tracing::{error, info};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    init_worker_tracing("info")?;
    run_worker_loop(|request| async move { handle_request(request).await }).await
}

async fn handle_request(request: VerifyRequest) -> Result<VerifyResponse> {
    if request.verifier != RETH {
        return Ok(VerifyResponse {
            ok: false,
            outputs: vec![],
            error: format!("unsupported verifier '{}'", request.verifier),
        });
    }

    info!("⚡ Verifying Reth proof");

    let proof = hyli_model::ProofData(request.proof);
    let program_id = hyli_model::ProgramId(request.program_id);

    let outputs = match hyli_reth_verifier_worker::verify(&proof, &program_id)
        .context("verifying Reth proof")
    {
        Ok(outputs) => outputs,
        Err(err) => {
            error!("❌ Reth proof verification failed: {err:#}");
            return Err(err);
        }
    };

    info!("✅ Reth proof verified.");

    Ok(VerifyResponse {
        ok: true,
        outputs: to_vec(&outputs).context("serializing proof outputs")?,
        error: String::new(),
    })
}

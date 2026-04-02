use anyhow::{Context, Result};
use borsh::to_vec;
use hyli_model::{
    verifier_worker::{VerifyRequest, VerifyResponse},
    verifiers::RISC0_3,
    HyliOutput, ProgramId,
};
use hyli_verifier_worker_core::{init_worker_tracing, run_worker_loop};
use tracing::{error, info};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    init_worker_tracing("info")?;
    run_worker_loop(|request| async move { handle_request(request).await }).await
}

async fn handle_request(request: VerifyRequest) -> Result<VerifyResponse> {
    if request.verifier != RISC0_3 {
        return Ok(VerifyResponse {
            ok: false,
            outputs: vec![],
            error: format!("unsupported verifier '{}'", request.verifier),
        });
    }

    if request.recursive {
        let result = verify_recursive(&request.proof, &request.program_id)
            .context("verifying Risc0 recursive proof");
        match result {
            Ok(outputs) => Ok(VerifyResponse {
                ok: true,
                outputs: to_vec(&outputs).context("serializing recursive proof outputs")?,
                error: String::new(),
            }),
            Err(err) => {
                error!("❌ Risc0 recursive proof verification failed: {err:#}");
                Err(err)
            }
        }
    } else {
        let result = verify(&request.proof, &request.program_id).context("verifying Risc0 proof");
        match result {
            Ok(outputs) => Ok(VerifyResponse {
                ok: true,
                outputs: to_vec(&outputs).context("serializing proof outputs")?,
                error: String::new(),
            }),
            Err(err) => {
                error!("❌ Risc0 proof verification failed: {err:#}");
                Err(err)
            }
        }
    }
}

type Risc0ProgramId = [u8; 32];
type Risc0Journal = Vec<u8>;

fn verify(proof_bytes: &[u8], program_id_bytes: &[u8]) -> Result<Vec<HyliOutput>> {
    let journal = risc0_proof_verifier(proof_bytes, program_id_bytes)?;
    Ok(
        match std::panic::catch_unwind(|| journal.decode::<Vec<HyliOutput>>()).unwrap_or(Err(
            risc0_zkvm::serde::Error::Custom("Failed to decode Vec<HyliOutput>".into()),
        )) {
            Ok(ho) => ho,
            Err(_) => {
                tracing::debug!("Failed to decode Vec<HyliOutput>, trying to decode as HyliOutput");
                let hyli_output = journal
                    .decode::<HyliOutput>()
                    .context("Failed to extract HyliOutput from Risc0's journal")?;
                vec![hyli_output]
            }
        },
    )
}

fn verify_recursive(
    proof_bytes: &[u8],
    program_id_bytes: &[u8],
) -> Result<(Vec<ProgramId>, Vec<HyliOutput>)> {
    let journal = risc0_proof_verifier(proof_bytes, program_id_bytes)?;
    let mut output = journal
        .decode::<Vec<(Risc0ProgramId, Risc0Journal)>>()
        .context("Failed to extract HyliOutput from Risc0's journal")?;

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

fn risc0_proof_verifier(encoded_receipt: &[u8], image_id: &[u8]) -> Result<risc0_zkvm::Journal> {
    let receipt = borsh::from_slice::<risc0_zkvm::Receipt>(encoded_receipt)
        .context("Error while decoding Risc0 proof's receipt")?;

    let image_bytes: risc0_zkvm::sha::Digest =
        image_id.try_into().context("Invalid Risc0 image ID")?;

    receipt
        .verify(image_bytes)
        .context("Risc0 proof verification failed")?;

    info!("✅ Risc0 proof verified.");

    Ok(receipt.journal)
}

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
use std::{cell::RefCell, path::PathBuf, rc::Rc};
use tracing::{error, info};

struct JoltCache {
    /// Most recently used entry, kept in memory to avoid disk I/O.
    last: Option<(Vec<u8>, Vec<u8>)>, // (program_id bytes, raw registry binary)
    /// Directory for on-disk cache of all other entries.
    cache_dir: PathBuf,
}

impl JoltCache {
    fn new(cache_dir: PathBuf) -> Result<Self> {
        std::fs::create_dir_all(&cache_dir)
            .with_context(|| format!("creating cache directory {}", cache_dir.display()))?;
        Ok(Self {
            last: None,
            cache_dir,
        })
    }

    fn get_from_memory(&self, program_id: &[u8]) -> Option<Vec<u8>> {
        self.last.as_ref().and_then(|(id, binary)| {
            if id == program_id {
                Some(binary.clone())
            } else {
                None
            }
        })
    }

    fn disk_path(&self, hex_id: &str) -> PathBuf {
        self.cache_dir.join(hex_id)
    }

    fn set_last(&mut self, program_id: Vec<u8>, binary: Vec<u8>) {
        self.last = Some((program_id, binary));
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    init_worker_tracing("info")?;

    let cache_dir = std::env::var("JOLT_CACHE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            let mut p = std::env::temp_dir();
            p.push("hyli-jolt-cache");
            p
        });

    let cache = Rc::new(RefCell::new(JoltCache::new(cache_dir)?));

    run_worker_loop(move |request| {
        let cache = Rc::clone(&cache);
        async move { handle_request(request, cache).await }
    })
    .await
}

async fn handle_request(
    request: VerifyRequest,
    cache: Rc<RefCell<JoltCache>>,
) -> Result<VerifyResponse> {
    if request.verifier != JOLT_0_1 {
        return Ok(VerifyResponse {
            ok: false,
            outputs: vec![],
            error: format!("unsupported verifier '{}'", request.verifier),
        });
    }

    let outputs =
        match verify_jolt(&ProofData(request.proof), &ProgramId(request.program_id), cache)
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

/// Fetches the raw registry binary for a program_id, using the cache.
/// Checks memory first, then disk, then downloads from the registry.
/// The returned binary is also promoted to the in-memory last-entry slot.
async fn get_registry_binary(program_id: &[u8], cache: &Rc<RefCell<JoltCache>>) -> Result<Vec<u8>> {
    let hex_id = hex::encode(program_id);

    // 1. Memory cache (fast path — no I/O)
    if let Some(binary) = cache.borrow().get_from_memory(program_id) {
        info!("Cache hit (memory) for program_id {}", hex_id);
        return Ok(binary);
    }

    // 2. Disk cache
    let disk_path = cache.borrow().disk_path(&hex_id);
    let binary = if disk_path.exists() {
        info!("Cache hit (disk) for program_id {}", hex_id);
        tokio::fs::read(&disk_path)
            .await
            .with_context(|| format!("reading disk cache for {}", hex_id))?
    } else {
        // 3. Download from registry
        info!("Downloading registry entry for program_id {}", hex_id);
        let binary = hyli_registry::download_elf_by_program_id(&hex_id).await?;
        info!(
            "Downloaded registry entry for program_id {} ({} bytes)",
            hex_id,
            binary.len()
        );
        tokio::fs::write(&disk_path, &binary)
            .await
            .with_context(|| format!("writing disk cache for {}", hex_id))?;
        info!("Cached registry entry to disk for program_id {}", hex_id);
        binary
    };

    // Promote to in-memory last-entry slot
    cache.borrow_mut().set_last(program_id.to_vec(), binary.clone());

    Ok(binary)
}

async fn verify_jolt(
    proof: &ProofData,
    program_id: &ProgramId,
    cache: Rc<RefCell<JoltCache>>,
) -> Result<Vec<HyliOutput>> {
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

    let binary = get_registry_binary(&program_id.0, &cache).await?;

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

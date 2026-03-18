use anyhow::Result;
use jolt_sdk::guest::program::Program;
use hyli_model::{Calldata, ProgramId, Proof, ProofData};
use sha3::Digest;

pub use hyli_jolt_model::{BorshMemoryConfig, BorshableJoltProverPreprocessing, JoltRegistryEntry};

pub fn verifier_preprocessing_to_program_id(verifier_preprocessing: &[u8]) -> ProgramId {
    let mut hasher = sha3::Sha3_256::new();
    hasher.update(verifier_preprocessing);
    ProgramId(hasher.finalize().to_vec())
}

type Handler = Box<dyn Fn(Vec<u8>, Vec<Calldata>) + Send + Sync>;

pub struct JoltProver {
    pub program_id: ProgramId,
    pub program: Program,
    pub prover_preprocessing:
        jolt_sdk::JoltProverPreprocessing<jolt_sdk::F, jolt_sdk::Curve, jolt_sdk::PCS>,
    pub memory_config: jolt_sdk::MemoryConfig,
    pub trace_handler: Option<Handler>,
}

impl JoltProver {
    pub fn new(entry: JoltRegistryEntry, program_id: ProgramId) -> Self {
        Self::new_with_handler(entry, program_id, None)
    }

    pub fn new_with_handler(
        entry: JoltRegistryEntry,
        program_id: ProgramId,
        trace_handler: Option<Handler>,
    ) -> Self {
        let JoltRegistryEntry {
            elf,
            memory_config,
            preprocessing,
        } = entry;

        let mut memory_config: jolt_sdk::MemoryConfig = memory_config.into();

        let program = Program::new(&elf, &memory_config);
        let preprocessing = preprocessing.0;

        let (_, _, d, _) = program.decode();

        memory_config.program_size = Some(d);

        Self {
            program,
            prover_preprocessing: preprocessing,
            program_id,
            trace_handler,
            memory_config,
        }
    }

    pub async fn prove(
        &self,
        commitment_metadata: Vec<u8>,
        calldatas: Vec<Calldata>,
    ) -> Result<Proof> {
        use jolt_sdk::Serializable;
        use hyli_model::ProofMetadata;

        let mut input_bytes = Vec::new();
        input_bytes.append(&mut jolt_sdk::postcard::to_stdvec(&commitment_metadata).unwrap());
        input_bytes.append(&mut jolt_sdk::postcard::to_stdvec(&calldatas).unwrap());

        let mut output_bytes = vec![0; self.memory_config.max_output_size as usize];

        if let Some(handler) = &self.trace_handler {
            handler(commitment_metadata, calldatas);
        }

        let elf_path: Option<std::path::PathBuf> = if std::env::var("JOLT_BACKTRACE").is_ok() {
            let tmp = tempfile::NamedTempFile::new()?;
            std::fs::write(tmp.path(), &self.program.elf_contents)?;
            Some(tmp.into_temp_path().keep()?)
        } else {
            None
        };

        let (_, _, _, _device, _) = jolt_sdk::host_utils::guest::program::trace(
            &self.program.elf_contents,
            elf_path.as_ref(),
            &input_bytes,
            &[],
            &[],
            &self.memory_config,
            None,
        );

        let (proof, _device, _debug_info) = jolt_sdk::host_utils::guest::prover::prove(
            &self.program,
            &input_bytes,
            &[],
            &[],
            None,
            None,
            &mut output_bytes,
            &self.prover_preprocessing,
        );

        let proof = proof
            .serialize_to_bytes()
            .map_err(|e| anyhow::anyhow!("serializing Jolt proof: {e}"))?;

        let data: ProofData = hyli_model::verifiers::jolt::JoltProofData {
            input: input_bytes,
            output: output_bytes,
            proof,
        }
        .try_into()?;

        let metadata = ProofMetadata::default();

        Ok(Proof { data, metadata })
    }
}

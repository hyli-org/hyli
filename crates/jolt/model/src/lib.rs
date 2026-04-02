use borsh::{BorshDeserialize, BorshSerialize};
use jolt_sdk::{JoltProverPreprocessing, Serializable};

#[derive(BorshDeserialize, BorshSerialize, Clone)]
pub struct BorshMemoryConfig {
    pub max_input_size: u64,
    pub max_trusted_advice_size: u64,
    pub max_untrusted_advice_size: u64,
    pub max_output_size: u64,
    pub stack_size: u64,
    pub heap_size: u64,
    pub program_size: Option<u64>,
}

impl From<BorshMemoryConfig> for jolt_sdk::MemoryConfig {
    fn from(config: BorshMemoryConfig) -> Self {
        Self {
            max_input_size: config.max_input_size,
            max_trusted_advice_size: config.max_trusted_advice_size,
            max_untrusted_advice_size: config.max_untrusted_advice_size,
            max_output_size: config.max_output_size,
            stack_size: config.stack_size,
            heap_size: config.heap_size,
            program_size: config.program_size,
        }
    }
}

impl From<jolt_sdk::MemoryConfig> for BorshMemoryConfig {
    fn from(config: jolt_sdk::MemoryConfig) -> Self {
        Self {
            max_input_size: config.max_input_size,
            max_trusted_advice_size: config.max_trusted_advice_size,
            max_untrusted_advice_size: config.max_untrusted_advice_size,
            max_output_size: config.max_output_size,
            stack_size: config.stack_size,
            heap_size: config.heap_size,
            program_size: config.program_size,
        }
    }
}

pub struct BorshableJoltProverPreprocessing(
    pub JoltProverPreprocessing<jolt_sdk::F, jolt_sdk::Curve, jolt_sdk::PCS>,
);

impl BorshSerialize for BorshableJoltProverPreprocessing {
    fn serialize<W: borsh::io::Write>(&self, writer: &mut W) -> borsh::io::Result<()> {
        let bytes = self.0.serialize_to_bytes().map_err(|e| {
            borsh::io::Error::other(format!("Failed to serialize JoltProverPreprocessing: {e}"))
        })?;
        (bytes.len() as u64).serialize(writer)?;
        writer.write_all(&bytes)
    }
}

impl BorshDeserialize for BorshableJoltProverPreprocessing {
    fn deserialize_reader<R: borsh::io::Read>(reader: &mut R) -> borsh::io::Result<Self> {
        let len = u64::deserialize_reader(reader)?;
        let mut bytes = vec![0u8; len as usize];
        reader.read_exact(&mut bytes)?;
        let preprocessing =
            JoltProverPreprocessing::deserialize_from_bytes(&bytes).map_err(|e| {
                borsh::io::Error::other(format!(
                    "Failed to deserialize JoltProverPreprocessing: {e}"
                ))
            })?;
        Ok(BorshableJoltProverPreprocessing(preprocessing))
    }
}

#[derive(BorshSerialize, BorshDeserialize)]
pub struct JoltRegistryEntry {
    pub preprocessing: BorshableJoltProverPreprocessing,
    pub memory_config: BorshMemoryConfig,
    pub elf: Vec<u8>,
}

impl JoltRegistryEntry {
    pub fn new(
        preprocessing: JoltProverPreprocessing<jolt_sdk::F, jolt_sdk::Curve, jolt_sdk::PCS>,
        memory_config: impl Into<BorshMemoryConfig>,
        elf: Vec<u8>,
    ) -> Self {
        Self {
            preprocessing: BorshableJoltProverPreprocessing(preprocessing),
            memory_config: memory_config.into(),
            elf,
        }
    }
}

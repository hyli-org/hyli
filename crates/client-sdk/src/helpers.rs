use std::pin::Pin;

use anyhow::Result;
use borsh::BorshSerialize;
use sdk::{
    Calldata, ContractName, HyleOutput, ProgramId, Proof, ProofData, RegisterContractAction,
    StateCommitment, TimeoutWindow, Verifier,
};

use crate::transaction_builder::ProvableBlobTx;

pub fn register_hyle_contract(
    builder: &mut ProvableBlobTx,
    new_contract_name: ContractName,
    verifier: Verifier,
    program_id: ProgramId,
    state_commitment: StateCommitment,
    timeout_window: Option<TimeoutWindow>,
    constructor_metadata: Option<Vec<u8>>,
) -> anyhow::Result<()> {
    builder.add_action(
        "hyle".into(),
        RegisterContractAction {
            contract_name: new_contract_name,
            verifier,
            program_id,
            state_commitment,
            timeout_window,
            constructor_metadata,
        },
        None,
        None,
        None,
    )?;
    Ok(())
}

#[derive(Debug, Clone)]
pub struct ProverInfo {
    pub name: String,
    pub zkvm: String,
    pub version: String,
}

pub trait ClientSdkProver<T: BorshSerialize + Send> {
    fn prove(
        &self,
        commitment_metadata: Vec<u8>,
        calldatas: T,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Proof>> + Send + '_>>;
    fn info(&self) -> ProverInfo;
}

#[cfg(feature = "risc0")]
pub mod risc0 {

    use borsh::BorshSerialize;
    use sdk::ProofMetadata;

    use super::*;

    pub struct Risc0Prover<'a> {
        binary: &'a [u8],
    }
    impl<'a> Risc0Prover<'a> {
        pub fn new(binary: &'a [u8]) -> Self {
            Self { binary }
        }
        pub async fn prove<T: BorshSerialize>(
            &self,
            commitment_metadata: Vec<u8>,
            calldatas: T,
        ) -> Result<Proof> {
            let explicit = std::env::var("RISC0_PROVER").unwrap_or_default();
            let (receipt, metadata) = match explicit.to_lowercase().as_str() {
                "bonsai" => {
                    let input_data =
                        bonsai_runner::as_input_data(&(commitment_metadata, calldatas))?;
                    let res = bonsai_runner::run_bonsai(self.binary, input_data.clone()).await?;
                    (
                        res.receipt,
                        ProofMetadata {
                            cycles: res.cycles,
                            prover: Some(explicit),
                            id: None,
                        },
                    )
                }
                "boundless" => {
                    let input_data =
                        bonsai_runner::as_input_data(&(commitment_metadata, calldatas))?;
                    let res = bonsai_runner::run_boundless(self.binary, input_data).await?;
                    (
                        res.receipt,
                        ProofMetadata {
                            cycles: res.cycles,
                            prover: Some(explicit),
                            id: None,
                        },
                    )
                }
                _ => {
                    let input_data = borsh::to_vec(&(commitment_metadata, calldatas))?;
                    let env = risc0_zkvm::ExecutorEnv::builder()
                        .write(&input_data.len())?
                        .write_slice(&input_data)
                        .build()
                        .unwrap();

                    let prover = risc0_zkvm::default_prover();
                    let prove_info = prover.prove(env, self.binary)?;
                    (
                        prove_info.receipt,
                        ProofMetadata {
                            cycles: Some(prove_info.stats.total_cycles),
                            prover: None,
                            id: None,
                        },
                    )
                }
            };

            let encoded_receipt = borsh::to_vec(&receipt).expect("Unable to encode receipt");
            Ok(Proof {
                data: ProofData(encoded_receipt),
                metadata,
            })
        }
    }

    impl<T: BorshSerialize + Send + 'static> ClientSdkProver<T> for Risc0Prover<'_> {
        fn prove(
            &self,
            commitment_metadata: Vec<u8>,
            calldatas: T,
        ) -> Pin<Box<dyn std::future::Future<Output = Result<Proof>> + Send + '_>> {
            Box::pin(self.prove(commitment_metadata, calldatas))
        }

        fn info(&self) -> ProverInfo {
            ProverInfo {
                name: std::env::var("RISC0_PROVER").unwrap_or_default(),
                zkvm: "risc0_zkvm".to_string(),
                version: risc0_zkvm::VERSION.to_string(),
            }
        }
    }
}

#[cfg(feature = "sp1")]
pub mod sp1 {
    use sdk::ProofMetadata;
    use sp1_sdk::{
        network::builder::NetworkProverBuilder, EnvProver, NetworkProver, ProverClient,
        SP1ProvingKey, SP1Stdin,
    };

    use super::*;

    pub struct SP1Prover {
        pk: SP1ProvingKey,
        client: ProverType,
    }

    enum ProverType {
        Local(EnvProver),
        Network(Box<NetworkProver>),
    }

    impl SP1Prover {
        pub async fn new(pk: SP1ProvingKey) -> Self {
            let prover_type = std::env::var("SP1_PROVER").unwrap_or_default();

            match prover_type.to_lowercase().as_str() {
                "network" => {
                    // Setup the program for network proving
                    let client = NetworkProverBuilder::default().build();

                    tracing::info!("Registering sp1 program on network");
                    client
                        .register_program(&pk.vk, &pk.elf)
                        .await
                        .expect("registering program");

                    Self {
                        pk,
                        client: ProverType::Network(Box::new(client)),
                    }
                }
                _ => {
                    // Setup the program for local proving
                    let client = ProverClient::from_env();
                    Self {
                        pk,
                        client: ProverType::Local(client),
                    }
                }
            }
        }

        pub fn program_id(&self) -> Result<sdk::ProgramId> {
            Ok(sdk::ProgramId(serde_json::to_vec(&self.pk.vk)?))
        }

        pub async fn prove<T: BorshSerialize>(
            &self,
            commitment_metadata: Vec<u8>,
            calldatas: T,
        ) -> Result<Proof> {
            // Setup the inputs.
            let mut stdin = SP1Stdin::new();
            let encoded = borsh::to_vec(&(commitment_metadata, calldatas))?;
            stdin.write_vec(encoded);

            // Generate the proof based on the prover type
            let (proof, metadata) = match &self.client {
                ProverType::Local(client) => {
                    let exec = client
                        .execute(&self.pk.elf, &stdin)
                        .run()
                        .expect("failed to execute program");
                    let cycles = exec.1.total_instruction_count();

                    (
                        client
                            .prove(&self.pk, &stdin)
                            .compressed()
                            .run()
                            .expect("failed to generate proof"),
                        ProofMetadata {
                            cycles: Some(cycles),
                            prover: Some("sp1_local".to_string()),
                            id: None,
                        },
                    )
                }
                ProverType::Network(client) => {
                    let exec = client
                        .execute(&self.pk.elf, &stdin)
                        .run()
                        .expect("failed to execute program");
                    let cycles = exec.1.total_instruction_count();

                    (
                        client
                            .prove(&self.pk, &stdin)
                            // Core proofs are limite to 5M cycles
                            .compressed()
                            // Reserved strategy is better for higher usage throughput
                            .strategy(sp1_sdk::network::FulfillmentStrategy::Reserved)
                            .run()
                            .expect("failed to generate proof"),
                        ProofMetadata {
                            cycles: Some(cycles),
                            prover: Some("sp1_network".to_string()),
                            id: None,
                        },
                    )
                }
            };

            let encoded_receipt = bincode::serialize(&proof)?;
            Ok(Proof {
                data: ProofData(encoded_receipt),
                metadata,
            })
        }
    }

    impl<T: BorshSerialize + Send + 'static> ClientSdkProver<T> for SP1Prover {
        fn prove(
            &self,
            commitment_metadata: Vec<u8>,
            calldatas: T,
        ) -> Pin<Box<dyn std::future::Future<Output = Result<Proof>> + Send + '_>> {
            Box::pin(self.prove(commitment_metadata, calldatas))
        }
        fn info(&self) -> ProverInfo {
            ProverInfo {
                name: std::env::var("SP1_PROVER").unwrap_or_default(),
                zkvm: "sp1".to_string(),
                version: sp1_sdk::SP1_CIRCUIT_VERSION.to_string(),
            }
        }
    }
}

pub mod test {
    use borsh::BorshDeserialize;
    use sdk::{ProofMetadata, TransactionalZkContract, ZkContract};

    use super::*;

    /// Generates valid proofs for the 'test' verifier using the TxExecutor
    pub struct TxExecutorTestProver<C: ZkContract> {
        phantom: std::marker::PhantomData<C>,
    }

    impl<C: ZkContract> TxExecutorTestProver<C> {
        pub fn new() -> Self {
            Self {
                phantom: std::marker::PhantomData,
            }
        }
    }

    impl<C: ZkContract> Default for TxExecutorTestProver<C> {
        fn default() -> Self {
            Self::new()
        }
    }

    impl<C: ZkContract + TransactionalZkContract + BorshDeserialize + 'static>
        ClientSdkProver<Vec<Calldata>> for TxExecutorTestProver<C>
    {
        fn prove(
            &self,
            commitment_metadata: Vec<u8>,
            calldatas: Vec<Calldata>,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Proof>> + Send + '_>>
        {
            let hos = sdk::guest::execute::<C>(&commitment_metadata, &calldatas);
            Box::pin(async move {
                Ok(Proof {
                    data: ProofData(borsh::to_vec(&hos).unwrap()),
                    metadata: ProofMetadata {
                        cycles: None,
                        prover: None,
                        id: None,
                    },
                })
            })
        }

        fn info(&self) -> ProverInfo {
            ProverInfo {
                name: "TxExecutorTestProver".to_string(),
                zkvm: "tx_executor".to_string(),
                version: "1.0.0".to_string(),
            }
        }
    }

    pub struct MockProver {}

    impl ClientSdkProver<Calldata> for MockProver {
        fn prove(
            &self,
            commitment_metadata: Vec<u8>,
            calldata: Calldata,
        ) -> Pin<Box<dyn std::future::Future<Output = Result<Proof>> + Send + '_>> {
            Box::pin(async move {
                let hyle_output = execute(commitment_metadata.clone(), calldata.clone())?;
                Ok(Proof {
                    data: ProofData(borsh::to_vec(&hyle_output).expect("Failed to encode proof")),
                    metadata: ProofMetadata {
                        cycles: None,
                        prover: None,
                        id: None,
                    },
                })
            })
        }
        fn info(&self) -> ProverInfo {
            ProverInfo {
                name: "MockProver".to_string(),
                zkvm: "mock".to_string(),
                version: "1.0.0".to_string(),
            }
        }
    }

    impl ClientSdkProver<Vec<Calldata>> for MockProver {
        fn prove(
            &self,
            commitment_metadata: Vec<u8>,
            calldata: Vec<Calldata>,
        ) -> Pin<Box<dyn std::future::Future<Output = Result<Proof>> + Send + '_>> {
            Box::pin(async move {
                let mut proofs = Vec::new();
                for call in calldata {
                    let hyle_output = test::execute(commitment_metadata.clone(), call)?;
                    proofs.push(hyle_output);
                }
                Ok(Proof {
                    data: ProofData(borsh::to_vec(&proofs)?),
                    metadata: ProofMetadata {
                        cycles: None,
                        prover: None,
                        id: None,
                    },
                })
            })
        }
        fn info(&self) -> ProverInfo {
            ProverInfo {
                name: "MockProver".to_string(),
                zkvm: "mock".to_string(),
                version: "1.0.0".to_string(),
            }
        }
    }

    pub fn execute(commitment_metadata: Vec<u8>, calldata: Calldata) -> Result<HyleOutput> {
        // FIXME: this is a hack to make the test pass.
        let initial_state = StateCommitment(commitment_metadata);
        let hyle_output = HyleOutput {
            version: 1,
            initial_state: initial_state.clone(),
            next_state: initial_state,
            identity: calldata.identity.clone(),
            index: calldata.index,
            blobs: calldata.blobs.clone(),
            tx_blob_count: calldata.tx_blob_count,
            success: true,
            tx_hash: calldata.tx_hash.clone(),
            state_reads: vec![],
            tx_ctx: None,
            onchain_effects: vec![],
            program_outputs: vec![],
        };
        Ok(hyle_output)
    }
}

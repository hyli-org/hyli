use std::{
    any::Any,
    collections::{BTreeMap, HashMap},
    future::Future,
    ops::{Deref, DerefMut},
    sync::{Arc, OnceLock},
};

use anyhow::{bail, Result};
use sdk::{
    verifiers::{Secp256k1Blob, NATIVE_VERIFIERS_CONTRACT_LIST},
    Blob, BlobIndex, BlobTransaction, Calldata, ContractAction, ContractName, Hashed, HyleOutput,
    Identity, ProofTransaction, RegisterContractEffect, TxContext,
};

use crate::helpers::ClientSdkProver;

pub struct ProvableBlobTx {
    pub identity: Identity,
    pub blobs: Vec<Blob>,
    runners: Vec<ContractRunner>,
    tx_context: Option<TxContext>,
}

impl ProvableBlobTx {
    pub fn new(identity: Identity) -> Self {
        ProvableBlobTx {
            identity,
            runners: vec![],
            blobs: vec![],
            tx_context: None,
        }
    }

    pub fn add_action<CF: ContractAction>(
        &mut self,
        contract_name: ContractName,
        action: CF,
        private_input: Option<Vec<u8>>,
        caller: Option<BlobIndex>,
        callees: Option<Vec<BlobIndex>>,
    ) -> Result<&'_ mut ContractRunner> {
        let runner = ContractRunner::new(
            contract_name.clone(),
            self.identity.clone(),
            BlobIndex(self.blobs.len()),
            private_input,
        )?;
        self.runners.push(runner);
        self.blobs
            .push(action.as_blob(contract_name, caller, callees));
        Ok(self.runners.last_mut().unwrap())
    }

    pub fn add_secp256k1_action(&mut self, action: Secp256k1Blob) -> Result<()> {
        self.blobs.push(action.as_blob());
        Ok(())
    }

    pub fn add_context(&mut self, tx_context: TxContext) {
        self.tx_context = Some(tx_context);
    }
}

impl From<ProvableBlobTx> for BlobTransaction {
    fn from(tx: ProvableBlobTx) -> Self {
        BlobTransaction::new(tx.identity, tx.blobs)
    }
}

pub struct ProofTxBuilder {
    pub identity: Identity,
    pub blobs: Vec<Blob>,
    runners: Vec<ContractRunner>,
    pub outputs: Vec<(ContractName, HyleOutput)>,
    provers: BTreeMap<ContractName, Arc<dyn ClientSdkProver<Vec<Calldata>> + Sync + Send>>,
}

impl ProofTxBuilder {
    /// Returns an iterator over the proofs of the transactions
    /// In order to send proofs when they are ready, without waiting for all of them to be ready
    /// Example usage:
    /// for (proof, contract_name) in transaction.iter_prove() {
    ///    let proof: ProofData = proof.await.unwrap();
    ///    ctx.client()
    ///        .send_tx_proof(&hyle::model::ProofTransaction {
    ///            blob_tx_hash: blob_tx_hash.clone(),
    ///            proof,
    ///            contract_name,
    ///        })
    ///        .await
    ///        .unwrap();
    ///}
    pub fn iter_prove(
        self,
    ) -> impl Iterator<Item = impl Future<Output = Result<ProofTransaction>> + Send> {
        self.runners.into_iter().map(move |mut runner| {
            tracing::info!("Proving transition for {}...", runner.contract_name);
            let prover = self
                .provers
                .get(&runner.contract_name)
                .expect("no prover defined")
                .clone();
            async move {
                let proof = prover
                    .prove(
                        runner
                            .commitment_metadata
                            .take()
                            .expect("no commitment metadata for prover"),
                        vec![runner.calldata.take().expect("no calldata for prover")],
                    )
                    .await;
                proof.map(|proof| ProofTransaction {
                    proof,
                    contract_name: runner.contract_name.clone(),
                })
            }
        })
    }

    pub fn to_blob_tx(&self) -> BlobTransaction {
        BlobTransaction::new(self.identity.clone(), self.blobs.clone())
    }
}

pub trait StateUpdater
where
    Self: std::marker::Sized,
{
    fn setup(&self, ctx: &mut TxExecutorBuilder<Self>);
    fn update(&mut self, contract_name: &ContractName, new_state: &mut dyn Any) -> Result<()>;
    fn get(&self, contract_name: &ContractName) -> Result<Box<dyn Any>>;
    fn build_commitment_metadata(
        &self,
        contract_name: &ContractName,
        blob: &Blob,
    ) -> anyhow::Result<Vec<u8>>;
    fn execute(
        &mut self,
        contract_name: &ContractName,
        calldata: &Calldata,
    ) -> anyhow::Result<HyleOutput>;
}

pub struct TxExecutor<S: StateUpdater> {
    states: S,
    provers: BTreeMap<ContractName, Arc<dyn ClientSdkProver<Vec<Calldata>> + Sync + Send>>,
}

impl<S: StateUpdater> Deref for TxExecutor<S> {
    type Target = S;

    fn deref(&self) -> &Self::Target {
        &self.states
    }
}
impl<S: StateUpdater> DerefMut for TxExecutor<S> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.states
    }
}

pub struct TxExecutorBuilder<S> {
    full_states: Option<S>,
    provers: BTreeMap<ContractName, Arc<dyn ClientSdkProver<Vec<Calldata>> + Sync + Send>>,
}

impl<S: StateUpdater> TxExecutorBuilder<S> {
    pub fn new(full_states: S) -> Self {
        let mut ret = Self {
            full_states: None,
            provers: BTreeMap::new(),
        };
        full_states.setup(&mut ret);
        ret.full_states = Some(full_states);
        ret
    }

    pub fn build(self) -> TxExecutor<S> {
        TxExecutor {
            // Safe to unwrap because we set it in the constructor
            states: self.full_states.unwrap(),
            provers: self.provers,
        }
    }

    pub fn init_with(
        &mut self,
        contract_name: ContractName,
        prover: impl ClientSdkProver<Vec<Calldata>> + Sync + Send + 'static,
    ) -> &mut Self {
        self.provers
            .entry(contract_name)
            .or_insert(Arc::new(prover));
        self
    }

    pub fn with_prover(
        mut self,
        contract_name: ContractName,
        prover: impl ClientSdkProver<Vec<Calldata>> + Sync + Send + 'static,
    ) -> Self {
        self.provers.insert(contract_name, Arc::new(prover));
        self
    }
}

impl<S: StateUpdater> TxExecutor<S> {
    pub fn process_all<I>(
        &mut self,
        iter: I,
    ) -> impl Iterator<Item = Result<ProofTxBuilder>> + use<'_, S, I>
    where
        I: IntoIterator<Item = ProvableBlobTx>,
    {
        iter.into_iter().map(move |tx| self.process(tx))
    }

    /// Executes the transaction and updates the state of the associated contracts.
    ///
    /// This function processes a given `ProvableBlobTx` by iterating over each blob,
    /// building the contract input, executing the contract, and updating the state
    /// accordingly. If the execution fails, it returns an error with the program output.
    ///
    /// # Arguments
    ///
    /// * `tx` - The transaction to be processed.
    ///
    /// # Returns
    ///
    /// A `Result` containing a `ProofTxBuilder` if successful, or an error if the execution fails.
    pub fn process(&mut self, mut tx: ProvableBlobTx) -> Result<ProofTxBuilder> {
        let mut outputs = vec![];
        let mut old_states = HashMap::new();

        // Keep track of all state involved in the transaction
        for blob in tx
            .blobs
            .iter()
            // Blob with native verifiers don't need proofs
            .filter(|b| !NATIVE_VERIFIERS_CONTRACT_LIST.contains(&b.contract_name.0.as_str()))
        {
            let state = self.states.get(&blob.contract_name)?;
            old_states.insert(blob.contract_name.clone(), state);
        }

        for runner in tx.runners.iter_mut() {
            // We get the blob that contains the action for that runner.
            // We build the commitment metadata for that blob. (i.e. the action that will be executed)
            let blob = &tx.blobs[runner.index.0];
            let commitment_metadata = self
                .states
                .build_commitment_metadata(&runner.contract_name, blob)
                .unwrap()
                .clone();

            runner.build_zk_program_input(
                tx.tx_context.clone(),
                tx.blobs.clone(),
                commitment_metadata,
            );

            tracing::info!("Checking transition for {}...", runner.contract_name);
            let out = match self
                .states
                .execute(&runner.contract_name, runner.calldata.get().unwrap())
            {
                Ok(result) => result,
                Err(e) => {
                    // Revert all state changes
                    for (contract_name, state) in old_states.iter_mut() {
                        self.states.update(contract_name, &mut *state)?;
                    }
                    bail!("Execution failed for {}: {}", runner.contract_name, e);
                }
            };
            if !out.success {
                // Revert all state changes
                for (contract_name, state) in old_states.iter_mut() {
                    self.states.update(contract_name, &mut *state)?;
                }
                let program_error = std::str::from_utf8(&out.program_outputs).unwrap();
                bail!(
                    "Execution failed on runner for blob {:?} on contrat {:?} ! Program output: {}",
                    runner.calldata.get().unwrap().index,
                    runner.contract_name,
                    program_error
                );
            }

            outputs.push((runner.contract_name.clone(), out));
        }

        Ok(ProofTxBuilder {
            identity: tx.identity,
            blobs: tx.blobs,
            runners: tx.runners,
            outputs,
            provers: self.provers.clone(),
        })
    }
}

#[derive(Debug)]
pub struct ContractRunner {
    pub contract_name: ContractName,
    identity: Identity,
    index: BlobIndex,
    private_input: Option<Vec<u8>>,
    commitment_metadata: OnceLock<Vec<u8>>,
    calldata: OnceLock<Calldata>,
}

impl ContractRunner {
    fn new(
        contract_name: ContractName,
        identity: Identity,
        index: BlobIndex,
        private_input: Option<Vec<u8>>,
    ) -> Result<Self> {
        Ok(Self {
            contract_name,
            identity,
            index,
            private_input,
            commitment_metadata: OnceLock::new(),
            calldata: OnceLock::new(),
        })
    }

    fn build_zk_program_input(
        &mut self,
        tx_context: Option<TxContext>,
        blobs: Vec<Blob>,
        commitment_metadata: Vec<u8>,
    ) {
        let tx_hash = BlobTransaction::new(self.identity.clone(), blobs.clone()).hashed();

        self.commitment_metadata.get_or_init(|| commitment_metadata);
        self.calldata.get_or_init(|| Calldata {
            identity: self.identity.clone(),
            index: self.index,
            tx_blob_count: blobs.len(),
            blobs: blobs.into(),
            tx_hash,
            tx_ctx: tx_context,
            private_input: self.private_input.clone().unwrap_or_default(),
        });
    }
}

// Reexport anyhow to avoid forcing users to include it explicitly.
pub type TxExecutorHandlerResult<T> = Result<T>;
pub use anyhow::Context as TxExecutorHandlerContext;
pub trait TxExecutorHandler {
    /// Entry point for contract execution for the SDK's TxExecutor tool
    /// This handler provides a way to execute contract logic with access to the full provable state,
    /// as opposed to the ZkContract trait which only works with commitment metadata.
    ///
    /// Example: For a contract using a MerkleTrie, this handler can access and update the entire trie,
    /// while the ZkContract would only work with the root hash.
    fn handle(&mut self, calldata: &Calldata) -> anyhow::Result<HyleOutput>;

    /// This is the function that creates the commitment metadata.
    /// It provides the minimum information necessary to construct the commitment_medata field of the input
    /// that will be used to execute the program in the zkvm.
    fn build_commitment_metadata(&self, blob: &Blob) -> anyhow::Result<Vec<u8>>;

    /// This function is used to merge the commitment metadata of the contract.
    /// Used for contracts that use only a partial state like MerkleTrie.
    fn merge_commitment_metadata(
        &self,
        initial: Vec<u8>,
        _next: Vec<u8>,
    ) -> Result<Vec<u8>, String> {
        Ok(initial)
    }

    /// Parse a registration blob and construct the initial state of the contract
    fn construct_state(
        register_blob: &RegisterContractEffect,
        metadata: &Option<Vec<u8>>,
    ) -> anyhow::Result<Self>
    where
        Self: Sized;
}

/// Macro to easily define the full state of a TxExecutor
/// Struct-like syntax.
/// Must have Calldata, ContractName, HyleOutput, TxExecutorHandler and anyhow in scope.
/// Example:
/// use anyhow;
/// use hyle_contract_sdk::{Blob, Calldata, ContractName, HyleOutput};
/// use client_sdk::transaction_builder::TxExecutorHandler;
#[macro_export]
macro_rules! contract_states {
    ($(#[$meta:meta])* $vis:vis struct $name:ident { $($mvis:vis $contract_name:ident: $contract_state:ty,)* }) => {
        $(#[$meta])*
        $vis struct $name {
            $($mvis $contract_name: $contract_state,
            )*
        }

        impl $crate::transaction_builder::StateUpdater for $name {
            fn setup(&self, ctx: &mut TxExecutorBuilder<Self>) {
                $(self.$contract_name.setup_builder::<Self>(stringify!($contract_name).into(), ctx);)*
            }

            fn update(
                &mut self,
                contract_name: &ContractName,
                new_state: &mut dyn std::any::Any,
            ) -> anyhow::Result<()> {
                match contract_name.0.as_str() {
                    $(stringify!($contract_name) => {
                        let Some(st) = new_state.downcast_mut::<$contract_state>() else {
                            anyhow::bail!("Incorrect state data passed for contract '{}'", contract_name);
                        };
                        std::mem::swap(&mut self.$contract_name, st);
                    })*
                    _ => anyhow::bail!("Unknown contract name: {contract_name}"),
                };
                Ok(())
            }

            fn get(&self, contract_name: &ContractName) -> anyhow::Result<Box<dyn std::any::Any>> {
                match contract_name.0.as_str() {
                    $(stringify!($contract_name) => Ok(Box::new(self.$contract_name.clone())),)*
                    _ => anyhow::bail!("Unknown contract name: {contract_name}"),
                }
            }

            fn build_commitment_metadata(&self, contract_name: &ContractName, blob: &Blob) -> anyhow::Result<Vec<u8>> {
                match contract_name.0.as_str() {
                    $(stringify!($contract_name) => Ok(self.$contract_name.build_commitment_metadata(blob).map_err(|e| anyhow::anyhow!(e))?),)*
                    _ => anyhow::bail!("Unknown contract name: {contract_name}"),
                }
            }

            fn execute(&mut self, contract_name: &ContractName, calldata: &Calldata) -> anyhow::Result<HyleOutput> {
                match contract_name.0.as_str() {
                    $(stringify!($contract_name) => {
                        self.$contract_name
                            .handle(calldata)
                            .map_err(|e| anyhow::anyhow!(e))
                    })*
                    _ => anyhow::bail!("Unknown contract name: {contract_name}"),
                }
            }
        }
    };
}

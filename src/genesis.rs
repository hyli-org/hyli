use std::collections::{BTreeMap, HashMap};

use crate::{model::*, p2p::network::PeerEvent, utils::conf::SharedConf};
use anyhow::{Error, Result};
use client_sdk::{
    contract_states,
    helpers::register_hyle_contract,
    transaction_builder::{
        ProofTxBuilder, ProvableBlobTx, TxExecutor, TxExecutorBuilder, TxExecutorHandler,
    },
};
use hydentity::{
    client::tx_executor_handler::{register_identity, verify_identity},
    Hydentity,
};
use hyle_contract_sdk::{
    Blob, Calldata, ContractName, Identity, ProgramId, StateCommitment, ZkContract,
};
use hyle_crypto::SharedBlstCrypto;
use hyle_modules::{
    bus::{BusClientSender, SharedMessageBus},
    bus_client, handle_messages,
    modules::Module,
};
use hyllar::{client::tx_executor_handler::transfer, Hyllar, FAUCET_ID};
use serde::{Deserialize, Serialize};
use smt_token::{account::AccountSMT, SmtTokenAction};
use staking::{
    client::tx_executor_handler::{delegate, deposit_for_fees, stake},
    state::Staking,
};
use tracing::{debug, error, info};
use utils::TimestampMs;
use verifiers::NativeVerifiers;

#[derive(Debug, Clone, Deserialize, Serialize, Eq, PartialEq)]
pub enum GenesisEvent {
    NoGenesis,
    GenesisBlock(SignedBlock),
}

bus_client! {
struct GenesisBusClient {
    sender(GenesisEvent),
    receiver(PeerEvent),
}
}

type PeerPublicKeyMap = BTreeMap<String, ValidatorPublicKey>;

pub struct Genesis {
    config: SharedConf,
    bus: GenesisBusClient,
    peer_pubkey: PeerPublicKeyMap,
    crypto: SharedBlstCrypto,
}

impl Module for Genesis {
    type Context = SharedRunContext;
    async fn build(bus: SharedMessageBus, ctx: Self::Context) -> Result<Self> {
        let bus = GenesisBusClient::new_from_bus(bus.new_handle()).await;
        Ok(Genesis {
            config: ctx.config.clone(),
            bus,
            peer_pubkey: BTreeMap::new(),
            crypto: ctx.crypto.clone(),
        })
    }

    fn run(&mut self) -> impl futures::Future<Output = Result<()>> + Send {
        self.start()
    }
}

contract_states!(
    #[derive(Debug, Clone)]
    pub struct States {
        pub hyllar: Hyllar,
        pub hydentity: Hydentity,
        pub staking: Staking,
    }
);

#[allow(clippy::expect_used, reason = "genesis should panic if incorrect")]
impl Genesis {
    pub async fn start(&mut self) -> Result<(), Error> {
        let file = self.config.data_directory.clone().join("genesis.bin");
        let already_handled_genesis: bool = Self::load_from_disk_or_default(&file);
        if already_handled_genesis {
            debug!("🌿 Genesis block already handled, skipping");
            // TODO: do we need a different message?
            _ = self.bus.send(GenesisEvent::NoGenesis {})?;
            return Ok(());
        }

        self.do_genesis().await?;

        // TODO: ideally we'd wait until everyone has processed it, as there's technically a data race.

        Self::save_on_disk(&file, &true)?;

        Ok(())
    }

    pub async fn do_genesis(&mut self) -> Result<()> {
        let single_node = self.config.consensus.solo;
        // Unless we're in single node mode, we must be a genesis staker to start the network.
        if !single_node && !self.config.genesis.stakers.contains_key(&self.config.id) {
            info!("📡 Not a genesis staker, need to catchup from peers.");
            _ = self.bus.send(GenesisEvent::NoGenesis {});
            return Ok(());
        }

        info!("🌱 Building genesis block");

        // We will start from the genesis block.
        self.peer_pubkey.insert(
            self.config.id.clone(),
            self.crypto.validator_pubkey().clone(),
        );

        // Wait until we've connected with all other genesis peers.
        // (We've already checked we're part of the stakers, so if we're alone carry on).
        if !single_node && self.config.genesis.stakers.len() > 1 {
            // Cumulate heights of peers, to know whether we need a genesis step or not
            let mut heights = vec![];
            info!("🌱 Waiting on other genesis peers to join");
            handle_messages! {
                on_bus self.bus,
                listen<PeerEvent> msg => {
                    match msg {
                        PeerEvent::NewPeer { name, pubkey, height, .. } => {
                            if !self.config.genesis.stakers.contains_key(&name) {
                                continue;
                            }

                            heights.push(height.0);

                            info!("🌱 New peer {}({}) added to genesis", &name, &pubkey);
                            self.peer_pubkey.insert(name.clone(), pubkey.clone());

                            // Once we know everyone in the initial quorum, craft & process the genesis block.
                            if self.peer_pubkey.len() == self.config.genesis.stakers.len() {
                                let height = heights
                                   .iter()
                                   .max()
                                   .unwrap_or(&0);

                                if height > &0 {
                                    info!(" Skipping Genesis because peers' height are higher than 0");
                                    _ = self.bus.send(GenesisEvent::NoGenesis {});
                                    return Ok(());
                                } else {
                                    break
                                }
                            } else {
                                info!("🌱 Waiting for {} more peers to join genesis", self.config.genesis.stakers.len() - self.peer_pubkey.len());
                            }
                        }
                    }
                }
            }
        }

        let mut initial_validators = self.peer_pubkey.values().cloned().collect::<Vec<_>>();
        initial_validators.sort();

        let genesis_txs = match self
            .generate_genesis_txs(&self.peer_pubkey, &self.config.genesis.stakers)
            .await
        {
            Ok(t) => t,
            Err(e) => {
                error!("🌱 Genesis block generation failed: {:?}", e);
                return Err(e);
            }
        };

        let signed_block = self.make_genesis_block(genesis_txs, initial_validators);

        // At this point, we can setup the genesis block.
        _ = self.bus.send(GenesisEvent::GenesisBlock(signed_block));

        Ok(())
    }

    pub async fn generate_genesis_txs(
        &self,
        peer_pubkey: &PeerPublicKeyMap,
        genesis_stake: &HashMap<String, u64>,
    ) -> Result<Vec<Transaction>> {
        let (contract_program_ids, mut genesis_txs, mut tx_executor) = self.genesis_contracts_txs();

        let register_txs = self
            .generate_register_txs(peer_pubkey, &mut tx_executor)
            .await?;

        let faucet_txs = self
            .generate_faucet_txs(peer_pubkey, &mut tx_executor, genesis_stake)
            .await?;

        let stake_txs =
            Self::generate_stake_txs(peer_pubkey, &mut tx_executor, genesis_stake).await?;

        let token_txs = if self.config.genesis.keep_tokens_in_faucet {
            vec![]
        } else {
            Self::generate_token_txs(&mut tx_executor)?
        };

        let builders = register_txs
            .into_iter()
            .chain(faucet_txs.into_iter())
            .chain(stake_txs.into_iter())
            .chain(token_txs.into_iter());

        for ProofTxBuilder {
            identity,
            blobs,
            mut outputs,
            ..
        } in builders
        {
            // On genesis we don't need an actual zkproof as the txs are not going through data
            // dissemination. We can create the same VerifiedProofTransaction on each genesis
            // validator, and assume it's the same.

            let tx = BlobTransaction::new(identity, blobs);
            let blob_tx_hash = tx.hashed();

            genesis_txs.push(tx.into());

            // Pretend we're verifying a recursive proof
            genesis_txs.push(
                VerifiedProofTransaction {
                    contract_name: "risc0-recursion".into(),
                    proven_blobs: outputs
                        .drain(..)
                        .map(|(contract_name, out)| BlobProofOutput {
                            original_proof_hash: ProofData::default().hashed(),
                            program_id: contract_program_ids
                                .get(&contract_name)
                                .expect("Genesis TXes on unregistered contracts")
                                .clone(),
                            blob_tx_hash: blob_tx_hash.clone(),
                            hyle_output: out,
                        })
                        .collect(),
                    is_recursive: true,
                    proof_hash: ProofData::default().hashed(),
                    proof_size: 0,
                    proof: None,
                }
                .into(),
            );
        }

        Ok(genesis_txs)
    }

    async fn generate_register_txs(
        &self,
        peer_pubkey: &PeerPublicKeyMap,
        tx_executor: &mut TxExecutor<States>,
    ) -> Result<Vec<ProofTxBuilder>> {
        // TODO: use an identity provider that checks BLST signature on a pubkey instead of
        // hydentity that checks password
        // The validator will send the signature for the register transaction in the handshake
        // in order to let all genesis validators to create the genesis register

        let mut txs = vec![];

        // register faucet identity
        let identity = Identity(FAUCET_ID.into());
        let mut transaction = ProvableBlobTx::new(identity.clone());
        register_identity(
            &mut transaction,
            ContractName::new("hydentity"),
            "password".to_owned(),
        )?;
        txs.push(tx_executor.process(transaction)?);

        for peer in peer_pubkey.values() {
            info!("🌱  Registering identity {peer}");

            let identity = Identity(format!("{peer}@hydentity"));
            let mut transaction = ProvableBlobTx::new(identity.clone());

            // Register
            register_identity(
                &mut transaction,
                ContractName::new("hydentity"),
                "password".to_owned(),
            )?;

            txs.push(tx_executor.process(transaction)?);
        }

        Ok(txs)
    }

    async fn generate_faucet_txs(
        &self,
        peer_pubkey: &PeerPublicKeyMap,
        tx_executor: &mut TxExecutor<States>,
        genesis_stakers: &HashMap<String, u64>,
    ) -> Result<Vec<ProofTxBuilder>> {
        let mut txs = vec![];
        for (id, peer) in peer_pubkey.iter() {
            let genesis_faucet = *genesis_stakers
                .get(id)
                .expect("Genesis stakers should be in the peer map")
                as u128;

            info!(
                "🌱  Fauceting {} hyllar to {peer}",
                genesis_faucet + 100_000_000_000
            );

            let identity = Identity::new(FAUCET_ID);
            let mut transaction = ProvableBlobTx::new(identity.clone());

            // Verify identity
            verify_identity(
                &mut transaction,
                ContractName::new("hydentity"),
                &tx_executor.hydentity,
                "password".to_string(),
            )?;

            // Transfer
            transfer(
                &mut transaction,
                ContractName::new("hyllar"),
                format!("{peer}@hydentity"),
                genesis_faucet + 100_000_000_000,
            )?;

            txs.push(tx_executor.process(transaction)?);
        }

        Ok(txs)
    }

    async fn generate_stake_txs(
        peer_pubkey: &PeerPublicKeyMap,
        tx_executor: &mut TxExecutor<States>,
        genesis_stakers: &HashMap<String, u64>,
    ) -> Result<Vec<ProofTxBuilder>> {
        let mut txs = vec![];
        for (id, peer) in peer_pubkey.iter() {
            let genesis_stake = *genesis_stakers
                .get(id)
                .expect("Genesis stakers should be in the peer map")
                as u128;

            info!("🌱  Staking {genesis_stake} hyllar from {peer}");

            let identity = Identity(format!("{peer}@hydentity").to_string());
            let mut transaction = ProvableBlobTx::new(identity.clone());

            // Verify identity
            verify_identity(
                &mut transaction,
                ContractName::new("hydentity"),
                &tx_executor.hydentity,
                "password".to_string(),
            )?;

            // Stake
            stake(
                &mut transaction,
                ContractName::new("staking"),
                genesis_stake,
            )?;

            // Transfer
            transfer(
                &mut transaction,
                ContractName::new("hyllar"),
                "staking".to_string(),
                genesis_stake,
            )?;

            // Deposit for fees
            deposit_for_fees(
                &mut transaction,
                ContractName::new("staking"),
                peer.clone(),
                100_000_000_000, // 100 GB at 1 token/byte
            )?;

            transfer(
                &mut transaction,
                ContractName::new("hyllar"),
                "staking".to_string(),
                100_000_000_000,
            )?;

            // Delegate
            delegate(&mut transaction, peer.clone())?;

            txs.push(tx_executor.process(transaction)?);
        }

        Ok(txs)
    }

    // Needs to run last
    fn generate_token_txs(tx_executor: &mut TxExecutor<States>) -> Result<Vec<ProofTxBuilder>> {
        let mut txs: Vec<ProofTxBuilder> = vec![];
        let identity = Identity::new(FAUCET_ID);

        // Send tokens to the Hyli wallet for later distribution, and so hydentity can't be used.
        let mut transaction = ProvableBlobTx::new(identity.clone());
        // Verify identity
        verify_identity(
            &mut transaction,
            ContractName::new("hydentity"),
            &tx_executor.hydentity,
            "password".to_string(),
        )?;

        // Transfer all tokens
        let remaining_hyllar = hyllar::erc20::ERC20::balance_of(&tx_executor.hyllar, &identity.0)
            .expect("Faucet should have hyllar balance");

        info!("🌱  Transferring remaining {remaining_hyllar} hyllar to hyli@wallet");
        transfer(
            &mut transaction,
            ContractName::new("hyllar"),
            "hyli@wallet".to_string(),
            remaining_hyllar,
        )?;

        txs.push(tx_executor.process(transaction)?);

        let mut smt = AccountSMT::default();
        let initial_root = *smt.0.root();
        let transfer_blob = SmtTokenAction::Transfer {
            sender: identity.clone(),
            recipient: Identity::new("hyli@wallet"),
            // Full supply
            amount: 100_000_000_000_000,
        };

        smt.handle(&Calldata {
            tx_hash: TxHash::default(),
            identity: identity.clone(),
            // Contract name doesn't matter here
            blobs: IndexedBlobs::from(vec![transfer_blob.as_blob(
                ContractName::new("smt"),
                None,
                None,
            )]),
            tx_blob_count: 1,
            index: BlobIndex(0),
            tx_ctx: None,
            private_input: vec![],
        })?;
        let next_root = *smt.0.root();
        let initial_state = StateCommitment(Into::<[u8; 32]>::into(initial_root).to_vec());
        let next_state = StateCommitment(Into::<[u8; 32]>::into(next_root).to_vec());

        #[allow(clippy::indexing_slicing, reason = "must exist")]
        for token in ["oranj", "oxygen", "vitamin"] {
            info!("🌱 Transferring all {token} tokens to 'hyli@wallet'");
            let mut transaction = ProvableBlobTx::new(identity.clone());
            // Verify identity
            verify_identity(
                &mut transaction,
                ContractName::new("hydentity"),
                &tx_executor.hydentity,
                "password".to_string(),
            )?;

            let mut ptx = tx_executor.process(transaction)?;
            ptx.blobs
                .push(transfer_blob.as_blob(ContractName::new(token), None, None));

            ptx.outputs[0].1.tx_hash = ptx.to_blob_tx().hashed();
            ptx.outputs[0].1.blobs = IndexedBlobs::from(ptx.blobs.clone());

            let tx_hash = ptx.to_blob_tx().hashed();
            ptx.outputs.push((
                token.into(),
                HyleOutput {
                    version: 1,
                    initial_state: initial_state.clone(),
                    next_state: next_state.clone(),
                    identity: identity.clone(),
                    index: BlobIndex(1),
                    blobs: IndexedBlobs::from(ptx.blobs.clone()),
                    tx_blob_count: 2,
                    tx_hash,
                    success: true,
                    state_reads: vec![],
                    tx_ctx: None,
                    onchain_effects: vec![],
                    program_outputs: vec![],
                },
            ));
            txs.push(ptx);
        }
        Ok(txs)
    }

    fn genesis_contracts_txs(
        &self,
    ) -> (
        BTreeMap<ContractName, ProgramId>,
        Vec<Transaction>,
        TxExecutor<States>,
    ) {
        let staking_program_id = hyle_contracts::STAKING_ID.to_vec();
        let hyllar_program_id = hyle_contracts::HYLLAR_ID.to_vec();
        let smt_token_program_id = hyle_contracts::SMT_TOKEN_ID.to_vec();
        let hydentity_program_id = hyle_contracts::HYDENTITY_ID.to_vec();

        let hydentity_state = hydentity::Hydentity::default();
        let staking_state = staking::state::Staking::new();

        let ctx = TxExecutorBuilder::new(States {
            hyllar: hyllar::Hyllar::default(),
            hydentity: hydentity_state,
            staking: staking_state,
        })
        .build();

        let mut map = BTreeMap::default();
        map.insert("hyle".into(), ProgramId(vec![0, 0, 0, 0]));
        map.insert("blst".into(), NativeVerifiers::Blst.into());
        map.insert("sha3_256".into(), NativeVerifiers::Sha3_256.into());
        map.insert("secp256k1".into(), NativeVerifiers::Secp256k1.into());
        map.insert("hyllar".into(), ProgramId(hyllar_program_id.clone()));
        map.insert("oranj".into(), ProgramId(smt_token_program_id.clone()));
        map.insert("oxygen".into(), ProgramId(smt_token_program_id.clone()));
        map.insert("vitamin".into(), ProgramId(smt_token_program_id.clone()));
        map.insert("hydentity".into(), ProgramId(hydentity_program_id.clone()));
        map.insert("staking".into(), ProgramId(staking_program_id.clone()));
        map.insert(
            "risc0-recursion".into(),
            ProgramId(hyle_contracts::RISC0_RECURSION_ID.to_vec()),
        );

        let mut register_tx = ProvableBlobTx::new("hyle@hyle".into());

        register_hyle_contract(
            &mut register_tx,
            "hyle".into(),
            "hyle".into(),
            ProgramId(vec![0, 0, 0, 0]),
            StateCommitment::default(),
            Some(TimeoutWindow::NoTimeout),
            None,
        )
        .expect("register hyle");

        register_hyle_contract(
            &mut register_tx,
            "blst".into(),
            "blst".into(),
            NativeVerifiers::Blst.into(),
            StateCommitment::default(),
            Some(TimeoutWindow::NoTimeout),
            None,
        )
        .expect("register blst");

        register_hyle_contract(
            &mut register_tx,
            "sha3_256".into(),
            "sha3_256".into(),
            NativeVerifiers::Sha3_256.into(),
            StateCommitment::default(),
            Some(TimeoutWindow::NoTimeout),
            None,
        )
        .expect("register sha3_256");

        register_hyle_contract(
            &mut register_tx,
            "secp256k1".into(),
            "secp256k1".into(),
            NativeVerifiers::Secp256k1.into(),
            StateCommitment::default(),
            Some(TimeoutWindow::NoTimeout),
            None,
        )
        .expect("register secp256k1");

        register_hyle_contract(
            &mut register_tx,
            "staking".into(),
            hyle_model::verifiers::RISC0_1.into(),
            staking_program_id.clone().into(),
            ctx.staking.commit(),
            None,
            None,
        )
        .expect("register staking");

        register_hyle_contract(
            &mut register_tx,
            "hyllar".into(),
            hyle_model::verifiers::RISC0_1.into(),
            hyllar_program_id.clone().into(),
            ctx.hyllar.commit(),
            None,
            None,
        )
        .expect("register hyllar");

        let smt = AccountSMT::default();
        let root = *smt.0.root();
        for token in ["oranj", "oxygen", "vitamin"] {
            info!("🌱 Registering SMT token {token}");
            register_hyle_contract(
                &mut register_tx,
                token.into(),
                hyle_model::verifiers::RISC0_1.into(),
                smt_token_program_id.clone().into(),
                StateCommitment(Into::<[u8; 32]>::into(root).to_vec()),
                None,
                None,
            )
            .expect("register SMT token");
        }

        register_hyle_contract(
            &mut register_tx,
            "hydentity".into(),
            hyle_model::verifiers::RISC0_1.into(),
            hydentity_program_id.clone().into(),
            ctx.hydentity.commit(),
            None,
            None,
        )
        .expect("register hydentity");

        register_hyle_contract(
            &mut register_tx,
            "risc0-recursion".into(),
            hyle_model::verifiers::RISC0_1.into(),
            hyle_contracts::RISC0_RECURSION_ID.to_vec().into(),
            StateCommitment::default(),
            None,
            None,
        )
        .expect("register risc0-recursion");

        let genesis_tx: BlobTransaction = register_tx.into();

        (map, vec![genesis_tx.into()], ctx)
    }

    fn make_genesis_block(
        &self,
        genesis_txs: Vec<Transaction>,
        initial_validators: Vec<ValidatorPublicKey>,
    ) -> SignedBlock {
        let dp = DataProposal::new(None, genesis_txs);

        // TODO: do something better?
        let round_leader = initial_validators
            .first()
            .expect("must have round leader")
            .clone();

        SignedBlock {
            data_proposals: vec![(LaneId(round_leader.clone()), vec![dp.clone()])],
            certificate: AggregateSignature {
                signature: Signature("fake".into()),
                validators: initial_validators.clone(),
            },
            consensus_proposal: ConsensusProposal {
                slot: 0,
                // TODO: genesis block should have a consistent, up-to-date timestamp
                timestamp: TimestampMs((self.config.consensus.genesis_timestamp * 1000) as u128),
                // TODO: We aren't actually storing the data proposal above, so we cannot store it here,
                // or we might mistakenly request data from that cut, but mempool hasn't seen it.
                // This should be fixed by storing the data proposal in mempool or handling this whole thing differently.
                cut: vec![/*(
                    round_leader.clone(), dp.hash(), AggregateSignature {
                        signature: Signature("fake".into()),
                        validators: initial_validators.clone()
                    }
                )*/],
                staking_actions: initial_validators
                    .iter()
                    .map(|v| {
                        SignedByValidator {
                            msg: ValidatorCandidacy {
                                peer_address: "".into(),
                            },
                            signature: ValidatorSignature {
                                signature: Signature("".into()),
                                validator: v.clone(),
                            },
                        }
                        .into()
                    })
                    .collect(),
                parent_hash: ConsensusProposalHash("genesis".into()),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use assertables::assert_matches;

    use super::*;
    use crate::bus::{BusClientReceiver, SharedMessageBus};
    use crate::utils::conf::Conf;
    use hyle_crypto::BlstCrypto;
    use std::sync::Arc;

    bus_client! {
    struct TestGenesisBusClient {
        sender(PeerEvent),
        receiver(GenesisEvent),
    }
    }

    async fn new(config: Conf) -> (Genesis, TestGenesisBusClient) {
        let shared_bus = SharedMessageBus::default();
        let bus = GenesisBusClient::new_from_bus(shared_bus.new_handle()).await;
        let test_bus = TestGenesisBusClient::new_from_bus(shared_bus.new_handle()).await;
        let crypto = Arc::new(BlstCrypto::new(&config.id).unwrap());
        (
            Genesis {
                config: Arc::new(config),
                bus,
                peer_pubkey: BTreeMap::new(),
                crypto,
            },
            test_bus,
        )
    }

    #[test_log::test(tokio::test)]
    async fn test_not_part_of_genesis() {
        let tmpdir = tempfile::Builder::new().tempdir().unwrap();

        let mut config =
            Conf::new(vec![], tmpdir.path().to_str().map(|s| s.to_owned()), None).unwrap();
        config.id = "node-4".to_string();
        config.consensus.solo = false;
        config.genesis.stakers = [("node-1".into(), 100)].into_iter().collect();

        let (mut genesis, _) = new(config).await;

        // Start the Genesis module
        let result = genesis.start().await;

        // Verify the start method executed correctly
        assert!(result.is_ok());
    }

    #[test_log::test(tokio::test)]
    async fn test_genesis_single() {
        let tmpdir = tempfile::Builder::new().tempdir().unwrap();
        let mut config =
            Conf::new(vec![], tmpdir.path().to_str().map(|s| s.to_owned()), None).unwrap();
        config.id = "single-node".to_string();
        config.consensus.solo = true;
        config.genesis.stakers = [("single-node".into(), 100)].into_iter().collect();
        let (mut genesis, mut bus) = new(config).await;

        // Start the Genesis module
        let result = genesis.start().await;

        assert!(result.is_ok());

        // Check it ran the genesis block
        let rec: GenesisEvent = bus.try_recv().expect("recv");
        assert_matches!(rec, GenesisEvent::GenesisBlock(..));
        if let GenesisEvent::GenesisBlock(signed_block) = rec {
            assert!(signed_block.has_txs());
            assert_eq!(signed_block.consensus_proposal.staking_actions.len(), 1);
        }
    }

    #[test_log::test(tokio::test)]
    async fn test_genesis_as_leader() {
        let tmpdir = tempfile::Builder::new().tempdir().unwrap();

        let mut config =
            Conf::new(vec![], tmpdir.path().to_str().map(|s| s.to_owned()), None).unwrap();
        config.id = "node-1".to_string();
        config.consensus.solo = false;
        config.genesis.stakers = [("node-1".into(), 100), ("node-2".into(), 100)]
            .into_iter()
            .collect();

        let (mut genesis, mut bus) = new(config).await;

        bus.send(PeerEvent::NewPeer {
            name: "node-2".into(),
            pubkey: ValidatorPublicKey("aaa".into()),
            da_address: "".into(),
            height: BlockHeight(0),
        })
        .expect("send");

        // Start the Genesis module
        let result = genesis.start().await;

        assert!(result.is_ok());

        let rec: GenesisEvent = bus.try_recv().expect("recv");
        assert_matches!(rec, GenesisEvent::GenesisBlock(..));
        if let GenesisEvent::GenesisBlock(signed_block) = rec {
            assert!(signed_block.has_txs());
            assert_eq!(signed_block.consensus_proposal.staking_actions.len(), 2);
        }
    }

    #[test_log::test(tokio::test)]
    async fn test_genesis_as_follower() {
        let tmpdir = tempfile::Builder::new().tempdir().unwrap();

        let mut config =
            Conf::new(vec![], tmpdir.path().to_str().map(|s| s.to_owned()), None).unwrap();
        config.id = "node-2".to_string();
        config.consensus.solo = false;
        config.genesis.stakers = [("node-1".into(), 100), ("node-2".into(), 100)]
            .into_iter()
            .collect();

        let (mut genesis, mut bus) = new(config).await;

        let node_1_pubkey = ValidatorPublicKey("bbb".into());

        bus.send(PeerEvent::NewPeer {
            name: "node-1".into(),
            pubkey: node_1_pubkey.clone(),
            height: BlockHeight(0),
            da_address: "".into(),
        })
        .expect("send");

        // Start the Genesis module
        let result = genesis.start().await;

        assert!(result.is_ok());

        let rec = bus.try_recv().expect("recv");
        assert_matches!(rec, GenesisEvent::GenesisBlock(..));
        if let GenesisEvent::GenesisBlock(signed_block) = rec {
            assert!(signed_block.has_txs());
            assert_eq!(signed_block.consensus_proposal.staking_actions.len(), 2);
        }
    }

    // test that the order of nodes connecting doesn't matter on genesis block creation
    #[test_log::test(tokio::test)]
    async fn test_genesis_connect_order() {
        let tmpdir = tempfile::Builder::new().tempdir().unwrap();

        let mut config =
            Conf::new(vec![], tmpdir.path().to_str().map(|s| s.to_owned()), None).unwrap();
        config.id = "node-1".to_string();
        config.genesis.stakers = [
            ("node-1".into(), 100),
            ("node-2".into(), 100),
            ("node-3".into(), 100),
            ("node-4".into(), 100),
        ]
        .into_iter()
        .collect();

        let build_new_peer = |name: &'static str, height: u64| PeerEvent::NewPeer {
            name: name.into(),
            pubkey: BlstCrypto::new(name).unwrap().validator_pubkey().clone(),
            height: BlockHeight(height),
            da_address: "".into(),
        };
        let rec1 = {
            let (mut genesis, mut bus) = new(config.clone()).await;
            bus.send(build_new_peer("node-2", 0)).expect("send");
            bus.send(build_new_peer("node-3", 0)).expect("send");
            bus.send(build_new_peer("node-4", 0)).expect("send");
            let _ = genesis.start().await;
            bus.try_recv().expect("recv")
        };
        let tmpdir = tempfile::Builder::new().tempdir().unwrap();
        config.data_directory = tmpdir.path().to_path_buf();
        let rec2 = {
            let (mut genesis, mut bus) = new(config).await;
            bus.send(build_new_peer("node-2", 0)).expect("send");
            bus.send(build_new_peer("node-3", 0)).expect("send");
            bus.send(build_new_peer("node-4", 0)).expect("send");
            let _ = genesis.start().await;
            bus.try_recv().expect("recv")
        };

        assert_eq!(rec1, rec2);
    }

    // Test that if at least 2f+1 stakers have a height > 0, we skip the genesis
    #[test_log::test(tokio::test)]
    async fn test_skip_genesis_when_height_is_high() {
        let tmpdir = tempfile::Builder::new().tempdir().unwrap();

        let mut config =
            Conf::new(vec![], tmpdir.path().to_str().map(|s| s.to_owned()), None).unwrap();
        config.id = "node-1".to_string();
        config.consensus.solo = false;
        config.genesis.stakers = [
            ("node-1".into(), 100),
            ("node-2".into(), 100),
            ("node-3".into(), 100),
            ("node-4".into(), 100),
        ]
        .into_iter()
        .collect();

        let build_new_peer = |name: &'static str, height: u64| PeerEvent::NewPeer {
            name: name.into(),
            pubkey: BlstCrypto::new(name).unwrap().validator_pubkey().clone(),
            height: BlockHeight(height),
            da_address: "".into(),
        };

        let rec1 = {
            let (mut genesis, mut bus) = new(config.clone()).await;
            bus.send(build_new_peer("node-2", 1)).expect("send");
            bus.send(build_new_peer("node-3", 1)).expect("send");
            bus.send(build_new_peer("node-4", 1)).expect("send");
            let _ = genesis.start().await;
            bus.try_recv().expect("recv")
        };
        let tmpdir = tempfile::Builder::new().tempdir().unwrap();
        config.data_directory = tmpdir.path().to_path_buf();
        let rec2 = {
            let (mut genesis, mut bus) = new(config).await;
            bus.send(build_new_peer("node-2", 1)).expect("send");
            bus.send(build_new_peer("node-3", 2)).expect("send");
            bus.send(build_new_peer("node-4", 2)).expect("send");
            let _ = genesis.start().await;
            bus.try_recv().expect("recv")
        };

        assert_eq!(rec1, rec2);
        assert_eq!(rec1, GenesisEvent::NoGenesis);
    }
}

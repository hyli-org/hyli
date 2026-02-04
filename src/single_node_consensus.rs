use std::path::PathBuf;
use std::time::Duration;

use crate::bus::command_response::CmdRespClient;
use crate::bus::BusClientSender;
use crate::consensus::ConfirmAckMarker;
use crate::consensus::{CommittedConsensusProposal, ConsensusEvent, QueryConsensusInfo};
use crate::genesis::GenesisEvent;
use crate::mempool::QueryNewCut;
use crate::model::*;
use crate::utils::conf::SharedConf;
use anyhow::Result;
use borsh::{BorshDeserialize, BorshSerialize};
use hyli_bus::modules::ModulePersistOutput;
use hyli_crypto::SharedBlstCrypto;
use hyli_model::utils::TimestampMs;
use hyli_modules::bus::command_response::Query;
use hyli_modules::bus::SharedMessageBus;
use hyli_modules::module_handle_messages;
use hyli_modules::modules::admin::{
    QueryConsensusCatchupStore, QueryConsensusCatchupStoreResponse,
};
use hyli_modules::modules::module_bus_client;
use hyli_modules::modules::Module;
use hyli_net::clock::TimestampMsClock;
use staking::state::Staking;
use tracing::{debug, warn};

module_bus_client! {
struct SingleNodeConsensusBusClient {
    sender(ConsensusEvent),
    sender(Query<QueryNewCut, Cut>),
    receiver(Query<QueryConsensusInfo, ConsensusInfo>),
    receiver(Query<QueryConsensusCatchupStore, QueryConsensusCatchupStoreResponse>),
    receiver(GenesisEvent),
}
}

#[derive(BorshSerialize, BorshDeserialize, Default)]
struct SingleNodeConsensusStore {
    staking: Staking,
    has_done_genesis: bool,
    last_consensus_proposal_hash: ConsensusProposalHash,
    last_slot: u64,
    last_cut: Cut,
    last_timestamp: TimestampMs,
}

pub struct SingleNodeConsensus {
    bus: SingleNodeConsensusBusClient,
    crypto: SharedBlstCrypto,
    config: SharedConf,
    store: SingleNodeConsensusStore,
    file: Option<PathBuf>,
}

/// The `SingleNodeConsensus` module listens to and sends the same messages as the `Consensus` module.
/// However, there are differences in behavior:
/// - It does not perform any consensus.
/// - It creates a fake validator that automatically votes for any `DataProposal` it receives.
/// - For every DataProposal received, it saves it automatically as a `Car`.
/// - For every slot_duration tick, it is able to retrieve a `Car`s and create a new `CommitCut`
impl Module for SingleNodeConsensus {
    type Context = SharedRunContext;

    async fn build(bus: SharedMessageBus, ctx: Self::Context) -> Result<Self> {
        let file = PathBuf::from("consensus_single_node.bin");

        let store: SingleNodeConsensusStore =
            match Self::load_from_disk(&ctx.config.data_directory, &file)? {
                Some(s) => s,
                None => {
                    warn!("Starting SingleNodeConsensus from default.");
                    SingleNodeConsensusStore::default()
                }
            };

        let api = super::consensus::api::api(&bus, &ctx).await;
        if let Ok(mut guard) = ctx.api.router.lock() {
            if let Some(router) = guard.take() {
                guard.replace(router.nest("/v1/consensus", api));
            }
        }

        let bus = SingleNodeConsensusBusClient::new_from_bus(bus.new_handle()).await;

        Ok(SingleNodeConsensus {
            bus,
            crypto: ctx.crypto.clone(),
            config: ctx.config.clone(),
            store,
            file: Some(file),
        })
    }

    fn run(&mut self) -> impl futures::Future<Output = Result<()>> + Send {
        self.start()
    }

    async fn persist(&mut self) -> Result<ModulePersistOutput> {
        if let Some(file) = &self.file {
            let checksum = Self::save_on_disk(&self.config.data_directory, file, &self.store)?;
            return Ok(vec![(self.config.data_directory.join(file), checksum)]);
        }

        Ok(vec![])
    }
}

impl SingleNodeConsensus {
    async fn start(&mut self) -> Result<()> {
        if !self.store.has_done_genesis {
            // We're starting fresh, need to generate a genesis block.
            tracing::trace!("Doing genesis");

            let should_shutdown = module_handle_messages! {
                on_self self,
                listen<GenesisEvent> msg => {
                    #[allow(clippy::expect_used, reason="We want to fail to start with misconfigured genesis block")]
                    match msg {
                        GenesisEvent::GenesisBlock (signed_block) => {
                            self.store.last_consensus_proposal_hash = signed_block.hashed();
                            // TODO: handle this from the block?
                            self.store
                                .staking
                                .stake("single".into(), 100)
                                .expect("Staking failed");
                            self.store
                                .staking
                                .delegate_to("single".into(), self.crypto.validator_pubkey().clone())
                                .expect("Delegation failed");
                            let _ = self.store.staking.bond(self.crypto.validator_pubkey().clone());

                            break;
                        },
                        GenesisEvent::NoGenesis => unreachable!("Single genesis mode should never go through this path")
                    }
                }
            };
            if should_shutdown {
                return Ok(());
            }
            self.store.has_done_genesis = true;
            // Save the genesis block to disk
            if let Some(file) = &self.file {
                if let Err(e) = Self::save_on_disk(&self.config.data_directory, file, &self.store) {
                    warn!(
                        "Failed to save consensus single node storage on disk: {}",
                        e
                    );
                }
            }
            tracing::trace!("Genesis block done");
        }

        self.store.last_timestamp = TimestampMsClock::now();

        let mut interval = tokio::time::interval(std::cmp::min(
            self.config.consensus.slot_duration,
            Duration::from_millis(250),
        ));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        interval.tick().await; // First tick is immediate

        module_handle_messages! {
            on_self self,
            command_response<QueryConsensusInfo, ConsensusInfo> _ => {
                let slot = self.store.last_slot;
                let view = 0;
                let round_leader = self.crypto.validator_pubkey().clone();
                let last_timestamp = TimestampMsClock::now();
                let validators = vec![];
                Ok(ConsensusInfo { slot, view, round_leader, last_timestamp, validators })
            },
            command_response<QueryConsensusCatchupStore, QueryConsensusCatchupStoreResponse> _ => {
                Ok(QueryConsensusCatchupStoreResponse(vec![]))
            },
            _ = interval.tick() => {
                self.handle_new_slot_tick().await?;
            },
        };

        Ok(())
    }
    async fn handle_new_slot_tick(&mut self) -> Result<()> {
        debug!("New slot tick");
        // Query a new cut to Mempool in order to create a new CommitCut
        match self
            .bus
            .shutdown_aware_request::<Self>(QueryNewCut {
                staking: self.store.staking.clone(),
                full: true,
            })
            .await
        {
            Ok(cut) => {
                if cut == self.store.last_cut
                    && TimestampMsClock::now()
                        < self.store.last_timestamp.clone() + self.config.consensus.slot_duration
                {
                    debug!("No new cut, skipping commit");
                    return Ok(());
                }
                self.store.last_cut = cut.clone();
            }
            Err(err) => {
                tracing::warn!("Error while requesting new cut: {:?}", err);
            }
        };

        let new_slot = self.store.last_slot + 1;
        let consensus_proposal = ConsensusProposal {
            slot: new_slot,
            timestamp: TimestampMsClock::now(),
            cut: self.store.last_cut.clone(),
            staking_actions: vec![],
            parent_hash: std::mem::take(&mut self.store.last_consensus_proposal_hash),
        };

        self.store.last_timestamp = consensus_proposal.timestamp.clone();
        self.store.last_consensus_proposal_hash = consensus_proposal.hashed();

        let certificate = self
            .crypto
            .sign_aggregate((consensus_proposal.hashed(), ConfirmAckMarker), &[])?;

        self.bus.send(ConsensusEvent::CommitConsensusProposal(
            CommittedConsensusProposal {
                staking: Staking::default(),
                consensus_proposal,
                certificate: certificate.signature,
            },
        ))?;

        self.store.last_slot = new_slot;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bus::dont_use_this::get_receiver;
    use crate::bus::{bus_client, SharedMessageBus};
    use crate::utils::conf::Conf;
    use anyhow::Result;
    use hyli_crypto::BlstCrypto;
    use hyli_modules::bus::dont_use_this::get_sender;
    use hyli_modules::bus::{BusReceiver, BusSender};
    use hyli_modules::handle_messages;
    use hyli_modules::modules::signal::ShutdownModule;
    use std::sync::Arc;

    pub struct TestContext {
        consensus_event_receiver: BusReceiver<ConsensusEvent>,
        #[allow(dead_code)]
        shutdown_sender: BusSender<ShutdownModule>,
        single_node_consensus: SingleNodeConsensus,
    }

    bus_client!(
        struct TestBusClient {
            receiver(Query<QueryNewCut, Cut>),
        }
    );

    impl TestContext {
        pub async fn new(name: &str) -> Self {
            let crypto = BlstCrypto::new(name).unwrap();
            let shared_bus = SharedMessageBus::new();
            let conf = Arc::new(Conf::default());
            let store = SingleNodeConsensusStore::default();

            let consensus_event_receiver = get_receiver::<ConsensusEvent>(&shared_bus).await;
            let shutdown_sender = get_sender::<ShutdownModule>(&shared_bus).await;
            let bus = SingleNodeConsensusBusClient::new_from_bus(shared_bus.new_handle()).await;

            // Initialize Mempool
            let single_node_consensus = SingleNodeConsensus {
                bus,
                crypto: Arc::new(crypto),
                config: conf,
                store,
                file: None,
            };

            let mut new_cut_query_receiver = TestBusClient::new_from_bus(shared_bus).await;
            tokio::spawn(async move {
                handle_messages! {
                    on_bus new_cut_query_receiver,
                    command_response<QueryNewCut, Cut> _ => {
                        Ok(vec![(LaneId::default(), DataProposalHash::default(), LaneBytesSize::default(), AggregateSignature::default())])
                    }
                }
            });

            TestContext {
                consensus_event_receiver,
                shutdown_sender,
                single_node_consensus,
            }
        }

        #[track_caller]
        fn assert_commit_cut(&mut self, err: &str) -> Cut {
            #[allow(clippy::expect_fun_call)]
            let rec = self
                .consensus_event_receiver
                .try_recv()
                .expect(format!("{err}: No message broadcasted").as_str())
                .into_message();

            match rec {
                ConsensusEvent::CommitConsensusProposal(CommittedConsensusProposal {
                    consensus_proposal,
                    ..
                }) => consensus_proposal.cut,
            }
        }
    }

    #[test_log::test(tokio::test)]
    async fn test_flow() -> Result<()> {
        let mut ctx = TestContext::new("single_node_consensus").await;

        ctx.single_node_consensus.handle_new_slot_tick().await?;

        let cut = ctx.assert_commit_cut("CommitCut");
        assert!(!cut.is_empty());

        Ok(())
    }
}

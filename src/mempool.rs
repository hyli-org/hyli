//! Mempool logic & pending transaction management.

use crate::{
    bus::{command_response::Query, BusClientSender},
    consensus::{CommittedConsensusProposal, ConsensusEvent},
    genesis::GenesisEvent,
    model::*,
    node_state::module::NodeStateEvent,
    p2p::network::{
        HeaderSignableData, HeaderSigner, IntoHeaderSignableData, MsgWithHeader, OutboundMessage,
    },
    utils::{
        conf::SharedConf,
        serialize::{arc_rwlock_borsh, BorshableIndexMap},
    },
};
use anyhow::{bail, Context, Result};
use api::RestApiMessage;
use block_construction::BlockUnderConstruction;
use borsh::{BorshDeserialize, BorshSerialize};
use client_sdk::tcp_client::TcpServerMessage;
use hyle_contract_sdk::{ContractName, ProgramId, Verifier};
use hyle_crypto::SharedBlstCrypto;
use hyle_modules::{log_warn, module_bus_client, utils::static_type_map::Pick};
use hyle_net::{logged_task::logged_task, ordered_join_set::OrderedJoinSet};
use indexmap::IndexSet;
use metrics::MempoolMetrics;
use serde::{Deserialize, Serialize};
use staking::state::Staking;
use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    fmt::Display,
    ops::{Deref, DerefMut},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};
use storage::{LaneEntryMetadata, Storage};
use sync_request_reply::{MempoolSync, SyncRequest};
use tokio::task::JoinSet;
use verify_tx::DataProposalVerdict;
// Pick one of the two implementations
// use storage_memory::LanesStorage;
use storage_fjall::LanesStorage;
use strum_macros::IntoStaticStr;
use tracing::{debug, info, trace, warn};

pub mod api;
pub mod block_construction;
pub mod metrics;
pub mod module;
pub mod own_lane;
pub mod storage;
pub mod storage_fjall;
pub mod storage_memory;
pub mod sync_request_reply;
pub mod verifiers;
pub mod verify_tx;

#[derive(Debug, Clone)]
pub struct QueryNewCut(pub Staking);

#[derive(Debug, Default, Clone, BorshSerialize, BorshDeserialize)]
pub struct KnownContracts(pub HashMap<ContractName, (Verifier, ProgramId)>);

impl KnownContracts {
    #[inline(always)]
    fn register_contract(
        &mut self,
        contract_name: &ContractName,
        verifier: &Verifier,
        program_id: &ProgramId,
    ) {
        debug!("🏊📝 Registering contract in mempool {:?}", contract_name);
        self.0.insert(
            contract_name.clone(),
            (verifier.clone(), program_id.clone()),
        );
    }
}

module_bus_client! {
struct MempoolBusClient {
    sender(OutboundMessage),
    sender(MempoolBlockEvent),
    sender(MempoolStatusEvent),
    receiver(MsgWithHeader<MempoolNetMessage>),
    receiver(RestApiMessage),
    receiver(TcpServerMessage),
    receiver(ConsensusEvent),
    receiver(GenesisEvent),
    receiver(NodeStateEvent),
    receiver(Query<QueryNewCut, Cut>),
}
}

type UnaggregatedPoDA = Vec<ValidatorDAG>;

#[derive(Default, BorshSerialize, BorshDeserialize)]
pub struct MempoolStore {
    // own_lane.rs
    // TODO: implement serialization, probably with a custom future that yields the unmodified Tx
    // on cancellation
    #[borsh(skip)]
    processing_txs: OrderedJoinSet<Result<Transaction>>,
    waiting_dissemination_txs: BorshableIndexMap<TxHash, Transaction>,
    #[borsh(skip)]
    own_data_proposal_in_preparation: JoinSet<(DataProposalHash, DataProposal)>,
    // Skipped to clear on reset
    #[borsh(skip)]
    buffered_proposals: BTreeMap<LaneId, IndexSet<DataProposal>>, // This is an indexSet just so we can pop by idx.
    // Skipped to clear on reset
    #[borsh(skip)]
    buffered_podas: BTreeMap<LaneId, BTreeMap<DataProposalHash, Vec<UnaggregatedPoDA>>>,

    // verify_tx.rs
    #[borsh(skip)]
    processing_dps: OrderedJoinSet<Result<ProcessedDPEvent>>,
    #[borsh(skip)]
    cached_dp_votes: HashMap<(LaneId, DataProposalHash), DataProposalVerdict>,

    // Dedicated thread pool for data proposal and tx hashing
    #[borsh(skip)]
    long_tasks_runtime: LongTasksRuntime,

    // block_construction.rs
    blocks_under_contruction: VecDeque<BlockUnderConstruction>,
    #[borsh(skip)]
    buc_build_start_height: Option<u64>,

    // Common
    last_ccp: Option<CommittedConsensusProposal>,
    staking: Staking,
    #[borsh(
        serialize_with = "arc_rwlock_borsh::serialize",
        deserialize_with = "arc_rwlock_borsh::deserialize"
    )]
    known_contracts: Arc<std::sync::RwLock<KnownContracts>>,
}

pub struct LongTasksRuntime(std::mem::ManuallyDrop<tokio::runtime::Runtime>);
impl Default for LongTasksRuntime {
    fn default() -> Self {
        Self(std::mem::ManuallyDrop::new(
            #[allow(clippy::expect_used, reason = "Fails at startup, is OK")]
            tokio::runtime::Builder::new_multi_thread()
                // Limit the number of threads arbitrarily to lower the maximal impact on the whole node
                .worker_threads(3)
                .thread_name("mempool-hashing")
                .build()
                .expect("Failed to create hashing runtime"),
        ))
    }
}

impl Drop for LongTasksRuntime {
    fn drop(&mut self) {
        // Shut down the hashing runtime.
        // TODO: serialize?
        // Safety: We'll manually drop the runtime below and it won't be double-dropped as we use ManuallyDrop.
        let rt = unsafe { std::mem::ManuallyDrop::take(&mut self.0) };
        // This has to be done outside the current runtime.
        tokio::task::spawn_blocking(move || {
            #[cfg(test)]
            rt.shutdown_timeout(Duration::from_millis(10));
            #[cfg(not(test))]
            rt.shutdown_timeout(Duration::from_secs(10));
        });
    }
}

impl Deref for LongTasksRuntime {
    type Target = tokio::runtime::Runtime;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for LongTasksRuntime {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

pub struct Mempool {
    bus: MempoolBusClient,
    file: Option<PathBuf>,
    conf: SharedConf,
    crypto: SharedBlstCrypto,
    metrics: MempoolMetrics,
    lanes: LanesStorage,
    inner: MempoolStore,
}

impl Deref for Mempool {
    type Target = MempoolStore;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for Mempool {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

#[derive(
    Debug,
    Serialize,
    Deserialize,
    Clone,
    BorshSerialize,
    BorshDeserialize,
    Eq,
    PartialEq,
    IntoStaticStr,
)]
pub enum MempoolNetMessage {
    DataProposal(DataProposalHash, DataProposal),
    DataVote(ValidatorDAG),
    PoDAUpdate(DataProposalHash, Vec<ValidatorDAG>),
    SyncRequest(Option<DataProposalHash>, Option<DataProposalHash>),
    SyncReply(LaneEntryMetadata, DataProposal),
}

/// Validator Data Availability Guarantee
/// This is a signed message that contains the hash of the data proposal and the size of the lane (DP included)
/// It acts as proof the validator committed to making this DP available.
pub type ValidatorDAG = SignedByValidator<(DataProposalHash, LaneBytesSize)>;

impl Display for MempoolNetMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let enum_variant: &'static str = self.into();
        write!(f, "{enum_variant}")
    }
}

impl IntoHeaderSignableData for MempoolNetMessage {
    fn to_header_signable_data(&self) -> HeaderSignableData {
        match self {
            // We get away with only signing the hash - verification must check the hash is correct
            MempoolNetMessage::DataProposal(hash, _) => {
                HeaderSignableData(hash.0.clone().into_bytes())
            }
            MempoolNetMessage::DataVote(vdag) => {
                HeaderSignableData(borsh::to_vec(&vdag.msg).unwrap_or_default())
            }
            MempoolNetMessage::PoDAUpdate(_, vdags) => {
                HeaderSignableData(borsh::to_vec(&vdags).unwrap_or_default())
            }
            MempoolNetMessage::SyncRequest(from, to) => HeaderSignableData(
                [from.clone(), to.clone()]
                    .map(|h| h.unwrap_or_default().0.into_bytes())
                    .concat(),
            ),
            MempoolNetMessage::SyncReply(metadata, data_proposal) => {
                let hash = [
                    borsh::to_vec(&metadata).unwrap_or_default(),
                    data_proposal.hashed().0.into_bytes(),
                ];
                HeaderSignableData(hash.concat())
            }
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum ProcessedDPEvent {
    OnHashedDataProposal((LaneId, DataProposal)),
    OnProcessedDataProposal((LaneId, DataProposalVerdict, DataProposal)),
}

impl Mempool {
    pub fn start_mempool_sync(&self) -> tokio::sync::mpsc::Sender<SyncRequest> {
        let (sync_request_sender, sync_request_receiver) =
            tokio::sync::mpsc::channel::<SyncRequest>(30);
        let net_sender =
            Pick::<tokio::sync::broadcast::Sender<OutboundMessage>>::get(&self.bus).clone();

        let mut mempool_sync = MempoolSync::create(
            self.own_lane_id().clone(),
            self.lanes.new_handle(),
            self.crypto.clone(),
            self.metrics.clone(),
            net_sender,
            sync_request_receiver,
        );

        logged_task(async move { mempool_sync.start().await });

        sync_request_sender
    }

    fn handle_contract_registration(&mut self, effect: RegisterContractEffect) {
        #[allow(clippy::expect_used, reason = "not held across await")]
        let mut known_contracts = self.known_contracts.write().expect("logic issue");
        known_contracts.register_contract(
            &effect.contract_name,
            &effect.verifier,
            &effect.program_id,
        );
    }

    fn handle_contract_update(&mut self, contract_name: ContractName, program_id: ProgramId) {
        #[allow(clippy::expect_used, reason = "not held across await")]
        let mut known_contracts = self.known_contracts.write().expect("logic issue");
        if let Some(c) = known_contracts.0.get_mut(&contract_name) {
            c.1 = program_id;
        } else {
            warn!(
                "Tried to update contract {} that is not registered",
                contract_name
            );
        }
    }

    // Optimistically parse Hyli tx blobs
    fn handle_hyle_contract_registration(&mut self, blob_tx: &BlobTransaction) {
        #[allow(clippy::expect_used, reason = "not held across await")]
        let mut known_contracts = self.known_contracts.write().expect("logic issue");
        blob_tx.blobs.iter().for_each(|blob| {
            if blob.contract_name.0 != "hyle" {
                return;
            }
            if let Ok(tx) =
                StructuredBlobData::<RegisterContractAction>::try_from(blob.data.clone())
            {
                let tx = tx.parameters;
                known_contracts.register_contract(&tx.contract_name, &tx.verifier, &tx.program_id);
            }
        });
    }

    /// Creates a cut with local material on QueryNewCut message reception (from consensus)
    fn handle_querynewcut(&mut self, staking: &mut QueryNewCut) -> Result<Cut> {
        self.metrics.query_new_cut(staking);
        let previous_cut = self
            .last_ccp
            .as_ref()
            .map(|ccp| ccp.consensus_proposal.cut.clone())
            .unwrap_or_default();

        // For each lane, we get the last CAR and put it in the cut
        let mut cut: Cut = vec![];
        for lane_id in self.lanes.get_lane_ids() {
            let previous_entry = previous_cut
                .iter()
                .find(|(lane_id_, _, _, _)| lane_id_ == lane_id);
            if let Some((dp_hash, cumul_size, poda)) =
                self.lanes
                    .get_latest_car(lane_id, &staking.0, previous_entry)?
            {
                cut.push((lane_id.clone(), dp_hash, cumul_size, poda));
            } else if let Some(lane) = previous_entry {
                cut.push(lane.clone());
            }
        }
        Ok(cut)
    }

    fn handle_internal_event(&mut self, event: ProcessedDPEvent) -> Result<()> {
        match event {
            ProcessedDPEvent::OnHashedDataProposal((lane_id, data_proposal)) => self
                .on_hashed_data_proposal(&lane_id, data_proposal)
                .context("Hashing data proposal"),
            ProcessedDPEvent::OnProcessedDataProposal((lane_id, verdict, data_proposal)) => self
                .on_processed_data_proposal(lane_id, verdict, data_proposal)
                .context("Processing data proposal"),
        }
    }

    async fn handle_consensus_event(&mut self, event: ConsensusEvent) -> Result<()> {
        match event {
            ConsensusEvent::CommitConsensusProposal(cpp) => {
                debug!(
                    "✂️ Received CommittedConsensusProposal (slot {}, {:?} cut)",
                    cpp.consensus_proposal.slot, cpp.consensus_proposal.cut
                );

                self.staking = cpp.staking.clone();

                let cut = cpp.consensus_proposal.cut.clone();
                let previous_cut = self
                    .last_ccp
                    .as_ref()
                    .map(|ccp| ccp.consensus_proposal.cut.clone());

                self.try_create_block_under_construction(cpp);

                self.try_to_send_full_signed_blocks().await?;

                // Removes all DPs that are not in the new cut, updates lane tip and sends SyncRequest for missing DPs
                self.clean_and_update_lanes(&cut, &previous_cut)?;

                Ok(())
            }
        }
    }

    async fn handle_net_message(
        &mut self,
        msg: MsgWithHeader<MempoolNetMessage>,
        sync_request_sender: &tokio::sync::mpsc::Sender<SyncRequest>,
    ) -> Result<()> {
        let validator = &msg.header.signature.validator;
        // TODO: adapt can_rejoin test to emit a stake tx before turning on the joining node
        // if !self.validators.contains(validator) {
        //     bail!(
        //         "Received {} message from unknown validator {validator}. Only accepting {:?}",
        //         msg.msg,
        //         self.validators
        //     );
        // }

        match msg.msg {
            MempoolNetMessage::DataProposal(data_proposal_hash, data_proposal) => {
                let lane_id = self.get_lane(validator);
                self.on_data_proposal(&lane_id, data_proposal_hash, data_proposal)?;
            }
            MempoolNetMessage::DataVote(vdag) => {
                self.on_data_vote(vdag)?;
            }
            MempoolNetMessage::PoDAUpdate(data_proposal_hash, signatures) => {
                let lane_id = self.get_lane(validator);
                self.on_poda_update(&lane_id, &data_proposal_hash, signatures)?
            }
            MempoolNetMessage::SyncRequest(from_data_proposal_hash, to_data_proposal_hash) => {
                self.on_sync_request(
                    sync_request_sender,
                    from_data_proposal_hash,
                    to_data_proposal_hash,
                    validator.clone(),
                )
                .await?;
            }
            MempoolNetMessage::SyncReply(metadata, data_proposal) => {
                self.on_sync_reply(validator, metadata, data_proposal)
                    .await?;
            }
        }
        Ok(())
    }

    async fn on_sync_request(
        &mut self,
        sync_request_sender: &tokio::sync::mpsc::Sender<SyncRequest>,
        from: Option<DataProposalHash>,
        to: Option<DataProposalHash>,
        validator: ValidatorPublicKey,
    ) -> Result<()> {
        debug!(
            "{} SyncRequest received from validator {validator} for last_data_proposal_hash {:?}",
            &self.own_lane_id(),
            to
        );

        let Some(to) = to.or(self
            .lanes
            .lanes_tip
            .get(&self.own_lane_id())
            .map(|lane_id| lane_id.0.clone()))
        else {
            info!("Nothing to do for this SyncRequest");
            return Ok(());
        };

        // Transmit sync request to the Mempool submodule, to build a reply
        sync_request_sender
            .send(SyncRequest {
                from,
                to,
                validator: validator.clone(),
            })
            .await
            .context("Sending SyncRequest to Mempool submodule")?;

        Ok(())
    }

    async fn on_sync_reply(
        &mut self,
        sender_validator: &ValidatorPublicKey,
        metadata: LaneEntryMetadata,
        data_proposal: DataProposal,
    ) -> Result<()> {
        debug!("SyncReply from validator {sender_validator}");

        // TODO: Introduce lane ids in sync reply
        self.metrics.sync_reply_receive(
            &LaneId(sender_validator.clone()),
            self.crypto.validator_pubkey(),
        );

        // TODO: this isn't necessarily the case - another validator could have sent us data for this lane.
        let lane_id = &LaneId(sender_validator.clone());
        let lane_operator = self.get_lane_operator(lane_id);

        let missing_entry_not_present = {
            let expected_message = (data_proposal.hashed(), metadata.cumul_size);

            !metadata
                .signatures
                .iter()
                .any(|s| &s.signature.validator == lane_operator && s.msg == expected_message)
        };

        // Ensure all lane entries are signed by the validator.
        if missing_entry_not_present {
            bail!(
                "At least one lane entry is missing signature from {}",
                lane_operator
            );
        }

        // Add missing lanes to the validator's lane
        trace!(
            "Filling hole with 1 entry (parent dp hash: {:?}) for {lane_id}",
            metadata.parent_data_proposal_hash
        );

        // SyncReply only comes for missing data proposals. We should NEVER update the lane tip
        self.lanes
            .put_no_verification(lane_id.clone(), (metadata, data_proposal))?;

        // Retry all buffered proposals in this lane.
        // We'll re-buffer them in the on_data_proposal logic if they fail to be processed.
        let waiting_proposals = match self.buffered_proposals.get_mut(lane_id) {
            Some(waiting_proposals) => std::mem::take(waiting_proposals),
            None => Default::default(),
        };

        // TODO: retry remaining wp when one succeeds to be processed
        for wp in waiting_proposals.into_iter() {
            if self.lanes.contains(lane_id, &wp.hashed()) {
                continue;
            }
            self.on_data_proposal(lane_id, wp.hashed(), wp)
                .context("Consuming waiting data proposal")?;
        }

        self.try_to_send_full_signed_blocks()
            .await
            .context("Try process queued CCP")?;

        Ok(())
    }

    fn on_poda_update(
        &mut self,
        lane_id: &LaneId,
        data_proposal_hash: &DataProposalHash,
        podas: Vec<ValidatorDAG>,
    ) -> Result<()> {
        debug!(
            "Received {} signatures for DataProposal {} of lane {}",
            podas.len(),
            data_proposal_hash,
            lane_id
        );

        if log_warn!(
            self.lanes
                .add_signatures(lane_id, data_proposal_hash, podas.clone()),
            "PodaUpdate"
        )
        .is_err()
        {
            debug!(
                "Buffering poda of {} signatures for DP: {}",
                podas.len(),
                data_proposal_hash
            );

            let lane = self
                .inner
                .buffered_podas
                .entry(lane_id.clone())
                .or_default()
                .entry(data_proposal_hash.clone())
                .or_default();

            lane.push(podas);
        }

        Ok(())
    }

    fn get_lane(&self, validator: &ValidatorPublicKey) -> LaneId {
        LaneId(validator.clone())
    }

    fn get_lane_operator<'a>(&self, lane_id: &'a LaneId) -> &'a ValidatorPublicKey {
        &lane_id.0
    }

    fn send_sync_request(
        &mut self,
        lane_id: &LaneId,
        from_data_proposal_hash: Option<&DataProposalHash>,
        to_data_proposal_hash: Option<&DataProposalHash>,
    ) -> Result<()> {
        // TODO: use a more clever targeting system.
        let validator = &lane_id.0;
        debug!(
            "🔍 Sending SyncRequest to {} for DataProposal from {:?} to {:?}",
            validator, from_data_proposal_hash, to_data_proposal_hash
        );
        self.metrics
            .sync_request_send(lane_id, self.crypto.validator_pubkey());
        self.send_net_message(
            validator.clone(),
            MempoolNetMessage::SyncRequest(
                from_data_proposal_hash.cloned(),
                to_data_proposal_hash.cloned(),
            ),
        )?;
        Ok(())
    }

    #[inline(always)]
    fn broadcast_net_message(&mut self, net_message: MempoolNetMessage) -> Result<()> {
        let enum_variant_name: &'static str = (&net_message).into();
        let error_msg =
            format!("Broadcasting MempoolNetMessage::{enum_variant_name} msg on the bus");
        self.bus
            .send(OutboundMessage::broadcast(
                self.crypto.sign_msg_with_header(net_message)?,
            ))
            .context(error_msg)?;
        Ok(())
    }

    #[inline(always)]
    fn broadcast_only_for_net_message(
        &mut self,
        only_for: HashSet<ValidatorPublicKey>,
        net_message: MempoolNetMessage,
    ) -> Result<()> {
        let enum_variant_name: &'static str = (&net_message).into();
        let error_msg = format!(
            "Broadcasting MempoolNetMessage::{enum_variant_name} msg only for: {only_for:?} on the bus"
        );
        self.bus
            .send(OutboundMessage::broadcast_only_for(
                only_for,
                self.crypto.sign_msg_with_header(net_message)?,
            ))
            .context(error_msg)?;
        Ok(())
    }

    #[inline(always)]
    fn send_net_message(
        &mut self,
        to: ValidatorPublicKey,
        net_message: MempoolNetMessage,
    ) -> Result<()> {
        let enum_variant_name: &'static str = (&net_message).into();
        let error_msg = format!("Sending MempoolNetMessage::{enum_variant_name} msg on the bus");
        self.bus
            .send(OutboundMessage::send(
                to,
                self.crypto.sign_msg_with_header(net_message)?,
            ))
            .context(error_msg)?;
        Ok(())
    }
}

#[cfg(test)]
pub mod test {

    mod async_data_proposals;
    mod native_verifier_test;

    use core::panic;
    use std::future::Future;
    use std::pin::Pin;

    use super::*;
    use crate::bus::metrics::BusMetrics;
    use crate::bus::SharedMessageBus;
    use crate::model;
    use crate::p2p::network::NetMessage;
    use crate::{
        bus::dont_use_this::get_receiver,
        p2p::network::{HeaderSigner, MsgWithHeader},
    };
    use anyhow::Result;
    use assertables::assert_ok;
    use hyle_contract_sdk::StateCommitment;
    use hyle_crypto::BlstCrypto;
    use hyle_modules::modules::Module;
    use tokio::sync::broadcast::Receiver;
    use utils::TimestampMs;

    pub struct MempoolTestCtx {
        pub name: String,
        pub out_receiver: Receiver<OutboundMessage>,
        pub mempool_sync_request_sender: tokio::sync::mpsc::Sender<SyncRequest>,
        pub mempool_event_receiver: Receiver<MempoolBlockEvent>,
        pub mempool_status_event_receiver: Receiver<MempoolStatusEvent>,
        pub mempool: Mempool,
    }

    impl MempoolTestCtx {
        pub async fn build_mempool(shared_bus: &SharedMessageBus, crypto: BlstCrypto) -> Mempool {
            let tmp_dir = tempfile::tempdir().unwrap().keep();
            let lanes = LanesStorage::new(&tmp_dir, BTreeMap::default()).unwrap();
            let bus = MempoolBusClient::new_from_bus(shared_bus.new_handle()).await;

            // Initialize Mempool
            Mempool {
                bus,
                file: None,
                conf: SharedConf::default(),
                crypto: Arc::new(crypto),
                metrics: MempoolMetrics::global("id".to_string()),
                lanes,
                inner: MempoolStore::default(),
            }
        }

        pub async fn new(name: &str) -> Self {
            let crypto = BlstCrypto::new(name).unwrap();
            let shared_bus = SharedMessageBus::new(BusMetrics::global("global".to_string()));

            let out_receiver = get_receiver::<OutboundMessage>(&shared_bus).await;
            let mempool_event_receiver = get_receiver::<MempoolBlockEvent>(&shared_bus).await;
            let mempool_status_event_receiver =
                get_receiver::<MempoolStatusEvent>(&shared_bus).await;
            let mempool = Self::build_mempool(&shared_bus, crypto).await;
            let mempool_sync_request_sender = mempool.start_mempool_sync();

            MempoolTestCtx {
                name: name.to_string(),
                out_receiver,
                mempool_sync_request_sender,
                mempool_event_receiver,
                mempool_status_event_receiver,
                mempool,
            }
        }

        pub fn setup_node(&mut self, cryptos: &[BlstCrypto]) {
            for other_crypto in cryptos.iter() {
                self.add_trusted_validator(other_crypto.validator_pubkey());
            }
        }

        pub fn own_lane(&self) -> LaneId {
            self.mempool
                .get_lane(self.mempool.crypto.validator_pubkey())
        }

        pub fn validator_pubkey(&self) -> &ValidatorPublicKey {
            self.mempool.crypto.validator_pubkey()
        }

        pub fn add_trusted_validator(&mut self, pubkey: &ValidatorPublicKey) {
            self.mempool
                .staking
                .stake(hex::encode(pubkey.0.clone()).into(), 100)
                .unwrap();

            self.mempool
                .staking
                .delegate_to(hex::encode(pubkey.0.clone()).into(), pubkey.clone())
                .unwrap();

            self.mempool
                .staking
                .bond(pubkey.clone())
                .expect("cannot bond trusted validator");
        }

        pub fn sign_data<T: borsh::BorshSerialize>(&self, data: T) -> Result<SignedByValidator<T>> {
            self.mempool.crypto.sign(data)
        }

        pub fn create_net_message(
            &self,
            msg: MempoolNetMessage,
        ) -> Result<MsgWithHeader<MempoolNetMessage>> {
            self.mempool.crypto.sign_msg_with_header(msg)
        }

        pub fn gen_cut(&mut self, staking: &Staking) -> Cut {
            self.mempool
                .handle_querynewcut(&mut QueryNewCut(staking.clone()))
                .unwrap()
        }

        pub async fn timer_tick(&mut self) -> Result<bool> {
            let Ok(true) = self.mempool.prepare_new_data_proposal() else {
                debug!("No new data proposal to prepare");
                return self.mempool.disseminate_data_proposals(None).await;
            };

            let (dp_hash, dp) = self
                .mempool
                .own_data_proposal_in_preparation
                .join_next()
                .await
                .context("join next data proposal in preparation")??;

            Ok(self.mempool.resume_new_data_proposal(dp, dp_hash).await?
                || self.mempool.disseminate_data_proposals(None).await?)
        }

        pub async fn disseminate_timer_tick(&mut self) -> Result<bool> {
            self.mempool.redisseminate_oldest_data_proposal().await
        }

        pub async fn handle_poda_update(
            &mut self,
            net_message: MsgWithHeader<MempoolNetMessage>,
            sync_request_sender: &tokio::sync::mpsc::Sender<SyncRequest>,
        ) {
            self.mempool
                .handle_net_message(net_message, sync_request_sender)
                .await
                .expect("fail to handle net message");
        }

        pub async fn handle_processed_data_proposals(&mut self) {
            let event = self
                .mempool
                .inner
                .processing_dps
                .join_next()
                .await
                .expect("No event received")
                .expect("No event received")
                .expect("No event received");
            self.mempool
                .handle_internal_event(event)
                .expect("fail to handle event");
        }

        #[track_caller]
        pub fn assert_broadcast_only_for(
            &mut self,
            description: &str,
        ) -> MsgWithHeader<MempoolNetMessage> {
            #[allow(clippy::expect_fun_call)]
            let rec = self
                .out_receiver
                .try_recv()
                .expect(format!("{description}: No message broadcasted").as_str());

            match rec {
                OutboundMessage::BroadcastMessageOnlyFor(_, net_msg) => {
                    if let NetMessage::MempoolMessage(msg) = net_msg {
                        msg
                    } else {
                        println!(
                            "{description}: Mempool OutboundMessage message is missing, found {net_msg}"
                        );
                        self.assert_broadcast_only_for(description)
                    }
                }
                _ => {
                    println!(
                        "{description}: Broadcast OutboundMessage message is missing, found {rec:?}",
                    );
                    self.assert_broadcast_only_for(description)
                }
            }
        }
        pub fn assert_send(
            &mut self,
            to: &ValidatorPublicKey,
            description: &str,
        ) -> Pin<Box<dyn Future<Output = MsgWithHeader<MempoolNetMessage>> + '_>> {
            let to = to.clone();
            let description = description.to_string().clone();
            Box::pin(async move {
                #[allow(clippy::expect_fun_call)]
                let rec =
                    tokio::time::timeout(Duration::from_millis(1000), self.out_receiver.recv())
                        .await
                        .expect(format!("{description}: No message broadcasted").as_str())
                        .expect(format!("{description}: No message broadcasted").as_str());

                match rec {
                    OutboundMessage::SendMessage { validator_id, msg } => {
                        if let NetMessage::MempoolMessage(msg) = msg {
                            if validator_id != to {
                                panic!(
                                "{description}: Send message was sent to {validator_id} instead of {}",
                                to
                            );
                            }

                            msg
                        } else {
                            tracing::warn!("{description}: skipping {:?}", msg);
                            self.assert_send(&to, description.as_str()).await
                        }
                    }
                    OutboundMessage::BroadcastMessage(NetMessage::ConsensusMessage(e)) => {
                        tracing::warn!("{description}: skipping broadcast message {:?}", e);
                        self.assert_send(&to, description.as_str()).await
                    }
                    OutboundMessage::BroadcastMessage(els) => {
                        panic!(
                            "{description}: received broadcast message instead of send {:?}",
                            els
                        );
                    }
                    OutboundMessage::BroadcastMessageOnlyFor(
                        _,
                        NetMessage::ConsensusMessage(e),
                    ) => {
                        tracing::warn!("{description}: skipping broadcast message {:?}", e);
                        self.assert_send(&to, description.as_str()).await
                    }
                    OutboundMessage::BroadcastMessageOnlyFor(_, els) => {
                        panic!(
                        "{description}: received broadcast only for message instead of send {:?}",
                        els
                    );
                    }
                }
            })
        }

        pub fn assert_broadcast(
            &mut self,
            description: &str,
        ) -> Pin<Box<dyn Future<Output = MsgWithHeader<MempoolNetMessage>> + '_>> {
            let description = description.to_string().clone();
            Box::pin(async move {
                #[allow(clippy::expect_fun_call)]
                let rec =
                    tokio::time::timeout(Duration::from_millis(1000), self.out_receiver.recv())
                        .await
                        .expect(format!("{description}: No message broadcasted").as_str())
                        .expect(format!("{description}: No message broadcasted").as_str());

                match rec {
                    OutboundMessage::BroadcastMessage(net_msg) => {
                        if let NetMessage::MempoolMessage(msg) = net_msg {
                            msg
                        } else {
                            println!("{description}: Mempool OutboundMessage message is missing, found {net_msg}");
                            self.assert_broadcast(description.as_str()).await
                        }
                    }
                    _ => {
                        println!("{description}: Broadcast OutboundMessage message is missing, found {rec:?}");
                        self.assert_broadcast(description.as_str()).await
                    }
                }
            })
        }

        pub async fn handle_msg(&mut self, msg: &MsgWithHeader<MempoolNetMessage>, _err: &str) {
            debug!("📥 {} Handling message: {:?}", self.name, msg);
            self.mempool
                .handle_net_message(msg.clone(), &self.mempool_sync_request_sender)
                .await
                .expect("should handle net msg");
        }

        pub fn current_hash(&self, lane_id: &LaneId) -> Option<DataProposalHash> {
            self.mempool
                .lanes
                .lanes_tip
                .get(lane_id)
                .cloned()
                .map(|(h, _)| h)
        }

        pub fn current_size_of(&self, lane_id: &LaneId) -> Option<LaneBytesSize> {
            self.mempool
                .lanes
                .lanes_tip
                .get(lane_id)
                .cloned()
                .map(|(_, s)| s)
        }

        pub fn last_lane_entry(
            &self,
            lane_id: &LaneId,
        ) -> ((LaneEntryMetadata, DataProposal), DataProposalHash) {
            let last_dp_hash = self.current_hash(lane_id).unwrap();
            let last_metadata = self
                .mempool
                .lanes
                .get_metadata_by_hash(lane_id, &last_dp_hash)
                .unwrap()
                .unwrap();
            let last_dp = self
                .mempool
                .lanes
                .get_dp_by_hash(lane_id, &last_dp_hash)
                .unwrap()
                .unwrap();

            ((last_metadata, last_dp), last_dp_hash.clone())
        }

        pub fn current_size(&self) -> Option<LaneBytesSize> {
            let lane_id = LaneId(self.validator_pubkey().clone());
            self.current_size_of(&lane_id)
        }

        pub fn push_data_proposal(&mut self, dp: DataProposal) {
            let lane_id = LaneId(self.validator_pubkey().clone());

            let lane_size = self.current_size().unwrap();
            let size = lane_size + dp.estimate_size();
            self.mempool
                .lanes
                .put_no_verification(
                    lane_id,
                    (
                        LaneEntryMetadata {
                            parent_data_proposal_hash: dp.parent_data_proposal_hash.clone(),
                            cumul_size: size,
                            signatures: vec![],
                        },
                        dp,
                    ),
                )
                .unwrap();
        }

        pub async fn handle_consensus_event(&mut self, consensus_proposal: ConsensusProposal) {
            self.mempool
                .handle_consensus_event(ConsensusEvent::CommitConsensusProposal(
                    CommittedConsensusProposal {
                        staking: self.mempool.staking.clone(),
                        consensus_proposal,
                        certificate: AggregateSignature::default(),
                    },
                ))
                .await
                .expect("Error while handling consensus event");
        }

        pub fn submit_tx(&mut self, tx: &Transaction) {
            self.mempool
                .handle_api_message(RestApiMessage::NewTx(tx.clone()))
                .unwrap();
        }

        pub fn create_data_proposal(
            &self,
            parent_hash: Option<DataProposalHash>,
            txs: &[Transaction],
        ) -> DataProposal {
            DataProposal::new(parent_hash, txs.to_vec())
        }

        pub fn create_data_proposal_on_top(
            &mut self,
            lane_id: LaneId,
            txs: &[Transaction],
        ) -> DataProposal {
            DataProposal::new(self.current_hash(&lane_id), txs.to_vec())
        }

        pub fn process_new_data_proposal(&mut self, dp: DataProposal) -> Result<()> {
            self.mempool.lanes.store_data_proposal(
                &self.mempool.crypto,
                &LaneId(self.mempool.crypto.validator_pubkey().clone()),
                dp,
            )?;
            Ok(())
        }

        pub async fn process_cut_with_dp(
            &mut self,
            leader: &ValidatorPublicKey,
            dp_hash: &DataProposalHash,
            cumul_size: LaneBytesSize,
            slot: u64,
        ) -> Result<Cut> {
            let cut = vec![(
                LaneId(leader.clone()),
                dp_hash.clone(),
                cumul_size,
                AggregateSignature::default(),
            )];

            self.mempool
                .handle_consensus_event(ConsensusEvent::CommitConsensusProposal(
                    CommittedConsensusProposal {
                        staking: self.mempool.staking.clone(),
                        consensus_proposal: model::ConsensusProposal {
                            slot,
                            cut: cut.clone(),
                            staking_actions: vec![],
                            timestamp: TimestampMs(777),
                            parent_hash: ConsensusProposalHash("test".to_string()),
                        },
                        certificate: AggregateSignature::default(),
                    },
                ))
                .await?;

            Ok(cut)
        }
    }

    pub fn create_data_vote(
        crypto: &BlstCrypto,
        hash: DataProposalHash,
        size: LaneBytesSize,
    ) -> Result<MempoolNetMessage> {
        Ok(MempoolNetMessage::DataVote(crypto.sign((hash, size))?))
    }

    pub fn make_register_contract_tx(name: ContractName) -> Transaction {
        BlobTransaction::new(
            "hyle@hyle",
            vec![RegisterContractAction {
                verifier: "test".into(),
                program_id: ProgramId(vec![]),
                state_commitment: StateCommitment(vec![0, 1, 2, 3]),
                contract_name: name,
                ..Default::default()
            }
            .as_blob("hyle".into(), None, None)],
        )
        .into()
    }

    #[test_log::test(tokio::test)]
    async fn test_sending_sync_request() -> Result<()> {
        let mut ctx = MempoolTestCtx::new("mempool").await;
        let crypto2 = BlstCrypto::new("2").unwrap();
        let pubkey2 = crypto2.validator_pubkey();

        ctx.handle_consensus_event(ConsensusProposal {
            cut: vec![(
                LaneId(pubkey2.clone()),
                DataProposalHash("dp_hash_in_cut".to_owned()),
                LaneBytesSize::default(),
                PoDA::default(),
            )],
            ..ConsensusProposal::default()
        })
        .await;

        // Assert that we send a SyncReply
        match ctx
            .assert_send(crypto2.validator_pubkey(), "SyncReply")
            .await
            .msg
        {
            MempoolNetMessage::SyncRequest(from, to) => {
                assert_eq!(from, None);
                assert_eq!(to, Some(DataProposalHash("dp_hash_in_cut".to_owned())));
            }
            _ => panic!("Expected SyncReply message"),
        };
        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn test_receiving_sync_request() -> Result<()> {
        let mut ctx = MempoolTestCtx::new("mempool").await;

        // Store the DP locally.
        let register_tx = make_register_contract_tx(ContractName::new("test1"));
        let data_proposal = ctx.create_data_proposal(None, &[register_tx.clone()]);
        ctx.process_new_data_proposal(data_proposal.clone())?;

        // Since mempool is alone, no broadcast
        let (..) = ctx.last_lane_entry(&LaneId(ctx.validator_pubkey().clone()));

        // Add new validator
        let crypto2 = BlstCrypto::new("2").unwrap();
        ctx.add_trusted_validator(crypto2.validator_pubkey());

        for _ in 1..5 {
            let signed_msg = crypto2.sign_msg_with_header(MempoolNetMessage::SyncRequest(
                None,
                Some(data_proposal.hashed()),
            ))?;

            ctx.mempool
                .handle_net_message(signed_msg, &ctx.mempool_sync_request_sender)
                .await
                .expect("should handle net message");
        }

        tokio::time::sleep(Duration::from_millis(500)).await;

        for _ in 1..5 {
            let signed_msg = crypto2.sign_msg_with_header(MempoolNetMessage::SyncRequest(
                None,
                Some(data_proposal.hashed()),
            ))?;

            ctx.mempool
                .handle_net_message(signed_msg, &ctx.mempool_sync_request_sender)
                .await
                .expect("should handle net message");
        }

        // Assert that we send a SyncReply
        match ctx
            .assert_send(crypto2.validator_pubkey(), "SyncReply")
            .await
            .msg
        {
            MempoolNetMessage::SyncReply(_metadata, data_proposal_r) => {
                assert_eq!(data_proposal_r, data_proposal);
            }
            _ => panic!("Expected SyncReply message"),
        };

        assert!(ctx.out_receiver.is_empty());
        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn test_receiving_sync_requests_multiple_dps() -> Result<()> {
        let mut ctx = MempoolTestCtx::new("mempool").await;

        // Store the DP locally.
        let register_tx = make_register_contract_tx(ContractName::new("test1"));
        let data_proposal = ctx.create_data_proposal(None, &[register_tx.clone()]);
        ctx.process_new_data_proposal(data_proposal.clone())?;

        let data_proposal2 =
            ctx.create_data_proposal(Some(data_proposal.hashed()), &[register_tx.clone()]);
        ctx.process_new_data_proposal(data_proposal2.clone())?;

        // Since mempool is alone, no broadcast
        let (..) = ctx.last_lane_entry(&LaneId(ctx.validator_pubkey().clone()));

        // Add new validator
        let crypto2 = BlstCrypto::new("2").unwrap();
        ctx.add_trusted_validator(crypto2.validator_pubkey());

        // Sync request for the interval up to data proposal
        for _ in 1..3 {
            let signed_msg = crypto2.sign_msg_with_header(MempoolNetMessage::SyncRequest(
                None,
                Some(data_proposal.hashed()),
            ))?;

            ctx.mempool
                .handle_net_message(signed_msg, &ctx.mempool_sync_request_sender)
                .await
                .expect("should handle net message");
        }

        tokio::time::sleep(Duration::from_millis(500)).await;

        // Sync request for the whole interval
        for _ in 1..3 {
            let signed_msg = crypto2.sign_msg_with_header(MempoolNetMessage::SyncRequest(
                None,
                Some(data_proposal2.hashed()),
            ))?;

            ctx.mempool
                .handle_net_message(signed_msg, &ctx.mempool_sync_request_sender)
                .await
                .expect("should handle net message");
        }

        tokio::time::sleep(Duration::from_millis(500)).await;

        // Sync request for the whole interval
        for _ in 1..3 {
            let signed_msg =
                crypto2.sign_msg_with_header(MempoolNetMessage::SyncRequest(None, None))?;

            ctx.mempool
                .handle_net_message(signed_msg, &ctx.mempool_sync_request_sender)
                .await
                .expect("should handle net message");
        }

        // Assert that we send a SyncReply #1
        match ctx
            .assert_send(crypto2.validator_pubkey(), "SyncReply")
            .await
            .msg
        {
            MempoolNetMessage::SyncReply(_metadata, data_proposal_r) => {
                assert_eq!(data_proposal_r, data_proposal);
            }
            _ => panic!("Expected SyncReply message"),
        };
        // Assert that we send a SyncReply #2
        match ctx
            .assert_send(crypto2.validator_pubkey(), "SyncReply")
            .await
            .msg
        {
            MempoolNetMessage::SyncReply(_metadata, data_proposal_r) => {
                assert_eq!(data_proposal_r, data_proposal2);
            }
            _ => panic!("Expected SyncReply message"),
        };

        assert!(ctx.out_receiver.is_empty());
        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn test_receiving_sync_reply() -> Result<()> {
        let mut ctx = MempoolTestCtx::new("mempool").await;

        // Create a DP and simulate we requested it.
        let register_tx = make_register_contract_tx(ContractName::new("test1"));
        let data_proposal = ctx.create_data_proposal(None, &[register_tx.clone()]);
        let cumul_size = LaneBytesSize(data_proposal.estimate_size() as u64);

        // Add new validator
        let crypto2 = BlstCrypto::new("2").unwrap();
        let crypto3 = BlstCrypto::new("3").unwrap();

        ctx.add_trusted_validator(crypto2.validator_pubkey());

        // First: the message is from crypto2, but the DP is not signed correctly
        let signed_msg = crypto2.sign_msg_with_header(MempoolNetMessage::SyncReply(
            LaneEntryMetadata {
                parent_data_proposal_hash: None,
                cumul_size,
                signatures: vec![crypto3
                    .sign((data_proposal.hashed(), cumul_size))
                    .expect("should sign")],
            },
            data_proposal.clone(),
        ))?;

        let handle = ctx
            .mempool
            .handle_net_message(signed_msg.clone(), &ctx.mempool_sync_request_sender)
            .await;
        assert_eq!(
            handle.expect_err("should fail").to_string(),
            format!(
                "At least one lane entry is missing signature from {}",
                crypto2.validator_pubkey()
            )
        );

        // Second: the message is NOT from crypto2, but the DP is signed by crypto2
        let signed_msg = crypto3.sign_msg_with_header(MempoolNetMessage::SyncReply(
            LaneEntryMetadata {
                parent_data_proposal_hash: None,
                cumul_size,
                signatures: vec![crypto2
                    .sign((data_proposal.hashed(), cumul_size))
                    .expect("should sign")],
            },
            data_proposal.clone(),
        ))?;

        // This actually fails - we don't know how to handle it
        let handle = ctx
            .mempool
            .handle_net_message(signed_msg.clone(), &ctx.mempool_sync_request_sender)
            .await;
        assert_eq!(
            handle.expect_err("should fail").to_string(),
            format!(
                "At least one lane entry is missing signature from {}",
                crypto3.validator_pubkey()
            )
        );

        // Third: the message is from crypto2, the signature is from crypto2, but the message is wrong
        let signed_msg = crypto3.sign_msg_with_header(MempoolNetMessage::SyncReply(
            LaneEntryMetadata {
                parent_data_proposal_hash: None,
                cumul_size,
                signatures: vec![crypto2
                    .sign((DataProposalHash("non_existent".to_owned()), cumul_size))
                    .expect("should sign")],
            },
            data_proposal.clone(),
        ))?;

        // This actually fails - we don't know how to handle it
        let handle = ctx
            .mempool
            .handle_net_message(signed_msg.clone(), &ctx.mempool_sync_request_sender)
            .await;
        assert_eq!(
            handle.expect_err("should fail").to_string(),
            format!(
                "At least one lane entry is missing signature from {}",
                crypto3.validator_pubkey()
            )
        );

        // Fourth: the message is from crypto2, the signature is from crypto2, but the size is wrong
        let signed_msg = crypto2.sign_msg_with_header(MempoolNetMessage::SyncReply(
            LaneEntryMetadata {
                parent_data_proposal_hash: None,
                cumul_size: LaneBytesSize(0),
                signatures: vec![crypto2
                    .sign((data_proposal.hashed(), cumul_size))
                    .expect("should sign")],
            },
            data_proposal.clone(),
        ))?;

        // This actually fails - we don't know how to handle it
        let handle = ctx
            .mempool
            .handle_net_message(signed_msg.clone(), &ctx.mempool_sync_request_sender)
            .await;
        assert_eq!(
            handle.expect_err("should fail").to_string(),
            format!(
                "At least one lane entry is missing signature from {}",
                crypto2.validator_pubkey()
            )
        );

        // Final case: message is correct
        let signed_msg = crypto2.sign_msg_with_header(MempoolNetMessage::SyncReply(
            LaneEntryMetadata {
                parent_data_proposal_hash: None,
                cumul_size,
                signatures: vec![crypto2
                    .sign((data_proposal.hashed(), cumul_size))
                    .expect("should sign")],
            },
            data_proposal.clone(),
        ))?;

        let handle = ctx
            .mempool
            .handle_net_message(signed_msg.clone(), &ctx.mempool_sync_request_sender)
            .await;
        assert_ok!(handle, "Should handle net message");

        // Assert that the lane entry was added
        assert!(ctx.mempool.lanes.contains(
            &ctx.mempool.get_lane(crypto2.validator_pubkey()),
            &data_proposal.hashed()
        ));

        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn test_sync_request_single_dp() -> Result<()> {
        let mut ctx = MempoolTestCtx::new("mempool").await;
        let lane_id = ctx.mempool.own_lane_id().clone();
        let crypto = ctx.mempool.crypto.clone();

        // Create a chain of 3 DataProposals
        let dp1 = DataProposal::new(None, vec![]);
        let _dp1_size = LaneBytesSize(dp1.estimate_size() as u64);
        let dp1_hash = dp1.hashed();
        ctx.mempool
            .lanes
            .store_data_proposal(&crypto, &lane_id, dp1.clone())?;

        let dp2 = DataProposal::new(Some(dp1_hash.clone()), vec![]);
        let dp2_size = LaneBytesSize(dp2.estimate_size() as u64);
        let dp2_hash = dp2.hashed();
        ctx.mempool
            .lanes
            .store_data_proposal(&crypto, &lane_id, dp2.clone())?;

        let dp3 = DataProposal::new(Some(dp2_hash.clone()), vec![]);
        let _dp3_size = LaneBytesSize(dp3.estimate_size() as u64);
        let _dp3_hash = dp3.hashed();
        ctx.mempool
            .lanes
            .store_data_proposal(&crypto, &lane_id, dp3.clone())?;

        // Send a sync request for dp2 only
        ctx.mempool
            .on_sync_request(
                &ctx.mempool_sync_request_sender,
                None,
                Some(dp2_hash.clone()),
                crypto.validator_pubkey().clone(),
            )
            .await?;

        tokio::time::sleep(Duration::from_millis(500)).await;

        // Verify that only dp2 was sent in the sync reply
        match ctx
            .assert_send(&crypto.validator_pubkey().clone(), "SyncReply")
            .await
            .msg
        {
            MempoolNetMessage::SyncReply(metadata, dp) => {
                assert_eq!(dp, dp2);
                assert_eq!(metadata.parent_data_proposal_hash, Some(dp1_hash));
                assert_eq!(metadata.cumul_size, dp2_size);
                assert_eq!(metadata.signatures.len(), 1);
            }
            _ => panic!("Expected SyncReply message"),
        }

        // Verify no other messages were sent
        assert!(ctx.out_receiver.try_recv().is_err());

        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn test_serialization_deserialization() -> Result<()> {
        let mut ctx = MempoolTestCtx::new("mempool").await;
        ctx.mempool.file = Some(".".into());

        assert!(Mempool::save_on_disk(
            ctx.mempool
                .file
                .clone()
                .unwrap()
                .join("test-mempool.bin")
                .as_path(),
            &ctx.mempool.inner
        )
        .is_ok());

        assert!(Mempool::load_from_disk::<MempoolStore>(
            ctx.mempool.file.unwrap().join("test-mempool.bin").as_path(),
        )
        .is_some());

        std::fs::remove_file("./test-mempool.bin").expect("Failed to delete test-mempool.bin");

        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn test_get_latest_car_and_new_cut() {
        let mut ctx = MempoolTestCtx::new("mempool").await;

        let dp_orig = ctx.create_data_proposal(None, &[]);
        ctx.process_new_data_proposal(dp_orig.clone()).unwrap();

        let staking = Staking::new();

        let latest = ctx
            .mempool
            .lanes
            .get_latest_car(&ctx.own_lane(), &staking, None)
            .unwrap();
        assert!(latest.is_none());

        // Force some signature for f+1 check if needed:
        // This requires more advanced stubbing of Staking if you want a real test.

        let cut = ctx
            .mempool
            .handle_querynewcut(&mut QueryNewCut(staking))
            .unwrap();
        assert_eq!(0, cut.len());
    }
}

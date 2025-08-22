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
    utils::{conf::SharedConf, serialize::BorshableIndexMap},
};
use anyhow::{bail, Context, Result};
use api::RestApiMessage;
use block_construction::BlockUnderConstruction;
use borsh::{BorshDeserialize, BorshSerialize};
use client_sdk::tcp_client::TcpServerMessage;
use hyle_crypto::SharedBlstCrypto;
use hyle_modules::{bus::BusMessage, log_warn, module_bus_client, utils::static_type_map::Pick};
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
use tracing::{debug, info, trace};

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

#[cfg(test)]
pub mod tests;

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

/// Validator Data Availability Guarantee
/// This is a signed message that contains the hash of the data proposal and the size of the lane (DP included)
/// It acts as proof the validator committed to making this DP available.
pub type ValidatorDAG = SignedByValidator<(DataProposalHash, LaneBytesSize)>;
type UnaggregatedPoDA = Vec<ValidatorDAG>;

#[derive(Default, BorshSerialize, BorshDeserialize)]
pub struct MempoolStore {
    // own_lane.rs
    waiting_dissemination_txs: BorshableIndexMap<TxHash, Transaction>,
    // TODO: implement serialization, probably with a custom future that yields the unmodified Tx
    // on cancellation
    #[borsh(skip)]
    processing_txs: OrderedJoinSet<Result<Transaction>>,
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

#[derive(Debug, Clone)]
pub struct QueryNewCut {
    pub staking: Staking,
    pub full: bool,
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

impl BusMessage for MempoolNetMessage {}

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

    /// Creates a cut with local material on QueryNewCut message reception (from consensus)
    fn handle_querynewcut(&mut self, staking: &mut QueryNewCut) -> Result<Cut> {
        self.metrics.query_new_cut(staking);
        let emptyvec = vec![];
        let previous_cut = self
            .last_ccp
            .as_ref()
            .map(|ccp| &ccp.consensus_proposal.cut)
            .unwrap_or(&emptyvec);

        let mut cut: Cut = vec![];
        for lane_id in self.lanes.get_lane_ids() {
            let previous_entry = previous_cut
                .iter()
                .find(|(lane_id_, _, _, _)| lane_id_ == lane_id);
            let latest_car =
                self.lanes
                    .get_latest_car(lane_id, &staking.staking, previous_entry)?;
            if staking.full {
                // Always push latest if present
                if let Some((dp_hash, cumul_size, poda)) = latest_car {
                    cut.push((lane_id.clone(), dp_hash, cumul_size, poda));
                }
            } else {
                match (previous_entry, latest_car) {
                    (Some((_, prev_hash, _, _)), Some((dp_hash, cumul_size, poda))) => {
                        // Only push if changed
                        if &dp_hash != prev_hash {
                            cut.push((lane_id.clone(), dp_hash, cumul_size, poda));
                        }
                    }
                    (None, Some((dp_hash, cumul_size, poda))) => {
                        // New lane, push
                        cut.push((lane_id.clone(), dp_hash, cumul_size, poda));
                    }
                    // If latest_car is None, do not push anything for this lane
                    _ => {}
                }
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

    fn broadcast_weak(&mut self, net_message: MempoolNetMessage) -> Result<()> {
        let own_key = self.crypto.validator_pubkey();
        let mut selected: HashSet<ValidatorPublicKey> = self
            .staking
            .choose_weak_quorum(vec![own_key], &mut rand::thread_rng())
            .context("Choosing validators for a weak certificate")?
            .into_iter()
            .map(|s| s.clone())
            .collect();

        selected.insert(own_key.clone());

        _ = self.broadcast_only_for_net_message(selected, net_message)?;

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

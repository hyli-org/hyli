//! Mempool logic & pending transaction management.

use crate::{
    bus::{command_response::Query, BusClientSender},
    consensus::{CommittedConsensusProposal, ConsensusEvent},
    genesis::GenesisEvent,
    model::*,
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
use hyli_crypto::SharedBlstCrypto;
use hyli_modules::{bus::BusMessage, log_warn, module_bus_client};
use hyli_net::ordered_join_set::OrderedJoinSet;
use indexmap::IndexSet;
use metrics::MempoolMetrics;
use serde::{Deserialize, Serialize};
use staking::state::Staking;
use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    fmt::Display,
    ops::{Deref, DerefMut},
    path::PathBuf,
    time::Duration,
};
use storage::{LaneEntryMetadata, Storage};
use tokio::task::JoinSet;
use verify_tx::DataProposalVerdict;
// Pick one of the two implementations
// use storage_memory::LanesStorage;
// Pick one of the two implementations by changing the re-export below.
// pub use storage_memory::{shared_lanes_storage, LanesStorage};
pub use storage_fjall::{shared_lanes_storage, LanesStorage};
use strum_macros::IntoStaticStr;
use tracing::{debug, info, trace};

pub mod api;
pub mod block_construction;
pub mod dissemination;
pub mod metrics;
pub mod module;
pub mod own_lane;
pub mod storage;
pub mod storage_fjall;
pub mod storage_memory;
pub mod verifiers;
pub mod verify_tx;

#[cfg(test)]
pub mod tests;

pub use dissemination::DisseminationEvent;

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
/// We don't actually sign the Lane ID itself, assuming DPs are unique enough across lanes, as BLS signatures are slow.
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
    sender(DisseminationEvent),
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
    DataProposal(LaneId, DataProposalHash, DataProposal),
    DataVote(LaneId, ValidatorDAG),
    PoDAUpdate(LaneId, DataProposalHash, Vec<ValidatorDAG>),
    SyncRequest(LaneId, Option<DataProposalHash>, Option<DataProposalHash>),
    SyncReply(LaneId, LaneEntryMetadata, DataProposal),
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
            MempoolNetMessage::DataProposal(lane_id, hash, _) => {
                HeaderSignableData(borsh::to_vec(&(lane_id, hash)).unwrap_or_default())
            }
            MempoolNetMessage::DataVote(lane_id, vdag) => {
                HeaderSignableData(borsh::to_vec(&(lane_id, vdag.msg.clone())).unwrap_or_default())
            }
            MempoolNetMessage::PoDAUpdate(lane_id, data_proposal_hash, vdags) => {
                HeaderSignableData(
                    borsh::to_vec(&(lane_id, data_proposal_hash, vdags)).unwrap_or_default(),
                )
            }
            MempoolNetMessage::SyncRequest(lane_id, from, to) => {
                HeaderSignableData(borsh::to_vec(&(lane_id, from, to)).unwrap_or_default())
            }
            MempoolNetMessage::SyncReply(lane_id, metadata, data_proposal) => {
                let hash = (lane_id, metadata, data_proposal.hashed().0.clone());
                HeaderSignableData(borsh::to_vec(&hash).unwrap_or_default())
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
    /// Creates a cut with local material on QueryNewCut message reception (from consensus)
    /// DO NOT make this async without proper deadlock considerations, see below.
    fn handle_querynewcut(&mut self, staking: &mut QueryNewCut) -> Result<Cut> {
        self.metrics.query_new_cut(staking);
        let emptyvec = vec![];
        let previous_cut = self
            .last_ccp
            .as_ref()
            .map(|ccp| &ccp.consensus_proposal.cut)
            .unwrap_or(&emptyvec);

        let mut cut: Cut = vec![];
        // We lock in read for the full loop for performance
        // Because all writes to tips happen in mempool, this should be essentially free.
        // NB: if this function ever becomes async, we must consider deadlocks here as this is a std::sync::RwLock
        let lane_tips = self.lanes.lane_tips_read();
        for lane_id in lane_tips.keys() {
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

    async fn handle_net_message(&mut self, msg: MsgWithHeader<MempoolNetMessage>) -> Result<()> {
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
            MempoolNetMessage::DataProposal(lane_id, data_proposal_hash, data_proposal) => {
                self.on_data_proposal(&lane_id, data_proposal_hash, data_proposal)?;
            }
            MempoolNetMessage::DataVote(lane_id, vdag) => {
                self.on_data_vote(lane_id, vdag)?;
            }
            MempoolNetMessage::PoDAUpdate(lane_id, data_proposal_hash, signatures) => {
                self.on_poda_update(&lane_id, &data_proposal_hash, signatures)?
            }
            MempoolNetMessage::SyncRequest(
                lane_id,
                from_data_proposal_hash,
                to_data_proposal_hash,
            ) => {
                self.on_sync_request(
                    lane_id,
                    from_data_proposal_hash,
                    to_data_proposal_hash,
                    validator.clone(),
                )
                .await?;
            }
            MempoolNetMessage::SyncReply(lane_id, metadata, data_proposal) => {
                self.on_sync_reply(&lane_id, validator, metadata, data_proposal)
                    .await?;
            }
        }
        Ok(())
    }

    async fn on_sync_request(
        &mut self,
        lane_id: LaneId,
        from: Option<DataProposalHash>,
        to: Option<DataProposalHash>,
        validator: ValidatorPublicKey,
    ) -> Result<()> {
        debug!(
            "{} SyncRequest received from validator {validator} for last_data_proposal_hash {:?}",
            lane_id, to
        );

        let Some(to) = to.or(self.lanes.get_lane_hash_tip(&lane_id)) else {
            info!("Nothing to do for this SyncRequest");
            return Ok(());
        };

        self.send_dissemination_event(DisseminationEvent::SyncRequestIn {
            lane_id: lane_id.clone(),
            from: from.clone(),
            to: Some(to.clone()),
            requester: validator.clone(),
        })?;

        Ok(())
    }

    async fn on_sync_reply(
        &mut self,
        lane_id: &LaneId,
        sender_validator: &ValidatorPublicKey,
        metadata: LaneEntryMetadata,
        data_proposal: DataProposal,
    ) -> Result<()> {
        debug!("SyncReply from validator {sender_validator}");

        self.metrics
            .sync_reply_receive(lane_id, self.crypto.validator_pubkey());

        let lane_operator = self.get_lane_operator(lane_id);

        let missing_entry_not_present = {
            let expected_message = (data_proposal.hashed(), metadata.cumul_size);

            !metadata
                .signatures
                .iter()
                .any(|s| &s.signature.validator == lane_operator && s.msg == expected_message)
        };

        // Ensure all lane entries are signed by the operator.
        if missing_entry_not_present {
            bail!(
                "At least one lane entry is missing signature from {}",
                lane_operator
            );
        }

        // Store the missing entry in the target lane.
        trace!(
            "Filling hole with 1 entry (parent dp hash: {:?}) for {lane_id}",
            metadata.parent_data_proposal_hash
        );

        let dp_hash = data_proposal.hashed();
        let cumul_size = metadata.cumul_size;

        // SyncReply only comes for missing data proposals. We should NEVER update the lane tip
        self.lanes
            .put_no_verification(lane_id.clone(), (metadata, data_proposal))?;

        self.send_dissemination_event(DisseminationEvent::DpStored {
            lane_id: lane_id.clone(),
            data_proposal_hash: dp_hash,
            cumul_size,
        })?;

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
        } else {
            self.send_dissemination_event(DisseminationEvent::PoDAUpdated {
                lane_id: lane_id.clone(),
                data_proposal_hash: data_proposal_hash.clone(),
                signatures: podas,
            })?;
        }

        Ok(())
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
        self.send_dissemination_event(DisseminationEvent::SyncRequestNeeded {
            lane_id: lane_id.clone(),
            from: from_data_proposal_hash.cloned(),
            to: to_data_proposal_hash.cloned(),
        })?;
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

    #[inline(always)]
    fn send_dissemination_event(&mut self, event: DisseminationEvent) -> Result<()> {
        let enum_variant_name: &'static str = (&event).into();
        let error_msg = format!("Sending DisseminationEvent::{enum_variant_name} msg on the bus");
        self.bus.send(event).context(error_msg)?;
        Ok(())
    }
}

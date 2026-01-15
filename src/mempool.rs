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
use hyli_modules::{bus::BusMessage, module_bus_client};
use hyli_net::ordered_join_set::OrderedJoinSet;
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
use storage::Storage;
use verify_tx::DataProposalVerdict;
// Pick one of the two implementations
// use storage_memory::LanesStorage;
// Pick one of the two implementations by changing the re-export below.
// pub use storage_memory::{shared_lanes_storage, LanesStorage};
use hyli_crypto::BlstCrypto;
pub use storage_fjall::{shared_lanes_storage, LanesStorage};
use strum_macros::IntoStaticStr;
use tracing::{debug, info};

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
pub type BufferedEntry = (Vec<ValidatorDAG>, DataProposal);

#[derive(Default, BorshSerialize, BorshDeserialize)]
pub struct MempoolStore {
    // own_lane.rs
    #[borsh(skip)]
    ready_to_create_dps: bool,
    waiting_dissemination_txs: HashMap<LaneId, BorshableIndexMap<TxHash, Transaction>>,
    // TODO: implement serialization, probably with a custom future that yields the unmodified Tx
    // on cancellation
    #[borsh(skip)]
    processing_txs: OrderedJoinSet<Result<(Transaction, LaneSuffix)>>,
    #[borsh(skip)]
    own_data_proposal_in_preparation: own_lane::OwnDataProposalPreparation,
    // Skipped to clear on reset
    #[borsh(skip)]
    buffered_entries: BTreeMap<LaneId, HashMap<DataProposalHash, BufferedEntry>>,
    // Skipped to clear on reset
    #[borsh(skip)]
    buffered_votes: BTreeMap<LaneId, BTreeMap<DataProposalHash, Vec<ValidatorDAG>>>,
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
    DataProposal(LaneId, DataProposalHash, DataProposal, ValidatorDAG),
    DataVote(LaneId, ValidatorDAG),
    SyncRequest(LaneId, Option<DataProposalHash>, Option<DataProposalHash>),
    SyncReply(LaneId, Vec<ValidatorDAG>, DataProposal),
}

impl BusMessage for MempoolNetMessage {}

impl Display for MempoolNetMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let enum_variant: &'static str = self.into();
        write!(f, "{enum_variant}")
    }
}

hyli_net::impl_tcp_message_label_with_prefix!(MempoolNetMessage, "MempoolNetMessage", {
    DataProposal,
    DataVote,
    SyncRequest,
    SyncReply,
});

impl IntoHeaderSignableData for MempoolNetMessage {
    fn to_header_signable_data(&self) -> HeaderSignableData {
        match self {
            // We get away with only signing the hash - verification must check the hash is correct
            MempoolNetMessage::DataProposal(lane_id, hash, _, vote) => HeaderSignableData(
                borsh::to_vec(&(lane_id, hash, vote.msg.clone())).unwrap_or_default(),
            ),
            MempoolNetMessage::DataVote(lane_id, vdag) => {
                HeaderSignableData(borsh::to_vec(&(lane_id, vdag.msg.clone())).unwrap_or_default())
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
    OnHashedDataProposal((LaneId, DataProposal, ValidatorDAG)),
    OnProcessedDataProposal((LaneId, DataProposalVerdict, DataProposal)),
    OnHashedSyncReply((LaneId, Vec<ValidatorDAG>, DataProposal, DataProposalHash)),
}

impl Mempool {
    pub(super) fn on_data_vote(&mut self, lane_id: LaneId, vdag: ValidatorDAG) -> Result<()> {
        self.metrics.on_data_vote.add(1, &[]);

        let validator = vdag.signature.validator.clone();
        let data_proposal_hash = vdag.msg.0.clone();
        let buffered_vote = vdag.clone();
        let signatures =
            match self
                .lanes
                .add_signatures(&lane_id, &data_proposal_hash, std::iter::once(vdag))
            {
                Ok(signatures) => signatures,
                Err(err) => {
                    debug!(
                        "Buffering vote from {} for lane {} (dp {}): {}",
                        validator, lane_id, data_proposal_hash, err
                    );
                    self.inner
                        .buffered_votes
                        .entry(lane_id)
                        .or_default()
                        .entry(data_proposal_hash)
                        .or_default()
                        .push(buffered_vote);
                    return Ok(());
                }
            };
        self.send_dissemination_event(DisseminationEvent::VoteReceived {
            lane_id: lane_id.clone(),
            data_proposal_hash: data_proposal_hash.clone(),
            voter: validator.clone(),
        })?;

        self.maybe_cache_poda(&lane_id, &data_proposal_hash, &signatures)?;
        Ok(())
    }

    fn maybe_cache_poda(
        &mut self,
        lane_id: &LaneId,
        data_proposal_hash: &DataProposalHash,
        signatures: &[ValidatorDAG],
    ) -> Result<()> {
        let Some(metadata) = self
            .lanes
            .get_metadata_by_hash(lane_id, data_proposal_hash)?
        else {
            return Ok(());
        };

        let f = self.staking.compute_f();
        let threshold = f + 1;
        if let Some(cached_poda) = &metadata.cached_poda {
            let cached_voting_power = self
                .staking
                .compute_voting_power(cached_poda.validators.as_slice());
            if cached_voting_power >= threshold {
                return Ok(());
            }
        }

        let bonded_validators = self.staking.bonded();
        let filtered: Vec<&ValidatorDAG> = signatures
            .iter()
            .filter(|s| bonded_validators.contains(&s.signature.validator))
            .collect();
        let filtered_validators: Vec<ValidatorPublicKey> = filtered
            .iter()
            .map(|s| s.signature.validator.clone())
            .collect();

        let voting_power = self.staking.compute_voting_power(&filtered_validators);
        if voting_power < threshold {
            return Ok(());
        }

        let aggregated = BlstCrypto::aggregate(
            (data_proposal_hash.clone(), metadata.cumul_size),
            filtered.as_slice(),
        )?;
        self.lanes
            .set_cached_poda(lane_id, data_proposal_hash, aggregated.signature)?;
        Ok(())
    }
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

    async fn handle_internal_event(&mut self, event: ProcessedDPEvent) -> Result<()> {
        match event {
            ProcessedDPEvent::OnHashedDataProposal((lane_id, data_proposal, vote)) => self
                .on_hashed_data_proposal(&lane_id, data_proposal, vote)
                .context("Hashing data proposal"),
            ProcessedDPEvent::OnHashedSyncReply((lane_id, signatures, data_proposal, dp_hash)) => {
                self.on_hashed_sync_reply(lane_id, signatures, data_proposal, dp_hash)
                    .await
                    .context("Handling sync reply data proposal")
            }
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

                // We need to wait until we've updated our lanes_tip when restarting.
                self.ready_to_create_dps = true;

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
            MempoolNetMessage::DataProposal(lane_id, data_proposal_hash, data_proposal, vdag) => {
                if lane_id.operator() != validator {
                    bail!("DP from non-operator {} for lane {}", validator, lane_id);
                }
                if &vdag.signature.validator != validator {
                    bail!(
                        "Vote signature validator {} does not match header signer {}",
                        vdag.signature.validator,
                        validator
                    );
                }
                BlstCrypto::verify(&vdag).context("Invalid DataProposal vote signature")?;
                self.on_data_proposal(&lane_id, data_proposal_hash, data_proposal, vdag.clone())?;
                self.on_data_vote(lane_id, vdag)?;
            }
            MempoolNetMessage::DataVote(lane_id, vdag) => {
                BlstCrypto::verify(&vdag).context("Invalid DataVote signature")?;
                self.on_data_vote(lane_id, vdag)?;
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
            MempoolNetMessage::SyncReply(lane_id, sigs, data_proposal) => {
                // Don't bother checking the signatures for sync replies, we trust our internal bookkeeping.
                self.on_sync_reply(&lane_id, validator, sigs, data_proposal)
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

    fn get_lane_operator<'a>(&self, lane_id: &'a LaneId) -> &'a ValidatorPublicKey {
        lane_id.operator()
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
    fn send_net_message_broadcast(&mut self, net_message: MempoolNetMessage) -> Result<()> {
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
    fn send_dissemination_event(&mut self, event: DisseminationEvent) -> Result<()> {
        Self::send_dissemination_event_over(&mut self.bus, event)
    }

    // Variant for one function from block_construction.rs
    #[inline(always)]
    fn send_dissemination_event_over(
        bus: &mut MempoolBusClient,
        event: DisseminationEvent,
    ) -> Result<()> {
        let enum_variant_name: &'static str = (&event).into();
        let error_msg = format!("Sending DisseminationEvent::{enum_variant_name} msg on the bus");
        bus.send(event).context(error_msg)?;
        Ok(())
    }
}

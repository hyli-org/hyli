//! Dissemination manager: routes data proposals, tracks peer knowledge, and schedules sync/reply.
//! It consumes events from mempool/consensus and emits outbound network messages.

use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

use anyhow::{bail, Context, Result};
use futures::StreamExt;
use hyli_model::{
    utils::TimestampMs, DataProposalHash, LaneBytesSize, LaneId, NodeStateEvent, ValidatorPublicKey,
};
use hyli_modules::{
    log_error, log_warn, module_bus_client, module_handle_messages, modules::Module,
};
use hyli_net::clock::TimestampMsClock;
use staking::state::Staking;
use strum_macros::IntoStaticStr;
use tracing::{debug, trace};

use crate::{
    bus::BusClientSender,
    consensus::ConsensusEvent,
    model::{Cut, Hashed},
    p2p::network::{HeaderSigner, OutboundMessage},
    utils::conf::{P2pMode, SharedConf},
};

use super::{
    metrics::MempoolMetrics,
    shared_lanes_storage,
    storage::{LaneEntryMetadata, MetadataOrMissingHash, Storage},
    LanesStorage, MempoolNetMessage, ValidatorDAG,
};

use crate::model::SharedRunContext;

const REQUEST_BACKOFF_INITIAL: Duration = Duration::from_secs(1);
const REQUEST_BACKOFF_MAX: Duration = Duration::from_secs(8);
const HOUSEKEEPING_INTERVAL: Duration = Duration::from_millis(400);
#[cfg(not(test))]
const SYNC_REPLY_DEBOUNCE: Duration = Duration::from_secs(2);
#[cfg(test)]
const SYNC_REPLY_DEBOUNCE: Duration = Duration::from_secs(0);

/// Events sent into the dissemination manager (mostly produced by mempool).
#[derive(Debug, Clone, IntoStaticStr)]
pub enum DisseminationEvent {
    NewDpCreated {
        lane_id: LaneId,
        data_proposal_hash: DataProposalHash,
    },
    DpStored {
        lane_id: LaneId,
        data_proposal_hash: DataProposalHash,
        cumul_size: LaneBytesSize,
    },
    PoDAUpdated {
        lane_id: LaneId,
        data_proposal_hash: DataProposalHash,
        signatures: Vec<ValidatorDAG>,
    },
    SyncRequestNeeded {
        lane_id: LaneId,
        from: Option<DataProposalHash>,
        to: Option<DataProposalHash>,
    },
    SyncRequestIn {
        lane_id: LaneId,
        from: Option<DataProposalHash>,
        to: Option<DataProposalHash>,
        requester: ValidatorPublicKey,
    },
    PoDAReady {
        lane_id: LaneId,
        data_proposal_hash: DataProposalHash,
        signatures: Vec<ValidatorDAG>,
    },
    VoteReceived {
        lane_id: LaneId,
        data_proposal_hash: DataProposalHash,
        voter: ValidatorPublicKey,
    },
}

impl hyli_modules::bus::BusMessage for DisseminationEvent {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvidenceState {
    ObservedHas,
    StrongHas,
    WeakHas,
    DefinitelyDoesNotHave,
    Unknown,
}

#[derive(Debug, Clone, Default)]
pub struct PeerState {
    pub last_seen: Option<u128>,
    pub last_dp_sent: HashMap<(LaneId, DataProposalHash), TimestampMs>,
    // Placeholder for per-peer inflight counters (UL/DL) in a later phase.
}

#[derive(Debug, Default)]
struct PeerKnowledge {
    by_peer: HashMap<ValidatorPublicKey, PeerState>,
    by_dp: HashMap<(LaneId, DataProposalHash, ValidatorPublicKey), EvidenceState>,
}

#[derive(Clone, Debug)]
struct ThrottleState {
    last_sent: TimestampMs,
    backoff: Duration,
}

#[derive(Debug, Clone)]
struct PendingSyncRequest {
    lane_id: LaneId,
    from: Option<DataProposalHash>,
    to: DataProposalHash,
    state: ThrottleState,
    next_peer_idx: usize,
}

impl PendingSyncRequest {
    fn select_peer(
        &self,
        peers: &[ValidatorPublicKey],
        knowledge: &HashMap<(LaneId, DataProposalHash, ValidatorPublicKey), EvidenceState>,
    ) -> Option<ValidatorPublicKey> {
        if peers.is_empty() {
            return None;
        }

        let lane_operator = self.lane_id.operator();
        let mut strong_candidates = Vec::new();
        let mut weak_candidates = Vec::new();
        let mut unknown_candidates = Vec::new();

        for peer in peers.iter() {
            let key = (self.lane_id.clone(), self.to.clone(), peer.clone());
            match knowledge.get(&key) {
                Some(EvidenceState::ObservedHas) | Some(EvidenceState::StrongHas) => {
                    strong_candidates.push(peer.clone());
                }
                Some(EvidenceState::WeakHas) => {
                    weak_candidates.push(peer.clone());
                }
                Some(EvidenceState::DefinitelyDoesNotHave) => {}
                None | Some(EvidenceState::Unknown) => {
                    unknown_candidates.push(peer.clone());
                }
            }
        }

        if strong_candidates.is_empty()
            && weak_candidates.is_empty()
            && self.next_peer_idx == 0
            && peers.contains(lane_operator)
        {
            return Some(lane_operator.clone());
        }

        let mut ordered_candidates = Vec::new();
        ordered_candidates.extend(strong_candidates);
        ordered_candidates.extend(weak_candidates);
        ordered_candidates.extend(unknown_candidates);

        if ordered_candidates.is_empty() {
            return None;
        }

        let idx = self.next_peer_idx % ordered_candidates.len();
        ordered_candidates.get(idx).cloned()
    }

    fn record_sent(&mut self, now: TimestampMs) {
        self.state.last_sent = now;
        self.state.backoff = next_backoff(Some(self.state.backoff));
        self.next_peer_idx = self.next_peer_idx.saturating_add(1);
    }
}

pub struct DisseminationManager {
    bus: DisseminationBusClient,
    conf: SharedConf,
    crypto: hyli_crypto::SharedBlstCrypto,
    metrics: MempoolMetrics,
    lanes: LanesStorage,
    knowledge: PeerKnowledge,
    pending_sync_requests: HashMap<(LaneId, DataProposalHash), PendingSyncRequest>,
    dp_first_seen_slot: HashMap<(LaneId, DataProposalHash), u64>,
    owned_lanes: HashSet<LaneId>,
    last_cut: Option<Cut>,
    staking: Staking,
    current_slot: u64,
}

module_bus_client! {
#[derive(Debug)]
struct DisseminationBusClient {
    sender(OutboundMessage),
    receiver(DisseminationEvent),
    receiver(ConsensusEvent),
    receiver(NodeStateEvent),
}
}

impl Module for DisseminationManager {
    type Context = SharedRunContext;

    async fn build(bus: hyli_modules::bus::SharedMessageBus, ctx: Self::Context) -> Result<Self> {
        let bus = DisseminationBusClient::new_from_bus(bus.new_handle()).await;
        let lanes = shared_lanes_storage(&ctx.config.data_directory)?;

        Ok(DisseminationManager {
            bus,
            conf: ctx.config.clone(),
            crypto: ctx.crypto.clone(),
            metrics: MempoolMetrics::global(ctx.config.id.clone()),
            lanes,
            knowledge: PeerKnowledge::default(),
            pending_sync_requests: HashMap::new(),
            dp_first_seen_slot: HashMap::new(),
            owned_lanes: HashSet::new(),
            last_cut: None,
            staking: Staking::default(),
            current_slot: 0,
        })
    }

    async fn run(&mut self) -> Result<()> {
        let mut housekeeping_timer = tokio::time::interval(HOUSEKEEPING_INTERVAL);
        housekeeping_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        module_handle_messages! {
            on_self self,
            listen<DisseminationEvent> evt => {
                _ = log_error!(self.on_event(evt).await, "Handling DisseminationEvent");
            }
            listen<ConsensusEvent> evt => {
                _ = log_error!(self.on_consensus_event(evt), "Handling ConsensusEvent in DisseminationManager");
            }
            listen<NodeStateEvent> evt => {
                _ = log_error!(self.on_node_state_event(evt), "Handling NodeStateEvent in DisseminationManager");
            }
            _ = housekeeping_timer.tick() => {
                _ = log_error!(self.redisseminate_owned_lanes().await, "Disseminate data proposals on tick");
                _ = log_error!(self.process_sync_requests_and_replies().await, "Processing sync requests/replies");
                housekeeping_timer.reset();
            }
        };

        Ok(())
    }
}

impl DisseminationManager {
    pub(crate) async fn on_event(&mut self, event: DisseminationEvent) -> Result<()> {
        match event {
            DisseminationEvent::NewDpCreated {
                lane_id,
                data_proposal_hash,
            } => {
                self.owned_lanes.insert(lane_id.clone());
                trace!(
                    "NewDpCreated for lane {} with hash {}",
                    lane_id,
                    data_proposal_hash
                );
            }
            DisseminationEvent::DpStored {
                lane_id,
                data_proposal_hash,
                ..
            } => {
                self.dp_first_seen_slot
                    .entry((lane_id.clone(), data_proposal_hash.clone()))
                    .or_insert(self.current_slot);
                if self.owned_lanes.contains(&lane_id) {
                    self.maybe_disseminate_dp(&lane_id, &data_proposal_hash)?;
                }
            }
            DisseminationEvent::PoDAUpdated {
                lane_id,
                data_proposal_hash,
                signatures,
            } => {
                self.on_poda_updated(lane_id, data_proposal_hash, signatures);
            }
            DisseminationEvent::SyncRequestNeeded { lane_id, from, to } => {
                let Some(to) = to else {
                    debug!("SyncRequestNeeded missing 'to' for lane {}", lane_id);
                    return Ok(());
                };
                let key = (lane_id.clone(), to.clone());
                self.pending_sync_requests
                    .entry(key)
                    .or_insert(PendingSyncRequest {
                        lane_id,
                        from,
                        to,
                        state: ThrottleState {
                            last_sent: TimestampMs(0),
                            backoff: REQUEST_BACKOFF_INITIAL,
                        },
                        next_peer_idx: 0,
                    });
            }
            DisseminationEvent::PoDAReady {
                lane_id,
                data_proposal_hash,
                signatures,
            } => {
                self.on_poda_updated(
                    lane_id.clone(),
                    data_proposal_hash.clone(),
                    signatures.clone(),
                );
                self.send_poda_update(lane_id, data_proposal_hash, signatures)?;
            }
            DisseminationEvent::VoteReceived {
                lane_id,
                data_proposal_hash,
                voter,
            } => {
                self.knowledge.by_dp.insert(
                    (lane_id.clone(), data_proposal_hash.clone(), voter.clone()),
                    EvidenceState::ObservedHas,
                );
            }
            DisseminationEvent::SyncRequestIn {
                lane_id,
                from,
                to,
                requester,
            } => {
                debug!(
                    "SyncRequestIn from {} for lane {}: {:?} -> {:?}",
                    requester, lane_id, from, to
                );
                self.knowledge
                    .by_peer
                    .entry(requester.clone())
                    .or_default()
                    .last_seen = Some(hyli_net::clock::TimestampMsClock::now().0);
                let Some(to) = to else {
                    debug!("SyncRequestIn missing 'to' for lane {}", lane_id);
                    return Ok(());
                };
                self.unfold_sync_request_interval(lane_id, from, to, requester)
                    .await?;
            }
        }
        Ok(())
    }

    pub(crate) fn on_consensus_event(&mut self, event: ConsensusEvent) -> Result<()> {
        match event {
            ConsensusEvent::CommitConsensusProposal(cpp) => {
                self.current_slot = cpp.consensus_proposal.slot;
                self.staking = cpp.staking;
                for (lane_id, dp_hash, _size, poda) in cpp.consensus_proposal.cut.iter() {
                    for validator in poda.validators.iter() {
                        self.knowledge.by_dp.insert(
                            (lane_id.clone(), dp_hash.clone(), validator.clone()),
                            EvidenceState::ObservedHas,
                        );
                    }
                }
                self.last_cut = Some(cpp.consensus_proposal.cut);
            }
        }
        Ok(())
    }

    fn on_node_state_event(&mut self, event: NodeStateEvent) -> Result<()> {
        // In LaneManager mode we don't receive consensus events, so use NodeStateEvent
        // to keep staking/cut/slot in sync for peer selection and sync routing.
        if self.conf.p2p.mode != P2pMode::LaneManager {
            return Ok(());
        }

        let NodeStateEvent::NewBlock(block) = event;
        self.current_slot = block.signed_block.consensus_proposal.slot;
        self.last_cut = Some(block.signed_block.consensus_proposal.cut.clone());
        if let Err(err) = self.staking.process_block(&block.staking_data) {
            tracing::error!("Error processing block in dissemination manager: {:?}", err);
        }

        Ok(())
    }

    async fn process_sync_requests_and_replies(&mut self) -> Result<()> {
        self.process_outbound_sync_requests()?;
        self.send_sync_replies()?;
        self.prune_peer_knowledge();
        Ok(())
    }

    fn process_outbound_sync_requests(&mut self) -> Result<()> {
        if self.pending_sync_requests.is_empty() {
            return Ok(());
        }

        let bonded_validators = self.staking.bonded();
        let self_validator = self.crypto.validator_pubkey();
        let peers: Vec<ValidatorPublicKey> = bonded_validators
            .iter()
            .filter(|pubkey| *pubkey != self_validator)
            .cloned()
            .collect();

        if peers.is_empty() {
            return Ok(());
        }

        let now = TimestampMsClock::now();
        let knowledge = &self.knowledge.by_dp;
        let mut completed = Vec::new();
        let mut to_send = Vec::new();

        for (key, request) in self.pending_sync_requests.iter_mut() {
            if self.lanes.contains(&request.lane_id, &request.to) {
                completed.push(key.clone());
                continue;
            }

            if should_throttle_since(&request.state, now.clone()) {
                continue;
            }

            let target = request.select_peer(&peers, knowledge);
            if let Some(target) = target {
                let lane_id = request.lane_id.clone();
                let from = request.from.clone();
                let to = request.to.clone();
                to_send.push((lane_id, from, to, target));
                request.record_sent(now.clone());
            }
        }

        for (lane_id, from, to, target) in to_send {
            self.send_sync_request(lane_id, from.as_ref(), Some(&to), target)?;
        }

        for key in completed {
            self.pending_sync_requests.remove(&key);
        }

        Ok(())
    }

    fn send_sync_request(
        &mut self,
        lane_id: LaneId,
        from: Option<&DataProposalHash>,
        to: Option<&DataProposalHash>,
        validator: ValidatorPublicKey,
    ) -> Result<()> {
        debug!(
            "🔍 Sending SyncRequest to {} for DataProposal from {:?} to {:?}",
            validator, from, to
        );
        self.metrics
            .sync_request_send(&lane_id, self.crypto.validator_pubkey());
        let signed_message = self
            .crypto
            .sign_msg_with_header(MempoolNetMessage::SyncRequest(
                lane_id,
                from.cloned(),
                to.cloned(),
            ))?;
        self.bus
            .send(OutboundMessage::send(validator, signed_message))
            .context("Sending MempoolNetMessage::SyncRequest msg on the bus")?;
        Ok(())
    }

    async fn unfold_sync_request_interval(
        &mut self,
        lane_id: LaneId,
        from: Option<DataProposalHash>,
        to: DataProposalHash,
        requester: ValidatorPublicKey,
    ) -> Result<()> {
        if from.as_ref() == Some(&to) {
            debug!(
                "No need to unfold empty interval for SyncRequest: from: {:?}, to: {}, validator: {}",
                from, to, requester
            );
            return Ok(());
        }

        let mut stream = Box::pin(self.lanes.get_entries_metadata_between_hashes(
            &lane_id,
            from.clone(),
            Some(to.clone()),
        ));

        while let Some(entry) = stream.next().await {
            if let Ok(MetadataOrMissingHash::Metadata(_metadata, dp_hash)) =
                log_warn!(entry, "Getting entry metadata to prepare sync replies")
            {
                self.dp_first_seen_slot
                    .insert((lane_id.clone(), dp_hash.clone()), self.current_slot);
                self.knowledge.by_dp.insert(
                    (lane_id.clone(), dp_hash.clone(), requester.clone()),
                    EvidenceState::DefinitelyDoesNotHave,
                );
            } else {
                tracing::warn!(
                    "Could not get entry metadata to prepare sync replies for SyncRequest: from: {:?}, to: {}, validator: {}",
                    from,
                    to,
                    requester
                );
            }

            if from.is_none() {
                break;
            }
        }

        Ok(())
    }

    fn send_sync_replies(&mut self) -> Result<()> {
        let now = TimestampMsClock::now();
        let mut pending = Vec::new();
        for ((lane_id, dp_hash, peer), state) in self.knowledge.by_dp.iter() {
            if *state == EvidenceState::DefinitelyDoesNotHave {
                pending.push((lane_id.clone(), dp_hash.clone(), peer.clone()));
            }
        }

        for (lane_id, dp_hash, validator) in pending {
            if self.should_debounce_sync_reply(&lane_id, &dp_hash, &validator, now.clone()) {
                self.metrics.mempool_sync_throttled(&lane_id, &validator);
                continue;
            }

            let Some(metadata) = self.lanes.get_metadata_by_hash(&lane_id, &dp_hash)? else {
                continue;
            };

            let mut send_succeeded = false;
            if let Ok(Some(mut data_proposal)) = log_error!(
                self.lanes.get_dp_by_hash(&lane_id, &dp_hash),
                "Getting data proposal to prepare a SyncReply"
            ) {
                if let Ok(Some(proofs)) = log_error!(
                    self.lanes.get_proofs_by_hash(&lane_id, &dp_hash),
                    "Getting proofs to prepare a SyncReply"
                ) {
                    data_proposal.hydrate_proofs(proofs);
                }

                let signed_reply = self
                    .crypto
                    .sign_msg_with_header(MempoolNetMessage::SyncReply(
                        lane_id.clone(),
                        metadata.clone(),
                        data_proposal,
                    ));

                if let Ok(signed_reply) = signed_reply {
                    if log_error!(
                        self.bus
                            .send(OutboundMessage::send(validator.clone(), signed_reply)),
                        "Sending MempoolNetMessage::SyncReply msg on the bus"
                    )
                    .is_ok()
                    {
                        debug!("Sent reply for DP Hash: {} to: {}", &dp_hash, &validator);
                        self.metrics.mempool_sync_processed(&lane_id, &validator);
                        self.record_dp_sent(&lane_id, &dp_hash, &validator);
                        self.mark_weak_has(&lane_id, &dp_hash, &validator);
                        send_succeeded = true;
                    }
                }
            }

            if send_succeeded {
                continue;
            }

            tracing::warn!(
                "Could not send reply for DP Hash: {} to: {}, retrying later.",
                &dp_hash,
                &validator
            );
            self.metrics.mempool_sync_failure(&lane_id, &validator);
        }

        Ok(())
    }

    fn prune_peer_knowledge(&mut self) {
        if self.dp_first_seen_slot.is_empty() {
            return;
        }

        let mut to_drop = Vec::new();
        let bonded = self.staking.bonded();

        for ((lane_id, dp_hash), first_seen) in self.dp_first_seen_slot.iter() {
            if self.current_slot < first_seen.saturating_add(50) {
                continue;
            }

            if self.owned_lanes.contains(lane_id)
                && !self.all_peers_have_weak_has(lane_id, dp_hash, bonded)
            {
                continue;
            }

            to_drop.push((lane_id.clone(), dp_hash.clone()));
        }

        if to_drop.is_empty() {
            return;
        }

        for (lane_id, dp_hash) in to_drop {
            self.dp_first_seen_slot
                .remove(&(lane_id.clone(), dp_hash.clone()));
            self.knowledge
                .by_dp
                .retain(|(l, d, _), _| l != &lane_id || d != &dp_hash);
        }
    }

    fn all_peers_have_weak_has(
        &self,
        lane_id: &LaneId,
        dp_hash: &DataProposalHash,
        bonded: &[ValidatorPublicKey],
    ) -> bool {
        for peer in bonded.iter() {
            if peer == self.crypto.validator_pubkey() {
                continue;
            }
            let key = (lane_id.clone(), dp_hash.clone(), peer.clone());
            match self.knowledge.by_dp.get(&key) {
                Some(EvidenceState::WeakHas)
                | Some(EvidenceState::StrongHas)
                | Some(EvidenceState::ObservedHas) => {}
                _ => return false,
            }
        }
        true
    }

    fn on_poda_updated(
        &mut self,
        lane_id: LaneId,
        data_proposal_hash: DataProposalHash,
        signatures: Vec<ValidatorDAG>,
    ) {
        for signature in signatures.into_iter() {
            let entry = (
                lane_id.clone(),
                data_proposal_hash.clone(),
                signature.signature.validator,
            );
            self.knowledge
                .by_dp
                .insert(entry, EvidenceState::ObservedHas);
        }
    }

    fn send_poda_update(
        &mut self,
        lane_id: LaneId,
        data_proposal_hash: DataProposalHash,
        signatures: Vec<ValidatorDAG>,
    ) -> Result<()> {
        self.send_net_message(MempoolNetMessage::PoDAUpdate(
            lane_id,
            data_proposal_hash,
            signatures,
        ))
    }

    pub(super) fn maybe_disseminate_dp(
        &mut self,
        lane_id: &LaneId,
        dp_hash: &DataProposalHash,
    ) -> Result<()> {
        let Some(metadata) = self.lanes.get_metadata_by_hash(lane_id, dp_hash)? else {
            bail!("Can't find metadata for DP {} in lane {}", dp_hash, lane_id);
        };

        self.rebroadcast_data_proposal(lane_id, dp_hash, &metadata)
            .context("Rebroadcasting data proposal")
            .map(|_| ())
    }

    pub(super) async fn redisseminate_owned_lanes(&mut self) -> Result<()> {
        for lane_id in self.owned_lanes.clone().into_iter() {
            let pending_entries = {
                let mut stream = Box::pin(
                    self.lanes
                        .get_pending_entries_in_lane(&lane_id, self.last_cut.clone()),
                );
                let mut entries = Vec::new();

                while let Some(entry) = stream.next().await {
                    match entry.context("Getting pending entry")? {
                        MetadataOrMissingHash::Metadata(metadata, dp_hash) => {
                            entries.push((metadata, dp_hash));
                        }
                        MetadataOrMissingHash::MissingHash(_) => {
                            break;
                        }
                    }
                }

                entries
            };

            for (metadata, dp_hash) in pending_entries {
                if self.rebroadcast_data_proposal(&lane_id, &dp_hash, &metadata)? {
                    break;
                }
            }
        }
        Ok(())
    }

    fn rebroadcast_data_proposal(
        &mut self,
        lane_id: &LaneId,
        dp_hash: &DataProposalHash,
        entry_metadata: &LaneEntryMetadata,
    ) -> Result<bool> {
        trace!(
            "Rebroadcasting DataProposal {} in lane {}",
            dp_hash,
            lane_id
        );
        let there_are_other_validators = !self.staking.is_bonded(self.crypto.validator_pubkey())
            || self.staking.bonded().len() >= 2;
        let signed_by: HashSet<ValidatorPublicKey> = entry_metadata
            .signatures
            .iter()
            .map(|s| s.signature.validator.clone())
            .collect();
        let bonded_validators = self.staking.bonded();
        let bonded_validators_len = bonded_validators.len();
        let self_validator = self.crypto.validator_pubkey();

        let mut candidate_targets: HashSet<ValidatorPublicKey> =
            if entry_metadata.signatures.len() == 1 && there_are_other_validators {
                bonded_validators.iter().cloned().collect()
            } else {
                bonded_validators
                    .iter()
                    .filter(|pubkey| !signed_by.contains(pubkey))
                    .cloned()
                    .collect()
            };
        candidate_targets.remove(self_validator);

        if candidate_targets.is_empty() {
            return Ok(false);
        }

        let filtered_targets = candidate_targets;
        if filtered_targets.is_empty() {
            return Ok(false);
        }

        let Some(mut data_proposal) = self.lanes.get_dp_by_hash(lane_id, dp_hash)? else {
            bail!("Can't find DataProposal {} in lane {}", dp_hash, lane_id);
        };

        let Some(proofs) = self.lanes.get_proofs_by_hash(lane_id, dp_hash)? else {
            bail!("Can't find Proofs for DP {} in lane {}", dp_hash, lane_id);
        };
        data_proposal.hydrate_proofs(proofs);

        let net_message =
            MempoolNetMessage::DataProposal(lane_id.clone(), data_proposal.hashed(), data_proposal);

        if filtered_targets.len() == bonded_validators_len && there_are_other_validators {
            self.metrics
                .dp_disseminations
                .add(filtered_targets.len() as u64, &[]);
            self.send_net_message(net_message)?;
        } else {
            self.metrics
                .dp_disseminations
                .add(filtered_targets.len() as u64, &[]);
            self.send_net_message_only_for(filtered_targets.clone(), net_message)?;
        }

        for peer in filtered_targets {
            self.record_dp_sent(lane_id, dp_hash, &peer);
            self.mark_weak_has(lane_id, dp_hash, &peer);
        }

        Ok(true)
    }

    fn mark_weak_has(
        &mut self,
        lane_id: &LaneId,
        dp_hash: &DataProposalHash,
        peer: &ValidatorPublicKey,
    ) {
        let key = (lane_id.clone(), dp_hash.clone(), peer.clone());
        match self.knowledge.by_dp.get(&key) {
            Some(EvidenceState::ObservedHas) | Some(EvidenceState::StrongHas) => {}
            _ => {
                self.knowledge.by_dp.insert(key, EvidenceState::WeakHas);
            }
        }
    }

    #[allow(dead_code)]
    fn mark_strong_has(
        &mut self,
        lane_id: &LaneId,
        dp_hash: &DataProposalHash,
        peer: &ValidatorPublicKey,
    ) {
        let key = (lane_id.clone(), dp_hash.clone(), peer.clone());
        match self.knowledge.by_dp.get(&key) {
            Some(EvidenceState::ObservedHas) => {}
            _ => {
                self.knowledge.by_dp.insert(key, EvidenceState::StrongHas);
            }
        }
    }

    fn send_net_message(&mut self, net_message: MempoolNetMessage) -> Result<()> {
        let signed_message = self.crypto.sign_msg_with_header(net_message)?;
        self.bus
            .send(OutboundMessage::broadcast(signed_message))
            .context("Broadcasting mempool message")?;
        Ok(())
    }

    fn send_net_message_only_for(
        &mut self,
        only_for: HashSet<ValidatorPublicKey>,
        net_message: MempoolNetMessage,
    ) -> Result<()> {
        let signed_message = self.crypto.sign_msg_with_header(net_message)?;
        self.bus
            .send(OutboundMessage::broadcast_only_for(
                only_for,
                signed_message,
            ))
            .context("Broadcasting mempool message only for targets")?;
        Ok(())
    }

    fn record_dp_sent(
        &mut self,
        lane_id: &LaneId,
        dp_hash: &DataProposalHash,
        peer: &ValidatorPublicKey,
    ) {
        let now = TimestampMsClock::now();
        self.knowledge
            .by_peer
            .entry(peer.clone())
            .or_default()
            .last_dp_sent
            .insert((lane_id.clone(), dp_hash.clone()), now);
    }

    fn should_debounce_sync_reply(
        &self,
        lane_id: &LaneId,
        dp_hash: &DataProposalHash,
        peer: &ValidatorPublicKey,
        now: TimestampMs,
    ) -> bool {
        self.knowledge
            .by_peer
            .get(peer)
            .and_then(|state| state.last_dp_sent.get(&(lane_id.clone(), dp_hash.clone())))
            .is_some_and(|last_sent| now.clone() - last_sent.clone() < SYNC_REPLY_DEBOUNCE)
    }

    #[cfg(test)]
    pub(super) fn add_owned_lane(&mut self, lane_id: LaneId) {
        self.owned_lanes.insert(lane_id);
    }

    #[cfg(test)]
    pub(crate) async fn process_sync_requests_and_replies_for_test(&mut self) -> Result<()> {
        self.process_sync_requests_and_replies().await
    }

    #[cfg(test)]
    pub(crate) fn set_request_backoff_for_test(
        &mut self,
        lane_id: LaneId,
        dp_hash: DataProposalHash,
        last_sent: TimestampMs,
        backoff: Duration,
        next_peer_idx: usize,
    ) {
        let key = (lane_id.clone(), dp_hash.clone());
        let request = self
            .pending_sync_requests
            .entry(key)
            .or_insert(PendingSyncRequest {
                lane_id,
                from: None,
                to: dp_hash,
                state: ThrottleState {
                    last_sent: last_sent.clone(),
                    backoff,
                },
                next_peer_idx,
            });
        request.state.last_sent = last_sent;
        request.state.backoff = backoff;
        request.next_peer_idx = next_peer_idx;
    }
}

fn should_throttle_since(state: &ThrottleState, now: TimestampMs) -> bool {
    now - state.last_sent.clone() < state.backoff
}

fn next_backoff(prev: Option<Duration>) -> Duration {
    match prev {
        None => REQUEST_BACKOFF_INITIAL,
        Some(current) => {
            let doubled_ms = current.as_millis().saturating_mul(2);
            let capped_ms = doubled_ms.min(REQUEST_BACKOFF_MAX.as_millis());
            Duration::from_millis(capped_ms as u64)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test_log::test(tokio::test)]
    async fn throttle_helper_respects_window() {
        let now = TimestampMs(10_000);
        let state_initial = ThrottleState {
            last_sent: now.clone() - (REQUEST_BACKOFF_INITIAL - Duration::from_millis(1)),
            backoff: REQUEST_BACKOFF_INITIAL,
        };
        assert!(
            should_throttle_since(&state_initial, now.clone()),
            "should throttle when last send is within initial window"
        );

        let state_outside = ThrottleState {
            last_sent: now.clone() - (REQUEST_BACKOFF_INITIAL + Duration::from_millis(1)),
            backoff: REQUEST_BACKOFF_INITIAL,
        };
        assert!(
            !should_throttle_since(&state_outside, now.clone()),
            "should not throttle when outside initial window"
        );

        let state_custom_backoff = ThrottleState {
            last_sent: now.clone() - Duration::from_secs(3),
            backoff: Duration::from_secs(4),
        };
        assert!(
            should_throttle_since(&state_custom_backoff, now.clone()),
            "should throttle when within custom backoff"
        );

        let state_custom_backoff_ok = ThrottleState {
            last_sent: now.clone() - Duration::from_secs(5),
            backoff: Duration::from_secs(4),
        };
        assert!(
            !should_throttle_since(&state_custom_backoff_ok, now),
            "should not throttle when past custom backoff"
        );
    }

    #[test_log::test(tokio::test)]
    async fn exponential_backoff_doubles_and_caps() {
        let mut backoff = next_backoff(None);
        assert_eq!(backoff, REQUEST_BACKOFF_INITIAL);

        backoff = next_backoff(Some(backoff));
        assert_eq!(backoff, Duration::from_secs(2));

        backoff = next_backoff(Some(backoff));
        assert_eq!(backoff, Duration::from_secs(4));

        backoff = next_backoff(Some(backoff));
        assert_eq!(backoff, REQUEST_BACKOFF_MAX);

        backoff = next_backoff(Some(backoff));
        assert_eq!(backoff, REQUEST_BACKOFF_MAX);
    }
}

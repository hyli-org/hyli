use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

use anyhow::{bail, Context, Result};
use futures::StreamExt;
use hyli_model::{utils::TimestampMs, DataProposalHash, LaneBytesSize, LaneId, ValidatorPublicKey};
use hyli_modules::{log_error, log_warn, module_bus_client, module_handle_messages, modules::Module};
use hyli_net::clock::TimestampMsClock;
use staking::state::Staking;
use tracing::{debug, trace};

use crate::{
    bus::BusClientSender,
    model::{Cut, Hashed},
    p2p::network::{HeaderSigner, OutboundMessage},
    utils::conf::SharedConf,
};

use super::{
    metrics::MempoolMetrics,
    storage::{LaneEntryMetadata, MetadataOrMissingHash, Storage},
    storage_fjall::{shared_lanes_storage, LanesStorage},
    MempoolNetMessage, ValidatorDAG,
};

use crate::model::SharedRunContext;

const REPLY_BACKOFF_INITIAL: Duration = Duration::from_secs(1);
const REPLY_BACKOFF_MAX: Duration = Duration::from_secs(8);
const REPLY_DISPATCH_INTERVAL: Duration = Duration::from_millis(400);
const MAX_SYNC_INFLIGHT_PER_PEER: usize = 256;

#[derive(Debug, Clone)]
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
    StakingUpdated {
        staking: Staking,
    },
    CcpCommitted {
        cut: Cut,
        previous_cut: Option<Cut>,
    },
}

impl hyli_modules::bus::BusMessage for DisseminationEvent {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvidenceState {
    DefinitelyHas,
    StrongHas,
    WeakHas,
    DefinitelyDoesNotHave,
    Unknown,
}

#[derive(Debug, Clone, Default)]
pub struct PeerState {
    pub last_seen: Option<u128>,
    // Placeholder for per-peer inflight counters (UL/DL) in a later phase.
}

#[derive(Debug, Default)]
struct PeerKnowledge {
    by_peer: HashMap<ValidatorPublicKey, PeerState>,
    by_dp: HashMap<(LaneId, DataProposalHash, ValidatorPublicKey), EvidenceState>,
}

#[derive(Debug, Default)]
struct InflightTracker {
    by_peer: HashMap<ValidatorPublicKey, usize>,
    by_dp: HashMap<(LaneId, DataProposalHash), HashSet<ValidatorPublicKey>>,
}

#[derive(Debug, Clone)]
struct PendingSyncRequest {
    lane_id: LaneId,
    from: Option<DataProposalHash>,
    to: DataProposalHash,
    requester: ValidatorPublicKey,
}

#[derive(Clone)]
struct ThrottleState {
    last_sent: TimestampMs,
    backoff: Duration,
}

pub struct DisseminationManager {
    bus: DisseminationBusClient,
    _conf: SharedConf,
    crypto: hyli_crypto::SharedBlstCrypto,
    metrics: MempoolMetrics,
    lanes: LanesStorage,
    knowledge: PeerKnowledge,
    inflight: InflightTracker,
    sync_backoff:
        HashMap<ValidatorPublicKey, HashMap<(LaneId, DataProposalHash), ThrottleState>>,
    sync_todo: HashMap<(LaneId, DataProposalHash), (LaneEntryMetadata, HashSet<ValidatorPublicKey>)>,
    pending_sync_requests: Vec<PendingSyncRequest>,
    owned_lanes: HashSet<LaneId>,
    last_cut: Option<Cut>,
    staking: Staking,
}

module_bus_client! {
#[derive(Debug)]
struct DisseminationBusClient {
    sender(OutboundMessage),
    receiver(DisseminationEvent),
}
}

impl Module for DisseminationManager {
    type Context = SharedRunContext;

    async fn build(bus: hyli_modules::bus::SharedMessageBus, ctx: Self::Context) -> Result<Self> {
        let bus = DisseminationBusClient::new_from_bus(bus.new_handle()).await;
        let lanes = shared_lanes_storage(&ctx.config.data_directory)?;

        Ok(DisseminationManager {
            bus,
            _conf: ctx.config.clone(),
            crypto: ctx.crypto.clone(),
            metrics: MempoolMetrics::global(ctx.config.id.clone()),
            lanes,
            knowledge: PeerKnowledge::default(),
            inflight: InflightTracker::default(),
            sync_backoff: HashMap::new(),
            sync_todo: HashMap::new(),
            pending_sync_requests: Vec::new(),
            owned_lanes: HashSet::new(),
            last_cut: None,
            staking: Staking::default(),
        })
    }

    async fn run(&mut self) -> Result<()> {
        let mut disseminate_timer = tokio::time::interval(std::time::Duration::from_secs(15));
        disseminate_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut reply_timer = tokio::time::interval(REPLY_DISPATCH_INTERVAL);
        reply_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        module_handle_messages! {
            on_self self,
            listen<DisseminationEvent> evt => {
                _ = log_error!(self.on_event(evt).await, "Handling DisseminationEvent");
            }
            _ = disseminate_timer.tick() => {
                _ = log_error!(self.redisseminate_owned_lanes().await, "Disseminate data proposals on tick");
                disseminate_timer.reset();
            }
            _ = reply_timer.tick() => {
                _ = log_error!(self.process_sync_requests_and_replies().await, "Processing sync requests/replies");
                reply_timer.reset();
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
                self.knowledge
                    .by_dp
                    .insert(
                        (lane_id.clone(), data_proposal_hash.clone(), voter.clone()),
                        EvidenceState::DefinitelyHas,
                    );
                self.clear_inflight_target(&lane_id, &data_proposal_hash, &voter);
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
                self.pending_sync_requests.push(PendingSyncRequest {
                    lane_id,
                    from,
                    to,
                    requester,
                });
            }
            DisseminationEvent::StakingUpdated { staking } => {
                self.staking = staking;
            }
            DisseminationEvent::CcpCommitted { cut, .. } => {
                self.last_cut = Some(cut.clone());
                self.on_ccp_committed(&cut);
            }
        }

        Ok(())
    }

    async fn process_sync_requests_and_replies(&mut self) -> Result<()> {
        self.process_pending_sync_requests().await?;
        self.send_sync_replies()
    }

    async fn process_pending_sync_requests(&mut self) -> Result<()> {
        if self.pending_sync_requests.is_empty() {
            return Ok(());
        }

        let mut pending = Vec::new();
        std::mem::swap(&mut pending, &mut self.pending_sync_requests);

        for request in pending {
            self.unfold_sync_request_interval(request).await?;
        }

        Ok(())
    }

    async fn unfold_sync_request_interval(
        &mut self,
        request: PendingSyncRequest,
    ) -> Result<()> {
        let PendingSyncRequest {
            lane_id,
            from,
            to,
            requester,
        } = request;

        if from.as_ref() == Some(&to) {
            debug!(
                "No need to unfold empty interval for SyncRequest: from: {:?}, to: {}, validator: {}",
                from, to, requester
            );
            return Ok(());
        }

        let mut stream = Box::pin(
            self.lanes
                .get_entries_metadata_between_hashes(&lane_id, from.clone(), Some(to.clone())),
        );

        while let Some(entry) = stream.next().await {
            if let Ok(MetadataOrMissingHash::Metadata(metadata, dp_hash)) =
                log_warn!(entry, "Getting entry metadata to prepare sync replies")
            {
                self.sync_todo
                    .entry((lane_id.clone(), dp_hash))
                    .or_insert((metadata, Default::default()))
                    .1
                    .insert(requester.clone());
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
        if self.sync_todo.is_empty() {
            return Ok(());
        }

        let mut todo = HashMap::new();
        std::mem::swap(&mut self.sync_todo, &mut todo);

        for ((lane_id, dp_hash), (metadata, validators)) in todo.into_iter() {
            for validator in validators.into_iter() {
                if self.should_backoff(&lane_id, &validator, &dp_hash) {
                    debug!(
                        "Backoff sync reply for DP Hash: {} to: {}",
                        &dp_hash, &validator
                    );
                    self.metrics
                        .mempool_sync_throttled(&lane_id, &validator);
                    self.sync_todo
                        .entry((lane_id.clone(), dp_hash.clone()))
                        .or_insert((metadata.clone(), Default::default()))
                        .1
                        .insert(validator);
                    continue;
                }

                let inflight = self.inflight.by_peer.get(&validator).copied().unwrap_or(0);
                if inflight >= MAX_SYNC_INFLIGHT_PER_PEER {
                    debug!(
                        "Skipping sync reply for DP Hash: {} to {} (inflight {})",
                        &dp_hash, &validator, inflight
                    );
                    self.metrics
                        .mempool_sync_throttled(&lane_id, &validator);
                    self.sync_todo
                        .entry((lane_id.clone(), dp_hash.clone()))
                        .or_insert((metadata.clone(), Default::default()))
                        .1
                        .insert(validator);
                    continue;
                }

                self.increment_inflight(&validator);
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
                            self.metrics
                                .mempool_sync_processed(&lane_id, &validator);
                            self.clear_backoff(&lane_id, &validator, &dp_hash);
                            self.mark_strong_has(&lane_id, &dp_hash, &validator);
                            send_succeeded = true;
                        }
                    }
                }

                self.decrement_inflight(&validator);

                if send_succeeded {
                    continue;
                }

                tracing::warn!(
                    "Could not send reply for DP Hash: {} to: {}, retrying later.",
                    &dp_hash,
                    &validator
                );
                self.metrics.mempool_sync_failure(&lane_id, &validator);
                self.record_failure(&lane_id, &validator, &dp_hash);

                self.sync_todo
                    .entry((lane_id.clone(), dp_hash.clone()))
                    .or_insert((metadata.clone(), Default::default()))
                    .1
                    .insert(validator);
            }
        }

        Ok(())
    }

    fn should_backoff(
        &self,
        lane_id: &LaneId,
        validator: &ValidatorPublicKey,
        data_proposal_hash: &DataProposalHash,
    ) -> bool {
        let Some(data_proposal_record) = self
            .sync_backoff
            .get(validator)
            .and_then(|validator_records| {
                validator_records.get(&(lane_id.clone(), data_proposal_hash.clone()))
            })
        else {
            return false;
        };

        should_throttle_since(data_proposal_record, TimestampMsClock::now())
    }

    fn clear_backoff(
        &mut self,
        lane_id: &LaneId,
        validator: &ValidatorPublicKey,
        dp_hash: &DataProposalHash,
    ) {
        if let Some(entries) = self.sync_backoff.get_mut(validator) {
            entries.remove(&(lane_id.clone(), dp_hash.clone()));
            if entries.is_empty() {
                self.sync_backoff.remove(validator);
            }
        }
    }

    fn record_failure(
        &mut self,
        lane_id: &LaneId,
        validator: &ValidatorPublicKey,
        dp_hash: &DataProposalHash,
    ) {
        let now = TimestampMsClock::now();
        let prev_backoff = self
            .sync_backoff
            .get(validator)
            .and_then(|m| m.get(&(lane_id.clone(), dp_hash.clone())))
            .map(|s| s.backoff);
        let backoff = next_backoff(prev_backoff);
        self.sync_backoff
            .entry(validator.clone())
            .or_default()
            .insert(
                (lane_id.clone(), dp_hash.clone()),
                ThrottleState {
                    last_sent: now,
                    backoff,
                },
            );
    }

    fn mark_strong_has(
        &mut self,
        lane_id: &LaneId,
        dp_hash: &DataProposalHash,
        peer: &ValidatorPublicKey,
    ) {
        let key = (lane_id.clone(), dp_hash.clone(), peer.clone());
        self.knowledge.by_dp.insert(key, EvidenceState::StrongHas);
    }

    fn increment_inflight(&mut self, peer: &ValidatorPublicKey) {
        let counter = self.inflight.by_peer.entry(peer.clone()).or_insert(0);
        *counter += 1;
    }

    fn decrement_inflight(&mut self, peer: &ValidatorPublicKey) {
        if let Some(inflight) = self.inflight.by_peer.get_mut(peer) {
            *inflight = inflight.saturating_sub(1);
            if *inflight == 0 {
                self.inflight.by_peer.remove(peer);
            }
        }
    }
    fn on_poda_updated(
        &mut self,
        lane_id: LaneId,
        data_proposal_hash: DataProposalHash,
        signatures: Vec<ValidatorDAG>,
    ) {
        for signature in signatures.into_iter() {
            self.clear_inflight_target(
                &lane_id,
                &data_proposal_hash,
                &signature.signature.validator,
            );
            let entry = (
                lane_id.clone(),
                data_proposal_hash.clone(),
                signature.signature.validator,
            );
            self.knowledge
                .by_dp
                .insert(entry, EvidenceState::DefinitelyHas);
        }
    }

    fn on_ccp_committed(&mut self, cut: &Cut) {
        for (lane_id, dp_hash, _size, poda) in cut.iter() {
            for validator in poda.validators.iter() {
                self.clear_inflight_target(lane_id, dp_hash, validator);
                self.knowledge
                    .by_dp
                    .insert(
                        (lane_id.clone(), dp_hash.clone(), validator.clone()),
                        EvidenceState::DefinitelyHas,
                    );
            }
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
        const MAX_INFLIGHT_PER_PEER: usize = 256;
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

        let filtered_targets = self.filter_targets_for_inflight(
            lane_id,
            dp_hash,
            candidate_targets,
            MAX_INFLIGHT_PER_PEER,
        );
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

        let net_message = MempoolNetMessage::DataProposal(
            lane_id.clone(),
            data_proposal.hashed(),
            data_proposal,
        );

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

        self.record_inflight_targets(lane_id, dp_hash, filtered_targets);

        Ok(true)
    }

    fn filter_targets_for_inflight(
        &self,
        lane_id: &LaneId,
        dp_hash: &DataProposalHash,
        candidates: HashSet<ValidatorPublicKey>,
        max_inflight_per_peer: usize,
    ) -> HashSet<ValidatorPublicKey> {
        let existing = self.inflight.by_dp.get(&(lane_id.clone(), dp_hash.clone()));
        candidates
            .into_iter()
            .filter(|peer| {
                if existing.map_or(false, |set| set.contains(peer)) {
                    return true;
                }
                let inflight = self.inflight.by_peer.get(peer).copied().unwrap_or(0);
                inflight < max_inflight_per_peer
            })
            .collect()
    }

    fn record_inflight_targets(
        &mut self,
        lane_id: &LaneId,
        dp_hash: &DataProposalHash,
        targets: HashSet<ValidatorPublicKey>,
    ) {
        let mut peers = Vec::with_capacity(targets.len());
        {
            let entry = self
                .inflight
                .by_dp
                .entry((lane_id.clone(), dp_hash.clone()))
                .or_default();

            for peer in targets.into_iter() {
                if entry.insert(peer.clone()) {
                    *self.inflight.by_peer.entry(peer.clone()).or_insert(0) += 1;
                }
                peers.push(peer);
            }
        }

        for peer in peers {
            self.mark_weak_has(lane_id, dp_hash, &peer);
        }
    }

    fn mark_weak_has(
        &mut self,
        lane_id: &LaneId,
        dp_hash: &DataProposalHash,
        peer: &ValidatorPublicKey,
    ) {
        let key = (lane_id.clone(), dp_hash.clone(), peer.clone());
        match self.knowledge.by_dp.get(&key) {
            Some(EvidenceState::DefinitelyHas) | Some(EvidenceState::StrongHas) => {}
            _ => {
                self.knowledge.by_dp.insert(key, EvidenceState::WeakHas);
            }
        }
    }

    fn clear_inflight_target(
        &mut self,
        lane_id: &LaneId,
        dp_hash: &DataProposalHash,
        peer: &ValidatorPublicKey,
    ) {
        let Some(entry) = self
            .inflight
            .by_dp
            .get_mut(&(lane_id.clone(), dp_hash.clone()))
        else {
            return;
        };

        if entry.remove(peer) {
            if let Some(inflight) = self.inflight.by_peer.get_mut(peer) {
                *inflight = inflight.saturating_sub(1);
                if *inflight == 0 {
                    self.inflight.by_peer.remove(peer);
                }
            }
        }

        if entry.is_empty() {
            self.inflight
                .by_dp
                .remove(&(lane_id.clone(), dp_hash.clone()));
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

    #[cfg(test)]
    pub(super) fn add_owned_lane(&mut self, lane_id: LaneId) {
        self.owned_lanes.insert(lane_id);
    }

    #[cfg(test)]
    pub(crate) async fn process_sync_requests_and_replies_for_test(&mut self) -> Result<()> {
        self.process_sync_requests_and_replies().await
    }

    #[cfg(test)]
    pub(crate) fn set_sync_backoff_for_test(
        &mut self,
        lane_id: LaneId,
        validator: ValidatorPublicKey,
        dp_hash: DataProposalHash,
        last_sent: TimestampMs,
        backoff: Duration,
    ) {
        self.sync_backoff
            .entry(validator)
            .or_default()
            .insert(
                (lane_id, dp_hash),
                ThrottleState {
                    last_sent,
                    backoff,
                },
            );
    }
}


fn should_throttle_since(state: &ThrottleState, now: TimestampMs) -> bool {
    now - state.last_sent.clone() < state.backoff
}

fn next_backoff(prev: Option<Duration>) -> Duration {
    match prev {
        None => REPLY_BACKOFF_INITIAL,
        Some(current) => {
            let doubled_ms = current.as_millis().saturating_mul(2);
            let capped_ms = doubled_ms.min(REPLY_BACKOFF_MAX.as_millis());
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
            last_sent: now.clone() - (REPLY_BACKOFF_INITIAL - Duration::from_millis(1)),
            backoff: REPLY_BACKOFF_INITIAL,
        };
        assert!(
            should_throttle_since(&state_initial, now.clone()),
            "should throttle when last send is within initial window"
        );

        let state_outside = ThrottleState {
            last_sent: now.clone() - (REPLY_BACKOFF_INITIAL + Duration::from_millis(1)),
            backoff: REPLY_BACKOFF_INITIAL,
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
        assert_eq!(backoff, REPLY_BACKOFF_INITIAL);

        backoff = next_backoff(Some(backoff));
        assert_eq!(backoff, Duration::from_secs(2));

        backoff = next_backoff(Some(backoff));
        assert_eq!(backoff, Duration::from_secs(4));

        backoff = next_backoff(Some(backoff));
        assert_eq!(backoff, REPLY_BACKOFF_MAX);

        backoff = next_backoff(Some(backoff));
        assert_eq!(backoff, REPLY_BACKOFF_MAX);
    }
}

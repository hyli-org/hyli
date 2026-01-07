//! Dissemination manager: routes data proposals, tracks peer knowledge, and schedules sync/reply.
//! It consumes events from mempool/consensus and emits outbound network messages.

use std::collections::{HashMap, HashSet};

use anyhow::{bail, Context, Result};
use futures::StreamExt;
use hyli_model::{DataProposalHash, LaneBytesSize, LaneId, ValidatorPublicKey};
use hyli_modules::{log_error, module_bus_client, module_handle_messages, modules::Module};
use staking::state::Staking;
use strum_macros::IntoStaticStr;
use tracing::{debug, trace};

use crate::{
    bus::BusClientSender,
    consensus::ConsensusEvent,
    model::{Cut, Hashed},
    p2p::network::{HeaderSigner, OutboundMessage},
    utils::conf::SharedConf,
};

use super::{
    metrics::MempoolMetrics,
    shared_lanes_storage,
    storage::{LaneEntryMetadata, MetadataOrMissingHash, Storage},
    LanesStorage, MempoolNetMessage, ValidatorDAG,
};

use crate::model::SharedRunContext;

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

pub struct DisseminationManager {
    bus: DisseminationBusClient,
    _conf: SharedConf,
    crypto: hyli_crypto::SharedBlstCrypto,
    metrics: MempoolMetrics,
    lanes: LanesStorage,
    knowledge: PeerKnowledge,
    owned_lanes: HashSet<LaneId>,
    last_cut: Option<Cut>,
    staking: Staking,
}

module_bus_client! {
#[derive(Debug)]
struct DisseminationBusClient {
    sender(OutboundMessage),
    receiver(DisseminationEvent),
    receiver(ConsensusEvent),
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
            owned_lanes: HashSet::new(),
            last_cut: None,
            staking: Staking::default(),
        })
    }

    async fn run(&mut self) -> Result<()> {
        let mut disseminate_timer = tokio::time::interval(std::time::Duration::from_secs(15));
        disseminate_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        module_handle_messages! {
            on_self self,
            listen<DisseminationEvent> evt => {
                _ = log_error!(self.on_event(evt), "Handling DisseminationEvent");
            }
            listen<ConsensusEvent> evt => {
                _ = log_error!(self.on_consensus_event(evt), "Handling ConsensusEvent in dissemination");
            }
            _ = disseminate_timer.tick() => {
                _ = log_error!(self.redisseminate_owned_lanes().await, "Disseminate data proposals on tick");
            }
        };

        Ok(())
    }
}

impl DisseminationManager {
    pub(crate) fn on_event(&mut self, event: DisseminationEvent) -> Result<()> {
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
                    .entry(requester)
                    .or_default()
                    .last_seen = Some(hyli_net::clock::TimestampMsClock::now().0);
            }
        }

        Ok(())
    }

    pub(crate) fn on_consensus_event(&mut self, event: ConsensusEvent) -> Result<()> {
        match event {
            ConsensusEvent::CommitConsensusProposal(cpp) => {
                self.staking = cpp.staking;
                self.last_cut = Some(cpp.consensus_proposal.cut);
            }
        }
        Ok(())
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
                .insert(entry, EvidenceState::DefinitelyHas);
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

        let targets = if entry_metadata.signatures.len() == 1 && there_are_other_validators {
            None
        } else {
            let only_for: HashSet<ValidatorPublicKey> = bonded_validators
                .iter()
                .filter(|pubkey| !signed_by.contains(pubkey))
                .cloned()
                .collect();

            if only_for.is_empty() {
                return Ok(false);
            }
            Some(only_for)
        };

        let Some(mut data_proposal) = self.lanes.get_dp_by_hash(lane_id, dp_hash)? else {
            bail!("Can't find DataProposal {} in lane {}", dp_hash, lane_id);
        };

        let Some(proofs) = self.lanes.get_proofs_by_hash(lane_id, dp_hash)? else {
            bail!("Can't find Proofs for DP {} in lane {}", dp_hash, lane_id);
        };
        data_proposal.hydrate_proofs(proofs);

        match targets {
            None => {
                self.metrics
                    .dp_disseminations
                    .add(bonded_validators.len() as u64, &[]);
                self.send_net_message(MempoolNetMessage::DataProposal(
                    lane_id.clone(),
                    data_proposal.hashed(),
                    data_proposal,
                ))?;
            }
            Some(only_for) => {
                self.metrics
                    .dp_disseminations
                    .add(only_for.len() as u64, &[]);
                self.send_net_message_only_for(
                    only_for,
                    MempoolNetMessage::DataProposal(
                        lane_id.clone(),
                        data_proposal.hashed(),
                        data_proposal,
                    ),
                )?;
            }
        }

        Ok(true)
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
}

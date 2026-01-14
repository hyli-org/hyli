//! Handles all consensus logic up to block commitment.

use crate::bus::BusClientSender;
use crate::model::*;
use crate::p2p::network::HeaderSigner;
use crate::{
    bus::command_response::Query,
    genesis::GenesisEvent,
    mempool::QueryNewCut,
    model::{Cut, Hashed, ValidatorPublicKey},
    p2p::{
        network::{MsgWithHeader, OutboundMessage},
        P2PCommand,
    },
    utils::conf::SharedConf,
};
use anyhow::{anyhow, bail, Context, Error, Result};
use borsh::{BorshDeserialize, BorshSerialize};
use hyli_crypto::BlstCrypto;
use hyli_crypto::SharedBlstCrypto;
use hyli_model::utils::TimestampMs;
use hyli_modules::bus::BusMessage;
use hyli_modules::info_span_ctx;
use hyli_modules::modules::admin::{
    QueryConsensusCatchupStore, QueryConsensusCatchupStoreResponse,
};
use hyli_modules::{log_error, module_bus_client, module_handle_messages, modules::Module};
use hyli_net::clock::TimestampMsClock;
use metrics::{ConsensusMetrics, ConsensusStateMetric};
use role_follower::FollowerState;
use role_leader::LeaderState;
use role_timeout::TimeoutRoleState;
use serde::{Deserialize, Serialize};
use staking::state::{Staking, MIN_STAKE};
use std::ops::Deref;
use std::ops::DerefMut;
use std::time::Duration;
use std::{collections::HashMap, collections::VecDeque, default::Default, path::PathBuf};
use tokio::time::interval;
use tracing::{debug, info, trace};

pub mod api;
pub mod metrics;
pub mod module;
mod network;
pub mod role_follower;
pub mod role_leader;
pub mod role_sync;
pub mod role_timeout;

pub use network::*;

// -----------------------------
// ------ Consensus bus --------
// -----------------------------

#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum ConsensusCommand {
    TimeoutTick,
    StartNewSlot(Option<TimestampMs>), // If none, may delay, if some, may delay up to that timestamp
}

#[derive(Debug, Clone, Deserialize, Serialize, BorshSerialize, BorshDeserialize)]
pub struct CommittedConsensusProposal {
    pub staking: Staking,
    pub consensus_proposal: ConsensusProposal,
    // NB: this can be different for different validators
    pub certificate: AggregateSignature,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum ConsensusEvent {
    CommitConsensusProposal(CommittedConsensusProposal),
}

#[derive(Clone)]
pub struct QueryConsensusInfo {}

#[derive(Clone)]
pub struct QueryConsensusStakingState {}

impl BusMessage for ConsensusEvent {}
impl BusMessage for ConsensusCommand {}
impl BusMessage for ConsensusNetMessage {}

module_bus_client! {
struct ConsensusBusClient {
sender(OutboundMessage),
sender(ConsensusEvent),
sender(ConsensusCommand),
sender(P2PCommand),
sender(Query<QueryNewCut, Cut>),
receiver(ConsensusCommand),
receiver(GenesisEvent),
receiver(NodeStateEvent),
receiver(MsgWithHeader<ConsensusNetMessage>),
receiver(Query<QueryConsensusInfo, ConsensusInfo>),
receiver(Query<QueryConsensusStakingState, Staking>),
receiver(Query<QueryConsensusCatchupStore, QueryConsensusCatchupStoreResponse>),
}
}

#[derive(BorshSerialize, BorshDeserialize, Default)]
pub struct BFTRoundState {
    // This should always reflect safe, trusted values
    staking: Staking,
    slot: Slot,
    view: View,
    // TODO: should we just store parent proposal
    parent_hash: ConsensusProposalHash,
    parent_timestamp: TimestampMs,
    parent_cut: Cut,

    current_proposal: Option<ConsensusProposal>,

    leader: LeaderState,
    follower: FollowerState,
    timeout: TimeoutRoleState,
    joining: JoiningState,
    genesis: GenesisState,
    state_tag: StateTag,
}

#[derive(BorshSerialize, BorshDeserialize, Default, Debug, Clone, Copy, PartialEq, Eq)]
enum StateTag {
    #[default]
    Joining,
    Leader,
    Follower,
}

#[derive(BorshSerialize, BorshDeserialize, Default)]
pub struct JoiningState {
    staking_updated_to: Slot,
}
#[derive(BorshSerialize, BorshDeserialize, Default)]
pub struct GenesisState {
    peer_pubkey: HashMap<String, ValidatorPublicKey>,
}

#[derive(BorshSerialize, BorshDeserialize, Default)]
pub struct ConsensusStore {
    bft_round_state: BFTRoundState,
    /// Validators that asked to be part of consensus
    validator_candidates: Vec<SignedByValidator<ValidatorCandidacy>>,
    recent_timeout_certificates: VecDeque<TimeoutCertificateCacheEntry>,
}

pub struct Consensus {
    metrics: ConsensusMetrics,
    bus: ConsensusBusClient,
    file: Option<PathBuf>,
    store: ConsensusStore,
    #[allow(dead_code)]
    config: SharedConf,
    crypto: SharedBlstCrypto,
}

#[derive(BorshSerialize, BorshDeserialize, Clone, Debug)]
struct TimeoutCertificateCacheEntry {
    slot: Slot,
    view: View,
    timeout_qc: TimeoutQC,
    tc_kind: TCKind,
}

impl Deref for Consensus {
    type Target = ConsensusStore;
    fn deref(&self) -> &Self::Target {
        &self.store
    }
}
impl DerefMut for Consensus {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.store
    }
}

macro_rules! with_metric {
    ($metrics:expr, $name:literal, $t:expr) => {
        {
            let ret = $t;
            paste::paste!(
            match &ret {
                Ok(_) => { $metrics.[<$name _ok>].add(1, &[]); }
                Err(_) => { $metrics.[<$name _err>].add(1, &[]); }
            }
            );
            ret
        }
    };
}

// Convenience macros for shorter lines, as macros to avoid borrow-checker issues from methods.
macro_rules! current_proposal {
    ($self:ident) => {
        $self.store.bft_round_state.current_proposal.as_ref()
    };
}
pub(crate) use current_proposal;

impl Consensus {
    fn set_state_tag(&mut self, state_tag: StateTag) {
        self.bft_round_state.state_tag = state_tag;
        self.record_consensus_state_metric();
    }

    fn record_consensus_state_metric(&self) {
        let state = if self.is_in_timeout_phase() {
            ConsensusStateMetric::Timeout
        } else {
            match self.bft_round_state.state_tag {
                StateTag::Joining => ConsensusStateMetric::Joining,
                StateTag::Leader => ConsensusStateMetric::Leader,
                StateTag::Follower => ConsensusStateMetric::Follower,
            }
        };

        self.metrics.set_state(state);
    }

    fn is_in_timeout_phase(&self) -> bool {
        self.bft_round_state.timeout.requests.iter().any(|r| {
            r.0.msg.0 == self.bft_round_state.slot
                && r.0.msg.1 == self.bft_round_state.view
                && &r.0.signature.validator == self.crypto.validator_pubkey()
        })
    }

    fn cache_timeout_certificate(
        &mut self,
        slot: Slot,
        view: View,
        timeout_qc: TimeoutQC,
        tc_kind: TCKind,
    ) {
        let cache_size = self.config.consensus.timeout_certificate_cache_size;
        if cache_size == 0 {
            return;
        }
        self.store
            .recent_timeout_certificates
            .retain(|entry| !(entry.slot == slot && entry.view == view));
        self.store
            .recent_timeout_certificates
            .push_back(TimeoutCertificateCacheEntry {
                slot,
                view,
                timeout_qc,
                tc_kind,
            });
        while self.store.recent_timeout_certificates.len() > cache_size {
            self.store.recent_timeout_certificates.pop_front();
        }
    }

    fn cached_timeout_certificate(&self, slot: Slot, view: View) -> Option<(TimeoutQC, TCKind)> {
        self.store
            .recent_timeout_certificates
            .iter()
            .rev()
            .find_map(|entry| {
                if entry.slot == slot && entry.view == view {
                    Some((entry.timeout_qc.clone(), entry.tc_kind.clone()))
                } else {
                    None
                }
            })
    }

    fn round_leader(&self) -> Result<ValidatorPublicKey> {
        // Find out who the next leader will be.
        // For now, this is round-robin on slot + view for simplicity.
        // (we remove 1 for backwards compatibility of the tests when making the change)
        let index = (self.bft_round_state.slot as usize + self.bft_round_state.view as usize)
            .wrapping_sub(1)
            % self.bft_round_state.staking.bonded().len();

        Ok(self
            .bft_round_state
            .staking
            .bonded()
            .get(index)
            .context("No next leader found")?
            .clone())
    }
    fn next_view_leader(&mut self) -> Result<ValidatorPublicKey> {
        self.bft_round_state.view += 1;
        let res = self.round_leader();
        self.bft_round_state.view -= 1;
        res
    }

    /// Update bft_round_state for the next round of consensus.
    fn apply_ticket(&mut self, ticket: Ticket) -> Result<(), Error> {
        match self.bft_round_state.state_tag {
            StateTag::Follower => {}
            StateTag::Leader => {}
            _ => bail!("Cannot finish_round unless synchronized to the consensus."),
        }

        // Reset some state
        self.bft_round_state.leader = LeaderState::default();
        self.bft_round_state.timeout.requests.clear();

        match ticket {
            // We finished the round with a committed proposal for the slot
            Ticket::CommitQC(..) | Ticket::ForcedCommitQC(..) => {
                let committed_proposal = self.bft_round_state.current_proposal.take().context(
                    "Cannot finish round without a current proposal. This should not happen.",
                )?;

                self.bft_round_state.slot += 1;
                self.bft_round_state.view = match ticket {
                    Ticket::CommitQC(..) => 0,
                    Ticket::ForcedCommitQC(view) => view,
                    _ => unreachable!(),
                };
                self.bft_round_state.parent_hash = committed_proposal.hashed();
                self.bft_round_state.parent_timestamp = committed_proposal.timestamp.clone();
                self.bft_round_state.parent_cut = committed_proposal.cut.clone();

                // Store the last commited QC to avoid issues when parsing Commit messages before Prepare
                self.bft_round_state.follower.buffered_quorum_certificate = match ticket {
                    Ticket::CommitQC(qc) => Some(qc),
                    Ticket::ForcedCommitQC(..) => None,
                    _ => unreachable!(),
                };
                for action in committed_proposal.staking_actions {
                    match action {
                        // Any new validators are added to the consensus and removed from candidates.
                        ConsensusStakingAction::Bond { candidate } => {
                            debug!("🎉 New validator bonded: {}", candidate.signature.validator);
                            self.store
                                .bft_round_state
                                .staking
                                .bond(candidate.signature.validator)
                                .map_err(|e| anyhow::anyhow!(e))?;
                        }
                        ConsensusStakingAction::PayFeesForDaDi {
                            lane_id,
                            cumul_size,
                        } => self
                            .store
                            .bft_round_state
                            .staking
                            .pay_for_dadi(lane_id, cumul_size)
                            .map_err(|e| anyhow::anyhow!(e))?,
                    }
                }
                self.store
                    .bft_round_state
                    .staking
                    .distribute()
                    .map_err(|e| anyhow::anyhow!(e))?;
            }
            // We finished the round with a timeout
            Ticket::TimeoutQC(..) => {
                // FIXME: I think TimeoutQC should hold the view, in case we missed multiple views
                // at once
                self.bft_round_state.view += 1;
                // TODO: buffer these?
                self.bft_round_state.follower.buffered_quorum_certificate = None;
            }
            Ticket::Genesis => {
                bail!("Genesis Ticket not accepted.");
            }
        }

        debug!(
            "🥋 Ready for slot {}, view {}",
            self.bft_round_state.slot, self.bft_round_state.view
        );

        self.metrics
            .at_round(self.bft_round_state.slot, self.bft_round_state.view);

        let round_leader = self.round_leader()?;
        if round_leader == *self.crypto.validator_pubkey() {
            self.set_state_tag(StateTag::Leader);
            debug!("👑 I'm the new leader! 👑")
        } else {
            self.set_state_tag(StateTag::Follower);
            self.store
                .bft_round_state
                .timeout
                .state
                .schedule_next(TimestampMsClock::now(), self.config.consensus.timeout_after);
        }

        Ok(())
    }

    fn current_slot_prepare_is_present(&self) -> bool {
        current_proposal!(self).is_some_and(|p| {
            p.slot == self.bft_round_state.slot && p.parent_hash == self.bft_round_state.parent_hash
        })
    }

    /// Verify that quorum certificate includes only validators that are part of the consensus
    fn verify_quorum_signers_part_of_consensus<T>(
        &self,
        quorum_certificate: &QuorumCertificate<T>,
    ) -> bool {
        quorum_certificate.validators.iter().all(|v| {
            self.bft_round_state
                .staking
                .bonded()
                .iter()
                .any(|v2| v2 == v)
        })
    }

    fn is_part_of_consensus(&self, pubkey: &ValidatorPublicKey) -> bool {
        self.bft_round_state.staking.is_bonded(pubkey)
    }

    fn get_own_voting_power(&self) -> u128 {
        if self.is_part_of_consensus(self.crypto.validator_pubkey()) {
            if let Some(my_stake) = self
                .bft_round_state
                .staking
                .get_stake(self.crypto.validator_pubkey())
            {
                my_stake
            } else {
                panic!("I'm not in my own staking registry !")
            }
        } else {
            0
        }
    }

    /// Verify that:
    ///  - the quorum certificate is for the given message.
    ///  - the signatures are above 2f+1 voting power.
    ///
    /// This ensures that we can trust the message.
    fn verify_quorum_certificate<
        T: borsh::BorshSerialize + std::fmt::Debug,
        Marker: std::fmt::Debug,
    >(
        &self,
        message: T,
        quorum_certificate: &QuorumCertificate<Marker>,
    ) -> Result<()> {
        debug!(
            "🔍 Verifying Quorum Certificate for message: {:?}, certificate: {:?}",
            message, quorum_certificate
        );
        // Construct the expected signed message
        let expected_signed_message = Signed {
            msg: message,
            signature: AggregateSignature {
                signature: quorum_certificate.signature.clone(),
                validators: quorum_certificate.validators.clone(),
            },
        };

        match (
            BlstCrypto::verify_aggregate(&expected_signed_message),
            self.verify_quorum_signers_part_of_consensus(quorum_certificate),
        ) {
            (Ok(()), true) => {}
            (Err(err), _) => bail!("Quorum Certificate verification failed: {}", err),
            (_, false) => {
                bail!("Quorum Certificate received contains non-consensus validators")
            }
        };

        // This helpfully ignores any signatures that would not be actually part of the consensus
        // since those would have voting power 0.
        // TODO: should we reject such messages?
        let voting_power = self
            .bft_round_state
            .staking
            .compute_voting_power(quorum_certificate.validators.as_slice());

        let f = self.bft_round_state.staking.compute_f();

        trace!(
            "📩 Slot {} validated votes: {} / {} ({} validators for a total bond = {})",
            self.bft_round_state.slot,
            voting_power,
            2 * f + 1,
            self.bft_round_state.staking.bonded().len(),
            self.bft_round_state.staking.total_bond()
        );

        // Verify enough validators signed
        if voting_power < 2 * f + 1 {
            bail!(
                "Quorum Certificate does not contain enough voting power ({} < {})",
                voting_power,
                2 * f + 1
            );
        }
        Ok(())
    }

    /// Connect to all validators & ask to be part of consensus
    fn send_candidacy(&mut self) -> Result<()> {
        let candidacy = self.crypto.sign(ValidatorCandidacy {
            peer_address: self.config.p2p.public_address.clone(),
        })?;
        info!(
            "📝 Sending candidacy message to be part of consensus. {}",
            candidacy
        );
        // TODO: it would be more optimal to send this to the next leaders only.
        self.broadcast_net_message(ConsensusNetMessage::ValidatorCandidacy(candidacy))?;
        Ok(())
    }

    fn handle_net_message(&mut self, msg: MsgWithHeader<ConsensusNetMessage>) -> Result<(), Error> {
        let MsgWithHeader::<ConsensusNetMessage> {
            msg: net_message,
            header:
                SignedByValidator {
                    signature:
                        ValidatorSignature {
                            validator: sender, ..
                        },
                    ..
                },
            ..
        } = msg;

        match net_message {
            ConsensusNetMessage::Prepare(consensus_proposal, ticket, view) => {
                with_metric!(
                    self.metrics,
                    "on_prepare",
                    self.on_prepare(sender, consensus_proposal, ticket, view)
                )
            }
            ConsensusNetMessage::PrepareVote(prepare_vote) => {
                BlstCrypto::verify(&prepare_vote).context("Invalid PrepareVote signature")?;
                with_metric!(
                    self.metrics,
                    "on_prepare_vote",
                    self.on_prepare_vote(prepare_vote)
                )
            }
            ConsensusNetMessage::Confirm(prepare_quorum_certificate, proposal_hash_hint) => {
                with_metric!(
                    self.metrics,
                    "on_confirm",
                    self.on_confirm(sender, prepare_quorum_certificate, proposal_hash_hint)
                )
            }
            ConsensusNetMessage::ConfirmAck(confirm_ack) => {
                BlstCrypto::verify(&confirm_ack).context("Invalid ConfirmAck signature")?;
                with_metric!(
                    self.metrics,
                    "on_confirm_ack",
                    self.on_confirm_ack(confirm_ack)
                )
            }
            ConsensusNetMessage::Commit(commit_quorum_certificate, proposal_hash_hint) => {
                self.on_commit(sender, commit_quorum_certificate, proposal_hash_hint)
            }
            ConsensusNetMessage::Timeout((timeout, tk)) => {
                BlstCrypto::verify(&timeout).context("Invalid Timeout signature")?;
                if let TimeoutKind::NilProposal(nil) = &tk {
                    BlstCrypto::verify(nil).context("Invalid NilProposal timeout signature")?;
                }
                with_metric!(self.metrics, "on_timeout", self.on_timeout(timeout, tk))
            }
            ConsensusNetMessage::TimeoutCertificate(
                certificate_of_timeout,
                certificate_of_proposal,
                slot,
                view,
            ) => with_metric!(
                self.metrics,
                "on_timeout_certificate",
                self.on_timeout_certificate(
                    certificate_of_timeout,
                    certificate_of_proposal,
                    slot,
                    view,
                )
            ),
            ConsensusNetMessage::ValidatorCandidacy(candidacy) => {
                BlstCrypto::verify(&candidacy).context("Invalid ValidatorCandidacy signature")?;
                self.on_validator_candidacy(candidacy)
            }
            ConsensusNetMessage::SyncRequest(proposal_hash) => {
                with_metric!(
                    self.metrics,
                    "on_sync_request",
                    self.on_sync_request(sender, proposal_hash)
                )
            }
            ConsensusNetMessage::SyncReply(prepare) => {
                with_metric!(self.metrics, "on_sync_reply", self.on_sync_reply(prepare))
            }
        }
    }

    /// Apply ticket locally, and start new round with it
    #[cfg_attr(feature = "instrumentation", tracing::instrument(skip(self, ticket)))]
    fn advance_round(&mut self, ticket: Ticket) -> Result<()> {
        self.apply_ticket(ticket.clone())?;

        // Decide what to do at the beginning of the next round
        // (Skip this if we are catching up, i.e. this is a regular commit and we have buffered children)
        if self.is_round_leader()
            && !matches!(ticket, Ticket::ForcedCommitQC(..))
            && (matches!(ticket, Ticket::TimeoutQC(..)) || self.has_no_buffered_children())
        {
            // Setup our ticket for the next round
            // Send Prepare message to all validators
            self.bft_round_state.leader.pending_ticket = Some(ticket);
            self.bus.send(ConsensusCommand::StartNewSlot(None))?;
            Ok(())
        } else if self.is_part_of_consensus(self.crypto.validator_pubkey()) {
            Ok(())
        } else if self
            .bft_round_state
            .staking
            .get_stake(self.crypto.validator_pubkey())
            .unwrap_or(0)
            > MIN_STAKE
        {
            self.send_candidacy()
        } else {
            info!(
                "😥 No stake on pubkey '{}'. Not sending candidacy.",
                self.crypto.validator_pubkey()
            );
            Ok(())
        }
    }

    fn verify_commit_quorum_certificate_against_current_proposal(
        &self,
        commit_quorum_certificate: &CommitQC,
    ) -> Result<()> {
        let current_proposal = current_proposal!(self)
            .context("Cannot verify commit quorum certificate without a current proposal")?;
        // Check that this is a QC for ConfirmAck for the expected proposal.
        // This also checks slot/view as those are part of the hash.
        // TODO: would probably be good to make that more explicit.
        self.verify_quorum_certificate(
            (current_proposal.hashed(), ConfirmAckMarker),
            commit_quorum_certificate,
        )
    }

    /// Emits an event that will make Mempool able to build a block
    fn emit_commit_event(&mut self, commit_quorum_certificate: &CommitQC) -> Result<()> {
        self.metrics.commit();

        let current_proposal = current_proposal!(self)
            .context("Cannot emit commit event without a current proposal")?
            .clone();

        self.bus
            .send(ConsensusEvent::CommitConsensusProposal(
                CommittedConsensusProposal {
                    staking: self.bft_round_state.staking.clone(),
                    consensus_proposal: current_proposal.clone(),
                    certificate: commit_quorum_certificate.0.clone(),
                },
            ))
            .context("Failed to send ConsensusEvent::CommittedConsensusProposal on the bus")?;

        debug!("📈 Slot {} committed", &self.bft_round_state.slot);

        Ok(())
    }

    /// Message received by leader & follower.
    #[cfg_attr(
        feature = "instrumentation",
        tracing::instrument(skip(self, candidacy))
    )]
    fn on_validator_candidacy(
        &mut self,
        candidacy: SignedByValidator<ValidatorCandidacy>,
    ) -> Result<()> {
        info!("📝 Received candidacy message: {}", candidacy);

        // Verify that the validator is not already part of the consensus
        if self.is_part_of_consensus(&candidacy.signature.validator) {
            debug!("Validator is already part of the consensus");
            return Ok(());
        }

        if self
            .bft_round_state
            .staking
            .is_bonded(&candidacy.signature.validator)
        {
            debug!("Validator is already bonded. Ignoring candidacy");
            return Ok(());
        }

        // Verify that the candidate has enough stake
        if let Some(stake) = self
            .bft_round_state
            .staking
            .get_stake(&candidacy.signature.validator)
        {
            if stake < staking::state::MIN_STAKE {
                bail!("🛑 Candidate validator does not have enough stake to be part of consensus");
            }
        } else {
            bail!("🛑 Candidate validator is not staking !");
        }

        // Add validator to consensus candidates
        self.validator_candidates.push(candidacy);
        Ok(())
    }

    async fn handle_node_state_event(&mut self, msg: NodeStateEvent) -> Result<()> {
        match msg {
            NodeStateEvent::NewBlock(block) => {
                self.store
                    .bft_round_state
                    .staking
                    .process_block(&block.staking_data)
                    .map_err(|e| anyhow!(e))?;

                if let StateTag::Joining = self.bft_round_state.state_tag {
                    let block_height = block.signed_block.consensus_proposal.slot;
                    if self.store.bft_round_state.joining.staking_updated_to < block_height {
                        info!("🚪 Processed block {}", block_height);
                        self.store.bft_round_state.joining.staking_updated_to = block_height;
                        self.store.bft_round_state.parent_cut =
                            block.signed_block.consensus_proposal.cut.clone();
                        self.store.bft_round_state.parent_timestamp =
                            block.signed_block.consensus_proposal.timestamp.clone();
                        self.store.bft_round_state.slot = block_height + 1;
                        self.store.bft_round_state.view = 0;
                        self.store.bft_round_state.parent_hash = block.signed_block.hashed();
                        // Some of our internal logic relies on BFT slot + 1 == cp slot to mean we have committed, so do that.
                        self.store.bft_round_state.current_proposal = Some(ConsensusProposal {
                            slot: block_height,
                            ..Default::default()
                        });

                        self.bft_round_state.timeout.requests.clear();
                    }
                }
                self.record_consensus_state_metric();
                Ok(())
            }
        }
    }

    async fn handle_command(&mut self, msg: ConsensusCommand) -> Result<()> {
        match msg {
            ConsensusCommand::TimeoutTick => self.on_timeout_tick(),
            ConsensusCommand::StartNewSlot(may_delay) => {
                self.start_round(TimestampMsClock::now(), may_delay).await?;
                Ok(())
            }
        }
    }

    #[inline(always)]
    fn broadcast_net_message(&mut self, net_message: ConsensusNetMessage) -> Result<()> {
        let signed_msg = self.sign_net_message(net_message)?;
        let enum_variant_name: &'static str = (&signed_msg.msg).into();
        self.bus
            .send(OutboundMessage::broadcast(signed_msg))
            .context(format!(
                "Failed to broadcast {enum_variant_name} msg on the bus"
            ))?;
        Ok(())
    }

    #[inline(always)]
    fn send_net_message(
        &mut self,
        to: ValidatorPublicKey,
        net_message: ConsensusNetMessage,
    ) -> Result<()> {
        let signed_msg = self.sign_net_message(net_message)?;
        let enum_variant_name: &'static str = (&signed_msg.msg).into();
        self.bus
            .send(OutboundMessage::send(to, signed_msg))
            .context(format!("Failed to send {enum_variant_name} msg on the bus"))?;
        Ok(())
    }

    async fn wait_genesis(&mut self) -> Result<()> {
        let should_shutdown = module_handle_messages! {
            on_self self,
            listen<GenesisEvent> msg => {
                match msg {
                    GenesisEvent::GenesisBlock(signed_block) => {
                        // Wait until we have processed the genesis block to update our Staking.
                        module_handle_messages! {
                            on_self self,
                            listen<NodeStateEvent> event => {
                                let NodeStateEvent::NewBlock(block) = &event;
                                if block.signed_block.consensus_proposal.slot != 0 {
                                    bail!("Non-genesis block received during consensus genesis");
                                }
                                match self.handle_node_state_event(event).await {
                                    Ok(_) => break,
                                    Err(e) => bail!("Error while handling Genesis block: {:#}", e),
                                }
                            }
                        };

                        self.bft_round_state.parent_hash = signed_block.hashed();
                        self.bft_round_state.parent_cut = signed_block.consensus_proposal.cut.clone();
                        self.bft_round_state.slot = 1;
                        self.bft_round_state.view = 0;
                        let round_leader = self.round_leader()?;

                        if round_leader == *self.crypto.validator_pubkey() {
                            self.set_state_tag(StateTag::Leader);
                            info!("👑 Starting consensus as leader");
                        } else {
                            self.set_state_tag(StateTag::Follower);
                            info!(
                                "💂‍♂️ Starting consensus as follower of leader {}",
                                round_leader
                            );
                        }

                        // Schedule timeout for both leader and follower to ensure recovery from stuck states
                        self.store.bft_round_state.timeout.state.schedule_next(
                            TimestampMsClock::now(),
                            self.config.consensus.timeout_after,
                        );

                        // Send a CommitConsensusProposal for the genesis block
                        _ = log_error!(self
                            .bus
                            .send(ConsensusEvent::CommitConsensusProposal(
                                CommittedConsensusProposal {
                                    staking: self.bft_round_state.staking.clone(),
                                    consensus_proposal: signed_block.consensus_proposal,
                                    certificate: signed_block.certificate,
                                },
                            )), "Failed to send ConsensusEvent::CommittedConsensusProposal on the bus");
                        break;
                    },
                    GenesisEvent::NoGenesis => {
                        // First wait a couple seconds to hopefully have reconnected with all peers.
                        info!("🧳 Waiting a few seconds before processing with rejoin to connect with peers.");
                        tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
                        // If we deserialized, we might be a follower or a leader.
                        // There's a few possibilities: maybe we're restarting quick enough that we're still synched,
                        // maybe we actually would block consensus by having a large stake
                        // maybe we were about to be the leader and got byzantined out.
                        // Regardless, we should probably assume that we need to catch up.
                        // TODO: this logic can be improved.
                        self.set_state_tag(StateTag::Joining);
                        // Set up an initial timeout to ensure we don't get stuck if we miss commits
                        self.store.bft_round_state.timeout.state.schedule_next(TimestampMsClock::now(), self.config.consensus.timeout_after);

                        break;
                    },
                }
            }
        };
        if should_shutdown {
            return Ok(());
        }

        if self.is_round_leader() {
            self.bft_round_state.leader.pending_ticket = Some(Ticket::Genesis);
            self.bus.send(ConsensusCommand::StartNewSlot(None))?;
        }
        self.start().await
    }

    async fn start(&mut self) -> Result<()> {
        info!(
            "🚀 Starting consensus as {:?}",
            self.bft_round_state.state_tag
        );
        self.record_consensus_state_metric();

        let mut timeout_ticker = interval(Duration::from_millis(200));
        timeout_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        module_handle_messages! {
            on_self self,
            listen<NodeStateEvent> event => {
                let _ = log_error!(self.handle_node_state_event(event).await, "Error while handling data event");
            }
            listen<ConsensusCommand> cmd => {
                let _ = log_error!(self.handle_command(cmd).await, "Error while handling consensus command");
            }
            listen<MsgWithHeader<ConsensusNetMessage>> msg, span(ctx) => {
                let _span = info_span_ctx!("handle_consensus_net_message", ctx).entered();
                let _ = log_error!(self.handle_net_message(msg), "Consensus message failed");
            }
            command_response<QueryConsensusInfo, ConsensusInfo> _ => {
                let slot = self.bft_round_state.slot;
                let view = self.bft_round_state.view;
                let round_leader = self.round_leader()?;
                let last_timestamp = self.bft_round_state.parent_timestamp.clone();
                let validators = self.bft_round_state.staking.bonded().clone();
                Ok(ConsensusInfo { slot, view, round_leader, last_timestamp, validators })
            }
            command_response<QueryConsensusStakingState, Staking> _ => {
                Ok(self.bft_round_state.staking.clone())
            }
            command_response<QueryConsensusCatchupStore, QueryConsensusCatchupStoreResponse> _ => {
                if !matches!(self.bft_round_state.state_tag, StateTag::Joining) {
                    self.serialize_bft_round_state()
                } else {
                    Err(anyhow::anyhow!(
                        "Cannot serialize BFT round state in state {:?}",
                        self.bft_round_state.state_tag
                    ))
                }
            }
            _ = timeout_ticker.tick() => {
                _ = log_error!(self.bus.send(ConsensusCommand::TimeoutTick), "Cannot send message over channel");
            }
        };

        Ok(())
    }

    fn serialize_bft_round_state(&self) -> Result<QueryConsensusCatchupStoreResponse> {
        let store_copy = ConsensusStore {
            bft_round_state: BFTRoundState {
                staking: self.bft_round_state.staking.clone(),
                slot: self.bft_round_state.slot,
                view: self.bft_round_state.view,
                parent_hash: self.bft_round_state.parent_hash.clone(),
                parent_timestamp: self.bft_round_state.parent_timestamp.clone(),
                parent_cut: self.bft_round_state.parent_cut.clone(),
                current_proposal: self.bft_round_state.current_proposal.clone(),
                ..Default::default()
            },
            validator_candidates: self.validator_candidates.clone(),
            recent_timeout_certificates: VecDeque::new(),
        };
        let res = borsh::to_vec(&store_copy).context("Failed to serialize BFT round state")?;
        Ok(QueryConsensusCatchupStoreResponse(res))
    }

    fn sign_net_message(
        &self,
        msg: ConsensusNetMessage,
    ) -> Result<MsgWithHeader<ConsensusNetMessage>> {
        trace!("🔏 Signing message: {}", msg);
        self.crypto.sign_msg_with_header(msg)
    }
}

#[cfg(test)]
impl Consensus {}

#[cfg(test)]
pub mod test {

    use crate::{
        bus::{bus_client, command_response::CmdRespClient},
        rest::RestApi,
        utils::integration_test::NodeIntegrationCtxBuilder,
    };
    use hyli_modules::{
        bus::BusEnvelope,
        handle_messages,
        node_state::{module::NodeStateModule, NodeState},
    };
    use std::{future::Future, pin::Pin, sync::Arc};
    use tracing::warn;

    use super::*;
    use crate::{
        bus::{dont_use_this::get_receiver, metrics::BusMetrics, SharedMessageBus},
        model::DataProposalHash,
        p2p::network::NetMessage,
        tests::autobahn_testing::*,
        utils::conf::Conf,
    };
    use assertables::{assert_contains, assert_none};
    use tokio::sync::broadcast::Receiver;
    use utils::TimestampMs;

    pub struct ConsensusTestCtx {
        pub out_receiver: Receiver<BusEnvelope<OutboundMessage>>,
        pub _event_receiver: Receiver<BusEnvelope<ConsensusEvent>>,
        pub _p2p_receiver: Receiver<BusEnvelope<P2PCommand>>,
        pub consensus: Consensus,
        pub name: String,
    }

    macro_rules! build_nodes {
        ($count:tt) => {{
            async {
                let cryptos: Vec<BlstCrypto> =
                    $crate::tests::autobahn_testing::AutobahnTestCtx::generate_cryptos($count);

                let mut nodes = vec![];

                for i in 0..$count {
                    let mut node = ConsensusTestCtx::new(
                        format!("node-{i}").as_ref(),
                        cryptos.get(i).unwrap().clone(),
                    )
                    .await;

                    node.setup_node(i, &cryptos);

                    nodes.push(node);
                }

                $crate::tests::autobahn_testing::build_tuple!(nodes.remove(0), $count)
            }
        }};
    }
    pub(crate) use build_nodes;

    impl ConsensusTestCtx {
        pub async fn build_consensus(
            shared_bus: &SharedMessageBus,
            crypto: BlstCrypto,
        ) -> Consensus {
            let store = ConsensusStore::default();
            let mut conf = Conf::default();
            conf.consensus.slot_duration = Duration::from_millis(1000);
            conf.consensus.timeout_after = Duration::from_millis(5000);
            conf.consensus.timeout_certificate_cache_size = 100;
            let bus = ConsensusBusClient::new_from_bus(shared_bus.new_handle()).await;

            Consensus {
                metrics: ConsensusMetrics::global("id".to_string()),
                bus,
                file: None,
                store,
                config: Arc::new(conf),
                crypto: Arc::new(crypto),
            }
        }

        pub async fn new(name: &str, crypto: BlstCrypto) -> Self {
            let shared_bus = SharedMessageBus::new(BusMetrics::global("global".to_string()));
            let out_receiver = get_receiver::<OutboundMessage>(&shared_bus).await;
            let event_receiver = get_receiver::<ConsensusEvent>(&shared_bus).await;
            let p2p_receiver = get_receiver::<P2PCommand>(&shared_bus).await;

            let mut new_cut_query_receiver =
                AutobahnBusClient::new_from_bus(shared_bus.new_handle()).await;
            tokio::spawn(async move {
                handle_messages! {
                    on_bus new_cut_query_receiver,
                    command_response<QueryNewCut, Cut> _ => {
                        Ok(Cut::default())
                    }
                };
            });

            let consensus = Self::build_consensus(&shared_bus, crypto).await;
            Self {
                out_receiver,
                _event_receiver: event_receiver,
                _p2p_receiver: p2p_receiver,
                consensus,
                name: name.to_string(),
            }
        }

        pub fn setup_node(&mut self, index: usize, cryptos: &[BlstCrypto]) {
            for other_crypto in cryptos.iter() {
                self.add_trusted_validator(other_crypto.validator_pubkey());
            }

            self.consensus.bft_round_state.slot = 1;
            self.consensus.bft_round_state.parent_hash =
                ConsensusProposalHash("genesis".to_string());
            self.consensus.bft_round_state.parent_cut = vec![(
                LaneId::new(
                    cryptos
                        .first()
                        .expect("No cryptos available")
                        .validator_pubkey()
                        .clone(),
                ),
                DataProposalHash("genesis".to_string()),
                LaneBytesSize(100),
                AggregateSignature::default(),
            )];

            if index == 0 {
                self.consensus.bft_round_state.state_tag = StateTag::Leader;
                self.consensus.bft_round_state.leader.pending_ticket = Some(Ticket::Genesis);
            } else {
                self.consensus.bft_round_state.state_tag = StateTag::Follower;
            }
        }

        /// Set a node up to catchup to the consensus without being part of it.
        pub fn setup_for_joining(&mut self, nodes: &[&ConsensusTestCtx]) {
            for other_node in nodes.iter() {
                self.add_trusted_validator(other_node.consensus.crypto.validator_pubkey());
            }
            self.consensus.bft_round_state.state_tag = StateTag::Joining;
        }

        /// Set a node up to catchup to the consensus while already being part of it (e.g. after starting)
        pub fn setup_for_rejoining(&mut self, slot: u64) {
            self.consensus.bft_round_state.state_tag = StateTag::Joining;
            self.consensus.bft_round_state.slot = slot;
            self.consensus.bft_round_state.view = 0;
        }

        pub fn validator_pubkey(&self) -> ValidatorPublicKey {
            self.consensus.crypto.validator_pubkey().clone()
        }

        pub fn staking(&self) -> Staking {
            self.consensus.bft_round_state.staking.clone()
        }

        pub async fn timeout(nodes: &mut [&mut ConsensusTestCtx]) {
            for n in nodes {
                n.consensus
                    .store
                    .bft_round_state
                    .timeout
                    .state
                    .schedule_next(
                        TimestampMsClock::now() - Duration::from_secs(10),
                        Duration::from_secs(5),
                    );
                n.consensus
                    .handle_command(ConsensusCommand::TimeoutTick)
                    .await
                    .unwrap_or_else(|err| panic!("Timeout tick for node {}: {:?}", n.name, err));
            }
        }

        pub fn add_trusted_validator(&mut self, pubkey: &ValidatorPublicKey) {
            self.consensus
                .bft_round_state
                .staking
                .stake(hex::encode(pubkey.0.clone()).into(), 100)
                .unwrap();

            self.consensus
                .bft_round_state
                .staking
                .deposit_for_fees(pubkey.clone(), 1_000_000_000)
                .unwrap();

            self.consensus
                .bft_round_state
                .staking
                .delegate_to(hex::encode(pubkey.0.clone()).into(), pubkey.clone())
                .unwrap();

            self.consensus
                .bft_round_state
                .staking
                .bond(pubkey.clone())
                .expect("cannot bond trusted validator");
            info!("🎉 Trusted validator added: {}", pubkey);
        }

        pub async fn new_node(name: &str) -> Self {
            let crypto = BlstCrypto::new(name).unwrap();
            Self::new(name, crypto.clone()).await
        }

        pub(crate) fn pubkey(&self) -> ValidatorPublicKey {
            self.consensus.crypto.validator_pubkey().clone()
        }

        pub(crate) fn is_joining(&self) -> bool {
            matches!(self.consensus.bft_round_state.state_tag, StateTag::Joining)
        }

        pub fn setup_for_round(nodes: &mut [&mut ConsensusTestCtx], slot: u64, view: u64) {
            // TODO: write a real one?
            let commit_qc = QuorumCertificate(AggregateSignature::default(), ConfirmAckMarker);

            for node in nodes.iter_mut() {
                node.consensus.bft_round_state.slot = slot;
                node.consensus.bft_round_state.view = view;

                node.consensus
                    .bft_round_state
                    .follower
                    .buffered_quorum_certificate = Some(commit_qc.clone());

                if node.consensus.round_leader().unwrap() == node.pubkey() {
                    node.consensus.bft_round_state.state_tag = StateTag::Leader;
                    node.consensus.bft_round_state.leader.pending_ticket =
                        Some(Ticket::CommitQC(commit_qc.clone()));
                } else {
                    node.consensus.bft_round_state.state_tag = StateTag::Follower;
                }
            }
        }

        pub(crate) async fn handle_msg(
            &mut self,
            msg: &MsgWithHeader<ConsensusNetMessage>,
            err: &str,
        ) {
            debug!("📥 {} Handling message: {:?}", self.name, msg);
            self.consensus.handle_net_message(msg.clone()).expect(err);
        }

        pub(crate) async fn handle_msg_err(
            &mut self,
            msg: &MsgWithHeader<ConsensusNetMessage>,
        ) -> Error {
            debug!("📥 {} Handling message expecting err: {:?}", self.name, msg);
            let err = self.consensus.handle_net_message(msg.clone()).unwrap_err();
            info!("Expected error: {:#}", err);
            err
        }

        pub(crate) async fn handle_node_state_event(&mut self, msg: NodeStateEvent) -> Result<()> {
            self.consensus.handle_node_state_event(msg).await
        }

        async fn add_staker(&mut self, staker: &Self, amount: u128, err: &str) {
            info!("➕ {} Add staker: {:?}", self.name, staker.name);
            self.consensus
                .handle_node_state_event(NodeStateEvent::NewBlock(NodeStateBlock {
                    staking_data: BlockStakingData {
                        staking_actions: vec![
                            (staker.name.clone().into(), StakingAction::Stake { amount }),
                            (
                                staker.name.clone().into(),
                                StakingAction::Delegate {
                                    validator: staker.pubkey(),
                                },
                            ),
                        ],
                        new_bounded_validators: vec![],
                    }
                    .into(),
                    ..Default::default()
                }))
                .await
                .expect(err);
        }

        async fn with_stake(&mut self, amount: u128, err: &str) {
            self.consensus
                .handle_node_state_event(NodeStateEvent::NewBlock(NodeStateBlock {
                    staking_data: BlockStakingData {
                        staking_actions: vec![
                            (self.name.clone().into(), StakingAction::Stake { amount }),
                            (
                                self.name.clone().into(),
                                StakingAction::Delegate {
                                    validator: self.consensus.crypto.validator_pubkey().clone(),
                                },
                            ),
                        ],
                        new_bounded_validators: vec![],
                    }
                    .into(),
                    ..Default::default()
                }))
                .await
                .expect(err)
        }

        pub async fn start_round(&mut self) {
            log_error!(
                self.consensus
                    .start_round(TimestampMsClock::now(), Some(TimestampMs(0)))
                    .await,
                "start-round"
            )
            .expect("Failed to start slot");
        }

        pub async fn start_round_at(&mut self, current_timestamp: TimestampMs) {
            log_error!(
                self.consensus
                    .start_round(current_timestamp, Some(TimestampMs(0)))
                    .await,
                "start-round"
            )
            .expect("Failed to start slot");
        }

        pub(crate) fn assert_broadcast(
            &mut self,
            description: &str,
        ) -> Pin<Box<dyn Future<Output = MsgWithHeader<ConsensusNetMessage>>>> {
            #[allow(clippy::expect_fun_call)]
            let rec = self
                .out_receiver
                .try_recv()
                .expect(format!("{description}: No message broadcasted").as_str());

            let rec = rec.into_message();

            if let OutboundMessage::BroadcastMessage(net_msg) = rec {
                if let NetMessage::ConsensusMessage(msg) = net_msg {
                    Box::pin(async move { msg })
                } else {
                    warn!("{description}: skipping {:?}", net_msg);
                    self.assert_broadcast(description)
                }
            } else if let OutboundMessage::SendMessage { msg: net_msg, .. } = rec {
                if let NetMessage::ConsensusMessage(_) = net_msg {
                    warn!("{description}: skipping send instead of broadcast");
                    self.assert_broadcast(description)
                } else {
                    warn!("{description}: skipping {:?}", net_msg);
                    self.assert_broadcast(description)
                }
            } else {
                warn!(
                    "{description}: OutboundMessage::BroadcastMessage message is missing, found {:?}",
                    rec
                );
                self.assert_broadcast(description)
            }
        }

        #[track_caller]
        pub(crate) fn assert_no_broadcast(&mut self, description: &str) {
            #[allow(clippy::expect_fun_call)]
            let rec = self.out_receiver.try_recv().map(|msg| msg.into_message());

            match rec {
                Ok(OutboundMessage::BroadcastMessage(net_msg)) => {
                    panic!("{description}: Broadcast message found: {net_msg:?}")
                }
                els => {
                    info!("{description}: Got {:?}", els);
                }
            }
        }

        pub(crate) fn assert_send(
            &mut self,
            to: &ValidatorPublicKey,
            description: &str,
        ) -> Pin<Box<dyn Future<Output = MsgWithHeader<ConsensusNetMessage>>>> {
            #[allow(clippy::expect_fun_call)]
            let rec = self
                .out_receiver
                .try_recv()
                .expect(format!("{description}: No message sent").as_str())
                .into_message();
            if let OutboundMessage::SendMessage {
                validator_id,
                msg: net_msg,
            } = rec
            {
                if let NetMessage::ConsensusMessage(msg) = net_msg {
                    if to == &validator_id {
                        Box::pin(async move { msg })
                    } else {
                        warn!(
                            "{description}: skipping send to {}, expected {}",
                            validator_id, to
                        );
                        self.assert_send(to, description)
                    }
                } else {
                    warn!("{description}: skipping non-consensus message, details in debug");
                    debug!("Message is: {:?}", net_msg);
                    self.assert_send(to, description)
                }
            } else {
                warn!("{description}: Skipping broadcast message, details in debug");
                debug!("Message is: {:?}", rec);
                self.assert_send(to, description)
            }
        }
    }

    pub(crate) async fn simple_timeout_round_at_4(
        next_leader: &mut ConsensusTestCtx,
        other_2: &mut ConsensusTestCtx,
        other_3: &mut ConsensusTestCtx,
    ) {
        // Make others timeout
        ConsensusTestCtx::timeout(&mut [next_leader, other_2, other_3]).await;

        broadcast! {
            description: "Follower - Timeout",
            from: next_leader, to: [other_2, other_3],
            message_matches: ConsensusNetMessage::Timeout(..)
        };
        broadcast! {
            description: "Follower - Timeout",
            from: other_2, to: [next_leader, other_3],
            message_matches: ConsensusNetMessage::Timeout(..)
        };

        // node 4 should join the mutiny
        broadcast! {
            description: "Follower - Timeout",
            from: other_3, to: [next_leader, other_2],
            message_matches: ConsensusNetMessage::Timeout(..)
        };

        // After this broadcast, every node has 2f+1 timeouts and can create a timeout certificate

        // Next leader does not emits a timeout certificate since it will broadcast the next Prepare with it
        next_leader.assert_no_broadcast("Timeout Certificate 2");

        other_2.assert_broadcast("Timeout Certificate 3").await;
        other_3.assert_broadcast("Timeout Certificate 4").await;
    }

    #[test_log::test(tokio::test)]
    async fn test_happy_path() {
        let (mut node1, mut node2): (ConsensusTestCtx, ConsensusTestCtx) = build_nodes!(2).await;

        node1.start_round().await;
        // Slot 0 - leader = node1

        let (cp1, ticket1, view1) = simple_commit_round! {
            leader: node1,
            followers: [node2]
        };

        assert_eq!(cp1.slot, 1);
        assert_eq!(view1, 0);
        assert_eq!(
            cp1.parent_hash,
            ConsensusProposalHash("genesis".to_string())
        );
        assert_eq!(ticket1, Ticket::Genesis);

        // Slot 1 - leader = node2
        node2.start_round().await;

        let (cp2, ticket2, view2) = simple_commit_round! {
            leader: node2,
            followers: [node1]
        };

        assert_eq!(cp2.slot, 2);
        assert_eq!(view2, 0);
        assert_eq!(cp2.parent_hash, cp1.hashed());
        assert!(matches!(ticket2, Ticket::CommitQC(_)));

        // Slot 2 - leader = node1
        node1.start_round().await;

        let (cp3, ticket3, view3) = simple_commit_round! {
            leader: node1,
            followers: [node2]
        };

        assert_eq!(cp3.slot, 3);
        assert_eq!(view3, 0);
        assert!(matches!(ticket3, Ticket::CommitQC(_)));
    }

    #[test_log::test(tokio::test)]
    async fn test_prepare_vote_invalid_signature_rejected() {
        let (mut node1, node2): (ConsensusTestCtx, ConsensusTestCtx) = build_nodes!(2).await;

        let signed_vote = node2
            .consensus
            .crypto
            .sign((
                ConsensusProposalHash("hash-a".to_string()),
                PrepareVoteMarker,
            ))
            .unwrap();
        let invalid_vote = Signed {
            msg: (
                ConsensusProposalHash("hash-b".to_string()),
                PrepareVoteMarker,
            ),
            signature: signed_vote.signature,
        };

        let net_msg = node2
            .consensus
            .crypto
            .sign_msg_with_header(ConsensusNetMessage::PrepareVote(invalid_vote))
            .unwrap();

        let err = node1.handle_msg_err(&net_msg).await;
        assert!(
            format!("{err:#}").contains("Invalid PrepareVote signature"),
            "unexpected error: {err:#}"
        );
    }

    #[test_log::test(tokio::test)]
    async fn basic_commit_4() {
        let (mut node1, mut node2, mut node3, mut node4): (
            ConsensusTestCtx,
            ConsensusTestCtx,
            ConsensusTestCtx,
            ConsensusTestCtx,
        ) = build_nodes!(4).await;

        node1.start_round().await;
        // Slot 1 - leader = node1
        // Ensuring one slot commits correctly before a timeout
        let cp_round;
        broadcast! {
            description: "Leader - Prepare",
            from: node1, to: [node2, node3, node4],
            message_matches: ConsensusNetMessage::Prepare(cp, ticket, view) => {
                cp_round = Some(cp.clone());
                assert_eq!(cp.slot, 1);
                assert_eq!(*view, 0);
                assert_eq!(ticket, &Ticket::Genesis);
            }
        };

        let cp_round_hash = &cp_round.unwrap().hashed();

        send! {
            description: "Follower - PrepareVote",
            from: [
                node2; ConsensusNetMessage::PrepareVote(Signed { msg: (cp_hash, _), .. }) => { assert_eq!(cp_hash, cp_round_hash); },
                node3; ConsensusNetMessage::PrepareVote(Signed { msg: (cp_hash, _), .. }) => { assert_eq!(cp_hash, cp_round_hash); },
                node4; ConsensusNetMessage::PrepareVote(Signed { msg: (cp_hash, _), .. }) => { assert_eq!(cp_hash, cp_round_hash); }
            ], to: node1
        };

        broadcast! {
            description: "Leader - Confirm",
            from: node1, to: [node2, node3, node4],
            message_matches: ConsensusNetMessage::Confirm(_, _)
        };

        send! {
            description: "Follower - Confirm Ack",
            from: [
                node2; ConsensusNetMessage::ConfirmAck(Signed { msg: (cp_hash, _), .. }) => { assert_eq!(cp_hash, cp_round_hash); },
                node3; ConsensusNetMessage::ConfirmAck(Signed { msg: (cp_hash, _), .. }) => { assert_eq!(cp_hash, cp_round_hash); },
                node4; ConsensusNetMessage::ConfirmAck(Signed { msg: (cp_hash, _), .. }) => { assert_eq!(cp_hash, cp_round_hash); }
            ], to: node1
        };

        broadcast! {
            description: "Leader - Commit",
            from: node1, to: [node2, node3, node4],
            message_matches: ConsensusNetMessage::Commit(_, cp_hash) => {
                assert_eq!(cp_hash, cp_round_hash);
            }
        };
    }

    #[test_log::test(tokio::test)]
    async fn prepare_wrong_slot() {
        let (mut node1, mut node2, mut node3, mut node4): (
            ConsensusTestCtx,
            ConsensusTestCtx,
            ConsensusTestCtx,
            ConsensusTestCtx,
        ) = build_nodes!(4).await;

        node1.start_round().await;

        let cp = ConsensusProposal {
            slot: 2,
            timestamp: TimestampMs(123),
            cut: vec![(
                LaneId::new(node2.pubkey()),
                DataProposalHash("test".to_string()),
                LaneBytesSize::default(),
                AggregateSignature::default(),
            )],
            staking_actions: vec![],
            parent_hash: ConsensusProposalHash("hash".into()),
        };

        // Create wrong prepare
        let prepare_msg = node1
            .consensus
            .sign_net_message(ConsensusNetMessage::Prepare(cp.clone(), Ticket::Genesis, 0))
            .expect("Error while signing");

        // Slot 1 - leader = node1
        // Ensuring one slot commits correctly before a

        assert_contains!(
            node2.handle_msg_err(&prepare_msg).await.to_string(),
            "wrong slot"
        );
        assert_eq!(
            node2
                .consensus
                .bft_round_state
                .follower
                .buffered_prepares
                .get(&cp.hashed()),
            None
        );

        assert_contains!(
            node3.handle_msg_err(&prepare_msg).await.to_string(),
            "wrong slot"
        );
        assert_eq!(
            node3
                .consensus
                .bft_round_state
                .follower
                .buffered_prepares
                .get(&cp.hashed()),
            None
        );

        assert_contains!(
            node4.handle_msg_err(&prepare_msg).await.to_string(),
            "wrong slot"
        );
        assert_eq!(
            node4
                .consensus
                .bft_round_state
                .follower
                .buffered_prepares
                .get(&cp.hashed()),
            None
        );
    }

    #[test_log::test(tokio::test)]
    async fn prepare_wrong_signature() {
        let (mut node1, mut node2, mut node3, mut node4): (
            ConsensusTestCtx,
            ConsensusTestCtx,
            ConsensusTestCtx,
            ConsensusTestCtx,
        ) = build_nodes!(4).await;

        node1.start_round().await;

        // Create wrong prepare signed by other node than leader
        let prepare_msg = node2
            .consensus
            .sign_net_message(ConsensusNetMessage::Prepare(
                ConsensusProposal {
                    slot: 1,
                    timestamp: TimestampMs(123),
                    cut: vec![(
                        LaneId::new(node2.pubkey()),
                        DataProposalHash("test".to_string()),
                        LaneBytesSize::default(),
                        AggregateSignature::default(),
                    )],
                    staking_actions: vec![],
                    parent_hash: ConsensusProposalHash("hash".into()),
                },
                Ticket::Genesis,
                0,
            ))
            .expect("Error while signing");

        // Slot 1 - leader = node1
        // Ensuring one slot commits correctly before a timeout

        assert_contains!(
            node2.handle_msg_err(&prepare_msg).await.to_string(),
            "does not come from current leader"
        );
        assert_contains!(
            node3.handle_msg_err(&prepare_msg).await.to_string(),
            "does not come from current leader"
        );
        assert_contains!(
            node4.handle_msg_err(&prepare_msg).await.to_string(),
            "does not come from current leader"
        );
    }

    #[test_log::test(tokio::test)]
    async fn prepare_wrong_leader() {
        let (mut node1, mut node2, mut node3, mut node4): (
            ConsensusTestCtx,
            ConsensusTestCtx,
            ConsensusTestCtx,
            ConsensusTestCtx,
        ) = build_nodes!(4).await;

        node1.start_round().await;

        // Create prepare with wrong round leader (node3) and signed accordingly
        let prepare_msg = node3
            .consensus
            .sign_net_message(ConsensusNetMessage::Prepare(
                ConsensusProposal {
                    slot: 1,
                    timestamp: TimestampMs(123),
                    cut: vec![(
                        LaneId::new(node2.pubkey()),
                        DataProposalHash("test".to_string()),
                        LaneBytesSize::default(),
                        AggregateSignature::default(),
                    )],
                    staking_actions: vec![],
                    parent_hash: ConsensusProposalHash("hash".into()),
                },
                Ticket::Genesis,
                0,
            ))
            .expect("Error while signing");

        // Slot 1 - leader = node1
        // Ensuring one slot commits correctly before a timeout

        assert_contains!(
            node2.handle_msg_err(&prepare_msg).await.to_string(),
            "does not come from current leader"
        );
        assert_contains!(
            node1.handle_msg_err(&prepare_msg).await.to_string(),
            "does not come from current leader"
        );
        assert_contains!(
            node3.handle_msg_err(&prepare_msg).await.to_string(),
            "does not come from current leader"
        );
        assert_contains!(
            node4.handle_msg_err(&prepare_msg).await.to_string(),
            "does not come from current leader"
        );
    }

    #[test_log::test(tokio::test)]
    async fn prepare_timestamp_too_old() {
        let (mut node1, mut node2, mut node3, mut node4): (
            ConsensusTestCtx,
            ConsensusTestCtx,
            ConsensusTestCtx,
            ConsensusTestCtx,
        ) = build_nodes!(4).await;

        node1.start_round_at(TimestampMs(1000)).await;

        let (cp, ..) = simple_commit_round! {
            leader: node1,
            followers: [node2, node3, node4]
        };

        assert_eq!(cp.timestamp, TimestampMs(1000));

        node2.start_round_at(TimestampMs(900)).await;

        broadcast! {
            description: "Leader Node2 second round",
            from: node2, to: [],
            message_matches: ConsensusNetMessage::Prepare(next_cp, next_ticket, cp_view) => {

                assert_eq!(next_cp.timestamp, TimestampMs(900));

                let prepare_msg = node2
                    .consensus
                    .sign_net_message(ConsensusNetMessage::Prepare(next_cp.clone(), next_ticket.clone(), *cp_view))
                    .unwrap();

                assert_contains!(
                    format!("{:#}", node1.handle_msg_err(&prepare_msg).await),
                    "too old"
                );
                assert_contains!(
                    format!("{:#}", node3.handle_msg_err(&prepare_msg).await),
                    "too old"
                );
                assert_contains!(
                    format!("{:#}", node4.handle_msg_err(&prepare_msg).await),
                    "too old"
                );
            }
        };

        // Do a timeout round to be able to repropse another prepare
        simple_timeout_round_at_4(&mut node3, &mut node1, &mut node4).await;

        // Propose a prepare with a timestamp identical to the timestamp of the previous block
        node3.start_round_at(TimestampMs(1000)).await;

        broadcast! {
            description: "Leader Node3 third round",
            from: node3, to: [],
            message_matches: ConsensusNetMessage::Prepare(next_cp, next_ticket, cp_view) => {

                assert_eq!(next_cp.timestamp, TimestampMs(1000));

                let prepare_msg = node3
                    .consensus
                    .sign_net_message(ConsensusNetMessage::Prepare(next_cp.clone(), next_ticket.clone(), *cp_view))
                    .unwrap();

                assert_contains!(
                    format!("{:#}", node1.handle_msg_err(&prepare_msg).await),
                    "too old"
                );

                // Node2 is at 900ms, so it is ok for 1000ms

                assert_contains!(
                    format!("{:#}", node4.handle_msg_err(&prepare_msg).await),
                    "too old"
                );
            }
        };
    }

    #[test_log::test(tokio::test)]
    async fn prepare_timestamp_valid() {
        let (mut node1, mut node2, mut node3, mut node4): (
            ConsensusTestCtx,
            ConsensusTestCtx,
            ConsensusTestCtx,
            ConsensusTestCtx,
        ) = build_nodes!(4).await;

        node1.start_round_at(TimestampMs(1000)).await;

        let (cp, ..) = simple_commit_round! {
            leader: node1,
            followers: [node2, node3, node4]
        };

        assert_eq!(cp.timestamp, TimestampMs(1000));

        node2.start_round_at(TimestampMs(3000)).await;

        assert_none!(node1.consensus.bft_round_state.current_proposal);
        assert_none!(node3.consensus.bft_round_state.current_proposal);
        assert_none!(node4.consensus.bft_round_state.current_proposal);

        broadcast! {
            description: "Leader Node2 second round",
            from: node2, to: [node1, node3, node4],
            message_matches: ConsensusNetMessage::Prepare(next_cp, ..) => {
                assert_eq!(next_cp.timestamp, TimestampMs(3000));
            }
        };

        assert_eq!(
            node1
                .consensus
                .bft_round_state
                .current_proposal
                .as_ref()
                .unwrap()
                .timestamp,
            TimestampMs(3000)
        );
        assert_eq!(
            node3
                .consensus
                .bft_round_state
                .current_proposal
                .as_ref()
                .unwrap()
                .timestamp,
            TimestampMs(3000)
        );
        assert_eq!(
            node4
                .consensus
                .bft_round_state
                .current_proposal
                .as_ref()
                .unwrap()
                .timestamp,
            TimestampMs(3000)
        );
    }

    #[test_log::test(tokio::test)]
    async fn prepare_timestamp_too_futuresque() {
        let (mut node1, mut node2, mut node3, mut node4): (
            ConsensusTestCtx,
            ConsensusTestCtx,
            ConsensusTestCtx,
            ConsensusTestCtx,
        ) = build_nodes!(4).await;

        node1.start_round_at(TimestampMs(1000)).await;

        let (cp, ..) = simple_commit_round! {
            leader: node1,
            followers: [node2, node3, node4]
        };

        assert_eq!(cp.timestamp, TimestampMs(1000));

        node2.start_round_at(TimestampMs(3000)).await;

        assert_none!(node1.consensus.bft_round_state.current_proposal);
        assert_none!(node3.consensus.bft_round_state.current_proposal);
        assert_none!(node4.consensus.bft_round_state.current_proposal);

        // Broadcasted prepare is ignored
        node2.assert_broadcast("Lost prepare").await;

        simple_timeout_round_at_4(&mut node3, &mut node1, &mut node4).await;

        // Legit timestamp increasing previous one of 5000ms (Timeout waiting time)
        node3.start_round_at(TimestampMs(8000)).await;

        let (cp, ticket, cp_view) = simple_commit_round! {
            leader: node3,
            followers: [node2, node1, node4]
        };

        assert_eq!(cp.slot, 2);
        assert_eq!(cp_view, 1);
        assert!(matches!(
            ticket,
            Ticket::TimeoutQC(_, TCKind::NilProposal(..))
        ));

        assert_eq!(
            node1.consensus.bft_round_state.parent_timestamp,
            TimestampMs(8000)
        );
        assert_eq!(
            node2.consensus.bft_round_state.parent_timestamp,
            TimestampMs(8000)
        );
        assert_eq!(
            node4.consensus.bft_round_state.parent_timestamp,
            TimestampMs(8000)
        );

        // Test sending a timestamp in the future
        node3.start_round_at(TimestampMs(10001)).await;

        let prepare_msg = node3
            .assert_broadcast("Prepare with future timestamp")
            .await;

        node1.handle_msg_err(&prepare_msg).await;
        node2.handle_msg_err(&prepare_msg).await;
        node4.handle_msg_err(&prepare_msg).await;
    }

    #[test_log::test(tokio::test)]
    async fn test_candidacy() {
        let (mut node1, mut node2): (ConsensusTestCtx, ConsensusTestCtx) = build_nodes!(2).await;

        // Slot 1
        {
            node1.start_round().await;

            let (cp, ticket, cp_view) = simple_commit_round! {
                leader: node1,
                followers: [node2]
            };

            assert_eq!(cp.slot, 1);
            assert_eq!(cp_view, 0);
            assert!(matches!(ticket, Ticket::Genesis));
        }

        let mut node3 = ConsensusTestCtx::new_node("node-3").await;
        node3.setup_for_joining([&node1, &node2].as_slice());
        node3.consensus.bft_round_state.joining.staking_updated_to = 1;

        // Slot 2: Node3 synchronizes its consensus to the others. - leader = node2
        {
            info!("➡️  Leader proposal");
            node2.start_round().await;

            broadcast! {
                description: "Leader Proposal",
                from: node2, to: [node1, node3]
            };
            send! {
                description: "Prepare Vote",
                from: [node1], to: node2,
                message_matches: ConsensusNetMessage::PrepareVote(_)
            };
            broadcast! {
                description: "Leader Confirm",
                from: node2, to: [node1, node3]
            };
            send! {
                description: "Confirm Ack",
                from: [node1], to: node2,
                message_matches: ConsensusNetMessage::ConfirmAck(_)
            };
            broadcast! {
                description: "Leader Commit",
                from: node2, to: [node1, node3]
            };
        }

        // Slot 3: New slave candidates - leader = node1
        {
            info!("➡️  Leader proposal");
            node1.start_round().await;
            let leader_proposal = node1.assert_broadcast("Leader proposal").await;
            node2.handle_msg(&leader_proposal, "Leader proposal").await;
            node3.handle_msg(&leader_proposal, "Leader proposal").await;
            info!("➡️  Slave vote");
            let slave_vote = node2
                .assert_send(&node1.validator_pubkey(), "Slave vote")
                .await;
            node1.handle_msg(&slave_vote, "Slave vote").await;
            info!("➡️  Leader confirm");
            let leader_confirm = node1.assert_broadcast("Leader confirm").await;
            node2.handle_msg(&leader_confirm, "Leader confirm").await;
            node3.handle_msg(&leader_confirm, "Leader confirm").await;
            info!("➡️  Slave confirm ack");
            let slave_confirm_ack = node2
                .assert_send(&node1.validator_pubkey(), "Slave confirm ack")
                .await;
            node1
                .handle_msg(&slave_confirm_ack, "Slave confirm ack")
                .await;
            info!("➡️  Leader commit");
            let leader_commit = node1.assert_broadcast("Leader commit").await;
            node2.handle_msg(&leader_commit, "Leader commit").await;

            info!("➡️  Slave 2 candidacy");
            node3.with_stake(100, "Add stake").await;
            // This should trigger send_candidacy as we now have stake.
            node3.handle_msg(&leader_commit, "Leader commit").await;
            let slave2_candidacy = node3.assert_broadcast("Slave 2 candidacy").await;
            assert_contains!(
                node1.handle_msg_err(&slave2_candidacy).await.to_string(),
                "validator is not staking"
            );
            node1.add_staker(&node3, 100, "Add staker").await;
            node1
                .handle_msg(&slave2_candidacy, "Slave 2 candidacy")
                .await;
            node2.add_staker(&node3, 100, "Add staker").await;
            node2
                .handle_msg(&slave2_candidacy, "Slave 2 candidacy")
                .await;
        }

        // Slot 4: Still a slot without slave 2 - leader = node 2
        {
            info!("➡️  Leader proposal - Slot 3");
            node2.start_round().await;

            broadcast! {
                description: "Leader Proposal",
                from: node2, to: [node1, node3],
                message_matches: ConsensusNetMessage::Prepare(..) => {
                    assert_eq!(node2.consensus.bft_round_state.staking.bonded().len(), 2);
                }
            };
            send! {
                description: "Prepare Vote",
                from: [node1], to: node2,
                message_matches: ConsensusNetMessage::PrepareVote(_)
            };
            broadcast! {
                description: "Leader Confirm",
                from: node2, to: [node1, node3]
            };
            send! {
                description: "Confirm Ack",
                from: [node1], to: node2,
                message_matches: ConsensusNetMessage::ConfirmAck(_)
            };
            broadcast! {
                description: "Leader Commit",
                from: node2, to: [node1, node3]
            };
        }

        // Slot 5: Slave 2 joined consensus, leader = node-3
        {
            info!("➡️  Leader proposal");
            node3.start_round().await;

            let (cp, ..) = simple_commit_round! {
                leader: node3,
                followers: [node1, node2]
            };
            assert_eq!(cp.slot, 5);
            assert_eq!(node2.consensus.bft_round_state.staking.bonded().len(), 3);
        }

        assert_eq!(node1.consensus.bft_round_state.slot, 6);
        assert_eq!(node2.consensus.bft_round_state.slot, 6);
        assert_eq!(node3.consensus.bft_round_state.slot, 6);
    }

    bus_client! {
        struct TestBC {
            sender(Query<QueryConsensusInfo, ConsensusInfo>),
            sender(NodeStateEvent),
        }
    }

    #[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
    async fn test_consensus_starts_after_genesis_is_processed() {
        let mut node_builder = NodeIntegrationCtxBuilder::new().await;
        node_builder.conf.run_indexer = false;
        node_builder.conf.consensus.solo = false;
        node_builder = node_builder.skip::<NodeStateModule>().skip::<RestApi>();
        let mut node = node_builder.build().await.unwrap();

        let mut bc = TestBC::new_from_bus(node.bus.new_handle()).await;

        let GenesisEvent::GenesisBlock(signed_block) = node.wait_for_genesis_event().await.unwrap()
        else {
            panic!("Expected a GenesisBlock event");
        };

        let block = NodeState::create("test".to_string(), "test")
            .handle_signed_block(signed_block)
            .unwrap();

        // Check that we haven't started the consensus yet
        // (this is awkward to do for now so assume that not receiving an answer is OK)
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(1)) => {}
            _ = bc.request(QueryConsensusInfo {}) => {
                panic!("Consensus should not have started yet");
            }
        }

        bc.send(NodeStateEvent::NewBlock(block)).unwrap();

        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(1)) => {
                panic!("Consensus should have started");
            }
            _ = bc.request(QueryConsensusInfo {}) => {
            }
        }
    }
}

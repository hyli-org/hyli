use anyhow::{bail, Context, Result};
use borsh::{BorshDeserialize, BorshSerialize};
use std::collections::BTreeMap;
use tracing::{debug, info, trace, warn};

use super::*;
#[cfg(not(test))]
use crate::utils::rng::deterministic_rng;
use crate::{
    bus::BusClientSender,
    consensus::StateTag,
    model::{Hashed, Signed, ValidatorPublicKey},
    p2p::P2PCommand,
    utils::conf::TimestampCheck,
};

use hyli_crypto::BlstCrypto;
use hyli_model::{
    utils::TimestampMs, AggregateSignature, ConsensusProposal, ConsensusProposalHash,
    ConsensusStakingAction, Cut, LaneBytesSize, LaneId, SignedByValidator, ValidatorCandidacy,
    View,
};
use hyli_modules::utils::ring_buffer_map::RingBufferMap;

#[derive(BorshSerialize, BorshDeserialize, Default)]
pub(super) struct FollowerState {
    pub(super) buffered_quorum_certificate: Option<CommitQC>, // if we receive a commit before the next prepare
    pub(super) buffered_prepares: BufferedPrepares, // History of seen prepares & buffer of future prepares
}

macro_rules! follower_state {
    ($self:expr) => {
        &mut $self.store.bft_round_state.follower
    };
}

pub(crate) use follower_state;

#[derive(Debug)]
pub(super) enum TicketVerificationError {
    // Ticket is confirmed invalid and should be ignored
    Invalid,
    // Ticket is unverifiable with our data -> buffer
    Unverifiable,
    // Error while trying to advance our state, but the TC is still valid.
    ProcessingError,
}

impl std::fmt::Display for TicketVerificationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TicketVerificationError::Invalid => write!(f, "invalid ticket"),
            TicketVerificationError::Unverifiable => write!(f, "unverifiable ticket"),
            TicketVerificationError::ProcessingError => write!(f, "ticket processing error"),
        }
    }
}

impl Consensus {
    pub(super) fn record_prepare_cache_sizes(&self) {
        self.metrics
            .record_prepare_cache_sizes(self.bft_round_state.follower.buffered_prepares.len());
    }

    #[tracing::instrument(skip(self, consensus_proposal, ticket))]
    pub(super) fn on_prepare(
        &mut self,
        sender: ValidatorPublicKey,
        consensus_proposal: ConsensusProposal,
        ticket: Ticket,
        view: View,
    ) -> Result<()> {
        debug!(
            sender = %sender,
            slot = %self.bft_round_state.slot,
            view = %self.bft_round_state.view,
            "Received Prepare message: {} (Ticket: {})", consensus_proposal, ticket
        );

        if matches!(self.bft_round_state.state_tag, StateTag::Joining) {
            // Shortcut - if this is the prepare we expected, exit joining mode.
            // This is a little eager (as verification can fail), but it should be fine - handled by sync-request-reply.
            // When we're joining, a couple cases to consider:
            // - we're catching up with DA - in this case we pretend to commit all blocks, so we only expect our current BFT slot / CP + 1 (this matches any view).
            let prepare_follows_commit = consensus_proposal.slot == self.bft_round_state.slot
                && !self.current_slot_prepare_is_present();
            // - we restarted but are still up-to-date - it's plausible that we have BFT slot == CP slot, indicating we haven't committed that. Expect slot + 1 or higher views.
            let is_for_current_slot = self.current_slot_prepare_is_present()
                && (consensus_proposal.slot == self.bft_round_state.slot + 1
                    || consensus_proposal.slot == self.bft_round_state.slot
                        && view > self.bft_round_state.view);
            if prepare_follows_commit || is_for_current_slot {
                info!(
                    "Received Prepare message for next slot while joining. Exiting joining mode."
                );
                self.set_state_tag(StateTag::Follower);
            } else {
                follower_state!(self).buffered_prepares.push((
                    sender.clone(),
                    consensus_proposal,
                    ticket,
                    view,
                ));
                self.record_prepare_cache_sizes();
                return Ok(());
            }
        }

        if consensus_proposal.slot < self.bft_round_state.slot {
            // Ignore outdated messages.
            info!(
                "🌑 Outdated Prepare message (Slot {} / view {} while at {}) received. Ignoring.",
                consensus_proposal.slot, view, self.bft_round_state.slot
            );
            return Ok(());
        }

        // Process the ticket
        let cp_hash = consensus_proposal.hashed();
        let parent_hash = consensus_proposal.parent_hash.clone();

        let ticket_processing_result = match &ticket {
            Ticket::Genesis => {
                if self.bft_round_state.slot != 1 {
                    bail!("Genesis ticket is only valid for the first slot.");
                }
                Ok(())
            }
            Ticket::CommitQC(commit_qc) => {
                self.verify_and_process_commit_ticket(&consensus_proposal, commit_qc.clone())
            }
            Ticket::TimeoutQC(timeout_qc, tc_kind_data) => self.verify_and_process_tc_ticket(
                timeout_qc.clone(),
                tc_kind_data,
                consensus_proposal.slot,
                view - 1,
                Some(&consensus_proposal),
                Some(view),
            ),
            els => {
                bail!("Cannot process invalid ticket here {:?}", els);
            }
        };

        match ticket_processing_result {
            Ok(()) => {}
            Err(TicketVerificationError::Unverifiable) => {
                self.buffer_prepare_message(
                    sender.clone(),
                    consensus_proposal.clone(),
                    ticket.clone(),
                    view,
                );
                self.request_missing_parent_prepare(&sender, cp_hash.clone(), parent_hash)?;
                return Ok(());
            }
            Err(TicketVerificationError::Invalid) => {
                // TODO: log the evidence of byzantine behaviour.
                bail!("Ignoring prepare {cp_hash} with invalid ticket by {sender}",);
            }
            Err(TicketVerificationError::ProcessingError) => {
                panic!("Unrecoverable error processing a ticket, stopping now.");
            }
        }

        // TODO: check we haven't voted for a proposal this slot/view already.

        // Sanity check: after processing the ticket, we should be in the right slot/view.
        // TODO: these checks are almost entirely redundant at this point because we process the ticket above.
        if consensus_proposal.slot != self.bft_round_state.slot {
            bail!(
                "Prepare message received for wrong slot ({} found, {} expected)",
                consensus_proposal.slot,
                self.bft_round_state.slot
            );
        }
        if view != self.bft_round_state.view {
            bail!(
                "Prepare message received for wrong view ({} found, {} expected)",
                view,
                self.bft_round_state.view
            );
        }

        // Validate message comes from the correct leader
        // (can't do this earlier as might need to process the ticket first)
        let round_leader = self.round_leader()?;
        if sender != round_leader {
            bail!(
                "Prepare consensus message for {} {} does not come from current leader {}. I won't vote for it.",
                self.bft_round_state.slot,
                self.bft_round_state.view,
                round_leader
            );
        }

        self.verify_poda(&consensus_proposal)?;

        self.verify_staking_actions(&consensus_proposal)?;

        self.verify_timestamp(&consensus_proposal)?;

        // At this point we are OK with this new consensus proposal, update locally and vote.
        self.bft_round_state.current_proposal = Some(consensus_proposal.clone());
        let cp_hash = consensus_proposal.hashed();

        follower_state!(self).buffered_prepares.push((
            sender.clone(),
            consensus_proposal,
            ticket,
            view,
        ));
        self.record_prepare_cache_sizes();

        // If we already have the next Prepare, fast-forward
        if let Some(prepare) = follower_state!(self)
            .buffered_prepares
            .next_prepare(cp_hash.clone())
        {
            debug!(
                "🏎️ Fast forwarding to next Prepare with prepare {:?}",
                prepare
            );
            // FIXME? In theory, we could have a stackoverflow if we need to catchup a lot of prepares
            // Note: If we want to vote on the passed proposal even if it's too late,
            // we can just remove the "return" here and continue.
            return self.on_prepare(prepare.0, prepare.1, prepare.2, prepare.3);
        }

        // Responds PrepareVote message to leader with validator's vote on this proposal
        if self.is_part_of_consensus(self.crypto.validator_pubkey()) {
            debug!(
                proposal_hash = %cp_hash,
                sender = %sender,
                "📤 Slot {} Prepare message validated. Sending PrepareVote to leader",
                self.bft_round_state.slot
            );
            self.send_net_message(
                round_leader,
                self.crypto.sign((cp_hash, PrepareVoteMarker))?.into(),
            )?;
        } else {
            info!(
                "😥 Not part of consensus ({}), not sending PrepareVote",
                self.crypto.validator_pubkey()
            );
        }

        Ok(())
    }

    #[tracing::instrument(skip(self, prepare_quorum_certificate, proposal_hash_hint))]
    pub(super) fn on_confirm(
        &mut self,
        sender: ValidatorPublicKey,
        prepare_quorum_certificate: PrepareQC,
        proposal_hash_hint: ConsensusProposalHash,
    ) -> Result<()> {
        match self.bft_round_state.state_tag {
            StateTag::Follower => {}
            StateTag::Joining => {
                return Ok(());
            }
            _ => {
                debug!(
                    sender = %sender,
                    proposal_hash = %proposal_hash_hint,
                    "Confirm message received while not follower. Ignoring."
                );
                return Ok(());
            }
        }

        if current_proposal!(self).is_none_or(|current| current.hashed() != proposal_hash_hint) {
            warn!(
                proposal_hash = %proposal_hash_hint,
                sender = %sender,
                "Confirm message received for wrong proposal. Ignoring."
            );
            // Note: We might want to buffer it to be able to vote on it if we catchup the proposal
            return Ok(());
        }

        // Check that this is a QC for PrepareVote for the expected proposal.
        // This also checks slot/view as those are part of the hash.
        // TODO: would probably be good to make that more explicit.
        self.verify_quorum_certificate(
            (proposal_hash_hint.clone(), PrepareVoteMarker),
            &prepare_quorum_certificate,
        )?;

        let slot = self.bft_round_state.slot;
        self.bft_round_state
            .timeout
            .update_highest_seen_prepare_qc(slot, prepare_quorum_certificate.clone());

        // Responds ConfirmAck to leader
        if self.is_part_of_consensus(self.crypto.validator_pubkey()) {
            debug!(
                proposal_hash = %proposal_hash_hint,
                sender = %sender,
                "📤 Slot {} Confirm message validated. Sending ConfirmAck to leader",
                self.bft_round_state.slot
            );
            self.send_net_message(
                self.round_leader()?,
                self.crypto
                    .sign((proposal_hash_hint, ConfirmAckMarker))?
                    .into(),
            )?;
        } else {
            info!("😥 Not part of consensus, not sending ConfirmAck");
        }
        Ok(())
    }

    #[tracing::instrument(skip(self, commit_quorum_certificate, proposal_hash_hint))]
    pub(super) fn on_commit(
        &mut self,
        sender: ValidatorPublicKey,
        commit_quorum_certificate: CommitQC,
        proposal_hash_hint: ConsensusProposalHash,
    ) -> Result<()> {
        // Unless joining, ignore commit messages for the wrong slot.
        if !matches!(self.bft_round_state.state_tag, StateTag::Joining)
            && self
                .bft_round_state
                .current_proposal
                .as_ref()
                .map(|cp| cp.slot != self.bft_round_state.slot)
                .unwrap_or(true)
        {
            return Ok(());
        }
        match self.bft_round_state.state_tag {
            StateTag::Leader | StateTag::Follower => {
                if self
                    .bft_round_state
                    .current_proposal
                    .as_ref()
                    .map(|cp| cp.hashed() != proposal_hash_hint)
                    .unwrap_or(true)
                {
                    warn!(
                        "Received Commit for proposal {:?} but expected {:?}. Ignoring.",
                        proposal_hash_hint,
                        self.bft_round_state
                            .current_proposal
                            .as_ref()
                            .map(|cp| cp.hashed())
                    );
                    return Ok(());
                }

                self.verify_commit_quorum_certificate_against_current_proposal(
                    &commit_quorum_certificate,
                )?;
                self.emit_commit_event(&commit_quorum_certificate)?;
                self.advance_round(Ticket::CommitQC(commit_quorum_certificate))
            }
            StateTag::Joining => {
                self.on_commit_while_joining(commit_quorum_certificate, proposal_hash_hint)
            }
        }
    }

    /// Verifies that the proposed cut in the consensus proposal is valid.
    ///
    /// For the cut to be considered valid:
    /// - Each DataProposal associated with a validator must have received sufficient signatures.
    /// - The aggregated signatures for each DataProposal must be valid.
    fn verify_poda(&mut self, consensus_proposal: &ConsensusProposal) -> Result<()> {
        let f = self.bft_round_state.staking.compute_f();

        trace!(
            "verify poda with staking: {:#?}",
            self.bft_round_state.staking
        );

        // Check the cut includes all lanes from the previous cut.
        // TODO: this is not really necessary on the consensus side.
        if self
            .bft_round_state
            .parent_cut
            .iter()
            .any(|(lane_id, _, _, _)| !consensus_proposal.cut.iter().any(|(l, ..)| l == lane_id))
        {
            bail!("Cut does not include all lanes from the previous cut");
        }

        // TODO: would be good to check that the parent cut we're comparing against isn't empty
        // (which can happen on restart-from-scratch).
        // However, the genesis block currently is an empty cut so that won't work.

        for (lane_id, data_proposal_hash, lane_size, poda_sig) in &consensus_proposal.cut {
            let voting_power = self
                .bft_round_state
                .staking
                .compute_voting_power(poda_sig.validators.as_slice());

            // Check that this lane's operator is a known validator.
            // This does not validate the lane suffix.
            // TODO: this prevents ever deleting lane which may or may not be desirable.
            if !self.bft_round_state.staking.is_valid_lane_operator(lane_id) {
                bail!(
                    "Lane {} is in cut but its operator is not a known validator",
                    lane_id
                );
            }

            // If this same data proposal was in the last cut, ignore.
            if let Some(cut) = self
                .bft_round_state
                .parent_cut
                .iter()
                .find(|(v, ..)| v == lane_id)
            {
                if &cut.1 == data_proposal_hash {
                    // TODO: ensure other metadata is the same as the previous cut.
                    debug!(
                        "DataProposal {} from lane {} was already in the last cut, not checking PoDA",
                        data_proposal_hash, lane_id
                    );
                    if &cut.2 != lane_size {
                        bail!(
                            "DataProposal {} from lane {} was already in the last cut but has different size: {} != {}",
                            data_proposal_hash,
                            lane_id,
                            cut.2,
                            lane_size
                        );
                    }
                    continue;
                }
                // Ensure we're not going backwards in the cut.
                if lane_size <= &cut.2 {
                    bail!(
                        "DataProposal {} from lane {} is smaller than the last one in the cut",
                        data_proposal_hash,
                        lane_id
                    );
                }
            }

            trace!("consensus_proposal: {:#?}", consensus_proposal);
            trace!("voting_power: {voting_power} < {f} + 1");

            // Verify that DataProposal received enough votes
            if voting_power < f + 1 {
                bail!(
                    "PoDA for lane {lane_id} does not have enough validators that signed his DataProposal"
                );
            }

            // Verify that PoDA signature is valid
            let msg = (data_proposal_hash.clone(), *lane_size);
            BlstCrypto::verify_aggregate(&Signed {
                msg,
                signature: poda_sig.clone(),
            })
            .context("Failed to verify PoDA")?;
        }
        Ok(())
    }

    fn verify_timestamp(
        &self,
        ConsensusProposal {
            timestamp, slot, ..
        }: &ConsensusProposal,
    ) -> Result<()> {
        let previous_timestamp = self.bft_round_state.parent_timestamp.clone();

        if previous_timestamp == TimestampMs::ZERO {
            warn!(
                "Previous timestamp is zero, accepting {} as next",
                timestamp
            );
            return Ok(());
        }

        let monotonic_check = || {
            if timestamp <= &previous_timestamp {
                bail!(
                    "Timestamp {} too old (should be > {}, {} ms too old)",
                    timestamp,
                    previous_timestamp,
                    (previous_timestamp.clone() - timestamp.clone()).as_millis()
                );
            }

            Ok(())
        };

        match self.config.consensus.timestamp_checks {
            TimestampCheck::NoCheck => {
                return Ok(());
            }
            TimestampCheck::Monotonic => {
                monotonic_check()?;
            }
            TimestampCheck::Full => {
                monotonic_check()?;

                let next_max_timestamp = previous_timestamp.clone()
                    + (self.config.consensus.slot_duration * 2
                        + self.config.consensus.timeout_after * (self.bft_round_state.view as u32));

                if &next_max_timestamp < timestamp {
                    bail!(
                        "Timestamp {} (slot: {}) too late (should be < {}, exceeded by {} ms)",
                        timestamp,
                        slot,
                        next_max_timestamp,
                        (timestamp.clone() - next_max_timestamp.clone()).as_millis()
                    );
                }
            }
        }

        trace!(
            "Consensus Proposal Timestamp verification ok {} -> {} ({} ms between the two rounds)",
            previous_timestamp,
            timestamp,
            (timestamp.clone() - previous_timestamp.clone()).as_millis()
        );

        Ok(())
    }

    fn current_proposal_changes_voting_power(&self) -> bool {
        current_proposal!(self).is_some_and(|cp| {
            cp.staking_actions
                .iter()
                .filter(|sa| matches!(sa, ConsensusStakingAction::Bond { .. }))
                .count()
                > 0
        })
    }

    /// Process a Timeout Certificate ticket.
    /// This functions is called in two paths: the on_prepare (which has some redundant checks)
    /// and the on_timeout_certificate paths (where we must be stricter).
    pub(super) fn verify_and_process_tc_ticket(
        &mut self,
        timeout_qc: TimeoutQC,
        tc_kind_data: &TCKind,
        tc_slot: Slot,
        tc_view: View,
        consensus_proposal: Option<&ConsensusProposal>,
        prepare_view: Option<View>,
    ) -> Result<(), TicketVerificationError> {
        // Cases:
        // - the prepare is for next slot
        //    - *and* we have the prepare for the current slot that matches -> we can process
        //    - otherwise we can't process the certificate
        // - the prepare is for the current slot -> we can process

        // Let's bail early on obviously invalid TCs
        // (we need to account for possible commit on our side to allow tc_view = view or tc_view = view - 1).
        if tc_slot < self.bft_round_state.slot || tc_slot > self.bft_round_state.slot + 1 {
            debug!(
                "Timeout Certificate slot {} view {} is not correct for current slot {} view {}",
                tc_slot, tc_view, self.bft_round_state.slot, self.bft_round_state.view,
            );
            if tc_slot > self.bft_round_state.slot + 1 {
                return Err(TicketVerificationError::Unverifiable);
            }
            return Err(TicketVerificationError::Invalid);
        }

        // Validity check: if we are processing a new Prepare, the ticket must be
        // either a NilProposal or the PrepareQC must match.
        if let Some(consensus_proposal) = consensus_proposal {
            if let TCKind::PrepareQC((_, cp)) = tc_kind_data {
                if cp != consensus_proposal {
                    debug!("Timeout Certificate does not match consensus proposal. Expected {}, got {}",
                        cp.hashed(),
                        consensus_proposal.hashed()
                    );
                    return Err(TicketVerificationError::Invalid);
                }
            }
            // Not processing a new prepare so we can accept either type of ticket.
        }

        tracing::debug!(
            "Slot info service: TC for slot {} view {}, bft round state slot {} {}, current proposal slot {:?}",
            tc_slot,
            tc_view,
            self.bft_round_state.slot,
            self.bft_round_state.view,
            self.bft_round_state.current_proposal.as_ref().map(|cp| cp.slot),
        );

        if tc_slot == self.bft_round_state.slot {
            // Let's check the view. This is actually a bit tricky.
            let mut skip_round_advance = false;
            if let Some(pv) = prepare_view {
                // The prepare for a TC must be for the next view.
                if pv != tc_view + 1 {
                    debug!("Invalid prepare view {pv} for TC view {tc_view}");
                    return Err(TicketVerificationError::Invalid);
                }
                match (self.bft_round_state.view, pv) {
                    // Already at prepare view -> nothing to do after verification.
                    (lv, pv) if lv == pv => skip_round_advance = true,
                    // Commit case: we're on tc_view or it's a future view, will advance to pv.
                    (lv, _) if lv <= tc_view => {}
                    // Other cases (we're farther ahead, ...) - invalid
                    _ => {
                        debug!("
                            Unexpected view combo: local {}, tc {tc_view}, prepare {pv} (expected local == tc or local == prepare)",
                            self.bft_round_state.view
                        );
                        return Err(TicketVerificationError::Invalid);
                    }
                }
            } else {
                // on_timeout_certificate path: we may already have advanced one view (commit path),
                // or be behind; accept tc_view in [local_view - 1, +inf).
                if self.bft_round_state.view == tc_view + 1 {
                    skip_round_advance = true
                } else if self.bft_round_state.view > tc_view {
                    debug!(
                        "Timeout Certificate slot {tc_slot} view {tc_view} is not correct for current slot {} view {}",
                        self.bft_round_state.slot,
                        self.bft_round_state.view,
                    );
                    return Err(TicketVerificationError::Invalid);
                }
            }

            // Back to the regular case -> this is a TC for the current or future views.
            // Staking is stable across views so we can verify it.
            self.verify_tc(
                &timeout_qc,
                tc_kind_data,
                self.bft_round_state.slot,
                tc_view,
                self.bft_round_state.parent_hash.clone(),
            )
            .map_err(|e| {
                debug!("Invalid TC: {}", e);
                TicketVerificationError::Invalid
            })?;

            if let TCKind::PrepareQC((qc, cp)) = tc_kind_data {
                // Update prepare QC & local CP
                if self
                    .store
                    .bft_round_state
                    .timeout
                    .update_highest_seen_prepare_qc(tc_slot, qc.clone())
                {
                    // Update our consensus proposal
                    self.bft_round_state.current_proposal = Some(cp.clone());
                    debug!("Highest seen PrepareQC updated");
                }
            }

            // Process it
            debug!(
                "Timeout Certificate for view {} received, processing it",
                tc_view
            );

            // This TC is for our current slot, so we can leave Joining mode if needed
            // (not used on the on_prepare path but useful on the on_tc path)
            if matches!(self.bft_round_state.state_tag, StateTag::Joining) {
                debug!("Leaving Joining mode after timeout certificate");
                self.set_state_tag(StateTag::Follower);
            }

            // If we already advanced to the prepare's view, nothing left to do.
            if skip_round_advance {
                return Ok(());
            }

            // Fake our view so we fast-forward properly, see TODO in apply_ticket.
            self.bft_round_state.view = tc_view;
            self.advance_round(Ticket::TimeoutQC(timeout_qc, tc_kind_data.clone()))
                .map_err(|e| {
                    debug!("Error advancing round with TC: {e}");
                    TicketVerificationError::ProcessingError
                })?;

            return Ok(());
        }

        // Special case for TC for next slot.
        // We can process TCs for the next slot iif we locally have the corresponding CP,
        // otherwise we can't fast-forward as there might be unknown staking changes.
        let local_cp_to_commit = match (current_proposal!(self), consensus_proposal) {
            (Some(local_cp), Some(tc_cp)) if local_cp.hashed() == tc_cp.parent_hash => {
                tc_cp.parent_hash.clone()
            }
            _ => {
                debug!("Cannot verify TC for next slot {tc_slot} view {tc_view}, no local prepare / different local prepare");
                return Err(TicketVerificationError::Unverifiable);
            }
        };

        // We can fast forward.

        // TODO: In theory we could commit our local prepare, update staking, check the TC,
        // but we would need to revert if there is an error, so that sounds annoying for now.
        // Let's just assert that our current proposal wouldn't change staking (it generally won't).
        if self.current_proposal_changes_voting_power() {
            debug!("Timeout Certificate slot {tc_slot} view {tc_view} is for the next slot, and current proposal changes voting power");
            return Err(TicketVerificationError::Unverifiable);
        }

        self.verify_tc(
            &timeout_qc,
            tc_kind_data,
            tc_slot,
            tc_view,
            local_cp_to_commit,
        )
        .map_err(|e| {
            debug!("Invalid TC for fast-forward: {e}");
            TicketVerificationError::Invalid
        })?;

        // Emit a placeholder commit event so downstream consumers see the fast-forwarded commit
        // even though we don't have a real commit QC for this parent.
        let placeholder_commit_qc =
            QuorumCertificate(AggregateSignature::default(), ConfirmAckMarker);

        // This TC is for our current slot, so we can leave Joining mode if needed
        if matches!(self.bft_round_state.state_tag, StateTag::Joining) {
            self.set_state_tag(StateTag::Follower);
        }

        self.emit_commit_event(&placeholder_commit_qc)
            .map_err(|e| {
                debug!("Error emitting commit event during FF: {e}");
                TicketVerificationError::ProcessingError
            })?;
        // Pass in the new view
        self.advance_round(Ticket::ForcedCommitQC(tc_view + 1))
            .map_err(|e| {
                debug!("Error advancing round during FF: {e}");
                TicketVerificationError::ProcessingError
            })?;

        info!(
            "🔀 Fast forwarded to slot {} view {}",
            &self.bft_round_state.slot, &self.bft_round_state.view,
        );
        Ok(())
    }

    fn on_commit_while_joining(
        &mut self,
        commit_quorum_certificate: CommitQC,
        proposal_hash_hint: ConsensusProposalHash,
    ) -> Result<()> {
        // We are joining consensus, try to sync our state.
        let Some((_, potential_proposal, _, _)) = self
            .store
            .bft_round_state
            .follower
            .buffered_prepares
            .get(&proposal_hash_hint)
        else {
            // Maybe we just missed it, carry on.
            return Ok(());
        };

        // Check it's actually in the future
        if potential_proposal.slot <= self.bft_round_state.slot {
            info!(
                "🏃Ignoring commit message, we are at slot {}, already beyond {}",
                self.bft_round_state.slot, potential_proposal.slot
            );
            return Ok(());
        }

        // At this point check that we're caught up enough that it's realistic to verify the QC.
        if self.bft_round_state.joining.staking_updated_to + 1 < potential_proposal.slot {
            info!(
                "🏃Ignoring commit message, we are only caught up to {} ({} needed).",
                self.bft_round_state.joining.staking_updated_to,
                potential_proposal.slot - 1
            );
            return Ok(());
        }

        self.store.bft_round_state.current_proposal = Some(potential_proposal.clone());

        info!(
            "📦 Commit message received for slot {}, trying to synchronize.",
            potential_proposal.slot
        );

        // Try to commit the proposal
        self.verify_commit_quorum_certificate_against_current_proposal(&commit_quorum_certificate)
            .context("On Commit when joining")?;

        self.bft_round_state.slot = potential_proposal.slot;
        self.bft_round_state.view = 0; // TODO
        self.set_state_tag(StateTag::Follower);

        self.emit_commit_event(&commit_quorum_certificate)?;

        self.advance_round(Ticket::CommitQC(commit_quorum_certificate))?;

        // We sucessfully joined the consensus
        info!("🏁 Synchronized to slot {}", self.bft_round_state.slot);
        Ok(())
    }

    pub(super) fn has_no_buffered_children(&self) -> bool {
        self.store
            .bft_round_state
            .follower
            .buffered_prepares
            .next_prepare(self.bft_round_state.parent_hash.clone())
            .is_none()
    }

    /// Buffer a prepare if we don't already have it.
    fn buffer_prepare_message(
        &mut self,
        sender: ValidatorPublicKey,
        consensus_proposal: ConsensusProposal,
        ticket: Ticket,
        view: View,
    ) {
        let cp_hash = consensus_proposal.hashed();
        if !follower_state!(self).buffered_prepares.contains(&cp_hash) {
            follower_state!(self).buffered_prepares.push((
                sender,
                consensus_proposal,
                ticket,
                view,
            ));
            self.record_prepare_cache_sizes();
        }
    }

    /// Request the first missing parent prepare on the path to our current DP, if any.
    fn request_missing_parent_prepare(
        &mut self,
        sender: &ValidatorPublicKey,
        cp_hash: ConsensusProposalHash,
        mut missing_dp_hash: ConsensusProposalHash,
    ) -> Result<()> {
        // Check if we have a missing DP up to our current known DP (this assumes we're not on a fork)
        let current_dp_hash = current_proposal!(self)
            .map(|cp| cp.hashed())
            .unwrap_or_else(|| self.bft_round_state.parent_hash.clone());

        // TODO: we should switch back to joining if we try to catch up on too many prepares.
        while let Some(prep) = follower_state!(self)
            .buffered_prepares
            .get(&missing_dp_hash)
        {
            if missing_dp_hash == current_dp_hash {
                return Ok(());
            }
            missing_dp_hash = prep.1.parent_hash.clone();
        }

        debug!(
            to = %sender,
            "🔉 Requesting missing parent prepare {} for proposal {}",
            missing_dp_hash,
            cp_hash,
        );
        // We use send instead of broadcast to avoid an exponential number of messages
        // TODO: improve on this.
        #[cfg(not(test))]
        let node_to_ask = {
            use rand::prelude::IteratorRandom;
            let bonded = self.bft_round_state.staking.bonded();
            if bonded.is_empty() {
                None
            } else {
                let mut rng = deterministic_rng();
                bonded
                    .iter()
                    .filter(|pk| pk != &self.crypto.validator_pubkey())
                    .choose(&mut rng)
                    .cloned()
            }
        };
        // For test deterministically always use the sender
        #[cfg(test)]
        let node_to_ask = Some(sender.clone());
        let mess = ConsensusNetMessage::SyncRequest(missing_dp_hash.clone());
        match node_to_ask {
            Some(n) => self.send_net_message(n, mess),
            None => self.broadcast_net_message(mess),
        }
        .context("Sending SyncRequest")?;

        Ok(())
    }

    fn verify_and_process_commit_ticket(
        &mut self,
        consensus_proposal: &ConsensusProposal,
        commit_qc: CommitQC,
    ) -> Result<(), TicketVerificationError> {
        // Two cases:
        // - the prepare is for next slot *and* we have the prepare for the current slot
        // - the prepare is for the current slot
        let next_slot_and_current_proposal_is_present = consensus_proposal.slot
            == self.bft_round_state.slot + 1
            && self.current_slot_prepare_is_present();

        if (consensus_proposal.slot == self.bft_round_state.slot)
            || next_slot_and_current_proposal_is_present
        {
            // Three options:
            // - we have already received the commit message for this ticket, so we already processed the QC.
            // - we haven't, so we process it right away
            // - the CQC is invalid and we just ignore it.
            if let Some(qc) = &self.bft_round_state.follower.buffered_quorum_certificate {
                if qc == &commit_qc {
                    return Ok(());
                }
            }

            // Edge case: we have already committed a different CQC for the CP
            // Let's verify that one too - the staking _must_be the same.
            if consensus_proposal.slot == self.bft_round_state.slot {
                self.verify_quorum_certificate(
                    (self.bft_round_state.parent_hash.clone(), ConfirmAckMarker),
                    &commit_qc,
                )
                .map_err(|e| {
                    debug!("Commit QC verification failed for current slot: {e}");
                    TicketVerificationError::Invalid
                })?;
                return Ok(());
            }

            // Now we are considering fast-forwarding. We can only do this if the commit QC
            // matches our consensus proposal - or there might be staking changes.
            // However we don't actually know what consensus proposal hash was used for signing here,
            // so all we can do is treat it as "not processed" if verification fails.
            if let Err(err) =
                self.verify_commit_quorum_certificate_against_current_proposal(&commit_qc)
            {
                warn!(
                    "Commit QC verification failed for current proposal: {err}. Buffering and requesting missing parent."
                );
                return Err(TicketVerificationError::Unverifiable);
            }
            self.emit_commit_event(&commit_qc).map_err(|e| {
                debug!("Error emitting commit event: {e}");
                TicketVerificationError::ProcessingError
            })?;
            self.advance_round(Ticket::CommitQC(commit_qc))
                .map_err(|e| {
                    debug!("Error advancing round after commit QC: {e}");
                    TicketVerificationError::ProcessingError
                })?;

            info!("🔀 Fast forwarded to slot {}", &self.bft_round_state.slot);
            return Ok(());
        }

        Err(TicketVerificationError::Unverifiable)
    }

    fn verify_staking_actions(&mut self, proposal: &ConsensusProposal) -> Result<()> {
        for action in &proposal.staking_actions {
            match action {
                ConsensusStakingAction::Bond { candidate } => {
                    self.verify_new_validators_to_bond(candidate)?;
                }
                ConsensusStakingAction::PayFeesForDaDi {
                    lane_id,
                    cumul_size,
                } => Self::verify_dadi_fees(&proposal.cut, lane_id, cumul_size)?,
            }
        }
        Ok(())
    }

    /// Verify that the fees paid by the disseminator are correct
    fn verify_dadi_fees(cut: &Cut, lane_id: &LaneId, cumul_size: &LaneBytesSize) -> Result<()> {
        cut.iter()
            .find(|l| &l.0 == lane_id && &l.2 == cumul_size)
            .map(|_| ())
            .ok_or(anyhow::anyhow!(
                "Malformed PayFeesForDadi. Not found in cut: {lane_id}, {cumul_size}"
            ))
    }

    /// Verify that new validators have enough stake
    /// and have a valid signature so can be bonded.
    fn verify_new_validators_to_bond(
        &mut self,
        new_validator: &SignedByValidator<ValidatorCandidacy>,
    ) -> Result<()> {
        let pubkey = &new_validator.signature.validator;
        // Verify that the new validator has enough stake
        if let Some(stake) = self.bft_round_state.staking.get_stake(pubkey) {
            if stake < staking::state::MIN_STAKE {
                bail!("New bonded validator has not enough stake to be bonded");
            }
        } else {
            bail!("New bonded validator has no stake");
        }
        // Verify that the new validator has a valid signature
        BlstCrypto::verify(new_validator)
            .context("New bonded validator has an invalid signature")?;
        // This validator is being bonded, so we can drop it from our own list of candidates.
        self.validator_candidates
            .retain(|v| v.signature.validator != new_validator.signature.validator);
        self.bus.send(P2PCommand::ConnectTo {
            peer: new_validator.msg.peer_address.clone(),
        })?;
        Ok(())
    }
}

pub type Prepare = (ValidatorPublicKey, ConsensusProposal, Ticket, View);

#[derive(BorshSerialize, BorshDeserialize, Default, Debug)]
pub(super) struct BufferedPrepares {
    prepares: RingBufferMap<ConsensusProposalHash, Prepare>,
    children: BTreeMap<ConsensusProposalHash, ConsensusProposalHash>,
}

impl BufferedPrepares {
    fn contains(&self, proposal_hash: &ConsensusProposalHash) -> bool {
        self.prepares.contains_key(proposal_hash)
    }

    pub(super) fn len(&self) -> usize {
        self.prepares.len()
    }

    pub(super) fn set_max_size(&mut self, max: Option<usize>) {
        self.prepares.set_max_size(max);
    }

    pub(super) fn push(&mut self, prepare_message: Prepare) {
        trace!(
            proposal_hash = %prepare_message.1.hashed(),
            "Buffering Prepare message"
        );
        let proposal_hash = prepare_message.1.hashed();
        let parent_hash = prepare_message.1.parent_hash.clone();
        // If a children exists for the parent we update it
        // TODO: make this more trustless, if we receive a prepare from a byzantine validator
        // we might get stuck
        self.children
            .entry(parent_hash)
            .and_modify(|h| {
                *h = proposal_hash.clone();
            })
            .or_insert(proposal_hash.clone());
        self.prepares.insert(proposal_hash, prepare_message);
    }

    pub(super) fn get(&self, proposal_hash: &ConsensusProposalHash) -> Option<&Prepare> {
        self.prepares.get(proposal_hash)
    }

    fn next_prepare(&self, proposal_hash: ConsensusProposalHash) -> Option<Prepare> {
        self.children
            .get(&proposal_hash)
            .and_then(|children| self.prepares.get(children).cloned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bus::dont_use_this::get_receiver;
    use crate::bus::SharedMessageBus;
    use crate::consensus::test::ConsensusTestCtx;
    use crate::consensus::*;
    use crate::p2p::network::{MsgWithHeader, NetMessage, OutboundMessage};
    use hyli_crypto::BlstCrypto;

    /// Helper to build a single-validator aggregate signature for the given message.
    fn agg_sig<T: borsh::BorshSerialize + Clone>(
        crypto: &BlstCrypto,
        msg: T,
    ) -> AggregateSignature {
        let signed = crypto.sign(msg.clone()).expect("sign");
        BlstCrypto::aggregate(msg, &[&signed])
            .expect("aggregate")
            .signature
    }

    #[test]
    fn test_contains() {
        let mut buffered_prepares = BufferedPrepares::default();
        let proposal = ConsensusProposal::default();
        let proposal_hash = proposal.hashed();
        assert!(!buffered_prepares.contains(&proposal_hash));

        let prepare_message = (
            ValidatorPublicKey::default(),
            proposal,
            Ticket::CommitQC(QuorumCertificate(
                AggregateSignature::default(),
                ConfirmAckMarker,
            )),
            0,
        );
        buffered_prepares.push(prepare_message);
        assert!(buffered_prepares.contains(&proposal_hash));
    }

    #[test]
    fn test_push_and_get() {
        let mut buffered_prepares = BufferedPrepares::default();
        let proposal = ConsensusProposal::default();
        let proposal_hash = proposal.hashed();

        let prepare_message = (
            ValidatorPublicKey::default(),
            proposal.clone(),
            Ticket::CommitQC(QuorumCertificate(
                AggregateSignature::default(),
                ConfirmAckMarker,
            )),
            0,
        );
        buffered_prepares.push(prepare_message);

        let retrieved_message = buffered_prepares.get(&proposal_hash);
        assert!(retrieved_message.is_some());
        let retrieved_message = retrieved_message.unwrap();
        assert_eq!(retrieved_message.1, proposal);
    }

    #[test_log::test(tokio::test)]
    async fn test_request_missing_parent_prepare_with_gaps() {
        // Create a chain of proposals
        let current_proposal = ConsensusProposal::default();
        let missing_prepare1 = ConsensusProposal {
            parent_hash: current_proposal.hashed(),
            ..ConsensusProposal::default()
        };
        let buffered_prepare = ConsensusProposal {
            parent_hash: missing_prepare1.hashed(),
            ..ConsensusProposal::default()
        };
        let missing_prepare2 = ConsensusProposal {
            parent_hash: buffered_prepare.hashed(),
            ..ConsensusProposal::default()
        };
        let just_received_prepare = ConsensusProposal {
            parent_hash: missing_prepare2.hashed(),
            ..ConsensusProposal::default()
        };

        let bus = SharedMessageBus::new();
        let mut sync_request_rv = get_receiver::<OutboundMessage>(&bus).await;

        let mut consensus =
            ConsensusTestCtx::build_consensus(&bus, BlstCrypto::new("test-node").unwrap()).await;

        consensus.bft_round_state.current_proposal = Some(current_proposal);

        // Add the buffered prepare to the state
        follower_state!(consensus).buffered_prepares.push((
            ValidatorPublicKey::default(),
            buffered_prepare,
            Ticket::Genesis,
            0,
        ));

        // Buffer the just received prepare and request missing parents
        consensus.buffer_prepare_message(
            ValidatorPublicKey::default(),
            just_received_prepare.clone(),
            Ticket::Genesis,
            0,
        );
        consensus
            .request_missing_parent_prepare(
                &ValidatorPublicKey::default(),
                just_received_prepare.hashed(),
                just_received_prepare.parent_hash.clone(),
            )
            .unwrap();

        // We should request missing_prepare2
        match sync_request_rv.recv().await.map(|msg| msg.into_message()) {
            Ok(OutboundMessage::SendMessage {
                msg:
                    NetMessage::ConsensusMessage(MsgWithHeader::<ConsensusNetMessage> {
                        msg: ConsensusNetMessage::SyncRequest(hash),
                        ..
                    }),
                ..
            }) => {
                assert_eq!(
                    hash,
                    missing_prepare2.hashed(),
                    "First sync request should be for missing_prepare2"
                );
            }
            _ => panic!("Expected SyncRequest for missing_prepare2"),
        }

        // Add the buffered prepare to the state
        follower_state!(consensus).buffered_prepares.push((
            ValidatorPublicKey::default(),
            missing_prepare2.clone(),
            Ticket::Genesis,
            0,
        ));

        // Request again starting from missing_prepare2
        consensus
            .request_missing_parent_prepare(
                &ValidatorPublicKey::default(),
                missing_prepare2.hashed(),
                missing_prepare2.parent_hash.clone(),
            )
            .unwrap();

        // We should request missing_prepare1
        match sync_request_rv.recv().await.map(|msg| msg.into_message()) {
            Ok(OutboundMessage::SendMessage {
                msg:
                    NetMessage::ConsensusMessage(MsgWithHeader::<ConsensusNetMessage> {
                        msg: ConsensusNetMessage::SyncRequest(hash),
                        ..
                    }),
                ..
            }) => {
                assert_eq!(
                    hash,
                    missing_prepare1.hashed(),
                    "First sync request should be for missing_prepare1"
                );
            }
            _ => panic!("Expected SyncRequest for missing_prepare1"),
        }
    }

    #[test]
    fn test_buffered_prepares_trim_limit() {
        let mut buffered_prepares = BufferedPrepares::default();
        let limit = 2;
        let p1 = ConsensusProposal {
            parent_hash: b"p1-parent".into(),
            ..ConsensusProposal::default()
        };
        let p2 = ConsensusProposal {
            parent_hash: b"p2-parent".into(),
            ..ConsensusProposal::default()
        };
        let p3 = ConsensusProposal {
            parent_hash: b"p3-parent".into(),
            ..ConsensusProposal::default()
        };

        let msg = |proposal: ConsensusProposal| {
            (ValidatorPublicKey::default(), proposal, Ticket::Genesis, 0)
        };

        buffered_prepares.set_max_size(Some(limit));
        buffered_prepares.push(msg(p1.clone()));
        buffered_prepares.push(msg(p2.clone()));
        buffered_prepares.push(msg(p3.clone()));

        assert!(buffered_prepares.get(&p1.hashed()).is_none());
        assert!(buffered_prepares.get(&p2.hashed()).is_some());
        assert!(buffered_prepares.get(&p3.hashed()).is_some());
    }

    #[test]
    fn test_buffered_prepares_limit_eviction() {
        let mut buffered_prepares = BufferedPrepares::default();
        buffered_prepares.set_max_size(Some(1));

        let p1 = ConsensusProposal {
            parent_hash: b"roundtrip-p1".into(),
            ..ConsensusProposal::default()
        };
        let p2 = ConsensusProposal {
            parent_hash: b"roundtrip-p2".into(),
            ..ConsensusProposal::default()
        };
        let p3 = ConsensusProposal {
            parent_hash: b"roundtrip-p3".into(),
            ..ConsensusProposal::default()
        };

        let msg = |proposal: ConsensusProposal| {
            (ValidatorPublicKey::default(), proposal, Ticket::Genesis, 0)
        };

        buffered_prepares.push(msg(p1.clone()));
        buffered_prepares.push(msg(p2.clone()));
        buffered_prepares.push(msg(p3.clone()));

        assert!(buffered_prepares.get(&p1.hashed()).is_none());
        assert!(buffered_prepares.get(&p2.hashed()).is_none());
        assert!(buffered_prepares.get(&p3.hashed()).is_some());
    }

    #[test_log::test(tokio::test)]
    async fn prepare_with_timeout_qc_is_verified() {
        // Build a single validator node configured as follower.
        let crypto = BlstCrypto::new("node-tc-verify").unwrap();
        let mut node = ConsensusTestCtx::new("node-tc-verify", crypto.clone()).await;
        node.setup_node(0, &[crypto]);
        node.consensus.bft_round_state.state_tag = StateTag::Follower;
        node.consensus.bft_round_state.slot = 1;
        node.consensus.bft_round_state.view = 1;
        node.consensus.bft_round_state.parent_hash = ConsensusProposalHash("parent".into());

        // Prepare for slot 1 / view 1 referencing the same parent cut as the node.
        let consensus_proposal = ConsensusProposal {
            slot: 1,
            parent_hash: node.consensus.bft_round_state.parent_hash.clone(),
            cut: node.consensus.bft_round_state.parent_cut.clone(),
            ..ConsensusProposal::default()
        };

        // Malformed TimeoutQC (no validators, empty signature) + NIL TCKind.
        let timeout_qc = QuorumCertificate(AggregateSignature::default(), ConsensusTimeoutMarker);
        let tc_kind = TCKind::NilProposal(QuorumCertificate(
            AggregateSignature::default(),
            NilConsensusTimeoutMarker,
        ));

        let result = node.consensus.on_prepare(
            node.validator_pubkey(),
            consensus_proposal,
            Ticket::TimeoutQC(timeout_qc, tc_kind),
            1,
        );

        let err = result.expect_err("TimeoutQC should be verified and rejected");
        let root = err.root_cause().to_string();
        assert!(
            root.contains("Ignoring prepare"),
            "TC should fail verification, got: {root}"
        );
    }

    #[test_log::test(tokio::test)]
    async fn tc_with_prepare_view_skips_round_and_updates_prepare_qc() {
        let crypto = BlstCrypto::new("tc-skip").unwrap();
        let mut ctx = ConsensusTestCtx::new("tc-skip", crypto.clone()).await;
        ctx.add_trusted_validator(&ctx.pubkey());

        ctx.consensus.bft_round_state.state_tag = StateTag::Follower;
        ctx.consensus.bft_round_state.slot = 10;
        ctx.consensus.bft_round_state.view = 2;
        ctx.consensus.bft_round_state.parent_hash = ConsensusProposalHash("parent".into());

        let cp = ConsensusProposal {
            slot: 10,
            parent_hash: ctx.consensus.bft_round_state.parent_hash.clone(),
            ..ConsensusProposal::default()
        };
        ctx.consensus.store.bft_round_state.current_proposal = Some(cp.clone());

        let prepare_qc = QuorumCertificate(
            agg_sig(&crypto, (cp.hashed(), PrepareVoteMarker)),
            PrepareVoteMarker,
        );
        let timeout_qc = QuorumCertificate(
            agg_sig(
                &crypto,
                (
                    10_u64,
                    1_u64,
                    ctx.consensus.bft_round_state.parent_hash.clone(),
                    ConsensusTimeoutMarker,
                ),
            ),
            ConsensusTimeoutMarker,
        );

        ctx.consensus
            .verify_and_process_tc_ticket(
                timeout_qc,
                &TCKind::PrepareQC((prepare_qc.clone(), cp.clone())),
                10,
                1,
                Some(&cp),
                Some(2),
            )
            .expect("TC should be accepted");

        // View stays at prepare view and highest seen prepare QC is recorded.
        assert_eq!(ctx.consensus.bft_round_state.view, 2);
        assert_eq!(
            ctx.consensus
                .store
                .bft_round_state
                .timeout
                .highest_seen_prepare_qc
                .as_ref()
                .map(|(slot, _)| *slot),
            Some(10)
        );
        assert_eq!(
            ctx.consensus
                .store
                .bft_round_state
                .timeout
                .highest_seen_prepare_qc
                .as_ref()
                .map(|(_, qc)| qc.clone()),
            Some(prepare_qc)
        );
        assert_eq!(
            ctx.consensus.bft_round_state.current_proposal.as_ref(),
            Some(&cp)
        );
    }

    #[test_log::test(tokio::test)]
    async fn tc_prepare_view_mismatch_is_rejected() {
        let crypto = BlstCrypto::new("tc-bad-pv").unwrap();
        let mut ctx = ConsensusTestCtx::new("tc-bad-pv", crypto.clone()).await;
        ctx.add_trusted_validator(&ctx.pubkey());

        ctx.consensus.bft_round_state.state_tag = StateTag::Follower;
        ctx.consensus.bft_round_state.slot = 3;
        ctx.consensus.bft_round_state.view = 2;
        ctx.consensus.bft_round_state.parent_hash = ConsensusProposalHash("p".into());

        let cp = ConsensusProposal {
            slot: 3,
            parent_hash: ctx.consensus.bft_round_state.parent_hash.clone(),
            ..ConsensusProposal::default()
        };
        ctx.consensus.store.bft_round_state.current_proposal = Some(cp.clone());

        let prepare_qc = QuorumCertificate(
            agg_sig(&crypto, (cp.hashed(), PrepareVoteMarker)),
            PrepareVoteMarker,
        );
        let timeout_qc = QuorumCertificate(
            agg_sig(
                &crypto,
                (
                    3_u64,
                    0_u64,
                    ctx.consensus.bft_round_state.parent_hash.clone(),
                    ConsensusTimeoutMarker,
                ),
            ),
            ConsensusTimeoutMarker,
        );

        let err = ctx
            .consensus
            .verify_and_process_tc_ticket(
                timeout_qc,
                &TCKind::PrepareQC((prepare_qc, cp.clone())),
                3,
                0,
                Some(&cp),
                Some(3), // prepare view should have been tc_view + 1 = 1
            )
            .expect_err("Mismatch should be rejected");

        assert!(matches!(err, TicketVerificationError::Invalid));
        // State unchanged
        assert_eq!(ctx.consensus.bft_round_state.view, 2);
    }

    #[test_log::test(tokio::test)]
    async fn tc_on_timeout_rejects_when_already_two_views_ahead() {
        let crypto = BlstCrypto::new("tc-ahead").unwrap();
        let mut ctx = ConsensusTestCtx::new("tc-ahead", crypto.clone()).await;
        ctx.add_trusted_validator(&ctx.pubkey());

        ctx.consensus.bft_round_state.state_tag = StateTag::Follower;
        ctx.consensus.bft_round_state.slot = 5;
        ctx.consensus.bft_round_state.view = 4; // already two views ahead of tc_view below
        ctx.consensus.bft_round_state.parent_hash = ConsensusProposalHash("p".into());

        let timeout_qc = QuorumCertificate(
            agg_sig(
                &crypto,
                (
                    5_u64,
                    2_u64,
                    ctx.consensus.bft_round_state.parent_hash.clone(),
                    ConsensusTimeoutMarker,
                ),
            ),
            ConsensusTimeoutMarker,
        );

        let err = ctx
            .consensus
            .verify_and_process_tc_ticket(
                timeout_qc,
                &TCKind::NilProposal(QuorumCertificate(
                    AggregateSignature::default(),
                    NilConsensusTimeoutMarker,
                )),
                5,
                2,
                None,
                None,
            )
            .expect_err("should reject TC when already >1 view ahead");

        assert!(matches!(err, TicketVerificationError::Invalid));
        assert_eq!(ctx.consensus.bft_round_state.view, 4);
    }

    #[test_log::test(tokio::test)]
    async fn commit_qc_mismatch_buffers_and_requests_sync() {
        // Node holds an uncommitted CP for slot 5. The network committed a different CP
        // and now sends the slot-6 Prepare carrying that CQC. Verification must fail,
        // causing the follower to buffer and issue a SyncRequest for the missing parent.
        let mut ctx =
            ConsensusTestCtx::new("node-sync", BlstCrypto::new("node-sync").unwrap()).await;
        ctx.add_trusted_validator(&ctx.pubkey());
        ctx.consensus.bft_round_state.state_tag = StateTag::Follower;
        ctx.consensus.bft_round_state.slot = 5;
        ctx.consensus.bft_round_state.view = 0;

        let local_parent = ConsensusProposalHash("local-parent".into());
        let local_cp = ConsensusProposal {
            slot: 5,
            parent_hash: local_parent.clone(),
            timestamp: TimestampMs(1),
            ..Default::default()
        };
        ctx.consensus.bft_round_state.parent_hash = local_parent;
        ctx.consensus.store.bft_round_state.current_proposal = Some(local_cp);

        let missing_parent = ConsensusProposalHash("committed-elsewhere".into());
        let incoming_prepare = ConsensusProposal {
            slot: 6,
            parent_hash: missing_parent.clone(),
            timestamp: TimestampMs(2),
            ..Default::default()
        };
        // NB - this test doesn't quite check the failure mode we had in practice
        // because the verifications fails to aggregate the signature, not ends up invalid,
        // but it behaves identically in the end.
        let commit_qc = QuorumCertificate(AggregateSignature::default(), ConfirmAckMarker);

        let _ = ctx.consensus.on_prepare(
            ctx.pubkey(),
            incoming_prepare.clone(),
            Ticket::CommitQC(commit_qc),
            0,
        );

        // The prepare should be buffered.
        assert!(ctx
            .consensus
            .bft_round_state
            .follower
            .buffered_prepares
            .contains(&incoming_prepare.hashed()));

        // And a SyncRequest for the missing parent should be sent to the sender.
        let outbound = ctx
            .out_receiver
            .try_recv()
            .expect("Should be a message")
            .into_message();
        match outbound {
            OutboundMessage::SendMessage {
                msg:
                    NetMessage::ConsensusMessage(MsgWithHeader::<ConsensusNetMessage> {
                        msg: ConsensusNetMessage::SyncRequest(hash),
                        ..
                    }),
                ..
            } => assert_eq!(hash, missing_parent),
            other => panic!("expected SyncRequest, got {:?}", other),
        }
    }

    #[test_log::test(tokio::test)]
    async fn leader_accepts_commit_for_next_slot() {
        let crypto = BlstCrypto::new("leader-commit").unwrap();
        let mut ctx = ConsensusTestCtx::new("leader-commit", crypto.clone()).await;
        ctx.add_trusted_validator(&ctx.pubkey());

        ctx.consensus.bft_round_state.state_tag = StateTag::Leader;
        ctx.consensus.bft_round_state.slot = 5;
        ctx.consensus.bft_round_state.view = 1;
        ctx.consensus.bft_round_state.parent_hash = ConsensusProposalHash("parent".into());

        let proposal = ConsensusProposal {
            slot: 5,
            parent_hash: ctx.consensus.bft_round_state.parent_hash.clone(),
            cut: ctx.consensus.bft_round_state.parent_cut.clone(),
            ..ConsensusProposal::default()
        };
        ctx.consensus.store.bft_round_state.current_proposal = Some(proposal.clone());

        let commit_qc = QuorumCertificate(
            agg_sig(&crypto, (proposal.hashed(), ConfirmAckMarker)),
            ConfirmAckMarker,
        );

        ctx.consensus
            .on_commit(ctx.pubkey(), commit_qc, proposal.hashed())
            .expect("leader should accept commit and advance");

        assert_eq!(ctx.consensus.bft_round_state.slot, 6);
        assert_eq!(ctx.consensus.bft_round_state.view, 0);
        assert_eq!(ctx.consensus.bft_round_state.parent_hash, proposal.hashed());
        assert!(ctx.consensus.bft_round_state.current_proposal.is_none());
    }

    #[test_log::test(tokio::test)]
    async fn leader_fast_forwards_on_prepare_with_commit_for_previous_slot() {
        let crypto = BlstCrypto::new("leader-prepare-commit").unwrap();
        let mut ctx = ConsensusTestCtx::new("leader-prepare-commit", crypto.clone()).await;
        ctx.add_trusted_validator(&ctx.pubkey());

        ctx.consensus.bft_round_state.state_tag = StateTag::Leader;
        ctx.consensus.bft_round_state.slot = 5;
        ctx.consensus.bft_round_state.view = 1;
        ctx.consensus.bft_round_state.parent_hash = ConsensusProposalHash("parent".into());

        let current_proposal = ConsensusProposal {
            slot: 5,
            parent_hash: ctx.consensus.bft_round_state.parent_hash.clone(),
            cut: ctx.consensus.bft_round_state.parent_cut.clone(),
            ..ConsensusProposal::default()
        };
        ctx.consensus.store.bft_round_state.current_proposal = Some(current_proposal.clone());

        let prepare_for_next = ConsensusProposal {
            slot: 6,
            parent_hash: current_proposal.hashed(),
            cut: current_proposal.cut.clone(),
            ..ConsensusProposal::default()
        };

        let commit_qc = QuorumCertificate(
            agg_sig(&crypto, (current_proposal.hashed(), ConfirmAckMarker)),
            ConfirmAckMarker,
        );

        ctx.consensus
            .on_prepare(
                ctx.pubkey(),
                prepare_for_next,
                Ticket::CommitQC(commit_qc),
                0,
            )
            .expect("leader should fast-forward on prepare with commit QC");

        assert_eq!(ctx.consensus.bft_round_state.slot, 6);
        assert_eq!(ctx.consensus.bft_round_state.view, 0);
        assert_eq!(
            ctx.consensus.bft_round_state.parent_hash,
            current_proposal.hashed()
        );
    }
}

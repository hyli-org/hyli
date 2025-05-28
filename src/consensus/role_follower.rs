use anyhow::{bail, Context, Result};
use borsh::{BorshDeserialize, BorshSerialize};
use std::collections::BTreeMap;
use tracing::{debug, info, trace, warn};

use super::*;
use crate::{
    bus::BusClientSender,
    consensus::{role_timeout::TimeoutState, StateTag},
    model::{Hashed, Signed, ValidatorPublicKey},
    p2p::P2PCommand,
    utils::conf::TimestampCheck,
};
use hyle_crypto::BlstCrypto;
use hyle_model::{
    utils::TimestampMs, AggregateSignature, ConsensusProposal, ConsensusProposalHash,
    ConsensusStakingAction, Cut, LaneBytesSize, LaneId, SignedByValidator, ValidatorCandidacy,
    View,
};

#[derive(BorshSerialize, BorshDeserialize, Default)]
pub(super) struct FollowerState {
    pub(super) buffered_quorum_certificate: Option<CommitQC>, // if we receive a commit before the next prepare
    pub(super) buffered_prepares: BufferedPrepares, // History of seen prepares & buffer of future prepares
}

enum TicketVerifyAndProcess {
    NotProcessed,
    Processed,
}

impl Consensus {
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
            "Received Prepare message: {}", consensus_proposal
        );

        if matches!(self.bft_round_state.state_tag, StateTag::Joining) {
            // Shortcut - if this is the prepare we expected, exit joining mode.
            // When we're joining, a couple cases to consider:
            // - we're catching up with DA - in this case we pretend to commit all blocks, so we only expect our current BFT slot / CP + 1 (this matches any view).
            let prepare_follows_commit = consensus_proposal.slot == self.bft_round_state.slot
                && !self.current_slot_prepare_is_present();
            // - we restarted but are still up-to-date - it's plausible that we have BFT slot == CP slot, indicating we haven't committed that. Expect slot + 1 or higher views.
            let is_for_current_slot = self.current_slot_prepare_is_present()
                && (consensus_proposal.slot == self.bft_round_state.slot + 1
                    || view > self.bft_round_state.view);
            if prepare_follows_commit || is_for_current_slot {
                info!(
                    "Received Prepare message for next slot while joining. Exiting joining mode."
                );
                self.bft_round_state.state_tag = StateTag::Follower;
            } else {
                self.follower_state().buffered_prepares.push((
                    sender.clone(),
                    consensus_proposal,
                    ticket,
                    view,
                ));
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
        let ticket_was_processed = match &ticket {
            Ticket::Genesis => {
                if self.bft_round_state.slot != 1 {
                    bail!("Genesis ticket is only valid for the first slot.");
                }
                TicketVerifyAndProcess::Processed
            }
            Ticket::CommitQC(commit_qc) => self
                .verify_and_process_commit_ticket(&consensus_proposal, commit_qc.clone())
                .context("Processing Commit Ticket")?,
            Ticket::TimeoutQC(timeout_qc, tc_kind_data) => self
                .verify_and_process_tc_ticket(
                    timeout_qc.clone(),
                    tc_kind_data,
                    &consensus_proposal,
                    view,
                )
                .context("Processing TC ticket")?,
            els => {
                bail!("Cannot process invalid ticket here {:?}", els);
            }
        };

        // Ticket is not processed, we must stop here (probably buffered)
        if matches!(ticket_was_processed, TicketVerifyAndProcess::NotProcessed) {
            let ticket_type: &'static str = (&ticket).into();
            warn!(
                proposal_hash = %consensus_proposal.hashed(),
                sender = %sender,
                "🚚 Prepare message for slot {} while at slot {}. Buffering after verifying ticket {}.",
                consensus_proposal.slot, self.bft_round_state.slot, ticket_type
            );

            return self.buffer_prepare_message_and_fetch_missing_parent(
                sender,
                consensus_proposal,
                ticket,
                view,
            );
        }

        // TODO: check we haven't voted for a proposal this slot/view already.

        // Sanity check: after processing the ticket, we should be in the right slot/view.
        // TODO: these checks are almost entirely redundant at this point because we process the ticket above.
        if consensus_proposal.slot != self.bft_round_state.slot {
            bail!("Prepare message received for wrong slot");
        }
        if view != self.bft_round_state.view {
            bail!("Prepare message received for wrong view");
        }

        // Validate message comes from the correct leader
        // (can't do this earlier as might need to process the ticket first)
        let round_leader = self.round_leader()?;
        if sender != round_leader {
            bail!(
                "Prepare consensus message for {} {} does not come from current leader {}. I won't vote for it.",
                self.bft_round_state.slot, self.bft_round_state.view, round_leader
            );
        }

        self.verify_poda(&consensus_proposal)?;

        self.verify_staking_actions(&consensus_proposal)?;

        self.verify_timestamp(&consensus_proposal)?;

        // At this point we are OK with this new consensus proposal, update locally and vote.
        self.bft_round_state.current_proposal = consensus_proposal.clone();
        let cp_hash = self.bft_round_state.current_proposal.hashed();

        self.follower_state().buffered_prepares.push((
            sender.clone(),
            consensus_proposal,
            ticket,
            view,
        ));

        // If we already have the next Prepare, fast-forward
        if let Some(prepare) = self
            .follower_state()
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

        if proposal_hash_hint != self.bft_round_state.current_proposal.hashed() {
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
                self.bft_round_state.current_proposal.slot
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

    pub(super) fn on_commit(
        &mut self,
        sender: ValidatorPublicKey,
        commit_quorum_certificate: CommitQC,
        proposal_hash_hint: ConsensusProposalHash,
    ) -> Result<()> {
        // Unless joining, ignore commit messages for the wrong slot.
        if !matches!(self.bft_round_state.state_tag, StateTag::Joining)
            && self.bft_round_state.current_proposal.slot != self.bft_round_state.slot
        {
            return Ok(());
        }
        match self.bft_round_state.state_tag {
            StateTag::Follower => {
                if self.bft_round_state.current_proposal.hashed() != proposal_hash_hint {
                    warn!(
                        "Received Commit for proposal {:?} but expected {:?}. Ignoring.",
                        proposal_hash_hint,
                        self.bft_round_state.current_proposal.hashed()
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
            _ => {
                debug!(
                    sender = %sender,
                    proposal_hash = %proposal_hash_hint,
                    "Commit message received while not follower. Ignoring."
                );
                Ok(())
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

        for (lane_id, data_proposal_hash, lane_size, poda_sig) in &consensus_proposal.cut {
            let voting_power = self
                .bft_round_state
                .staking
                .compute_voting_power(poda_sig.validators.as_slice());

            // Check that this is a known lane.
            // TODO: this prevents ever deleting lane which may or may not be desirable.
            if !self.bft_round_state.staking.is_known(&lane_id.0) {
                bail!("Lane {} is in cut but is not a valid lane", lane_id);
            }

            // If this same data proposal was in the last cut, ignore.
            if let Some(cut) = self
                .bft_round_state
                .parent_cut
                .iter()
                .find(|(v, ..)| v == lane_id)
            {
                if &cut.1 == data_proposal_hash {
                    debug!(
                        "DataProposal {} from lane {} was already in the last cut, not checking PoDA",
                        data_proposal_hash, lane_id
                    );
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
                bail!("PoDA for lane {lane_id} does not have enough validators that signed his DataProposal");
            }

            // Verify that PoDA signature is valid
            let msg = (data_proposal_hash.clone(), *lane_size);
            match BlstCrypto::verify_aggregate(&Signed {
                msg,
                signature: poda_sig.clone(),
            }) {
                Ok(valid) => {
                    if !valid {
                        bail!("Failed to aggregate signatures into valid one. Messages might be different.");
                    }
                }
                Err(err) => bail!("Failed to verify PoDA: {}", err),
            };
        }
        Ok(())
    }

    fn verify_timestamp(
        &self,
        ConsensusProposal { timestamp, .. }: &ConsensusProposal,
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
                        + TimeoutState::TIMEOUT_SECS * (self.bft_round_state.view as u32));

                if &next_max_timestamp < timestamp {
                    bail!(
                        "Timestamp {} too late (should be < {}, exceeded by {} ms)",
                        timestamp,
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
        self.bft_round_state
            .current_proposal
            .staking_actions
            .iter()
            .filter(|sa| matches!(sa, ConsensusStakingAction::Bond { .. }))
            .count()
            > 0
    }

    fn verify_and_process_tc_ticket(
        &mut self,
        timeout_qc: TimeoutQC,
        tc_kind_data: &TCKind,
        consensus_proposal: &ConsensusProposal,
        prepare_view: View,
    ) -> Result<TicketVerifyAndProcess> {
        let prepare_slot = consensus_proposal.slot;
        // Two cases:
        // - the prepare is for next slot *and* we have the prepare for the current slot
        // - the prepare is for the current slot
        let is_next_slot_and_current_proposal_is_present =
            prepare_slot == self.bft_round_state.slot + 1 && self.current_slot_prepare_is_present();

        if (consensus_proposal.slot == self.bft_round_state.slot)
            || is_next_slot_and_current_proposal_is_present
        {
            debug!(
            "Trying to process timeout Certificate against consensus proposal slot: {}, view: {}",
            prepare_slot, prepare_view,
        );

            // Check the ticket matches the CP
            if let TCKind::PrepareQC((_, cp)) = tc_kind_data {
                if cp != consensus_proposal {
                    bail!(
                    "Timeout Certificate does not match consensus proposal. Expected {}, got {}",
                    cp.hashed(),
                    consensus_proposal.hashed()
                );
                }
            }

            tracing::debug!(
            "Slot info service: prepare slot {}, bft round state slot {}, current proposal slot {}",
            prepare_slot,
            self.bft_round_state.slot,
            self.bft_round_state.current_proposal.slot
        );

            // If ticket for next slot && correct parent hash, fast forward
            if is_next_slot_and_current_proposal_is_present
                && consensus_proposal.parent_hash == self.bft_round_state.current_proposal.hashed()
            {
                // Try to commit our current prepare & fast-forward.

                // Safety assumption: we can't actually verify a TC for the next slot, but since it matches our hash,
                // since we have no staking actions in the prepare we're good.
                if self.current_proposal_changes_voting_power() {
                    bail!("Timeout Certificate slot {} view {} is for the next slot, and current proposal changes voting power", prepare_slot, prepare_view);
                }

                // SOOOOO here we're stuck actually, because we don't have the commit certificate.
                // It's safe to fast-forward, but our SignedBlock ultimately won't be verifiable.
                // To not pretend otherwise, just store an empty commit QC.
                self.emit_commit_event(&QuorumCertificate(
                    AggregateSignature::default(),
                    ConfirmAckMarker,
                ))
                .context("Processing TC ticket")?;

                // We have received a timeout certificate for the next slot,
                // and it matches our know prepare for this slot, so try and commit that one then the TC.
                self.advance_round(Ticket::ForcedCommitQc)?;

                info!(
                    "🔀 Fast forwarded to slot {} view 0",
                    &self.bft_round_state.slot
                );
            }
            if prepare_slot != self.bft_round_state.slot {
                bail!(
                    "Timeout Certificate slot {} view {} is not the current slot {}",
                    prepare_slot,
                    prepare_view,
                    self.bft_round_state.slot
                );
            }
            self.verify_tc(
                &timeout_qc,
                tc_kind_data,
                self.bft_round_state.slot,
                prepare_view - 1,
            )?;
            if prepare_view == self.bft_round_state.view + 1 {
                // Process it
                debug!(
                    "Timeout Certificate for next view {} received, processing it",
                    prepare_view
                );
                self.advance_round(Ticket::TimeoutQC(timeout_qc, tc_kind_data.clone()))?;
            }
            return Ok(TicketVerifyAndProcess::Processed);
        }
        Ok(TicketVerifyAndProcess::NotProcessed)
    }

    fn on_commit_while_joining(
        &mut self,
        commit_quorum_certificate: CommitQC,
        proposal_hash_hint: ConsensusProposalHash,
    ) -> Result<()> {
        // We are joining consensus, try to sync our state.
        let Some((_, potential_proposal, _, _)) = self
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

        self.bft_round_state.current_proposal = potential_proposal.clone();

        info!(
            "📦 Commit message received for slot {}, trying to synchronize.",
            self.bft_round_state.current_proposal.slot
        );

        // Try to commit the proposal
        self.verify_commit_quorum_certificate_against_current_proposal(&commit_quorum_certificate)
            .context("On Commit when joining")?;

        self.bft_round_state.slot = self.bft_round_state.current_proposal.slot;
        self.bft_round_state.view = 0; // TODO
        self.bft_round_state.state_tag = StateTag::Follower;

        self.emit_commit_event(&commit_quorum_certificate)?;

        self.advance_round(Ticket::CommitQC(commit_quorum_certificate))?;

        // We sucessfully joined the consensus
        info!("🏁 Synchronized to slot {}", self.bft_round_state.slot);
        Ok(())
    }

    #[inline]
    pub(super) fn follower_state(&mut self) -> &mut FollowerState {
        &mut self.store.bft_round_state.follower
    }

    pub(super) fn has_no_buffered_children(&self) -> bool {
        self.store
            .bft_round_state
            .follower
            .buffered_prepares
            .next_prepare(self.bft_round_state.current_proposal.hashed())
            .is_none()
    }

    /// When a prepare is received with a too high slot, buffer it and fetch its parents before you can process it.
    fn buffer_prepare_message_and_fetch_missing_parent(
        &mut self,
        sender: ValidatorPublicKey,
        consensus_proposal: ConsensusProposal,
        ticket: Ticket,
        view: View,
    ) -> Result<()> {
        let mut missing_dp_hash = consensus_proposal.parent_hash.clone();

        // Buffer this prepare if we don't know one.
        if !self
            .follower_state()
            .buffered_prepares
            .contains(&consensus_proposal.hashed())
        {
            let prepare_message = (sender.clone(), consensus_proposal, ticket, view);
            self.follower_state()
                .buffered_prepares
                .push(prepare_message);
        }

        // Check if we have a missing DP up to our current known DP (this assumes we're not on a fork)
        let current_dp_hash = self.bft_round_state.current_proposal.hashed();
        // TODO: we should switch back to joining if we try to catch up on too many prepares.
        while let Some(prep) = self
            .follower_state()
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
            "🔉 Requesting missing parent prepare for proposal {}",
            missing_dp_hash
        );
        // We use send instead of broadcast to avoid an exponential number of messages
        // TODO: improve on this.
        #[cfg(not(test))]
        let node_to_ask = {
            let bonded = self.bft_round_state.staking.bonded();
            if bonded.is_empty() {
                None
            } else {
                let i: usize = rand::random();
                // Skip us
                if bonded
                    .get(i)
                    .map(|x| x == self.crypto.validator_pubkey())
                    .unwrap_or(true)
                {
                    bonded.get((i + 1) % bonded.len()).cloned()
                } else {
                    bonded.get(i).cloned()
                }
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
    ) -> Result<TicketVerifyAndProcess> {
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
                    return Ok(TicketVerifyAndProcess::Processed);
                }
            }

            // Edge case: we have already committed a different CQC (this check that bft slot == cp slot + 1 means we committed)
            if !self.current_slot_prepare_is_present() {
                warn!(
                    "Received an unknown commit QC for slot {}. This is unsafe to verify as we have updated staking with changes in that slot.
                    Proceeding with current staking anyways.",
                    self.bft_round_state.slot
                );
                // To still sorta make this work, verify the CQC with our current staking and hope for the best.
                self.verify_quorum_certificate(
                    (self.bft_round_state.parent_hash.clone(), ConfirmAckMarker),
                    &commit_qc,
                )?;
                return Ok(TicketVerifyAndProcess::Processed);
            }

            self.verify_commit_quorum_certificate_against_current_proposal(&commit_qc)?;
            self.emit_commit_event(&commit_qc)?;
            self.advance_round(Ticket::CommitQC(commit_qc))?;

            info!("🔀 Fast forwarded to slot {}", &self.bft_round_state.slot);
            return Ok(TicketVerifyAndProcess::Processed);
        }

        Ok(TicketVerifyAndProcess::NotProcessed)
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
        if !BlstCrypto::verify(new_validator)? {
            bail!("New bonded validator has an invalid signature");
        }
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
    prepares: BTreeMap<ConsensusProposalHash, Prepare>,
    children: BTreeMap<ConsensusProposalHash, ConsensusProposalHash>,
}

impl BufferedPrepares {
    fn contains(&self, proposal_hash: &ConsensusProposalHash) -> bool {
        self.prepares.contains_key(proposal_hash)
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
    use crate::bus::metrics::BusMetrics;
    use crate::bus::SharedMessageBus;
    use crate::consensus::test::ConsensusTestCtx;
    use crate::consensus::*;
    use crate::p2p::network::{MsgWithHeader, NetMessage, OutboundMessage};

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
    async fn test_buffer_prepare_message_and_fetch_missing_parent_with_gaps() {
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

        let bus = SharedMessageBus::new(BusMetrics::global("global".to_string()));
        let mut sync_request_rv = get_receiver::<OutboundMessage>(&bus).await;

        let mut consensus =
            ConsensusTestCtx::build_consensus(&bus, BlstCrypto::new("test-node").unwrap()).await;

        consensus.bft_round_state.current_proposal = current_proposal;

        // Add the buffered prepare to the state
        consensus.follower_state().buffered_prepares.push((
            ValidatorPublicKey::default(),
            buffered_prepare,
            Ticket::Genesis,
            0,
        ));

        // Call the function with the just received prepare
        consensus
            .buffer_prepare_message_and_fetch_missing_parent(
                ValidatorPublicKey::default(),
                just_received_prepare.clone(),
                Ticket::Genesis,
                0,
            )
            .unwrap();

        // We should request missing_prepare2
        match sync_request_rv.recv().await {
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
        consensus.follower_state().buffered_prepares.push((
            ValidatorPublicKey::default(),
            missing_prepare2.clone(),
            Ticket::Genesis,
            0,
        ));

        // Call the function again with the prepare we received
        consensus
            .buffer_prepare_message_and_fetch_missing_parent(
                ValidatorPublicKey::default(),
                missing_prepare2.clone(),
                Ticket::Genesis,
                0,
            )
            .unwrap();

        // We should request missing_prepare1
        match sync_request_rv.recv().await {
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
}

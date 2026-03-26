use anyhow::{bail, Context, Result};
use borsh::{BorshDeserialize, BorshSerialize};
use std::{collections::HashSet, future::Future, pin::Pin};
use tracing::{debug, info, trace, warn};

use super::*;
use crate::{
    consensus::role_follower::TicketVerificationError,
    model::{Slot, View},
};
use hyli_model::{ConsensusProposalHash, Hashed, Signed, SignedByValidator};

#[derive(Debug, BorshSerialize, BorshDeserialize, Default)]
pub(super) enum TimeoutState {
    #[default]
    Voting,
    Timeout,
    Certificate,
}

impl TimeoutState {
    pub fn timeout(&mut self) {
        match self {
            TimeoutState::Voting => {
                trace!("⏲️ Entering timeout phase");
            }
            TimeoutState::Timeout => {
                trace!("⏲️ Already in timeout phase");
            }
            TimeoutState::Certificate => {
                warn!("⏲️ Try to re-enter timeout phase after a certificate was emitted");
            }
        }
        *self = TimeoutState::Timeout;
    }

    pub fn enter_certificate_phase(&mut self) {
        match self {
            TimeoutState::Certificate => {
                warn!("⏲️ Try to emit a certificate after it was already emitted");
            }
            TimeoutState::Voting => {
                warn!("⏲️ Mark TimeoutCertificate as emitted while still in voting phase");
            }
            TimeoutState::Timeout => {
                trace!("⏲️ Mark TimeoutCertificate as emitted");
            }
        }
        *self = TimeoutState::Certificate;
    }
}

pub(super) type TimeoutFuture = Pin<Box<dyn Future<Output = ()> + Send>>;

pub(super) struct ScheduledTimeout(pub(super) TimeoutFuture);

impl Default for ScheduledTimeout {
    fn default() -> Self {
        Self(Box::pin(std::future::pending()))
    }
}

impl ScheduledTimeout {
    pub(super) fn sleep(duration: std::time::Duration) -> Self {
        Self(Box::pin(tokio::time::sleep(duration)))
    }
}

#[derive(BorshSerialize, BorshDeserialize, Default)]
pub(super) struct TimeoutRoleState {
    pub(super) requests: HashSet<ConsensusTimeout>,
    pub(super) state: TimeoutState,
    #[borsh(skip)]
    pub(super) next_scheduled: ScheduledTimeout,
    pub(super) highest_seen_prepare_qc: Option<(Slot, PrepareQC)>,
}

impl TimeoutRoleState {
    pub(super) fn reset_for_new_round(&mut self) {
        self.requests.clear();
        self.state = TimeoutState::Voting;
        self.next_scheduled = ScheduledTimeout::default();
    }

    pub(super) fn update_highest_seen_prepare_qc(&mut self, slot: Slot, qc: PrepareQC) -> bool {
        if let Some((s, _)) = &self.highest_seen_prepare_qc {
            if slot < *s {
                return false;
            }
        }
        self.highest_seen_prepare_qc = Some((slot, qc));
        true
    }
}

impl Consensus {
    fn is_relevant_timeout(
        &self,
        signed_message: &SignedByValidator<(
            Slot,
            View,
            ConsensusProposalHash,
            ConsensusTimeoutMarker,
        )>,
        slot: Slot,
        view: View,
    ) -> bool {
        signed_message.msg.0 == slot
            && signed_message.msg.1 == view
            && signed_message.msg.2 == self.bft_round_state.parent_hash
    }

    fn relevant_timeout_requests(
        &self,
        slot: Slot,
        view: View,
    ) -> impl Iterator<Item = &ConsensusTimeout> {
        self.store
            .bft_round_state
            .timeout
            .requests
            .iter()
            .filter(move |(signed_message, _)| self.is_relevant_timeout(signed_message, slot, view))
    }

    #[cfg_attr(feature = "instrumentation", tracing::instrument(skip(self)))]
    pub(super) fn verify_tc(
        &self,
        tc_qc: &TimeoutQC,
        tc_kind: &TCKind,
        tc_slot: Slot,
        tc_view: View,
        tc_consensus_proposal_hash: ConsensusProposalHash,
    ) -> Result<()> {
        info!(
            "Verifying TC for {}/{}, kind: {:?}",
            tc_slot, tc_view, tc_kind
        );

        self.verify_quorum_certificate(
            (
                tc_slot,
                tc_view,
                tc_consensus_proposal_hash.clone(),
                ConsensusTimeoutMarker,
            ),
            tc_qc,
        )
        .context(format!(
            "Verifying timeout certificate with prepare QC for (slot: {tc_slot}, view: {tc_view})",
        ))?;

        // Two options
        match tc_kind {
            TCKind::NilProposal(nqc) => {
                // If this is a Nil timout certificate, then we should also be receiving  2f+1 signatures of a full timeout message with nil proposal
                self.verify_quorum_certificate(
                    (
                        tc_slot,
                        tc_view,
                        tc_consensus_proposal_hash,
                        NilConsensusTimeoutMarker,
                    ),
                    nqc,
                )
                .context(format!(
                    "Verifying Nil timeout certificate for (slot: {tc_slot}, view: {tc_view})"
                ))?;
            }
            TCKind::PrepareQC((qc, cp)) => {
                if cp.slot != tc_slot {
                    bail!(
                        "Received timeout certificate with prepare QC for slot {}, but timeout is for slot {}",
                        cp.slot,
                        tc_slot
                    );
                }
                // Then check the prepare quorum certificate
                self.verify_quorum_certificate((cp.hashed(), PrepareVoteMarker), qc)
                    .context("Verifying PrepareQC")?;
            }
        }
        Ok(())
    }

    #[cfg_attr(feature = "instrumentation", tracing::instrument(skip(self)))]
    pub(super) fn on_timeout_certificate(
        &mut self,
        received_timeout_certificate: TimeoutQC,
        received_proposal_qc: TCKind,
        received_slot: Slot,
        received_view: View,
    ) -> Result<()> {
        match self.verify_and_process_tc_ticket(
            received_timeout_certificate.clone(),
            &received_proposal_qc,
            received_slot,
            received_view,
            None,
            None,
        ) {
            Err(TicketVerificationError::ProcessingError) => {
                panic!("Unrecoverable error processing TC")
            }
            Err(TicketVerificationError::Unverifiable) => bail!("Unverifiable ticket"), // should we send a sync request?
            Err(TicketVerificationError::Invalid) => bail!("Invalid ticket"),
            Ok(()) => {}
        };
        self.cache_timeout_certificate(
            received_slot,
            received_view,
            received_timeout_certificate,
            received_proposal_qc,
        );
        Ok(())
    }

    pub(super) fn on_timeout_trigger(&mut self) -> Result<()> {
        let _span = tracing::info_span!(
            "TimeoutTick",
            slot = self.bft_round_state.slot as i64,
            view = self.bft_round_state.view as i64
        )
        .entered();
        // Trigger state transition to mutiny
        info!(
            "⏰ Trigger timeout for slot {} and view {}",
            self.bft_round_state.slot, self.bft_round_state.view
        );
        let (timeout, kind) = self.get_timeout_message()?;
        self.bft_round_state.timeout.state.timeout();

        self.on_timeout(timeout.clone(), kind.clone())?;

        self.broadcast_net_message((timeout, kind).into())?;

        // Rescheduling broadcast of this same timeout message, but sooner than the regular waiting time
        self.schedule_timeout_at(
            TimestampMsClock::now(),
            self.config.consensus.timeout_after / 2,
        );

        Ok(())
    }

    #[cfg_attr(feature = "instrumentation", tracing::instrument(skip(self)))]
    pub(super) fn on_timeout(
        &mut self,
        received_timeout: SignedByValidator<(
            Slot,
            View,
            ConsensusProposalHash,
            ConsensusTimeoutMarker,
        )>,
        received_tk: TimeoutKind,
    ) -> Result<()> {
        // Only timeout if it is in consensus
        if !self.is_part_of_consensus(self.crypto.validator_pubkey()) {
            info!(
                "Received timeout message while not being part of the consensus: {}",
                self.crypto.validator_pubkey()
            );
            return Ok(());
        }

        let Signed {
            msg: (received_slot, received_view, received_parent_hash, _),
            ..
        } = &received_timeout;

        let sender = received_timeout.signature.validator.clone();
        if sender != *self.crypto.validator_pubkey()
            && received_slot == &self.bft_round_state.slot
            && received_view < &self.bft_round_state.view
        {
            if let Some((timeout_qc, tc_kind)) =
                self.cached_timeout_certificate(*received_slot, *received_view)
            {
                self.send_net_message(
                    sender.clone(),
                    ConsensusNetMessage::TimeoutCertificate(
                        timeout_qc,
                        tc_kind,
                        *received_slot,
                        *received_view,
                    ),
                )?;
            }
        }

        if received_slot < &self.bft_round_state.slot {
            debug!(
                "🌘 Ignoring timeout for older slot {}, am at {}",
                received_slot, self.bft_round_state.slot
            );
            return Ok(());
        }

        if received_slot != &self.bft_round_state.slot || received_view < &self.bft_round_state.view
        {
            info!(
                "🌘 Timeout (Slot: {}, view: {}) does not match expected (Slot == {}, view >= {})",
                received_slot, received_view, self.bft_round_state.slot, self.bft_round_state.view,
            );
            return Ok(());
        }

        if received_parent_hash != &self.bft_round_state.parent_hash {
            debug!(
                "🌘 Ignoring timeout with incorrect parent hash {}, expected {}",
                received_parent_hash, self.bft_round_state.parent_hash
            );
            return Ok(());
        }

        // If there is a prepareQC along with this message, verify it (we can, it's the same slot),
        // and then potentially update our highest seen PrepareQC.
        if let TimeoutKind::PrepareQC((qc, cp)) = &received_tk {
            if cp.slot == *received_slot {
                self.verify_quorum_certificate((cp.hashed(), PrepareVoteMarker), qc)
                    .context("Verifying PrepareQC")?;
                if self
                    .store
                    .bft_round_state
                    .timeout
                    .update_highest_seen_prepare_qc(*received_slot, qc.clone())
                {
                    // Update our consensus proposal
                    self.bft_round_state.current_proposal = Some(cp.clone());
                    debug!("Highest seen PrepareQC updated");
                }
            } else {
                // We actually cannot process this, or we might end up thinking we have 2f+1 timeouts but not working.
                bail!(
                    "Received incorrect timeout message. PrepareQC is for slot {}, but Timeout is about slot {}",
                    cp.slot,
                    received_slot
                );
            }
        }

        let is_own_timeout = sender == *self.crypto.validator_pubkey();

        // Keep external timeout votes only. Our local timeout state already tracks whether we joined.
        if !is_own_timeout
            && !self
                .store
                .bft_round_state
                .timeout
                .requests
                .insert((received_timeout.clone(), received_tk.clone()))
        {
            info!("Timeout has already been processed");
            return Ok(());
        }

        let f = self.bft_round_state.staking.compute_f();

        let (mut len, mut voting_power) = {
            let relevant_timeout_messages = self
                .relevant_timeout_requests(*received_slot, *received_view)
                .map(|(signed_message, _)| signed_message)
                .collect::<Vec<_>>();

            (
                relevant_timeout_messages.len(),
                self.store.bft_round_state.staking.compute_voting_power(
                    &relevant_timeout_messages
                        .iter()
                        .map(|s| s.signature.validator.clone())
                        .collect::<Vec<_>>(),
                ),
            )
        };
        if self.is_in_timeout_phase() {
            len += 1;
            voting_power += self.get_own_voting_power();
        }

        info!(
            "Got {voting_power} voting power with {len} timeout requests for the slot {received_slot} view {received_view}. f is {f}",
        );

        // Count requests and if f+1 requests, and not already part of it, join the mutiny
        if voting_power > f && !self.is_in_timeout_phase() {
            info!("Joining timeout mutiny!");

            self.store.bft_round_state.view = *received_view;
            self.store.bft_round_state.timeout.state.timeout();

            let (timeout, kind) = self.get_timeout_message()?;

            self.broadcast_net_message((timeout, kind).into())
                .context(format!(
                    "Sending timeout message for slot: {} view: {}",
                    self.bft_round_state.slot, self.bft_round_state.view,
                ))?;

            len += 1;
            voting_power += self.get_own_voting_power();

            self.schedule_timeout_at(TimestampMsClock::now(), self.config.consensus.timeout_after);
        }

        // Create TC if applicable
        if voting_power > 2 * f
            && !matches!(
                self.bft_round_state.timeout.state,
                TimeoutState::Certificate
            )
        {
            debug!(
                "⏲️ ⏲️ Creating a timeout certificate with {len} timeout requests and {voting_power} voting power"
            );

            let relevant_timeout_message_refs = self
                .relevant_timeout_requests(*received_slot, *received_view)
                .map(|(signed_message, _)| signed_message)
                .collect::<Vec<_>>();
            let tqc = QuorumCertificate(
                self.crypto
                    .sign_aggregate(
                        (
                            self.bft_round_state.slot,
                            self.bft_round_state.view,
                            self.bft_round_state.parent_hash.clone(),
                            ConsensusTimeoutMarker,
                        ),
                        relevant_timeout_message_refs.as_slice(),
                    )?
                    .signature,
                ConsensusTimeoutMarker,
            );
            let tqc_kind = match &self.bft_round_state.timeout.highest_seen_prepare_qc {
                Some((s, qc))
                    if s == received_slot
                        && self
                            .bft_round_state
                            .current_proposal
                            .as_ref()
                            .map(|cp| cp.slot == *s)
                            .unwrap_or(false) =>
                {
                    // We have a prepare QC for this round, so let's send that.
                    #[allow(clippy::unwrap_used, reason = "must exist because of above")]
                    TCKind::PrepareQC((
                        qc.clone(),
                        self.bft_round_state.current_proposal.clone().unwrap(),
                    ))
                }
                _ => {
                    // Simple case - we will aggregate a 'nil' certificate. We need 2f+1 NIL signed messages
                    // In principle we can't be here unless they're all NIL.
                    if !matches!(received_tk, TimeoutKind::NilProposal(_)) {
                        bail!("Received timeout message with PrepareQC, but highest seen PrepareQC is not for this slot");
                    }
                    // Ergo, this should successfully aggregate.
                    let nil_quorum = QuorumCertificate(
                        self.crypto
                            .sign_aggregate(
                                (
                                    self.bft_round_state.slot,
                                    self.bft_round_state.view,
                                    self.bft_round_state.parent_hash.clone(),
                                    NilConsensusTimeoutMarker,
                                ),
                                self.relevant_timeout_requests(*received_slot, *received_view)
                                    .map(|(_, tk)| tk)
                                    .map(|tk| match tk {
                                        TimeoutKind::NilProposal(signed_nil) => Ok(signed_nil),
                                        _ => bail!("Expected NilProposal, got {:?}", tk),
                                    })
                                    .collect::<Result<Vec<_>>>()?
                                    .as_slice(),
                            )?
                            .signature,
                        NilConsensusTimeoutMarker,
                    );
                    TCKind::NilProposal(nil_quorum)
                }
            };
            let ticket = (tqc, tqc_kind);

            self.cache_timeout_certificate(
                *received_slot,
                *received_view,
                ticket.0.clone(),
                ticket.1.clone(),
            );

            self.schedule_timeout_at(TimestampMsClock::now(), self.config.consensus.timeout_after);

            let round_leader = self.next_view_leader()?;
            if &round_leader == self.crypto.validator_pubkey() {
                // This TC is for our current slot and view (by construction), so we can leave Joining mode
                if matches!(self.bft_round_state.state_tag, StateTag::Joining) {
                    debug!("Leaving Joining mode as leader after timeout certificate");
                    self.set_state_tag(StateTag::Leader);
                }
            } else {
                // This TC is for our current slot and view (by construction), so we can leave Joining mode
                if matches!(self.bft_round_state.state_tag, StateTag::Joining) {
                    debug!("Leaving Joining mode as follower after timeout certificate");
                    self.set_state_tag(StateTag::Follower);
                }
                // Broadcast the Timeout Certificate to all validators
                self.broadcast_net_message(ConsensusNetMessage::TimeoutCertificate(
                    ticket.0.clone(),
                    ticket.1.clone(),
                    *received_slot,
                    *received_view,
                ))?;
                self.bft_round_state.timeout.state.enter_certificate_phase();
            }
            self.advance_round(Ticket::TimeoutQC(ticket.0, ticket.1))?;
        }

        self.record_consensus_state_metric();
        Ok(())
    }

    fn get_timeout_message(&self) -> Result<ConsensusTimeout> {
        let signed_timeout_metadata = self.crypto.sign((
            self.bft_round_state.slot,
            self.bft_round_state.view,
            self.bft_round_state.parent_hash.clone(),
            ConsensusTimeoutMarker,
        ))?;
        tracing::debug!(
            "Sending timeout message for slot {} and view {}.\nHighest seen {:?}",
            self.bft_round_state.slot,
            self.bft_round_state.view,
            self.bft_round_state.timeout.highest_seen_prepare_qc
        );
        Ok(
            match &self.bft_round_state.timeout.highest_seen_prepare_qc {
                Some((s, qc))
                    if s == &self.bft_round_state.slot
                        && self
                            .bft_round_state
                            .current_proposal
                            .as_ref()
                            .map(|cp| cp.slot == *s)
                            .unwrap_or(false) =>
                {
                    // If we have a PrepareQC for this slot (any view), use it
                    #[allow(clippy::unwrap_used, reason = "must exist because of above")]
                    (
                        signed_timeout_metadata,
                        TimeoutKind::PrepareQC((
                            qc.clone(),
                            self.bft_round_state.current_proposal.clone().unwrap(),
                        )),
                    )
                }
                _ => (
                    signed_timeout_metadata,
                    TimeoutKind::NilProposal(self.crypto.sign((
                        self.bft_round_state.slot,
                        self.bft_round_state.view,
                        self.bft_round_state.parent_hash.clone(),
                        NilConsensusTimeoutMarker,
                    ))?),
                ),
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::consensus::test::*;
    use crate::tests::autobahn_testing_macros::{broadcast, send, simple_commit_round};

    use super::*;

    #[test_log::test(tokio::test)]
    async fn timeout_only_one_4() {
        let (mut node1, mut node2, mut node3, mut node4): (
            ConsensusTestCtx,
            ConsensusTestCtx,
            ConsensusTestCtx,
            ConsensusTestCtx,
        ) = build_nodes!(4).await;

        node1.start_round().await;

        ConsensusTestCtx::timeout(&mut [&mut node2]).await;

        node2.assert_broadcast("Timeout message").await;

        // Slot 1 - leader = node1
        // A node that already timed out in this view must not later vote on Confirm.
        let cp;
        let ticket;
        let cp_view;

        broadcast! {
            description: "Leader - Prepare",
            from: node1, to: [node2, node3, node4],
            message_matches: ConsensusNetMessage::Prepare(round_cp, round_ticket, round_view) => {
                cp = round_cp.clone();
                ticket = round_ticket.clone();
                cp_view = *round_view;
            }
        };

        node2.assert_no_send(
            &node1.validator_pubkey(),
            "Timed out follower must not PrepareVote",
        );

        send! {
            description: "Follower - PrepareVote",
            from: [node3, node4], to: node1,
            message_matches: ConsensusNetMessage::PrepareVote(_)
        };

        broadcast! {
            description: "Leader - Confirm",
            from: node1, to: [node2, node3, node4],
            message_matches: ConsensusNetMessage::Confirm(..)
        };

        node2.assert_no_broadcast("Timed out follower must not ConfirmAck");

        send! {
            description: "Follower - Confirm Ack",
            from: [node3, node4], to: node1,
            message_matches: ConsensusNetMessage::ConfirmAck(_)
        };

        broadcast! {
            description: "Leader - Commit",
            from: node1, to: [node2, node3, node4],
            message_matches: ConsensusNetMessage::Commit(..)
        };

        assert_eq!(cp.slot, 1);
        assert_eq!(cp_view, 0);
        assert!(matches!(ticket, Ticket::Genesis));
    }

    #[test_log::test(tokio::test)]
    async fn own_timeout_is_not_stored_but_still_counts_for_tc() {
        let (mut node1, mut node2, mut node3, mut node4): (
            ConsensusTestCtx,
            ConsensusTestCtx,
            ConsensusTestCtx,
            ConsensusTestCtx,
        ) = build_nodes!(4).await;

        ConsensusTestCtx::timeout(&mut [&mut node2, &mut node3, &mut node4]).await;

        assert!(matches!(
            node2.consensus.bft_round_state.timeout.state,
            TimeoutState::Timeout
        ));
        assert!(
            node2.consensus.bft_round_state.timeout.requests.is_empty(),
            "own timeout should not be stored in timeout.requests"
        );

        broadcast! {
            description: "Follower - Timeout",
            from: node3, to: [node1, node2, node4],
            message_matches: ConsensusNetMessage::Timeout(..)
        };

        assert_eq!(node2.consensus.bft_round_state.timeout.requests.len(), 1);
        assert!(node2
            .consensus
            .bft_round_state
            .timeout
            .requests
            .iter()
            .all(|(timeout, _)| timeout.signature.validator != node2.validator_pubkey()));
        assert_eq!(node2.consensus.bft_round_state.view, 0);
        assert!(
            node2.consensus.cached_timeout_certificate(1, 0).is_none(),
            "one external timeout plus our local timeout must not be enough to form a TC"
        );

        broadcast! {
            description: "Follower - Timeout",
            from: node4, to: [node1, node2, node3],
            message_matches: ConsensusNetMessage::Timeout(..)
        };

        assert_eq!(
            node2.consensus.bft_round_state.view, 1,
            "node should advance using its local timeout plus two external timeouts"
        );
    }

    #[test_log::test(tokio::test)]
    async fn test_timeout_multi_view() {
        let (mut node1, mut node2, mut node3, mut node4): (
            ConsensusTestCtx,
            ConsensusTestCtx,
            ConsensusTestCtx,
            ConsensusTestCtx,
        ) = build_nodes!(4).await;

        assert_eq!(node1.consensus.bft_round_state.view, 0);
        assert_eq!(node2.consensus.bft_round_state.view, 0);
        assert_eq!(node3.consensus.bft_round_state.view, 0);
        assert_eq!(node4.consensus.bft_round_state.view, 0);

        ConsensusTestCtx::timeout(&mut [&mut node1, &mut node2, &mut node3]).await;

        // Skip node 4
        broadcast! {
            description: "Follower - Timeout",
            from: node1, to: [node2, node3],
            message_matches: ConsensusNetMessage::Timeout(..)
        };
        broadcast! {
            description: "Follower - Timeout",
            from: node2, to: [node1, node3],
            message_matches: ConsensusNetMessage::Timeout(..)
        };
        broadcast! {
            description: "Follower - Timeout",
            from: node3, to: [node2, node1],
            message_matches: ConsensusNetMessage::Timeout(..)
        };

        node1.assert_broadcast("TC").await;
        // Node 2 won't TC as it's next leader
        //node2.assert_broadcast("TC").await;
        node3.assert_broadcast("TC").await;

        assert_eq!(node1.consensus.bft_round_state.view, 1);
        assert_eq!(node2.consensus.bft_round_state.view, 1);
        assert_eq!(node3.consensus.bft_round_state.view, 1);
        assert_eq!(node4.consensus.bft_round_state.view, 0);

        ConsensusTestCtx::timeout(&mut [&mut node1, &mut node2, &mut node3]).await;

        // Include node 4
        broadcast! {
            description: "Follower - Timeout",
            from: node1, to: [node2, node3, node4],
            message_matches: ConsensusNetMessage::Timeout(..)
        };
        broadcast! {
            description: "Follower - Timeout",
            from: node2, to: [node1, node3, node4],
            message_matches: ConsensusNetMessage::Timeout(..)
        };
        broadcast! {
            description: "Follower - Timeout",
            from: node3, to: [node2, node1, node4],
            message_matches: ConsensusNetMessage::Timeout(..)
        };

        assert_eq!(node1.consensus.bft_round_state.view, 2);
        assert_eq!(node2.consensus.bft_round_state.view, 2);
        assert_eq!(node3.consensus.bft_round_state.view, 2);
        assert_eq!(node4.consensus.bft_round_state.view, 2);
    }

    #[test_log::test(tokio::test)]
    async fn test_timeout_multi_view_only_see_tc() {
        // Same as test_timeout_multi_view, but node 4 will not see timeouts, just the TC.
        let (mut node1, mut node2, mut node3, mut node4): (
            ConsensusTestCtx,
            ConsensusTestCtx,
            ConsensusTestCtx,
            ConsensusTestCtx,
        ) = build_nodes!(4).await;

        assert_eq!(node1.consensus.bft_round_state.view, 0);
        assert_eq!(node2.consensus.bft_round_state.view, 0);
        assert_eq!(node3.consensus.bft_round_state.view, 0);
        assert_eq!(node4.consensus.bft_round_state.view, 0);

        ConsensusTestCtx::timeout(&mut [&mut node1, &mut node2, &mut node3]).await;

        // Skip node 4
        broadcast! {
            description: "Follower - Timeout",
            from: node1, to: [node2, node3],
            message_matches: ConsensusNetMessage::Timeout(..)
        };
        broadcast! {
            description: "Follower - Timeout",
            from: node2, to: [node1, node3],
            message_matches: ConsensusNetMessage::Timeout(..)
        };
        broadcast! {
            description: "Follower - Timeout",
            from: node3, to: [node2, node1],
            message_matches: ConsensusNetMessage::Timeout(..)
        };

        node1.assert_broadcast("TC").await;
        // Node 2 won't TC as it's next leader
        //node2.assert_broadcast("TC").await;
        node3.assert_broadcast("TC").await;

        assert_eq!(node1.consensus.bft_round_state.view, 1);
        assert_eq!(node2.consensus.bft_round_state.view, 1);
        assert_eq!(node3.consensus.bft_round_state.view, 1);
        assert_eq!(node4.consensus.bft_round_state.view, 0);

        ConsensusTestCtx::timeout(&mut [&mut node1, &mut node2, &mut node3]).await;

        // Skip node 4
        broadcast! {
            description: "Follower - Timeout",
            from: node1, to: [node2, node3],
            message_matches: ConsensusNetMessage::Timeout(..)
        };
        broadcast! {
            description: "Follower - Timeout",
            from: node2, to: [node1, node3],
            message_matches: ConsensusNetMessage::Timeout(..)
        };
        broadcast! {
            description: "Follower - Timeout",
            from: node3, to: [node2, node1],
            message_matches: ConsensusNetMessage::Timeout(..)
        };

        // Node 4 will only see the TC
        broadcast! {
            description: "Follower - TimeoutTC",
            from: node1, to: [node2, node3, node4],
            message_matches: ConsensusNetMessage::TimeoutCertificate(..)
        };

        assert_eq!(node1.consensus.bft_round_state.view, 2);
        assert_eq!(node2.consensus.bft_round_state.view, 2);
        assert_eq!(node3.consensus.bft_round_state.view, 2);
        assert_eq!(node4.consensus.bft_round_state.view, 2);
    }

    #[test_log::test(tokio::test)]
    async fn test_timeout_multi_view_only_see_new_prepare() {
        // Same as test_timeout_multi_view, but node 4 will not see timeouts, just the TC as part of the next prepare.
        let (mut node1, mut node2, mut node3, mut node4): (
            ConsensusTestCtx,
            ConsensusTestCtx,
            ConsensusTestCtx,
            ConsensusTestCtx,
        ) = build_nodes!(4).await;

        assert_eq!(node1.consensus.bft_round_state.view, 0);
        assert_eq!(node2.consensus.bft_round_state.view, 0);
        assert_eq!(node3.consensus.bft_round_state.view, 0);
        assert_eq!(node4.consensus.bft_round_state.view, 0);

        ConsensusTestCtx::timeout(&mut [&mut node1, &mut node2, &mut node3]).await;

        // Skip node 4
        broadcast! {
            description: "Follower - Timeout",
            from: node1, to: [node2, node3],
            message_matches: ConsensusNetMessage::Timeout(..)
        };
        broadcast! {
            description: "Follower - Timeout",
            from: node2, to: [node1, node3],
            message_matches: ConsensusNetMessage::Timeout(..)
        };
        broadcast! {
            description: "Follower - Timeout",
            from: node3, to: [node2, node1],
            message_matches: ConsensusNetMessage::Timeout(..)
        };

        node1.assert_broadcast("TC").await;
        // Node 2 won't TC as it's next leader
        //node2.assert_broadcast("TC").await;
        node3.assert_broadcast("TC").await;

        assert_eq!(node1.consensus.bft_round_state.view, 1);
        assert_eq!(node2.consensus.bft_round_state.view, 1);
        assert_eq!(node3.consensus.bft_round_state.view, 1);
        assert_eq!(node4.consensus.bft_round_state.view, 0);

        ConsensusTestCtx::timeout(&mut [&mut node1, &mut node2, &mut node3]).await;

        // Skip node 4
        broadcast! {
            description: "Follower - Timeout",
            from: node1, to: [node2, node3],
            message_matches: ConsensusNetMessage::Timeout(..)
        };
        broadcast! {
            description: "Follower - Timeout",
            from: node2, to: [node1, node3],
            message_matches: ConsensusNetMessage::Timeout(..)
        };
        broadcast! {
            description: "Follower - Timeout",
            from: node3, to: [node2, node1],
            message_matches: ConsensusNetMessage::Timeout(..)
        };

        node1.assert_broadcast("TC").await;
        node2.assert_broadcast("TC").await;
        // Node 3 won't TC as it's next leader
        //node3.assert_broadcast("TC").await;

        node3.start_round().await;

        broadcast! {
            description: "New prepare",
            from: node3, to: [node1, node2, node4],
            message_matches: ConsensusNetMessage::Prepare(..)
        };

        // And it catches up

        assert_eq!(node1.consensus.bft_round_state.view, 2);
        assert_eq!(node2.consensus.bft_round_state.view, 2);
        assert_eq!(node3.consensus.bft_round_state.view, 2);
        assert_eq!(node4.consensus.bft_round_state.view, 2);
    }

    #[test_log::test(tokio::test)]
    async fn test_timeout_join_mutiny_4() {
        let (mut node1, mut node2, mut node3, mut node4): (
            ConsensusTestCtx,
            ConsensusTestCtx,
            ConsensusTestCtx,
            ConsensusTestCtx,
        ) = build_nodes!(4).await;

        node1.start_round().await;
        // Slot 1 - leader = node1

        // Broadcasted prepare is ignored
        node1.assert_broadcast("Lost prepare").await;

        simple_timeout_round_at_4(&mut node2, &mut node3, &mut node4).await;

        node2.start_round().await;

        // Slot 2 view 1 (following a timeout round)
        let (cp, ticket, cp_view) = simple_commit_round! {
          leader: node2,
          followers: [node1, node3, node4]
        };

        assert!(matches!(ticket, Ticket::TimeoutQC(_, _)));
        assert_eq!(cp.slot, 1);
        assert_eq!(cp_view, 1);
        assert_eq!(cp.parent_hash, b"genesis".into());
    }

    #[test_log::test(tokio::test)]
    async fn test_timeout_join_mutiny_leader_4() {
        let (mut node1, mut node2, mut node3, mut node4): (
            ConsensusTestCtx,
            ConsensusTestCtx,
            ConsensusTestCtx,
            ConsensusTestCtx,
        ) = build_nodes!(4).await;

        node1.start_round().await;
        // Slot 1 - leader = node1

        // Broadcasted prepare is ignored
        node1.assert_broadcast("Lost prepare").await;

        ConsensusTestCtx::timeout(&mut [&mut node3]).await;

        broadcast! {
            description: "Follower - Timeout",
            from: node3, to: [node1, node4],
            message_matches: ConsensusNetMessage::Timeout(..)
        };

        ConsensusTestCtx::timeout(&mut [&mut node2]).await;

        broadcast! {
            description: "Follower - Timeout",
            from: node2, to: [node1, node3],
            message_matches: ConsensusNetMessage::Timeout(..)
        };

        // node 1:leader should join the mutiny
        broadcast! {
            description: "Follower - Timeout",
            from: node1, to: [node2, node3, node4],
            message_matches: ConsensusNetMessage::Timeout(..)
        };

        // After this broadcast, every node has 2f+1 timeouts and can create a timeout certificate

        // Node 2 is next leader, but has not yet a timeout certificate
        node2.assert_no_broadcast("Timeout Certificate 2");

        broadcast! {
            description: "Leader - timeout certificate",
            from: node1, to: [node2, node3, node4],
            message_matches: ConsensusNetMessage::TimeoutCertificate(_, _, _, _)
        };

        // Node2 will use node1's timeout certificate

        node3.assert_broadcast("Timeout Certificate 3").await;
        node4.assert_broadcast("Timeout Certificate 4").await;

        node2.start_round().await;

        // Slot 2 view 1 (following a timeout round)
        let (cp, ticket, cp_view) = simple_commit_round! {
          leader: node2,
          followers: [node1, node3, node4]
        };

        assert!(matches!(ticket, Ticket::TimeoutQC(_, _)));
        assert_eq!(cp.slot, 1);
        assert_eq!(cp_view, 1);
        assert_eq!(cp.parent_hash, b"genesis".into());
    }

    #[test_log::test(tokio::test)]
    async fn test_timeout_rebroadcast() {
        let (_node1, _node2, mut node3, _node4): (
            ConsensusTestCtx,
            ConsensusTestCtx,
            ConsensusTestCtx,
            ConsensusTestCtx,
        ) = build_nodes!(4).await;

        ConsensusTestCtx::timeout(&mut [&mut node3]).await;

        broadcast! {
            description: "Follower - Timeout",
            from: node3, to: [],
            message_matches: ConsensusNetMessage::Timeout((signed_slot_view, _)) => {
                assert_eq!(signed_slot_view.msg.0, 1);
                assert_eq!(signed_slot_view.msg.1, 0);
            }
        };

        ConsensusTestCtx::timeout(&mut [&mut node3]).await;

        broadcast! {
            description: "Follower - Timeout",
            from: node3, to: [],
            message_matches: ConsensusNetMessage::Timeout((signed_slot_view, _)) => {
                assert_eq!(signed_slot_view.msg.0, 1);
                assert_eq!(signed_slot_view.msg.1, 0);
            }
        };
    }

    #[test_log::test(tokio::test)]
    async fn test_timeout_join_mutiny_when_triggering_timeout_4() {
        let (mut node1, _node2, mut node3, mut node4): (
            ConsensusTestCtx,
            ConsensusTestCtx,
            ConsensusTestCtx,
            ConsensusTestCtx,
        ) = build_nodes!(4).await;

        node1.start_round().await;
        // Slot 1 - leader = node1

        // Broadcasted prepare is ignored
        node1.assert_broadcast("Lost prepare").await;

        ConsensusTestCtx::timeout(&mut [&mut node3]).await;

        broadcast! {
            description: "Follower - Timeout",
            from: node3, to: [node4],
            message_matches: ConsensusNetMessage::Timeout((signed_slot_view, _)) => {
                assert_eq!(signed_slot_view.msg.0, 1);
                assert_eq!(signed_slot_view.msg.1, 0);
            }
        };

        ConsensusTestCtx::timeout(&mut [&mut node4]).await;

        node4.assert_broadcast("Timeout Message 4").await;
        node4.assert_no_broadcast("Timeout Certificate 4");
    }

    #[test_log::test(tokio::test)]
    async fn test_timeout_next_leader_build_and_use_its_timeout_certificate() {
        let (mut node1, mut node2, mut node3, mut node4): (
            ConsensusTestCtx,
            ConsensusTestCtx,
            ConsensusTestCtx,
            ConsensusTestCtx,
        ) = build_nodes!(4).await;

        node1.start_round().await;
        // Slot 1 - leader = node1

        // Broadcasted prepare is ignored
        node1.assert_broadcast("Lost prepare").await;

        ConsensusTestCtx::timeout(&mut [&mut node3, &mut node4]).await;

        broadcast! {
            description: "Follower - Timeout",
            from: node3, to: [node1, node2],
            message_matches: ConsensusNetMessage::Timeout((signed_slot_view, _)) => {
                assert_eq!(signed_slot_view.msg.0, 1);
                assert_eq!(signed_slot_view.msg.1, 0);
            }
        };

        // Only node1 current leader receives the timeout message from node4
        // Since it already received a timeout from node3, it enters the mutiny

        broadcast! {
            description: "Follower - Timeout",
            from: node4, to: [node1],
            message_matches: ConsensusNetMessage::Timeout((signed_slot_view, _)) => {
                assert_eq!(signed_slot_view.msg.0, 1);
                assert_eq!(signed_slot_view.msg.1, 0);
            }
        };

        // Node 1 joined the mutiny, and sends its timeout to node2 (next leader) which already has one timeout from node3

        broadcast! {
            description: "Follower - Timeout",
            from: node1, to: [node2],
            message_matches: ConsensusNetMessage::Timeout((signed_slot_view, _)) => {
                assert_eq!(signed_slot_view.msg.0, 1);
                assert_eq!(signed_slot_view.msg.1, 0);
            }
        };

        // Now node2 has 2 timeouts, so it joined the mutiny, and since at 4 nodes joining mutiny == timeout certificate, it is ready for round 2

        // Node 2 is next leader, and does not emits a timeout certificate since it will use it for its next Prepare
        node2.assert_broadcast("Timeout Message 2").await;
        node2.assert_no_broadcast("Timeout Certificate 2");

        node2.start_round().await;

        // Slot 2 view 1 (following a timeout round)
        let (cp, ticket, cp_view) = simple_commit_round! {
          leader: node2,
          followers: [node1, node3, node4]
        };

        assert!(matches!(ticket, Ticket::TimeoutQC(_, _)));
        assert_eq!(cp.slot, 1);
        assert_eq!(cp_view, 1);
        assert_eq!(cp.parent_hash, b"genesis".into());
    }

    #[test_log::test(tokio::test)]
    async fn timeout_only_emit_certificate_once() {
        let (mut node1, mut node2, mut node3, mut node4, mut node5): (
            ConsensusTestCtx,
            ConsensusTestCtx,
            ConsensusTestCtx,
            ConsensusTestCtx,
            ConsensusTestCtx,
        ) = build_nodes!(5).await;

        node1.start_round().await;
        // Slot 1 - leader = node1

        node1.assert_broadcast("Lost prepare").await;

        // Make node2 and node3 timeout, node4 will not timeout but follow mutiny,
        // because at f+1, mutiny join
        ConsensusTestCtx::timeout(&mut [&mut node2, &mut node3]).await;

        broadcast! {
            description: "Follower - Timeout",
            from: node2, to: [node3, node4, node5],
            message_matches: ConsensusNetMessage::Timeout(..)
        };
        broadcast! {
            description: "Follower - Timeout",
            from: node3, to: [node2, node4, node5],
            message_matches: ConsensusNetMessage::Timeout(..)
        };

        // node 4 should join the mutiny
        broadcast! {
            description: "Follower - Timeout",
            from: node4, to: [node2, node3, node5],
            message_matches: ConsensusNetMessage::Timeout((signed_slot_view, _)) => {
                assert_eq!(signed_slot_view.msg.0, 1);
                assert_eq!(signed_slot_view.msg.1, 0);
            }
        };

        // By receiving this, other nodes should not produce another timeout certificate
        broadcast! {
            description: "Follower - Timeout",
            from: node5, to: [node2, node3, node4],
            message_matches: ConsensusNetMessage::Timeout(..)
        };

        // After this broadcast, every node has 2f+1 timeouts and can create a timeout certificate

        // Node 2 is next leader, and emits a timeout certificate it will use to broadcast the next Prepare
        node2.assert_no_broadcast("Timeout Certificate 2");
        node3.assert_broadcast("Timeout Certificate 3").await;
        node4.assert_broadcast("Timeout Certificate 4").await;
        node5.assert_broadcast("Timeout Certificate 5").await;

        // No
        node2.assert_no_broadcast("Timeout certificate 2");
        node3.assert_no_broadcast("Timeout certificate 3");
        node4.assert_no_broadcast("Timeout certificate 4");

        node2.start_round().await;

        // Slot 2 view 1 (following a timeout round)
        let (cp, ticket, cp_view) = simple_commit_round! {
          leader: node2,
          followers: [node1, node3, node4, node5]
        };

        assert!(matches!(ticket, Ticket::TimeoutQC(_, _)));
        assert_eq!(cp.slot, 1);
        assert_eq!(cp_view, 1);
        assert_eq!(cp.parent_hash, b"genesis".into());
    }

    #[test_log::test(tokio::test)]
    async fn timeout_next_leader_receive_timeout_certificate_without_timeouting() {
        let (mut node1, mut node2, mut node3, mut node4, mut node5, mut node6, mut node7): (
            ConsensusTestCtx,
            ConsensusTestCtx,
            ConsensusTestCtx,
            ConsensusTestCtx,
            ConsensusTestCtx,
            ConsensusTestCtx,
            ConsensusTestCtx,
        ) = build_nodes!(7).await;

        node1.start_round().await;

        let lost_prepare = node1
            .assert_broadcast("Lost Prepare slot 1/view 0")
            .await
            .msg;

        // node2 is the next leader, let the others timeout and create a certificate and send it to node2.
        // It should be able to build a prepare message with it

        ConsensusTestCtx::timeout(&mut [
            &mut node3, &mut node4, &mut node5, &mut node6, &mut node7,
        ])
        .await;

        broadcast! {
            description: "Follower - Timeout",
            from: node3, to: [node4, node5, node6, node7],
            message_matches: ConsensusNetMessage::Timeout((signed_slot_view, _)) => {
                assert_eq!(signed_slot_view.msg.0, 1);
                assert_eq!(signed_slot_view.msg.1, 0);
            }
        };
        broadcast! {
            description: "Follower - Timeout",
            from: node4, to: [node3, node5, node6, node7],
            message_matches: ConsensusNetMessage::Timeout(..)
        };
        broadcast! {
            description: "Follower - Timeout",
            from: node5, to: [node3, node4, node6, node7],
            message_matches: ConsensusNetMessage::Timeout(..)
        };
        broadcast! {
            description: "Follower - Timeout",
            from: node6, to: [node3, node4, node5, node7],
            message_matches: ConsensusNetMessage::Timeout(..)
        };
        broadcast! {
            description: "Follower - Timeout",
            from: node7, to: [node3, node4, node5, node6],
            message_matches: ConsensusNetMessage::Timeout(..)
        };

        // Send node5 timeout certificate to node2
        broadcast! {
            description: "Follower - Timeout Certificate to next leader",
            from: node5, to: [node2],
            message_matches: ConsensusNetMessage::TimeoutCertificate(_, _, slot, view) => {
                if let ConsensusNetMessage::Prepare(cp, ticket, prep_view) = lost_prepare {
                    assert_eq!(&cp.slot, slot);
                    assert_eq!(&prep_view, view);
                    assert_eq!(ticket, Ticket::Genesis);
                }
            }
        };

        // Clean timeout certificates
        node3.assert_broadcast("Timeout certificate 3").await;
        node4.assert_broadcast("Timeout certificate 4").await;
        node6.assert_broadcast("Timeout certificate 6").await;
        node7.assert_broadcast("Timeout certificate 7").await;

        node2.start_round().await;

        let (cp, ticket, cp_view) = simple_commit_round! {
            leader: node2,
            followers: [node1, node3, node4, node5, node6, node7]
        };

        assert_eq!(cp.slot, 1);
        assert_eq!(cp_view, 1);
        assert!(matches!(ticket, Ticket::TimeoutQC(_, _)));
        assert_eq!(cp.parent_hash, b"genesis".into());
    }

    #[test_log::test(tokio::test)]
    async fn future_timeout_certificate_keeps_joining() {
        let (mut node1, node2): (ConsensusTestCtx, ConsensusTestCtx) = build_nodes!(2).await;

        node1.consensus.bft_round_state.state_tag = StateTag::Joining;
        node1.consensus.bft_round_state.slot = 1;
        node1.consensus.bft_round_state.view = 0;

        let future_slot = node1.consensus.bft_round_state.slot + 2;

        let msg = node2
            .consensus
            .sign_net_message(ConsensusNetMessage::TimeoutCertificate(
                QuorumCertificate(AggregateSignature::default(), ConsensusTimeoutMarker),
                TCKind::NilProposal(QuorumCertificate(
                    AggregateSignature::default(),
                    NilConsensusTimeoutMarker,
                )),
                future_slot,
                0,
            ))
            .expect("Error while signing");

        let err = node1.handle_msg_err(&msg).await;
        let root = err.root_cause().to_string();
        assert!(
            root.contains("Unverifiable ticket"),
            "TC should fail verification, got: {root}"
        );

        assert!(matches!(
            node1.consensus.bft_round_state.state_tag,
            StateTag::Joining
        ));
        assert_eq!(node1.consensus.bft_round_state.slot, 1);
    }
}

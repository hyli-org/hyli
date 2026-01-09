use anyhow::{bail, Context, Result};
use hyli_model::{DataProposalHash, DataSized, LaneBytesSize, LaneId, ValidatorPublicKey};
use serde::{Deserialize, Serialize};
use tracing::{debug, trace, warn};

use crate::{
    mempool::{DisseminationEvent, MempoolNetMessage, ProcessedDPEvent},
    model::{BlobProofOutput, DataProposal, Hashed, TransactionData},
};

use super::{
    storage::{CanBePutOnTop, Storage},
    verifiers::{verify_proof, verify_recursive_proof},
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DataProposalVerdict {
    Process,
    Empty,
    Wait,
    Vote,
    Refuse,
    Ignore,
}

impl super::Mempool {
    pub(super) fn on_data_proposal(
        &mut self,
        lane_id: &LaneId,
        received_hash: DataProposalHash,
        data_proposal: DataProposal,
    ) -> Result<()> {
        debug!(
            "Received DataProposal {:?} (unchecked) on lane {} ({} txs)",
            received_hash,
            lane_id,
            data_proposal.txs.len(),
        );

        self.metrics.add_received_dp(lane_id);

        // Check if we have a cached response to this DP hash (we can safely trust the hash here)
        // TODO: if we are currently hashing the same DP we'll still re-hash it
        // but this requires a signed header to quickly process the message.
        match self
            .cached_dp_votes
            .get(&(lane_id.clone(), received_hash.clone()))
        {
            // Ignore
            Some(
                DataProposalVerdict::Empty
                | DataProposalVerdict::Refuse
                | DataProposalVerdict::Process
                | DataProposalVerdict::Ignore,
            ) => {
                debug!(
                    "Ignoring DataProposal {:?} on lane {} (cached verdict)",
                    received_hash, lane_id
                );
                return Ok(());
            }
            Some(DataProposalVerdict::Vote) => {
                // Resend our vote
                // First fetch the lane size, if we somehow don't have it ignore.
                if let Ok(lane_size) = self.lanes.get_lane_size_at(lane_id, &received_hash) {
                    debug!(
                        "Resending vote for DataProposal {:?} on lane {}",
                        received_hash, lane_id
                    );
                    return self.send_vote(
                        lane_id,
                        self.get_lane_operator(lane_id),
                        received_hash,
                        lane_size,
                    );
                }
            }
            Some(DataProposalVerdict::Wait) | None => {}
        }

        // This is annoying to run in tests because we don't have the event loop setup, so go synchronous.
        #[cfg(test)]
        {
            // We must verify the hash
            if data_proposal.hashed() != received_hash {
                bail!(
                    "Received DataProposal with wrong hash: expected {:?}, got {:?}",
                    received_hash,
                    data_proposal.hashed()
                );
            }
            self.on_hashed_data_proposal(lane_id, data_proposal.clone())?;
        }
        #[cfg(not(test))]
        {
            let lane_id_clone = lane_id.clone();
            self.inner.processing_dps.spawn_on(
                async move {
                    // We must verify the hash
                    if data_proposal.hashed() != received_hash {
                        bail!(
                            "Received DataProposal with wrong hash: expected {:?}, got {:?}",
                            received_hash,
                            data_proposal.hashed()
                        );
                    }
                    Ok(ProcessedDPEvent::OnHashedDataProposal((
                        lane_id_clone,
                        data_proposal,
                    )))
                },
                self.inner.long_tasks_runtime.handle(),
            );
        }
        Ok(())
    }

    pub(super) fn on_hashed_data_proposal(
        &mut self,
        lane_id: &LaneId,
        mut data_proposal: DataProposal,
    ) -> Result<()> {
        debug!(
            "Hashing done for DataProposal {:?} on lane {} ({} txs, {})",
            data_proposal.hashed(),
            lane_id,
            data_proposal.txs.len(),
            data_proposal.estimate_size()
        );
        self.metrics.add_hashed_dp(lane_id);

        let data_proposal_hash = data_proposal.hashed();
        let (verdict, lane_size) = self.get_verdict(lane_id, &data_proposal)?;
        self.cached_dp_votes.insert(
            (lane_id.clone(), data_proposal_hash.clone()),
            verdict.clone(),
        );
        match verdict {
            DataProposalVerdict::Empty => {
                warn!(
                    "received empty DataProposal on lane {}, ignoring...",
                    lane_id
                );
            }
            DataProposalVerdict::Vote => {
                // Normal case, we receive a proposal we already have the parent in store
                trace!("Send vote for DataProposal");
                #[allow(clippy::unwrap_used, reason = "we always have a size for Vote")]
                self.send_vote(
                    lane_id,
                    self.get_lane_operator(lane_id),
                    data_proposal_hash,
                    lane_size.unwrap(),
                )?;
            }
            DataProposalVerdict::Process => {
                trace!("Further processing for DataProposal");
                let lane_id = lane_id.clone();
                self.inner.processing_dps.spawn_on(
                    async move {
                        let decision = Self::process_data_proposal(&mut data_proposal);
                        Ok(ProcessedDPEvent::OnProcessedDataProposal((
                            lane_id,
                            decision,
                            data_proposal,
                        )))
                    },
                    self.inner.long_tasks_runtime.handle(),
                );
            }
            DataProposalVerdict::Wait => {
                debug!("Buffering DataProposal {}", data_proposal_hash);
                // Push the data proposal in the waiting list
                self.buffered_proposals
                    .entry(lane_id.clone())
                    .or_default()
                    .insert(data_proposal);
            }
            DataProposalVerdict::Refuse => {
                debug!("Refuse vote for DataProposal {}", data_proposal.hashed());
            }
            DataProposalVerdict::Ignore => {
                debug!("Ignore DataProposal {}", data_proposal_hash);
            }
        }
        Ok(())
    }

    pub(super) fn on_processed_data_proposal(
        &mut self,
        lane_id: LaneId,
        verdict: DataProposalVerdict,
        data_proposal: DataProposal,
    ) -> Result<()> {
        debug!(
            "Handling processed DataProposal {:?} one lane {} ({} txs)",
            data_proposal.hashed(),
            lane_id,
            data_proposal.txs.len()
        );

        self.metrics.add_processed_dp(&lane_id);

        self.cached_dp_votes
            .insert((lane_id.clone(), data_proposal.hashed()), verdict.clone());
        match verdict {
            DataProposalVerdict::Empty => {
                unreachable!("Empty DataProposal should never be processed");
            }
            DataProposalVerdict::Process => {
                unreachable!("DataProposal has already been processed");
            }
            DataProposalVerdict::Wait => {
                unreachable!("DataProposal has already been processed");
            }
            DataProposalVerdict::Vote => {
                trace!("Send vote for DataProposal");
                let crypto = self.crypto.clone();
                let (hash, size) =
                    self.lanes
                        .store_data_proposal(&crypto, &lane_id, data_proposal)?;
                self.send_dissemination_event(DisseminationEvent::DpStored {
                    lane_id: lane_id.clone(),
                    data_proposal_hash: hash.clone(),
                    cumul_size: size,
                })?;
                self.send_vote(
                    &lane_id,
                    self.get_lane_operator(&lane_id),
                    hash.clone(),
                    size,
                )?;

                while let Some(poda_signatures) = self
                    .inner
                    .buffered_podas
                    .get_mut(&lane_id)
                    .and_then(|lane| lane.get_mut(&hash))
                    .and_then(|podas_list| podas_list.pop())
                {
                    self.on_poda_update(&lane_id, &hash, poda_signatures)
                        .context("Processing buffered poda")?;
                }

                // Check if we maybe buffered a descendant of this DP.
                let mut dp = None;
                if let Some(buffered_proposals) = self.buffered_proposals.get_mut(&lane_id) {
                    // Check if we have a buffered proposal that is a child of this DP
                    let child_idx = buffered_proposals.iter().position(|dp| {
                        if let Some(parent_hash) = &dp.parent_data_proposal_hash {
                            parent_hash == &hash
                        } else {
                            false
                        }
                    });
                    if let Some(child_idx) = child_idx {
                        // We have a buffered proposal that is a child of this DP, process it.
                        // (I _would_ use a HashSet, but this requires https://github.com/rust-lang/rust/issues/59618
                        // which is coming in 1.88)
                        dp = buffered_proposals.swap_remove_index(child_idx);
                    }
                }
                if let Some(dp) = dp {
                    // We can process this DP
                    debug!(
                        "Processing buffered DataProposal {:?} on lane {}",
                        dp.hashed(),
                        lane_id
                    );
                    self.on_hashed_data_proposal(&lane_id, dp)?;
                }
            }
            DataProposalVerdict::Refuse => {
                debug!("Refuse vote for DataProposal");
            }
            DataProposalVerdict::Ignore => {
                debug!("Ignore DataProposal {}", data_proposal.hashed());
            }
        }
        Ok(())
    }

    fn get_verdict(
        &mut self,
        lane_id: &LaneId,
        data_proposal: &DataProposal,
    ) -> Result<(DataProposalVerdict, Option<LaneBytesSize>)> {
        // Check that data_proposal is not empty
        if data_proposal.txs.is_empty() {
            return Ok((DataProposalVerdict::Empty, None));
        }

        let dp_hash = data_proposal.hashed();

        // ALREADY STORED
        if self.lanes.contains(lane_id, &dp_hash) {
            let lane_size = self.lanes.get_lane_size_at(lane_id, &dp_hash)?;
            // just resend a vote
            return Ok((DataProposalVerdict::Vote, Some(lane_size)));
        }

        match self.lanes.can_be_put_on_top(
            lane_id,
            &dp_hash,
            data_proposal.parent_data_proposal_hash.as_ref(),
        ) {
            // PARENT UNKNOWN
            CanBePutOnTop::No => {
                // Get the last known parent hash in order to get all the next ones
                Ok((DataProposalVerdict::Wait, None))
            }
            // LEGIT DATA PROPOSAL
            CanBePutOnTop::Yes => Ok((DataProposalVerdict::Process, None)),
            CanBePutOnTop::Fork => {
                // FORK DETECTED
                let last_known_hash = self.lanes.get_lane_hash_tip(lane_id);
                warn!(
                    "DataProposal ({dp_hash}) cannot be handled because it creates a fork: last dp hash {:?} while proposed {:?}",
                    last_known_hash,
                    data_proposal.parent_data_proposal_hash
                );
                Ok((DataProposalVerdict::Refuse, None))
            }
            CanBePutOnTop::AlreadyOnTop => {
                // Lane tip was updated (via commit) before this proposal arrived, so we ignore it.
                Ok((DataProposalVerdict::Ignore, None))
            }
        }
    }

    fn process_data_proposal(data_proposal: &mut DataProposal) -> DataProposalVerdict {
        for tx in &data_proposal.txs {
            match &tx.transaction_data {
                TransactionData::Blob(_) => {
                    // Accepting all blob transactions
                    // TODO: find out what we want to do here
                }
                TransactionData::Proof(_) => {
                    warn!("Refusing DataProposal: unverified recursive proof transaction");
                    return DataProposalVerdict::Refuse;
                }
                TransactionData::VerifiedProof(proof_tx) => {
                    // TODO: figure out what we want to do with the contracts.
                    // Extract the proof
                    let proof = match &proof_tx.proof {
                        Some(proof) => proof,
                        None => {
                            warn!("Refusing DataProposal: proof is missing");
                            return DataProposalVerdict::Refuse;
                        }
                    };
                    let verifier = &proof_tx.verifier;
                    let program_id = &proof_tx.program_id;
                    // TODO: figure out how to generalize this
                    let is_recursive = proof_tx.contract_name.0 == "risc0-recursion";

                    if is_recursive {
                        match verify_recursive_proof(proof, verifier, program_id) {
                            Ok((local_program_ids, local_hyli_outputs)) => {
                                let data_matches = local_program_ids
                                    .iter()
                                    .zip(local_hyli_outputs.iter())
                                    .zip(proof_tx.proven_blobs.iter())
                                    .all(
                                        |(
                                            (local_program_id, local_hyli_output),
                                            BlobProofOutput {
                                                program_id,
                                                hyli_output,
                                                ..
                                            },
                                        )| {
                                            local_hyli_output == hyli_output
                                                && local_program_id == program_id
                                        },
                                    );
                                if local_program_ids.len() != proof_tx.proven_blobs.len()
                                    || !data_matches
                                {
                                    warn!("Refusing DataProposal: incorrect HyliOutput in proof transaction");
                                    return DataProposalVerdict::Refuse;
                                }
                            }
                            Err(e) => {
                                warn!("Refusing DataProposal: invalid recursive proof transaction: {}", e);
                                return DataProposalVerdict::Refuse;
                            }
                        }
                    } else {
                        match verify_proof(proof, verifier, program_id) {
                            Ok(outputs) => {
                                // TODO: we could check the blob hash here too.
                                if outputs.len() != proof_tx.proven_blobs.len()
                                    && std::iter::zip(outputs.iter(), proof_tx.proven_blobs.iter())
                                        .any(|(output, BlobProofOutput { hyli_output, .. })| {
                                            output != hyli_output
                                        })
                                {
                                    warn!("Refusing DataProposal: incorrect HyliOutput in proof transaction");
                                    return DataProposalVerdict::Refuse;
                                }
                            }
                            Err(e) => {
                                warn!("Refusing DataProposal: invalid proof transaction: {}", e);
                                return DataProposalVerdict::Refuse;
                            }
                        }
                    }
                }
            }
        }

        // Remove proofs from transactions
        Self::remove_proofs(data_proposal);

        DataProposalVerdict::Vote
    }

    /// Remove proofs from all transactions in the DataProposal
    fn remove_proofs(dp: &mut DataProposal) {
        dp.remove_proofs();
    }

    fn send_vote(
        &mut self,
        lane_id: &LaneId,
        validator: &ValidatorPublicKey,
        data_proposal_hash: DataProposalHash,
        size: LaneBytesSize,
    ) -> Result<()> {
        self.metrics
            .add_dp_vote(self.crypto.validator_pubkey(), validator);
        debug!("🗳️ Sending vote for DataProposal {data_proposal_hash} to {validator} (lane size: {size})");
        self.send_net_message(
            validator.clone(),
            MempoolNetMessage::DataVote(
                lane_id.clone(),
                self.crypto.sign((data_proposal_hash, size))?,
            ),
        )?;
        Ok(())
    }
}

#[cfg(test)]
pub mod test {
    use super::*;
    use crate::{
        mempool::{
            tests::{make_register_contract_tx, MempoolTestCtx},
            MempoolNetMessage,
        },
        p2p::network::HeaderSigner,
    };
    use hyli_crypto::BlstCrypto;
    use hyli_model::{ContractName, DataProposalHash, SignedByValidator, Transaction};

    #[test_log::test(tokio::test)]
    async fn test_get_verdict() {
        let mut ctx = MempoolTestCtx::new("mempool").await;
        let crypto2: BlstCrypto = BlstCrypto::new("2").unwrap();
        let lane_id2 = LaneId::new(crypto2.validator_pubkey().clone());

        let dp = DataProposal::new(None, vec![]);
        // 2 send a DP to 1
        let (verdict, _) = ctx.mempool.get_verdict(&lane_id2, &dp).unwrap();
        assert_eq!(verdict, DataProposalVerdict::Empty);

        let dp = DataProposal::new(None, vec![Transaction::default()]);
        let (verdict, _) = ctx.mempool.get_verdict(&lane_id2, &dp).unwrap();
        assert_eq!(verdict, DataProposalVerdict::Process);

        let dp_unknown_parent = DataProposal::new(
            Some(DataProposalHash::default()),
            vec![Transaction::default()],
        );
        let (verdict, _) = ctx
            .mempool
            .get_verdict(&lane_id2, &dp_unknown_parent)
            .unwrap();
        assert_eq!(verdict, DataProposalVerdict::Wait);
    }

    #[test_log::test(tokio::test)]
    async fn test_get_verdict_fork() {
        let mut ctx = MempoolTestCtx::new("mempool").await;
        let crypto2: BlstCrypto = BlstCrypto::new("2").unwrap();
        let lane_id2 = LaneId::new(crypto2.validator_pubkey().clone());

        let dp = DataProposal::new(None, vec![Transaction::default()]);
        let dp2 = DataProposal::new(Some(dp.hashed()), vec![Transaction::default()]);

        ctx.mempool
            .lanes
            .store_data_proposal(&ctx.mempool.crypto, &lane_id2, dp.clone())
            .unwrap();
        ctx.mempool
            .lanes
            .store_data_proposal(&ctx.mempool.crypto, &lane_id2, dp2.clone())
            .unwrap();

        assert!(ctx
            .mempool
            .lanes
            .store_data_proposal(&ctx.mempool.crypto, &lane_id2, dp2)
            .is_err());

        let dp2_fork = DataProposal::new(
            Some(dp.hashed()),
            vec![Transaction::default(), Transaction::default()],
        );

        let (verdict, _) = ctx.mempool.get_verdict(&lane_id2, &dp2_fork).unwrap();
        assert_eq!(verdict, DataProposalVerdict::Refuse);
    }

    #[test_log::test(tokio::test)]
    async fn test_receiving_data_proposal() -> Result<()> {
        let mut ctx = MempoolTestCtx::new("mempool").await;

        let data_proposal = ctx.create_data_proposal(
            None,
            &[make_register_contract_tx(ContractName::new("test1"))],
        );
        let size = LaneBytesSize(data_proposal.estimate_size() as u64);
        let hash = data_proposal.hashed();
        let lane_id = LaneId::new(ctx.mempool.crypto.validator_pubkey().clone());

        let signed_msg =
            ctx.mempool
                .crypto
                .sign_msg_with_header(MempoolNetMessage::DataProposal(
                    lane_id,
                    hash.clone(),
                    data_proposal.clone(),
                ))?;

        ctx.mempool
            .handle_net_message(signed_msg)
            .await
            .expect("should handle net message");

        ctx.handle_processed_data_proposals().await;

        // Assert that we vote for that specific DataProposal
        match ctx
            .assert_send(&ctx.mempool.crypto.validator_pubkey().clone(), "DataVote")
            .await
            .msg
        {
            MempoolNetMessage::DataVote(
                _,
                SignedByValidator {
                    msg: (data_vote, voted_size),
                    ..
                },
            ) => {
                assert_eq!(data_vote, hash);
                assert_eq!(size, voted_size);
            }
            _ => panic!("Expected DataProposal message"),
        };
        Ok(())
    }
}

use std::collections::HashMap;

use crate::{
    bus::BusClientSender, consensus::CommittedConsensusProposal,
    mempool::storage::EntryOrMissingHash, model::*,
};
use futures::StreamExt;
use hyli_modules::{log_error, log_warn};

use super::{
    storage::{LaneEntryMetadata, MetadataOrMissingHash, Storage},
    DisseminationEvent,
};
use anyhow::{bail, Context, Result};
use borsh::{BorshDeserialize, BorshSerialize};
use tracing::{debug, error, trace, warn};

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct BlockUnderConstruction {
    pub from: Option<Cut>,
    pub ccp: CommittedConsensusProposal,
    pub holes_tops: HashMap<LaneId, DataProposalHash>,
    pub holes_materialized: bool,
}

impl super::Mempool {
    pub(super) async fn on_sync_reply(
        &mut self,
        lane_id: &LaneId,
        sender_validator: &ValidatorPublicKey,
        metadata: LaneEntryMetadata,
        data_proposal: DataProposal,
    ) -> Result<()> {
        debug!("SyncReply from validator {sender_validator}");

        if metadata.parent_data_proposal_hash != data_proposal.parent_data_proposal_hash {
            bail!(
                "SyncReply parent hash mismatch for lane {}: metadata {:?} vs data proposal {:?}",
                lane_id,
                metadata.parent_data_proposal_hash,
                data_proposal.parent_data_proposal_hash
            );
        }

        self.metrics
            .sync_reply_receive(lane_id, self.crypto.validator_pubkey());

        #[cfg(test)]
        {
            let dp_hash = data_proposal.hashed();
            self.on_hashed_sync_reply(lane_id.clone(), metadata, data_proposal, dp_hash)
                .await?;
        }
        #[cfg(not(test))]
        {
            let lane_id_clone = lane_id.clone();
            self.inner.processing_dps.spawn_on(
                async move {
                    let dp_hash = data_proposal.hashed();
                    Ok(crate::mempool::ProcessedDPEvent::OnHashedSyncReply((
                        lane_id_clone,
                        metadata,
                        data_proposal,
                        dp_hash,
                    )))
                },
                self.inner.long_tasks_runtime.handle(),
            );
        }

        Ok(())
    }

    pub(super) async fn on_hashed_sync_reply(
        &mut self,
        lane_id: LaneId,
        metadata: LaneEntryMetadata,
        data_proposal: DataProposal,
        dp_hash: DataProposalHash,
    ) -> Result<()> {
        // We don't check if we already have stored the DP just in case
        // we processed it async between hole materialization and now.

        // We checked metadata parent / dp parent hash consistency earlier so skip this now.

        // Check that the lane operator signed these entries
        // TODO: this feels technically un-necessary as we have committed them already?

        let lane_operator = self.get_lane_operator(&lane_id);
        let expected_message = (dp_hash.clone(), metadata.cumul_size);
        let missing_entry_not_present = !metadata
            .signatures
            .iter()
            .any(|s| &s.signature.validator == lane_operator && s.msg == expected_message);

        if missing_entry_not_present {
            bail!(
                "At least one lane entry is missing signature from {}",
                lane_operator
            );
        }

        trace!(
            "Filling hole with 1 entry (parent dp hash: {:?}) for {lane_id}",
            metadata.parent_data_proposal_hash
        );

        // Check if we can find the BUC this belongs to.
        // For borrowing + practicality reasons, it's easier
        // to do this loop pop / push.
        for _ in 0..self.blocks_under_contruction.len() {
            if let Some(mut buc) = self.blocks_under_contruction.pop_front() {
                let hole_top = buc.holes_tops.get(&lane_id);
                if hole_top == Some(&dp_hash) {
                    // This uses a non-recursive loop to avoid stack overflow,
                    // unfortunately results in rather cumbersome code to write.
                    self.try_to_fill_hole_in_lane(
                        &mut buc,
                        &lane_id,
                        metadata,
                        data_proposal,
                        dp_hash.clone(),
                    )
                    .await?;

                    let can_send = buc.holes_tops.is_empty();
                    self.blocks_under_contruction.push_back(buc);

                    // If we no longer have holes, try to send the built block
                    if can_send {
                        self.try_to_send_full_signed_blocks()
                            .await
                            .context("Try process queued CCP")?;
                    }
                    return Ok(());
                } else {
                    self.blocks_under_contruction.push_back(buc);
                }
            }
        }

        // If we're here, we didn't find the BUC, so buffer.
        debug!(
            "Buffering SyncReply entry for lane {} with hash {}",
            lane_id, dp_hash
        );
        self.buffered_sync_replies
            .entry(lane_id.clone())
            .or_default()
            .insert(dp_hash.clone(), (metadata, data_proposal));

        Ok(())
    }

    async fn try_to_fill_hole_in_lane(
        &mut self,
        buc: &mut BlockUnderConstruction,
        lane_id: &LaneId,
        mut metadata: LaneEntryMetadata,
        mut data_proposal: DataProposal,
        mut dp_hash: DataProposalHash,
    ) -> Result<()> {
        let mut hole_top = buc.holes_tops.get(lane_id);
        while Some(&dp_hash) == hole_top {
            // Move the hole top down
            if let Some(ref parent_hash) = metadata.parent_data_proposal_hash {
                // If we reached the 'from' cut, remove the hole
                if buc.from.as_ref().is_some_and(|from_cut| {
                    from_cut.iter().any(|(from_lane_id, from_dp_hash, _, _)| {
                        from_lane_id == lane_id && from_dp_hash == parent_hash
                    })
                }) {
                    buc.holes_tops.remove(lane_id);
                    hole_top = None;
                } else {
                    let r = buc.holes_tops.entry(lane_id.clone()).or_default();
                    *r = parent_hash.clone();
                    hole_top = Some(r);
                }
            } else {
                buc.holes_tops.remove(lane_id); // Just remove otherwise
                hole_top = None;
            }
            let cumul_size = metadata.cumul_size;
            self.lanes
                .put_no_verification(lane_id.clone(), (metadata, data_proposal))?;
            Self::send_dissemination_event_over(
                &mut self.bus,
                DisseminationEvent::DpStored {
                    lane_id: lane_id.clone(),
                    data_proposal_hash: dp_hash.clone(),
                    cumul_size,
                },
            )?;
            Self::send_dissemination_event_over(
                &mut self.bus,
                DisseminationEvent::SyncRequestProgress {
                    lane_id: lane_id.clone(),
                    old_to: dp_hash.clone(),
                    new_to: hole_top.cloned(),
                },
            )?;
            debug!(
                "Filled hole for lane {} in BUC(slot: {})",
                lane_id, buc.ccp.consensus_proposal.slot
            );

            // Try to fill further holes
            let Some(ht) = hole_top else {
                break;
            };
            let Some(buffered) = self.inner.buffered_sync_replies.get_mut(lane_id) else {
                break;
            };
            let Some((m, dp)) = buffered.remove(ht) else {
                break;
            };
            dp_hash = ht.clone();
            metadata = m;
            data_proposal = dp;
        }
        Ok(())
    }

    pub(super) async fn try_to_send_full_signed_blocks(&mut self) -> Result<()> {
        let length = self.blocks_under_contruction.len();
        for _ in 0..length {
            if let Some(mut block_under_contruction) = self.blocks_under_contruction.pop_front() {
                if log_warn!(
                    self.build_signed_block_and_emit(&mut block_under_contruction)
                        .await,
                    "Processing queued committedConsensusProposal"
                )
                .is_err()
                {
                    // if failure, we push the ccp at the end
                    self.blocks_under_contruction
                        .push_back(block_under_contruction);
                }
            }
        }

        Ok(())
    }

    /// Materializes holes for the Block under construction.
    /// If data is not available locally, records pending holes.
    /// Returns true if any hole was recorded.
    async fn materialize_holes_for_signed_block(
        &mut self,
        buc: &mut BlockUnderConstruction,
    ) -> Result<bool> {
        trace!(
            "Materializing holes for Block Under Construction {:?}",
            buc.clone()
        );
        debug!(
            "Materializing holes for Block Under Construction {} from parent hash {}",
            buc.ccp.consensus_proposal.slot, buc.ccp.consensus_proposal.parent_hash
        );

        let mut any_missing = false;

        for (lane_id, to_hash, _, _) in buc.ccp.consensus_proposal.cut.iter() {
            trace!("Processing lane {} with to_hash {}", lane_id, to_hash);
            let from_hash = buc
                .from
                .as_ref()
                .and_then(|f| f.iter().find(|el| &el.0 == lane_id))
                .map(|el| &el.1);

            // iterate over the lane entries between from_hash and to_hash of the lane
            trace!("Fetching data proposals for lane {}", lane_id);

            // TODO: explicit "get top" function?
            // Because we pass an explicit to_hash, this will always return at least one entry.
            let mut entries = Box::pin(self.lanes.get_entries_metadata_between_hashes(
                lane_id,
                from_hash.cloned(),
                Some(to_hash.clone()),
            ));
            while let Some(entry) = entries.next().await {
                trace!("Processing lane entry {:?}", entry);
                match entry {
                    Ok(MetadataOrMissingHash::Metadata(_, _)) => {}
                    Ok(MetadataOrMissingHash::MissingHash(hash)) => {
                        debug!(
                            "Data proposal {} not available locally for lane {}",
                            hash, lane_id
                        );
                        // Record missing hash as hole top
                        buc.holes_tops.insert(lane_id.clone(), hash);
                        any_missing = true;
                        break;
                    }
                    Err(_) => {
                        debug!(
                            "Lane {lane_id} missing data between {:?} and {:?}",
                            from_hash, to_hash
                        );
                        // Record the to_hash as hole top
                        buc.holes_tops.insert(lane_id.clone(), to_hash.clone());
                        any_missing = true;
                        break;
                    }
                }
            }
        }
        buc.holes_materialized = true;
        Ok(any_missing)
    }

    /// Retrieves data proposals matching the Block under construction.
    /// Assumes that holes have been materialized and filled.
    async fn get_full_data_for_signed_block(
        &mut self,
        buc: &BlockUnderConstruction,
    ) -> Result<Vec<(LaneId, Vec<DataProposal>)>> {
        let mut result = vec![];

        for (lane_id, to_hash, _, _) in buc.ccp.consensus_proposal.cut.iter() {
            let from_hash = buc
                .from
                .as_ref()
                .and_then(|f| f.iter().find(|el| &el.0 == lane_id))
                .map(|el| &el.1);

            let mut dps = vec![];

            // Because we pass an explicit to_hash, this will always return at least one entry.
            let mut entries = Box::pin(self.lanes.get_entries_between_hashes(
                lane_id,
                from_hash.cloned(),
                Some(to_hash.clone()),
            ));

            while let Some(entry) = entries.next().await {
                match entry {
                    Ok(EntryOrMissingHash::Entry(_, mut dp)) => {
                        dp.remove_proofs();
                        dps.push(dp);
                    }
                    Ok(EntryOrMissingHash::MissingHash(hash)) => {
                        bail!("Unexpected missing data proposal {hash} for lane {lane_id}");
                    }
                    Err(e) => {
                        bail!(
                            "Lane entries from {:?} to {:?} not available locally: {e}",
                            buc.from,
                            buc.ccp.consensus_proposal.cut,
                        );
                    }
                }
            }
            // Reverse to maintain the correct order (most recent first)
            dps.reverse();

            result.push((lane_id.clone(), dps));
        }

        Ok(result)
    }

    pub async fn build_signed_block_and_emit(
        &mut self,
        buc: &mut BlockUnderConstruction,
    ) -> Result<()> {
        // First time, materialize holes (this is done somewhat async as it can be slow,
        // but we might want to refactor this in the future.)
        if !buc.holes_materialized && self.materialize_holes_for_signed_block(buc).await? {
            // Borrowing makes this unworkable without the vec workaround.
            let mut fill_from = vec![];
            for (lane_id, to_hash) in &buc.holes_tops {
                if let Some((metadata, data_proposal)) = self
                    .buffered_sync_replies
                    .get_mut(lane_id)
                    .and_then(|m| m.remove(to_hash))
                {
                    fill_from.push((lane_id.clone(), metadata, data_proposal, to_hash.clone()));
                }
            }
            for (lane_id, metadata, data_proposal, to_hash) in fill_from {
                self.try_to_fill_hole_in_lane(buc, &lane_id, metadata, data_proposal, to_hash)
                    .await?;
            }
            // Now send sync requests for remaining holes.
            // For simplicity I'll just re-loop here.
            for (lane_id, to_hash) in &buc.holes_tops {
                debug!(
                    "Still have hole for lane {} at top {} in BUC(slot: {})",
                    lane_id, to_hash, buc.ccp.consensus_proposal.slot
                );
                self.send_sync_request(
                    lane_id,
                    buc.from.as_ref().and_then(|from_cut| {
                        from_cut
                            .iter()
                            .find(|(from_lane_id, ..)| from_lane_id == lane_id)
                            .map(|(_, from_dp_hash, ..)| from_dp_hash)
                    }),
                    Some(to_hash),
                )?;
            }
        }

        if !buc.holes_tops.is_empty() {
            bail!("Block under construction has pending holes");
        }

        let mut block_data = self
            .get_full_data_for_signed_block(buc)
            .await
            .context("Processing queued committedConsensusProposal")?;

        self.metrics.constructed_block.add(1, &[]);

        debug!(
            "🚧 Built signed block for slot {} with {} data proposals",
            buc.ccp.consensus_proposal.slot,
            block_data.len()
        );

        // Delete stored proofs for all committed DataProposals - we don't need them anymore
        for (lane_id, dps) in &mut block_data {
            for dp in dps {
                self.lanes.delete_proofs(lane_id, &dp.hashed())?;
            }
        }

        self.bus
            .send(MempoolBlockEvent::BuiltSignedBlock(SignedBlock {
                data_proposals: block_data,
                certificate: buc.ccp.certificate.clone(),
                consensus_proposal: buc.ccp.consensus_proposal.clone(),
            }))?;

        Ok(())
    }

    /// Send an event if none was broadcast before
    fn set_ccp_build_start_height(&mut self, slot: Slot) {
        if self.buc_build_start_height.is_none()
            && log_error!(
                self.bus
                    .send(MempoolBlockEvent::StartedBuildingBlocks(BlockHeight(slot))),
                "Sending StartedBuilding event at height {}",
                slot
            )
            .is_ok()
        {
            self.buc_build_start_height = Some(slot);
        }
    }

    pub(super) fn try_create_block_under_construction(&mut self, ccp: CommittedConsensusProposal) {
        if let Some(last_buc) = self.last_ccp.take() {
            // CCP slot too old compared with the last we processed, weird, CCP should come in the right order
            if last_buc.consensus_proposal.slot >= ccp.consensus_proposal.slot {
                let last_buc_slot = last_buc.consensus_proposal.slot;
                self.last_ccp = Some(last_buc);
                error!("CommitConsensusProposal is older than the last processed CCP slot {} should be higher than {}, not updating last_ccp", last_buc_slot, ccp.consensus_proposal.slot);
                return;
            }

            self.last_ccp = Some(ccp.clone());

            // Matching the next slot
            if last_buc.consensus_proposal.slot == ccp.consensus_proposal.slot - 1 {
                tracing::trace!(
                    "Creating interval from slot {} to {}",
                    last_buc.consensus_proposal.slot,
                    ccp.consensus_proposal.slot
                );

                self.set_ccp_build_start_height(ccp.consensus_proposal.slot);

                self.blocks_under_contruction
                    .push_back(BlockUnderConstruction {
                        from: Some(last_buc.consensus_proposal.cut.clone()),
                        ccp: ccp.clone(),
                        holes_tops: HashMap::new(),
                        holes_materialized: false,
                    });
            } else {
                // CCP slot received is way higher, then just store it
                warn!("Could not create an interval, because incoming ccp slot {} should be {}+1 (last_ccp)", ccp.consensus_proposal.slot, last_buc.consensus_proposal.slot);
            }
        }
        // No last ccp
        else {
            // Update the last ccp with the received ccp, either we create a block or not.
            self.last_ccp = Some(ccp.clone());

            if ccp.consensus_proposal.slot == 1 {
                self.set_ccp_build_start_height(ccp.consensus_proposal.slot);
                // If no last cut, make sure the slot is 1
                self.blocks_under_contruction
                    .push_back(BlockUnderConstruction {
                        from: None,
                        ccp,
                        holes_tops: HashMap::new(),
                        holes_materialized: false,
                    });
            } else {
                debug!(
                    "Could not create an interval with CCP(slot: {})",
                    ccp.consensus_proposal.slot
                );
            }
        }
    }

    /// Requests all DP between the previous Cut and the new Cut.
    pub(super) fn clean_and_update_lanes(
        &mut self,
        cut: &Cut,
        previous_cut: &Option<Cut>,
    ) -> Result<()> {
        for (lane_id, data_proposal_hash, cumul_size, _) in cut.iter() {
            if !self.lanes.contains(lane_id, data_proposal_hash) {
                // We want to start from the lane tip, and remove all DP until we find the data proposal of the previous cut
                let previous_committed_dp_hash = previous_cut
                    .as_ref()
                    .and_then(|cut| cut.iter().find(|(v, _, _, _)| v == lane_id))
                    .map(|(_, h, _, _)| h);
                if previous_committed_dp_hash == Some(data_proposal_hash) {
                    // No cut have been made for this validator; we keep the DPs
                    continue;
                }
                // Removes all DP after the previous cut & update lane_tip with new cut
                self.lanes.clean_and_update_lane(
                    lane_id,
                    previous_committed_dp_hash,
                    data_proposal_hash,
                    cumul_size,
                )?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
pub mod test {
    use staking::state::Staking;
    use utils::TimestampMs;

    use crate::mempool::MempoolNetMessage;
    use crate::tests::autobahn_testing::assert_chanmsg_matches;

    use super::super::tests::*;
    use super::*;

    #[test_log::test(tokio::test)]
    async fn signed_block_basic() -> Result<()> {
        let mut ctx = MempoolTestCtx::new("mempool").await;

        // Store a DP, process the commit message for the cut containing it.
        let register_tx = make_register_contract_tx(ContractName::new("test1"));
        let dp_orig = ctx.create_data_proposal(None, std::slice::from_ref(&register_tx));
        ctx.process_new_data_proposal(dp_orig.clone())?;
        let cumul_size = LaneBytesSize(dp_orig.estimate_size() as u64);
        let dp_hash = dp_orig.hashed();

        let key = ctx.validator_pubkey().clone();
        ctx.add_trusted_validator(&key).await;

        let cut = ctx
            .process_cut_with_dp(&key, &dp_hash, cumul_size, 1)
            .await?;

        assert_chanmsg_matches!(
            ctx.mempool_event_receiver,
            MempoolBlockEvent::StartedBuildingBlocks(height) => {
                assert_eq!(height, BlockHeight(1));
            }
        );

        assert_chanmsg_matches!(
            ctx.mempool_event_receiver,
            MempoolBlockEvent::BuiltSignedBlock(sb) => {
                assert_eq!(sb.consensus_proposal.cut, cut);
                assert_eq!(
                    sb.data_proposals,
                    vec![(LaneId::new(key.clone()), vec![dp_orig])]
                );
            }
        );

        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn proofs_deleted_after_commit() -> Result<()> {
        use crate::model::{
            BlobProofOutput, ContractName, HyliOutput, ProgramId, ProofData, ProofDataHash,
            Transaction, TransactionData, VerifiedProofTransaction, Verifier,
        };

        let mut ctx = MempoolTestCtx::new("mempool").await;

        // Create a DP with a VerifiedProof tx containing an inlined proof
        let proof = ProofData(vec![1, 2, 3, 4, 5]);
        let proof_hash = ProofDataHash(proof.hashed().0);
        let vpt = VerifiedProofTransaction {
            contract_name: ContractName::new("cleanup-proof"),
            program_id: ProgramId(vec![]),
            verifier: Verifier("test".into()),
            proof: Some(proof.clone()),
            proof_hash: proof_hash.clone(),
            proof_size: proof.0.len(),
            proven_blobs: vec![BlobProofOutput {
                original_proof_hash: proof_hash,
                blob_tx_hash: crate::model::TxHash("blob-tx".into()),
                program_id: ProgramId(vec![]),
                verifier: Verifier("test".into()),
                hyli_output: HyliOutput::default(),
            }],
            is_recursive: false,
        };
        let dp = ctx.create_data_proposal(
            None,
            &[Transaction::from(TransactionData::VerifiedProof(vpt))],
        );
        let dp_hash = dp.hashed();
        let cumul_size = LaneBytesSize(dp.estimate_size() as u64);

        // Store it locally; this strips proofs into side-store
        ctx.process_new_data_proposal(dp.clone())?;

        let lane_id = LaneId::new(ctx.validator_pubkey().clone());
        // Ensure proofs exist before commit
        let proofs_before = ctx
            .mempool
            .lanes
            .get_proofs_by_hash(&lane_id, &dp_hash)?
            .expect("proofs should be present before commit");
        assert_eq!(proofs_before.len(), 1);

        // Process a cut committing this DP
        let key = ctx.validator_pubkey().clone();
        ctx.add_trusted_validator(&key).await;
        let cut = ctx
            .process_cut_with_dp(&key, &dp_hash, cumul_size, 1)
            .await?;

        // Wait for BuiltSignedBlock as a sanity check
        assert_chanmsg_matches!(
            ctx.mempool_event_receiver,
            MempoolBlockEvent::StartedBuildingBlocks(_) => {}
        );
        assert_chanmsg_matches!(
            ctx.mempool_event_receiver,
            MempoolBlockEvent::BuiltSignedBlock(sb) => {
                assert_eq!(sb.consensus_proposal.cut, cut);
            }
        );

        // Proofs should be removed from storage after block build
        let proofs_after = ctx.mempool.lanes.get_proofs_by_hash(&lane_id, &dp_hash)?;
        assert!(
            proofs_after.is_none(),
            "proofs must be deleted after commit"
        );

        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn signed_block_data_proposals_in_order() -> Result<()> {
        let mut ctx = MempoolTestCtx::new("mempool").await;

        // Store a DP, process the commit message for the cut containing it.
        let register_tx = make_register_contract_tx(ContractName::new("test1"));
        let dp_orig = ctx.create_data_proposal(None, std::slice::from_ref(&register_tx));
        ctx.process_new_data_proposal(dp_orig.clone())?;
        let cumul_size = LaneBytesSize(dp_orig.estimate_size() as u64);
        let dp_hash = dp_orig.hashed();

        let register_tx2 = make_register_contract_tx(ContractName::new("test2"));
        let dp_orig2 =
            ctx.create_data_proposal(Some(dp_hash.clone()), std::slice::from_ref(&register_tx2));
        ctx.process_new_data_proposal(dp_orig2.clone())?;
        let cumul_size = LaneBytesSize(cumul_size.0 + dp_orig2.estimate_size() as u64);
        let dp_hash2 = dp_orig2.hashed();

        let register_tx3 = make_register_contract_tx(ContractName::new("test3"));
        let dp_orig3 =
            ctx.create_data_proposal(Some(dp_hash2.clone()), std::slice::from_ref(&register_tx3));
        ctx.process_new_data_proposal(dp_orig3.clone())?;
        let cumul_size = LaneBytesSize(cumul_size.0 + dp_orig3.estimate_size() as u64);
        let dp_hash3 = dp_orig3.hashed();

        let key = ctx.validator_pubkey().clone();
        ctx.add_trusted_validator(&key).await;

        let cut = ctx
            .process_cut_with_dp(&key, &dp_hash3, cumul_size, 1)
            .await?;

        assert_chanmsg_matches!(
            ctx.mempool_event_receiver,
            MempoolBlockEvent::StartedBuildingBlocks(height) => {
                assert_eq!(height, BlockHeight(1));
            }
        );

        assert_chanmsg_matches!(
            ctx.mempool_event_receiver,
            MempoolBlockEvent::BuiltSignedBlock(sb) => {
                assert_eq!(sb.consensus_proposal.cut, cut);
                assert_eq!(
                    sb.data_proposals,
                    vec![(LaneId::new(key.clone()), vec![dp_orig, dp_orig2, dp_orig3])]
                );
            }
        );

        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn signed_block_start_building_later() -> Result<()> {
        let mut ctx = MempoolTestCtx::new("mempool").await;

        let dp2_size = LaneBytesSize(20);
        let dp2_hash = DataProposalHash("dp2".to_string());
        let dp5_size = LaneBytesSize(50);
        let dp5_hash = DataProposalHash("dp5".to_string());
        let dp6_size = LaneBytesSize(60);
        let dp6_hash = DataProposalHash("dp6".to_string());

        let ctx_key = ctx.validator_pubkey().clone();
        let expect_nothing = |ctx: &mut MempoolTestCtx| {
            ctx.mempool_event_receiver
                .try_recv()
                .expect_err("Should not build signed block");
        };

        ctx.process_cut_with_dp(&ctx_key, &dp2_hash, dp2_size, 2)
            .await?;
        expect_nothing(&mut ctx);

        ctx.process_cut_with_dp(&ctx_key, &dp5_hash, dp5_size, 5)
            .await?;
        expect_nothing(&mut ctx);

        // Process it twice to check idempotency
        ctx.process_cut_with_dp(&ctx_key, &dp5_hash, dp5_size, 5)
            .await?;
        expect_nothing(&mut ctx);

        // Process the old one again as well
        ctx.process_cut_with_dp(&ctx_key, &dp2_hash, dp2_size, 2)
            .await?;
        expect_nothing(&mut ctx);

        // Finally process two consecutive ones
        ctx.process_cut_with_dp(&ctx_key, &dp6_hash, dp6_size, 6)
            .await?;

        assert_chanmsg_matches!(
            ctx.mempool_event_receiver,
            MempoolBlockEvent::StartedBuildingBlocks(height) => {
                assert_eq!(height, BlockHeight(6));
            }
        );

        Ok(())
    }

    #[allow(clippy::indexing_slicing)]
    #[test_log::test(tokio::test)]
    async fn test_signed_buffer_block_ccp() -> Result<()> {
        let mut ctx = MempoolTestCtx::new("mempool").await;
        let mut ctx2 = MempoolTestCtx::new("mempool2").await;

        let crypto1 = ctx.mempool.crypto.clone();
        let crypto2 = ctx2.mempool.crypto.clone();

        let lane_id1 = ctx.mempool.own_lane_id().clone();
        let lane_id2 = ctx2.mempool.own_lane_id().clone();
        ctx.add_trusted_validator(crypto2.validator_pubkey()).await;

        // Create a chain of 2 DataProposals in lane 1
        let dp1 = DataProposal::new(None, vec![]);
        let dp1_hash = dp1.hashed();
        ctx.mempool
            .lanes
            .store_data_proposal(&crypto1, &lane_id1, dp1.clone())?;

        let dp2 = DataProposal::new(Some(dp1_hash.clone()), vec![]);
        let dp2_hash = dp2.hashed();
        ctx.mempool
            .lanes
            .store_data_proposal(&crypto1, &lane_id1, dp2.clone())?;

        // Create a chain of 2 DataProposals in lane 2
        let dp3 = DataProposal::new(None, vec![]);
        let dp3_hash = dp3.hashed();
        ctx2.mempool
            .lanes
            .store_data_proposal(&crypto2, &lane_id2, dp3.clone())?;

        let dp4 = DataProposal::new(Some(dp3_hash.clone()), vec![]);
        let dp4_hash = dp4.hashed();
        ctx2.mempool
            .lanes
            .store_data_proposal(&crypto2, &lane_id2, dp4.clone())?;

        // Create a ConsensusProposal that references both lanes
        let ccp = ConsensusProposal {
            slot: 1,
            cut: vec![
                (
                    lane_id1.clone(),
                    dp2_hash,
                    LaneBytesSize(100),
                    AggregateSignature::default(),
                ),
                (
                    lane_id2.clone(),
                    dp4_hash.clone(),
                    LaneBytesSize(100),
                    AggregateSignature::default(),
                ),
            ],
            staking_actions: vec![],
            timestamp: TimestampMs(0),
            parent_hash: ConsensusProposalHash("test".to_string()),
        };

        // Add the block to mempool 1
        let mut buc = BlockUnderConstruction {
            from: None,
            ccp: CommittedConsensusProposal {
                consensus_proposal: ccp,
                staking: Staking::default(),
                certificate: AggregateSignature::default(),
            },
            holes_tops: HashMap::new(),
            holes_materialized: false,
        };

        // Try to build a signed block in mempool 1
        let result = ctx.mempool.build_signed_block_and_emit(&mut buc).await;
        assert!(result.is_err());

        ctx.process_sync().await?;

        assert!(
            ctx.mempool_event_receiver.try_recv().is_err(),
            "Should not have started building blocks yet"
        );

        // Verify that a sync request was sent for lane 2's DataProposals
        match ctx
            .assert_send(
                &ctx2.mempool.crypto.validator_pubkey().clone(),
                "SyncRequest",
            )
            .await
            .msg
        {
            MempoolNetMessage::SyncRequest(lane_id, from, to) => {
                assert_eq!(lane_id, lane_id2);
                assert_eq!(from, None);
                assert_eq!(to, Some(dp4_hash.clone()));
            }
            _ => panic!("Expected SyncRequest message"),
        }

        // Send the DataProposals from lane 2 to mempool 1
        ctx.mempool
            .lanes
            .store_data_proposal(&crypto2, &lane_id2, dp3.clone())?;
        ctx.mempool
            .lanes
            .store_data_proposal(&crypto2, &lane_id2, dp4.clone())?;

        // Try to build a signed block again
        let result = ctx.mempool.build_signed_block_and_emit(&mut buc).await;
        assert!(result.is_ok());

        // We've received consecutive blocks so start building
        assert_chanmsg_matches!(
            ctx.mempool_event_receiver,
            MempoolBlockEvent::BuiltSignedBlock(signed_block) => {
                assert_eq!(signed_block.data_proposals.len(), 2);
                assert_eq!(
                    signed_block.data_proposals[0],
                    (LaneId::new(crypto1.validator_pubkey().clone()), vec![dp1, dp2])
                );
                assert_eq!(
                    signed_block.data_proposals[1],
                    (LaneId::new(crypto2.validator_pubkey().clone()), vec![dp3, dp4])
                );
            }
        );

        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn test_sync_missing_dp() -> Result<()> {
        let mut ctx = MempoolTestCtx::new("mempool").await;
        let ctx_owner = MempoolTestCtx::new("mempool_owner").await;
        let lane_id = ctx_owner.mempool.own_lane_id().clone();
        let crypto = ctx_owner.mempool.crypto.clone();
        ctx.add_trusted_validator(crypto.validator_pubkey()).await;

        // Create a chain of 3 DataProposals
        let dp1 = DataProposal::new(None, vec![]);
        let dp1_hash = dp1.hashed();
        ctx.mempool
            .lanes
            .store_data_proposal(&crypto, &lane_id, dp1)?;

        let dp2 = DataProposal::new(Some(dp1_hash.clone()), vec![]);
        let dp2_hash = dp2.hashed();
        ctx.mempool
            .lanes
            .store_data_proposal(&crypto, &lane_id, dp2)?;

        let dp3 = DataProposal::new(Some(dp2_hash.clone()), vec![]);
        let dp3_hash = dp3.hashed();
        ctx.mempool
            .lanes
            .store_data_proposal(&crypto, &lane_id, dp3)?;

        // Remove dp2 to simulate a missing DataProposal
        ctx.mempool.lanes.remove_lane_entry(&lane_id, &dp2_hash);

        // Create a ConsensusProposal that references dp3
        let ccp = ConsensusProposal {
            slot: 1,
            cut: vec![(
                lane_id.clone(),
                dp3_hash,
                LaneBytesSize(100),
                AggregateSignature::default(),
            )],
            staking_actions: vec![],
            timestamp: TimestampMs(0),
            parent_hash: ConsensusProposalHash("test".to_string()),
        };

        // Add the block to the mempool
        let mut buc = BlockUnderConstruction {
            from: None,
            ccp: CommittedConsensusProposal {
                consensus_proposal: ccp,
                staking: Staking::default(),
                certificate: AggregateSignature::default(),
            },
            holes_tops: HashMap::new(),
            holes_materialized: false,
        };

        // Try to build a signed block
        let result = ctx.mempool.build_signed_block_and_emit(&mut buc).await;
        assert!(result.is_err());

        ctx.process_sync().await?;

        match ctx
            .assert_send(
                &ctx_owner.mempool.crypto.validator_pubkey().clone(),
                "SyncRequest",
            )
            .await
            .msg
        {
            MempoolNetMessage::SyncRequest(req_lane_id, from, to) => {
                assert_eq!(req_lane_id, lane_id);
                assert_eq!(from, None);
                assert_eq!(to, Some(dp2_hash.clone()));
            }
            _ => panic!("Expected SyncRequest message"),
        };

        Ok(())
    }
}

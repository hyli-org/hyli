use crate::{
    bus::BusClientSender, consensus::CommittedConsensusProposal,
    mempool::storage::EntryOrMissingHash, model::*,
};
use futures::StreamExt;
use hyle_modules::{log_error, log_warn};

use super::storage::Storage;
use anyhow::{bail, Context, Result};
use borsh::{BorshDeserialize, BorshSerialize};
use tracing::{debug, error, trace, warn};

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct BlockUnderConstruction {
    pub from: Option<Cut>,
    pub ccp: CommittedConsensusProposal,
}

impl super::Mempool {
    pub(super) async fn try_to_send_full_signed_blocks(&mut self) -> Result<()> {
        let length = self.blocks_under_contruction.len();
        for _ in 0..length {
            if let Some(block_under_contruction) = self.blocks_under_contruction.pop_front() {
                if log_warn!(
                    self.build_signed_block_and_emit(&block_under_contruction)
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

    /// Retrieves data proposals matching the Block under construction.
    /// If data is not available locally, fails and do nothing
    async fn try_get_full_data_for_signed_block(
        &mut self,
        buc: &BlockUnderConstruction,
    ) -> Result<Vec<(LaneId, Vec<DataProposal>)>> {
        trace!("Handling Block Under Construction {:?}", buc.clone());
        debug!(
            "Handling Block Under Construction {} from parent hash {}",
            buc.ccp.consensus_proposal.slot, buc.ccp.consensus_proposal.parent_hash
        );

        let mut result = vec![];

        for (lane_id, to_hash, _, _) in buc.ccp.consensus_proposal.cut.iter() {
            trace!("Processing lane {} with to_hash {}", lane_id, to_hash);
            let from_hash = buc
                .from
                .as_ref()
                .and_then(|f| f.iter().find(|el| &el.0 == lane_id))
                .map(|el| &el.1);

            // iterate over the lane entries between from_hash and to_hash of the lane
            let (dps, missing_hash) = {
                trace!("Fetching data proposals for lane {}", lane_id);
                let mut missing_hash = None;
                let mut dps = vec![];

                let mut entries = Box::pin(self.lanes.get_entries_between_hashes(
                    lane_id,
                    from_hash.cloned(),
                    Some(to_hash.clone()),
                ));

                while let Some(entry) = entries.next().await {
                    trace!("Processing lane entry {:?}", entry);
                    let entry_or_missing = entry.context(format!(
                        "Lane entries from {:?} to {:?} not available locally",
                        buc.from, buc.ccp.consensus_proposal.cut
                    ))?;

                    match entry_or_missing {
                        EntryOrMissingHash::Entry(_, dp) => dps.insert(0, dp),
                        EntryOrMissingHash::MissingHash(hash) => {
                            debug!(
                                "Data proposal {} not available locally for lane {}",
                                hash, lane_id
                            );
                            missing_hash = Some(hash.clone());
                            break;
                        }
                    }
                }

                (dps, missing_hash)
            };

            if let Some(missing_hash) = missing_hash {
                self.send_sync_request(lane_id, from_hash, Some(&missing_hash))?;
                bail!(
                    "Data proposal {} not available locally for lane {}",
                    missing_hash,
                    lane_id
                );
            }

            result.push((lane_id.clone(), dps));
        }

        Ok(result)
    }

    pub async fn build_signed_block_and_emit(
        &mut self,
        buc: &BlockUnderConstruction,
    ) -> Result<()> {
        let block_data = self
            .try_get_full_data_for_signed_block(buc)
            .await
            .context("Processing queued committedConsensusProposal")?;

        self.metrics.constructed_block.add(1, &[]);

        debug!(
            "🚧 Built signed block for slot {} with {} data proposals",
            buc.ccp.consensus_proposal.slot,
            block_data.len()
        );

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
                    .push_back(BlockUnderConstruction { from: None, ccp });
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

                // Send SyncRequest for all data proposals between previous cut and new one
                self.send_sync_request(
                    lane_id,
                    previous_committed_dp_hash,
                    Some(data_proposal_hash),
                )
                .context("Fetching unknown data")?;
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

    use super::super::test::*;
    use super::*;

    #[test_log::test(tokio::test)]
    async fn signed_block_basic() -> Result<()> {
        let mut ctx = MempoolTestCtx::new("mempool").await;

        // Store a DP, process the commit message for the cut containing it.
        let register_tx = make_register_contract_tx(ContractName::new("test1"));
        let dp_orig = ctx.create_data_proposal(None, &[register_tx.clone()]);
        ctx.process_new_data_proposal(dp_orig.clone())?;
        let cumul_size = LaneBytesSize(dp_orig.estimate_size() as u64);
        let dp_hash = dp_orig.hashed();

        let key = ctx.validator_pubkey().clone();
        ctx.add_trusted_validator(&key);

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
                    vec![(LaneId(key.clone()), vec![dp_orig])]
                );
            }
        );

        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn signed_block_data_proposals_in_order() -> Result<()> {
        let mut ctx = MempoolTestCtx::new("mempool").await;

        // Store a DP, process the commit message for the cut containing it.
        let register_tx = make_register_contract_tx(ContractName::new("test1"));
        let dp_orig = ctx.create_data_proposal(None, &[register_tx.clone()]);
        ctx.process_new_data_proposal(dp_orig.clone())?;
        let cumul_size = LaneBytesSize(dp_orig.estimate_size() as u64);
        let dp_hash = dp_orig.hashed();

        let register_tx2 = make_register_contract_tx(ContractName::new("test2"));
        let dp_orig2 = ctx.create_data_proposal(Some(dp_hash.clone()), &[register_tx2.clone()]);
        ctx.process_new_data_proposal(dp_orig2.clone())?;
        let cumul_size = LaneBytesSize(cumul_size.0 + dp_orig2.estimate_size() as u64);
        let dp_hash2 = dp_orig2.hashed();

        let register_tx3 = make_register_contract_tx(ContractName::new("test3"));
        let dp_orig3 = ctx.create_data_proposal(Some(dp_hash2.clone()), &[register_tx3.clone()]);
        ctx.process_new_data_proposal(dp_orig3.clone())?;
        let cumul_size = LaneBytesSize(cumul_size.0 + dp_orig3.estimate_size() as u64);
        let dp_hash3 = dp_orig3.hashed();

        let key = ctx.validator_pubkey().clone();
        ctx.add_trusted_validator(&key);

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
                    vec![(LaneId(key.clone()), vec![dp_orig, dp_orig2, dp_orig3])]
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
        let buc = BlockUnderConstruction {
            from: None,
            ccp: CommittedConsensusProposal {
                consensus_proposal: ccp,
                staking: Staking::default(),
                certificate: AggregateSignature::default(),
            },
        };

        // Try to build a signed block in mempool 1
        let result = ctx.mempool.build_signed_block_and_emit(&buc).await;
        assert!(result.is_err());

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
            MempoolNetMessage::SyncRequest(from, to) => {
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
        let result = ctx.mempool.build_signed_block_and_emit(&buc).await;
        assert!(result.is_ok());

        // We've received consecutive blocks so start building
        assert_chanmsg_matches!(
            ctx.mempool_event_receiver,
            MempoolBlockEvent::BuiltSignedBlock(signed_block) => {
                assert_eq!(signed_block.data_proposals.len(), 2);
                assert_eq!(
                    signed_block.data_proposals[0],
                    (LaneId(crypto1.validator_pubkey().clone()), vec![dp1, dp2])
                );
                assert_eq!(
                    signed_block.data_proposals[1],
                    (LaneId(crypto2.validator_pubkey().clone()), vec![dp3, dp4])
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
        let buc = BlockUnderConstruction {
            from: None,
            ccp: CommittedConsensusProposal {
                consensus_proposal: ccp,
                staking: Staking::default(),
                certificate: AggregateSignature::default(),
            },
        };

        // Try to build a signed block
        let result = ctx.mempool.build_signed_block_and_emit(&buc).await;
        assert!(result.is_err());

        match ctx
            .assert_send(
                &ctx_owner.mempool.crypto.validator_pubkey().clone(),
                "SyncRequest",
            )
            .await
            .msg
        {
            MempoolNetMessage::SyncRequest(from, to) => {
                assert_eq!(from, None);
                assert_eq!(to, Some(dp2_hash.clone()));
            }
            _ => panic!("Expected DataProposal message"),
        };

        Ok(())
    }
}

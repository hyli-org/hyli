use crate::{bus::BusClientSender, consensus::CommittedConsensusProposal, model::*};
use futures::StreamExt;
use hyle_modules::log_error;

use super::storage::Storage;
use anyhow::{Context, Result};
use borsh::{BorshDeserialize, BorshSerialize};
use tracing::{debug, error, warn};

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
                if self
                    .build_signed_block_and_emit(&block_under_contruction)
                    .await
                    .context("Processing queued committedConsensusProposal")
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
        &self,
        buc: &BlockUnderConstruction,
    ) -> Result<Vec<(LaneId, Vec<DataProposal>)>> {
        debug!("Handling Block Under Construction {:?}", buc.clone());

        let mut result = vec![];
        // Try to return the asked data proposals between the last_processed_cut and the one being handled
        for (lane_id, to_hash, _, _) in buc.ccp.consensus_proposal.cut.iter() {
            // FIXME: use from : &Cut instead of Option
            let from_hash = buc
                .from
                .as_ref()
                .and_then(|f| f.iter().find(|el| &el.0 == lane_id))
                .map(|el| &el.1);

            let mut entries = Box::pin(self.lanes.get_entries_between_hashes(
                lane_id, // get start hash for validator
                from_hash.cloned(),
                Some(to_hash.clone()),
            ));

            let mut dps = vec![];

            while let Some(entry) = entries.next().await {
                let (_, dp) = entry.context(format!(
                    "Lane entries from {:?} to {:?} not available locally",
                    buc.from, buc.ccp.consensus_proposal.cut
                ))?;

                dps.insert(0, dp);
            }

            result.push((lane_id.clone(), dps));
        }

        Ok(result)
    }

    async fn build_signed_block_and_emit(&mut self, buc: &BlockUnderConstruction) -> Result<()> {
        let block_data = self
            .try_get_full_data_for_signed_block(buc)
            .await
            .context("Processing queued committedConsensusProposal")?;

        self.metrics.constructed_block.add(1, &[]);

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
                debug!(
                    "Creating interval from slot {} to {}",
                    last_buc.consensus_proposal.slot, ccp.consensus_proposal.slot
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
    use utils::TimestampMs;

    use crate::consensus::ConsensusEvent;
    use crate::mempool::storage::LaneEntryMetadata;
    use crate::mempool::MempoolNetMessage;
    use crate::tests::autobahn_testing::assert_chanmsg_matches;
    use hyle_crypto::BlstCrypto;

    use super::super::test::*;
    use super::*;
    use crate::model;

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

    #[test_log::test(tokio::test)]
    async fn signed_block_buffer_ccp() -> Result<()> {
        let mut ctx = MempoolTestCtx::new("mempool").await;

        let dp1 = DataProposal::new(None, vec![]);
        let dp1_size = LaneBytesSize(dp1.estimate_size() as u64);
        let dp1_hash = dp1.hashed();
        let dp1b = DataProposal::new(None, vec![make_register_contract_tx("toto".into())]);
        let dp1b_size = LaneBytesSize(dp1b.estimate_size() as u64);
        let dp1b_hash = dp1b.hashed();
        let dp2_size = LaneBytesSize(60);
        let dp2_hash = DataProposalHash("dp2".to_string());

        let crypto2 = BlstCrypto::new("2").unwrap();

        // This simulates a cut where we somehow don't have our own DP and another DP (they also happen to be the same hash)
        let cut1 = vec![
            (
                ctx.mempool.get_lane(ctx.validator_pubkey()),
                dp1_hash.clone(),
                dp1_size,
                AggregateSignature::default(),
            ),
            (
                ctx.mempool.get_lane(crypto2.validator_pubkey()),
                dp1b_hash.clone(),
                dp1b_size,
                AggregateSignature::default(),
            ),
        ];

        ctx.mempool
            .handle_consensus_event(ConsensusEvent::CommitConsensusProposal(
                CommittedConsensusProposal {
                    staking: ctx.mempool.staking.clone(),
                    consensus_proposal: model::ConsensusProposal {
                        slot: 1,
                        cut: cut1.clone(),
                        staking_actions: vec![],
                        timestamp: TimestampMs(777),
                        parent_hash: ConsensusProposalHash("test".to_string()),
                    },
                    certificate: AggregateSignature::default(),
                },
            ))
            .await?;

        // We've received consecutive blocks so start building
        assert_chanmsg_matches!(
            ctx.mempool_event_receiver,
            MempoolBlockEvent::StartedBuildingBlocks(height) => {
                assert_eq!(height, BlockHeight(1));
            }
        );

        ctx.mempool_event_receiver
            .try_recv()
            .expect_err("Should not build signed block");

        // Now handle a second cut, one dp down
        let cut2 = vec![
            (
                ctx.mempool.get_lane(ctx.validator_pubkey()),
                dp2_hash.clone(),
                dp2_size,
                AggregateSignature::default(),
            ),
            (
                ctx.mempool.get_lane(crypto2.validator_pubkey()),
                dp2_hash.clone(),
                dp2_size,
                AggregateSignature::default(),
            ),
        ];

        ctx.mempool
            .handle_consensus_event(ConsensusEvent::CommitConsensusProposal(
                CommittedConsensusProposal {
                    staking: ctx.mempool.staking.clone(),
                    consensus_proposal: model::ConsensusProposal {
                        slot: 2,
                        cut: cut2.clone(),
                        staking_actions: vec![],
                        timestamp: TimestampMs(888),
                        parent_hash: ConsensusProposalHash("test".to_string()),
                    },
                    certificate: AggregateSignature::default(),
                },
            ))
            .await?;

        // We don't have the data so we still don't send anything.
        ctx.mempool_event_receiver
            .try_recv()
            .expect_err("Should not build signed block");

        // We send sync requests - we don't have the data.
        match ctx
            .assert_send(&ctx.validator_pubkey().clone(), "SyncRequest")
            .await
            .msg
        {
            MempoolNetMessage::SyncRequest(from, to) => {
                assert_eq!(from, None);
                assert_eq!(to, Some(dp1_hash.clone()));
            }
            _ => panic!("Expected DataProposal message"),
        };
        match ctx
            .assert_send(&crypto2.validator_pubkey().clone(), "SyncRequest")
            .await
            .msg
        {
            MempoolNetMessage::SyncRequest(from, to) => {
                assert_eq!(from, None);
                assert_eq!(to, Some(dp1b_hash.clone()));
            }
            _ => panic!("Expected DataProposal message"),
        };
        match ctx
            .assert_send(&ctx.validator_pubkey().clone(), "SyncRequest")
            .await
            .msg
        {
            MempoolNetMessage::SyncRequest(from, to) => {
                assert_eq!(from, Some(dp1_hash.clone()));
                assert_eq!(to, Some(dp2_hash.clone()));
            }
            _ => panic!("Expected DataProposal message"),
        };
        match ctx
            .assert_send(&crypto2.validator_pubkey().clone(), "SyncRequest")
            .await
            .msg
        {
            MempoolNetMessage::SyncRequest(from, to) => {
                assert_eq!(from, Some(dp1b_hash.clone()));
                assert_eq!(to, Some(dp2_hash.clone()));
            }
            _ => panic!("Expected DataProposal message"),
        };

        // Receive the two DPs.

        ctx.mempool
            .on_sync_reply(
                &ctx.validator_pubkey().clone(),
                LaneEntryMetadata {
                    parent_data_proposal_hash: dp1.parent_data_proposal_hash.clone(),
                    cumul_size: dp1_size,
                    signatures: vec![ctx
                        .mempool
                        .crypto
                        .sign((dp1_hash, dp1_size))
                        .expect("should sign")],
                },
                dp1.clone(),
            )
            .await?;

        // We don't have the data so we still don't send anything.
        ctx.mempool_event_receiver
            .try_recv()
            .expect_err("Should not build signed block");

        ctx.mempool
            .on_sync_reply(
                &crypto2.validator_pubkey().clone(),
                LaneEntryMetadata {
                    parent_data_proposal_hash: dp1b.parent_data_proposal_hash.clone(),
                    cumul_size: dp1b_size,
                    signatures: vec![crypto2.sign((dp1b_hash, dp1b_size)).expect("should sign")],
                },
                dp1b.clone(),
            )
            .await?;

        assert_chanmsg_matches!(
            ctx.mempool_event_receiver,
            MempoolBlockEvent::BuiltSignedBlock(sb) => {
                assert_eq!(sb.consensus_proposal.cut, cut1);
                assert_eq!(
                    sb.data_proposals,
                    vec![
                        (ctx.mempool.get_lane(ctx.validator_pubkey()), vec![dp1.clone()]),
                        (ctx.mempool.get_lane(crypto2.validator_pubkey()), vec![dp1b.clone()]
                    )]
                );
            }
        );

        // We don't have the data for the second one so no further messages.
        ctx.mempool_event_receiver
            .try_recv()
            .expect_err("Should not build signed block");

        Ok(())
    }
}
